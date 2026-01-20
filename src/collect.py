from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# /sports/{sport_key}/odds supports these (v4)
ALLOWED_ODDS_MARKETS = {"h2h", "spreads", "totals", "outrights"}

# /sports/{sport_key}/events/{event_id}/odds supports these (for our use)
ALLOWED_EVENT_MARKETS = {"alternate_totals", "totals", "h2h", "spreads"}


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def sanitize_markets_for_odds_endpoint(markets: str) -> str:
    parts = [_norm(x) for x in (markets or "").split(",") if _norm(x)]
    keep = [p for p in parts if p in ALLOWED_ODDS_MARKETS]
    if not keep:
        return "totals"
    out: list[str] = []
    seen = set()
    for k in keep:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return ",".join(out)


def sanitize_markets_for_event_endpoint(markets: str) -> str:
    parts = [_norm(x) for x in (markets or "").split(",") if _norm(x)]
    keep = [p for p in parts if p in ALLOWED_EVENT_MARKETS]
    if not keep:
        return "alternate_totals"
    out: list[str] = []
    seen = set()
    for k in keep:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return ",".join(out)


def fetch_pl_matches(fd_token: str, days_back: int, days_forward: int) -> dict:
    today = datetime.now(timezone.utc).date()
    date_from = (today - timedelta(days=days_back)).isoformat()
    date_to = (today + timedelta(days=days_forward)).isoformat()

    r = requests.get(
        f"{FOOTBALL_DATA_BASE}/competitions/PL/matches",
        headers={"X-Auth-Token": fd_token},
        params={"dateFrom": date_from, "dateTo": date_to},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def upsert_fixtures(con, matches_json: dict) -> int:
    rows = []
    now_iso = utcnow_iso()

    for m in matches_json.get("matches", []):
        fixture_id = str(m["id"])
        commence = m.get("utcDate")
        status = m.get("status", "UNKNOWN")
        matchday = m.get("matchday")
        home = (m.get("homeTeam") or {}).get("name") or "UNKNOWN_HOME"
        away = (m.get("awayTeam") or {}).get("name") or "UNKNOWN_AWAY"
        score = m.get("score") or {}
        full = score.get("fullTime") or {}
        hg = full.get("home")
        ag = full.get("away")

        rows.append((fixture_id, commence, matchday, status, home, away, hg, ag, now_iso))

    executemany(
        con,
        """
        INSERT INTO fixtures (
          fixture_id, commence_time_utc, matchweek, status, home_team, away_team,
          home_goals, away_goals, last_updated_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fixture_id) DO UPDATE SET
          commence_time_utc=excluded.commence_time_utc,
          matchweek=excluded.matchweek,
          status=excluded.status,
          home_team=excluded.home_team,
          away_team=excluded.away_team,
          home_goals=excluded.home_goals,
          away_goals=excluded.away_goals,
          last_updated_utc=excluded.last_updated_utc
        """,
        rows,
    )
    return len(rows)


def fetch_odds_list(
    odds_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> list[dict]:
    markets_clean = sanitize_markets_for_odds_endpoint(markets)
    r = requests.get(
        f"{ODDS_API_BASE}/sports/{sport_key}/odds",
        params={
            "apiKey": odds_key,
            "regions": regions,
            "markets": markets_clean,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def fetch_event_odds(
    odds_key: str,
    sport_key: str,
    event_id: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> dict:
    markets_clean = sanitize_markets_for_event_endpoint(markets)
    r = requests.get(
        f"{ODDS_API_BASE}/sports/{sport_key}/events/{event_id}/odds",
        params={
            "apiKey": odds_key,
            "regions": regions,
            "markets": markets_clean,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _find_fixture_id(con, commence: str, home: str, away: str) -> str | None:
    cur = con.cursor()
    got = cur.execute(
        """
        SELECT fixture_id FROM fixtures
        WHERE commence_time_utc = ?
          AND lower(home_team) = ?
          AND lower(away_team) = ?
        LIMIT 1
        """,
        (commence, _norm(home), _norm(away)),
    ).fetchone()
    if not got:
        return None
    return got["fixture_id"]


def _extract_totals_rows(fixture_id: str, bookmakers: list[dict], captured_at: str, market_key: str) -> list[tuple]:
    rows: list[tuple] = []

    for bm in bookmakers:
        bm_title = bm.get("title") or bm.get("key") or "unknown_book"

        for mk in bm.get("markets", []):
            if mk.get("key") != market_key:
                continue

            by_line: dict[float, dict[str, float]] = {}

            for out in mk.get("outcomes", []):
                point = out.get("point")
                name = _norm(out.get("name"))
                price = out.get("price")
                if point is None or price is None:
                    continue

                try:
                    line = float(point)
                    pr = float(price)
                except (TypeError, ValueError):
                    continue

                if line not in by_line:
                    by_line[line] = {}
                if name in ("over", "under"):
                    by_line[line][name] = pr

            for line, ou in by_line.items():
                if "over" in ou and "under" in ou:
                    rows.append(
                        (
                            captured_at,
                            fixture_id,
                            bm_title,
                            market_key,
                            line,
                            ou["over"],
                            ou["under"],
                        )
                    )

    return rows


def store_totals_from_odds_list(con, events: list[dict], captured_at: str) -> int:
    all_rows: list[tuple] = []

    for ev in events:
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not (commence and home and away):
            continue

        fixture_id = _find_fixture_id(con, commence, home, away)
        if not fixture_id:
            continue

        # odds list endpoint returns bookmakers list directly on the event object
        all_rows.extend(_extract_totals_rows(fixture_id, ev.get("bookmakers", []), captured_at, "totals"))

    executemany(
        con,
        """
        INSERT INTO odds_snapshots (
          captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        all_rows,
    )
    return len(all_rows)


def store_alternate_totals_from_event(con, event_payload: dict, captured_at: str) -> int:
    # event endpoint payload structure: has "commence_time", "home_team", "away_team", and "bookmakers"
    commence = event_payload.get("commence_time")
    home = event_payload.get("home_team")
    away = event_payload.get("away_team")
    if not (commence and home and away):
        return 0

    fixture_id = _find_fixture_id(con, commence, home, away)
    if not fixture_id:
        return 0

    rows = _extract_totals_rows(
        fixture_id=fixture_id,
        bookmakers=event_payload.get("bookmakers", []),
        captured_at=captured_at,
        market_key="alternate_totals",
    )

    if not rows:
        return 0

    executemany(
        con,
        """
        INSERT INTO odds_snapshots (
          captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--days-back", type=int, default=14)
    p.add_argument("--days-forward", type=int, default=14)
    args = p.parse_args()

    s = get_settings()
    con = connect(s.db_path)
    init_db(con)

    matches = fetch_pl_matches(s.football_data_token, args.days_back, args.days_forward)
    n_fix = upsert_fixtures(con, matches)

    captured_at = utcnow_iso()

    # 1) Standard odds list (mainstream)
    events = fetch_odds_list(
        odds_key=s.odds_api_key,
        sport_key=s.odds_sport_key,
        regions=s.odds_regions,
        markets=s.odds_markets,  # can include h2h,spreads,totals etc
        odds_format=s.odds_format,
        date_format=s.date_format,
    )
    n_totals = store_totals_from_odds_list(con, events, captured_at)

    # 2) Alternate totals (extra lines) using event endpoint
    n_alt = 0
    # event_id is in odds list response as "id" (The Odds API event id)
    for ev in events:
        ev_id = ev.get("id")
        if not ev_id:
            continue

        try:
            payload = fetch_event_odds(
                odds_key=s.odds_api_key,
                sport_key=s.odds_sport_key,
                event_id=str(ev_id),
                regions=s.odds_regions,
                markets="alternate_totals",
                odds_format=s.odds_format,
                date_format=s.date_format,
            )
            n_alt += store_alternate_totals_from_event(con, payload, captured_at)
        except requests.HTTPError:
            # Some books/regions may not support alternates for some events.
            # Skip quietly so the job doesn't die.
            continue

    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored totals snapshots: {n_totals}")
    print(f"Stored alternate_totals snapshots: {n_alt}")
    print(f"Markets used (odds list): {sanitize_markets_for_odds_endpoint(s.odds_markets)}")


if __name__ == "__main__":
    main()
