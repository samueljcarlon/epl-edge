from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# Supported by:
#   GET /sports/{sport_key}/odds
BASE_ODDS_MARKETS = {"h2h", "spreads", "totals", "outrights"}

# Requires:
#   GET /sports/{sport_key}/events/{event_id}/odds
EVENT_ONLY_MARKETS = {"btts"}


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _dedupe_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in items:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def sanitize_base_markets(markets: str) -> str:
    """
    Keep only markets supported by /sports/{sport_key}/odds.
    Prevents 422 errors.
    """
    parts = [_norm(x) for x in (markets or "").split(",") if _norm(x)]
    keep = [p for p in parts if p in BASE_ODDS_MARKETS]
    if not keep:
        keep = ["totals"]
    return ",".join(_dedupe_keep_order(keep))


def fetch_pl_matches(fd_token: str, days_back: int, days_forward: int) -> dict[str, Any]:
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


def upsert_fixtures(con, matches_json: dict[str, Any]) -> int:
    rows: list[tuple[Any, ...]] = []
    now_iso = utcnow_iso()

    for m in matches_json.get("matches", []):
        fixture_id = str(m["id"])
        commence = m.get("utcDate")
        status = m.get("status", "UNKNOWN")
        matchday = m.get("matchday")
        home = (m.get("homeTeam") or {}).get("name") or "UNKNOWN_HOME"
        away = (m.get("awayTeam") or {}).get("name") or "UNKNOWN_AWAY"
        score = m.get("score") or {}
        full = (score.get("fullTime") or {})
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


def fetch_odds_base(
    odds_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> list[dict[str, Any]]:
    markets_clean = sanitize_base_markets(markets)

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
) -> dict[str, Any] | None:
    """
    Fetch event-only markets like BTTS.
    If rejected (422 etc), skip without killing the run.
    """
    r = requests.get(
        f"{ODDS_API_BASE}/sports/{sport_key}/events/{event_id}/odds",
        params={
            "apiKey": odds_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        },
        timeout=30,
    )

    if r.status_code >= 400:
        return None

    try:
        return r.json()
    except Exception:
        return None


def _fixture_id_for_event(con, commence: str, home: str, away: str) -> str | None:
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
    return got["fixture_id"] if got else None


def _store_rows(con, rows: list[tuple[Any, ...]]) -> int:
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


def store_base_market_snapshots(con, events: list[dict[str, Any]], captured_at: str) -> int:
    """
    Stores totals + spreads from /sports/{sport_key}/odds.

    totals:
      line = total points (2.5)
      over_price = Over
      under_price = Under

    spreads:
      line = HOME handicap (example -0.5)
      over_price = Home price
      under_price = Away price
    """
    rows: list[tuple[Any, ...]] = []

    for ev in events:
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not (commence and home and away):
            continue

        fixture_id = _fixture_id_for_event(con, commence, home, away)
        if not fixture_id:
            continue

        for bm in ev.get("bookmakers", []):
            bm_title = bm.get("title") or bm.get("key") or "unknown_book"

            for mk in bm.get("markets", []):
                mkey = mk.get("key")

                if mkey == "totals":
                    by_line: dict[float, dict[str, float]] = {}
                    for out in mk.get("outcomes", []):
                        point = out.get("point")
                        name = _norm(out.get("name"))
                        price = out.get("price")
                        if point is None or price is None:
                            continue
                        try:
                            ln = float(point)
                            pr = float(price)
                        except (TypeError, ValueError):
                            continue
                        if name not in ("over", "under"):
                            continue
                        by_line.setdefault(ln, {})[name] = pr

                    for ln, ou in by_line.items():
                        if "over" in ou and "under" in ou:
                            rows.append((captured_at, fixture_id, bm_title, "totals", ln, ou["over"], ou["under"]))

                elif mkey == "spreads":
                    by_line: dict[float, dict[str, float]] = {}
                    for out in mk.get("outcomes", []):
                        point = out.get("point")
                        name = out.get("name")
                        price = out.get("price")
                        if point is None or name is None or price is None:
                            continue
                        try:
                            pr = float(price)
                            ln = float(point)
                        except (TypeError, ValueError):
                            continue

                        nm = _norm(str(name))
                        if nm == _norm(home):
                            by_line.setdefault(ln, {})["home"] = pr
                        elif nm == _norm(away):
                            by_line.setdefault(-ln, {})["away"] = pr

                    for ln, vals in by_line.items():
                        if "home" in vals and "away" in vals:
                            rows.append((captured_at, fixture_id, bm_title, "spreads", ln, vals["home"], vals["away"]))

    return _store_rows(con, rows)


def store_btts_snapshots(con, base_events: list[dict[str, Any]], captured_at: str, settings) -> int:
    """
    BTTS must be fetched per-event.
    Mapping:
      Yes -> over_price
      No  -> under_price
      line -> NULL
    """
    rows: list[tuple[Any, ...]] = []
    seen_event_ids: set[str] = set()

    for ev in base_events:
        event_id = ev.get("id")
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not (event_id and commence and home and away):
            continue
        if event_id in seen_event_ids:
            continue
        seen_event_ids.add(event_id)

        fixture_id = _fixture_id_for_event(con, commence, home, away)
        if not fixture_id:
            continue

        payload = fetch_event_odds(
            odds_key=settings.odds_api_key,
            sport_key=settings.odds_sport_key,
            event_id=str(event_id),
            regions=settings.odds_regions,
            markets="btts",
            odds_format=settings.odds_format,
            date_format=settings.date_format,
        )
        if not payload:
            continue

        for bm in payload.get("bookmakers", []):
            bm_title = bm.get("title") or bm.get("key") or "unknown_book"
            for mk in bm.get("markets", []):
                if mk.get("key") != "btts":
                    continue

                yes_price = None
                no_price = None

                for out in mk.get("outcomes", []):
                    nm = _norm(out.get("name"))
                    pr = out.get("price")
                    if pr is None:
                        continue
                    try:
                        prf = float(pr)
                    except (TypeError, ValueError):
                        continue

                    if nm in ("yes", "y"):
                        yes_price = prf
                    elif nm in ("no", "n"):
                        no_price = prf

                if yes_price is not None and no_price is not None:
                    rows.append((captured_at, fixture_id, bm_title, "btts", None, yes_price, no_price))

    return _store_rows(con, rows)


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

    base_events = fetch_odds_base(
        odds_key=s.odds_api_key,
        sport_key=s.odds_sport_key,
        regions=s.odds_regions,
        markets=s.odds_markets,
        odds_format=s.odds_format,
        date_format=s.date_format,
    )

    n_base = store_base_market_snapshots(con, base_events, captured_at)
    n_btts = store_btts_snapshots(con, base_events, captured_at, s)

    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored base odds snapshots: {n_base}")
    print(f"Stored BTTS snapshots: {n_btts}")
    print(f"Base markets used: {sanitize_base_markets(s.odds_markets)}")


if __name__ == "__main__":
    main()
