from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# The Odds API v4 supported markets for /sports/{sport_key}/odds
# Docs: h2h, spreads, totals, outrights
ALLOWED_ODDS_MARKETS = {"h2h", "spreads", "totals", "outrights"}


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def sanitize_markets(markets: str) -> str:
    """
    Accept a comma-separated markets string and return only those supported by The Odds API v4.
    Prevents 422 errors (e.g., 'alternate_totals' is not supported on this endpoint).
    """
    parts = [_norm(x) for x in (markets or "").split(",") if _norm(x)]
    keep = [p for p in parts if p in ALLOWED_ODDS_MARKETS]

    # Default to totals if nothing valid remains
    if not keep:
        return "totals"

    # Keep order but dedupe
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


def fetch_odds(
    odds_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> list[dict]:
    markets = sanitize_markets(markets)

    r = requests.get(
        f"{ODDS_API_BASE}/sports/{sport_key}/odds",
        params={
            "apiKey": odds_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def store_totals_snapshots(con, events: list[dict], captured_at: str) -> int:
    """
    Store ALL totals lines available (2.5, 3.5, 1.5, etc.) per bookmaker per fixture.
    Previously you were effectively only keeping one line, which makes the UI line dropdown useless.
    """
    cur = con.cursor()
    rows = []

    for ev in events:
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not (commence and home and away):
            continue

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
            continue

        fixture_id = got["fixture_id"]

        for bm in ev.get("bookmakers", []):
            bm_title = bm.get("title") or bm.get("key") or "unknown_book"

            for mk in bm.get("markets", []):
                if mk.get("key") != "totals":
                    continue

                # Aggregate outcomes by point (line)
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

                # Only write rows where we have both prices
                for line, ou in by_line.items():
                    if "over" in ou and "under" in ou:
                        rows.append(
                            (
                                captured_at,
                                fixture_id,
                                bm_title,
                                "totals",
                                line,
                                ou["over"],
                                ou["under"],
                            )
                        )

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
    events = fetch_odds(
        odds_key=s.odds_api_key,
        sport_key=s.odds_sport_key,
        regions=s.odds_regions,
        markets=s.odds_markets,
        odds_format=s.odds_format,
        date_format=s.date_format,
    )

    n_odds = store_totals_snapshots(con, events, captured_at)

    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored odds snapshots: {n_odds}")
    print(f"Markets used: {sanitize_markets(s.odds_markets)}")


if __name__ == "__main__":
    main()
