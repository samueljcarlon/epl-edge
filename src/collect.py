from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso


FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"


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

    executemany(con, """
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
    """, rows)
    return len(rows)


def fetch_odds(odds_key: str, sport_key: str, regions: str, markets: str, odds_format: str, date_format: str) -> list[dict]:
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


def store_totals_snapshots(con, events: list[dict], captured_at: str, target_line: float = 2.5) -> int:
    cur = con.cursor()
    rows = []

    def norm(s: str) -> str:
        return (s or "").strip().lower()

    for ev in events:
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not (commence and home and away):
            continue

        # try exact match first
        got = cur.execute(
            """
            SELECT fixture_id FROM fixtures
            WHERE commence_time_utc = ?
              AND lower(home_team) = ?
              AND lower(away_team) = ?
            LIMIT 1
            """,
            (commence, norm(home), norm(away)),
        ).fetchone()

        if not got:
            # fallback: match by commence_time only, then pick closest name match later (MVP skip)
            continue

        fixture_id = got["fixture_id"]

        for bm in ev.get("bookmakers", []):
            bm_title = bm.get("title") or bm.get("key") or "unknown_book"
            for mk in bm.get("markets", []):
                if mk.get("key") != "totals":
                    continue

                over_price = None
                under_price = None
                line = None

                for out in mk.get("outcomes", []):
                    point = out.get("point")
                    name = norm(out.get("name"))
                    price = out.get("price")
                    if point is None or price is None:
                        continue
                    if float(point) != float(target_line):
                        continue
                    line = float(point)
                    if name == "over":
                        over_price = float(price)
                    elif name == "under":
                        under_price = float(price)

                if line is None:
                    continue

                rows.append((captured_at, fixture_id, bm_title, "totals", line, over_price, under_price))

    executemany(con, """
    INSERT INTO odds_snapshots (
      captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)

    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--days-back", type=int, default=14)
    p.add_argument("--days-forward", type=int, default=14)
    p.add_argument("--line", type=float, default=2.5)
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
    n_odds = store_totals_snapshots(con, events, captured_at, target_line=args.line)

    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored odds snapshots: {n_odds}")


if __name__ == "__main__":
    main()

