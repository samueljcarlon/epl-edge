from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from src.db import connect, init_db


FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_fixtures(competition: str, token: str) -> List[Dict[str, Any]]:
    url = f"{FOOTBALL_DATA_BASE}/competitions/{competition}/matches"
    r = requests.get(url, headers={"X-Auth-Token": token}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("matches", [])


def upsert_fixtures(con: sqlite3.Connection, matches: List[Dict[str, Any]]) -> int:
    cur = con.cursor()
    n = 0
    for m in matches:
        fixture_id = str(m.get("id"))
        utc_date = m.get("utcDate")  # already ISO Z
        matchday = m.get("matchday")
        status = m.get("status")

        home = (m.get("homeTeam") or {}).get("name")
        away = (m.get("awayTeam") or {}).get("name")

        score = (m.get("score") or {}).get("fullTime") or {}
        home_goals = score.get("home")
        away_goals = score.get("away")

        cur.execute(
            """
            INSERT INTO fixtures (
                fixture_id, commence_time_utc, matchweek, status,
                home_team, away_team, home_goals, away_goals
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fixture_id) DO UPDATE SET
                commence_time_utc=excluded.commence_time_utc,
                matchweek=excluded.matchweek,
                status=excluded.status,
                home_team=excluded.home_team,
                away_team=excluded.away_team,
                home_goals=excluded.home_goals,
                away_goals=excluded.away_goals
            """,
            (fixture_id, utc_date, matchday, status, home, away, home_goals, away_goals),
        )
        n += 1

    con.commit()
    return n


def fetch_odds(
    sport_key: str,
    api_key: str,
    regions: str,
    markets: str,
    odds_format: str = "decimal",
    date_format: str = "iso",
) -> List[Dict[str, Any]]:
    """
    The Odds API v4: /sports/{sport_key}/odds
    markets can be comma-separated, for example: "totals,alternate_totals"
    """
    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _extract_totals_points(bookmaker_obj: Dict[str, Any], allowed_market_keys: set[str]) -> List[Dict[str, Any]]:
    """
    Returns rows like:
    { "bookmaker": "...", "market": "totals", "line": 2.5, "over": 1.9, "under": 1.9 }
    Handles markets with outcomes containing name + price + point.
    """
    out: List[Dict[str, Any]] = []

    bm_name = bookmaker_obj.get("title") or bookmaker_obj.get("key")
    markets = bookmaker_obj.get("markets") or []
    for mk in markets:
        mk_key = mk.get("key")
        if mk_key not in allowed_market_keys:
            continue

        # Group outcomes by point so we can pair Over and Under
        by_point: Dict[float, Dict[str, float]] = {}
        for o in (mk.get("outcomes") or []):
            name = o.get("name")
            price = o.get("price")
            point = o.get("point")

            if name not in ("Over", "Under"):
                continue
            if price is None or point is None:
                continue

            try:
                p = float(point)
                pr = float(price)
            except (TypeError, ValueError):
                continue

            by_point.setdefault(p, {})
            by_point[p][name.lower()] = pr

        for line, vals in by_point.items():
            if "over" in vals and "under" in vals:
                out.append(
                    {
                        "bookmaker": bm_name,
                        "market": mk_key,
                        "line": line,
                        "over_price": vals["over"],
                        "under_price": vals["under"],
                    }
                )

    return out


def store_odds_snapshots(
    con: sqlite3.Connection,
    odds_payload: List[Dict[str, Any]],
    allowed_market_keys: Optional[List[str]] = None,
) -> int:
    """
    Stores ALL lines (points) for totals-style markets.
    Default allowed markets: totals + alternate_totals.
    """
    if allowed_market_keys is None:
        allowed_market_keys = ["totals", "alternate_totals"]

    allowed = set(allowed_market_keys)
    cur = con.cursor()
    captured_at = utc_now_iso()

    inserted = 0

    for event in odds_payload:
        fixture_id = str(event.get("id") or "")
        if not fixture_id:
            continue

        bookmakers = event.get("bookmakers") or []
        for bm in bookmakers:
            rows = _extract_totals_points(bm, allowed)
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO odds_snapshots (
                        captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        captured_at,
                        fixture_id,
                        row["bookmaker"],
                        row["market"],
                        row["line"],
                        row["over_price"],
                        row["under_price"],
                    ),
                )
                inserted += 1

    con.commit()
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.environ.get("DB_PATH", "data/app.db"))
    parser.add_argument("--competition", default=os.environ.get("COMPETITION", "PL"))
    parser.add_argument("--sport-key", default=os.environ.get("ODDS_SPORT_KEY", "soccer_epl"))
    parser.add_argument("--regions", default=os.environ.get("ODDS_REGIONS", "uk,eu,us,au"))
    parser.add_argument(
        "--markets",
        default=os.environ.get("ODDS_MARKETS", "totals,alternate_totals"),
        help="Comma separated Odds API markets, e.g. totals,alternate_totals",
    )
    args = parser.parse_args()

    football_token = os.environ.get("FOOTBALL_DATA_TOKEN")
    odds_key = os.environ.get("ODDS_API_KEY") or os.environ.get("ODDS_API_KEY".replace("ODDS_API_KEY", "ODDS_API_KEY"))

    # Your workflow uses ODDS_API_KEY already, keep that name.
    odds_key = os.environ.get("ODDS_API_KEY") or os.environ.get("ODDS_API_KEY".replace("ODDS_API_KEY", "ODDS_API_KEY"))
    odds_key = os.environ.get("ODDS_API_KEY")  # final truth

    if not football_token:
        raise SystemExit("Missing FOOTBALL_DATA_TOKEN")
    if not odds_key:
        raise SystemExit("Missing ODDS_API_KEY")

    con = connect(args.db)
    init_db(con)

    matches = fetch_fixtures(args.competition, football_token)
    n_fx = upsert_fixtures(con, matches)

    odds_payload = fetch_odds(
        sport_key=args.sport_key,
        api_key=odds_key,
        regions=args.regions,
        markets=args.markets,
    )
    n_odds = store_odds_snapshots(con, odds_payload)

    print(f"Upserted fixtures: {n_fx}")
    print(f"Stored odds snapshots: {n_odds}")


if __name__ == "__main__":
    main()
