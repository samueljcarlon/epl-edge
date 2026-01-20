from __future__ import annotations

import requests
from datetime import datetime, timezone
from typing import List, Dict

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso


ODDS_API_BASE = "https://api.the-odds-api.com/v4"


SUPPORTED_MARKETS = [
    "totals",                  # O/U 2.5 main line
    "h2h",                     # 1X2
    "spreads",                 # Asian handicap style
    "both_teams_to_score"      # BTTS
]


def fetch_odds_events(settings) -> List[dict]:
    params = {
        "apiKey": settings.odds_api_key,
        "regions": settings.odds_regions,
        "markets": ",".join(SUPPORTED_MARKETS),
        "oddsFormat": settings.odds_format,
        "dateFormat": settings.date_format,
    }

    url = f"{ODDS_API_BASE}/sports/{settings.odds_sport_key}/odds"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def store_fixtures(con, events: List[dict]) -> None:
    rows = []
    for e in events:
        rows.append({
            "fixture_id": e["id"],
            "commence_time_utc": e["commence_time"],
            "home_team": e["home_team"],
            "away_team": e["away_team"],
            "status": "TIMED"
        })

    sql = """
    INSERT INTO fixtures (fixture_id, commence_time_utc, home_team, away_team, status)
    VALUES (:fixture_id, :commence_time_utc, :home_team, :away_team, :status)
    ON CONFLICT(fixture_id) DO UPDATE SET
        commence_time_utc=excluded.commence_time_utc,
        home_team=excluded.home_team,
        away_team=excluded.away_team,
        status=excluded.status
    """
    executemany(con, sql, rows)


def store_odds(con, events: List[dict]) -> None:
    rows = []
    captured = utcnow_iso()

    for e in events:
        fixture_id = e["id"]

        for bm in e.get("bookmakers", []):
            name = bm["title"]

            for market in bm.get("markets", []):
                mkey = market["key"]

                for o in market.get("outcomes", []):
                    rows.append({
                        "captured_at_utc": captured,
                        "fixture_id": fixture_id,
                        "bookmaker": name,
                        "market": mkey,
                        "line": o.get("point"),
                        "selection": o["name"],
                        "price": o["price"],
                    })

    sql = """
    INSERT INTO odds (
        captured_at_utc,
        fixture_id,
        bookmaker,
        market,
        line,
        selection,
        price
    )
    VALUES (
        :captured_at_utc,
        :fixture_id,
        :bookmaker,
        :market,
        :line,
        :selection,
        :price
    )
    """
    executemany(con, sql, rows)


def main():
    settings = get_settings()
    con = connect(settings.db_path)
    init_db(con)

    events = fetch_odds_events(settings)

    store_fixtures(con, events)
    store_odds(con, events)

    print(f"Stored {len(events)} fixtures")


if __name__ == "__main__":
    main()
