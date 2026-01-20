from __future__ import annotations

import requests
from datetime import datetime, timezone
from pathlib import Path

from src.settings import get_settings
from src.db import connect, init_db, executemany

ODDS_API_BASE = "https://api.the-odds-api.com/v4"

SAFE_MARKETS = [
    "totals",  # 2.5 goals etc
    "btts",
    "h2h",
]


def fetch_market(settings, market: str):
    url = f"{ODDS_API_BASE}/sports/{settings.odds_sport_key}/odds"
    params = {
        "apiKey": settings.odds_api_key,
        "regions": settings.odds_regions,
        "markets": market,
        "oddsFormat": settings.odds_format,
        "dateFormat": settings.date_format,
    }

    r = requests.get(url, params=params, timeout=20)

    # 422 means this market is not supported right now, not a hard failure
    if r.status_code == 422:
        print(f"[collect] Market unsupported right now: {market}")
        return []

    r.raise_for_status()
    return r.json()


def main():
    settings = get_settings()

    # Ensure DB folder exists (extra safety, connect() also handles it)
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)

    con = connect(settings.db_path)
    init_db(con)

    # Fail fast if schema is wrong
    cur = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='odds';"
    )
    if cur.fetchone() is None:
        raise RuntimeError("DB init failed: table 'odds' was not created")

    captured = datetime.now(timezone.utc).isoformat()
    rows = []

    for market in SAFE_MARKETS:
        events = fetch_market(settings, market)

        for ev in events:
            fixture_id = ev.get("id")
            commence = ev.get("commence_time")
            status = ev.get("status")
            home = ev.get("home_team")
            away = ev.get("away_team")

            for bm in ev.get("bookmakers", []):
                book = bm.get("title")

                for m in bm.get("markets", []):
                    if m.get("key") != market:
                        continue

                    # NOTE: This currently only writes totals as Over/Under prices.
                    # For btts and h2h, over_price/under_price will usually remain None.
                    for o in m.get("outcomes", []):
                        rows.append(
                            {
                                "captured_at_utc": captured,
                                "fixture_id": fixture_id,
                                "commence_time_utc": commence,
                                "status": status,
                                "home_team": home,
                                "away_team": away,
                                "bookmaker": book,
                                "market": market,
                                "line": o.get("point"),  # only totals has this
                                "over_price": o.get("price")
                                if o.get("name") == "Over"
                                else None,
                                "under_price": o.get("price")
                                if o.get("name") == "Under"
                                else None,
                            }
                        )

    if rows:
        sql = """
        INSERT INTO odds (
            captured_at_utc,
            fixture_id,
            commence_time_utc,
            status,
            home_team,
            away_team,
            bookmaker,
            market,
            line,
            over_price,
            under_price
        ) VALUES (
            :captured_at_utc,
            :fixture_id,
            :commence_time_utc,
            :status,
            :home_team,
            :away_team,
            :bookmaker,
            :market,
            :line,
            :over_price,
            :under_price
        );
        """
        executemany(con, sql, rows)

    print(f"[collect] Stored rows: {len(rows)}")


if __name__ == "__main__":
    main()
