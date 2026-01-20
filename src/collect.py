from __future__ import annotations

import requests
from datetime import datetime, timezone
from src.settings import get_settings
from src.db import connect, init_db, executemany

ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# These are the ones that matter for multiple goal lines
SAFE_MARKETS = [
    "totals",
    "alternate_totals",
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

    # 422 means market unsupported right now, don't crash the pipeline
    if r.status_code == 422:
        print(f"[collect] Market unsupported right now: {market}")
        return []

    r.raise_for_status()
    return r.json()


def main():
    settings = get_settings()

    con = connect(settings.db_path)
    init_db(con)

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

                    # For totals and alternate_totals, outcomes are Over/Under with a "point"
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
                                "line": o.get("point"),
                                "over_price": o.get("price") if o.get("name") == "Over" else None,
                                "under_price": o.get("price") if o.get("name") == "Under" else None,
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
