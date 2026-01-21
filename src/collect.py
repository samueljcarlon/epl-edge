from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings


ODDS_API_BASE = "https://api.the-odds-api.com/v4"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _norm_team(s: str) -> str:
    return (s or "").strip().lower()


def fetch_odds_events(
    *,
    api_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> List[Dict[str, Any]]:
    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
    }

    r = requests.get(url, params=params, timeout=30)
    # If it errors, print enough info so Actions logs show exactly why.
    if not r.ok:
        raise RuntimeError(f"Odds API HTTP {r.status_code}: {r.text[:400]}")

    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected Odds API response shape: {type(data)}")

    return data


def upsert_fixtures(con, events: List[Dict[str, Any]], captured_at_utc: str) -> int:
    rows = []
    for ev in events:
        fixture_id = str(ev.get("id") or "").strip()
        if not fixture_id:
            continue

        commence = ev.get("commence_time")
        home = ev.get("home_team") or ""
        away = ev.get("away_team") or ""

        rows.append(
            {
                "fixture_id": fixture_id,
                "commence_time_utc": commence,
                "matchweek": None,
                "status": "TIMED",
                "home_team": home,
                "away_team": away,
                "home_goals": None,
                "away_goals": None,
                "last_updated_utc": captured_at_utc,
            }
        )

    if not rows:
        return 0

    sql = """
    INSERT INTO fixtures (
      fixture_id, commence_time_utc, matchweek, status,
      home_team, away_team, home_goals, away_goals, last_updated_utc
    )
    VALUES (
      :fixture_id, :commence_time_utc, :matchweek, :status,
      :home_team, :away_team, :home_goals, :away_goals, :last_updated_utc
    )
    ON CONFLICT(fixture_id) DO UPDATE SET
      commence_time_utc = excluded.commence_time_utc,
      matchweek = excluded.matchweek,
      status = excluded.status,
      home_team = excluded.home_team,
      away_team = excluded.away_team,
      home_goals = excluded.home_goals,
      away_goals = excluded.away_goals,
      last_updated_utc = excluded.last_updated_utc
    """
    executemany(con, sql, rows)
    return len(rows)


def store_totals_25(con, events: List[Dict[str, Any]], captured_at_utc: str) -> int:
    """
    Store only totals market at line=2.5.
    Creates one row per bookmaker for that line.
    """
    rows = []

    for ev in events:
        fixture_id = str(ev.get("id") or "").strip()
        if not fixture_id:
            continue

        bms = ev.get("bookmakers") or []
        for bm in bms:
            bm_title = bm.get("title") or bm.get("key") or ""
            markets = bm.get("markets") or []

            for m in markets:
                if (m.get("key") or "").strip().lower() != "totals":
                    continue

                outcomes = m.get("outcomes") or []
                over_price = None
                under_price = None
                line_val = None

                # outcomes look like:
                # [{"name":"Over","price":2.1,"point":2.5}, {"name":"Under","price":1.8,"point":2.5}]
                for o in outcomes:
                    name = (o.get("name") or "").strip().lower()
                    price = o.get("price")
                    point = o.get("point")

                    # Only keep 2.5 (what you asked)
                    try:
                        if point is None:
                            continue
                        if float(point) != 2.5:
                            continue
                        line_val = 2.5
                    except Exception:
                        continue

                    if name == "over":
                        over_price = float(price) if price is not None else None
                    elif name == "under":
                        under_price = float(price) if price is not None else None

                # Need both sides to be useful
                if line_val == 2.5 and over_price and under_price:
                    rows.append(
                        {
                            "captured_at_utc": captured_at_utc,
                            "fixture_id": fixture_id,
                            "bookmaker": bm_title,
                            "market": "totals",
                            "line": 2.5,
                            "over_price": over_price,
                            "under_price": under_price,
                        }
                    )

    if not rows:
        return 0

    sql = """
    INSERT INTO odds_snapshots (
      captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
    )
    VALUES (
      :captured_at_utc, :fixture_id, :bookmaker, :market, :line, :over_price, :under_price
    )
    """
    executemany(con, sql, rows)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport", default=None)
    args = parser.parse_args()

    settings = get_settings()

    sport_key = args.sport or settings.odds_sport_key

    # IMPORTANT: keep this sane so it stops 422’ing
    # You said: keep it at 2.5 totals for now, add other bet types later.
    markets = "totals"

    captured = utcnow_iso()

    events = fetch_odds_events(
        api_key=settings.odds_api_key,
        sport_key=sport_key,
        regions=settings.odds_regions,
        markets=markets,
        odds_format=settings.odds_format,
        date_format=settings.date_format,
    )

    con = connect(settings.db_path)
    init_db(con)

    n_fx = upsert_fixtures(con, events, captured)
    n_odds = store_totals_25(con, events, captured)

    con.commit()
    con.close()

    print(f"Upserted fixtures: {n_fx}")
    print(f"Stored odds snapshots (totals 2.5): {n_odds}")


if __name__ == "__main__":
    main()
