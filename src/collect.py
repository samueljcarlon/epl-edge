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

    url = f"{FOOTBALL_DATA_BASE}/competitions/PL/matches"
    headers = {"X-Auth-Token": fd_token}
    params = {"dateFrom": date_from, "dateTo": date_to}

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def upsert_fixtures(con, payload: dict) -> int:
    matches = payload.get("matches", []) or []
    rows = []

    for m in matches:
        fixture_id = str(m.get("id", ""))
        if not fixture_id:
            continue

        utc_date = m.get("utcDate")
        commence_time_utc = utc_date if utc_date else None

        matchday = m.get("matchday")
        matchweek = int(matchday) if matchday is not None else None

        status = m.get("status")

        home_team = (m.get("homeTeam") or {}).get("name")
        away_team = (m.get("awayTeam") or {}).get("name")

        score = m.get("score") or {}
        ft = score.get("fullTime") or {}
        home_goals = ft.get("home")
        away_goals = ft.get("away")

        rows.append(
            (
                fixture_id,
                commence_time_utc,
                matchweek,
                status,
                home_team,
                away_team,
                home_goals,
                away_goals,
            )
        )

    if not rows:
        return 0

    sql = """
    INSERT INTO fixtures (
        fixture_id,
        commence_time_utc,
        matchweek,
        status,
        home_team,
        away_team,
        home_goals,
        away_goals
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(fixture_id) DO UPDATE SET
        commence_time_utc = excluded.commence_time_utc,
        matchweek = excluded.matchweek,
        status = excluded.status,
        home_team = excluded.home_team,
        away_team = excluded.away_team,
        home_goals = excluded.home_goals,
        away_goals = excluded.away_goals
    """

    executemany(con, sql, rows)
    return len(rows)


def fetch_odds(
    oa_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> list[dict]:
    url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": oa_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
    }

    r = requests.get(url, params=params, timeout=30)

    # Useful when the API rejects one market (some sports/regions do)
    if r.status_code == 422 and "alternate_totals" in markets:
        fallback = markets.replace("alternate_totals", "").replace(",,", ",").strip(",")
        params["markets"] = fallback or "totals"
        r = requests.get(url, params=params, timeout=30)

    r.raise_for_status()
    return r.json()


def store_totals_all_lines(con, odds_events: list[dict], captured_at_utc: str) -> int:
    """
    Stores totals-like markets (totals, alternate_totals) for every line returned.
    Schema assumption: odds_snapshots has (captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price)
    """
    rows = []

    for ev in odds_events:
        fixture_id = str(ev.get("id", "")).strip()
        if not fixture_id:
            continue

        bookmakers = ev.get("bookmakers") or []
        for bm in bookmakers:
            bm_name = bm.get("title") or bm.get("key") or ""
            if not bm_name:
                continue

            markets = bm.get("markets") or []
            for mk in markets:
                mk_key = mk.get("key") or ""
                if mk_key not in ("totals", "alternate_totals"):
                    continue

                outcomes = mk.get("outcomes") or []
                # Group outcomes by point (line)
                tmp: dict[float, dict[str, float]] = {}

                for o in outcomes:
                    name = (o.get("name") or "").lower().strip()
                    price = o.get("price")
                    point = o.get("point")

                    if price is None or point is None:
                        continue

                    try:
                        price_f = float(price)
                        point_f = float(point)
                    except (TypeError, ValueError):
                        continue

                    if point_f not in tmp:
                        tmp[point_f] = {}

                    if name == "over":
                        tmp[point_f]["over"] = price_f
                    elif name == "under":
                        tmp[point_f]["under"] = price_f

                for point_f, ou in tmp.items():
                    over_price = ou.get("over")
                    under_price = ou.get("under")
                    if over_price is None and under_price is None:
                        continue

                    rows.append(
                        (
                            captured_at_utc,
                            fixture_id,
                            bm_name,
                            mk_key,
                            point_f,
                            over_price,
                            under_price,
                        )
                    )

    if not rows:
        return 0

    sql = """
    INSERT INTO odds_snapshots (
        captured_at_utc,
        fixture_id,
        bookmaker,
        market,
        line,
        over_price,
        under_price
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    executemany(con, sql, rows)
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days-back", type=int, default=2)
    ap.add_argument("--days-forward", type=int, default=14)
    args = ap.parse_args()

    s = get_settings()

    con = connect(s.db_path)
    init_db(con)

    # 1) fixtures
    fixtures_payload = fetch_pl_matches(s.football_data_token, args.days_back, args.days_forward)
    n_fix = upsert_fixtures(con, fixtures_payload)
    print(f"Upserted fixtures: {n_fix}")

    # 2) odds, grab totals + alternate_totals so we get many lines
    captured = utcnow_iso()

    markets = "totals,alternate_totals"
    odds_events = fetch_odds(
        oa_key=s.odds_api_key,
        sport_key=s.odds_sport_key,
        regions=s.odds_regions,
        markets=markets,
        odds_format=s.odds_format,
        date_format=s.date_format,
    )

    n_odds = store_totals_all_lines(con, odds_events, captured)
    print(f"Stored odds snapshots: {n_odds}")


if __name__ == "__main__":
    main()
