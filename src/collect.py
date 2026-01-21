from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# TheOddsAPI market keys that are realistic and mostly 2-outcome-friendly for our schema
SUPPORTED_MARKETS = {
    "totals",
    "alternate_totals",
    "spreads",
    "alternate_spreads",
    "btts",
}

# Map market -> the two outcome labels we store into (over_price, under_price)
# UI still says Over/Under, we just re-use those two columns.
MARKET_OUTCOME_NAMES = {
    "totals": ("Over", "Under"),
    "alternate_totals": ("Over", "Under"),
    "spreads": ("Home", "Away"),
    "alternate_spreads": ("Home", "Away"),
    "btts": ("Yes", "No"),
}


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


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


def store_fixtures_from_football_data(con, payload: dict) -> int:
    matches = payload.get("matches", []) or []
    rows = []
    now = utcnow_iso()

    for m in matches:
        fixture_id = str(m.get("id"))
        utc_date = m.get("utcDate")
        status = m.get("status") or "UNKNOWN"
        mw = (m.get("matchday") if m.get("matchday") is not None else None)

        home_team = (m.get("homeTeam") or {}).get("name") or ""
        away_team = (m.get("awayTeam") or {}).get("name") or ""

        score = m.get("score") or {}
        full_time = score.get("fullTime") or {}
        home_goals = full_time.get("home")
        away_goals = full_time.get("away")

        rows.append(
            (
                fixture_id,
                utc_date,
                mw,
                status,
                home_team,
                away_team,
                home_goals,
                away_goals,
                now,
            )
        )

    sql = """
    INSERT INTO fixtures (
      fixture_id, commence_time_utc, matchweek, status,
      home_team, away_team, home_goals, away_goals, last_updated_utc
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
    """
    executemany(con, sql, rows)
    return len(rows)


def _request_odds(
    api_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> List[dict]:
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
    data = r.json()
    if not isinstance(data, list):
        return []
    return data


def fetch_odds_events_resilient(settings) -> List[dict]:
    # Parse markets from settings, keep only supported ones
    raw = (settings.odds_markets or "").strip()
    requested = [m.strip() for m in raw.split(",") if m.strip()]
    markets = [m for m in requested if m in SUPPORTED_MARKETS]

    # If user configured nonsense, fall back hard to totals only
    if not markets:
        markets = ["totals"]

    joined = ",".join(markets)

    try:
        return _request_odds(
            api_key=settings.odds_api_key,
            sport_key=settings.odds_sport_key,
            regions=settings.odds_regions,
            markets=joined,
            odds_format=settings.odds_format,
            date_format=settings.date_format,
        )
    except requests.HTTPError as e:
        # If 422 or similar, retry each market individually and keep what works
        resp = getattr(e, "response", None)
        status = getattr(resp, "status_code", None)

        if status != 422:
            raise

        ok: List[dict] = []
        for m in markets:
            try:
                part = _request_odds(
                    api_key=settings.odds_api_key,
                    sport_key=settings.odds_sport_key,
                    regions=settings.odds_regions,
                    markets=m,
                    odds_format=settings.odds_format,
                    date_format=settings.date_format,
                )
                ok.extend(part)
            except requests.HTTPError:
                # skip unsupported market
                continue

        # de-dupe events by id+markets, keep them all (we store per market anyway)
        return ok


def ensure_fixture_exists_from_odds_event(con, ev: dict, captured_at: str) -> None:
    fixture_id = str(ev.get("id") or "")
    if not fixture_id:
        return

    commence = ev.get("commence_time") or ev.get("commence_time_utc")
    home_team = ev.get("home_team") or ""
    away_team = ev.get("away_team") or ""
    status = ev.get("status") or "TIMED"

    # Insert minimal fixture if missing. If football-data already inserted it, this is ignored.
    sql = """
    INSERT OR IGNORE INTO fixtures (
      fixture_id, commence_time_utc, matchweek, status,
      home_team, away_team, home_goals, away_goals, last_updated_utc
    )
    VALUES (?, ?, NULL, ?, ?, ?, NULL, NULL, ?)
    """
    executemany(con, sql, [(fixture_id, commence, status, home_team, away_team, captured_at)])


def extract_two_way_prices(
    market_key: str, outcomes: List[dict]
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (line, over_price, under_price) but "over/under" is generic:
    - totals: Over/Under, line is points
    - spreads: Home/Away, line is handicap points
    - btts: Yes/No, line forced to 0.0
    """
    want_a, want_b = MARKET_OUTCOME_NAMES.get(market_key, ("Over", "Under"))
    a_price = None
    b_price = None
    line = None

    for o in outcomes or []:
        name = (o.get("name") or "").strip()
        price = _safe_float(o.get("price"))
        point = _safe_float(o.get("point"))

        if market_key == "btts":
            # some feeds don't provide point, treat as 0.0
            line = 0.0
        else:
            if point is not None:
                line = point

        if name.lower() == want_a.lower():
            a_price = price
        elif name.lower() == want_b.lower():
            b_price = price

    return line, a_price, b_price


def store_odds_snapshots(con, odds_events: List[dict], captured_at: str) -> int:
    rows = []

    for ev in odds_events:
        fixture_id = str(ev.get("id") or "")
        if not fixture_id:
            continue

        ensure_fixture_exists_from_odds_event(con, ev, captured_at)

        bookmakers = ev.get("bookmakers") or []
        for bm in bookmakers:
            bm_name = (bm.get("title") or "").strip()
            markets = bm.get("markets") or []
            for mk in markets:
                market_key = (mk.get("key") or "").strip()
                if market_key not in SUPPORTED_MARKETS:
                    continue

                line, a_price, b_price = extract_two_way_prices(market_key, mk.get("outcomes") or [])
                if line is None:
                    # totals/spreads require a line
                    if market_key in {"totals", "alternate_totals", "spreads", "alternate_spreads"}:
                        continue
                    line = 0.0

                rows.append(
                    (
                        captured_at,
                        fixture_id,
                        bm_name,
                        market_key,
                        float(line),
                        a_price,
                        b_price,
                    )
                )

    if not rows:
        return 0

    sql = """
    INSERT INTO odds_snapshots (
      captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    executemany(con, sql, rows)
    return len(rows)


def main() -> None:
    settings = get_settings()

    con = connect(settings.db_path)
    con.execute("PRAGMA foreign_keys = OFF")
    init_db(con)

    # 1) Upsert fixtures (football-data)
    fixtures_payload = fetch_pl_matches(settings.football_data_token, days_back=3, days_forward=21)
    n_fx = store_fixtures_from_football_data(con, fixtures_payload)

    # 2) Pull odds (TheOddsAPI), resilient to 422
    captured_at = utcnow_iso()
    odds_events = fetch_odds_events_resilient(settings)

    # 3) Store odds snapshots
    n_odds = store_odds_snapshots(con, odds_events, captured_at)

    con.commit()
    con.close()

    print(f"Upserted fixtures: {n_fx}")
    print(f"Stored odds snapshots: {n_odds}")


if __name__ == "__main__":
    main()

