from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


ODDS_API_BASE = "https://api.the-odds-api.com/v4"


def utcnow_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON;")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fixtures (
          fixture_id TEXT PRIMARY KEY,
          commence_time_utc TEXT,
          matchweek INTEGER,
          status TEXT,
          home_team TEXT,
          away_team TEXT,
          home_goals INTEGER,
          away_goals INTEGER
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS odds_snapshots (
          captured_at_utc TEXT NOT NULL,
          fixture_id TEXT NOT NULL,
          bookmaker TEXT NOT NULL,
          market TEXT NOT NULL,
          line REAL,
          over_price REAL,
          under_price REAL,
          PRIMARY KEY (captured_at_utc, fixture_id, bookmaker, market, COALESCE(line, -999999.0)),
          FOREIGN KEY (fixture_id) REFERENCES fixtures(fixture_id) ON DELETE CASCADE
        );
        """
    )

    return con


def fetch_odds_events(
    api_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str = "decimal",
    date_format: str = "iso",
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
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    return data


def parse_totals_25(market_obj: Dict[str, Any]) -> Optional[Tuple[float, float, float]]:
    """
    Returns (line, over_price, under_price) for totals line 2.5 only.
    Odds API totals outcomes include: { name: "Over"/"Under", price: X, point: Y }.
    """
    outcomes = market_obj.get("outcomes") or []
    if not isinstance(outcomes, list):
        return None

    over_price = None
    under_price = None
    line_val = None

    for o in outcomes:
        try:
            name = str(o.get("name", "")).strip().lower()
            price = float(o.get("price"))
            point = o.get("point", None)
            if point is None:
                continue
            point_f = float(point)
        except Exception:
            continue

        # hard lock to 2.5
        if abs(point_f - 2.5) > 1e-9:
            continue

        line_val = 2.5
        if name == "over":
            over_price = price
        elif name == "under":
            under_price = price

    if line_val is None:
        return None
    if over_price is None or under_price is None:
        return None
    return (line_val, over_price, under_price)


def parse_btts(market_obj: Dict[str, Any]) -> Optional[Tuple[Optional[float], float, float]]:
    """
    Returns (line=None, yes_price, no_price) stored as (over_price, under_price).
    Odds API BTTS outcomes include: { name: "Yes"/"No", price: X }.
    """
    outcomes = market_obj.get("outcomes") or []
    if not isinstance(outcomes, list):
        return None

    yes_price = None
    no_price = None

    for o in outcomes:
        try:
            name = str(o.get("name", "")).strip().lower()
            price = float(o.get("price"))
        except Exception:
            continue

        if name == "yes":
            yes_price = price
        elif name == "no":
            no_price = price

    if yes_price is None or no_price is None:
        return None
    return (None, yes_price, no_price)


def upsert_fixtures(con: sqlite3.Connection, events: List[Dict[str, Any]]) -> int:
    rows = []
    for ev in events:
        fixture_id = str(ev.get("id", "")).strip()
        if not fixture_id:
            continue

        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")

        rows.append(
            (
                fixture_id,
                commence,
                None,
                "TIMED",
                home,
                away,
                None,
                None,
            )
        )

    con.executemany(
        """
        INSERT INTO fixtures (
          fixture_id, commence_time_utc, matchweek, status,
          home_team, away_team, home_goals, away_goals
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fixture_id) DO UPDATE SET
          commence_time_utc=excluded.commence_time_utc,
          status=excluded.status,
          home_team=excluded.home_team,
          away_team=excluded.away_team;
        """,
        rows,
    )
    return len(rows)


def insert_snapshots(con: sqlite3.Connection, events: List[Dict[str, Any]], captured_at_utc: str) -> int:
    snap_rows = []

    for ev in events:
        fixture_id = str(ev.get("id", "")).strip()
        if not fixture_id:
            continue

        bookmakers = ev.get("bookmakers") or []
        if not isinstance(bookmakers, list):
            continue

        for bm in bookmakers:
            bm_title = str(bm.get("title") or bm.get("key") or "").strip()
            if not bm_title:
                continue

            markets = bm.get("markets") or []
            if not isinstance(markets, list):
                continue

            for m in markets:
                mkey = str(m.get("key", "")).strip().lower()
                if not mkey:
                    continue

                if mkey == "totals":
                    parsed = parse_totals_25(m)
                    if not parsed:
                        continue
                    ln, over_p, under_p = parsed
                    snap_rows.append((captured_at_utc, fixture_id, bm_title, "totals", ln, over_p, under_p))

                elif mkey == "btts":
                    parsed = parse_btts(m)
                    if not parsed:
                        continue
                    ln, yes_p, no_p = parsed
                    snap_rows.append((captured_at_utc, fixture_id, bm_title, "btts", ln, yes_p, no_p))

                else:
                    # ignore everything else for now, keeps schema sane and stops crashes
                    continue

    if not snap_rows:
        return 0

    con.executemany(
        """
        INSERT OR REPLACE INTO odds_snapshots (
          captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        snap_rows,
    )
    return len(snap_rows)


def main() -> None:
    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key or api_key.startswith("YOUR_"):
        raise RuntimeError("Missing ODDS_API_KEY in GitHub Secrets")

    # keep it explicit and safe
    sport_key = "soccer_epl"
    regions = "uk,eu,us,us2"

    db_path = "data/app.db"
    con = ensure_db(db_path)

    captured = utcnow_z()

    # try totals + btts, fall back to totals if API complains
    try:
        events = fetch_odds_events(api_key, sport_key, regions, markets="totals,btts")
    except requests.HTTPError as e:
        # fallback to totals only
        events = fetch_odds_events(api_key, sport_key, regions, markets="totals")

    with con:
        n_fix = upsert_fixtures(con, events)
        n_odds = insert_snapshots(con, events, captured)

        # optional cleanup: keep last ~50k snapshots
        con.execute(
            """
            DELETE FROM odds_snapshots
            WHERE rowid NOT IN (
              SELECT rowid FROM odds_snapshots
              ORDER BY captured_at_utc DESC
              LIMIT 50000
            );
            """
        )

    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored odds snapshots: {n_odds}")


if __name__ == "__main__":
    main()
