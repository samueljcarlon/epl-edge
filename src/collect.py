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
    return data if isinstance(data, list) else []


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if v == v else None
    except Exception:
        return None


def parse_totals_all_lines(market_obj: Dict[str, Any]) -> List[Tuple[float, float, float]]:
    """
    Returns list of (line, over_price, under_price) for every totals line available.
    Odds API totals outcomes: { name: "Over"/"Under", price: X, point: Y }.
    """
    outcomes = market_obj.get("outcomes") or []
    if not isinstance(outcomes, list):
        return []

    by_point: Dict[float, Dict[str, float]] = {}

    for o in outcomes:
        name = str(o.get("name", "")).strip().lower()
        price = _safe_float(o.get("price"))
        point = _safe_float(o.get("point"))
        if price is None or point is None:
            continue
        if name not in ("over", "under"):
            continue

        if point not in by_point:
            by_point[point] = {}
        by_point[point][name] = price

    rows: List[Tuple[float, float, float]] = []
    for point, vals in by_point.items():
        if "over" in vals and "under" in vals:
            rows.append((point, vals["over"], vals["under"]))

    # stable ordering
    rows.sort(key=lambda t: t[0])
    return rows


def parse_btts(market_obj: Dict[str, Any]) -> Optional[Tuple[Optional[float], float, float]]:
    """
    Returns (line=None, yes_price, no_price) stored as (over_price, under_price).
    Odds API BTTS outcomes: { name: "Yes"/"No", price: X }.
    """
    outcomes = market_obj.get("outcomes") or []
    if not isinstance(outcomes, list):
        return None

    yes_price = None
    no_price = None

    for o in outcomes:
        name = str(o.get("name", "")).strip().lower()
        price = _safe_float(o.get("price"))
        if price is None:
            continue
        if name == "yes":
            yes_price = price
        elif name == "no":
            no_price = price

    if yes_price is None or no_price is None:
        return None
    return (None, yes_price, no_price)


def parse_spreads_all_lines(
    market_obj: Dict[str, Any],
    home_team: str,
    away_team: str,
) -> List[Tuple[float, float, float]]:
    """
    Returns list of (line, home_price, away_price) for every spread line available.
    Odds API spreads outcomes typically: { name: <team>, price: X, point: Y }.
    We'll store:
      market = "spreads"
      line   = point (handicap for the home side)
      over_price  = home_price
      under_price = away_price
    """
    outcomes = market_obj.get("outcomes") or []
    if not isinstance(outcomes, list):
        return []

    ht = home_team.strip().lower()
    at = away_team.strip().lower()

    by_point: Dict[float, Dict[str, float]] = {}

    for o in outcomes:
        name = str(o.get("name", "")).strip().lower()
        price = _safe_float(o.get("price"))
        point = _safe_float(o.get("point"))
        if price is None or point is None:
            continue

        side: Optional[str] = None
        if name == ht or name == "home":
            side = "home"
        elif name == at or name == "away":
            side = "away"
        else:
            # sometimes the API gives short names, we avoid guessing
            continue

        if point not in by_point:
            by_point[point] = {}
        by_point[point][side] = price

    rows: List[Tuple[float, float, float]] = []
    for point, vals in by_point.items():
        if "home" in vals and "away" in vals:
            rows.append((point, vals["home"], vals["away"]))

    rows.sort(key=lambda t: t[0])
    return rows


def upsert_fixtures(con: sqlite3.Connection, events: List[Dict[str, Any]]) -> int:
    rows = []
    for ev in events:
        fixture_id = str(ev.get("id", "")).strip()
        if not fixture_id:
            continue
        rows.append(
            (
                fixture_id,
                ev.get("commence_time"),
                None,
                "TIMED",
                ev.get("home_team"),
                ev.get("away_team"),
                None,
                None,
            )
        )

    if not rows:
        return 0

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
    snap_rows: List[Tuple[str, str, str, str, Optional[float], float, float]] = []

    for ev in events:
        fixture_id = str(ev.get("id", "")).strip()
        if not fixture_id:
            continue

        home_team = str(ev.get("home_team") or "").strip()
        away_team = str(ev.get("away_team") or "").strip()

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
                    for ln, over_p, under_p in parse_totals_all_lines(m):
                        snap_rows.append((captured_at_utc, fixture_id, bm_title, "totals", ln, over_p, under_p))

                elif mkey == "spreads":
                    for ln, home_p, away_p in parse_spreads_all_lines(m, home_team, away_team):
                        snap_rows.append((captured_at_utc, fixture_id, bm_title, "spreads", ln, home_p, away_p))

                elif mkey == "btts":
                    parsed = parse_btts(m)
                    if parsed:
                        ln, yes_p, no_p = parsed
                        snap_rows.append((captured_at_utc, fixture_id, bm_title, "btts", ln, yes_p, no_p))

                else:
                    # keep the DB/UI sane: only store what we actually display
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


def _fetch_with_fallback(api_key: str, sport_key: str, regions: str, markets_csv: str) -> List[Dict[str, Any]]:
    """
    The "C" thing: stop the pipeline crashing if the API rejects a market set.
    We try the full list, then progressively drop markets until it works.
    """
    wanted = [m.strip() for m in markets_csv.split(",") if m.strip()]
    if not wanted:
        wanted = ["totals"]

    # try full, then remove one-by-one from the end
    for k in range(len(wanted), 0, -1):
        mk = ",".join(wanted[:k])
        try:
            return fetch_odds_events(api_key, sport_key, regions, mk)
        except requests.HTTPError:
            continue

    # last resort
    return fetch_odds_events(api_key, sport_key, regions, "totals")


def main() -> None:
    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key or api_key.startswith("YOUR_"):
        raise RuntimeError("Missing ODDS_API_KEY in GitHub Secrets")

    sport_key = "soccer_epl"
    regions = os.getenv("ODDS_REGIONS", "uk,eu,us,us2").strip() or "uk,eu,us,us2"

    # mainstream markets that fit your 2-price schema + UI
    markets = os.getenv("ODDS_MARKETS", "totals,spreads,btts").strip() or "totals,spreads,btts"

    db_path = "data/app.db"
    con = ensure_db(db_path)

    captured = utcnow_z()
    events = _fetch_with_fallback(api_key, sport_key, regions, markets)

    with con:
        n_fix = upsert_fixtures(con, events)
        n_odds = insert_snapshots(con, events, captured)

        # keep last ~50k snapshot rows
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
