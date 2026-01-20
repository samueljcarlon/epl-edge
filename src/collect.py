from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso


FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"


def _norm_team(name: str) -> str:
    """
    Normalise team names to improve matching between football-data and odds-api.
    Strips common suffixes/prefixes and punctuation, collapses whitespace.
    """
    s = (name or "").lower()
    s = re.sub(r"&", "and", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\b(fc|afc|cf|sc|sv|fk|sk)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_iso(dt: str) -> Optional[datetime]:
    if not dt:
        return None
    try:
        # odds-api often returns Z
        if dt.endswith("Z"):
            dt = dt.replace("Z", "+00:00")
        return datetime.fromisoformat(dt)
    except Exception:
        return None


def fetch_pl_matches(fd_token: str, days_back: int, days_forward: int) -> Dict[str, Any]:
    today = datetime.now(timezone.utc).date()
    date_from = (today - timedelta(days=days_back)).isoformat()
    date_to = (today + timedelta(days=days_forward)).isoformat()

    url = f"{FOOTBALL_DATA_BASE}/competitions/PL/matches"
    headers = {"X-Auth-Token": fd_token}
    params = {"dateFrom": date_from, "dateTo": date_to}

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_odds_events(
    oa_key: str,
    regions: str,
    markets: str,
    odds_format: str = "decimal",
    date_format: str = "iso",
) -> List[Dict[str, Any]]:
    """
    Odds API endpoint:
    /sports/{sport_key}/odds

    markets:
      - totals
      - alternate_totals
    """
    url = f"{ODDS_API_BASE}/sports/soccer_epl/odds"
    params = {
        "apiKey": oa_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def upsert_fixtures(con, matches_payload: Dict[str, Any]) -> int:
    """
    Stores PL fixtures from football-data.
    fixture_id is football-data match id (string).
    """
    matches = matches_payload.get("matches", []) or []
    rows = []

    for m in matches:
        mid = str(m.get("id"))
        utc_date = m.get("utcDate")
        status = m.get("status")
        matchday = m.get("matchday")
        home = (m.get("homeTeam") or {}).get("name")
        away = (m.get("awayTeam") or {}).get("name")

        score = m.get("score") or {}
        ft = score.get("fullTime") or {}
        home_goals = ft.get("home")
        away_goals = ft.get("away")

        if not mid or not utc_date or not home or not away:
            continue

        rows.append(
            (
                mid,
                utc_date,
                matchday,
                status,
                home,
                away,
                home_goals,
                away_goals,
            )
        )

    sql = """
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
    """

    executemany(con, sql, rows)
    return len(rows)


def build_fixture_index(con) -> Dict[Tuple[str, str, str], str]:
    """
    Index fixtures by (kickoff_date_utc, norm_home, norm_away) -> fixture_id
    kickoff_date_utc is YYYY-MM-DD from commence_time_utc.
    """
    cur = con.cursor()
    fx = cur.execute(
        """
        SELECT fixture_id, commence_time_utc, home_team, away_team
        FROM fixtures
        """
    ).fetchall()

    idx: Dict[Tuple[str, str, str], str] = {}
    for r in fx:
        dt = _parse_iso(r["commence_time_utc"])
        if not dt:
            continue
        d = dt.date().isoformat()
        key = (d, _norm_team(r["home_team"]), _norm_team(r["away_team"]))
        idx[key] = r["fixture_id"]
    return idx


def map_odds_to_fixture_id(
    fixture_idx: Dict[Tuple[str, str, str], str],
    commence_time_utc: str,
    home_team: str,
    away_team: str,
) -> Optional[str]:
    dt = _parse_iso(commence_time_utc)
    if not dt:
        return None
    d = dt.date().isoformat()
    key = (d, _norm_team(home_team), _norm_team(away_team))
    return fixture_idx.get(key)


def store_totals_and_alternates(con, odds_events: List[Dict[str, Any]], captured_at_utc: str) -> int:
    """
    Stores totals + alternate_totals as rows in odds_snapshots.
    Each row: one bookmaker, one fixture, one market, one line, over/under prices.
    Skips events that cannot be mapped to a fixture to avoid FK crashes.
    """
    fixture_idx = build_fixture_index(con)

    rows = []

    for ev in odds_events:
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not commence or not home or not away:
            continue

        fixture_id = map_odds_to_fixture_id(fixture_idx, commence, home, away)
        if not fixture_id:
            # Do NOT insert rows that would violate FK
            continue

        for bm in ev.get("bookmakers", []) or []:
            bm_name = bm.get("title")
            if not bm_name:
                continue

            for mk in bm.get("markets", []) or []:
                mk_key = mk.get("key")
                if mk_key not in ("totals", "alternate_totals"):
                    continue

                # outcomes: Over/Under with "point" and "price"
                # group by line (point) because alternate_totals yields many points
                by_line: Dict[float, Dict[str, float]] = {}
                for out in mk.get("outcomes", []) or []:
                    name = out.get("name")
                    price = out.get("price")
                    point = out.get("point")
                    if name not in ("Over", "Under"):
                        continue
                    try:
                        p = float(price)
                        ln = float(point)
                    except Exception:
                        continue

                    if ln not in by_line:
                        by_line[ln] = {}
                    by_line[ln][name] = p

                for ln, prices in by_line.items():
                    over_p = prices.get("Over")
                    under_p = prices.get("Under")
                    # keep rows even if one side missing
                    rows.append(
                        (
                            captured_at_utc,
                            fixture_id,
                            bm_name,
                            mk_key,
                            ln,
                            over_p,
                            under_p,
                        )
                    )

    sql = """
    INSERT INTO odds_snapshots (
      captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(captured_at_utc, fixture_id, bookmaker, market, line) DO UPDATE SET
      over_price=excluded.over_price,
      under_price=excluded.under_price
    """

    executemany(con, sql, rows)
    return len(rows)


def main() -> None:
    settings = get_settings()
    con = connect(settings.db_path)
    init_db(con)

    captured_at = utcnow_iso()

    # Fixtures first
    pl = fetch_pl_matches(settings.football_data_token, days_back=2, days_forward=14)
    n_fx = upsert_fixtures(con, pl)

    # Then odds
    # IMPORTANT: totals is often just the main line, alternate_totals gives the other lines.
    markets = "totals,alternate_totals"
    odds_events = fetch_odds_events(
        oa_key=settings.odds_api_key,
        regions=settings.odds_regions,
        markets=markets,
        odds_format=settings.odds_format,
        date_format=settings.date_format,
    )
    n_odds = store_totals_and_alternates(con, odds_events, captured_at)

    print(f"Upserted fixtures: {n_fx}")
    print(f"Stored odds snapshots: {n_odds}")


if __name__ == "__main__":
    main()
