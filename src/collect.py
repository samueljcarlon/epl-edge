from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"


# -----------------------------
# Time + string normalisation
# -----------------------------
def parse_utc(dt_str: str) -> datetime | None:
    """
    Parse common ISO timestamps into an aware UTC datetime.
    Handles:
      - 2026-01-19T09:59:39Z
      - 2026-01-19T09:59:39+00:00
      - 2026-01-19T09:59:39.751488Z
    """
    if not dt_str:
        return None

    s = dt_str.strip()
    # Handle Zulu
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def iso_z_seconds(dt: datetime) -> str:
    """UTC ISO string to seconds, with trailing Z."""
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def norm_name(s: str) -> str:
    return (s or "").strip().lower()


def name_score(a: str, b: str) -> int:
    """
    Cheap similarity score for team names.
    - exact match => high
    - shared tokens => medium
    - otherwise => low
    """
    a = norm_name(a)
    b = norm_name(b)
    if not a or not b:
        return 0
    if a == b:
        return 100

    ta = set(a.replace("&", "and").split())
    tb = set(b.replace("&", "and").split())
    inter = len(ta & tb)
    if inter == 0:
        return 0
    return 10 * inter


# -----------------------------
# Football-Data fixtures
# -----------------------------
def fetch_pl_matches(fd_token: str, days_back: int, days_forward: int) -> dict:
    today = datetime.now(timezone.utc).date()
    date_from = (today - timedelta(days=days_back)).isoformat()
    date_to = (today + timedelta(days=days_forward)).isoformat()

    r = requests.get(
        f"{FOOTBALL_DATA_BASE}/competitions/PL/matches",
        headers={"X-Auth-Token": fd_token},
        params={"dateFrom": date_from, "dateTo": date_to},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def upsert_fixtures(con, matches_json: dict) -> int:
    rows = []
    now_iso = utcnow_iso()

    for m in matches_json.get("matches", []):
        fixture_id = str(m["id"])

        raw_commence = m.get("utcDate")
        dt = parse_utc(raw_commence) if raw_commence else None
        commence = iso_z_seconds(dt) if dt else raw_commence

        status = m.get("status", "UNKNOWN")
        matchday = m.get("matchday")
        home = (m.get("homeTeam") or {}).get("name") or "UNKNOWN_HOME"
        away = (m.get("awayTeam") or {}).get("name") or "UNKNOWN_AWAY"

        score = m.get("score") or {}
        full = score.get("fullTime") or {}
        hg = full.get("home")
        ag = full.get("away")

        rows.append((fixture_id, commence, matchday, status, home, away, hg, ag, now_iso))

    executemany(
        con,
        """
        INSERT INTO fixtures (
          fixture_id, commence_time_utc, matchweek, status, home_team, away_team,
          home_goals, away_goals, last_updated_utc
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
        """,
        rows,
    )
    return len(rows)


# -----------------------------
# Odds API
# -----------------------------
def fetch_odds(
    odds_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> list[dict]:
    r = requests.get(
        f"{ODDS_API_BASE}/sports/{sport_key}/odds",
        params={
            "apiKey": odds_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def find_fixture_id(con, commence_time: str, home: str, away: str, window_minutes: int = 10) -> str | None:
    """
    Match odds event -> fixtures row.
    We use:
      1) Normalised commence time
      2) A ±window query in SQL
      3) Pick best name match
    """
    dt = parse_utc(commence_time)
    if not dt:
        return None

    start = iso_z_seconds(dt - timedelta(minutes=window_minutes))
    end = iso_z_seconds(dt + timedelta(minutes=window_minutes))
    target_commence = iso_z_seconds(dt)

    cur = con.cursor()
    candidates = cur.execute(
        """
        SELECT fixture_id, commence_time_utc, home_team, away_team
        FROM fixtures
        WHERE commence_time_utc >= ?
          AND commence_time_utc <= ?
        """,
        (start, end),
    ).fetchall()

    if not candidates:
        return None

    # Score candidates by time closeness + name similarity
    best_id = None
    best_score = -1

    for c in candidates:
        c_dt = parse_utc(c["commence_time_utc"]) or dt
        time_penalty = abs(int((c_dt - dt).total_seconds()))  # seconds
        time_score = max(0, 60 * window_minutes - time_penalty)  # higher is better

        hs = name_score(home, c["home_team"])
        as_ = name_score(away, c["away_team"])
        ns = hs + as_

        # Prefer exact time string too if it happens to match
        exact_bonus = 50 if c["commence_time_utc"] == target_commence else 0

        score = ns * 10 + time_score + exact_bonus
        if score > best_score:
            best_score = score
            best_id = c["fixture_id"]

    return best_id


def store_totals_snapshots(
    con,
    events: list[dict],
    captured_at: str,
    target_line: float = 2.5,
    window_minutes: int = 10,
) -> int:
    rows = []

    for ev in events:
        commence_raw = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not (commence_raw and home and away):
            continue

        fixture_id = find_fixture_id(con, commence_raw, home, away, window_minutes=window_minutes)
        if not fixture_id:
            continue

        for bm in ev.get("bookmakers", []):
            bm_title = bm.get("title") or bm.get("key") or "unknown_book"

            for mk in bm.get("markets", []):
                if mk.get("key") != "totals":
                    continue

                over_price = None
                under_price = None
                line = None

                for out in mk.get("outcomes", []):
                    point = out.get("point")
                    price = out.get("price")
                    name = norm_name(out.get("name"))

                    if point is None or price is None:
                        continue

                    try:
                        p = float(point)
                        pr = float(price)
                    except (TypeError, ValueError):
                        continue

                    if p != float(target_line):
                        continue

                    line = p
                    if name == "over":
                        over_price = pr
                    elif name == "under":
                        under_price = pr

                if line is None:
                    continue

                rows.append(
                    (
                        captured_at,
                        fixture_id,
                        bm_title,
                        "totals",
                        line,
                        over_price,
                        under_price,
                    )
                )

    executemany(
        con,
        """
        INSERT INTO odds_snapshots (
          captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--days-back", type=int, default=14)
    p.add_argument("--days-forward", type=int, default=14)
    p.add_argument("--line", type=float, default=2.5)
    p.add_argument("--match-window-mins", type=int, default=10)
    args = p.parse_args()

    s = get_settings()
    con = connect(s.db_path)
    init_db(con)

    matches = fetch_pl_matches(s.football_data_token, args.days_back, args.days_forward)
    n_fix = upsert_fixtures(con, matches)

    captured_at = utcnow_iso()
    events = fetch_odds(
        odds_key=s.odds_api_key,
        sport_key=s.odds_sport_key,
        regions=s.odds_regions,
        markets=s.odds_markets,
        odds_format=s.odds_format,
        date_format=s.date_format,
    )
    n_odds = store_totals_snapshots(
        con,
        events,
        captured_at,
        target_line=args.line,
        window_minutes=args.match_window_mins,
    )

    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored odds snapshots: {n_odds}")


if __name__ == "__main__":
    main()
