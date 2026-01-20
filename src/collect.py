from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# Bulk odds endpoint supported markets (v4 /sports/{sport_key}/odds)
BULK_ALLOWED_MARKETS = {"h2h", "spreads", "totals", "outrights"}

# Event odds endpoint markets commonly supported (v4 /sports/{sport_key}/events/{event_id}/odds)
# We care about alternate_totals for multiple goal lines.
EVENT_ALLOWED_MARKETS = {"h2h", "spreads", "totals", "alternate_totals", "outrights"}


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _dedupe_keep_order(items: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for x in items:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def sanitize_bulk_markets(markets: str) -> str:
    parts = [_norm(x) for x in (markets or "").split(",") if _norm(x)]
    keep = [p for p in parts if p in BULK_ALLOWED_MARKETS]
    if not keep:
        return "totals"
    return ",".join(_dedupe_keep_order(keep))


def sanitize_event_markets(markets: str) -> str:
    parts = [_norm(x) for x in (markets or "").split(",") if _norm(x)]
    keep = [p for p in parts if p in EVENT_ALLOWED_MARKETS]
    if not keep:
        return "alternate_totals"
    return ",".join(_dedupe_keep_order(keep))


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
        commence = m.get("utcDate")
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


def fetch_odds_bulk(
    odds_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> list[dict]:
    markets = sanitize_bulk_markets(markets)
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


def fetch_event_odds(
    odds_key: str,
    sport_key: str,
    event_id: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> dict:
    markets = sanitize_event_markets(markets)
    r = requests.get(
        f"{ODDS_API_BASE}/sports/{sport_key}/events/{event_id}/odds",
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


def _fixture_id_from_event(con, ev: dict) -> str | None:
    """
    Map Odds API event -> our fixtures table.
    We match by commence_time_utc + team names (lower).
    """
    commence = ev.get("commence_time")
    home = ev.get("home_team")
    away = ev.get("away_team")
    if not (commence and home and away):
        return None

    cur = con.cursor()
    got = cur.execute(
        """
        SELECT fixture_id FROM fixtures
        WHERE commence_time_utc = ?
          AND lower(home_team) = ?
          AND lower(away_team) = ?
        LIMIT 1
        """,
        (commence, _norm(home), _norm(away)),
    ).fetchone()
    if not got:
        return None
    return got["fixture_id"]


def store_totals_like_market(con, ev_or_events, captured_at: str, market_key: str) -> int:
    """
    Store totals-style markets (totals or alternate_totals) into odds_snapshots as:
      over_price, under_price, line
    """
    if isinstance(ev_or_events, dict):
        events = [ev_or_events]
    else:
        events = list(ev_or_events)

    rows = []
    cur = con.cursor()

    for ev in events:
        fixture_id = _fixture_id_from_event(con, ev)
        if not fixture_id:
            continue

        for bm in ev.get("bookmakers", []):
            bm_title = bm.get("title") or bm.get("key") or "unknown_book"

            for mk in bm.get("markets", []):
                if mk.get("key") != market_key:
                    continue

                # outcomes include Over/Under with "point" as the line
                by_line: dict[float, dict[str, float]] = {}

                for out in mk.get("outcomes", []):
                    point = out.get("point")
                    name = _norm(out.get("name"))
                    price = out.get("price")
                    if point is None or price is None:
                        continue
                    try:
                        ln = float(point)
                        pr = float(price)
                    except (TypeError, ValueError):
                        continue

                    if name in ("over", "under"):
                        by_line.setdefault(ln, {})[name] = pr

                for ln, ou in by_line.items():
                    if "over" in ou and "under" in ou:
                        rows.append(
                            (
                                captured_at,
                                fixture_id,
                                bm_title,
                                market_key,
                                ln,
                                ou["over"],
                                ou["under"],
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
    p.add_argument("--days-forward", type=int, default=21)
    p.add_argument("--alt-sleep", type=float, default=0.25, help="Seconds to sleep between event odds calls")
    args = p.parse_args()

    s = get_settings()
    con = connect(s.db_path)
    init_db(con)

    # 1) Fixtures from football-data
    matches = fetch_pl_matches(s.football_data_token, args.days_back, args.days_forward)
    n_fix = upsert_fixtures(con, matches)

    # 2) Bulk odds (fast) for main totals
    captured_at = utcnow_iso()
    bulk_events = fetch_odds_bulk(
        odds_key=s.odds_api_key,
        sport_key=s.odds_sport_key,
        regions=s.odds_regions,
        markets=s.odds_markets,  # should include "totals" at least
        odds_format=s.odds_format,
        date_format=s.date_format,
    )

    n_totals = store_totals_like_market(con, bulk_events, captured_at, "totals")

    # 3) Alternate totals (slow) using per-event endpoint
    n_alt = 0
    n_events = 0
    for ev in bulk_events:
        event_id = ev.get("id")
        if not event_id:
            continue

        n_events += 1
        try:
            ev_detail = fetch_event_odds(
                odds_key=s.odds_api_key,
                sport_key=s.odds_sport_key,
                event_id=str(event_id),
                regions=s.odds_regions,
                markets="alternate_totals",
                odds_format=s.odds_format,
                date_format=s.date_format,
            )
            # Event endpoint returns a single event object with bookmakers/markets
            n_alt += store_totals_like_market(con, ev_detail, captured_at, "alternate_totals")
        except requests.HTTPError as e:
            # If a specific event has no alternates in your regions, ignore it
            print(f"Alternate totals skipped for event {event_id}: {e}")
        time.sleep(max(0.0, float(args.alt_sleep)))

    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored totals snapshots: {n_totals}")
    print(f"Stored alternate_totals snapshots: {n_alt}")
    print(f"Events checked for alternates: {n_events}")
    print(f"Bulk markets used: {sanitize_bulk_markets(s.odds_markets)}")


if __name__ == "__main__":
    main()
