from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from src.db import connect, init_db, executemany
from src.settings import get_settings
from src.utils import utcnow_iso

FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# Markets supported by:
#   GET /v4/sports/{sport_key}/odds
BASE_ODDS_MARKETS = {"h2h", "spreads", "totals", "outrights"}

# Markets that often require:
#   GET /v4/sports/{sport_key}/events/{event_id}/odds
EVENT_ONLY_MARKETS = {"btts"}


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _dedupe_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in items:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def sanitize_base_markets(markets: str) -> str:
    """
    Keep only markets supported by /sports/{sport_key}/odds.
    """
    parts = [_norm(x) for x in (markets or "").split(",") if _norm(x)]
    keep = [p for p in parts if p in BASE_ODDS_MARKETS]
    if not keep:
        keep = ["totals"]
    return ",".join(_dedupe_keep_order(keep))


def fetch_pl_matches(fd_token: str, days_back: int, days_forward: int) -> dict[str, Any]:
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


def upsert_fixtures(con, matches_json: dict[str, Any]) -> int:
    rows: list[tuple[Any, ...]] = []
    now_iso = utcnow_iso()

    for m in matches_json.get("matches", []):
        fixture_id = str(m["id"])
        commence = m.get("utcDate")
        status = m.get("status", "UNKNOWN")
        matchday = m.get("matchday")
        home = (m.get("homeTeam") or {}).get("name") or "UNKNOWN_HOME"
        away = (m.get("awayTeam") or {}).get("name") or "UNKNOWN_AWAY"
        score = m.get("score") or {}
        full = (score.get("fullTime") or {})
        hg = full.get("home")
        ag = full.get("away")

        rows.append((fixture_id, commence, matchday, status, home, away, hg, ag, now_iso))

    if not rows:
        return 0

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


def _debug_odds_response(tag: str, r: requests.Response) -> None:
    print(tag, r.status_code)
    try:
        hdrs = {
            k: v
            for k, v in r.headers.items()
            if ("request" in k.lower() or "rate" in k.lower() or "quota" in k.lower())
        }
        if hdrs:
            print(tag + "_HEADERS", hdrs)
    except Exception:
        pass

    try:
        txt = (r.text or "")[:600]
        if txt:
            print(tag + "_BODY", txt)
    except Exception:
        pass


def fetch_odds_base(
    odds_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> list[dict[str, Any]]:
    """
    Base odds call. If out of usage credits, return [] and do NOT crash the workflow.
    """
    markets_clean = sanitize_base_markets(markets)

    r = requests.get(
        f"{ODDS_API_BASE}/sports/{sport_key}/odds",
        params={
            "apiKey": odds_key,
            "regions": regions,
            "markets": markets_clean,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        },
        timeout=30,
    )

    if r.status_code >= 400:
        _debug_odds_response("ODDS_BASE_FAIL", r)

        # Critical: do not hard-fail the job when you're out of credits.
        # The Odds API returns:
        #   {"error_code":"OUT_OF_USAGE_CREDITS", ...}
        try:
            j = r.json()
            if isinstance(j, dict) and j.get("error_code") == "OUT_OF_USAGE_CREDITS":
                print("ODDS_BASE_SKIP out of usage credits, returning [] so workflow continues")
                return []
        except Exception:
            pass

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
) -> dict[str, Any] | None:
    """
    Event-only odds call (used for BTTS here). Non-fatal by design.
    """
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

    if r.status_code >= 400:
        _debug_odds_response("ODDS_EVENT_FAIL", r)
        return None

    try:
        return r.json()
    except Exception:
        print("ODDS_EVENT_JSON_FAIL", sport_key, event_id, markets)
        return None


def _fixture_id_for_event(con, commence: str, home: str, away: str) -> str | None:
    """
    Match Odds API event to our fixtures table by (commence_time_utc, home, away).
    Handles sqlite3.Row or tuple.
    """
    cur = con.cursor()
    got = cur.execute(
        """
        SELECT fixture_id
        FROM fixtures
        WHERE commence_time_utc = ?
          AND lower(home_team) = ?
          AND lower(away_team) = ?
        LIMIT 1
        """,
        (commence, _norm(home), _norm(away)),
    ).fetchone()

    if not got:
        return None

    try:
        return str(got["fixture_id"])  # sqlite3.Row
    except Exception:
        try:
            return str(got[0])  # tuple fallback
        except Exception:
            return None


def btts_already_captured_today(con, fixture_id: str) -> bool:
    """
    True if we already wrote BTTS rows for this fixture today.
    Uses captured_at_utc YYYY-MM-DD prefix.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    row = con.execute(
        """
        SELECT 1
        FROM odds_snapshots
        WHERE fixture_id = ?
          AND market = 'btts'
          AND substr(captured_at_utc, 1, 10) = ?
        LIMIT 1
        """,
        (fixture_id, today),
    ).fetchone()
    return row is not None


def _store_rows(con, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0

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


def store_base_market_snapshots(con, events: list[dict[str, Any]], captured_at: str) -> int:
    """
    Stores totals + spreads from /sports/{sport_key}/odds.
    totals:
      line = total points
      over_price = Over
      under_price = Under
    spreads:
      line = HOME handicap (stored as float)
      over_price = Home price
      under_price = Away price
    """
    rows: list[tuple[Any, ...]] = []

    for ev in events or []:
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not (commence and home and away):
            continue

        fixture_id = _fixture_id_for_event(con, commence, home, away)
        if not fixture_id:
            continue

        for bm in ev.get("bookmakers", []) or []:
            bm_title = bm.get("title") or bm.get("key") or "unknown_book"

            for mk in bm.get("markets", []) or []:
                mkey = mk.get("key")

                if mkey == "totals":
                    by_line: dict[float, dict[str, float]] = {}
                    for out in mk.get("outcomes", []) or []:
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
                        if name not in ("over", "under"):
                            continue
                        by_line.setdefault(ln, {})[name] = pr

                    for ln, ou in by_line.items():
                        if "over" in ou and "under" in ou:
                            rows.append((captured_at, fixture_id, bm_title, "totals", ln, ou["over"], ou["under"]))

                elif mkey == "spreads":
                    # map line -> {home: price, away: price}
                    by_line: dict[float, dict[str, float]] = {}
                    for out in mk.get("outcomes", []) or []:
                        point = out.get("point")
                        name = out.get("name")
                        price = out.get("price")
                        if point is None or name is None or price is None:
                            continue
                        try:
                            ln = float(point)
                            pr = float(price)
                        except (TypeError, ValueError):
                            continue

                        nm = _norm(str(name))
                        if nm == _norm(home):
                            by_line.setdefault(ln, {})["home"] = pr
                        elif nm == _norm(away):
                            by_line.setdefault(ln, {})["away"] = pr

                    for ln, vals in by_line.items():
                        if "home" in vals and "away" in vals:
                            rows.append((captured_at, fixture_id, bm_title, "spreads", ln, vals["home"], vals["away"]))

    return _store_rows(con, rows)


def store_btts_snapshots(
    con,
    base_events: list[dict[str, Any]],
    captured_at: str,
    settings,
    max_btts_events: int,
) -> int:
    """
    BTTS must be fetched per-event. To stop nuking credits:
      - only consider upcoming events
      - only first max_btts_events events
      - skip if BTTS already captured today
    Stored as:
      market='btts'
      line=0.0
      over_price=Yes
      under_price=No
    """
    rows: list[tuple[Any, ...]] = []
    now = datetime.now(timezone.utc)

    # Filter to upcoming events only
    upcoming: list[dict[str, Any]] = []
    for ev in base_events or []:
        ct = ev.get("commence_time")
        if not ct:
            continue
        try:
            # Odds API gives ISO like "2026-01-22T19:45:00Z"
            dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt >= now:
            upcoming.append(ev)

    # Sort by kick-off time and take first N
    upcoming.sort(key=lambda e: e.get("commence_time") or "")
    upcoming = upcoming[: max(0, int(max_btts_events))]

    for ev in upcoming:
        event_id = ev.get("id")
        commence = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not (event_id and commence and home and away):
            continue

        fixture_id = _fixture_id_for_event(con, commence, home, away)
        if not fixture_id:
            continue

        # Skip if we already did BTTS today for this fixture
        if btts_already_captured_today(con, fixture_id):
            continue

        payload = fetch_event_odds(
            odds_key=settings.odds_api_key,
            sport_key=settings.odds_sport_key,
            event_id=str(event_id),
            regions=settings.odds_regions,
            markets="btts",
            odds_format=settings.odds_format,
            date_format=settings.date_format,
        )
        if not payload:
            continue

        for bm in payload.get("bookmakers", []) or []:
            bm_title = bm.get("title") or bm.get("key") or "unknown_book"

            for mk in bm.get("markets", []) or []:
                if mk.get("key") != "btts":
                    continue

                yes_price = None
                no_price = None

                for out in mk.get("outcomes", []) or []:
                    nm = _norm(out.get("name"))
                    pr = out.get("price")
                    if pr is None:
                        continue
                    try:
                        prf = float(pr)
                    except (TypeError, ValueError):
                        continue

                    if nm in ("yes", "y"):
                        yes_price = prf
                    elif nm in ("no", "n"):
                        no_price = prf

                if yes_price is not None and no_price is not None:
                    rows.append((captured_at, fixture_id, bm_title, "btts", 0.0, yes_price, no_price))

    return _store_rows(con, rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--days-back", type=int, default=None)
    p.add_argument("--days-forward", type=int, default=None)
    p.add_argument("--max-btts-events", type=int, default=5)
    args = p.parse_args()

    s = get_settings()

    days_back = int(args.days_back) if args.days_back is not None else int(s.days_back)
    days_forward = int(args.days_forward) if args.days_forward is not None else int(s.days_forward)

    con = connect(s.db_path)
    init_db(con)

    matches = fetch_pl_matches(s.football_data_token, days_back, days_forward)
    n_fix = upsert_fixtures(con, matches)

    captured_at = utcnow_iso()

    base_events = fetch_odds_base(
        odds_key=s.odds_api_key,
        sport_key=s.odds_sport_key,
        regions=s.odds_regions,
        markets=s.odds_markets,
        odds_format=s.odds_format,
        date_format=s.date_format,
    )

    n_base = store_base_market_snapshots(con, base_events, captured_at)

    # BTTS is optional and tightly rate-limited
    n_btts = store_btts_snapshots(con, base_events, captured_at, s, max_btts_events=args.max_btts_events)

    con.close()

    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored base odds snapshots: {n_base}")
    print(f"Stored BTTS snapshots: {n_btts}")
    print(f"Base markets used: {sanitize_base_markets(s.odds_markets)}")
    print(f"Max BTTS events this run: {args.max_btts_events}")


if __name__ == "__main__":
    main()
