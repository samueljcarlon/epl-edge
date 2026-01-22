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

# Supported by:
#   GET /sports/{sport}/odds            (base, cheap, supports multiple markets)
#   GET /sports/{sport}/events/{id}/odds (event-only, for certain markets like BTTS)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, default=None)
    p.add_argument("--days-back", type=int, default=3)
    p.add_argument("--days-forward", type=int, default=14)
    return p.parse_args()


def ensure_env(settings) -> None:
    if not settings.football_data_token:
        raise RuntimeError("Missing FOOTBALL_DATA_TOKEN")
    if not settings.odds_api_key:
        raise RuntimeError("Missing ODDS_API_KEY")


def sanitize_base_markets(markets: str) -> str:
    """
    Only allow markets that are known to be supported by the base endpoint.
    Anything else gets dropped silently.
    """
    allowed = {"h2h", "spreads", "totals"}
    want = [m.strip() for m in markets.split(",") if m.strip()]
    kept = [m for m in want if m in allowed]
    return ",".join(kept) if kept else "h2h"


def fetch_matches(
    token: str,
    competition: str,
    date_from: str,
    date_to: str,
) -> dict[str, Any]:
    r = requests.get(
        f"{FOOTBALL_DATA_BASE}/competitions/{competition}/matches",
        headers={"X-Auth-Token": token},
        params={"dateFrom": date_from, "dateTo": date_to},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def fetch_odds_base(
    odds_key: str,
    sport_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    date_format: str,
) -> list[dict[str, Any]]:
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

    # Debug for 401/429/403 etc without printing the key
    if r.status_code >= 400:
        print("ODDS_BASE_FAIL", r.status_code)
        print(
            "ODDS_BASE_HEADERS",
            {
                k: v
                for k, v in r.headers.items()
                if ("request" in k.lower() or "rate" in k.lower())
            },
        )
        print("ODDS_BASE_BODY", r.text[:300])

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
    Fetch event-only markets like BTTS.
    If rejected (422, 401, 429 etc), skip without killing the run.
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
        print("ODDS_EVENT_FAIL", r.status_code, sport_key, event_id, markets)
        print(
            "ODDS_EVENT_HEADERS",
            {
                k: v
                for k, v in r.headers.items()
                if ("request" in k.lower() or "rate" in k.lower())
            },
        )
        print("ODDS_EVENT_BODY", r.text[:300])
        return None

    try:
        return r.json()
    except Exception:
        return None


def upsert_fixtures(con, matches_json: dict[str, Any]) -> None:
    rows: list[tuple[Any, ...]] = []
    for m in matches_json.get("matches", []):
        fixture_id = str(m.get("id"))
        commence = m.get("utcDate")
        status = m.get("status")
        home = (m.get("homeTeam") or {}).get("name")
        away = (m.get("awayTeam") or {}).get("name")
        score = m.get("score") or {}
        full = score.get("fullTime") or {}
        home_goals = full.get("home")
        away_goals = full.get("away")
        last_updated = m.get("lastUpdated")

        # matchweek can be missing
        season = m.get("season") or {}
        matchweek = season.get("currentMatchday")

        rows.append(
            (
                fixture_id,
                commence,
                matchweek,
                status,
                home,
                away,
                home_goals,
                away_goals,
                last_updated,
            )
        )

    if not rows:
        return

    executemany(
        con,
        """
        INSERT INTO fixtures(
            fixture_id, commence_time_utc, matchweek, status,
            home_team, away_team, home_goals, away_goals, last_updated_utc
        )
        VALUES(?,?,?,?,?,?,?,?,?)
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


def _fixture_id_for_event(con, commence: str, home: str, away: str) -> str | None:
    """
    IMPORTANT:
    sqlite fetchone() might return:
      - tuple (default)
      - sqlite3.Row (indexable by name)
      - dict (if you set a dict row_factory elsewhere)
    So we handle all three.
    """
    cur = con.cursor()
    got = cur.execute(
        """
        SELECT fixture_id
        FROM fixtures
        WHERE commence_time_utc = ?
          AND home_team = ?
          AND away_team = ?
        LIMIT 1
        """,
        (commence, home, away),
    ).fetchone()

    if got is None:
        return None
    if isinstance(got, dict):
        return got.get("fixture_id")
    try:
        return got["fixture_id"]  # sqlite3.Row
    except Exception:
        return got[0]  # tuple fallback


def extract_total_prices(
    bookmakers: list[dict[str, Any]], wanted_lines: list[float]
) -> list[tuple[str, float, float, float]]:
    """
    Return rows:
      (bookmaker_key, line, over_price, under_price)
    for totals market only.
    """
    out: list[tuple[str, float, float, float]] = []

    for b in bookmakers or []:
        bkey = b.get("key") or b.get("title") or ""
        for market in b.get("markets") or []:
            if market.get("key") != "totals":
                continue

            for o in market.get("outcomes") or []:
                # outcome: {name: "Over"/"Under", price: 1.9, point: 2.5}
                line = o.get("point")
                if line is None:
                    continue
                try:
                    line_f = float(line)
                except Exception:
                    continue
                if wanted_lines and line_f not in wanted_lines:
                    continue

            # We need pairs for Over/Under per line
            # Build a map line -> (over, under)
            pairs: dict[float, dict[str, float]] = {}
            for o in market.get("outcomes") or []:
                line = o.get("point")
                price = o.get("price")
                name = (o.get("name") or "").lower()
                if line is None or price is None:
                    continue
                try:
                    line_f = float(line)
                    price_f = float(price)
                except Exception:
                    continue
                if wanted_lines and line_f not in wanted_lines:
                    continue

                if line_f not in pairs:
                    pairs[line_f] = {}
                if "over" in name:
                    pairs[line_f]["over"] = price_f
                elif "under" in name:
                    pairs[line_f]["under"] = price_f

            for line_f, pu in pairs.items():
                if "over" in pu and "under" in pu:
                    out.append((bkey, line_f, pu["over"], pu["under"]))

    return out


def extract_btts_prices(bookmakers: list[dict[str, Any]]) -> list[tuple[str, float, float]]:
    """
    Return rows:
      (bookmaker_key, yes_price, no_price)
    from btts market outcomes.
    """
    out: list[tuple[str, float, float]] = []

    for b in bookmakers or []:
        bkey = b.get("key") or b.get("title") or ""
        for market in b.get("markets") or []:
            if market.get("key") != "btts":
                continue

            yes_p = None
            no_p = None

            for o in market.get("outcomes") or []:
                name = (o.get("name") or "").strip().lower()
                price = o.get("price")
                if price is None:
                    continue
                try:
                    price_f = float(price)
                except Exception:
                    continue

                if name in {"yes", "y", "true"}:
                    yes_p = price_f
                elif name in {"no", "n", "false"}:
                    no_p = price_f

            if yes_p is not None and no_p is not None:
                out.append((bkey, yes_p, no_p))

    return out


def write_totals_odds_snapshots(
    con,
    captured_at_utc: str,
    event: dict[str, Any],
    wanted_lines: list[float],
) -> None:
    event_id = event.get("id")
    commence = event.get("commence_time")
    home = event.get("home_team")
    away = event.get("away_team")

    if not (event_id and commence and home and away):
        return

    fixture_id = _fixture_id_for_event(con, commence, home, away)
    if fixture_id is None:
        return

    rows: list[tuple[Any, ...]] = []
    bookmakers = event.get("bookmakers") or []
    totals_rows = extract_total_prices(bookmakers, wanted_lines)

    for bookmaker_key, line, over_p, under_p in totals_rows:
        rows.append(
            (
                captured_at_utc,
                fixture_id,
                bookmaker_key,
                "totals",
                line,
                over_p,
                under_p,
            )
        )

    if rows:
        executemany(
            con,
            """
            INSERT INTO odds_snapshots(
                captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            rows,
        )


def write_btts_odds_snapshots(
    con,
    captured_at_utc: str,
    event_odds_json: dict[str, Any],
) -> None:
    event_id = event_odds_json.get("id")
    commence = event_odds_json.get("commence_time")
    home = event_odds_json.get("home_team")
    away = event_odds_json.get("away_team")

    if not (event_id and commence and home and away):
        return

    fixture_id = _fixture_id_for_event(con, commence, home, away)
    if fixture_id is None:
        return

    rows: list[tuple[Any, ...]] = []
    bookmakers = event_odds_json.get("bookmakers") or []
    btts_rows = extract_btts_prices(bookmakers)

    for bookmaker_key, yes_p, no_p in btts_rows:
        # Store btts as "line=None", encode yes as over_price, no as under_price
        rows.append(
            (
                captured_at_utc,
                fixture_id,
                bookmaker_key,
                "btts",
                None,
                yes_p,
                no_p,
            )
        )

    if rows:
        executemany(
            con,
            """
            INSERT INTO odds_snapshots(
                captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            rows,
        )


def iso_date(d: datetime) -> str:
    return d.date().isoformat()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    ensure_env(settings)

    now = datetime.now(timezone.utc)
    date_from = iso_date(now - timedelta(days=args.days_back))
    date_to = iso_date(now + timedelta(days=args.days_forward))

    matches_json = fetch_matches(
        token=settings.football_data_token,
        competition=settings.football_competition,
        date_from=date_from,
        date_to=date_to,
    )

    con = connect(settings.db_path)
    init_db(con)

    upsert_fixtures(con, matches_json)

    captured_at = utcnow_iso()

    base_events = fetch_odds_base(
        odds_key=settings.odds_api_key,
        sport_key=settings.odds_sport_key,
        regions=settings.odds_regions,
        markets=settings.odds_markets,
        odds_format=settings.odds_format,
        date_format=settings.odds_date_format,
    )

    # Write totals snapshots from base events
    try:
        wanted_lines = [float(x) for x in (settings.totals_lines or "").split(",") if x.strip()]
    except Exception:
        wanted_lines = []

    for e in base_events or []:
        write_totals_odds_snapshots(con, captured_at, e, wanted_lines)

    # Event-only BTTS fetch (optional, non-fatal)
    # This is what can explode request volume, so it is kept non-fatal by design.
    for e in base_events or []:
        event_id = e.get("id")
        if not event_id:
            continue

        event_json = fetch_event_odds(
            odds_key=settings.odds_api_key,
            sport_key=settings.odds_sport_key,
            event_id=event_id,
            regions=settings.odds_regions,
            markets="btts",
            odds_format=settings.odds_format,
            date_format=settings.odds_date_format,
        )

        if event_json is None:
            continue

        write_btts_odds_snapshots(con, captured_at, event_json)

    con.close()
    print("collect done", captured_at)


if __name__ == "__main__":
    main()
