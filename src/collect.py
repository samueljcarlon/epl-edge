from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable, Optional

from src.db import connect, init_db
from src.sources import football_data_fixtures, odds_api_odds


def _now_utc_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_csv_floats(s: str) -> list[float]:
    out: list[float] = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out


def _parse_csv_strings(s: str) -> list[str]:
    return [p.strip() for p in (s or "").split(",") if p.strip()]


def _safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _extract_two_outcomes(odds_event: dict, market_key: str) -> list[dict]:
    """
    Returns a list of dict rows with keys:
    - bookmaker
    - market
    - line (float or None)
    - over_price
    - under_price

    For:
      totals: over, under with a line
      btts: yes, no (line None)
      spreads: home, away with a line (handicap)
    """
    out: list[dict] = []
    bookmakers = odds_event.get("bookmakers") or []
    for bm in bookmakers:
        bm_key = bm.get("key") or ""
        bm_title = bm.get("title") or bm_key or "Unknown"

        markets = bm.get("markets") or []
        m = None
        for mm in markets:
            if (mm.get("key") or "") == market_key:
                m = mm
                break
        if not m:
            continue

        outcomes = m.get("outcomes") or []
        if market_key == "totals":
            # outcomes typically have name "Over"/"Under", plus point
            over = next((o for o in outcomes if (o.get("name") or "").lower() == "over"), None)
            under = next((o for o in outcomes if (o.get("name") or "").lower() == "under"), None)
            if not over or not under:
                continue

            line = _safe_float(over.get("point"))
            if line is None:
                continue

            out.append(
                {
                    "bookmaker": bm_title,
                    "market": "totals",
                    "line": line,
                    "over_price": _safe_float(over.get("price")),
                    "under_price": _safe_float(under.get("price")),
                }
            )

        elif market_key == "btts":
            yes = next((o for o in outcomes if (o.get("name") or "").lower() in ("yes", "y")), None)
            no = next((o for o in outcomes if (o.get("name") or "").lower() in ("no", "n")), None)
            if not yes or not no:
                continue

            out.append(
                {
                    "bookmaker": bm_title,
                    "market": "btts",
                    "line": None,
                    "over_price": _safe_float(yes.get("price")),
                    "under_price": _safe_float(no.get("price")),
                }
            )

        elif market_key == "spreads":
            # outcomes are usually home team and away team, with point (handicap)
            if len(outcomes) < 2:
                continue
            a = outcomes[0]
            b = outcomes[1]
            line = _safe_float(a.get("point"))
            if line is None:
                line = _safe_float(b.get("point"))
            out.append(
                {
                    "bookmaker": bm_title,
                    "market": "spreads",
                    "line": line,
                    "over_price": _safe_float(a.get("price")),  # treat as "side A"
                    "under_price": _safe_float(b.get("price")),  # treat as "side B"
                }
            )

    # Filter any rows missing prices
    cleaned = [
        r for r in out
        if r.get("over_price") is not None and r.get("under_price") is not None
    ]
    return cleaned


def collect(db_path: str) -> tuple[int, int]:
    con = connect(db_path)
    init_db(con)
    cur = con.cursor()

    # 1) Fixtures from football-data
    fixtures = football_data_fixtures()
    n_fix = 0
    for f in fixtures:
        cur.execute(
            """
            INSERT INTO fixtures (
              fixture_id, commence_time_utc, matchweek, status,
              home_team, away_team, home_goals, away_goals, last_seen_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fixture_id) DO UPDATE SET
              commence_time_utc = excluded.commence_time_utc,
              matchweek = excluded.matchweek,
              status = excluded.status,
              home_team = excluded.home_team,
              away_team = excluded.away_team,
              home_goals = excluded.home_goals,
              away_goals = excluded.away_goals,
              last_seen_utc = excluded.last_seen_utc
            """,
            (
                f["fixture_id"],
                f["commence_time_utc"],
                f["matchweek"],
                f["status"],
                f["home_team"],
                f["away_team"],
                f.get("home_goals"),
                f.get("away_goals"),
                _now_utc_iso_z(),
            ),
        )
        n_fix += 1

    con.commit()

    # 2) Odds from The Odds API
    # You can override these in GitHub Actions env later if you want.
    markets = _parse_csv_strings(os.environ.get("ODDS_MARKETS", "totals,btts"))
    totals_lines = _parse_csv_floats(os.environ.get("TOTALS_LINES", "0.5,1.5,2.5,3.5"))
    regions = os.environ.get("ODDS_REGIONS", "uk")
    sport_key = os.environ.get("ODDS_SPORT_KEY", "soccer_epl")

    # Fetch raw odds events for markets.
    # Your src.sources.odds_api_odds should accept markets and totals lines.
    odds_events = odds_api_odds(
        sport_key=sport_key,
        regions=regions,
        markets=",".join(markets),
        totals_points=",".join(str(x) for x in totals_lines) if "totals" in markets else None,
    )

    captured_at = _now_utc_iso_z()
    n_snap = 0

    # Build quick fixture match map for lookup by teams + kickoff time if needed.
    fixture_rows = cur.execute(
        "SELECT fixture_id, home_team, away_team, commence_time_utc FROM fixtures"
    ).fetchall()

    def find_fixture_id(home: str, away: str, commence: str) -> Optional[str]:
        # Most robust match is by exact teams + exact commence, but we fall back to teams only.
        for fr in fixture_rows:
            if fr["home_team"] == home and fr["away_team"] == away and fr["commence_time_utc"] == commence:
                return fr["fixture_id"]
        for fr in fixture_rows:
            if fr["home_team"] == home and fr["away_team"] == away:
                return fr["fixture_id"]
        return None

    for ev in odds_events:
        home = ev.get("home_team")
        away = ev.get("away_team")
        commence = ev.get("commence_time")
        if not home or not away or not commence:
            continue

        fixture_id = find_fixture_id(home, away, commence)
        if not fixture_id:
            continue

        for mk in markets:
            rows = _extract_two_outcomes(ev, mk)
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO odds_snapshots (
                      captured_at_utc, fixture_id, bookmaker, market, line, over_price, under_price
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        captured_at,
                        fixture_id,
                        r["bookmaker"],
                        r["market"],
                        r["line"],
                        r["over_price"],
                        r["under_price"],
                    ),
                )
                n_snap += 1

    con.commit()
    return n_fix, n_snap


def main() -> None:
    db_path = os.environ.get("DB_PATH", "data/app.db")
    n_fix, n_snap = collect(db_path=db_path)
    print(f"Upserted fixtures: {n_fix}")
    print(f"Stored odds snapshots: {n_snap}")


if __name__ == "__main__":
    main()
