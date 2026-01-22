from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from src.db import connect, init_db


ALLOWED_MARKETS = ("totals", "alternate_totals", "spreads", "btts")


def export_odds_json(db_path: str, out_path: str, limit: int = 5000) -> int:
    con = connect(db_path)
    init_db(con)
    cur = con.cursor()

    # Latest snapshot per (fixture_id, bookmaker, market, line)
    # Export only markets we actually support in the UI (2-outcome style: over/under or yes/no).
    rows = cur.execute(
        """
        WITH ranked AS (
          SELECT
            o.captured_at_utc,
            o.fixture_id,
            f.commence_time_utc,
            f.matchweek,
            f.status,
            f.home_team,
            f.away_team,
            f.home_goals,
            f.away_goals,
            o.bookmaker,
            o.market,
            o.line,
            o.over_price,
            o.under_price,
            ROW_NUMBER() OVER (
              PARTITION BY o.fixture_id, o.bookmaker, o.market, o.line
              ORDER BY o.captured_at_utc DESC
            ) AS rn
          FROM odds_snapshots o
          JOIN fixtures f ON f.fixture_id = o.fixture_id
          WHERE o.market IN (?, ?, ?, ?)
        )
        SELECT
          captured_at_utc,
          fixture_id,
          commence_time_utc,
          matchweek,
          status,
          home_team,
          away_team,
          home_goals,
          away_goals,
          bookmaker,
          market,
          line,
          over_price,
          under_price
        FROM ranked
        WHERE rn = 1
        ORDER BY commence_time_utc ASC, fixture_id ASC, bookmaker ASC, market ASC, line ASC
        LIMIT ?
        """,
        (*ALLOWED_MARKETS, limit),
    ).fetchall()

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(rows),
        "items": [],
    }

    for r in rows:
        mk = r["market"]
        ln = r["line"]

        # For BTTS we don't want the UI showing a fake "line" like 0.0, so export as null.
        if mk == "btts":
            ln = None

        payload["items"].append(
            {
                "captured_at_utc": r["captured_at_utc"],
                "fixture_id": r["fixture_id"],
                "commence_time_utc": r["commence_time_utc"],
                "matchweek": r["matchweek"],
                "status": r["status"],
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "home_goals": r["home_goals"],
                "away_goals": r["away_goals"],
                "bookmaker": r["bookmaker"],
                "market": mk,
                "line": ln,
                "over_price": r["over_price"],
                "under_price": r["under_price"],
            }
        )

    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return len(rows)


def main() -> None:
    db_path = os.environ.get("DB_PATH", "data/app.db")
    out_path = os.environ.get("OUT_JSON_PATH", "site/public/odds.json")
    limit = int(os.environ.get("EXPORT_LIMIT", "5000"))

    n = export_odds_json(db_path=db_path, out_path=out_path, limit=limit)
    print(f"Exported {n} rows to {out_path}")


if __name__ == "__main__":
    main()
