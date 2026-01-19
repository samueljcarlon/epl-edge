from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from datetime import datetime, timezone


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def export_latest(db_path: str, out_path: str) -> dict:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Latest snapshot per fixture + bookmaker + market + line
    rows = cur.execute(
        """
        WITH ranked AS (
          SELECT
            o.*,
            ROW_NUMBER() OVER (
              PARTITION BY o.fixture_id, o.bookmaker, o.market, o.line
              ORDER BY o.captured_at_utc DESC
            ) AS rn
          FROM odds_snapshots o
        )
        SELECT
          r.captured_at_utc,
          r.fixture_id,
          f.commence_time_utc,
          f.matchweek,
          f.status,
          f.home_team,
          f.away_team,
          f.home_goals,
          f.away_goals,
          r.bookmaker,
          r.market,
          r.line,
          r.over_price,
          r.under_price
        FROM ranked r
        JOIN fixtures f ON f.fixture_id = r.fixture_id
        WHERE r.rn = 1
        ORDER BY f.commence_time_utc ASC, f.home_team ASC, r.bookmaker ASC, r.line ASC
        """
    ).fetchall()

    items = []
    for row in rows:
        items.append(
            {
                "captured_at_utc": row["captured_at_utc"],
                "fixture_id": str(row["fixture_id"]),
                "commence_time_utc": row["commence_time_utc"],
                "matchweek": row["matchweek"],
                "status": row["status"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "home_goals": row["home_goals"],
                "away_goals": row["away_goals"],
                "bookmaker": row["bookmaker"],
                "market": row["market"],
                "line": row["line"],
                "over_price": row["over_price"],
                "under_price": row["under_price"],
            }
        )

    payload = {
        "generated_at_utc": utcnow_iso(),
        "count": len(items),
        "items": items,
    }

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/app.db")
    p.add_argument("--out", default="site/public/odds.json")
    args = p.parse_args()

    payload = export_latest(args.db, args.out)
    print(f"Exported {payload['count']} rows to {args.out}")


if __name__ == "__main__":
    main()
