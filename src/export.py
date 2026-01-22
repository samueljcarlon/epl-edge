from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from src.db import connect, init_db


def export_odds_json(db_path: str, out_path: str, limit: int = 5000) -> int:
    con = connect(db_path)
    init_db(con)

    con.row_factory = lambda cursor, row: {
        col[0]: row[idx] for idx, col in enumerate(cursor.description)
    }
    cur = con.cursor()

    rows = cur.execute(
        """
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
          o.under_price
        FROM odds_snapshots o
        JOIN fixtures f USING (fixture_id)
        ORDER BY
          f.commence_time_utc ASC,
          o.market ASC,
          COALESCE(o.line, -9999) ASC,
          o.bookmaker ASC,
          o.captured_at_utc DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(rows),
        "items": rows,
    }

    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/app.db")
    p.add_argument("--out", default="site/public/odds.json")
    p.add_argument("--limit", type=int, default=5000)
    args = p.parse_args()

    n = export_odds_json(args.db, args.out, args.limit)
    print(f"Exported {n} rows to {args.out}")


if __name__ == "__main__":
    main()
