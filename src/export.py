from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from src.db import connect, init_db
from src.settings import get_settings


def export_odds_json(db_path: str, out_path: str, limit: int = 20000) -> int:
    con = connect(db_path)
    init_db(con)

    # return rows as dicts
    con.row_factory = lambda cursor, row: {
        col[0]: row[idx] for idx, col in enumerate(cursor.description)
    }
    cur = con.cursor()

    # Do NOT use odds_view, export directly from tables that definitely exist
    rows = cur.execute(
        """
        SELECT
          os.captured_at_utc,
          os.fixture_id,
          f.commence_time_utc,
          f.matchweek,
          f.status,
          f.home_team,
          f.away_team,
          f.home_goals,
          f.away_goals,
          os.bookmaker,
          os.market,
          os.line,
          os.over_price,
          os.under_price
        FROM odds_snapshots os
        JOIN fixtures f
          ON f.fixture_id = os.fixture_id
        ORDER BY
          os.captured_at_utc DESC,
          f.commence_time_utc ASC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(rows),
        "items": rows,
    }

    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="site/public/odds.json")
    p.add_argument("--limit", type=int, default=20000)
    args = p.parse_args()

    s = get_settings()

    n = export_odds_json(db_path=s.db_path, out_path=args.out, limit=args.limit)
    print(f"Exported {n} rows to {args.out}")


if __name__ == "__main__":
    main()
