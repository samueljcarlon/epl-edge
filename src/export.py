from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def export_odds_json(db_path: str, out_path: str, limit: int = 5000) -> int:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON;")

    # return dict rows
    con.row_factory = sqlite3.Row

    sql = """
    WITH ranked AS (
      SELECT
        s.captured_at_utc,
        s.fixture_id,
        s.bookmaker,
        s.market,
        s.line,
        s.over_price,
        s.under_price,
        ROW_NUMBER() OVER (
          PARTITION BY s.fixture_id, s.bookmaker, s.market, COALESCE(s.line, -999999.0)
          ORDER BY s.captured_at_utc DESC
        ) AS rn
      FROM odds_snapshots s
    )
    SELECT
      r.captured_at_utc,
      f.fixture_id,
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
    ORDER BY f.commence_time_utc ASC
    LIMIT ?;
    """

    rows = con.execute(sql, (limit,)).fetchall()
    items: List[Dict[str, Any]] = [dict(r) for r in rows]

    payload = {
        "generated_at_utc": utcnow_iso(),
        "count": len(items),
        "items": items,
    }

    outp = Path(out_path)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(payload, ensure_ascii=False, indent=2))

    return len(items)


def main() -> None:
    db_path = os.getenv("DB_PATH", "data/app.db")
    out_path = os.getenv("OUT_PATH", "site/public/odds.json")
    n = export_odds_json(db_path, out_path)
    print(f"Exported {n} rows to {out_path}")


if __name__ == "__main__":
    main()
