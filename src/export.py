from __future__ import annotations

import json
import os
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
            captured_at_utc,
            fixture_id,
            commence_time_utc,
            status,
            home_team,
            away_team,
            bookmaker,
            market,
            line,
            over_price,
            under_price
        FROM odds
        ORDER BY captured_at_utc DESC
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
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return len(rows)


def main() -> None:
    db_path = os.environ.get("DB_PATH", "data/app.db")
    out_path = os.environ.get("OUT_JSON_PATH", "site/public/odds.json")

    try:
        limit = int(os.environ.get("EXPORT_LIMIT", "5000"))
    except ValueError:
        limit = 5000

    n = export_odds_json(
        db_path=db_path,
        out_path=out_path,
        limit=limit,
    )

    print(f"[export] Exported {n} rows to {out_path}")


if __name__ == "__main__":
    main()
