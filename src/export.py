from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.db import connect, init_db


def export_odds_json(db_path: str, out_path: str, limit: int = 5000) -> int:
    con = connect(db_path)
    init_db(con)

    # Make rows come back as dicts
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
          matchweek,
          status,
          home_team,
          away_team,
          home_goals,
          away_goals,
          bookmaker,
          market,
          CASE WHEN market = 'btts' THEN NULL ELSE line END AS line,
          over_price,
          under_price
        FROM odds_view
        ORDER BY commence_time_utc ASC, fixture_id ASC, bookmaker ASC, market ASC, line ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "items": rows,
    }

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return len(rows)


def main() -> None:
    # Keep paths simple and repo-relative
    db_path = "data/app.db"
    out_path = "site/public/odds.json"
    n = export_odds_json(db_path=db_path, out_path=out_path, limit=20000)
    print(f"Exported {n} rows to {out_path}")


if __name__ == "__main__":
    main()
