from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row  # <-- CRITICAL FIX
    return con


def export_odds_json(db_path: str, out_path: str, limit: int | None) -> int:
    con = connect(db_path)
    cur = con.cursor()

    sql = """
    SELECT
      f.fixture_id,
      f.commence_time_utc,
      f.home_team,
      f.away_team,
      o.market,
      o.line,
      o.bookmaker,
      o.over_price,
      o.under_price,
      o.captured_at_utc
    FROM odds_snapshots o
    JOIN fixtures f ON f.fixture_id = o.fixture_id
    ORDER BY o.captured_at_utc DESC
    """

    if limit:
        sql += " LIMIT ?"
        rows = cur.execute(sql, (limit,)).fetchall()
    else:
        rows = cur.execute(sql).fetchall()

    # Build structured output
    out: dict[str, Any] = {"fixtures": []}
    by_fixture: dict[str, dict[str, Any]] = {}

    for r in rows:
        fid = r["fixture_id"]

        if fid not in by_fixture:
            by_fixture[fid] = {
                "fixture_id": fid,
                "commence_time_utc": r["commence_time_utc"],
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "markets": {},
            }

        mk = r["market"]
        market_bucket = by_fixture[fid]["markets"].setdefault(mk, [])

        market_bucket.append(
            {
                "bookmaker": r["bookmaker"],
                "line": r["line"],
                "over_price": r["over_price"],
                "under_price": r["under_price"],
                "captured_at_utc": r["captured_at_utc"],
            }
        )

    out["fixtures"] = list(by_fixture.values())

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    con.close()
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--out-path", required=True)
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    n = export_odds_json(
        db_path=args.db_path,
        out_path=args.out_path,
        limit=args.limit,
    )

    print(f"Exported {n} odds rows to {args.out_path}")


if __name__ == "__main__":
    main()
