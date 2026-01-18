from __future__ import annotations

import json
import sqlite3
from pathlib import Path

OUT = Path("site/public/data")
DB = Path("data/app.db")

def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row

    fixtures = [dict(r) for r in con.execute("""
        SELECT * FROM fixtures
        ORDER BY commence_time_utc ASC
        LIMIT 200
    """).fetchall()]

    odds = [dict(r) for r in con.execute("""
        SELECT * FROM odds_snapshots
        ORDER BY captured_at_utc DESC
        LIMIT 500
    """).fetchall()] if table_exists(con, "odds_snapshots") else []

    (OUT / "fixtures.json").write_text(json.dumps(fixtures, indent=2), encoding="utf-8")
    (OUT / "odds_snapshots.json").write_text(json.dumps(odds, indent=2), encoding="utf-8")

    print(f"Wrote {OUT / 'fixtures.json'} ({len(fixtures)} rows)")
    print(f"Wrote {OUT / 'odds_snapshots.json'} ({len(odds)} rows)")

def table_exists(con: sqlite3.Connection, name: str) -> bool:
    r = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return r is not None

if __name__ == "__main__":
    main()
