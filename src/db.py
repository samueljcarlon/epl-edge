from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Mapping, Any


def connect(db_path: str) -> sqlite3.Connection:
    # Ensure folder exists so SQLite doesn't create a new empty DB somewhere dumb
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(p))
    con.execute("PRAGMA foreign_keys = ON;")
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at_utc TEXT NOT NULL,
            fixture_id TEXT,
            commence_time_utc TEXT,
            status TEXT,
            home_team TEXT,
            away_team TEXT,
            bookmaker TEXT,
            market TEXT,
            line REAL,
            over_price REAL,
            under_price REAL
        );

        CREATE INDEX IF NOT EXISTS idx_odds_fixture_market_time
            ON odds (fixture_id, market, captured_at_utc);
        """
    )
    con.commit()


def executemany(
    con: sqlite3.Connection,
    sql: str,
    rows: Iterable[Mapping[str, Any]],
) -> None:
    con.executemany(sql, list(rows))
    con.commit()
