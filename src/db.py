from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS fixtures (
  fixture_id TEXT PRIMARY KEY,
  commence_time_utc TEXT NOT NULL,
  matchweek INTEGER,
  status TEXT NOT NULL,
  home_team TEXT NOT NULL,
  away_team TEXT NOT NULL,
  home_goals INTEGER,
  away_goals INTEGER,
  last_updated_utc TEXT
);

CREATE TABLE IF NOT EXISTS odds_snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  captured_at_utc TEXT NOT NULL,
  fixture_id TEXT NOT NULL,
  bookmaker TEXT NOT NULL,
  market TEXT NOT NULL,
  line REAL NOT NULL,
  over_price REAL,
  under_price REAL,
  FOREIGN KEY (fixture_id) REFERENCES fixtures(fixture_id)
);

CREATE INDEX IF NOT EXISTS idx_odds_fixture_time
ON odds_snapshots(fixture_id, captured_at_utc);
"""


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA_SQL)
    con.commit()


def executemany(con: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    con.executemany(sql, list(rows))
    con.commit()
