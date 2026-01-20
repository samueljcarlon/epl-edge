from __future__ import annotations

import os
from dataclasses import dataclass


def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class Settings:
    football_data_token: str
    odds_api_key: str
    db_path: str

    # The Odds API config
    odds_sport_key: str
    odds_regions: str
    odds_markets: str
    odds_format: str
    date_format: str

    # Optional controls that your collect/export can use if you want
    days_back: int
    days_forward: int


def get_settings() -> Settings:
    fd = _get_env("FOOTBALL_DATA_TOKEN")
    oa = _get_env("ODDS_API_KEY")

    if not fd or fd.startswith("YOUR_"):
        raise RuntimeError("Missing FOOTBALL_DATA_TOKEN")
    if not oa or oa.startswith("YOUR_"):
        raise RuntimeError("Missing ODDS_API_KEY")

    # Defaults are sensible for EPL
    db_path = _get_env("DB_PATH", "data/app.db")

    odds_sport_key = _get_env("ODDS_SPORT_KEY", "soccer_epl")
    odds_regions = _get_env("ODDS_REGIONS", "uk,eu,us,us2")
    odds_markets = _get_env("ODDS_MARKETS", "totals")
    odds_format = _get_env("ODDS_FORMAT", "decimal")
    date_format = _get_env("ODDS_DATE_FORMAT", "iso")

    # Optional, only matters if your collect.py uses them
    try:
        days_back = int(_get_env("DAYS_BACK", "2"))
    except ValueError:
        days_back = 2

    try:
        days_forward = int(_get_env("DAYS_FORWARD", "10"))
    except ValueError:
        days_forward = 10

    return Settings(
        football_data_token=fd,
        odds_api_key=oa,
        db_path=db_path,
        odds_sport_key=odds_sport_key,
        odds_regions=odds_regions,
        odds_markets=odds_markets,
        odds_format=odds_format,
        date_format=date_format,
        days_back=days_back,
        days_forward=days_forward,
    )
