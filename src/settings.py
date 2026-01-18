from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    football_data_token: str
    odds_api_key: str
    db_path: str = "data/app.db"

    odds_sport_key: str = "soccer_epl"
    odds_regions: str = "uk,eu"
    odds_markets: str = "totals"
    odds_format: str = "decimal"
    date_format: str = "iso"


def get_settings() -> Settings:
    fd = os.getenv("FOOTBALL_DATA_TOKEN", "").strip()
    oa = os.getenv("ODDS_API_KEY", "").strip()
    if not fd or fd.startswith("YOUR_"):
        raise RuntimeError("Missing FOOTBALL_DATA_TOKEN")
    if not oa or oa.startswith("YOUR_"):
        raise RuntimeError("Missing ODDS_API_KEY")
    return Settings(football_data_token=fd, odds_api_key=oa)
