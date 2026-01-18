from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Tuple


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def devig_two_way(decimal_over: float, decimal_under: float) -> Optional[Tuple[float, float]]:
    q_over = 1.0 / decimal_over
    q_under = 1.0 / decimal_under
    r = q_over + q_under
    return q_over / r, q_under / r
