from __future__ import annotations

import numpy as np
from scipy.stats import poisson
from src.db import connect


def main() -> None:
    con = connect("data/app.db")
    rows = con.execute("""
        SELECT home_goals, away_goals
        FROM fixtures
        WHERE home_goals IS NOT NULL
    """).fetchall()

    if not rows:
        print("No finished matches yet")
        return

    totals = [r["home_goals"] + r["away_goals"] for r in rows]
    lam = np.mean(totals)

    p_over_2_5 = 1 - poisson.cdf(2, lam)
    print(f"League implied P(Over 2.5): {p_over_2_5:.3f}")


if __name__ == "__main__":
    main()
