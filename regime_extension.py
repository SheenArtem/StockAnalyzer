"""
regime_extension.py - Banner regime extension signal.

Per System 2 Phase 2.5 verdict (commit 20fe53a):
- D policy (ma_dist_60 rolling 252d rank >= 0.95 -> cash) gave Sharpe 0.729 /
  CAGR 10.6% / MDD -54% on 11yr backtest -- best Sharpe of 6 portfolio policies.
- Adds a leading regime extension signal that complements the existing
  coincident composite (B+E v3) and HMM regime row.

Signal:
  rank = rolling-252d rank-pct of (-ma_dist_60), where ma_dist_60 = (close - MA60) / MA60
  Higher rank = more dangerous (close has been further below MA60 historically)

Levels (calibrated 2026-05-09 on TAIEX 1999-2026, N=6404 trading days):
  Green  (0.00-0.65) : 62.2% of days, P(60d MDD<=-10%) =18.1% (baseline 23.5%, lift 0.77x)
  Yellow (0.65-0.85) : 19.5% of days, 29.2% (lift 1.24x)
  Orange (0.85-0.95) : 10.1% of days, 35.3% (lift 1.50x)
  Red    (>=0.95)    :  8.2% of days, 35.6% (lift 1.51x)

SOP-14 informational tier:
  - Do NOT auto-rebalance based on this signal alone.
  - Display "historical co-occurrence rate" not "predicted probability".
  - Pair with composite + HMM regime + System 2 alert for triangulation.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
TAIEX_PATH = REPO / "data_cache" / "TAIEX_price.parquet"

# Calibrated 2026-05-09 on TAIEX 1999-2026 (N=6404)
LEVEL_STATS = {
    "green":  {"co10": 18.1, "co5": 37.4, "mdd_median": -3.41, "ann_pct_days": 62.2,
               "lift10": 0.77},
    "yellow": {"co10": 29.2, "co5": 46.8, "mdd_median": -4.30, "ann_pct_days": 19.5,
               "lift10": 1.24},
    "orange": {"co10": 35.3, "co5": 62.0, "mdd_median": -7.54, "ann_pct_days": 10.1,
               "lift10": 1.50},
    "red":    {"co10": 35.6, "co5": 58.0, "mdd_median": -6.30, "ann_pct_days":  8.2,
               "lift10": 1.51},
}
BASELINE_10PCT = 23.5  # baseline % all days P(60d MDD <= -10%)
BASELINE_5PCT = 43.4

# Lead recall stats from Phase 3.4 audit (70 events 1999-2026)
LEAD_RECALL = {
    "yellow": "59% (41/70)",
    "orange": "~48%",
    "red":    "36% (25/70)",
}

LEVEL_COLORS = {
    "green":  "#00CC44",
    "yellow": "#FFD700",
    "orange": "#FF8800",
    "red":    "#FF4444",
    "unknown": "#888888",
}


def _classify(rank: float) -> str:
    if rank >= 0.95:
        return "red"
    if rank >= 0.85:
        return "orange"
    if rank >= 0.65:
        return "yellow"
    return "green"


def compute_extension_signal(taiex_close: Optional[pd.Series] = None) -> dict:
    """Compute today's regime extension signal.

    Parameters
    ----------
    taiex_close : pd.Series, optional
        TAIEX daily close indexed by date. If None, loads from
        data_cache/TAIEX_price.parquet.

    Returns
    -------
    dict with keys:
        rank          : float in [0, 1] (None if insufficient history)
        ma_dist_60    : float, today's (close - MA60) / MA60
        level         : 'green' | 'yellow' | 'orange' | 'red' | 'unknown'
        color         : hex
        data_date     : pd.Timestamp
        days_above_85 : int, # of trailing 60d with rank >= 0.85
        stats         : LEVEL_STATS[level]
    """
    if taiex_close is None:
        try:
            df = pd.read_parquet(TAIEX_PATH)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").set_index("date")
            taiex_close = df["close"].astype(float)
        except Exception as e:
            logger.warning("regime_extension: TAIEX load failed: %s", e)
            return {"rank": None, "level": "unknown", "color": LEVEL_COLORS["unknown"]}

    if len(taiex_close) < 252 + 60:
        return {"rank": None, "level": "unknown", "color": LEVEL_COLORS["unknown"]}

    ma60 = taiex_close.rolling(60).mean()
    ma_dist = (taiex_close - ma60) / ma60
    danger = -ma_dist
    rank_series = danger.rolling(252).rank(pct=True)

    today_rank = rank_series.iloc[-1]
    today_ma_dist = ma_dist.iloc[-1]
    if pd.isna(today_rank):
        return {"rank": None, "level": "unknown", "color": LEVEL_COLORS["unknown"]}

    level = _classify(float(today_rank))
    days_above_85 = int((rank_series.iloc[-60:] >= 0.85).sum())

    return {
        "rank": float(today_rank),
        "ma_dist_60": float(today_ma_dist),
        "level": level,
        "color": LEVEL_COLORS[level],
        "data_date": taiex_close.index[-1],
        "days_above_85": days_above_85,
        "stats": LEVEL_STATS[level],
        "baseline_10pct": BASELINE_10PCT,
    }


if __name__ == "__main__":
    import json
    out = compute_extension_signal()
    out_clean = {k: (str(v) if isinstance(v, pd.Timestamp) else v) for k, v in out.items()}
    print(json.dumps(out_clean, indent=2, ensure_ascii=False, default=str))
