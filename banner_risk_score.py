"""
banner_risk_score.py -- Banner 綜合風險指標

基於 v3 calibration（reports/banner_risk_score_calibration_v2.md）：
- 6 訊號 weighted composite，2002-2026 N=5906 校準
- m1b/rv10/rv30/pcr_volume/pcr_oi/fgi_score 各按 lift10 反推 weight
- Orange ≥ 70.7 (P85) / Yellow 55.2-70.7 (P65-85) / Green < 55.2
- Lift10 = 2.042（過 SOP-12: composite > best-single 1.843）

⚠️ SOP-14 informational tier:
- 不接 portfolio rebalance / 不發紅燈 / 文案禁「預警/預測」
- 顯示「同期重合率」非「預測機率」
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
PCR_HISTORY = REPO / "data" / "sentiment" / "pcr_history.parquet"
FGI_HISTORY = REPO / "data" / "sentiment" / "fgi_history.parquet"

# v3 calibration constants (2026-05-08, panel 2002-2026 N=5906)
WEIGHTS = {
    "m1b_ratio":  0.192,   # lift10 = 1.922
    "rv10":       0.172,   # lift10 = 1.720
    "rv30":       0.169,   # lift10 = 1.688
    "pcr_volume": 0.167,   # lift10 = 1.670
    "pcr_oi":     0.152,   # lift10 = 1.514
    "fgi_score":  0.148,   # lift10 = 1.479
}

# Direction: True = high value is danger (rank as-is); False = low value is danger (reverse)
DIRECTION_HIGH_DANGER = {
    "m1b_ratio":  True,
    "rv10":       True,
    "rv30":       True,
    "pcr_volume": True,
    "pcr_oi":     True,
    "fgi_score":  False,
}

# Reverse-engineered thresholds from v3 calibration
ORANGE_THRESH = 70.7
YELLOW_THRESH = 55.2

# Co-occurrence rates from v3 (for UI tooltip)
ZONE_STATS = {
    "orange": {"co10": 53.3, "co5": 62.3, "mdd_median": -11.2, "ann_days": 37},
    "yellow": {"co10": 33.3, "co5": 55.7, "mdd_median": -5.6,  "ann_days": 49},
    "green":  {"co10": 17.6, "co5": 45.4, "mdd_median": -4.2,  "ann_days": 159},
}
BASELINE_10PCT = 19.2  # P(MDD ≥ 10%) baseline 2002-2026


def _percentile_rank(value: float, history: pd.Series, high_is_danger: bool) -> Optional[float]:
    """
    Compute today's percentile rank vs historical distribution.

    Returns rank in [0, 100] where higher = more dangerous.
    For high_is_danger=True: rank = % of history values ≤ today
    For high_is_danger=False: rank = 100 - (% of history values ≤ today)
    """
    if value is None or pd.isna(value):
        return None
    hist = history.dropna()
    if len(hist) < 100:
        logger.warning("Insufficient history (%d rows < 100) for percentile rank", len(hist))
        return None
    rank_pct = float((hist <= value).mean()) * 100
    if not high_is_danger:
        rank_pct = 100 - rank_pct
    return rank_pct


def _load_history():
    """Load PCR + FGI history parquets. m1b/rv10/rv30 history must come from caller."""
    pcr = pd.read_parquet(PCR_HISTORY) if PCR_HISTORY.exists() else None
    fgi = pd.read_parquet(FGI_HISTORY) if FGI_HISTORY.exists() else None
    return pcr, fgi


def compute_risk_score(today_signals: dict, panel_history: Optional[dict] = None) -> dict:
    """
    Parameters
    ----------
    today_signals : dict with keys subset of {m1b_ratio, rv10, rv30, pcr_volume, pcr_oi, fgi_score}
        Today's value for each signal. Missing values returned as None breakdown,
        weight redistributed to available signals.
    panel_history : dict, optional
        Pre-loaded {sig_name: pd.Series} for m1b/rv10/rv30 from crash_predictor panel.
        If None, only PCR/FGI computed (m1b/rv10/rv30 contribute None).

    Returns
    -------
    dict with keys:
        composite : float (0-100) or None if all signals missing
        zone : 'green' | 'yellow' | 'orange' | 'unknown'
        zone_color : hex color
        breakdown : {sig: {value, rank, weight, contribution}}
        baseline_10pct : reference number for UI
        zone_stats : {co10, co5, mdd_median, ann_days} for current zone
    """
    pcr_hist, fgi_hist = _load_history()
    panel_history = panel_history or {}

    breakdown = {}
    weighted_sum = 0.0
    total_weight_used = 0.0

    for sig, weight in WEIGHTS.items():
        value = today_signals.get(sig)
        # Pick history series for this signal
        if sig == "pcr_volume" and pcr_hist is not None:
            hist = pcr_hist["pc_ratio_volume"]
        elif sig == "pcr_oi" and pcr_hist is not None:
            hist = pcr_hist["pc_ratio_oi"]
        elif sig == "fgi_score" and fgi_hist is not None:
            hist = fgi_hist["score"]
        elif sig in panel_history:
            hist = panel_history[sig]
        else:
            breakdown[sig] = {"value": value, "rank": None, "weight": weight,
                              "contribution": None, "missing_reason": "no history"}
            continue

        rank = _percentile_rank(value, hist, DIRECTION_HIGH_DANGER[sig])
        if rank is None:
            breakdown[sig] = {"value": value, "rank": None, "weight": weight,
                              "contribution": None, "missing_reason": "no value"}
            continue

        contribution = rank * weight
        breakdown[sig] = {"value": value, "rank": rank, "weight": weight,
                          "contribution": contribution, "missing_reason": None}
        weighted_sum += contribution
        total_weight_used += weight

    # Normalize composite if some signals missing (re-weight to sum=1)
    if total_weight_used >= 0.5:  # at least half weight present
        composite = weighted_sum / total_weight_used  # rescale to [0,100] equivalent
    else:
        composite = None

    if composite is None:
        zone = "unknown"
        zone_color = "#888888"
    elif composite >= ORANGE_THRESH:
        zone = "orange"
        zone_color = "#FF8800"
    elif composite >= YELLOW_THRESH:
        zone = "yellow"
        zone_color = "#FFD700"
    else:
        zone = "green"
        zone_color = "#00CC44"

    return {
        "composite": composite,
        "zone": zone,
        "zone_color": zone_color,
        "breakdown": breakdown,
        "total_weight_used": total_weight_used,
        "baseline_10pct": BASELINE_10PCT,
        "zone_stats": ZONE_STATS.get(zone, {}),
    }


# -------- helpers for banner integration --------

def get_panel_history(crash_panel_path: Optional[Path] = None) -> dict:
    """
    Load m1b/rv10/rv30 history from crash_predictor_tw_panel.parquet.

    Returns {m1b_ratio: Series, rv10: Series, rv30: Series}
    If panel missing, returns empty dict (m1b/rv contributions skipped).
    """
    if crash_panel_path is None:
        crash_panel_path = (REPO / "reports" / "_history" /
                            "2026_05_crash_predictor_closed" /
                            "crash_predictor_tw_panel.parquet")
    if not crash_panel_path.exists():
        logger.warning("crash predictor panel not found: %s", crash_panel_path)
        return {}
    panel = pd.read_parquet(crash_panel_path)
    return {
        "m1b_ratio": panel["m1b_ratio_pct"].dropna(),
        "rv10": panel["rv10"].dropna(),
        "rv30": panel["rv30"].dropna(),
    }


def get_today_signals_from_banner(banner_data: dict) -> dict:
    """
    Extract today's signal values from banner _get_banner_data() output.

    banner_data structure (see market_banner.py):
        {
            'tw': {price, ma20_bias, ma60_bias, k, d, ...},
            'tw_fgi': {score, label, components: {...}, ...},
            'pcr': {pc_ratio, call_oi, put_oi, ...},
            'm1b_ratio': {ratio_pct, ...},
            ...
        }

    Returns subset of {m1b_ratio, rv10, rv30, pcr_volume, pcr_oi, fgi_score}.
    rv10/rv30 require fetching ^TWII close history (banner does not provide).
    """
    out = {}

    # FGI score
    fgi = banner_data.get("tw_fgi") or {}
    fgi_score = fgi.get("score")
    if fgi_score is not None:
        out["fgi_score"] = float(fgi_score)

    # PCR (banner returns OI-based; volume needs separate fetch or from history parquet)
    pcr = banner_data.get("pcr") or {}
    pc_ratio_oi = pcr.get("pc_ratio")
    if pc_ratio_oi is not None and pc_ratio_oi > 0:
        out["pcr_oi"] = float(pc_ratio_oi)

    # PCR volume: read from latest pcr_history.parquet (archiver writes)
    if PCR_HISTORY.exists():
        try:
            df = pd.read_parquet(PCR_HISTORY)
            if not df.empty and "pc_ratio_volume" in df.columns:
                latest = df["pc_ratio_volume"].dropna().iloc[-1]
                out["pcr_volume"] = float(latest)
        except Exception as e:
            logger.debug("PCR volume fetch failed: %s", e)

    # m1b
    m1b = banner_data.get("m1b_ratio") or {}
    m1b_pct = m1b.get("ratio_pct")
    if m1b_pct is not None:
        out["m1b_ratio"] = float(m1b_pct)

    # rv10/rv30: compute from ^TWII close
    try:
        import yfinance as yf
        df = yf.Ticker("^TWII").history(period="3mo")
        if not df.empty and len(df) >= 30:
            close = df["Close"]
            log_ret = np.log(close / close.shift(1))
            out["rv10"] = float(log_ret.iloc[-10:].std() * np.sqrt(252))
            out["rv30"] = float(log_ret.iloc[-30:].std() * np.sqrt(252))
    except Exception as e:
        logger.debug("^TWII rv compute failed: %s", e)

    return out
