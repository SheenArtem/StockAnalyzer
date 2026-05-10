"""
archive_risk_score.py — Banner Risk Score 日頻 archiver

從既有 PCR/FGI parquet 讀子訊號 + 計算 m1b/rv10/rv30 → 算當日 risk_score
寫進 data/sentiment/risk_score_history.parquet (dedupe by date)

Why archiver not live compute:
- 6 個子訊號 (FGI/PCR_vol/PCR_oi/M1B/rv10/rv30) 全部日頻收盤後算
- 盤中重算只是用昨日值再壓一次 → 浪費 8.9s cold load
- archiver 跑 1 次 / banner 純讀 (<50ms)；跟 atm_put/mtx/opt_inst 既有 sentiment archiver pattern 一致

Schedule: 接在 run_taifex_signals_afterclose.bat 第 5 stage（PCR/FGI append 之後）
依賴：append_today_pcr_fgi.py 必須先跑（PCR/FGI parquet 寫好）

執行: python tools/archive_risk_score.py
"""
from __future__ import annotations

import logging
import sys
from datetime import date as ddate
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

OUT = REPO / "data" / "sentiment" / "risk_score_history.parquet"
PCR = REPO / "data" / "sentiment" / "pcr_history.parquet"
FGI = REPO / "data" / "sentiment" / "fgi_history.parquet"


def _today_signals() -> dict:
    """組裝當日 6 訊號 dict for banner_risk_score.compute_risk_score."""
    today = {}

    # FGI score (read from archive parquet)
    if FGI.exists():
        try:
            df = pd.read_parquet(FGI)
            score = df['score'].dropna().iloc[-1]
            today['fgi_score'] = float(score)
        except Exception as e:
            logger.warning("FGI from parquet failed: %s", e)

    # PCR volume + OI (both columns in pcr_history)
    if PCR.exists():
        try:
            df = pd.read_parquet(PCR)
            if 'pc_ratio_volume' in df.columns:
                today['pcr_volume'] = float(df['pc_ratio_volume'].dropna().iloc[-1])
            if 'pc_ratio_oi' in df.columns:
                today['pcr_oi'] = float(df['pc_ratio_oi'].dropna().iloc[-1])
        except Exception as e:
            logger.warning("PCR from parquet failed: %s", e)

    # m1b_ratio (live compute — CBC + TWSE)
    try:
        from money_supply import compute_m1b_ratio
        m1b = compute_m1b_ratio()
        if m1b and m1b.get('ratio_pct') is not None:
            today['m1b_ratio'] = float(m1b['ratio_pct'])
    except Exception as e:
        logger.warning("m1b_ratio failed: %s", e)

    # rv10 / rv30 (yfinance ^TWII close)
    try:
        import yfinance as yf
        df = yf.Ticker('^TWII').history(period='3mo')
        if not df.empty and len(df) >= 30:
            close = df['Close']
            log_ret = np.log(close / close.shift(1))
            today['rv10'] = float(log_ret.iloc[-10:].std() * np.sqrt(252))
            today['rv30'] = float(log_ret.iloc[-30:].std() * np.sqrt(252))
    except Exception as e:
        logger.warning("rv compute failed: %s", e)

    return today


def _flatten_for_parquet(result: dict) -> dict:
    """把 compute_risk_score 結果攤平給 parquet 存。

    breakdown / zone_stats 是 nested dict，attach JSON-stringified 給 backtest 用。
    """
    import json
    out = {
        'date': pd.Timestamp(ddate.today()),
        'composite': result.get('composite'),
        'zone': result.get('zone'),
        'zone_color': result.get('zone_color'),
        'total_weight_used': result.get('total_weight_used'),
        'baseline_10pct': result.get('baseline_10pct'),
    }
    # zone_stats 攤平
    zs = result.get('zone_stats') or {}
    for k in ['co10', 'co5', 'mdd_median', 'ann_days']:
        out[f'zone_{k}'] = zs.get(k)
    # breakdown 各子訊號 rank/value
    bd = result.get('breakdown') or {}
    for sig in ['m1b_ratio', 'rv10', 'rv30', 'pcr_volume', 'pcr_oi', 'fgi_score']:
        s = bd.get(sig) or {}
        out[f'{sig}_value'] = s.get('value')
        out[f'{sig}_rank'] = s.get('rank')
    # 完整 breakdown 序列化保存（debug / 重算用）
    out['breakdown_json'] = json.dumps(bd, default=str)
    return out


def main():
    today = _today_signals()
    logger.info("Today signals: %s", list(today.keys()))

    if not today:
        logger.error("ABORT: no signals collected")
        return 1

    import banner_risk_score as brs
    panel_hist = brs.get_panel_history()
    result = brs.compute_risk_score(today, panel_history=panel_hist)

    if result.get('composite') is None:
        logger.error("ABORT: composite is None (signals: %s, weight_used: %s)",
                     list(today.keys()), result.get('total_weight_used'))
        return 1

    row = _flatten_for_parquet(result)
    new_df = pd.DataFrame([row])

    # Append + dedupe by date
    if OUT.exists():
        old = pd.read_parquet(OUT)
        old['date'] = pd.to_datetime(old['date'])
        merged = pd.concat([old, new_df], ignore_index=True)
        merged = merged.drop_duplicates('date', keep='last').sort_values('date').reset_index(drop=True)
    else:
        merged = new_df

    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(OUT, index=False)
    logger.info("Saved -> %s (%d rows total)", OUT, len(merged))
    logger.info("Today: composite=%.1f zone=%s",
                result['composite'], result['zone'])
    return 0


if __name__ == '__main__':
    sys.exit(main())
