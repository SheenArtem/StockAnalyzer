"""
archive_m1b_ratio.py -- Banner 成交量/M1B 比 日頻 archiver

呼叫 compute_m1b_ratio() 一次，flat 寫進 data/sentiment/m1b_ratio_history.parquet
(dedupe by date)

Why archiver not live compute:
- M1B 央行月底發布，FMTQIK 收盤後固定，盤中重算意義為 0
- live compute 要打 CBC EF15M01 + TWSE FMTQIK 兩月 → cold load 3.1s
- archiver 跑 1 次 / banner 純讀 (<50ms)

Schedule: run_taifex_signals_afterclose.bat 第 5 stage 後
(跟 archive_tw_fgi / archive_risk_score 同層級)。
"""
from __future__ import annotations

import logging
import sys
from datetime import date as ddate
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

OUT = REPO / "data" / "sentiment" / "m1b_ratio_history.parquet"


def main():
    from money_supply import compute_m1b_ratio

    result = compute_m1b_ratio()
    if not result or result.get('ratio_pct') is None:
        logger.error("ABORT: compute_m1b_ratio returned no ratio: %s", result)
        return 1

    end_date = result.get('end_date') or ddate.today()
    if isinstance(end_date, ddate):
        data_date_str = end_date.isoformat()
    else:
        data_date_str = str(end_date)

    row = {
        'date': pd.Timestamp(ddate.today()),
        'data_date': data_date_str,
        'ratio_pct': float(result['ratio_pct']),
        'm1b_period': str(result.get('m1b_period', '')),
        'm1b_mil_twd': float(result.get('m1b_mil_twd') or 0.0),
        'trading_value_twd': float(result.get('trading_value_twd') or 0.0),
        'n_days': int(result.get('n_days') or 0),
        'label': str(result.get('label', '')),
        'color': str(result.get('color', '')),
    }
    new_df = pd.DataFrame([row])

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
    logger.info("Today: ratio=%.2f%% label=%s m1b_period=%s",
                row['ratio_pct'], row['label'], row['m1b_period'])
    return 0


if __name__ == '__main__':
    sys.exit(main())
