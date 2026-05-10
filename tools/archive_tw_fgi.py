"""
archive_tw_fgi.py -- Banner Taiwan FGI 日頻 archiver

呼叫 TaiwanFearGreedIndex().calculate() 一次，序列化 components dict
寫進 data/sentiment/tw_fgi_history.parquet (dedupe by date)

Why archiver not live compute:
- 5 子分數 (momentum/breadth/PCR/vol/margin) 全部日頻收盤後算
  (margin 20:00 才更新)，盤中重算只是用昨日值再壓一次無意義
- live compute 要打 TAIFEX/TWSE 5 次（含 build_fgi_history ~3s）
  → cold load 4.7s 主導 banner 整體載入時間
- archiver 跑 1 次 / banner 純讀 (<50ms)；跟 atm_put/mtx/opt_inst 既有
  sentiment archiver pattern 一致

Schedule: run_taifex_signals_afterclose.bat 第 5 stage 後（PCR/FGI append 之後，
risk_score archive 同層級）。

執行: python tools/archive_tw_fgi.py
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import date as ddate
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

OUT = REPO / "data" / "sentiment" / "tw_fgi_history.parquet"


def _to_jsonable(obj):
    """遞迴把 dict 內 numpy / pandas / date scalar 轉成 JSON-friendly type."""
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(x) for x in obj]
    if hasattr(obj, 'item'):  # numpy scalar
        try:
            return obj.item()
        except Exception:
            pass
    if isinstance(obj, (pd.Timestamp, ddate)):
        return str(obj)
    return obj


def main():
    from taifex_data import TaiwanFearGreedIndex

    result = TaiwanFearGreedIndex().calculate()
    if not result or result.get('score') is None:
        logger.error("ABORT: calculate() returned no score: %s", result)
        return 1

    components = result.get('components') or {}
    data_date = result.get('data_date')
    if isinstance(data_date, ddate):
        data_date_str = data_date.isoformat()
    elif data_date is None:
        data_date_str = ddate.today().isoformat()
    else:
        data_date_str = str(data_date)

    row = {
        'date': pd.Timestamp(ddate.today()),
        'data_date': data_date_str,
        'score': float(result['score']),
        'label': result.get('label', ''),
        'components_json': json.dumps(_to_jsonable(components), default=str),
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
    logger.info("Today: score=%.1f label=%s data_date=%s",
                row['score'], row['label'], data_date_str)
    return 0


if __name__ == '__main__':
    sys.exit(main())
