"""
sync_backtest_to_live_cache.py
===============================
把 data_cache/backtest/financials_revenue.parquet 的新鮮資料同步到
data_cache/fundamental_cache/month_revenue_*.parquet (live scanner 用)。

用途: VF-VC P3-a 後發現 207 檔 (1101/1102/...) 的 live cache 還停在 2015，
導致 live scanner 嘗試 refresh 觸發 MOPS。同步後可跳過 MOPS。
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sync")


def main():
    bt_path = ROOT / "data_cache" / "backtest" / "financials_revenue.parquet"
    live_dir = ROOT / "data_cache" / "fundamental_cache"

    logger.info("Loading backtest parquet...")
    bt = pd.read_parquet(bt_path)
    bt['date'] = pd.to_datetime(bt['date'])
    logger.info("  %d rows, %d stocks", len(bt), bt['stock_id'].nunique())

    synced = 0
    created = 0
    skipped = 0

    for sid, g in bt.groupby('stock_id'):
        live_path = live_dir / f"month_revenue_{sid}.parquet"
        bt_latest = g['date'].max()

        if live_path.exists():
            live = pd.read_parquet(live_path)
            live['date'] = pd.to_datetime(live['date'])
            live_latest = live['date'].max()
            if live_latest >= bt_latest:
                skipped += 1
                continue
            # Backtest has fresher data - overwrite
            g.to_parquet(live_path)
            synced += 1
        else:
            g.to_parquet(live_path)
            created += 1

    logger.info("Synced: %d updated, %d created, %d skipped (live already fresh)",
                synced, created, skipped)


if __name__ == "__main__":
    main()
