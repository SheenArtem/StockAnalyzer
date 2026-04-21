"""
vfvc_backfill_monthly_rev.py
============================
VF-VC P3-a: 補回 207 檔在 snapshot 有但在 financials_revenue.parquet 幾乎無資料的股票
(1101/1102/1303/... 水泥大型股)。目標：把 monthly revenue 回填完整供 VF-VC 1m YoY 使用。

只拉 monthly revenue (不動其他 4 類基本面)。
輸出：直接覆寫 financials_revenue.parquet 的對應 stock_id 區段。

用法: python tools/vfvc_backfill_monthly_rev.py --universe data_cache/vfvc_missing_monthly_rev.txt
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("vfvc_bf")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="data_cache/vfvc_missing_monthly_rev.txt")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default="2026-04-30")
    args = ap.parse_args()

    stocks = [l.strip() for l in open(args.universe) if l.strip()]
    logger.info("Universe: %d stocks", len(stocks))

    from cache_manager import get_finmind_loader
    dl = get_finmind_loader()
    logger.info("FinMind loaded, has_token=%s", dl.has_token)

    rev_path = ROOT / "data_cache" / "backtest" / "financials_revenue.parquet"
    existing = pd.read_parquet(rev_path) if rev_path.exists() else pd.DataFrame()
    logger.info("Existing parquet: %d rows, %d stocks",
                len(existing), existing['stock_id'].nunique() if not existing.empty else 0)

    all_new = []
    t0 = time.time()
    call_count = 0
    hour_start = time.time()
    fail_stocks = []

    for i, sid in enumerate(stocks):
        # Rate-limit: 600/hr = 1 req / 6 sec, leave buffer
        if call_count >= 580:
            wait = 3600 - (time.time() - hour_start) + 10
            if wait > 0:
                logger.warning("Rate limit, sleep %.0fs...", wait)
                time.sleep(wait)
            call_count = 0
            hour_start = time.time()

        try:
            raw = dl.taiwan_stock_month_revenue(
                stock_id=sid, start_date=args.start, end_date=args.end,
            )
            call_count += 1
            time.sleep(1.2)  # Throttle to ~1/sec

            if raw is None or raw.empty:
                fail_stocks.append(sid)
                logger.warning("[%s] FinMind empty", sid)
                continue

            # Match schema of financials_revenue.parquet
            raw = raw.copy()
            if 'revenue' not in raw.columns:
                fail_stocks.append(sid)
                continue
            raw['revenue'] = pd.to_numeric(raw['revenue'], errors='coerce')
            raw = raw.dropna(subset=['revenue'])
            if raw.empty:
                fail_stocks.append(sid)
                continue

            # Ensure column names match parquet schema
            keep_cols = ['date', 'stock_id', 'country', 'revenue',
                         'revenue_month', 'revenue_year', 'revenue_last_year',
                         'revenue_year_growth', 'revenue_last_month',
                         'revenue_month_growth']
            present = [c for c in keep_cols if c in raw.columns]
            all_new.append(raw[present])

        except Exception as e:
            fail_stocks.append(sid)
            logger.warning("[%s] error: %s", sid, e)

        if (i + 1) % 25 == 0:
            logger.info("[%d/%d] %.1fmin elapsed", i + 1, len(stocks),
                        (time.time() - t0) / 60)

    if all_new:
        new_df = pd.concat(all_new, ignore_index=True)
        logger.info("Fetched %d rows for %d stocks",
                    len(new_df), new_df['stock_id'].nunique())

        # Merge with existing: remove old rows for these stocks, append new
        if not existing.empty:
            existing = existing[~existing['stock_id'].isin(new_df['stock_id'])]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        combined = combined.sort_values(['stock_id', 'date']).reset_index(drop=True)

        # Backup before overwrite
        bk = rev_path.with_suffix('.parquet.bak_vfvc')
        if rev_path.exists() and not bk.exists():
            import shutil
            shutil.copy2(rev_path, bk)
            logger.info("Backup: %s", bk)

        combined.to_parquet(rev_path)
        logger.info("Saved: %s (%d rows, %d stocks)",
                    rev_path, len(combined), combined['stock_id'].nunique())

    if fail_stocks:
        logger.warning("Failed (%d): %s", len(fail_stocks), fail_stocks[:20])


if __name__ == "__main__":
    main()
