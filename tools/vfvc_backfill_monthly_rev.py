"""
vfvc_backfill_monthly_rev.py
============================
VF-VC P3-a: 補回在 snapshot 有但 live cache 幾乎無資料的股票 monthly revenue
(1101/1102/1303/... 水泥大型股)。目標：把 monthly revenue 回填完整供 VF-VC 1m YoY 使用。

2026-04-21 RF-1 重構：
---------------------
原版只寫 data_cache/backtest/financials_revenue.parquet，導致 live scanner
讀 data_cache/fundamental_cache/month_revenue_*.parquet 時看不到新資料 →
觸發 MOPS 重抓 → WAF ban（VF-VC 事件）。

新版：
  1. per-stock 寫入 data_cache/fundamental_cache/month_revenue_{sid}.parquet
     （這是 cache_manager.get_cached_fundamentals() 的 live 路徑）
  2. 全部 backfill 完後自動呼叫 aggregate_fundamental_cache.py --category revenue
     聚合到 data_cache/backtest/financials_revenue.parquet（simulator 用）

規則：任何 backfill tool 只能寫 fundamental_cache/，絕對禁止直接寫 backtest/。
詳見 feedback_unified_cache.md。

用法: python tools/vfvc_backfill_monthly_rev.py --universe data_cache/vfvc_missing_monthly_rev.txt
"""
from __future__ import annotations

import argparse
import logging
import subprocess
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

LIVE_CACHE_DIR = ROOT / "data_cache" / "fundamental_cache"


def run_bulk_update():
    """Cache 三層 Layer 2: 用 mopsfin bulk CSV 一次更新全市場最新月營收。

    特性:
    - 2 個 HTTP request (上市 + 上櫃, ~1954 stocks)
    - 僅補最新月 (公告月後 10-15 日更新可用), 歷史 backfill 仍走 FinMind
    - 不消耗 FinMind 600 req/hr, 不打 MOPS 個股 API (避 WAF)
    - merge_into_existing_cache 按期數比對, 已有不覆寫
    """
    from mops_bulk_fetcher import fetch_bulk_monthly_revenue, merge_into_existing_cache

    logger.info("=== BULK UPDATE mode (Cache Layer 2) ===")
    df = fetch_bulk_monthly_revenue(include_otc=True)
    if df.empty:
        logger.error("Bulk fetch returned empty, abort.")
        return False
    logger.info("Bulk fetched: %d rows / %d unique stocks", len(df), df['stock_id'].nunique())
    logger.info("Date range: %s ~ %s", df['date'].min(), df['date'].max())

    stats = merge_into_existing_cache(df, dry_run=False)
    logger.info("Merge stats: %s", stats)

    # 跑 aggregate 同步 backtest/financials_revenue.parquet
    logger.info("Running aggregate_fundamental_cache.py --category revenue ...")
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "aggregate_fundamental_cache.py"),
         "--category", "revenue"],
        cwd=str(ROOT),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.error("Aggregate FAILED (rc=%d):\n%s", result.returncode, result.stderr)
        return False
    logger.info("Aggregate OK")
    for line in result.stdout.splitlines()[-6:]:
        logger.info("  %s", line)
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="data_cache/vfvc_missing_monthly_rev.txt")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default="2026-04-30")
    ap.add_argument("--skip-aggregate", action="store_true",
                    help="不自動跑 aggregate（僅測試用）")
    ap.add_argument("--bulk-update", action="store_true",
                    help="走 Cache Layer 2 全市場 bulk 更新最新月（Cache 三層架構）；"
                         "不需 universe 檔，跳過 per-stock FinMind 路徑")
    args = ap.parse_args()

    if args.bulk_update:
        ok = run_bulk_update()
        sys.exit(0 if ok else 1)

    stocks = [l.strip() for l in open(args.universe) if l.strip()]
    logger.info("Universe: %d stocks", len(stocks))

    from cache_manager import get_finmind_loader
    dl = get_finmind_loader()
    logger.info("FinMind loaded, has_token=%s", dl.has_token)

    LIVE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    call_count = 0
    hour_start = time.time()
    fail_stocks = []
    ok_stocks = []

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

            if 'revenue' not in raw.columns:
                fail_stocks.append(sid)
                continue
            raw = raw.copy()
            raw['revenue'] = pd.to_numeric(raw['revenue'], errors='coerce')
            raw = raw.dropna(subset=['revenue'])
            if raw.empty:
                fail_stocks.append(sid)
                continue

            # Schema 對齊 live cache（cache_manager 寫入 FinMind 原始 schema）
            keep_cols = ['date', 'stock_id', 'country', 'revenue',
                         'revenue_month', 'revenue_year', 'revenue_last_year',
                         'revenue_year_growth', 'revenue_last_month',
                         'revenue_month_growth']
            present = [c for c in keep_cols if c in raw.columns]
            out_df = raw[present].copy()

            # 寫 per-stock live cache（這是 RF-1 的關鍵改動：從 backtest 改寫到 fundamental_cache）
            live_path = LIVE_CACHE_DIR / f"month_revenue_{sid}.parquet"
            out_df.to_parquet(live_path)
            ok_stocks.append(sid)

        except Exception as e:
            fail_stocks.append(sid)
            logger.warning("[%s] error: %s", sid, e)

        if (i + 1) % 25 == 0:
            logger.info("[%d/%d] %.1fmin elapsed, ok=%d fail=%d",
                        i + 1, len(stocks), (time.time() - t0) / 60,
                        len(ok_stocks), len(fail_stocks))

    logger.info("Backfill done: %d ok / %d fail / %.1fmin",
                len(ok_stocks), len(fail_stocks), (time.time() - t0) / 60)

    if fail_stocks:
        logger.warning("Failed (%d): %s", len(fail_stocks), fail_stocks[:20])

    # ================================================================
    # RF-1 鐵則：per-stock 寫完後，呼叫 aggregate 聚合到 backtest/
    # 確保 backfill 不會再造成 live 與 backtest 資料不一致
    # ================================================================
    if args.skip_aggregate:
        logger.warning("--skip-aggregate 啟用，未聚合 backtest/financials_revenue.parquet")
        return

    if not ok_stocks:
        logger.warning("No stocks backfilled, skip aggregate")
        return

    logger.info("Running aggregate_fundamental_cache.py --category revenue ...")
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "aggregate_fundamental_cache.py"),
         "--category", "revenue"],
        cwd=str(ROOT),
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Aggregate OK")
        # print tail of aggregate output
        for line in result.stdout.splitlines()[-8:]:
            logger.info("  %s", line)
    else:
        logger.error("Aggregate FAILED (rc=%d):\n%s", result.returncode, result.stderr)
        sys.exit(result.returncode)


if __name__ == "__main__":
    main()
