"""
批次抓取 FinMind 歷史季報（損益/資產負債/現金流/月營收）

2026-04-21 RF-1 重構：
---------------------
原版直接寫 data_cache/backtest/financials_*.parquet，導致 live scanner
讀 data_cache/fundamental_cache/ 時看不到。現改成：
  1. per-stock 寫 data_cache/fundamental_cache/{cache_key}_{sid}.parquet
  2. 最後自動呼叫 aggregate_fundamental_cache.py 聚合到 backtest/

規則：backfill tool 只能寫 fundamental_cache/，禁止直接寫 backtest/。
詳見 feedback_unified_cache.md。

FinMind 600 call/hr 限制，300 股 x 4 dataset = 1200 call，約 2hr。

用法:
    python tools/fetch_financial_history.py              # 抓 top300
    python tools/fetch_financial_history.py --sample 10  # 測試 10 支
    python tools/fetch_financial_history.py --skip-aggregate  # 不跑 aggregate
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("fetch_fin")

OUT_DIR = _ROOT / "data_cache" / "backtest"  # for universe + checkpoint only
LIVE_CACHE_DIR = _ROOT / "data_cache" / "fundamental_cache"  # RF-1: backfill 寫入目標
OUT_DIR.mkdir(parents=True, exist_ok=True)
LIVE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# FinMind datasets to fetch: (finmind_dataset, short_name, fundamental_cache_key)
# cache_key 對齊 cache_manager.get_cached_fundamentals() + backfill_fundamentals.CATEGORIES
DATASETS = [
    ('TaiwanStockFinancialStatements', 'income',   'financial_statement'),
    ('TaiwanStockBalanceSheet',        'balance',  'balance_sheet'),
    ('TaiwanStockCashFlowsStatement',  'cashflow', 'cash_flows_statement'),
    ('TaiwanStockMonthRevenue',        'revenue',  'month_revenue'),
]

# Rate limiting
FINMIND_RATE_LIMIT = 600
CALLS_PER_STOCK = len(DATASETS)  # 4
BATCH_PAUSE = 0.5  # seconds between calls
HOURLY_PAUSE = 65  # seconds to wait when approaching limit


def load_universe(sample=None):
    universe_file = OUT_DIR / "top300_universe.json"
    with open(universe_file, 'r') as f:
        tickers = json.load(f)
    if sample:
        tickers = tickers[:sample]
    return tickers


def fetch_all(tickers, start_date='2020-01-01'):
    """Fetch all datasets for all tickers, with rate limiting and checkpoint."""
    from cache_manager import get_finmind_loader
    dl = get_finmind_loader()

    checkpoint_file = OUT_DIR / "_fetch_checkpoint.json"
    all_data = {}  # dataset_name -> list of DataFrames

    # Load checkpoint
    done_ids = set()
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r') as f:
                cp = json.load(f)
            done_ids = set(cp.get('done', []))
            logger.info(f"Resuming: {len(done_ids)} stocks already fetched")
        except Exception:
            pass

    call_count = 0
    hour_start = time.time()
    total = len(tickers)

    for i, sid in enumerate(tickers):
        if sid in done_ids:
            continue

        # Rate limit check
        elapsed = time.time() - hour_start
        if elapsed < 3600 and call_count >= FINMIND_RATE_LIMIT - 10:
            wait = 3600 - elapsed + 5
            logger.warning(f"Approaching rate limit ({call_count} calls in {elapsed:.0f}s), waiting {wait:.0f}s...")
            time.sleep(wait)
            call_count = 0
            hour_start = time.time()
        elif elapsed >= 3600:
            call_count = 0
            hour_start = time.time()

        if (i + 1) % 20 == 0 or i < 3:
            logger.info(f"[{i+1}/{total}] {sid} (calls={call_count})")

        for dataset, name, cache_key in DATASETS:
            try:
                if dataset == 'TaiwanStockFinancialStatements':
                    df = dl.taiwan_stock_financial_statement(
                        stock_id=sid, start_date=start_date)
                elif dataset == 'TaiwanStockBalanceSheet':
                    df = dl.taiwan_stock_balance_sheet(
                        stock_id=sid, start_date=start_date)
                elif dataset == 'TaiwanStockCashFlowsStatement':
                    df = dl.taiwan_stock_cash_flows_statement(
                        stock_id=sid, start_date=start_date)
                elif dataset == 'TaiwanStockMonthRevenue':
                    df = dl.taiwan_stock_month_revenue(
                        stock_id=sid, start_date=start_date)

                if df is not None and not df.empty:
                    df = df.copy()
                    df['stock_id'] = sid
                    # RF-1: 寫 per-stock live cache（取代舊版 in-memory accumulate + 寫 backtest）
                    live_path = LIVE_CACHE_DIR / f"{cache_key}_{sid}.parquet"
                    df.to_parquet(live_path)

                call_count += 1
            except Exception as e:
                logger.warning(f"  {sid}/{name} failed: {e}")
                call_count += 1

            time.sleep(BATCH_PAUSE)

        done_ids.add(sid)

        # Save checkpoint every 20 stocks
        if len(done_ids) % 20 == 0:
            with open(checkpoint_file, 'w') as f:
                json.dump({'done': list(done_ids), 'timestamp': datetime.now().isoformat()}, f)

    # Save checkpoint final
    with open(checkpoint_file, 'w') as f:
        json.dump({'done': list(done_ids), 'timestamp': datetime.now().isoformat()}, f)

    return all_data


def run_aggregate():
    """RF-1: 呼叫 aggregate_fundamental_cache.py 聚合 fundamental_cache/ → backtest/financials_*.parquet。"""
    logger.info("Running aggregate_fundamental_cache.py (all categories)...")
    result = subprocess.run(
        [sys.executable, str(_ROOT / "tools" / "aggregate_fundamental_cache.py")],
        cwd=str(_ROOT),
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Aggregate OK")
        for line in result.stdout.splitlines()[-12:]:
            logger.info(f"  {line}")
    else:
        logger.error(f"Aggregate FAILED (rc={result.returncode}):\n{result.stderr}")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description='Fetch FinMind historical financials')
    parser.add_argument('--sample', type=int, default=None, help='Only fetch N stocks (for testing)')
    parser.add_argument('--since', default='2020-01-01', help='Start date (default: 2020-01-01)')
    parser.add_argument('--skip-aggregate', action='store_true',
                        help='不自動跑 aggregate（僅測試用）')
    args = parser.parse_args()

    tickers = load_universe(args.sample)
    logger.info(f"Universe: {len(tickers)} stocks, since {args.since}")
    logger.info(f"Estimated: {len(tickers) * CALLS_PER_STOCK} API calls, "
                f"~{len(tickers) * CALLS_PER_STOCK / FINMIND_RATE_LIMIT:.1f} hours")

    start = time.time()
    fetch_all(tickers, start_date=args.since)

    elapsed = time.time() - start
    logger.info(f"Fetch done in {elapsed/60:.1f} min")

    # RF-1: backfill 完後自動聚合，不然 backtest/financials_*.parquet 會 stale
    if args.skip_aggregate:
        logger.warning("--skip-aggregate 啟用，未聚合 backtest/")
    else:
        run_aggregate()

    # Clean up checkpoint
    cp = OUT_DIR / "_fetch_checkpoint.json"
    if cp.exists():
        cp.unlink()


if __name__ == '__main__':
    main()
