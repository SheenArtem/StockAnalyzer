"""
批次抓取 FinMind 歷史季報（損益/資產負債/現金流/月營收）

存到 data_cache/backtest/financials_tw.parquet（合併所有 dataset）
FinMind 600 call/hr 限制，300 股 x 4 dataset = 1200 call，約 2hr。

用法:
    python tools/fetch_financial_history.py              # 抓 top300
    python tools/fetch_financial_history.py --sample 10  # 測試 10 支
"""

import argparse
import json
import logging
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

OUT_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# FinMind datasets to fetch
DATASETS = [
    ('TaiwanStockFinancialStatements', 'income'),
    ('TaiwanStockBalanceSheet', 'balance'),
    ('TaiwanStockCashFlowsStatement', 'cashflow'),
    ('TaiwanStockMonthRevenue', 'revenue'),
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

        for dataset, name in DATASETS:
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
                    all_data.setdefault(name, []).append(df)

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


def save_parquets(all_data):
    """Save each dataset as a separate parquet file."""
    for name, dfs in all_data.items():
        if not dfs:
            continue
        combined = pd.concat(dfs, ignore_index=True)
        out_path = OUT_DIR / f"financials_{name}.parquet"
        combined.to_parquet(out_path, index=False)
        logger.info(f"Saved {out_path}: {len(combined)} rows, {combined['stock_id'].nunique()} stocks")


def main():
    parser = argparse.ArgumentParser(description='Fetch FinMind historical financials')
    parser.add_argument('--sample', type=int, default=None, help='Only fetch N stocks (for testing)')
    parser.add_argument('--since', default='2020-01-01', help='Start date (default: 2020-01-01)')
    args = parser.parse_args()

    tickers = load_universe(args.sample)
    logger.info(f"Universe: {len(tickers)} stocks, since {args.since}")
    logger.info(f"Estimated: {len(tickers) * CALLS_PER_STOCK} API calls, "
                f"~{len(tickers) * CALLS_PER_STOCK / FINMIND_RATE_LIMIT:.1f} hours")

    start = time.time()
    all_data = fetch_all(tickers, start_date=args.since)
    save_parquets(all_data)

    elapsed = time.time() - start
    logger.info(f"Done in {elapsed/60:.1f} min")

    # Clean up checkpoint
    cp = OUT_DIR / "_fetch_checkpoint.json"
    if cp.exists():
        cp.unlink()


if __name__ == '__main__':
    main()
