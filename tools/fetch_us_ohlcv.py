"""
Fetch US OHLCV historical data for VF-L1b.

Source: yfinance (bulk download, chunked to avoid 429).
Output: data_cache/backtest/ohlcv_us.parquet
    columns: [ticker, date, Open, High, Low, Close, AdjClose, Volume, Dividends, Splits]

Usage:
    python tools/fetch_us_ohlcv.py                # full S&P 500, 10.5 yr
    python tools/fetch_us_ohlcv.py --sample 20    # test 20 stocks
    python tools/fetch_us_ohlcv.py --start 2015-01-01 --end 2026-04-22
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
UNIVERSE = ROOT / 'data_cache' / 'backtest' / 'universe_us.parquet'
OUT = ROOT / 'data_cache' / 'backtest' / 'ohlcv_us.parquet'

CHUNK_SIZE = 50  # yfinance bulk handles ~50 tickers well


def load_universe(sample=None):
    if not UNIVERSE.exists():
        logger.error('Universe not found. Run build_us_universe.py first.')
        sys.exit(1)
    u = pd.read_parquet(UNIVERSE)
    tickers = u['ticker'].tolist()
    if sample:
        tickers = tickers[:sample]
    return tickers, u


def fetch_chunk(tickers, start, end):
    import yfinance as yf
    data = yf.download(
        tickers=' '.join(tickers),
        start=start,
        end=end,
        auto_adjust=False,
        actions=True,
        group_by='ticker',
        progress=False,
        threads=True,
    )
    if data.empty:
        return pd.DataFrame()
    rows = []
    for t in tickers:
        try:
            if t in data.columns.get_level_values(0):
                sub = data[t].reset_index()
            else:
                continue
            sub['ticker'] = t
            rows.append(sub)
        except Exception as e:
            logger.debug('skip %s: %s', t, e)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out = out.rename(columns={
        'Adj Close': 'AdjClose',
        'Date': 'date',
    })
    cols = ['ticker', 'date', 'Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume', 'Dividends', 'Stock Splits']
    out = out[[c for c in cols if c in out.columns]].copy()
    out = out.rename(columns={'Stock Splits': 'Splits'})
    out = out.dropna(subset=['Close'])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sample', type=int, default=None)
    ap.add_argument('--start', default='2015-06-01')
    ap.add_argument('--end', default='2026-04-22')
    ap.add_argument('--chunk-size', type=int, default=CHUNK_SIZE)
    args = ap.parse_args()

    tickers, universe = load_universe(args.sample)
    logger.info('%d tickers, %s ~ %s', len(tickers), args.start, args.end)

    all_dfs = []
    t0 = time.time()
    for i in range(0, len(tickers), args.chunk_size):
        chunk = tickers[i:i + args.chunk_size]
        elapsed = time.time() - t0
        logger.info('[%d/%d] chunk %s..%s (%.1fs elapsed)', i + 1, len(tickers), chunk[0], chunk[-1], elapsed)
        try:
            df = fetch_chunk(chunk, args.start, args.end)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.warning('chunk failed: %s', e)
        time.sleep(1)  # be nice to yfinance

    if not all_dfs:
        logger.error('no data fetched')
        sys.exit(1)

    out = pd.concat(all_dfs, ignore_index=True)
    # Join stock_name / sector for convenience
    meta = universe[['ticker', 'stock_name', 'sector', 'industry']]
    out = out.merge(meta, on='ticker', how='left')

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    logger.info('Saved %d rows, %d unique tickers -> %s', len(out), out['ticker'].nunique(), OUT)
    logger.info('Date range: %s ~ %s', out['date'].min(), out['date'].max())
    logger.info('Total time: %.1f min', (time.time() - t0) / 60)


if __name__ == '__main__':
    main()
