"""
Fetch US quarterly financial statements for VF-L1b.

Source: yfinance Ticker.quarterly_income_stmt / quarterly_balance_sheet / quarterly_cashflow
Output: data_cache/backtest/financials_us.parquet
    columns: [ticker, date, statement, line_item, value]

yfinance returns 4-5 recent quarters only (limitation). For deeper history we'd need
a paid source (FactSet/Sharadar/SimFin). VF-L1b Phase 1 = recent 1-2yr F-Score + validate;
extend to 10yr requires SimFin free tier or similar later.

Usage:
    python tools/fetch_us_financials.py              # full universe
    python tools/fetch_us_financials.py --sample 20  # test
    python tools/fetch_us_financials.py --resume     # skip tickers already in output
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
OUT = ROOT / 'data_cache' / 'backtest' / 'financials_us.parquet'

# yfinance quarterly statement attribute names
STATEMENTS = {
    'income': 'quarterly_income_stmt',
    'balance': 'quarterly_balance_sheet',
    'cashflow': 'quarterly_cashflow',
}

RATE_PAUSE = 0.8  # seconds between ticker calls
CHECKPOINT_EVERY = 100  # flush to parquet every N tickers


def load_tickers(sample=None):
    u = pd.read_parquet(UNIVERSE)
    tickers = u['ticker'].tolist()
    if sample:
        tickers = tickers[:sample]
    return tickers


def fetch_statements(ticker, retries=2):
    import yfinance as yf
    rows = []
    for attempt in range(retries + 1):
        try:
            t = yf.Ticker(ticker)
            for stmt_key, attr in STATEMENTS.items():
                df = getattr(t, attr, None)
                if df is None or df.empty:
                    continue
                # df.columns = dates, df.index = line items
                for date_col in df.columns:
                    date_str = pd.Timestamp(date_col).strftime('%Y-%m-%d')
                    for line_item, val in df[date_col].items():
                        if pd.isna(val):
                            continue
                        rows.append({
                            'ticker': ticker,
                            'date': date_str,
                            'statement': stmt_key,
                            'line_item': str(line_item),
                            'value': float(val),
                        })
            return rows
        except Exception as e:
            logger.debug('%s attempt %d: %s', ticker, attempt + 1, e)
            if attempt < retries:
                time.sleep(2)
    logger.warning('%s: all attempts failed', ticker)
    return []


def flush(all_rows, existing_df):
    """Merge new rows with existing parquet, write back."""
    if not all_rows and existing_df is None:
        return existing_df
    new_df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
    if existing_df is not None and not existing_df.empty:
        merged = pd.concat([existing_df, new_df], ignore_index=True) if not new_df.empty else existing_df
    else:
        merged = new_df
    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(OUT, index=False)
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sample', type=int, default=None)
    ap.add_argument('--resume', action='store_true', help='skip tickers already in existing output')
    args = ap.parse_args()

    tickers = load_tickers(args.sample)

    existing_df = None
    done = set()
    if args.resume and OUT.exists():
        existing_df = pd.read_parquet(OUT)
        done = set(existing_df['ticker'].unique())
        logger.info('Resume: %d tickers already in %s, will skip', len(done), OUT.name)
    todo = [t for t in tickers if t not in done]
    logger.info('%d total tickers, %d todo (skipped %d)', len(tickers), len(todo), len(tickers) - len(todo))

    all_rows = []
    t0 = time.time()
    ok, fail = 0, 0
    for i, ticker in enumerate(todo, 1):
        rows = fetch_statements(ticker)
        if rows:
            all_rows.extend(rows)
            ok += 1
        else:
            fail += 1
        if i % 20 == 0 or i == len(todo):
            elapsed = time.time() - t0
            rate = i / elapsed * 60
            eta = (len(todo) - i) / rate if rate > 0 else 0
            logger.info('[%d/%d] ok=%d fail=%d elapsed=%.1fmin eta=%.1fmin rate=%.1f/min',
                        i, len(todo), ok, fail, elapsed / 60, eta, rate)
        if i % CHECKPOINT_EVERY == 0:
            existing_df = flush(all_rows, existing_df)
            logger.info('  checkpoint: flushed %d rows to %s (total rows now %d)',
                        len(all_rows), OUT.name, len(existing_df))
            all_rows = []
        time.sleep(RATE_PAUSE)

    # final flush
    existing_df = flush(all_rows, existing_df)

    if existing_df is None or existing_df.empty:
        logger.error('no data')
        sys.exit(1)

    df = existing_df
    logger.info('Saved %d rows, %d tickers -> %s', len(df), df['ticker'].nunique(), OUT)
    logger.info('Date range: %s ~ %s', df['date'].min(), df['date'].max())
    logger.info('By statement: %s', df['statement'].value_counts().to_dict())
    logger.info('Total: %.1f min (ok=%d fail=%d)', (time.time() - t0) / 60, ok, fail)


if __name__ == '__main__':
    main()
