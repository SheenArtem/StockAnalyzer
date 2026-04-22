"""
Build US universe parquet for VF-L1b Phase 2.

2026-04-22 expansion to ~1570 tickers:
  - S&P 500              (~500)
  - S&P 400 MidCap       (~400)
  - S&P 600 SmallCap     (~600)
  - Nasdaq 100 non-SP    (~10 ADRs not in SP500)
  - CURATED_ADRS         (~40 major foreign ADRs + edge cases)

Source: Wikipedia (free, reliable). Excludes US-domicile rule so captures
TSM/ASML/TM/SAP/NVO etc. which S&P indices systematically exclude.

Output: data_cache/backtest/universe_us.parquet
    cols: [ticker, stock_name, sector, industry, source]

Usage:
    python tools/build_us_universe.py
"""

import argparse
import logging
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'data_cache' / 'backtest' / 'universe_us.parquet'

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

# Curated foreign ADRs + edge-case domicile (Bermuda etc.) that slip through all
# US-domicile-only indices. Expand as needed.
CURATED_ADRS = [
    # Taiwan
    'TSM',
    # Europe (NYSE/Nasdaq listed ADRs)
    'ASML', 'SAP', 'NVO', 'AZN', 'DEO', 'UL', 'NSRGY', 'BP', 'SHEL', 'TTE',
    'NVS', 'BTI', 'STM', 'NGG', 'ARM', 'BCS', 'ING',
    # Japan
    'TM', 'SONY', 'HMC', 'MUFG', 'SMFG',
    # China / HK
    'BABA', 'PDD', 'JD', 'BIDU', 'NTES', 'TCOM', 'TME', 'NIO', 'XPEV', 'LI', 'YMM',
    # Latin America
    'MELI', 'VALE', 'PBR', 'ITUB', 'BBD',
    # Canada
    'SHOP', 'RY', 'TD', 'BNS', 'BMO', 'CM', 'BN',
    # India
    'HDB', 'IBN', 'INFY', 'WIT',
    # Australia / Misc
    'BHP', 'RIO', 'TEL',
    # Bermuda/Caribbean domiciled US-listed (often missed by SP indices)
    'MRVL', 'CP', 'CNI',
    # Small-cap semi / specialty that rotate in/out of SP indices
    'WOLF',
]


def _fetch_wiki_table(url, symbol_col_candidates=('Symbol', 'Ticker', 'Ticker symbol')):
    """Fetch first Wikipedia table with a symbol column."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    dfs = pd.read_html(StringIO(resp.text))
    for df in dfs:
        for col in symbol_col_candidates:
            if col in df.columns:
                return df, col
    raise ValueError(f'No symbol column found at {url}')


def _normalize_ticker(t):
    """BRK.B -> BRK-B for yfinance."""
    return str(t).strip().replace('.', '-')


def fetch_sp500():
    df, col = _fetch_wiki_table(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        ('Symbol',),
    )
    df = df.rename(columns={
        col: 'ticker',
        'Security': 'stock_name',
        'GICS Sector': 'sector',
        'GICS Sub-Industry': 'industry',
    })
    out = df[['ticker', 'stock_name', 'sector', 'industry']].copy()
    out['ticker'] = out['ticker'].map(_normalize_ticker)
    out['source'] = 'SP500'
    return out


def fetch_sp400():
    df, col = _fetch_wiki_table(
        'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies',
        ('Symbol', 'Ticker'),
    )
    rename = {col: 'ticker'}
    if 'Security' in df.columns:
        rename['Security'] = 'stock_name'
    elif 'Company' in df.columns:
        rename['Company'] = 'stock_name'
    if 'GICS Sector' in df.columns:
        rename['GICS Sector'] = 'sector'
    if 'GICS Sub-Industry' in df.columns:
        rename['GICS Sub-Industry'] = 'industry'
    df = df.rename(columns=rename)
    cols = [c for c in ['ticker', 'stock_name', 'sector', 'industry'] if c in df.columns]
    out = df[cols].copy()
    out['ticker'] = out['ticker'].map(_normalize_ticker)
    for missing in ['stock_name', 'sector', 'industry']:
        if missing not in out.columns:
            out[missing] = ''
    out['source'] = 'SP400'
    return out[['ticker', 'stock_name', 'sector', 'industry', 'source']]


def fetch_sp600():
    df, col = _fetch_wiki_table(
        'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies',
        ('Symbol', 'Ticker'),
    )
    rename = {col: 'ticker'}
    if 'Security' in df.columns:
        rename['Security'] = 'stock_name'
    elif 'Company' in df.columns:
        rename['Company'] = 'stock_name'
    if 'GICS Sector' in df.columns:
        rename['GICS Sector'] = 'sector'
    if 'GICS Sub-Industry' in df.columns:
        rename['GICS Sub-Industry'] = 'industry'
    df = df.rename(columns=rename)
    cols = [c for c in ['ticker', 'stock_name', 'sector', 'industry'] if c in df.columns]
    out = df[cols].copy()
    out['ticker'] = out['ticker'].map(_normalize_ticker)
    for missing in ['stock_name', 'sector', 'industry']:
        if missing not in out.columns:
            out[missing] = ''
    out['source'] = 'SP600'
    return out[['ticker', 'stock_name', 'sector', 'industry', 'source']]


def fetch_nasdaq100():
    df, col = _fetch_wiki_table(
        'https://en.wikipedia.org/wiki/Nasdaq-100',
        ('Ticker', 'Symbol'),
    )
    rename = {col: 'ticker'}
    if 'Company' in df.columns:
        rename['Company'] = 'stock_name'
    elif 'Security' in df.columns:
        rename['Security'] = 'stock_name'
    if 'GICS Sector' in df.columns:
        rename['GICS Sector'] = 'sector'
    if 'GICS Sub-Industry' in df.columns:
        rename['GICS Sub-Industry'] = 'industry'
    df = df.rename(columns=rename)
    cols = [c for c in ['ticker', 'stock_name', 'sector', 'industry'] if c in df.columns]
    out = df[cols].copy()
    out['ticker'] = out['ticker'].map(_normalize_ticker)
    for missing in ['stock_name', 'sector', 'industry']:
        if missing not in out.columns:
            out[missing] = ''
    out['source'] = 'N100'
    return out[['ticker', 'stock_name', 'sector', 'industry', 'source']]


def build_curated_adrs():
    """Build DataFrame for hand-curated ADRs (metadata filled later via yfinance)."""
    return pd.DataFrame({
        'ticker': CURATED_ADRS,
        'stock_name': '',
        'sector': '',
        'industry': '',
        'source': 'ADR',
    })


def main():
    parts = []
    for name, fn in [
        ('S&P 500', fetch_sp500),
        ('S&P 400', fetch_sp400),
        ('S&P 600', fetch_sp600),
        ('Nasdaq 100', fetch_nasdaq100),
    ]:
        logger.info('Fetching %s...', name)
        try:
            df = fn()
            logger.info('  %d tickers', len(df))
            parts.append(df)
        except Exception as e:
            logger.warning('  %s failed: %s', name, e)

    parts.append(build_curated_adrs())
    logger.info('Curated ADRs: %d tickers', len(parts[-1]))

    universe = pd.concat(parts, ignore_index=True)
    # Dedupe by ticker, keep first source (priority: SP500 > SP400 > SP600 > N100 > ADR)
    before = len(universe)
    universe = universe.drop_duplicates('ticker', keep='first').reset_index(drop=True)
    logger.info('Dedupe: %d -> %d tickers', before, len(universe))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    universe.to_parquet(OUT, index=False)
    logger.info('Saved %d tickers -> %s', len(universe), OUT)
    logger.info('Source distribution:')
    print(universe['source'].value_counts().to_string())
    logger.info('Sector distribution (top):')
    print(universe['sector'].value_counts().head(12).to_string())


if __name__ == '__main__':
    main()
