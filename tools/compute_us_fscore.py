"""
Compute Piotroski F-Score for US stocks from yfinance quarterly financials.

Input: data_cache/backtest/financials_us.parquet
Output: data_cache/backtest/quality_scores_us.parquet
    columns: [ticker, date, f_score, profitability, leverage, efficiency]

Limitation: yfinance only returns ~5 quarters, so this computes **current** F-Score
(most recent quarter vs 4 quarters ago). Historical panel requires paid source
(SimFin / Sharadar). For VF-Value-ex2, this enables descriptive TW vs US
comparison but not full historical IC.

Piotroski F-Score (9 criteria):
  Profitability (4):
    P1 Net Income > 0
    P2 CFO > 0
    P3 delta ROA > 0 (vs year ago)
    P4 CFO > Net Income (accrual quality)
  Leverage/Liquidity (3):
    L1 delta LT Debt / Assets < 0
    L2 delta Current Ratio > 0
    L3 No new shares issued
  Efficiency (2):
    E1 delta Gross Margin > 0
    E2 delta Asset Turnover > 0
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
IN_FIN = ROOT / 'data_cache' / 'backtest' / 'financials_us.parquet'
OUT = ROOT / 'data_cache' / 'backtest' / 'quality_scores_us.parquet'


def pivot_stock(df_ticker):
    """Pivot (date, line_item) -> wide table per ticker."""
    wide = df_ticker.pivot_table(
        index='date', columns='line_item', values='value', aggfunc='first'
    ).sort_index()
    return wide


def _get(w, *keys):
    """Get first available key from wide row as float, or NaN."""
    for k in keys:
        if k in w.index and pd.notna(w[k]):
            return float(w[k])
    return np.nan


def compute_fscore_one(ticker, df_ticker):
    """Compute F-Score for single ticker using 2 quarters: latest Q and Q minus 4.

    Actually Piotroski F-Score uses TTM (trailing 12 months) or annual, so we use:
      latest quarter vs Q-4 (year ago) for YoY deltas.
    """
    df_inc = df_ticker[df_ticker['statement'] == 'income']
    df_bal = df_ticker[df_ticker['statement'] == 'balance']
    df_cf = df_ticker[df_ticker['statement'] == 'cashflow']

    inc = pivot_stock(df_inc)
    bal = pivot_stock(df_bal)
    cf = pivot_stock(df_cf)

    if len(inc) < 2 or len(bal) < 2 or len(cf) < 2:
        return None

    # latest + year ago (use 4-quarter lag if available, else penultimate)
    dates_inc = list(inc.index)
    dates_bal = list(bal.index)
    dates_cf = list(cf.index)

    # Use last date as "current", 4 quarters earlier (or earliest) as "prior"
    def pick(dates):
        if len(dates) >= 5:
            return dates[-1], dates[-5]  # YoY
        return dates[-1], dates[0]

    d_inc, d_inc_prev = pick(dates_inc)
    d_bal, d_bal_prev = pick(dates_bal)
    d_cf, d_cf_prev = pick(dates_cf)

    cur_inc = inc.loc[d_inc]
    prev_inc = inc.loc[d_inc_prev]
    cur_bal = bal.loc[d_bal]
    prev_bal = bal.loc[d_bal_prev]
    cur_cf = cf.loc[d_cf]
    # cashflow prev not strictly needed for most criteria

    # --- Extract values with fallback key names ---
    ni_cur = _get(cur_inc, 'Net Income', 'Net Income Common Stockholders', 'Net Income From Continuing Operation Net Minority Interest')
    ni_prev = _get(prev_inc, 'Net Income', 'Net Income Common Stockholders')
    cfo_cur = _get(cur_cf, 'Operating Cash Flow', 'Cash Flow From Continuing Operating Activities')
    assets_cur = _get(cur_bal, 'Total Assets')
    assets_prev = _get(prev_bal, 'Total Assets')
    lt_debt_cur = _get(cur_bal, 'Long Term Debt', 'Long Term Debt And Capital Lease Obligation', 'Total Debt')
    lt_debt_prev = _get(prev_bal, 'Long Term Debt', 'Long Term Debt And Capital Lease Obligation', 'Total Debt')
    curr_assets_cur = _get(cur_bal, 'Current Assets')
    curr_assets_prev = _get(prev_bal, 'Current Assets')
    curr_liab_cur = _get(cur_bal, 'Current Liabilities')
    curr_liab_prev = _get(prev_bal, 'Current Liabilities')
    shares_cur = _get(cur_bal, 'Share Issued', 'Ordinary Shares Number')
    shares_prev = _get(prev_bal, 'Share Issued', 'Ordinary Shares Number')
    revenue_cur = _get(cur_inc, 'Total Revenue', 'Operating Revenue')
    revenue_prev = _get(prev_inc, 'Total Revenue', 'Operating Revenue')
    gross_cur = _get(cur_inc, 'Gross Profit')
    gross_prev = _get(prev_inc, 'Gross Profit')

    # --- 9 criteria ---
    score = 0
    components = {'profitability': 0, 'leverage': 0, 'efficiency': 0}

    # P1: NI > 0
    if pd.notna(ni_cur) and ni_cur > 0:
        score += 1; components['profitability'] += 1
    # P2: CFO > 0
    if pd.notna(cfo_cur) and cfo_cur > 0:
        score += 1; components['profitability'] += 1
    # P3: delta ROA > 0 (net income / assets)
    if pd.notna(ni_cur) and pd.notna(ni_prev) and pd.notna(assets_cur) and pd.notna(assets_prev) and assets_cur > 0 and assets_prev > 0:
        roa_cur = ni_cur / assets_cur
        roa_prev = ni_prev / assets_prev
        if roa_cur > roa_prev:
            score += 1; components['profitability'] += 1
    # P4: CFO > NI (accrual quality)
    if pd.notna(cfo_cur) and pd.notna(ni_cur) and cfo_cur > ni_cur:
        score += 1; components['profitability'] += 1

    # L1: delta LT Debt / Assets decreased (lower leverage)
    if pd.notna(lt_debt_cur) and pd.notna(lt_debt_prev) and pd.notna(assets_cur) and pd.notna(assets_prev) and assets_cur > 0 and assets_prev > 0:
        lev_cur = lt_debt_cur / assets_cur
        lev_prev = lt_debt_prev / assets_prev
        if lev_cur < lev_prev:
            score += 1; components['leverage'] += 1
    # L2: delta Current Ratio improved
    if pd.notna(curr_assets_cur) and pd.notna(curr_liab_cur) and pd.notna(curr_assets_prev) and pd.notna(curr_liab_prev) and curr_liab_cur > 0 and curr_liab_prev > 0:
        cr_cur = curr_assets_cur / curr_liab_cur
        cr_prev = curr_assets_prev / curr_liab_prev
        if cr_cur > cr_prev:
            score += 1; components['leverage'] += 1
    # L3: No new shares (shares flat or lower)
    if pd.notna(shares_cur) and pd.notna(shares_prev) and shares_cur <= shares_prev * 1.01:
        score += 1; components['leverage'] += 1

    # E1: delta Gross Margin > 0
    if pd.notna(gross_cur) and pd.notna(revenue_cur) and pd.notna(gross_prev) and pd.notna(revenue_prev) and revenue_cur > 0 and revenue_prev > 0:
        gm_cur = gross_cur / revenue_cur
        gm_prev = gross_prev / revenue_prev
        if gm_cur > gm_prev:
            score += 1; components['efficiency'] += 1
    # E2: delta Asset Turnover > 0
    if pd.notna(revenue_cur) and pd.notna(revenue_prev) and pd.notna(assets_cur) and pd.notna(assets_prev) and assets_cur > 0 and assets_prev > 0:
        at_cur = revenue_cur / assets_cur
        at_prev = revenue_prev / assets_prev
        if at_cur > at_prev:
            score += 1; components['efficiency'] += 1

    return {
        'ticker': ticker,
        'date': pd.Timestamp(d_inc).strftime('%Y-%m-%d'),
        'f_score': score,
        'profitability': components['profitability'],
        'leverage': components['leverage'],
        'efficiency': components['efficiency'],
    }


def main():
    if not IN_FIN.exists():
        logger.error('financials_us.parquet missing. Run fetch_us_financials.py first.')
        sys.exit(1)

    df = pd.read_parquet(IN_FIN)
    logger.info('Loaded %d rows, %d tickers', len(df), df['ticker'].nunique())

    rows = []
    skipped = 0
    for ticker, g in df.groupby('ticker'):
        try:
            r = compute_fscore_one(ticker, g)
            if r:
                rows.append(r)
            else:
                skipped += 1
        except Exception as e:
            logger.debug('%s failed: %s', ticker, e)
            skipped += 1

    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    logger.info('Saved %d tickers -> %s (skipped %d)', len(out), OUT, skipped)
    logger.info('F-Score distribution:')
    print(out['f_score'].value_counts().sort_index().to_string())
    logger.info('Summary: mean=%.2f median=%d std=%.2f', out['f_score'].mean(), out['f_score'].median(), out['f_score'].std())


if __name__ == '__main__':
    main()
