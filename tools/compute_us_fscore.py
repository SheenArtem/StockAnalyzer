"""
Compute Piotroski F-Score for US stocks from yfinance quarterly financials.

Input:
  yfinance (default): data_cache/backtest/financials_us.parquet
  EDGAR:              data_cache/backtest/financials_us_edgar.parquet

Output:
  yfinance: data_cache/backtest/quality_scores_us.parquet        (live — do NOT overwrite)
  EDGAR:    data_cache/backtest/quality_scores_us_edgar.parquet

Schema auto-detect:
  If line_item contains 'NetIncome' (no space) → EDGAR schema
  If line_item contains 'Net Income' (with space) → yfinance schema

Limitation: yfinance only returns ~5 quarters, so this computes **current** F-Score
(most recent quarter vs 4 quarters ago). Historical panel requires paid source
(SimFin / Sharadar). EDGAR provides 16+ years of history.

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

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
IN_FIN_YFINANCE = ROOT / 'data_cache' / 'backtest' / 'financials_us.parquet'
IN_FIN_EDGAR = ROOT / 'data_cache' / 'backtest' / 'financials_us_edgar.parquet'
OUT_YFINANCE = ROOT / 'data_cache' / 'backtest' / 'quality_scores_us.parquet'
OUT_EDGAR = ROOT / 'data_cache' / 'backtest' / 'quality_scores_us_edgar.parquet'

# 向舊程式碼相容的別名
IN_FIN = IN_FIN_YFINANCE
OUT = OUT_YFINANCE

# EDGAR schema → yfinance key 對照（EDGAR 優先，yfinance fallback）
# _get() 會依序嘗試，第一個存在且非 NaN 的值就採用
EDGAR_TO_YF = {
    'net_income': ('NetIncome', 'Net Income', 'Net Income Common Stockholders',
                   'Net Income From Continuing Operation Net Minority Interest'),
    'cfo':        ('CFO', 'Operating Cash Flow', 'Cash Flow From Continuing Operating Activities'),
    'assets':     ('TotalAssets', 'Total Assets'),
    'lt_debt':    ('LongTermDebt', 'Long Term Debt', 'Long Term Debt And Capital Lease Obligation', 'Total Debt'),
    'curr_assets':('CurrentAssets', 'Current Assets'),
    'curr_liab':  ('CurrentLiabilities', 'Current Liabilities'),
    'shares':     ('SharesOutstanding', 'Share Issued', 'Ordinary Shares Number'),
    'revenue':    ('Revenue', 'Total Revenue', 'Operating Revenue'),
    'gross':      ('GrossProfit', 'Gross Profit'),
}


def _detect_schema(df: pd.DataFrame) -> str:
    """
    自動偵測 parquet 的 schema。
    回 'edgar' 或 'yfinance'。
    """
    line_items = set(df['line_item'].unique())
    if 'NetIncome' in line_items:
        return 'edgar'
    if 'Net Income' in line_items or 'Net Income Common Stockholders' in line_items:
        return 'yfinance'
    # 無法判斷，預設 yfinance（向後相容）
    logger.warning('Cannot detect schema from line_items, defaulting to yfinance')
    return 'yfinance'


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

    支援 EDGAR schema（NetIncome）與 yfinance schema（Net Income）兩種命名，
    透過 EDGAR_TO_YF 對照表 _get() 依序嘗試。
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

    # --- Extract values with fallback key names (EDGAR keys first, yfinance fallback) ---
    ni_cur = _get(cur_inc, *EDGAR_TO_YF['net_income'])
    ni_prev = _get(prev_inc, *EDGAR_TO_YF['net_income'])
    cfo_cur = _get(cur_cf, *EDGAR_TO_YF['cfo'])
    assets_cur = _get(cur_bal, *EDGAR_TO_YF['assets'])
    assets_prev = _get(prev_bal, *EDGAR_TO_YF['assets'])
    lt_debt_cur = _get(cur_bal, *EDGAR_TO_YF['lt_debt'])
    lt_debt_prev = _get(prev_bal, *EDGAR_TO_YF['lt_debt'])
    curr_assets_cur = _get(cur_bal, *EDGAR_TO_YF['curr_assets'])
    curr_assets_prev = _get(prev_bal, *EDGAR_TO_YF['curr_assets'])
    curr_liab_cur = _get(cur_bal, *EDGAR_TO_YF['curr_liab'])
    curr_liab_prev = _get(prev_bal, *EDGAR_TO_YF['curr_liab'])
    shares_cur = _get(cur_bal, *EDGAR_TO_YF['shares'])
    shares_prev = _get(prev_bal, *EDGAR_TO_YF['shares'])
    revenue_cur = _get(cur_inc, *EDGAR_TO_YF['revenue'])
    revenue_prev = _get(prev_inc, *EDGAR_TO_YF['revenue'])
    gross_cur = _get(cur_inc, *EDGAR_TO_YF['gross'])
    gross_prev = _get(prev_inc, *EDGAR_TO_YF['gross'])

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
    ap = argparse.ArgumentParser(description='Compute Piotroski F-Score for US stocks')
    ap.add_argument(
        '--source', choices=['yfinance', 'edgar', 'auto'], default='auto',
        help='資料來源: yfinance / edgar / auto（自動偵測，預設）'
    )
    ap.add_argument(
        '--input', default=None,
        help='覆寫輸入 parquet 路徑（預設依 --source 選擇）'
    )
    ap.add_argument(
        '--output', default=None,
        help='覆寫輸出 parquet 路徑（預設依偵測到的 schema 選擇）'
    )
    args = ap.parse_args()

    # --- 決定輸入路徑 ---
    if args.input:
        in_path = Path(args.input)
    elif args.source == 'edgar':
        in_path = IN_FIN_EDGAR
    elif args.source == 'yfinance':
        in_path = IN_FIN_YFINANCE
    else:
        # auto: 優先 edgar 若存在，否則 yfinance
        in_path = IN_FIN_EDGAR if IN_FIN_EDGAR.exists() else IN_FIN_YFINANCE

    if not in_path.exists():
        logger.error('Input parquet missing: %s', in_path)
        sys.exit(1)

    df = pd.read_parquet(in_path)
    logger.info('Loaded %d rows, %d tickers from %s', len(df), df['ticker'].nunique(), in_path.name)

    # --- 偵測 schema ---
    schema = _detect_schema(df)
    logger.info('Detected schema: %s', schema)

    # --- 決定輸出路徑 ---
    if args.output:
        out_path = Path(args.output)
    elif schema == 'edgar':
        out_path = OUT_EDGAR
    else:
        out_path = OUT_YFINANCE

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
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    logger.info('Saved %d tickers -> %s (skipped %d)', len(out), out_path, skipped)
    logger.info('F-Score distribution:')
    print(out['f_score'].value_counts().sort_index().to_string())
    logger.info('Summary: mean=%.2f median=%d std=%.2f', out['f_score'].mean(), out['f_score'].median(), out['f_score'].std())


if __name__ == '__main__':
    main()
