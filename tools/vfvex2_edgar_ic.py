"""
VF-Value-ex2 EDGAR 歷史 walk-forward IC 驗證

目的：
  驗證「US F≥8 +25 / US F≥7 +10」分拆門檻是否有真實 alpha。
  不只驗 IC，也驗 decile spread / F≥8 vs F≤5 超額報酬。

流程：
  Step 1a: 對每檔 ticker 的每個 quarter end，用 <= q 的 EDGAR financials 算 F-Score（PIT）
  Step 1b: 每個 (ticker, quarter_end) 取 entry = quarter_end + 45d，算 3m/6m/12m forward ret
  Step 2 : 每季算 Spearman IC / decile spread / F>=8 vs F<=5 / F>=7 vs F<=5
  Step 3 : regime subsample（S&P 500 200DMA slope / VIX 用 SPY realized vol 代替）
  Step 4 : 輸出 summary

輸出：
  reports/vfvex2_edgar_ic_by_quarter.csv
  reports/vfvex2_edgar_decile_spread.csv
  reports/vfvex2_edgar_ic_summary.md

執行：
  python tools/vfvex2_edgar_ic.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FIN_PATH = ROOT / 'data_cache' / 'backtest' / 'financials_us_edgar.parquet'
OHLCV_PATH = ROOT / 'data_cache' / 'backtest' / 'ohlcv_us.parquet'
SPY_PATH = ROOT / 'data_cache' / 'backtest' / '_spy_bench.parquet'
OUT_DIR = ROOT / 'reports'
OUT_DIR.mkdir(parents=True, exist_ok=True)

TODAY = pd.Timestamp('2026-04-22')

# EDGAR line_item keys
KEYS = {
    'ni': 'NetIncome',
    'cfo': 'CFO',
    'assets': 'TotalAssets',
    'lt_debt': 'LongTermDebt',
    'curr_assets': 'CurrentAssets',
    'curr_liab': 'CurrentLiabilities',
    'shares': 'SharesOutstanding',
    'revenue': 'Revenue',
    'gross': 'GrossProfit',
}


# ---------------------------------------------------------------------------
# Step 1a: Historical F-Score panel (per ticker per quarter)
# ---------------------------------------------------------------------------
def _pivot_ticker(df_t: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Pivot per statement -> wide tables (date x line_item)."""
    out = {}
    for stmt in ('income', 'balance', 'cashflow'):
        sub = df_t[df_t['statement'] == stmt]
        if len(sub) == 0:
            out[stmt] = pd.DataFrame()
            continue
        w = sub.pivot_table(index='date', columns='line_item', values='value', aggfunc='first')
        out[stmt] = w.sort_index()
    return out


def _get(row: pd.Series | None, key: str) -> float:
    if row is None:
        return np.nan
    if key not in row.index:
        return np.nan
    v = row[key]
    if pd.isna(v):
        return np.nan
    return float(v)


def _closest_on_or_before(df_wide: pd.DataFrame, target_date: pd.Timestamp) -> pd.Series | None:
    """Return latest row with date <= target. None if no such row."""
    if len(df_wide) == 0:
        return None
    idx = df_wide.index
    valid = idx[idx <= target_date]
    if len(valid) == 0:
        return None
    return df_wide.loc[valid.max()]


def _closest_around(df_wide: pd.DataFrame, target_date: pd.Timestamp,
                    tol_days: int = 60) -> pd.Series | None:
    """Return row closest to target (within tol_days). Used for Q-4 YoY lookup."""
    if len(df_wide) == 0:
        return None
    idx = df_wide.index
    diff = (idx - target_date).days
    valid_mask = np.abs(diff) <= tol_days
    if not valid_mask.any():
        return None
    valid_idx = idx[valid_mask]
    diff_valid = np.abs((valid_idx - target_date).days)
    best = valid_idx[np.argmin(diff_valid)]
    return df_wide.loc[best]


def compute_fscore_at_date(wides: dict, q_end: pd.Timestamp) -> dict | None:
    """Compute F-Score using data as of q_end (PIT). YoY deltas use q_end - ~365d."""
    inc, bal, cf = wides['income'], wides['balance'], wides['cashflow']
    if len(inc) == 0 or len(bal) == 0 or len(cf) == 0:
        return None

    # Current row: latest on or before q_end
    cur_inc = _closest_on_or_before(inc, q_end)
    cur_bal = _closest_on_or_before(bal, q_end)
    cur_cf = _closest_on_or_before(cf, q_end)
    if cur_inc is None or cur_bal is None or cur_cf is None:
        return None

    # Prior row: ~365 days before q_end (YoY)
    prior_target = q_end - pd.Timedelta(days=365)
    prev_inc = _closest_around(inc, prior_target, tol_days=60)
    prev_bal = _closest_around(bal, prior_target, tol_days=60)

    ni_cur = _get(cur_inc, KEYS['ni'])
    ni_prev = _get(prev_inc, KEYS['ni'])
    cfo_cur = _get(cur_cf, KEYS['cfo'])
    assets_cur = _get(cur_bal, KEYS['assets'])
    assets_prev = _get(prev_bal, KEYS['assets'])
    lt_debt_cur = _get(cur_bal, KEYS['lt_debt'])
    lt_debt_prev = _get(prev_bal, KEYS['lt_debt'])
    ca_cur = _get(cur_bal, KEYS['curr_assets'])
    ca_prev = _get(prev_bal, KEYS['curr_assets'])
    cl_cur = _get(cur_bal, KEYS['curr_liab'])
    cl_prev = _get(prev_bal, KEYS['curr_liab'])
    sh_cur = _get(cur_bal, KEYS['shares'])
    sh_prev = _get(prev_bal, KEYS['shares'])
    rev_cur = _get(cur_inc, KEYS['revenue'])
    rev_prev = _get(prev_inc, KEYS['revenue'])
    gp_cur = _get(cur_inc, KEYS['gross'])
    gp_prev = _get(prev_inc, KEYS['gross'])

    score = 0
    p, l, e = 0, 0, 0

    if pd.notna(ni_cur) and ni_cur > 0:
        score += 1; p += 1
    if pd.notna(cfo_cur) and cfo_cur > 0:
        score += 1; p += 1
    if (pd.notna(ni_cur) and pd.notna(ni_prev) and pd.notna(assets_cur) and pd.notna(assets_prev)
            and assets_cur > 0 and assets_prev > 0):
        if (ni_cur / assets_cur) > (ni_prev / assets_prev):
            score += 1; p += 1
    if pd.notna(cfo_cur) and pd.notna(ni_cur) and cfo_cur > ni_cur:
        score += 1; p += 1

    if (pd.notna(lt_debt_cur) and pd.notna(lt_debt_prev) and pd.notna(assets_cur)
            and pd.notna(assets_prev) and assets_cur > 0 and assets_prev > 0):
        if (lt_debt_cur / assets_cur) < (lt_debt_prev / assets_prev):
            score += 1; l += 1
    if (pd.notna(ca_cur) and pd.notna(cl_cur) and pd.notna(ca_prev) and pd.notna(cl_prev)
            and cl_cur > 0 and cl_prev > 0):
        if (ca_cur / cl_cur) > (ca_prev / cl_prev):
            score += 1; l += 1
    if pd.notna(sh_cur) and pd.notna(sh_prev) and sh_cur <= sh_prev * 1.01:
        score += 1; l += 1

    if (pd.notna(gp_cur) and pd.notna(rev_cur) and pd.notna(gp_prev) and pd.notna(rev_prev)
            and rev_cur > 0 and rev_prev > 0):
        if (gp_cur / rev_cur) > (gp_prev / rev_prev):
            score += 1; e += 1
    if (pd.notna(rev_cur) and pd.notna(rev_prev) and pd.notna(assets_cur) and pd.notna(assets_prev)
            and assets_cur > 0 and assets_prev > 0):
        if (rev_cur / assets_cur) > (rev_prev / assets_prev):
            score += 1; e += 1

    return {
        'f_score': score,
        'profitability': p,
        'leverage': l,
        'efficiency': e,
    }


def build_fscore_panel(fin: pd.DataFrame,
                       quarter_ends: list[pd.Timestamp]) -> pd.DataFrame:
    """For each ticker, compute F-Score at every quarter_end."""
    rows = []
    tickers = sorted(fin['ticker'].unique())
    total = len(tickers)
    logger.info('Computing historical F-Score for %d tickers x %d quarters', total, len(quarter_ends))

    for i, ticker in enumerate(tickers, 1):
        if i % 100 == 0:
            logger.info('  progress: %d / %d tickers', i, total)
        df_t = fin[fin['ticker'] == ticker]
        if len(df_t) < 9:  # need at least a few rows
            continue
        wides = _pivot_ticker(df_t)

        # Only iterate quarters where ticker actually has data starting
        first_date = df_t['date'].min()
        for q in quarter_ends:
            if q < first_date + pd.Timedelta(days=365):
                # Need at least one year of history for YoY
                continue
            r = compute_fscore_at_date(wides, q)
            if r is None:
                continue
            rows.append({'ticker': ticker, 'quarter_end': q, **r})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 1b: Forward return join
# ---------------------------------------------------------------------------
def compute_forward_returns(panel: pd.DataFrame, ohlcv: pd.DataFrame,
                            entry_lag_days: int = 45) -> pd.DataFrame:
    """For each (ticker, quarter_end) compute forward return at 3m/6m/12m."""
    ohlcv = ohlcv.sort_values(['ticker', 'date']).copy()
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])

    # Build per-ticker close series as (date -> AdjClose)
    logger.info('Building per-ticker price lookups...')
    ticker_prices = {}
    for ticker, g in ohlcv.groupby('ticker'):
        s = g.set_index('date')['AdjClose'].astype(float)
        ticker_prices[ticker] = s

    def _px_on_or_after(s: pd.Series, target: pd.Timestamp) -> tuple[pd.Timestamp, float] | None:
        idx = s.index
        valid = idx[idx >= target]
        if len(valid) == 0:
            return None
        d0 = valid.min()
        return d0, float(s.loc[d0])

    def _px_after_trading_days(s: pd.Series, start: pd.Timestamp, n_days: int) -> float | None:
        idx = s.index
        try:
            pos = idx.get_indexer([start])[0]
        except Exception:
            return None
        if pos == -1:
            return None
        tgt = pos + n_days
        if tgt >= len(idx):
            return None
        return float(s.iloc[tgt])

    out_rows = []
    total = len(panel)
    logger.info('Computing forward returns for %d (ticker, quarter) pairs...', total)

    for i, row in enumerate(panel.itertuples(index=False), 1):
        if i % 5000 == 0:
            logger.info('  progress: %d / %d', i, total)
        ticker = row.ticker
        q = row.quarter_end
        if ticker not in ticker_prices:
            continue
        s = ticker_prices[ticker]
        entry_target = q + pd.Timedelta(days=entry_lag_days)
        entry = _px_on_or_after(s, entry_target)
        if entry is None:
            continue
        entry_date, entry_px = entry
        # Forward 3m (63 TD), 6m (126 TD), 12m (252 TD)
        p3 = _px_after_trading_days(s, entry_date, 63)
        p6 = _px_after_trading_days(s, entry_date, 126)
        p12 = _px_after_trading_days(s, entry_date, 252)

        ret_3m = (p3 / entry_px - 1.0) if p3 is not None else np.nan
        ret_6m = (p6 / entry_px - 1.0) if p6 is not None else np.nan
        ret_12m = (p12 / entry_px - 1.0) if p12 is not None else np.nan

        out_rows.append({
            'ticker': ticker,
            'quarter_end': q,
            'entry_date': entry_date,
            'f_score': row.f_score,
            'profitability': row.profitability,
            'leverage': row.leverage,
            'efficiency': row.efficiency,
            'ret_3m': ret_3m,
            'ret_6m': ret_6m,
            'ret_12m': ret_12m,
        })

    return pd.DataFrame(out_rows)


# ---------------------------------------------------------------------------
# Step 2: IC / decile spread
# ---------------------------------------------------------------------------
def per_quarter_ic(panel: pd.DataFrame) -> pd.DataFrame:
    """Per-quarter Spearman IC between f_score and each forward return."""
    rows = []
    for q, g in panel.groupby('quarter_end'):
        g = g.dropna(subset=['f_score'])
        if len(g) < 20:
            continue
        row = {'quarter_end': q, 'n': len(g)}
        for h in ('ret_3m', 'ret_6m', 'ret_12m'):
            sub = g.dropna(subset=[h])
            if len(sub) < 20 or sub['f_score'].nunique() < 3:
                row[f'ic_{h}'] = np.nan
                continue
            ic, _p = spearmanr(sub['f_score'], sub[h])
            row[f'ic_{h}'] = ic
        rows.append(row)
    return pd.DataFrame(rows).sort_values('quarter_end')


def per_quarter_group_returns(panel: pd.DataFrame) -> pd.DataFrame:
    """Per-quarter mean return for each F-Score threshold group."""
    rows = []
    for q, g in panel.groupby('quarter_end'):
        row = {'quarter_end': q, 'n_total': len(g)}
        for h in ('ret_3m', 'ret_6m', 'ret_12m'):
            sub = g.dropna(subset=[h, 'f_score'])
            if len(sub) == 0:
                continue
            # Decile — rank by f_score into 10 buckets (may not always be 10 since
            # f_score is 0..9 discrete; we directly slice by thresholds)
            top_dec = sub[sub['f_score'] >= 8][h].mean()
            bot_dec = sub[sub['f_score'] <= 3][h].mean()
            f_ge_8 = sub[sub['f_score'] >= 8][h].mean()
            f_ge_7 = sub[sub['f_score'] >= 7][h].mean()
            f_le_5 = sub[sub['f_score'] <= 5][h].mean()
            n_f_ge_8 = (sub['f_score'] >= 8).sum()
            n_f_ge_7 = (sub['f_score'] >= 7).sum()
            n_f_le_5 = (sub['f_score'] <= 5).sum()
            row[f'top_dec_{h}'] = top_dec
            row[f'bot_dec_{h}'] = bot_dec
            row[f'f_ge_8_{h}'] = f_ge_8
            row[f'f_ge_7_{h}'] = f_ge_7
            row[f'f_le_5_{h}'] = f_le_5
            row[f'n_ge_8'] = n_f_ge_8
            row[f'n_ge_7'] = n_f_ge_7
            row[f'n_le_5'] = n_f_le_5
        rows.append(row)
    return pd.DataFrame(rows).sort_values('quarter_end')


# ---------------------------------------------------------------------------
# Step 3: Regime classification
# ---------------------------------------------------------------------------
def classify_regime(spy_df: pd.DataFrame) -> pd.DataFrame:
    """Based on ^GSPC close:
       - bull: 200DMA slope > 0 AND realized 20d vol < 18% (annualized)
       - bear: 200DMA slope < 0
       - volatile: realized 20d vol >= 25% annualized (overrides)
       - ranged: otherwise
    """
    # Normalize columns (yfinance multi-level)
    if isinstance(spy_df.columns, pd.MultiIndex):
        spy_df.columns = [c[0] for c in spy_df.columns]
    close = spy_df['Adj Close'] if 'Adj Close' in spy_df.columns else spy_df['Close']
    close = close.astype(float).sort_index()

    ma200 = close.rolling(200).mean()
    slope = ma200.diff(20)  # 20d slope of 200DMA
    log_ret = np.log(close / close.shift(1))
    rv20 = log_ret.rolling(20).std() * np.sqrt(252)

    regime = pd.Series(index=close.index, dtype='object')
    regime[:] = 'ranged'
    regime[slope > 0] = 'bull'
    regime[slope < 0] = 'bear'
    regime[rv20 >= 0.25] = 'volatile'
    return pd.DataFrame({'close': close, 'ma200': ma200, 'slope': slope, 'rv20': rv20, 'regime': regime})


def attach_regime(panel: pd.DataFrame, regime_df: pd.DataFrame) -> pd.DataFrame:
    """Attach regime based on entry_date."""
    panel = panel.copy()
    panel['entry_date'] = pd.to_datetime(panel['entry_date'])
    # asof merge
    regime_df = regime_df.reset_index().rename(columns={'Date': 'date', 'index': 'date'})
    if 'date' not in regime_df.columns:
        regime_df = regime_df.reset_index()
        regime_df.columns = ['date'] + list(regime_df.columns[1:])
    regime_df = regime_df.sort_values('date')
    panel = panel.sort_values('entry_date')
    merged = pd.merge_asof(panel, regime_df[['date', 'regime']],
                            left_on='entry_date', right_on='date',
                            direction='backward')
    return merged


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------
def summarize_ic(ic_df: pd.DataFrame) -> dict:
    out = {}
    for h in ('ret_3m', 'ret_6m', 'ret_12m'):
        col = f'ic_{h}'
        if col not in ic_df.columns:
            continue
        s = ic_df[col].dropna()
        if len(s) == 0:
            continue
        mean = s.mean()
        std = s.std()
        ir = mean / std if std > 0 else np.nan
        out[h] = {
            'mean_ic': mean,
            'std_ic': std,
            'ic_ir': ir,
            't_stat': mean / (std / np.sqrt(len(s))) if std > 0 else np.nan,
            'n_quarters': len(s),
            'pct_positive': (s > 0).mean(),
        }
    return out


def annualize_quarterly(group_ret: pd.Series, horizon_months: int) -> float:
    """Annualize mean return over horizon. group_ret is per-quarter sample mean."""
    if len(group_ret) == 0:
        return np.nan
    mean_per_period = group_ret.mean()
    # horizon_months months -> 12/horizon_months periods per year
    return (1 + mean_per_period) ** (12 / horizon_months) - 1


def summarize_groups(group_df: pd.DataFrame) -> dict:
    out = {}
    for h, months in (('ret_3m', 3), ('ret_6m', 6), ('ret_12m', 12)):
        key = h
        cols = [f'top_dec_{h}', f'bot_dec_{h}', f'f_ge_8_{h}', f'f_ge_7_{h}', f'f_le_5_{h}']
        if not all(c in group_df.columns for c in cols):
            continue
        out[key] = {
            'top_dec_ann': annualize_quarterly(group_df[f'top_dec_{h}'].dropna(), months),
            'bot_dec_ann': annualize_quarterly(group_df[f'bot_dec_{h}'].dropna(), months),
            'top_minus_bot_ann': (
                annualize_quarterly(group_df[f'top_dec_{h}'].dropna(), months)
                - annualize_quarterly(group_df[f'bot_dec_{h}'].dropna(), months)
            ),
            'f_ge_8_ann': annualize_quarterly(group_df[f'f_ge_8_{h}'].dropna(), months),
            'f_ge_7_ann': annualize_quarterly(group_df[f'f_ge_7_{h}'].dropna(), months),
            'f_le_5_ann': annualize_quarterly(group_df[f'f_le_5_{h}'].dropna(), months),
            'f_ge_8_minus_le_5_ann': (
                annualize_quarterly(group_df[f'f_ge_8_{h}'].dropna(), months)
                - annualize_quarterly(group_df[f'f_le_5_{h}'].dropna(), months)
            ),
            'f_ge_7_minus_le_5_ann': (
                annualize_quarterly(group_df[f'f_ge_7_{h}'].dropna(), months)
                - annualize_quarterly(group_df[f'f_le_5_{h}'].dropna(), months)
            ),
        }
    return out


def judge_grade(ic_ir: float) -> str:
    if pd.isna(ic_ir):
        return 'N/A'
    if abs(ic_ir) >= 0.5:
        return 'A (strong)'
    if abs(ic_ir) >= 0.3:
        return 'B (tradable)'
    if abs(ic_ir) >= 0.2:
        return 'C (weak)'
    return 'D (noise)'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info('=== VF-Value-ex2 EDGAR IC validation ===')

    logger.info('Loading %s', FIN_PATH.name)
    fin = pd.read_parquet(FIN_PATH)
    fin['date'] = pd.to_datetime(fin['date'])
    fin = fin[fin['date'] <= TODAY]
    fin = fin[['ticker', 'date', 'statement', 'line_item', 'value']]
    logger.info('  %d rows, %d tickers', len(fin), fin['ticker'].nunique())

    logger.info('Loading %s', OHLCV_PATH.name)
    ohlcv = pd.read_parquet(OHLCV_PATH, columns=['ticker', 'date', 'AdjClose'])
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])
    logger.info('  %d rows, %d tickers', len(ohlcv), ohlcv['ticker'].nunique())

    # Only validate tickers that appear in both
    common = set(fin['ticker'].unique()) & set(ohlcv['ticker'].unique())
    logger.info('Common tickers: %d', len(common))
    fin = fin[fin['ticker'].isin(common)]
    ohlcv = ohlcv[ohlcv['ticker'].isin(common)]

    # Build quarter_ends list: 2015Q4 ~ 2024Q4 (inclusive)
    # Need OHLCV for both entry and entry+252 TD, so entry must be <= 2025-04-20 approx
    quarter_ends = pd.date_range('2015-12-31', '2024-12-31', freq='QE')
    logger.info('Quarter ends: %d (%s ~ %s)',
                len(quarter_ends), quarter_ends.min(), quarter_ends.max())

    # Step 1a: F-Score panel
    logger.info('=== Step 1a: Historical F-Score panel ===')
    fscore_panel = build_fscore_panel(fin, list(quarter_ends))
    logger.info('F-Score panel: %d rows', len(fscore_panel))
    logger.info('F-Score distribution:')
    print(fscore_panel['f_score'].value_counts().sort_index().to_string())

    # Step 1b: Forward returns
    logger.info('=== Step 1b: Forward returns (entry = q + 45d) ===')
    panel = compute_forward_returns(fscore_panel, ohlcv, entry_lag_days=45)
    logger.info('Panel with returns: %d rows', len(panel))
    logger.info('Non-null ret_3m: %d', panel['ret_3m'].notna().sum())
    logger.info('Non-null ret_6m: %d', panel['ret_6m'].notna().sum())
    logger.info('Non-null ret_12m: %d', panel['ret_12m'].notna().sum())

    # Save intermediate panel (parquet for further digging)
    panel_path = OUT_DIR / 'vfvex2_edgar_panel.parquet'
    panel.to_parquet(panel_path, index=False)
    logger.info('Saved panel -> %s', panel_path)

    # Step 2: IC / group returns
    logger.info('=== Step 2: Per-quarter IC + group returns ===')
    ic_df = per_quarter_ic(panel)
    group_df = per_quarter_group_returns(panel)

    ic_path = OUT_DIR / 'vfvex2_edgar_ic_by_quarter.csv'
    ic_df.to_csv(ic_path, index=False)
    logger.info('Saved IC -> %s', ic_path)

    spread_path = OUT_DIR / 'vfvex2_edgar_decile_spread.csv'
    group_df.to_csv(spread_path, index=False)
    logger.info('Saved decile spread -> %s', spread_path)

    ic_summary = summarize_ic(ic_df)
    group_summary = summarize_groups(group_df)

    logger.info('=== IC summary ===')
    for h, stats in ic_summary.items():
        logger.info('  %s: mean=%.4f std=%.4f IR=%.3f t=%.2f pct_pos=%.1f%% n=%d',
                    h, stats['mean_ic'], stats['std_ic'], stats['ic_ir'],
                    stats['t_stat'], stats['pct_positive']*100, stats['n_quarters'])

    logger.info('=== Group summary (annualized) ===')
    for h, stats in group_summary.items():
        logger.info('  %s:', h)
        for k, v in stats.items():
            logger.info('    %s: %.2f%%', k, v*100 if pd.notna(v) else np.nan)

    # Step 3: regime subsample
    logger.info('=== Step 3: Regime breakdown ===')
    if SPY_PATH.exists():
        spy = pd.read_parquet(SPY_PATH)
        regime_df = classify_regime(spy)
        regime_df.index.name = 'date'
        panel_r = attach_regime(panel, regime_df)
        regime_cut = {}
        for reg, g in panel_r.groupby('regime'):
            if len(g) < 50:
                continue
            ic_r = per_quarter_ic(g)
            grp_r = per_quarter_group_returns(g)
            ic_s = summarize_ic(ic_r)
            grp_s = summarize_groups(grp_r)
            regime_cut[reg] = {'ic': ic_s, 'group': grp_s, 'n_obs': len(g)}
            logger.info('Regime %s (n=%d):', reg, len(g))
            for h, stats in ic_s.items():
                logger.info('  %s IC IR=%.3f mean=%.4f n_quarters=%d',
                            h, stats['ic_ir'], stats['mean_ic'], stats['n_quarters'])
    else:
        logger.warning('SPY benchmark missing, skipping regime analysis')
        regime_cut = {}

    # Step 4: Write markdown summary
    logger.info('=== Step 4: Write summary markdown ===')
    md = _format_summary(ic_summary, group_summary, regime_cut, panel, ic_df)
    md_path = OUT_DIR / 'vfvex2_edgar_ic_summary.md'
    md_path.write_text(md, encoding='utf-8')
    logger.info('Saved summary -> %s', md_path)

    # Also print summary to stdout
    print('\n' + '='*80)
    print(md)
    print('='*80)


def _format_summary(ic_summary, group_summary, regime_cut, panel, ic_df) -> str:
    lines = []
    lines.append('# VF-Value-ex2 EDGAR IC Validation Summary')
    lines.append('')
    lines.append(f'Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}')
    lines.append('')
    n_tickers = panel['ticker'].nunique()
    n_quarters = panel['quarter_end'].nunique()
    n_obs = len(panel)
    lines.append(f'Sample: {n_obs:,} (ticker, quarter) observations')
    lines.append(f'        {n_tickers} unique tickers x {n_quarters} quarters')
    lines.append(f'        Quarter range: {panel["quarter_end"].min():%Y-%m-%d} ~ {panel["quarter_end"].max():%Y-%m-%d}')
    lines.append('')
    # F-Score distribution
    lines.append('## F-Score Distribution')
    lines.append('')
    lines.append('| F-Score | Count | Pct |')
    lines.append('|---|---|---|')
    dist = panel['f_score'].value_counts().sort_index()
    total = dist.sum()
    for fs, c in dist.items():
        lines.append(f'| {fs} | {c:,} | {c/total*100:.1f}% |')
    lines.append('')
    pct_ge_8 = (panel['f_score'] >= 8).mean() * 100
    pct_ge_7 = (panel['f_score'] >= 7).mean() * 100
    pct_le_5 = (panel['f_score'] <= 5).mean() * 100
    lines.append(f'Historical incidence: F>=8 = {pct_ge_8:.1f}%, F>=7 = {pct_ge_7:.1f}%, F<=5 = {pct_le_5:.1f}%')
    lines.append('')

    # IC summary
    lines.append('## IC Summary (Spearman, f_score vs forward return)')
    lines.append('')
    lines.append('| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N quarters | Grade |')
    lines.append('|---|---|---|---|---|---|---|---|')
    for h, s in ic_summary.items():
        grade = judge_grade(s['ic_ir'])
        lines.append(
            f'| {h} | {s["mean_ic"]:+.4f} | {s["std_ic"]:.4f} | {s["ic_ir"]:+.3f} | '
            f'{s["t_stat"]:+.2f} | {s["pct_positive"]*100:.1f}% | {s["n_quarters"]} | {grade} |'
        )
    lines.append('')

    # Group summary
    lines.append('## Group Annualized Returns')
    lines.append('')
    for h, s in group_summary.items():
        lines.append(f'### {h}')
        lines.append('')
        lines.append('| Group | Ann. Return |')
        lines.append('|---|---|')
        lines.append(f'| Top decile (F>=8) | {s["top_dec_ann"]*100:+.2f}% |')
        lines.append(f'| Bot decile (F<=3) | {s["bot_dec_ann"]*100:+.2f}% |')
        lines.append(f'| **Top - Bot spread** | **{s["top_minus_bot_ann"]*100:+.2f}%** |')
        lines.append(f'| F>=8 | {s["f_ge_8_ann"]*100:+.2f}% |')
        lines.append(f'| F>=7 | {s["f_ge_7_ann"]*100:+.2f}% |')
        lines.append(f'| F<=5 | {s["f_le_5_ann"]*100:+.2f}% |')
        lines.append(f'| **F>=8 alpha vs F<=5** | **{s["f_ge_8_minus_le_5_ann"]*100:+.2f}%** |')
        lines.append(f'| F>=7 alpha vs F<=5 | {s["f_ge_7_minus_le_5_ann"]*100:+.2f}% |')
        lines.append('')

    # Regime
    if regime_cut:
        lines.append('## By Regime')
        lines.append('')
        lines.append('### IC IR by regime (3m / 6m / 12m)')
        lines.append('')
        lines.append('| Regime | N obs | IC IR 3m | IC IR 6m | IC IR 12m |')
        lines.append('|---|---|---|---|---|')
        for reg, d in regime_cut.items():
            ic = d['ic']
            ir3 = ic.get('ret_3m', {}).get('ic_ir', np.nan)
            ir6 = ic.get('ret_6m', {}).get('ic_ir', np.nan)
            ir12 = ic.get('ret_12m', {}).get('ic_ir', np.nan)
            lines.append(f'| {reg} | {d["n_obs"]:,} | {ir3:+.3f} | {ir6:+.3f} | {ir12:+.3f} |')
        lines.append('')
        lines.append('### F>=8 alpha vs F<=5 (annualized, 6m horizon)')
        lines.append('')
        lines.append('| Regime | N | F>=8 ann | F<=5 ann | Alpha |')
        lines.append('|---|---|---|---|---|')
        for reg, d in regime_cut.items():
            grp = d['group'].get('ret_6m', {})
            f8 = grp.get('f_ge_8_ann', np.nan)
            f5 = grp.get('f_le_5_ann', np.nan)
            alpha = grp.get('f_ge_8_minus_le_5_ann', np.nan)
            lines.append(
                f'| {reg} | {d["n_obs"]:,} | '
                f'{f8*100:+.2f}% | {f5*100:+.2f}% | **{alpha*100:+.2f}%** |'
            )
        lines.append('')

    # Conclusion
    lines.append('## Conclusion')
    lines.append('')
    best = max(ic_summary.items(), key=lambda x: abs(x[1]['ic_ir']))
    best_h, best_s = best
    lines.append(f'- Best IC horizon: **{best_h}** with IR = **{best_s["ic_ir"]:+.3f}** (grade {judge_grade(best_s["ic_ir"])})')

    # F>=8 alpha at best horizon
    g = group_summary.get(best_h, {})
    if g:
        lines.append(f'- **F>=8 alpha vs F<=5 (annualized, {best_h})**: **{g["f_ge_8_minus_le_5_ann"]*100:+.2f}%**')
        lines.append(f'- F>=7 alpha vs F<=5 (annualized, {best_h}): {g["f_ge_7_minus_le_5_ann"]*100:+.2f}%')
        lines.append(f'- Top-Bot decile spread (annualized, {best_h}): {g["top_minus_bot_ann"]*100:+.2f}%')
    lines.append('')

    return '\n'.join(lines)


if __name__ == '__main__':
    main()
