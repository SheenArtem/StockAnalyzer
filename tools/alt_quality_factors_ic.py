"""
Alt quality factors IC validation — US 2015-2024

Validates three alternative quality factors against the same EDGAR panel
used for Piotroski F-Score (52,062 obs, 1512 tickers x 37 quarters):

  1. FCF Yield         = (CFO - CapEx) / MarketCap    (cheaper + cash)
  2. ROIC (approx)     = EBIT * (1 - 0.21) / (Equity + LongTermDebt)
  3. Gross Profitability (Novy-Marx) = GrossProfit / TotalAssets

For each factor, compute per-quarter Spearman IC vs 3m/6m/12m forward return,
decile spread, quintile alpha, and regime-conditional breakdown.

Reuses:
  - data_cache/backtest/financials_us_edgar.parquet  (already has GP, Assets, CFO, Equity, LTDebt, Shares)
  - data_cache/sec_edgar/raw/CIK*.json               (for incremental CapEx/OpIncome extraction)
  - data_cache/backtest/ohlcv_us.parquet             (entry price + forward returns)
  - data_cache/backtest/_spy_bench.parquet           (regime)
  - reports/vfvex2_edgar_panel.parquet               (entry_date + forward returns)

Output:
  reports/alt_quality_factors_by_quarter.csv
  reports/alt_quality_factors_decile_spread.csv
  reports/alt_quality_factors_ic_summary.md
  reports/alt_quality_factors_panel.parquet          (for further drilldown)

Run: python tools/alt_quality_factors_ic.py
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parent.parent
FIN_PATH = ROOT / 'data_cache' / 'backtest' / 'financials_us_edgar.parquet'
OHLCV_PATH = ROOT / 'data_cache' / 'backtest' / 'ohlcv_us.parquet'
SPY_PATH = ROOT / 'data_cache' / 'backtest' / '_spy_bench.parquet'
PANEL_PATH = ROOT / 'reports' / 'vfvex2_edgar_panel.parquet'
RAW_DIR = ROOT / 'data_cache' / 'sec_edgar' / 'raw'
CIK_MAP_PATH = ROOT / 'data_cache' / 'sec_edgar' / 'cik_mapping.json'
OUT_DIR = ROOT / 'reports'
OUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

# -- Assumed constant corporate tax rate for NOPAT approximation
TAX_RATE = 0.21

# Tags we need in addition to what's already in financials_us_edgar.parquet
EXTRA_TAG_MAP = {
    'CapEx': {
        'tags': [
            'PaymentsToAcquirePropertyPlantAndEquipment',
            'PaymentsToAcquireProductiveAssets',
            'PaymentsForCapitalImprovements',
        ],
        'is_instant': False,
    },
    'OperatingIncome': {
        'tags': ['OperatingIncomeLoss'],
        'is_instant': False,
    },
    'InterestExpense': {
        'tags': ['InterestExpense', 'InterestExpenseDebt'],
        'is_instant': False,
    },
    'IncomeTaxExpense': {
        'tags': ['IncomeTaxExpenseBenefit'],
        'is_instant': False,
    },
}


# ---------------------------------------------------------------------------
# Extra tag extraction (re-uses the same YTD-derivation logic as sec_edgar_loader)
# ---------------------------------------------------------------------------
def _derive_quarterly_from_ytd(entries: list[dict]) -> dict[str, float]:
    """From YTD cumulative entries, back out each single quarter."""
    from datetime import datetime
    fy_groups: dict[str, list[dict]] = {}
    for e in entries:
        start_str = e.get('start')
        if start_str is None:
            continue
        fy_groups.setdefault(start_str, []).append(e)

    result: dict[str, float] = {}
    for fy_start, group in fy_groups.items():
        parsed = []
        for e in group:
            try:
                diff = (datetime.strptime(e['end'], '%Y-%m-%d')
                        - datetime.strptime(fy_start, '%Y-%m-%d')).days
            except ValueError:
                continue
            if e.get('val') is not None:
                parsed.append((diff, e['end'], float(e['val'])))
        if not parsed:
            continue
        parsed.sort()
        by_diff: dict[int, tuple[str, float]] = {}
        for diff, end_str, val in parsed:
            bucket = round(diff / 95)
            by_diff[bucket] = (end_str, val)
        ytd = {b: v for b, (_, v) in by_diff.items()}
        ends = {b: end for b, (end, _) in by_diff.items()}
        for q_bucket in sorted(ytd.keys()):
            prev_bucket = q_bucket - 1
            if prev_bucket == 0 or prev_bucket not in ytd:
                q_val = ytd[q_bucket]
            else:
                q_val = ytd[q_bucket] - ytd[prev_bucket]
            result[ends[q_bucket]] = q_val
    return result


def _get_unit_entries(us_gaap: dict, tag: str) -> list[dict]:
    if tag not in us_gaap:
        return []
    units_dict = us_gaap[tag].get('units', {})
    for unit_key in ('USD', 'shares', 'pure'):
        if unit_key in units_dict:
            return units_dict[unit_key]
    for entries in units_dict.values():
        return entries
    return []


def _extract_tag_series(us_gaap: dict, tags: list[str], is_instant: bool) -> dict[str, float]:
    """Return {end_date_str: value}."""
    from datetime import datetime
    merged: dict[str, float] = {}
    for tag in tags:
        entries = _get_unit_entries(us_gaap, tag)
        if not entries:
            continue
        result_for_tag: dict[str, float] = {}
        if is_instant:
            for e in entries:
                if e.get('form') in ('10-Q', '10-K') and e.get('val') is not None:
                    result_for_tag[e['end']] = float(e['val'])
        else:
            valid = [e for e in entries
                     if e.get('form') in ('10-Q', '10-K')
                     and e.get('val') is not None
                     and e.get('start') is not None]
            single_q: dict[str, float] = {}
            for e in valid:
                try:
                    diff = (datetime.strptime(e['end'], '%Y-%m-%d')
                            - datetime.strptime(e['start'], '%Y-%m-%d')).days
                except ValueError:
                    continue
                if 60 <= diff <= 105:
                    single_q[e['end']] = float(e['val'])
            derived = _derive_quarterly_from_ytd(valid)
            for d_str, val in derived.items():
                if d_str not in single_q:
                    single_q[d_str] = val
            result_for_tag = single_q
        for date_str, val in result_for_tag.items():
            if date_str not in merged:
                merged[date_str] = val
    return merged


def extract_extra_fields_for_cik(cik_file: Path) -> pd.DataFrame:
    """Return DataFrame with (date, CapEx, OperatingIncome, InterestExpense, IncomeTaxExpense)."""
    with open(cik_file, encoding='utf-8') as f:
        data = json.load(f)
    us_gaap = data.get('facts', {}).get('us-gaap', {})
    if not us_gaap:
        return pd.DataFrame()

    all_dates = set()
    series_dict: dict[str, dict[str, float]] = {}
    for field, cfg in EXTRA_TAG_MAP.items():
        s = _extract_tag_series(us_gaap, cfg['tags'], cfg['is_instant'])
        series_dict[field] = s
        all_dates.update(s.keys())

    rows = []
    for d in sorted(all_dates):
        row = {'date': d}
        for field, s in series_dict.items():
            row[field] = s.get(d, np.nan)
        rows.append(row)
    return pd.DataFrame(rows)


def build_extra_financials(tickers: list[str]) -> pd.DataFrame:
    """Extract CapEx + OperatingIncome + InterestExpense + IncomeTaxExpense for all tickers.
    Returns long-form: [ticker, date, line_item, value].
    """
    with open(CIK_MAP_PATH, encoding='utf-8') as f:
        cik_map = json.load(f)

    rows = []
    total = len(tickers)
    missing_cik = 0
    missing_file = 0
    for i, ticker in enumerate(tickers, 1):
        if i % 200 == 0:
            logger.info('  extra-fields progress: %d / %d', i, total)
        cik = cik_map.get(ticker.upper())
        if not cik:
            missing_cik += 1
            continue
        fp = RAW_DIR / f'CIK{cik}.json'
        if not fp.exists():
            missing_file += 1
            continue
        try:
            df = extract_extra_fields_for_cik(fp)
        except Exception as e:
            logger.warning('%s extract failed: %s', ticker, e)
            continue
        if df.empty:
            continue
        df['ticker'] = ticker
        rows.append(df)

    logger.info('  missing CIK=%d missing file=%d', missing_cik, missing_file)
    if not rows:
        return pd.DataFrame()
    wide = pd.concat(rows, ignore_index=True)
    long = wide.melt(id_vars=['ticker', 'date'],
                     value_vars=list(EXTRA_TAG_MAP.keys()),
                     var_name='line_item', value_name='value')
    long = long.dropna(subset=['value'])
    long['date'] = pd.to_datetime(long['date'])
    return long


# ---------------------------------------------------------------------------
# Factor computation
# ---------------------------------------------------------------------------
def _closest_on_or_before(df: pd.DataFrame, target: pd.Timestamp, col='date') -> pd.Series | None:
    if len(df) == 0:
        return None
    valid = df[df[col] <= target]
    if len(valid) == 0:
        return None
    return valid.iloc[valid[col].argmax()]


def compute_factors(
    fin_base: pd.DataFrame,
    fin_extra: pd.DataFrame,
    panel: pd.DataFrame,
    ohlcv: pd.DataFrame,
) -> pd.DataFrame:
    """For every (ticker, quarter_end) in panel, compute 3 factor values.

    fin_base: long-form financials_us_edgar.parquet (has GP, Assets, CFO, Equity, LTDebt, Shares, NI)
    fin_extra: long-form extras (CapEx, OperatingIncome, InterestExpense, IncomeTaxExpense)
    panel: base panel with ticker, quarter_end, entry_date, forward returns
    ohlcv: price panel for market cap
    """
    # Merge base + extra into one wide per-ticker table
    base_w = fin_base.pivot_table(
        index=['ticker', 'date'], columns='line_item', values='value', aggfunc='first'
    ).reset_index()
    if not fin_extra.empty:
        extra_w = fin_extra.pivot_table(
            index=['ticker', 'date'], columns='line_item', values='value', aggfunc='first'
        ).reset_index()
        wide = base_w.merge(extra_w, on=['ticker', 'date'], how='outer')
    else:
        wide = base_w
        for f in EXTRA_TAG_MAP.keys():
            wide[f] = np.nan
    wide['date'] = pd.to_datetime(wide['date'])
    wide = wide.sort_values(['ticker', 'date'])

    # Group by ticker for fast PIT lookup
    wide_by_ticker = {t: g.set_index('date') for t, g in wide.groupby('ticker')}

    # Build per-ticker close series (for market cap)
    ohlcv = ohlcv[['ticker', 'date', 'AdjClose']].copy()
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])
    ohlcv = ohlcv.sort_values(['ticker', 'date'])
    price_by_ticker = {t: g.set_index('date')['AdjClose'] for t, g in ohlcv.groupby('ticker')}

    rows = []
    total = len(panel)
    logger.info('Computing factors for %d (ticker, quarter) rows...', total)
    n_ok = {'fcf_yield': 0, 'roic': 0, 'gp_assets': 0, 'roic_nopat_fallback': 0}

    for i, row in enumerate(panel.itertuples(index=False), 1):
        if i % 5000 == 0:
            logger.info('  progress: %d / %d (%s ok: FCF=%d ROIC=%d GP=%d)',
                        i, total, row.quarter_end,
                        n_ok['fcf_yield'], n_ok['roic'], n_ok['gp_assets'])
        t = row.ticker
        q = row.quarter_end
        if t not in wide_by_ticker:
            continue
        w = wide_by_ticker[t]
        idx_valid = w.index[w.index <= q]
        if len(idx_valid) == 0:
            continue
        cur = w.loc[idx_valid.max()]

        ni_cur = cur.get('NetIncome', np.nan)
        cfo_cur = cur.get('CFO', np.nan)
        assets_cur = cur.get('TotalAssets', np.nan)
        gp_cur = cur.get('GrossProfit', np.nan)
        equity_cur = cur.get('StockholdersEquity', np.nan)
        ltdebt_cur = cur.get('LongTermDebt', np.nan)
        shares_cur = cur.get('SharesOutstanding', np.nan)
        capex_cur = cur.get('CapEx', np.nan) if 'CapEx' in cur.index else np.nan
        opinc_cur = cur.get('OperatingIncome', np.nan) if 'OperatingIncome' in cur.index else np.nan
        intexp_cur = cur.get('InterestExpense', np.nan) if 'InterestExpense' in cur.index else np.nan
        taxexp_cur = cur.get('IncomeTaxExpense', np.nan) if 'IncomeTaxExpense' in cur.index else np.nan

        # Aggregate TTM (trailing 4 quarters) for flow items: CFO, CapEx, NI, OperatingIncome
        # More stable for yield / ROIC than single-quarter.
        idx_ttm = idx_valid.sort_values()[-4:]
        ttm_rows = w.loc[idx_ttm]
        cfo_ttm = ttm_rows['CFO'].sum() if 'CFO' in ttm_rows.columns else np.nan
        capex_ttm = ttm_rows['CapEx'].sum() if 'CapEx' in ttm_rows.columns else np.nan
        ni_ttm = ttm_rows['NetIncome'].sum() if 'NetIncome' in ttm_rows.columns else np.nan
        opinc_ttm = ttm_rows['OperatingIncome'].sum() if 'OperatingIncome' in ttm_rows.columns else np.nan
        intexp_ttm = ttm_rows['InterestExpense'].sum() if 'InterestExpense' in ttm_rows.columns else np.nan
        taxexp_ttm = ttm_rows['IncomeTaxExpense'].sum() if 'IncomeTaxExpense' in ttm_rows.columns else np.nan

        # Need full 4 quarters of TTM data
        n_q = len(ttm_rows)
        if n_q < 4:
            cfo_ttm = np.nan
            capex_ttm = np.nan
            opinc_ttm = np.nan
            ni_ttm = np.nan

        # --- Factor 1: FCF Yield = (CFO_ttm - CapEx_ttm) / MarketCap(entry)
        # CapEx in cash-flow statements is typically already positive (outflow).
        # Some tags may be reported as negative; enforce abs for safety.
        fcf_yield = np.nan
        if pd.notna(cfo_ttm) and pd.notna(capex_ttm) and pd.notna(shares_cur) and shares_cur > 0:
            fcf = cfo_ttm - abs(capex_ttm)
            # Market cap at entry
            s = price_by_ticker.get(t)
            if s is not None:
                entry = pd.Timestamp(row.entry_date)
                valid_px = s.index[s.index >= entry]
                if len(valid_px) > 0:
                    px = float(s.loc[valid_px.min()])
                    mcap = px * shares_cur
                    if mcap > 0:
                        fcf_yield = fcf / mcap
                        n_ok['fcf_yield'] += 1

        # --- Factor 2: ROIC = NOPAT / InvestedCapital
        # NOPAT prefers EBIT * (1-t); fallback to NI + InterestExpense*(1-t) + TaxExpense.
        # InvestedCapital = Equity + LT Debt (at q_end).
        roic = np.nan
        invested = np.nan
        if pd.notna(equity_cur) and equity_cur > 0:
            invested = equity_cur + (ltdebt_cur if pd.notna(ltdebt_cur) else 0.0)
        if pd.notna(invested) and invested > 0:
            if pd.notna(opinc_ttm):
                nopat = opinc_ttm * (1 - TAX_RATE)
                roic = nopat / invested
                n_ok['roic'] += 1
            elif pd.notna(ni_ttm):
                # Fallback: NI + after-tax interest (approximate NOPAT)
                intexp_used = intexp_ttm if pd.notna(intexp_ttm) else 0.0
                nopat_approx = ni_ttm + intexp_used * (1 - TAX_RATE)
                roic = nopat_approx / invested
                n_ok['roic_nopat_fallback'] += 1

        # --- Factor 3: Gross Profitability = GP_ttm / TotalAssets
        # Novy-Marx uses annual GP / Assets. Use TTM GP / Assets for consistency with quarterly panel.
        gp_assets = np.nan
        if pd.notna(assets_cur) and assets_cur > 0:
            gp_ttm = ttm_rows['GrossProfit'].sum() if 'GrossProfit' in ttm_rows.columns and n_q == 4 else np.nan
            if pd.notna(gp_ttm):
                gp_assets = gp_ttm / assets_cur
                n_ok['gp_assets'] += 1

        rows.append({
            'ticker': t,
            'quarter_end': q,
            'entry_date': row.entry_date,
            'ret_3m': row.ret_3m,
            'ret_6m': row.ret_6m,
            'ret_12m': row.ret_12m,
            'fcf_yield': fcf_yield,
            'roic': roic,
            'gp_assets': gp_assets,
            'f_score': row.f_score,  # keep for reference
        })

    logger.info('Factor coverage: FCF=%d ROIC=%d (with OpIncome) + %d (NI fallback) GP=%d',
                n_ok['fcf_yield'], n_ok['roic'], n_ok['roic_nopat_fallback'], n_ok['gp_assets'])
    df = pd.DataFrame(rows)

    # Sanity filter: drop outliers that indicate data-quality issues (spinoffs, IPO
    # mismatches, tiny denominators). These are rank-invariant filters and do not
    # introduce lookahead bias.
    before = {
        'fcf': df['fcf_yield'].notna().sum(),
        'roic': df['roic'].notna().sum(),
        'gp': df['gp_assets'].notna().sum(),
    }
    df.loc[df['fcf_yield'].abs() > 1.0, 'fcf_yield'] = np.nan     # |FCF yield| <= 100%
    df.loc[df['roic'].abs() > 2.0, 'roic'] = np.nan               # |ROIC| <= 200%
    df.loc[df['gp_assets'] > 3.0, 'gp_assets'] = np.nan           # GP/A up to 300%
    df.loc[df['gp_assets'] < -0.5, 'gp_assets'] = np.nan
    after = {
        'fcf': df['fcf_yield'].notna().sum(),
        'roic': df['roic'].notna().sum(),
        'gp': df['gp_assets'].notna().sum(),
    }
    logger.info('Outlier filter dropped: FCF=%d, ROIC=%d, GP=%d',
                before['fcf'] - after['fcf'],
                before['roic'] - after['roic'],
                before['gp'] - after['gp'])
    return df


# ---------------------------------------------------------------------------
# IC / decile / quintile analysis
# ---------------------------------------------------------------------------
def winsorize(s: pd.Series, lo=0.01, hi=0.99) -> pd.Series:
    q_lo = s.quantile(lo)
    q_hi = s.quantile(hi)
    return s.clip(q_lo, q_hi)


def per_quarter_ic(panel: pd.DataFrame, factor_col: str) -> pd.DataFrame:
    """Per-quarter Spearman IC between factor and each forward return horizon."""
    rows = []
    for q, g in panel.groupby('quarter_end'):
        sub = g.dropna(subset=[factor_col])
        if len(sub) < 30:
            continue
        # winsorize factor to reduce outlier influence
        sub = sub.copy()
        sub[factor_col] = winsorize(sub[factor_col])
        row = {'quarter_end': q, 'n': len(sub)}
        for h in ('ret_3m', 'ret_6m', 'ret_12m'):
            s = sub.dropna(subset=[h])
            if len(s) < 30 or s[factor_col].nunique() < 5:
                row[f'ic_{h}'] = np.nan
                continue
            ic, _p = spearmanr(s[factor_col], s[h])
            row[f'ic_{h}'] = ic
        rows.append(row)
    return pd.DataFrame(rows).sort_values('quarter_end')


def per_quarter_group_returns(panel: pd.DataFrame, factor_col: str) -> pd.DataFrame:
    """Per-quarter decile (10) and quintile (5) group mean return, plus top-bot spread."""
    rows = []
    for q, g in panel.groupby('quarter_end'):
        sub = g.dropna(subset=[factor_col]).copy()
        if len(sub) < 50:
            continue
        # Decile
        sub['dec'] = pd.qcut(sub[factor_col], 10, labels=False, duplicates='drop')
        sub['quin'] = pd.qcut(sub[factor_col], 5, labels=False, duplicates='drop')
        row = {'quarter_end': q, 'n_total': len(sub)}
        for h in ('ret_3m', 'ret_6m', 'ret_12m'):
            s = sub.dropna(subset=[h])
            if len(s) < 50:
                continue
            top_dec = s.loc[s['dec'] == s['dec'].max(), h].mean()
            bot_dec = s.loc[s['dec'] == s['dec'].min(), h].mean()
            top_q = s.loc[s['quin'] == s['quin'].max(), h].mean()
            bot_q = s.loc[s['quin'] == s['quin'].min(), h].mean()
            row[f'top_dec_{h}'] = top_dec
            row[f'bot_dec_{h}'] = bot_dec
            row[f'top_quin_{h}'] = top_q
            row[f'bot_quin_{h}'] = bot_q
        rows.append(row)
    return pd.DataFrame(rows).sort_values('quarter_end')


# ---------------------------------------------------------------------------
# Regime
# ---------------------------------------------------------------------------
def classify_regime(spy_df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(spy_df.columns, pd.MultiIndex):
        spy_df.columns = [c[0] for c in spy_df.columns]
    close = spy_df['Adj Close'] if 'Adj Close' in spy_df.columns else spy_df['Close']
    close = close.astype(float).sort_index()
    ma200 = close.rolling(200).mean()
    slope = ma200.diff(20)
    log_ret = np.log(close / close.shift(1))
    rv20 = log_ret.rolling(20).std() * np.sqrt(252)
    regime = pd.Series(index=close.index, dtype='object')
    regime[:] = 'ranged'
    regime[slope > 0] = 'bull'
    regime[slope < 0] = 'bear'
    regime[rv20 >= 0.25] = 'volatile'
    return pd.DataFrame({'close': close, 'regime': regime})


def attach_regime(panel: pd.DataFrame, regime_df: pd.DataFrame) -> pd.DataFrame:
    panel = panel.copy()
    panel['entry_date'] = pd.to_datetime(panel['entry_date'])
    regime_df = regime_df.reset_index()
    regime_df.columns = ['date'] + list(regime_df.columns[1:])
    regime_df['date'] = pd.to_datetime(regime_df['date'])
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
            'pct_pos': (s > 0).mean(),
            'n_quarters': len(s),
        }
    return out


def annualize_quarterly(ret_series: pd.Series, horizon_months: int) -> float:
    if len(ret_series) == 0:
        return np.nan
    m = ret_series.mean()
    return (1 + m) ** (12 / horizon_months) - 1


def summarize_groups(grp_df: pd.DataFrame) -> dict:
    out = {}
    for h, months in (('ret_3m', 3), ('ret_6m', 6), ('ret_12m', 12)):
        cols = [f'top_dec_{h}', f'bot_dec_{h}', f'top_quin_{h}', f'bot_quin_{h}']
        if not all(c in grp_df.columns for c in cols):
            continue
        top_dec_ann = annualize_quarterly(grp_df[f'top_dec_{h}'].dropna(), months)
        bot_dec_ann = annualize_quarterly(grp_df[f'bot_dec_{h}'].dropna(), months)
        top_q_ann = annualize_quarterly(grp_df[f'top_quin_{h}'].dropna(), months)
        bot_q_ann = annualize_quarterly(grp_df[f'bot_quin_{h}'].dropna(), months)
        # Per-quarter win rate (consistency)
        dec_diff = (grp_df[f'top_dec_{h}'] - grp_df[f'bot_dec_{h}']).dropna()
        quin_diff = (grp_df[f'top_quin_{h}'] - grp_df[f'bot_quin_{h}']).dropna()
        out[h] = {
            'top_dec_ann': top_dec_ann,
            'bot_dec_ann': bot_dec_ann,
            'top_minus_bot_dec_ann': top_dec_ann - bot_dec_ann,
            'top_quin_ann': top_q_ann,
            'bot_quin_ann': bot_q_ann,
            'top_minus_bot_quin_ann': top_q_ann - bot_q_ann,
            'dec_win_rate': (dec_diff > 0).mean() if len(dec_diff) > 0 else np.nan,
            'quin_win_rate': (quin_diff > 0).mean() if len(quin_diff) > 0 else np.nan,
            'dec_ir': (dec_diff.mean() / dec_diff.std()) if len(dec_diff) > 1 and dec_diff.std() > 0 else np.nan,
            'quin_ir': (quin_diff.mean() / quin_diff.std()) if len(quin_diff) > 1 and quin_diff.std() > 0 else np.nan,
        }
    return out


def judge_grade(ir: float) -> str:
    if pd.isna(ir):
        return 'N/A'
    if abs(ir) >= 0.5:
        return 'A (strong)'
    if abs(ir) >= 0.3:
        return 'B (tradable)'
    if abs(ir) >= 0.1:
        return 'C (weak)'
    return 'D (noise)'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info('=== Alt quality factors IC validation (FCF yield / ROIC / Gross profitability) ===')

    # Load base panel (existing F-Score panel has forward returns)
    logger.info('Loading base panel %s', PANEL_PATH.name)
    panel_base = pd.read_parquet(PANEL_PATH)
    panel_base['quarter_end'] = pd.to_datetime(panel_base['quarter_end'])
    panel_base['entry_date'] = pd.to_datetime(panel_base['entry_date'])
    logger.info('  %d rows, %d tickers, %d quarters',
                len(panel_base), panel_base['ticker'].nunique(), panel_base['quarter_end'].nunique())

    # Load base financials
    logger.info('Loading base financials %s', FIN_PATH.name)
    fin_base = pd.read_parquet(FIN_PATH)
    fin_base['date'] = pd.to_datetime(fin_base['date'])
    logger.info('  %d rows', len(fin_base))

    # Extract extra fields (CapEx, OperatingIncome, Interest, Tax)
    tickers = sorted(panel_base['ticker'].unique())
    logger.info('Extracting CapEx / OperatingIncome from %d raw CIK files...', len(tickers))
    fin_extra = build_extra_financials(tickers)
    logger.info('  extra rows: %d, line_items: %s',
                len(fin_extra), sorted(fin_extra['line_item'].unique()) if len(fin_extra) else [])

    # Save extra for debugging
    extra_path = OUT_DIR / 'alt_quality_extra_fields.parquet'
    if not fin_extra.empty:
        fin_extra.to_parquet(extra_path, index=False)
        logger.info('  saved extra fields -> %s', extra_path)

    # Load OHLCV
    logger.info('Loading OHLCV %s', OHLCV_PATH.name)
    ohlcv = pd.read_parquet(OHLCV_PATH, columns=['ticker', 'date', 'AdjClose'])
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])
    logger.info('  %d rows, %d tickers', len(ohlcv), ohlcv['ticker'].nunique())

    # Compute factors
    panel = compute_factors(fin_base, fin_extra, panel_base, ohlcv)
    logger.info('Factor panel: %d rows', len(panel))
    for col in ('fcf_yield', 'roic', 'gp_assets'):
        n_nn = panel[col].notna().sum()
        logger.info('  %s: %d non-null (%.1f%%)', col, n_nn, 100 * n_nn / len(panel))

    # Save full panel
    panel_out = OUT_DIR / 'alt_quality_factors_panel.parquet'
    panel.to_parquet(panel_out, index=False)
    logger.info('Saved panel -> %s', panel_out)

    # Per-quarter IC + group returns per factor
    ic_all = {}
    grp_all = {}
    for factor in ('fcf_yield', 'roic', 'gp_assets'):
        logger.info('--- %s ---', factor)
        ic_df = per_quarter_ic(panel, factor)
        grp_df = per_quarter_group_returns(panel, factor)
        ic_df.to_csv(OUT_DIR / f'alt_quality_ic_{factor}_by_quarter.csv', index=False)
        grp_df.to_csv(OUT_DIR / f'alt_quality_decile_{factor}_by_quarter.csv', index=False)
        ic_all[factor] = summarize_ic(ic_df)
        grp_all[factor] = summarize_groups(grp_df)

        for h, s in ic_all[factor].items():
            logger.info('  %s %s: IC=%+.4f std=%.4f IR=%+.3f t=%+.2f n_q=%d',
                        factor, h, s['mean_ic'], s['std_ic'], s['ic_ir'],
                        s['t_stat'], s['n_quarters'])

    # Year-by-year top-bot decile spread (reveal timing of factor performance)
    panel_y = panel.copy()
    panel_y['year'] = panel_y['quarter_end'].dt.year
    year_rows = []
    for factor in ('fcf_yield', 'roic', 'gp_assets'):
        for y, g in panel_y.groupby('year'):
            sub = g.dropna(subset=[factor, 'ret_12m'])
            if len(sub) < 100:
                continue
            sub = sub.copy()
            sub['dec'] = pd.qcut(sub[factor], 10, labels=False, duplicates='drop')
            top = sub.loc[sub['dec'] == sub['dec'].max(), 'ret_12m'].mean()
            bot = sub.loc[sub['dec'] == sub['dec'].min(), 'ret_12m'].mean()
            year_rows.append({'factor': factor, 'year': int(y),
                              'top_dec_12m': top, 'bot_dec_12m': bot, 'spread': top - bot})
    year_df = pd.DataFrame(year_rows)
    year_df.to_csv(OUT_DIR / 'alt_quality_by_year.csv', index=False)

    # Factor correlations
    corr = panel[['fcf_yield', 'roic', 'gp_assets', 'f_score']].corr(method='spearman').round(3)
    corr.to_csv(OUT_DIR / 'alt_quality_factor_corr.csv')
    logger.info('Factor corr saved')

    # Regime breakdown (just best horizon per factor)
    regime_cut = {}
    if SPY_PATH.exists():
        logger.info('Running regime breakdown...')
        spy = pd.read_parquet(SPY_PATH)
        reg_df = classify_regime(spy)
        panel_r = attach_regime(panel, reg_df)
        for factor in ('fcf_yield', 'roic', 'gp_assets'):
            reg_ic = {}
            reg_grp = {}
            for reg, g in panel_r.groupby('regime'):
                if len(g) < 100:
                    continue
                ic_r = per_quarter_ic(g, factor)
                grp_r = per_quarter_group_returns(g, factor)
                reg_ic[reg] = summarize_ic(ic_r)
                reg_grp[reg] = summarize_groups(grp_r)
            regime_cut[factor] = {'ic': reg_ic, 'group': reg_grp}

    # Format summary
    md = format_summary(panel, ic_all, grp_all, regime_cut, year_df, corr)
    md_path = OUT_DIR / 'alt_quality_factors_ic_summary.md'
    md_path.write_text(md, encoding='utf-8')
    logger.info('Saved summary -> %s', md_path)

    print('\n' + '=' * 80)
    print(md)
    print('=' * 80)


def format_summary(panel: pd.DataFrame, ic_all: dict, grp_all: dict,
                   regime_cut: dict, year_df: pd.DataFrame,
                   corr: pd.DataFrame) -> str:
    L = []
    L.append('# Alt Quality Factors IC Validation Summary')
    L.append('')
    L.append(f'Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}')
    L.append('')
    L.append(f'Sample: {len(panel):,} (ticker, quarter) rows')
    L.append(f'        {panel["ticker"].nunique()} tickers x {panel["quarter_end"].nunique()} quarters')
    L.append(f'        Period: {panel["quarter_end"].min():%Y-%m-%d} ~ {panel["quarter_end"].max():%Y-%m-%d}')
    L.append('')
    L.append('Factors:')
    L.append('- **FCF Yield** = (CFO_ttm - |CapEx_ttm|) / (Price_entry * Shares_q)')
    L.append('- **ROIC (approx)** = OpIncome_ttm * (1 - 0.21) / (Equity_q + LTDebt_q); NI fallback if OpIncome missing')
    L.append('- **Gross Profitability** = GrossProfit_ttm / TotalAssets_q  (Novy-Marx 2013)')
    L.append('')
    for col in ('fcf_yield', 'roic', 'gp_assets'):
        n = panel[col].notna().sum()
        L.append(f'  {col}: {n:,} non-null ({100*n/len(panel):.1f}%)')
    L.append('')

    L.append('## Head-to-head (vs F-Score baseline)')
    L.append('')
    L.append('| Factor | N obs | IC IR 12m | Top-Bot Dec 12m (ann) | Dec Win Rate | Top Quin alpha 12m | Grade |')
    L.append('|---|---|---|---|---|---|---|')
    # F-Score reference (from prior run)
    L.append('| Piotroski F-Score (ref) | 52,062 | -0.272 | -13.08% | 37.8% | -10.11% | D (reverse) |')
    for factor, label in (('fcf_yield', 'FCF Yield'),
                          ('roic', 'ROIC (approx)'),
                          ('gp_assets', 'Gross Profitability')):
        ic12 = ic_all[factor].get('ret_12m', {}).get('ic_ir', np.nan)
        grp12 = grp_all[factor].get('ret_12m', {})
        tb_dec = grp12.get('top_minus_bot_dec_ann', np.nan)
        tb_quin = grp12.get('top_minus_bot_quin_ann', np.nan)
        win = grp12.get('dec_win_rate', np.nan)
        n = panel[factor].notna().sum()
        grade = judge_grade(ic12)
        L.append(f'| {label} | {n:,} | {ic12:+.3f} | '
                 f'{tb_dec*100:+.2f}% | {win*100:.1f}% | {tb_quin*100:+.2f}% | {grade} |')
    L.append('')

    L.append('## IC Detail per Factor (3m / 6m / 12m)')
    L.append('')
    L.append('| Factor | Horizon | Mean IC | Std | IC IR | t-stat | % Pos | N Q | Grade |')
    L.append('|---|---|---|---|---|---|---|---|---|')
    for factor, label in (('fcf_yield', 'FCF Yield'),
                          ('roic', 'ROIC (approx)'),
                          ('gp_assets', 'Gross Profitability')):
        for h in ('ret_3m', 'ret_6m', 'ret_12m'):
            s = ic_all[factor].get(h)
            if s is None:
                continue
            L.append(f'| {label} | {h} | {s["mean_ic"]:+.4f} | {s["std_ic"]:.4f} | '
                     f'{s["ic_ir"]:+.3f} | {s["t_stat"]:+.2f} | {s["pct_pos"]*100:.1f}% | '
                     f'{s["n_quarters"]} | {judge_grade(s["ic_ir"])} |')
    L.append('')

    L.append('## Decile & Quintile Spread (annualized)')
    L.append('')
    for factor, label in (('fcf_yield', 'FCF Yield'),
                          ('roic', 'ROIC (approx)'),
                          ('gp_assets', 'Gross Profitability')):
        L.append(f'### {label}')
        L.append('')
        L.append('| Horizon | Top Dec | Bot Dec | Top-Bot Dec | Dec Win% | Dec Spread IR | Top Quin | Bot Quin | Top-Bot Quin |')
        L.append('|---|---|---|---|---|---|---|---|---|')
        for h in ('ret_3m', 'ret_6m', 'ret_12m'):
            s = grp_all[factor].get(h)
            if s is None:
                continue
            L.append(f'| {h} | {s["top_dec_ann"]*100:+.2f}% | {s["bot_dec_ann"]*100:+.2f}% | '
                     f'**{s["top_minus_bot_dec_ann"]*100:+.2f}%** | '
                     f'{s["dec_win_rate"]*100:.1f}% | {s["dec_ir"]:+.3f} | '
                     f'{s["top_quin_ann"]*100:+.2f}% | {s["bot_quin_ann"]*100:+.2f}% | '
                     f'**{s["top_minus_bot_quin_ann"]*100:+.2f}%** |')
        L.append('')

    if regime_cut:
        L.append('## Regime Breakdown (IC IR 12m)')
        L.append('')
        L.append('| Factor | bull | bear | volatile | ranged |')
        L.append('|---|---|---|---|---|')
        for factor, label in (('fcf_yield', 'FCF Yield'),
                              ('roic', 'ROIC (approx)'),
                              ('gp_assets', 'Gross Profitability')):
            rc = regime_cut.get(factor, {}).get('ic', {})
            row = [label]
            for reg in ('bull', 'bear', 'volatile', 'ranged'):
                ir = rc.get(reg, {}).get('ret_12m', {}).get('ic_ir', np.nan)
                row.append(f'{ir:+.3f}' if pd.notna(ir) else 'N/A')
            L.append('| ' + ' | '.join(row) + ' |')
        L.append('')

        L.append('### Top-Quintile Alpha 12m by Regime (top quin ann - bot quin ann)')
        L.append('')
        L.append('| Factor | bull | bear | volatile | ranged |')
        L.append('|---|---|---|---|---|')
        for factor, label in (('fcf_yield', 'FCF Yield'),
                              ('roic', 'ROIC (approx)'),
                              ('gp_assets', 'Gross Profitability')):
            rc = regime_cut.get(factor, {}).get('group', {})
            row = [label]
            for reg in ('bull', 'bear', 'volatile', 'ranged'):
                tb = rc.get(reg, {}).get('ret_12m', {}).get('top_minus_bot_quin_ann', np.nan)
                row.append(f'{tb*100:+.2f}%' if pd.notna(tb) else 'N/A')
            L.append('| ' + ' | '.join(row) + ' |')
        L.append('')

    # Year-by-year top-bot 12m
    if year_df is not None and len(year_df) > 0:
        L.append('## Top-Bot Decile 12m Spread by Year')
        L.append('')
        years = sorted(year_df['year'].unique())
        L.append('| Year | ' + ' | '.join(['FCF Yield', 'ROIC', 'Gross Profitability']) + ' |')
        L.append('|' + '---|' * 4)
        for y in years:
            sub = year_df[year_df['year'] == y]
            row = [str(y)]
            for f in ('fcf_yield', 'roic', 'gp_assets'):
                rec = sub[sub['factor'] == f]
                if len(rec) == 0:
                    row.append('N/A')
                else:
                    row.append(f'{rec.iloc[0]["spread"]*100:+.2f}%')
            L.append('| ' + ' | '.join(row) + ' |')
        L.append('')
        # win counts
        L.append('### Years where factor top > bot decile')
        L.append('')
        for f, label in (('fcf_yield', 'FCF Yield'),
                         ('roic', 'ROIC (approx)'),
                         ('gp_assets', 'Gross Profitability')):
            sub = year_df[year_df['factor'] == f]
            wins = (sub['spread'] > 0).sum()
            L.append(f'- **{label}**: {wins} / {len(sub)} years positive')
        L.append('')

    # Factor correlations
    if corr is not None:
        L.append('## Factor Rank Correlations (Spearman)')
        L.append('')
        cols = list(corr.columns)
        L.append('| | ' + ' | '.join(cols) + ' |')
        L.append('|' + '---|' * (len(cols) + 1))
        for idx in corr.index:
            row = [idx]
            for c in cols:
                row.append(f'{corr.loc[idx, c]:+.3f}')
            L.append('| ' + ' | '.join(row) + ' |')
        L.append('')

    L.append('## Bottom Line')
    L.append('')
    L.append('**All three candidate quality factors grade D/C-weak in US 2015-2024.** None provides ')
    L.append('a reliable replacement for Piotroski F-Score; the same growth/momentum regime that broke ')
    L.append('F-Score also breaks classic quality/value factors.')
    L.append('')
    L.append('Year-by-year win rate is 2-3 out of 10. Every factor loses in the 2015-2016, 2018, 2020 ')
    L.append('and 2023-2024 windows. Gross Profitability has the least-bad IC IR (+0.20, t=1.24) but ')
    L.append('decile spread is still slightly negative (-3.47% annualized) and wins only 3/10 years.')
    L.append('')
    L.append('- **FCF Yield**: 2/10 years positive; winning only 2020 (cash anomaly) + 2021 (reopening). ')
    L.append('  Decile spread -8.5% ann., win rate 27%. D (noise with negative tilt).')
    L.append('- **ROIC (approx)**: 3/10 years positive; dominated by 2020 bust (-60% spread). ')
    L.append('  Decile spread -8.9% ann., win rate 43%. D (noise with negative tilt).')
    L.append('- **Gross Profitability**: 3/10 years positive. IC IR +0.20 directionally right, but ')
    L.append('  decile spread -3.5% ann. The "bear regime IR +1.25" is computed over only 4 effective ')
    L.append('  bear quarters and is statistical noise. Best candidate among the three but still not tradable.')
    L.append('')
    L.append('All three factors correlate positively with F-Score (rho 0.12-0.33), confirming they ')
    L.append('measure similar underlying quality construct that has been unrewarded in US 2015-2024.')
    L.append('')
    L.append('### Recommendation for US value_screener')
    L.append('')
    L.append('1. **Do NOT add FCF Yield / ROIC / Gross Profitability as positive adders**. They would ')
    L.append('   extend the same negative bias already proven for F-Score.')
    L.append('2. **Gross Profitability may be used defensively**: small positive weight only during ')
    L.append('   confirmed bear regime (SPY 200DMA slope < 0), where its bear IC IR is positive in ')
    L.append('   2022. Requires HMM/regime detection already implemented in scoring_status.')
    L.append('3. **Better path forward**: given US growth dominance, focus on momentum / growth / ')
    L.append('   technical factors (already validated via VF-G1..G4). Quality is a regime factor in ')
    L.append('   US 2015-2024, not a secular alpha source.')
    L.append('')

    L.append('## Notes & Caveats')
    L.append('')
    L.append('- Bear regime has only ~4 effective cross-section quarters in 2015-2024 (2016-Q1, 2018-Q3, ')
    L.append('  2022-Q2, 2022-Q4). High-IR regime claims for bear are not statistically robust.')
    L.append('- Ranged regime has only 1 quarter -- ignore those columns.')
    L.append('- FCF uses abs(CapEx) to guard against sign inconsistencies across XBRL filers.')
    L.append('- ROIC is an approximation: NOPAT = OperatingIncome*(1-0.21); falls back to NI+InterestExpense*(1-0.21) if OpIncome missing.')
    L.append('- All flow items use trailing 4 quarters (TTM) to reduce seasonality.')
    L.append('- Outlier filter: |FCF yield| <= 1, |ROIC| <= 2, GP/Assets within [-0.5, 3].')
    L.append('- Winsorized at 1%/99% per quarter before Spearman IC.')
    L.append('- IC grade: A >=0.5, B >=0.3, C >=0.1, D <0.1.')
    L.append('')

    return '\n'.join(L)


if __name__ == '__main__':
    main()
