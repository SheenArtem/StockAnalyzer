"""
VF-Value-ex3 Mohanram 5-lite G-Score IC validation (US + TW)

目的：
  驗證 Mohanram (2005) G-Score 5-signal 簡化版作為替代品質因子，
  特別是在金融業 (Financials) 子集是否有 alpha。

  Mohanram 原版 8 signals 中，R&D/CapEx/Advertising 資料不足，
  因此只實作 5 signals（G-Lite）：
    G1 ROA > market median           (NI_ttm / TotalAssets)
    G2 CFOA > market median          (CFO_ttm / TotalAssets)
    G3 Accruals = ROA - CFOA < 0     (獲利有 cash backing)
    G4 Earnings variance < median    (5-yr quarterly ROA stdev)
    G5 Sales growth variance < median(5-yr quarterly Revenue YoY stdev)

  分數 0-5 (G_Score)。

流程（沿用 VF-Value-ex2 EDGAR IC 架構）：
  1a. 對每檔 ticker / quarter_end，用 <= q 的 financials 算 G-Score (PIT)
       - TTM 由最近 4 季加總得
       - 5yr stdev 由最近 20 季得
  1b. 每個 (ticker, q) 取 entry = q + 45d (US) / q + 75d (TW)
       — TW 財報 qtr_end + 公告延遲 45d + 我們保守再多 30d
       — 算 3m/6m/12m forward ret
  2.  每季算 Spearman IC(g_score, ret_*) + decile spread + top/bot
       針對兩組 universe：
         (a) 全市場
         (b) 金融業 subset
  3.  Regime 拆分 (bull/bear/volatile/ranged)
  4.  輸出 markdown summary

資料源：
  US: financials_us_edgar.parquet (1506 ticker x 16yr)
      Financials: universe_us.parquet sector == 'Financials' (254)
      Prices: ohlcv_us.parquet
  TW: financials_income/balance/cashflow.parquet (2385 stocks)
      Financials: industry_category 含 金融/銀行/保險/證券 bytes (56)
      Prices: ohlcv_tw.parquet

執行：
  python tools/vf_value_ex3_gscore_ic.py --market us
  python tools/vf_value_ex3_gscore_ic.py --market tw

輸出：
  reports/vf_value_ex3_gscore_ic_{market}.md
  reports/vf_value_ex3_gscore_panel_{market}.parquet
  reports/vf_value_ex3_gscore_ic_by_quarter_{market}.csv
  reports/vf_value_ex3_gscore_decile_spread_{market}.csv
"""
from __future__ import annotations

import argparse
import logging
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

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / 'reports'
OUT_DIR.mkdir(parents=True, exist_ok=True)

TODAY = pd.Timestamp('2026-04-22')

# TW Financials industry (UTF-8 substring match)
TW_FIN_KEYWORDS = [
    '金融',   # 金融
    '銀行',   # 銀行
    '保險',   # 保險
    '證券',   # 證券
]


# ---------------------------------------------------------------------------
# US EDGAR adaptor -> unified long DataFrame (ticker, date, statement, line_item, value)
# ---------------------------------------------------------------------------
BENCH_FILE_MAP = {
    'spy': '_spy_bench.parquet',
    'twii': '_twii_bench.parquet',
}


def _load_bench(regime_bench: str) -> pd.DataFrame:
    fname = BENCH_FILE_MAP.get(regime_bench)
    if fname is None:
        raise ValueError(f'Unknown regime-bench: {regime_bench!r}; '
                         f'available: {list(BENCH_FILE_MAP)}')
    path = ROOT / 'data_cache' / 'backtest' / fname
    if not path.exists():
        raise FileNotFoundError(f'Regime bench file not found: {path}')
    df = pd.read_parquet(path)
    logger.info('Loaded regime benchmark %s (%s): %d rows, %s ~ %s',
                regime_bench, fname, len(df), df.index.min(), df.index.max())
    return df


def load_us_data(regime_bench: str = 'spy'):
    fin = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'financials_us_edgar.parquet')
    fin['date'] = pd.to_datetime(fin['date'])
    fin = fin[fin['date'] <= TODAY]
    fin = fin[['ticker', 'date', 'statement', 'line_item', 'value']].copy()

    ohlcv = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'ohlcv_us.parquet',
                            columns=['ticker', 'date', 'AdjClose'])
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])

    uni = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'universe_us.parquet')
    financials_set = set(uni[uni['sector'] == 'Financials']['ticker'].tolist())

    bench = _load_bench(regime_bench)

    return {
        'fin': fin,
        'ohlcv': ohlcv,
        'financials_set': financials_set,
        'spy': bench,
        'regime_bench': regime_bench,
        'line_items': {
            'NetIncome': 'NetIncome',
            'CFO': 'CFO',
            'Revenue': 'Revenue',
            'TotalAssets': 'TotalAssets',
        },
        'entry_lag_days': 45,
    }


# ---------------------------------------------------------------------------
# TW adaptor -> unified long DataFrame
# ---------------------------------------------------------------------------
def _is_tw_financial(industry_str: str) -> bool:
    if not isinstance(industry_str, str):
        return False
    return any(kw in industry_str for kw in TW_FIN_KEYWORDS)


def load_tw_data(regime_bench: str = 'twii'):
    inc = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'financials_income.parquet')
    bal = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'financials_balance.parquet')
    cf = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'financials_cashflow.parquet')

    # Convert to unified long format (ticker, date, statement, line_item, value)
    # TW stock_id is already str. Use IncomeAfterTaxes if no NetIncome.
    def _norm(df, stmt_name, type_map):
        df = df.copy()
        df['statement'] = stmt_name
        keep_types = list(type_map.keys())
        df = df[df['type'].isin(keep_types)]
        df['line_item'] = df['type'].map(type_map)
        df = df.rename(columns={'stock_id': 'ticker'})
        df = df[['ticker', 'date', 'statement', 'line_item', 'value']]
        return df

    inc_n = _norm(inc, 'income', {
        'NetIncome': 'NetIncome',
        'IncomeAfterTaxes': 'NetIncome_fallback',   # will coalesce below
        'Revenue': 'Revenue',
    })
    bal_n = _norm(bal, 'balance', {
        'TotalAssets': 'TotalAssets',
    })
    cf_n = _norm(cf, 'cashflow', {
        'CashFlowsFromOperatingActivities': 'CFO',
    })

    fin = pd.concat([inc_n, bal_n, cf_n], ignore_index=True)
    fin['date'] = pd.to_datetime(fin['date'])
    fin = fin[fin['date'] <= TODAY]

    # Coalesce NetIncome + NetIncome_fallback per (ticker, date)
    # Prefer 'NetIncome' if present, otherwise use 'NetIncome_fallback' (IncomeAfterTaxes)
    ni = fin[fin['line_item'].isin(['NetIncome', 'NetIncome_fallback'])].copy()
    # For each (ticker, date), if NetIncome exists keep it, else use fallback
    ni_sorted = ni.sort_values(
        ['ticker', 'date', 'line_item'],
        key=lambda c: c if c.name != 'line_item' else c.map({'NetIncome': 0, 'NetIncome_fallback': 1}),
    )
    ni_first = ni_sorted.groupby(['ticker', 'date'], as_index=False).first()
    ni_first['line_item'] = 'NetIncome'
    ni_first['statement'] = 'income'

    fin = pd.concat([
        fin[~fin['line_item'].isin(['NetIncome', 'NetIncome_fallback'])],
        ni_first,
    ], ignore_index=True)

    # OHLCV - use stock_id as ticker
    ohlcv = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'ohlcv_tw.parquet',
                            columns=['stock_id', 'date', 'AdjClose'])
    ohlcv = ohlcv.rename(columns={'stock_id': 'ticker'})
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])

    # Financials subset
    uni = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'universe_tw.parquet')
    uni['is_fin'] = uni['industry_category'].apply(_is_tw_financial)
    financials_set = set(uni[uni['is_fin']]['stock_id'].astype(str).tolist())
    logger.info('TW Financials: %d stocks', len(financials_set))

    # Regime benchmark — default TWII (台灣加權指數) for TW; SPY selectable for cross-asset check.
    bench = _load_bench(regime_bench)

    return {
        'fin': fin,
        'ohlcv': ohlcv,
        'financials_set': financials_set,
        'spy': bench,
        'regime_bench': regime_bench,
        'line_items': {
            'NetIncome': 'NetIncome',
            'CFO': 'CFO',
            'Revenue': 'Revenue',
            'TotalAssets': 'TotalAssets',
        },
        'entry_lag_days': 75,   # TW 公告延遲較長
    }


# ---------------------------------------------------------------------------
# Build wide per-ticker time series from long format
# ---------------------------------------------------------------------------
def build_ticker_series(fin: pd.DataFrame, line_items: dict) -> dict:
    """Returns: {ticker: {metric: Series(date -> value)}}."""
    logger.info('Building per-ticker time series ...')
    out = {}
    # Keep only needed line_items
    needed = list(line_items.values())
    fin2 = fin[fin['line_item'].isin(needed)].copy()
    for (ticker, li), g in fin2.groupby(['ticker', 'line_item']):
        s = g.set_index('date')['value'].astype(float).sort_index()
        # Dedupe dates (keep first)
        s = s[~s.index.duplicated(keep='first')]
        out.setdefault(ticker, {})[li] = s
    return out


# ---------------------------------------------------------------------------
# Compute TTM from quarterly discrete data
# ---------------------------------------------------------------------------
def ttm_at(s: pd.Series, q_end: pd.Timestamp, min_quarters: int = 4) -> float:
    """Sum of last 4 quarters up to q_end. NaN if <4 quarters available."""
    if len(s) == 0 or not isinstance(s.index, pd.DatetimeIndex):
        return np.nan
    idx = s.index
    valid = idx[idx <= q_end]
    if len(valid) < min_quarters:
        return np.nan
    last4 = s.loc[valid].tail(min_quarters)
    if last4.isna().any():
        return np.nan
    return float(last4.sum())


def latest_point_at(s: pd.Series, q_end: pd.Timestamp) -> float:
    """Latest balance-sheet value at or before q_end."""
    if len(s) == 0 or not isinstance(s.index, pd.DatetimeIndex):
        return np.nan
    idx = s.index
    valid = idx[idx <= q_end]
    if len(valid) == 0:
        return np.nan
    v = s.loc[valid.max()]
    return float(v) if pd.notna(v) else np.nan


def rolling_std_ttm_roa(ni_series: pd.Series, assets_series: pd.Series,
                        q_end: pd.Timestamp, n_quarters: int = 20) -> float:
    """stdev of quarterly ROA (= single-Q NI / avg assets) over last n_quarters."""
    if (len(ni_series) == 0 or len(assets_series) == 0
            or not isinstance(ni_series.index, pd.DatetimeIndex)
            or not isinstance(assets_series.index, pd.DatetimeIndex)):
        return np.nan
    ni_hist = ni_series[ni_series.index <= q_end].tail(n_quarters)
    a_hist = assets_series[assets_series.index <= q_end].tail(n_quarters)
    if len(ni_hist) < 8 or len(a_hist) < 8:   # need at least 8 quarters
        return np.nan
    df = pd.DataFrame({'ni': ni_hist, 'assets': a_hist}).dropna()
    if len(df) < 8:
        return np.nan
    # ROA per quarter = NI / assets (use same-quarter assets as approximation)
    df['roa'] = df['ni'] / df['assets']
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    if len(df) < 8:
        return np.nan
    return float(df['roa'].std())


def rolling_std_rev_yoy(rev_series: pd.Series, q_end: pd.Timestamp,
                        n_quarters: int = 20) -> float:
    """stdev of quarterly Revenue YoY growth over last n_quarters."""
    if len(rev_series) == 0 or not isinstance(rev_series.index, pd.DatetimeIndex):
        return np.nan
    hist = rev_series[rev_series.index <= q_end].tail(n_quarters + 4)   # need +4 for YoY
    if len(hist) < 12:  # at least 12 quarters (4 for YoY baseline + 8 YoY points)
        return np.nan
    hist = hist.dropna()
    if len(hist) < 12:
        return np.nan
    yoy = hist.pct_change(4).dropna().tail(n_quarters)
    if len(yoy) < 8:
        return np.nan
    yoy = yoy.replace([np.inf, -np.inf], np.nan).dropna()
    if len(yoy) < 8:
        return np.nan
    return float(yoy.std())


# ---------------------------------------------------------------------------
# Build raw-value panel per (ticker, quarter_end)
# ---------------------------------------------------------------------------
def build_raw_panel(tick_data: dict, quarter_ends: list) -> pd.DataFrame:
    """For each (ticker, q), compute raw inputs: ROA, CFOA, Accruals, EarnStd, RevStd.
    Median-comparison is done cross-sectionally after this step.
    """
    rows = []
    total_tickers = len(tick_data)
    logger.info('Building raw panel for %d tickers x %d quarters',
                total_tickers, len(quarter_ends))
    for i, (ticker, metrics) in enumerate(tick_data.items(), 1):
        if i % 200 == 0:
            logger.info('  progress: %d / %d tickers', i, total_tickers)

        ni_s = metrics.get('NetIncome', pd.Series(dtype=float))
        cfo_s = metrics.get('CFO', pd.Series(dtype=float))
        rev_s = metrics.get('Revenue', pd.Series(dtype=float))
        assets_s = metrics.get('TotalAssets', pd.Series(dtype=float))

        if len(ni_s) == 0 or len(assets_s) == 0:
            continue

        _all_dates = []
        for s in (ni_s, cfo_s, rev_s, assets_s):
            if len(s) > 0 and isinstance(s.index, pd.DatetimeIndex):
                _all_dates.append(s.index.min())
        if not _all_dates:
            continue
        first_date = min(_all_dates)

        for q in quarter_ends:
            if q < first_date + pd.Timedelta(days=365):
                continue

            ni_ttm = ttm_at(ni_s, q)
            cfo_ttm = ttm_at(cfo_s, q)
            assets = latest_point_at(assets_s, q)
            if pd.isna(assets) or assets <= 0:
                continue
            if pd.isna(ni_ttm) and pd.isna(cfo_ttm):
                continue

            roa = ni_ttm / assets if pd.notna(ni_ttm) else np.nan
            cfoa = cfo_ttm / assets if pd.notna(cfo_ttm) else np.nan
            accruals = (roa - cfoa) if (pd.notna(roa) and pd.notna(cfoa)) else np.nan

            earn_std = rolling_std_ttm_roa(ni_s, assets_s, q)
            rev_std = rolling_std_rev_yoy(rev_s, q)

            rows.append({
                'ticker': ticker,
                'quarter_end': q,
                'roa': roa,
                'cfoa': cfoa,
                'accruals': accruals,
                'earn_std': earn_std,
                'rev_std': rev_std,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Compute G-Score per (ticker, quarter_end) using cross-section medians
# ---------------------------------------------------------------------------
def assign_gscore(raw: pd.DataFrame, sector_filter: set | None = None,
                  sector_label: str = 'all') -> pd.DataFrame:
    """Cross-section (per quarter) median-based G-Score.

    If sector_filter provided, medians are computed *within that sector*
    (relative quality benchmark).
    """
    if sector_filter is not None:
        sub = raw[raw['ticker'].isin(sector_filter)].copy()
    else:
        sub = raw.copy()

    if len(sub) == 0:
        return pd.DataFrame()

    rows = []
    for q, g in sub.groupby('quarter_end'):
        # Per-quarter medians (drop NaN)
        med_roa = g['roa'].dropna().median()
        med_cfoa = g['cfoa'].dropna().median()
        med_earn_std = g['earn_std'].dropna().median()
        med_rev_std = g['rev_std'].dropna().median()

        for r in g.itertuples(index=False):
            g1 = int(pd.notna(r.roa) and pd.notna(med_roa) and r.roa > med_roa)
            g2 = int(pd.notna(r.cfoa) and pd.notna(med_cfoa) and r.cfoa > med_cfoa)
            g3 = int(pd.notna(r.accruals) and r.accruals < 0)
            g4 = int(pd.notna(r.earn_std) and pd.notna(med_earn_std) and r.earn_std < med_earn_std)
            g5 = int(pd.notna(r.rev_std) and pd.notna(med_rev_std) and r.rev_std < med_rev_std)

            # Only keep if we could evaluate at least 3 signals (non-NaN inputs)
            n_eval = (
                (pd.notna(r.roa) and pd.notna(med_roa)) +
                (pd.notna(r.cfoa) and pd.notna(med_cfoa)) +
                (pd.notna(r.accruals)) +
                (pd.notna(r.earn_std) and pd.notna(med_earn_std)) +
                (pd.notna(r.rev_std) and pd.notna(med_rev_std))
            )
            if n_eval < 3:
                continue

            g_score = g1 + g2 + g3 + g4 + g5
            rows.append({
                'ticker': r.ticker,
                'quarter_end': q,
                'g_score': g_score,
                'g1_roa': g1,
                'g2_cfoa': g2,
                'g3_accruals': g3,
                'g4_earn_std': g4,
                'g5_rev_std': g5,
                'n_eval': n_eval,
                'sector_label': sector_label,
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Forward return join
# ---------------------------------------------------------------------------
def compute_forward_returns(panel: pd.DataFrame, ohlcv: pd.DataFrame,
                            entry_lag_days: int) -> pd.DataFrame:
    ohlcv = ohlcv.sort_values(['ticker', 'date']).copy()
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])

    logger.info('Building price lookups ...')
    ticker_prices = {}
    for ticker, g in ohlcv.groupby('ticker'):
        s = g.set_index('date')['AdjClose'].astype(float)
        s = s[~s.index.duplicated(keep='first')]
        ticker_prices[ticker] = s

    def _px_on_or_after(s, target):
        valid = s.index[s.index >= target]
        if len(valid) == 0:
            return None
        d0 = valid.min()
        return d0, float(s.loc[d0])

    def _px_after_td(s, start, n):
        idx = s.index
        try:
            pos = idx.get_indexer([start])[0]
        except Exception:
            return None
        if pos == -1:
            return None
        tgt = pos + n
        if tgt >= len(idx):
            return None
        return float(s.iloc[tgt])

    rows = []
    total = len(panel)
    logger.info('Computing forward returns for %d obs', total)
    for i, row in enumerate(panel.itertuples(index=False), 1):
        if i % 10000 == 0:
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
        p3 = _px_after_td(s, entry_date, 63)
        p6 = _px_after_td(s, entry_date, 126)
        p12 = _px_after_td(s, entry_date, 252)
        rows.append({
            'ticker': ticker,
            'quarter_end': q,
            'entry_date': entry_date,
            'g_score': row.g_score,
            'g1_roa': row.g1_roa,
            'g2_cfoa': row.g2_cfoa,
            'g3_accruals': row.g3_accruals,
            'g4_earn_std': row.g4_earn_std,
            'g5_rev_std': row.g5_rev_std,
            'sector_label': row.sector_label,
            'ret_3m': (p3 / entry_px - 1.0) if p3 is not None else np.nan,
            'ret_6m': (p6 / entry_px - 1.0) if p6 is not None else np.nan,
            'ret_12m': (p12 / entry_px - 1.0) if p12 is not None else np.nan,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# IC / Groups
# ---------------------------------------------------------------------------
def per_quarter_ic(panel: pd.DataFrame, min_obs: int = 15) -> pd.DataFrame:
    rows = []
    for q, g in panel.groupby('quarter_end'):
        g = g.dropna(subset=['g_score'])
        if len(g) < min_obs:
            continue
        row = {'quarter_end': q, 'n': len(g)}
        for h in ('ret_3m', 'ret_6m', 'ret_12m'):
            sub = g.dropna(subset=[h])
            if len(sub) < min_obs or sub['g_score'].nunique() < 3:
                row[f'ic_{h}'] = np.nan
                continue
            ic, _ = spearmanr(sub['g_score'], sub[h])
            row[f'ic_{h}'] = ic
        rows.append(row)
    return pd.DataFrame(rows).sort_values('quarter_end')


def per_quarter_groups(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for q, g in panel.groupby('quarter_end'):
        row = {'quarter_end': q, 'n_total': len(g)}
        for h in ('ret_3m', 'ret_6m', 'ret_12m'):
            sub = g.dropna(subset=[h, 'g_score'])
            if len(sub) == 0:
                continue
            top = sub[sub['g_score'] >= 4][h].mean()
            bot = sub[sub['g_score'] <= 1][h].mean()
            row[f'top_{h}'] = top
            row[f'bot_{h}'] = bot
            row[f'n_top_{h}'] = (sub['g_score'] >= 4).sum()
            row[f'n_bot_{h}'] = (sub['g_score'] <= 1).sum()
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
    reg = regime_df.reset_index()
    reg.columns = ['date'] + list(reg.columns[1:])
    reg['date'] = pd.to_datetime(reg['date'])
    reg = reg.sort_values('date')
    panel = panel.sort_values('entry_date')
    merged = pd.merge_asof(panel, reg[['date', 'regime']],
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


def annualize_q(ser: pd.Series, months: int) -> float:
    if len(ser) == 0:
        return np.nan
    m = ser.mean()
    return (1 + m) ** (12 / months) - 1


def summarize_groups(group_df: pd.DataFrame) -> dict:
    out = {}
    for h, m in (('ret_3m', 3), ('ret_6m', 6), ('ret_12m', 12)):
        if f'top_{h}' not in group_df.columns:
            continue
        top = group_df[f'top_{h}'].dropna()
        bot = group_df[f'bot_{h}'].dropna()
        out[h] = {
            'top_ann': annualize_q(top, m),
            'bot_ann': annualize_q(bot, m),
            'spread_ann': annualize_q(top, m) - annualize_q(bot, m),
            'n_q_top': len(top),
            'n_q_bot': len(bot),
        }
    return out


def judge_grade(ir: float) -> str:
    if pd.isna(ir):
        return 'N/A'
    a = abs(ir)
    if a >= 0.5:
        return 'A (strong)'
    if a >= 0.3:
        return 'B (tradable)'
    if a >= 0.1:
        return 'C (weak)'
    return 'D (noise)'


# ---------------------------------------------------------------------------
# Run one validation track (all-market or financials)
# ---------------------------------------------------------------------------
def run_track(raw_panel: pd.DataFrame, ohlcv: pd.DataFrame,
              entry_lag_days: int, spy_df: pd.DataFrame,
              sector_filter: set | None, sector_label: str):
    logger.info('=== Track: %s (filter=%s) ===',
                sector_label,
                'none' if sector_filter is None else f'{len(sector_filter)} tickers')

    g_panel = assign_gscore(raw_panel, sector_filter=sector_filter, sector_label=sector_label)
    if len(g_panel) == 0:
        logger.warning('No panel rows for %s — skipping', sector_label)
        return None

    logger.info('  G-Score distribution:')
    dist = g_panel['g_score'].value_counts().sort_index()
    for gs, c in dist.items():
        logger.info('    G=%d: %d (%.1f%%)', gs, c, c / len(g_panel) * 100)

    panel_ret = compute_forward_returns(g_panel, ohlcv, entry_lag_days=entry_lag_days)
    logger.info('  Panel w/ returns: %d rows', len(panel_ret))

    ic_df = per_quarter_ic(panel_ret)
    grp_df = per_quarter_groups(panel_ret)
    ic_s = summarize_ic(ic_df)
    grp_s = summarize_groups(grp_df)

    regime_df = classify_regime(spy_df)
    panel_r = attach_regime(panel_ret, regime_df)
    regime_cut = {}
    for reg, g in panel_r.groupby('regime'):
        if len(g) < 50:
            continue
        ic_r = per_quarter_ic(g, min_obs=10)
        grp_r = per_quarter_groups(g)
        regime_cut[reg] = {
            'ic': summarize_ic(ic_r),
            'group': summarize_groups(grp_r),
            'n_obs': len(g),
        }

    return {
        'panel': panel_ret,
        'g_panel': g_panel,
        'ic_df': ic_df,
        'grp_df': grp_df,
        'ic_summary': ic_s,
        'grp_summary': grp_s,
        'regime_cut': regime_cut,
        'sector_label': sector_label,
    }


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------
def format_report(market: str, track_all, track_fin, regime_bench: str = 'spy') -> str:
    lines = []
    lines.append(f'# VF-Value-ex3 Mohanram G-Score IC Validation — {market.upper()} '
                 f'(regime={regime_bench.upper()})')
    lines.append('')
    lines.append(f'Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}')
    lines.append('')
    lines.append('5-signal G-Lite: ROA / CFOA / Accruals / EarnStd / RevStd '
                 '(vs per-quarter cross-section median within track)')
    lines.append('')
    lines.append(f'Regime benchmark: **{regime_bench.upper()}** '
                 f'(MA200 slope + 20d realized vol, threshold 25%)')
    lines.append('')
    lines.append('Grade: A (|IR|>=0.5) / B (|IR|>=0.3) / C (|IR|>=0.1) / D (noise)')
    lines.append('')

    for track, title in ((track_all, 'All Market'), (track_fin, 'Financials Subset')):
        lines.append(f'## {title}')
        lines.append('')
        if track is None:
            lines.append('_No data_')
            lines.append('')
            continue

        panel = track['panel']
        g_panel = track['g_panel']
        lines.append(f'- Sample: {len(panel):,} (ticker, quarter) obs after price join, '
                     f'{g_panel["ticker"].nunique()} tickers, '
                     f'{g_panel["quarter_end"].nunique()} quarters')
        lines.append(f'- Quarter range: {panel["quarter_end"].min():%Y-%m-%d} ~ '
                     f'{panel["quarter_end"].max():%Y-%m-%d}')
        lines.append('')

        # Distribution
        lines.append('### G-Score distribution')
        lines.append('')
        lines.append('| G | Count | Pct |')
        lines.append('|---|---|---|')
        dist = g_panel['g_score'].value_counts().sort_index()
        total = dist.sum()
        for gs, c in dist.items():
            lines.append(f'| {gs} | {c:,} | {c/total*100:.1f}% |')
        lines.append('')

        # IC summary
        lines.append('### IC Summary (Spearman, g_score vs forward return)')
        lines.append('')
        lines.append('| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Q | Grade |')
        lines.append('|---|---|---|---|---|---|---|---|')
        for h, s in track['ic_summary'].items():
            lines.append(
                f'| {h} | {s["mean_ic"]:+.4f} | {s["std_ic"]:.4f} | {s["ic_ir"]:+.3f} '
                f'| {s["t_stat"]:+.2f} | {s["pct_positive"]*100:.1f}% | {s["n_quarters"]} | '
                f'{judge_grade(s["ic_ir"])} |'
            )
        lines.append('')

        # Group returns
        lines.append('### Top (G>=4) vs Bot (G<=1) annualized returns')
        lines.append('')
        lines.append('| Horizon | Top Ann | Bot Ann | Spread | N Q top | N Q bot |')
        lines.append('|---|---|---|---|---|---|')
        for h, s in track['grp_summary'].items():
            lines.append(
                f'| {h} | {s["top_ann"]*100:+.2f}% | {s["bot_ann"]*100:+.2f}% | '
                f'**{s["spread_ann"]*100:+.2f}%** | {s["n_q_top"]} | {s["n_q_bot"]} |'
            )
        lines.append('')

        # Regime
        if track['regime_cut']:
            lines.append('### By Regime (IC IR 6m / Top-Bot spread ann)')
            lines.append('')
            lines.append('| Regime | N obs | IC IR 6m | Top ann | Bot ann | Spread |')
            lines.append('|---|---|---|---|---|---|')
            for reg, d in track['regime_cut'].items():
                ir6 = d['ic'].get('ret_6m', {}).get('ic_ir', np.nan)
                g6 = d['group'].get('ret_6m', {})
                top = g6.get('top_ann', np.nan)
                bot = g6.get('bot_ann', np.nan)
                sp = g6.get('spread_ann', np.nan)
                def _fmt(x):
                    return f'{x*100:+.2f}%' if pd.notna(x) else 'n/a'
                lines.append(f'| {reg} | {d["n_obs"]:,} | {ir6:+.3f} | '
                             f'{_fmt(top)} | {_fmt(bot)} | **{_fmt(sp)}** |')
            lines.append('')

        # Best horizon conclusion
        if track['ic_summary']:
            best = max(track['ic_summary'].items(), key=lambda x: abs(x[1]['ic_ir']))
            best_h, best_s = best
            lines.append(f'**Best horizon**: {best_h} IR={best_s["ic_ir"]:+.3f} '
                         f'({judge_grade(best_s["ic_ir"])})')
            g = track['grp_summary'].get(best_h, {})
            if g:
                lines.append(f'- Top(G>=4) vs Bot(G<=1) annualized spread: '
                             f'**{g["spread_ann"]*100:+.2f}%**')
            lines.append('')

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(market: str, regime_bench: str | None = None, out_suffix: str = ''):
    # Default regime-bench by market: US -> SPY, TW -> TWII
    if regime_bench is None:
        regime_bench = 'spy' if market == 'us' else 'twii'
    logger.info('=== VF-Value-ex3 G-Score IC (%s, regime=%s) ===',
                market.upper(), regime_bench.upper())

    if market == 'us':
        data = load_us_data(regime_bench=regime_bench)
        quarter_ends = pd.date_range('2010-12-31', '2024-12-31', freq='QE')
    elif market == 'tw':
        data = load_tw_data(regime_bench=regime_bench)
        quarter_ends = pd.date_range('2016-12-31', '2024-12-31', freq='QE')
    else:
        raise ValueError(f'Unknown market: {market}')

    logger.info('Quarter range: %s ~ %s (n=%d)',
                quarter_ends.min(), quarter_ends.max(), len(quarter_ends))
    logger.info('Financials subset: %d tickers', len(data['financials_set']))

    tick_data = build_ticker_series(data['fin'], data['line_items'])
    logger.info('Built time series for %d tickers', len(tick_data))

    logger.info('--- Computing raw panel (ROA/CFOA/Accruals/Stds) ---')
    raw = build_raw_panel(tick_data, list(quarter_ends))
    logger.info('Raw panel: %d rows, %d tickers', len(raw), raw['ticker'].nunique())
    logger.info('Non-null counts: roa=%d cfoa=%d accruals=%d earn_std=%d rev_std=%d',
                raw['roa'].notna().sum(), raw['cfoa'].notna().sum(),
                raw['accruals'].notna().sum(),
                raw['earn_std'].notna().sum(), raw['rev_std'].notna().sum())

    # Track 1: all-market
    track_all = run_track(
        raw, data['ohlcv'], data['entry_lag_days'], data['spy'],
        sector_filter=None, sector_label='all',
    )

    # Track 2: financials-only (medians computed within financials)
    track_fin = run_track(
        raw, data['ohlcv'], data['entry_lag_days'], data['spy'],
        sector_filter=data['financials_set'], sector_label='financials',
    )

    # Save artifacts
    sfx = out_suffix or ''
    if track_all is not None:
        track_all['panel'].to_parquet(
            OUT_DIR / f'vf_value_ex3_gscore_panel_all_{market}{sfx}.parquet', index=False)
        track_all['ic_df'].to_csv(
            OUT_DIR / f'vf_value_ex3_gscore_ic_by_quarter_all_{market}{sfx}.csv', index=False)
        track_all['grp_df'].to_csv(
            OUT_DIR / f'vf_value_ex3_gscore_decile_spread_all_{market}{sfx}.csv', index=False)

    if track_fin is not None:
        track_fin['panel'].to_parquet(
            OUT_DIR / f'vf_value_ex3_gscore_panel_fin_{market}{sfx}.parquet', index=False)
        track_fin['ic_df'].to_csv(
            OUT_DIR / f'vf_value_ex3_gscore_ic_by_quarter_fin_{market}{sfx}.csv', index=False)
        track_fin['grp_df'].to_csv(
            OUT_DIR / f'vf_value_ex3_gscore_decile_spread_fin_{market}{sfx}.csv', index=False)

    md = format_report(market, track_all, track_fin, regime_bench=regime_bench)
    md_path = OUT_DIR / f'vf_value_ex3_gscore_ic_{market}{sfx}.md'
    md_path.write_text(md, encoding='utf-8')
    logger.info('Saved summary -> %s', md_path)
    print()
    print('=' * 80)
    print(md)
    print('=' * 80)

    return track_all, track_fin


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--market', choices=['us', 'tw'], required=True)
    ap.add_argument('--regime-bench', choices=['spy', 'twii'], default=None,
                    help='Regime benchmark (default: spy for US, twii for TW)')
    ap.add_argument('--out-suffix', default='',
                    help='Output file suffix (e.g. "_twii" or "_spy"); default empty '
                         'to preserve original filenames')
    args = ap.parse_args()
    main(args.market, regime_bench=args.regime_bench, out_suffix=args.out_suffix)
