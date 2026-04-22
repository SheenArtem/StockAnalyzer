"""
VF-Turnover IC Validation — 周轉率因子驗證 (QM + Value 雙池)

目的：
  驗證 turnover rate (周轉率) 作為 alpha 因子，並檢查是否與既有 RVOL_20 重疊。

Factor 定義：
  turnover_20d = mean(Volume over last 20 trading days) / Shares_Outstanding * 100
  turnover_5d  = mean(Volume over last 5  trading days) / Shares_Outstanding * 100

Shares Outstanding PIT：
  財報 balance.OrdinaryShare 是股本 TWD。流通股數 = OrdinaryShare / 10（台股面額 10 TWD）。
  quarterly 更新；用 merge_asof (direction='backward') 作 PIT join，加 45 天公告延遲。

兩個 universe：
  A) QM 池：trade_journal_qm_tw_mixed.parquet (4923 trades, 205 stocks, 2015-2025)
  B) Value 池：trade_journal_value_tw_snapshot.parquet (70760 rows, 857 stocks, 2020-2025)

決策規則：
  |IR| >= 0.5 A / 0.3 B / 0.1 C / <0.1 D
  橫截面 top-bot spread 年化 > 3% 才值得落地
  與 RVOL Spearman rank corr > 0.7 視為重疊，拒絕落地

執行：
  python tools/vf_turnover_ic.py --pool qm
  python tools/vf_turnover_ic.py --pool value
  python tools/vf_turnover_ic.py --pool both
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

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / 'reports'
OUT_DIR.mkdir(parents=True, exist_ok=True)

TODAY = pd.Timestamp('2026-04-22')
TW_FIN_LAG_DAYS = 45   # 台股財報公告延遲


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_ohlcv() -> pd.DataFrame:
    logger.info('Loading ohlcv_tw.parquet ...')
    df = pd.read_parquet(
        ROOT / 'data_cache' / 'backtest' / 'ohlcv_tw.parquet',
        columns=['stock_id', 'date', 'Volume', 'Close'],
    )
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)
    logger.info('  OHLCV: %d rows, %d stocks', len(df), df['stock_id'].nunique())
    return df


def load_shares_outstanding() -> pd.DataFrame:
    """Load OrdinaryShare (in TWD) from balance sheet, PIT-ready with 45d lag."""
    logger.info('Loading OrdinaryShare PIT ...')
    bal = pd.read_parquet(ROOT / 'data_cache' / 'backtest' / 'financials_balance.parquet')
    os_df = bal[bal['type'] == 'OrdinaryShare'][['stock_id', 'date', 'value']].copy()
    os_df.columns = ['stock_id', 'fin_date', 'ordinary_share']
    os_df['fin_date'] = pd.to_datetime(os_df['fin_date'])
    # TW 公告延遲 45 天
    os_df['avail_date'] = os_df['fin_date'] + pd.Timedelta(days=TW_FIN_LAG_DAYS)
    # 流通股數 = OrdinaryShare / 10（面額 10 TWD）
    os_df['shares_out'] = os_df['ordinary_share'] / 10
    os_df = os_df[os_df['shares_out'] > 0]
    os_df = os_df.sort_values(['stock_id', 'avail_date']).reset_index(drop=True)
    logger.info('  OrdinaryShare: %d rows, %d stocks', len(os_df), os_df['stock_id'].nunique())
    return os_df[['stock_id', 'avail_date', 'shares_out']]


def load_qm_journal() -> pd.DataFrame:
    path = ROOT / 'data_cache' / 'backtest' / 'trade_journal_qm_tw_mixed.parquet'
    df = pd.read_parquet(path)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    logger.info('QM journal: %d rows, %d stocks, %d weeks',
                len(df), df['stock_id'].nunique(), df['week_end_date'].nunique())
    return df


def load_value_journal() -> pd.DataFrame:
    path = ROOT / 'data_cache' / 'backtest' / 'trade_journal_value_tw_snapshot.parquet'
    df = pd.read_parquet(path)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    logger.info('Value journal: %d rows, %d stocks, %d weeks',
                len(df), df['stock_id'].nunique(), df['week_end_date'].nunique())
    return df


def load_twii_regime() -> pd.DataFrame:
    path = ROOT / 'data_cache' / 'backtest' / '_twii_bench.parquet'
    df = pd.read_parquet(path)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    close = df['Adj Close'] if 'Adj Close' in df.columns else df['Close']
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
    out = pd.DataFrame({'close': close, 'regime': regime})
    out.index = pd.to_datetime(out.index)
    return out


# ---------------------------------------------------------------------------
# Turnover computation per (stock_id, week_end_date)
# ---------------------------------------------------------------------------
def compute_turnover_per_key(
    keys: pd.DataFrame,
    ohlcv: pd.DataFrame,
    shares_df: pd.DataFrame,
    windows: tuple[int, ...] = (5, 20),
) -> pd.DataFrame:
    """For each (stock_id, week_end_date), compute turnover_{w}d and RVOL.

    turnover_{w}d = mean(Vol over last w trading days ending <= week_end_date) / shares_out * 100
    rvol_{w}d = mean(Vol last w) / mean(Vol prior 60 TD)  (check vs Value journal rvol_20)
    """
    logger.info('Computing turnover for %d (stock, week) keys ...', len(keys))
    # pre-index ohlcv per stock for efficient trailing window
    keys = keys[['stock_id', 'week_end_date']].drop_duplicates().copy()
    keys = keys.sort_values(['stock_id', 'week_end_date']).reset_index(drop=True)

    # Build per-stock Volume Series
    stock_vol: dict[str, pd.Series] = {}
    for sid, g in ohlcv.groupby('stock_id', sort=False):
        s = g.set_index('date')['Volume'].astype(float).sort_index()
        s = s[~s.index.duplicated(keep='first')]
        stock_vol[sid] = s

    max_win = max(max(windows), 60)
    rows = []
    for sid, g in keys.groupby('stock_id', sort=False):
        s = stock_vol.get(sid)
        if s is None or len(s) == 0:
            continue
        td_idx = s.index
        for week_end in g['week_end_date']:
            # Positional: last trading day with date <= week_end
            pos = td_idx.searchsorted(week_end, side='right') - 1
            if pos < max_win - 1:
                continue
            row = {'stock_id': sid, 'week_end_date': week_end}
            for w in windows:
                start = pos - w + 1
                if start < 0:
                    row[f'avg_vol_{w}d'] = np.nan
                    continue
                window_vol = s.iloc[start:pos + 1]
                if len(window_vol) < w or window_vol.isna().any():
                    row[f'avg_vol_{w}d'] = np.nan
                else:
                    row[f'avg_vol_{w}d'] = float(window_vol.mean())
            # RVOL_20 = mean(last 20) / mean(prior 60)
            end_pos = pos
            last20 = s.iloc[end_pos - 19:end_pos + 1]
            prior60_start = end_pos - 79
            prior60_end = end_pos - 19
            if prior60_start >= 0:
                prior60 = s.iloc[prior60_start:prior60_end]
                if len(last20) == 20 and len(prior60) == 60 and prior60.mean() > 0:
                    row['rvol_20_calc'] = float(last20.mean() / prior60.mean())
                else:
                    row['rvol_20_calc'] = np.nan
            else:
                row['rvol_20_calc'] = np.nan
            rows.append(row)

    out = pd.DataFrame(rows)

    # Merge shares_out (asof backward on avail_date)
    out = out.sort_values('week_end_date').reset_index(drop=True)
    shares_df = shares_df.sort_values('avail_date').reset_index(drop=True)
    merged = pd.merge_asof(
        out,
        shares_df,
        left_on='week_end_date',
        right_on='avail_date',
        by='stock_id',
        direction='backward',
    )
    for w in windows:
        merged[f'turnover_{w}d'] = merged[f'avg_vol_{w}d'] / merged['shares_out'] * 100
    return merged


# ---------------------------------------------------------------------------
# IC / decile / regime
# ---------------------------------------------------------------------------
def per_week_ic(panel: pd.DataFrame, factor: str, ret_cols: list[str],
                min_obs: int = 15) -> pd.DataFrame:
    rows = []
    for wk, g in panel.groupby('week_end_date'):
        g = g.dropna(subset=[factor])
        if len(g) < min_obs:
            continue
        row = {'week_end_date': wk, 'n': len(g)}
        for h in ret_cols:
            sub = g.dropna(subset=[h])
            if len(sub) < min_obs or sub[factor].nunique() < 5:
                row[f'ic_{h}'] = np.nan
                continue
            ic, _ = spearmanr(sub[factor], sub[h])
            row[f'ic_{h}'] = ic
        rows.append(row)
    return pd.DataFrame(rows).sort_values('week_end_date')


def decile_spread(panel: pd.DataFrame, factor: str, ret_cols: list[str],
                  min_obs_per_week: int = 30, n_bins: int = 10) -> pd.DataFrame:
    """Per-week: rank into n_bins quantiles, compute mean return per bin, then time-avg.

    n_bins=10 for broad universe; n_bins=5 for narrow (e.g., QM top-50).
    """
    dec_rows = []
    for wk, g in panel.groupby('week_end_date'):
        g = g.dropna(subset=[factor])
        if len(g) < min_obs_per_week:
            continue
        try:
            g = g.assign(decile=pd.qcut(g[factor], n_bins, labels=False, duplicates='drop'))
        except ValueError:
            continue
        for h in ret_cols:
            for d in range(n_bins):
                sub = g[(g['decile'] == d) & g[h].notna()]
                if len(sub) == 0:
                    continue
                dec_rows.append({
                    'week_end_date': wk,
                    'horizon': h,
                    'decile': d,
                    'ret_mean': sub[h].mean(),
                    'n': len(sub),
                })
    dec_df = pd.DataFrame(dec_rows)
    if len(dec_df) == 0:
        return pd.DataFrame(columns=['horizon', 'decile', 'ret_mean', 'n_weeks', 'n_stocks_total'])
    agg = dec_df.groupby(['horizon', 'decile']).agg(
        ret_mean=('ret_mean', 'mean'),
        n_weeks=('ret_mean', 'count'),
        n_stocks_total=('n', 'sum'),
    ).reset_index()
    return agg


def annualize_weekly_from_horizon(ret_mean: float, horizon_days: int) -> float:
    """Convert per-period return (horizon_days forward) to annualized."""
    if pd.isna(ret_mean):
        return np.nan
    n_per_year = 252 / horizon_days
    return (1 + ret_mean) ** n_per_year - 1


def summarize_ic(ic_df: pd.DataFrame, ret_cols: list[str]) -> dict:
    out = {}
    for h in ret_cols:
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
            'n_weeks': len(s),
            'pct_positive': (s > 0).mean(),
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


def attach_regime(panel: pd.DataFrame, regime_df: pd.DataFrame) -> pd.DataFrame:
    """Attach TWII-based regime. If panel already has 'regime', rename to avoid clash."""
    out = panel.copy()
    out['week_end_date'] = pd.to_datetime(out['week_end_date'])
    # If panel already has 'regime' (e.g. QM journal), rename it aside
    if 'regime' in out.columns:
        out = out.rename(columns={'regime': 'regime_orig'})
    reg = regime_df[['regime']].copy()
    reg.index = pd.to_datetime(reg.index)
    reg = reg.sort_index().reset_index()
    # After reset_index, the datetime index becomes a column (name varies). Rename to 'date'.
    first_col = reg.columns[0]
    reg = reg.rename(columns={first_col: 'date'})
    reg['date'] = pd.to_datetime(reg['date'])
    reg = reg.sort_values('date')
    out = out.sort_values('week_end_date')
    merged = pd.merge_asof(
        out, reg[['date', 'regime']],
        left_on='week_end_date', right_on='date',
        direction='backward',
    )
    return merged


# ---------------------------------------------------------------------------
# Correlation with RVOL
# ---------------------------------------------------------------------------
def rvol_corr_check(panel: pd.DataFrame, factor: str, rvol_col: str) -> dict:
    sub = panel.dropna(subset=[factor, rvol_col])
    if len(sub) < 100:
        return {'n': len(sub), 'rho_spearman': np.nan, 'rho_pearson': np.nan}
    rho_s, _ = spearmanr(sub[factor], sub[rvol_col])
    rho_p = sub[[factor, rvol_col]].corr().iloc[0, 1]
    return {'n': len(sub), 'rho_spearman': float(rho_s), 'rho_pearson': float(rho_p)}


# ---------------------------------------------------------------------------
# QM pool runner
# ---------------------------------------------------------------------------
def run_qm_pool(ohlcv: pd.DataFrame, shares_df: pd.DataFrame,
                regime_df: pd.DataFrame) -> dict:
    logger.info('=== QM pool validation ===')
    qm = load_qm_journal()
    keys = qm[['stock_id', 'week_end_date']].drop_duplicates()
    turn = compute_turnover_per_key(keys, ohlcv, shares_df)

    panel = qm.merge(turn, on=['stock_id', 'week_end_date'], how='left')
    coverage = panel['turnover_20d'].notna().sum()
    logger.info('  Panel: %d rows, turnover_20d coverage: %d (%.1f%%)',
                len(panel), coverage, coverage / len(panel) * 100)
    logger.info('  turnover_20d stats: mean=%.3f%% median=%.3f%% p95=%.3f%%',
                panel['turnover_20d'].mean(),
                panel['turnover_20d'].median(),
                panel['turnover_20d'].quantile(0.95))

    ret_cols = ['fwd_5d', 'fwd_10d', 'fwd_20d', 'fwd_40d', 'fwd_60d']

    results = {}
    # QM weekly cohort median ~9 stocks -> use quintile (n_bins=5) with min 5/week
    for factor in ('turnover_5d', 'turnover_20d'):
        ic_df = per_week_ic(panel, factor, ret_cols, min_obs=5)
        dec = decile_spread(panel, factor, ret_cols, min_obs_per_week=5, n_bins=5)
        ic_s = summarize_ic(ic_df, ret_cols)
        results[factor] = {'ic_df': ic_df, 'decile': dec, 'ic_summary': ic_s, 'n_bins': 5}

    # Regime cut on turnover_20d
    panel_r = attach_regime(panel, regime_df)
    regime_cut = {}
    for reg_name, g in panel_r.groupby('regime'):
        if len(g.dropna(subset=['turnover_20d'])) < 100:
            continue
        ic_r = per_week_ic(g, 'turnover_20d', ret_cols, min_obs=5)
        regime_cut[reg_name] = {
            'ic': summarize_ic(ic_r, ret_cols),
            'n_obs': len(g),
        }

    # RVOL overlap check — use locally-computed rvol_20_calc (panel doesn't have rvol column for QM)
    rvol_chk = {
        'turnover_20d_vs_rvol_20_calc': rvol_corr_check(panel, 'turnover_20d', 'rvol_20_calc'),
        'turnover_5d_vs_rvol_20_calc': rvol_corr_check(panel, 'turnover_5d', 'rvol_20_calc'),
    }

    return {
        'panel': panel,
        'results': results,
        'regime_cut': regime_cut,
        'rvol_corr': rvol_chk,
    }


# ---------------------------------------------------------------------------
# Value pool runner
# ---------------------------------------------------------------------------
def run_value_pool(ohlcv: pd.DataFrame, shares_df: pd.DataFrame,
                   regime_df: pd.DataFrame) -> dict:
    logger.info('=== Value pool validation ===')
    val = load_value_journal()
    keys = val[['stock_id', 'week_end_date']].drop_duplicates()
    turn = compute_turnover_per_key(keys, ohlcv, shares_df)

    panel = val.merge(turn, on=['stock_id', 'week_end_date'], how='left')
    coverage = panel['turnover_20d'].notna().sum()
    logger.info('  Panel: %d rows, turnover_20d coverage: %d (%.1f%%)',
                len(panel), coverage, coverage / len(panel) * 100)
    logger.info('  turnover_20d stats: mean=%.3f%% median=%.3f%% p95=%.3f%%',
                panel['turnover_20d'].mean(),
                panel['turnover_20d'].median(),
                panel['turnover_20d'].quantile(0.95))

    ret_cols = ['fwd_5d', 'fwd_10d', 'fwd_20d', 'fwd_40d', 'fwd_60d']

    results = {}
    for factor in ('turnover_5d', 'turnover_20d'):
        ic_df = per_week_ic(panel, factor, ret_cols, min_obs=30)
        dec = decile_spread(panel, factor, ret_cols, min_obs_per_week=50)
        ic_s = summarize_ic(ic_df, ret_cols)
        results[factor] = {'ic_df': ic_df, 'decile': dec, 'ic_summary': ic_s}

    # Regime cut on turnover_20d
    panel_r = attach_regime(panel, regime_df)
    regime_cut = {}
    for reg_name, g in panel_r.groupby('regime'):
        if len(g.dropna(subset=['turnover_20d'])) < 500:
            continue
        ic_r = per_week_ic(g, 'turnover_20d', ret_cols, min_obs=20)
        regime_cut[reg_name] = {
            'ic': summarize_ic(ic_r, ret_cols),
            'n_obs': len(g),
        }

    # RVOL overlap — use native rvol_20 from Value journal + our rvol_20_calc
    rvol_chk = {
        'turnover_20d_vs_rvol_20': rvol_corr_check(panel, 'turnover_20d', 'rvol_20'),
        'turnover_5d_vs_rvol_20': rvol_corr_check(panel, 'turnover_5d', 'rvol_20'),
        'turnover_20d_vs_rvol_20_calc': rvol_corr_check(panel, 'turnover_20d', 'rvol_20_calc'),
    }

    return {
        'panel': panel,
        'results': results,
        'regime_cut': regime_cut,
        'rvol_corr': rvol_chk,
    }


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------
HORIZON_DAYS = {'fwd_5d': 5, 'fwd_10d': 10, 'fwd_20d': 20, 'fwd_40d': 40, 'fwd_60d': 60}


def fmt_ic_table(ic_summary: dict) -> list[str]:
    lines = []
    lines.append('| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Weeks | Grade |')
    lines.append('|---|---|---|---|---|---|---|---|')
    for h, s in ic_summary.items():
        lines.append(
            f'| {h} | {s["mean_ic"]:+.4f} | {s["std_ic"]:.4f} | {s["ic_ir"]:+.3f} '
            f'| {s["t_stat"]:+.2f} | {s["pct_positive"]*100:.1f}% | {s["n_weeks"]} | '
            f'{judge_grade(s["ic_ir"])} |'
        )
    return lines


def fmt_decile_table(dec: pd.DataFrame, horizon: str, bin_label: str = 'D') -> list[str]:
    lines = []
    if 'horizon' not in dec.columns or len(dec) == 0:
        lines.append(f'_No decile data for {horizon}_')
        return lines
    sub = dec[dec['horizon'] == horizon].sort_values('decile')
    if len(sub) == 0:
        lines.append(f'_No decile data for {horizon}_')
        return lines
    hd = HORIZON_DAYS.get(horizon, 20)
    # bins: 5 (quintile) or 10 (decile)
    n_bins = int(sub['decile'].max()) + 1 if len(sub) > 0 else 0
    lines.append(f'| {bin_label} | Mean Ret ({horizon}) | Annualized | N Weeks | N Stocks-Total |')
    lines.append('|---|---|---|---|---|')
    for row in sub.itertuples(index=False):
        ann = annualize_weekly_from_horizon(row.ret_mean, hd)
        lines.append(f'| {bin_label}{int(row.decile)+1} | {row.ret_mean:+.4f} | {ann:+.2%} '
                     f'| {row.n_weeks} | {row.n_stocks_total:,} |')
    # Top-Bot spread
    top_idx = n_bins - 1
    top = sub[sub['decile'] == top_idx]['ret_mean'].iloc[0] if (sub['decile'] == top_idx).any() else np.nan
    bot = sub[sub['decile'] == 0]['ret_mean'].iloc[0] if (sub['decile'] == 0).any() else np.nan
    if pd.notna(top) and pd.notna(bot):
        spread = top - bot
        spread_ann = annualize_weekly_from_horizon(top, hd) - annualize_weekly_from_horizon(bot, hd)
        lines.append('')
        lines.append(f'- **{bin_label}{n_bins} - {bin_label}1 spread ({horizon})**: '
                     f'{spread:+.4f} per period, **{spread_ann:+.2%} annualized**')
    return lines


def fmt_rvol_table(rvol_corr: dict) -> list[str]:
    lines = []
    lines.append('| Pair | N | Spearman rho | Pearson rho | Overlap? |')
    lines.append('|---|---|---|---|---|')
    for k, v in rvol_corr.items():
        overlap = ('YES (>0.7)' if pd.notna(v['rho_spearman']) and abs(v['rho_spearman']) > 0.7
                   else 'no')
        lines.append(f'| {k} | {v["n"]:,} | {v["rho_spearman"]:+.3f} '
                     f'| {v["rho_pearson"]:+.3f} | {overlap} |')
    return lines


def fmt_regime_table(regime_cut: dict, ret_cols: list[str]) -> list[str]:
    lines = []
    lines.append('| Regime | N Obs | Horizon | Mean IC | IC IR | t-stat | Grade |')
    lines.append('|---|---|---|---|---|---|---|')
    for reg, info in regime_cut.items():
        ic = info['ic']
        n = info['n_obs']
        for h in ret_cols:
            if h not in ic:
                continue
            s = ic[h]
            lines.append(f'| {reg} | {n} | {h} | {s["mean_ic"]:+.4f} | {s["ic_ir"]:+.3f} '
                         f'| {s["t_stat"]:+.2f} | {judge_grade(s["ic_ir"])} |')
    return lines


def write_qm_report(qm_out: dict, path: Path):
    lines = []
    lines.append('# VF-Turnover IC Validation — QM Pool (TW)')
    lines.append('')
    lines.append(f'Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}')
    lines.append('')
    lines.append('**Factor**: turnover = mean(Volume over W trading days) / Shares_Outstanding * 100')
    lines.append('')
    lines.append('**Universe**: trade_journal_qm_tw_mixed.parquet (QM 動能池, 2015-07 ~ 2025-12)')
    lines.append('')
    lines.append('**Grading**: |IR|>=0.5 A / >=0.3 B / >=0.1 C / <0.1 D')
    lines.append('')
    lines.append('**Overlap test**: rho_spearman(turnover, RVOL) > 0.7 => duplicate factor')
    lines.append('')

    panel = qm_out['panel']
    lines.append('## Coverage')
    lines.append('')
    lines.append(f'- Total rows: {len(panel):,}')
    lines.append(f'- turnover_20d coverage: {panel["turnover_20d"].notna().sum():,} '
                 f'({panel["turnover_20d"].notna().mean()*100:.1f}%)')
    lines.append(f'- turnover_5d coverage:  {panel["turnover_5d"].notna().sum():,} '
                 f'({panel["turnover_5d"].notna().mean()*100:.1f}%)')
    lines.append(f'- Unique stocks: {panel["stock_id"].nunique()}')
    lines.append(f'- Unique weeks: {panel["week_end_date"].nunique()}')
    lines.append(f'- turnover_20d mean: {panel["turnover_20d"].mean():.3f}%')
    lines.append(f'- turnover_20d median: {panel["turnover_20d"].median():.3f}%')
    lines.append(f'- turnover_20d p95: {panel["turnover_20d"].quantile(0.95):.3f}%')
    lines.append('')

    lines.append('## RVOL Overlap Check')
    lines.append('')
    lines.append('(rvol_20_calc = mean(vol last 20d) / mean(vol prior 60d), computed on-the-fly)')
    lines.append('')
    lines.extend(fmt_rvol_table(qm_out['rvol_corr']))
    lines.append('')

    for factor in ('turnover_5d', 'turnover_20d'):
        res = qm_out['results'][factor]
        lines.append(f'## Factor: {factor}')
        lines.append('')
        lines.append('### IC Summary (Spearman, cross-section per week)')
        lines.append('')
        lines.extend(fmt_ic_table(res['ic_summary']))
        lines.append('')
        lines.append('### Quintile Spread (QM top-50 => n_bins=5, per week then time-avg)')
        lines.append('')
        for h in ('fwd_20d', 'fwd_40d', 'fwd_60d'):
            lines.append(f'#### {h}')
            lines.append('')
            lines.extend(fmt_decile_table(res['decile'], h, bin_label='Q'))
            lines.append('')

    lines.append('## Regime Cut (turnover_20d)')
    lines.append('')
    lines.extend(fmt_regime_table(qm_out['regime_cut'],
                                  ['fwd_20d', 'fwd_40d', 'fwd_60d']))
    lines.append('')

    path.write_text('\n'.join(lines), encoding='utf-8')
    logger.info('Wrote %s', path)


def write_value_report(val_out: dict, path: Path):
    lines = []
    lines.append('# VF-Turnover IC Validation — Value Pool (TW)')
    lines.append('')
    lines.append(f'Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}')
    lines.append('')
    lines.append('**Factor**: turnover = mean(Volume over W trading days) / Shares_Outstanding * 100')
    lines.append('')
    lines.append('**Universe**: trade_journal_value_tw_snapshot.parquet (Value 價值池, 2020-01 ~ 2025-12)')
    lines.append('')
    lines.append('**Grading**: |IR|>=0.5 A / >=0.3 B / >=0.1 C / <0.1 D')
    lines.append('')
    lines.append('**Overlap test**: rho_spearman(turnover, RVOL) > 0.7 => duplicate factor')
    lines.append('')

    panel = val_out['panel']
    lines.append('## Coverage')
    lines.append('')
    lines.append(f'- Total rows: {len(panel):,}')
    lines.append(f'- turnover_20d coverage: {panel["turnover_20d"].notna().sum():,} '
                 f'({panel["turnover_20d"].notna().mean()*100:.1f}%)')
    lines.append(f'- Unique stocks: {panel["stock_id"].nunique()}')
    lines.append(f'- Unique weeks: {panel["week_end_date"].nunique()}')
    lines.append(f'- turnover_20d mean: {panel["turnover_20d"].mean():.3f}%')
    lines.append(f'- turnover_20d median: {panel["turnover_20d"].median():.3f}%')
    lines.append(f'- turnover_20d p95: {panel["turnover_20d"].quantile(0.95):.3f}%')
    lines.append('')

    lines.append('## RVOL Overlap Check')
    lines.append('')
    lines.append('(rvol_20 = native column from Value journal; rvol_20_calc = our formula)')
    lines.append('')
    lines.extend(fmt_rvol_table(val_out['rvol_corr']))
    lines.append('')

    for factor in ('turnover_5d', 'turnover_20d'):
        res = val_out['results'][factor]
        lines.append(f'## Factor: {factor}')
        lines.append('')
        lines.append('### IC Summary (Spearman, cross-section per week)')
        lines.append('')
        lines.extend(fmt_ic_table(res['ic_summary']))
        lines.append('')
        lines.append('### Decile Spread (per week, then time-avg)')
        lines.append('')
        for h in ('fwd_20d', 'fwd_40d', 'fwd_60d'):
            lines.append(f'#### {h}')
            lines.append('')
            lines.extend(fmt_decile_table(res['decile'], h))
            lines.append('')

    lines.append('## Regime Cut (turnover_20d)')
    lines.append('')
    lines.extend(fmt_regime_table(val_out['regime_cut'],
                                  ['fwd_20d', 'fwd_40d', 'fwd_60d']))
    lines.append('')

    path.write_text('\n'.join(lines), encoding='utf-8')
    logger.info('Wrote %s', path)


def write_summary(qm_out: dict | None, val_out: dict | None, path: Path):
    """Final decision summary across both pools."""
    lines = []
    lines.append('# VF-Turnover Validation Summary — Decision')
    lines.append('')
    lines.append(f'Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}')
    lines.append('')
    lines.append('## Best-horizon IR per pool (factor=turnover_20d)')
    lines.append('')
    lines.append('| Pool | Horizon | Mean IC | IC IR | t-stat | Grade |')
    lines.append('|---|---|---|---|---|---|')

    def _best_row(pool_label: str, result_key: str, out: dict):
        res = out['results'][result_key]['ic_summary']
        if not res:
            return
        best_h = max(res, key=lambda h: abs(res[h]['ic_ir']) if pd.notna(res[h]['ic_ir']) else 0)
        s = res[best_h]
        lines.append(f'| {pool_label} | {best_h} | {s["mean_ic"]:+.4f} | {s["ic_ir"]:+.3f} '
                     f'| {s["t_stat"]:+.2f} | {judge_grade(s["ic_ir"])} |')

    if qm_out is not None:
        _best_row('QM (turnover_20d)', 'turnover_20d', qm_out)
        _best_row('QM (turnover_5d)', 'turnover_5d', qm_out)
    if val_out is not None:
        _best_row('Value (turnover_20d)', 'turnover_20d', val_out)
        _best_row('Value (turnover_5d)', 'turnover_5d', val_out)
    lines.append('')

    lines.append('## RVOL Overlap (turnover_20d vs RVOL)')
    lines.append('')
    lines.append('| Pool | Pair | Spearman rho | Overlap? |')
    lines.append('|---|---|---|---|')
    if qm_out is not None:
        for k, v in qm_out['rvol_corr'].items():
            overlap = ('YES (>0.7)' if pd.notna(v['rho_spearman']) and abs(v['rho_spearman']) > 0.7
                       else 'no')
            lines.append(f'| QM | {k} | {v["rho_spearman"]:+.3f} | {overlap} |')
    if val_out is not None:
        for k, v in val_out['rvol_corr'].items():
            overlap = ('YES (>0.7)' if pd.notna(v['rho_spearman']) and abs(v['rho_spearman']) > 0.7
                       else 'no')
            lines.append(f'| Value | {k} | {v["rho_spearman"]:+.3f} | {overlap} |')
    lines.append('')

    lines.append('## Top-Bot Quantile Spread Annualized (turnover_20d, fwd_40d)')
    lines.append('')
    lines.append('| Pool | Bins | Low ann | High ann | High - Low ann |')
    lines.append('|---|---|---|---|---|')

    def _spread(out: dict, label: str):
        dec = out['results']['turnover_20d']['decile']
        if 'horizon' not in dec.columns or len(dec) == 0:
            return
        sub = dec[dec['horizon'] == 'fwd_40d']
        if len(sub) == 0:
            return
        top_idx = int(sub['decile'].max())
        n_bins = top_idx + 1
        d1 = sub[sub['decile'] == 0]['ret_mean']
        d10 = sub[sub['decile'] == top_idx]['ret_mean']
        if len(d1) == 0 or len(d10) == 0:
            return
        ann_d1 = annualize_weekly_from_horizon(d1.iloc[0], 40)
        ann_d10 = annualize_weekly_from_horizon(d10.iloc[0], 40)
        spread = ann_d10 - ann_d1
        lines.append(f'| {label} | {n_bins} | {ann_d1:+.2%} | {ann_d10:+.2%} | {spread:+.2%} |')

    if qm_out is not None:
        _spread(qm_out, 'QM')
    if val_out is not None:
        _spread(val_out, 'Value')
    lines.append('')

    # Regime breakdown (turnover_20d, fwd_40d)
    lines.append('## Regime Cut (turnover_20d, fwd_40d)')
    lines.append('')
    lines.append('| Pool | Regime | N obs | Mean IC | IC IR | Grade |')
    lines.append('|---|---|---|---|---|---|')

    def _reg_rows(pool_label: str, out: dict):
        for reg, info in out['regime_cut'].items():
            ic = info.get('ic', {})
            if 'fwd_40d' not in ic:
                continue
            s = ic['fwd_40d']
            lines.append(f'| {pool_label} | {reg} | {info["n_obs"]} | '
                         f'{s["mean_ic"]:+.4f} | {s["ic_ir"]:+.3f} | '
                         f'{judge_grade(s["ic_ir"])} |')

    if qm_out is not None:
        _reg_rows('QM', qm_out)
    if val_out is not None:
        _reg_rows('Value', val_out)
    lines.append('')

    lines.append('## Decision Framework')
    lines.append('')
    lines.append('**Required for B+落地**：')
    lines.append('')
    lines.append('1. Pooled IC IR >= 0.3 OR')
    lines.append('2. Decile spread annualized > 3% with monotonic trend OR')
    lines.append('3. Regime-conditional IC IR >= 0.3 with t-stat >= 2')
    lines.append('')
    lines.append('AND:')
    lines.append('')
    lines.append('4. RVOL overlap (Spearman) < 0.7')
    lines.append('')
    lines.append('See individual reports for full details:')
    lines.append('')
    lines.append('- `reports/vf_turnover_ic_qm.md`')
    lines.append('- `reports/vf_turnover_ic_value.md`')
    lines.append('')

    path.write_text('\n'.join(lines), encoding='utf-8')
    logger.info('Wrote %s', path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pool', choices=['qm', 'value', 'both'], default='both')
    args = ap.parse_args()

    logger.info('=== VF-Turnover IC Validation ===')
    ohlcv = load_ohlcv()
    shares_df = load_shares_outstanding()
    regime_df = load_twii_regime()

    qm_out = None
    val_out = None

    if args.pool in ('qm', 'both'):
        qm_out = run_qm_pool(ohlcv, shares_df, regime_df)
        write_qm_report(qm_out, OUT_DIR / 'vf_turnover_ic_qm.md')
        # Save panel for reference
        qm_out['panel'].to_parquet(
            OUT_DIR / 'vf_turnover_ic_qm_panel.parquet', index=False)

    if args.pool in ('value', 'both'):
        val_out = run_value_pool(ohlcv, shares_df, regime_df)
        write_value_report(val_out, OUT_DIR / 'vf_turnover_ic_value.md')
        val_out['panel'].to_parquet(
            OUT_DIR / 'vf_turnover_ic_value_panel.parquet', index=False)

    write_summary(qm_out, val_out, OUT_DIR / 'vf_turnover_summary_raw.md')
    logger.info('=== Done ===')


if __name__ == '__main__':
    main()
