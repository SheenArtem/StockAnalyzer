"""
V1 Layer 1 低基期盤整 factor IC 驗證

4 個 sub-factor：
  F1: 距 52 週高 -20% ~ -40%   (0.60 <= close/52w_high <= 0.80)
  F2: BB squeeze               (BB_width 20d 低於自身 60d 歷史 20 percentile)
  F3: ATR 衰減                 (ATR20 / ATR20_60d_ago < 0.8)
  F4: 近 20-40 日無新低         (min(close,20-40d) > min(close,60-120d))

合成 signal:
  - any-1 / any-2 / all-3 / all-4 (binary)

三段 regime:
  - 2016-2019 Pre-COVID
  - 2020-2022 COVID/post-COVID
  - 2023-2025 AI era

Horizons: fwd_20d, fwd_60d

輸出:
  reports/vf_step1_layer1_lowbase_ic.csv
  reports/vf_step1_layer1_lowbase_ic.md
"""
import argparse
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))

PARQUET_PATH = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "vf_step1_layer1_lowbase_ic.csv"
OUT_MD = OUT_DIR / "vf_step1_layer1_lowbase_ic.md"

HORIZONS = [20, 60]
MIN_CROSS_SECTION = 30
MIN_HISTORY = 300

REGIMES = {
    "2016-2019": ("2016-01-01", "2019-12-31"),
    "2020-2022": ("2020-01-01", "2022-12-31"),
    "2023-2025": ("2023-01-01", "2025-12-31"),
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("l1")


# ============================================================
# Load
# ============================================================
def load_ohlcv(sample=None, seed=42):
    logger.info(f"Loading {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    df['date'] = pd.to_datetime(df['date'])

    for col in ['Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    counts = df.groupby('yf_ticker').size()
    keep = counts[counts >= MIN_HISTORY].index
    df = df[df['yf_ticker'].isin(keep)].copy()

    if sample:
        rng = np.random.default_rng(seed)
        tickers = sorted(df['yf_ticker'].unique())
        chosen = rng.choice(tickers, min(sample, len(tickers)), replace=False)
        df = df[df['yf_ticker'].isin(chosen)].copy()

    df = df.sort_values(['yf_ticker', 'date']).reset_index(drop=True)
    logger.info(f"Loaded {len(df):,} rows, {df['yf_ticker'].nunique()} tickers, "
                f"{df['date'].min().date()} ~ {df['date'].max().date()}")
    return df


# ============================================================
# Sub-factor computation (per ticker)
# ============================================================
def _compute_one_ticker(sub):
    """
    回傳加上 F1/F2/F3/F4 binary signals 的 sub。
    """
    sub = sub.copy()
    close = sub['Close']

    # ------ F1: 距 52 週高 -20% ~ -40% ------
    high_252 = close.rolling(252, min_periods=200).max()
    ratio_52w = close / high_252
    sub['F1_lowbase'] = ((ratio_52w >= 0.60) & (ratio_52w <= 0.80)).astype(float)

    # ------ F2: BB squeeze ------
    # BB width = (MA20 + 2*std20) - (MA20 - 2*std20) = 4 * std20
    # 用 BB width / MA20 以跨股可比
    ma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    bb_width_norm = (4 * std20) / ma20

    # 20 percentile of own 60d history → squeeze on
    bb_width_rank = bb_width_norm.rolling(60, min_periods=40).apply(
        lambda x: (x.iloc[-1] <= np.quantile(x, 0.20)) * 1.0 if len(x) >= 20 else np.nan,
        raw=False,
    )
    sub['F2_squeeze'] = bb_width_rank.fillna(0).astype(float)

    # ------ F3: ATR 衰減 ------
    high = sub['High']
    low = sub['Low']
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr20 = tr.rolling(20).mean()
    atr20_60d_ago = atr20.shift(60)
    atr_decay_ratio = atr20 / atr20_60d_ago
    sub['F3_atr_decay'] = (atr_decay_ratio < 0.8).astype(float)

    # ------ F4: 近 20-40 日無新低 (vs 60-120 前) ------
    # 定義: 過去 20 日最低 > 過去 60-120 日最低 => 確定脫離底部
    min_20 = close.rolling(20).min()
    min_60_to_120 = close.shift(60).rolling(60).min()   # 60d ago ~ 120d ago 區間
    sub['F4_no_newlow'] = (min_20 > min_60_to_120).astype(float)

    # set to NaN if insufficient history
    for col in ['F1_lowbase', 'F2_squeeze', 'F3_atr_decay', 'F4_no_newlow']:
        sub[col] = sub[col].where(sub[col].notna(), np.nan)

    return sub


def compute_all_factors(df):
    logger.info("Computing Layer 1 sub-factors...")
    t0 = time.time()
    df = df.groupby('yf_ticker', group_keys=False).apply(_compute_one_ticker)
    logger.info(f"Sub-factors done in {time.time()-t0:.1f}s")
    return df


# ============================================================
# Composites
# ============================================================
SUB_FACTORS = ['F1_lowbase', 'F2_squeeze', 'F3_atr_decay', 'F4_no_newlow']


def add_composites(df):
    """
    建 any-k / all-k binary composites.
    """
    logger.info("Building composites...")
    count = df[SUB_FACTORS].sum(axis=1)
    df['C_any1'] = (count >= 1).astype(float)
    df['C_any2'] = (count >= 2).astype(float)
    df['C_all3'] = (count >= 3).astype(float)
    df['C_all4'] = (count >= 4).astype(float)
    df['C_count'] = count  # 0..4 continuous for spread test

    # If any sub-factor is NaN, composite is NaN
    mask = df[SUB_FACTORS].isna().any(axis=1)
    for col in ['C_any1', 'C_any2', 'C_all3', 'C_all4', 'C_count']:
        df.loc[mask, col] = np.nan
    return df


# ============================================================
# Forward returns
# ============================================================
def add_fwd_returns(df):
    logger.info("Computing forward returns...")
    df = df.sort_values(['yf_ticker', 'date']).reset_index(drop=True)
    for h in HORIZONS:
        df[f'fwd_{h}d'] = df.groupby('yf_ticker')['AdjClose'].pct_change(h).shift(-h)
    return df


# ============================================================
# IC + spread for binary signals
# ============================================================
def compute_binary_stats(df, signal_col, return_col, regime_start=None, regime_end=None):
    """
    對 binary signal (0/1) 計算：
      - mean fwd ret of (signal=1) vs (signal=0)
      - cross-sectional spread t-stat
      - daily IC (Spearman between 0/1 signal and return) then IR
    """
    x = df[['date', signal_col, return_col]].copy()
    if regime_start:
        x = x[x['date'] >= regime_start]
    if regime_end:
        x = x[x['date'] <= regime_end]
    x = x.dropna(subset=[signal_col, return_col])

    if len(x) == 0:
        return dict(
            n_obs=0, n_days=0, signal_rate=np.nan,
            mean_ret_1=np.nan, mean_ret_0=np.nan, spread=np.nan,
            ic_mean=np.nan, ic_std=np.nan, ic_ir=np.nan, ic_tstat=np.nan, ic_p=np.nan,
            win_rate=np.nan,
        )

    # Daily Spearman IC
    ics = []
    spreads = []
    for date, g in x.groupby('date'):
        if len(g) < MIN_CROSS_SECTION:
            continue
        if g[signal_col].nunique() < 2:
            continue
        try:
            ic, _ = stats.spearmanr(g[signal_col], g[return_col])
            if not np.isnan(ic):
                ics.append(ic)
            r1 = g.loc[g[signal_col] == 1, return_col].mean()
            r0 = g.loc[g[signal_col] == 0, return_col].mean()
            if not (np.isnan(r1) or np.isnan(r0)):
                spreads.append(r1 - r0)
        except Exception:
            continue

    ic_arr = np.array(ics)
    spread_arr = np.array(spreads)
    n_days = len(ic_arr)

    if n_days < 20:
        ic_mean = float(ic_arr.mean()) if n_days > 0 else np.nan
        ic_std = float(ic_arr.std(ddof=1)) if n_days > 1 else np.nan
        return dict(
            n_obs=len(x), n_days=n_days,
            signal_rate=float(x[signal_col].mean()),
            mean_ret_1=float(x.loc[x[signal_col] == 1, return_col].mean()) if (x[signal_col] == 1).any() else np.nan,
            mean_ret_0=float(x.loc[x[signal_col] == 0, return_col].mean()) if (x[signal_col] == 0).any() else np.nan,
            spread=float(spread_arr.mean()) if len(spread_arr) > 0 else np.nan,
            ic_mean=ic_mean, ic_std=ic_std, ic_ir=np.nan, ic_tstat=np.nan, ic_p=np.nan,
            win_rate=np.nan,
        )

    ic_mean = ic_arr.mean()
    ic_std = ic_arr.std(ddof=1)
    ic_ir = ic_mean / ic_std if ic_std > 0 else np.nan
    t_stat = ic_mean * np.sqrt(n_days) / ic_std if ic_std > 0 else 0
    p_val = 2 * (1 - stats.t.cdf(abs(t_stat), df=n_days - 1))
    win_rate = (ic_arr > 0).mean() * 100

    return dict(
        n_obs=len(x),
        n_days=n_days,
        signal_rate=float(x[signal_col].mean()),
        mean_ret_1=float(x.loc[x[signal_col] == 1, return_col].mean()) if (x[signal_col] == 1).any() else np.nan,
        mean_ret_0=float(x.loc[x[signal_col] == 0, return_col].mean()) if (x[signal_col] == 0).any() else np.nan,
        spread=float(spread_arr.mean()) if len(spread_arr) > 0 else np.nan,
        ic_mean=float(ic_mean),
        ic_std=float(ic_std),
        ic_ir=float(ic_ir) if not np.isnan(ic_ir) else np.nan,
        ic_tstat=float(t_stat),
        ic_p=float(p_val),
        win_rate=float(win_rate),
    )


def compute_continuous_ic(df, signal_col, return_col, regime_start=None, regime_end=None):
    """
    Continuous signal (e.g. C_count 0..4) Spearman IC.
    """
    x = df[['date', signal_col, return_col]].copy()
    if regime_start:
        x = x[x['date'] >= regime_start]
    if regime_end:
        x = x[x['date'] <= regime_end]
    x = x.dropna(subset=[signal_col, return_col])

    if len(x) == 0:
        return dict(n_obs=0, n_days=0, ic_mean=np.nan, ic_std=np.nan, ic_ir=np.nan,
                    ic_tstat=np.nan, ic_p=np.nan, win_rate=np.nan)

    ics = []
    for date, g in x.groupby('date'):
        if len(g) < MIN_CROSS_SECTION:
            continue
        if g[signal_col].nunique() < 2:
            continue
        try:
            ic, _ = stats.spearmanr(g[signal_col], g[return_col])
            if not np.isnan(ic):
                ics.append(ic)
        except Exception:
            continue

    ic_arr = np.array(ics)
    n_days = len(ic_arr)
    if n_days < 20:
        return dict(n_obs=len(x), n_days=n_days,
                    ic_mean=float(ic_arr.mean()) if n_days > 0 else np.nan,
                    ic_std=float(ic_arr.std(ddof=1)) if n_days > 1 else np.nan,
                    ic_ir=np.nan, ic_tstat=np.nan, ic_p=np.nan, win_rate=np.nan)

    ic_mean = ic_arr.mean()
    ic_std = ic_arr.std(ddof=1)
    ic_ir = ic_mean / ic_std if ic_std > 0 else np.nan
    t_stat = ic_mean * np.sqrt(n_days) / ic_std if ic_std > 0 else 0
    p_val = 2 * (1 - stats.t.cdf(abs(t_stat), df=n_days - 1))
    win_rate = (ic_arr > 0).mean() * 100
    return dict(
        n_obs=len(x), n_days=n_days,
        ic_mean=float(ic_mean), ic_std=float(ic_std),
        ic_ir=float(ic_ir) if not np.isnan(ic_ir) else np.nan,
        ic_tstat=float(t_stat), ic_p=float(p_val), win_rate=float(win_rate),
    )


# ============================================================
# Main
# ============================================================
def grade(ic_mean, ic_ir, p_val, spread):
    """
    A: IC > 0.02 且 IR > 0.5 且 p < 0.05 且 spread > 0.01 (+1% 20d)
    B: IC > 0.015 且 p < 0.10 且 spread > 0
    C: IC > 0.005 且 spread > 0 但 p >= 0.10
    D: 負或不顯著
    """
    if pd.isna(ic_mean) or pd.isna(p_val):
        return 'N/A'
    if ic_mean > 0.02 and (ic_ir or 0) > 0.5 and p_val < 0.05 and (spread or 0) > 0.01:
        return 'A'
    if ic_mean > 0.015 and p_val < 0.10 and (spread or 0) > 0:
        return 'B'
    if ic_mean > 0.005 and (spread or 0) > 0:
        return 'C'
    return 'D'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', type=int, default=250)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--full', action='store_true',
                        help='Use all tickers (ignore --sample)')
    args = parser.parse_args()

    t0 = time.time()
    sample = None if args.full else args.sample
    df = load_ohlcv(sample=sample, seed=args.seed)

    df = compute_all_factors(df)
    df = add_composites(df)
    df = add_fwd_returns(df)

    # ---- Main matrix ----
    results = []

    all_signals = SUB_FACTORS + ['C_any1', 'C_any2', 'C_all3', 'C_all4']

    for sig in all_signals:
        for h in HORIZONS:
            ret_col = f'fwd_{h}d'
            # Regime: all
            stats_all = compute_binary_stats(df, sig, ret_col)
            results.append(dict(signal=sig, horizon=h, regime='all', **stats_all))
            # Per-regime
            for reg_name, (start, end) in REGIMES.items():
                stats_r = compute_binary_stats(df, sig, ret_col,
                                               regime_start=start, regime_end=end)
                results.append(dict(signal=sig, horizon=h, regime=reg_name, **stats_r))

    # Continuous C_count (for monotonicity check)
    for h in HORIZONS:
        ret_col = f'fwd_{h}d'
        stats_all = compute_continuous_ic(df, 'C_count', ret_col)
        results.append(dict(signal='C_count', horizon=h, regime='all',
                            n_obs=stats_all['n_obs'], n_days=stats_all['n_days'],
                            signal_rate=np.nan, mean_ret_1=np.nan, mean_ret_0=np.nan,
                            spread=np.nan,
                            ic_mean=stats_all['ic_mean'], ic_std=stats_all['ic_std'],
                            ic_ir=stats_all['ic_ir'], ic_tstat=stats_all['ic_tstat'],
                            ic_p=stats_all['ic_p'], win_rate=stats_all['win_rate']))
        for reg_name, (start, end) in REGIMES.items():
            stats_r = compute_continuous_ic(df, 'C_count', ret_col, start, end)
            results.append(dict(signal='C_count', horizon=h, regime=reg_name,
                                n_obs=stats_r['n_obs'], n_days=stats_r['n_days'],
                                signal_rate=np.nan, mean_ret_1=np.nan, mean_ret_0=np.nan,
                                spread=np.nan,
                                ic_mean=stats_r['ic_mean'], ic_std=stats_r['ic_std'],
                                ic_ir=stats_r['ic_ir'], ic_tstat=stats_r['ic_tstat'],
                                ic_p=stats_r['ic_p'], win_rate=stats_r['win_rate']))

    # C_count deciles (monotonicity)
    logger.info("Computing C_count level-wise mean returns (monotonicity)...")
    mono_rows = []
    for h in HORIZONS:
        ret_col = f'fwd_{h}d'
        for reg_name, bounds in [('all', (None, None))] + [(n, REGIMES[n]) for n in REGIMES]:
            sub = df.dropna(subset=['C_count', ret_col])
            if bounds[0]:
                sub = sub[sub['date'] >= bounds[0]]
            if bounds[1]:
                sub = sub[sub['date'] <= bounds[1]]
            if len(sub) == 0:
                continue
            grp = sub.groupby('C_count')[ret_col].agg(['mean', 'count']).reset_index()
            grp['signal'] = 'C_count_level'
            grp['horizon'] = h
            grp['regime'] = reg_name
            mono_rows.append(grp)

    mono_df = pd.concat(mono_rows, ignore_index=True) if mono_rows else pd.DataFrame()

    # ---- Output CSV ----
    out_df = pd.DataFrame(results)
    out_df = out_df.round({'signal_rate': 4, 'mean_ret_1': 5, 'mean_ret_0': 5,
                           'spread': 5, 'ic_mean': 5, 'ic_std': 5, 'ic_ir': 3,
                           'ic_tstat': 3, 'ic_p': 4, 'win_rate': 2})
    out_df['grade'] = out_df.apply(
        lambda r: grade(r['ic_mean'], r['ic_ir'], r['ic_p'], r['spread']), axis=1
    )
    out_df.to_csv(OUT_CSV, index=False, encoding='utf-8-sig')
    logger.info(f"Main CSV saved: {OUT_CSV} ({len(out_df)} rows)")

    if len(mono_df) > 0:
        mono_path = OUT_DIR / "vf_step1_layer1_lowbase_monotonicity.csv"
        mono_df.to_csv(mono_path, index=False, encoding='utf-8-sig')
        logger.info(f"Monotonicity CSV saved: {mono_path}")

    # ---- Console summary ----
    print("\n" + "=" * 100)
    print("  V1 Layer 1 低基期盤整 IC Matrix  (universe=all, binary signals)")
    print("=" * 100)
    for h in HORIZONS:
        print(f"\n  ── Horizon fwd_{h}d ──")
        sub = out_df[(out_df['horizon'] == h) & (out_df['regime'] == 'all')
                     & (out_df['signal'] != 'C_count')]
        print(f"  {'signal':<12} {'rate':>6} {'ic_mean':>9} {'ic_ir':>7} "
              f"{'tstat':>7} {'p':>7} {'spread':>8} {'win%':>6} {'grade':>6}")
        for _, r in sub.iterrows():
            print(f"  {r['signal']:<12} {r['signal_rate']:>6.3f} "
                  f"{r['ic_mean']:>+9.4f} {r['ic_ir'] if pd.notna(r['ic_ir']) else 0:>+7.2f} "
                  f"{r['ic_tstat'] if pd.notna(r['ic_tstat']) else 0:>+7.2f} "
                  f"{r['ic_p'] if pd.notna(r['ic_p']) else 0:>7.4f} "
                  f"{r['spread'] if pd.notna(r['spread']) else 0:>+8.4f} "
                  f"{r['win_rate'] if pd.notna(r['win_rate']) else 0:>5.1f}% "
                  f"{r['grade']:>6}")

    print("\n  ── By Regime (fwd_20d) ──")
    for sig in all_signals:
        row_per_reg = out_df[(out_df['horizon'] == 20) & (out_df['signal'] == sig)]
        line = f"  {sig:<12} "
        for reg in ['all'] + list(REGIMES.keys()):
            r = row_per_reg[row_per_reg['regime'] == reg]
            if len(r) == 0:
                continue
            r = r.iloc[0]
            line += f"{reg}:{r['ic_mean']:>+.4f}({r['grade']}) "
        print(line)

    # ---- Markdown ----
    write_markdown(out_df, mono_df)
    logger.info(f"MD saved: {OUT_MD}")

    print(f"\nTotal time: {(time.time()-t0)/60:.1f} min")
    print(f"CSV:  {OUT_CSV}")
    print(f"MD:   {OUT_MD}")


def write_markdown(out_df, mono_df):
    lines = []
    lines.append("# V1 Layer 1 低基期盤整 IC 驗證\n")
    lines.append(f"- Signals: F1 低基期 / F2 BB squeeze / F3 ATR 衰減 / F4 近期無新低")
    lines.append(f"- Composites: C_any1 / C_any2 / C_all3 / C_all4 / C_count (0..4)")
    lines.append(f"- Horizons: {HORIZONS}")
    lines.append(f"- Regimes: {list(REGIMES.keys())}")
    lines.append(f"- Universe: TW all (sample)\n")

    # -- Summary by signal --
    lines.append("## 核心結論\n")
    h_focus = 20
    focus = out_df[(out_df['horizon'] == h_focus) & (out_df['regime'] == 'all')
                   & (out_df['signal'] != 'C_count')].copy()
    focus = focus.sort_values('ic_mean', ascending=False)

    lines.append(f"### fwd_{h_focus}d Rank (universe=all, regime=all)\n")
    lines.append("| signal | rate | ic_mean | ic_ir | t-stat | p | spread | win% | grade |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for _, r in focus.iterrows():
        lines.append(f"| {r['signal']} | {r['signal_rate']:.3f} | "
                     f"{r['ic_mean']:+.4f} | {r['ic_ir'] if pd.notna(r['ic_ir']) else 0:+.2f} | "
                     f"{r['ic_tstat'] if pd.notna(r['ic_tstat']) else 0:+.2f} | "
                     f"{r['ic_p'] if pd.notna(r['ic_p']) else 0:.4f} | "
                     f"{r['spread'] if pd.notna(r['spread']) else 0:+.4f} | "
                     f"{r['win_rate'] if pd.notna(r['win_rate']) else 0:.1f}% | "
                     f"**{r['grade']}** |")

    lines.append("")

    # -- By regime --
    lines.append("### 分段 IC (fwd_20d)\n")
    lines.append("| signal | 2016-2019 | 2020-2022 | 2023-2025 AI era |")
    lines.append("| --- | --- | --- | --- |")
    for sig in SUB_FACTORS + ['C_any1', 'C_any2', 'C_all3', 'C_all4', 'C_count']:
        row = f"| {sig} |"
        for reg in REGIMES:
            r = out_df[(out_df['horizon'] == 20) & (out_df['signal'] == sig)
                       & (out_df['regime'] == reg)]
            if len(r) > 0:
                rr = r.iloc[0]
                grade_str = rr.get('grade', '')
                row += (f" IC={rr['ic_mean']:+.4f}"
                        f" IR={rr['ic_ir'] if pd.notna(rr['ic_ir']) else 0:+.2f}"
                        f" ({grade_str}) |")
            else:
                row += " - |"
        lines.append(row)
    lines.append("")

    # -- Monotonicity --
    if len(mono_df) > 0:
        lines.append("### C_count 層級平均報酬 (單調性檢查, fwd_20d, regime=all)\n")
        mono_f = mono_df[(mono_df['horizon'] == 20) & (mono_df['regime'] == 'all')].copy()
        lines.append("| C_count | mean fwd_20d | n_obs |")
        lines.append("| --- | --- | --- |")
        for _, r in mono_f.iterrows():
            lines.append(f"| {int(r['C_count'])} | {r['mean']:+.4f} | {int(r['count'])} |")
        lines.append("")

    # Verdict
    lines.append("## Verdict\n")
    best_sub = focus[focus['signal'].isin(SUB_FACTORS)].iloc[0]
    best_comp = focus[focus['signal'].isin(['C_any1', 'C_any2', 'C_all3', 'C_all4'])]
    best_comp = best_comp.iloc[0] if len(best_comp) > 0 else None

    lines.append(f"- **最強 sub-factor (fwd_20d)**: `{best_sub['signal']}` "
                 f"IC={best_sub['ic_mean']:+.4f} IR={best_sub['ic_ir'] if pd.notna(best_sub['ic_ir']) else 0:+.2f}"
                 f" grade={best_sub['grade']}")
    if best_comp is not None:
        lines.append(f"- **最強 composite (fwd_20d)**: `{best_comp['signal']}` "
                     f"IC={best_comp['ic_mean']:+.4f} IR={best_comp['ic_ir'] if pd.notna(best_comp['ic_ir']) else 0:+.2f}"
                     f" grade={best_comp['grade']}")

    # Overall grade rule
    good_count = (focus['grade'].isin(['A', 'B'])).sum()
    if good_count >= 4:
        overall = "A"
    elif good_count >= 2:
        overall = "B"
    elif good_count >= 1:
        overall = "C"
    else:
        overall = "D"
    lines.append(f"- **Overall grade**: **{overall}** ({good_count}/8 signal A/B @ fwd_20d)")

    # AI era check
    ai_rows = out_df[(out_df['horizon'] == 20) & (out_df['regime'] == '2023-2025')
                     & (out_df['signal'] != 'C_count')]
    ai_neg = (ai_rows['ic_mean'] < 0).sum()
    lines.append(f"- **AI era 2023-2025**: {ai_neg}/{len(ai_rows)} signal IC 為負 "
                 "(低基期系統性劣勢?)")

    lines.append("")
    lines.append("## 建議下一步\n")
    if overall in ('A', 'B'):
        lines.append("- 優秀 sub-factor 可獨立考慮；合成若 IR 更好則採用")
        lines.append("- 擴大至全 universe 重驗")
        lines.append("- 進 Step 2 portfolio / Top-N Sharpe")
    elif overall == 'C':
        lines.append("- 單因子偏弱，試用連續版 C_count (0..4) + 技術強勢 / F-Score 組合")
        lines.append("- 觀察 regime 敏感性，若僅某段有效則 regime-gated 使用")
    else:
        lines.append("- Layer 1 低基期在本樣本沒有穩定 alpha")
        lines.append("- 可能需要加入催化劑 (EPS 好轉 / 券資比 / 營收 YoY) 再構成策略")
        lines.append("- 不建議單獨拿來做 signal")

    OUT_MD.write_text("\n".join(lines), encoding='utf-8')


if __name__ == '__main__':
    main()
