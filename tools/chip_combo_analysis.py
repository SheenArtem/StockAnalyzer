"""
C2-c: 技術+籌碼跨域組合 IC 驗證

結合技術面 15 因子 + 籌碼面 20 因子，驗證組合 IC 是否超越單獨任一面。

組合清單:
  1. combo_tech_only       -- 技術 baseline (rvol_lowatr)
  2. combo_chip_equal      -- 全 20 籌碼等權 (flipped)
  3. combo_chip_top3       -- top 3 籌碼 (flipped)
  4. combo_tech_chip_equal -- 技術 15 + 籌碼 20 全等權
  5. combo_tech_chip_top   -- 技術 top + 籌碼 top3 等權
  6. combo_tech70_chip30   -- 技術 70% + 籌碼 30%
  7. combo_tech50_chip50   -- 技術 50% + 籌碼 50%
  8. combo_ols_tech_chip   -- OLS 最佳權重

輸出:
  reports/chip_combo_ic.csv   -- 組合 IC matrix
  reports/chip_combo_corr.csv -- 技術 x 籌碼 兩兩相關矩陣

用法:
    python tools/chip_combo_analysis.py              # 全量
    python tools/chip_combo_analysis.py --sample 300 # 測試
"""
import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from tools.indicator_ic_analysis import (
    load_ohlcv as load_ohlcv_tech,
    compute_all_indicators, add_fwd_returns, add_regime,
    add_universe_flags, compute_daily_ic, summarize_ic,
    SIGNAL_COLS as TECH_SIGNAL_COLS,
    SIGNAL_LABELS as TECH_SIGNAL_LABELS,
    HORIZONS, MIN_CROSS_SECTION,
)
from tools.indicator_combo_analysis import SIGNAL_DIRECTION as TECH_DIRECTION
from tools.chip_ic_analysis import (
    load_chip_data,
    SIGNAL_COLS as CHIP_SIGNAL_COLS,
    SIGNAL_LABELS as CHIP_SIGNAL_LABELS,
)

OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_COMBO = OUT_DIR / "chip_combo_ic.csv"
OUT_CORR = OUT_DIR / "chip_combo_corr.csv"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("chip_combo")


# ============================================================
# Chip signal direction (C2-b: all negative IC -> flip)
# ============================================================
CHIP_DIRECTION = {
    'inst_foreign_5d': +1,
    'inst_foreign_10d': +1,
    'inst_foreign_20d': +1,
    'inst_trust_5d': -1,
    'inst_trust_10d': -1,
    'inst_trust_20d': -1,
    'inst_total_5d': -1,
    'inst_total_10d': -1,
    'inst_total_20d': -1,
    'margin_chg_5d': -1,
    'margin_chg_10d': -1,
    'margin_chg_20d': -1,
    'short_chg_5d': -1,
    'short_chg_10d': -1,
    'short_chg_20d': -1,
    'margin_short_ratio': -1,
    'sbl_chg_5d': -1,
    'sbl_chg_10d': -1,
    'sbl_chg_20d': -1,
    'inst_foreign_trust_sync': -1,
}

# Rename to avoid collision: chip uses inst_foreign_trust_sync,
# the old script used inst_ft_sync
CHIP_TOP3 = ['margin_short_ratio', 'inst_trust_10d', 'margin_chg_20d']


# ============================================================
# 1. Load and merge tech + chip data
# ============================================================
def load_and_merge(sample=None, since=None):
    """Load ohlcv with tech indicators + chip signals, merge on (stock_id, date)."""
    # --- Tech side ---
    df = load_ohlcv_tech(sample=sample, since=since)
    df = compute_all_indicators(df)
    df = add_fwd_returns(df)
    df = add_regime(df)
    df = add_universe_flags(df)
    logger.info(f"Tech panel: {len(df):,} rows, {df['stock_id'].nunique()} stocks")

    # --- Chip side ---
    inst, margin, short = load_chip_data()

    # Restrict chip to stocks in ohlcv
    valid_ids = set(df['stock_id'].unique())
    inst = inst[inst['stock_id'].isin(valid_ids)].copy()
    margin = margin[margin['stock_id'].isin(valid_ids)].copy()
    short = short[short['stock_id'].isin(valid_ids)].copy()

    # Compute chip signals (inline from chip_ic_analysis logic)
    df = _compute_chip_signals(df, inst, margin, short)

    # Log coverage
    chip_valid = df[CHIP_SIGNAL_COLS].notna().mean()
    logger.info("Chip signal coverage:")
    for col in CHIP_SIGNAL_COLS:
        logger.info(f"  {col}: {chip_valid[col]*100:.1f}%")

    return df


def _compute_chip_signals(df, inst, margin, short):
    """Compute chip signals onto the tech panel."""
    logger.info("Computing chip signals on merged panel...")
    t0 = time.time()

    # 20d avg volume for normalization
    df = df.sort_values(['stock_id', 'date'])
    df['vol_20d_avg'] = df.groupby('stock_id')['Volume'].transform(
        lambda s: s.rolling(20, min_periods=10).mean()
    )
    df['vol_20d_avg'] = df['vol_20d_avg'].replace(0, np.nan)

    # --- Institutional ---
    inst_cols = ['date', 'stock_id', 'foreign_net', 'trust_net', 'total_net']
    df = df.merge(inst[inst_cols], on=['stock_id', 'date'], how='left')

    df = df.sort_values(['stock_id', 'date'])
    for entity, src_col in [('foreign', 'foreign_net'), ('trust', 'trust_net'), ('total', 'total_net')]:
        for w in [5, 10, 20]:
            sig_col = f'inst_{entity}_{w}d'
            raw = df.groupby('stock_id')[src_col].transform(
                lambda s: s.rolling(w, min_periods=1).sum()
            )
            df[sig_col] = raw / df['vol_20d_avg']

    # Foreign + Trust sync
    f5 = df.groupby('stock_id')['foreign_net'].transform(
        lambda s: s.rolling(5, min_periods=1).sum()
    )
    t5 = df.groupby('stock_id')['trust_net'].transform(
        lambda s: s.rolling(5, min_periods=1).sum()
    )
    df['inst_foreign_trust_sync'] = np.where(
        (f5 > 0) & (t5 > 0), 1.0,
        np.where((f5 < 0) & (t5 < 0), -1.0, 0.0)
    )

    # --- Margin ---
    margin_cols = ['date', 'stock_id', 'margin_balance', 'short_balance']
    df = df.merge(margin[margin_cols], on=['stock_id', 'date'], how='left')

    for w in [5, 10, 20]:
        raw = df.groupby('stock_id')['margin_balance'].transform(
            lambda s: s.diff(w)
        )
        df[f'margin_chg_{w}d'] = raw / df['vol_20d_avg']

    for w in [5, 10, 20]:
        raw = df.groupby('stock_id')['short_balance'].transform(
            lambda s: s.diff(w)
        )
        df[f'short_chg_{w}d'] = raw / df['vol_20d_avg']

    df['margin_short_ratio'] = (
        df['short_balance'] / df['margin_balance'].replace(0, np.nan)
    ).clip(upper=999)

    # --- SBL ---
    sbl_cols = ['date', 'stock_id', 'sbl_balance']
    df = df.merge(short[sbl_cols], on=['stock_id', 'date'], how='left')

    for w in [5, 10, 20]:
        raw = df.groupby('stock_id')['sbl_balance'].transform(
            lambda s: s.diff(w)
        )
        df[f'sbl_chg_{w}d'] = raw / df['vol_20d_avg']

    logger.info(f"Chip signals computed in {time.time()-t0:.1f}s")
    return df


# ============================================================
# 2. Rank normalize all signals
# ============================================================
ALL_SIGNAL_COLS = TECH_SIGNAL_COLS + CHIP_SIGNAL_COLS
ALL_SIGNAL_LABELS = {**TECH_SIGNAL_LABELS, **CHIP_SIGNAL_LABELS}
ALL_DIRECTION = {**TECH_DIRECTION, **CHIP_DIRECTION}


def rank_normalize_all(df):
    """Rank-normalize all tech + chip signals per day, range [-1, +1]."""
    logger.info(f"Rank-normalizing {len(ALL_SIGNAL_COLS)} signals per day...")
    for col in ALL_SIGNAL_COLS:
        df[f'{col}_rn'] = df.groupby('date')[col].transform(
            lambda s: (s.rank(pct=True) - 0.5) * 2
        )
    # Directional columns
    for col, d in ALL_DIRECTION.items():
        df[f'{col}_dir'] = df[f'{col}_rn'] * d
    return df


# ============================================================
# 3. Cross-domain correlation matrix
# ============================================================
def cross_domain_corr(df):
    """Spearman corr between tech top signals and chip top signals."""
    logger.info("Computing cross-domain correlation...")
    # Use directional columns to see how the flipped signals correlate
    tech_top = ['sig_rvol_log', 'sig_atr_pct', 'sig_squeeze', 'sig_td_setup',
                'sig_bb_pos', 'sig_efi', 'sig_rsi_dev', 'sig_ma20_dev']
    chip_top = ['margin_short_ratio', 'inst_trust_10d', 'margin_chg_20d',
                'short_chg_5d', 'sbl_chg_5d', 'inst_trust_20d',
                'inst_foreign_5d', 'margin_chg_10d']

    tech_cols = [f'{c}_dir' for c in tech_top]
    chip_cols = [f'{c}_dir' for c in chip_top]
    all_cols = tech_cols + chip_cols

    sub = df[all_cols].dropna()
    corr = sub.corr()

    # Relabel
    label_map = {}
    for c in tech_top:
        label_map[f'{c}_dir'] = f'[TECH] {TECH_SIGNAL_LABELS[c]}'
    for c in chip_top:
        label_map[f'{c}_dir'] = f'[CHIP] {CHIP_SIGNAL_LABELS[c]}'
    corr.index = corr.index.map(label_map)
    corr.columns = corr.columns.map(label_map)

    return corr


# ============================================================
# 4. Build 8 combo factors
# ============================================================
def build_combos(df):
    """Build all 8 combo columns. Returns {name: description}."""
    logger.info("Building 8 combo factors...")
    combos = {}

    # --- 1. Tech only baseline (rvol_lowatr) ---
    df['combo_tech_only'] = (
        df['sig_rvol_log_rn'] - df['sig_atr_pct_rn']
    ) / 2
    combos['combo_tech_only'] = 'Tech baseline: RVOL - ATR%'

    # --- 2. Chip equal weight (all 20 flipped) ---
    chip_dir_cols = [f'{c}_dir' for c in CHIP_SIGNAL_COLS]
    df['combo_chip_equal'] = df[chip_dir_cols].mean(axis=1)
    combos['combo_chip_equal'] = 'Chip 20 equal-wt (flipped)'

    # --- 3. Chip top 3 ---
    chip_top3_cols = [f'{c}_dir' for c in CHIP_TOP3]
    df['combo_chip_top3'] = df[chip_top3_cols].mean(axis=1)
    combos['combo_chip_top3'] = 'Chip top3 (margin_short + trust10 + margin20)'

    # --- 4. Tech 15 + Chip 20 all equal weight ---
    tech_dir_cols = [f'{c}_dir' for c in TECH_SIGNAL_COLS]
    all_dir_cols = tech_dir_cols + chip_dir_cols
    df['combo_tech_chip_equal'] = df[all_dir_cols].mean(axis=1)
    combos['combo_tech_chip_equal'] = 'Tech15 + Chip20 all equal-wt'

    # --- 5. Tech top (rvol_lowatr) + Chip top3, equal weight ---
    # rvol_lowatr is (rvol_dir + (-atr_dir)) / 2, combine with chip top3
    # 4 components equal weight
    df['combo_tech_chip_top'] = (
        df['sig_rvol_log_dir'] + df['sig_atr_pct_dir']  # atr_pct dir=-1, so _dir is already flipped
        + df[chip_top3_cols].sum(axis=1)
    ) / 5
    combos['combo_tech_chip_top'] = 'Tech top (RVOL+ATR) + Chip top3'

    # --- 6. Tech 70% + Chip 30% ---
    tech_composite = df[tech_dir_cols].mean(axis=1)
    chip_composite = df[chip_dir_cols].mean(axis=1)
    df['combo_tech70_chip30'] = tech_composite * 0.7 + chip_composite * 0.3
    combos['combo_tech70_chip30'] = 'Tech 70% + Chip 30%'

    # --- 7. Tech 50% + Chip 50% ---
    df['combo_tech50_chip50'] = tech_composite * 0.5 + chip_composite * 0.5
    combos['combo_tech50_chip50'] = 'Tech 50% + Chip 50%'

    # --- 8. OLS (placeholder, computed separately) ---
    # Will be added in fit_ols_all()

    return combos


# ============================================================
# 5. OLS optimal weights
# ============================================================
def fit_ols_all(df, horizons):
    """Fit OLS on all tech+chip signals for each horizon.

    Instead of writing predictions back to the 6M-row main df (slow),
    we compute OLS weights + in-sample IC directly and return the
    daily cross-sectional IC for the OLS predicted score.
    """
    logger.info("Fitting OLS weights (all tech + chip)...")
    rn_cols = [f'{c}_rn' for c in ALL_SIGNAL_COLS]
    ols_results = {}
    ols_ic_results = []  # for IC matrix output

    for h in horizons:
        ret_col = f'fwd_{h}d'
        X_df = df[rn_cols + [ret_col, 'date']].dropna()
        if len(X_df) < 100:
            continue

        X = X_df[rn_cols].values
        y = X_df[ret_col].values

        X_int = np.column_stack([np.ones(len(X)), X])
        try:
            beta, *_ = np.linalg.lstsq(X_int, y, rcond=None)
        except Exception as e:
            logger.warning(f"OLS failed h={h}: {e}")
            continue

        weights = dict(zip(ALL_SIGNAL_COLS, beta[1:]))
        pred = X @ beta[1:] + beta[0]
        ic_is, _ = stats.spearmanr(pred, y)

        ols_results[h] = {
            'weights': weights,
            'intercept': float(beta[0]),
            'in_sample_ic': float(ic_is),
            'n_obs': len(X),
        }
        logger.info(f"  OLS h={h}d: IS-IC={ic_is:+.4f}, n={len(X):,}")

        # Compute daily cross-sectional IC for OLS predicted score
        ols_tmp = X_df[['date', ret_col]].copy()
        ols_tmp['ols_pred'] = pred
        ics = []
        dates = []
        for date, g in ols_tmp.groupby('date'):
            if len(g) < MIN_CROSS_SECTION:
                continue
            try:
                ic_val, _ = stats.spearmanr(g['ols_pred'], g[ret_col])
                if not np.isnan(ic_val):
                    ics.append(ic_val)
                    dates.append(date)
            except Exception:
                continue
        ic_series = pd.Series(ics, index=dates, name='ic')
        stats_dict = summarize_ic(ic_series)

        for uni_name in ['all']:  # OLS is already full sample
            ols_ic_results.append({
                'combo': f'combo_ols_h{h}',
                'description': f'OLS optimal (h={h}d, in-sample)',
                'horizon': h,
                'universe': uni_name,
                **stats_dict,
            })
        del ols_tmp, pred

    return ols_results, ols_ic_results


# ============================================================
# 6. Run IC for all combos (optimized: precompute groupby once)
# ============================================================
def _fast_daily_ic(groups, signal_col, return_col):
    """Compute daily IC using pre-grouped data."""
    ics = []
    dates = []
    for date, g in groups:
        sub = g.dropna(subset=[signal_col, return_col])
        if len(sub) < MIN_CROSS_SECTION:
            continue
        try:
            ic, _ = stats.spearmanr(sub[signal_col], sub[return_col])
            if not np.isnan(ic):
                ics.append(ic)
                dates.append(date)
        except Exception:
            continue
    return pd.Series(ics, index=dates, name='ic')


def run_combo_ic(df, combos, horizons):
    """Compute daily IC for all combos x horizons x universes.

    Optimized: pre-groups df by date once per universe, reuses groups.
    """
    combo_names = list(combos.keys())
    ret_cols = [f'fwd_{h}d' for h in horizons]
    needed_cols = combo_names + ret_cols + ['date', 'in_momentum_universe']

    # Only keep needed columns to reduce memory
    sub_df = df[[c for c in needed_cols if c in df.columns]].copy()

    universes = [('all', None), ('momentum', 'in_momentum_universe')]
    results = []
    total = len(combo_names) * len(horizons) * len(universes)
    done = 0

    for uni_name, uni_col in universes:
        if uni_col:
            uni_df = sub_df[sub_df[uni_col] == True]
        else:
            uni_df = sub_df
        groups = list(uni_df.groupby('date'))
        logger.info(f"  Universe={uni_name}: {len(groups)} dates, "
                     f"{len(uni_df):,} rows")

        for combo_name in combo_names:
            desc = combos[combo_name]
            for h in horizons:
                ret_col = f'fwd_{h}d'
                ic_series = _fast_daily_ic(groups, combo_name, ret_col)
                stats_dict = summarize_ic(ic_series)
                results.append({
                    'combo': combo_name,
                    'description': desc,
                    'horizon': h,
                    'universe': uni_name,
                    **stats_dict,
                })
                done += 1
            if done % 12 == 0:
                logger.info(f"  IC progress: {done}/{total}")

    return pd.DataFrame(results)


# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="C2-c: Tech + Chip combo IC")
    parser.add_argument('--sample', type=int, default=None)
    parser.add_argument('--since', type=str, default=None)
    parser.add_argument('--horizon', type=int, default=None,
                        help='Single horizon (else all)')
    args = parser.parse_args()

    t0 = time.time()

    # Load & merge
    df = load_and_merge(sample=args.sample, since=args.since)
    logger.info(f"Merged panel: {len(df):,} rows, {df['stock_id'].nunique()} stocks")

    # Rank normalize
    df = rank_normalize_all(df)

    # Cross-domain correlation
    corr = cross_domain_corr(df)
    corr.to_csv(OUT_CORR, encoding='utf-8-sig')
    logger.info(f"Correlation saved: {OUT_CORR}")

    print("\n" + "=" * 80)
    print("  Cross-Domain Correlation (Tech x Chip, directional)")
    print("=" * 80)
    # Show top cross-domain pairs
    tech_labels = [c for c in corr.index if c.startswith('[TECH]')]
    chip_labels = [c for c in corr.index if c.startswith('[CHIP]')]
    pairs = []
    for t in tech_labels:
        for c in chip_labels:
            pairs.append((t, c, corr.loc[t, c]))
    pairs.sort(key=lambda x: abs(x[2]), reverse=True)
    print(f"\n  Top cross-domain correlations:")
    for t, c, v in pairs[:15]:
        flag = '!!' if abs(v) > 0.3 else '..'
        print(f"  {flag} {t:<25} -- {c:<25}  corr={v:+.4f}")
    avg_abs = np.mean([abs(x[2]) for x in pairs])
    print(f"\n  Average |corr| across domains: {avg_abs:.4f}")

    # Build combos
    combos = build_combos(df)

    # OLS (computed separately, not added to main df)
    horizons = [args.horizon] if args.horizon else HORIZONS
    ols_results, ols_ic_rows = fit_ols_all(df, horizons)

    # Run IC for non-OLS combos
    t_ic = time.time()
    combo_df = run_combo_ic(df, combos, horizons)
    logger.info(f"Combo IC done in {time.time()-t_ic:.1f}s")

    # Append OLS IC results
    if ols_ic_rows:
        combo_df = pd.concat([combo_df, pd.DataFrame(ols_ic_rows)], ignore_index=True)

    combo_df = combo_df.sort_values(['horizon', 'universe', 'ir'],
                                    ascending=[True, True, False])
    combo_df.to_csv(OUT_COMBO, index=False, encoding='utf-8-sig')
    logger.info(f"Combo IC saved: {OUT_COMBO}")

    # ---- Print results ----
    print("\n" + "=" * 120)
    print("  Combo IC Results (universe=all)")
    print("=" * 120)
    print(f"  {'Combo':<40} {'H':>3} {'mean_IC':>9} {'IR':>7} "
          f"{'95% CI':<22} {'p':>10} {'win%':>6} {'n':>6}")
    print("  " + "-" * 114)
    for h in horizons:
        sub = combo_df[(combo_df['universe'] == 'all') & (combo_df['horizon'] == h)]
        for _, r in sub.iterrows():
            name = r['combo'][:40]
            ci_str = (f"[{r['ci_low']:+.4f}, {r['ci_high']:+.4f}]"
                      if pd.notna(r['ci_low']) else "")
            flag = '*' if r['p'] < 0.05 else ' '
            print(f"  {name:<40} {h:>3d} {r['mean']:>+9.4f} {r['ir']:>+7.3f} "
                  f"{ci_str:<22} {r['p']:>10.2e} {r['win_rate']:>5.1f}% "
                  f"{r['n']:>6d} {flag}")
        print()

    # Momentum universe
    print("=" * 120)
    print("  Combo IC Results (universe=momentum)")
    print("=" * 120)
    for h in horizons:
        sub = combo_df[(combo_df['universe'] == 'momentum') & (combo_df['horizon'] == h)]
        for _, r in sub.iterrows():
            name = r['combo'][:40]
            ci_str = (f"[{r['ci_low']:+.4f}, {r['ci_high']:+.4f}]"
                      if pd.notna(r['ci_low']) else "")
            flag = '*' if r['p'] < 0.05 else ' '
            print(f"  {name:<40} {h:>3d} {r['mean']:>+9.4f} {r['ir']:>+7.3f} "
                  f"{ci_str:<22} {r['p']:>10.2e} {r['win_rate']:>5.1f}% "
                  f"{r['n']:>6d} {flag}")
        print()

    # OLS weight summary
    print("=" * 120)
    print("  OLS Weight Summary (top 10 per horizon)")
    print("=" * 120)
    for h, res in ols_results.items():
        print(f"\n  h={h}d, in-sample IC={res['in_sample_ic']:+.4f}, n={res['n_obs']:,}")
        sorted_w = sorted(res['weights'].items(), key=lambda x: abs(x[1]), reverse=True)
        print(f"  {'Signal':<30} {'Weight':>10} {'Domain':>8}")
        print(f"  {'-'*50}")
        for sig, w in sorted_w[:10]:
            domain = 'TECH' if sig in TECH_SIGNAL_LABELS else 'CHIP'
            label = ALL_SIGNAL_LABELS.get(sig, sig)
            print(f"  {label:<30} {w:>+10.5f} {domain:>8}")

    print(f"\nTotal time: {(time.time()-t0)/60:.1f} min")


if __name__ == '__main__':
    main()
