"""
VF-G3 Part 2: analysis_engine.py REGIME_GROUP_WEIGHTS + REGIME_ADDON_MULT validation

Validate 4-regime x 3-group = 12 multipliers in analysis_engine.py:
  REGIME_GROUP_WEIGHTS = {
      'trending': {'trend': 1.3, 'momentum': 1.0, 'volume': 0.7},
      'ranging':  {'trend': 0.7, 'momentum': 1.0, 'volume': 1.3},
      'volatile': {'trend': 0.9, 'momentum': 0.9, 'volume': 1.2},
      'neutral':  {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0},
  }

Plus REGIME_ADDON_MULT (chip/sentiment/revenue/etf caps).

METHODOLOGY
-----------
Trade journal `trade_journal_qm_tw_pure_right.parquet` (9263 picks, VF-6 winner)
carries qm_score = F/9*50 + B/100*30 + T/10*20 plus the live 'regime' label.

Mapping to analysis_engine groups:
  - f_score       -> quality anchor (no direct group analog; keep fixed 50 weight)
  - body_score    -> 'momentum' group proxy (candle body quality)
  - trend_score   -> 'trend' group
  - no 'volume' group in QM -> omit (tested later if needed)

We thus test whether re-weighting (trend, body) by regime improves cross-sectional
selection, i.e. whether picks at the top after regime-reweighting outperform picks
at the top of the current (flat) QM score.

VERSIONS
--------
  V1 current : apply REGIME_GROUP_WEIGHTS to (trend, body)
  V2 all 1.0 : no regime overlay (plain QM)  -- the STATUS QUO in journal
  V3 grid    : per-regime sweep over (w_trend, w_body) in {0.5,0.7,1.0,1.3,1.5}
               find best combo per regime by IC

METRICS
-------
  - Spearman rank IC vs fwd_20d and fwd_40d (per week, then mean + IR)
  - Top-20 portfolio mean fwd_20d and Sharpe by regime
  - Walk-forward: 12 weeks train (find best w) / 4 weeks test (apply); stride 4

DECISION RULE
-------------
  - V1 vs V2 IC delta < 0.005 AND top20 return delta < 0.3% -> CUT (set all 1.0)
  - V1 > V2 by IC >= 0.01 AND walk-forward OOS stable       -> KEEP
  - V3 better than V1 AND walk-forward consistent           -> REPLACE with V3 best

OUTPUTS
-------
  reports/vfg3_part2_regime_selection_mult.md
  reports/vfg3_part2_versions.csv
  reports/vfg3_part2_by_regime.csv
  reports/vfg3_part2_walkforward.csv
  reports/vfg3_part2_grid.csv
"""

from __future__ import annotations

import sys
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(r"c:\GIT\StockAnalyzer")
# 2026-04-21: swapped from trade_journal_qm_tw_pure_right.parquet (5yr only) to
# trade_journal_qm_tw.parquet (mixed 10.5yr) for the VF-G 10.5yr rerun. Cross-sectional
# IC tests regime re-weighting on trend/body scores, which exist in both journals.
JOURNAL = ROOT / "data_cache" / "backtest" / "trade_journal_qm_tw.parquet"
OUT_MD = ROOT / "reports" / "vfg3_part2_regime_selection_mult.md"
OUT_VERSIONS = ROOT / "reports" / "vfg3_part2_versions.csv"
OUT_REGIME = ROOT / "reports" / "vfg3_part2_by_regime.csv"
OUT_WF = ROOT / "reports" / "vfg3_part2_walkforward.csv"
OUT_GRID = ROOT / "reports" / "vfg3_part2_grid.csv"

REGIMES = ['trending', 'ranging', 'volatile', 'neutral']

# Current live weights (analysis_engine.py lines 31-36)
V1_CURRENT = {
    'trending': {'trend': 1.3, 'momentum': 1.0, 'volume': 0.7},
    'ranging':  {'trend': 0.7, 'momentum': 1.0, 'volume': 1.3},
    'volatile': {'trend': 0.9, 'momentum': 0.9, 'volume': 1.2},
    'neutral':  {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0},
}
# No regime overlay
V2_ALL_ONE = {r: {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0} for r in REGIMES}

# QM component base weights (F/B/T = 50/30/20 from momentum_screener)
W_F_BASE = 50.0
W_B_BASE = 30.0
W_T_BASE = 20.0

# Grid for V3 search
WEIGHT_GRID = [0.5, 0.7, 1.0, 1.3, 1.5]


def compute_regime_score(df: pd.DataFrame, weights: dict) -> pd.Series:
    """Recompute qm_score with regime-dependent multipliers on trend/body.

    score = (W_F * f/9) + (W_B * gw[momentum] * body/100) + (W_T * gw[trend] * trend/10)
    Normalized so w_sum is preserved per regime.
    """
    out = np.full(len(df), np.nan)
    regimes = df['regime'].values
    f = df['f_score'].values / 9.0
    b = df['body_score'].values / 100.0
    t = df['trend_score'].values / 10.0
    for i, reg in enumerate(regimes):
        gw = weights.get(reg, weights['neutral'])
        w_t = W_T_BASE * gw['trend']
        w_b = W_B_BASE * gw['momentum']
        w_f = W_F_BASE  # quality anchor not regime-scaled
        # Normalize so (w_f + w_b + w_t) keeps sum = 100 base
        scale = (W_F_BASE + W_B_BASE + W_T_BASE) / (w_f + w_b + w_t)
        out[i] = (w_f * f[i] + w_b * b[i] + w_t * t[i]) * scale
    return pd.Series(out, index=df.index)


def weekly_ic(df: pd.DataFrame, score_col: str, fwd_col: str) -> tuple[float, float, int]:
    """Spearman rank IC per week_end_date, return mean, IR (mean/std), n_weeks."""
    weekly = []
    for wk, g in df.groupby('week_end_date'):
        if len(g) < 5:
            continue
        s = g[score_col].values
        r = g[fwd_col].values
        mask = ~(np.isnan(s) | np.isnan(r))
        if mask.sum() < 5:
            continue
        ic, _ = spearmanr(s[mask], r[mask])
        if not np.isnan(ic):
            weekly.append(ic)
    if not weekly:
        return np.nan, np.nan, 0
    arr = np.array(weekly)
    mean = arr.mean()
    std = arr.std(ddof=1) if len(arr) > 1 else np.nan
    ir = mean / std * np.sqrt(52) if std and std > 0 else np.nan
    return mean, ir, len(arr)


def topn_portfolio(df: pd.DataFrame, score_col: str, fwd_col: str, n: int = 20) -> tuple[float, float, int]:
    """Each week take top-N by score_col, compute equal-weight return. Return mean, Sharpe, n_weeks."""
    rets = []
    for wk, g in df.groupby('week_end_date'):
        if len(g) < n:
            continue
        top = g.nlargest(n, score_col)
        r = top[fwd_col].dropna()
        if len(r) > 0:
            rets.append(r.mean())
    if not rets:
        return np.nan, np.nan, 0
    arr = np.array(rets)
    mean = arr.mean()
    std = arr.std(ddof=1) if len(arr) > 1 else np.nan
    sharpe = mean / std * np.sqrt(52 / 4) if std and std > 0 else np.nan  # 4-week horizon ~ 13/yr
    return mean, sharpe, len(arr)


def evaluate_version(df: pd.DataFrame, weights: dict, label: str) -> dict:
    """Compute IC and top-N metrics for a regime-weights version."""
    df = df.copy()
    df['_score'] = compute_regime_score(df, weights)
    ic20, ir20, n20 = weekly_ic(df, '_score', 'fwd_20d')
    ic40, ir40, n40 = weekly_ic(df, '_score', 'fwd_40d')
    r20_top, s20_top, nt20 = topn_portfolio(df, '_score', 'fwd_20d', n=20)
    r40_top, s40_top, nt40 = topn_portfolio(df, '_score', 'fwd_40d', n=20)
    return {
        'version': label,
        'ic_20d': ic20, 'ir_20d': ir20, 'n_weeks_20d': n20,
        'ic_40d': ic40, 'ir_40d': ir40, 'n_weeks_40d': n40,
        'top20_ret_20d': r20_top, 'top20_sharpe_20d': s20_top,
        'top20_ret_40d': r40_top, 'top20_sharpe_40d': s40_top,
    }


def per_regime_breakdown(df: pd.DataFrame, v1: dict, v2: dict) -> pd.DataFrame:
    """Break down V1 vs V2 by regime (within picks of that regime)."""
    df = df.copy()
    df['_s_v1'] = compute_regime_score(df, v1)
    df['_s_v2'] = compute_regime_score(df, v2)
    rows = []
    for reg in REGIMES:
        sub = df[df['regime'] == reg]
        if len(sub) < 30:
            continue
        for fwd in ['fwd_20d', 'fwd_40d']:
            ic_v1, ir_v1, nw_v1 = weekly_ic(sub, '_s_v1', fwd)
            ic_v2, ir_v2, nw_v2 = weekly_ic(sub, '_s_v2', fwd)
            t_v1, sh_v1, _ = topn_portfolio(sub, '_s_v1', fwd, n=10)
            t_v2, sh_v2, _ = topn_portfolio(sub, '_s_v2', fwd, n=10)
            rows.append({
                'regime': reg, 'fwd': fwd, 'n_picks': len(sub),
                'ic_v1': ic_v1, 'ic_v2': ic_v2, 'ic_delta': ic_v1 - ic_v2,
                'ir_v1': ir_v1, 'ir_v2': ir_v2,
                'top10_ret_v1': t_v1, 'top10_ret_v2': t_v2,
                'top10_delta_ret': (t_v1 - t_v2) if pd.notna(t_v1) and pd.notna(t_v2) else np.nan,
            })
    return pd.DataFrame(rows)


def grid_search(df: pd.DataFrame) -> pd.DataFrame:
    """For each regime subset, find (w_trend, w_body) maximizing IC on fwd_20d."""
    rows = []
    for reg in REGIMES:
        sub = df[df['regime'] == reg]
        if len(sub) < 30:
            continue
        best = {'ic': -np.inf, 'w_t': None, 'w_b': None}
        for w_t, w_b in product(WEIGHT_GRID, WEIGHT_GRID):
            w = {r: {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0} for r in REGIMES}
            w[reg] = {'trend': w_t, 'momentum': w_b, 'volume': 1.0}
            s = compute_regime_score(sub, w)
            tmp = sub.copy()
            tmp['_score'] = s
            ic, ir, _ = weekly_ic(tmp, '_score', 'fwd_20d')
            rows.append({'regime': reg, 'w_trend': w_t, 'w_body': w_b,
                         'ic_20d': ic, 'ir_20d': ir})
            if pd.notna(ic) and ic > best['ic']:
                best = {'ic': ic, 'w_t': w_t, 'w_b': w_b}
        rows.append({'regime': reg + '_BEST', 'w_trend': best['w_t'],
                     'w_body': best['w_b'], 'ic_20d': best['ic'], 'ir_20d': np.nan})
    return pd.DataFrame(rows)


def walkforward(df: pd.DataFrame, train_w: int = 12, test_w: int = 4, stride: int = 4) -> pd.DataFrame:
    """Walk-forward: on train window, pick best per-regime weights on grid; test on next window.
    Compare test-window IC of V1-current vs best-trained vs V2-flat.
    """
    weeks = sorted(df['week_end_date'].unique())
    rows = []
    i = 0
    while i + train_w + test_w <= len(weeks):
        train_weeks = weeks[i:i + train_w]
        test_weeks = weeks[i + train_w:i + train_w + test_w]
        train = df[df['week_end_date'].isin(train_weeks)]
        test = df[df['week_end_date'].isin(test_weeks)]

        # Train: per-regime best (w_t, w_b) by IC on fwd_20d within train
        best_weights = {r: {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0} for r in REGIMES}
        for reg in REGIMES:
            sub = train[train['regime'] == reg]
            if len(sub) < 20:
                continue
            best_ic = -np.inf
            best_pair = (1.0, 1.0)
            for w_t, w_b in product(WEIGHT_GRID, WEIGHT_GRID):
                w = {r: {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0} for r in REGIMES}
                w[reg] = {'trend': w_t, 'momentum': w_b, 'volume': 1.0}
                s = compute_regime_score(sub, w)
                tmp = sub.copy()
                tmp['_score'] = s
                ic, _, _ = weekly_ic(tmp, '_score', 'fwd_20d')
                if pd.notna(ic) and ic > best_ic:
                    best_ic = ic
                    best_pair = (w_t, w_b)
            best_weights[reg] = {'trend': best_pair[0], 'momentum': best_pair[1], 'volume': 1.0}

        # Evaluate V1, V2, V_TRAINED on test window IC
        for label, w in [('v1_current', V1_CURRENT),
                         ('v2_flat', V2_ALL_ONE),
                         ('v3_trained', best_weights)]:
            s = compute_regime_score(test, w)
            tmp = test.copy()
            tmp['_score'] = s
            ic20, ir20, nw = weekly_ic(tmp, '_score', 'fwd_20d')
            t20, s20, _ = topn_portfolio(tmp, '_score', 'fwd_20d', n=20)
            rows.append({
                'train_start': train_weeks[0], 'train_end': train_weeks[-1],
                'test_start': test_weeks[0], 'test_end': test_weeks[-1],
                'version': label,
                'test_ic_20d': ic20, 'test_ir_20d': ir20, 'test_n_weeks': nw,
                'test_top20_ret': t20,
                'best_weights': str(best_weights) if label == 'v3_trained' else '',
            })
        i += stride
    return pd.DataFrame(rows)


def main():
    print(f"[VF-G3 Part 2] loading journal: {JOURNAL}")
    df = pd.read_parquet(JOURNAL)
    df = df.dropna(subset=['regime', 'f_score', 'body_score', 'trend_score'])
    print(f"  rows: {len(df)}  weeks: {df['week_end_date'].nunique()}  regimes: {df['regime'].value_counts().to_dict()}")

    # --- A) Overall versions V1 vs V2 ---
    print("\n[A] overall version comparison...")
    v1 = evaluate_version(df, V1_CURRENT, 'V1_current')
    v2 = evaluate_version(df, V2_ALL_ONE, 'V2_flat')
    versions = pd.DataFrame([v1, v2])
    versions.to_csv(OUT_VERSIONS, index=False)
    print(versions.to_string(index=False))

    # --- B) Per-regime breakdown ---
    print("\n[B] per-regime breakdown...")
    regime_df = per_regime_breakdown(df, V1_CURRENT, V2_ALL_ONE)
    regime_df.to_csv(OUT_REGIME, index=False)
    print(regime_df.to_string(index=False))

    # --- C) Grid search (in-sample best per regime) ---
    print("\n[C] grid search (in-sample)...")
    grid = grid_search(df)
    grid.to_csv(OUT_GRID, index=False)
    print(grid[grid['regime'].str.endswith('_BEST')].to_string(index=False))

    # --- D) Walk-forward ---
    print("\n[D] walk-forward (12w train / 4w test, stride=4)...")
    wf = walkforward(df)
    wf.to_csv(OUT_WF, index=False)
    if len(wf) > 0:
        summary = wf.groupby('version').agg(
            mean_test_ic=('test_ic_20d', 'mean'),
            median_test_ic=('test_ic_20d', 'median'),
            pct_positive_ic=('test_ic_20d', lambda x: (x > 0).mean()),
            mean_top20_ret=('test_top20_ret', 'mean'),
            n_windows=('test_ic_20d', 'count'),
        )
        print(summary.to_string())
    else:
        summary = pd.DataFrame()
        print("  (no WF windows generated)")

    # --- E) Decision ---
    print("\n[E] decision rule...")
    delta_ic20 = v1['ic_20d'] - v2['ic_20d']
    delta_ret20 = v1['top20_ret_20d'] - v2['top20_ret_20d']
    print(f"  V1 IC_20d = {v1['ic_20d']:.4f}   V2 IC_20d = {v2['ic_20d']:.4f}   delta = {delta_ic20:+.4f}")
    print(f"  V1 top20_ret_20d = {v1['top20_ret_20d']*100:.2f}%   V2 = {v2['top20_ret_20d']*100:.2f}%   delta = {delta_ret20*100:+.2f}%")

    if len(summary) > 0 and 'v3_trained' in summary.index:
        v1_wf = summary.loc['v1_current', 'mean_test_ic']
        v2_wf = summary.loc['v2_flat', 'mean_test_ic']
        v3_wf = summary.loc['v3_trained', 'mean_test_ic']
        print(f"  WF mean test IC: V1={v1_wf:+.4f} V2={v2_wf:+.4f} V3_trained={v3_wf:+.4f}")

    # Decision
    if abs(delta_ic20) < 0.005 and abs(delta_ret20) < 0.003:
        decision = "CUT: set GROUP_WEIGHTS all 1.0 (no evidence of regime alpha in selection)"
        grade = "D"
    elif delta_ic20 >= 0.01 and delta_ret20 >= 0.005:
        decision = "KEEP: V1 shows material IC + return edge"
        grade = "A/B"
    else:
        decision = "D-grade KEEP: marginal or inconsistent -- prefer simpler V2"
        grade = "D"
    print(f"  >>> decision: {decision} ({grade})")

    # --- F) Markdown summary ---
    lines = []
    lines.append("# VF-G3 Part 2: REGIME_GROUP_WEIGHTS + ADDON_MULT validation")
    lines.append("")
    lines.append(f"**Universe**: {JOURNAL.name} -- {len(df):,} picks, {df['week_end_date'].nunique()} weeks")
    lines.append("")
    lines.append("## TL;DR")
    lines.append("")
    lines.append(f"- **Decision**: {decision}")
    lines.append(f"- **Grade**: {grade}")
    lines.append(f"- V1 vs V2 IC_20d delta = {delta_ic20:+.4f}  (V1={v1['ic_20d']:+.4f}, V2={v2['ic_20d']:+.4f})")
    lines.append(f"- V1 vs V2 top-20 fwd_20d return delta = {delta_ret20*100:+.2f}% (V1={v1['top20_ret_20d']*100:.2f}%, V2={v2['top20_ret_20d']*100:.2f}%)")
    if len(summary) > 0 and 'v3_trained' in summary.index:
        lines.append(f"- Walk-forward mean test IC_20d: V1={summary.loc['v1_current','mean_test_ic']:+.4f}  V2={summary.loc['v2_flat','mean_test_ic']:+.4f}  V3_trained={summary.loc['v3_trained','mean_test_ic']:+.4f}")
    lines.append("")
    lines.append("## Methodology")
    lines.append("")
    lines.append("- Trade journal `trade_journal_qm_tw_pure_right.parquet` (VF-6 winner)")
    lines.append("- Components: `f_score` (quality anchor, base weight 50), `body_score` (momentum proxy, 30), `trend_score` (trend, 20)")
    lines.append("- Regime weights applied multiplicatively to (trend, body) and renormalized so total = 100")
    lines.append("- Metrics: weekly Spearman IC vs fwd_20d/fwd_40d; top-20 equal-weight portfolio returns")
    lines.append("")
    def _to_md(df_, floatfmt='.4f'):
        if df_ is None or len(df_) == 0:
            return "(empty)"
        cols = list(df_.columns) if isinstance(df_, pd.DataFrame) else [df_.name]
        hdr = "| " + " | ".join(map(str, cols)) + " |"
        sep = "| " + " | ".join(['---'] * len(cols)) + " |"
        rows_ = [hdr, sep]
        iterable = df_.itertuples(index=False) if isinstance(df_, pd.DataFrame) else df_.items()
        for row in iterable:
            vals = []
            for v in (row if isinstance(df_, pd.DataFrame) else row):
                if isinstance(v, float):
                    vals.append(f"{v:{floatfmt}}" if not np.isnan(v) else "nan")
                else:
                    vals.append(str(v))
            rows_.append("| " + " | ".join(vals) + " |")
        return "\n".join(rows_)

    lines.append("## V1 vs V2 (overall)")
    lines.append("")
    lines.append(_to_md(versions))
    lines.append("")
    lines.append("## Per-regime breakdown")
    lines.append("")
    lines.append(_to_md(regime_df))
    lines.append("")
    lines.append("## Walk-forward summary")
    lines.append("")
    if len(summary) > 0:
        s_df = summary.reset_index()
        lines.append(_to_md(s_df))
    else:
        lines.append("(no WF windows)")
    lines.append("")
    lines.append("## Grid search best per regime (in-sample)")
    lines.append("")
    best_rows = grid[grid['regime'].str.endswith('_BEST')]
    lines.append(_to_md(best_rows))
    lines.append("")
    lines.append("## Suggested diff (if CUT)")
    lines.append("")
    lines.append("```python")
    lines.append("# analysis_engine.py lines 31-43 -> replace with:")
    lines.append("REGIME_GROUP_WEIGHTS = {r: {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0}")
    lines.append("                        for r in ('trending', 'ranging', 'volatile', 'neutral')}")
    lines.append("REGIME_ADDON_MULT    = {r: {'chip': 1.0, 'sentiment': 1.0, 'revenue': 1.0, 'etf': 1.0}")
    lines.append("                        for r in ('trending', 'ranging', 'volatile', 'neutral')}")
    lines.append("```")
    OUT_MD.write_text("\n".join(lines), encoding='utf-8')
    print(f"\n[output] {OUT_MD}")
    print(f"[output] {OUT_VERSIONS}")
    print(f"[output] {OUT_REGIME}")
    print(f"[output] {OUT_GRID}")
    print(f"[output] {OUT_WF}")


if __name__ == "__main__":
    main()
