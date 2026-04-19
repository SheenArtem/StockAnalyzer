"""
vf_vf_validation.py - VF-VF Value 5 面向權重驗證
===============================================
測試 Value 的 30/25/15/15/15 權重是否最佳，對比其他權重方案。

比較的權重方案:
  V1 current:  valuation=30%, quality=25%, revenue=15%, technical=15%, smart_money=15%
  V2 equal:    20/20/20/20/20
  V3 val-heavy: 50/20/10/10/10
  V4 qm-like:  0/50/30/20/0  (F50/body30/trend20 without smart_money)
  V5 quality:  20/40/20/15/5
  V6 no_sm:    35/30/15/20/0 (remove unproven smart_money)

衡量:
  - Sharpe ratio (mean / std of weekly basket return)
  - IC (per-week rank corr between value_score and fwd_60d)
  - Top 50 avg fwd return
  - Consistency across years / regimes

用法:
  python tools/vf_vf_validation.py --journal ... --horizon 60
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

JOURNAL_PATH = ROOT / "data_cache" / "backtest" / "trade_journal_value_tw.parquet"

# (name, weights dict) — must sum to ~1.0
SCHEMES = [
    ("V1_current",    {'valuation': 0.30, 'quality': 0.25, 'revenue': 0.15, 'technical': 0.15, 'smart_money': 0.15}),
    ("V2_equal",      {'valuation': 0.20, 'quality': 0.20, 'revenue': 0.20, 'technical': 0.20, 'smart_money': 0.20}),
    ("V3_val_heavy",  {'valuation': 0.50, 'quality': 0.20, 'revenue': 0.10, 'technical': 0.10, 'smart_money': 0.10}),
    ("V4_qm_like",    {'valuation': 0.00, 'quality': 0.50, 'revenue': 0.30, 'technical': 0.20, 'smart_money': 0.00}),
    ("V5_quality",    {'valuation': 0.20, 'quality': 0.40, 'revenue': 0.20, 'technical': 0.15, 'smart_money': 0.05}),
    ("V6_no_sm",      {'valuation': 0.35, 'quality': 0.30, 'revenue': 0.15, 'technical': 0.20, 'smart_money': 0.00}),
]


def load_journal(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    return df


def compute_score(df: pd.DataFrame, weights: dict) -> pd.Series:
    """Compute composite value_score from 5-dim raw scores using given weights."""
    return (
        df['valuation_s'] * weights['valuation'] +
        df['quality_s'] * weights['quality'] +
        df['revenue_s'] * weights['revenue'] +
        df['technical_s'] * weights['technical'] +
        df['smart_money_s'] * weights['smart_money']
    )


def basket_returns(df: pd.DataFrame, score_col: str, horizon: int,
                    top_n: int = 50) -> pd.DataFrame:
    """For each week, take top_n by score_col and compute equal-weighted basket return."""
    target = f'fwd_{horizon}d'
    results = []
    for wd, grp in df.groupby('week_end_date'):
        sub = grp.dropna(subset=[score_col, target])
        if len(sub) < 10:
            continue
        top = sub.nlargest(top_n, score_col)
        basket_ret = top[target].mean()
        results.append({'week': wd, 'ret': basket_ret, 'n': len(top)})
    return pd.DataFrame(results)


def ic_score_vs_fwd(df: pd.DataFrame, score_col: str, horizon: int) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[score_col, target, 'week_end_date']].dropna()
    if sub.empty:
        return {'IC': np.nan, 'IR': np.nan, 'n_weeks': 0}
    weekly = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 10:
            continue
        rho, _ = stats.spearmanr(grp[score_col], grp[target])
        if not np.isnan(rho):
            weekly.append(rho)
    if not weekly:
        return {'IC': np.nan, 'IR': np.nan, 'n_weeks': 0}
    ic_arr = np.array(weekly)
    ic_mean = ic_arr.mean()
    ic_std = ic_arr.std(ddof=1) if len(ic_arr) > 1 else np.nan
    ir = ic_mean / ic_std if ic_std and ic_std > 0 else np.nan
    return {'IC': ic_mean, 'IR': ir, 'n_weeks': len(ic_arr)}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--journal", default=str(JOURNAL_PATH))
    ap.add_argument("--horizon", type=int, default=60)
    ap.add_argument("--top-n", type=int, default=50)
    args = ap.parse_args()

    journal = load_journal(Path(args.journal))
    print(f"Loaded: {len(journal)} picks, {journal['week_end_date'].nunique()} weeks, "
          f"{journal['stock_id'].nunique()} unique stocks")
    print(f"Date range: {journal['week_end_date'].min().date()} - "
          f"{journal['week_end_date'].max().date()}")
    print(f"Horizon: fwd_{args.horizon}d, Top N: {args.top_n}")
    print()

    # CAVEAT: trade_journal already filtered by V1 current (top 50 ranking by V1).
    # Alternative schemes rerank within these top 50 only, which biases toward V1.
    # Full comparison requires simulator to save all-stocks snapshot.
    # For now: treat as limited comparison + IC remains meaningful (cross-sectional).

    print("=" * 80)
    print("Scheme Comparison (re-ranked within existing top 50 pool)")
    print("⚠️  Caveat: current trade_journal was ranked by V1 weights, so alt schemes")
    print("   only re-rank within V1's top 50 universe. Full test needs snapshot data.")
    print("=" * 80)
    print(f"{'Scheme':<15}{'BasketRet':>12}{'Std':>10}{'Sharpe':>10}"
          f"{'IC':>10}{'IR':>8}{'Weeks':>8}")
    print("-" * 80)

    results_summary = []
    for name, weights in SCHEMES:
        col = f'score_{name}'
        journal[col] = compute_score(journal, weights)

        # Basket return
        baskets = basket_returns(journal, col, args.horizon, top_n=args.top_n)
        if baskets.empty:
            continue
        basket_mean = baskets['ret'].mean()
        basket_std = baskets['ret'].std(ddof=1)
        sharpe = basket_mean / basket_std if basket_std > 0 else np.nan

        # IC
        ic = ic_score_vs_fwd(journal, col, args.horizon)

        print(f"{name:<15}{basket_mean:>12.2%}{basket_std:>10.2%}{sharpe:>10.3f}"
              f"{ic['IC']:>10.4f}{ic['IR']:>8.3f}{ic['n_weeks']:>8}")
        results_summary.append({
            'scheme': name, 'basket_ret': basket_mean, 'std': basket_std,
            'sharpe': sharpe, 'ic': ic['IC'], 'ir': ic['IR'], 'n_weeks': ic['n_weeks']
        })

    # Identify winner
    print()
    print("=" * 80)
    print("Decision")
    print("=" * 80)
    r = pd.DataFrame(results_summary)
    if not r.empty:
        best_sharpe = r.loc[r['sharpe'].idxmax(), 'scheme'] if r['sharpe'].notna().any() else None
        best_ir = r.loc[r['ir'].idxmax(), 'scheme'] if r['ir'].notna().any() else None
        best_ret = r.loc[r['basket_ret'].idxmax(), 'scheme'] if r['basket_ret'].notna().any() else None

        print(f"  Best Sharpe:       {best_sharpe}")
        print(f"  Best IR:           {best_ir}")
        print(f"  Best basket ret:   {best_ret}")
        print()
        v1 = r[r['scheme'] == 'V1_current']
        if not v1.empty:
            v1_sharpe = v1['sharpe'].iloc[0]
            print(f"  V1 (current) Sharpe: {v1_sharpe:.3f}")
            # Gap
            max_sharpe = r['sharpe'].max()
            gap = (max_sharpe - v1_sharpe) if pd.notna(max_sharpe) and pd.notna(v1_sharpe) else np.nan
            if pd.notna(gap):
                print(f"  Gap vs best:       {gap:+.3f}")
                if gap < 0.1:
                    print(f"  → V1 competitive (gap < 0.1), no change needed (D 級)")
                elif gap < 0.3:
                    print(f"  → 另案有小優勢 (C 級，需 walk-forward 再驗)")
                else:
                    print(f"  → 另案明顯勝 (A/B 級，值得切換)")


if __name__ == "__main__":
    main()
