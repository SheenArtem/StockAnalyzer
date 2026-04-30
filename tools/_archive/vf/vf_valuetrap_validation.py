"""
vf_valuetrap_validation.py - 驗證高槓桿股是否為 Value trap

Hypothesis: 高負債公司在熊市更容易「便宜變更便宜」（Value trap）。
加入 Debt/Equity 過濾或扣分可能改善熊市表現。

測試:
  - IC: debt_to_equity vs fwd_60d 有反 alpha?
  - Threshold: D/E > 2 / > 1.5 排除是否改善平均報酬
  - 熊市期 (2022-01 ~ 2022-10) 特別檢查

從 balance_sheet 算 D/E = Liabilities / Equity per stock × quarter.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_cache" / "backtest"


def build_de_ratio() -> pd.DataFrame:
    """Compute debt_to_equity from balance sheet per (stock_id, date)."""
    bal = pd.read_parquet(DATA_DIR / "financials_balance.parquet")
    bal['date'] = pd.to_datetime(bal['date'])
    bal['value'] = pd.to_numeric(bal['value'], errors='coerce')
    liab = bal[bal['type'] == 'Liabilities'][['stock_id', 'date', 'value']].rename(
        columns={'value': 'liab'})
    eq = bal[bal['type'] == 'Equity'][['stock_id', 'date', 'value']].rename(
        columns={'value': 'eq'})
    merged = liab.merge(eq, on=['stock_id', 'date'])
    merged = merged[(merged['eq'] > 0)]
    merged['de'] = merged['liab'] / merged['eq']
    return merged[['stock_id', 'date', 'de']].sort_values(['stock_id', 'date'])


def ic_analysis(df: pd.DataFrame, factor: str, horizon: int) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    weekly = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 10:
            continue
        rho, _ = stats.spearmanr(grp[factor], grp[target])
        if not np.isnan(rho):
            weekly.append(rho)
    if not weekly:
        return {'IC': np.nan, 'IR': np.nan}
    arr = np.array(weekly)
    std = arr.std(ddof=1)
    return {'IC': arr.mean(), 'IR': arr.mean()/std if std > 0 else np.nan,
            'weeks': len(arr)}


def threshold_test(df: pd.DataFrame, threshold: float, horizon: int, op: str) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[['de', target]].dropna()
    if op == 'gt':
        pass_mask = sub['de'] > threshold
    else:
        pass_mask = sub['de'] < threshold
    passed = sub[pass_mask]
    failed = sub[~pass_mask]
    return {
        'pass_n': len(passed),
        'pass_ret': passed[target].mean() if len(passed) else np.nan,
        'fail_ret': failed[target].mean() if len(failed) else np.nan,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot",
                    default=str(DATA_DIR / "trade_journal_value_tw_snapshot.parquet"))
    ap.add_argument("--horizon", type=int, default=60)
    args = ap.parse_args()

    print("Building D/E ratio...")
    de = build_de_ratio()
    print(f"  D/E rows: {len(de)}, {de['stock_id'].nunique()} stocks")
    print(f"  D/E quantiles: p25={de['de'].quantile(0.25):.2f}, "
          f"median={de['de'].median():.2f}, p75={de['de'].quantile(0.75):.2f}, "
          f"p95={de['de'].quantile(0.95):.2f}")
    print()

    # Load snapshot
    snap = pd.read_parquet(args.snapshot)
    snap['week_end_date'] = pd.to_datetime(snap['week_end_date'])

    # PIT lookup of D/E per (stock, week_end_date)
    de_sorted = de.sort_values(['stock_id', 'date'])
    pit_rows = []
    for d in snap['week_end_date'].unique():
        pit = de_sorted[de_sorted['date'] <= d].groupby('stock_id').last().reset_index()
        pit['week_end_date'] = d
        pit_rows.append(pit[['stock_id', 'week_end_date', 'de']])
    pit_all = pd.concat(pit_rows, ignore_index=True)
    merged = snap.merge(pit_all, on=['stock_id', 'week_end_date'], how='left')

    print(f"Snapshot rows with D/E: {merged['de'].notna().sum()} / {len(merged)}")
    print()

    # === Layer 1: IC ===
    print("=" * 70)
    print("Layer 1: D/E IC (negative IR = higher leverage 預測更差)")
    print("=" * 70)
    res = ic_analysis(merged, 'de', args.horizon)
    grade = 'A' if abs(res['IR']) >= 0.3 else 'B' if abs(res['IR']) >= 0.1 else 'C' if abs(res['IR']) >= 0.05 else 'D'
    direction = ' (low D/E better)' if res['IR'] < 0 else ' (high D/E better?)'
    print(f"D/E ratio IC={res['IC']:.4f}, IR={res['IR']:.3f} ({grade}{direction})")
    print()

    # === Layer 2: Threshold ===
    print("=" * 70)
    print(f"Layer 2: D/E Threshold (pass = D/E <= threshold)")
    print("=" * 70)
    print(f"{'Threshold':<15}{'#Pass':>8}{'#Fail':>8}{'PassRet':>10}{'FailRet':>10}{'Diff':>10}")
    print("-" * 70)
    for th in [1.0, 1.5, 2.0, 3.0]:
        r = threshold_test(merged, th, args.horizon, op='lt')
        diff = r['pass_ret'] - r['fail_ret']
        print(f"D/E <= {th:<8}{r['pass_n']:>8}{len(merged) - r['pass_n']:>8}"
              f"{r['pass_ret']:>10.2%}{r['fail_ret']:>10.2%}{diff:>10.2%}")
    print()

    # === Layer 3: Bear market test ===
    print("=" * 70)
    print("Layer 3: 熊市 (2022-01 to 2022-10) D/E 影響")
    print("=" * 70)
    bear = merged[(merged['week_end_date'] >= '2022-01-01') &
                   (merged['week_end_date'] < '2022-11-01')].copy()
    if not bear.empty:
        print(f"Bear sample: {len(bear)} rows")
        for th in [1.5, 2.0, 3.0]:
            sub = bear[bear['de'].notna()].copy()
            target = f'fwd_{args.horizon}d'
            lo = sub[sub['de'] <= th][target].mean()
            hi = sub[sub['de'] > th][target].mean()
            print(f"  D/E <= {th}: {lo:.2%} / D/E > {th}: {hi:.2%} "
                  f"(gap={lo-hi:+.2%})")
    print()

    # Summary
    print("=" * 70)
    print("Decision")
    print("=" * 70)
    if res['IR'] < -0.1:
        print(f"→ IR={res['IR']:.3f} B 反轉，**應加 D/E > 2 過濾**")
    elif res['IR'] < -0.05:
        print(f"→ IR={res['IR']:.3f} C 級，考慮加過濾")
    elif res['IR'] > 0.05:
        print(f"→ IR={res['IR']:.3f} (positive!) 高 D/E 反而好，保留槓桿股")
    else:
        print(f"→ IR={res['IR']:.3f} D 級無信號，不需 D/E 過濾")


if __name__ == "__main__":
    main()
