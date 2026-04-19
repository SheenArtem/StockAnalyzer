"""
vf_vb_validation.py - VF-VB 體質門檻驗證 (F-Score / Z-Score)

驗證:
  - F-Score IR (目前 Value 門檻 F≥7，對比 F≥5/6/8)
  - Z-Score IR（目前 Value 給 -20 破產罰分）
  - Decile spread (top vs bottom)
  - Threshold hit rate

IR 門檻 (IC信心):
  ≥0.3 = A; 0.1-0.3 = B; 0.05-0.1 = C; <0.05 = D (無 alpha)
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

SNAPSHOT_PATH = ROOT / "data_cache" / "backtest" / "trade_journal_value_tw_snapshot.parquet"


def ic_analysis(df: pd.DataFrame, factor: str, horizon: int) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    if sub.empty:
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    weekly = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 10:
            continue
        rho, _ = stats.spearmanr(grp[factor], grp[target])
        if not np.isnan(rho):
            weekly.append(rho)
    if not weekly:
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    ic_arr = np.array(weekly)
    return {
        'IC': ic_arr.mean(),
        'std': ic_arr.std(ddof=1) if len(ic_arr) > 1 else np.nan,
        'IR': ic_arr.mean() / ic_arr.std(ddof=1) if len(ic_arr) > 1 and ic_arr.std(ddof=1) > 0 else np.nan,
        'weeks': len(ic_arr),
    }


def threshold_test(df: pd.DataFrame, factor: str, threshold: float,
                    horizon: int, op: str = 'ge') -> dict:
    """op='ge' means pass = factor >= threshold."""
    target = f'fwd_{horizon}d'
    sub = df[[factor, target]].dropna()
    if sub.empty:
        return {}

    if op == 'ge':
        pass_mask = sub[factor] >= threshold
    elif op == 'le':
        pass_mask = sub[factor] <= threshold
    else:
        pass_mask = sub[factor] > threshold

    passed = sub[pass_mask]
    failed = sub[~pass_mask]
    return {
        'pass_n': len(passed),
        'fail_n': len(failed),
        'pass_ret': passed[target].mean() if len(passed) else np.nan,
        'fail_ret': failed[target].mean() if len(failed) else np.nan,
        'pass_hit': (passed[target] > 0).mean() if len(passed) else np.nan,
        'fail_hit': (failed[target] > 0).mean() if len(failed) else np.nan,
    }


def grade(ir: float) -> str:
    if pd.isna(ir):
        return 'N/A'
    if abs(ir) >= 0.3:
        return 'A' if ir > 0 else 'A (rev)'
    elif abs(ir) >= 0.1:
        return 'B' if ir > 0 else 'B (rev)'
    elif abs(ir) >= 0.05:
        return 'C'
    return 'D'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", default=str(SNAPSHOT_PATH))
    ap.add_argument("--horizon", type=int, default=60)
    args = ap.parse_args()

    df = pd.read_parquet(args.snapshot)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    print(f"Loaded: {len(df)} rows, {df['week_end_date'].nunique()} weeks, "
          f"{df['stock_id'].nunique()} unique stocks")
    print(f"Horizon: fwd_{args.horizon}d\n")

    # === Layer 1: IC ===
    print("=" * 70)
    print("Layer 1: IC / IR (higher f_score = better expected)")
    print("=" * 70)
    print(f"{'Factor':<18}{'IC':>10}{'IR':>8}{'Weeks':>8}{'Grade':>12}")
    print("-" * 70)
    for factor in ['f_score', 'z_score', 'quality_score']:
        if factor not in df.columns:
            continue
        res = ic_analysis(df, factor, args.horizon)
        print(f"{factor:<18}{res['IC']:>10.4f}{res['IR']:>8.3f}{res['weeks']:>8}{grade(res['IR']):>12}")
    print()

    # === Layer 2: Threshold ===
    print("=" * 70)
    print(f"Layer 2: F-Score 門檻測試 (pass = F >= threshold)")
    print("=" * 70)
    print(f"{'Threshold':<12}{'#Pass':>8}{'#Fail':>8}{'PassRet':>10}{'FailRet':>10}"
          f"{'PassHit':>10}{'FailHit':>10}{'Diff':>10}")
    print("-" * 90)
    for th in [5, 6, 7, 8]:
        r = threshold_test(df, 'f_score', th, args.horizon, op='ge')
        if r:
            diff = (r['pass_ret'] or 0) - (r['fail_ret'] or 0)
            print(f"F >= {th:<8}{r['pass_n']:>8}{r['fail_n']:>8}"
                  f"{r['pass_ret']:>10.2%}{r['fail_ret']:>10.2%}"
                  f"{r['pass_hit']:>10.1%}{r['fail_hit']:>10.1%}{diff:>10.2%}")
    print()

    # === Layer 3: Z-Score distress threshold ===
    print("=" * 70)
    print(f"Layer 3: Z-Score 破產區測試 (fail = Z < threshold)")
    print("=" * 70)
    print(f"{'Threshold':<14}{'#Pass':>8}{'#Fail':>8}{'PassRet':>10}{'FailRet':>10}{'Diff':>10}")
    print("-" * 70)
    for th in [1.8, 2.6, 3.0]:
        r = threshold_test(df, 'z_score', th, args.horizon, op='ge')
        if r:
            diff = (r['pass_ret'] or 0) - (r['fail_ret'] or 0)
            print(f"Z >= {th:<10}{r['pass_n']:>8}{r['fail_n']:>8}"
                  f"{(r['pass_ret'] or 0):>10.2%}{(r['fail_ret'] or 0):>10.2%}{diff:>10.2%}")
    print()

    # === Layer 4: F-Score decile spread ===
    print("=" * 70)
    print("Layer 4: Decile spread (F-Score top 10% vs bottom 10%)")
    print("=" * 70)
    target = f'fwd_{args.horizon}d'
    sub = df[['f_score', target, 'week_end_date']].dropna()
    weekly_r = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 30:
            continue
        grp_s = grp.sort_values('f_score').reset_index(drop=True)
        n = len(grp_s)
        cut = max(1, n // 10)
        bottom = grp_s.iloc[:cut][target].mean()
        top = grp_s.iloc[-cut:][target].mean()
        weekly_r.append({'bottom': bottom, 'top': top, 'spread': top - bottom})
    if weekly_r:
        r = pd.DataFrame(weekly_r)
        print(f"  Top 10% avg ret: {r['top'].mean():.2%}")
        print(f"  Bottom 10% avg ret: {r['bottom'].mean():.2%}")
        print(f"  Spread: {r['spread'].mean():.2%} (winrate {(r['spread']>0).mean():.1%})")
    print()


if __name__ == "__main__":
    main()
