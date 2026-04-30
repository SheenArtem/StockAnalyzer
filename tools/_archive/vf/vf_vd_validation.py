"""
vf_vd_validation.py - VF-VD 技術指標 IC 驗證

測試 Value 技術面的 4 個指標:
  - RSI 14: 超賣 / 超買
  - RVOL 20: 量能萎縮 / 爆量
  - 52 週低距離 (low52w_prox): 離底部遠近
  - (squeeze 未納入 simulator，暫缺)

每個指標:
  - IC / IR (spearman rank corr vs fwd_60d)
  - Decile spread
  - Threshold hit rate
  - Grade: A (IR>=0.3) / B (0.1-0.3) / C (0.05-0.1) / D (<0.05)
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

SNAPSHOT_PATH = ROOT / "data_cache" / "backtest" / "trade_journal_value_tw_snapshot.parquet"


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
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    arr = np.array(weekly)
    std = arr.std(ddof=1) if len(arr) > 1 else np.nan
    return {
        'IC': arr.mean(),
        'IR': arr.mean() / std if std and std > 0 else np.nan,
        'weeks': len(arr),
    }


def decile_spread(df: pd.DataFrame, factor: str, horizon: int,
                   higher_better: bool = False) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    results = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 30:
            continue
        grp_s = grp.sort_values(factor, ascending=not higher_better).reset_index(drop=True)
        n = len(grp_s)
        cut = max(1, n // 10)
        # After sort, index 0 is "most favorable" value (lowest if lower_better, highest if higher_better)
        favorable = grp_s.iloc[:cut][target].mean()
        unfavorable = grp_s.iloc[-cut:][target].mean()
        results.append({'favorable': favorable, 'unfavorable': unfavorable,
                         'spread': favorable - unfavorable})
    if not results:
        return {}
    r = pd.DataFrame(results)
    return {
        'favorable_mean': r['favorable'].mean(),
        'unfavorable_mean': r['unfavorable'].mean(),
        'spread': r['spread'].mean(),
        'winrate': (r['spread'] > 0).mean(),
        'weeks': len(r),
    }


def threshold_test(df: pd.DataFrame, factor: str, threshold: float,
                    horizon: int, op: str) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target]].dropna()
    if sub.empty:
        return {}
    if op == 'lt':
        pass_mask = sub[factor] < threshold
    elif op == 'gt':
        pass_mask = sub[factor] > threshold
    elif op == 'le':
        pass_mask = sub[factor] <= threshold
    else:
        pass_mask = sub[factor] >= threshold
    passed = sub[pass_mask]
    failed = sub[~pass_mask]
    return {
        'pass_n': len(passed),
        'pass_ret': passed[target].mean() if len(passed) else np.nan,
        'fail_ret': failed[target].mean() if len(failed) else np.nan,
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

    # (factor, higher_better_for_IC, description)
    # For Value reversal, lower RSI / lower RVOL / lower 52w proxy = more oversold = potential rebound
    factors = [
        ('rsi_14', False, 'RSI 14 (lower=more oversold)'),
        ('rvol_20', False, 'RVOL 20 (lower=量能萎縮)'),
        ('low52w_prox', False, 'Close/52w-low ratio (lower=closer to bottom)'),
    ]

    # === Layer 1: IC ===
    print("=" * 70)
    print("Layer 1: IC / IR (spearman rank corr vs fwd 60d)")
    print("=" * 70)
    print(f"{'Factor':<16}{'IC':>10}{'IR':>8}{'Weeks':>8}{'Grade':>12}{'Note':<25}")
    print("-" * 90)
    for factor, higher_better, note in factors:
        # negate for lower=better factors to get positive IC expected
        tmp = df.copy()
        col = f'{factor}_neg'
        tmp[col] = -tmp[factor] if not higher_better else tmp[factor]
        res = ic_analysis(tmp, col, args.horizon)
        print(f"{factor:<16}{res['IC']:>10.4f}{res['IR']:>8.3f}{res['weeks']:>8}"
              f"{grade(res['IR']):>12} {note}")
    print()

    # === Layer 2: Decile Spread ===
    print("=" * 70)
    print(f"Layer 2: Decile spread (most oversold 10% vs least)")
    print("=" * 70)
    print(f"{'Factor':<16}{'Oversold':>12}{'Normal':>12}{'Spread':>10}{'WinPct':>10}")
    print("-" * 60)
    for factor, higher_better, _ in factors:
        res = decile_spread(df, factor, args.horizon, higher_better=False)
        if res:
            print(f"{factor:<16}{res['favorable_mean']:>12.2%}{res['unfavorable_mean']:>12.2%}"
                  f"{res['spread']:>10.2%}{res['winrate']:>10.1%}")
    print()

    # === Layer 3: Threshold ===
    print("=" * 70)
    print(f"Layer 3: Threshold tests")
    print("=" * 70)
    threshold_cases = [
        ('rsi_14', 30, 'lt', 'RSI < 30 超賣'),
        ('rsi_14', 40, 'lt', 'RSI < 40'),
        ('rsi_14', 70, 'gt', 'RSI > 70 超買'),
        ('rvol_20', 0.5, 'lt', 'RVOL < 0.5 量萎縮'),
        ('rvol_20', 0.7, 'lt', 'RVOL < 0.7'),
        ('low52w_prox', 1.10, 'lt', '近 52w 低 <10%'),
        ('low52w_prox', 1.20, 'lt', '近 52w 低 <20%'),
    ]
    print(f"{'Label':<22}{'#Pass':>8}{'PassRet':>10}{'FailRet':>10}{'Diff':>10}")
    print("-" * 60)
    for factor, th, op, label in threshold_cases:
        r = threshold_test(df, factor, th, args.horizon, op)
        if r and not pd.isna(r.get('pass_ret')):
            diff = (r['pass_ret'] or 0) - (r['fail_ret'] or 0)
            print(f"{label:<22}{r['pass_n']:>8}{(r['pass_ret'] or 0):>10.2%}"
                  f"{(r['fail_ret'] or 0):>10.2%}{diff:>10.2%}")
    print()


if __name__ == "__main__":
    main()
