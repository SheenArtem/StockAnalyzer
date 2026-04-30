"""
vf_ve_validation.py - VF-VE Value SmartMoney IC 驗證

驗證目標：
  1. SmartMoney (法人淨買) 對 fwd_60d return 是否有 alpha?
  2. 三大法人子因子 (外資/投信/自營) 單獨 IC
  3. Threshold tests (淨買 > 0 / > 1% tv / > 2% tv)
  4. 評估 15% 權重是否合理 → 建議 保留 / 降到 5% / 刪除
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

SNAPSHOT_PATH = ROOT / "data_cache" / "backtest" / "trade_journal_value_tw_sm_snapshot.parquet"
SM_SCORES_PATH = ROOT / "data_cache" / "backtest" / "smart_money_scores.parquet"


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
                   higher_better: bool = True) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    results = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 30:
            continue
        grp_s = grp.sort_values(factor, ascending=not higher_better).reset_index(drop=True)
        n = len(grp_s)
        cut = max(1, n // 10)
        top = grp_s.iloc[:cut][target].mean()
        bot = grp_s.iloc[-cut:][target].mean()
        results.append({'top': top, 'bot': bot, 'spread': top - bot})
    if not results:
        return {}
    r = pd.DataFrame(results)
    return {
        'top_mean': r['top'].mean(),
        'bot_mean': r['bot'].mean(),
        'spread': r['spread'].mean(),
        'winrate': (r['spread'] > 0).mean(),
        'weeks': len(r),
    }


def threshold_test(df: pd.DataFrame, factor: str, threshold: float,
                    horizon: int, op: str = 'gt') -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target]].dropna()
    if op == 'gt':
        pass_mask = sub[factor] > threshold
    else:
        pass_mask = sub[factor] < threshold
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
    return 'D (no alpha)'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", default=str(SNAPSHOT_PATH))
    ap.add_argument("--sm-scores", default=str(SM_SCORES_PATH))
    ap.add_argument("--horizon", type=int, default=60)
    args = ap.parse_args()

    # Load snapshot (has 5 dim scores + fwd returns)
    snap = pd.read_parquet(args.snapshot)
    snap['week_end_date'] = pd.to_datetime(snap['week_end_date'])
    print(f"Snapshot: {len(snap)} rows, {snap['week_end_date'].nunique()} weeks, "
          f"{snap['stock_id'].nunique()} unique stocks")
    print(f"Date range: {snap['week_end_date'].min().date()} - "
          f"{snap['week_end_date'].max().date()}")

    # Load raw SM scores to test sub-factors
    sm = pd.read_parquet(args.sm_scores)
    sm['week_end_date'] = pd.to_datetime(sm['week_end_date'])
    print(f"SM raw: {len(sm)} rows, {sm['stock_id'].nunique()} stocks")
    print()

    # Merge snap with raw SM pct factors for richer testing
    merged = snap.merge(
        sm[['stock_id', 'week_end_date', 'foreign_pct', 'trust_pct',
            'dealer_pct', 'total_pct', 'foreign_net_5d',
            'trust_net_5d', 'total_net_5d']],
        on=['stock_id', 'week_end_date'], how='left',
    )

    # === Layer 1: IC ===
    print("=" * 70)
    print(f"Layer 1: IC / IR of SM factors vs fwd_{args.horizon}d")
    print("=" * 70)
    print(f"{'Factor':<22}{'IC':>10}{'IR':>8}{'Weeks':>8}{'Grade':>15}")
    print("-" * 70)
    factors = [
        ('smart_money_s', 'SM composite score'),
        ('total_pct', '三大法人合計 5d %'),
        ('foreign_pct', '外資 5d %'),
        ('trust_pct', '投信 5d %'),
        ('dealer_pct', '自營商 5d %'),
    ]
    for col, label in factors:
        if col not in merged.columns:
            continue
        res = ic_analysis(merged, col, args.horizon)
        print(f"{label:<22}{res['IC']:>10.4f}{res['IR']:>8.3f}{res['weeks']:>8}"
              f"{grade(res['IR']):>15}")
    print()

    # === Layer 2: Decile Spread ===
    print("=" * 70)
    print(f"Layer 2: Decile spread (top 10% vs bot 10% by factor)")
    print("=" * 70)
    print(f"{'Factor':<22}{'Top':>10}{'Bot':>10}{'Spread':>10}{'WinPct':>10}")
    print("-" * 70)
    for col, label in factors:
        if col not in merged.columns:
            continue
        res = decile_spread(merged, col, args.horizon, higher_better=True)
        if res:
            print(f"{label:<22}{res['top_mean']:>10.2%}{res['bot_mean']:>10.2%}"
                  f"{res['spread']:>10.2%}{res['winrate']:>10.1%}")
    print()

    # === Layer 3: Threshold ===
    print("=" * 70)
    print(f"Layer 3: Threshold test (法人淨買 %)")
    print("=" * 70)
    print(f"{'Label':<30}{'#Pass':>8}{'PassRet':>10}{'FailRet':>10}{'Diff':>10}")
    print("-" * 70)
    cases = [
        ('total_pct', 0, '三大法人 淨買 > 0'),
        ('total_pct', 0.5, '淨買 > 0.5% tv'),
        ('total_pct', 2, '淨買 > 2% tv (大買)'),
        ('total_pct', -2, '淨賣 < -2% tv (大賣) 的 pass=未大賣'),
        ('foreign_pct', 0, '外資淨買 > 0'),
        ('foreign_pct', 1, '外資 > 1% tv'),
        ('trust_pct', 0, '投信淨買 > 0'),
    ]
    for factor, th, label in cases:
        r = threshold_test(merged, factor, th, args.horizon, op='gt')
        if r and not pd.isna(r.get('pass_ret')):
            diff = (r['pass_ret'] or 0) - (r['fail_ret'] or 0)
            print(f"{label:<30}{r['pass_n']:>8}"
                  f"{(r['pass_ret'] or 0):>10.2%}"
                  f"{(r['fail_ret'] or 0):>10.2%}"
                  f"{diff:>10.2%}")
    print()

    # === Summary ===
    print("=" * 70)
    print("Summary / Decision")
    print("=" * 70)
    sm_ir = ic_analysis(merged, 'smart_money_s', args.horizon)['IR']
    print(f"SmartMoney composite score IR: {sm_ir:.3f} ({grade(sm_ir)})")
    print()
    if pd.isna(sm_ir) or abs(sm_ir) < 0.05:
        print("→ IR < 0.05 (D 級無 alpha) → 建議**刪除 SmartMoney 維度**")
        print("  Value 5 面向 → 4 面向 (val/quality/rev/tech)")
        print("  15% 權重重分配到 valuation 或 quality")
    elif abs(sm_ir) < 0.1:
        print("→ IR 0.05-0.1 (C 級) → 建議**降權到 5-10%**")
    elif abs(sm_ir) < 0.3:
        print("→ IR 0.1-0.3 (B 級) → **維持 15% 或小幅調整**")
    else:
        print("→ IR >= 0.3 (A 級) → **可考慮加權到 20-25%**")


if __name__ == "__main__":
    main()
