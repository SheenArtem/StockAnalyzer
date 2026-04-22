"""
VF-G1 per-stock ATR% bucket feasibility quick test (2026-04-22)

目的：在做完整 Phase 2 (per-stock 差異化出場) 前，先檢查 **ATR% bucket 是否真的有
per-stock 出場行為差異**。若無 signal 差異 → 直接停，省 3-4h Phase 2 完整工。

檢驗假說 (Phase 2 動機)：
  H1: 高 ATR% 股票（波動大）→ fwd drawdown 深、應該用更寬 stop
  H2: 低 ATR% 股票（穩）→ stop 可以收緊，不會觸發洗盤
  H3: 最佳 SL 距離（fwd_min 分位）隨 ATR% 單調變化

通過條件 (任一)：
  - 三 bucket 的 fwd_40d mean spread > 1.5pp
  - 三 bucket 的 fwd_40d_min 分位（1%-percentile）spread > 3pp
  - 最佳 SL (20d/40d fwd_min 分位) 隨 ATR% 顯著單調（斜率 t-stat > 2）

若三條全不過 → D 級，不做 Phase 2 per-stock 差異化（只做 market-wide regime filter
就夠，VF-G4 已經 A 級）。

用法:
  python tools/vf_g1_atr_bucket_feasibility.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

JOURNAL = ROOT / 'data_cache' / 'backtest' / 'trade_journal_qm_tw_mixed.parquet'


def main():
    j = pd.read_parquet(JOURNAL)
    print(f"Loaded journal: {len(j)} trades, "
          f"{j['week_end_date'].nunique()} weeks, "
          f"{j['stock_id'].nunique()} stocks")
    print(f"Date range: {j['week_end_date'].min()} ~ {j['week_end_date'].max()}")
    print()

    # ATR% bucket — terciles
    j = j.dropna(subset=['atr_pct', 'fwd_40d', 'fwd_40d_min']).copy()
    q33, q67 = j['atr_pct'].quantile([0.333, 0.667])
    j['atr_bucket'] = pd.cut(j['atr_pct'],
                              bins=[-np.inf, q33, q67, np.inf],
                              labels=['low (<%.2f)' % q33,
                                      'mid (%.2f-%.2f)' % (q33, q67),
                                      'high (>%.2f)' % q67])
    print(f"ATR% terciles: low<{q33:.2f} / mid {q33:.2f}-{q67:.2f} / high>{q67:.2f}")
    print()

    # === Test 1: forward return mean / std by bucket ===
    print("=" * 80)
    print("Test 1: Forward return mean / std by ATR% bucket")
    print("=" * 80)
    print(f"{'Bucket':<25}{'n':>6}{'fwd20_mean':>12}{'fwd40_mean':>12}{'fwd60_mean':>12}"
          f"{'fwd40_std':>12}")
    print("-" * 80)
    rows = []
    for b in j['atr_bucket'].unique():
        sub = j[j['atr_bucket'] == b]
        row = {
            'bucket': b,
            'n': len(sub),
            'fwd20_mean': sub['fwd_20d'].mean(),
            'fwd40_mean': sub['fwd_40d'].mean(),
            'fwd60_mean': sub['fwd_60d'].mean() if 'fwd_60d' in sub else np.nan,
            'fwd40_std': sub['fwd_40d'].std(),
            'fwd40_min_p1': sub['fwd_40d_min'].quantile(0.01),
            'fwd40_min_p5': sub['fwd_40d_min'].quantile(0.05),
            'fwd40_min_p25': sub['fwd_40d_min'].quantile(0.25),
            'fwd40_min_median': sub['fwd_40d_min'].median(),
        }
        rows.append(row)
        print(f"{str(b):<25}{len(sub):>6}{row['fwd20_mean']:>12.2%}"
              f"{row['fwd40_mean']:>12.2%}{row['fwd60_mean']:>12.2%}"
              f"{row['fwd40_std']:>12.2%}")
    df_bucket = pd.DataFrame(rows).sort_values('bucket')
    print()

    fwd40_spread = df_bucket['fwd40_mean'].max() - df_bucket['fwd40_mean'].min()
    print(f"fwd40_mean spread (high-low): {fwd40_spread*100:+.2f}pp")
    print()

    # === Test 2: drawdown distribution ===
    print("=" * 80)
    print("Test 2: 40d drawdown percentile by ATR% bucket")
    print("=" * 80)
    print(f"{'Bucket':<25}{'n':>6}{'DD_p1':>10}{'DD_p5':>10}{'DD_p25':>10}{'DD_median':>12}")
    print("-" * 80)
    for _, r in df_bucket.iterrows():
        print(f"{str(r['bucket']):<25}{int(r['n']):>6}"
              f"{r['fwd40_min_p1']:>10.2%}{r['fwd40_min_p5']:>10.2%}"
              f"{r['fwd40_min_p25']:>10.2%}{r['fwd40_min_median']:>12.2%}")
    print()

    dd_p5_spread = df_bucket['fwd40_min_p5'].max() - df_bucket['fwd40_min_p5'].min()
    dd_median_spread = df_bucket['fwd40_min_median'].max() - df_bucket['fwd40_min_median'].min()
    print(f"DD_p5 spread: {dd_p5_spread*100:+.2f}pp (抽 5%-tile 壞情境跨 bucket 差異)")
    print(f"DD_median spread: {dd_median_spread*100:+.2f}pp")
    print()

    # === Test 3: monotonicity — linear regression slope test ===
    print("=" * 80)
    print("Test 3: ATR% vs optimal SL placement (linear slope, 5% tail focus)")
    print("=" * 80)
    from scipy import stats as scistats
    # Use p5 drawdown = proxy for "where SL needs to be to protect 95% of trades"
    # If ATR% matters, we'd expect slope = 正 (higher ATR% → deeper DD → wider SL needed)
    for metric in ['fwd_40d_min', 'fwd_20d_min']:
        slope, intercept, r, pval, stderr = scistats.linregress(
            j['atr_pct'], j[metric]
        )
        print(f"  {metric} ~ atr_pct: slope={slope:.4f} ± {stderr:.4f}, "
              f"r={r:.3f}, t={slope/stderr:.2f}, p={pval:.4f}")
    print()

    # === Verdict ===
    print("=" * 80)
    print("VERDICT")
    print("=" * 80)
    passed = []
    failed = []

    if abs(fwd40_spread) >= 0.015:
        passed.append(f"fwd40_mean spread {fwd40_spread*100:+.2f}pp >= 1.5pp")
    else:
        failed.append(f"fwd40_mean spread {fwd40_spread*100:+.2f}pp < 1.5pp")

    if abs(dd_p5_spread) >= 0.03:
        passed.append(f"DD_p5 spread {dd_p5_spread*100:+.2f}pp >= 3pp")
    else:
        failed.append(f"DD_p5 spread {dd_p5_spread*100:+.2f}pp < 3pp")

    slope40, _, _, _, stderr40 = scistats.linregress(j['atr_pct'], j['fwd_40d_min'])
    t40 = slope40 / stderr40 if stderr40 > 0 else 0
    if abs(t40) >= 2:
        passed.append(f"fwd_40d_min ~ atr_pct slope t={t40:.2f} (|t|>=2)")
    else:
        failed.append(f"fwd_40d_min ~ atr_pct slope t={t40:.2f} (|t|<2)")

    print(f"\n通過 {len(passed)}/3:")
    for p in passed:
        print(f"  [PASS] {p}")
    for f in failed:
        print(f"  [FAIL] {f}")

    if len(passed) >= 2:
        print("\n結論: 有 signal → 可繼續做 Phase 2 完整 per-stock 差異化")
    elif len(passed) == 1:
        print("\n結論: MARGINAL → Phase 2 預期 D 級但 may try，由人決定")
    else:
        print("\n結論: D 級 → 不做 Phase 2 per-stock 差異化，alpha 不在這")


if __name__ == '__main__':
    main()
