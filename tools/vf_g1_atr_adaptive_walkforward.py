"""
VF-G1 Phase 2 minimal: ATR-adaptive SL walk-forward (2026-04-22)

可行性快測 (tools/vf_g1_atr_bucket_feasibility.py) 已證：
  fwd40 mean spread 跨 ATR tercile +2.9pp
  DD_p5 spread +17pp
  fwd_40d_min ~ atr_pct t=-25.91 (|t|>>2)

本工具進一步驗證：**把固定 3.0× 改為 ATR-bucket 調整的倍數**是否真的改善
Sharpe / maxdd / winrate。方法：trade-level 模擬 + quarterly walk-forward OOS。

模擬規則：
  每筆 trade 的實際報酬 =
    if fwd_40d_min <= -sl_pct:  stopped out at -sl_pct (簡化，忽略 slippage)
    else: realized = fwd_40d

  sl_pct = clip(atr_pct/100 * multiplier, 0.05, 0.14)  # 同 exit_manager.py 邏輯

Grid 測試：
  (a) Fixed baselines: 2.5 / 3.0 (live) / 3.5
  (b) Adaptive schemes (low / mid / high ATR tercile multipliers):
      - ADAPT_A: 2.0 / 3.0 / 3.5 (low tighten, high keep)
      - ADAPT_B: 2.5 / 3.0 / 3.5 (low tighten mild, high loosen)
      - ADAPT_C: 2.5 / 3.0 / 4.0 (high loosen more)
      - ADAPT_D: 3.0 / 3.0 / 4.0 (only loosen high)
      - ADAPT_E: 2.0 / 3.0 / 4.0 (aggressive both ends)

判定：OOS quarterly walk-forward (24 季)
  贏 fixed-3.0 baseline quarterly mean_ret > baseline AND winrate >= 14/24 = LAND
  輸 mean_ret 或 winrate < 12/24 = REJECT

用法:
  python tools/vf_g1_atr_adaptive_walkforward.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

JOURNAL = ROOT / 'data_cache' / 'backtest' / 'trade_journal_qm_tw_mixed.parquet'

SL_FLOOR = 0.05
SL_CEIL = 0.14


def sl_pct(atr_pct: float, multiplier: float) -> float:
    """同 exit_manager.compute_exit_plan Phase 2 邏輯."""
    if atr_pct is None or atr_pct <= 0:
        return 0.08  # DEFAULT_HARD_STOP_PCT
    return float(np.clip(atr_pct / 100.0 * multiplier, SL_FLOOR, SL_CEIL))


def realized_return(fwd_min: float, fwd_final: float, sl: float) -> float:
    """If drawdown touched SL, stop there; else realize final."""
    if fwd_min <= -sl:
        return -sl  # stopped out at SL (simplified)
    return fwd_final


def apply_scheme(df: pd.DataFrame, scheme_fn, horizon: str = 'fwd_40d') -> pd.Series:
    """Apply SL scheme across all trades, return realized returns."""
    min_col = f'{horizon}_min'
    sl_pcts = df.apply(lambda r: scheme_fn(r['atr_pct'], r['atr_bucket']), axis=1)
    return df.apply(
        lambda r: realized_return(r[min_col], r[horizon], sl_pcts.loc[r.name]),
        axis=1,
    )


def fixed_scheme(multiplier: float):
    def fn(atr, bucket): return sl_pct(atr, multiplier)
    return fn


def adaptive_scheme(mults: tuple):
    """mults = (low_mult, mid_mult, high_mult) per ATR tercile."""
    def fn(atr, bucket):
        return sl_pct(atr, mults[bucket])
    return fn


def portfolio_metrics(returns: pd.Series) -> dict:
    rs = returns.dropna()
    if len(rs) < 10:
        return {}
    mean_r = rs.mean()
    std_r = rs.std()
    return {
        'n': len(rs),
        'mean': mean_r,
        'std': std_r,
        'sharpe': mean_r / std_r if std_r > 0 else np.nan,
        'winrate': (rs > 0).mean(),
        'median': rs.median(),
    }


def quarterly_walkforward(df: pd.DataFrame, scheme_fn, horizon: str,
                           baseline_fn=None) -> pd.DataFrame:
    """Per-quarter mean return of this scheme."""
    df = df.copy()
    df['realized'] = apply_scheme(df, scheme_fn, horizon)
    if baseline_fn is not None:
        df['baseline'] = apply_scheme(df, baseline_fn, horizon)
    df['quarter'] = pd.to_datetime(df['week_end_date']).dt.to_period('Q')
    grp = df.groupby('quarter').agg(
        n=('realized', 'count'),
        mean_ret=('realized', 'mean'),
        baseline=('baseline', 'mean') if baseline_fn is not None else ('realized', 'count'),
    )
    return grp.reset_index()


def main():
    j = pd.read_parquet(JOURNAL)
    print(f"Loaded: {len(j)} trades, "
          f"{j['week_end_date'].nunique()} weeks, "
          f"{j['stock_id'].nunique()} stocks")

    j = j.dropna(subset=['atr_pct', 'fwd_40d', 'fwd_40d_min']).copy()

    # ATR tercile bucketing (fixed thresholds from feasibility test)
    q33, q67 = j['atr_pct'].quantile([0.333, 0.667])
    j['atr_bucket'] = pd.cut(j['atr_pct'],
                              bins=[-np.inf, q33, q67, np.inf],
                              labels=[0, 1, 2]).astype(int)
    print(f"ATR terciles: <{q33:.2f} / {q33:.2f}-{q67:.2f} / >{q67:.2f}")
    print()

    # Define schemes
    schemes = {
        'FIXED_2.5': fixed_scheme(2.5),
        'FIXED_3.0 (live)': fixed_scheme(3.0),
        'FIXED_3.5': fixed_scheme(3.5),
        'ADAPT_A (2.0/3.0/3.5)': adaptive_scheme((2.0, 3.0, 3.5)),
        'ADAPT_B (2.5/3.0/3.5)': adaptive_scheme((2.5, 3.0, 3.5)),
        'ADAPT_C (2.5/3.0/4.0)': adaptive_scheme((2.5, 3.0, 4.0)),
        'ADAPT_D (3.0/3.0/4.0)': adaptive_scheme((3.0, 3.0, 4.0)),
        'ADAPT_E (2.0/3.0/4.0)': adaptive_scheme((2.0, 3.0, 4.0)),
        'NO_STOP (theoretical)': None,  # no stop, pure hold
    }

    # === All-period metrics ===
    print("=" * 95)
    print(f"All-period metrics (fwd_40d, simulated SL stop)")
    print("=" * 95)
    print(f"{'Scheme':<28}{'n':>6}{'mean':>10}{'std':>10}{'Sharpe':>10}"
          f"{'winrate':>10}{'median':>10}")
    print("-" * 95)

    all_period = {}
    for name, fn in schemes.items():
        if fn is None:
            realized = j['fwd_40d']
        else:
            realized = apply_scheme(j, fn, 'fwd_40d')
        m = portfolio_metrics(realized)
        all_period[name] = m
        print(f"{name:<28}{m['n']:>6}{m['mean']:>10.2%}{m['std']:>10.2%}"
              f"{m['sharpe']:>10.3f}{m['winrate']:>10.1%}{m['median']:>10.2%}")
    print()

    # === Quarterly walk-forward vs FIXED_3.0 ===
    print("=" * 95)
    print(f"Quarterly walk-forward vs FIXED_3.0 (live baseline)")
    print("=" * 95)
    baseline_fn = schemes['FIXED_3.0 (live)']
    print(f"{'Scheme':<28}{'qMean':>10}{'qSharpe':>10}{'Wins/N':>10}"
          f"{'WinRate':>10}{'Verdict':>20}")
    print("-" * 95)

    # Compute per-quarter baseline
    j_base = j.copy()
    j_base['base_real'] = apply_scheme(j_base, baseline_fn, 'fwd_40d')
    j_base['quarter'] = pd.to_datetime(j_base['week_end_date']).dt.to_period('Q')
    base_q = j_base.groupby('quarter')['base_real'].mean()

    base_mean = all_period['FIXED_3.0 (live)']['mean']

    for name, fn in schemes.items():
        if name == 'FIXED_3.0 (live)':
            print(f"{name:<28}{'(baseline)':>10}{'':>10}{'':>10}{'':>10}{'baseline':>20}")
            continue
        if fn is None:
            # No-stop theoretical — aggregate only, not walk-forward judgment
            print(f"{name:<28}{'(info only)':>10}{'':>10}{'':>10}{'':>10}{'N/A':>20}")
            continue

        j_test = j.copy()
        j_test['realized'] = apply_scheme(j_test, fn, 'fwd_40d')
        j_test['quarter'] = pd.to_datetime(j_test['week_end_date']).dt.to_period('Q')
        test_q = j_test.groupby('quarter')['realized'].mean()

        # Align
        aligned = pd.DataFrame({'base': base_q, 'test': test_q}).dropna()
        wins = (aligned['test'] > aligned['base']).sum()
        total = len(aligned)
        winrate = wins / total if total else np.nan
        q_mean = test_q.mean()
        q_sharpe = test_q.mean() / test_q.std() if test_q.std() > 0 else np.nan

        all_mean = all_period[name]['mean']
        if winrate >= 14/24 and all_mean > base_mean:
            verdict = "LAND candidate"
        elif winrate < 12/24 or all_mean < base_mean:
            verdict = "REJECT"
        else:
            verdict = "MARGINAL"

        print(f"{name:<28}{q_mean:>10.2%}{q_sharpe:>10.3f}{wins:>4}/{total:<5}"
              f"{winrate:>10.1%}{verdict:>20}")
    print()

    # === By-bucket breakdown for best adaptive scheme ===
    print("=" * 95)
    print("By-bucket breakdown (FIXED_3.0 vs best ADAPT candidate)")
    print("=" * 95)

    # Find best adaptive
    adapt_schemes = {n: m for n, m in all_period.items() if n.startswith('ADAPT')}
    best_adapt = max(adapt_schemes.items(), key=lambda x: x[1]['sharpe'] or -999)
    print(f"Best ADAPT by Sharpe: {best_adapt[0]}")
    print()

    print(f"{'Bucket':<15}{'Scheme':<28}{'n':>6}{'mean':>10}{'std':>10}"
          f"{'Sharpe':>10}{'winrate':>10}")
    print("-" * 95)
    for bucket_id, bucket_name in [(0, 'low'), (1, 'mid'), (2, 'high')]:
        jb = j[j['atr_bucket'] == bucket_id]
        for scheme_name in ['FIXED_3.0 (live)', best_adapt[0]]:
            fn = schemes[scheme_name]
            realized = apply_scheme(jb, fn, 'fwd_40d')
            m = portfolio_metrics(realized)
            print(f"{bucket_name:<15}{scheme_name:<28}{m['n']:>6}{m['mean']:>10.2%}"
                  f"{m['std']:>10.2%}{m['sharpe']:>10.3f}{m['winrate']:>10.1%}")
    print()

    # === Summary ===
    print("=" * 95)
    print("SUMMARY")
    print("=" * 95)
    print(f"FIXED_3.0 baseline Sharpe: {all_period['FIXED_3.0 (live)']['sharpe']:.3f}")
    print(f"Best ADAPT Sharpe: {best_adapt[1]['sharpe']:.3f} ({best_adapt[0]})")
    delta = best_adapt[1]['sharpe'] - all_period['FIXED_3.0 (live)']['sharpe']
    print(f"Sharpe delta: {delta:+.3f}")
    print()
    print(f"NO_STOP theoretical ceiling: Sharpe {all_period['NO_STOP (theoretical)']['sharpe']:.3f}")


if __name__ == '__main__':
    main()
