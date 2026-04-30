"""
vf_value_trigger_proxy_ic.py - 價值池 trigger proxy quintile IC

Background:
  Historical trigger_score 未存 snapshot。用 3 個 trigger 組成成分作為 proxy：
    - rsi_14 (momentum, 低=反轉候選)
    - rvol_20 (成交量，低=冷淡)
    - low52w_prox (低點近度, 1.0=正好在 52w 低, 越高越遠)

  Core question:
    價值池內「弱勢」（低 RSI / 低 RVOL / 近 52w 低）是否為「抄底」訊號 = fwd return 優於強勢組？
    原始提案（project_value_enhancement.md）預期：Yes，低 trigger = 抄底
    VF-VD 結論（2026-04-19）：各個弱勢加分反向，全砍
    本驗證確認 quintile 層級是否與 VF-VD 結論一致

Design:
  Universe: trade_journal_value_tw_snapshot (70,760 rows, 857 stocks, 309 週, 2020-2025)
  Quintile: 每週按 factor 分 5 組，Q1 (低) vs Q5 (高) 的 fwd 報酬差
  Horizon: 20 / 60 / 120 天
  Also: 合成 "trigger_proxy" = -rsi_norm - rvol_norm - (low52w_prox-1)_norm（高 = 弱勢）

Output:
  reports/vf_value_trigger_proxy_ic.csv
  Console summary with 判讀
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
DATA = ROOT / "data_cache" / "backtest"


def load_snapshot():
    df = pd.read_parquet(DATA / "trade_journal_value_tw_snapshot.parquet")
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df['stock_id'] = df['stock_id'].astype(str)
    return df


def zscore_weekly(df, col):
    """Per-week cross-sectional z-score."""
    return df.groupby('week_end_date')[col].transform(
        lambda s: (s - s.mean()) / s.std(ddof=1) if s.std(ddof=1) > 0 else 0
    )


def weekly_quintile_ret(df, factor, horizon):
    """每週把 factor 分 5 組，看各組 fwd 報酬。

    Returns: dict with Q1..Q5 mean return, Q5-Q1 spread, weekly IC (continuous)
    """
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()

    # Spearman IC (continuous)
    ics = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 15:
            continue
        if grp[factor].nunique() < 3:
            continue
        rho, _ = stats.spearmanr(grp[factor], grp[target])
        if not np.isnan(rho):
            ics.append(rho)

    ic_mean = np.mean(ics) if ics else np.nan
    ic_std = np.std(ics, ddof=1) if len(ics) > 1 else np.nan
    ic_ir = ic_mean / ic_std if (ic_std and ic_std > 0) else np.nan

    # Quintile: 每週 rank 分 5 組
    quintile_rets = {f'Q{i}': [] for i in range(1, 6)}
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 15:
            continue
        try:
            grp = grp.copy()
            grp['q'] = pd.qcut(grp[factor], 5, labels=False, duplicates='drop')
            for q in range(5):
                mask = grp['q'] == q
                if mask.any():
                    quintile_rets[f'Q{q+1}'].append(grp.loc[mask, target].mean())
        except Exception:
            continue

    qmean = {k: np.mean(v) if v else np.nan for k, v in quintile_rets.items()}
    spread_q5_q1 = qmean['Q5'] - qmean['Q1'] if not (np.isnan(qmean['Q5']) or np.isnan(qmean['Q1'])) else np.nan

    return {
        'IC': ic_mean,
        'IR': ic_ir,
        'weeks': len(ics),
        **qmean,
        'Q5-Q1': spread_q5_q1,
    }


def verdict(ir, spread_q5_q1, factor):
    """判讀：預期低 RSI/rvol/52w 低 = 抄底 = Q1 (低) 報酬 > Q5 (高)
    → Q5-Q1 < 0 + IC 負向 = 符合提案

    對合成 trigger_proxy（高 = 弱勢）：預期 Q5 (高 = 弱) > Q1 (強) → Q5-Q1 > 0 + IC 正向
    """
    if pd.isna(ir):
        return "no data"
    is_proxy = factor == 'trigger_proxy'
    sign = 1 if is_proxy else -1
    # 對 IR 做 sign-flip 讓 "+" 代表符合預期
    aligned_ir = sign * ir
    if aligned_ir > 0.3:
        return f"A conf. 預期 (aligned IR={aligned_ir:+.2f})"
    if aligned_ir > 0.1:
        return f"B weak 符合 (aligned IR={aligned_ir:+.2f})"
    if aligned_ir > -0.1:
        return f"C 平原 (aligned IR={aligned_ir:+.2f})"
    if aligned_ir > -0.3:
        return f"D weak 反向 (aligned IR={aligned_ir:+.2f})"
    return f"D+ 強反向 (aligned IR={aligned_ir:+.2f})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='reports/')
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[1/3] Loading snapshot...")
    snap = load_snapshot()
    print(f"  {len(snap)} rows, {snap['stock_id'].nunique()} stocks, {snap['week_end_date'].nunique()} 週")

    print("[2/3] Building composite trigger_proxy (higher = weaker)...")
    # 標準化三因子（每週 z-score），然後合成 "弱勢分數"
    # 高 proxy = 低 RSI + 低 RVOL + 近 52w 低 (即原提案的「低 trigger」)
    snap['rsi_z'] = zscore_weekly(snap, 'rsi_14')
    snap['rvol_z'] = zscore_weekly(snap, 'rvol_20')
    snap['low52_z'] = zscore_weekly(snap, 'low52w_prox')
    # low52w_prox 越小 (靠近 1.0) 越近 52w 低；但 z 越小也越弱勢，所以符號直接取負
    # trigger_proxy 高 = 弱勢 = 低 trigger
    snap['trigger_proxy'] = -snap['rsi_z'] - snap['rvol_z'] - snap['low52_z']

    print("[3/3] Running quintile + IC analysis...")
    factors = ['rsi_14', 'rvol_20', 'low52w_prox', 'trigger_proxy']
    horizons = [20, 60, 120]

    rows = []
    print(f"\n{'Factor':15s} {'H':>4s} {'IC':>8s} {'IR':>7s} {'weeks':>6s} "
          f"{'Q1':>8s} {'Q2':>8s} {'Q3':>8s} {'Q4':>8s} {'Q5':>8s} {'Q5-Q1':>8s}")
    print("-" * 110)
    for f in factors:
        for h in horizons:
            r = weekly_quintile_ret(snap, f, h)
            rows.append({'factor': f, 'horizon': h, **r})
            print(f"{f:15s} {h:>3d}d {r['IC']:>+8.4f} {r['IR']:>+7.3f} {r['weeks']:>6d} "
                  f"{r['Q1']:>+8.4f} {r['Q2']:>+8.4f} {r['Q3']:>+8.4f} {r['Q4']:>+8.4f} "
                  f"{r['Q5']:>+8.4f} {r['Q5-Q1']:>+8.4f}")

    df = pd.DataFrame(rows)
    out_csv = out_dir / 'vf_value_trigger_proxy_ic.csv'
    df.to_csv(out_csv, index=False)
    print(f"\nSaved: {out_csv}")

    # 判讀
    print("\n=== Decision Summary ===")
    print("預期（project_value_enhancement.md）：價值池內低 trigger/弱勢 = 抄底 = fwd 報酬較佳")
    print("  - 對 rsi_14/rvol_20/low52w_prox：Q1 (低) 應 > Q5 (高) → IR 應為負")
    print("  - 對 trigger_proxy (高=弱)：Q5 (高) 應 > Q1 (低) → IR 應為正")
    print()
    for f in factors:
        sub = df[df['factor'] == f]
        best_row = sub.loc[sub['IR'].abs().idxmax()]
        v = verdict(best_row['IR'], best_row['Q5-Q1'], f)
        print(f"  {f:15s} best IR = {best_row['IR']:+.3f} @ {int(best_row['horizon'])}d "
              f"Q5-Q1={best_row['Q5-Q1']:+.4f} → {v}")


if __name__ == '__main__':
    main()
