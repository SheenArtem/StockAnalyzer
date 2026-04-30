"""
vf_value_trap_pockets.py - 價值陷阱 pocket 交集驗證

Background:
  Phase 1 #3 殘餘：weekly MA20 跌破 / 月營收 YoY 連 2 月轉負 / 52w 低點附近
  這 3 條件需要 daily/monthly 重建管線，工時 3-4h+。
  既有 snapshot 已驗 low52w_prox 120d IR=-0.31 D weak（接近 52w 低 = 報酬差）。

  本驗證用既有 snapshot 欄位做 pairwise + 3-way 交集，看是否能找到「比單一弱勢更強」
  的 trap pocket。如果 3-way 交集 IR < -0.5，可加 trap_warning 到 value_screener。

Universe: trade_journal_value_tw_snapshot.parquet (70,760 rows, 309 週, 2020-2025)

Trap signal 候選（snapshot 既有欄位）：
  T1: low52w_prox bottom 30% (接近 52w 低)
  T2: rsi_14 bottom 30% (弱動能)
  T3: revenue_score bottom 30% (月營收弱)
  T4: f_score <= 4 (體質差) — 已落地 -20 比較 baseline

Combinations:
  單條件 → pairwise (T1∩T2, T1∩T3, T2∩T3) → 3-way (T1∩T2∩T3)

Metrics:
  - hit count (單週平均落入 pocket 的股數)
  - mean fwd return @ 60d / 120d (vs universe baseline)
  - alpha = pocket_mean - universe_mean
  - signal-to-noise: alpha / (universe std / sqrt(weeks))

Decision:
  - 3-way alpha @ 120d < -3% AND |IR-equivalent| > 0.5 → 落地 trap_warning
  - alpha @ 120d -1% ~ -3% → weak 警告候選，邊際無感
  - alpha > -1% → 平原不落地

Output: reports/vf_value_trap_pockets.csv + console summary
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
DATA = ROOT / "data_cache" / "backtest"


def load_snapshot():
    df = pd.read_parquet(DATA / "trade_journal_value_tw_snapshot.parquet")
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df['stock_id'] = df['stock_id'].astype(str)
    return df


def build_signals(df):
    """Per-week cross-sectional thresholds. 每週分位避免 regime drift bias."""
    out = df.copy()

    def weekly_quantile_flag(col, q, lower=True):
        """每週按 col 取 lower/upper q 分位，回傳 bool flag."""
        def fn(s):
            if s.dropna().shape[0] < 15:
                return pd.Series(False, index=s.index)
            thr = s.quantile(q)
            return s <= thr if lower else s >= thr
        return out.groupby('week_end_date')[col].transform(fn).fillna(False).astype(bool)

    out['T1_low52w'] = weekly_quantile_flag('low52w_prox', 0.30, lower=True)
    out['T2_weak_rsi'] = weekly_quantile_flag('rsi_14', 0.30, lower=True)
    out['T3_weak_rev'] = weekly_quantile_flag('revenue_score', 0.30, lower=True)
    out['T4_low_fscore'] = (out['f_score'].fillna(99) <= 4)

    return out


def evaluate(df, mask, label, horizons=(60, 120)):
    """評估某 mask 在不同 horizon 的 fwd return vs universe baseline."""
    rows = []
    for h in horizons:
        target = f'fwd_{h}d'
        sub = df[[target]].dropna()
        universe_mean = sub[target].mean()
        universe_std = sub[target].std(ddof=1)

        pocket = df.loc[mask & df[target].notna(), target]
        if len(pocket) < 100:
            rows.append({
                'signal': label, 'horizon': h, 'n_obs': len(pocket),
                'pocket_mean': np.nan, 'universe_mean': universe_mean,
                'alpha': np.nan, 'snr': np.nan,
            })
            continue

        pocket_mean = pocket.mean()
        alpha = pocket_mean - universe_mean
        snr = alpha / (universe_std / np.sqrt(len(pocket))) if universe_std > 0 else np.nan

        rows.append({
            'signal': label, 'horizon': h, 'n_obs': len(pocket),
            'pocket_mean': pocket_mean, 'universe_mean': universe_mean,
            'alpha': alpha, 'snr': snr,
        })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='reports/')
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[1/3] Loading snapshot...")
    df = load_snapshot()
    print(f"  {len(df)} rows, {df['stock_id'].nunique()} stocks, {df['week_end_date'].nunique()} 週")

    print("[2/3] Building trap signals...")
    df = build_signals(df)
    for sig in ['T1_low52w', 'T2_weak_rsi', 'T3_weak_rev', 'T4_low_fscore']:
        rate = df[sig].mean() * 100
        print(f"  {sig}: {df[sig].sum():>6d} / {len(df):>6d} ({rate:.1f}%)")

    print("[3/3] Evaluating signal pockets...")
    print(f"\n{'Signal':40s} {'H':>4s} {'n_obs':>8s} {'pocket':>10s} {'univ':>10s} {'alpha':>10s} {'snr':>8s}")
    print("-" * 100)

    rows = []
    # Single conditions
    rows += evaluate(df, df['T1_low52w'], 'T1_low52w')
    rows += evaluate(df, df['T2_weak_rsi'], 'T2_weak_rsi')
    rows += evaluate(df, df['T3_weak_rev'], 'T3_weak_rev')
    rows += evaluate(df, df['T4_low_fscore'], 'T4_low_fscore (baseline)')

    # Pairwise
    rows += evaluate(df, df['T1_low52w'] & df['T2_weak_rsi'], 'T1∩T2 (52w_low + weak_rsi)')
    rows += evaluate(df, df['T1_low52w'] & df['T3_weak_rev'], 'T1∩T3 (52w_low + weak_rev)')
    rows += evaluate(df, df['T2_weak_rsi'] & df['T3_weak_rev'], 'T2∩T3 (weak_rsi + weak_rev)')
    rows += evaluate(df, df['T1_low52w'] & df['T4_low_fscore'], 'T1∩T4 (52w_low + low_F)')
    rows += evaluate(df, df['T2_weak_rsi'] & df['T4_low_fscore'], 'T2∩T4 (weak_rsi + low_F)')
    rows += evaluate(df, df['T3_weak_rev'] & df['T4_low_fscore'], 'T3∩T4 (weak_rev + low_F)')

    # 3-way
    rows += evaluate(df, df['T1_low52w'] & df['T2_weak_rsi'] & df['T3_weak_rev'],
                     'T1∩T2∩T3 (3-way no F)')
    rows += evaluate(df, df['T1_low52w'] & df['T2_weak_rsi'] & df['T4_low_fscore'],
                     'T1∩T2∩T4 (52w+rsi+lowF)')
    rows += evaluate(df, df['T1_low52w'] & df['T3_weak_rev'] & df['T4_low_fscore'],
                     'T1∩T3∩T4 (52w+rev+lowF)')

    # 4-way
    rows += evaluate(df, df['T1_low52w'] & df['T2_weak_rsi'] & df['T3_weak_rev'] & df['T4_low_fscore'],
                     'T1∩T2∩T3∩T4 (full trap)')

    for r in rows:
        if pd.isna(r['pocket_mean']):
            print(f"{r['signal']:40s} {r['horizon']:>3d}d {r['n_obs']:>8d} {'n/a':>10s} {r['universe_mean']:>+10.4f} {'n/a':>10s} {'n/a':>8s}")
        else:
            print(f"{r['signal']:40s} {r['horizon']:>3d}d {r['n_obs']:>8d} {r['pocket_mean']:>+10.4f} {r['universe_mean']:>+10.4f} {r['alpha']:>+10.4f} {r['snr']:>+8.2f}")

    out_csv = out_dir / 'vf_value_trap_pockets.csv'
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    print(f"\nSaved: {out_csv}")

    print("\n=== Decision Threshold ===")
    print("  落地：alpha @ 120d <= -0.03 AND |snr| >= 2.0")
    print("  weak warning：alpha @ 120d -0.01 ~ -0.03")
    print("  歸檔：alpha @ 120d > -0.01")

    print("\n=== Top 5 Most Negative Alpha @ 120d ===")
    df_r = pd.DataFrame(rows)
    h120 = df_r[df_r['horizon'] == 120].dropna(subset=['alpha']).sort_values('alpha').head(5)
    for _, r in h120.iterrows():
        verdict = "LAND" if (r['alpha'] <= -0.03 and abs(r['snr']) >= 2.0) else \
                  "WARN" if (r['alpha'] <= -0.01) else "PLAIN"
        print(f"  {r['signal']:40s} alpha={r['alpha']:+.4f} snr={r['snr']:+.2f} n={r['n_obs']:>6d} -> {verdict}")

    # ============================================================
    # Yearly breakdown for top trap candidates -- guard against
    # multi-year outlier driven false positives (per
    # project_validation_bias_warning.md SOP: leave-one-year-out)
    # ============================================================
    print("\n=== Yearly breakdown (alpha @ 120d) for key trap signals ===")
    df['year'] = df['week_end_date'].dt.year
    target = 'fwd_120d'

    key_signals = [
        ('T4_low_fscore (baseline)', df['T4_low_fscore']),
        ('T3_only (weak_rev)', df['T3_weak_rev']),
        ('T3 cap T4 (rev+lowF)', df['T3_weak_rev'] & df['T4_low_fscore']),
        ('T2 cap T4 (rsi+lowF)', df['T2_weak_rsi'] & df['T4_low_fscore']),
        ('T1 cap T2 cap T3 cap T4 (full)', df['T1_low52w'] & df['T2_weak_rsi'] & df['T3_weak_rev'] & df['T4_low_fscore']),
    ]

    yearly_rows = []
    print(f"\n{'Signal':40s} {'2020':>9s} {'2021':>9s} {'2022':>9s} {'2023':>9s} {'2024':>9s} {'2025':>9s} {'years_neg':>10s}")
    print("-" * 110)
    for label, mask in key_signals:
        line = f"{label:40s}"
        years_neg = 0
        for yr in range(2020, 2026):
            yr_mask = (df['year'] == yr) & mask & df[target].notna()
            yr_univ_mask = (df['year'] == yr) & df[target].notna()
            if yr_mask.sum() < 50:
                line += f" {'n/a':>9s}"
                continue
            pocket_mean = df.loc[yr_mask, target].mean()
            univ_mean = df.loc[yr_univ_mask, target].mean()
            alpha = pocket_mean - univ_mean
            if alpha < 0:
                years_neg += 1
            line += f" {alpha:>+9.4f}"
            yearly_rows.append({'signal': label, 'year': yr, 'alpha': alpha,
                                'pocket_mean': pocket_mean, 'univ_mean': univ_mean,
                                'n_obs': int(yr_mask.sum())})
        line += f" {years_neg:>9d}/6"
        print(line)

    pd.DataFrame(yearly_rows).to_csv(out_dir / 'vf_value_trap_pockets_yearly.csv', index=False)
    print(f"\nSaved yearly: {out_dir / 'vf_value_trap_pockets_yearly.csv'}")
    print("\nDecision rule: 落地需 years_neg >= 5/6 (僅容許 1 年偏差)")


if __name__ == '__main__':
    main()
