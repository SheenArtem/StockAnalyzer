"""
vf_valuetrap_3cond_validation.py - 驗證 Value-#3 價值陷阱三條件是否有 alpha

Hypothesis（來源 project_value_enhancement.md）：
  候選條件刷掉「便宜但持續下跌」的股票：
    C1. 週 MA20 跌破超過 5%        (Close / MA20w - 1 < -5%)
    C2. 月營收 YoY 連 2 月轉負       (revenue_year_growth < 0 兩月)
    C3. 股價 52 週低點附近 20% 內     (low52w_prox <= 1.20)

測試設計：
  Universe: trade_journal_value_tw_snapshot (70K, 309 週, 2020-2025)
  Horizon: fwd_20d / fwd_60d / fwd_120d
  IC: 每週截面 Spearman rank corr (flag int 0/1 vs fwd return)
  Portfolio: 比較 flagged=1 vs flagged=0 的 mean fwd return

判讀：
  - IR < -0.3 → D 級扣分 (Stage 2 -5~-10)
  - IR < -0.5 → D+ 級 hard filter (Stage 1)
  - IR > -0.3 → 無 alpha 不整合（VF 系列原則）

Usage:
  python tools/vf_valuetrap_3cond_validation.py --market tw --out reports/
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
    """Weekly value snapshot with fwd returns."""
    df = pd.read_parquet(DATA / "trade_journal_value_tw_snapshot.parquet")
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df['stock_id'] = df['stock_id'].astype(str)
    return df


def compute_ma_break(snap):
    """Condition 1: Close / MA100_daily - 1 < -0.05  (weekly MA20 proxy).

    Compute 100-day rolling mean from daily indicators, align to week_end_date.
    """
    daily = pd.read_parquet(DATA / "value_sim_indicators.parquet",
                            columns=['stock_id', 'date', 'Close'])
    daily['stock_id'] = daily['stock_id'].astype(str)
    daily['date'] = pd.to_datetime(daily['date'])
    daily = daily.sort_values(['stock_id', 'date'])
    daily['ma100'] = daily.groupby('stock_id')['Close'].transform(
        lambda s: s.rolling(100, min_periods=60).mean()
    )

    # merge on (stock_id, date=week_end_date)
    snap_ma = snap[['stock_id', 'week_end_date']].merge(
        daily[['stock_id', 'date', 'ma100']].rename(columns={'date': 'week_end_date'}),
        on=['stock_id', 'week_end_date'], how='left'
    )
    # fallback: 若 exact week_end 無 daily 紀錄（週末/假日），往前找 5 天內最近一筆
    if snap_ma['ma100'].isna().sum() > 0:
        daily_idx = daily.set_index(['stock_id', 'date'])['ma100']
        for idx, row in snap_ma[snap_ma['ma100'].isna()].iterrows():
            sid = row['stock_id']
            wed = row['week_end_date']
            for back in range(1, 6):
                try:
                    ma = daily_idx.loc[(sid, wed - pd.Timedelta(days=back))]
                    snap_ma.at[idx, 'ma100'] = ma
                    break
                except KeyError:
                    continue

    snap = snap.merge(snap_ma, on=['stock_id', 'week_end_date'], how='left')
    snap['ma_break_5pct'] = (snap['Close'] / snap['ma100'] - 1 < -0.05).astype(int)
    # NaN → 0 (未知狀態視為沒破 MA，conservative)
    snap.loc[snap['ma100'].isna(), 'ma_break_5pct'] = 0
    return snap


def compute_rev_yoy_consecutive(snap):
    """Condition 2: 連 2 月 revenue_year_growth < 0."""
    rev = pd.read_parquet(DATA / "financials_revenue.parquet",
                          columns=['stock_id', 'date', 'revenue_year_growth'])
    rev['stock_id'] = rev['stock_id'].astype(str)
    rev['date'] = pd.to_datetime(rev['date'])
    rev = rev.sort_values(['stock_id', 'date'])
    # 連 2 月 < 0: current < 0 AND previous < 0
    rev['yoy_prev'] = rev.groupby('stock_id')['revenue_year_growth'].shift(1)
    rev['rev_yoy_neg2'] = ((rev['revenue_year_growth'] < 0) &
                           (rev['yoy_prev'] < 0)).astype(int)

    # rev 是月頻，snap 是週頻。對每個 (stock_id, week_end_date) 取最近的 monthly rev 記錄
    # Revenue publish lag: ~10 days after month end (e.g. Jan revenue published ~Feb 10)
    # 用 merge_asof backward，但要求整體按 time key 單調（不能 by-group sort）
    rev = rev.rename(columns={'date': 'rev_date'})
    snap_sorted = snap.sort_values('week_end_date').reset_index(drop=True)
    rev_sorted = rev[['stock_id', 'rev_date', 'rev_yoy_neg2']].sort_values('rev_date').reset_index(drop=True)

    out = pd.merge_asof(
        snap_sorted,
        rev_sorted,
        by='stock_id',
        left_on='week_end_date',
        right_on='rev_date',
        tolerance=pd.Timedelta(days=45),
        direction='backward',
    )
    out['rev_yoy_neg2'] = out['rev_yoy_neg2'].fillna(0).astype(int)
    return out


def compute_52w_low(snap):
    """Condition 3: low52w_prox <= 1.20 (Close within 20% above 52w low)."""
    snap['near_52w_low'] = (snap['low52w_prox'] <= 1.20).fillna(False).astype(int)
    return snap


def weekly_ic(df, factor, horizon):
    """Weekly cross-sectional Spearman IC."""
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    ics = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 10:
            continue
        # 若 factor 全同值（全 0 或全 1）Spearman 會給 NaN，跳過
        if grp[factor].nunique() < 2:
            continue
        rho, _ = stats.spearmanr(grp[factor], grp[target])
        if not np.isnan(rho):
            ics.append(rho)
    if not ics:
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    arr = np.array(ics)
    return {
        'IC': float(arr.mean()),
        'IR': float(arr.mean() / arr.std(ddof=1)) if arr.std(ddof=1) > 0 else float('nan'),
        'weeks': len(arr),
    }


def flag_vs_nonflag_ret(df, factor, horizon):
    """Portfolio-level: mean fwd return, flagged (factor=1) vs not (factor=0)."""
    target = f'fwd_{horizon}d'
    sub = df[[factor, target]].dropna()
    flagged = sub[sub[factor] == 1][target]
    nonflag = sub[sub[factor] == 0][target]
    return {
        'n_flag': len(flagged),
        'n_nonflag': len(nonflag),
        'flag_mean_ret': float(flagged.mean()) if len(flagged) else float('nan'),
        'nonflag_mean_ret': float(nonflag.mean()) if len(nonflag) else float('nan'),
        'spread': float(flagged.mean() - nonflag.mean()) if len(flagged) and len(nonflag)
                  else float('nan'),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--market', default='tw')
    ap.add_argument('--out', default='reports/')
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[1/4] Loading snapshot...")
    snap = load_snapshot()
    print(f"  {len(snap)} rows, {snap['stock_id'].nunique()} stocks, "
          f"{snap['week_end_date'].nunique()} weeks "
          f"({snap['week_end_date'].min().date()} ~ {snap['week_end_date'].max().date()})")

    print("[2/4] Computing C1 (MA100 break)...")
    snap = compute_ma_break(snap)
    print(f"  ma_break_5pct flag rate: {snap['ma_break_5pct'].mean():.1%}")

    print("[3/4] Computing C2 (rev YoY 2-month neg)...")
    snap = compute_rev_yoy_consecutive(snap)
    print(f"  rev_yoy_neg2 flag rate: {snap['rev_yoy_neg2'].mean():.1%}")

    print("[4/4] Computing C3 (near 52w low)...")
    snap = compute_52w_low(snap)
    print(f"  near_52w_low flag rate: {snap['near_52w_low'].mean():.1%}")

    # Combined: any 1, majority (>=2), all 3
    snap['trap_any'] = (snap['ma_break_5pct'] + snap['rev_yoy_neg2']
                        + snap['near_52w_low']).clip(upper=1)
    snap['trap_count'] = snap['ma_break_5pct'] + snap['rev_yoy_neg2'] + snap['near_52w_low']
    snap['trap_majority'] = (snap['trap_count'] >= 2).astype(int)
    snap['trap_all3'] = (snap['trap_count'] == 3).astype(int)

    print(f"\nCombined flag rates:")
    for col in ['trap_any', 'trap_majority', 'trap_all3']:
        print(f"  {col}: {snap[col].mean():.1%}")

    # IC + portfolio spread
    factors = ['ma_break_5pct', 'rev_yoy_neg2', 'near_52w_low',
               'trap_any', 'trap_majority', 'trap_all3']
    horizons = [20, 60, 120]

    rows = []
    print(f"\n{'Factor':20s} {'Horizon':>8s} {'IC':>8s} {'IR':>8s} "
          f"{'N_flag':>8s} {'Flag_ret':>10s} {'NonFlag':>10s} {'Spread':>10s}")
    print("-" * 100)
    for f in factors:
        for h in horizons:
            ic = weekly_ic(snap, f, h)
            pt = flag_vs_nonflag_ret(snap, f, h)
            row = {'factor': f, 'horizon': h, **ic, **pt}
            rows.append(row)
            print(f"{f:20s} {h:>7d}d {ic['IC']:>+8.4f} {ic['IR']:>+8.3f} "
                  f"{pt['n_flag']:>8d} {pt['flag_mean_ret']:>+10.4f} "
                  f"{pt['nonflag_mean_ret']:>+10.4f} {pt['spread']:>+10.4f}")

    df = pd.DataFrame(rows)
    out_csv = out_dir / 'vf_valuetrap_3cond_ic.csv'
    df.to_csv(out_csv, index=False)
    print(f"\nSaved: {out_csv}")

    # 判讀 summary
    print("\n=== Decision Summary (VF 原則: IR < -0.3 D 扣分 / IR < -0.5 升為 hard filter) ===")
    for f in factors:
        sub = df[df['factor'] == f]
        best_ir = sub['IR'].min()  # most negative (best for trap indicator)
        if pd.isna(best_ir):
            verdict = "no data"
        elif best_ir < -0.5:
            verdict = "D+ hard filter"
        elif best_ir < -0.3:
            verdict = "D -5~-10 penalty"
        elif best_ir < 0:
            verdict = "weak negative (keep observation)"
        else:
            verdict = "no alpha (skip)"
        print(f"  {f:20s} best IR = {best_ir:+.3f} -> {verdict}")


if __name__ == '__main__':
    main()
