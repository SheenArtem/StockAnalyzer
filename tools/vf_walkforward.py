"""
vf_walkforward.py - Walk-forward 穩定性驗證 (V1 vs V3 權重方案)

將 6 年歷史切成多個 rolling window：
  - 12 個月 training (看 basket return、算 Sharpe)
  - 3 個月 test (forward return)
  - 每 3 個月滑動一次
  - 共約 20 slides

對每個 slide，用 V1 (30/25/15/15/15) 和 V3 (50/20/10/10/10) 各挑 top 50，
比較 test 階段的 basket return + Sharpe。

若 V3 在 > 70% slides 上勝 V1 → 穩定（落地信心強）
若 V3 只勝 55-70% → marginal（繼續觀察）
若 V3 勝 < 55% → 不穩定，可能 overfit

輸出:
  console 詳表 + reports/vf_walkforward_result.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SNAPSHOT_PATH = ROOT / "data_cache" / "backtest" / "trade_journal_value_tw_snapshot.parquet"
REPORT_DIR = ROOT / "reports"

SCHEMES = {
    "V1":      {'valuation': 0.30, 'quality': 0.25, 'revenue': 0.15, 'technical': 0.15, 'smart_money': 0.15},
    # V_clean_a: VF-VE 砍 SM 後正式落地版 (2026-04-19)
    "Vcleana": {'valuation': 0.35, 'quality': 0.30, 'revenue': 0.18, 'technical': 0.17, 'smart_money': 0.00},
    # 其他 option 對比 (VF-VF walk-forward 備存)
    "V8":      {'valuation': 0.40, 'quality': 0.40, 'revenue': 0.20, 'technical': 0.00, 'smart_money': 0.00},
    "V10":     {'valuation': 0.55, 'quality': 0.30, 'revenue': 0.15, 'technical': 0.00, 'smart_money': 0.00},
    "V11":     {'valuation': 0.60, 'quality': 0.25, 'revenue': 0.10, 'technical': 0.05, 'smart_money': 0.00},
}


def compute_score(df: pd.DataFrame, w: dict) -> pd.Series:
    return (df['valuation_s'] * w['valuation']
            + df['quality_s'] * w['quality']
            + df['revenue_s'] * w['revenue']
            + df['technical_s'] * w['technical']
            + df['smart_money_s'] * w['smart_money'])


def basket_returns(df: pd.DataFrame, score_col: str, horizon: int,
                    top_n: int = 50) -> pd.DataFrame:
    """Per-week top_n basket return."""
    target = f'fwd_{horizon}d'
    rows = []
    for wd, grp in df.groupby('week_end_date'):
        sub = grp.dropna(subset=[score_col, target])
        if len(sub) < 10:
            continue
        top = sub.nlargest(top_n, score_col)
        rows.append({'week': wd, 'ret': top[target].mean()})
    return pd.DataFrame(rows)


def window_stats(baskets: pd.DataFrame) -> dict:
    if baskets.empty:
        return {'mean': np.nan, 'std': np.nan, 'sharpe': np.nan, 'n': 0}
    mean = baskets['ret'].mean()
    std = baskets['ret'].std(ddof=1)
    sharpe = mean / std if std > 0 else np.nan
    return {'mean': mean, 'std': std, 'sharpe': sharpe, 'n': len(baskets)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", default=str(SNAPSHOT_PATH))
    ap.add_argument("--horizon", type=int, default=60)
    ap.add_argument("--train-months", type=int, default=12)
    ap.add_argument("--test-months", type=int, default=3)
    ap.add_argument("--step-months", type=int, default=3)
    args = ap.parse_args()

    df = pd.read_parquet(args.snapshot)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    print(f"Loaded: {len(df)} rows, {df['week_end_date'].nunique()} weeks, "
          f"{df['stock_id'].nunique()} unique stocks")
    print(f"Horizon: fwd_{args.horizon}d")
    print(f"Window: {args.train_months}m train / {args.test_months}m test, slide {args.step_months}m")
    print()

    # Compute all score columns once
    for name, w in SCHEMES.items():
        df[f'score_{name}'] = compute_score(df, w)

    start_date = df['week_end_date'].min() + pd.DateOffset(months=args.train_months)
    end_date = df['week_end_date'].max() - pd.DateOffset(months=args.test_months)

    slides = []
    current = start_date
    while current <= end_date:
        test_start = current
        test_end = current + pd.DateOffset(months=args.test_months)
        slides.append((test_start, test_end))
        current = current + pd.DateOffset(months=args.step_months)

    print(f"Total slides: {len(slides)}")
    print()
    print("=" * (22 + 14 * len(SCHEMES)))
    hdr = f"{'Slide':<20}"
    for name in SCHEMES:
        hdr += f"{name+' Sh':>14}"
    print(hdr)
    print("-" * (22 + 14 * len(SCHEMES)))

    results = []
    for i, (t0, t1) in enumerate(slides):
        test_df = df[(df['week_end_date'] >= t0) & (df['week_end_date'] < t1)]
        if test_df.empty:
            continue
        row = {'slide': i, 'test_start': t0.date(), 'test_end': t1.date()}
        for name in SCHEMES:
            baskets = basket_returns(test_df, f'score_{name}', args.horizon)
            stats = window_stats(baskets)
            row[f'{name}_ret'] = stats['mean']
            row[f'{name}_sharpe'] = stats['sharpe']
            row[f'{name}_n'] = stats['n']
        results.append(row)
        label = f"{t0.strftime('%Y-%m')}→{t1.strftime('%Y-%m')}"
        line = f"{label:<20}"
        for name in SCHEMES:
            s = row.get(f'{name}_sharpe', np.nan)
            line += f"{s:>14.3f}"
        print(line)

    # Summary
    r = pd.DataFrame(results)
    print()
    print("=" * 100)
    print("Summary Statistics")
    print("=" * 100)
    for name in SCHEMES:
        ret_col = f'{name}_ret'
        sharpe_col = f'{name}_sharpe'
        print(f"  {name}: avg_ret={r[ret_col].mean():.2%}, "
              f"avg_sharpe={r[sharpe_col].mean():.3f}, "
              f"std_sharpe={r[sharpe_col].std(ddof=1):.3f}")
    # Pairwise win rate vs V1
    print()
    print("Each scheme vs V1 baseline:")
    for name in SCHEMES:
        if name == 'V1':
            continue
        sh_col = f'{name}_sharpe'
        ret_col = f'{name}_ret'
        if sh_col not in r.columns:
            continue
        sh_win = (r[sh_col] > r['V1_sharpe']).sum()
        ret_win = (r[ret_col] > r['V1_ret']).sum()
        print(f"  {name}: Sharpe wins {sh_win}/{len(r)} ({100*sh_win/len(r):.0f}%), "
              f"Ret wins {ret_win}/{len(r)} ({100*ret_win/len(r):.0f}%)")

    # Save CSV
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "vf_walkforward_result.csv"
    r.to_csv(out, index=False)
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
