"""
vf_va_walkforward.py - VF-VA Layer 4: PE threshold 20 -> 12 quarterly walk-forward

VF-VA Layer 1-3 (vf_va_validation.py) 已證 PE (lower) IR 0.242 B / decile spread +3.41%。
Action 候選：Stage 1 max_pe 20 -> 12。本檔檢驗該候選是否穩定到足以 live 落地。

方法:
  每季 (2020Q1 - 2025Q4, 24 季):
    - 對 fair snapshot filter by PE<20 (baseline) vs PE<12 (proposed)
    - 用 V_live weights (30/25/30/15/0) 算 value_score, 取 top 50
    - 算 basket mean fwd_60d return (均等加權)
  比較: PE<12 在幾季贏 PE<20?

判定:
  PE<12 贏 >= 14/24 季 (58%) 且 all-period mean > baseline -> 落地候選
  Win% < 55% 或 all-mean < baseline -> 拒絕 (overfit / 方向不穩)

用法:
  python tools/vf_va_walkforward.py
  python tools/vf_va_walkforward.py --horizon 40
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

# V_live weights (VF-VC P3-b 落地 2026-04-20)
W_LIVE = {
    'valuation': 0.30,
    'quality': 0.25,
    'revenue': 0.30,
    'technical': 0.15,
    'smart_money': 0.00,
}


def compute_value_score(df: pd.DataFrame, w: dict) -> pd.Series:
    return (df['valuation_s'] * w['valuation']
            + df['quality_s'] * w['quality']
            + df['revenue_s'] * w['revenue']
            + df['technical_s'] * w['technical']
            + df['smart_money_s'] * w['smart_money'])


def basket_ret(df: pd.DataFrame, score_col: str, top_n: int, horizon: int) -> pd.DataFrame:
    target = f'fwd_{horizon}d'
    out = []
    for wd, grp in df.groupby('week_end_date'):
        sub = grp.dropna(subset=[score_col, target])
        if len(sub) < 10:
            continue
        top = sub.nlargest(top_n, score_col)
        out.append({'week': wd, 'ret': top[target].mean(), 'n': len(top)})
    return pd.DataFrame(out)


def quarterly_basket(df: pd.DataFrame, score_col: str, top_n: int, horizon: int) -> pd.DataFrame:
    b = basket_ret(df, score_col, top_n, horizon)
    if b.empty:
        return b
    b['quarter'] = pd.to_datetime(b['week']).dt.to_period('Q')
    return b.groupby('quarter').agg(
        mean_ret=('ret', 'mean'),
        std_ret=('ret', 'std'),
        n_weeks=('ret', 'count'),
    ).reset_index()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--horizon', type=int, default=60)
    ap.add_argument('--top-n', type=int, default=50)
    ap.add_argument('--save-md', action='store_true')
    args = ap.parse_args()

    if not SNAPSHOT_PATH.exists():
        print(f"FATAL: snapshot not found at {SNAPSHOT_PATH}")
        sys.exit(1)

    snap = pd.read_parquet(SNAPSHOT_PATH)
    snap['week_end_date'] = pd.to_datetime(snap['week_end_date'])
    # Apply live weights fresh
    snap['sc_live'] = compute_value_score(snap, W_LIVE)

    # Stage 1 filters: Graham pe_x_pb_max=22.5 + min_pe=0.1 + other fields held same,
    # only vary max_pe
    # Also apply same pe_x_pb<22.5 gate to both scenarios
    snap_pe_valid = snap.dropna(subset=['pe']).copy()
    snap_pe_valid = snap_pe_valid[snap_pe_valid['pe'] >= 0.1]

    # Graham Stage 1 gate (same for both scenarios): pe_x_pb <= 22.5 if both present
    has_both = snap_pe_valid['pe'].notna() & snap_pe_valid['pb'].notna()
    pe_x_pb = snap_pe_valid['pe'] * snap_pe_valid['pb']
    graham_pass = (has_both & (pe_x_pb <= 22.5)) | ~has_both
    snap_g = snap_pe_valid[graham_pass].copy()

    scenarios = {
        'PE<20 (baseline)': snap_g[snap_g['pe'] < 20].copy(),
        'PE<15': snap_g[snap_g['pe'] < 15].copy(),
        'PE<12 (proposed)': snap_g[snap_g['pe'] < 12].copy(),
        'PE<10': snap_g[snap_g['pe'] < 10].copy(),
    }

    print(f"Horizon: fwd_{args.horizon}d, Top-N: {args.top_n}")
    print(f"Date range: {snap['week_end_date'].min().date()} ~ {snap['week_end_date'].max().date()}")
    print(f"Total snapshot rows: {len(snap)}")
    print(f"After PE valid + Graham gate: {len(snap_g)}")
    print()

    # Compute baskets
    baskets = {}
    for name, sub in scenarios.items():
        if sub.empty:
            print(f"  {name}: EMPTY (0 rows after filter)")
            continue
        b = basket_ret(sub, 'sc_live', args.top_n, args.horizon)
        baskets[name] = b
        if b.empty:
            print(f"  {name}: {len(sub)} rows, 0 valid weeks")
            continue
        print(f"  {name}: {len(sub)} rows, {b['n'].mean():.0f} picks/week avg, {len(b)} weeks valid")
    print()

    # All-period metrics
    print("=" * 90)
    print(f"All-period (fwd_{args.horizon}d)")
    print("=" * 90)
    print(f"{'Scenario':<25}{'MeanRet':>12}{'Std':>10}{'Sharpe':>10}{'Weeks':>8}{'vs Base':>10}")
    print("-" * 90)
    base_mean = baskets['PE<20 (baseline)']['ret'].mean() if 'PE<20 (baseline)' in baskets else np.nan
    for name, b in baskets.items():
        mean_r = b['ret'].mean()
        std_r = b['ret'].std()
        sh = mean_r / std_r if std_r > 0 else np.nan
        delta = mean_r - base_mean
        print(f"{name:<25}{mean_r:>12.4%}{std_r:>10.4%}{sh:>10.3f}{len(b):>8}{delta:>+10.2%}")
    print()

    # Quarterly walk-forward vs baseline
    print("=" * 90)
    print(f"Quarterly walk-forward vs baseline PE<20")
    print("=" * 90)
    base_q = quarterly_basket(scenarios['PE<20 (baseline)'], 'sc_live', args.top_n, args.horizon)
    base_q_idx = base_q.set_index('quarter')['mean_ret']

    header = f"{'Scenario':<25}{'qMean':>10}{'qSharpe':>10}{'Wins/N':>10}{'WinRate':>10}{'Verdict':>20}"
    print(header)
    print("-" * 90)

    results = {}
    for name in ['PE<15', 'PE<12 (proposed)', 'PE<10']:
        if name not in baskets:
            continue
        q = quarterly_basket(scenarios[name], 'sc_live', args.top_n, args.horizon)
        q_idx = q.set_index('quarter')['mean_ret']
        aligned = pd.DataFrame({'base': base_q_idx, 'this': q_idx}).dropna()
        wins = (aligned['this'] > aligned['base']).sum()
        total = len(aligned)
        win_rate = wins / total if total else np.nan
        q_mean = q['mean_ret'].mean()
        q_sharpe = q['mean_ret'].mean() / q['mean_ret'].std() if q['mean_ret'].std() > 0 else np.nan

        if win_rate >= 0.58 and baskets[name]['ret'].mean() > base_mean:
            verdict = "LAND candidate"
        elif win_rate < 0.50 or baskets[name]['ret'].mean() < base_mean:
            verdict = "REJECT"
        else:
            verdict = "MARGINAL"

        print(f"{name:<25}{q_mean:>10.4%}{q_sharpe:>10.3f}{wins:>4}/{total:<5}{win_rate:>10.1%}{verdict:>20}")
        results[name] = {'win_rate': win_rate, 'wins': wins, 'total': total,
                         'q_mean': q_mean, 'verdict': verdict}
    print()

    # By-year stability
    print("=" * 90)
    print(f"By-year basket mean (fwd_{args.horizon}d)")
    print("=" * 90)
    years = sorted(pd.to_datetime(baskets['PE<20 (baseline)']['week']).dt.year.unique())
    print(f"{'Year':<8}" + ''.join(f"{n:>22}" for n in baskets.keys()))
    print("-" * 100)
    for y in years:
        row = f"{y:<8}"
        for name, b in baskets.items():
            mask = pd.to_datetime(b['week']).dt.year == y
            r = b[mask]['ret'].mean() if mask.any() else np.nan
            n = mask.sum()
            row += f"{r:>15.4%} ({n:>3}w)"
        print(row)
    print()

    # Decision
    print("=" * 90)
    print("Decision")
    print("=" * 90)
    print("判定規則:")
    print("  LAND:     qWF win rate >= 58% AND all-period mean > baseline")
    print("  REJECT:   qWF win rate < 50% OR all-period mean < baseline")
    print("  MARGINAL: 50% <= win rate < 58%")
    print()
    proposed = results.get('PE<12 (proposed)', {})
    print(f"PE<12 (proposed): {proposed.get('verdict', 'N/A')}")
    print(f"  Win rate: {proposed.get('win_rate', np.nan):.1%} ({proposed.get('wins')}/{proposed.get('total')})")
    print(f"  Quarterly mean: {proposed.get('q_mean', np.nan):.4%}")

    if args.save_md:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        md = REPORT_DIR / 'vf_va_walkforward.md'
        md.write_text(
            f"# VF-VA Walk-forward — PE 20 -> 12\n\n"
            f"Snapshot: {SNAPSHOT_PATH.name}\n\n"
            f"Horizon: fwd_{args.horizon}d / Top-{args.top_n}\n\n"
            f"## Verdict\n\n"
            f"- PE<12: **{proposed.get('verdict', 'N/A')}** "
            f"({proposed.get('win_rate', np.nan):.1%} qWF win rate)\n\n"
            f"(See console output for full table.)\n",
            encoding='utf-8'
        )
        print(f"\nReport -> {md}")


if __name__ == '__main__':
    main()
