"""
walkforward_chip_validation.py -- Walk-Forward Phase 2 (Tech + Chip)

Phase 1 tested pure tech combos (IS 2011-2020 / OOS 2021-2025).
Phase 2 adds chip factors in the chip-data era (2021-2026).

Chip data: 2021-04-16 ~ 2026-04-15 (5 years)
Default split: IS 2021-07 ~ 2023-12, OOS 2024-01 ~ 2026-04
(start from July to let rolling windows warm up)

Key output: tech_only vs tech_chip_top on SAME stock universe (fair comparison).

Usage:
    python tools/walkforward_chip_validation.py                # full
    python tools/walkforward_chip_validation.py --sample 300   # quick test
    python tools/walkforward_chip_validation.py --split 2024   # custom split year
"""
import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from tools.chip_combo_analysis import (
    load_and_merge,
    rank_normalize_all,
    build_combos as build_chip_combos,
    CHIP_SIGNAL_COLS,
)
from tools.indicator_combo_analysis import build_combos as build_tech_combos
from tools.indicator_ic_analysis import HORIZONS
from tools.indicator_quantile_returns import simulate_topn_portfolio

OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "walkforward_chip_results.csv"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("wf_chip")

TOP_N = [20]
CHIP_WARMUP = '2021-07-01'  # skip first ~2 months for rolling window warmup


def run_period(df, period_name, combos, horizons, top_n_list, universes):
    """Run Top-N simulation for one time period. Reuse Phase 1 logic."""
    rows = []
    for combo_col, combo_label in combos.items():
        for h in horizons:
            ret_col = f'fwd_{h}d'
            if ret_col not in df.columns:
                continue
            for n in top_n_list:
                for uni_name, uni_filter in universes:
                    result = simulate_topn_portfolio(
                        df, combo_col, ret_col, n=n,
                        universe_filter=uni_filter,
                        long_direction='top',
                    )
                    if result:
                        rows.append({
                            'period': period_name,
                            'score': combo_col,
                            'label': combo_label,
                            'horizon': h,
                            'universe': uni_name,
                            'top_n': n,
                            'mean_ret': result['mean_ret'],
                            'std_ret': result['std_ret'],
                            'sharpe': result['sharpe_proxy'],
                            'win_rate': result['win_rate'],
                            'n_days': result['n_days'],
                        })
    return rows


def main():
    parser = argparse.ArgumentParser(description='Walk-forward Phase 2: Tech + Chip')
    parser.add_argument('--sample', type=int, default=None, help='Sample N tickers')
    parser.add_argument('--split', type=int, default=2024, help='OOS start year (default: 2024)')
    args = parser.parse_args()

    t0 = time.time()
    split_date = f'{args.split}-01-01'

    # 1. Load tech + chip merged panel
    logger.info("Loading tech + chip merged panel...")
    df = load_and_merge(sample=args.sample)

    # 2. Rank normalize all signals (tech + chip)
    df = rank_normalize_all(df)

    # 3. Build BOTH tech combos and chip combos
    tech_combos = build_tech_combos(df)
    chip_combos = build_chip_combos(df)

    # Select key combos for comparison
    combos = {}
    # Tech baselines
    for key in ['combo_3group_median_raw', 'combo_rvol_lowatr']:
        if key in tech_combos:
            combos[key] = tech_combos[key]
    # Chip combos
    for key in ['combo_chip_top3', 'combo_tech_chip_top', 'combo_tech70_chip30']:
        if key in chip_combos:
            combos[key] = chip_combos[key]

    # 4. Add has_chip flag (for fair comparison universe)
    chip_check_col = CHIP_SIGNAL_COLS[0]  # inst_foreign_5d
    df['has_chip'] = df[chip_check_col].notna()
    chip_pct = df['has_chip'].mean() * 100
    logger.info(f"Chip coverage: {chip_pct:.1f}% of rows")

    # 5. Trim to chip era + warm-up
    df = df[df['date'] >= CHIP_WARMUP].copy()
    logger.info(f"After chip warmup filter: {len(df):,} rows, "
                f"{df['date'].min().date()} ~ {df['date'].max().date()}")

    # 6. Split IS / OOS
    df_is = df[df['date'] < split_date].copy()
    df_oos = df[df['date'] >= split_date].copy()
    logger.info(f"IS:  {len(df_is):,} rows ({df_is['date'].min().date()} ~ {df_is['date'].max().date()})")
    logger.info(f"OOS: {len(df_oos):,} rows ({df_oos['date'].min().date()} ~ {df_oos['date'].max().date()})")

    # 7. Define universes
    #   - 'all': full market (tech combos use all, chip combos naturally drop NaN)
    #   - 'chip_avail': restrict to rows with chip data (fair tech vs chip comparison)
    universes = [
        ('all', None),
        ('chip_avail', 'has_chip'),
    ]

    # 8. Run simulations
    horizons = HORIZONS
    logger.info("Running IS simulations...")
    is_rows = run_period(df_is, 'IS', combos, horizons, TOP_N, universes)
    logger.info("Running OOS simulations...")
    oos_rows = run_period(df_oos, 'OOS', combos, horizons, TOP_N, universes)

    # 9. Build result table
    all_rows = is_rows + oos_rows
    result_df = pd.DataFrame(all_rows)
    result_df.to_csv(OUT_FILE, index=False)
    logger.info(f"Saved {len(result_df)} rows to {OUT_FILE}")

    # 10. Print comparison tables
    print("\n" + "=" * 100)
    print(f"  WALK-FORWARD PHASE 2: TECH + CHIP (split: {split_date})")
    print(f"  IS: {df_is['date'].min().date()} ~ {df_is['date'].max().date()}")
    print(f"  OOS: {df_oos['date'].min().date()} ~ {df_oos['date'].max().date()}")
    print("=" * 100)

    for uni_name in ['all', 'chip_avail']:
        print(f"\n{'=' * 100}")
        print(f"  Universe: {uni_name}")
        print(f"{'=' * 100}")

        sub = result_df[
            (result_df['top_n'] == 20) &
            (result_df['universe'] == uni_name)
        ]
        if sub.empty:
            print("  (no data)")
            continue

        # Pivot IS vs OOS
        pivot = sub.pivot_table(
            index=['score', 'label', 'horizon'],
            columns='period',
            values=['sharpe', 'mean_ret', 'win_rate'],
            aggfunc='first',
        )
        pivot.columns = [f'{val}_{period}' for val, period in pivot.columns]
        pivot = pivot.reset_index()

        # Decay ratio
        if 'sharpe_IS' in pivot.columns and 'sharpe_OOS' in pivot.columns:
            pivot['decay%'] = pivot.apply(
                lambda r: (r['sharpe_OOS'] / r['sharpe_IS'] - 1) * 100
                if r['sharpe_IS'] != 0 else 0, axis=1
            )

        # Print per combo
        for combo in combos:
            combo_sub = pivot[pivot['score'] == combo]
            if combo_sub.empty:
                continue
            label = combo_sub.iloc[0]['label']
            domain = 'CHIP' if 'chip' in combo else 'TECH'
            print(f"\n  [{domain}] {combo}")
            print(f"  {label}")
            print(f"  {'H':>5} {'IS Sharpe':>10} {'OOS Sharpe':>11} {'Decay%':>8} "
                  f"{'IS Win%':>8} {'OOS Win%':>9} {'IS Ret%':>8} {'OOS Ret%':>9}")
            print(f"  {'-' * 82}")
            for _, row in combo_sub.sort_values('horizon').iterrows():
                h = int(row['horizon'])
                is_sh = row.get('sharpe_IS', 0) or 0
                oos_sh = row.get('sharpe_OOS', 0) or 0
                decay = row.get('decay%', 0) or 0
                is_wr = (row.get('win_rate_IS', 0) or 0) * 100
                oos_wr = (row.get('win_rate_OOS', 0) or 0) * 100
                is_mr = (row.get('mean_ret_IS', 0) or 0) * 100
                oos_mr = (row.get('mean_ret_OOS', 0) or 0) * 100
                # Flag: OOS > IS = good (green), OOS much worse = bad (red)
                flag = '+' if oos_sh >= is_sh else '-'
                print(f"  {h:>4}d {is_sh:>+10.2f} {oos_sh:>+11.2f} {decay:>+7.1f}% "
                      f"{is_wr:>7.1f}% {oos_wr:>8.1f}% "
                      f"{is_mr:>+7.3f}% {oos_mr:>+8.3f}% {flag}")

    # 11. Summary: head-to-head at 20d horizon, chip_avail universe
    print("\n" + "=" * 100)
    print("  HEAD-TO-HEAD SUMMARY (20d horizon, chip_avail universe, Top-20)")
    print("=" * 100)

    summary_sub = result_df[
        (result_df['horizon'] == 20) &
        (result_df['top_n'] == 20) &
        (result_df['universe'] == 'chip_avail')
    ]
    if not summary_sub.empty:
        pivot_s = summary_sub.pivot_table(
            index=['score', 'label'],
            columns='period',
            values='sharpe',
            aggfunc='first',
        ).reset_index()
        pivot_s.columns.name = None
        if 'IS' in pivot_s.columns and 'OOS' in pivot_s.columns:
            pivot_s['decay%'] = ((pivot_s['OOS'] / pivot_s['IS']) - 1) * 100
            pivot_s = pivot_s.sort_values('OOS', ascending=False)
            print(f"\n  {'Combo':<35} {'IS Sharpe':>10} {'OOS Sharpe':>11} {'Decay%':>8}")
            print(f"  {'-' * 68}")
            for _, r in pivot_s.iterrows():
                name = r['score'][:35]
                print(f"  {name:<35} {r['IS']:>+10.2f} {r['OOS']:>+11.2f} {r['decay%']:>+7.1f}%")

            # Verdict
            best = pivot_s.iloc[0]
            print(f"\n  Best OOS: {best['score']} (Sharpe {best['OOS']:+.2f})")
            tech_baseline = pivot_s[pivot_s['score'] == 'combo_rvol_lowatr']
            chip_top = pivot_s[pivot_s['score'] == 'combo_tech_chip_top']
            if not tech_baseline.empty and not chip_top.empty:
                t_oos = tech_baseline.iloc[0]['OOS']
                c_oos = chip_top.iloc[0]['OOS']
                diff = c_oos - t_oos
                print(f"  tech_chip_top vs rvol_lowatr: {diff:+.2f} Sharpe difference")
                if c_oos > t_oos and chip_top.iloc[0].get('decay%', -999) > -30:
                    print("  --> Chip adds value, recommend updating scanner weights")
                elif c_oos <= t_oos:
                    print("  --> Chip does NOT add value in OOS, keep tech-only")
                else:
                    print("  --> Chip OOS decays too much, likely overfit")

    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed/60:.1f} min")
    print(f"Full results: {OUT_FILE}")


if __name__ == '__main__':
    main()
