"""
walkforward_validation.py -- Walk-Forward 驗證（P4 B3-d）

把歷史切成 in-sample (2011-2020) vs out-of-sample (2021-2025)，
對所有 combo scores 跑 Top-N portfolio 績效比較。

如果 OOS Sharpe 接近 IS → 策略穩健。差很多 → overfit。

目前只含技術面，籌碼面之後補做。

用法:
    python tools/walkforward_validation.py                # 全量
    python tools/walkforward_validation.py --sample 300   # 快速測試
    python tools/walkforward_validation.py --split 2022   # 自訂分割年
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

from tools.indicator_ic_analysis import (
    load_ohlcv, compute_all_indicators, add_fwd_returns, add_regime,
    add_universe_flags, HORIZONS,
)
from tools.indicator_combo_analysis import rank_normalize_signals, build_combos
from tools.indicator_quantile_returns import simulate_topn_portfolio

OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "walkforward_results.csv"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("walkforward")

TOP_N = [20]  # scanner default
UNIVERSES = [('all', None)]  # full universe


def run_period(df, period_name, combos, horizons, top_n_list, universes):
    """Run Top-N simulation for one time period."""
    rows = []
    for combo_col, combo_label in combos.items():
        for h in horizons:
            ret_col = f'fwd_{h}d'
            if ret_col not in df.columns:
                continue
            for n in top_n_list:
                for uni_name, uni_filter in universes:
                    for direction in ['top', 'bot']:
                        result = simulate_topn_portfolio(
                            df, combo_col, ret_col, n=n,
                            universe_filter=uni_filter,
                            long_direction=direction,
                        )
                        if result:
                            rows.append({
                                'period': period_name,
                                'score': combo_col,
                                'label': combo_label,
                                'horizon': h,
                                'universe': uni_name,
                                'top_n': n,
                                'direction': direction,
                                'mean_ret': result['mean_ret'],
                                'std_ret': result['std_ret'],
                                'sharpe': result['sharpe_proxy'],
                                'win_rate': result['win_rate'],
                                'n_days': result['n_days'],
                            })
    return rows


def main():
    parser = argparse.ArgumentParser(description='Walk-forward validation')
    parser.add_argument('--sample', type=int, default=None, help='Sample N tickers')
    parser.add_argument('--split', type=int, default=2021, help='OOS start year')
    args = parser.parse_args()

    t0 = time.time()
    split_date = f'{args.split}-01-01'

    # 1. Load + compute (full history)
    df = load_ohlcv(sample=args.sample)
    df = compute_all_indicators(df)
    df = add_fwd_returns(df)
    df = add_regime(df)
    df = add_universe_flags(df)
    df = rank_normalize_signals(df)
    combos = build_combos(df)

    logger.info(f"Data ready: {len(df):,} rows, {df['date'].min().date()} ~ {df['date'].max().date()}")

    # 2. Split
    df_is = df[df['date'] < split_date].copy()
    df_oos = df[df['date'] >= split_date].copy()
    logger.info(f"In-sample:  {len(df_is):,} rows ({df_is['date'].min().date()} ~ {df_is['date'].max().date()})")
    logger.info(f"Out-of-sample: {len(df_oos):,} rows ({df_oos['date'].min().date()} ~ {df_oos['date'].max().date()})")

    # 3. Run both periods
    horizons = HORIZONS
    logger.info("Running in-sample...")
    is_rows = run_period(df_is, 'in_sample', combos, horizons, TOP_N, UNIVERSES)
    logger.info("Running out-of-sample...")
    oos_rows = run_period(df_oos, 'out_of_sample', combos, horizons, TOP_N, UNIVERSES)

    # 4. Merge + compare
    all_rows = is_rows + oos_rows
    result_df = pd.DataFrame(all_rows)
    result_df.to_csv(OUT_FILE, index=False)
    logger.info(f"Saved {len(result_df)} rows to {OUT_FILE}")

    # 5. Print comparison table
    print("\n" + "=" * 80)
    print(f"WALK-FORWARD VALIDATION (split: {split_date})")
    print("=" * 80)

    # Pivot: IS vs OOS side by side (Top-20, direction=top only)
    top_only = result_df[
        (result_df['direction'] == 'top') &
        (result_df['top_n'] == 20) &
        (result_df['universe'] == 'all')
    ].copy()

    if top_only.empty:
        print("No results. Try --sample with more stocks.")
        return

    pivot = top_only.pivot_table(
        index=['score', 'label', 'horizon'],
        columns='period',
        values=['sharpe', 'mean_ret', 'win_rate'],
        aggfunc='first',
    )

    # Flatten columns
    pivot.columns = [f'{val}_{period}' for val, period in pivot.columns]
    pivot = pivot.reset_index()

    # Compute decay ratio
    if 'sharpe_in_sample' in pivot.columns and 'sharpe_out_of_sample' in pivot.columns:
        pivot['sharpe_decay'] = pivot.apply(
            lambda r: (r['sharpe_out_of_sample'] / r['sharpe_in_sample'] - 1) * 100
            if r['sharpe_in_sample'] != 0 else 0, axis=1
        )

    # Print key combos
    key_combos = [
        'combo_3group_median_raw',
        'combo_rvol_lowatr',
        'combo_meanrev_pure',
        'combo_rvol_meanrev',
    ]
    for combo in key_combos:
        sub = pivot[pivot['score'] == combo]
        if sub.empty:
            continue
        label = sub.iloc[0]['label']
        print(f"\n--- {combo} ({label}) ---")
        print(f"{'Horizon':>8} {'IS Sharpe':>10} {'OOS Sharpe':>11} {'Decay%':>8} "
              f"{'IS Win%':>8} {'OOS Win%':>9} {'IS Ret%':>8} {'OOS Ret%':>9}")
        print("-" * 78)
        for _, row in sub.sort_values('horizon').iterrows():
            h = row['horizon']
            is_sh = row.get('sharpe_in_sample', 0) or 0
            oos_sh = row.get('sharpe_out_of_sample', 0) or 0
            decay = row.get('sharpe_decay', 0) or 0
            is_wr = (row.get('win_rate_in_sample', 0) or 0) * 100
            oos_wr = (row.get('win_rate_out_of_sample', 0) or 0) * 100
            is_mr = (row.get('mean_ret_in_sample', 0) or 0) * 100
            oos_mr = (row.get('mean_ret_out_of_sample', 0) or 0) * 100
            print(f"{h:>5}d   {is_sh:>+10.2f} {oos_sh:>+11.2f} {decay:>+7.1f}% "
                  f"{is_wr:>7.1f}% {oos_wr:>8.1f}% {is_mr:>+7.2f}% {oos_mr:>+8.2f}%")

    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.0f}s")
    print(f"Full results: {OUT_FILE}")


if __name__ == '__main__':
    main()
