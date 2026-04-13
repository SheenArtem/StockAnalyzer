"""
Phase 2c: Quantile 報酬驗證

IC 只測**線性相關**，scanner 實際取 Top N（右尾），兩者未必一致。
本腳本做兩件事:
1. 把股票按訊號值分 10 deciles，看各 decile 未來平均報酬（是否單調？頂部是否真賺？）
2. 模擬「每日取 Top N 持有 h 天」的 portfolio 報酬 + Sharpe（scanner 實戰情境）

這才能回答：「IC=-0.037 實際在 scanner 手上是不是賠錢？」

輸入: data_cache/backtest/ohlcv_tw.parquet
輸出:
    - reports/quantile_decile_returns.csv (每 combo × horizon × universe × decile 的日均報酬)
    - reports/quantile_topn_portfolio.csv (Top-N 投組績效)

用法:
    python tools/indicator_quantile_returns.py                      # 全量
    python tools/indicator_quantile_returns.py --sample 300         # 測試
    python tools/indicator_quantile_returns.py --horizon 20         # 單 horizon
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
    add_universe_flags, SIGNAL_COLS, SIGNAL_LABELS, HORIZONS,
)
from tools.indicator_combo_analysis import (
    rank_normalize_signals, build_combos,
)

OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_DECILE = OUT_DIR / "quantile_decile_returns.csv"
OUT_TOPN = OUT_DIR / "quantile_topn_portfolio.csv"

N_DECILES = 10
TOP_N_VARIANTS = [10, 20, 50]       # 模擬不同 portfolio 大小
MIN_CROSS_SECTION = 50              # 當日至少 N 檔才算

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("quantile")


# ============================================================
# 1. Decile returns
# ============================================================
def compute_decile_returns(df, score_col, return_col, universe_filter=None):
    """
    每日按 score 分 N_DECILES，算各 decile 平均 forward return。
    回傳 DataFrame: decile (1-10), mean_ret, std_ret, win_rate, n_days
    """
    x = df
    if universe_filter is not None:
        x = x[x[universe_filter] == True]
    x = x[[score_col, return_col, 'date']].dropna()

    # 每日給 decile
    def _bucket(g):
        if len(g) < MIN_CROSS_SECTION:
            return pd.Series([np.nan] * len(g), index=g.index)
        return pd.qcut(g[score_col].rank(method='first'),
                       N_DECILES, labels=False, duplicates='drop')

    x['decile'] = x.groupby('date', group_keys=False).apply(_bucket)
    x = x.dropna(subset=['decile'])
    x['decile'] = x['decile'].astype(int) + 1  # 1-10 instead of 0-9

    # 每日每 decile 的平均 fwd return
    daily = x.groupby(['date', 'decile'])[return_col].mean().reset_index()

    # 跨日聚合
    summary = daily.groupby('decile')[return_col].agg(
        mean_ret='mean',
        median_ret='median',
        std_ret='std',
        win_rate=lambda s: (s > 0).mean(),
        n_days='count',
    ).reset_index()
    return summary


# ============================================================
# 2. Top-N portfolio simulation
# ============================================================
def simulate_topn_portfolio(df, score_col, return_col, n=20,
                             universe_filter=None, long_direction='top'):
    """
    模擬每日取 Top N (by score) 持有直到 return_col horizon，測量 portfolio 日均報酬。

    long_direction:
      'top' = 每天取最高分 N 檔（多頭：做多頂部）
      'bot' = 每天取最低分 N 檔（反向：若 IC 負，做多底部）

    回傳 dict: mean_ret, std_ret, sharpe (annualized), win_rate, n_days
    """
    x = df
    if universe_filter is not None:
        x = x[x[universe_filter] == True]
    x = x[[score_col, return_col, 'date']].dropna()

    daily_rets = []
    for date, g in x.groupby('date'):
        if len(g) < n * 2:
            continue
        if long_direction == 'top':
            picks = g.nlargest(n, score_col)
        else:
            picks = g.nsmallest(n, score_col)
        daily_rets.append(picks[return_col].mean())

    if not daily_rets:
        return None

    arr = np.array(daily_rets)
    mean_r = arr.mean()
    std_r = arr.std(ddof=1)

    # Annualized Sharpe (假設 252 交易日、單日持有視為一天報酬)
    # 注意：此處 fwd return 已是 h 日總報酬，非日報酬；Sharpe 當量化指標看
    sharpe_annual = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0

    return {
        'mean_ret': float(mean_r),
        'std_ret': float(std_r),
        'sharpe_proxy': float(sharpe_annual),
        'win_rate': float((arr > 0).mean()),
        'n_days': len(arr),
    }


# ============================================================
# 3. Main
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', type=int, default=None)
    parser.add_argument('--since', type=str, default=None)
    parser.add_argument('--horizon', type=int, default=None)
    args = parser.parse_args()

    t0 = time.time()

    # ----- Load + compute signals (reuse Phase 2a/2b logic) -----
    df = load_ohlcv(sample=args.sample, since=args.since)
    df = compute_all_indicators(df)
    df = add_fwd_returns(df)
    df = add_regime(df)
    df = add_universe_flags(df)
    df = rank_normalize_signals(df)

    combos_dict = build_combos(df)    # 把 combo 欄位建進 df

    # ----- 要分析的 score columns -----
    # 1) 10 個 combos
    combo_cols = list(combos_dict.keys())
    # 2) 全部 rank-normalized individual signals（供對比）
    ind_cols = [f'{c}_rn' for c in SIGNAL_COLS]
    all_scores = [(c, combos_dict[c]) for c in combo_cols] + \
                 [(c, SIGNAL_LABELS[c.replace('_rn', '')]) for c in ind_cols]

    horizons = [args.horizon] if args.horizon else [5, 10, 20]
    universes = [('all', None), ('momentum', 'in_momentum_universe')]

    # ----- Decile analysis -----
    logger.info("Computing decile returns...")
    t1 = time.time()
    decile_rows = []
    for score_col, label in all_scores:
        for h in horizons:
            for uni_name, uni_col in universes:
                summary = compute_decile_returns(
                    df, score_col, f'fwd_{h}d', universe_filter=uni_col,
                )
                for _, r in summary.iterrows():
                    decile_rows.append({
                        'score': score_col,
                        'label': label,
                        'horizon': h,
                        'universe': uni_name,
                        'decile': int(r['decile']),
                        'mean_ret': r['mean_ret'],
                        'median_ret': r['median_ret'],
                        'std_ret': r['std_ret'],
                        'win_rate': r['win_rate'],
                        'n_days': int(r['n_days']),
                    })
    decile_df = pd.DataFrame(decile_rows)
    decile_df.to_csv(OUT_DECILE, index=False, encoding='utf-8-sig')
    logger.info(f"Decile returns saved: {OUT_DECILE} ({len(decile_df)} rows) in {time.time()-t1:.1f}s")

    # ----- Top-N portfolio simulation -----
    logger.info("Simulating Top-N portfolios...")
    t2 = time.time()
    topn_rows = []
    for score_col, label in all_scores:
        for h in horizons:
            for uni_name, uni_col in universes:
                for n in TOP_N_VARIANTS:
                    for direction in ['top', 'bot']:
                        res = simulate_topn_portfolio(
                            df, score_col, f'fwd_{h}d',
                            n=n, universe_filter=uni_col,
                            long_direction=direction,
                        )
                        if res is None:
                            continue
                        topn_rows.append({
                            'score': score_col,
                            'label': label,
                            'horizon': h,
                            'universe': uni_name,
                            'top_n': n,
                            'direction': direction,
                            **res,
                        })
    topn_df = pd.DataFrame(topn_rows)
    topn_df.to_csv(OUT_TOPN, index=False, encoding='utf-8-sig')
    logger.info(f"Top-N portfolio saved: {OUT_TOPN} ({len(topn_df)} rows) in {time.time()-t2:.1f}s")

    # ----- Summary print -----
    print("\n" + "=" * 100)
    print("  KEY RESULTS: 現行 scanner 的 Top-20 實戰報酬?")
    print("=" * 100)
    # 聚焦 combo_3group_median_raw (scanner 現行邏輯)
    scanner_raw = topn_df[
        (topn_df['score'] == 'combo_3group_median_raw')
        & (topn_df['top_n'] == 20)
    ].sort_values(['universe', 'horizon', 'direction'])
    print(f"  {'Uni':<10} {'H':>3} {'Dir':<4} {'mean%':>8} {'std%':>7} "
          f"{'Sharpe':>7} {'win%':>6} {'n_days':>7}")
    print("  " + "-" * 70)
    for _, r in scanner_raw.iterrows():
        dir_label = 'Top20' if r['direction'] == 'top' else 'Bot20'
        print(f"  {r['universe']:<10} {r['horizon']:>3d} {dir_label:<6} "
              f"{r['mean_ret']*100:>+7.3f} {r['std_ret']*100:>6.3f} "
              f"{r['sharpe_proxy']:>+6.2f} {r['win_rate']*100:>5.1f}% {r['n_days']:>7d}")

    print("\n" + "=" * 100)
    print("  TOP COMBO: rvol_lowatr (預期 IC 最強) Top-20 portfolio")
    print("=" * 100)
    rvol = topn_df[
        (topn_df['score'] == 'combo_rvol_lowatr')
        & (topn_df['top_n'] == 20)
        & (topn_df['direction'] == 'top')
    ].sort_values(['universe', 'horizon'])
    print(f"  {'Uni':<10} {'H':>3} {'mean%':>8} {'std%':>7} "
          f"{'Sharpe':>7} {'win%':>6} {'n_days':>7}")
    print("  " + "-" * 60)
    for _, r in rvol.iterrows():
        print(f"  {r['universe']:<10} {r['horizon']:>3d} "
              f"{r['mean_ret']*100:>+7.3f} {r['std_ret']*100:>6.3f} "
              f"{r['sharpe_proxy']:>+6.2f} {r['win_rate']*100:>5.1f}% {r['n_days']:>7d}")

    print("\n" + "=" * 100)
    print("  DECILE SPREAD: 比對 combo_3group_median_raw (scanner 現行邏輯)")
    print("  (universe=all, h=20d)")
    print("=" * 100)
    d_raw = decile_df[
        (decile_df['score'] == 'combo_3group_median_raw')
        & (decile_df['universe'] == 'all')
        & (decile_df['horizon'] == 20)
    ].sort_values('decile')
    print(f"  {'Decile':>7} {'mean%':>9} {'win%':>7} (+ means earn, - means lose)")
    for _, r in d_raw.iterrows():
        bar = '#' * min(40, max(1, int(abs(r['mean_ret']) * 2000)))
        print(f"  D{r['decile']:>4d}  {r['mean_ret']*100:>+7.3f}  {r['win_rate']*100:>5.1f}%  {bar}")

    print("\n  DECILE SPREAD: combo_rvol_lowatr (預期最強 alpha)")
    d_rvol = decile_df[
        (decile_df['score'] == 'combo_rvol_lowatr')
        & (decile_df['universe'] == 'all')
        & (decile_df['horizon'] == 20)
    ].sort_values('decile')
    print(f"  {'Decile':>7} {'mean%':>9} {'win%':>7}")
    for _, r in d_rvol.iterrows():
        bar = '#' * min(40, max(1, int(abs(r['mean_ret']) * 2000)))
        print(f"  D{r['decile']:>4d}  {r['mean_ret']*100:>+7.3f}  {r['win_rate']*100:>5.1f}%  {bar}")

    print(f"\nTotal time: {(time.time()-t0)/60:.1f} min")


if __name__ == '__main__':
    main()
