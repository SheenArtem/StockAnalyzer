"""
vfg4_regime_filter_validation.py
================================
VF-G4：Regime-aware Entry Filter 驗證

背景（2026-04-21）：
  VF-G1 / VF-G3 重跑 10.5yr 樣本結論仍 D 級（停損乘數/regime exit mult 不改動）。
  但 regime × year breakdown 顯示 trending regime 結構性賠錢：
    volatile: AVG +2.60%，11 年負次 1/11
    trending: AVG -0.46%，11 年負次 7/11
    trending × 2022 Fed 熊: -8.06% / win 25%
    trending × 2021 多頭年: -2.37% / win 32.5%（多頭也虧！）

假說：trending regime 是 HMM 的滯後訊號，QM 動能選股等到 trending 確認已是尾部，
     反而 volatile 才是真正的早期訊號。

驗證方法：
  對 trade_journal_qm_tw_mixed.parquet（10.5yr, 4923 picks）套用 entry filter，
  比較 baseline vs 各 filter 策略的 Sharpe / max drawdown / 2022 單年 / 整體 win rate。

Filter 策略：
  F0 baseline        - 無過濾（對照組）
  F1 exclude_trending - 移除 trending regime picks
  F2 only_volatile    - 只保留 volatile regime picks
  F3 exclude_t_and_n  - 移除 trending + neutral
  F4 weighted         - 保留全部但 trending 60% 倉位, volatile 120%，其他 100%

輸出：
  reports/vfg4_regime_filter_results.csv  - 各策略 metrics
  reports/vfg4_regime_filter_by_year.csv  - 每年每策略細節
  reports/vfg4_regime_filter.md           - 決策報告
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("vfg4")

JOURNAL = ROOT / "data_cache" / "backtest" / "trade_journal_qm_tw_mixed.parquet"
OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------
# Filter definitions — 每個 filter 回傳 (filtered_df, weight_series)
# weight_series 是相對 baseline=1.0 的倉位乘數
# --------------------------------------------------------------------
def filter_baseline(df: pd.DataFrame):
    return df.copy(), pd.Series(1.0, index=df.index)


def filter_exclude_trending(df: pd.DataFrame):
    kept = df[df['regime'] != 'trending'].copy()
    return kept, pd.Series(1.0, index=kept.index)


def filter_only_volatile(df: pd.DataFrame):
    kept = df[df['regime'] == 'volatile'].copy()
    return kept, pd.Series(1.0, index=kept.index)


def filter_exclude_trending_neutral(df: pd.DataFrame):
    kept = df[~df['regime'].isin(['trending', 'neutral'])].copy()
    return kept, pd.Series(1.0, index=kept.index)


def filter_weighted(df: pd.DataFrame):
    """保留全部，依 regime 調倉位。"""
    kept = df.copy()
    w = pd.Series(1.0, index=kept.index)
    w.loc[kept['regime'] == 'trending'] = 0.6
    w.loc[kept['regime'] == 'volatile'] = 1.2
    return kept, w


FILTERS: dict[str, Callable] = {
    "F0_baseline":          filter_baseline,
    "F1_exclude_trending":  filter_exclude_trending,
    "F2_only_volatile":     filter_only_volatile,
    "F3_excl_trend_neutral": filter_exclude_trending_neutral,
    "F4_weighted":          filter_weighted,
}


# --------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------
def compute_metrics(df: pd.DataFrame, weight: pd.Series, horizon: str = "fwd_20d") -> dict:
    """對 filtered picks 計算統計量。weight 調整每筆報酬 (simulates 倉位大小)。"""
    if df.empty:
        return {'n': 0, 'mean': 0, 'std': 0, 'sharpe': 0, 'win_rate': 0,
                'cum_ret': 0, 'max_dd': 0}
    ret = df[horizon].values
    w = weight.reindex(df.index).fillna(1.0).values
    ret_weighted = ret * w

    mean = ret_weighted.mean()
    std = ret_weighted.std()
    sharpe = mean / std if std > 0 else 0
    win_rate = (ret > 0).mean()

    # Equity curve 用 pick-by-pick 累積（不精確，但足夠比較）
    # 對 picks 依 week_end_date 分組後 sum，再 cumsum
    tmp = df.copy()
    tmp['r'] = ret_weighted
    weekly = tmp.groupby('week_end_date')['r'].mean()
    cum = (1 + weekly).cumprod() - 1
    peak = cum.cummax()
    dd = (cum - peak)  # drawdown from peak (負值)
    max_dd = dd.min() if len(dd) > 0 else 0

    return {
        'n': len(df),
        'mean': mean * 100,
        'std': std * 100,
        'sharpe': sharpe,
        'win_rate': win_rate * 100,
        'cum_ret': cum.iloc[-1] * 100 if len(cum) > 0 else 0,
        'max_dd': max_dd * 100,
    }


def compute_yearly_metrics(df: pd.DataFrame, weight: pd.Series, year: int,
                            horizon: str = "fwd_20d") -> dict:
    """單年 metrics。"""
    mask = df['week_end_date'].dt.year == year
    yr = df[mask]
    yr_w = weight[mask]
    if yr.empty:
        return {'n': 0, 'mean': 0, 'sharpe': 0, 'win_rate': 0, 'max_dd': 0}
    return compute_metrics(yr, yr_w, horizon)


# --------------------------------------------------------------------
# Main validation
# --------------------------------------------------------------------
def main():
    logger.info("Loading trade_journal: %s", JOURNAL)
    df = pd.read_parquet(JOURNAL)
    logger.info("Rows: %d, date: %s - %s", len(df),
                df['week_end_date'].min().date(), df['week_end_date'].max().date())
    df = df.dropna(subset=['fwd_20d', 'regime']).copy()
    logger.info("After dropna: %d rows", len(df))

    # === Overall metrics ===
    logger.info("=" * 70)
    logger.info("Overall (10.5yr) metrics for each filter (horizon=fwd_20d)")
    logger.info("=" * 70)

    overall_rows = []
    for name, filt in FILTERS.items():
        f_df, w = filt(df)
        m = compute_metrics(f_df, w, 'fwd_20d')
        m20 = compute_metrics(f_df, w, 'fwd_20d')
        m40 = compute_metrics(f_df, w, 'fwd_40d')
        m60 = compute_metrics(f_df, w, 'fwd_60d')
        overall_rows.append({
            'filter': name,
            'n': m['n'],
            'mean_20d': m['mean'],
            'mean_40d': m40['mean'],
            'mean_60d': m60['mean'],
            'sharpe_20d': m['sharpe'],
            'sharpe_60d': m60['sharpe'],
            'win_20d': m['win_rate'],
            'cum_ret_20d': m['cum_ret'],
            'max_dd_20d': m['max_dd'],
        })
    overall_df = pd.DataFrame(overall_rows)
    print(overall_df.round(3).to_string(index=False))
    overall_df.to_csv(OUT_DIR / "vfg4_regime_filter_results.csv", index=False)

    # === Yearly breakdown focus on bear years ===
    logger.info("\n" + "=" * 70)
    logger.info("Per-year breakdown (focus on bear years 2015/2018/2022)")
    logger.info("=" * 70)

    year_rows = []
    for name, filt in FILTERS.items():
        f_df, w = filt(df)
        for y in sorted(df['week_end_date'].dt.year.unique()):
            m = compute_yearly_metrics(f_df, w, y, 'fwd_20d')
            year_rows.append({
                'filter': name, 'year': y,
                'n': m['n'], 'mean_20d': m['mean'],
                'win_20d': m['win_rate'], 'sharpe_20d': m['sharpe'],
                'max_dd_20d': m['max_dd'],
            })
    year_df = pd.DataFrame(year_rows)
    year_df.to_csv(OUT_DIR / "vfg4_regime_filter_by_year.csv", index=False)

    # 印 bear years comparison
    bear_years = [2015, 2018, 2022]
    for y in bear_years:
        logger.info("\n--- Year %d (bear/震盪) ---", y)
        yr = year_df[year_df['year'] == y]
        print(yr[['filter', 'n', 'mean_20d', 'win_20d', 'max_dd_20d']].round(2).to_string(index=False))

    # === Decision matrix ===
    logger.info("\n" + "=" * 70)
    logger.info("Decision matrix: vs baseline (F0) delta")
    logger.info("=" * 70)
    base = overall_df[overall_df['filter'] == 'F0_baseline'].iloc[0]
    decision_rows = []
    for _, row in overall_df.iterrows():
        if row['filter'] == 'F0_baseline':
            continue
        decision_rows.append({
            'filter': row['filter'],
            'n_delta': row['n'] - base['n'],
            'n_kept_pct': row['n'] / base['n'] * 100,
            'sharpe_20d_delta': row['sharpe_20d'] - base['sharpe_20d'],
            'mean_20d_delta_pp': row['mean_20d'] - base['mean_20d'],
            'max_dd_20d_delta_pp': row['max_dd_20d'] - base['max_dd_20d'],
            'cum_ret_delta_pp': row['cum_ret_20d'] - base['cum_ret_20d'],
        })
    dec_df = pd.DataFrame(decision_rows)
    print(dec_df.round(3).to_string(index=False))

    # === 2022 specific ===
    logger.info("\n" + "=" * 70)
    logger.info("2022 Bear year survival: baseline vs filters")
    logger.info("=" * 70)
    y22 = year_df[year_df['year'] == 2022][['filter', 'n', 'mean_20d', 'win_20d', 'max_dd_20d']]
    print(y22.round(2).to_string(index=False))

    logger.info("\nDone. Reports in %s", OUT_DIR)


if __name__ == "__main__":
    main()
