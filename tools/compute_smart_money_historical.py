"""
compute_smart_money_historical.py
=================================
從 data_cache/chip_history/institutional.parquet (5 年 2M rows)
算每個 stock × 每個 Friday week_end_date 的 SmartMoney 歷史分數。

輸出: data_cache/backtest/smart_money_scores.parquet
Schema:
  stock_id, date (week_end_date),
  foreign_net_5d, trust_net_5d, dealer_net_5d, total_net_5d,  # 5-day cumulative
  foreign_pct, trust_pct, dealer_pct, total_pct,  # 標準化 = net / 20d avg_tv * 100
  smart_money_score  # 綜合 0-100 分數

用於 value_historical_simulator 取代 placeholder 50。
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

CHIP_DIR = ROOT / "data_cache" / "chip_history"
BACKTEST_DIR = ROOT / "data_cache" / "backtest"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("sm")


def load_inst() -> pd.DataFrame:
    logger.info("Loading institutional.parquet...")
    df = pd.read_parquet(CHIP_DIR / "institutional.parquet")
    df['date'] = pd.to_datetime(df['date'])
    logger.info("  %d rows, %d stocks, %s to %s",
                len(df), df['stock_id'].nunique(),
                df['date'].min().date(), df['date'].max().date())
    return df


def load_avg_tv() -> pd.DataFrame:
    """Load pre-computed indicators for avg_tv_60d (fallback 20d if needed)."""
    logger.info("Loading indicators for avg_tv_60d normalization...")
    df = pd.read_parquet(BACKTEST_DIR / "value_sim_indicators.parquet")
    df['date'] = pd.to_datetime(df['date'])
    return df[['stock_id', 'date', 'avg_tv_60d']]


def score_smart_money(row: pd.Series) -> float:
    """Convert normalized 5d net buy % into 0-100 score (aligned with _score_smart_money logic).

    - 外資 + 投信 + 自營商 5 日淨買佔 20 日均成交值比率
    - 正向加分、負向扣分
    """
    score = 50.0
    total_pct = row.get('total_pct', 0)
    foreign_pct = row.get('foreign_pct', 0)
    trust_pct = row.get('trust_pct', 0)

    # 三大法人合計 (total)
    if pd.notna(total_pct):
        if total_pct > 2:
            score += 15
        elif total_pct > 0.5:
            score += 8
        elif total_pct < -2:
            score -= 15
        elif total_pct < -0.5:
            score -= 8

    # 外資 + 投信同步買超 bonus (vs 自營)
    if pd.notna(foreign_pct) and pd.notna(trust_pct):
        if foreign_pct > 0.3 and trust_pct > 0.3:
            score += 10  # 外資投信同步買超 (機構型 smart money)
        elif foreign_pct < -0.3 and trust_pct < -0.3:
            score -= 10

    return max(0, min(100, score))


def main():
    inst = load_inst()
    tv = load_avg_tv()

    # Build week_end_date (Friday) per row's date
    inst['week_end_date'] = inst['date'] + pd.offsets.Week(weekday=4)  # next Friday or same if Friday
    # Actually we want "the Friday OF the week that this trading day falls in"
    # pd.offsets.Week(weekday=4) forwards to NEXT Friday. We want the week's Friday.
    # Use: date + (4 - date.weekday()) % 7 days, or resample
    inst['weekday'] = inst['date'].dt.weekday
    inst['week_end_date'] = inst['date'] + pd.to_timedelta((4 - inst['weekday']) % 7, unit='D')
    inst.drop(columns=['weekday'], inplace=True)

    logger.info("Aggregating 5-day net buy per stock × week_end_date...")

    # For each (stock_id, week_end_date), sum net buy for past 5 trading days
    # ending on week_end_date
    inst_sorted = inst.sort_values(['stock_id', 'date'])

    # Rolling 5-day sum per stock
    agg = []
    stocks = inst_sorted['stock_id'].unique()
    for i, sid in enumerate(stocks):
        if (i + 1) % 500 == 0:
            logger.info("  [%d/%d] compute 5d...", i + 1, len(stocks))

        sdf = inst_sorted[inst_sorted['stock_id'] == sid].set_index('date').sort_index()
        # 5 trading day rolling sum
        for col in ['foreign_net', 'trust_net', 'dealer_net', 'total_net']:
            sdf[f'{col}_5d'] = sdf[col].rolling(5, min_periods=1).sum()
        sdf = sdf.reset_index()
        agg.append(sdf[['stock_id', 'date', 'foreign_net_5d', 'trust_net_5d',
                         'dealer_net_5d', 'total_net_5d']])

    df_5d = pd.concat(agg, ignore_index=True)
    logger.info("  5d sum: %d rows", len(df_5d))

    # Keep only week_end (Friday) rows
    df_5d['weekday'] = df_5d['date'].dt.weekday
    df_fri = df_5d[df_5d['weekday'] == 4].drop(columns=['weekday']).copy()
    df_fri = df_fri.rename(columns={'date': 'week_end_date'})
    logger.info("  Friday rows: %d", len(df_fri))

    # Merge avg_tv_60d for normalization
    tv_fri = tv.rename(columns={'date': 'week_end_date'})
    merged = df_fri.merge(tv_fri, on=['stock_id', 'week_end_date'], how='left')

    # Normalize: net_5d / (avg_tv_60d * 5) * 100
    # avg_tv_60d 是日均，5 天總 TV = avg_tv_60d × 5
    merged['denom'] = merged['avg_tv_60d'] * 5
    merged['denom'] = merged['denom'].replace(0, np.nan)

    for src, dst in [('foreign_net_5d', 'foreign_pct'),
                      ('trust_net_5d', 'trust_pct'),
                      ('dealer_net_5d', 'dealer_pct'),
                      ('total_net_5d', 'total_pct')]:
        merged[dst] = (merged[src] / merged['denom']) * 100

    # Compute smart_money_score
    merged['smart_money_score'] = merged.apply(score_smart_money, axis=1)

    # Drop helper col
    merged = merged.drop(columns=['denom'])

    # Stats
    logger.info("Non-null smart_money_score: %d / %d (%.1f%%)",
                merged['smart_money_score'].notna().sum(), len(merged),
                100 * merged['smart_money_score'].notna().sum() / len(merged))
    logger.info("Score distribution: min=%.1f, p25=%.1f, median=%.1f, p75=%.1f, max=%.1f",
                merged['smart_money_score'].min(),
                merged['smart_money_score'].quantile(0.25),
                merged['smart_money_score'].median(),
                merged['smart_money_score'].quantile(0.75),
                merged['smart_money_score'].max())

    out = BACKTEST_DIR / "smart_money_scores.parquet"
    merged.to_parquet(out)
    logger.info("Saved: %s (%d rows, %d stocks, %s to %s)",
                out, len(merged), merged['stock_id'].nunique(),
                merged['week_end_date'].min().date() if merged['week_end_date'].notna().any() else None,
                merged['week_end_date'].max().date() if merged['week_end_date'].notna().any() else None)


if __name__ == "__main__":
    main()
