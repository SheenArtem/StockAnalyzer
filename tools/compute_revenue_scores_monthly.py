"""
compute_revenue_scores_monthly.py
==================================
從 data_cache/backtest/financials_revenue.parquet (月營收) 計算每支股票每月
月末的 1m 單月 YoY revenue_score。

輸出: data_cache/backtest/revenue_scores_monthly.parquet
Schema: stock_id, date (月末, pd.Timestamp), revenue_score (0-100)

VF-VC 驗證 (2026-04-20) 結論: 1m 單月 YoY IR +0.335，quarterly walk-forward
+0.615 (A)。季度更新太慢 (IR -0.757)，必須月度更新才能抓到 alpha。
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_cache" / "backtest"
IN_PATH = DATA_DIR / "financials_revenue.parquet"
OUT_PATH = DATA_DIR / "revenue_scores_monthly.parquet"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("rev_monthly")


def yoy_to_score(yoy_latest: float, yoy_prev: float | None) -> float:
    score = 50.0
    if pd.isna(yoy_latest):
        return score
    if yoy_latest > 0:
        score += 10
    elif yoy_prev is not None and not pd.isna(yoy_prev):
        if abs(yoy_latest - yoy_prev) >= 0.5:
            if yoy_latest > yoy_prev:
                score += min(20, (yoy_latest - yoy_prev) * 2)
            else:
                score -= min(20, abs(yoy_latest - yoy_prev) * 2)
    return max(0.0, min(100.0, score))


def main():
    logger.info("Loading monthly revenue parquet...")
    df = pd.read_parquet(IN_PATH)
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue'])
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)
    logger.info("  %d rows, %d stocks, %s - %s",
                len(df), df['stock_id'].nunique(),
                df['date'].min().date(), df['date'].max().date())

    out_rows = []
    stocks = df['stock_id'].unique()
    for i, sid in enumerate(stocks):
        if (i + 1) % 500 == 0:
            logger.info("  [%d/%d] scoring...", i + 1, len(stocks))
        g = df[df['stock_id'] == sid].reset_index(drop=True)
        if len(g) < 13:
            continue

        for idx in range(12, len(g)):
            d = g.loc[idx, 'date']
            # FinMind 月營收 date = 2024-03-01 代表「3 月營收」，公告日為 4/10。
            # PIT 安全: available_date = 月末 + 10 天 buffer (法規要求 10 號前公告)
            month_end = d + pd.offsets.MonthEnd(0)
            available_date = month_end + pd.Timedelta(days=10)

            latest = g.loc[idx, 'revenue']
            yr_ago = g.loc[idx - 12, 'revenue']
            if yr_ago <= 0:
                continue
            yoy_latest = (latest / yr_ago - 1) * 100

            yoy_prev = None
            if idx >= 15:
                prev = g.loc[idx - 3, 'revenue']
                yr_ago_prev = g.loc[idx - 15, 'revenue']
                if yr_ago_prev > 0:
                    yoy_prev = (prev / yr_ago_prev - 1) * 100

            score = yoy_to_score(yoy_latest, yoy_prev)
            out_rows.append({
                'stock_id': sid,
                'date': available_date,
                'revenue_score': score,
            })

    out = pd.DataFrame(out_rows)
    logger.info("Built %d (stock, month) rows", len(out))
    logger.info("  distribution: min=%.1f p25=%.1f p50=%.1f p75=%.1f max=%.1f mean=%.2f std=%.2f",
                out['revenue_score'].min(),
                out['revenue_score'].quantile(0.25),
                out['revenue_score'].quantile(0.5),
                out['revenue_score'].quantile(0.75),
                out['revenue_score'].max(),
                out['revenue_score'].mean(),
                out['revenue_score'].std())

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT_PATH)
    logger.info("Saved: %s", OUT_PATH)


if __name__ == "__main__":
    main()
