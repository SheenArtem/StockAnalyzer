"""
fetch_fred_macro.py -- FRED macro series 抓取 + panel 建立

抓取以下 FRED 序列，產出 data/macro/fred_panel.parquet：

  HYIOAS    BofA US HY Index OAS (daily, 1996+)         — 信用利差，1-3mo lead
  T10Y2Y    10Y-2Y Treasury Spread (daily, 1976+)       — 殖利率曲線
  T10Y3M    10Y-3M Treasury Spread (daily, 1982+)       — 殖利率曲線
  DTWEXBGS  Nominal Broad USD Index (daily, 2006+)      — 美元指數
  VIXCLS    CBOE Volatility Index (daily, 1990+)        — 同步波動
  WALCL     Fed Total Assets (weekly, 2002+)            — 流動性

無需 API key（用 fredgraph.csv 公開端點）。

執行：
  python tools/fetch_fred_macro.py [--from-year YYYY]
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "macro" / "fred_panel.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd={start}"

SERIES = {
    'BAMLH0A0HYM2': 'hy_oas',                # ICE BofA US HY OAS (1996+)
    'T10Y2Y':       'yield_curve_10y_2y',    # 10Y-2Y Treasury Spread
    'T10Y3M':       'yield_curve_10y_3m',    # 10Y-3M Treasury Spread
    'DTWEXBGS':     'dxy_close',             # Nominal Broad USD Index
    'VIXCLS':       'vix_close',             # CBOE VIX
    'WALCL':        'fed_bs_million_usd',    # Fed Balance Sheet (weekly)
}


def fetch_one(series_id: str, start: str) -> pd.DataFrame:
    """從 FRED 抓單一序列，回傳 (date, value) df。"""
    url = FRED_BASE.format(sid=series_id, start=start)
    logger.info("Fetching %s from %s", series_id, start)
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=30, verify=False)
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
            df.columns = ['date', series_id]
            df['date'] = pd.to_datetime(df['date'])
            # FRED 用 "." 表示缺值
            df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
            df = df.dropna(subset=[series_id])
            return df
        except Exception as e:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, series_id, e)
            if attempt < 2:
                time.sleep(2)
            else:
                raise


def build_panel(start: str = "2014-01-01") -> pd.DataFrame:
    """抓 6 序列、merge 成日頻 panel、補 derived columns。"""
    panel = None
    for sid, _col in SERIES.items():
        try:
            df = fetch_one(sid, start)
        except Exception as e:
            logger.error("Failed to fetch %s: %s", sid, e)
            continue
        if panel is None:
            panel = df
        else:
            panel = panel.merge(df, on='date', how='outer')

    if panel is None or panel.empty:
        raise RuntimeError("All FRED series failed")

    panel = panel.sort_values('date').reset_index(drop=True)

    # rename to friendly names
    rename_map = {sid: name for sid, name in SERIES.items() if sid in panel.columns}
    panel = panel.rename(columns=rename_map)

    # forward fill (處理 weekly/monthly 序列 align 到日頻)
    for col in panel.columns:
        if col == 'date':
            continue
        panel[col] = panel[col].ffill()

    # derived columns
    if 'fed_bs_million_usd' in panel.columns:
        panel['fed_bs_trillion'] = panel['fed_bs_million_usd'] / 1e6  # million → trillion
        panel['fed_bs_chg_4w'] = panel['fed_bs_trillion'].pct_change(20) * 100

    if 'dxy_close' in panel.columns:
        panel['dxy_chg_4w'] = panel['dxy_close'].pct_change(20) * 100

    # rolling rank for HY OAS (10yr 滾動百分位，hi=danger)
    if 'hy_oas' in panel.columns:
        panel['hy_oas_rank'] = panel['hy_oas'].rolling(2520, min_periods=252).rank(pct=True) * 100

    # yield curve inverted flag
    if 'yield_curve_10y_2y' in panel.columns:
        panel['yield_curve_10y_2y_inverted'] = (panel['yield_curve_10y_2y'] < 0).astype(int)
    if 'yield_curve_10y_3m' in panel.columns:
        panel['yield_curve_10y_3m_inverted'] = (panel['yield_curve_10y_3m'] < 0).astype(int)

    return panel


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-year', type=int, default=2014,
                        help='起始年份（預設 2014，11+ 年 panel）')
    args = parser.parse_args()

    start = f"{args.from_year}-01-01"
    panel = build_panel(start=start)

    logger.info("Panel rows=%d cols=%d", len(panel), len(panel.columns))
    logger.info("Date range: %s ~ %s", panel['date'].min().date(), panel['date'].max().date())
    logger.info("Last row:\n%s", panel.iloc[-1].to_dict())

    panel.to_parquet(OUT, index=False)
    logger.info("Saved → %s", OUT)


if __name__ == '__main__':
    main()
