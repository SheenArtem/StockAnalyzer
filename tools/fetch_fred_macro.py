"""
fetch_fred_macro.py -- FRED macro series 抓取 + panel 建立

抓取以下 FRED 序列，產出 data/macro/fred_panel.parquet：

  HYIOAS    BofA US HY Index OAS (daily, 1996+)         — 信用利差，1-3mo lead
  T10Y2Y    10Y-2Y Treasury Spread (daily, 1976+)       — 殖利率曲線
  T10Y3M    10Y-3M Treasury Spread (daily, 1982+)       — 殖利率曲線
  VIXCLS    CBOE Volatility Index (daily, 1990+)        — 同步波動
  WALCL     Fed Total Assets (weekly, 2002+)            — 流動性
  ...其餘 Tier 1 / Phase 3-C 擴充見下

DXY (美元指數) 改抓 yfinance ICE DXY (DX-Y.NYB)，與 Yahoo / TradingView 一致；
   先前用 FRED DTWEXBGS (Trade-Weighted Broad，~120 區間，26 國貨幣) 與一般人看到的
   ICE DXY (~100 區間，6 國 EUR-heavy) 數值差 ~20-30 點，造成混淆，2026-05-09 換掉。

無需 API key（FRED 用 fredgraph.csv 公開端點，DXY 用 yfinance）。

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
    # DXY 改走 yfinance ICE DXY，見 fetch_dxy_yfinance() — 不放 SERIES dict 內
    'DTWEXBGS':     'dxy_broad_close',       # Nominal Broad USD Index (留作 backup，非主 DXY)
    'VIXCLS':       'vix_close',             # CBOE VIX
    'WALCL':        'fed_bs_million_usd',    # Fed Balance Sheet (weekly)
    # Tier 1 擴充 (2026-05-09)
    'UNRATE':       'us_unemployment_rate',  # 美國失業率 (月)
    'ICSA':         'us_initial_claims',     # 初請失業金 (週)
    'UMCSENT':      'us_consumer_sentiment', # 密西根消費者信心 (月)
    'DGORDER':      'us_durable_goods_orders', # 耐久財新訂單 (月)
    'NCBEILQ027S':  'us_nonfin_corp_equity', # 非金融企業股權市值 (季) - 嚴格 Buffett 用
    'GDP':          'us_gdp_billion',        # 美國 GDP (季)
    'SP500':        'sp500_close',           # S&P500 (日)
    # AAII proxy 替代 (AAII 直接抓被 robot block)
    'STLFSI4':      'st_louis_fsi',          # St. Louis Fed Financial Stress Index (週)
    'NFCI':         'chicago_nfci',          # Chicago Fed National Financial Conditions (週)
    'ANFCI':        'chicago_anfci',         # Adjusted NFCI (週)
    # Phase 3-C P1 (2026-05-09 AI 報告建議)
    'DEXJPUS':      'usdjpy_close',          # USD/JPY exchange rate (日) - carry trade 風向
    'DEXTAUS':      'usdtwd_close',          # USD/TWD exchange rate (日) - 台幣
    # P3 (2026-05-30 AI 報告建議): 尾部信用 + 流動性 plumbing
    'BAMLH0A3HYC':  'ccc_oas',               # ICE BofA CCC & Lower US HY OAS (尾部信用壓力,領先 broad HY)
    'RRPONTSYD':    'rrp_balance',           # 隔夜逆回購餘額 ($B,升=流動性回籠)
    'WTREGEN':      'tga_balance',           # 財政部 TGA 國庫帳 ($B,升=抽走銀行準備)
    'SOFR':         'sofr',                  # 擔保隔夜融資利率 (%,飆升=資金面緊張)
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


def fetch_dxy_yfinance(start: str) -> pd.DataFrame:
    """從 yfinance 抓 ICE DXY (DX-Y.NYB)，回傳 (date, dxy_close) df。

    DX-Y.NYB = ICE U.S. Dollar Index (NYBOT)，6 國貨幣加權 (EUR 57.6% / JPY 13.6%
    / GBP 11.9% / CAD 9.1% / SEK 4.2% / CHF 3.6%)，是一般財經網站講的 "DXY"。
    歷史可回 1985+。
    """
    import yfinance as yf
    ticker = yf.Ticker('DX-Y.NYB')
    end = datetime.now().strftime('%Y-%m-%d')
    hist = ticker.history(start=start, end=end, auto_adjust=False)
    if hist.empty:
        logger.warning("yfinance DX-Y.NYB returned empty for start=%s", start)
        return pd.DataFrame(columns=['date', 'dxy_close'])
    df = pd.DataFrame({
        'date': pd.to_datetime(hist.index.date),
        'dxy_close': hist['Close'].astype(float).values,
    })
    df = df.dropna(subset=['dxy_close']).sort_values('date').reset_index(drop=True)
    logger.info("ICE DXY (DX-Y.NYB) fetched: %d rows %s ~ %s",
                len(df), df['date'].min().date(), df['date'].max().date())
    return df


def build_panel(start: str = "2014-01-01") -> pd.DataFrame:
    """抓所有序列、merge 成日頻 panel、補 derived columns。"""
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

    # 加 ICE DXY (yfinance)，與 FRED 序列 outer-merge
    try:
        dxy_df = fetch_dxy_yfinance(start)
        if not dxy_df.empty:
            panel = panel.merge(dxy_df, on='date', how='outer')
    except Exception as e:
        logger.error("DXY yfinance fetch failed: %s", e)

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
    if 'ccc_oas' in panel.columns:
        panel['ccc_oas_rank'] = panel['ccc_oas'].rolling(2520, min_periods=252).rank(pct=True) * 100

    # 單位統一成「十億美元 ($B)」：WTREGEN(TGA) FRED 原單位是百萬，RRPONTSYD(RRP) 原即十億
    if 'tga_balance' in panel.columns:
        panel['tga_balance'] = panel['tga_balance'] / 1000.0  # 百萬 -> 十億

    # Net Liquidity ($B) = Fed BS - RRP - TGA (真實流動性 plumbing；升=注水 risk-on)
    # fed_bs_million_usd(百萬) / 1000 = 十億；rrp/tga 已統一為十億
    if all(c in panel.columns for c in ['fed_bs_million_usd', 'rrp_balance', 'tga_balance']):
        panel['net_liquidity_bil'] = (
            panel['fed_bs_million_usd'] / 1000.0 - panel['rrp_balance'] - panel['tga_balance']
        )
        panel['net_liquidity_chg_4w'] = panel['net_liquidity_bil'].diff(20)

    # yield curve inverted flag
    if 'yield_curve_10y_2y' in panel.columns:
        panel['yield_curve_10y_2y_inverted'] = (panel['yield_curve_10y_2y'] < 0).astype(int)
    if 'yield_curve_10y_3m' in panel.columns:
        panel['yield_curve_10y_3m_inverted'] = (panel['yield_curve_10y_3m'] < 0).astype(int)

    # Tier 1 derived (2026-05-09)
    # 失業率變化 (3m / 12m)
    if 'us_unemployment_rate' in panel.columns:
        panel['us_unemp_chg_3m'] = panel['us_unemployment_rate'].diff(63)  # 3m ≈ 63 trading days
        panel['us_unemp_chg_12m'] = panel['us_unemployment_rate'].diff(252)
    # 初請失業金 4w MA + YoY
    if 'us_initial_claims' in panel.columns:
        panel['us_claims_ma4'] = panel['us_initial_claims'].rolling(20).mean()
        panel['us_claims_yoy'] = panel['us_initial_claims'].pct_change(252) * 100
    # 消費者信心 YoY
    if 'us_consumer_sentiment' in panel.columns:
        panel['us_sent_yoy'] = panel['us_consumer_sentiment'].pct_change(252) * 100
    # 耐久財訂單 YoY
    if 'us_durable_goods_orders' in panel.columns:
        panel['us_durable_yoy'] = panel['us_durable_goods_orders'].pct_change(252) * 100
    # 嚴格 Buffett (Nonfin Corp Equity / GDP)
    if 'us_nonfin_corp_equity' in panel.columns and 'us_gdp_billion' in panel.columns:
        panel['us_buffett_strict'] = panel['us_nonfin_corp_equity'] / 1000 / panel['us_gdp_billion'] * 100  # NCBEILQ 是 millions, GDP 是 billions
        panel['us_buffett_strict_rank'] = (
            panel['us_buffett_strict'].rolling(2520, min_periods=252).rank(pct=True) * 100
        )

    # USDJPY carry trade (Phase 3-C P1)
    if 'usdjpy_close' in panel.columns:
        panel['usdjpy_chg_4w'] = panel['usdjpy_close'].pct_change(20) * 100
        panel['usdjpy_chg_2w'] = panel['usdjpy_close'].pct_change(10) * 100

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
