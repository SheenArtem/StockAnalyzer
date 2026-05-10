"""
build_valuation_panel.py -- 估值面板 (TWSE PE/PB/Yield + 巴菲特指標)

來源：
  - TWSE 大盤 PE/PB/yield 月資料：staticFiles ZIP（官方市值加權月報，2010-01+）
    https://www.twse.com.tw/staticFiles/inspection/inspection/04/001/YYYYMM_C04001.zip
    內含 Big5 XLS, Sheet 0 'new' R5: col 5=PE, col 7=Yield(%), col 9=PBR
    跟 macromicro.me 同來源（2026-05-10 reverse-engineering 確認）
  - 巴菲特指標 US：SP500 (FRED) / US GDP (FRED, 季資料) — rank-based proxy
  - 巴菲特指標 TW：簡化版用 ^TWII rank 作 buffett proxy

輸出：data/macro/valuation_panel.parquet
欄位：
  date / tw_market_pe / tw_market_pb / tw_market_yield
  buffett_indicator_us / buffett_rank_us
  buffett_indicator_tw / buffett_rank_tw

執行：python tools/build_valuation_panel.py
"""
from __future__ import annotations

import logging
import time
import zipfile
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from pathlib import Path

import pandas as pd
import requests
import urllib3
import xlrd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "macro" / "valuation_panel.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}"


def fetch_fred(sid: str) -> pd.DataFrame:
    url = FRED_BASE.format(sid=sid)
    r = requests.get(url, timeout=30, verify=False)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    df.columns = ['date', sid]
    df['date'] = pd.to_datetime(df['date'])
    df[sid] = pd.to_numeric(df[sid], errors='coerce')
    return df.dropna(subset=[sid])


def build_buffett_us() -> pd.DataFrame:
    """巴菲特指標 (US, rank-based proxy)：SP500 / US GDP × 100。

    嚴格 Buffett = Total US Market Cap / GDP × 100（傳統 Wilshire 5000 ÷ GDP）。
    本實作使用 SP500 ÷ GDP × constant 作 rank-based proxy：
      - SP500 跟 Wilshire 5000 相關 0.99+，rank 結果幾乎一致
      - 重點是相對位置（過去 10 年 P85/P50 等），絕對值有偏差但不影響告警邏輯
    """
    logger.info("Fetching FRED SP500 + GDP for US Buffett proxy...")
    sp = fetch_fred('SP500')   # SP500 daily index, 2014+
    gdp = fetch_fred('GDP')    # US Nominal GDP, quarterly billions

    sp = sp.set_index('date').rename(columns={'SP500': 'sp500_close'})
    gdp = gdp.set_index('date').rename(columns={'GDP': 'us_gdp_billion'})
    df = sp.join(gdp, how='outer').sort_index()
    df['us_gdp_billion'] = df['us_gdp_billion'].ffill()
    df['sp500_close'] = df['sp500_close'].ffill()
    df = df.dropna(subset=['sp500_close', 'us_gdp_billion'])

    # rank-based proxy: sp500 / gdp × 100
    df['buffett_indicator_us'] = df['sp500_close'] / df['us_gdp_billion'] * 100
    df['buffett_rank_us'] = df['buffett_indicator_us'].rolling(2520, min_periods=252).rank(pct=True) * 100

    return df.reset_index()[['date', 'sp500_close', 'us_gdp_billion',
                              'buffett_indicator_us', 'buffett_rank_us']]


TWSE_PE_ZIP_BASE = (
    "https://www.twse.com.tw/staticFiles/inspection/inspection/04/001/{ym}_C04001.zip"
)


def fetch_twse_market_pe_official(year_month: str) -> dict | None:
    """抓 TWSE 大盤本益比月報 ZIP（官方市值加權，2010-01+ schema 穩定）。

    來源：staticFiles ZIP 內含 Big5 XLS, Sheet 0 'new' R5:
      col 5 = 大盤 PE / col 7 = Yield(%) / col 9 = PBR
    跟 macromicro.me 同來源（2026-05-10 reverse-engineering 確認）。

    Args:
        year_month: 'YYYYMM' e.g. '202601'

    Returns:
        dict {date, tw_market_pe, tw_market_pb, tw_market_yield} or None
    """
    url = TWSE_PE_ZIP_BASE.format(ym=year_month)
    try:
        r = requests.get(url, timeout=20, verify=False)
        if r.status_code != 200:
            return None
        with zipfile.ZipFile(BytesIO(r.content)) as z:
            xls_name = z.namelist()[0]
            with z.open(xls_name) as f:
                wb = xlrd.open_workbook(file_contents=f.read())
        sh = wb.sheet_by_index(0)
        # R5 schema: col 5 PE / col 7 Yield / col 9 PBR (2010-01+ 一致)
        pe = sh.cell_value(5, 5)
        yld = sh.cell_value(5, 7)
        pbr = sh.cell_value(5, 9)
        yyyy = int(year_month[:4])
        mm = int(year_month[4:])
        date = pd.Timestamp(yyyy, mm, 1) + pd.offsets.MonthEnd(0)
        return {
            'date': date,
            'tw_market_pe': float(pe) if pe not in ('', None) else None,
            'tw_market_pb': float(pbr) if pbr not in ('', None) else None,
            'tw_market_yield': float(yld) if yld not in ('', None) else None,
        }
    except Exception as e:
        logger.warning("TWSE PE %s failed: %s", year_month, e)
        return None


def build_twse_market_pe_history(
    start_year: int = 2010,
    end_year: int | None = None,
) -> pd.DataFrame:
    """Backfill TWSE 大盤 PE/PB/Yield 月歷史（官方市值加權）。

    起點 2010-01 — 2009 以前 XLS schema 不同（cols=21 / 個股式），不處理。
    """
    if end_year is None:
        end_year = datetime.now().year
    rows = []
    now_ts = pd.Timestamp.now()
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            if pd.Timestamp(y, m, 1) > now_ts:
                break
            ym = f"{y:04d}{m:02d}"
            rec = fetch_twse_market_pe_official(ym)
            if rec:
                rows.append(rec)
                if m == 1 or m == 7:  # 半年 log 一次
                    logger.info("TWSE PE %s: pe=%.2f pb=%.2f yld=%.2f",
                                ym, rec.get('tw_market_pe') or 0,
                                rec.get('tw_market_pb') or 0,
                                rec.get('tw_market_yield') or 0)
            time.sleep(1.5)  # TWSE rate limit
    if not rows:
        logger.warning("No TWSE PE data fetched")
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    return df


def build_buffett_tw(twii_close: pd.Series, gdp_tw_quarterly: pd.DataFrame) -> pd.DataFrame:
    """巴菲特指標 (TW) — 簡化版用 ^TWII 做 market cap proxy。

    嚴格算法：TWSE 上市總市值 / TW GDP × 100
    簡化：market_cap_proxy = ^TWII × constant，rank-based 比較
    對 IC 用途來說，rank 比絕對值更重要。
    """
    df = pd.DataFrame({'twii_close': twii_close})
    df = df.merge(gdp_tw_quarterly, how='left', left_index=True, right_index=True)
    df['tw_gdp_billion'] = df.get('tw_gdp_billion', pd.NA)
    if 'tw_gdp_billion' in df.columns:
        df['tw_gdp_billion'] = df['tw_gdp_billion'].ffill()
    # 簡化：market_cap_proxy = twii_close (可視為 rank-only 指標)
    df['buffett_indicator_tw'] = df['twii_close']  # rank-base 比較，數值不直接乘 GDP 因為缺乾淨資料
    df['buffett_rank_tw'] = df['buffett_indicator_tw'].rolling(2520, min_periods=252).rank(pct=True) * 100
    return df.reset_index().rename(columns={'index': 'date'})


def build_panel():
    # 1. US Buffett
    try:
        us = build_buffett_us()
        logger.info("US Buffett: %d rows, last buffett=%.1f rank=%.1f",
                    len(us), us['buffett_indicator_us'].iloc[-1], us['buffett_rank_us'].iloc[-1])
    except Exception as e:
        logger.error("US Buffett failed: %s", e)
        us = pd.DataFrame()

    # 2. TWSE 大盤 PE/PB/Yield (官方 staticFiles ZIP，2010-01+ 市值加權)
    try:
        tw_pe = build_twse_market_pe_history(start_year=2010)
        if not tw_pe.empty:
            last = tw_pe.iloc[-1]
            logger.info("TWSE market PE: %d months, last %s pe=%.2f pb=%.2f yld=%.2f",
                        len(tw_pe), last['date'].strftime('%Y-%m'),
                        last.get('tw_market_pe') or 0,
                        last.get('tw_market_pb') or 0,
                        last.get('tw_market_yield') or 0)
    except Exception as e:
        logger.error("TWSE market PE failed: %s", e)
        tw_pe = pd.DataFrame()

    # 3. TW Buffett (簡化版)
    try:
        import yfinance as yf
        twii = yf.Ticker('^TWII').history(period='15y')['Close']
        twii.index = pd.to_datetime(twii.index.tz_localize(None).date) if twii.index.tz else pd.to_datetime(twii.index.date)
        # 直接用 TWII rank 作 buffett proxy
        df_tw = pd.DataFrame({'date': twii.index, 'buffett_indicator_tw': twii.values})
        df_tw['buffett_rank_tw'] = df_tw['buffett_indicator_tw'].rolling(
            2520, min_periods=252).rank(pct=True) * 100
        logger.info("TW Buffett (proxy via TWII rank): %d rows, last rank=%.1f",
                    len(df_tw), df_tw['buffett_rank_tw'].iloc[-1])
    except Exception as e:
        logger.error("TW Buffett failed: %s", e)
        df_tw = pd.DataFrame()

    # Merge 全部到日頻 panel
    if not us.empty:
        panel = us.copy()
    else:
        panel = pd.DataFrame()

    if not df_tw.empty:
        panel = panel.merge(df_tw, on='date', how='outer') if not panel.empty else df_tw

    if not tw_pe.empty:
        panel = panel.merge(tw_pe, on='date', how='outer') if not panel.empty else tw_pe

    panel = panel.sort_values('date').reset_index(drop=True)
    # ffill 月資料 (TWSE PE 月) 到日
    for col in ['tw_market_pe', 'tw_market_pb', 'tw_market_yield']:
        if col in panel.columns:
            panel[col] = panel[col].ffill()

    return panel


def main():
    panel = build_panel()
    if panel.empty:
        logger.error("Panel empty, abort")
        return

    logger.info("Final panel rows=%d cols=%d", len(panel), len(panel.columns))
    logger.info("Date range: %s ~ %s", panel['date'].min(), panel['date'].max())
    last = panel.dropna(how='all', subset=[c for c in panel.columns if c != 'date']).iloc[-1].to_dict()
    logger.info("Last row: %s", last)

    panel.to_parquet(OUT, index=False)
    logger.info("Saved -> %s", OUT)


if __name__ == '__main__':
    main()
