"""
build_valuation_panel.py -- 估值面板 (TWSE PE/PB/Yield + 巴菲特指標)

來源：
  - TWSE 大盤 PE/PB/yield 月資料：BWIBBU_d (日資料) 太多，改抓月報
    https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date=YYYYMMDD
  - 巴菲特指標 US：Wilshire 5000 (FRED WILL5000PR / 2009+) / US GDP (FRED GDP / 季資料)
  - 巴菲特指標 TW：TWSE 上市總市值 / TW GDP（主計處）
    簡化版：用 TWSE 加權指數市值 (FMTQIK) 對 TW GDP 抓 quarterly average

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
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import urllib3

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


def fetch_twse_market_pe(year: int) -> pd.DataFrame:
    """抓 TWSE 月報「大盤本益比、殖利率、股價淨值比」一年的資料。

    TWSE: BWIBBU_d 是日資料；月報用 BWIBBU 路徑。
    不過實務上 BWIBBU_d response=json&date=YYYYMM01 會回該月的資料。
    """
    rows = []
    for month in range(1, 13):
        date_str = f"{year}{month:02d}01"
        url = f"https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date={date_str}"
        try:
            r = requests.get(url, timeout=20, verify=False)
            if r.status_code != 200:
                continue
            data = r.json()
            if data.get('stat') != 'OK':
                continue
            fields = data.get('fields', [])
            for row in data.get('data', []):
                rec = dict(zip(fields, row))
                rows.append(rec)
            time.sleep(1.5)  # TWSE rate limit
        except Exception as e:
            logger.warning("TWSE PE %s%02d failed: %s", year, month, e)
            continue
    return pd.DataFrame(rows)


def build_twse_pe(start_year: int = 2014) -> pd.DataFrame:
    """整合多年的 TWSE 大盤 PE/PB/Yield 月歷史。"""
    cur_year = datetime.now().year
    all_rows = []
    for y in range(start_year, cur_year + 1):
        logger.info("Fetching TWSE BWIBBU year=%d", y)
        df = fetch_twse_market_pe(y)
        if not df.empty:
            all_rows.append(df)
    if not all_rows:
        logger.warning("No TWSE PE data fetched")
        return pd.DataFrame()

    df = pd.concat(all_rows, ignore_index=True)
    # 欄位中文，要 normalize
    # 常見：日期 / 殖利率(%) / 股價淨值比 / 本益比
    rename = {
        '日期': 'date_str',
        '殖利率(%)': 'tw_market_yield',
        '股價淨值比': 'tw_market_pb',
        '本益比': 'tw_market_pe',
        # 也可能有 'Date', 'Yield', 'PBR', 'PER' 英文版
    }
    df = df.rename(columns=rename)

    # 處理日期 (民國年)
    if 'date_str' in df.columns:
        def _parse_roc(s):
            try:
                parts = s.split('/')
                roc_year = int(parts[0])
                return pd.Timestamp(roc_year + 1911, int(parts[1]), int(parts[2]))
            except Exception:
                return pd.NaT
        df['date'] = df['date_str'].apply(_parse_roc)
    elif 'Date' in df.columns:
        df['date'] = pd.to_datetime(df['Date'])

    df = df.dropna(subset=['date'])
    for col in ['tw_market_yield', 'tw_market_pb', 'tw_market_pe']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.sort_values('date').drop_duplicates('date', keep='last').reset_index(drop=True)
    cols = ['date'] + [c for c in ['tw_market_pe', 'tw_market_pb', 'tw_market_yield']
                       if c in df.columns]
    return df[cols]


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

    # 2. TWSE 大盤 PE/PB/Yield (BWIBBU_d 是 per-stock，不是大盤；defer Phase 3)
    tw_pe = pd.DataFrame()  # placeholder — TWSE 大盤統計 endpoint 待解析，先用空

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
