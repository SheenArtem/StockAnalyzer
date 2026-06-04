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
from io import BytesIO
from pathlib import Path

import numpy as np
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

def build_buffett_us() -> pd.DataFrame:
    """巴菲特指標 (US, rank-based proxy)：SP500 / US GDP × 100。

    嚴格 Buffett = Total US Market Cap / GDP × 100（傳統 Wilshire 5000 ÷ GDP）。
    本實作使用 SP500 ÷ GDP × constant 作 rank-based proxy：
      - SP500 跟 Wilshire 5000 相關 0.99+，rank 結果幾乎一致
      - 重點是相對位置（過去 10 年 P85/P50 等），絕對值有偏差但不影響告警邏輯

    2026-06-04 改讀 fred_panel.parquet（fetch_fred_macro 產出，已含 chunked fallback
    + carry-forward 三層防禦），不再自抓 FRED：原自抓 SP500 撞上 FRED 大範圍 504
    時會 silent 掉 buffett_us 整欄（本專案最強 IC 特徵），且與 fred_panel 重複抓取
    違反 reuse-upstream 原則。fred_panel 缺欄時 fail loud。
    """
    fred_path = REPO / "data" / "macro" / "fred_panel.parquet"
    if not fred_path.exists():
        raise RuntimeError("fred_panel.parquet 不存在 — 先跑 tools/fetch_fred_macro.py")
    fred = pd.read_parquet(fred_path)
    missing = [c for c in ('sp500_close', 'us_gdp_billion') if c not in fred.columns]
    if missing:
        raise RuntimeError(f"fred_panel 缺 {missing} — 先跑 tools/fetch_fred_macro.py")

    df = (fred[['date', 'sp500_close', 'us_gdp_billion']]
          .dropna(subset=['sp500_close', 'us_gdp_billion'])
          .sort_values('date').set_index('date'))

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
    incremental: bool = True,
) -> pd.DataFrame:
    """Backfill TWSE 大盤 PE/PB/Yield 月歷史（官方市值加權）。

    起點 2010-01 — 2009 以前 XLS schema 不同（cols=21 / 個股式），不處理。

    回傳 sparse monthly DataFrame (一個月一 row，date = month-end)，跟原版相容。

    Incremental (2026-05-28)：預設只抓現有 parquet 推導出來的「缺少 + 當月」月份；
    daily 跑時通常 0-1 ZIP；首次無 parquet 時退回完整 backfill (192 ZIPs)。
    重建 sparse panel 用 month-period dedup，避開現有 parquet 為 daily-ffill 的混淆。
    """
    existing_monthly: pd.DataFrame | None = None
    have_months: set[str] = set()
    if incremental and OUT.exists():
        try:
            existing = pd.read_parquet(OUT)
            if 'tw_market_pe' in existing.columns:
                pe_rows = existing.dropna(subset=['tw_market_pe']).copy()
                if not pe_rows.empty:
                    pe_rows['date'] = pd.to_datetime(pe_rows['date'])
                    pe_rows['_ym'] = pe_rows['date'].dt.to_period('M')
                    # 每月取「最後一筆」daily row 的 PE 值：
                    # build_panel 對 tw_market_pe 是月底寫入 + ffill，所以
                    #   first(2026-04) = 4/1 ffilled = 前月底值 (錯)
                    #   last(2026-04)  = 4/30 month-end actual = 該月實際值 (對)
                    monthly = pe_rows.groupby('_ym').agg(
                        tw_market_pe=('tw_market_pe', 'last'),
                        tw_market_pb=('tw_market_pb', 'last'),
                        tw_market_yield=('tw_market_yield', 'last'),
                    ).reset_index()
                    monthly['date'] = monthly['_ym'].dt.to_timestamp(how='end').dt.normalize()
                    monthly = monthly[['date', 'tw_market_pe', 'tw_market_pb', 'tw_market_yield']]
                    existing_monthly = monthly
                    # 除了當月以外的全部跳過 (當月強制重抓以涵蓋月底 publish)
                    now_ym = pd.Timestamp.now().to_period('M')
                    have_months = {
                        str(p) for p in pe_rows['_ym'].unique()
                        if p != now_ym
                    }
                    logger.info("Incremental: existing has %d monthly rows, skip %d already-fetched months",
                                len(existing_monthly), len(have_months))
        except Exception as e:
            logger.warning("Failed to read existing parquet for incremental: %s", e)

    if end_year is None:
        end_year = datetime.now().year
    rows = []
    now_ts = pd.Timestamp.now()
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            if pd.Timestamp(y, m, 1) > now_ts:
                break
            ym = f"{y:04d}{m:02d}"
            ym_period = f"{y:04d}-{m:02d}"
            if ym_period in have_months:
                continue  # incremental: 已有資料的月份直接跳過
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
        if existing_monthly is not None and not existing_monthly.empty:
            logger.info("No new TWSE PE months fetched; reusing existing monthly panel (%d rows)",
                        len(existing_monthly))
            return existing_monthly
        logger.warning("No TWSE PE data fetched")
        return pd.DataFrame()

    df_new = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    if existing_monthly is not None and not existing_monthly.empty:
        # new 覆寫 existing 同月份 (當月強制重抓)
        df = pd.concat([existing_monthly, df_new], ignore_index=True)
        df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last').reset_index(drop=True)
        logger.info("Merged incremental: %d existing + %d new -> %d monthly rows",
                    len(existing_monthly), len(df_new), len(df))
        return df
    return df_new


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

    # 盈餘殖利率 = 1/PE (純面板,無外部輸入)。原擬算 ERP(=盈餘殖利率 - TW10年公債),
    # 但 TW 10Y 無免費 daily 來源 (FRED 無 / yfinance 無 ticker / FinMind 付費),硬編
    # 常數會 silent 過期,故只呈現盈餘殖利率,由報告 LLM 自行對照公債/現金殖利率判斷貴賤。
    if 'tw_market_pe' in panel.columns:
        panel['tw_earnings_yield'] = 100.0 / panel['tw_market_pe'].replace(0, np.nan)

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
