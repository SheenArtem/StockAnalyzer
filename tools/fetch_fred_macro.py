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
# FRED fredgraph.csv 從本機 latency 高(實測 20-50s/序列，大型 daily 序列尤甚)；
# timeout=30 會讓 T10Y2Y/VIXCLS/SOFR 等慢序列 timeout → 整欄被 drop。提高到 60s。
FRED_TIMEOUT = 60

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
    # 第 5 段資料缺口補充 (2026-06-03 macro 報告建議): 實質利率/通膨預期/準備金/IORB/IG OAS
    'DFII10':       'us_real_yield_10y',     # 10年期 TIPS 實質殖利率 (日,估值折現率核心)
    'T10YIE':       'us_breakeven_10y',      # 10年期 breakeven 通膨預期 (日)
    'WRESBAL':      'bank_reserves',         # 銀行存款準備金餘額 (週,RRP 乾涸後的邊際流動性緩衝)
    'IORB':         'iorb',                  # 存款準備金利率 (日,2021-07+;SOFR-IORB 利差看資金壓力)
    'BAMLC0A0CM':   'ig_oas',                # ICE BofA 美投資級公司債 OAS (日,補信用品質階梯 IG->HY->CCC)
}


def _parse_fred_csv(text: str, series_id: str) -> pd.DataFrame:
    """fredgraph.csv 回應 -> (date, series_id) df。FRED 用 '.' 表示缺值。"""
    df = pd.read_csv(StringIO(text))
    df.columns = ['date', series_id]
    df['date'] = pd.to_datetime(df['date'])
    df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
    return df.dropna(subset=[series_id])


def fetch_chunked(series_id: str, start: str, years_per_chunk: int = 3) -> pd.DataFrame:
    """全範圍 fetch 失敗時的分段 fallback。

    2026-06-04 實測：FRED 後端對部分熱門 daily 序列 (VIXCLS/DEXJPUS/SOFR/T10Y2Y...)
    大範圍 CSV 生成逾時回 504 (換 IP/VPN 同樣 504 = 全球性)，但小日期範圍 200/數秒
    -> 按 cosd/coed 切 3 年一塊各自抓再 concat 可繞過。
    """
    logger.warning("%s full-range failed -> chunked fallback (%dyr/chunk)", series_id, years_per_chunk)
    start_year = int(start[:4])
    end_year = datetime.now().year
    parts = []
    for y0 in range(start_year, end_year + 1, years_per_chunk):
        y1 = min(y0 + years_per_chunk - 1, end_year)
        url = FRED_BASE.format(sid=series_id, start=f"{y0}-01-01") + f"&coed={y1}-12-31"
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=FRED_TIMEOUT, verify=False)
                r.raise_for_status()
                parts.append(_parse_fred_csv(r.text, series_id))
                break
            except Exception as e:
                logger.warning("chunk %d-%d attempt %d failed for %s: %s", y0, y1, attempt + 1, series_id, e)
                if attempt < 2:
                    time.sleep(2)
                else:
                    raise  # 連分段都失敗 -> 交給 build_panel 的 carry-forward
        time.sleep(1)  # 塊間禮貌間隔，避免觸發 FRED 限流
    out = (pd.concat(parts, ignore_index=True)
           .drop_duplicates(subset='date').sort_values('date').reset_index(drop=True))
    logger.info("%s chunked OK: %d rows", series_id, len(out))
    return out


def fetch_one(series_id: str, start: str) -> pd.DataFrame:
    """從 FRED 抓單一序列，回傳 (date, value) df。"""
    url = FRED_BASE.format(sid=series_id, start=start)
    logger.info("Fetching %s from %s", series_id, start)
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=FRED_TIMEOUT, verify=False)
            r.raise_for_status()
            return _parse_fred_csv(r.text, series_id)
        except Exception as e:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, series_id, e)
            if attempt < 2:
                time.sleep(2)
            else:
                # 全範圍 3 次皆失敗 (FRED 大 CSV 504 degradation 模式) -> 分段抓取 fallback
                return fetch_chunked(series_id, start)


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
    """抓所有序列、merge 成日頻 panel、補 derived columns。

    Robustness (2026-06-04)：FRED 個別序列抓失敗 (本機 latency 高常 timeout) 時，
    不再 silent drop 整欄 (曾害報告掉 VIX/USDJPY/SOFR)，改從既有 panel carry-forward
    最後好值 + log ERROR (fail loud)。新序列若失敗則本輪缺欄，待下次成功。
    """
    prev = None
    if OUT.exists():
        try:
            prev = pd.read_parquet(OUT)
        except Exception as e:
            logger.warning("讀既有 panel 供 carry-forward 失敗: %s", e)

    panel = None
    failed = []  # (sid, friendly_col) 抓失敗者，rename 後從 prev carry-forward
    for sid, col in SERIES.items():
        try:
            df = fetch_one(sid, start)
        except Exception as e:
            logger.error("Failed to fetch %s (%s): %s", sid, col, e)
            failed.append((sid, col))
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

    # carry-forward 抓失敗的序列 (避免 silent 掉欄；置於 ffill/derived 前，使 derived 能用沿用值)
    for sid, col in failed:
        if prev is not None and col in prev.columns and col not in panel.columns:
            panel = panel.merge(prev[['date', col]], on='date', how='left')
            logger.error("CARRY-FORWARD %s (%s): fetch 失敗 -> 沿用既有 panel 最後好值 (stale，非當日抓取)", col, sid)
        else:
            logger.error("MISSING %s (%s): fetch 失敗且既有 panel 無此欄 -> 本輪缺欄，待下次成功", col, sid)

    # 砍掉「真實市場資料」之後的尾列 (forward-stamped 未來/週末列)。
    # FRED 行政利率 IORB 帶 forward-effective 日期 (實測戳到今日+1 的 06-15)，會把
    # outer-merge 後的 spine 推到未來日 -> 全域 ffill 把每欄沿用到未來日 -> 下游
    # valuation_panel (build_buffett_us 讀本 panel) 與 Slow Track as_of 顯示未來日
    # (2026-06-14 稽核確認；leadership_panel 已有「裁到價格源最大日期」護欄故免疫)。
    # 用「非 forward-stamp 序列至少一欄有真值」界定最後真實交易日；置於 ffill 前，
    # 否則 ffill 會先把真值填進未來列使其看似有資料。⚠️ 未來若再加 FRED 行政利率
    # (如 DFEDTARU/DFEDTARL) 也會 forward-stamp，需一併加進 FORWARD_STAMPED。
    FORWARD_STAMPED = {'iorb'}
    real_cols = [c for c in panel.columns if c not in ({'date'} | FORWARD_STAMPED)]
    if real_cols:
        real_mask = panel[real_cols].notna().any(axis=1)
        if real_mask.any():
            real_max = panel.loc[real_mask, 'date'].max()
            n_before = len(panel)
            panel = panel[panel['date'] <= real_max].reset_index(drop=True)
            if n_before > len(panel):
                logger.info("Trimmed %d forward-stamped 尾列 (IORB effective-date) -> spine 收到 %s",
                            n_before - len(panel), real_max.date())

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
    if 'ig_oas' in panel.columns:
        panel['ig_oas_rank'] = panel['ig_oas'].rolling(2520, min_periods=252).rank(pct=True) * 100

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

    # 銀行存準 WRESBAL：FRED 單位歷史在百萬/十億間變動過，統一正規化到十億 ($B) 與 RRP/TGA 一致
    # (準備金 ~$3T：百萬級會是 ~3,000,000、十億級 ~3,000；門檻 1e5 兩者間隔極大故安全)
    if 'bank_reserves' in panel.columns:
        _med = panel['bank_reserves'].dropna().median()
        if pd.notna(_med) and _med > 1e5:
            panel['bank_reserves'] = panel['bank_reserves'] / 1000.0
        panel['bank_reserves_chg_4w'] = panel['bank_reserves'].diff(20)  # $B 近4週變化(負=準備金流失)

    # SOFR - IORB 利差 (資金壓力在「利差」非「水位」；走闊=回購市場緊/準備金稀缺早警；IORB 2021-07+)
    if 'sofr' in panel.columns and 'iorb' in panel.columns:
        panel['sofr_iorb_spread'] = panel['sofr'] - panel['iorb']

    # 10年實質殖利率近4週變化 (實質利率上行=估值折現率升,壓抑高估值)
    if 'us_real_yield_10y' in panel.columns:
        panel['us_real_yield_10y_chg_4w'] = panel['us_real_yield_10y'].diff(20)

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
