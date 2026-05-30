"""
build_leadership_panel.py -- 領頭羊 / 跨市場領先訊號面板

窄幅領漲 (narrow-leadership) regime 下，少數權值股 (半導體/AI 巨頭) 撐起整個
市值加權指數，廣度卻背離 (ADL 史低)。本面板補兩個「領頭羊裂痕」早期警報：

  1. SOX 相對強弱 (sox_to_twii_ratio + 4w 變化)
       ^SOX (費城半導體指數) vs ^TWII。半導體是台股領頭複合體；SOX 相對
       TWII 由領先轉落後 = 領頭羊鬆動的早期訊號。

  2. TSM ADR 隔夜溢價 (tsm_adr_premium_pct)
       NYSE: TSM (台積電 ADR, 1 ADR = 5 股) 在台北休市期間交易，其 FX 調整後
       相對 2330 的溢/折價，是 TWII 開盤的近 1 日領先 -- 直接捕捉外資對
       「最重要單一成分股」的隔夜態度。日期對齊：同曆日 D 的 TSM(US 收盤,
       台北深夜) vs 2330(TW 收盤,當日 13:30) -> 溢價預示 D+1 台股開盤跳空。

資料源：yfinance (^SOX / ^TWII / 2330.TW / TSM，全免費)；USDTWD 重用既有
  data/macro/fred_panel.parquet 的 usdtwd_close (避免重複抓 FX)，缺則 fallback
  yfinance TWD=X。

輸出：data/macro/leadership_panel.parquet

informational tier：僅供 macro 報告 + UI 顯示，未經 IC 驗證，不接 composite /
  portfolio gating。

執行：python tools/build_leadership_panel.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "macro" / "leadership_panel.parquet"
FRED_PANEL = REPO / "data" / "macro" / "fred_panel.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

# TSMC ADR 比例：1 ADR = 5 股普通股 (2330)
TSM_ADR_RATIO = 5


def fetch_close(ticker: str, label: str, period: str = "15y") -> pd.DataFrame:
    """抓 yfinance 收盤，回傳 (date, <label>) df。失敗回空 df 不拋。"""
    try:
        import yfinance as yf
        logger.info("Fetching %s (%s)...", ticker, label)
        df = yf.Ticker(ticker).history(period=period, auto_adjust=False)
    except Exception as e:
        logger.warning("  %s fetch error: %s", ticker, e)
        return pd.DataFrame(columns=['date', label])
    if df.empty:
        logger.warning("  %s returned empty", ticker)
        return pd.DataFrame(columns=['date', label])
    s = df['Close']
    idx = s.index
    idx = idx.tz_localize(None) if getattr(idx, 'tz', None) is not None else idx
    out = pd.DataFrame({'date': pd.to_datetime(idx.date), label: s.astype(float).values})
    return out.dropna(subset=[label]).drop_duplicates('date').reset_index(drop=True)


def load_usdtwd() -> pd.DataFrame:
    """USDTWD：優先重用 fred_panel.usdtwd_close，缺則 fallback yfinance TWD=X。"""
    if FRED_PANEL.exists():
        try:
            fp = pd.read_parquet(FRED_PANEL, columns=['date', 'usdtwd_close'])
            fp = fp.dropna(subset=['usdtwd_close'])
            if not fp.empty:
                fp['date'] = pd.to_datetime(fp['date'])
                logger.info("USDTWD reused from fred_panel: %d rows", len(fp))
                return fp.rename(columns={'usdtwd_close': 'usdtwd'})
        except Exception as e:
            logger.warning("read fred_panel usdtwd failed: %s", e)
    logger.info("USDTWD fallback to yfinance TWD=X")
    return fetch_close('TWD=X', 'usdtwd')


def build_panel() -> pd.DataFrame:
    sox = fetch_close('^SOX', 'sox_close')
    twii = fetch_close('^TWII', 'twii_close')
    tsm = fetch_close('TSM', 'tsm_adr_usd')
    tw2330 = fetch_close('2330.TW', 'tw2330_close')
    usdtwd = load_usdtwd()

    panel = None
    for df in (sox, twii, tsm, tw2330, usdtwd):
        if df is None or df.empty:
            continue
        panel = df if panel is None else panel.merge(df, on='date', how='outer')

    if panel is None or panel.empty:
        raise RuntimeError("All leadership fetches failed")

    panel = panel.sort_values('date').reset_index(drop=True)
    for col in panel.columns:
        if col != 'date':
            panel[col] = panel[col].ffill()

    # 1. SOX 相對強弱 vs TWII (絕對比值無意義，看 4w 變化)
    if 'sox_close' in panel.columns and 'twii_close' in panel.columns:
        panel['sox_to_twii_ratio'] = panel['sox_close'] / panel['twii_close'].replace(0, np.nan)
        panel['sox_rs_chg_4w'] = panel['sox_to_twii_ratio'].pct_change(20) * 100

    # 2. TSM ADR 隔夜溢價 (FX 調整後 vs 2330)
    #    隱含 2330 (TWD) = TSM_ADR(USD) / 5 * USDTWD；溢價% = (隱含/2330 - 1) * 100
    if all(c in panel.columns for c in ('tsm_adr_usd', 'tw2330_close', 'usdtwd')):
        implied_2330 = panel['tsm_adr_usd'] / TSM_ADR_RATIO * panel['usdtwd']
        panel['tsm_adr_premium_pct'] = (
            implied_2330 / panel['tw2330_close'].replace(0, np.nan) - 1.0
        ) * 100

    return panel


def main():
    panel = build_panel()
    panel.to_parquet(OUT, index=False)
    logger.info("Panel rows=%d cols=%d -> %s", len(panel), len(panel.columns), OUT)
    logger.info("Date range: %s ~ %s", panel['date'].min().date(), panel['date'].max().date())
    last = panel.iloc[-1]
    for col in ['sox_to_twii_ratio', 'sox_rs_chg_4w', 'tsm_adr_premium_pct']:
        if col in panel.columns and pd.notna(last.get(col)):
            logger.info("  %s = %.4f", col, last.get(col))


if __name__ == '__main__':
    main()
