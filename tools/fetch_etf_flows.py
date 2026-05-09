"""
fetch_etf_flows.py -- 信用市場 / 風險偏好 ETF 流量 proxy

抓 yfinance 收盤 + 成交量，推估資金流向：
  HYG  iShares iBoxx HY Corp Bond ETF — 信用避險強度
  JNK  SPDR Bloomberg HY Bond ETF     — 信用避險強度
  LQD  iShares iBoxx IG Corp Bond ETF — 比較組
  TLT  iShares 20+ Yr Treasury Bond   — risk-off proxy
  SPY  S&P 500 ETF                    — risk-on proxy

Derived:
  hyg_to_lqd_ratio   ：HY 相對 IG 表現（>1 = risk on）
  tlt_spy_ratio      ：避險相對風險（高 = risk off）
  hyg_volume_ma20    ：HYG 成交量 4w MA（量增 = 流動性事件）
  hyg_chg_4w         ：HYG 4w 變化率

執行：python tools/fetch_etf_flows.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "macro" / "etf_flows.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

TICKERS = ['HYG', 'JNK', 'LQD', 'TLT', 'SPY']


def fetch_one(ticker: str) -> pd.DataFrame:
    import yfinance as yf
    logger.info("Fetching %s...", ticker)
    df = yf.Ticker(ticker).history(period='15y')
    if df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    df['date'] = pd.to_datetime(df['Date'].dt.date) if 'Date' in df.columns else pd.to_datetime(df.index.date)
    return df[['date', 'Close', 'Volume']].rename(
        columns={'Close': f'{ticker.lower()}_close', 'Volume': f'{ticker.lower()}_volume'}
    )


def main():
    panel = None
    for t in TICKERS:
        df = fetch_one(t)
        if df.empty:
            logger.warning("No data for %s", t)
            continue
        if panel is None:
            panel = df
        else:
            panel = panel.merge(df, on='date', how='outer')

    if panel is None or panel.empty:
        logger.error("All ETF fetches failed")
        return

    panel = panel.sort_values('date').reset_index(drop=True)
    for col in panel.columns:
        if col == 'date':
            continue
        panel[col] = panel[col].ffill()

    # derived
    if 'hyg_close' in panel.columns and 'lqd_close' in panel.columns:
        panel['hyg_to_lqd_ratio'] = panel['hyg_close'] / panel['lqd_close']
        panel['hyg_to_lqd_chg_4w'] = panel['hyg_to_lqd_ratio'].pct_change(20) * 100

    if 'tlt_close' in panel.columns and 'spy_close' in panel.columns:
        panel['tlt_spy_ratio'] = panel['tlt_close'] / panel['spy_close']
        panel['tlt_spy_chg_4w'] = panel['tlt_spy_ratio'].pct_change(20) * 100

    if 'hyg_volume' in panel.columns:
        panel['hyg_volume_ma20'] = panel['hyg_volume'].rolling(20).mean()
        panel['hyg_volume_z_252d'] = (
            (panel['hyg_volume'] - panel['hyg_volume'].rolling(252).mean()) /
            panel['hyg_volume'].rolling(252).std()
        )

    if 'hyg_close' in panel.columns:
        panel['hyg_chg_4w'] = panel['hyg_close'].pct_change(20) * 100

    logger.info("Panel rows=%d cols=%d", len(panel), len(panel.columns))
    logger.info("Last row keys: %s", list(panel.iloc[-1].dropna().keys())[:15])
    last = panel.iloc[-1]
    for col in ['hyg_close', 'jnk_close', 'lqd_close', 'tlt_close', 'spy_close',
                'hyg_to_lqd_ratio', 'tlt_spy_ratio', 'hyg_chg_4w', 'tlt_spy_chg_4w']:
        if col in panel.columns:
            v = last.get(col)
            if pd.notna(v):
                logger.info("  %s = %.4f", col, v)

    panel.to_parquet(OUT, index=False)
    logger.info("Saved -> %s", OUT)


if __name__ == '__main__':
    main()
