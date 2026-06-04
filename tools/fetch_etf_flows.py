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

TICKERS = ['HYG', 'JNK', 'LQD', 'TLT', 'SPY', '^MOVE',
           # P2-8 (AI 報告 2026-05-09 建議): 新興市場資金流
           'EEM',   # iShares MSCI Emerging Markets ETF
           'EMB',   # iShares JPM USD EM Bond ETF
           'FXI',   # iShares China Large-Cap ETF (中國)
           'EWJ',   # iShares MSCI Japan ETF (日股)
           # 第 5 段缺口補充 (2026-06-03 macro 報告建議): 成長/通膨代理商品
           'HG=F',  # 銅期貨 (Dr. Copper,景氣成長代理)
           'GC=F',  # 黃金期貨 (避險)
           'CL=F']  # WTI 原油期貨 (面板有 OVX 油波動卻無油價本身)


def fetch_one(ticker: str) -> pd.DataFrame:
    import yfinance as yf
    logger.info("Fetching %s...", ticker)
    df = yf.Ticker(ticker).history(period='15y')
    if df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    df['date'] = pd.to_datetime(df['Date'].dt.date) if 'Date' in df.columns else pd.to_datetime(df.index.date)
    # ^MOVE 沒有有意義的 volume，用 close；HG=F/GC=F/CL=F 商品期貨去掉 '=F' 後綴 (HG=F->hg)
    safe_ticker = ticker.lstrip('^').replace('=F', '').lower()
    cols = ['date', 'Close']
    rename = {'Close': f'{safe_ticker}_close'}
    if 'Volume' in df.columns and df['Volume'].sum() > 0:
        cols.append('Volume')
        rename['Volume'] = f'{safe_ticker}_volume'
    return df[cols].rename(columns=rename)


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

    # P0-3 HY ETF dollar volume flow (price × volume), 4w MA + z-score
    if 'hyg_close' in panel.columns and 'hyg_volume' in panel.columns:
        panel['hyg_dollar_flow'] = panel['hyg_close'] * panel['hyg_volume']
        panel['hyg_dollar_flow_ma20'] = panel['hyg_dollar_flow'].rolling(20).mean()
        panel['hyg_dollar_flow_z_252d'] = (
            (panel['hyg_dollar_flow'] - panel['hyg_dollar_flow'].rolling(252).mean()) /
            panel['hyg_dollar_flow'].rolling(252).std()
        )
    if 'jnk_close' in panel.columns and 'jnk_volume' in panel.columns:
        panel['jnk_dollar_flow'] = panel['jnk_close'] * panel['jnk_volume']
        panel['jnk_dollar_flow_z_252d'] = (
            (panel['jnk_dollar_flow'] - panel['jnk_dollar_flow'].rolling(252).mean()) /
            panel['jnk_dollar_flow'].rolling(252).std()
        )

    # P0-2 MOVE Index 變化率（高 = 美債波動 = 5-15d crash lead per AI 報告）
    if 'move_close' in panel.columns:
        panel['move_chg_2w'] = panel['move_close'].pct_change(10) * 100
        panel['move_chg_4w'] = panel['move_close'].pct_change(20) * 100
        panel['move_z_252d'] = (
            (panel['move_close'] - panel['move_close'].rolling(252).mean()) /
            panel['move_close'].rolling(252).std()
        )

    # P2-8 EM 資金流 (2026-05-09 AI 報告建議：外資對台買賣超是 EM allocation 子集)
    if 'eem_close' in panel.columns:
        panel['eem_chg_4w'] = panel['eem_close'].pct_change(20) * 100
        if 'eem_volume' in panel.columns:
            panel['eem_dollar_flow'] = panel['eem_close'] * panel['eem_volume']
            panel['eem_dollar_flow_z_252d'] = (
                (panel['eem_dollar_flow'] - panel['eem_dollar_flow'].rolling(252).mean()) /
                panel['eem_dollar_flow'].rolling(252).std()
            )

    if 'emb_close' in panel.columns:
        panel['emb_chg_4w'] = panel['emb_close'].pct_change(20) * 100

    # SPY 相對 EEM (US 相對 EM 表現)，下跌 = EM weak / US flight to quality
    if 'spy_close' in panel.columns and 'eem_close' in panel.columns:
        panel['eem_to_spy_ratio'] = panel['eem_close'] / panel['spy_close']
        panel['eem_to_spy_chg_4w'] = panel['eem_to_spy_ratio'].pct_change(20) * 100

    # 銅金比 (銅/金；成長預期 vs 避險需求,景氣循環代理) + 原油價格 level (2026-06-03 報告建議)
    if 'hg_close' in panel.columns and 'gc_close' in panel.columns:
        panel['copper_gold_ratio'] = panel['hg_close'] / panel['gc_close']
        panel['copper_gold_chg_4w'] = panel['copper_gold_ratio'].pct_change(20) * 100
    if 'cl_close' in panel.columns:
        panel['cl_chg_4w'] = panel['cl_close'].pct_change(20) * 100

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
