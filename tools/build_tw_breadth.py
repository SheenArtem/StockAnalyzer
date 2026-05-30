"""
build_tw_breadth.py -- 台股市場廣度面板

從 data_cache/*_price.csv 聚合算出大盤廣度：
  - advances / declines / unchanged (daily)
  - ADL (Advance-Decline Line, cumulative A-D)
  - ADL MA20
  - McClellan Oscillator (EMA(A-D, 19) - EMA(A-D, 39))
  - A/D Ratio (volume-weighted: 上漲股總量 / 下跌股總量)
  - Breadth Thrust 10d (10 日 advances 占比)
  - 新高新低家數 (252 日)

輸出：data/breadth/tw_breadth.parquet (date, columns above)

執行：python tools/build_tw_breadth.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
CACHE = REPO / "data_cache"
OUT = REPO / "data" / "breadth" / "tw_breadth.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)


def load_all_prices(min_rows: int = 200) -> tuple[pd.DataFrame, pd.DataFrame]:
    """讀取 cache 內所有 *_price.csv，回傳 close pivot + volume pivot。"""
    files = sorted(CACHE.glob("*_price.csv"))
    logger.info("Loading %d price files...", len(files))

    closes = {}
    volumes = {}
    skipped = 0
    for i, f in enumerate(files):
        if i % 200 == 0 and i > 0:
            logger.info("  ...loaded %d/%d (skipped %d)", i, len(files), skipped)
        ticker = f.stem.replace('_price', '')
        try:
            df = pd.read_csv(f, index_col=0)
            df.index = pd.to_datetime(df.index, errors='coerce')
            df = df[~df.index.isna()]
            if df.empty or len(df) < min_rows:
                skipped += 1
                continue
            if 'Close' not in df.columns:
                skipped += 1
                continue
            close_s = pd.to_numeric(df['Close'], errors='coerce').dropna()
            if len(close_s) < min_rows:
                skipped += 1
                continue
            closes[ticker] = close_s
            if 'Volume' in df.columns:
                volumes[ticker] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        except Exception as e:
            logger.debug("skip %s: %s", ticker, e)
            skipped += 1

    logger.info("Loaded close=%d volume=%d (skipped=%d)",
                len(closes), len(volumes), skipped)

    close_df = pd.DataFrame(closes).sort_index()
    vol_df = pd.DataFrame(volumes).sort_index()
    return close_df, vol_df


def compute_breadth(close_df: pd.DataFrame, vol_df: pd.DataFrame) -> pd.DataFrame:
    """逐日計算廣度。"""
    logger.info("Computing daily diff...")
    chg = close_df.pct_change()

    advances = (chg > 0).sum(axis=1)
    declines = (chg < 0).sum(axis=1)
    unchanged = ((chg == 0) | chg.isna()).sum(axis=1)

    # Up/Down Volume Ratio (UVOL/DVOL): 上漲股總成交量 / 下跌股總成交量
    # 量能版漲跌比，非漲跌「家數」比(家數見 advances/declines)；亦為 Arms Index/TRIN 的分母
    logger.info("Computing Up/Down Volume Ratio (UVOL/DVOL)...")
    up_mask = chg > 0
    dn_mask = chg < 0
    up_vol = (vol_df * up_mask.reindex_like(vol_df).astype(float)).sum(axis=1)
    dn_vol = (vol_df * dn_mask.reindex_like(vol_df).astype(float)).sum(axis=1)
    up_down_vol_ratio = up_vol / dn_vol.replace(0, np.nan)

    # ADL = cumulative (A - D)
    ad_diff = advances - declines
    adl = ad_diff.cumsum()
    adl_ma20 = adl.rolling(20).mean()

    # McClellan Oscillator: EMA(A-D, 19) - EMA(A-D, 39)
    mco = ad_diff.ewm(span=19, adjust=False).mean() - ad_diff.ewm(span=39, adjust=False).mean()

    # Breadth Thrust 10d: 10d 內 advances 占 (advances+declines) 的比率
    total_ad = advances + declines
    bt = (advances.rolling(10).sum() / total_ad.rolling(10).sum().replace(0, np.nan))

    # 52w 新高 / 新低家數 (252 trading days)
    logger.info("Computing 52w highs/lows...")
    rolling_max = close_df.rolling(252, min_periods=60).max()
    rolling_min = close_df.rolling(252, min_periods=60).min()
    new_highs = (close_df >= rolling_max).sum(axis=1)
    new_lows = (close_df <= rolling_min).sum(axis=1)
    new_high_minus_low = new_highs - new_lows

    # P1-4: % Above 50DMA / 200DMA (中期結構廣度，AI 報告 2026-05-09 建議)
    logger.info("Computing % above 50/200 DMA...")
    ma50 = close_df.rolling(50, min_periods=20).mean()
    ma200 = close_df.rolling(200, min_periods=60).mean()
    valid_count = close_df.notna().sum(axis=1)
    above_50dma_count = (close_df > ma50).sum(axis=1)
    above_200dma_count = (close_df > ma200).sum(axis=1)
    pct_above_50dma = above_50dma_count / valid_count.replace(0, np.nan) * 100
    pct_above_200dma = above_200dma_count / valid_count.replace(0, np.nan) * 100

    out = pd.DataFrame({
        'date': close_df.index,
        'advances': advances.values,
        'declines': declines.values,
        'unchanged': unchanged.values,
        'ad_diff': ad_diff.values,
        'adl': adl.values,
        'adl_ma20': adl_ma20.values,
        'mcclellan_oscillator': mco.values,
        'up_down_vol_ratio': up_down_vol_ratio.values,
        'breadth_thrust_10d': bt.values,
        'new_highs_52w': new_highs.values,
        'new_lows_52w': new_lows.values,
        'new_high_minus_low': new_high_minus_low.values,
        'pct_above_50dma': pct_above_50dma.values,
        'pct_above_200dma': pct_above_200dma.values,
    })

    return out


def main():
    close_df, vol_df = load_all_prices()
    if close_df.empty:
        raise RuntimeError("No price data found in data_cache/")

    panel = compute_breadth(close_df, vol_df)
    panel = panel.dropna(subset=['advances'])  # 早期日子可能沒資料

    # 過濾掉 advances+declines < 50 的日子（資料不足）
    panel = panel[(panel['advances'] + panel['declines']) >= 50].reset_index(drop=True)

    logger.info("Panel rows=%d", len(panel))
    logger.info("Date range: %s ~ %s",
                panel['date'].min().date() if hasattr(panel['date'].min(), 'date') else panel['date'].min(),
                panel['date'].max().date() if hasattr(panel['date'].max(), 'date') else panel['date'].max())
    logger.info("Last row:\n%s", panel.iloc[-1].to_dict())

    panel.to_parquet(OUT, index=False)
    logger.info("Saved -> %s", OUT)


if __name__ == '__main__':
    main()
