"""
build_market_cap_panel.py -- 上市總市值 + 融資餘額佔市值比 (大盤層級, 上市 only)

產出 data/macro/market_cap.parquet:
  date, total_market_cap (上市總市值, 元),
  margin_value (官方融資金額, 元), margin_to_mktcap_pct (融資金額/總市值 x100),
  margin_mktcap_z_252d (252日 z-score)

資料源 (全免費, 零重複抓取):
  - 分母 總市值: 重用 data_cache/backtest/ohlcv_tw.parquet (refresh_universe_prices.py
    每日更新, run_scanner.bat:357) x TWSE OpenAPI t187ap03_L 發行股數
    (cache 在 data/macro/listed_shares.parquet, 月刷一次)。
    NOTE: 歷史用「當前發行股數 snapshot」近似 (資本變動慢, 對 252d z 影響小)。
  - 分子 融資金額: TWSE MI_MARGN selectType=MS 官方融資金額今日餘額 (仟元)。
    !! 用官方金額, 不自己 (融資張數 x 收盤) 重建 -- 後者會高估 ~2x
    (融資金額含融資成數 ~6成 + 用融資成本價, 非現價x100%)。

執行:
  python tools/build_market_cap_panel.py                       # 日更 (append 最新交易日)
  python tools/build_market_cap_panel.py --refresh-shares      # 強制重抓 t187ap03_L 股數
  python tools/build_market_cap_panel.py --backfill-margin 504 # 一次性回填 ~2yr 融資金額
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OHLCV_TW = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
MACRO = REPO / "data" / "macro"
SHARES_CACHE = MACRO / "listed_shares.parquet"
OUT = MACRO / "market_cap.parquet"
MACRO.mkdir(parents=True, exist_ok=True)

_HEADERS = {'User-Agent': 'Mozilla/5.0'}
_THROTTLE_SEC = 1.5


# --------------------------------------------------------------------------- #
#  發行股數 (t187ap03_L, 上市) -- cache 月刷
# --------------------------------------------------------------------------- #
def load_listed_shares(refresh: bool = False, max_age_days: int = 25) -> pd.Series:
    """上市公司在外流通普通股數 (股)。回 Series index=stock_id。
    cache 在 listed_shares.parquet, 超過 max_age_days 或 --refresh-shares 才重抓。"""
    if SHARES_CACHE.exists() and not refresh:
        age_days = (pd.Timestamp.now() - pd.Timestamp(SHARES_CACHE.stat().st_mtime, unit='s')).days
        if age_days <= max_age_days:
            s = pd.read_parquet(SHARES_CACHE)['shares']
            logger.info("listed_shares: %d 檔 (cache, %dd old)", len(s), age_days)
            return s
    logger.info("Fetching TWSE t187ap03_L 上市公司基本資料 (發行股數)...")
    url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    rows = requests.get(url, headers=_HEADERS, verify=False, timeout=30).json()
    shares = {}
    for row in rows:
        sid = str(row.get('公司代號', '')).strip()
        raw = str(row.get('已發行普通股數或TDR原股發行股數', '')).replace(',', '')
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if sid and val > 0:
            shares[sid] = val
    s = pd.Series(shares, name='shares')
    s.index.name = 'stock_id'
    s.to_frame().to_parquet(SHARES_CACHE)
    logger.info("listed_shares: %d 檔 (fresh -> %s)", len(s), SHARES_CACHE.name)
    return s


# --------------------------------------------------------------------------- #
#  總市值 (上市) -- 重用 ohlcv_tw x shares
# --------------------------------------------------------------------------- #
def build_market_cap_series(shares: pd.Series) -> pd.Series:
    """每日上市總市值 (元) = sum(收盤 x 發行股數)。重用 ohlcv_tw.parquet, 零抓取。"""
    if not OHLCV_TW.exists():
        raise FileNotFoundError(f"{OHLCV_TW} 不存在 (應由 refresh_universe_prices.py 維護)")
    df = pd.read_parquet(OHLCV_TW, columns=['stock_id', 'date', 'Close'])
    df['stock_id'] = df['stock_id'].astype(str)
    df = df[df['stock_id'].isin(shares.index)]  # 上市 only
    df['cap'] = df['Close'] * df['stock_id'].map(shares)
    grp = df.groupby('date')
    mktcap = grp['cap'].sum().sort_index()
    cnt = grp['stock_id'].count().sort_index()
    mktcap.index = pd.to_datetime(mktcap.index)
    cnt.index = pd.to_datetime(cnt.index)
    # 覆蓋率守門：partial-update 日 (檔數 << 近期中位數) 會給偏低總市值 -> 砍成 NaN，
    # 避免假的 margin/mktcap 比值尖峰污染 z-score (e.g. 2025-08-01 僅 452 檔 vs ~900)。
    # (a) 絕對覆蓋 floor：上市市值覆蓋 2016+ 才達 ~86-97%；2015 僅 ~511 檔 (~54% 覆蓋)
    #     -> 比值假性偏高 (2015-10~2016-01 衝到 0.9-1.0% 是 artifact 非真實) -> 砍 NaN。
    #     ⚠️ 即便 2016+ 仍 ~86-97% 覆蓋 (比值略偏高 ~10-16%, 逐年收斂)，但與危險帶校準
    #     同基礎故可用；2015 (54%) 失真過大必砍。
    MIN_STOCKS = 800
    low_abs = cnt < MIN_STOCKS
    if low_abs.any():
        logger.info("  絕對覆蓋 floor: 砍 %d 個 <%d 檔的日 (市值覆蓋不足, e.g. 2015)", int(low_abs.sum()), MIN_STOCKS)
        mktcap[low_abs] = np.nan
    # (b) 相對守門：partial-update 日 (檔數 << 近期中位數) 也砍
    med = cnt.rolling(60, min_periods=20).median()
    low_cov = cnt < (0.7 * med)
    if low_cov.any():
        logger.info("  覆蓋率守門: 砍 %d 個低覆蓋日 (檔數 < 0.7x 近60日中位數)", int(low_cov.sum()))
        mktcap[low_cov] = np.nan
    logger.info("total_market_cap: %d days, last(%s) = %.2f 兆 (%d 上市檔)",
                len(mktcap), mktcap.index[-1].date(), mktcap.iloc[-1] / 1e12, len(shares))
    return mktcap.rename('total_market_cap')


# --------------------------------------------------------------------------- #
#  融資金額 (官方 MI_MARGN selectType=MS) -- 元
# --------------------------------------------------------------------------- #
def fetch_margin_value(date: pd.Timestamp) -> float | None:
    """TWSE 官方上市融資金額今日餘額 (元)。MI_MARGN selectType=MS, 來源仟元 x1000。"""
    url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
    params = {'response': 'json', 'date': date.strftime('%Y%m%d'), 'selectType': 'MS'}
    try:
        d = requests.get(url, params=params, headers=_HEADERS, verify=False, timeout=25).json()
    except Exception as e:
        logger.warning("  MI_MARGN %s fetch err: %s", date.date(), e)
        return None
    if d.get('stat') != 'OK':
        return None  # 非交易日 / 無資料
    for t in d.get('tables', []):
        for row in t.get('data', []):
            if row and '融資金額' in str(row[0]):
                try:
                    return float(str(row[-1]).replace(',', '')) * 1000.0  # 今日餘額, 仟元 -> 元
                except (ValueError, IndexError):
                    return None
    return None


def backfill_margin_values(dates: list[pd.Timestamp], existing: pd.Series) -> pd.Series:
    """對 dates 中尚無資料者抓官方融資金額 (throttled)。回更新後 Series。"""
    out = existing.copy()
    todo = [d for d in dates if d not in out.index or pd.isna(out.get(d))]
    logger.info("融資金額 backfill: %d 待抓 / %d 已有", len(todo), len(out))
    for i, d in enumerate(todo, 1):
        v = fetch_margin_value(d)
        if v is not None:
            out.loc[d] = v
        if i % 20 == 0:
            logger.info("  ... %d/%d (last %s = %s)", i, len(todo), d.date(),
                        f"{v/1e8:.0f}億" if v else "None")
        time.sleep(_THROTTLE_SEC)
    return out.sort_index()


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--refresh-shares', action='store_true', help='強制重抓 t187ap03_L 股數')
    ap.add_argument('--backfill-margin', type=int, default=0,
                    help='回填最近 N 個交易日的官方融資金額 (default 0 = 只抓最新交易日)')
    args = ap.parse_args()

    shares = load_listed_shares(refresh=args.refresh_shares)
    mktcap = build_market_cap_series(shares)

    # 既有融資金額 series (從現有 panel 讀, 否則空)
    if OUT.exists():
        prev = pd.read_parquet(OUT)
        prev['date'] = pd.to_datetime(prev['date'])
        margin_val = prev.set_index('date')['margin_value'].dropna()
    else:
        margin_val = pd.Series(dtype=float, name='margin_value')

    # 決定要抓哪些日: 最新交易日 (日更) 或最近 N 日 (backfill)
    trade_dates = list(mktcap.index)
    n = args.backfill_margin if args.backfill_margin > 0 else 1
    target_dates = trade_dates[-n:]
    margin_val = backfill_margin_values(target_dates, margin_val)

    # margin_value 異常守門：融資餘額具黏性 (單日變動通常 <5%)，相對 10 日中位數
    # deviation > 20% = 抓取雜訊 (backfill 暫時性壞回應, e.g. 2025-02-07 曾存成 1727億
    # vs 官方 3060億) -> 砍 NaN，避免污染 z-score。真實崩盤是漸進去槓桿不會單日 -20%。
    if len(margin_val) >= 5:
        mv_med = margin_val.rolling(10, center=True, min_periods=3).median()
        bad = (margin_val - mv_med).abs() > 0.20 * mv_med
        if bad.any():
            logger.info("margin_value 守門: 砍 %d 個異常值 (單日 deviation > 20%% vs 10日中位數): %s",
                        int(bad.sum()), [str(d.date()) for d in margin_val.index[bad]])
            margin_val = margin_val.mask(bad)

    # 組 panel: 對齊到有總市值的交易日
    panel = mktcap.to_frame()
    panel['margin_value'] = margin_val.reindex(panel.index)
    panel['margin_to_mktcap_pct'] = panel['margin_value'] / panel['total_market_cap'] * 100.0
    # 252d z-score (只在 ratio 有值處)
    # z-score: min_periods=120 (需 ~6 月歷史才算, 避免 startup 低變異窗口放大成假極端 z;
    # 此比值很穩定 0.3-0.5%, 早期窗口 std 過小會噴極端值)
    r = panel['margin_to_mktcap_pct']
    panel['margin_mktcap_z_252d'] = (
        (r - r.rolling(252, min_periods=120).mean()) / r.rolling(252, min_periods=120).std()
    )
    panel = panel.reset_index().rename(columns={'index': 'date'})

    panel.to_parquet(OUT, index=False)
    last = panel.dropna(subset=['margin_to_mktcap_pct']).iloc[-1] if panel['margin_to_mktcap_pct'].notna().any() else None
    if last is not None:
        logger.info("OK -> %s | %s: 總市值 %.2f兆 / 融資 %.0f億 / 佔比 %.3f%% / z=%.2f",
                    OUT.name, pd.Timestamp(last['date']).date(),
                    last['total_market_cap'] / 1e12, last['margin_value'] / 1e8,
                    last['margin_to_mktcap_pct'],
                    last['margin_mktcap_z_252d'] if pd.notna(last['margin_mktcap_z_252d']) else float('nan'))
    else:
        logger.warning("OK -> %s | 尚無融資金額 (跑 --backfill-margin N 補歷史)", OUT.name)


if __name__ == '__main__':
    main()
