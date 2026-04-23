"""
money_supply.py -- M1B 貨幣供給 + 成交量/M1B 比 (台股過熱指標)

資料源:
  - M1B: 中央銀行 EF15M01.csv (月資料日平均，1987/05 起)
    https://data.gov.tw/dataset/6024
  - 成交值: TWSE FMTQIK (日資料，每日大盤統計)

指標定義:
  成交量 M1B 比 = 近 20 交易日成交金額總和 / 最近發布之 M1B × 100%

警戒線 (基於台股 2016-2026 近十年經驗值):
  <15%   清淡 (grey)
  15-25% 正常 (green)
  25-40% 偏熱 (yellow)
  40-60% 過熱 (orange)
  >=60%  瘋狂 (red)
參考: 2021 AI / meme 狂熱約 30%；2026 Q2 AI 資本狂潮一度衝破 50%。

快取策略:
  - M1B CSV: 7 天 (月更新資料，寬鬆即可)
  - TWSE 日成交值: 一天一次 (14:00 後抓)
  - compute_m1b_ratio 本身不做快取，由 market_banner.py per-indicator cache 負責
"""

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

CBC_M1B_URL = "https://www.cbc.gov.tw/public/data/OpenData/經研處/EF15M01.csv"
TWSE_FMTQIK_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"

# CSV 欄位索引 (2026-04-23 驗證)
# [0] 期間 "1987M05" … "2026M01"
# [25] 貨幣總計數-1A-原始值
# [27] 貨幣總計數-1B-原始值 (百萬 TWD, 日平均餘額) <-- 使用這欄
# [29] 貨幣總計數-2-原始值
M1B_PERIOD_COL = 0
M1B_VALUE_COL = 27

_CACHE_DIR = Path(__file__).parent / "data_cache"
_M1B_CACHE_FILE = _CACHE_DIR / "cbc_m1b.csv"
_M1B_CACHE_TTL = 7 * 24 * 3600  # 7 天

_TWSE_REQ_INTERVAL = 3.0
_last_twse_req = 0.0


def _cache_is_fresh(path: Path, ttl: int) -> bool:
    if not path.exists():
        return False
    return (time.time() - path.stat().st_mtime) < ttl


def _download_cbc_m1b():
    """下載 CBC M1B CSV → 寫入磁碟快取"""
    resp = requests.get(CBC_M1B_URL, timeout=30, verify=False)
    resp.raise_for_status()
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _M1B_CACHE_FILE.write_bytes(resp.content)
    logger.info("M1B CSV downloaded: %d bytes", len(resp.content))


def get_m1b_series(force_refresh: bool = False) -> pd.DataFrame:
    """
    回傳 M1B 月資料 DataFrame [period(str YYYYMM), m1b_mil_twd(百萬)]

    Raises
    ------
    Exception
        若下載失敗且無磁碟快取可用
    """
    if force_refresh or not _cache_is_fresh(_M1B_CACHE_FILE, _M1B_CACHE_TTL):
        try:
            _download_cbc_m1b()
        except Exception as e:
            logger.warning("CBC M1B download failed, fallback to stale cache: %s", e)
            if not _M1B_CACHE_FILE.exists():
                raise
    df = pd.read_csv(_M1B_CACHE_FILE, encoding='utf-8-sig')
    period = df.iloc[:, M1B_PERIOD_COL].astype(str).str.replace('M', '', regex=False)
    m1b = pd.to_numeric(df.iloc[:, M1B_VALUE_COL], errors='coerce')
    out = pd.DataFrame({'period': period, 'm1b_mil_twd': m1b}).dropna()
    return out.reset_index(drop=True)


def get_latest_m1b():
    """回傳最近一筆已發布的 M1B → (period_str "YYYYMM", value_mil_twd)"""
    df = get_m1b_series()
    if df.empty:
        return None, None
    row = df.iloc[-1]
    return str(row['period']), float(row['m1b_mil_twd'])


def _throttle_twse():
    global _last_twse_req
    elapsed = time.time() - _last_twse_req
    if elapsed < _TWSE_REQ_INTERVAL:
        time.sleep(_TWSE_REQ_INTERVAL - elapsed)
    _last_twse_req = time.time()


def _fetch_twse_month(year: int, month: int):
    """
    抓 TWSE 某月全部交易日的日成交金額。

    Returns
    -------
    list[(datetime.date, int_trading_value_twd)]
    """
    _throttle_twse()
    url = f"{TWSE_FMTQIK_URL}?date={year:04d}{month:02d}01&response=json"
    resp = requests.get(
        url, timeout=15, verify=False,
        headers={'User-Agent': 'Mozilla/5.0'},
    )
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for r in data.get('data', []):
        # r[0] 'YYY/MM/DD' (民國), r[2] '成交金額' TWD with commas
        try:
            parts = r[0].split('/')
            ad_date = datetime(int(parts[0]) + 1911, int(parts[1]), int(parts[2])).date()
            tv = int(r[2].replace(',', ''))
            rows.append((ad_date, tv))
        except (ValueError, IndexError, AttributeError) as e:
            logger.debug("Skip malformed FMTQIK row %r: %s", r, e)
            continue
    return rows


def get_trailing_trading_value(n_days: int = 20):
    """
    抓近 n 個交易日的總成交金額（TWD）。

    Returns
    -------
    dict | None
        {total_twd, n_days (actual), start_date, end_date}
    """
    today = datetime.now().date()
    rows = []
    for offset in (1, 0):  # 先抓上月、再抓本月，時序一致
        y, m = today.year, today.month - offset
        if m <= 0:
            m += 12
            y -= 1
        try:
            rows.extend(_fetch_twse_month(y, m))
        except Exception as e:
            logger.warning("TWSE FMTQIK %04d%02d failed: %s", y, m, e)
    if not rows:
        return None
    rows.sort(key=lambda x: x[0])
    recent = rows[-n_days:]
    return {
        'total_twd': sum(r[1] for r in recent),
        'n_days': len(recent),
        'start_date': recent[0][0],
        'end_date': recent[-1][0],
    }


def _classify_ratio(ratio_pct: float):
    """依近十年警戒線分類 → (label, color_hex)"""
    if ratio_pct < 15:
        return '清淡', '#888888'
    if ratio_pct < 25:
        return '正常', '#00AA00'
    if ratio_pct < 40:
        return '偏熱', '#FFD700'
    if ratio_pct < 60:
        return '過熱', '#FF8800'
    return '瘋狂', '#FF4444'


def compute_m1b_ratio(n_days: int = 20):
    """
    計算「近 n 交易日成交金額 / M1B × 100」(台股過熱指標)。

    Returns
    -------
    dict | None
        ratio_pct, m1b_period, m1b_mil_twd,
        trading_value_twd, n_days, end_date (data_date),
        label, color
    """
    try:
        period, m1b_mil = get_latest_m1b()
    except Exception as e:
        logger.warning("M1B fetch failed: %s", e)
        return None
    if m1b_mil is None or m1b_mil <= 0:
        return None

    tv = get_trailing_trading_value(n_days)
    if tv is None or tv.get('total_twd', 0) <= 0:
        return None

    m1b_twd = m1b_mil * 1e6
    ratio_pct = tv['total_twd'] / m1b_twd * 100
    label, color = _classify_ratio(ratio_pct)

    return {
        'ratio_pct': round(ratio_pct, 2),
        'm1b_period': period,
        'm1b_mil_twd': m1b_mil,
        'trading_value_twd': tv['total_twd'],
        'n_days': tv['n_days'],
        'end_date': tv['end_date'],
        'data_date': tv['end_date'],
        'label': label,
        'color': color,
    }
