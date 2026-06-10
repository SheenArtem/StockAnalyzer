"""
market_banner.py -- 大盤儀表板 Banner

在 app.py 最上方顯示全市場級指標，所有模式共用（個股/選股/AI報告）。

內容（4 欄）：
  C1: 加權指數（月線/季線乖離+KD）+ 台指期(全)夜盤 gap + 基差/PCR + M1B + 期權避險
  C2: 台灣 FGI + 子指標
  C3: S&P 500 + 那斯達克 + 費城半導體（各含乖離/KD）+ VIX 期限結構
  C4: CNN FGI + 子指標 + 歷史

資料源：
  - 大盤 OHLCV：yfinance（^TWII / ^GSPC / ^IXIC / ^SOX）
  - 台指期(全)：taifex_data.TAIFEXData.get_full_session_quote（同 macro_dashboard 領頭羊）
  - 台灣 FGI：taifex_data.TaiwanFearGreedIndex
  - CNN FGI：cnn_fear_greed.CNNFearGreedIndex
  - 期貨/選擇權：taifex_data.TAIFEXData

快取策略（per-indicator，依 reference_banner_cache_timing）：
  - 台股 14:00 類（tw_index/basis/pcr）：抓到 today → 快取到隔天 14:00；
    未抓到 → 5 分鐘 retry
  - 台灣 FGI（20:00 融資）：抓到 today → 快取到隔天 20:00；
    未抓到 → 30 分鐘 retry
  - CNN FGI：美股盤中 1h / 閉市 4h
  - S&P 500：美股盤中 5 分鐘 / 閉市到下個開盤
"""

import json
import logging
import threading
import time
from datetime import datetime, date as ddate, time as dtime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
SENTIMENT_DIR = REPO / "data" / "sentiment"

# ============================================================
#  快取配置 (per-indicator)
# ============================================================

_CACHE_KEY = '_banner_cache_v2'

# 台股日盤後資料發布時間
_TW_14_CUTOFF = dtime(14, 0)      # 加權指數/漲跌家數/基差/PCR
_TW_1435_CUTOFF = dtime(14, 35)   # TAIFEX 三大法人 + 期權盤後 (atm_put / mtx_ratio / opt_inst)
_TW_20_CUTOFF = dtime(20, 0)      # 融資餘額
_TW_14_RETRY = 300                 # 5 分鐘
_TW_1435_RETRY = 1800              # 30 分鐘 (parquet 是 archiver 寫的, 不必狂 retry)
_TW_20_RETRY = 1800                # 30 分鐘

# 美股盤中 TW 時區 (粗估 ET 夏令時 21:30 TW - 04:00 TW，冬令 22:30 - 05:00)
# 為簡單起見採 22:30 - 04:00，實際會因 DST 略有 1 小時誤差，不影響 TTL 策略
_US_OPEN_TW = dtime(22, 30)
_US_CLOSE_TW = dtime(4, 0)

_CNN_INTRADAY_TTL = 3600           # 1h
_CNN_CLOSED_TTL = 4 * 3600         # 4h

_US_INDEX_INTRADAY_TTL = 300       # 5 分鐘


# ============================================================
#  交易日 / 時區 helpers
# ============================================================

def _now_tw():
    """Server 運行於 TW 時區，直接回傳 local time。"""
    return datetime.now()


def _is_tw_trading_day(d):
    """粗判：週一至週五。國定假日不處理（會在隔日被 last_trading_day 覆蓋）。"""
    return d.weekday() < 5


def _last_tw_trading_day_on_or_before(d):
    """回傳 <= d 的最近交易日。"""
    while not _is_tw_trading_day(d):
        d -= timedelta(days=1)
    return d


def _next_tw_trading_day_after(d):
    """回傳 > d 的下一個交易日。"""
    d = d + timedelta(days=1)
    while not _is_tw_trading_day(d):
        d += timedelta(days=1)
    return d


def _expected_tw_date(cutoff, now=None):
    """
    依 cutoff 時間判斷目前應該要拿到的資料日期：
      - 今天是交易日 且 現在時間 >= cutoff → expected = today
      - 否則 → expected = 上一個交易日
    """
    now = now or _now_tw()
    today = now.date()
    if _is_tw_trading_day(today) and now.time() >= cutoff:
        return today
    # cutoff 前 或 非交易日 → 上一交易日
    return _last_tw_trading_day_on_or_before(today - timedelta(days=1))


def _next_tw_refresh_at(cutoff, now=None):
    """
    下一個應該 refresh 的 datetime：
      - 今日是交易日 且 現在 < cutoff → 今天 cutoff（今天發布時間還沒到）
      - 否則 → 下個交易日 cutoff
    """
    now = now or _now_tw()
    today = now.date()
    if _is_tw_trading_day(today) and now.time() < cutoff:
        return datetime.combine(today, cutoff)
    next_day = _next_tw_trading_day_after(today)
    return datetime.combine(next_day, cutoff)


def _is_us_market_hours(now=None):
    """
    粗估美股開盤時段 (TW 22:30 ~ 04:00)。
    DST 冬令時會晚 1 小時，這裡不嚴格區分，影響僅 TTL 粒度。
    """
    now = now or _now_tw()
    t = now.time()
    # 晚盤：22:30 之後到午夜
    if t >= _US_OPEN_TW:
        return _is_tw_trading_day(now.date())
    # 凌晨：0:00 ~ 04:00 → 視為「昨天」的美股盤中
    if t <= _US_CLOSE_TW:
        return _is_tw_trading_day(now.date() - timedelta(days=1))
    return False


def _next_us_open_at(now=None):
    """下一次美股開盤時間 (TW 22:30 on trading day)。"""
    now = now or _now_tw()
    today = now.date()
    # 若今日是交易日且現在 < 22:30 → 今晚 22:30
    if _is_tw_trading_day(today) and now.time() < _US_OPEN_TW:
        return datetime.combine(today, _US_OPEN_TW)
    # 否則找下一個交易日
    next_day = _next_tw_trading_day_after(today)
    return datetime.combine(next_day, _US_OPEN_TW)


# ============================================================
#  指數技術指標計算
# ============================================================

# 指數 last-good 落盤 (2026-06-10)：跨 app 重啟的 stale-OK — fetch 失敗/NaN 時
# 回上次成功值（帶舊 data_date 誠實呈現）而非空白。worker thread 並行寫同檔，加鎖。
_INDEX_LAST_GOOD_PATH = Path('data_cache') / 'banner_index_last_good.json'
_index_last_good_lock = threading.Lock()


def _save_index_last_good(ticker, result):
    try:
        with _index_last_good_lock:
            data = {}
            if _INDEX_LAST_GOOD_PATH.exists():
                try:
                    data = json.loads(_INDEX_LAST_GOOD_PATH.read_text(encoding='utf-8'))
                except Exception:
                    data = {}
            rec = dict(result)
            rec['data_date'] = str(rec['data_date']) if rec.get('data_date') else None
            rec['saved_at'] = time.time()
            data[ticker] = rec
            _INDEX_LAST_GOOD_PATH.parent.mkdir(parents=True, exist_ok=True)
            _INDEX_LAST_GOOD_PATH.write_text(json.dumps(data, ensure_ascii=False),
                                             encoding='utf-8')
    except Exception as e:
        logger.debug("banner index last-good save failed %s: %s", ticker, e)


def _load_index_last_good(ticker):
    try:
        with _index_last_good_lock:
            if not _INDEX_LAST_GOOD_PATH.exists():
                return None
            rec = json.loads(_INDEX_LAST_GOOD_PATH.read_text(encoding='utf-8')).get(ticker)
        if rec and rec.get('price') is not None:
            rec['stale'] = True  # _cache_set 看到 stale -> 短 TTL 持續重試補抓
            return rec
    except Exception as e:
        logger.debug("banner index last-good load failed %s: %s", ticker, e)
    return None


# 美股指數第二來源：FRED 官方 close-only 序列 (2026-06-10)。yfinance 幽靈 NaN/
# 整掛時補位；實測 api.stlouisfed.org JSON 帶 key 0.5s（歷史 timeout 的是無 key
# 的 fredgraph.csv 端點，勿混淆）。只有 Close → KD 算不出留 None。
_FRED_INDEX_SID = {'^GSPC': 'SP500', '^IXIC': 'NASDAQCOM'}


def _load_fred_key():
    try:
        env = Path(__file__).resolve().parent / 'local' / '.env'
        if env.exists():
            for line in env.read_text(encoding='utf-8').splitlines():
                if line.strip().startswith('FRED_API_KEY='):
                    return line.split('=', 1)[1].strip() or None
    except Exception:
        pass
    return None


def _fetch_index_close_fred(ticker):
    """回 pd.Series (DatetimeIndex, close) 或 None。僅支援 _FRED_INDEX_SID 內指數。"""
    sid = _FRED_INDEX_SID.get(ticker)
    if not sid:
        return None
    key = _load_fred_key()
    if not key:
        return None
    try:
        import requests
        start = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
        r = requests.get('https://api.stlouisfed.org/fred/series/observations',
                         params={'series_id': sid, 'api_key': key, 'file_type': 'json',
                                 'observation_start': start},
                         timeout=15)
        r.raise_for_status()
        obs = r.json().get('observations', [])
        s = pd.Series({o['date']: float(o['value'])
                       for o in obs if o.get('value') not in ('.', '', None)})
        if s.empty:
            return None
        s.index = pd.to_datetime(s.index)
        return s.sort_index()
    except Exception as e:
        logger.warning("FRED index fallback failed for %s: %s", ticker, e)
        return None


def _close_metrics(result, close):
    """從 Close 序列算 price/change/乖離/rv（KD 需 High/Low，不在此）。"""
    result['price'] = round(float(close.iloc[-1]), 2)
    if len(close) >= 2:
        prev = float(close.iloc[-2])
        if prev > 0:
            result['change_pct'] = round((close.iloc[-1] / prev - 1) * 100, 2)
    ma20 = close.rolling(20).mean().iloc[-1]
    if pd.notna(ma20) and ma20 > 0:
        result['ma20_bias'] = round((close.iloc[-1] / ma20 - 1) * 100, 2)
    ma60 = close.rolling(60).mean().iloc[-1]
    if pd.notna(ma60) and ma60 > 0:
        result['ma60_bias'] = round((close.iloc[-1] / ma60 - 1) * 100, 2)
    if len(close) >= 30:
        log_ret = np.log(close / close.shift(1))
        result['rv10'] = float(log_ret.iloc[-10:].std() * np.sqrt(252))
        result['rv30'] = float(log_ret.iloc[-30:].std() * np.sqrt(252))


def _finalize_index_result(ticker, result):
    """收口（所有 return 路徑必經，否則繞過回退鏈）：

    price 無效視同失敗 → 失敗先試 FRED 第二來源（^GSPC/^IXIC, close-only）
    → 仍失敗回 last-good stale → 成功值落盤。
    """
    try:
        if result.get('error') is None and (
                result.get('price') is None or pd.isna(result.get('price'))):
            result['error'] = 'price NaN/unavailable'
        if result.get('error') is not None:
            # 第二來源：FRED（網路級故障/空資料/NaN 全走這裡）。今日實案：yfinance
            # 6/9 bar 整根 NaN，FRED 反而有 6/9 收盤。
            close_f = _fetch_index_close_fred(ticker)
            if close_f is not None and len(close_f) >= 60:
                logger.warning("index %s yfinance failed (%s) -- FRED 第二來源補位 (%s)",
                               ticker, result.get('error'), close_f.index[-1].date())
                try:
                    result['data_date'] = close_f.index[-1].date()
                    _close_metrics(result, close_f)
                    result['k'] = None   # close-only 無 High/Low，KD 不可算
                    result['d'] = None
                    result['source'] = 'FRED'
                    result['error'] = None
                except Exception as e:
                    logger.warning("FRED metrics compute failed for %s: %s", ticker, e)
        if result.get('error') is None:
            _save_index_last_good(ticker, result)
        else:
            stale = _load_index_last_good(ticker)
            if stale is not None:
                logger.warning("index %s fetch degraded (%s) -- 回退 last-good (data_date=%s)",
                               ticker, result.get('error'), stale.get('data_date'))
                return stale
    except Exception as e:
        logger.debug("index last-good guard failed %s: %s", ticker, e)
    return result


def _fetch_index_metrics(ticker, name):
    """
    抓指數 OHLCV 並計算月線/季線乖離率 + KD + rv10/rv30（年化波動）。

    來源鏈：yfinance (OHLC 全指標) → FRED close-only (^GSPC/^IXIC, KD=None)
    → last-good 落盤 stale 回退。

    Returns
    -------
    dict: price, ma20_bias, ma60_bias, k, d, change_pct, data_date, rv10, rv30
    """
    result = {
        'name': name, 'price': None, 'change_pct': None,
        'ma20_bias': None, 'ma60_bias': None,
        'k': None, 'd': None, 'error': None, 'data_date': None,
        'rv10': None, 'rv30': None,
    }
    try:
        import yfinance as yf
        df = yf.Ticker(ticker).history(period='6mo')
        if not df.empty and 'Close' in df.columns:
            # yfinance 幽靈尾列防線 (Close=NaN, Volume 正常) — 6/2 全市場污染事件同病；
            # 不濾掉 price/KD 全變 nan，banner 顯示空值且 error=None 被當成功長快取
            df = df[df['Close'].notna()]
        if df.empty or len(df) < 60:
            result['error'] = 'data insufficient'
            return _finalize_index_result(ticker, result)  # 收口統一走 FRED -> stale

        close = df['Close']
        high = df['High']
        low = df['Low']

        # 資料日期（最後一根 K 的日期）
        try:
            result['data_date'] = df.index[-1].date()
        except Exception:
            pass

        # 現價/漲跌幅/乖離/rv（共用 close-only helper；KD 在下方需 High/Low）
        _close_metrics(result, close)

        # KD (9, 3, 3)
        n = 9
        low_n = low.rolling(n).min()
        high_n = high.rolling(n).max()
        rsv = (close - low_n) / (high_n - low_n) * 100
        rsv = rsv.fillna(50)
        # EMA-style smoothing for K and D
        k = rsv.ewm(com=2, adjust=False).mean()  # alpha=1/3
        d = k.ewm(com=2, adjust=False).mean()
        result['k'] = round(float(k.iloc[-1]), 1)
        result['d'] = round(float(d.iloc[-1]), 1)

        # 盤中 override (僅限台股加權指數)：用 mis.twse 即時點位蓋掉 yfinance close。
        # yfinance ^TWII 在盤中 today bar 由 Yahoo 後端隨機生成，常常停在昨日收盤。
        # ma20/ma60/KD 維持用 yfinance daily close 算（盤中一根 partial bar 不影響）。
        # change_pct 改用 mis.twse prev_close 算（避免 yfinance 昨日 vs 前日的時序錯位）。
        if ticker == '^TWII':
            try:
                from mis_twse_client import is_tw_trading_hours, get_quote
                if is_tw_trading_hours():
                    q = get_quote('t00')
                    if q is not None:
                        result['price'] = round(q['price'], 2)
                        prev = q.get('prev_close')
                        if prev and prev > 0:
                            result['change_pct'] = round((q['price'] / prev - 1) * 100, 2)
                        result['intraday_source'] = 'mis.twse'
                        result['intraday_time'] = q.get('time')
                        # 盤中 partial bar: data_date 維持 yfinance 那筆 (= 上交易日)，
                        # 讓 _compute_expiry 走 5min retry path 而非「cache 到隔天 14:00」
                        logger.debug("^TWII intraday override: %s @ %s", q['price'], q.get('time'))
            except Exception as e:
                logger.debug("mis.twse ^TWII override failed: %s", e)

    except Exception as e:
        logger.warning("Failed to fetch index %s: %s", ticker, e)
        result['error'] = str(e)

    return _finalize_index_result(ticker, result)


# ============================================================
#  Per-indicator 快取
# ============================================================

def _get_cache():
    """取得 session_state 中的快取字典。"""
    if _CACHE_KEY not in st.session_state:
        st.session_state[_CACHE_KEY] = {}
    return st.session_state[_CACHE_KEY]


def _cache_get(indicator):
    """若 cache 仍有效 → 回傳 value；否則 None。"""
    entry = _get_cache().get(indicator)
    if not entry:
        return None
    if time.time() < entry.get('expires_at', 0):
        return entry.get('value')
    return None


def _cache_set(indicator, value, data_date, now=None):
    """寫入快取並依 indicator 規則計算 expires_at。

    失敗/stale 結果不得長快取 (2026-06-10)：原本 us_index 閉市時 TTL=到下次開盤，
    一次 fetch 失敗/NaN = 空值卡整個下午。error/None -> 5 分鐘重試；stale 回退值
    -> 30 分鐘（值大致正確但持續嘗試補抓新鮮值）。
    """
    now = now or _now_tw()
    if value is None or (isinstance(value, dict) and value.get('error')):
        expires_at = time.time() + 300
    elif isinstance(value, dict) and value.get('stale'):
        expires_at = time.time() + 1800
    else:
        expires_at = _compute_expiry(indicator, data_date, now)
    _get_cache()[indicator] = {
        'value': value,
        'fetched_at': time.time(),
        'expires_at': expires_at,
        'data_date': data_date,
    }
    logger.debug(
        "banner cache set: %s data_date=%s expires_in=%.0fs",
        indicator, data_date, expires_at - time.time(),
    )


def _compute_expiry(indicator, data_date, now):
    """
    依指標型別計算 expires_at (epoch secs)。

    規則：
      - tw_index/basis/pcr (14:00)：data_date==expected → 下個交易日 14:00；
        否則 → 5 分鐘後重試
      - tw_fgi (20:00 融資)：data_date==expected → 下個交易日 20:00；
        否則 → 30 分鐘後重試
      - us_index：盤中 5 分鐘；閉市 → 下次開盤
      - cnn_fgi：盤中 1h；閉市 4h
      - risk_score / regime: 跟 tw_index 同步刷新
    """
    # 台股 14:00 類
    if indicator in ('tw_index', 'basis', 'pcr', 'm1b_ratio', 'risk_score', 'regime', 'regime_ext'):
        expected = _expected_tw_date(_TW_14_CUTOFF, now)
        if data_date == expected:
            return _next_tw_refresh_at(_TW_14_CUTOFF, now).timestamp()
        return time.time() + _TW_14_RETRY

    # 14:35 TAIFEX 盤後類 (atm_put / mtx_ratio / opt_inst)
    # archiver 寫 parquet, banner 純 disk read; 命中 today -> 隔日 14:35
    if indicator in ('atm_put', 'mtx_ratio', 'opt_inst'):
        expected = _expected_tw_date(_TW_1435_CUTOFF, now)
        if data_date == expected:
            return _next_tw_refresh_at(_TW_1435_CUTOFF, now).timestamp()
        return time.time() + _TW_1435_RETRY

    # 台灣 FGI (20:00 融資為主)
    if indicator == 'tw_fgi':
        expected = _expected_tw_date(_TW_20_CUTOFF, now)
        if data_date == expected:
            return _next_tw_refresh_at(_TW_20_CUTOFF, now).timestamp()
        return time.time() + _TW_20_RETRY

    # 美股指數 (S&P / 那斯達克 / 費半 同規則：盤中 5 分鐘 / 閉市到下個開盤)
    if indicator in ('us_index', 'nasdaq_index', 'sox_index'):
        if _is_us_market_hours(now):
            return time.time() + _US_INDEX_INTRADAY_TTL
        return _next_us_open_at(now).timestamp()

    # 台指期(全)：夜盤 05:00 收 / 日盤 13:45 結算，quote 變動點分散
    # → 固定 30 分鐘 (1 次 TAIFEX request，與 macro_dashboard ttl=300 同源不衝突)
    if indicator == 'txf_full':
        return time.time() + 1800

    # CNN FGI
    if indicator == 'cnn_fgi':
        if _is_us_market_hours(now):
            return time.time() + _CNN_INTRADAY_TTL
        return time.time() + _CNN_CLOSED_TTL

    # 預設
    return time.time() + 300


# ============================================================
#  Pure fetch (thread-safe, no session_state access)
# ============================================================

def _read_sentiment_parquet(name: str) -> dict | None:
    """讀 data/sentiment/<name>.parquet 最後一筆轉 dict.

    archiver (run_taifex_signals_afterclose.bat 14:35 / scanner 00:00) 寫，
    banner 純讀，零 network。data_date 字串 'YYYY-MM-DD' 還原為 date 物件
    供 _compute_expiry 比對。檔案不存在 / 為空 -> None.
    """
    p = SENTIMENT_DIR / f"{name}.parquet"
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        logger.warning("Read sentiment parquet %s failed: %s", name, e)
        return None
    if df.empty:
        return None
    row = df.iloc[-1].to_dict()
    d = row.get('data_date')
    if isinstance(d, str) and d:
        try:
            row['data_date'] = ddate.fromisoformat(d)
        except ValueError:
            pass
    return row


def _read_tw_fgi_parquet() -> dict | None:
    """讀 data/sentiment/tw_fgi_history.parquet 最後一筆 + 還原 components dict.

    Schema 來自 tools/archive_tw_fgi.py: 平面 score/label/data_date + components_json.
    Renderer 期望 nested {score, label, components:{...}, data_date}.
    """
    p = SENTIMENT_DIR / "tw_fgi_history.parquet"
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        logger.warning("Read tw_fgi parquet failed: %s", e)
        return None
    if df.empty:
        return None
    row = df.iloc[-1].to_dict()

    import json
    try:
        components = json.loads(row.get('components_json') or '{}')
    except Exception:
        components = {}

    data_date = None
    raw = row.get('data_date')
    if isinstance(raw, str) and raw:
        try:
            data_date = ddate.fromisoformat(raw)
        except ValueError:
            pass

    return {
        'score': row.get('score'),
        'label': row.get('label', ''),
        'components': components,
        'data_date': data_date,
    }


def _read_m1b_ratio_parquet() -> dict | None:
    """讀 data/sentiment/m1b_ratio_history.parquet 最後一筆.

    Schema 來自 tools/archive_m1b_ratio.py: 純 flat，跟 compute_m1b_ratio 回傳對齊。
    """
    p = SENTIMENT_DIR / "m1b_ratio_history.parquet"
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        logger.warning("Read m1b_ratio parquet failed: %s", e)
        return None
    if df.empty:
        return None
    row = df.iloc[-1].to_dict()

    data_date = None
    raw = row.get('data_date')
    if isinstance(raw, str) and raw:
        try:
            data_date = ddate.fromisoformat(raw)
        except ValueError:
            pass

    return {
        'ratio_pct': row.get('ratio_pct'),
        'm1b_period': row.get('m1b_period', ''),
        'm1b_mil_twd': row.get('m1b_mil_twd'),
        'trading_value_twd': row.get('trading_value_twd'),
        'n_days': int(row.get('n_days') or 0),
        'end_date': data_date,
        'data_date': data_date,
        'label': row.get('label', ''),
        'color': row.get('color', ''),
    }


def _read_risk_score_parquet() -> dict | None:
    """讀 data/sentiment/risk_score_history.parquet 最後一筆 + reconstruct nested dict.

    Schema 來自 tools/archive_risk_score.py 的 _flatten_for_parquet：
    flat composite/zone/zone_xxx/<sig>_value/<sig>_rank + breakdown_json (full breakdown dict).
    Renderer (_render_risk_row) 期望 nested {composite, zone, breakdown:{...}, zone_stats:{...}}.
    """
    p = SENTIMENT_DIR / "risk_score_history.parquet"
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        logger.warning("Read risk_score parquet failed: %s", e)
        return None
    if df.empty:
        return None
    row = df.iloc[-1].to_dict()

    import json
    try:
        breakdown = json.loads(row.get('breakdown_json') or '{}')
    except Exception:
        breakdown = {}

    # date 還原（archiver 存 pd.Timestamp）
    data_date = None
    raw_date = row.get('date')
    if raw_date is not None:
        try:
            data_date = pd.to_datetime(raw_date).date()
        except Exception:
            pass

    return {
        'composite': row.get('composite'),
        'zone': row.get('zone'),
        'zone_color': row.get('zone_color'),
        'breakdown': breakdown,
        'total_weight_used': row.get('total_weight_used'),
        'baseline_10pct': row.get('baseline_10pct'),
        'zone_stats': {
            'co10': row.get('zone_co10'),
            'co5': row.get('zone_co5'),
            'mdd_median': row.get('zone_mdd_median'),
            'ann_days': row.get('zone_ann_days'),
        },
        'data_date': data_date,
    }


# Banner 輸出 key -> session_state 快取 key
_OUT_TO_CACHE = {
    'tw': 'tw_index',
    'us': 'us_index',
    'nasdaq': 'nasdaq_index',
    'sox': 'sox_index',
    'txf_full': 'txf_full',
    'tw_fgi': 'tw_fgi',
    'cnn_fgi': 'cnn_fgi',
    'basis': 'basis',
    'pcr': 'pcr',
    'm1b_ratio': 'm1b_ratio',
    'atm_put': 'atm_put',
    'mtx_ratio': 'mtx_ratio',
    'opt_inst': 'opt_inst',
    'risk_score': 'risk_score',
    'regime': 'regime',
    'regime_ext': 'regime_ext',
}


def _pure_fetch(cache_key):
    """純抓取，不讀寫 session_state —— worker thread safe。

    Streamlit session_state 不支援 background thread 存取，
    並行抓資料時必須用純 I/O 函式，cache 讀寫留給主執行緒。
    """
    try:
        if cache_key == 'tw_index':
            return _fetch_index_metrics('^TWII', '加權指數')
        if cache_key == 'us_index':
            return _fetch_index_metrics('^GSPC', 'S&P 500')
        if cache_key == 'nasdaq_index':
            return _fetch_index_metrics('^IXIC', '那斯達克')
        if cache_key == 'sox_index':
            return _fetch_index_metrics('^SOX', '費城半導體')
        if cache_key == 'txf_full':
            # 台指期(全) 日盤結算 + 夜盤收盤 (live TAIFEX)。
            # 同 macro_dashboard 領頭羊區塊資料源；夜盤 gap 預示次日開盤跳空。
            from taifex_data import TAIFEXData
            return TAIFEXData().get_full_session_quote()
        if cache_key == 'tw_fgi':
            # 讀 archive parquet（archiver = run_taifex_signals_afterclose.bat
            # 第 5 stage / scanner 00:00 後 stage）。零 network，cold load <50ms。
            # 5 子分數 (momentum/breadth/PCR/vol/margin) 全日頻收盤後算，
            # 盤中重算只是用昨日值再壓一次無意義（margin 20:00 才更新）。
            return _read_tw_fgi_parquet()
        if cache_key == 'cnn_fgi':
            from cnn_fear_greed import CNNFearGreedIndex
            return CNNFearGreedIndex().get_index()
        if cache_key == 'basis':
            from taifex_data import TAIFEXData
            return TAIFEXData().get_futures_basis()
        if cache_key == 'pcr':
            from taifex_data import TAIFEXData
            return TAIFEXData().get_put_call_ratio()
        if cache_key == 'm1b_ratio':
            # 讀 archive parquet。M1B 央行月底發布 + FMTQIK 收盤後固定，
            # 盤中重算意義為 0；archiver 跑 1 次省 3.1s cold load。
            return _read_m1b_ratio_parquet()
        # 期權避險訊號改讀 archive parquet (archiver TUE-SAT 14:35 + scanner 00:00 寫)
        # 避免 banner intraday 反覆打 TAIFEX 浪費請求
        if cache_key == 'atm_put':
            return _read_sentiment_parquet('atm_put_premium')
        if cache_key == 'mtx_ratio':
            return _read_sentiment_parquet('minifutures_ratio')
        if cache_key == 'opt_inst':
            return _read_sentiment_parquet('options_institutional')
        if cache_key == 'risk_score':
            # 讀 archive parquet（archiver = run_taifex_signals_afterclose.bat
            # 第 5 stage / scanner 00:00 後 stage）。零 network，cold load <50ms。
            # 6 訊號全日頻收盤後算，盤中重算只是用昨日值再壓一次無意義。
            return _read_risk_score_parquet()
        if cache_key == 'regime':
            return _fetch_regime()
        if cache_key == 'regime_ext':
            return _fetch_regime_extension()
    except Exception as e:
        logger.debug("Banner fetch %s failed: %s", cache_key, e)
    return None


def _fetch_regime():
    """Read latest HMM regime from data/tracking/regime_log.jsonl.

    Returns dict with regime/regime_label/evidence/data_date OR None.
    """
    log_path = REPO / 'data' / 'tracking' / 'regime_log.jsonl'
    if not log_path.exists():
        return None
    try:
        import json
        last = None
        with log_path.open('r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    last = line
        if not last:
            return None
        rec = json.loads(last)
        regime = rec.get('regime', 'unknown')
        # VF-G4 evidence: only_volatile Sharpe 0.208 vs baseline 0.117
        evidence_map = {
            'volatile': '歷史此狀態下 entry-side Sharpe 0.208（baseline 0.117）',
            'trending': '歷史此狀態下 11/11 年負報酬偏多 (-0.46%/年)',
            'ranging': '歷史此狀態下 mean fwd_20d +0.75%',
            'neutral': '歷史此狀態下 mean fwd_20d +0.54%',
        }
        # Color
        color_map = {
            'volatile': '#FF8800',  # orange (entry alpha 但 vol 高)
            'trending': '#FF4444',  # red (歷史虧損偏多)
            'ranging': '#FFD700',   # yellow
            'neutral': '#888888',   # gray
        }
        date_str = rec.get('date')
        try:
            data_date = ddate.fromisoformat(date_str) if date_str else None
        except Exception:
            data_date = None
        return {
            'regime': regime,
            'regime_label': {'volatile': '震盪', 'trending': '趨勢', 'ranging': '盤整', 'neutral': '中性'}.get(regime, regime),
            'evidence': evidence_map.get(regime, ''),
            'color': color_map.get(regime, '#888888'),
            'data_date': data_date,
        }
    except Exception as e:
        logger.debug("regime fetch failed: %s", e)
        return None


def _fetch_regime_extension():
    """Read TAIEX, compute today's ma_dist_60 rolling 252d rank.

    Returns dict with rank/level/color/data_date OR None.
    Source: regime_extension.compute_extension_signal (TAIEX 1999+).
    """
    try:
        import regime_extension as re_mod
        result = re_mod.compute_extension_signal()
        if result.get("rank") is None:
            return None
        # data_date already a Timestamp; normalize to date for cache compare
        dd = result.get("data_date")
        if hasattr(dd, "date"):
            result["data_date"] = dd.date()
        return result
    except Exception as e:
        logger.debug("regime_extension fetch failed: %s", e)
        return None


# ============================================================
#  主入口
# ============================================================

@st.cache_data(ttl=120, show_spinner=False)
def _get_banner_data():
    """取得 banner 所有資料，cache miss 部分並行抓取。

    流程：
      1. 主執行緒讀快取（session_state）→ 分離已命中 vs 待抓
      2. 未命中 indicators 用 ThreadPoolExecutor 並行抓（I/O bound，max=7）
      3. 主執行緒寫快取 + 組合結果

    外層 @st.cache_data(ttl=120) 加速同 session 重複 render (切 tab / rerun)。
    底層 per-indicator 的 session_state 快取是 disk-like TTL 層。
    """
    from concurrent.futures import ThreadPoolExecutor

    now = _now_tw()

    # 1. 主執行緒讀快取
    results = {}
    to_fetch = []
    for out_key, cache_key in _OUT_TO_CACHE.items():
        cached = _cache_get(cache_key)
        if cached is not None:
            results[out_key] = cached
        else:
            to_fetch.append((out_key, cache_key))

    if not to_fetch:
        return results

    # 2. 並行抓取 cache miss 的 indicators
    fetched = []
    with ThreadPoolExecutor(max_workers=len(to_fetch)) as pool:
        future_map = {
            pool.submit(_pure_fetch, cache_key): (out_key, cache_key)
            for out_key, cache_key in to_fetch
        }
        for fut in future_map:
            out_key, cache_key = future_map[fut]
            try:
                value = fut.result(timeout=15)
            except Exception as e:
                logger.debug("Banner worker %s timeout/error: %s", out_key, e)
                value = None
            fetched.append((out_key, cache_key, value))

    # 3. 主執行緒寫快取 + 組合結果
    for out_key, cache_key, value in fetched:
        results[out_key] = value
        if value is not None:
            data_date = value.get('data_date') if isinstance(value, dict) else None
            _cache_set(cache_key, value, data_date, now)

    return results


# ============================================================
#  UI 渲染
# ============================================================

def _fgi_color(score):
    """FGI 分數 → 顏色。"""
    if score is None:
        return 'gray'
    if score < 25:
        return '#FF4444'
    if score < 40:
        return '#FF8800'
    if score < 60:
        return '#FFD700'
    if score < 75:
        return '#88CC00'
    return '#00CC44'


def _bias_delta_color(bias):
    """乖離率正負 → delta 字串。"""
    if bias is None:
        return "N/A", "off"
    return f"{bias:+.2f}%", "normal"


def _render_risk_row(risk, regime, regime_ext=None):
    """Render 綜合風險指標 + HMM 市場狀態 + 跌深 regime 延伸 row.

    SOP-14 informational tier 文案守則：
      - 禁: 「預警 / 預測 / 領先 / 即將 / 接下來會」
      - OK: 「同期重合率 / 歷史此狀態下 / 當前 readings 落在 / informational」
    """
    risk = risk or {}
    regime = regime or {}
    regime_ext = regime_ext or {}

    composite = risk.get('composite')
    zone = risk.get('zone', 'unknown')
    zone_color = risk.get('zone_color', '#888888')
    breakdown = risk.get('breakdown', {})
    zone_stats = risk.get('zone_stats', {})
    baseline = risk.get('baseline_10pct', 19.2)

    # 綜合風險 + HMM regime + 跌深延伸 三欄並排
    rc1, rc2, rc3 = st.columns([3, 2, 2])

    with rc1:
        if composite is not None:
            zone_emoji = {'green': '🟢', 'yellow': '🟡', 'orange': '🟠', 'unknown': '⚪'}.get(zone, '⚪')
            zone_label_zh = {'green': '綠', 'yellow': '黃', 'orange': '橘', 'unknown': '資料不足'}.get(zone, '資料不足')

            co10 = zone_stats.get('co10')
            co5 = zone_stats.get('co5')
            ann = zone_stats.get('ann_days')
            mdd = zone_stats.get('mdd_median')

            # SOP-14 文案：強調「同期重合率」+ baseline
            zone_text = (
                f'歷史此區間 60d 內 ≥10% 回檔同期重合率 <b>{co10:.0f}%</b> '
                f'（baseline {baseline:.0f}%；≥5% 為 {co5:.0f}%；MDD 中位數 {mdd:.0f}%；年化 {ann} 天）'
                if co10 is not None else '資料不足'
            )

            # breakdown bullets
            sig_short = {
                'm1b_ratio': 'M1B', 'rv10': 'RV10', 'rv30': 'RV30',
                'pcr_volume': 'PCR量', 'pcr_oi': 'PCR倉', 'fgi_score': 'FGI',
            }
            parts = []
            for sig, info in breakdown.items():
                rank = info.get('rank')
                weight = info.get('weight', 0.0)
                short = sig_short.get(sig, sig)
                if rank is None:
                    parts.append(f'<span style="color:#aaa">{short} N/A</span>')
                else:
                    rc = '#FF4444' if rank >= 85 else '#FF8800' if rank >= 65 else '#888888'
                    tip_w = f'weight {weight:.2f}'
                    parts.append(
                        f'<span title="{tip_w}">{short} '
                        f'<span style="color:{rc};font-weight:bold">{rank:.0f}</span></span>'
                    )
            breakdown_html = ' &nbsp;|&nbsp; '.join(parts)

            tip = ('SOP-14 informational tier — 此分數為「同期重合率」非預測機率；'
                   '禁用於 portfolio 自動調整；3 級 Orange ≥70.7 / Yellow ≥55.2 / Green <55.2；'
                   '6 訊號 lift-based weighted（v3 calibration 2002-2026 N=5906）')

            st.markdown(
                f'<div style="font-size:1.0rem;line-height:1.7" title="{tip}">'
                f'<b>綜合風險指標</b> '
                f'<span style="color:{zone_color};font-size:1.4rem;font-weight:bold">'
                f'{zone_emoji} {composite:.0f}</span> '
                f'<span style="color:{zone_color};font-weight:bold">{zone_label_zh}燈</span> '
                f'<span style="font-size:0.85rem;color:#666">— {zone_text}</span>'
                f'</div>'
                f'<div style="font-size:0.85rem;line-height:1.5;margin-top:4px">'
                f'{breakdown_html}'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="font-size:0.95rem;color:#888">'
                '綜合風險指標：資料不足（需 6 訊號中 ≥3 個有效）'
                '</div>',
                unsafe_allow_html=True,
            )

    with rc2:
        regime_label = regime.get('regime_label')
        regime_color = regime.get('color', '#888888')
        evidence = regime.get('evidence', '')
        if regime_label:
            tip2 = ('HMM regime（VF-G4 entry-gate 用），'
                    f'{evidence}；資料源 data/tracking/regime_log.jsonl')
            st.markdown(
                f'<div style="font-size:1.0rem;line-height:1.7" title="{tip2}">'
                f'<b>市場狀態 (HMM)</b> '
                f'<span style="color:{regime_color};font-size:1.2rem;font-weight:bold">'
                f'{regime_label}</span>'
                f'<span style="font-size:0.8rem;color:#666"> ({regime.get("regime", "")})</span>'
                f'</div>'
                f'<div style="font-size:0.8rem;color:#888;line-height:1.4">'
                f'{evidence}'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="font-size:0.9rem;color:#888">市場狀態 (HMM)：資料不足</div>',
                unsafe_allow_html=True,
            )

    with rc3:
        ext_rank = regime_ext.get('rank')
        ext_level = regime_ext.get('level', 'unknown')
        ext_color = regime_ext.get('color', '#888888')
        ext_stats = regime_ext.get('stats', {})
        ext_ma_dist = regime_ext.get('ma_dist_60')
        ext_baseline = regime_ext.get('baseline_10pct', 23.5)

        if ext_rank is not None:
            level_emoji = {'green': '🟢', 'yellow': '🟡', 'orange': '🟠', 'red': '🔴'}.get(ext_level, '⚪')
            level_label_zh = {'green': '正常', 'yellow': '注意', 'orange': '偏高', 'red': '極端'}.get(ext_level, '?')
            co10 = ext_stats.get('co10')
            mdd_med = ext_stats.get('mdd_median')
            tip3 = (
                f'跌深 regime 延伸訊號（System 2 Phase 2.5 D 政策；ma_dist_60 rolling 252d rank）。'
                f'歷史此 rank 區間 60d 內 ≥10% 回檔同期重合率 {co10:.0f}%（baseline {ext_baseline:.0f}%；'
                f'此區間 MDD 中位數 {mdd_med:.1f}%）。SOP-14 informational tier — 禁用於自動調倉。'
            )
            st.markdown(
                f'<div style="font-size:1.0rem;line-height:1.7" title="{tip3}">'
                f'<b>跌深延伸 (D)</b> '
                f'<span style="color:{ext_color};font-size:1.2rem;font-weight:bold">'
                f'{level_emoji} {ext_rank*100:.0f}</span>'
                f'<span style="font-size:0.8rem;color:#666"> ({level_label_zh})</span>'
                f'</div>'
                f'<div style="font-size:0.8rem;color:#888;line-height:1.4">'
                f'ma_dist_60 = {ext_ma_dist*100:+.1f}%；歷史 60d 內 ≥10% 回檔同期 {co10:.0f}%（baseline {ext_baseline:.0f}%）'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="font-size:0.9rem;color:#888">跌深延伸 (D)：資料不足</div>',
                unsafe_allow_html=True,
            )

    st.markdown('<hr style="margin:8px 0 12px;border:0;border-top:1px solid #eee">',
                unsafe_allow_html=True)


def _render_index_card(col, data):
    """在 st.column 內渲染一個指數卡片。"""
    name = data['name']
    price = data.get('price')
    change = data.get('change_pct')

    if price is None:
        col.metric(name, "N/A")
        return

    delta_str = f"{change:+.2f}%" if change is not None else None
    col.metric(name, f"{price:,.0f}" if price > 1000 else f"{price:,.2f}",
               delta=delta_str)

    # 乖離率 + KD — 用 markdown + HTML 顯眼呈現
    ma20 = data.get('ma20_bias')
    ma60 = data.get('ma60_bias')
    k = data.get('k')
    d = data.get('d')

    lines = []
    if ma20 is not None:
        c = '#FF4444' if ma20 < -5 else '#FF8800' if ma20 < 0 else '#00AA00' if ma20 < 5 else '#CC0000'
        lines.append(f'月線乖離 <span style="color:{c};font-weight:bold">{ma20:+.2f}%</span>')
    if ma60 is not None:
        c = '#FF4444' if ma60 < -5 else '#FF8800' if ma60 < 0 else '#00AA00' if ma60 < 5 else '#CC0000'
        lines.append(f'季線乖離 <span style="color:{c};font-weight:bold">{ma60:+.2f}%</span>')
    if k is not None and d is not None:
        kd_c = '#FF4444' if k < 20 else '#CC0000' if k > 80 else '#333333'
        lines.append(f'KD <span style="color:{kd_c};font-weight:bold">{k:.0f} / {d:.0f}</span>')
    if lines:
        col.markdown(
            '<div style="font-size:0.95rem;line-height:1.6">'
            + ' &nbsp;|&nbsp; '.join(lines)
            + '</div>',
            unsafe_allow_html=True,
        )


@st.fragment
def render_market_banner():
    """
    渲染大盤儀表板 Banner。在 app.py 主內容區頂端呼叫。
    使用 st.expander 包裝，預設展開。

    @st.fragment 使此 banner 成為獨立 rerun 單位，切 tab / 按鈕不會觸發
    主 page 全部 rerun（即使 cache 命中，Streamlit rerun 本身也有 overhead）。
    """
    with st.expander("📊 大盤儀表板", expanded=True):
        data = _get_banner_data()

        tw = data.get('tw', {})
        us = data.get('us', {})
        tw_fgi = data.get('tw_fgi') or {}
        cnn_fgi = data.get('cnn_fgi') or {}

        # ── Row 0: 綜合風險指標 + HMM 市場狀態 + 跌深延伸 D（informational tier, SOP-14） ──
        _render_risk_row(data.get('risk_score'), data.get('regime'), data.get('regime_ext'))

        # 單排 4 欄：所有內容垂直堆疊在各欄內，無 Row 2 間距
        c1, c2, c3, c4 = st.columns(4)

        # ── C1: 加權指數 + 台指期(全)夜盤 + 乖離/KD + 基差&PCR 併排 ──
        _render_index_card(c1, tw)

        # 台指期(全) 夜盤收盤 + 隔夜 gap（同 macro_dashboard 領頭羊資料源）
        txf = data.get('txf_full') or {}
        if txf.get('night_close'):
            n_chg = txf.get('night_chg_pct')
            t_color = ('#888888' if n_chg is None
                       else '#00AA00' if n_chg > 0 else '#FF4444')
            chg_str = f" {n_chg:+.2f}%" if n_chg is not None else ""
            txf_tip = (f"台指期(全) 夜盤 15:00~次日 05:00（交易日 {txf.get('night_date', '')}）；"
                       f"漲跌基準=日盤結算 {txf.get('day_settle', 0):,.0f}（{txf.get('day_date', '')}）"
                       f"→ 隔夜 gap 預示次一交易日開盤跳空方向")
            c1.markdown(
                f'<div style="font-size:0.95rem;line-height:1.6" title="{txf_tip}">'
                f'台指期(全)夜盤 <span style="color:{t_color};font-weight:bold">'
                f'{txf["night_close"]:,.0f}{chg_str}</span> '
                f'<span style="color:#888;font-size:0.85em">vs 日盤結算</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        # 期貨基差 + P/C Ratio 用 HTML 併排顯示
        basis = data.get('basis') or {}
        b_val = basis.get('basis')
        pcr = data.get('pcr') or {}
        pc = pcr.get('pc_ratio')

        parts = []
        if b_val is not None:
            b_color = '#00AA00' if b_val > 0 else '#FF4444'
            b_label = '正價差' if b_val > 0 else '逆價差'
            parts.append(f'基差 <span style="color:{b_color};font-weight:bold">'
                         f'{b_val:.0f}點 {b_label}</span>')
        if pc is not None:
            pc_pct = pc * 100
            pc_color = '#FF4444' if pc > 1.0 else '#00AA00' if pc < 0.7 else '#888888'
            parts.append(f'PCR <span style="color:{pc_color};font-weight:bold">'
                         f'{pc_pct:.0f}%</span>')
        if parts:
            c1.markdown(
                '<div style="font-size:0.95rem;line-height:1.6">'
                + ' &nbsp;|&nbsp; '.join(parts) + '</div>',
                unsafe_allow_html=True,
            )

        # 成交量 / M1B 比 (過熱指標)
        m1b = data.get('m1b_ratio') or {}
        r = m1b.get('ratio_pct')
        if r is not None:
            color = m1b.get('color', '#333')
            label = m1b.get('label', '')
            m_period = m1b.get('m1b_period', '')
            # 格式化 YYYYMM → YYYY/MM
            if len(m_period) == 6:
                m_period = f"{m_period[:4]}/{m_period[4:]}"
            tip = (f"近 {m1b.get('n_days', 20)} 交易日成交金額 / M1B × 100%；"
                   f"M1B={m1b.get('m1b_mil_twd', 0)/1e6:.1f}兆 ({m_period})")
            c1.markdown(
                f'<div style="font-size:0.95rem;line-height:1.6" title="{tip}">'
                f'成交量/M1B <span style="color:{color};font-weight:bold">'
                f'{r:.1f}% {label}</span> '
                f'<span style="color:#888;font-size:0.85em">(M1B {m_period})</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        # ── 期權避險訊號 (ATM PUT 成本 / MTX/TXF / 法人 PCskew) ──
        # baseline 累積中 (≥30 交易日才能 z-score)，目前先顯 raw + 解讀色
        atm = data.get('atm_put') or {}
        mtx = data.get('mtx_ratio') or {}
        opt_inst = data.get('opt_inst') or {}

        sig_parts = []
        atm_pct = atm.get('atm_put_pct')
        if atm_pct:
            atm_color = '#FF4444' if atm_pct > 1.5 else '#FF8800' if atm_pct > 1.0 else '#888888'
            atm_tip = (f"近月 ATM 賣權權利金 / 現貨 × 100%；"
                       f"strike={atm.get('atm_strike', 0)} close={atm.get('atm_put_close', 0)} "
                       f"skew={atm.get('put_skew', 0):.2f}（OTM5/ATM）")
            sig_parts.append(
                f'<span title="{atm_tip}">避險成本 '
                f'<span style="color:{atm_color};font-weight:bold">{atm_pct:.2f}%</span></span>'
            )
        mtx_r = mtx.get('mtx_txf_ratio')
        if mtx_r:
            mtx_color = '#FF4444' if mtx_r > 1.0 else '#00AA00' if mtx_r < 0.5 else '#888888'
            mtx_tip = (f"小台 (散戶為主) / 大台 (法人為主) 近月 OI 比；"
                       f"高 = 散戶倉位過大反向訊號；MTX_OI={mtx.get('mtx_oi', 0):,} "
                       f"TXF_OI={mtx.get('txf_oi', 0):,}")
            sig_parts.append(
                f'<span title="{mtx_tip}">小/大台 OI '
                f'<span style="color:{mtx_color};font-weight:bold">{mtx_r:.2f}</span></span>'
            )
        skew = opt_inst.get('inst_pc_oi_skew')
        if skew is not None and (
            opt_inst.get('foreign_call_net') or opt_inst.get('foreign_put_net')
            or opt_inst.get('dealer_call_net')
        ):
            skew_color = '#FF4444' if skew > 5000 else '#FF8800' if skew > 0 else '#00AA00'
            skew_label = '偏空避險' if skew > 0 else '偏多'
            skew_tip = (f"三大法人 TXO 賣權淨多 - 買權淨多 OI；>0 = 法人增加 PUT 避險；"
                        f"foreign C/P={opt_inst.get('foreign_call_net', 0)}/"
                        f"{opt_inst.get('foreign_put_net', 0)} "
                        f"dealer C/P={opt_inst.get('dealer_call_net', 0)}/"
                        f"{opt_inst.get('dealer_put_net', 0)}")
            sig_parts.append(
                f'<span title="{skew_tip}">法人PCskew '
                f'<span style="color:{skew_color};font-weight:bold">{skew:+,d} {skew_label}</span></span>'
            )
        if sig_parts:
            c1.markdown(
                '<div style="font-size:0.9rem;line-height:1.5">'
                + ' &nbsp;|&nbsp; '.join(sig_parts) + '</div>',
                unsafe_allow_html=True,
            )

        # ── C2: 台灣 FGI + 進度條 + 子指標表格 ──
        tw_score = tw_fgi.get('score')
        tw_label = tw_fgi.get('label', '')
        if tw_score is not None:
            c2.metric("台灣 FGI", f"{tw_score:.0f}", delta=tw_label)
            c2.progress(int(min(max(tw_score, 0), 100)))
        else:
            c2.metric("台灣 FGI", "N/A")

        components = tw_fgi.get('components', {})
        if components:
            comp_data = []
            for name, val in components.items():
                if not isinstance(val, dict):
                    continue
                score = val.get('score')
                status = ("恐懼" if score is not None and score < 40
                          else "貪婪" if score is not None and score > 60
                          else "中性") if score is not None else "N/A"
                # 顯示實際數值而非分數
                if name == 'market_momentum':
                    cur = val.get('current', 0)
                    hi = val.get('high_52w', 0)
                    pct = (cur / hi - 1) * 100 if hi > 0 else 0
                    actual = f"距52週高 {pct:+.1f}%"
                elif name == 'market_breadth':
                    adv = val.get('advances', 0)
                    dec = val.get('declines', 0)
                    actual = f"{adv} 漲 / {dec} 跌"
                elif name == 'put_call_ratio':
                    actual = f"{val.get('pc_ratio', 0) * 100:.0f}%"
                elif name == 'volatility':
                    actual = f"{val.get('volatility_20d', 0):.1f}%"
                elif name == 'margin_balance':
                    # FGI 融資分項衡量的是「融資餘額日變化率」(增=貪婪/減=恐懼)，狀態即由此算。
                    # 數值顯示變化率本身 (與狀態一致)；絕對水位「融資佔市值比」另列下方一行
                    # (走 build_market_cap_panel 官方金額/上市總市值，不用粗估的 指數x15)。
                    cr = val.get('change_rate_pct')
                    actual = f"日 {cr:+.2f}%" if cr is not None else "N/A"
                else:
                    actual = f"{score:.0f}" if score is not None else "N/A"
                label_map = {
                    'market_momentum': '市場動能',
                    'market_breadth': '漲跌家數',
                    'put_call_ratio': 'Put/Call比',
                    'volatility': '波動率',
                    'margin_balance': '融資餘額',
                }
                comp_data.append({"指標": label_map.get(name, name),
                                  "數值": actual, "狀態": status})
            # 附加一行：融資餘額佔上市總市值 (官方 MI_MARGN 融資金額 / 上市總市值,
            # build_market_cap_panel.py)。與上方 FGI「融資餘額」分項(日變化率)互補：
            # 這行是絕對槓桿水位 + 252d z 偏離 (informational, 非 FGI 計分項)。
            try:
                _mc = REPO / "data" / "macro" / "market_cap.parquet"
                if _mc.exists():
                    _df = pd.read_parquet(_mc).dropna(subset=['margin_to_mktcap_pct'])
                    if not _df.empty:
                        _r = _df.iloc[-1]
                        _z = _r.get('margin_mktcap_z_252d')
                        _st = ("偏高" if pd.notna(_z) and _z > 1.5
                               else "偏低" if pd.notna(_z) and _z < -1.5 else "中性")
                        comp_data.append({"指標": "融資/上市總市值",
                                          "數值": f"{_r['margin_to_mktcap_pct']:.2f}%",
                                          "狀態": _st})
            except Exception:
                pass
            if comp_data:
                c2.table(pd.DataFrame(comp_data))

        # ── C3: S&P 500 + 那斯達克 + 費城半導體 (各含乖離/KD) ──
        _render_index_card(c3, us)
        nasdaq = data.get('nasdaq')
        if nasdaq:
            _render_index_card(c3, nasdaq)
        sox = data.get('sox')
        if sox:
            _render_index_card(c3, sox)

        # ── VIX 期限結構 (vol_complex archiver, IC 4.06x lift @ red) ──
        try:
            vc_path = SENTIMENT_DIR / "vol_complex_history.parquet"
            if vc_path.exists():
                vc_df = pd.read_parquet(vc_path)
                if not vc_df.empty:
                    latest = vc_df.iloc[-1]
                    ratio = float(latest['vix_vix3m_ratio'])
                    light = str(latest['vix_vix3m_ratio_light'])
                    vix_v = float(latest['vix'])
                    vix3m_v = float(latest['vix3m'])
                    vc_date = pd.Timestamp(latest['date']).strftime('%m/%d')

                    color_map = {'green': '#00AA00', 'yellow': '#FFD700',
                                 'orange': '#FF8800', 'red': '#FF4444'}
                    label_map = {'green': '深度 contango (正常)',
                                 'yellow': '期限平坦化',
                                 'orange': 'backwardation 恐慌',
                                 'red': '急性恐慌期'}
                    color = color_map.get(light, '#888')
                    label = label_map.get(light, '')
                    tip = (f"^VIX {vix_v:.1f} / ^VIX3M {vix3m_v:.1f}（{vc_date}）；"
                           f">=1.00 backwardation；台股 IC 驗證 red 級 fwd 20d hit -10% 機率 27%（4.06x baseline）")
                    c3.markdown(
                        f'<div style="font-size:0.95rem;line-height:1.6" title="{tip}">'
                        f'VIX/VIX3M <span style="color:{color};font-weight:bold">'
                        f'{ratio:.3f} {label}</span> '
                        f'<span style="color:#888;font-size:0.85em">({vc_date})</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
        except Exception as e:
            logger.warning("VIX term tile render failed: %s", e)

        # ── C4: CNN FGI + 進度條 + 子指標表格 ──
        cnn_score = cnn_fgi.get('score')
        cnn_label = cnn_fgi.get('label', '')
        if cnn_score is not None:
            c4.metric("CNN FGI", f"{cnn_score:.0f}", delta=cnn_label)
            c4.progress(int(min(max(cnn_score, 0), 100)))
        else:
            c4.metric("CNN FGI", "N/A")

        cnn_components = cnn_fgi.get('components', {})
        if cnn_components:
            cnn_comp_data = []
            for name, val in cnn_components.items():
                c_score = val.get('score')
                c_rating = val.get('rating', 'N/A')
                if c_score is not None:
                    status = "恐懼" if c_score < 40 else "貪婪" if c_score > 60 else "中性"
                    cnn_comp_data.append({"指標": name, "數值": f"{c_score:.0f}",
                                          "狀態": status})
                else:
                    cnn_comp_data.append({"指標": name, "數值": "N/A",
                                          "狀態": c_rating})
            if cnn_comp_data:
                c4.table(pd.DataFrame(cnn_comp_data))

        # CNN FGI 歷史（從 C3 移入：歸 CNN 欄位，C3 騰給那斯達克/費半指數卡）
        if cnn_fgi:
            hist_data = []
            for key, label in [('previous_close', '前日收盤'), ('one_week_ago', '一週前'),
                               ('one_month_ago', '一月前'), ('one_year_ago', '一年前')]:
                val = cnn_fgi.get(key)
                if val is not None:
                    hist_data.append({"時間": label, "數值": f"{val:.0f}"})
            if hist_data:
                c4.markdown("**CNN FGI 歷史**")
                c4.table(pd.DataFrame(hist_data))
