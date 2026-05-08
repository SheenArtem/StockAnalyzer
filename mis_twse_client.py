"""mis_twse_client.py -- TWSE 即時報價 client (mis.twse.com.tw)

用途：
    盤中（09:00-13:30 週一至週五）拿台股單檔 / 加權指數的「即時」成交價。
    補 yfinance daily 在盤中不可靠（today bar 由 Yahoo 後端隨機生成）的痛點。

不是用來做：
    - 全市場 batch（Scanner 半夜跑，FinMind/yfinance 日線已足夠）
    - 高頻 polling（社群實測 5sec/3req 上限，超過 IP block 30min~數小時）
    - 收盤後資料（盤後 FinMind 才是 SoT）

風險：
    - mis.twse 沒 token，純 IP 粒度 rate limit
    - 證書缺 Subject Key Identifier，必須 verify=False（known issue, won't fix）
    - z='-' 表示該秒無撮合，fallback 到 pz (上次成交價)

Reference:
    https://zys-notes.blogspot.com/2020/01/api.html
    https://github.com/mlouielu/twstock/issues/39
"""
import json
import logging
import threading
import time
from datetime import datetime
from typing import Optional

import requests
import urllib3

logger = logging.getLogger(__name__)

# TWSE 憑證缺 SKI，已知問題（CLAUDE.md 列為 won't fix #3）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_BASE_URL = 'https://mis.twse.com.tw/stock/api/getStockInfo.jsp'
_REFERER = 'https://mis.twse.com.tw/stock/fibest.jsp?stock=2330'
_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)

# 社群實測上限 5sec/3req = 1.67s/req；保守取 3.0s
# (2.0s 實測會在 ban edge，部分 response 被截斷成 "HTTP 200 + truncated body")
_MIN_INTERVAL_SEC = 3.0
_REQ_TIMEOUT_SEC = 10

# 指數代碼用 tse_ 前綴，跟普通股同 endpoint
# t00 = 加權指數, t13 = 電子類, etc
_INDEX_IDS = {'t00', 't13', 't14', 't40', 't51', 'tx0'}


class MisTwseClient:
    """單例 client。建議用 module-level get_quote() 而非自行 new。"""

    def __init__(self):
        self._session = requests.Session()
        self._session.verify = False
        self._session.headers.update({
            'User-Agent': _USER_AGENT,
            'Referer': _REFERER,
            'Accept': 'application/json',
        })
        self._last_req_ts: float = 0.0
        self._lock = threading.Lock()
        # stock_id -> 'tse' | 'otc' | None (None = 兩端都試過, 都沒)
        self._prefix_cache: dict[str, Optional[str]] = {}
        self._cookie_initialized = False

    def _ensure_cookie(self):
        """首次呼叫前先 GET 首頁拿 session cookie (有些情境 mis.twse 會驗)。"""
        if self._cookie_initialized:
            return
        try:
            self._session.get(_REFERER, timeout=_REQ_TIMEOUT_SEC)
            self._cookie_initialized = True
        except requests.RequestException as e:
            logger.warning("mis.twse cookie init failed: %s", e)
            # 不阻擋，下游請求自己會失敗

    def _throttle(self):
        elapsed = time.time() - self._last_req_ts
        wait = _MIN_INTERVAL_SEC - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_req_ts = time.time()

    def _fetch(self, ex_ch: str) -> Optional[dict]:
        """打 API，回 raw msgArray[0] dict 或 None。"""
        with self._lock:
            self._ensure_cookie()
            self._throttle()
            try:
                r = self._session.get(
                    _BASE_URL,
                    params={'ex_ch': ex_ch, 'json': 1, 'delay': 0},
                    timeout=_REQ_TIMEOUT_SEC,
                )
            except requests.RequestException as e:
                logger.warning("mis.twse fetch %s failed: %s", ex_ch, e)
                return None

        if r.status_code != 200:
            logger.warning("mis.twse %s HTTP %d (possibly banned)", ex_ch, r.status_code)
            return None

        try:
            j = r.json()
        except json.JSONDecodeError:
            logger.error("mis.twse %s non-JSON response (possibly banned)", ex_ch)
            return None

        arr = j.get('msgArray', [])
        if not arr:
            return None
        # mis.twse 對「給錯 listing」(tse_ 給上櫃股 / otc_ 給上市股) 會回 stub element
        # (z='-' + pz=None + a/b 空 + v=None)，不會 404 也不會空 array。
        # 必須過濾掉，否則 prefix 探測會誤判。
        elem = arr[0]
        if not _has_real_data(elem):
            return None
        return elem

    def _resolve_prefix_and_fetch(self, stock_id: str) -> tuple[Optional[str], Optional[dict]]:
        """決定 stock_id 用 tse_ 還 otc_，並回傳第一次 fetch 拿到的 data。
        若 prefix 已 cache 過，仍會發一次 fetch 拿最新 data（cache 只省 prefix 探測）。
        回傳 (prefix, data)；都拿不到時 (None, None)。"""
        # 指數一律 tse_
        if stock_id in _INDEX_IDS:
            self._prefix_cache[stock_id] = 'tse'
            data = self._fetch(f'tse_{stock_id}.tw')
            return ('tse', data) if data else (None, None)

        cached = self._prefix_cache.get(stock_id, '__missing__')
        if cached == '__missing__':
            # 未探測過：tse → otc fallback，第一個拿到的就用
            for prefix in ('tse', 'otc'):
                data = self._fetch(f'{prefix}_{stock_id}.tw')
                if data is not None:
                    self._prefix_cache[stock_id] = prefix
                    return prefix, data
            self._prefix_cache[stock_id] = None
            return None, None

        if cached is None:
            # 之前探測過兩端都沒
            return None, None

        # cached prefix 已知，直接 fetch
        data = self._fetch(f'{cached}_{stock_id}.tw')
        return (cached, data) if data else (cached, None)

    def get_quote(self, stock_id: str) -> Optional[dict]:
        """
        Args:
            stock_id: 純台股代號 ('2330', '6488', 't00')。允許 '2330.TW' / '2330.TWO',
                會自動 strip 後綴。

        Returns:
            dict 含以下欄位（盤中 partial bar）：
                price (float)         成交價 (z 為 '-' 則 fallback pz)
                volume (int|None)     當日累積量 (指數為 None)
                open (float|None)
                high (float|None)
                low (float|None)
                prev_close (float|None)
                time (str)            'HH:MM:SS'
                date (str)            'YYYY-MM-DD'
                listing (str)         'tse' 或 'otc'
                source (str)          'mis.twse'
            或 None (查無此檔 / API 失敗 / 停牌且無 pz)
        """
        # 標準化 ticker (上層可能傳 '2330.TW' / '2330.TWO' / 大寫指數 'T00')
        # .TWO 必須先 strip，否則 .TW 會把 .TWO 截成 'O'
        sid = stock_id.upper().replace('.TWO', '').replace('.TW', '').strip().lower()

        prefix, data = self._resolve_prefix_and_fetch(sid)
        if prefix is None or data is None:
            logger.info("mis.twse %s not found / fetch failed", sid)
            return None

        return _parse_quote(data, prefix)


def _has_real_data(elem: dict) -> bool:
    """判斷 mis.twse 回的 msgArray element 是否真的有報價資料。
    給錯前綴會回 stub: z='-' / pz=None / a=空字串 / b=空字串 / v=None。"""
    # v (累積量) 有數字 → 一定有撮合過 → 有效
    v = elem.get('v')
    if v not in (None, '', '-'):
        return True
    # 五檔有任一邊有報價 → 開盤後正常掛單
    a = elem.get('a') or ''
    b = elem.get('b') or ''
    if a.split('_', 1)[0] not in ('', '-') or b.split('_', 1)[0] not in ('', '-'):
        return True
    # 開盤前可能 v=None+a=空+b=空，但 y (昨收) / o (試撮) 有數字也算
    if elem.get('o') not in (None, '', '-'):
        return True
    return False


def _to_float(v) -> Optional[float]:
    if v is None or v == '' or v == '-':
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v) -> Optional[int]:
    if v is None or v == '' or v == '-':
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _best_quote_mid(data: dict) -> Optional[float]:
    """從五檔 a/b 拿最佳買賣價中點。盤中只要有掛單就有值。
    a/b 是 'p1_p2_p3_p4_p5_' 格式，第一個 token 是 best ask / best bid。"""
    def _first(s):
        if not s or s == '-':
            return None
        token = s.split('_', 1)[0]
        return _to_float(token)

    ask1 = _first(data.get('a'))
    bid1 = _first(data.get('b'))
    if ask1 and bid1:
        return (ask1 + bid1) / 2.0
    return ask1 or bid1  # 單邊也接受


def _parse_quote(data: dict, listing: str) -> Optional[dict]:
    """msgArray[0] -> 標準化 dict。
    price 取得順序: z (該秒成交) -> pz (上次成交) -> 五檔買賣 1 中點 -> y (昨收)
    mis.twse 在 5 秒撮合視窗外 z 跟 pz 都會是 '-'，必須有 mid 兜底才不會回 None。"""
    price = _to_float(data.get('z'))
    price_source = 'z'
    if price is None:
        price = _to_float(data.get('pz'))
        price_source = 'pz'
    if price is None:
        price = _best_quote_mid(data)
        price_source = 'mid'
    if price is None:
        # 五檔也空 -> 使用昨收墊底（停牌 / 未開盤）
        price = _to_float(data.get('y'))
        price_source = 'prev_close'
    if price is None:
        return None

    # tlong 是 epoch ms (TW local)，比 t (HH:MM:SS) 更可靠拿到日期
    tlong = data.get('tlong')
    try:
        ts = datetime.fromtimestamp(int(tlong) / 1000) if tlong else datetime.now()
    except (TypeError, ValueError):
        ts = datetime.now()

    return {
        'price': price,
        'price_source': price_source,  # 'z' | 'pz' | 'mid' | 'prev_close'
        'volume': _to_int(data.get('v')),
        'open': _to_float(data.get('o')),
        'high': _to_float(data.get('h')),
        'low': _to_float(data.get('l')),
        'prev_close': _to_float(data.get('y')),
        'time': data.get('t', ts.strftime('%H:%M:%S')),
        'date': ts.strftime('%Y-%m-%d'),
        'listing': listing,
        'source': 'mis.twse',
    }


# ================================================================
# Module-level singleton
# ================================================================
_singleton: Optional[MisTwseClient] = None
_singleton_lock = threading.Lock()


def get_client() -> MisTwseClient:
    global _singleton
    if _singleton is None:
        with _singleton_lock:
            if _singleton is None:
                _singleton = MisTwseClient()
    return _singleton


def get_quote(stock_id: str) -> Optional[dict]:
    """便利函式，等同 get_client().get_quote(stock_id)。"""
    return get_client().get_quote(stock_id)


def is_tw_trading_hours(now: Optional[datetime] = None) -> bool:
    """週一至週五 09:00 ~ 13:30 (本地時區假設為 TW)。
    跟 cache_manager._is_tw_trading_hours 邏輯一致。"""
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False
    open_t = now.replace(hour=9, minute=0, second=0, microsecond=0)
    close_t = now.replace(hour=13, minute=30, second=0, microsecond=0)
    return open_t <= now <= close_t
