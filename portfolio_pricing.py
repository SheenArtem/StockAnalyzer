"""
投組報價層 — 現價 + 歷史價（💼 投資組合 tab，2026-07-01）

全部複用既有 fetcher，不自寫每日線：
  現價 get_current_prices(tickers, live):
    - 預設 (live=False)：load_and_resample disk cache 的最後收盤 + 前收（TW/US 通吃，秒級、零外呼）
    - live=True 台股盤中：mis.twse ex_ch 批次（≤50/請求，一個投組 1~2 請求；真即時 tick）
    - live=True 美股：Yahoo v8 chart 逐檔（range=1d + interval=1m + includePrePost）。
        常規盤實測延遲數秒（近即時，非早年那句「15 分鐘延遲」）；盤前/盤後最新價取
        1 分 K 棒最後一根非空收盤（Yahoo meta 不給 pre/postMarketPrice 標量）。
  歷史價 get_price_history(tickers)：load_and_resample 的 df_day Close（供 Phase 3 NAV）。

回傳 quote schema（每檔一個 dict）：
    {'price', 'prev_close', 'change_pct'(小數), 'currency'('TWD'|'USD'),
     'source'('mis.twse'|'yahoo'|'eod'|'none'), 'market_state', 'name', 'asof'}
"""
import logging
import time

import requests

import mis_twse_client
from portfolio_store import detect_market, normalize_ticker
from technical_analysis import load_and_resample

logger = logging.getLogger(__name__)

_YF_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)
# 美股用 v8 chart endpoint（單檔一 URL），參數 range=1d + interval=1m +
# includePrePost=true：
#   - meta.regularMarketPrice = 常規盤最後成交，實測延遲數秒（近即時，非早年那句
#     「15 分鐘延遲」——2026-07-10 量測 AAPL/NVDA/TSLA/SPY lag 0~4 秒）。
#   - 盤前/盤後最新價「不在 meta」（Yahoo 這個 endpoint 不給 pre/postMarketPrice
#     標量，marketState 也回 None），而在 1 分 K 棒 indicators.quote.close 裡，取最後
#     一根非空收盤。盤別改用 meta.currentTradingPeriod 的 pre/regular/post 時間窗判斷。
#   - range=1d 時 meta.chartPreviousClose/previousClose 即為前一交易日常規盤收盤
#     （range=5d 時 chartPreviousClose 會是視窗基準日、非昨收，故不可用 5d）。
# 實測本機 v7 /quote 被擋(401 Unauthorized，market-pulse 靠 Cloudflare Worker IP 才行)，
# v8 chart 則回 200。無 mis 的硬節流，投組數檔迴圈即可。
_YF_CHART_URL = 'https://query1.finance.yahoo.com/v8/finance/chart/{sym}'


def _now_epoch() -> float:
    """現在時間 (epoch 秒)。抽成函式方便測試注入盤別。"""
    return time.time()


def _empty_quote(ticker: str, source: str = 'none') -> dict:
    return {'price': None, 'prev_close': None, 'change_pct': None,
            'currency': 'TWD' if detect_market(ticker) == 'tw' else 'USD',
            'source': source, 'market_state': None, 'name': None, 'asof': None}


def _pct(price, prev):
    if price is None or prev in (None, 0):
        return None
    return (price - prev) / prev


# ====================================================================
#  美股：Yahoo Finance v8 chart（單檔迴圈；常規盤近即時 + 盤前盤後）
# ====================================================================

def _yahoo_chart_result(symbol: str) -> dict:
    """Return the first Yahoo v8 chart result for a symbol, or None.

    range=1d + interval=1m + includePrePost=true：回應同時帶常規盤與盤前/盤後的
    1 分 K 棒，meta 含 currentTradingPeriod（判斷盤別）與 previousClose（前一交易日收）。
    """
    r = requests.get(_YF_CHART_URL.format(sym=symbol),
                     params={'range': '1d', 'interval': '1m',
                             'includePrePost': 'true'},
                     headers={'User-Agent': _YF_UA}, timeout=15)
    j = r.json()
    res = (j.get('chart', {}) or {}).get('result')
    return res[0] if res else None


def _prev_close(meta: dict) -> float | None:
    """前收（前一交易日常規盤收盤）。range=1d 時 chartPreviousClose 即為正解。"""
    for key in ('previousClose', 'chartPreviousClose', 'regularMarketPreviousClose'):
        v = meta.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


def _classify_period(now_epoch: float, ctp: dict) -> str | None:
    """用 meta.currentTradingPeriod 的 pre/regular/post 時間窗判斷現在盤別。

    回 'PRE' | 'REGULAR' | 'POST' | 'CLOSED'；無 currentTradingPeriod 回 None。
    """
    if not ctp:
        return None
    for state, key in (('PRE', 'pre'), ('REGULAR', 'regular'), ('POST', 'post')):
        w = ctp.get(key) or {}
        start, end = w.get('start'), w.get('end')
        if start is not None and end is not None and start <= now_epoch < end:
            return state
    return 'CLOSED'


def _last_extended_bar(result: dict, now_epoch: float):
    """回 (price, epoch) = 最後一根「時間 <= now」的非空 1 分 K 棒收盤；無則 (None, None)。

    盤前/盤後用：Yahoo meta 不給 pre/postMarketPrice 標量，最新成交只在 K 棒裡。
    以 now 上界過濾，避免抓到尚未成交（未來）或跨盤別的 K 棒。
    """
    try:
        ts = result.get('timestamp') or []
        quotes = (result.get('indicators', {}) or {}).get('quote') or [{}]
        closes = quotes[0].get('close') or []
    except (AttributeError, IndexError, TypeError):
        return None, None
    for i in range(len(closes) - 1, -1, -1):
        close = closes[i]
        if close is None:
            continue
        epoch = ts[i] if i < len(ts) else None
        if epoch is not None and epoch > now_epoch:
            continue
        try:
            return float(close), epoch
        except (TypeError, ValueError):
            continue
    return None, None


def _pick_us_price(result: dict, now_epoch: float) -> dict:
    """從一個 chart result 挑現價 + 盤別 + asof。

    常規盤 / 收盤 / 未知盤別 -> meta.regularMarketPrice（近即時 / 最後收盤，秒級新鮮）
    盤前 / 盤後             -> 最後一根非空 1 分 K 棒收盤（meta 此時的常規盤價已凍結）
    前收一律用前一交易日常規盤收盤，故 change_pct 反映「自昨收以來的當日漲跌」。
    """
    meta = result.get('meta') or {}
    state = _classify_period(now_epoch, meta.get('currentTradingPeriod'))
    reg = meta.get('regularMarketPrice')
    reg = float(reg) if reg is not None else None
    reg_time = meta.get('regularMarketTime')

    if state in ('PRE', 'POST'):
        price, epoch = _last_extended_bar(result, now_epoch)
        if price is None:                 # 盤前盤後尚無成交 -> 退回常規盤價
            price, epoch = reg, reg_time
    else:                                  # REGULAR / CLOSED / 未知 -> 常規盤價
        price, epoch = reg, reg_time

    return {
        'price': price,
        'prev_close': _prev_close(meta),
        'market_state': state,
        'asof': epoch,
        'currency': meta.get('currency') or 'USD',
        'name': meta.get('shortName') or meta.get('longName'),
    }


def get_us_quotes(tickers: list) -> dict:
    """美股現價（v8 chart，逐檔，含盤前/盤後）。回 {ticker: quote_dict}（抓不到者不放入）。"""
    now = _now_epoch()
    out = {}
    for t in [t for t in tickers if t]:
        try:
            result = _yahoo_chart_result(t)
        except (requests.RequestException, ValueError) as e:
            logger.warning("yahoo v8 chart %s failed: %s", t, e)
            continue
        if not result or not result.get('meta'):
            continue
        picked = _pick_us_price(result, now)
        if picked['price'] is None:
            continue
        out[t] = {
            'price': picked['price'],
            'prev_close': picked['prev_close'],
            'change_pct': _pct(picked['price'], picked['prev_close']),
            'currency': picked['currency'],
            'source': 'yahoo',
            'market_state': picked['market_state'],
            'name': picked['name'],
            'asof': picked['asof'],
        }
    return out


# ====================================================================
#  台股：mis.twse 批次現價（盤中）
# ====================================================================

def get_tw_quotes(tickers: list) -> dict:
    """台股批次即時現價（盤中）。回 {ticker: quote_dict}（抓不到者不放入）。"""
    tickers = [t for t in tickers if t]
    raw = mis_twse_client.get_quotes(tickers)
    out = {}
    for t in tickers:
        q = raw.get(t)
        if not q:
            continue
        price = q.get('price')
        prev = q.get('prev_close')
        out[t] = {
            'price': price,
            'prev_close': prev,
            'change_pct': _pct(price, prev),
            'currency': 'TWD',
            'source': 'mis.twse',
            'market_state': 'REGULAR',
            'name': None,
            'asof': q.get('time'),
        }
    return out


# ====================================================================
#  EOD（預設）：load_and_resample disk cache 的最後收盤 + 前收
# ====================================================================

def _eod_quote(ticker: str) -> dict:
    try:
        _name, df, _week, meta = load_and_resample(ticker)
    except Exception as e:
        logger.warning("EOD load_and_resample %s failed: %s", ticker, e)
        return _empty_quote(ticker)
    if df is None or getattr(df, 'empty', True) or 'Close' not in df.columns:
        return _empty_quote(ticker)
    close = df['Close'].dropna()
    if len(close) == 0:
        return _empty_quote(ticker)
    price = float(close.iloc[-1])
    prev = float(close.iloc[-2]) if len(close) >= 2 else None
    name = None
    if isinstance(meta, dict):
        name = meta.get('name') or meta.get('longName')
    return {
        'price': price,
        'prev_close': prev,
        'change_pct': _pct(price, prev),
        'currency': 'TWD' if detect_market(ticker) == 'tw' else 'USD',
        'source': 'eod',
        'market_state': 'CLOSED',
        'name': name,
        'asof': close.index[-1].strftime('%Y-%m-%d') if len(close) else None,
    }


# ====================================================================
#  對外：orchestration
# ====================================================================

def get_current_prices(tickers, live: bool = False) -> dict:
    """投組現價。回 {ticker: quote_dict}。

    live=False（預設）：全部走 EOD（秒級、零外呼、TW/US 通吃）。
    live=True：台股盤中走 mis.twse 批次、美股走 Yahoo v8 chart 逐檔（含盤前/盤後）；
               live 抓不到的個別代號自動 fallback 到該檔 EOD。
    """
    tickers = [normalize_ticker(t) for t in tickers if str(t or '').strip()]
    tickers = list(dict.fromkeys(tickers))  # 去重、保序
    result = {}

    if live:
        tw = [t for t in tickers if detect_market(t) == 'tw']
        us = [t for t in tickers if detect_market(t) == 'us']
        if tw and mis_twse_client.is_tw_trading_hours():
            try:
                result.update(get_tw_quotes(tw))
            except Exception as e:
                logger.warning("TW live quotes failed, fallback EOD: %s", e)
        if us:
            try:
                result.update(get_us_quotes(us))
            except Exception as e:
                logger.warning("US live quotes failed, fallback EOD: %s", e)

    # 未 live 或 live 沒抓到的 -> EOD 補齊
    for t in tickers:
        if t not in result or result[t].get('price') is None:
            result[t] = _eod_quote(t)
    return result


def get_price_history(tickers) -> dict:
    """回 {ticker: pandas.Series(Close, DatetimeIndex)}（供 Phase 3 NAV）；抓不到者略過。"""
    out = {}
    for t in tickers:
        t = normalize_ticker(t)
        if not t or t in out:
            continue
        try:
            _name, df, _week, _meta = load_and_resample(t)
        except Exception as e:
            logger.warning("history load_and_resample %s failed: %s", t, e)
            continue
        if df is not None and not getattr(df, 'empty', True) and 'Close' in df.columns:
            s = df['Close'].dropna()
            if len(s):
                out[t] = s
    return out
