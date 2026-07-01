"""
投組報價層 — 現價 + 歷史價（💼 投資組合 tab，2026-07-01）

全部複用既有 fetcher，不自寫每日線：
  現價 get_current_prices(tickers, live):
    - 預設 (live=False)：load_and_resample disk cache 的最後收盤 + 前收（TW/US 通吃，秒級、零外呼）
    - live=True 台股盤中：mis.twse ex_ch 批次（≤50/請求，一個投組 1~2 請求；真即時 tick）
    - live=True 美股：Yahoo v7 quote 批次（crumb+cookie，≤50/請求，~15min 延遲；港自 market-pulse worker.js）
  歷史價 get_price_history(tickers)：load_and_resample 的 df_day Close（供 Phase 3 NAV）。

回傳 quote schema（每檔一個 dict）：
    {'price', 'prev_close', 'change_pct'(小數), 'currency'('TWD'|'USD'),
     'source'('mis.twse'|'yahoo'|'eod'|'none'), 'market_state', 'name', 'asof'}
"""
import logging

import requests

import mis_twse_client
from portfolio_store import detect_market, normalize_ticker
from technical_analysis import load_and_resample

logger = logging.getLogger(__name__)

_YF_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
)
# 美股用 v8 chart endpoint：單檔一 URL，回 meta.regularMarketPrice(~15min 延遲) +
# chartPreviousClose。實測本機 v7 /quote 被擋(401 Unauthorized，market-pulse 靠
# Cloudflare Worker IP 才行)，v8 chart 則回 200。無 mis 的硬節流，投組數檔迴圈即可。
_YF_CHART_URL = 'https://query1.finance.yahoo.com/v8/finance/chart/{sym}'


def _empty_quote(ticker: str, source: str = 'none') -> dict:
    return {'price': None, 'prev_close': None, 'change_pct': None,
            'currency': 'TWD' if detect_market(ticker) == 'tw' else 'USD',
            'source': source, 'market_state': None, 'name': None, 'asof': None}


def _pct(price, prev):
    if price is None or prev in (None, 0):
        return None
    return (price - prev) / prev


# ====================================================================
#  美股：Yahoo Finance v8 chart（單檔迴圈；~15min 延遲的近即時價）
# ====================================================================

def _yahoo_chart_result(symbol: str) -> dict:
    """Return the first Yahoo v8 chart result for a symbol, or None."""
    r = requests.get(_YF_CHART_URL.format(sym=symbol),
                     params={'range': '5d', 'interval': '1d'},
                     headers={'User-Agent': _YF_UA}, timeout=15)
    j = r.json()
    res = (j.get('chart', {}) or {}).get('result')
    return res[0] if res else None


def _yahoo_chart_meta(symbol: str) -> dict:
    result = _yahoo_chart_result(symbol)
    return (result.get('meta') if result else None)


def _prev_close_from_chart(result: dict, price) -> float | None:
    """Derive previous close from Yahoo daily bars.

    Yahoo's chartPreviousClose is the close before the requested chart window
    when range=5d, not necessarily yesterday's close.
    Do not compare with the Taiwan calendar date; a US trading session crosses
    two Taiwan calendar days.
    """
    try:
        quotes = ((result.get('indicators', {}) or {}).get('quote') or [])
        closes = quotes[0].get('close') or []
    except (AttributeError, IndexError):
        return None

    vals = []
    for close in closes:
        if close is None:
            continue
        try:
            vals.append(float(close))
        except (TypeError, ValueError):
            continue
    if not vals:
        return None

    try:
        px = float(price) if price is not None else None
    except (TypeError, ValueError):
        px = None

    if px is not None and len(vals) >= 2:
        tolerance = max(0.01, abs(px) * 0.0005)
        if abs(vals[-1] - px) <= tolerance:
            return vals[-2]
    return vals[-1]


def get_us_quotes(tickers: list) -> dict:
    """美股現價（v8 chart，逐檔）。回 {ticker: quote_dict}（抓不到者不放入）。"""
    out = {}
    for t in [t for t in tickers if t]:
        try:
            result = _yahoo_chart_result(t)
        except (requests.RequestException, ValueError) as e:
            logger.warning("yahoo v8 chart %s failed: %s", t, e)
            continue
        meta = (result.get('meta') if result else None)
        if not meta:
            continue
        price = meta.get('regularMarketPrice')
        prev = _prev_close_from_chart(result, price)
        if prev is None:
            prev = meta.get('regularMarketPreviousClose')
        if prev is None:
            prev = meta.get('previousClose')
        if prev is None:
            prev = meta.get('chartPreviousClose')
        price = float(price) if price is not None else None
        prev = float(prev) if prev is not None else None
        out[t] = {
            'price': price,
            'prev_close': prev,
            'change_pct': _pct(price, prev),
            'currency': meta.get('currency') or 'USD',
            'source': 'yahoo',
            'market_state': meta.get('marketState'),
            'name': meta.get('shortName') or meta.get('longName'),
            'asof': meta.get('regularMarketTime'),
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
    live=True：台股盤中走 mis.twse 批次、美股走 Yahoo v7 批次；
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
