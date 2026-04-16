"""
market_banner.py -- 大盤儀表板 Banner

在 app.py 最上方顯示全市場級指標，所有模式共用（個股/選股/AI報告）。

內容：
  Row 1: 台股大盤（月線乖離/季線乖離/KD）+ 美股大盤（同上）
  Row 2: 台灣 FGI + CNN FGI + 期貨基差 + P/C Ratio

資料源：
  - 大盤 OHLCV：yfinance（^TWII / ^GSPC）
  - 台灣 FGI：taifex_data.TaiwanFearGreedIndex
  - CNN FGI：cnn_fear_greed.CNNFearGreedIndex
  - 期貨/選擇權：taifex_data.TAIFEXData
"""

import logging
import time

import numpy as np
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

# 快取 TTL（秒）
_BANNER_CACHE_TTL = 300  # 5 分鐘


# ============================================================
#  指數技術指標計算
# ============================================================

def _fetch_index_metrics(ticker, name):
    """
    抓指數 OHLCV 並計算月線/季線乖離率 + KD。

    Returns
    -------
    dict: price, ma20_bias, ma60_bias, k, d, change_pct
    """
    result = {
        'name': name, 'price': None, 'change_pct': None,
        'ma20_bias': None, 'ma60_bias': None,
        'k': None, 'd': None, 'error': None,
    }
    try:
        import yfinance as yf
        df = yf.Ticker(ticker).history(period='6mo')
        if df.empty or len(df) < 60:
            result['error'] = 'data insufficient'
            return result

        close = df['Close']
        high = df['High']
        low = df['Low']

        # 現價 + 漲跌幅
        result['price'] = round(float(close.iloc[-1]), 2)
        if len(close) >= 2:
            prev = float(close.iloc[-2])
            if prev > 0:
                result['change_pct'] = round((close.iloc[-1] / prev - 1) * 100, 2)

        # 月線乖離率（MA20）
        ma20 = close.rolling(20).mean().iloc[-1]
        if pd.notna(ma20) and ma20 > 0:
            result['ma20_bias'] = round((close.iloc[-1] / ma20 - 1) * 100, 2)

        # 季線乖離率（MA60）
        ma60 = close.rolling(60).mean().iloc[-1]
        if pd.notna(ma60) and ma60 > 0:
            result['ma60_bias'] = round((close.iloc[-1] / ma60 - 1) * 100, 2)

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

    except Exception as e:
        logger.warning("Failed to fetch index %s: %s", ticker, e)
        result['error'] = str(e)
    return result


# ============================================================
#  情緒 + 期權指標
# ============================================================

def _fetch_sentiment_data():
    """
    抓台灣 FGI / CNN FGI / 期貨基差 / P/C Ratio。

    Returns
    -------
    dict: tw_fgi, cnn_fgi, basis, pcr (each is a sub-dict or None)
    """
    data = {'tw_fgi': None, 'cnn_fgi': None, 'basis': None, 'pcr': None}

    # 台灣 FGI
    try:
        from taifex_data import TaiwanFearGreedIndex
        fgi = TaiwanFearGreedIndex()
        data['tw_fgi'] = fgi.calculate()
    except Exception as e:
        logger.debug("TW FGI failed: %s", e)

    # CNN FGI
    try:
        from cnn_fear_greed import CNNFearGreedIndex
        cnn = CNNFearGreedIndex()
        data['cnn_fgi'] = cnn.get_index()
    except Exception as e:
        logger.debug("CNN FGI failed: %s", e)

    # 期貨基差
    try:
        from taifex_data import TAIFEXData
        taifex = TAIFEXData()
        data['basis'] = taifex.get_futures_basis()
    except Exception as e:
        logger.debug("Futures basis failed: %s", e)

    # P/C Ratio
    try:
        from taifex_data import TAIFEXData
        taifex = TAIFEXData()
        data['pcr'] = taifex.get_put_call_ratio()
    except Exception as e:
        logger.debug("PCR failed: %s", e)

    return data


# ============================================================
#  帶快取的主入口
# ============================================================

def _get_banner_data():
    """取得 banner 所有資料，帶 session_state 快取。"""
    cache_key = '_market_banner_cache'
    cache_ts_key = '_market_banner_ts'

    cached = st.session_state.get(cache_key)
    cached_ts = st.session_state.get(cache_ts_key, 0)

    if cached and (time.time() - cached_ts) < _BANNER_CACHE_TTL:
        return cached

    # 重新抓取
    tw = _fetch_index_metrics('^TWII', '加權指數')
    us = _fetch_index_metrics('^GSPC', 'S&P 500')
    sentiment = _fetch_sentiment_data()

    data = {'tw': tw, 'us': us, **sentiment}
    st.session_state[cache_key] = data
    st.session_state[cache_ts_key] = time.time()
    return data


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

    # 乖離率 + KD 用 caption 緊湊顯示
    ma20 = data.get('ma20_bias')
    ma60 = data.get('ma60_bias')
    k = data.get('k')
    d = data.get('d')

    parts = []
    if ma20 is not None:
        parts.append(f"月線乖離 {ma20:+.2f}%")
    if ma60 is not None:
        parts.append(f"季線乖離 {ma60:+.2f}%")
    if k is not None and d is not None:
        parts.append(f"K={k:.0f} D={d:.0f}")
    if parts:
        col.caption(" | ".join(parts))


def render_market_banner():
    """
    渲染大盤儀表板 Banner。在 app.py 主內容區頂端呼叫。
    使用 st.expander 包裝，預設展開。
    """
    with st.expander("📊 大盤儀表板", expanded=True):
        data = _get_banner_data()

        # --- Row 1: 大盤技術指標 ---
        tw = data.get('tw', {})
        us = data.get('us', {})

        r1c1, r1c2, r1c3, r1c4 = st.columns(4)

        # 台股
        _render_index_card(r1c1, tw)

        # 美股
        _render_index_card(r1c2, us)

        # --- FGI ---
        tw_fgi = data.get('tw_fgi') or {}
        tw_score = tw_fgi.get('score')
        tw_label = tw_fgi.get('label', '')

        if tw_score is not None:
            r1c3.metric("台灣 FGI", f"{tw_score:.0f}", delta=tw_label)
            r1c3.progress(int(min(max(tw_score, 0), 100)))
        else:
            r1c3.metric("台灣 FGI", "N/A")

        cnn_fgi = data.get('cnn_fgi') or {}
        cnn_score = cnn_fgi.get('score')
        cnn_label = cnn_fgi.get('label', '')

        if cnn_score is not None:
            r1c4.metric("CNN FGI", f"{cnn_score:.0f}", delta=cnn_label)
            r1c4.progress(int(min(max(cnn_score, 0), 100)))
        else:
            r1c4.metric("CNN FGI", "N/A")

        # --- Row 2: 期貨/選擇權 + FGI 子指標 ---
        r2c1, r2c2, r2c3 = st.columns(3)

        # 期貨基差
        basis = data.get('basis') or {}
        b_val = basis.get('basis')
        if b_val is not None:
            r2c1.metric("期貨基差", f"{b_val:.0f} 點",
                        delta="正價差 (偏多)" if b_val > 0 else "逆價差 (偏空)")
        else:
            r2c1.metric("期貨基差", "N/A")

        # P/C Ratio
        pcr = data.get('pcr') or {}
        pc = pcr.get('pc_ratio')
        if pc is not None:
            pc_delta = "恐懼" if pc > 1.0 else "貪婪" if pc < 0.7 else "中性"
            r2c2.metric("P/C Ratio", f"{pc:.2f}", delta=pc_delta)
        else:
            r2c2.metric("P/C Ratio", "N/A")

        # 台灣 FGI 子指標（簡表）
        components = tw_fgi.get('components', {})
        if components:
            label_map = {
                'market_momentum': '市場動能',
                'market_breadth': '漲跌家數',
                'put_call_ratio': 'P/C比',
                'volatility': '波動率',
                'margin_balance': '融資餘額',
            }
            comp_parts = []
            for name, val in components.items():
                if isinstance(val, dict):
                    s = val.get('score')
                    if s is not None:
                        tag = "Fear" if s < 40 else "Greed" if s > 60 else "Neutral"
                        comp_parts.append(f"{label_map.get(name, name)} {s:.0f}({tag})")
            if comp_parts:
                r2c3.caption("TW FGI 子指標: " + " | ".join(comp_parts))

        # CNN 子指標
        cnn_components = cnn_fgi.get('components', {})
        if cnn_components:
            cnn_parts = []
            for name, val in cnn_components.items():
                s = val.get('score')
                if s is not None:
                    cnn_parts.append(f"{name} {s:.0f}")
            if cnn_parts:
                r2c3.caption("CNN 子指標: " + " | ".join(cnn_parts))
