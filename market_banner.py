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


def render_market_banner():
    """
    渲染大盤儀表板 Banner。在 app.py 主內容區頂端呼叫。
    使用 st.expander 包裝，預設展開。
    """
    with st.expander("📊 大盤儀表板", expanded=True):
        data = _get_banner_data()

        tw = data.get('tw', {})
        us = data.get('us', {})
        tw_fgi = data.get('tw_fgi') or {}
        cnn_fgi = data.get('cnn_fgi') or {}

        # 單排 4 欄：所有內容垂直堆疊在各欄內，無 Row 2 間距
        c1, c2, c3, c4 = st.columns(4)

        # ── C1: 加權指數 + 乖離/KD + 基差&PCR 併排 ──
        _render_index_card(c1, tw)

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
                    actual = f"{val.get('pc_ratio', 0):.2f}"
                elif name == 'volatility':
                    actual = f"{val.get('volatility_20d', 0):.1f}%"
                elif name == 'margin_balance':
                    mb = val.get('margin_balance', 0)
                    actual = f"{mb:,.0f} 張"
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
            if comp_data:
                c2.table(pd.DataFrame(comp_data))

        # ── C3: S&P 500 + 乖離/KD + CNN FGI 歷史 ──
        _render_index_card(c3, us)

        if cnn_fgi:
            hist_data = []
            for key, label in [('previous_close', '前日收盤'), ('one_week_ago', '一週前'),
                               ('one_month_ago', '一月前'), ('one_year_ago', '一年前')]:
                val = cnn_fgi.get(key)
                if val is not None:
                    hist_data.append({"時間": label, "數值": f"{val:.0f}"})
            if hist_data:
                c3.markdown("**CNN FGI 歷史**")
                c3.table(pd.DataFrame(hist_data))

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
