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

快取策略（per-indicator，依 reference_banner_cache_timing）：
  - 台股 14:00 類（tw_index/basis/pcr）：抓到 today → 快取到隔天 14:00；
    未抓到 → 5 分鐘 retry
  - 台灣 FGI（20:00 融資）：抓到 today → 快取到隔天 20:00；
    未抓到 → 30 分鐘 retry
  - CNN FGI：美股盤中 1h / 閉市 4h
  - S&P 500：美股盤中 5 分鐘 / 閉市到下個開盤
"""

import logging
import time
from datetime import datetime, date as ddate, time as dtime, timedelta

import numpy as np
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

# ============================================================
#  快取配置 (per-indicator)
# ============================================================

_CACHE_KEY = '_banner_cache_v2'

# 台股日盤後資料發布時間
_TW_14_CUTOFF = dtime(14, 0)      # 加權指數/漲跌家數/基差/PCR
_TW_20_CUTOFF = dtime(20, 0)      # 融資餘額
_TW_14_RETRY = 300                 # 5 分鐘
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

def _fetch_index_metrics(ticker, name):
    """
    抓指數 OHLCV 並計算月線/季線乖離率 + KD。

    Returns
    -------
    dict: price, ma20_bias, ma60_bias, k, d, change_pct, data_date
    """
    result = {
        'name': name, 'price': None, 'change_pct': None,
        'ma20_bias': None, 'ma60_bias': None,
        'k': None, 'd': None, 'error': None, 'data_date': None,
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

        # 資料日期（最後一根 K 的日期）
        try:
            result['data_date'] = df.index[-1].date()
        except Exception:
            pass

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
    """寫入快取並依 indicator 規則計算 expires_at。"""
    now = now or _now_tw()
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
    """
    # 台股 14:00 類
    if indicator in ('tw_index', 'basis', 'pcr', 'm1b_ratio'):
        expected = _expected_tw_date(_TW_14_CUTOFF, now)
        if data_date == expected:
            return _next_tw_refresh_at(_TW_14_CUTOFF, now).timestamp()
        return time.time() + _TW_14_RETRY

    # 台灣 FGI (20:00 融資為主)
    if indicator == 'tw_fgi':
        expected = _expected_tw_date(_TW_20_CUTOFF, now)
        if data_date == expected:
            return _next_tw_refresh_at(_TW_20_CUTOFF, now).timestamp()
        return time.time() + _TW_20_RETRY

    # 美股指數
    if indicator == 'us_index':
        if _is_us_market_hours(now):
            return time.time() + _US_INDEX_INTRADAY_TTL
        return _next_us_open_at(now).timestamp()

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

# Banner 輸出 key -> session_state 快取 key
_OUT_TO_CACHE = {
    'tw': 'tw_index',
    'us': 'us_index',
    'tw_fgi': 'tw_fgi',
    'cnn_fgi': 'cnn_fgi',
    'basis': 'basis',
    'pcr': 'pcr',
    'm1b_ratio': 'm1b_ratio',
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
        if cache_key == 'tw_fgi':
            from taifex_data import TaiwanFearGreedIndex
            return TaiwanFearGreedIndex().calculate()
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
            from money_supply import compute_m1b_ratio
            return compute_m1b_ratio()
    except Exception as e:
        logger.debug("Banner fetch %s failed: %s", cache_key, e)
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
                    # 融資金額(仟元) / 估計大盤市值 → 百分比
                    # margin_balance 來自 MI_MARGN，單位是張(shares)
                    # 但 _calc_margin_score 回傳的 change_rate_pct 是變化率
                    # 直接用 margin 金額計算市值比需要另外抓金額
                    # 這裡用回傳的 margin_balance(張) 搭配大盤指數粗估
                    mb = val.get('margin_balance', 0)
                    tw_price = tw.get('price', 0)
                    if tw_price > 0 and mb > 0:
                        # 融資金額(仟元) → 億TWD：/1e5
                        # 大盤市值(億TWD) ≈ 指數 × 15（每點約15億）
                        margin_bil = mb / 1e5
                        mktcap_bil = tw_price * 15
                        ratio = margin_bil / mktcap_bil * 100
                        actual = f"{ratio:.2f}%"
                    else:
                        actual = f"{mb:,.0f}"
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
