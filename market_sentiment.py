"""市場 vs 個股情緒分數 (2026-05-01 Day 1+2)

Two scores in -100~+100 range:
  - 市場情緒 (get_market_sentiment_score): 復用 TaiwanFearGreedIndex (5 子指標
    Market Momentum / Breadth / PCR / Volatility / Margin) 0-100 → 線性 map
    到 -100~+100。M1B/成交比過熱當 overlay warning。
  - 個股情緒 v1 (get_stock_sentiment_score): 訊號組合
    法人 5d 買賣超 (35%) + 融資增減反向 (15%) + 股價 vs 5/10MA (30%) +
    News tone aggregate (20%, 從 data/news_themes.parquet 30d 內 sentiment 平均)。
    News tone 不命中時退回 v0 三訊號 + 用全 valid weight 重 normalize。
  - 對比 (get_sentiment_divergence): 兩 score 差值 → 4 種訊號標籤。

News tone 來源：tools/news_theme_extract.py 每日由 scanner 跑出，
sentiment 是 -1.0~+1.0 (LLM Sonnet 萃取)。
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _score_to_label(score: float) -> str:
    """-100~+100 → 5 級標籤"""
    if score >= 50:
        return '極度貪婪'
    if score >= 20:
        return '貪婪'
    if score >= -20:
        return '中性'
    if score >= -50:
        return '恐懼'
    return '極度恐懼'


def _stock_label(score: float) -> str:
    if score >= 50:
        return '強催化'
    if score >= 20:
        return '正向'
    if score >= -20:
        return '中性'
    if score >= -50:
        return '逆風'
    return '弱勢'


def get_market_sentiment_score() -> Dict[str, Any]:
    """市場情緒分數 -100~+100 (復用 TaiwanFearGreedIndex 0-100 線性 map)。

    Returns dict:
      score: float -100~+100
      label: str (極度貪婪/貪婪/中性/恐懼/極度恐懼)
      raw_fgi: float 0-100 (原始 TFGI)
      components: dict (5 子指標 score + 實際數值)
      m1b_overlay: dict | None (M1B 過熱/偏冷 warning)
      data_date: str
    """
    try:
        from taifex_data import TaiwanFearGreedIndex
        fgi = TaiwanFearGreedIndex().calculate()
        raw = fgi.get('score', 50.0)
        score = round((raw - 50) * 2, 1)  # 0-100 → -100~+100
    except Exception as e:
        logger.warning("TaiwanFearGreedIndex 計算失敗: %s", e)
        return {
            'score': 0.0, 'label': '中性 (no data)', 'raw_fgi': None,
            'components': {}, 'm1b_overlay': None, 'data_date': None,
            'error': str(e),
        }

    # M1B overheat overlay (warning，不改 score)
    m1b_overlay = None
    try:
        from money_supply import compute_m1b_ratio
        m1b = compute_m1b_ratio()
        if m1b and m1b.get('ratio_pct') is not None:
            ratio = float(m1b['ratio_pct'])
            if ratio >= 70:
                m1b_overlay = {
                    'warn': 'overheat',
                    'msg': f'M1B/成交比 {ratio:.1f}% 過熱',
                    'ratio': ratio,
                }
            elif ratio <= 30:
                m1b_overlay = {
                    'warn': 'cold',
                    'msg': f'M1B/成交比 {ratio:.1f}% 偏冷',
                    'ratio': ratio,
                }
    except Exception as e:
        logger.debug("M1B overlay 失敗: %s", e)

    return {
        'score': score,
        'label': _score_to_label(score),
        'raw_fgi': raw,
        'components': fgi.get('components', {}),
        'm1b_overlay': m1b_overlay,
        'data_date': fgi.get('data_date'),
    }


def _calc_inst_signal(institutional_df) -> float | None:
    """法人 5d 買賣超 → -1~+1。

    用近 5 日「外資 + 投信」買賣超總和 (張)，相對近 20 日平均成交量做 normalize。
    tanh squash 到 -1~+1。
    """
    if institutional_df is None or institutional_df.empty:
        return None
    try:
        # ChipAnalyzer institutional df cols (Chinese): 外資 / 投信 / 自營商 / 合計 / 三大法人合計
        df = institutional_df.tail(5).copy()
        if len(df) == 0:
            return None
        # 用第 5 column (三大法人合計) 比較穩定
        col = df.columns[-1]
        net_5d = float(df[col].sum())
        # Normalize: 100M shares = saturate (TSMC 量級)，小股 1M = saturate
        # 用 tanh 自適應 — 5d net / 1M shares
        return math.tanh(net_5d / 1_000_000)
    except Exception as e:
        logger.debug("inst_signal 計算失敗: %s", e)
        return None


def _calc_margin_signal(margin_df) -> float | None:
    """融資 5d 增減反向 → -1~+1。

    融資增 = 散戶追漲，視為個股情緒負分；融資減 = 散戶撤退，視為負壓力解除。
    用近 5 日 vs 前 5 日的「融資餘額」%變化 tanh squash。
    """
    if margin_df is None or margin_df.empty or len(margin_df) < 10:
        return None
    try:
        col = margin_df.columns[0]  # 融資餘額
        recent_5 = float(margin_df[col].tail(5).mean())
        prior_5 = float(margin_df[col].iloc[-10:-5].mean())
        if prior_5 <= 0:
            return None
        chg_pct = (recent_5 - prior_5) / prior_5 * 100
        # 反向：融資增 → 負分；10% 變化 saturate
        return -math.tanh(chg_pct / 10)
    except Exception as e:
        logger.debug("margin_signal 計算失敗: %s", e)
        return None


_NEWS_PARQUET = None  # lazy load cache
_NEWS_LOAD_TS = 0.0


def _load_news_parquet():
    """Lazy load articles_recent (90d hot view) with 1h cache.

    News Initiative Phase 0 Commit 6: 從 data/news_themes.parquet (legacy)
    切到 data/news/articles_recent.parquet (新 hot view)。
    Apply BLOCKER #1 dedupe_by_event_id 防同事件 cnyes+UDN 灌票 sentiment 分數。
    Reader fallback to legacy: 新 path 不存在或讀取失敗時 fallback 舊 path,
    提供 graceful degradation (Robustness > cleanliness, 永久保留)。
    """
    global _NEWS_PARQUET, _NEWS_LOAD_TS
    import time as _t
    from pathlib import Path as _P
    if _NEWS_PARQUET is not None and _t.time() - _NEWS_LOAD_TS < 3600:
        return _NEWS_PARQUET
    repo = _P(__file__).resolve().parent
    new_path = repo / 'data' / 'news' / 'articles_recent.parquet'
    legacy_path = repo / 'data' / 'news_themes.parquet'
    path = new_path if new_path.exists() else legacy_path
    if not path.exists():
        _NEWS_PARQUET = None
        _NEWS_LOAD_TS = _t.time()
        return None
    try:
        import pandas as pd
        df = pd.read_parquet(path)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # BLOCKER #1: dedupe by event_id 防同事件多 source 灌票 (僅新 schema 有)
        if 'event_id' in df.columns:
            try:
                import sys
                sys.path.insert(0, str(repo / 'tools'))
                from news_theme_extract import dedupe_by_event_id
                df = dedupe_by_event_id(df)
            except Exception as e:
                logger.debug("dedupe_by_event_id failed (fallback no dedupe): %s", e)
        _NEWS_PARQUET = df
    except Exception as e:
        logger.debug("news parquet load 失敗 %s: %s", path, e)
        _NEWS_PARQUET = None
    _NEWS_LOAD_TS = _t.time()
    return _NEWS_PARQUET


def _calc_news_tone_signal(stock_id: str, days: int = 30) -> float | None:
    """近 N 天 news sentiment confidence-weighted average → -1~+1。"""
    df = _load_news_parquet()
    if df is None or df.empty:
        return None
    try:
        import pandas as pd
        cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=days)
        sub = df[(df['ticker'].astype(str) == str(stock_id)) & (df['date'] >= cutoff)]
        if sub.empty:
            return None
        # confidence-weighted average (avoid low-confidence rows dominating)
        total_w = float(sub['confidence'].sum())
        if total_w <= 0:
            return None
        weighted = float((sub['sentiment'] * sub['confidence']).sum() / total_w)
        # 限縮到 [-1, 1] (sentiment LLM 預期就在這範圍但保險)
        return max(-1.0, min(1.0, weighted))
    except Exception as e:
        logger.debug("news_tone_signal 計算失敗 %s: %s", stock_id, e)
        return None


def _calc_price_ma_signal(stock_id: str) -> float | None:
    """股價 vs 5MA + 10MA 平均乖離 → -1~+1。"""
    try:
        from technical_analysis import load_and_resample
        _, df_day, _, _ = load_and_resample(stock_id)
        if df_day.empty or len(df_day) < 10:
            return None
        close = float(df_day['Close'].iloc[-1])
        ma5 = float(df_day['Close'].rolling(5).mean().iloc[-1])
        ma10 = float(df_day['Close'].rolling(10).mean().iloc[-1])
        if ma5 <= 0 or ma10 <= 0:
            return None
        bias5 = (close / ma5 - 1)
        bias10 = (close / ma10 - 1)
        avg_bias = (bias5 + bias10) / 2
        # 2% bias = saturate
        return math.tanh(avg_bias * 50)
    except Exception as e:
        logger.debug("price_ma_signal 計算失敗 %s: %s", stock_id, e)
        return None


def get_stock_sentiment_score(stock_id: str, chip_data=None) -> Dict[str, Any]:
    """個股情緒分數 -100~+100 (v1 含 News tone)。

    訊號:
      - 法人 5d (35%): 三大法人合計買賣超 / 1M shares tanh
      - 融資反向 (15%): 5d 融資餘額 % 變化 / 10% tanh，反號
      - 股價 vs MA (30%): 5MA + 10MA 平均乖離 × 50 tanh
      - News tone (20%): 近 30 天 news sentiment confidence-weighted (LLM 萃)

    News 不命中時退回 v0 三訊號，total_w 重 normalize 維持比例。
    可傳入既算好的 chip_data dict 避免重抓 (caller 復用最佳)。
    """
    weights = {'inst': 0.35, 'margin': 0.15, 'ma': 0.30, 'news': 0.20}
    signals: Dict[str, float] = {}

    # 取 chip 資料
    if chip_data is None:
        try:
            from chip_analysis import ChipAnalyzer
            chip_data = ChipAnalyzer().fetch_chip(stock_id, force_update=False)
        except Exception as e:
            logger.debug("ChipAnalyzer 抓 %s 失敗: %s", stock_id, e)
            chip_data = None

    if chip_data:
        v = _calc_inst_signal(chip_data.get('institutional'))
        if v is not None:
            signals['inst'] = v
        v = _calc_margin_signal(chip_data.get('margin'))
        if v is not None:
            signals['margin'] = v

    v = _calc_price_ma_signal(stock_id)
    if v is not None:
        signals['ma'] = v

    v = _calc_news_tone_signal(stock_id)
    if v is not None:
        signals['news'] = v

    if not signals:
        return {
            'score': 0.0, 'label': '中性 (no data)',
            'components': {}, 'note': 'no signal available',
        }

    # 加權平均 (用 valid weight 重新 normalize)
    total_w = sum(weights[k] for k in signals)
    if total_w <= 0:
        return {'score': 0.0, 'label': '中性 (no weight)',
                'components': signals, 'note': 'zero weight'}
    weighted = sum(signals[k] * weights[k] / total_w for k in signals)
    score = round(weighted * 100, 1)

    return {
        'score': score,
        'label': _stock_label(score),
        'components': {k: round(v, 3) for k, v in signals.items()},
        'available_signals': len(signals),
    }


# ============================================================
#  Streamlit UI helpers (Day 3, 2026-05-01)
# ============================================================

def _color_for_score(score: float) -> str:
    """-100~+100 → traffic light hex"""
    if score >= 50:
        return '#1e8e3e'  # 深綠
    if score >= 20:
        return '#34a853'  # 綠
    if score >= -20:
        return '#9aa0a6'  # 灰中性
    if score >= -50:
        return '#fbbc04'  # 橘
    return '#ea4335'  # 紅


def render_market_sentiment_block():
    """渲染只有市場情緒的 compact block（給 Mode D Thesis Panel 用）。"""
    try:
        import streamlit as st
    except ImportError:
        return
    m = get_market_sentiment_score()
    score = m['score']
    color = _color_for_score(score)
    cols = st.columns([1, 2])
    with cols[0]:
        st.markdown(
            f"<div style='font-size:0.85em;color:#666'>大盤情緒</div>"
            f"<div style='font-size:1.8em;font-weight:bold;color:{color}'>"
            f"{score:+.1f} <span style='font-size:0.6em;color:#999'>/100</span></div>"
            f"<div style='color:{color};font-weight:600'>{m['label']}</div>",
            unsafe_allow_html=True,
        )
    with cols[1]:
        comp = m.get('components', {})
        comp_lines = []
        for cn, label in [
            ('market_momentum', '市場動能'),
            ('market_breadth', '漲跌家數'),
            ('put_call_ratio', 'Put/Call'),
            ('volatility', '波動率'),
            ('margin_balance', '融資餘額'),
        ]:
            sub = comp.get(cn) or {}
            if isinstance(sub, dict) and sub.get('score') is not None:
                comp_lines.append(f"  - {label}: {sub['score']:.0f}/100")
        if comp_lines:
            st.markdown(
                "<div style='font-size:0.85em;color:#666;margin-bottom:6px'>"
                "TFGI 5 子指標 (0-100, 50=中性)</div>",
                unsafe_allow_html=True,
            )
            st.markdown("\n".join(comp_lines))
        if m.get('m1b_overlay'):
            ov = m['m1b_overlay']
            ov_color = '#ea4335' if ov['warn'] == 'overheat' else '#4285f4'
            st.markdown(
                f"<div style='color:{ov_color};font-weight:600'>⚠️ {ov['msg']}</div>",
                unsafe_allow_html=True,
            )
    if m.get('data_date'):
        st.caption(f"資料日: {m['data_date']}")


def render_sentiment_divergence_block(stock_id: str, chip_data=None):
    """渲染市場 vs 個股對比 compact block（給個股分析 tab 用）。

    3 columns: 市場 | 個股 | 對比訊號 + 訊號明細。
    """
    try:
        import streamlit as st
    except ImportError:
        return

    d = get_sentiment_divergence(stock_id, chip_data=chip_data)
    m, s = d['market'], d['stock']

    cols = st.columns(3)
    # 市場
    with cols[0]:
        c = _color_for_score(m['score'])
        st.markdown(
            f"<div style='font-size:0.85em;color:#666'>大盤情緒</div>"
            f"<div style='font-size:1.6em;font-weight:bold;color:{c}'>"
            f"{m['score']:+.1f}</div>"
            f"<div style='color:{c};font-weight:500;font-size:0.95em'>{m['label']}</div>",
            unsafe_allow_html=True,
        )
        if m.get('m1b_overlay'):
            ov = m['m1b_overlay']
            ov_color = '#ea4335' if ov['warn'] == 'overheat' else '#4285f4'
            st.markdown(
                f"<div style='color:{ov_color};font-size:0.85em'>⚠️ {ov['msg']}</div>",
                unsafe_allow_html=True,
            )
    # 個股
    with cols[1]:
        c = _color_for_score(s['score'])
        st.markdown(
            f"<div style='font-size:0.85em;color:#666'>個股情緒</div>"
            f"<div style='font-size:1.6em;font-weight:bold;color:{c}'>"
            f"{s['score']:+.1f}</div>"
            f"<div style='color:{c};font-weight:500;font-size:0.95em'>{s['label']}</div>",
            unsafe_allow_html=True,
        )
        comp = s.get('components', {})
        sig_lines = []
        for k, label in [
            ('inst', '法人 5d'),
            ('margin', '融資反向'),
            ('ma', 'MA 乖離'),
            ('news', 'News tone'),
        ]:
            if k in comp:
                sig_lines.append(f"  - {label}: {comp[k]:+.2f}")
        if sig_lines:
            st.markdown(
                "<div style='font-size:0.85em;color:#666;margin-top:4px'>訊號 -1~+1:</div>",
                unsafe_allow_html=True,
            )
            st.markdown("\n".join(sig_lines))
    # 對比
    with cols[2]:
        diff = d['diff']
        diff_c = _color_for_score(diff)  # 用 diff 自己的色
        signal = d['signal']
        # 強訊號標警示色
        if abs(diff) >= 50:
            label_color = '#1e8e3e' if diff > 0 else '#ea4335'
            label_prefix = '⚡' if diff > 0 else '⚠️'
        else:
            label_color = '#666'
            label_prefix = ''
        st.markdown(
            f"<div style='font-size:0.85em;color:#666'>個股 - 大盤</div>"
            f"<div style='font-size:1.6em;font-weight:bold;color:{diff_c}'>"
            f"{diff:+.1f}</div>"
            f"<div style='color:{label_color};font-weight:600'>{label_prefix} {signal}</div>",
            unsafe_allow_html=True,
        )


def get_sentiment_divergence(stock_id: str, chip_data=None) -> Dict[str, Any]:
    """市場 vs 個股對比訊號。

    Returns:
      market: dict (get_market_sentiment_score 結果)
      stock: dict (get_stock_sentiment_score 結果)
      diff: float (stock.score - market.score)
      signal: 4 級標籤
    """
    market = get_market_sentiment_score()
    stock = get_stock_sentiment_score(stock_id, chip_data=chip_data)

    diff = round(stock['score'] - market['score'], 1)

    if abs(diff) >= 50:
        if stock['score'] > market['score']:
            signal = '個股獨立催化 (good signal)'
        else:
            signal = '個股逆風 (warning)'
    elif abs(diff) >= 20:
        if stock['score'] > market['score']:
            signal = '個股相對強勢'
        else:
            signal = '個股相對弱勢'
    else:
        signal = '跟大盤同步'

    return {
        'market': market,
        'stock': stock,
        'diff': diff,
        'signal': signal,
    }
