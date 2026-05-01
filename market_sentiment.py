"""市場 vs 個股情緒分數 (2026-05-01 Day 1)

Two scores in -100~+100 range:
  - 市場情緒 (get_market_sentiment_score): 復用 TaiwanFearGreedIndex (5 子指標
    Market Momentum / Breadth / PCR / Volatility / Margin) 0-100 → 線性 map
    到 -100~+100。M1B/成交比過熱當 overlay warning。
  - 個股情緒 v0 (get_stock_sentiment_score): 純訊號組合 no LLM。
    法人 5d 買賣超 (40%) + 融資增減反向 (20%) + 股價 vs 5/10MA (40%)。
  - 對比 (get_sentiment_divergence): 兩 score 差值 → 4 種訊號標籤。

Day 2 將擴 News tone (LLM) 加進個股情緒；目前 v0 不打 LLM。
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
    """個股情緒分數 -100~+100 (no LLM, v0)。

    訊號:
      - 法人 5d (40%): 三大法人合計買賣超 / 1M shares tanh
      - 融資反向 (20%): 5d 融資餘額 % 變化 / 10% tanh，反號
      - 股價 vs MA (40%): 5MA + 10MA 平均乖離 × 50 tanh

    可傳入既算好的 chip_data dict 避免重抓 (caller 復用最佳)。
    """
    weights = {'inst': 0.40, 'margin': 0.20, 'ma': 0.40}
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
