"""
exit_manager.py -- 統一出場策略管理

Phase 1: 統一 SL/TP 計算介面（position_monitor + momentum_screener 共用）
Phase 2: 依 ATR% 動態調整停損停利（高波動放寬、低波動收緊）

使用者:
  - position_monitor.py  -- 每日監控停損閾值
  - momentum_screener.py -- QM action_plan 生成 SL/TP
  - (未來) backtest_engine.py -- 回測出場邏輯
"""

import numpy as np

# ============================================================
#  Phase 1 預設值（與重構前硬編碼相同）
# ============================================================
DEFAULT_HARD_STOP_PCT = 0.08       # -8%
DEFAULT_MA20_BREAK_PCT = 0.03      # 週 MA20 跌破 3% 才觸發
DEFAULT_MIN_SL_GAP = 0.03         # 停損距進場至少 3%
DEFAULT_TP_PCTS = (0.15, 0.25, 0.40)  # +15%, +25%, +40%

# ============================================================
#  Phase 2 ATR% 動態參數
# ============================================================
# ATR% 台股典型分布: 1%-2% (低波動金融/傳產)、2%-3% (中等)、3%-5%+ (高波動生技/小型)
ATR_PCT_MEDIAN = 2.5              # 中位數基準（%）
ATR_STOP_FLOOR = 0.05             # 停損下限 -5%
ATR_STOP_CEIL = 0.14              # 停損上限 -14%
ATR_STOP_MULTIPLIER = 3.0         # stop_pct = atr_pct * multiplier
ATR_TP_SCALE_FLOOR = 0.7          # 低波動: TP 打 7 折
ATR_TP_SCALE_CEIL = 1.6           # 高波動: TP 放大 1.6 倍


def compute_exit_plan(entry_price, weekly_ma20=None, atr_pct=None,
                      min_sl_gap=DEFAULT_MIN_SL_GAP):
    """
    計算停損 + 停利計畫。

    Parameters
    ----------
    entry_price : float
        進場價格
    weekly_ma20 : float or None
        週 MA20 值（可選，作為趨勢停損參考）
    atr_pct : float or None
        ATR / Close × 100（%）。提供時啟用 Phase 2 動態調整。
        例：2.5 表示每日平均振幅為價格的 2.5%。
    min_sl_gap : float
        停損距進場最小距離（比例），避免噪音觸發

    Returns
    -------
    dict with keys:
        stop_loss       : float  -- 建議停損價
        stop_loss_pct   : float  -- 停損百分比（負值，如 -0.08）
        stop_method     : str    -- 停損方法描述
        hard_stop       : float  -- 硬停損價（純百分比）
        hard_stop_pct   : float  -- 硬停損百分比
        tp_levels       : list   -- [{price, pct, action}, ...]
        tp_scale        : float  -- TP 縮放因子（Phase 2）
        method          : str    -- 'fixed' | 'atr_dynamic'
    """
    if entry_price <= 0:
        return _empty_plan()

    # --- 停損計算 ---
    if atr_pct is not None and atr_pct > 0:
        # Phase 2: ATR% 動態
        stop_pct = np.clip(atr_pct / 100.0 * ATR_STOP_MULTIPLIER,
                           ATR_STOP_FLOOR, ATR_STOP_CEIL)
        method = 'atr_dynamic'
    else:
        # Phase 1: 固定 -8%
        stop_pct = DEFAULT_HARD_STOP_PCT
        method = 'fixed'

    hard_stop = entry_price * (1 - stop_pct)

    # 週 MA20 趨勢停損（取較高者，但距離需 >= min_sl_gap）
    stop_loss = hard_stop
    stop_method = f"-{stop_pct*100:.1f}% 硬停損"

    if weekly_ma20 is not None and 0 < weekly_ma20 < entry_price:
        ma20_gap = (entry_price - weekly_ma20) / entry_price
        if weekly_ma20 > hard_stop and ma20_gap >= min_sl_gap:
            stop_loss = weekly_ma20
            stop_method = "週 MA20 趨勢停損"

    stop_loss_pct = (stop_loss / entry_price) - 1.0

    # --- 停利計算 ---
    if atr_pct is not None and atr_pct > 0:
        # Phase 2: 高波動放大、低波動縮小
        tp_scale = np.clip(atr_pct / ATR_PCT_MEDIAN,
                           ATR_TP_SCALE_FLOOR, ATR_TP_SCALE_CEIL)
    else:
        tp_scale = 1.0

    tp_levels = []
    actions = [
        "減碼 1/3，落袋第一段",
        "移動停損至週 MA10",
        "清倉 (或持倉到期)",
    ]
    for i, base_pct in enumerate(DEFAULT_TP_PCTS):
        scaled_pct = base_pct * tp_scale
        tp_price = entry_price * (1 + scaled_pct)
        tp_levels.append({
            'price': round(tp_price, 2),
            'pct': round(scaled_pct * 100, 1),
            'action': actions[i] if i < len(actions) else '',
        })

    return {
        'stop_loss': round(stop_loss, 2),
        'stop_loss_pct': round(stop_loss_pct, 4),
        'stop_method': stop_method,
        'hard_stop': round(hard_stop, 2),
        'hard_stop_pct': round(-stop_pct, 4),
        'tp_levels': tp_levels,
        'tp_scale': round(tp_scale, 2),
        'method': method,
    }


def compute_ma20_break_threshold(weekly_ma20, atr_pct=None):
    """
    計算週 MA20 跌破閾值。

    Phase 1: 固定 MA20 × 0.97（-3%）
    Phase 2: 依 ATR% 調整容忍度（高波動放寬到 -5%，低波動收緊到 -2%）

    Returns
    -------
    float : 觸發價格（低於此價 = 破位）
    """
    if weekly_ma20 is None or weekly_ma20 <= 0:
        return 0.0

    if atr_pct is not None and atr_pct > 0:
        # 高波動給更寬容忍：break_pct 範圍 2% ~ 5%
        break_pct = np.clip(atr_pct / 100.0 * 1.2, 0.02, 0.05)
    else:
        break_pct = DEFAULT_MA20_BREAK_PCT

    return round(weekly_ma20 * (1 - break_pct), 2)


def _empty_plan():
    """entry_price invalid 時的空回傳。"""
    return {
        'stop_loss': 0.0,
        'stop_loss_pct': 0.0,
        'stop_method': '',
        'hard_stop': 0.0,
        'hard_stop_pct': 0.0,
        'tp_levels': [],
        'tp_scale': 1.0,
        'method': 'fixed',
    }
