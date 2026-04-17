"""
exit_manager.py -- 統一出場策略管理

Phase 1: 統一 SL/TP 計算介面（position_monitor + momentum_screener 共用）
Phase 2: 依 ATR% 動態調整停損停利（高波動放寬、低波動收緊）
Phase 3: 防甩轎（緩衝期 + 連續跌破 + 量能確認）+ break-even 保護
Phase 4: Regime overlay（HMM 市場狀態調整 SL/TP 乘數）

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
DEFAULT_MIN_SL_GAP = 0.03         # 停損距進場至少 3%（下限）
MIN_SL_GAP_ATR_MULT = 1.5         # VF-1 驗證：高 ATR 股需至少 1.5 個日 ATR 距離
DEFAULT_TP_PCTS = (0.15, 0.25, 0.40)  # +15%, +25%, +40%


def compute_min_sl_gap(atr_pct=None):
    """VF-1 驗證：動態 min_sl_gap = max(3%, atr% × 1.5)。

    高波動股（ATR > 2%）將提高門檻避免 MA20W 距 entry 過近被噪音打穿。
    低波動 / 未提供 atr_pct 時退回固定 3%。
    驗證報告：reports/vf1_min_sl_gap_validation.md（B 級，勝率 +8.3pp、R:R>5 虛高率 -71%）。
    """
    if atr_pct is None or atr_pct <= 0:
        return DEFAULT_MIN_SL_GAP
    return max(DEFAULT_MIN_SL_GAP, atr_pct * MIN_SL_GAP_ATR_MULT / 100.0)

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
MA20_BREAK_ATR_MULT = 1.2         # MA20 跌破容忍 = atr% × 此倍數（clip MA20_BREAK_FLOOR~CEIL）
MA20_BREAK_FLOOR = 0.02           # 低波動下限 2%
MA20_BREAK_CEIL = 0.05            # 高波動上限 5%

# ============================================================
#  Phase 3 防甩轎 + Break-even
# ============================================================
GRACE_PERIOD_DAYS = 5              # 進場後 5 個交易日內不觸發硬停損
CONSEC_BREACH_DAYS = 2             # 需連續 N 日收盤跌破才確認
VOLUME_CONFIRM_RATIO = 0.8        # 量 < 均量 × 此比例 → 視為洗盤（降為 soft）
BREAKEVEN_TRIGGER_PCT = 0.08      # 獲利 >= 8% 後停損提升至成本價（Phase 2 會動態調整）
BREAKEVEN_ATR_MULTIPLIER = 3.0    # breakeven_trigger = atr_pct * multiplier (動態)

# ============================================================
#  Phase 4 Regime Overlay — ⚠️ 已停用（VF-G3 Part 1 驗證 D 級，2026-04-17）
# ============================================================
# 驗證結果：8 組 regime 乘數跑輸 (1.0, 1.0)：
#   - V1 vs V2 delta mean -0.13%, Sharpe -0.006
#   - Walk-forward 61 windows 中 V1 只勝 17 次（28%，跨期不穩）
#   - Volatile (最大樣本 n=5349) 的 (1.2, 0.8) 反而拖累 mean -0.343%
# 參數保留成 dict 以便 regression 時復活比對，但預設全 (1.0, 1.0)。
# 報告：reports/vfg3_part1_regime_exit_mult.md
REGIME_EXIT_MULT = {
    'trending': (1.00, 1.00),
    'ranging':  (1.00, 1.00),
    'volatile': (1.00, 1.00),
    'neutral':  (1.00, 1.00),
}


def compute_exit_plan(entry_price, weekly_ma20=None, atr_pct=None,
                      regime=None, min_sl_gap=None):
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
    regime : str or None
        HMM 市場狀態 ('trending'/'ranging'/'volatile'/'neutral')。
        ⚠️ Phase 4 regime overlay 已停用（VF-G3 Part 1 驗證 D 級）。
        參數保留供 UI 顯示與未來研究，不影響 SL/TP 計算。
    min_sl_gap : float or None
        停損距進場最小距離（比例），避免噪音觸發。None 時依 atr_pct 動態計算
        （VF-1 驗證：max(3%, atr% × 1.5)）；傳入數字則強制覆寫。

    Returns
    -------
    dict with keys:
        stop_loss       : float  -- 建議停損價
        stop_loss_pct   : float  -- 停損百分比（負值，如 -0.08）
        stop_method     : str    -- 停損方法描述
        hard_stop       : float  -- 硬停損價（純百分比）
        hard_stop_pct   : float  -- 硬停損百分比
        tp_levels       : list   -- [{price, pct, action}, ...]
        tp_scale        : float  -- TP 縮放因子（Phase 2 × Phase 4）
        regime_sl_mult  : float  -- regime SL 乘數（Phase 4）
        regime_tp_mult  : float  -- regime TP 乘數（Phase 4）
        method          : str    -- 'fixed' | 'atr_dynamic'
    """
    if entry_price <= 0:
        return _empty_plan()

    # VF-1：min_sl_gap 未指定時依 atr_pct 動態
    if min_sl_gap is None:
        min_sl_gap = compute_min_sl_gap(atr_pct)

    # Phase 4: regime overlay 乘數
    sl_mult, tp_mult = REGIME_EXIT_MULT.get(regime, (1.0, 1.0))

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

    # Phase 4: regime 調整停損（乘數 > 1 = 放寬 = stop_pct 加大）
    stop_pct = np.clip(stop_pct * sl_mult, ATR_STOP_FLOOR, ATR_STOP_CEIL)

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

    # Phase 4: regime 調整停利（乘數 > 1 = 放寬 = TP 目標更遠）
    tp_scale = np.clip(tp_scale * tp_mult, ATR_TP_SCALE_FLOOR, ATR_TP_SCALE_CEIL)

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
        'regime_sl_mult': round(sl_mult, 2),
        'regime_tp_mult': round(tp_mult, 2),
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
        # 高波動給更寬容忍（VF-G1 TODO：此倍數與 floor/ceil 待 grid search 驗證）
        break_pct = np.clip(atr_pct / 100.0 * MA20_BREAK_ATR_MULT,
                            MA20_BREAK_FLOOR, MA20_BREAK_CEIL)
    else:
        break_pct = DEFAULT_MA20_BREAK_PCT

    return round(weekly_ma20 * (1 - break_pct), 2)


def compute_breakeven_stop(entry_price, current_price, hard_stop, atr_pct=None):
    """
    Break-even 保護：獲利達門檻後，停損提升至成本價。

    Parameters
    ----------
    entry_price : float
    current_price : float
    hard_stop : float  -- 原始硬停損價
    atr_pct : float or None

    Returns
    -------
    float : 調整後的停損價（可能等於原 hard_stop 或提升至 entry_price）
    """
    if entry_price <= 0 or current_price <= 0:
        return hard_stop

    pnl_pct = (current_price / entry_price) - 1.0

    # 動態門檻：高波動需要更多空間才啟動 breakeven
    if atr_pct is not None and atr_pct > 0:
        trigger = np.clip(atr_pct / 100.0 * BREAKEVEN_ATR_MULTIPLIER, 0.05, 0.15)
    else:
        trigger = BREAKEVEN_TRIGGER_PCT

    if pnl_pct >= trigger:
        return max(hard_stop, entry_price)
    return hard_stop


def check_stop_breach(closes, volumes, threshold):
    """
    Phase 3 防甩轎：檢查近期收盤價是否確認跌破停損。

    回傳 (confirmed, detail_dict):
      - confirmed=True:  連續 N 日收盤跌破 + 量能確認 → 觸發 hard
      - confirmed=False + detail: 跌破但未確認 → 可作 soft 警報
      - confirmed=None:  未跌破

    Parameters
    ----------
    closes : array-like  -- 近 N 日收盤價（最少 CONSEC_BREACH_DAYS 筆，最後一筆為今日）
    volumes : array-like  -- 同期成交量
    threshold : float     -- 停損價格

    Returns
    -------
    tuple: (bool or None, dict)
    """
    import pandas as pd

    closes = pd.Series(closes).dropna()
    volumes = pd.Series(volumes).dropna()
    n = CONSEC_BREACH_DAYS

    if len(closes) < 1:
        return None, {}

    current = float(closes.iloc[-1])
    if current >= threshold:
        return None, {}

    # 今日收盤跌破 → 看連續性
    recent = closes.tail(n)
    breach_count = int((recent < threshold).sum())
    all_breached = breach_count >= n and len(recent) >= n

    # 量能確認（今日量 vs 20 日均量）
    vol_confirmed = True
    avg_vol = 0.0
    if len(volumes) >= 20:
        avg_vol = float(volumes.iloc[-21:-1].mean())  # 不含今日
        today_vol = float(volumes.iloc[-1])
        if avg_vol > 0 and today_vol < avg_vol * VOLUME_CONFIRM_RATIO:
            vol_confirmed = False

    detail = {
        'current': current,
        'threshold': threshold,
        'breach_days': breach_count,
        'required_days': n,
        'vol_confirmed': vol_confirmed,
        'avg_vol': round(avg_vol),
        'today_vol': round(float(volumes.iloc[-1])) if len(volumes) > 0 else 0,
    }

    if all_breached and vol_confirmed:
        return True, detail    # confirmed hard stop
    else:
        return False, detail   # breach but not confirmed (soft warning)


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
        'regime_sl_mult': 1.0,
        'regime_tp_mult': 1.0,
        'method': 'fixed',
    }
