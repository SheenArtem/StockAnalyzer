"""
劇本判斷 / 操作計畫 / 監控清單 — 從 analysis_engine.py TechnicalAnalyzer 抽出（M2 拆分）。

包含:
  - determine_scenario            — 根據趨勢分數 + ADX 修正產生劇本 A/B/C/D
  - generate_action_plan          — 進場區間 / 停損 / 停利 / RR / 型態確認信心
  - generate_monitoring_checklist — 🛑 停損、🚀 追價、🔭 未來觀察三區段動態警示
  - ActionPlan (Phase 2 治本)     — frozen dataclass with restricted iteration

`_safe_get` 共用 helper 也移到這裡，避免相依 analysis_engine 的靜態方法。

注意：原 `_determine_scenario(trend_score, daily_details)` 的 `daily_details` 參數
是 dead arg（函式內讀的是 `self.df_day`，從未用 daily_details）。抽出後拿掉。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict

import pandas as pd

logger = logging.getLogger(__name__)

# 與 analysis_engine.py 共用的門檻常數（保持同步）
DEFAULT_BUY_THRESHOLD = 3
DEFAULT_SELL_THRESHOLD = -2


# ============================================================
# ActionPlan dataclass (Phase 2 治本，2026-05-01)
#
# Background：council 5 視角共識，ai_report.py:590 用 `for k, v in ap.items()`
# 把整個 action_plan dict 攤平塞進 prompt，讓 Claude 看到
# sl_atr/sl_ma/sl_low/tp_list/sl_list 多 candidate，造成漂移
# (NVDA 報告同列三停損 candidate / 2330 列兩停損 + 第三組憑空生成)。
#
# Phase 1 (`4777f8e`) 在 ai_report.py 改成只丟 final 4，且 prompt 鎖 hard rule，
# 2345 實測 100% 服從。但結構上沒保護 — 任何新 caller 仍能 `for k, v in ap.items()`
# 攤平洩漏 candidate。
#
# Phase 2 提供「結構性保護」：
# - `__getitem__` / `.get()` 不限制（含 candidate 都能取）— 給 individual_view
#   完整支撐壓力清單顯示、momentum_screener QM 覆蓋計算等合法用例
# - `items()` / `keys()` / `__iter__` / `dict(ap)` 限制只給 _PUBLIC_FIELDS 14 個
#   → 治本：未來誰寫 `for k, v in ap.items()` 攤平給 LLM 也只洩漏 final + 上下文
# - `alternatives()` / `to_dict_full()` 顯式 method 給需要 candidate 的場景
#
# horizon / source 欄位：給未來 dispatch table 用（短線 / QM 波段 / value 左側）。
# ============================================================

# Public fields exposed via items() / keys() / __iter__
# 不含 candidate（sl_atr/sl_ma/sl_low/sl_key_candle/tp_list/sl_list）
_ACTION_PLAN_PUBLIC_FIELDS = (
    'is_actionable',
    'current_price',
    'rec_entry_low',
    'rec_entry_high',
    'rec_entry_desc',
    'rec_sl_price',
    'rec_sl_method',
    'rec_tp_price',
    'rr_ratio',
    'entry_confidence',
    'pattern_note',
    'strategy',
    'is_us_stock',
    'horizon',
    'source',
)


@dataclass(frozen=True)
class ActionPlan:
    """Frozen action plan with restricted iteration to prevent prompt leakage.

    See module docstring for design rationale.
    """
    # Final 4 數字
    rec_entry_low: float = 0.0
    rec_entry_high: float = 0.0
    rec_sl_price: float = 0.0
    rec_tp_price: float = 0.0

    # Final 上下文
    rec_entry_desc: str = ''
    rec_sl_method: str = 'N/A'
    rr_ratio: float = 0.0
    is_actionable: bool = False
    current_price: float = 0.0
    strategy: str = ''
    pattern_note: str = ''
    entry_confidence: str = 'standard'
    is_us_stock: bool = False

    # horizon / source — 給未來 dispatch table 用
    horizon: str = 'intraday'   # 'intraday' / 'swing_40_60d' (QM)
    source: str = 'scenario'    # 'scenario' / 'qm_override' / 'value_screener'

    # Candidates（Mapping 介面隱藏，僅 .get/__getitem__/alternatives() 可取）
    sl_atr: float = 0.0
    sl_ma: float = 0.0
    sl_low: float = 0.0
    sl_key_candle: float = 0.0
    tp_list: tuple = field(default_factory=tuple)
    sl_list: tuple = field(default_factory=tuple)

    # ---- Mapping 介面（dict-style backward compat）----
    def get(self, key, default=None):
        """Backward-compat dict.get() — 不限制 candidate access."""
        return getattr(self, key, default)

    def __getitem__(self, key):
        if not hasattr(self, key):
            raise KeyError(key)
        return getattr(self, key)

    def __contains__(self, key):
        return hasattr(self, key)

    def __iter__(self):
        """Yield public field 名稱（治本：dict(ap) / for k in ap 只給 public）."""
        return iter(_ACTION_PLAN_PUBLIC_FIELDS)

    def keys(self):
        return list(_ACTION_PLAN_PUBLIC_FIELDS)

    def values(self):
        return [getattr(self, k) for k in _ACTION_PLAN_PUBLIC_FIELDS]

    def items(self):
        return [(k, getattr(self, k)) for k in _ACTION_PLAN_PUBLIC_FIELDS]

    # ---- 顯式 method 給 candidate ----
    def alternatives(self):
        """Return all SL/TP candidates as dict.

        ⚠️ 不要餵給 LLM — 會造成漂移。給 momentum_screener QM 覆蓋計算 /
        individual_view 完整支撐壓力清單顯示 等合法 caller 用。
        """
        return {
            'sl_atr': self.sl_atr,
            'sl_ma': self.sl_ma,
            'sl_low': self.sl_low,
            'sl_key_candle': self.sl_key_candle,
            'tp_list': list(self.tp_list),
            'sl_list': list(self.sl_list),
        }

    def to_dict_full(self):
        """Full dict for JSON serialization (含 candidate)."""
        d = asdict(self)
        d['tp_list'] = list(self.tp_list)
        d['sl_list'] = list(self.sl_list)
        return d


def safe_get(series, key, default=0):
    """Get value from Series, returning default if key missing or value is NaN."""
    val = series.get(key, default)
    if pd.isna(val):
        return default
    return val


def determine_scenario(trend_score, df_day):
    """
    判斷劇本 Scenario A/B/C/D
    含 ADX 特殊修正：當日線趨勢方向與週線矛盾且 ADX > 30 時，修正劇本
    """
    scenario = {"code": "N", "title": "觀察中 (Neutral)", "color": "gray", "desc": "多空不明，建議觀望。"}

    if trend_score >= 3:
        scenario = {"code": "A", "title": "🔥 劇本 A：強力進攻", "color": "red", "desc": "週線強多 + 日線訊號佳，順勢重倉。"}
    elif 1 <= trend_score < 3:
        scenario = {"code": "B", "title": "⏳ 劇本 B：拉回關注", "color": "orange", "desc": "長線多頭，短線震盪。等待止穩。"}
    elif -2 <= trend_score <= 0:
        scenario = {"code": "C", "title": "⚠️ 劇本 C：反彈搶短", "color": "blue", "desc": "逆勢操作，嚴設停損。"}
    else:
        scenario = {"code": "D", "title": "🛑 劇本 D：空手/做空", "color": "green", "desc": "趨勢向下，切勿摸底。"}

    # === ADX 特殊修正 ===
    # 當日線 ADX > 30（強趨勢）且方向與週線劇本矛盾時，進行劇本修正
    if not df_day.empty and len(df_day) >= 20:
        current_day = df_day.iloc[-1]
        adx = safe_get(current_day, 'ADX', 0)
        plus_di = safe_get(current_day, '+DI', 0)
        minus_di = safe_get(current_day, '-DI', 0)

        if adx > 30:
            daily_bullish = plus_di > minus_di
            code = scenario['code']

            # 週線強多(A) + 日線強空 → 降級為 B（短線反轉風險高）
            if code == 'A' and not daily_bullish:
                scenario = {
                    "code": "B",
                    "title": "⏳ 劇本 B：拉回關注 (ADX 修正)",
                    "color": "orange",
                    "desc": f"週線多頭但日線 ADX={adx:.0f} 空方強勢，短線有回檔壓力，等待止穩。"
                }
                logger.info(f"Scenario A→B: daily ADX={adx:.1f}, -DI>+DI")

            # 週線偏多(B) + 日線強空 → 降級為 C（短線走弱）
            elif code == 'B' and not daily_bullish:
                scenario = {
                    "code": "C",
                    "title": "⚠️ 劇本 C：反彈搶短 (ADX 修正)",
                    "color": "blue",
                    "desc": f"週線偏多但日線 ADX={adx:.0f} 空方強勢，短線已走弱，嚴設停損。"
                }
                logger.info(f"Scenario B→C: daily ADX={adx:.1f}, -DI>+DI")

            # 週線偏空(C) + 日線強多 → 升級為 B（反彈動能強）
            elif code == 'C' and daily_bullish:
                scenario = {
                    "code": "B",
                    "title": "⏳ 劇本 B：拉回關注 (ADX 修正)",
                    "color": "orange",
                    "desc": f"週線偏空但日線 ADX={adx:.0f} 多方強攻，短線有反彈動能，可關注進場。"
                }
                logger.info(f"Scenario C→B: daily ADX={adx:.1f}, +DI>-DI")

            # 週線空頭(D) + 日線強多 → 升級為 C（可搶反彈）
            elif code == 'D' and daily_bullish:
                scenario = {
                    "code": "C",
                    "title": "⚠️ 劇本 C：反彈搶短 (ADX 修正)",
                    "color": "blue",
                    "desc": f"週線空頭但日線 ADX={adx:.0f} 多方反攻，可搶反彈但嚴設停損。"
                }
                logger.info(f"Scenario D→C: daily ADX={adx:.1f}, +DI>-DI")

    return scenario


def generate_monitoring_checklist(df, scenario, is_us_stock=False):
    """
    生成盤中監控與未來展望清單 (Dynamic Strategy Alerts)
    分為:
    1. 🛑 停損/調節 (Risk Control) -> 下跌觸發
    2. 🚀 追價/加碼 (Active Entry) -> 上漲觸發
    3. 🔭 未來觀察 (Future Opportunity) -> 等待特定條件
    """
    checklist = {
        "risk": [],
        "active": [],
        "future": []
    }

    if df.empty or len(df) < 60: return checklist

    current = df.iloc[-1]
    close = current['Close']
    ma5 = safe_get(current, 'MA5', 0)
    ma20 = safe_get(current, 'MA20', 0)
    ma60 = safe_get(current, 'MA60', 0)
    vol_ma5 = safe_get(current, 'Vol_MA5', 0)

    # --- 1. Risk Control (Stop Loss / Trim) ---
    # A. 破線停損
    if close > ma20:
        checklist['risk'].append(f"若收盤跌破 **月線 ({ma20:.2f})**，短期轉弱，建議減碼或停損。")
    elif close > ma60:
         checklist['risk'].append(f"若收盤跌破 **季線 ({ma60:.2f})**，波段轉弱，建議清倉觀望。")

    # B. 爆量長黑 — 使用成交值判斷，避免低價股灌量誤觸發
    tv_ma5 = safe_get(current, 'TV_MA5', 0)
    if tv_ma5 > 0:
        tv_threshold = tv_ma5 * 2
        if not is_us_stock:
            tv_display = f"{tv_threshold/1e8:,.1f} 億"
        else:
            tv_display = f"${tv_threshold/1e6:,.1f}M"
        checklist['risk'].append(f"若出現 **爆量長黑** (成交值 > {tv_display}) 且收跌，視為主力出貨訊號。")
    elif vol_ma5 > 0:
        # Fallback: TV_MA5 不存在時用傳統成交量
        vol_threshold = vol_ma5 * 2
        if not is_us_stock:
            vol_display = f"{vol_threshold/1000:,.0f} 張"
        else:
            vol_display = f"{vol_threshold:,.0f}"
        checklist['risk'].append(f"若出現 **爆量長黑** (成交量 > {vol_display}) 且收跌，視為主力出貨訊號。")

    # C. KD 高檔鈍化結束
    if safe_get(current, 'K', 0) > 80:
         checklist['risk'].append("指標位於高檔，若 KD 出現 **死亡交叉 (K<D)**，請獲利了結。")

    # --- 2. Active Entry (Add / Chase) ---
    # A. 突破前高
    recent_high = df['High'].iloc[-20:].max()
    if close < recent_high:
         checklist['active'].append(f"若帶量突破 **波段前高 ({recent_high:.2f})**，趨勢續攻，可嘗試加碼。")

    # B. 突破均線
    if close < ma20:
         checklist['active'].append(f"若帶量站上 **月線 ({ma20:.2f})**，短線翻多，可試單進場。")

    # --- 3. Future Opportunity (Watchlist) ---
    # A. 拉回買點 (Pullback)
    if close > ma20 * 1.05: # 正乖離過大
         checklist['future'].append(f"目前正乖離過大 ({((close/ma20)-1)*100:.1f}%)，不宜追高。等待 **拉回測 10日線** 不破時再佈局。")
    elif close > ma60 and close < ma20: # 在月季線之間整理
         checklist['future'].append(f"股價處於整理階段。若 **量縮回測季線 ({ma60:.2f})** 獲支撐收紅 K，為絕佳波段買點。")

    # B. 底部反轉 (Reversal)
    if close < ma60: # 空頭走勢
         checklist['future'].append("目前處於空頭趨勢。需等待 **底部形態 (如W底)** 出現，或 **站上月線** 後再考慮進場。")

    # C. 轉折訊號
    checklist['future'].append("持續關注 K 線形態，若出現 **晨星** 或 **多頭吞噬**，視為止跌訊號。")

    return checklist


def generate_action_plan(df, scenario, is_us_stock=False, strategy_params=None, trigger_score=0):
    """
    生成操作建議與風控數值
    (2025 Refined: Entry-based SL/TP, Conditionally Actionable)
    """
    if df.empty or len(df) < 20:
        return None

    current = df.iloc[-1]
    close_price = current['Close']
    code = scenario['code']

    # 1. Actionability & Entry Basis
    is_actionable = False
    entry_basis = close_price
    rec_entry_low = 0
    rec_entry_high = 0
    rec_entry_desc = "觀望"
    strategy_text = "觀望"

    # Indicators
    ma5 = safe_get(current, 'MA5', 0)
    ma10 = safe_get(current, 'MA10', 0)
    ma20 = safe_get(current, 'MA20', 0)
    ma60 = safe_get(current, 'MA60', 0)
    atr_val = safe_get(current, 'ATR', 0)
    sl_low = df['Low'].iloc[-20:].min()
    sl_ma = ma20

    # 關鍵紅K: 近20日最大量那根K棒的低點（真正的大量支撐）
    if 'Volume' in df.columns and len(df) >= 20:
        recent_20 = df.iloc[-20:]
        key_vol_idx = recent_20['Volume'].idxmax()
        sl_key = recent_20.loc[key_vol_idx, 'Low']
    else:
        sl_key = sl_low

    sl_atr = close_price - (2.0 * atr_val) if atr_val > 0 else close_price * 0.9
    sl_key_candle = sl_key

    # Default S/L Method
    rec_sl_method = "ATR 波動停損 (科學)" # Updated simplified name logic later if needed
    rec_sl_price = 0

    # [Optimization Override] - 由 run_analysis 層級處理 scenario 覆蓋，這裡讀取 optimizer 標記
    optimizer_active = False
    optimizer = scenario.get('optimizer')
    if optimizer == 'buy':
        optimizer_active = True
        is_actionable = True
        buy_th = strategy_params.get('buy', DEFAULT_BUY_THRESHOLD) if strategy_params else DEFAULT_BUY_THRESHOLD
        strategy_text = f"🔥 **AI 最佳化訊號 (買進)**：評分 ({trigger_score:.1f}) 已達買進門檻 ({buy_th})，建議進場。"
        rec_entry_low, rec_entry_high = close_price * 0.99, close_price * 1.01
        rec_entry_desc = "現價進場 (AI 訊號)"
        entry_basis = close_price
    elif optimizer == 'sell':
        optimizer_active = True
        is_actionable = False
        sell_th = strategy_params.get('sell', DEFAULT_SELL_THRESHOLD) if strategy_params else DEFAULT_SELL_THRESHOLD
        strategy_text = f"🛑 **AI 最佳化訊號 (賣出)**：評分 ({trigger_score:.1f}) 已達賣出門檻 ({sell_th})，建議出場觀望。"

    # Determine Scenario Intent (Only if not overridden by optimizer)
    if not optimizer_active:
        if code == 'A': # Active
            is_actionable = True
            if close_price > ma5 * 1.05 and ma5 > 0:
                # 乖離過大，等待拉回
                lo, hi = sorted([v for v in [ma10, ma5] if v > 0]) if ma10 > 0 and ma5 > 0 else (ma5 * 0.98, ma5)
                rec_entry_low, rec_entry_high = lo, hi
                rec_entry_desc = "等待拉回 (5MA-10MA)"
                entry_basis = ma5
                strategy_text = "🚀 **強勢股 (等待拉回)**：乖離過大，建議掛單在 5MA 附近接，不追高。"
            else:
                rec_entry_low, rec_entry_high = ma5 if ma5 > 0 else close_price * 0.99, close_price
                rec_entry_desc = "積極操作 (5MA-現價)"
                entry_basis = close_price
                strategy_text = "🚀 **積極進場**：趨勢強勁，目標看向波段滿足點。"

        elif code == 'B': # Mid-strength trend - 改為右側進場（VF-6 A 級驗證）
            # VF-6 驗證 2026-04-17：原「MA20/60 支撐掛單」pullback 邏輯跑輸純右側
            # mixed (trend+MA 支撐 AND) CAGR 11.8% vs pure_right CAGR 34.6%，差 +22.8pp
            # 改為右側進場：現價附近或小幅拉回 5MA，不再等深支撐
            is_actionable = True
            if close_price > ma5 * 1.05 and ma5 > 0:
                # 乖離過大，等小幅拉回
                lo, hi = sorted([v for v in [ma10, ma5] if v > 0]) if ma10 > 0 and ma5 > 0 else (ma5 * 0.98, ma5)
                rec_entry_low, rec_entry_high = lo, hi
                rec_entry_desc = "等拉回 (5MA-10MA)"
                entry_basis = ma5
                strategy_text = "⏳ **中等趨勢 (等拉回)**：乖離過大，等小幅拉回 5MA-10MA 接。VF-6 驗證：不要等月季線深支撐（跑輸 +22.8pp）。"
            else:
                # 現價附近進場
                rec_entry_low, rec_entry_high = ma5 if ma5 > 0 else close_price * 0.98, close_price
                rec_entry_desc = "右側進場 (5MA-現價)"
                entry_basis = close_price
                strategy_text = "🎯 **中等趨勢 (右側進場)**：VF-6 驗證純右側勝混合版 (CAGR +22.8pp)。"

        elif code == 'C': # Rebound
            is_actionable = True
            bb_lo = safe_get(current, 'BB_Lo', 0)
            rec_entry_low, rec_entry_high = sl_low * 0.99, (bb_lo if bb_lo > sl_low else sl_low * 1.02)
            rec_entry_desc = "抄底區間 (前低-布林下)"
            entry_basis = rec_entry_high
            strategy_text = "⚠️ **搶反彈**：逆勢操作風險高的。建議在布林下緣或前低嘗試。"
            rec_sl_method = "波段低點停損 (形態)" # Override default

        elif code == 'D':
            is_actionable = False
            strategy_text = "🛑 **空手觀望**：下方無支撐，不建議進場。"
        else:
            is_actionable = False
            strategy_text = "💤 **觀望**：多空分歧，等待方向明確。"

    # [MOVED] Construct Stop Loss List (sl_list) for UI - Calculate BEFORE actionable check
    # 重算 ATR 停損：基於 entry_basis 而非 close_price，與推薦值一致
    sl_atr_entry = entry_basis - (2.0 * atr_val) if atr_val > 0 else entry_basis * 0.9
    final_sl_list = []
    sl_candidates = [
        {"method": "A. ATR 波動停損 (科學)", "price": sl_atr_entry, "desc": "2倍 ATR"},
        {"method": "B. 均線停損 (趨勢)", "price": sl_ma, "desc": "MA20/60"},
        {"method": "C. 關鍵紅K (籌碼)", "price": sl_key, "desc": "大量低點"},
        {"method": "D. 波段低點停損 (形態)", "price": sl_low, "desc": "前波低點"}
    ]

    for item in sl_candidates:
        if item['price'] > 0: # Show all valid calculated supports
            diff = item['price'] - entry_basis
            loss_pct = (diff / entry_basis) * 100 if entry_basis > 0 else 0

            # Add note if broken
            note = item['desc']
            if diff > 0:
                 note += " (壓力/已破)"

            final_sl_list.append({
                "method": item['method'],
                "price": item['price'],
                "desc": note,
                "loss": round(loss_pct, 2)
            })

    # Sort by price descending (closest to current price first)
    final_sl_list.sort(key=lambda x: x['price'], reverse=True)

    if not is_actionable:
        return ActionPlan(
            current_price=close_price,
            strategy=strategy_text,
            is_actionable=False,
            is_us_stock=is_us_stock,
            entry_confidence="n/a",
            pattern_note="",
            rec_entry_low=0, rec_entry_high=0, rec_entry_desc="",
            rec_tp_price=0, rec_sl_price=0,
            tp_list=tuple(),
            sl_list=tuple(final_sl_list),
            rec_sl_method="N/A",
            sl_atr=sl_atr,
            sl_ma=sl_ma,
            sl_key_candle=sl_key_candle,
            sl_low=sl_low,
        )

    # --- Logic continues ONLY if actionable ---

    # 1. Stop Loss — 依劇本選擇合適方法
    if code == 'C':
        # 反彈搶短：用前波低點 -3% 作停損，緊貼進場價控制風險
        rec_sl_price = sl_low * 0.97 if sl_low > 0 else entry_basis * 0.93
        rec_sl_method = "D. 波段低點停損 (形態)"
    else:
        # A / B / Optimizer：標準 ATR 波動停損
        rec_sl_price = entry_basis - (2.0 * atr_val) if atr_val > 0 else entry_basis * 0.9
        rec_sl_method = "A. ATR 波動停損 (科學)"

    # 2. Take Profit (Based on Entry)
    recent_high_20 = df['High'].iloc[-20:].max()
    recent_low_20 = df['Low'].iloc[-20:].min()
    wave_height = recent_high_20 - recent_low_20
    bb_up = safe_get(current, 'BB_Up', 0)
    ma60 = safe_get(current, 'MA60', 0)
    ma120 = safe_get(current, 'MA120', 0)
    ma240 = safe_get(current, 'MA240', 0)

    tp_candidates = []
    tp_candidates.append({"method": "N 字測量 (1.0)", "price": entry_basis + wave_height, "desc": "等幅測距"})
    tp_candidates.append({"method": "費波南希 (1.618)", "price": entry_basis + (wave_height * 1.618), "desc": "強勢目標"})

    if ma60 > entry_basis: tp_candidates.append({"method": "MA60 季線反壓", "price": ma60, "desc": "生命線"})
    if ma120 > entry_basis: tp_candidates.append({"method": "MA120 半年線", "price": ma120, "desc": "長線反壓"})
    if ma240 > entry_basis: tp_candidates.append({"method": "MA240 年線", "price": ma240, "desc": "超級反壓"})
    if bb_up > entry_basis: tp_candidates.append({"method": "布林上緣", "price": bb_up, "desc": "通道壓力"})
    if recent_high_20 > entry_basis: tp_candidates.append({"method": "前波高點", "price": recent_high_20, "desc": "解套賣壓"})

    valid_candidates = [t for t in tp_candidates if t['price'] > entry_basis * 1.02]
    valid_candidates.sort(key=lambda x: x['price'])

    final_tp_list = []
    rec_tp_price = 0
    rec_method_name = ""

    if valid_candidates:
        if code == 'A':
            rec_cand = next((t for t in valid_candidates if "1.618" in t['method']), None)
            if not rec_cand: rec_cand = next((t for t in valid_candidates if "N 字" in t['method']), None)
            if rec_cand: rec_method_name = rec_cand['method']
        elif code == 'B':
            rec_cand = next((t for t in valid_candidates if "布林" in t['method']), None)
            if rec_cand: rec_method_name = rec_cand['method']
        elif code == 'C':
            # 反彈搶短：優先前波高點（解套賣壓），其次 MA60 季線反壓
            rec_cand = next((t for t in valid_candidates if "前波高點" in t['method']), None)
            if not rec_cand: rec_cand = next((t for t in valid_candidates if "MA60" in t['method']), None)
            if not rec_cand: rec_cand = next((t for t in valid_candidates if "N 字" in t['method']), None)
            if rec_cand: rec_method_name = rec_cand['method']

    for item in valid_candidates:
        is_rec = (item['method'] == rec_method_name)
        if is_rec: rec_tp_price = item['price']

        final_tp_list.append({
            "method": item['method'],
            "price": item['price'],
            "desc": item['desc'],
            "is_rec": is_rec
        })

    # Fallback if no valid candidates or no recommendation found
    if not final_tp_list:
         rec_tp_price = entry_basis * 1.1
         final_tp_list.append({"method": "🛡️ 短線獲利", "price": rec_tp_price, "desc": "預設 10%", "is_rec": True})
    elif not any(x['is_rec'] for x in final_tp_list):
         final_tp_list[0]['is_rec'] = True
         rec_tp_price = final_tp_list[0]['price']



    # ============================================================
    # PATTERN ENTRY FILTER — 型態確認進場信心
    # 型態不預測漲跌 (IC≈0)，但能定義進場信心與停損位
    # ============================================================
    entry_confidence = "standard"
    pattern_note = ""
    pattern_sl = None  # 型態衍生停損

    if is_actionable and len(df) >= 3:
        is_buy_direction = code in ('A', 'B', 'C')

        # 收集近 3 根 K 線的型態
        recent_patterns = []
        for offset in range(-3, 0):
            try:
                row = df.iloc[offset]
                pat = row.get('Pattern', None)
                pat_type = row.get('Pattern_Type', None)
                if pat and isinstance(pat, str) and pat not in ['None', 'nan', '']:
                    recent_patterns.append({
                        'name': pat, 'type': pat_type,
                        'low': row['Low'], 'high': row['High'],
                        'offset': offset
                    })
            except (IndexError, KeyError):
                pass

        if recent_patterns and is_buy_direction:
            # 優先看最近的型態
            last = recent_patterns[-1]

            if last['type'] == 'Bullish':
                entry_confidence = "high"
                pattern_note = f"K線型態【{last['name']}】確認多方進場"
                # 型態停損：該型態 K 棒的最低點
                if last['low'] > 0 and last['low'] < entry_basis:
                    pattern_sl = last['low'] * 0.99  # 留 1% buffer
            elif last['type'] == 'Bearish':
                entry_confidence = "wait"
                pattern_note = f"K線型態【{last['name']}】與買進方向矛盾，建議等待確認"

            # 特殊: Scenario C (搶反彈) 遇到看漲反轉型態 → 額外加強信心
            if code == 'C' and entry_confidence == 'high':
                reversal_patterns = ['Hammer', 'Morning Star', 'Engulfing', 'Piercing', '槌子', '晨星', '吞噬', '貫穿']
                if any(rp in last['name'] for rp in reversal_patterns):
                    pattern_note += " (反轉型態+搶反彈=高勝率)"

        # 加入型態停損到 SL 列表
        if pattern_sl and pattern_sl > 0:
            loss_pct = ((pattern_sl - entry_basis) / entry_basis) * 100 if entry_basis > 0 else 0
            final_sl_list.append({
                "method": "E. 型態停損 (K線)",
                "price": pattern_sl,
                "desc": f"型態低點",
                "loss": round(loss_pct, 2)
            })
            final_sl_list.sort(key=lambda x: x['price'], reverse=True)

        # 更新策略文字
        if pattern_note:
            if entry_confidence == "high":
                strategy_text += f"\n\n**進場信心: 高** — {pattern_note}"
            elif entry_confidence == "wait":
                strategy_text += f"\n\n**進場信心: 等待確認** — {pattern_note}"

    # Calculate Risk-Reward Ratio (RR)
    rr_ratio = 0.0
    if is_actionable and entry_basis > 0 and rec_sl_price > 0:
        potential_reward = rec_tp_price - entry_basis
        potential_risk = entry_basis - rec_sl_price
        if potential_risk > 0:
            rr_ratio = potential_reward / potential_risk

    return ActionPlan(
        current_price=close_price,
        strategy=strategy_text,
        is_actionable=True,
        is_us_stock=is_us_stock,
        entry_confidence=entry_confidence,
        pattern_note=pattern_note,
        rec_entry_low=rec_entry_low,
        rec_entry_high=rec_entry_high,
        rec_entry_desc=rec_entry_desc,
        rec_sl_method=rec_sl_method,
        rec_sl_price=rec_sl_price,
        rec_tp_price=rec_tp_price,
        rr_ratio=rr_ratio,
        tp_list=tuple(final_tp_list),
        sl_list=tuple(final_sl_list),
        sl_atr=sl_atr,
        sl_ma=sl_ma,
        sl_key_candle=sl_key,
        sl_low=sl_low,
    )
