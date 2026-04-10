import pandas as pd
import numpy as np
import logging
from scipy.signal import argrelextrema

# Configure logging
logger = logging.getLogger(__name__)

class TechnicalAnalyzer:
    def __init__(self, ticker, df_week, df_day, strategy_params=None, chip_data=None, us_chip_data=None):
        self.ticker = ticker
        self.df_week = df_week
        self.df_day = df_day
        self.strategy_params = strategy_params # { 'buy': 3, 'sell': -2 }
        self.chip_data = chip_data  # 台股籌碼數據
        self.us_chip_data = us_chip_data  # 美股籌碼數據
        
        # 判斷是否為美股
        self._is_us_stock = self._detect_us_stock(ticker)
    
    def _detect_us_stock(self, ticker):
        """
        判斷是否為美股
        """
        if not ticker:
            return False
        
        ticker = ticker.upper().strip()
        
        # 台股特徵: 數字或 .TW/.TWO 結尾
        if ticker.isdigit():
            return False
        if ticker.endswith('.TW') or ticker.endswith('.TWO'):
            return False
        
        # ADR 如 TSM 也算美股
        # 其他英文代號視為美股
        if ticker.replace('.', '').replace('-', '').isalpha():
            return True
        
        return False

    @staticmethod
    def _safe_get(series, key, default=0):
        """Get value from Series, returning default if key missing or value is NaN."""
        val = series.get(key, default)
        if pd.isna(val):
            return default
        return val

    def run_analysis(self):
        """
        執行完整分析流程
        Returns:
            dict: 包含 趨勢分數, 觸發分數, 劇本, 詳細評分項目
        """
        trend_score, trend_details = self._calculate_trend_score(self.df_week)
        # 傳入趨勢分數以啟用籌碼動態權重
        trigger_score, trigger_details, trigger_breakdown = self._calculate_trigger_score(self.df_day, trend_score=trend_score)

        scenario = self._determine_scenario(trend_score, trigger_details)

        # 3.5 Strategy Optimizer Override (覆蓋劇本，確保劇本卡與策略建議一致)
        if self.strategy_params:
            buy_th = self.strategy_params.get('buy', 3)
            sell_th = self.strategy_params.get('sell', -2)
            if trigger_score >= buy_th:
                scenario = {
                    "code": "A",
                    "title": "🔥 劇本 A：AI 最佳化買進",
                    "color": "red",
                    "desc": f"AI 評分 ({trigger_score:.1f}) 達買進門檻 ({buy_th})，趨勢+訊號共振，建議積極進場。",
                    "optimizer": "buy"
                }
            elif trigger_score <= sell_th:
                scenario = {
                    "code": "D",
                    "title": "🛑 劇本 D：AI 最佳化賣出",
                    "color": "green",
                    "desc": f"AI 評分 ({trigger_score:.1f}) 達賣出門檻 ({sell_th})，建議出場觀望。",
                    "optimizer": "sell"
                }

        # 4. 操作劇本與風控 (Action Plan & Risk)
        action_plan = self._generate_action_plan(self.df_day, scenario, trigger_score)
        
        # 5. [NEW] Dynamic Monitoring Checklist (Conditional Alerts)
        checklist = self._generate_monitoring_checklist(self.df_day, scenario)
        
        return {
            "ticker": self.ticker,
            "trend_score": trend_score,
            "trend_details": trend_details,
            "trigger_score": trigger_score,
            "trigger_details": trigger_details,
            "trigger_breakdown": trigger_breakdown,
            "scenario": scenario,
            "action_plan": action_plan,
            "checklist": checklist
        }

    def _generate_monitoring_checklist(self, df, scenario):
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
        ma5 = self._safe_get(current, 'MA5', 0)
        ma20 = self._safe_get(current, 'MA20', 0)
        ma60 = self._safe_get(current, 'MA60', 0)
        vol_ma5 = self._safe_get(current, 'Vol_MA5', 0)

        # --- 1. Risk Control (Stop Loss / Trim) ---
        # A. 破線停損
        if close > ma20:
            checklist['risk'].append(f"若收盤跌破 **月線 ({ma20:.2f})**，短期轉弱，建議減碼或停損。")
        elif close > ma60:
             checklist['risk'].append(f"若收盤跌破 **季線 ({ma60:.2f})**，波段轉弱，建議清倉觀望。")

        # B. 爆量長黑
        if vol_ma5 > 0:
            vol_threshold = vol_ma5 * 2
            if not self._is_us_stock:
                vol_display = f"{vol_threshold/1000:,.0f} 張"
            else:
                vol_display = f"{vol_threshold:,.0f}"
            checklist['risk'].append(f"若出現 **爆量長黑** (成交量 > {vol_display}) 且收跌，視為主力出貨訊號。")

        # C. KD 高檔鈍化結束
        if self._safe_get(current, 'K', 0) > 80:
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

    def _generate_action_plan(self, df, scenario, trigger_score=0):
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
        ma5 = self._safe_get(current, 'MA5', 0)
        ma10 = self._safe_get(current, 'MA10', 0)
        ma20 = self._safe_get(current, 'MA20', 0)
        ma60 = self._safe_get(current, 'MA60', 0)
        atr_val = self._safe_get(current, 'ATR', 0)
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
            buy_th = self.strategy_params.get('buy', 3) if self.strategy_params else 3
            strategy_text = f"🔥 **AI 最佳化訊號 (買進)**：評分 ({trigger_score:.1f}) 已達買進門檻 ({buy_th})，建議進場。"
            rec_entry_low, rec_entry_high = close_price * 0.99, close_price * 1.01
            rec_entry_desc = "現價進場 (AI 訊號)"
            entry_basis = close_price
        elif optimizer == 'sell':
            optimizer_active = True
            is_actionable = False
            sell_th = self.strategy_params.get('sell', -2) if self.strategy_params else -2
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
                
            elif code == 'B': # Pullback (Actionable Limit Buy)
                is_actionable = True
                support_candidates = [m for m in [ma20, ma60] if m > 0]
                support = min(support_candidates) if support_candidates else close_price * 0.95
                rec_entry_low, rec_entry_high = support * 0.98, support * 1.02
                rec_entry_desc = "回測支撐 (月季線)"
                entry_basis = support
                strategy_text = "⏳ **等待訊號**：建議掛單在月季線支撐附近，不要追高。"

            elif code == 'C': # Rebound
                is_actionable = True
                bb_lo = self._safe_get(current, 'BB_Lo', 0)
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
             return {
                "current_price": close_price,
                "strategy": strategy_text,
                "is_actionable": False,
                "is_us_stock": self._is_us_stock,
                "rec_entry_low": 0, "rec_entry_high": 0, "rec_entry_desc": "",
                "rec_tp_price": 0, "rec_sl_price": 0,
                "tp_list": [],
                "sl_list": final_sl_list,
                "rec_sl_method": "N/A",
                "sl_atr": sl_atr,
                "sl_ma": sl_ma,
                "sl_key_candle": sl_key_candle,
                "sl_low": sl_low
            }
            
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
        bb_up = self._safe_get(current, 'BB_Up', 0)
        ma60 = self._safe_get(current, 'MA60', 0)
        ma120 = self._safe_get(current, 'MA120', 0)
        ma240 = self._safe_get(current, 'MA240', 0)

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



        # Calculate Risk-Reward Ratio (RR)
        rr_ratio = 0.0
        if is_actionable and entry_basis > 0 and rec_sl_price > 0:
            potential_reward = rec_tp_price - entry_basis
            potential_risk = entry_basis - rec_sl_price
            if potential_risk > 0:
                rr_ratio = potential_reward / potential_risk

        # Position Sizing (部位管理計算)
        # 2% 法則: 單筆風險不超過總資金的 2%
        position_sizing = {}
        if is_actionable and entry_basis > 0 and rec_sl_price > 0:
            risk_per_share = entry_basis - rec_sl_price
            if risk_per_share > 0:
                if self._is_us_stock:
                    # 美股: 以股為單位，資金以 USD 計
                    for capital in [10000, 50000, 100000]:
                        max_risk = capital * 0.02
                        shares = int(max_risk / risk_per_share)
                        if shares > 0:
                            cost = shares * entry_basis
                            loss_if_stopped = shares * risk_per_share
                            position_sizing[capital] = {
                                "lots": shares,
                                "shares": shares,
                                "cost": cost,
                                "risk_amount": loss_if_stopped,
                                "risk_pct": (loss_if_stopped / capital * 100) if capital > 0 else 0
                            }
                else:
                    # 台股: 1張=1000股
                    for capital in [500000, 1000000, 3000000]:
                        max_risk = capital * 0.02
                        shares = int(max_risk / risk_per_share)
                        lots = shares // 1000
                        cost = lots * 1000 * entry_basis
                        loss_if_stopped = lots * 1000 * risk_per_share
                        position_sizing[capital] = {
                            "lots": lots,
                            "shares": lots * 1000,
                            "cost": cost,
                            "risk_amount": loss_if_stopped,
                            "risk_pct": (loss_if_stopped / capital * 100) if capital > 0 else 0
                        }

        return {
            "current_price": close_price,
            "strategy": strategy_text,
            "is_actionable": True,
            "is_us_stock": self._is_us_stock,
            "rec_entry_low": rec_entry_low,
            "rec_entry_high": rec_entry_high,
            "rec_entry_desc": rec_entry_desc,
            "rec_sl_method": rec_sl_method,
            "rec_sl_price": rec_sl_price,
            "rec_tp_price": rec_tp_price,
            "rr_ratio": rr_ratio,
            "tp_list": final_tp_list,
            "sl_list": final_sl_list,
            "sl_atr": sl_atr,
            "sl_ma": sl_ma,
            "sl_key_candle": sl_key,
            "sl_low": sl_low,
            "position_sizing": position_sizing
        }
        




    def _calculate_trend_score(self, df):
        """
        計算週線趨勢分數 (Trend Score)
        範圍: -5 ~ +5 (clamp)
        因子: MA架構(±2), DMI(±1), OBV(±1), EFI(±1,含死區), 形態學(±2,cap), 量價(±1)
        """
        score = 0
        details = []

        if df.empty or len(df) < 5:
            return 0, ["數據不足"]

        current = df.iloc[-1]
        prev = df.iloc[-2]

        # 1. 均線架構 (MA Structure)
        # 多頭排列: 收盤 > MA20 > MA60
        close = self._safe_get(current, 'Close', 0)
        ma20 = self._safe_get(current, 'MA20', 0)
        ma60 = self._safe_get(current, 'MA60', 0)
        adx = self._safe_get(current, 'ADX', 0)
        plus_di = self._safe_get(current, '+DI', 0)
        minus_di = self._safe_get(current, '-DI', 0)

        if close > ma20 and ma20 > ma60:
            score += 2
            details.append("✅ 週線均線多頭排列 (Close > 20MA > 60MA) (+2)")
        elif close > ma20:
            score += 1
            details.append("✅ 股價站上週 20MA (+1)")
        elif close < ma20 and ma20 < ma60:
            score -= 2
            details.append("🔻 均線空頭排列 (Close < 20MA < 60MA) (-2)")
        else:
            details.append("⚠️ 均線糾結混亂 (0)")

        # 2. DMI 趨勢強度
        if adx > 25:
            if plus_di > minus_di:
                score += 1
                details.append(f"✅ DMI 多方趨勢成形 (ADX={adx:.1f} > 25, +DI > -DI) (+1)")
            else:
                score -= 1
                details.append(f"🔻 DMI 空方趨勢成形 (ADX={adx:.1f} > 25, -DI > +DI) (-1)")
        else:
            details.append(f"⚠️ DMI 趨勢不明 (ADX={adx:.1f} < 25) (0)")

        # 3. OBV 能量潮 (比較近5週趨勢) — 對稱化 ±1
        try:
            obv_5w_ago = df['OBV'].iloc[-5]
            if self._safe_get(current, 'OBV', 0) > obv_5w_ago:
                score += 1
                details.append("✅ OBV 能量潮近 5 週上升 (+1)")
            else:
                score -= 1
                details.append("🔻 OBV 能量潮近 5 週下降 (-1)")
        except (KeyError, IndexError) as e:
            logger.debug(f"OBV calculation skipped: {e}")
            
        # 4. EFI 強力指標 (每週資金流向) — 加死區避免零附近震盪噪音
        efi_week = self._safe_get(current, 'EFI_EMA13', 0)
        # 死區: EFI 接近零時不計分，用近20週 EFI 標準差作門檻
        try:
            efi_series = df['EFI_EMA13'].dropna().iloc[-20:]
            efi_threshold = efi_series.std() * 0.3 if len(efi_series) >= 10 else 0
        except (KeyError, IndexError):
            efi_threshold = 0
        if efi_week > efi_threshold:
             score += 1
             details.append(f"✅ 週線 EFI 主力作多 (EFI={efi_week:,.0f}) (+1)")
        elif efi_week < -efi_threshold:
             score -= 1
             details.append(f"🔻 週線 EFI 主力調節 (EFI={efi_week:,.0f}) (-1)")
        else:
             details.append(f"⚠️ 週線 EFI 力道不明 (EFI={efi_week:,.0f}, 死區內) (0)")

        # 5. 形態度 (W底/M頭) - 週線級別，cap ±2 避免單一形態主導
        try:
             morph_score, morph_msgs = self._detect_morphology(df)
             morph_score = max(-2, min(2, morph_score))  # cap 形態分數
             score += morph_score
             if morph_score != 0:
                 # 修改訊息以標示這是週線
                 morph_msgs = [f"📅 週線{m}" for m in morph_msgs]
             details.extend(morph_msgs)
        except Exception as e:
             logger.debug(f"Morphology detection skipped: {e}")

        # 6. 量價關係 (Price-Volume)
        pv_score, pv_msgs = self._analyze_price_volume(df)
        score += pv_score
        details.extend(pv_msgs)

        # Clamp to valid range
        score = max(-5, min(5, score))

        return score, details

    @staticmethod
    def _chip_weight_multiplier(trend_score):
        """計算籌碼動態權重乘數"""
        if trend_score >= 3:
            return 1.5
        elif trend_score <= -2:
            return 0.5
        return 1.0

    def _analyze_chip_factors(self, df, trend_score=0):
        """
        籌碼面評分 (Chip Analysis) - 精簡版
        只保留 IC 有效的子因子:
        - 台股: 法人動向 (T+1, 最即時的主動交易信號)
        - 美股: 內部人交易 + 空頭變化
        移除: 融資水位(慢指標), 當沖佔比(極少觸發), 連續買賣超(與法人重疊),
              機構持股比例(靜態), 分析師評等(幾乎永遠buy)
        不使用動態權重乘數 (低 IC 信號不應放大)
        """
        score = 0
        details = []

        # === 美股籌碼分析 ===
        if self._is_us_stock:
            return self._analyze_us_chip_factors(df, trend_score)

        # === 台股籌碼分析 ===
        if not self.chip_data:
            return 0, []

        try:
            # 法人動向 (Institutional) — 唯一保留的台股籌碼計分因子
            # 近 5 日外資+投信總買賣超，需過顯著性門檻
            df_inst = self.chip_data.get('institutional')
            if df_inst is not None and not df_inst.empty and not df.empty:
                recent_inst = df_inst.iloc[-5:]

                total_buy_shares = 0
                foreign_buy = 0
                trust_buy = 0

                if '外資' in recent_inst.columns:
                    foreign_buy = recent_inst['外資'].sum()
                    total_buy_shares += foreign_buy
                if '投信' in recent_inst.columns:
                    trust_buy = recent_inst['投信'].sum()
                    total_buy_shares += trust_buy

                total_buy_lots = total_buy_shares / 1000
                foreign_lots = foreign_buy / 1000
                trust_lots = trust_buy / 1000

                current_price = df.iloc[-1]['Close']
                buy_amount_million = (abs(total_buy_lots) * current_price * 1000) / 1_000_000
                recent_volume = df.iloc[-5:]['Volume'].mean() / 1000
                volume_ratio = abs(total_buy_lots) / recent_volume if recent_volume > 0 else 0
                is_significant = (buy_amount_million > 50) or (volume_ratio > 0.15)

                base_score = 0
                if total_buy_lots > 0 and is_significant:
                    base_score = 1.0
                    if foreign_lots > 0 and trust_lots > 0:
                        base_score += 0.5
                elif total_buy_lots < 0 and is_significant:
                    base_score = -1.0
                    if foreign_lots < 0 and trust_lots < 0:
                        base_score -= 0.5

                score += base_score

                if base_score != 0:
                    direction = "買超" if total_buy_lots > 0 else "賣超"
                    sync_note = ""
                    if (foreign_lots > 0 and trust_lots > 0) or (foreign_lots < 0 and trust_lots < 0):
                        sync_note = " [外資+投信同步]"
                    emoji = "💰" if total_buy_lots > 0 else "💸"
                    details.append(
                        f"{emoji} 法人近5日{direction} ({total_buy_lots:,.0f}張, {buy_amount_million:.0f}百萬){sync_note} "
                        f"({base_score:+.1f})"
                    )

            # 融資/當沖/連續買賣超 — 僅顯示資訊，不計分
            df_margin = self.chip_data.get('margin')
            if df_margin is not None and not df_margin.empty:
               last_m = df_margin.iloc[-1]
               lim = last_m.get('融資限額', 0)
               bal = last_m.get('融資餘額', 0)
               if lim > 0:
                   util = (bal / lim) * 100
                   if util > 60:
                       details.append(f"⚠️ 融資使用率偏高 ({util:.1f}%) [資訊]")
                   elif util < 20:
                       details.append(f"✨ 融資水位偏低 ({util:.1f}%) [資訊]")

        except Exception as e:
            logger.warning(f"Chip scoring error: {e}")

        return score, details

    def _analyze_us_chip_factors(self, df, trend_score=0):
        """
        美股籌碼面評分 (US Stock Chip Analysis) - 精簡版
        只保留 IC 有效因子:
        - 內部人交易 (學術驗證最強的籌碼信號)
        - 空頭變化 (動態指標)
        移除: 機構持股比例(靜態,幾乎永遠>60%), 分析師評等(幾乎永遠buy)
        """
        score = 0
        details = []

        if not self.us_chip_data:
            try:
                from us_stock_chip import USStockChipAnalyzer
                us_analyzer = USStockChipAnalyzer()
                self.us_chip_data, err = us_analyzer.get_chip_data(self.ticker)

                if err or not self.us_chip_data:
                    details.append(f"ℹ️ 美股籌碼數據暫無法取得")
                    return 0, details
            except Exception as e:
                logger.warning(f"US Chip load error: {e}")
                return 0, []

        try:
            # 1. 內部人交易 — 計分因子
            insider = self.us_chip_data.get('insider_trades', {})
            sentiment = insider.get('sentiment', 'neutral')
            buy_count = insider.get('buy_count', 0)
            sell_count = insider.get('sell_count', 0)

            if sentiment == 'bullish' and buy_count > 3:
                score += 1.5
                details.append(f"💎 內部人積極買入 (買{buy_count}/賣{sell_count}) (+1.5)")
            elif sentiment == 'bullish':
                score += 0.5
                details.append(f"✅ 內部人偏向買入 (買{buy_count}/賣{sell_count}) (+0.5)")
            elif sentiment == 'bearish' and sell_count > 5:
                score -= 1.5
                details.append(f"💀 內部人大量拋售 (買{buy_count}/賣{sell_count}) (-1.5)")
            elif sentiment == 'bearish':
                score -= 0.5
                details.append(f"⚠️ 內部人偏向賣出 (買{buy_count}/賣{sell_count}) (-0.5)")

            # 2. 空頭變化 — 計分因子 (只看變化量，不看靜態水位)
            short = self.us_chip_data.get('short_interest', {})
            short_change = short.get('short_change_pct', 0)
            short_pct = short.get('short_percent_of_float', 0)

            if short_change < -20:
                score += 0.5
                details.append(f"✅ 空頭大幅回補 ({short_change:+.1f}%) (+0.5)")
            elif short_change > 20:
                score -= 0.5
                details.append(f"⚠️ 空頭大幅增加 ({short_change:+.1f}%) (-0.5)")

            # 空頭比例 — 僅資訊顯示，不計分
            if short_pct > 10:
                details.append(f"⚠️ 空頭比例偏高 ({short_pct:.1f}%) [資訊]")

            # 機構持股/分析師 — 僅資訊顯示，不計分
            inst = self.us_chip_data.get('institutional', {})
            inst_pct = inst.get('percent_held', 0)
            if inst_pct > 0:
                details.append(f"📊 機構持股 {inst_pct:.1f}% [資訊]")

        except Exception as e:
            logger.warning(f"US Chip scoring error: {e}")

        return score, details

    def _calculate_trigger_score(self, df, trend_score=0):
        """
        計算日線進場訊號 (Trigger Score) -10 ~ +10
        使用四群組中位數架構：Trend / Momentum / Volume / Pattern
        各群組內信號正規化至 [-1, +1]，取中位數後加總乘以 2.5 映射至 [-10, +10]
        籌碼面為獨立加項（不參與中位數計算）。

        Args:
            df: 日線 DataFrame
            trend_score: 週線趨勢分數，用於籌碼動態權重計算
        Returns:
            (score, details, breakdown) — breakdown dict 含各群組中位數與籌碼分數
        """
        details = []

        if df.empty or len(df) < 20:
            return 0, ["數據不足"], {'trend_group': 0, 'momentum_group': 0, 'volume_group': 0, 'pattern_group': 0, 'chip_score': 0}

        current = df.iloc[-1]
        prev = df.iloc[-2]
        close = self._safe_get(current, 'Close', 0)

        def _median_of_signals(signals):
            """Take median of non-None signals."""
            valid = [s for s in signals if s is not None]
            return float(np.median(valid)) if valid else 0.0

        # ============================================================
        # TREND GROUP (4 signals, each normalized to [-1, +1])
        # ============================================================
        trend_signals = []

        # T1. 均線位置 (MA Position): close > MA20 → +1, else -1
        ma20 = self._safe_get(current, 'MA20', 0)
        if close > ma20:
            t1 = 1.0
            details.append("✅ 站上日線 20MA (+1)")
        else:
            t1 = -1.0
            details.append("🔻 跌破日線 20MA (-1)")
        trend_signals.append(t1 / 1.0)

        # T2. Supertrend: dir=1 → +1, dir=-1 → -1, flip bonus +/-1 → normalize /2
        st_dir = self._safe_get(current, 'Supertrend_Dir', 0)
        prev_st_dir = self._safe_get(prev, 'Supertrend_Dir', 0)
        t2_raw = 0.0
        if st_dir == 1:
            t2_raw += 1
            details.append("📈 Supertrend 多頭趨勢 (+1)")
            if prev_st_dir == -1:
                t2_raw += 1
                details.append("🔄 Supertrend 空轉多翻轉！(+1)")
        elif st_dir == -1:
            t2_raw -= 1
            details.append("📉 Supertrend 空頭趨勢 (-1)")
            if prev_st_dir == 1:
                t2_raw -= 1
                details.append("🔄 Supertrend 多轉空翻轉！(-1)")
        trend_signals.append(t2_raw / 2.0)

        # T3. (VWAP removed — 橫截面 IC 無顯著貢獻，已移除)

        # T4. DMI: ADX_z > 1.0 (or ADX > 25 fallback) + DI direction
        adx = self._safe_get(current, 'ADX', 0)
        adx_z = self._safe_get(current, 'ADX_z', None)
        plus_di = self._safe_get(current, '+DI', 0)
        minus_di = self._safe_get(current, '-DI', 0)
        t4_raw = None  # None = no signal (ADX too low)

        # Determine if trend is strong enough
        adx_strong = False
        if adx_z is not None and not pd.isna(adx_z):
            adx_strong = adx_z > 1.0
        else:
            adx_strong = adx > 25

        if adx_strong:
            if plus_di > minus_di:
                t4_raw = 1.0
                details.append(f"✅ 日線 DMI 多方攻擊 (ADX={adx:.1f}) (+1)")
            else:
                t4_raw = -1.0
                details.append(f"🔻 日線 DMI 空方下殺 (ADX={adx:.1f}) (-1)")
        trend_signals.append(t4_raw / 1.0 if t4_raw is not None else None)

        # ============================================================
        # MOMENTUM GROUP (4 signals, each normalized to [-1, +1])
        # ============================================================
        momentum_signals = []

        # M1. MACD + divergence: histogram + divergence bonus → range ~[-4.5, +4.5] → /4.5
        hist = self._safe_get(current, 'Hist', 0)
        prev_hist = self._safe_get(prev, 'Hist', 0)
        m1_raw = 0.0
        if hist > 0:
            m1_raw += 1
            details.append("✅ MACD 柱狀體翻紅 (+1)")
            if hist > prev_hist:
                m1_raw += 0.5
                details.append("🔥 MACD 動能持續增強 (+0.5)")
        else:
            m1_raw -= 1
            details.append("🔻 MACD 柱狀體翻綠 (-1)")

        # MACD 背離偵測 [UPGRADED - Pivot Points 標準檢測]
        div_macd = self._detect_divergence(df, 'MACD')
        if div_macd == 'bull_strong':
            m1_raw += 3
            details.append("💎💎 MACD 出現【強烈底背離】訊號 (高勝率反轉) (+3)")
        elif div_macd == 'bull':
            m1_raw += 2
            details.append("💎 MACD 出現【底背離】訊號 (+2)")
        elif div_macd == 'bull_weak':
            m1_raw += 1
            details.append("📈 MACD 出現【隱藏底背離】(多頭趨勢延續) (+1)")
        elif div_macd == 'bear_strong':
            m1_raw -= 3
            details.append("💀💀 MACD 出現【強烈頂背離】訊號 (高風險反轉) (-3)")
        elif div_macd == 'bear':
            m1_raw -= 2
            details.append("💀 MACD 出現【頂背離】訊號 (-2)")
        elif div_macd == 'bear_weak':
            m1_raw -= 1
            details.append("📉 MACD 出現【隱藏頂背離】(空頭趨勢延續) (-1)")
        momentum_signals.append(max(-1.0, min(1.0, m1_raw / 4.5)))

        # M2. KD: K>D → +1, else -1 → /1
        k_val = self._safe_get(current, 'K', 0)
        d_val = self._safe_get(current, 'D', 0)
        if k_val > d_val:
            m2_raw = 1.0
            details.append("✅ KD 黃金交叉/多方排列 (+1)")
        else:
            m2_raw = -1.0
            details.append("🔻 KD 死亡交叉/空方排列 (-1)")
        momentum_signals.append(m2_raw / 1.0)

        # M3. RSI divergence: ±1.5 → /1.5
        div_rsi = self._detect_divergence(df, 'RSI')
        m3_raw = 0.0
        if div_rsi in ['bull_strong', 'bull']:
            m3_raw = 1.5 if div_rsi == 'bull_strong' else 1.0
            details.append(f"✅ RSI 出現{'強烈' if div_rsi == 'bull_strong' else ''}底背離 (+{m3_raw})")
        elif div_rsi in ['bear_strong', 'bear']:
            m3_raw = -1.5 if div_rsi == 'bear_strong' else -1.0
            details.append(f"🔻 RSI 出現{'強烈' if div_rsi == 'bear_strong' else ''}頂背離 ({m3_raw:+.1f})")
        momentum_signals.append(m3_raw / 1.5 if m3_raw != 0 else None)

        # (Squeeze removed — 橫截面 IC 為負，已從 Momentum 組移除)

        # ============================================================
        # VOLUME GROUP (精簡為 RVOL only — OBV/EFI/量價 IC≈0 或為負，已移除)
        # ============================================================
        volume_signals = []

        # V1. RVOL: 橫截面 IC 最強因子 (+0.013), use z-score if available
        rvol = self._safe_get(current, 'RVOL', 0)
        rvol_z = self._safe_get(current, 'RVOL_z', None)
        v3_raw = 0.0
        if rvol_z is not None and not pd.isna(rvol_z):
            # z-score based
            if rvol_z > 1.5:
                v3_raw = 1.0
                details.append(f"🔊 爆量確認 RVOL={rvol:.1f}x (z={rvol_z:.1f}) (+1.0)")
            elif rvol_z < -1.5:
                v3_raw = -1.0
                details.append(f"🔇 量能萎縮 RVOL={rvol:.1f}x (z={rvol_z:.1f}) (-1.0)")
            else:
                # Proportional in [-1, +1]
                v3_raw = max(-1.0, min(1.0, rvol_z / 1.5))
                if abs(v3_raw) > 0.3:
                    details.append(f"📊 RVOL={rvol:.1f}x (z={rvol_z:.1f}) ({v3_raw:+.2f})")
        else:
            # Fallback to absolute thresholds
            if rvol > 2.0:
                v3_raw = 1.0
                details.append(f"🔊 爆量確認 RVOL={rvol:.1f}x (>2.0) (+1.0)")
            elif rvol > 1.5:
                v3_raw = 0.67
                details.append(f"🔊 量能放大 RVOL={rvol:.1f}x (>1.5) (+0.67)")
            elif rvol < 0.5:
                v3_raw = -0.33
                details.append(f"🔇 量能萎縮 RVOL={rvol:.1f}x (<0.5) (-0.33)")
        volume_signals.append(v3_raw)

        # (V4 量價關係 removed — IC 無顯著貢獻)

        # ============================================================
        # PATTERN GROUP — 已移至進場過濾器 (_generate_action_plan)
        # 型態不預測漲跌 (IC=-0.004)，但能定義風險（停損位、進場點）
        # ============================================================
        pattern_signals = []  # 空組，不參與評分

        # (BIAS removed — 橫截面 IC 為負，已從 Trend 組移除)

        # ============================================================
        # GROUP MEDIANS → FINAL SCORE
        # ============================================================
        trend_median = _median_of_signals(trend_signals)
        momentum_median = _median_of_signals(momentum_signals)
        volume_median = _median_of_signals(volume_signals)
        pattern_median = _median_of_signals(pattern_signals)  # 空組 = 0

        # 精簡後 3 個有效組: Trend + Momentum + Volume
        # IC 加權: Trend(0.011) + Momentum(0.011) + Volume/RVOL(0.013) ≈ 等權
        # 3 groups × median in [-1,+1] → sum in [-3,+3] → ×3.33 → [-10,+10]
        score = (trend_median + momentum_median + volume_median) * 3.33

        # ============================================================
        # CHIP FACTORS (additive, separate from groups)
        # ============================================================
        chip_score, chip_details = self._analyze_chip_factors(df, trend_score=trend_score)
        # Cap 籌碼分數至 [-1.0, +1.0]，避免低 IC 信號稀釋技術面
        chip_score = max(-1.0, min(1.0, chip_score))
        score += chip_score
        details.extend(chip_details)

        # (矛盾獎勵已移除 — 籌碼 IC=0.006 < 技術面，不應在矛盾時信任籌碼)

        # Clamp score to valid range
        score = max(-10, min(10, score))

        breakdown = {
            'trend_group': trend_median,
            'momentum_group': momentum_median,
            'volume_group': volume_median,
            'pattern_group': pattern_median,
            'chip_score': chip_score,
        }
        return score, details, breakdown

    def _determine_scenario(self, trend_score, daily_details):
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
        # 直接讀取 self.df_day 而非解析 daily_details 字串，更可靠
        if not self.df_day.empty and len(self.df_day) >= 20:
            current_day = self.df_day.iloc[-1]
            adx = self._safe_get(current_day, 'ADX', 0)
            plus_di = self._safe_get(current_day, '+DI', 0)
            minus_di = self._safe_get(current_day, '-DI', 0)

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

    def _detect_kline_patterns(self, df):
        """
        K線形態偵測 (K-Line Patterns)
        回傳: (score_delta, list_of_messages)
        """
        if len(df) < 5:
            return 0, []
        
        score = 0
        msgs = []
        
        # 取得最後 3 根 K 線
        c = df.iloc[-1]  # 今天 (Current)
        p = df.iloc[-2]  # 昨天 (Previous)
        pp = df.iloc[-3] # 前天 (Pre-Previous)
        
        # 基礎數據計算
        # 實體長度 (Body)
        body_c = abs(c['Close'] - c['Open'])
        body_p = abs(p['Close'] - p['Open'])
        
        # K棒方向 (1:陽, -1:陰)
        dir_c = 1 if c['Close'] > c['Open'] else -1
        dir_p = 1 if p['Close'] > p['Open'] else -1
        dir_pp = 1 if pp['Close'] > pp['Open'] else -1
        
        # 平均實體長度 (用來判斷是否為長紅/長黑)
        avg_body = (abs(df['Close'] - df['Open']).rolling(10).mean().iloc[-1])
        is_long_c = body_c > 1.5 * avg_body
        
        # 1. 吞噬形態 (Engulfing)
        # 多頭吞噬: 昨陰 今陽, 今實體完全包覆昨實體
        if dir_p == -1 and dir_c == 1:
            if c['Open'] <= p['Close'] and c['Close'] >= p['Open']: # 寬鬆定義
                # 量能輔助確認: 成交量放大
                if c['Volume'] > p['Volume']:
                    score += 2
                    msgs.append("🕯️ 出現【多頭吞噬】+【量增】強力反轉訊號 (+2)")
                else:
                    score += 1
                    msgs.append("🕯️ 出現【多頭吞噬】反轉訊號 (量能未出) (+1)")
        
        # 空頭吞噬: 昨陽 今陰, 今實體包覆昨實體
        if dir_p == 1 and dir_c == -1:
            if c['Open'] >= p['Close'] and c['Close'] <= p['Open']:
                # 量能輔助確認: 下殺出量
                if c['Volume'] > p['Volume']:
                    score -= 2
                    msgs.append("🕯️ 出現【空頭吞噬】+【量增】高檔出貨訊號 (-2)")
                else:
                    score -= 1.5
                    msgs.append("🕯️ 出現【空頭吞噬】高檔反轉訊號 (-1.5)")
                
        # 2. 爆量長紅 (Explosive Volume Attack)
        # 成交量 > 5日均量 * 2 且 收長紅
        vol_ma5 = df['Volume'].rolling(5).mean().iloc[-1]
        
        if c['Volume'] > 2.0 * vol_ma5 and dir_c == 1 and is_long_c:
             score += 2
             msgs.append(f"💣 出現【爆量長紅】攻擊訊號 (量增{c['Volume']/vol_ma5:.1f}倍) (+2)")

        # 3. 晨星 (Morning Star) - 嚴格版
        # 定義: 
        # 1. 第一根長黑 (pp)
        # 2. 第二根跳空低開，收小實體 (p)，且實體在第一根實體之下 (Gap check)
        # 3. 第三根長紅 (c)，收盤攻入第一根實體一半以上
        
        # 1. 前天長黑
        is_long_pp = abs(pp['Close'] - pp['Open']) > avg_body
        
        # 2. 昨天星線 (實體小 + 實體部分與前天有缺口 或 極低)
        # 簡單判定: 昨天最高價(或實體上緣) < 前天收盤價 (Gap Down) 或是 昨天收盤 < 前天收盤
        # 這裡用較寬鬆的 Gap: 昨天實體上緣 < 前天實體下緣 (Body Gap)
        p_body_top = max(p['Open'], p['Close'])
        pp_body_bottom = min(pp['Open'], pp['Close'])
        is_gap_down = p_body_top < pp_body_bottom
        
        # Define is_star_p (missing in previous edit)
        is_star_p = body_p < 0.5 * avg_body

        # 3. 今天長紅反擊
        micpoint_pp = (pp['Open'] + pp['Close']) / 2
        
        if (dir_pp == -1 and is_long_pp) and \
           (is_star_p and is_gap_down) and \
           (dir_c == 1 and c['Close'] > micpoint_pp):
           
             if c['Volume'] > p['Volume']:
                  score += 2
                  msgs.append("✨ 出現【晨星】+【量增】標準底部轉折訊號 (+2)")
             else:
                  score += 1.5
                  msgs.append("✨ 出現【晨星】標準底部轉折訊號 (+1.5)")
                
        # 4. 十字變盤線 (Doji)
        # 開收盤極度接近
        if body_c < 0.1 * avg_body:
            # 判斷量能：爆量十字 vs 量縮十字
            if c['Volume'] > 2.0 * vol_ma5:
                 msgs.append("⚠️ 出現【爆量十字線】多空劇烈交戰，留意變盤 (Info)")
            else:
                 msgs.append("⚠️ 出現【量縮十字線】多空觀望 (Info)")

        # 5. [NEW] Check for Extra Patterns from pattern_recognition.py
        # These are informational only (+0)
        current_pattern = c.get('Pattern', None)
        if current_pattern and isinstance(current_pattern, str) and current_pattern not in [None, 'None', 'nan']:
            # Avoid duplicating what we already detected manually (Engulfing, Morning Star)
            # Simple check: if msg already contains the pattern name
            is_duplicate = False
            for m in msgs:
                if current_pattern.split('(')[0] in m: 
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                msgs.append(f"🕯️ 形態識別: {current_pattern} (+0)")

        return score, msgs

    def _detect_morphology(self, df):
        """
        高階形態學偵測 (Chart Patterns) - 總成
        包含: W底/M頭, 頭肩頂/底, 三角收斂
        """
        if len(df) < 60:
            return 0, []
        
        score = 0
        msgs = []
        
        # 1. 基礎 W底 / M頭
        s1, m1 = self._detect_double_patterns(df)
        score += s1
        msgs.extend(m1)
        
        # 2. 進階 頭肩頂 / 頭肩底
        s2, m2 = self._detect_head_and_shoulders(df)
        score += s2
        msgs.extend(m2)
        
        # 3. 三角收斂
        s3, m3 = self._detect_triangle_convergence(df)
        score += s3
        msgs.extend(m3)
        
        return score, msgs

    def _detect_double_patterns(self, df):
        """
        W底 (Double Bottom) 與 M頭 (Double Top) - 這裡保留原邏輯但抽離出來
        """
        score = 0
        msgs = []
        prices = df['Close'].values
        
        # 尋找極值 (左右各5根)
        max_idx = argrelextrema(prices, np.greater, order=5)[0]
        min_idx = argrelextrema(prices, np.less, order=5)[0]
        
        recent_max = max_idx[max_idx > len(df) - 60]
        recent_min = min_idx[min_idx > len(df) - 60]
        current_price = prices[-1]

        # W底
        if len(recent_min) >= 2:
            l2 = prices[recent_min[-1]]
            l1 = prices[recent_min[-2]]
            if (recent_min[-1] - recent_min[-2]) > 5:
                diff_pct = abs(l1 - l2) / l1
                if diff_pct < 0.03:
                    if current_price > l2 and current_price < l2 * 1.15:
                        score += 2
                        msgs.append(f"🦋 形態學: 潛在【W底 (雙重底)】成形中 (+2)")

        # M頭
        if len(recent_max) >= 2:
            h2 = prices[recent_max[-1]]
            h1 = prices[recent_max[-2]]
            if (recent_max[-1] - recent_max[-2]) > 5:
                diff_pct = abs(h1 - h2) / h1
                if diff_pct < 0.03:
                    if current_price < h2 and current_price > h2 * 0.85:
                        score -= 2
                        msgs.append(f"🦇 形態學: 潛在【M頭 (雙重頂)】成形中 (-2)")
                        
        return score, msgs

    def _detect_head_and_shoulders(self, df):
        """
        偵測 頭肩頂 / 頭肩底 (Head and Shoulders)
        並且【嚴格要求成交量】驗證
        """
        score = 0
        msgs = []
        prices = df['Close'].values
        volumes = df['Volume'].values
        
        # 尋找極值 (左右各4根，稍微寬鬆一點找點)
        # 注意: 這裡我們需要找最近的三個極值點
        max_idx = argrelextrema(prices, np.greater, order=4)[0]
        min_idx = argrelextrema(prices, np.less, order=4)[0]
        
        # --- A. 頭肩底 (Bottom) ---
        # 形態: 左肩(L) - 頭(H) - 右肩(R)
        # 價格關係: H < L, H < R
        # 成交量關係: 頭部量大(恐慌), 右肩量縮(沉澱) 
        recent_min = min_idx[min_idx > len(df) - 80] # 看近80根
        
        if len(recent_min) >= 3:
            # 取得最近三個谷底 idx
            i_ls, i_h, i_rs = recent_min[-3], recent_min[-2], recent_min[-1]
            p_ls, p_h, p_rs = prices[i_ls], prices[i_h], prices[i_rs]
            
            # 幾何驗證
            is_head_lowest = (p_h < p_ls) and (p_h < p_rs)
            is_shoulder_level = abs(p_ls - p_rs) / p_ls < 0.10 # 左右肩高度差 10% 內
            
            if is_head_lowest and is_shoulder_level:
                # 成交量驗證 (Volume Confirmation)
                # 右肩量 < 左肩量 OR 右肩量明顯小於均量 (量縮整理)
                v_ls = volumes[i_ls-2:i_ls+3].mean() # 區間均量
                v_rs = volumes[i_rs-2:i_rs+3].mean()
                
                if v_rs < v_ls * 1.2: # 寬鬆一點，只要右肩沒有爆量失控即可
                     # 檢查目前價格是否在頸線附近準備突破
                     neckline = max(prices[i_h:i_rs].max(), prices[i_ls:i_h].max()) 
                     current = prices[-1]
                     
                     if current > p_rs: # 價格要在右肩底之上
                         score += 3
                         msg = f"👑 形態學: 潛在【頭肩底】右肩成形 (+3)"
                         if v_rs < v_ls:
                             msg += " (量縮價穩✅)"
                         else:
                             msg += " (留意量能)"
                         msgs.append(msg)

        # --- B. 頭肩頂 (Top) ---
        # 價格關係: H > L, H > R
        # 成交量關係: 右肩量縮 (買盤無力)
        recent_max = max_idx[max_idx > len(df) - 80]
        
        if len(recent_max) >= 3:
            i_ls, i_h, i_rs = recent_max[-3], recent_max[-2], recent_max[-1]
            p_ls, p_h, p_rs = prices[i_ls], prices[i_h], prices[i_rs]
            
            is_head_highest = (p_h > p_ls) and (p_h > p_rs)
            is_shoulder_level = abs(p_ls - p_rs) / p_ls < 0.10
            
            if is_head_highest and is_shoulder_level:
                # 成交量驗證: 右肩量縮 (Buyer exhaustion)
                v_ls = volumes[i_ls-2:i_ls+3].mean()
                v_rs = volumes[i_rs-2:i_rs+3].mean()
                
                if v_rs < v_ls:
                     score -= 3
                     msgs.append(f"💀 形態學: 潛在【頭肩頂】右肩成形 (量縮無力) (-3)")

        return score, msgs

    def _detect_triangle_convergence(self, df):
        """
        偵測 三角收斂 (Triangle Convergence / Squeeze)
        邏輯: 高點越來越低 + 低點越來越高 + 成交量萎縮
        """
        score = 0
        msgs = []
        
        # 至少要有一些數據來計算趨勢
        if len(df) < 30: return 0, []
        
        recent = df.iloc[-30:] # 近30根
        
        # 1. 價格壓縮偵測 (High Lower, Low Higher)
        # 簡單做法：切兩半，比較前半與後半的 High/Low 區間
        mid = len(recent) // 2
        part1 = recent.iloc[:mid]
        part2 = recent.iloc[mid:]
        
        h1 = part1['High'].max()
        l1 = part1['Low'].min()
        h2 = part2['High'].max()
        l2 = part2['Low'].min()
        
        # 區間 1 高度
        range1 = h1 - l1
        # 區間 2 高度
        range2 = h2 - l2
        
        # 條件: 波動率下降 (壓縮)
        is_squeezing = range2 < range1 * 0.8 # 後半段波動 < 前半段 80%
        
        # 條件: 形態 (高不過高，低不破低)
        is_triangle = (h2 < h1) and (l2 > l1)
        
        if is_triangle and is_squeezing:
            # 2. 成交量驗證 (Volume Squeeze)
            # 檢查最近 5 天均量 vs 20 天均量
            vol_ma5 = recent['Volume'].rolling(5).mean().iloc[-1]
            vol_ma20 = recent['Volume'].rolling(20).mean().iloc[-1]
            
            if vol_ma5 < vol_ma20 * 0.8:
                score += 1 # 中性偏多 (視為即將變盤，給予關注分，但不一定是多空)
                # 這裡給正分是因為通常這是在尋找機會，提示使用者關注
                msgs.append(f"📐 形態學: 【三角收斂】末端 (量縮極致) 等待變盤 (+1)")
            else:
                msgs.append(f"📐 形態學: 【三角收斂】整理中 (量能未縮) (Monitor)")
                
        return score, msgs

    def _detect_divergence(self, df, indicator_name, window=40):
        """
        [UPGRADED] 標準背離偵測引擎 - 使用 Pivot Points
        
        標準背離定義:
        - 底背離 (Bullish): 價格形成「更低的低點」，但指標形成「更高的低點」
        - 頂背離 (Bearish): 價格形成「更高的高點」，但指標形成「更低的高點」
        
        背離強度評級:
        - 'bull_strong' / 'bear_strong': 強烈背離 (兩波以上)
        - 'bull' / 'bear': 標準背離
        - 'bull_weak' / 'bear_weak': 隱藏背離 (Hidden Divergence)
        
        Args:
            df: DataFrame with price and indicator data
            indicator_name: 要檢測背離的指標欄位名
            window: 回看窗口大小
        
        Returns:
            str or None: 背離類型 ('bull', 'bear', 'bull_strong', 'bear_strong', etc.)
        """
        if len(df) < window or indicator_name not in df.columns:
            return None
        
        # 只看最近 window 根 K 棒
        subset = df.iloc[-window:].copy()
        
        prices_low = subset['Low'].values
        prices_high = subset['High'].values
        indicator = subset[indicator_name].values
        
        # 使用 order=3 找局部極值 (左右各3根比較)
        order = 3
        
        # 找波谷 (用於底背離)
        price_min_idx = argrelextrema(prices_low, np.less, order=order)[0]
        ind_min_idx = argrelextrema(indicator, np.less, order=order)[0]
        
        # 找波峰 (用於頂背離)
        price_max_idx = argrelextrema(prices_high, np.greater, order=order)[0]
        ind_max_idx = argrelextrema(indicator, np.greater, order=order)[0]
        
        # === 底背離檢測 ===
        # 需要至少 2 個波谷來比較
        if len(price_min_idx) >= 2 and len(ind_min_idx) >= 2:
            # 取最近兩個價格波谷
            p1_idx, p2_idx = price_min_idx[-2], price_min_idx[-1]
            p1_price, p2_price = prices_low[p1_idx], prices_low[p2_idx]
            
            # 找對應的指標波谷 (最接近價格波谷的位置)
            # 波谷1 對應的指標
            ind1_candidates = ind_min_idx[ind_min_idx <= p1_idx + order]
            ind1_candidates = ind1_candidates[ind1_candidates >= max(0, p1_idx - order)]
            
            # 波谷2 對應的指標
            ind2_candidates = ind_min_idx[ind_min_idx <= p2_idx + order]
            ind2_candidates = ind2_candidates[ind2_candidates >= max(p1_idx, p2_idx - order)]
            
            if len(ind1_candidates) > 0 and len(ind2_candidates) > 0:
                ind1_idx = ind1_candidates[-1] if len(ind1_candidates) > 0 else p1_idx
                ind2_idx = ind2_candidates[-1] if len(ind2_candidates) > 0 else p2_idx
                
                ind1_val = indicator[ind1_idx]
                ind2_val = indicator[ind2_idx]
                
                # 標準底背離: 價格更低低點 + 指標更高低點
                if p2_price < p1_price and ind2_val > ind1_val:
                    # 計算背離強度
                    price_drop_pct = (p1_price - p2_price) / p1_price * 100
                    ind_rise_pct = min((ind2_val - ind1_val) / abs(ind1_val) * 100, 500) if ind1_val != 0 else 0

                    # 強烈背離: 價格跌幅 > 3% 且 指標上升 > 10%
                    if price_drop_pct > 3 and ind_rise_pct > 10:
                        return 'bull_strong'
                    return 'bull'
                
                # 隱藏底背離 (Hidden Bullish): 價格更高低點 + 指標更低低點 (趨勢延續)
                if p2_price > p1_price and ind2_val < ind1_val:
                    return 'bull_weak'
        
        # === 頂背離檢測 ===
        if len(price_max_idx) >= 2 and len(ind_max_idx) >= 2:
            # 取最近兩個價格波峰
            p1_idx, p2_idx = price_max_idx[-2], price_max_idx[-1]
            p1_price, p2_price = prices_high[p1_idx], prices_high[p2_idx]
            
            # 找對應的指標波峰
            ind1_candidates = ind_max_idx[ind_max_idx <= p1_idx + order]
            ind1_candidates = ind1_candidates[ind1_candidates >= max(0, p1_idx - order)]
            
            ind2_candidates = ind_max_idx[ind_max_idx <= p2_idx + order]
            ind2_candidates = ind2_candidates[ind2_candidates >= max(p1_idx, p2_idx - order)]
            
            if len(ind1_candidates) > 0 and len(ind2_candidates) > 0:
                ind1_idx = ind1_candidates[-1] if len(ind1_candidates) > 0 else p1_idx
                ind2_idx = ind2_candidates[-1] if len(ind2_candidates) > 0 else p2_idx
                
                ind1_val = indicator[ind1_idx]
                ind2_val = indicator[ind2_idx]
                
                # 標準頂背離: 價格更高高點 + 指標更低高點
                if p2_price > p1_price and ind2_val < ind1_val:
                    # 計算背離強度
                    price_rise_pct = (p2_price - p1_price) / p1_price * 100
                    ind_drop_pct = min((ind1_val - ind2_val) / abs(ind1_val) * 100, 500) if ind1_val != 0 else 0
                    
                    # 強烈背離
                    if price_rise_pct > 3 and ind_drop_pct > 10:
                        return 'bear_strong'
                    return 'bear'
                
                # 隱藏頂背離 (Hidden Bearish): 價格更低高點 + 指標更高高點 (趨勢延續)
                if p2_price < p1_price and ind2_val > ind1_val:
                    return 'bear_weak'
        
        return None

    def _analyze_price_volume(self, df):
        """
        量價關係分析 (Price-Volume Analysis)
        邏輯:
          - 價漲量增 (+): 多頭健康攻擊
          - 價漲量縮 (-): 量價背離 (惜售 or 買盤力竭)
          - 價跌量增 (-): 恐慌殺盤 (出貨)
          - 價跌量縮 (+): 籌碼沉澱 (洗盤)
        """
        if len(df) < 20: 
            return 0, []
            
        score = 0
        msgs = []
        
        c = df.iloc[-1]
        p = df.iloc[-2]
        
        # 計算 5MA / 20MA 成交量
        vol_ma5 = df['Volume'].rolling(5).mean().iloc[-1]
        vol_ma20 = df['Volume'].rolling(20).mean().iloc[-1]
        
        # 判斷當日/當週 價漲跌
        price_up = c['Close'] > p['Close']
        price_down = c['Close'] < p['Close']
        
        # 判斷成交量相對強弱 (比 MA5 大算增，比 MA5 小算縮)
        # 也可以比昨天 (c['Volume'] > p['Volume'])，這裡採用比均量較客觀
        vol_up = c['Volume'] > vol_ma5
        vol_down = c['Volume'] < vol_ma5
        
        # 1. 價漲量增 (Healthy Uptrend)
        if price_up and vol_up:
            score += 1
            msgs.append(f"📈 量價配合：價漲量增 (Vol > 5MA) 多方攻擊 (+1)")
            
        # 2. 價漲量縮 (Divergence / Warning)
        elif price_up and vol_down:
            score -= 0.5
            msgs.append(f"⚠️ 量價背離：價漲量縮 (追價意願不足) (-0.5)")
            
        # 3. 價跌量增 (Panic Selling / Heavy Pressure)
        elif price_down and vol_up:
            score -= 1
            msgs.append(f"🔻 賣壓湧現：價跌量增 (恐慌殺盤) (-1)")
            
        # 4. 價跌量縮 (Healthy Correction / Washout)
        elif price_down and vol_down:
            score += 0.5
            msgs.append(f"♻️ 籌碼沉澱：價跌量縮 (惜售/洗盤) (+0.5)")
            
        return score, msgs
