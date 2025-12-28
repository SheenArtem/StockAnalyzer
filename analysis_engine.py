import pandas as pd
import numpy as np

class TechnicalAnalyzer:
    def __init__(self, ticker, df_week, df_day, strategy_params=None):
        self.ticker = ticker
        self.df_week = df_week
        self.df_day = df_day
        self.strategy_params = strategy_params # { 'buy': 3, 'sell': -2 }

    def run_analysis(self):
        """
        執行完整分析流程
        Returns:
            dict: 包含 趨勢分數, 觸發分數, 劇本, 詳細評分項目
        """
        trend_score, trend_details = self._calculate_trend_score(self.df_week)
        trigger_score, trigger_details = self._calculate_trigger_score(self.df_day)
        
        scenario = self._determine_scenario(trend_score, trigger_details) # Check details for ADX special case
        
        # 4. 操作劇本與風控 (Action Plan & Risk)
        # Pass trigger_score to link with optimized strategy
        action_plan = self._generate_action_plan(self.df_day, scenario, trigger_score)
        
        # 5. [NEW] Dynamic Monitoring Checklist (Conditional Alerts)
        checklist = self._generate_monitoring_checklist(self.df_day, scenario)
        
        return {
            "ticker": self.ticker,
            "trend_score": trend_score,
            "trend_details": trend_details,
            "trigger_score": trigger_score,
            "trigger_details": trigger_details,
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
        ma5 = current.get('MA5', 0)
        ma20 = current.get('MA20', 0)
        ma60 = current.get('MA60', 0)
        vol_ma5 = current.get('Vol_MA5', 0) if 'Vol_MA5' in current else 0
        
        # --- 1. Risk Control (Stop Loss / Trim) ---
        # A. 破線停損
        if close > ma20:
            checklist['risk'].append(f"若收盤跌破 **月線 ({ma20:.2f})**，短期轉弱，建議減碼或停損。")
        elif close > ma60:
             checklist['risk'].append(f"若收盤跌破 **季線 ({ma60:.2f})**，波段轉弱，建議清倉觀望。")
             
        # B. 爆量長黑
        checklist['risk'].append(f"若出現 **爆量長黑** (成交量 > {vol_ma5*2:.0f}) 且收跌，視為主力出貨訊號。")
        
        # C. KD 高檔鈍化結束
        if current.get('K', 0) > 80:
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
        ma5 = current.get('MA5', 0)
        ma10 = current.get('MA10', 0)
        ma20 = current.get('MA20', 0)
        ma60 = current.get('MA60', 0)
        atr_val = current.get('ATR', 0)
        sl_low = df['Low'].iloc[-20:].min()
        sl_ma = ma20
        sl_key = sl_low # fallback
        sl_atr = close_price - (2.0 * atr_val) if atr_val > 0 else close_price * 0.9
        sl_key_candle = sl_low # Default for key candle if logic complex

        # Default S/L Method
        rec_sl_method = "ATR 波動停損 (科學)" # Updated simplified name logic later if needed
        rec_sl_price = 0
        
        # [Optimization Override]
        optimizer_active = False
        if self.strategy_params:
            buy_th = self.strategy_params.get('buy', 3)
            sell_th = self.strategy_params.get('sell', -2)
            
            if trigger_score >= buy_th:
                 optimizer_active = True
                 is_actionable = True
                 code = 'A' # Treat as Active
                 strategy_text = f"🔥 **AI 最佳化訊號 (買進)**：評分 ({trigger_score}) 已達買進門檻 ({buy_th})，建議進場。"
                 # Dynamic Entry
                 rec_entry_low, rec_entry_high = close_price * 0.99, close_price * 1.01
                 rec_entry_desc = "現價進場 (AI 訊號)"
                 entry_basis = close_price
                 
            elif trigger_score <= sell_th:
                 optimizer_active = True
                 is_actionable = False
                 code = 'D'
                 strategy_text = f"🛑 **AI 最佳化訊號 (賣出)**：評分 ({trigger_score}) 已達賣出門檻 ({sell_th})，建議出場觀望。"

        
        # Determine Scenario Intent (Only if not overridden by optimizer)
        if not optimizer_active:
            if code == 'A': # Active
                is_actionable = True
                if close_price > ma5 * 1.05:
                    # ... (rest of logic same)
                    rec_entry_low, rec_entry_high = ma10, ma5
                    rec_entry_desc = "等待拉回 (5MA-10MA)"
                    entry_basis = ma5
                    strategy_text = "🚀 **強勢股 (等待拉回)**：乖離過大，建議掛單在 5MA 附近接，不追高。"
                else:
                    rec_entry_low, rec_entry_high = ma5, close_price
                    rec_entry_desc = "積極操作 (5MA-現價)"
                entry_basis = close_price
                strategy_text = "🚀 **積極進場**：趨勢強勁，目標看向波段滿足點。"
                
        elif code == 'B': # Pullback (Actionable Limit Buy)
            is_actionable = True
            support = ma60 if ma60 < ma20 else ma20
            rec_entry_low, rec_entry_high = support * 0.98, support * 1.02
            rec_entry_desc = "回測支撐 (月季線)"
            entry_basis = support
            strategy_text = "⏳ **等待訊號**：建議掛單在月季線支撐附近，不要追高。"

        elif code == 'C': # Rebound
            is_actionable = True
            bb_lo = current.get('BB_Lo', 0)
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
        final_sl_list = []
        sl_candidates = [
            {"method": "A. ATR 波動停損 (科學)", "price": sl_atr, "desc": "2倍 ATR"},
            {"method": "B. 均線停損 (趨勢)", "price": sl_ma, "desc": "MA20/60"},
            {"method": "C. 關鍵紅K (籌碼)", "price": sl_key, "desc": "大量低點"},
            {"method": "D. 波段低點停損 (形態)", "price": sl_low, "desc": "前波低點"}
        ]
        
        for item in sl_candidates:
            if item['price'] > 0: # Show all valid calculated supports
                diff = item['price'] - entry_basis
                loss_pct = (diff / entry_basis) * 100
                
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
                "rec_entry_low": 0, "rec_entry_high": 0, "rec_entry_desc": "",
                "rec_tp_price": 0, "rec_sl_price": 0,
                "tp_list": [],
                "sl_list": final_sl_list, # [FIX] Return SL list even if not actionable
                # [FIX] Populate missing S/L keys for UI display
                "rec_sl_method": "N/A", # Or rec_sl_method
                "sl_atr": sl_atr,
                "sl_ma": sl_ma,
                "sl_key_candle": sl_key_candle,
                "sl_low": sl_low
            }
            
        # --- Logic continues ONLY if actionable ---
        
        # 1. Stop Loss (Based on Entry)
        # Recalculate based on Entry Basis
        rec_sl_price = entry_basis - (2.0 * atr_val) if atr_val > 0 else entry_basis * 0.9
        
        # Update Method Name to match UI exact string
        if code == 'C':
             # Already set above to match UI? 
             # UI expects: "A. ATR 波動停損 (科學)", "D. 波段低點停損 (形態)"
             # Let's map it
             pass 
             
        # Map simple method string to UI full string
        if "ATR" in rec_sl_method:
             rec_sl_method = "A. ATR 波動停損 (科學)"
        elif "波段" in rec_sl_method:
             rec_sl_method = "D. 波段低點停損 (形態)"
        else:
             rec_sl_method = "A. ATR 波動停損 (科學)" # Default
        
        # 2. Take Profit (Based on Entry)
        recent_high_20 = df['High'].iloc[-20:].max()
        recent_low_20 = df['Low'].iloc[-20:].min()
        wave_height = recent_high_20 - recent_low_20
        bb_up = current.get('BB_Up', 0)
        ma60 = current.get('MA60', 0)
        ma120 = current.get('MA120', 0)
        ma240 = current.get('MA240', 0)

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

        return {
            "current_price": close_price,
            "strategy": strategy_text,
            "is_actionable": True,
            "rec_entry_low": rec_entry_low,
            "rec_entry_high": rec_entry_high,
            "rec_entry_desc": rec_entry_desc,
            "rec_sl_method": rec_sl_method,
            "rec_sl_price": rec_sl_price,
            "rec_tp_price": rec_tp_price,
            "rr_ratio": rr_ratio, # [NEW] RR Ratio
            "tp_list": final_tp_list,
            "sl_list": final_sl_list,
            "sl_atr": sl_atr,
            "sl_ma": sl_ma,
            "sl_key_candle": sl_key, 
            "sl_low": sl_low
        }
        




    def _calculate_trend_score(self, df):
        """
        計算週線趨勢分數 (Trend Score) -3 ~ +3
        """
        score = 0
        details = []

        if df.empty or len(df) < 5:
            return 0, ["數據不足"]

        current = df.iloc[-1]
        prev = df.iloc[-2]

        # 1. 均線架構 (MA Structure)
        # 多頭排列: 收盤 > MA20 > MA60
        if current['Close'] > current['MA20'] and current['MA20'] > current['MA60']:
            score += 2
            details.append("✅ 週線均線多頭排列 (Close > 20MA > 60MA) (+2)")
        elif current['Close'] > current['MA20']:
            score += 1
            details.append("✅ 股價站上週 20MA (+1)")
        elif current['Close'] < current['MA20'] and current['MA20'] < current['MA60']:
            score -= 2
            details.append("🔻 均線空頭排列 (Close < 20MA < 60MA) (-2)")
        else:
            details.append("⚠️ 均線糾結混亂 (0)")

        # 2. DMI 趨勢強度
        if current['ADX'] > 25:
            if current['+DI'] > current['-DI']:
                score += 1
                details.append(f"✅ DMI 多方趨勢成形 (ADX={current['ADX']:.1f} > 25, +DI > -DI) (+1)")
            else:
                score -= 1
                details.append(f"🔻 DMI 空方趨勢成形 (ADX={current['ADX']:.1f} > 25, -DI > +DI) (-1)")
        else:
            details.append(f"⚠️ DMI 趨勢不明 (ADX={current['ADX']:.1f} < 25) (0)")

        # 3. OBV 能量潮 (比較近5週趨勢)
        # 簡單邏輯: 現在 OBV > 5週前 OBV
        try:
            obv_5w_ago = df['OBV'].iloc[-5]
            if current['OBV'] > obv_5w_ago:
                score += 1
                details.append("✅ OBV 能量潮近 5 週上升 (+1)")
            else:
                details.append("🔻 OBV 能量潮下降 (0)")
        except:
            pass
            
        # 4. EFI 強力指標 (每週資金流向)
        efi_week = current.get('EFI_EMA13', 0)
        if efi_week > 0:
             score += 1
             details.append(f"✅ 週線 EFI 主力作多 (EFI={efi_week:,.0f}) (+1)")
        else:
             score -= 1
             details.append(f"🔻 週線 EFI 主力調節 (EFI={efi_week:,.0f}) (-1)")

        # 5. 形態度 (W底/M頭) - 週線級別威力更大
        try:
             morph_score, morph_msgs = self._detect_morphology(df)
             score += morph_score
             if morph_score != 0:
                 # 修改訊息以標示這是週線
                 morph_msgs = [f"📅 週線{m}" for m in morph_msgs]
             details.extend(morph_msgs)
        except Exception as e:
             pass

        # 6. 量價關係 (Price-Volume)
        pv_score, pv_msgs = self._analyze_price_volume(df)
        score += pv_score
        details.extend(pv_msgs)

        return score, details

    def _calculate_trigger_score(self, df):
        """
        計算日線進場訊號 (Trigger Score) -5 ~ +5 (擴大範圍)
        """
        score = 0
        details = []

        if df.empty or len(df) < 20:
            return 0, ["數據不足"]

        current = df.iloc[-1]
        prev = df.iloc[-2]

        # 1. 均線位置 (MA Position)
        if current['Close'] > current['MA20']:
            score += 1
            details.append("✅ 站上日線 20MA (+1)")
        else:
            score -= 1
            details.append("🔻 跌破日線 20MA (-1)")

        # 2. 乖離率 (BIAS)
        # 假設: 正乖離 > 10% 過熱, 負乖離 < -10% 超賣
        bias = current.get('BIAS', 0)
        if 0 < bias < 10:
            score += 1
            details.append(f"✅ 乖離率健康 ({bias:.1f}%) (+1)")
        elif bias > 10:
            score -= 1
            details.append(f"⚠️ 正乖離過大 ({bias:.1f}%) 慎防回檔 (-1)")
        elif bias < -10:
            score += 1
            details.append(f"🟢 負乖離過大 ({bias:.1f}%) 醞釀反彈 (+1)")
        
        # 3. EFI 埃爾德強力指標 (主力力度)
        efi_day = current.get('EFI_EMA13', 0)
        if efi_day > 0:
             score += 1
             details.append(f"✅ EFI 主力資金控盤 (EFI>0) (+1)")
             # 輔助：力道增強中
             if efi_day > prev.get('EFI_EMA13', 0):
                 score += 0.5
                 details.append("🔥 EFI 買盤力道增強 (+0.5)")
        else:
             score -= 1
             details.append(f"🔻 EFI 空方資金控盤 (EFI<0) (-1)")

        # 4. MACD 動能與背離
        if current['Hist'] > 0:
            score += 1
            details.append("✅ MACD 柱狀體翻紅 (+1)")
            if current['Hist'] > prev['Hist']:
                score += 0.5
                details.append("🔥 MACD 動能持續增強 (+0.5)")
        else:
            score -= 1
            details.append("🔻 MACD 柱狀體翻綠 (-1)")
            
        # MACD 背離偵測
        div_macd = self._detect_divergence(df, 'MACD')
        if div_macd == 'bull':
            score += 2
            details.append("💎 MACD 出現【底背離】訊號 (+2)")
        elif div_macd == 'bear':
            score -= 2
            details.append("💀 MACD 出現【頂背離】訊號 (-2)")

        # 5. KD指標
        if current['K'] > current['D']:
            score += 1
            details.append("✅ KD 黃金交叉/多方排列 (+1)")
        else:
            score -= 1
            details.append("🔻 KD 死亡交叉/空方排列 (-1)")

        # 6. OBV 籌碼與背離
        # 日線 OBV 趨勢 (簡單看近3日)
        if len(df) >= 3 and current['OBV'] > df['OBV'].iloc[-3]:
            score += 1
            details.append("✅ 短線 OBV 資金進駐 (+1)")
            
        # OBV 背離偵測
        div_obv = self._detect_divergence(df, 'OBV')
        if div_obv == 'bull':
            score += 2
            details.append("💎 OBV 出現【量價底背離】(主力吃貨) (+2)")
        elif div_obv == 'bear':
            score -= 2
            details.append("💀 OBV 出現【量價頂背離】(主力出貨) (-2)")

        # 6. DMI 短線趨勢
        if current['ADX'] > 25:
             if current['+DI'] > current['-DI']:
                 score += 1
                 details.append(f"✅ 日線 DMI 多方攻擊 (ADX={current['ADX']:.1f}) (+1)")
             else:
                 score -= 1
                 details.append(f"🔻 日線 DMI 空方下殺 (ADX={current['ADX']:.1f}) (-1)")

        # 7. RSI 背離 (輔助)
        div_rsi = self._detect_divergence(df, 'RSI')
        if div_rsi == 'bull':
            score += 1
            details.append("✅ RSI 出現底背離 (+1)")
        elif div_rsi == 'bear':
            score -= 1
            details.append("🔻 RSI 出現頂背離 (-1)")

        # 9. K線形態學 (K-Line Patterns)
        kline_score, kline_msgs = self._detect_kline_patterns(df)
        score += kline_score
        details.extend(kline_msgs)
        
        # 10. 高階形態學 (W底/M頭) - 新增
        try:
             morph_score, morph_msgs = self._detect_morphology(df)
             score += morph_score
             details.extend(morph_msgs)
        except Exception as e:
             pass # 防止 scipy 運算錯誤影響整體

        # 11. 量價關係 (Price-Volume)
        pv_score, pv_msgs = self._analyze_price_volume(df)
        score += pv_score
        details.extend(pv_msgs)

        # 12. 神奇九轉 (Magic Nine Turns)
        td_buy = current.get('TD_Buy_Setup', 0)
        td_sell = current.get('TD_Sell_Setup', 0)
        
        if td_buy == 9:
             score += 2
             details.append("9️⃣ 神奇九轉【買進訊號】(低檔鈍化轉折) (+2)")
        elif td_buy == 8:
             details.append("8️⃣ 神奇九轉【買進前夕】(數到 8 了) (+0.5)")
             
        if td_sell == 9:
             score -= 2
             details.append("9️⃣ 神奇九轉【賣出訊號】(高檔鈍化轉折) (-2)")
        elif td_sell == 8:
             details.append("8️⃣ 神奇九轉【賣出前夕】(數到 8 了) (-0.5)")

        return score, details

    def _determine_scenario(self, trend_score, daily_details):
        """
        判斷劇本 Scenario A/B/C/D
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
        from scipy.signal import argrelextrema
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
        from scipy.signal import argrelextrema
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

    def _detect_divergence(self, df, indicator_name, window=20):
        """
        簡易背離偵測引擎
        window: 觀察最近 N 根 K 棒
        邏輯:
           - 底背離 (Bull): 股價創新低 (Price < Price_min)，但指標沒創新低 (Ind > Ind_min)
           - 頂背離 (Bear): 股價創新高 (Price > Price_max)，但指標沒創新高 (Ind < Ind_max)
        注意：這只是極簡版偵測，標準背離需要找 Pivot Points，這裡用區間極值比較法。
        """
        if len(df) < window + 5:
            return None
            
        recent = df.iloc[-5:] # 最近 5 天
        past = df.iloc[-window:-5] # 過去 5~20 天
        
        # 指標數據
        ind_recent = recent[indicator_name]
        ind_past = past[indicator_name]
        
        # 股價數據 (通常看 Close 或 Low/High)
        price_recent_low = recent['Low'].min()
        price_past_low = past['Low'].min()
        
        price_recent_high = recent['High'].max()
        price_past_high = past['High'].max()
        
        # 底背離判定:
        # 最近股價破新低, 但最近指標最低點 > 過去指標最低點
        if price_recent_low < price_past_low:
             if ind_recent.min() > ind_past.min():
                 return 'bull'
                 
        # 頂背離判定:
        # 最近股價創新高, 但最近指標最高點 < 過去指標最高點
        if price_recent_high > price_past_high:
            if ind_recent.max() < ind_past.max():
                return 'bear'
                
        return None
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
