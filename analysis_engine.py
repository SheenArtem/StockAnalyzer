import pandas as pd
import numpy as np

class TechnicalAnalyzer:
    def __init__(self, ticker, df_week, df_day):
        self.ticker = ticker
        self.df_week = df_week
        self.df_day = df_day

    def run_analysis(self):
        """
        執行完整分析流程
        Returns:
            dict: 包含 趨勢分數, 觸發分數, 劇本, 詳細評分項目
        """
        trend_score, trend_details = self._calculate_trend_score(self.df_week)
        trigger_score, trigger_details = self._calculate_trigger_score(self.df_day)
        
        scenario = self._determine_scenario(trend_score, trigger_details) # Check details for ADX special case
        
        return {
            "ticker": self.ticker,
            "trend_score": trend_score,
            "trend_details": trend_details,
            "trigger_score": trigger_score,
            "trigger_details": trigger_details,
            "scenario": scenario
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
        
        # 3. MACD 動能與背離
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

        # 4. KD指標
        if current['K'] > current['D']:
            score += 1
            details.append("✅ KD 黃金交叉/多方排列 (+1)")
        else:
            score -= 1
            details.append("🔻 KD 死亡交叉/空方排列 (-1)")

        # 5. OBV 籌碼與背離
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
                score += 2
                msgs.append("🕯️ 出現【多頭吞噬】強力反轉訊號 (+2)")
        
        # 空頭吞噬: 昨陽 今陰, 今實體包覆昨實體
        if dir_p == 1 and dir_c == -1:
            if c['Open'] >= p['Close'] and c['Close'] <= p['Open']:
                score -= 2
                msgs.append("🕯️ 出現【空頭吞噬】高檔反轉訊號 (-2)")
                
        # 2. 爆量長紅 (Explosive Volume Attack)
        # 成交量 > 5日均量 * 2 且 收長紅
        vol_ma5 = df['Volume'].rolling(5).mean().iloc[-1]
        if c['Volume'] > 2.0 * vol_ma5 and dir_c == 1 and is_long_c:
             score += 2
             msgs.append(f"💣 出現【爆量長紅】攻擊訊號 (量增{c['Volume']/vol_ma5:.1f}倍) (+2)")

        # 3. 晨星 (Morning Star) - 簡化版
        # 跌 -> 小十字 -> 漲
        # 定義: 前日跌, 昨日實體小(十字/紡錘), 今日漲且收盤高於前日實體的一半
        is_star_p = body_p < 0.5 * avg_body # 昨日是小黑或十字
        if dir_pp == -1 and is_star_p and dir_c == 1:
            midpoint_pp = (pp['Open'] + pp['Close']) / 2
            if c['Close'] > midpoint_pp:
                score += 2
                msgs.append("✨ 出現【晨星】底部轉折訊號 (+2)")
                
        # 4. 十字變盤線 (Doji)
        # 開收盤極度接近
        if body_c < 0.1 * avg_body:
            msgs.append("⚠️ 出現【十字線】多空變盤訊號 (Info)")

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
