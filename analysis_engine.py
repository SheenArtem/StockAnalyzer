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
        計算日線進場訊號 (Trigger Score) -3 ~ +3
        """
        score = 0
        details = []

        if df.empty or len(df) < 5:
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

        # 2. MACD 動能
        if current['Hist'] > 0:
            score += 1
            details.append("✅ MACD 柱狀體翻紅 (+1)")
            if current['Hist'] > prev['Hist']:
                score += 0.5 # 動能增強
                details.append("🔥 MACD 動能持續增強 (+0.5)")
        else:
            score -= 1
            details.append("🔻 MACD 柱狀體翻綠 (-1)")

        # 3. KD指標
        # 黃金交叉: K > D 且 前一天 K < D (或是單純看 K > D 判斷多方優勢)
        if current['K'] > current['D']:
            score += 1
            details.append("✅ KD 黃金交叉/呈現多方排列 (+1)")
        else:
            score -= 1
            details.append("🔻 KD 死亡交叉/呈現空方排列 (-1)")

        # 4. 布林通道 (輔助)
        bandwidth = (current['BB_Up'] - current['BB_Lo']) / current['MA20']
        details.append(f"ℹ️ 布林通道帶寬: {bandwidth*100:.1f}%")

        return score, details

    def _determine_scenario(self, trend_score, daily_details):
        """
        判斷劇本 Scenario A/B/C/D
        """
        # 0. 先檢查是否為盤整 (ADX 在 daily_details 裡不好拿，改用 trend_score 判斷)
        # 這裡簡化邏輯，直接用 Trend Score 分類
        
        scenario = {
            "code": "N",
            "title": "觀察中 (Neutral)",
            "color": "gray",
            "desc": "多空不明，建議觀望。"
        }

        # 劇本 A: 週線強多 (>=3)
        if trend_score >= 3:
            scenario = {
                "code": "A",
                "title": "🔥 劇本 A：強力進攻 (Aggressive Buy)",
                "color": "red", # 台股紅漲綠跌
                "desc": "週線趨勢強勁，日線若有買訊應順勢重倉。"
            }
        # 劇本 B: 週線偏多 (1~2)
        elif 1 <= trend_score < 3:
            scenario = {
                "code": "B",
                "title": "⏳ 劇本 B：拉回關注 (Pullback Watch)",
                "color": "orange",
                "desc": "長線多頭保護，但力道未全開。等待日線回檔止穩後進場。"
            }
        # 劇本 C: 週線偏空 (-2~0)
        elif -2 <= trend_score <= 0:
            scenario = {
                "code": "C",
                "title": "⚠️ 劇本 C：反彈搶短 (Rebound)",
                "color": "blue", # 偏冷色調
                "desc": "逆勢操作，僅適合短線高手，嚴設停損。"
            }
        # 劇本 D: 週線強空 (<-2)
        else:
            scenario = {
                "code": "D",
                "title": "🛑 劇本 D：空手/做空 (Avoid)",
                "color": "green", # 台股綠跌
                "desc": "趨勢顯著向下，切勿隨意摸底。"
            }
            
        return scenario
