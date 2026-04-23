"""
Trigger score 的 add-on 因子 — 從 analysis_engine.py TechnicalAnalyzer 抽出（M2 拆分）。

全部為純函式，不需要 TechnicalAnalyzer self 狀態。輸入 df / ticker / chip_data，
輸出 (score, details)。

包含:
  - analyze_tw_chip_factors   — 台股籌碼 (外資/投信/融資/券資比/借券, C2-b IC 驗證版)
  - analyze_us_chip_factors   — 美股籌碼 (內部人交易 / 空頭變化)
  - analyze_tw_market_sentiment  — 台指 PCR + 期貨正逆價差
  - analyze_us_market_sentiment  — CNN Fear & Greed Index
  - analyze_revenue_catalyst  — 台股營收驚喜 + 連續成長/衰退
  - analyze_etf_signal        — 台股主動型 ETF 同步買賣超

NOTE: market_sentiment / revenue_catalyst 2026-04-22 已從 trigger_score 移除,
保留函式供未來重啟或其他模組使用。
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# 與 analysis_engine.py 共用的 cap 常數（保持同步）
MARKET_SENTIMENT_CAP = 0.8
REVENUE_CATALYST_CAP = 0.5
ETF_SIGNAL_CAP = 0.6


def analyze_tw_chip_factors(df, chip_data, trend_score=0):
    """
    籌碼面評分 (Chip Analysis) — C2-b IC 驗證版（台股）

    方向依據 C2-b 截面 IC 驗證結果 (2026-04-16)：
    - 外資：IC 微弱正但不顯著 → 保留小幅正分
    - 投信：IC 顯著負 (IR -0.32) → 反轉：買超=減分（過熱訊號）
    - 融資：IC 顯著負 (IR -0.24) → 增加=減分（散戶追漲逆向指標）
    - 券資比：IC 最強負 (IR -0.57) → 高=減分（空方正確看空）
    - 借券：IC 負 → 增加=減分（維持原方向）
    """
    score = 0
    details = []

    if not chip_data:
        return 0, []

    try:
        current_price = df.iloc[-1]['Close'] if not df.empty else 0
        recent_volume = df.iloc[-5:]['Volume'].mean() / 1000 if len(df) >= 5 else 0

        # --- 1. 法人動向：外資(微正) + 投信(反轉) ---
        df_inst = chip_data.get('institutional')
        if df_inst is not None and not df_inst.empty and not df.empty:
            recent_inst = df_inst.iloc[-5:]

            foreign_buy = recent_inst['外資'].sum() if '外資' in recent_inst.columns else 0
            trust_buy = recent_inst['投信'].sum() if '投信' in recent_inst.columns else 0
            foreign_lots = foreign_buy / 1000
            trust_lots = trust_buy / 1000

            # 顯著性門檻（成交值比率）
            buy_amt_m = (abs(foreign_lots + trust_lots) * current_price * 1000) / 1e6 if current_price > 0 else 0
            is_significant = (buy_amt_m > 50) or (abs(foreign_lots + trust_lots) / max(recent_volume, 1) > 0.15)

            # 外資：微弱正向（IC +0.06 不顯著，保守給小分）
            if is_significant and abs(foreign_lots) > 0:
                if foreign_lots > 0:
                    score += 0.3
                    details.append(f"💰 外資近5日買超 ({foreign_lots:+,.0f}張) (+0.3)")
                else:
                    score -= 0.3
                    details.append(f"💸 外資近5日賣超 ({foreign_lots:+,.0f}張) (-0.3)")

            # 投信：IC 顯著負 → 反轉（買超=減分，賣超=加分）
            if is_significant and abs(trust_lots) > 0:
                if trust_lots > 0:
                    score -= 0.5
                    details.append(f"🔥 投信近5日買超 ({trust_lots:+,.0f}張) → 過熱警報 (-0.5)")
                else:
                    score += 0.3
                    details.append(f"❄️ 投信近5日賣超 ({trust_lots:+,.0f}張) → 籌碼沉澱 (+0.3)")

            # 外資+投信同步賣超（雙重沉澱）= 加分
            if is_significant and foreign_lots < 0 and trust_lots < 0:
                score += 0.3
                details.append(f"❄️ 外資+投信同步賣超 → 籌碼乾淨 (+0.3)")

        # --- 2. 融資：IC 顯著負 → 增加=減分 ---
        df_margin = chip_data.get('margin')
        if df_margin is not None and not df_margin.empty:
            last_m = df_margin.iloc[-1]
            lim = last_m.get('融資限額', 0)
            bal = last_m.get('融資餘額', 0)
            short_bal = last_m.get('融券餘額', 0)

            # 融資使用率
            if lim > 0:
                util = (bal / lim) * 100
                if util > 60:
                    score -= 0.4
                    details.append(f"⚠️ 融資使用率偏高 ({util:.1f}%) → 散戶追漲 (-0.4)")
                elif util < 20:
                    score += 0.2
                    details.append(f"✨ 融資水位偏低 ({util:.1f}%) → 籌碼乾淨 (+0.2)")

            # 融資增量（近 5 日變動 vs 20 日均量標準化）
            if len(df_margin) >= 20 and '融資餘額' in df_margin.columns:
                margin_now = df_margin['融資餘額'].iloc[-1]
                margin_5ago = df_margin['融資餘額'].iloc[-5] if len(df_margin) >= 5 else margin_now
                margin_chg_5d = margin_now - margin_5ago
                margin_avg20 = df_margin['融資餘額'].iloc[-20:].mean()
                if margin_avg20 > 0:
                    chg_pct = (margin_chg_5d / margin_avg20) * 100
                    if chg_pct > 5:
                        score -= 0.3
                        details.append(f"📈 融資5日增 {chg_pct:+.1f}% → 散戶追漲 (-0.3)")
                    elif chg_pct < -5:
                        score += 0.2
                        details.append(f"📉 融資5日減 {chg_pct:+.1f}% → 籌碼沉澱 (+0.2)")

            # --- 3. 券資比：IC 最強 (IR -0.57) → 高=減分 ---
            if bal > 0 and short_bal >= 0:
                ms_ratio = short_bal / bal
                if ms_ratio > 0.3:
                    score -= 0.6
                    details.append(f"🔴 券資比 {ms_ratio:.1%} 偏高 → 空方看空 (-0.6)")
                elif ms_ratio > 0.15:
                    score -= 0.3
                    details.append(f"⚠️ 券資比 {ms_ratio:.1%} → 空方關注 (-0.3)")
                elif ms_ratio < 0.03:
                    score += 0.2
                    details.append(f"✨ 券資比 {ms_ratio:.1%} 極低 → 無空方壓力 (+0.2)")

        # --- 4. 借券 (SBL)：方向維持（IC 負 = 增加=減分）---
        df_sbl = chip_data.get('sbl')
        if df_sbl is not None and not df_sbl.empty and len(df_sbl) >= 30:
            if '借券賣出餘額' in df_sbl.columns and '借券賣出' in df_sbl.columns and '借券還券' in df_sbl.columns:
                recent5 = df_sbl.iloc[-5:]
                net5d = recent5['借券賣出'].sum() - recent5['借券還券'].sum()
                ma30_bal = df_sbl['借券賣出餘額'].iloc[-30:].mean()

                if ma30_bal > 0:
                    net5d_pct = (net5d / ma30_bal) * 100

                    if net5d_pct > 10:
                        score -= 0.6
                        details.append(f"🔴 借券大量增加 5日淨增{net5d/1000:+,.0f}張 ({net5d_pct:+.1f}% of 30日均) (-0.6)")
                    elif net5d_pct > 5:
                        score -= 0.3
                        details.append(f"⚠️ 借券增加 5日淨增{net5d/1000:+,.0f}張 ({net5d_pct:+.1f}%) (-0.3)")
                    elif net5d_pct < -10:
                        score += 0.4
                        details.append(f"🟢 借券大量回補 5日淨減{abs(net5d)/1000:,.0f}張 ({net5d_pct:.1f}%) (+0.4)")
                    elif net5d_pct < -5:
                        score += 0.2
                        details.append(f"✨ 借券回補 5日淨減{abs(net5d)/1000:,.0f}張 ({net5d_pct:.1f}%) (+0.2)")

    except Exception as e:
        logger.warning(f"Chip scoring error: {e}")

    return score, details


def analyze_us_chip_factors(df, ticker, us_chip_data, trend_score=0):
    """
    美股籌碼面評分 (US Stock Chip Analysis) - 精簡版
    只保留 IC 有效因子:
    - 內部人交易 (學術驗證最強的籌碼信號)
    - 空頭變化 (動態指標)
    移除: 機構持股比例(靜態,幾乎永遠>60%), 分析師評等(幾乎永遠buy)

    us_chip_data=None 時會 lazy-load（呼叫 USStockChipAnalyzer）。
    """
    score = 0
    details = []

    if not us_chip_data:
        try:
            from us_stock_chip import USStockChipAnalyzer
            us_analyzer = USStockChipAnalyzer()
            us_chip_data, err = us_analyzer.get_chip_data(ticker)

            if err or not us_chip_data:
                details.append(f"ℹ️ 美股籌碼數據暫無法取得")
                return 0, details
        except Exception as e:
            logger.warning(f"US Chip load error: {e}")
            return 0, []

    try:
        # 1. 內部人交易 — 計分因子
        insider = us_chip_data.get('insider_trades', {})
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
        short = us_chip_data.get('short_interest', {})
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
        inst = us_chip_data.get('institutional', {})
        inst_pct = inst.get('percent_held', 0)
        if inst_pct > 0:
            details.append(f"📊 機構持股 {inst_pct:.1f}% [資訊]")

    except Exception as e:
        logger.warning(f"US Chip scoring error: {e}")

    return score, details


def analyze_tw_market_sentiment():
    """
    台指市場情緒因子 — TAIFEX PCR + 期貨正逆價差
    Cap: +/- MARKET_SENTIMENT_CAP
    """
    score = 0.0
    details = []

    try:
        from taifex_data import TAIFEXData
        taifex = TAIFEXData()

        # --- PCR (反向指標) ---
        pcr_data = taifex.get_put_call_ratio()
        pc_ratio = pcr_data.get('pc_ratio', 0.0)
        if pc_ratio > 0:
            if pc_ratio > 1.3:
                # 極度恐懼 → 反向看多
                score += 0.5
                details.append(f"📊 PCR={pc_ratio:.2f} 極度恐懼 → 反向看多 (+0.5)")
            elif pc_ratio > 1.1:
                score += 0.25
                details.append(f"📊 PCR={pc_ratio:.2f} 偏恐懼 → 反向偏多 (+0.25)")
            elif pc_ratio < 0.7:
                # 極度貪婪 → 反向看空
                score -= 0.5
                details.append(f"📊 PCR={pc_ratio:.2f} 極度貪婪 → 反向看空 (-0.5)")
            elif pc_ratio < 0.9:
                score -= 0.25
                details.append(f"📊 PCR={pc_ratio:.2f} 偏貪婪 → 反向偏空 (-0.25)")
            else:
                details.append(f"📊 PCR={pc_ratio:.2f} 中性 [資訊]")

        # --- 期貨正逆價差 (順向指標) ---
        basis_data = taifex.get_futures_basis()
        basis_pct = basis_data.get('basis_pct', 0.0)
        if basis_data.get('futures_price', 0) > 0:
            if basis_pct > 0.3:
                # 正價差偏大 → 市場偏多
                score += 0.3
                details.append(f"📈 期貨正價差 {basis_pct:.2f}% → 偏多 (+0.3)")
            elif basis_pct < -0.3:
                # 逆價差 → 市場偏空
                score -= 0.3
                details.append(f"📉 期貨逆價差 {basis_pct:.2f}% → 偏空 (-0.3)")
            else:
                details.append(f"📊 期貨價差 {basis_pct:+.2f}% 中性 [資訊]")

    except Exception as e:
        logger.debug(f"Market sentiment scoring skipped: {e}")

    score = max(-MARKET_SENTIMENT_CAP, min(MARKET_SENTIMENT_CAP, score))
    return score, details


def analyze_us_market_sentiment():
    """
    CNN Fear & Greed Index — 美股市場情緒（反向指標）
    Extreme Fear (<25) → 反向看多; Extreme Greed (>75) → 反向看空
    Cap: +/- MARKET_SENTIMENT_CAP
    """
    score = 0.0
    details = []

    try:
        from cnn_fear_greed import CNNFearGreedIndex
        cnn = CNNFearGreedIndex()
        fg = cnn.get_index()

        fg_score = fg.get('score', 50)
        label = fg.get('label', 'Neutral')

        if fg_score < 20:
            score = 0.6
            details.append(f"🇺🇸 CNN F&G={fg_score:.0f} Extreme Fear → contrarian bullish (+0.6)")
        elif fg_score < 35:
            score = 0.3
            details.append(f"🇺🇸 CNN F&G={fg_score:.0f} Fear → contrarian bullish (+0.3)")
        elif fg_score > 80:
            score = -0.6
            details.append(f"🇺🇸 CNN F&G={fg_score:.0f} Extreme Greed → contrarian bearish (-0.6)")
        elif fg_score > 65:
            score = -0.3
            details.append(f"🇺🇸 CNN F&G={fg_score:.0f} Greed → contrarian bearish (-0.3)")
        else:
            details.append(f"🇺🇸 CNN F&G={fg_score:.0f} {label} [info]")

    except Exception as e:
        logger.debug(f"CNN F&G scoring skipped: {e}")

    score = max(-MARKET_SENTIMENT_CAP, min(MARKET_SENTIMENT_CAP, score))
    return score, details


def analyze_revenue_catalyst(ticker, is_us_stock=False):
    """
    營收催化劑因子 — 台股限定
    營收驚喜 + 連續成長/衰退 → 基本面動能
    Cap: +/- REVENUE_CATALYST_CAP
    """
    score = 0.0
    details = []

    if is_us_stock:
        return score, details

    ticker = ticker.replace('.TW', '').replace('.TWO', '').strip()
    if not ticker.isdigit():
        return score, details

    try:
        from dividend_revenue import RevenueTracker
        rt = RevenueTracker()

        # --- 營收驚喜 ---
        surprise = rt.detect_revenue_surprise(ticker)
        if surprise.get('is_surprise'):
            magnitude = surprise.get('magnitude', 0)
            if surprise['direction'] == 'positive':
                score += 0.5
                details.append(f"🚀 營收正驚喜 +{magnitude:.1f}% → 基本面催化 (+0.5)")
            else:
                score -= 0.5
                details.append(f"⚠️ 營收負驚喜 {magnitude:.1f}% → 基本面利空 (-0.5)")

        # --- 連續成長/衰退 ---
        rev_alert = rt.get_revenue_alert(ticker)
        consec = rev_alert.get('consecutive_growth_months', 0)
        if consec >= 3:
            bonus = min(0.3, consec * 0.1)
            score += bonus
            details.append(f"📈 營收連續 {consec} 個月成長 (+{bonus:.1f})")
        elif consec <= -3:
            penalty = min(0.3, abs(consec) * 0.1)
            score -= penalty
            details.append(f"📉 營收連續 {abs(consec)} 個月衰退 (-{penalty:.1f})")

    except Exception as e:
        logger.debug(f"Revenue catalyst scoring skipped: {e}")

    score = max(-REVENUE_CATALYST_CAP, min(REVENUE_CATALYST_CAP, score))
    return score, details


def analyze_etf_signal(ticker, is_us_stock=False):
    """
    主動型 ETF 同步買賣超因子 — 台股限定
    多檔主動型 ETF 同時增持 → 聰明錢訊號
    Cap: +/- ETF_SIGNAL_CAP
    """
    score = 0.0
    details = []

    if is_us_stock:
        return score, details

    ticker = ticker.replace('.TW', '').replace('.TWO', '').strip()
    if not ticker.isdigit():
        return score, details

    try:
        from etf_signal import ETFSignal
        etf = ETFSignal()
        sig = etf.get_stock_signal(ticker, days=5)

        if sig is None:
            return score, details

        buy_count = sig['buy_count']
        sell_count = sig['sell_count']
        net_lots = sig['net_lots']

        if buy_count >= 3 and buy_count > sell_count:
            score = 0.6
            details.append(f"🏦 ETF 同步買超 {buy_count} 檔 (淨 {net_lots:+.0f} 張) (+0.6)")
        elif buy_count >= 2 and buy_count > sell_count:
            score = 0.3
            details.append(f"🏦 ETF 買超 {buy_count} 檔 (淨 {net_lots:+.0f} 張) (+0.3)")
        elif sell_count >= 3 and sell_count > buy_count:
            score = -0.6
            details.append(f"🏦 ETF 同步賣超 {sell_count} 檔 (淨 {net_lots:+.0f} 張) (-0.6)")
        elif sell_count >= 2 and sell_count > buy_count:
            score = -0.3
            details.append(f"🏦 ETF 賣超 {sell_count} 檔 (淨 {net_lots:+.0f} 張) (-0.3)")
        elif buy_count > 0 or sell_count > 0:
            details.append(f"🏦 ETF 活動: 買 {buy_count} / 賣 {sell_count} 檔 (淨 {net_lots:+.0f} 張) [資訊]")

    except Exception as e:
        logger.debug(f"ETF signal scoring skipped: {e}")

    score = max(-ETF_SIGNAL_CAP, min(ETF_SIGNAL_CAP, score))
    return score, details
