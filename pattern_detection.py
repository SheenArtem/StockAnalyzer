"""
K 線形態與圖表形態偵測 — 從 analysis_engine.py TechnicalAnalyzer 抽出（M2 拆分）。

全部是純函式：input 是 DataFrame（需含 OHLCV + 指標欄位），output 是 (score, msgs)
或 divergence 類型字串。沒有 self 狀態相依，便於單獨測試。

函式：
  - detect_kline_patterns(df)        — 吞噬 / 晨星 / 爆量長紅 / 十字等 K 線組合
  - detect_morphology(df)            — W 底 / M 頭 / 頭肩 / 三角收斂（總成）
  - detect_double_patterns(df)       — W 底 / M 頭
  - detect_head_and_shoulders(df)    — 頭肩頂 / 頭肩底（含量能驗證）
  - detect_triangle_convergence(df)  — 三角收斂（量縮壓縮）
  - detect_divergence(df, indicator_name, window=40) — 價格 vs 指標背離
  - analyze_price_volume(df)         — 價量四象限關係
"""
from __future__ import annotations

import numpy as np
from scipy.signal import argrelextrema


def detect_kline_patterns(df):
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


def detect_morphology(df):
    """
    高階形態學偵測 (Chart Patterns) - 總成
    包含: W底/M頭, 頭肩頂/底, 三角收斂
    """
    if len(df) < 60:
        return 0, []

    score = 0
    msgs = []

    # 1. 基礎 W底 / M頭
    s1, m1 = detect_double_patterns(df)
    score += s1
    msgs.extend(m1)

    # 2. 進階 頭肩頂 / 頭肩底
    s2, m2 = detect_head_and_shoulders(df)
    score += s2
    msgs.extend(m2)

    # 3. 三角收斂
    s3, m3 = detect_triangle_convergence(df)
    score += s3
    msgs.extend(m3)

    return score, msgs


def detect_double_patterns(df):
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


def detect_head_and_shoulders(df):
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


def detect_triangle_convergence(df):
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


def detect_divergence(df, indicator_name, window=40):
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


def analyze_price_volume(df):
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
