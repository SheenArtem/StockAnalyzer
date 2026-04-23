import pandas as pd
import numpy as np
import logging
import time

from pattern_detection import (
    detect_morphology,
    detect_divergence,
    analyze_price_volume,
)
from addon_factors import (
    analyze_tw_chip_factors,
    analyze_us_chip_factors,
    analyze_etf_signal,
)

# Configure logging
logger = logging.getLogger(__name__)

# === 可調參數 (Tunable Constants) ===
DEFAULT_BUY_THRESHOLD = 3       # 觸發分數買進門檻
DEFAULT_SELL_THRESHOLD = -2     # 觸發分數賣出門檻
CHIP_SCORE_CAP = 2.0            # 籌碼分數上下限 (±)  C2-b 增加因子後放寬
TREND_SCORE_RANGE = (-5, 5)     # 趨勢分數範圍
TRIGGER_SCORE_RANGE = (-10, 10) # 觸發分數範圍
GROUP_SCALE_FACTOR = 3.33       # 3 組 median → [-10,+10] 的縮放因子
MORPHOLOGY_CAP = 2              # 形態學分數上限 (±)
EFI_DEADZONE_RATIO = 0.3       # EFI 死區 = std × 此比例
CALIBRATION_MEAN = 0.07         # 校準分佈 mean (196K 樣本)
CALIBRATION_STD = 4.32          # 校準分佈 std
MARKET_SENTIMENT_CAP = 0.8      # (2026-04-22 已停用) PCR+基差是大盤訊號, market_banner 顯示即可
REVENUE_CATALYST_CAP = 0.5      # (2026-04-22 已停用) 月頻資料塞日線 trigger 不當, 改走 📋 基本面快照
ETF_SIGNAL_CAP = 0.6            # ETF 同步買賣超分數上下限 (±) — 主動型 ETF 持倉變化

# === Regime HMM -- Group weight profiles per market regime ===
# Weights are relative multipliers; normalized before use so score range is preserved.
# trending: trust trend signals, discount volume noise
# ranging:  discount trend (false breakouts), trust volume confirmation
# volatile: reduce all group confidence, emphasize volume
# neutral:  equal weights (fallback)
# ⚠️ VF-G3 Part 2 驗證（2026-04-17, D 級）：regime selection 乘數無 alpha
#   V1 vs V2 全 1.0 差距：IC +0.0003、Sharpe +0.003，遠低於決策門檻
#   walk-forward 61 windows：V1 OOS 無穩定勝 flat
#   空頭年驗證缺失警告：樣本 2021-25 僅 2022 空頭，空頭年可能反轉
#   → 全改 1.0 停用 regime 選股 overlay；保留 dict 結構便於未來 regression / 空頭年復活比對
#   報告：reports/vfg3_part2_regime_selection_mult.md
REGIME_GROUP_WEIGHTS = {
    'trending': {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0},
    'ranging':  {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0},
    'volatile': {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0},
    'neutral':  {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0},
}
# Add-on factor cap multipliers per regime — 同 VF-G3 P2 驗證結論，全 1.0 停用
REGIME_ADDON_MULT = {
    'trending': {'chip': 1.0, 'sentiment': 1.0, 'revenue': 1.0, 'etf': 1.0},
    'ranging':  {'chip': 1.0, 'sentiment': 1.0, 'revenue': 1.0, 'etf': 1.0},
    'volatile': {'chip': 1.0, 'sentiment': 1.0, 'revenue': 1.0, 'etf': 1.0},
    'neutral':  {'chip': 1.0, 'sentiment': 1.0, 'revenue': 1.0, 'etf': 1.0},
}

# ====================================================================
# Module-level HMM market regime cache (shared across all analyzers)
# ====================================================================
_hmm_cache = {}  # key: market ('tw'/'us') -> {'regime', 'confidence', 'ts'}
_HMM_CACHE_TTL = 3600  # 1 hour


def detect_market_regime_hmm(market='tw'):
    """
    HMM-based market regime detection using index data.
    Fits a 3-state GaussianHMM on recent index returns + volatility,
    then labels states as trending / ranging / volatile.

    Args:
        market: 'tw' or 'us'

    Returns:
        dict: {'regime': str, 'confidence': float, 'details': str}
    """
    fallback = {'regime': 'neutral', 'confidence': 0.0, 'details': 'HMM unavailable'}

    # Check cache (includes failed attempts with shorter TTL)
    cached = _hmm_cache.get(market)
    if cached and (time.time() - cached['ts']) < _HMM_CACHE_TTL:
        return {k: v for k, v in cached.items() if k != 'ts'}

    def _cache_fallback(fb):
        """Cache fallback with shorter TTL to avoid repeated failures."""
        _hmm_cache[market] = {**fb, 'ts': time.time() - _HMM_CACHE_TTL + 300}
        return fb

    try:
        from hmmlearn.hmm import GaussianHMM
        import yfinance as yf
    except ImportError:
        logger.warning("hmmlearn not installed, falling back to neutral regime")
        return _cache_fallback(fallback)

    # Fetch index data
    index_ticker = '^TWII' if market == 'tw' else '^GSPC'
    try:
        idx = yf.download(index_ticker, period='8mo', interval='1d',
                          progress=False, auto_adjust=True)
        if idx is None or len(idx) < 60:
            logger.warning("Insufficient index data for HMM (%s)", index_ticker)
            return _cache_fallback(fallback)
        # Flatten MultiIndex columns if present (yfinance >= 0.2)
        if isinstance(idx.columns, pd.MultiIndex):
            idx.columns = idx.columns.get_level_values(0)
    except Exception as e:
        logger.warning("Failed to fetch index data for HMM: %s", e)
        return _cache_fallback(fallback)

    try:
        close = idx['Close'].dropna()
        if len(close) < 60:
            return _cache_fallback(fallback)

        # Features: log returns, 10d rolling volatility, 20d rolling volatility
        log_ret = np.log(close / close.shift(1)).dropna()
        vol_10 = log_ret.rolling(10).std().dropna()
        vol_20 = log_ret.rolling(20).std().dropna()

        # Align all series
        common_idx = vol_20.index
        features = pd.DataFrame({
            'ret': log_ret.reindex(common_idx),
            'vol_10': vol_10.reindex(common_idx),
            'vol_20': vol_20.reindex(common_idx),
        }).dropna()

        if len(features) < 40:
            return fallback

        X_raw = features.values

        # Standardize features for numerical stability
        X_mean = X_raw.mean(axis=0)
        X_std = X_raw.std(axis=0)
        X_std[X_std < 1e-10] = 1.0  # avoid division by zero
        X = (X_raw - X_mean) / X_std

        # Fit 3-state GaussianHMM (diag covariance for robustness)
        model = GaussianHMM(
            n_components=3, covariance_type='diag',
            n_iter=100, random_state=42, verbose=False,
        )
        model.fit(X)
        states = model.predict(X)
        current_state = int(states[-1])

        # Label states by mean return and mean volatility
        # Use standardized X for classification, raw X for display
        state_stats = []
        state_raw = []
        for s in range(3):
            mask = states == s
            if mask.sum() == 0:
                state_stats.append({'ret': 0, 'vol': 999})
                state_raw.append({'ret': 0, 'vol': 0})
                continue
            state_stats.append({
                'ret': float(np.mean(X[mask, 0])),
                'vol': float(np.mean(X[mask, 1])),
            })
            state_raw.append({
                'ret': float(np.mean(X_raw[mask, 0])),
                'vol': float(np.mean(X_raw[mask, 1])),
            })

        # Sort states: trending = highest abs(mean return), ranging = lowest vol,
        # volatile = highest vol
        abs_rets = [abs(s['ret']) for s in state_stats]
        vols = [s['vol'] for s in state_stats]

        # Identify: highest vol = volatile, lowest vol = ranging, other = trending
        volatile_state = int(np.argmax(vols))
        ranging_state = int(np.argmin(vols))
        trending_state = [i for i in range(3) if i != volatile_state and i != ranging_state][0]

        # If two states tie (e.g. volatile == ranging), use abs return as tiebreaker
        if volatile_state == ranging_state:
            # Fallback: highest abs return = trending, lowest = ranging, middle = volatile
            sorted_by_absret = sorted(range(3), key=lambda i: abs_rets[i])
            ranging_state, volatile_state, trending_state = sorted_by_absret

        state_map = {
            trending_state: 'trending',
            ranging_state: 'ranging',
            volatile_state: 'volatile',
        }

        regime = state_map[current_state]

        # Confidence from posterior probability
        posteriors = model.predict_proba(X)
        confidence = float(posteriors[-1, current_state])

        # State info for details (use raw values for interpretability)
        cur_raw = state_raw[current_state]
        detail = (f"HMM {regime} (conf={confidence:.0%}, "
                  f"avg ret={cur_raw['ret']*100:.2f}%/d, vol={cur_raw['vol']*100:.1f}%)")

        result = {'regime': regime, 'confidence': confidence, 'details': detail}

        # Cache result
        _hmm_cache[market] = {**result, 'ts': time.time()}
        logger.info("HMM regime [%s]: %s (conf=%.2f)", market, regime, confidence)
        return result

    except Exception as e:
        logger.warning("HMM fitting failed: %s", e)
        return _cache_fallback(fallback)

class TechnicalAnalyzer:
    def __init__(self, ticker, df_week, df_day, strategy_params=None, chip_data=None, us_chip_data=None, scan_mode=False):
        self.ticker = ticker
        self.df_week = df_week
        self.df_day = df_day
        self.strategy_params = strategy_params # { 'buy': 3, 'sell': -2 }
        self.chip_data = chip_data  # 台股籌碼數據
        self.us_chip_data = us_chip_data  # 美股籌碼數據
        self.scan_mode = scan_mode  # True = 批次掃描模式，跳過 UI-only 的資料抓取（PE/月營收重複等）

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

        # Regime detection (HMM market-level + per-stock ADX/Squeeze)
        # Must run BEFORE trigger scoring so weights can be adjusted
        regime = self._detect_regime(self.df_day)

        # 傳入趨勢分數 + regime 以啟用動態權重
        trigger_score, trigger_details, trigger_breakdown = self._calculate_trigger_score(
            self.df_day, trend_score=trend_score, regime=regime)

        scenario = self._determine_scenario(trend_score, trigger_details)

        # 3.5 Strategy Optimizer Override (覆蓋劇本，確保劇本卡與策略建議一致)
        if self.strategy_params:
            buy_th = self.strategy_params.get('buy', DEFAULT_BUY_THRESHOLD)
            sell_th = self.strategy_params.get('sell', DEFAULT_SELL_THRESHOLD)
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

        # 6. 基本面快照 (台股限定，不計分，資訊提示)
        # scan_mode 跳過：純 UI 提示，不影響評分，節省 FinMind 配額
        fundamental_alerts = [] if self.scan_mode else self._fetch_fundamental_snapshot()

        # 7. 評分百分位 (基於校準分佈 196K 樣本: mean=0.07, std=4.32)
        from scipy.stats import norm
        score_percentile = round(norm.cdf(trigger_score, loc=CALIBRATION_MEAN, scale=CALIBRATION_STD) * 100, 1)

        # 8. Regime — already computed before scoring (line 233)

        return {
            "ticker": self.ticker,
            "trend_score": trend_score,
            "trend_details": trend_details,
            "trigger_score": trigger_score,
            "trigger_details": trigger_details,
            "trigger_breakdown": trigger_breakdown,
            "score_percentile": score_percentile,
            "scenario": scenario,
            "action_plan": action_plan,
            "checklist": checklist,
            "fundamental_alerts": fundamental_alerts,
            "regime": regime
        }

    def _fetch_fundamental_snapshot(self):
        """
        基本面快照 — 台股限定，不計分，僅資訊提示
        整合: 月營收 YoY 驚喜 + PE 本益比位置
        """
        alerts = []
        if self._is_us_stock:
            return alerts

        ticker = self.ticker.replace('.TW', '').replace('.TWO', '').strip()
        if not ticker.isdigit():
            return alerts

        # 1. 月營收驚喜偵測
        try:
            from dividend_revenue import RevenueTracker
            rt = RevenueTracker()
            surprise = rt.detect_revenue_surprise(ticker)
            if surprise.get('is_surprise'):
                direction = surprise['direction']
                emoji = "🚀" if direction == 'positive' else "⚠️"
                alerts.append(f"{emoji} {surprise['text']}")
            else:
                alerts.append(f"📊 {surprise['text']}")

            # 營收趨勢
            rev_alert = rt.get_revenue_alert(ticker)
            trend = rev_alert.get('trend', '')
            consec = rev_alert.get('consecutive_growth_months', 0)
            if consec >= 3:
                alerts.append(f"📈 營收連續 {consec} 個月成長")
            elif consec <= -3:
                alerts.append(f"📉 營收連續 {abs(consec)} 個月衰退")
        except Exception as e:
            logger.debug(f"Revenue snapshot skipped: {e}")

        # 2. 本益比位置
        try:
            from fundamental_analysis import get_taiwan_stock_fundamentals
            fund = get_taiwan_stock_fundamentals(ticker)
            if fund:
                pe_str = fund.get('PE Ratio', 'N/A')
                pb_str = fund.get('PB Ratio', 'N/A')
                dy = fund.get('Dividend Yield', 0)
                if pe_str != 'N/A':
                    pe = float(pe_str)
                    if pe < 10:
                        alerts.append(f"💰 本益比偏低 (PE={pe:.1f})，可能被低估")
                    elif pe > 30:
                        alerts.append(f"⚠️ 本益比偏高 (PE={pe:.1f})，評價偏貴")
                    else:
                        alerts.append(f"📊 本益比 PE={pe:.1f}, PB={pb_str}")
                if dy and isinstance(dy, (int, float)) and dy > 3:
                    alerts.append(f"💵 殖利率 {dy:.2f}% (高息股)")
        except Exception as e:
            logger.debug(f"PE snapshot skipped: {e}")

        return alerts

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

        # B. 爆量長黑 — 使用成交值判斷，避免低價股灌量誤觸發
        tv_ma5 = self._safe_get(current, 'TV_MA5', 0)
        if tv_ma5 > 0:
            tv_threshold = tv_ma5 * 2
            if not self._is_us_stock:
                tv_display = f"{tv_threshold/1e8:,.1f} 億"
            else:
                tv_display = f"${tv_threshold/1e6:,.1f}M"
            checklist['risk'].append(f"若出現 **爆量長黑** (成交值 > {tv_display}) 且收跌，視為主力出貨訊號。")
        elif vol_ma5 > 0:
            # Fallback: TV_MA5 不存在時用傳統成交量
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
            buy_th = self.strategy_params.get('buy', DEFAULT_BUY_THRESHOLD) if self.strategy_params else DEFAULT_BUY_THRESHOLD
            strategy_text = f"🔥 **AI 最佳化訊號 (買進)**：評分 ({trigger_score:.1f}) 已達買進門檻 ({buy_th})，建議進場。"
            rec_entry_low, rec_entry_high = close_price * 0.99, close_price * 1.01
            rec_entry_desc = "現價進場 (AI 訊號)"
            entry_basis = close_price
        elif optimizer == 'sell':
            optimizer_active = True
            is_actionable = False
            sell_th = self.strategy_params.get('sell', DEFAULT_SELL_THRESHOLD) if self.strategy_params else DEFAULT_SELL_THRESHOLD
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
                "entry_confidence": "n/a",
                "pattern_note": "",
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

        return {
            "current_price": close_price,
            "strategy": strategy_text,
            "is_actionable": True,
            "is_us_stock": self._is_us_stock,
            "entry_confidence": entry_confidence,
            "pattern_note": pattern_note,
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

        # 3. OBV 能量潮 (比較近5週趨勢) — 使用成交值加權版本 ±1
        try:
            obv_col = 'OBV_Value' if 'OBV_Value' in df.columns else 'OBV'
            obv_5w_ago = df[obv_col].iloc[-5]
            if self._safe_get(current, obv_col, 0) > obv_5w_ago:
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
            efi_threshold = efi_series.std() * EFI_DEADZONE_RATIO if len(efi_series) >= 10 else 0
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
             morph_score, morph_msgs = detect_morphology(df)
             morph_score = max(-MORPHOLOGY_CAP, min(MORPHOLOGY_CAP, morph_score))
             score += morph_score
             if morph_score != 0:
                 # 修改訊息以標示這是週線
                 morph_msgs = [f"📅 週線{m}" for m in morph_msgs]
             details.extend(morph_msgs)
        except Exception as e:
             logger.debug(f"Morphology detection skipped: {e}")

        # 6. 量價關係 (Price-Volume)
        pv_score, pv_msgs = analyze_price_volume(df)
        score += pv_score
        details.extend(pv_msgs)

        # Clamp to valid range
        score = max(TREND_SCORE_RANGE[0], min(TREND_SCORE_RANGE[1], score))

        return score, details

    def _detect_regime(self, df):
        """
        Regime Detection — HMM market-level + per-stock ADX/Squeeze

        1. HMM on market index (TAIEX / S&P 500) -> market regime
        2. Per-stock ADX + Squeeze -> stock-level context
        3. Final regime = HMM primary, per-stock as modifier

        Returns:
            dict: regime, confidence, details, position_adj, hmm_state
        """
        result = {
            'regime': 'neutral',
            'confidence': 0.5,
            'details': [],
            'position_adj': 1.0,
            'hmm_state': None,
        }

        # --- Step 1: HMM market-level regime ---
        market = 'us' if self._is_us_stock else 'tw'
        hmm = detect_market_regime_hmm(market)
        hmm_regime = hmm['regime']
        hmm_conf = hmm['confidence']
        result['hmm_state'] = hmm_regime
        result['details'].append(hmm['details'])

        # --- Step 2: Per-stock ADX + Squeeze signals ---
        adx = 0
        squeeze_on = False
        atr_expanding = False

        if not df.empty and len(df) >= 30:
            current = df.iloc[-1]
            adx = self._safe_get(current, 'ADX', 0)

            try:
                bb_upper = self._safe_get(current, 'BB_upper', 0)
                bb_lower = self._safe_get(current, 'BB_lower', 0)
                kc_upper = self._safe_get(current, 'KC_upper', 0)
                kc_lower = self._safe_get(current, 'KC_lower', 0)
                if kc_upper > 0 and bb_upper > 0:
                    squeeze_on = bb_upper < kc_upper and bb_lower > kc_lower
            except (KeyError, TypeError):
                pass

            try:
                if len(df) >= 20:
                    atr_col = 'ATR' if 'ATR' in df.columns else None
                    if atr_col:
                        atr_now = self._safe_get(current, atr_col, 0)
                        atr_20ago = self._safe_get(df.iloc[-20], atr_col, 0)
                        if atr_20ago > 0:
                            atr_expanding = (atr_now / atr_20ago) > 1.2
            except (IndexError, KeyError):
                pass

        # Per-stock signal tally
        stock_trend = 0
        stock_range = 0
        if adx > 35:
            stock_trend += 2
        elif adx > 25:
            stock_trend += 1
        if adx < 20:
            stock_range += 1
        if squeeze_on:
            stock_range += 1
            result['details'].append(f"Squeeze ON -- BB inside KC")
        elif atr_expanding:
            stock_trend += 1
            result['details'].append(f"ATR expanding -- volatility rising")

        # --- Step 3: Combine HMM + per-stock ---
        # HMM is primary (market-level); per-stock can override if strong disagreement
        if hmm_conf >= 0.5:
            # Trust HMM as primary
            regime = hmm_regime
            confidence = hmm_conf

            # Per-stock override: if HMM says trending but stock ADX < 20, downgrade
            if regime == 'trending' and stock_range >= 2:
                regime = 'ranging'
                confidence *= 0.7
                result['details'].append(
                    f"Stock ADX={adx:.0f} disagrees with market trend -- downgraded to ranging")
            # If HMM says ranging but stock has strong trend, upgrade
            elif regime == 'ranging' and stock_trend >= 2:
                regime = 'trending'
                confidence *= 0.7
                result['details'].append(
                    f"Stock ADX={adx:.0f} strong trend despite ranging market -- upgraded to trending")
        else:
            # Low HMM confidence -- fall back to per-stock signals
            if stock_trend >= 2:
                regime = 'trending'
                confidence = min(1.0, stock_trend / 3)
            elif stock_range >= 2:
                regime = 'ranging'
                confidence = min(1.0, stock_range / 2)
            elif squeeze_on:
                regime = 'volatile'
                confidence = 0.6
            else:
                regime = 'neutral'
                confidence = 0.5

        # Position adjustment
        pos_map = {'trending': 1.0, 'ranging': 0.5, 'volatile': 0.7, 'neutral': 1.0}
        result['regime'] = regime
        result['confidence'] = confidence
        result['position_adj'] = pos_map.get(regime, 1.0)

        # Summary detail
        label_map = {
            'trending': f"Trending (ADX={adx:.0f}) -- trust breakout signals",
            'ranging':  f"Ranging (ADX={adx:.0f}) -- beware false breakouts, halve position",
            'volatile': f"Volatile (ADX={adx:.0f}) -- wait for direction, reduce position",
            'neutral':  f"Neutral (ADX={adx:.0f}) -- no clear regime signal",
        }
        result['details'].insert(0, label_map.get(regime, 'Unknown'))

        return result

    def _calculate_trigger_score(self, df, trend_score=0, regime=None):
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

        # T1. Mean Reversion Composite (replaces binary MA20 position)
        # 5 correlated signals averaged via z-score: BIAS/VWAP_dev/BB_pct/RSI_dev/EFI
        # tanh maps to [-1, +1] continuously (smoother than binary)
        mr = self._safe_get(current, 'MeanRev_Composite', None)
        if mr is not None and not pd.isna(mr):
            import math
            t1 = math.tanh(mr)  # z-score avg ~[-3,+3] -> tanh -> [-1,+1]
            t1_label = f"{'📈' if t1 > 0 else '📉'} MeanRev={mr:+.2f} (tanh={t1:+.2f})"
            details.append(t1_label)
            trend_signals.append(t1)
        else:
            # Fallback: binary MA20 position (for stocks with <60 days data)
            ma20 = self._safe_get(current, 'MA20', 0)
            if close > ma20:
                t1 = 1.0
                details.append("✅ 站上日線 20MA (+1)")
            else:
                t1 = -1.0
                details.append("🔻 跌破日線 20MA (-1)")
            trend_signals.append(t1)

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
        div_macd = detect_divergence(df, 'MACD')
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
        div_rsi = detect_divergence(df, 'RSI')
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

        # ============================================================
        # VOLUME ANOMALY DETECTION (不計分，僅資訊提示)
        # OBV/EFI 的 IC≈0 不適合計分，但極端值可偵測異常事件
        # ============================================================

        # VA1. 量價背離 — 價格創新高/低但 OBV 未跟隨（使用成交值加權版本）
        try:
            if len(df) >= 20:
                obv_col = 'OBV_Value' if 'OBV_Value' in df.columns else 'OBV'
                price_5d = df['Close'].iloc[-5:]
                obv_5d = df[obv_col].iloc[-5:]
                price_20d = df['Close'].iloc[-20:]

                price_near_high = close > price_20d.quantile(0.9)
                obv_declining = obv_5d.iloc[-1] < obv_5d.iloc[0]
                price_near_low = close < price_20d.quantile(0.1)
                obv_rising = obv_5d.iloc[-1] > obv_5d.iloc[0]

                if price_near_high and obv_declining:
                    details.append("⚠️ 量價背離：股價近高但 OBV 下降 (假突破風險) [異常]")
                elif price_near_low and obv_rising:
                    details.append("💡 量價背離：股價近低但 OBV 上升 (底部吸籌跡象) [異常]")
        except (KeyError, IndexError):
            pass

        # VA2. EFI 極端資金流 — z-score 超過 ±2.0
        efi_z = self._safe_get(current, 'EFI_z', None)
        if efi_z is not None and not pd.isna(efi_z):
            if efi_z > 2.0:
                details.append(f"🔥 EFI 資金異常流入 (z={efi_z:.1f}) [異常]")
            elif efi_z < -2.0:
                details.append(f"💀 EFI 資金異常流出 (z={efi_z:.1f}) [異常]")

        # VA3. 冷門股突爆量 — 平常量能低迷但突然 RVOL > 3.0
        if rvol > 3.0 and rvol_z is not None and not pd.isna(rvol_z) and rvol_z > 2.5:
            details.append(f"🚨 異常爆量 RVOL={rvol:.1f}x (z={rvol_z:.1f})，留意消息面 [異常]")

        # ============================================================
        # PATTERN GROUP — 已移至進場過濾器 (_generate_action_plan)
        # 型態不預測漲跌 (IC=-0.004)，但能定義風險（停損位、進場點）
        # ============================================================
        pattern_signals = []  # 空組，不參與評分

        # (BIAS removed — 橫截面 IC 為負，已從 Trend 組移除)

        # ============================================================
        # GROUP MEDIANS → FINAL SCORE (with Regime HMM dynamic weights)
        # ============================================================
        trend_median = _median_of_signals(trend_signals)
        momentum_median = _median_of_signals(momentum_signals)
        volume_median = _median_of_signals(volume_signals)
        pattern_median = _median_of_signals(pattern_signals)  # empty = 0

        # Regime-dependent group weights
        regime_name = regime.get('regime', 'neutral') if regime else 'neutral'
        gw = REGIME_GROUP_WEIGHTS.get(regime_name, REGIME_GROUP_WEIGHTS['neutral'])
        am = REGIME_ADDON_MULT.get(regime_name, REGIME_ADDON_MULT['neutral'])

        # Weighted sum, normalized to preserve [-3,+3] range before scaling
        w_sum = gw['trend'] + gw['momentum'] + gw['volume']
        weighted_raw = (trend_median * gw['trend']
                        + momentum_median * gw['momentum']
                        + volume_median * gw['volume'])
        score = weighted_raw / w_sum * 3 * GROUP_SCALE_FACTOR

        if regime_name != 'neutral':
            details.append(
                f"[Regime] {regime_name} -- weights: "
                f"T={gw['trend']:.1f} M={gw['momentum']:.1f} V={gw['volume']:.1f}")

        # ============================================================
        # CHIP FACTORS (additive, cap adjusted by regime)
        # ============================================================
        if self._is_us_stock:
            chip_score, chip_details = analyze_us_chip_factors(
                df, self.ticker, self.us_chip_data, trend_score=trend_score)
        else:
            chip_score, chip_details = analyze_tw_chip_factors(
                df, self.chip_data, trend_score=trend_score)
        chip_cap = CHIP_SCORE_CAP * am['chip']
        chip_score = max(-chip_cap, min(chip_cap, chip_score))
        score += chip_score
        details.extend(chip_details)

        # ============================================================
        # MARKET SENTIMENT / REVENUE CATALYST — 2026-04-22 從 trigger_score 移除
        # 原因:
        #   PCR + 期貨基差屬「大盤層級」訊號，對每檔個股加同樣分數語意錯誤
        #     (已在 market_banner.py 顯示，不需重複塞進個股 trigger)
        #   營收驚喜屬「月頻基本面」，塞進日線 trigger 會連續整月推高/壓低分數
        #     (已在 _fetch_fundamental_snapshot 顯示於 📋 基本面快照)
        # ============================================================
        sentiment_score = 0.0
        revenue_score = 0.0

        # ============================================================
        # ETF SIGNAL (Active ETF Sync Buy/Sell, Taiwan only)
        # ============================================================
        etf_score, etf_details = analyze_etf_signal(self.ticker, self._is_us_stock)
        etf_cap = ETF_SIGNAL_CAP * am['etf']
        etf_score = max(-etf_cap, min(etf_cap, etf_score))
        score += etf_score
        details.extend(etf_details)

        # Clamp score to valid range
        score = max(TRIGGER_SCORE_RANGE[0], min(TRIGGER_SCORE_RANGE[1], score))

        breakdown = {
            'trend_group': trend_median,
            'momentum_group': momentum_median,
            'volume_group': volume_median,
            'pattern_group': pattern_median,
            'chip_score': chip_score,
            'sentiment_score': sentiment_score,  # 保留 key 避免下游 breakdown 讀取噴錯
            'revenue_score': revenue_score,      # 同上
            'etf_score': etf_score,
            'regime': regime_name,
            'regime_weights': gw,
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
