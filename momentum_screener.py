"""
右側動能選股引擎 — 全市場掃描找出動能最強的股票

Stage 1: 快速初篩（TWSE/TPEX 全市場日行情）
  - 成交值佔比門檻過濾低流動性股票
  - 動能條件（漲幅 > 0 或近期有爆發）

Stage 2: 完整觸發分數（復用 analysis_engine.py）
  - 批量跑 TechnicalAnalyzer.run_analysis()
  - 排序輸出 Top N
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ================================================================
# Default Configuration
# ================================================================
DEFAULT_CONFIG = {
    # Stage 1: 初篩門檻
    'twse_value_pct': 0.0002,   # (legacy fallback) 上市成交值佔比
    'tpex_value_pct': 0.0005,   # (legacy fallback) 上櫃成交值佔比
    'market_cap_top_n': 300,     # 市值前 N 大
    'min_avg_tv_20d': 5e8,       # 20 日均成交值門檻（5 億）
    'min_price': 0,              # 最低股價門檻（預設關閉）
    'momentum_change_min': -1.0, # 當日漲跌幅下限 %（允許微跌）

    # Stage 2: 精篩設定
    'top_n': 20,                 # 輸出前 N 名
    'history_days': 365,         # 抓取歷史天數
    'include_chip': True,        # 是否抓籌碼資料（慢但更準）
    'batch_delay': 0.3,          # 每檔間隔秒數（控速）
    'max_failures': 10,          # 連續失敗上限，超過就停止

    # 排除清單
    'exclude_ids': set(),        # 手動排除的股票代號

    # US market settings
    'us_universe': 'sp500',      # 'sp500', 'nasdaq100', or list of tickers
    'us_min_volume': 500_000,    # Minimum daily volume
    'us_min_price': 5.0,         # Minimum price (skip penny stocks)
    'us_include_chip': False,    # US chip data is slow, default off
}


_CHECKPOINT_DIR = Path('data/.checkpoints')


def _percentile_rank(values):
    """回傳每個元素在 list 中的百分位排名 (0~100)，None 值給 50。"""
    valid = [(i, v) for i, v in enumerate(values) if v is not None]
    result = [50.0] * len(values)
    if len(valid) <= 1:
        return result
    sorted_vals = sorted(v for _, v in valid)
    n = len(sorted_vals)
    for i, v in valid:
        # 排名百分位: 有多少比自己小 / (n-1) * 100
        rank = sum(1 for sv in sorted_vals if sv < v)
        result[i] = rank / (n - 1) * 100 if n > 1 else 50
    return result


def _compute_composite_score(top_n):
    """
    綜合評分 0-100: F-Score 50% + 體質分 30% + 趨勢分數 20%

    權重來自 2026-04-15 驗證 (tools/qm_validation.py):
    - F-Score 60d IC=+0.113 IR=0.903 勝率 81%（最強單因子）
    - 體質分 60d IC=+0.073 IR=0.627 勝率 76%
    - 趨勢分數 60d IC=+0.043 IR=0.277
    - 低波放量 60d IC=-0.037 IR=-0.250（顯著為負，已移除）
    - 觸發分數 60d IC=+0.010 IR=0.073（幾乎無效，已移除）

    F50/Body30/Trend20 組合 60d Sharpe 1.67, 勝率 76%, 報酬 +14%
    """
    if not top_n:
        return

    fscore_vals = [s.get('qm_f_score') for s in top_n]
    body_vals = [s.get('qm_body_score') for s in top_n]
    trend_vals = [s.get('trend_score') for s in top_n]

    fscore_pct = _percentile_rank(fscore_vals)
    body_pct = _percentile_rank(body_vals)
    trend_pct = _percentile_rank(trend_vals)

    for i, s in enumerate(top_n):
        composite = (fscore_pct[i] * 0.50
                     + body_pct[i] * 0.30
                     + trend_pct[i] * 0.20)
        s['composite_score'] = round(composite, 1)

    # 部位調整器 (A#3)：composite × trigger 動態倉位
    for s in top_n:
        sz = _qm_position_size(s.get('composite_score'), s.get('trigger_score'))
        if sz is not None:
            s['qm_position_size'] = sz
            ap = s.get('action_plan') or {}
            ap['qm_position_size'] = sz
            s['action_plan'] = ap

    # 依綜合評分重新排序
    top_n.sort(key=lambda x: x.get('composite_score', 0), reverse=True)


def _qm_position_size(composite_score, trigger_score, base_pct=8.0):
    """動態倉位計算（擇時工具 #3）。

    公式（project_trigger_score_usage.md）：
      base = base_pct × (composite_score / 80)     # 綜合評分決定基礎倉位
      multiplier = clip(trigger / 5, 0.5, 1.5)     # 擇時微調 ±50%
      actual = base × multiplier

    範例：
      composite=80, trigger=5  → 8.0% × 1.00 = 8.0%    (標準)
      composite=100, trigger=10 → 10.0% × 1.50 = 15.0% (QM 強 + 擇時好 → 加倉)
      composite=60, trigger=-3  → 6.0% × 0.50 = 3.0%   (QM 弱 + 擇時差 → 半倉)

    Returns:
        dict {recommended_pct, base_pct, multiplier, rationale} 或 None
    """
    if composite_score is None:
        return None
    try:
        cs = float(composite_score)
    except Exception:
        return None
    base = base_pct * (cs / 80.0)
    if trigger_score is None:
        multiplier = 1.0
        rationale = f'base={base:.1f}% (composite {cs:.0f}/80，擇時中性)'
    else:
        ts = float(trigger_score)
        multiplier = max(0.5, min(1.5, ts / 5.0))
        rationale = (
            f'base={base:.1f}% (composite {cs:.0f}/80) × '
            f'multiplier={multiplier:.2f} (trigger {ts:+.1f}/5)'
        )
    actual = base * multiplier
    return {
        'recommended_pct': round(actual, 1),
        'base_pct': round(base, 1),
        'multiplier': round(multiplier, 2),
        'rationale': rationale,
    }


def _qm_entry_gate(trigger_score):
    """依 trigger_score 判定 QM 第 1 批進場閘門（擇時工具 #2）。

    閾值（project_trigger_score_usage.md）：
      trigger >= 3: green，當日 50% 可進場
      0 <= trigger < 3: yellow，等訊號轉強
      trigger < 0: red，QM 與擇時矛盾，觀望
    """
    if trigger_score is None:
        return {
            'level': 'unknown',
            'ready': False,
            'text': '第 1 批 50%（QM 預設：當日進場）',
        }
    ts = float(trigger_score)
    if ts >= 3:
        return {
            'level': 'green',
            'ready': True,
            'text': f'🟢 第 1 批 50% 可當日進場（擇時 trigger={ts:+.1f} ≥ 3）',
        }
    if ts >= 0:
        return {
            'level': 'yellow',
            'ready': False,
            'text': f'🟡 第 1 批觀望 — 等 trigger 轉強至 ≥ 3（目前 {ts:+.1f}）',
        }
    return {
        'level': 'red',
        'ready': False,
        'text': f'🔴 第 1 批暫緩 — QM 選上但擇時 trigger={ts:+.1f} < 0，訊號矛盾',
    }


def _apply_qm_action_plan(action_plan, df_week, trigger_score=None,
                          atr_pct=None, regime=None):
    """
    QM 專屬 action_plan：覆蓋通用版的短線 SL/TP，改為 40-60d horizon 的波段操作參數。

    驗證錨點 (2026-04-15, tools/qm_validation.py Round 4)：
      60d Sharpe 1.67, 勝率 76%, 平均報酬 +14%
      40d Sharpe 1.81, 20d Sharpe 1.99

    Phase 2: atr_pct 傳入時 exit_manager 動態調整 SL/TP。
    Phase 4: regime 傳入時 exit_manager 依市場狀態調整 SL/TP 乘數。

    覆蓋邏輯：
      - 停損: exit_manager.compute_exit_plan（依 ATR% + regime 動態）
      - 停利: exit_manager 三段（依 ATR% + regime 縮放）
      - R:R 以 TP1 對停損價計算
      - 進場閘門 (trigger_score): 依 _qm_entry_gate() 分 green/yellow/red
      - strategy 加上 QM 操作要點 + 出場訊號 + 分批進場

    Returns:
        覆蓋後的 action_plan dict (不修改原物件)
    """
    if not action_plan or not action_plan.get('is_actionable'):
        return action_plan

    entry_basis = action_plan.get('rec_entry_high') or action_plan.get('current_price', 0)
    if entry_basis is None or entry_basis <= 0:
        return action_plan

    # --- 週 MA20 ---
    week_ma20 = None
    if df_week is not None and not df_week.empty and 'MA20' in df_week.columns:
        try:
            w = df_week.iloc[-1].get('MA20', 0)
            if pd.notna(w) and w > 0:
                week_ma20 = float(w)
        except Exception:
            pass

    # --- exit_manager 統一計算 SL/TP ---
    from exit_manager import compute_exit_plan
    plan = compute_exit_plan(entry_basis, weekly_ma20=week_ma20,
                             atr_pct=atr_pct, regime=regime)
    rec_sl_price = plan['stop_loss']
    rec_sl_method = f"QM. {plan['stop_method']}"
    hard_sl = plan['hard_stop']

    # --- 停利三段（exit_manager 依 ATR% 縮放） ---
    tp_lvls = plan['tp_levels']
    tp1 = tp_lvls[0]['price'] if len(tp_lvls) > 0 else entry_basis * 1.15
    tp2 = tp_lvls[1]['price'] if len(tp_lvls) > 1 else entry_basis * 1.25
    tp3 = tp_lvls[2]['price'] if len(tp_lvls) > 2 else entry_basis * 1.40
    tp1_pct = tp_lvls[0]['pct'] if len(tp_lvls) > 0 else 15.0
    tp2_pct = tp_lvls[1]['pct'] if len(tp_lvls) > 1 else 25.0
    tp3_pct = tp_lvls[2]['pct'] if len(tp_lvls) > 2 else 40.0

    qm_tp_list = [
        {"method": f"QM1. +{tp1_pct:.0f}% 減碼 1/3", "price": tp1, "desc": "落袋第一段", "is_rec": True},
        {"method": f"QM2. +{tp2_pct:.0f}% 移動停損至 MA10W", "price": tp2, "desc": "鎖住超額", "is_rec": False},
        {"method": f"QM3. +{tp3_pct:.0f}% 清倉 (或 60 日到期)", "price": tp3, "desc": "換股輪動", "is_rec": False},
    ]
    existing_tp = [dict(t) for t in action_plan.get('tp_list', [])]
    for t in existing_tp:
        t['is_rec'] = False
    full_tp_list = qm_tp_list + existing_tp

    # --- 停損列表 ---
    hard_stop_label = f"QM-A. 硬停損 {plan['hard_stop_pct']*100:+.1f}%"
    qm_sl_list = [
        {"method": hard_stop_label, "price": hard_sl,
         "desc": f"期望值保護 ({plan['method']})", "loss": round(plan['hard_stop_pct'] * 100, 1)},
    ]
    if week_ma20 is not None and week_ma20 > 0:
        loss_pct = ((week_ma20 - entry_basis) / entry_basis) * 100
        qm_sl_list.append({
            "method": "QM-B. 週 MA20 趨勢停損",
            "price": week_ma20,
            "desc": "趨勢結構破壞",
            "loss": round(loss_pct, 2),
        })
    existing_sl = [dict(s) for s in action_plan.get('sl_list', [])]
    full_sl_list = qm_sl_list + existing_sl

    # --- R:R (以 TP1 vs 停損) ---
    potential_reward = tp1 - entry_basis
    potential_risk = entry_basis - rec_sl_price
    rr_ratio = round(potential_reward / potential_risk, 2) if potential_risk > 0 else 0.0
    rr_ratio = min(rr_ratio, 10.0)  # sanity cap: R:R > 10 通常代表 SL 算法異常

    # --- 進場閘門 (A#2) ---
    gate = _qm_entry_gate(trigger_score)
    batch2_text = (
        "第 2 批 50% — 回補條件：trigger 從低點回升至 ≥ +2 且 RVOL > 1.2 "
        "(或回調至日 MA10 / RSI 45-55)"
    )
    qm_entry_batches_text = f"{gate['text']}；{batch2_text}"

    # --- 覆蓋 ---
    qm_plan = dict(action_plan)
    qm_plan.update({
        'rec_sl_price': rec_sl_price,
        'rec_sl_method': rec_sl_method,
        'rec_tp_price': tp1,
        'rr_ratio': rr_ratio,
        'tp_list': full_tp_list,
        'sl_list': full_sl_list,
        'qm_horizon': '40-60d',
        'qm_hold_days_target': 60,
        'qm_entry_batches': qm_entry_batches_text,
        'qm_entry_gate': gate,  # UI 可用 level 欄位決定顏色
        'qm_exit_signals': [
            '週線 Supertrend 翻空',
            '週 MA20 跌破 3% 以上 (非插針)',
            '月營收 YoY 連續 2 個月轉負',
            'F-Score 季更新後下降 2 分以上',
        ],
    })

    # Regime tag for display
    regime_tag = ''
    if regime and regime != 'neutral':
        sl_m, tp_m = plan.get('regime_sl_mult', 1.0), plan.get('regime_tp_mult', 1.0)
        regime_tag = f" [regime={regime}: SL×{sl_m}, TP×{tp_m}]"

    base_strategy = qm_plan.get('strategy', '')
    qm_note = (
        "\n\n**QM 波段操作要點** (驗證: 60d Sharpe 1.67 / 勝率 76% / 平均 +14%)\n"
        f"- 第 1 批閘門: {gate['text']}\n"
        f"- 第 2 批補倉: {batch2_text}\n"
        "- 持倉目標 **40-60 日**，不要短抱 (Sharpe 高點在 20d，報酬高點在 60d)\n"
        f"- 停損: **{rec_sl_method}** → {rec_sl_price:.2f}{regime_tag}\n"
        f"- 停利: +{tp1_pct:.0f}% 減 1/3 → +{tp2_pct:.0f}% 改用週 MA10 移動停損 → +{tp3_pct:.0f}% 清倉\n"
        "- 出場訊號: 週 Supertrend 翻空 / 週 MA20 跌破 / 月營收連 2 月 YoY 負 / F-Score 掉 2 分"
    )
    qm_plan['strategy'] = base_strategy + qm_note

    return qm_plan


class MomentumScreener:
    """右側動能選股引擎"""

    def __init__(self, config=None, progress_callback=None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.progress = progress_callback or (lambda msg: print(msg))
        self._failures = []

    # ================================================================
    # Public API
    # ================================================================

    def run(self, market='tw', mode='momentum'):
        """
        Execute full screening pipeline.

        Args:
            market: 'tw' for Taiwan, 'us' for US stocks
            mode: 'momentum' (5-20d), 'swing' (2w-3m), or 'qm' (quality momentum)

        Returns:
            dict with scan_date, total_scanned, passed_initial, results
        """
        start_time = time.time()
        self._market = market
        self._mode = mode

        # --- Stage 1 ---
        self.progress(f"Stage 1: Fetching {'US' if market == 'us' else 'TW'} market data...")
        if market == 'us':
            market_df = self._fetch_us_market_data()
            candidates = self._stage1_filter_us(market_df) if not market_df.empty else pd.DataFrame()
        else:
            market_df = self._fetch_market_data()
            candidates = self._stage1_filter(market_df) if not market_df.empty else pd.DataFrame()

        if market_df.empty:
            return self._make_result([], 0, 0, time.time() - start_time)

        self.progress(f"Stage 1 done: {len(candidates)}/{len(market_df)} passed")

        if candidates.empty:
            return self._make_result([], len(market_df), 0, time.time() - start_time)

        # --- QM Quality Gate (between Stage 1 and Stage 2) ---
        if mode == 'qm':
            before = len(candidates)
            candidates = self._quality_gate(candidates, market)
            self.progress(f"Quality gate: {before} -> {len(candidates)} passed")
            if candidates.empty:
                return self._make_result([], len(market_df), before, time.time() - start_time)

        # --- Stage 2 ---
        self.progress(f"Stage 2: Analyzing {len(candidates)} candidates...")
        scored = self._stage2_analyze(candidates)
        self.progress(f"Stage 2 done: {len(scored)} scored, {len(self._failures)} failed")

        elapsed = time.time() - start_time
        self.progress(f"Scan complete in {elapsed:.0f}s")
        return self._make_result(scored, len(market_df), len(candidates), elapsed)

    def run_stage1_only(self, market='tw'):
        """Only run Stage 1 for quick preview (no trigger scores)."""
        if market == 'us':
            market_df = self._fetch_us_market_data()
            return self._stage1_filter_us(market_df) if not market_df.empty else pd.DataFrame()
        market_df = self._fetch_market_data()
        if market_df.empty:
            return pd.DataFrame()
        return self._stage1_filter(market_df)

    # ================================================================
    # Stage 1: Quick Initial Filter
    # ================================================================

    def _fetch_market_data(self):
        """Fetch full market daily data from TWSE + TPEX."""
        from twse_api import TWSEOpenData
        api = TWSEOpenData()
        return api.get_market_daily_all()

    def _stage1_filter(self, df):
        """
        Filter stocks by market cap / avg trading value union + momentum.

        Criteria:
        1. 市值前 N 大 OR 20 日均成交值 > 門檻（聯集）
        2. Change % > minimum (allow small dips)
        3. Not in exclude list / ETF
        """
        cfg = self.config

        # 排除 ETF
        df = df[~df['stock_id'].str.startswith('00')].copy()

        # 取 TradingView 市值 + 均量（免費 batch，1hr cache）
        tv_data = self._fetch_tv_marketcap_volume()

        if tv_data:
            # 市值前 N 大
            mc_top_n = cfg.get('market_cap_top_n', 300)
            mc_sorted = sorted(tv_data.items(), key=lambda x: x[1].get('market_cap', 0), reverse=True)
            mc_top_ids = {sid for sid, _ in mc_sorted[:mc_top_n]}

            # 20 日均成交值 > 門檻
            min_avg_tv = cfg.get('min_avg_tv_20d', 5e8)
            tv_pass_ids = {sid for sid, d in tv_data.items()
                           if d.get('avg_tv_20d', 0) >= min_avg_tv}

            # 聯集
            eligible_ids = mc_top_ids | tv_pass_ids
            self.progress(f"  Stage 1: market_cap top {mc_top_n}={len(mc_top_ids)}, "
                          f"avg_tv>={min_avg_tv/1e8:.0f}億={len(tv_pass_ids)}, "
                          f"union={len(eligible_ids)}")

            passed = df[df['stock_id'].isin(eligible_ids)].copy()
        else:
            # TradingView 不可用時 fallback 到舊邏輯（成交值佔比）
            self.progress("  Stage 1: TradingView unavailable, using legacy pct filter")
            results = []
            for market, threshold_pct in [('twse', cfg['twse_value_pct']),
                                           ('tpex', cfg['tpex_value_pct'])]:
                mdf = df[df['market'] == market].copy()
                if mdf.empty:
                    continue
                total_tv = mdf['trading_value'].sum()
                if total_tv <= 0:
                    continue
                mdf['tv_pct'] = mdf['trading_value'] / total_tv
                results.append(mdf[mdf['tv_pct'] >= threshold_pct].copy())
            passed = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

        if passed.empty:
            return pd.DataFrame()

        # Momentum filter
        passed = passed[passed['change_pct'] >= cfg['momentum_change_min']]

        # Exclude list
        exclude = cfg['exclude_ids']
        if exclude:
            passed = passed[~passed['stock_id'].isin(exclude)]

        # Price filter
        if cfg['min_price'] > 0:
            passed = passed[passed['close'] >= cfg['min_price']]

        passed.sort_values('trading_value', ascending=False, inplace=True)
        return passed

    @staticmethod
    def _fetch_tv_marketcap_volume():
        """
        TradingView batch: 市值 + 均量（1hr cache）。
        Returns: {stock_id: {market_cap, avg_tv_20d}} or {}
        """
        import time as _time

        cache_attr = '_tv_mc_cache'
        cache_ts_attr = '_tv_mc_ts'
        cached = getattr(MomentumScreener, cache_attr, None)
        ts = getattr(MomentumScreener, cache_ts_attr, 0)
        if cached and _time.time() - ts < 3600:
            return cached

        try:
            from tradingview_screener import Query
            result = (Query()
                .select('name', 'market_cap_basic', 'close',
                        'average_volume_10d_calc', 'average_volume_30d_calc')
                .set_markets('taiwan')
                .limit(5000)
                .get_scanner_data()
            )
            df = result[1]
            data = {}
            for _, row in df.iterrows():
                sid = str(row.get('name', '')).strip()
                if not sid:
                    continue
                mc = row.get('market_cap_basic')
                close = row.get('close')
                v10 = row.get('average_volume_10d_calc')
                v30 = row.get('average_volume_30d_calc')
                if mc is None or (isinstance(mc, float) and np.isnan(mc)):
                    mc = 0
                # 20d avg TV ~= close * avg(10d_vol, 30d_vol)
                avg_tv = 0
                if close and v10 and v30:
                    avg_vol = (v10 + v30) / 2
                    avg_tv = close * avg_vol
                elif close and v10:
                    avg_tv = close * v10
                data[sid] = {'market_cap': mc, 'avg_tv_20d': avg_tv}

            logger.info("TV market cap batch: %d stocks", len(data))
            setattr(MomentumScreener, cache_attr, data)
            setattr(MomentumScreener, cache_ts_attr, _time.time())
            return data
        except Exception as e:
            logger.warning("TV market cap fetch failed: %s", e)
            return {}

    # ================================================================
    # US Market: Fetch + Filter
    # ================================================================

    # 防禦性排除常見美股 ETF（S&P 500 / Nasdaq 100 清單本身是成分公司，
    # 不含 ETF，但若未來 us_universe 改用其他清單，這層過濾可避免 ETF 混入）
    _US_ETF_EXCLUDE = frozenset({
        'SPY', 'IVV', 'VOO', 'QQQ', 'DIA', 'IWM', 'VTI', 'VEA', 'VWO',
        'ARKK', 'ARKG', 'ARKW', 'ARKF', 'ARKQ', 'RSP', 'XLK', 'XLF',
        'XLE', 'XLV', 'XLY', 'XLP', 'XLI', 'XLU', 'XLB', 'XLRE', 'XLC',
        'SMH', 'SOXX', 'TQQQ', 'SQQQ', 'UPRO', 'SPXL', 'GLD', 'SLV',
        'TLT', 'HYG', 'LQD', 'EEM', 'EFA', 'FXI', 'GDX', 'USO', 'UNG',
    })

    def _get_us_universe(self):
        """Get list of US stock tickers based on config (ETF excluded)."""
        universe = self.config.get('us_universe', 'sp500')

        if isinstance(universe, list):
            tickers = universe
        elif universe == 'nasdaq100':
            tickers = self._fetch_nasdaq100()
        else:
            # Default: S&P 500
            tickers = self._fetch_sp500()

        return [t for t in tickers if t not in self._US_ETF_EXCLUDE]

    @staticmethod
    def _fetch_sp500():
        """Fetch S&P 500 ticker list from Wikipedia."""
        try:
            import requests as _req
            from io import StringIO
            headers = {'User-Agent': 'StockAnalyzer/1.0'}
            resp = _req.get(
                'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
                headers=headers, timeout=15,
            )
            tables = pd.read_html(StringIO(resp.text))
            tickers = tables[0]['Symbol'].tolist()
            # Fix BRK.B → BRK-B for yfinance
            return [t.replace('.', '-') for t in tickers]
        except Exception as e:
            logger.error("Failed to fetch S&P 500 list: %s", e)
            return []

    @staticmethod
    def _fetch_nasdaq100():
        """Fetch Nasdaq 100 ticker list from Wikipedia."""
        try:
            import requests as _req
            from io import StringIO
            headers = {'User-Agent': 'StockAnalyzer/1.0'}
            resp = _req.get(
                'https://en.wikipedia.org/wiki/Nasdaq-100',
                headers=headers, timeout=15,
            )
            tables = pd.read_html(StringIO(resp.text))
            # Nasdaq-100 table usually has 'Ticker' or 'Symbol' column
            for t in tables:
                for col in ['Ticker', 'Symbol']:
                    if col in t.columns:
                        tickers = t[col].tolist()
                        return [str(tk).replace('.', '-') for tk in tickers]
            return []
        except Exception as e:
            logger.error("Failed to fetch Nasdaq 100 list: %s", e)
            return []

    def _fetch_us_market_data(self):
        """
        Fetch daily data for US stock universe via yfinance batch download.

        Returns:
            DataFrame with columns matching TW format:
            stock_id, stock_name, market, close, change, change_pct,
            open, high, low, volume, trading_value
        """
        import yfinance as yf

        tickers = self._get_us_universe()
        if not tickers:
            logger.error("Empty US stock universe")
            return pd.DataFrame()

        self.progress(f"  Downloading {len(tickers)} US tickers...")

        try:
            data = yf.download(
                tickers, period='2d', interval='1d',
                progress=False, auto_adjust=False, timeout=30,
            )
        except Exception as e:
            logger.error("yfinance batch download failed: %s", e)
            return pd.DataFrame()

        if data.empty:
            return pd.DataFrame()

        results = []
        for ticker in tickers:
            try:
                if data.columns.nlevels == 2:
                    close = data[('Close', ticker)].dropna()
                    volume = data[('Volume', ticker)].dropna()
                    _open = data[('Open', ticker)].dropna()
                    high = data[('High', ticker)].dropna()
                    low = data[('Low', ticker)].dropna()
                else:
                    # Single ticker
                    close = data['Close'].dropna()
                    volume = data['Volume'].dropna()
                    _open = data['Open'].dropna()
                    high = data['High'].dropna()
                    low = data['Low'].dropna()

                if len(close) < 1:
                    continue

                latest_close = float(close.iloc[-1])
                latest_vol = float(volume.iloc[-1])

                if len(close) >= 2:
                    prev_close = float(close.iloc[-2])
                    change = latest_close - prev_close
                    change_pct = (change / prev_close * 100) if prev_close != 0 else 0
                else:
                    change = 0
                    change_pct = 0

                results.append({
                    'stock_id': ticker,
                    'stock_name': ticker,  # yfinance doesn't give names in batch
                    'market': 'us',
                    'close': latest_close,
                    'change': round(change, 2),
                    'change_pct': round(change_pct, 2),
                    'open': float(_open.iloc[-1]) if len(_open) > 0 else 0,
                    'high': float(high.iloc[-1]) if len(high) > 0 else 0,
                    'low': float(low.iloc[-1]) if len(low) > 0 else 0,
                    'volume': int(latest_vol),
                    'trading_value': int(latest_close * latest_vol),
                })
            except Exception:
                continue

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        logger.info("US market data: %d tickers fetched", len(df))
        return df

    def _stage1_filter_us(self, df):
        """
        Filter US stocks by volume and momentum.

        Criteria:
        1. Volume > minimum
        2. Price > minimum (skip penny stocks)
        3. Change % > momentum minimum
        """
        cfg = self.config

        mask = pd.Series(True, index=df.index)
        mask &= df['volume'] >= cfg.get('us_min_volume', 500_000)
        mask &= df['close'] >= cfg.get('us_min_price', 5.0)
        mask &= df['change_pct'] >= cfg['momentum_change_min']

        result = df[mask].copy()
        result.sort_values('trading_value', ascending=False, inplace=True)
        return result

    # ================================================================
    # Stage 2: Full Trigger Score Analysis
    # ================================================================

    def _stage2_analyze(self, candidates):
        """
        Batch-run TechnicalAnalyzer on each candidate.
        Supports checkpoint/resume: saves progress after each stock.
        """
        from technical_analysis import (
            calculate_all_indicators,
            load_and_resample,
        )
        from analysis_engine import TechnicalAnalyzer

        cfg = self.config
        market = getattr(self, '_market', 'tw')
        cp_file = _CHECKPOINT_DIR / f'momentum_{market}.json'

        # Pre-fetch batch institutional data (TWSE/TPEX) as FinMind fallback
        self._inst_batch = {}
        if market == 'tw' and cfg.get('include_chip', True):
            try:
                from twse_api import TWSEOpenData
                twse = TWSEOpenData()
                self._inst_batch = twse.get_institutional_batch(days=5)
                self.progress(f"  Pre-fetched institutional data: {len(self._inst_batch)} stocks")
            except Exception as e:
                logger.warning("Batch institutional fetch failed: %s", e)

        # Load checkpoint if exists
        scored, done_ids = self._load_checkpoint(cp_file)
        if scored:
            self.progress(f"  Resuming: {len(scored)} stocks already scored, {len(done_ids)} processed")

        total = len(candidates)
        consecutive_fails = 0

        for idx, row in candidates.iterrows():
            sid = row['stock_id']
            if sid in done_ids:
                continue

            sname = row.get('stock_name', '')
            pos = len(scored) + len(self._failures) + 1

            if pos % 10 == 0 or pos <= 3:
                self.progress(f"  [{pos}/{total}] {sid} {sname}")

            try:
                result = self._analyze_single(sid, row)
                if result:
                    scored.append(result)
                    consecutive_fails = 0
                else:
                    self._failures.append(sid)
                    consecutive_fails += 1
            except Exception as e:
                err_str = str(type(e).__name__)
                if 'RateLimit' in err_str or '429' in str(e):
                    self.progress(f"  [Rate Limit] Pausing 60s then retrying {sid}...")
                    time.sleep(60)
                    try:
                        result = self._analyze_single(sid, row)
                        if result:
                            scored.append(result)
                            consecutive_fails = 0
                        else:
                            self._failures.append(sid)
                    except Exception:
                        self._failures.append(sid)
                    continue
                logger.warning("Failed to analyze %s: %s", sid, e)
                self._failures.append(sid)
                consecutive_fails += 1

            # Save checkpoint after each stock
            done_ids.add(sid)
            self._save_checkpoint(cp_file, scored, done_ids)

            if consecutive_fails >= cfg['max_failures']:
                self.progress(f"  Stopping: {consecutive_fails} consecutive failures")
                break

            if cfg['batch_delay'] > 0:
                time.sleep(cfg['batch_delay'])

        # Clean up checkpoint on completion
        self._clear_checkpoint(cp_file)

        mode = getattr(self, '_mode', 'momentum')

        if mode == 'qm':
            # QM mode: trend>=1 → 全部算品質分 → 四維綜合評分 → Top N
            scored = [s for s in scored if s.get('trend_score', 0) >= 1]
            self.progress(f"  trend_score >= 1: {len(scored)} stocks")

            # 品質分（FinMind F-Score + 營收）— 對所有 trend>=1 的股票
            self.progress(f"  QM quality scoring: {len(scored)} stocks (F-Score + revenue)...")
            from value_screener import ValueScreener
            _vs = ValueScreener(progress_callback=lambda m: None)
            _vs._tv_batch = getattr(self, '_tv_quality', {})
            for i, s in enumerate(scored):
                sid = s['stock_id']
                try:
                    q_details = []
                    q_score = _vs._score_quality(sid, q_details, s.get('price', 0))
                    r_score = _vs._score_revenue(sid, q_details)
                    s['qm_quality_score'] = round(q_score * 0.6 + r_score * 0.4)  # combined 顯示用
                    s['qm_body_score'] = q_score  # 体质分 (排序用)
                    s['qm_revenue_score'] = r_score
                    s['qm_quality_details'] = q_details

                    # 抽出 F-Score (綜合評分 50% 權重)
                    s['qm_f_score'] = None
                    for d in q_details:
                        if d.startswith('F-Score='):
                            try:
                                s['qm_f_score'] = int(d.split('=')[1].split('/')[0])
                                break
                            except Exception:
                                pass
                except Exception:
                    s['qm_quality_score'] = None
                    s['qm_body_score'] = None
                    s['qm_revenue_score'] = None
                    s['qm_quality_details'] = []
                    s['qm_f_score'] = None
                if (i + 1) % 10 == 0:
                    self.progress(f"    [{i+1}/{len(scored)}] quality scored")
            self.progress(f"  QM quality scoring done")

            # 四維綜合評分 → 排序 → 取 top_n
            _compute_composite_score(scored)
            top_n = scored[:cfg['top_n']]

        elif mode == 'swing':
            # Swing mode: trend>=1 + rvol_lowatr 排序
            scored = [s for s in scored if s.get('trend_score', 0) >= 1]
            has_rvol = [s for s in scored if s.get('rvol_lowatr') is not None]
            no_rvol = [s for s in scored if s.get('rvol_lowatr') is None]
            has_rvol.sort(key=lambda x: x['rvol_lowatr'], reverse=True)
            no_rvol.sort(key=lambda x: x['trigger_score'], reverse=True)
            scored = has_rvol + no_rvol
            top_n = scored[:cfg['top_n']]
            for s in top_n:
                s['rvol_lowatr_top20'] = s in has_rvol[:20] if has_rvol else None
        else:
            # Momentum mode: trigger_score 排序 + rvol_lowatr Top 20 標記
            scored.sort(key=lambda x: x['trigger_score'], reverse=True)
            top_n = scored[:cfg['top_n']]
            has_rvol = [s for s in top_n if s.get('rvol_lowatr') is not None]
            if has_rvol:
                has_rvol.sort(key=lambda x: x['rvol_lowatr'], reverse=True)
                top20_ids = {s['stock_id'] for s in has_rvol[:20]}
                for s in top_n:
                    s['rvol_lowatr_top20'] = s['stock_id'] in top20_ids
            else:
                for s in top_n:
                    s['rvol_lowatr_top20'] = None

        return top_n

    # ================================================================
    # Checkpoint helpers
    # ================================================================

    @staticmethod
    def _load_checkpoint(cp_file):
        """Load checkpoint: returns (scored_list, done_ids_set)."""
        if cp_file.exists():
            try:
                with open(cp_file, 'r', encoding='utf-8') as f:
                    cp = json.load(f)
                scored = cp.get('scored', [])
                done_ids = set(cp.get('done_ids', []))
                return scored, done_ids
            except Exception:
                pass
        return [], set()

    @staticmethod
    def _save_checkpoint(cp_file, scored, done_ids):
        """Save checkpoint (every N stocks to avoid I/O overhead)."""
        # Only save every 5 stocks
        if len(done_ids) % 5 != 0:
            return
        try:
            _CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
            with open(cp_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'scored': scored,
                    'done_ids': list(done_ids),
                    'timestamp': datetime.now().isoformat(),
                }, f, ensure_ascii=False)
        except Exception:
            pass

    @staticmethod
    def _clear_checkpoint(cp_file):
        """Remove checkpoint file after successful completion."""
        try:
            if cp_file.exists():
                cp_file.unlink()
        except Exception:
            pass

    def _analyze_single(self, stock_id, market_row):
        """
        Analyze a single stock and return result dict.

        Returns:
            dict or None (if analysis failed)
        """
        from technical_analysis import calculate_all_indicators, load_and_resample
        from analysis_engine import TechnicalAnalyzer

        # 1. Load price data (uses existing cache_manager)
        try:
            ticker, df_day, df_week, meta = load_and_resample(stock_id)
        except Exception as e:
            logger.debug("load_and_resample failed for %s: %s", stock_id, e)
            return None

        if df_day.empty or len(df_day) < 60:
            logger.debug("Insufficient data for %s (%d rows)", stock_id, len(df_day))
            return None

        # 2. Calculate indicators
        try:
            df_day = calculate_all_indicators(df_day)
            df_week = calculate_all_indicators(df_week)
        except Exception as e:
            logger.debug("Indicator calculation failed for %s: %s", stock_id, e)
            return None

        # 3. Optionally fetch chip data
        chip_data = None
        us_chip_data = None
        is_us = not stock_id.isdigit()

        if is_us:
            if self.config.get('us_include_chip', False):
                try:
                    from us_stock_chip import USStockChipAnalyzer
                    usc = USStockChipAnalyzer()
                    us_chip_data, _ = usc.get_chip_data(stock_id)
                except Exception:
                    pass
        else:
            if self.config['include_chip']:
                # 1st: use pre-fetched TWSE/TPEX batch for institutional (no FinMind cost)
                batch = getattr(self, '_inst_batch', {})
                if stock_id in batch:
                    chip_data = {'institutional': batch[stock_id]}
                else:
                    # 2nd: fallback to FinMind institutional only（scan_mode 跳過 margin/day_trading/shareholding/sbl）
                    try:
                        from chip_analysis import ChipAnalyzer
                        ca = ChipAnalyzer()
                        chip_data, _ = ca.get_chip_data(stock_id, scan_mode=True)
                    except Exception:
                        pass

        # 4. Run analysis (scan_mode=True 跳過 PE/除權息等 UI-only 資料)
        try:
            analyzer = TechnicalAnalyzer(
                stock_id, df_week, df_day,
                chip_data=chip_data,
                us_chip_data=us_chip_data,
                scan_mode=True,
            )
            report = analyzer.run_analysis()
        except Exception as e:
            logger.debug("TechnicalAnalyzer failed for %s: %s", stock_id, e)
            return None

        trigger = report.get('trigger_score', 0)
        trend = report.get('trend_score', 0)

        # 5. Extract key signals
        signals = self._extract_signals(report)

        # 6. ETF buy count (for display; scoring already in trigger via analysis_engine)
        etf_buy_count = 0
        breakdown = report.get('trigger_breakdown', {})
        if breakdown.get('etf_score', 0) > 0:
            # Estimate count from score: 0.3 → 2, 0.6 → 3+
            etf_buy_count = 3 if breakdown['etf_score'] >= 0.5 else 2

        # 7. 近 5 日平均成交值
        avg_tv_5d = 0
        try:
            if 'Close' in df_day.columns and 'Volume' in df_day.columns:
                tv = (df_day['Close'] * df_day['Volume']).tail(5)
                avg_tv_5d = int(tv.mean()) if len(tv) > 0 else 0
        except Exception:
            pass

        # 8. rvol_lowatr score (P2: 第二層 filter 用)
        #    = RVOL_z - ATR_pct_z (higher = 放量+低波動，IC v2 Sharpe 6.07)
        rvol_lowatr = None
        try:
            last = df_day.iloc[-1]
            _rvol_z = last.get('RVOL_z', None)
            _atr_z = last.get('ATR_pct_z', None)
            if _rvol_z is not None and _atr_z is not None:
                import math
                if not (math.isnan(_rvol_z) or math.isnan(_atr_z)):
                    rvol_lowatr = round(float(_rvol_z) - float(_atr_z), 4)
        except Exception:
            pass

        # 9. Scenario + action plan (already computed, just save)
        scenario = report.get('scenario', {})
        action_plan = report.get('action_plan', {})
        checklist = report.get('checklist', {})

        # QM mode: override action_plan with 40-60d horizon parameters
        if getattr(self, '_mode', 'momentum') == 'qm':
            # ATR% for exit_manager dynamic SL/TP
            _atr_pct = None
            try:
                _a = df_day['ATR_pct'].iloc[-1]
                if pd.notna(_a):
                    _atr_pct = float(_a)
            except Exception:
                pass
            # Regime for Phase 4 overlay
            _regime = None
            _regime_info = report.get('regime', {})
            if _regime_info:
                _regime = _regime_info.get('regime')
            action_plan = _apply_qm_action_plan(
                action_plan, df_week, trigger_score=trigger,
                atr_pct=_atr_pct, regime=_regime)

        return {
            'stock_id': stock_id,
            'name': market_row.get('stock_name', meta.get('name', '')),
            'market': market_row.get('market', 'twse'),
            'price': market_row.get('close', 0),
            'change_pct': round(market_row.get('change_pct', 0), 2),
            'trading_value': int(market_row.get('trading_value', 0)),
            'avg_trading_value_5d': avg_tv_5d,
            'trigger_score': round(trigger, 2),
            'trend_score': round(trend, 2),
            'score_percentile': report.get('score_percentile', None),
            'regime': report.get('regime', {}).get('regime', 'unknown'),
            'etf_buy_count': etf_buy_count,
            'rvol_lowatr': rvol_lowatr,
            'signals': signals,
            'trigger_details': report.get('trigger_details', []),
            'scenario': {
                'code': scenario.get('code', 'N'),
                'title': scenario.get('title', ''),
                'desc': scenario.get('desc', ''),
            },
            'action_plan': {
                'strategy': action_plan.get('strategy', ''),
                'rec_entry_low': action_plan.get('rec_entry_low'),
                'rec_entry_high': action_plan.get('rec_entry_high'),
                'rec_entry_desc': action_plan.get('rec_entry_desc', ''),
                'rec_sl_price': action_plan.get('rec_sl_price'),
                'rec_sl_method': action_plan.get('rec_sl_method', ''),
                'rec_tp_price': action_plan.get('rec_tp_price'),
                'rr_ratio': action_plan.get('rr_ratio'),
                'tp_list': action_plan.get('tp_list', []),
                'sl_list': action_plan.get('sl_list', []),
                # QM-specific fields (populated only when mode='qm')
                'qm_horizon': action_plan.get('qm_horizon'),
                'qm_hold_days_target': action_plan.get('qm_hold_days_target'),
                'qm_entry_batches': action_plan.get('qm_entry_batches'),
                'qm_entry_gate': action_plan.get('qm_entry_gate'),
                'qm_position_size': action_plan.get('qm_position_size'),
                'qm_exit_signals': action_plan.get('qm_exit_signals', []),
            },
            'checklist': checklist,
        }

    def _extract_signals(self, report):
        """Extract key signal tags from analysis report."""
        signals = []
        details = report.get('trigger_details', [])
        detail_text = ' '.join(details)

        # Supertrend
        if 'Supertrend 多方' in detail_text:
            signals.append('supertrend_bull')
        elif 'Supertrend 空方' in detail_text:
            signals.append('supertrend_bear')

        # MACD
        if 'MACD 黃金交叉' in detail_text or 'MACD 柱狀體翻正' in detail_text:
            signals.append('macd_golden')
        elif 'MACD 死亡交叉' in detail_text or 'MACD 柱狀體翻負' in detail_text:
            signals.append('macd_dead')

        # RSI
        if 'RSI 底背離' in detail_text:
            signals.append('rsi_bull_div')
        elif 'RSI 頂背離' in detail_text:
            signals.append('rsi_bear_div')

        # Volume
        if '爆量確認' in detail_text:
            signals.append('rvol_high')
        elif '量能萎縮' in detail_text:
            signals.append('rvol_low')

        # Chip (institutional)
        if '法人積極買超' in detail_text or '法人持續買超' in detail_text:
            signals.append('inst_buy')
        elif '法人大量賣超' in detail_text:
            signals.append('inst_sell')

        # ETF sync buy/sell
        if 'ETF 同步買超' in detail_text:
            signals.append('etf_sync_buy')
        elif 'ETF 買超' in detail_text:
            signals.append('etf_buy')
        elif 'ETF 同步賣超' in detail_text:
            signals.append('etf_sync_sell')

        # Squeeze
        if '壓縮' in detail_text and '釋放' in detail_text:
            signals.append('squeeze_fire')

        return signals

    # ================================================================
    # QM Quality Gate
    # ================================================================

    def _quality_gate(self, candidates, market):
        """
        QM 品質門檻: TradingView batch 篩掉虧損/高負債/營收崩的股票。
        設計原則: 寬鬆過濾（刷掉明顯地雷），不懲罰資料缺失。
        """
        from value_screener import _fetch_tradingview_batch

        tv_market = 'us' if market == 'us' else 'tw'
        tv_batch = _fetch_tradingview_batch(tv_market)

        if not tv_batch:
            self.progress("  Quality gate: TradingView unavailable, skipping")
            self._tv_quality = {}
            return candidates

        self._tv_quality = tv_batch  # 存起來，Stage 2 後附加到結果

        pass_ids = []
        fail_count = 0
        skip_count = 0

        for _, row in candidates.iterrows():
            sid = row['stock_id']
            tv = tv_batch.get(sid)

            if not tv:
                pass_ids.append(sid)
                skip_count += 1
                continue

            # ROE > 0 (有在賺錢)
            roe = tv.get('ROE')
            if roe is not None and roe <= 0:
                fail_count += 1
                continue

            # net_margin > 0 (本業不虧)
            nm = tv.get('net_margin')
            if nm is not None and nm <= 0:
                fail_count += 1
                continue

            # debt_to_equity < 200 (不要過度槓桿)
            de = tv.get('debt_to_equity')
            if de is not None and de > 200:
                fail_count += 1
                continue

            # revenue_yoy > -20% (營收不要崩盤)
            ry = tv.get('revenue_yoy')
            if ry is not None and ry < -20:
                fail_count += 1
                continue

            pass_ids.append(sid)

        result = candidates[candidates['stock_id'].isin(pass_ids)]
        self.progress(f"  Removed {fail_count} (ROE<=0/虧損/高負債/營收崩), {skip_count} no data (passed)")
        return result

    # ================================================================
    # Result Formatting
    # ================================================================

    def _make_result(self, scored, total_scanned, passed_initial, elapsed):
        """Build the final result dict."""
        now = datetime.now()
        market = getattr(self, '_market', 'tw')
        mode = getattr(self, '_mode', 'momentum')
        return {
            'scan_type': mode,
            'scan_date': now.strftime('%Y-%m-%d'),
            'scan_time': now.strftime('%H:%M'),
            'market': market,
            'total_scanned': total_scanned,
            'passed_initial': passed_initial,
            'scored_count': len(scored),
            'elapsed_seconds': round(elapsed, 1),
            'failures': self._failures[:20],
            'config': {
                'momentum_change_min': self.config['momentum_change_min'],
                'top_n': self.config['top_n'],
            },
            'results': scored,
        }

    @staticmethod
    def save_results(result, output_dir='data'):
        """
        Save results to data/latest/ and data/history/.

        Args:
            result: dict from run()
            output_dir: base directory for output
        """
        base = Path(output_dir)
        latest_dir = base / 'latest'
        history_dir = base / 'history'
        latest_dir.mkdir(parents=True, exist_ok=True)
        history_dir.mkdir(parents=True, exist_ok=True)

        # Determine filename prefix + suffix based on scan_type and market
        scan_type = result.get('scan_type', 'momentum')
        market = result.get('market', 'tw')
        suffix = '_us' if market == 'us' else ''

        # Latest result (overwritten each run)
        latest_file = latest_dir / f'{scan_type}{suffix}_result.json'
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        # History (appended by date)
        date_str = result.get('scan_date', datetime.now().strftime('%Y-%m-%d'))
        history_file = history_dir / f'{date_str}_{scan_type}{suffix}.json'
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        return str(latest_file), str(history_file)


# ====================================================================== #
#  CLI Entry Point (for testing)
# ====================================================================== #

if __name__ == '__main__':
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    )

    parser = argparse.ArgumentParser(description='Momentum Screener')
    parser.add_argument('--stage1-only', action='store_true',
                        help='Only run Stage 1 (no trigger scores)')
    parser.add_argument('--no-chip', action='store_true',
                        help='Skip chip data fetching (faster)')
    parser.add_argument('--top', type=int, default=50,
                        help='Number of results to return')
    parser.add_argument('--save', action='store_true',
                        help='Save results to data/ directory')
    args = parser.parse_args()

    config = {'top_n': args.top}
    if args.no_chip:
        config['include_chip'] = False

    screener = MomentumScreener(config=config)

    if args.stage1_only:
        df = screener.run_stage1_only()
        print(f"\nStage 1 Results: {len(df)} candidates")
        if not df.empty:
            cols = ['stock_id', 'stock_name', 'market', 'close',
                    'change_pct', 'trading_value', 'tv_pct']
            show_cols = [c for c in cols if c in df.columns]
            print(df[show_cols].head(30).to_string(index=False))
    else:
        result = screener.run()
        print(f"\nResults: {result['scored_count']} stocks scored")
        print(f"Scanned: {result['total_scanned']}, Passed: {result['passed_initial']}")
        print(f"Time: {result['elapsed_seconds']}s")

        if result['results']:
            print(f"\nTop {min(20, len(result['results']))}:")
            for i, r in enumerate(result['results'][:20], 1):
                sigs = ', '.join(r['signals'][:3])
                print(f"  {i:2d}. {r['stock_id']} {r['name'][:6]:6s} "
                      f"${r['price']:>8.1f}  {r['change_pct']:+5.1f}%  "
                      f"Score={r['trigger_score']:+5.1f}  [{sigs}]")

        if args.save:
            paths = MomentumScreener.save_results(result)
            print(f"\nSaved to: {paths[0]}")
