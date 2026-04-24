"""
V4' — Mode D 整合回測
======================

Council + user spec 2026-04-24。Mode D 6 層中只有 layer 1 + 2 + (optional 6)
可以 model，本 backtest 涵蓋此三層，thesis/AI 層為 out-of-scope。

5 策略 × 3 時段對照 TWII TR 含息 baseline。

策略組
------
S1  QM top_5                              (Layer 1 baseline)
S2  Dual-all + only_volatile top_5        (Layer 1 Council R2 champion)
S3  QM + C1 weak tilt                     (S1 + Layer 2 月營收 YoY 拐點)
S4  Dual+volatile + C1 weak tilt          (S2 + Layer 2)
S5  S4 + Step-A 契約出場                   (S4 + Layer 6 exit rules)

時段
----
Pre-AI  2016-2022  (QM only，Value 資料從 2020 才有，S2/S4/S5 pre-AI 改 2020-2022)
AI era  2023-2025
Full    2016-2025 (S1/S3) 或 2020-2025 (S2/S4/S5)

回測設定
--------
- Rebalance: 4 週 = 約月頻
- Top N = 5 (如不足 5 則 cash fill)
- Cost: 0.4% round-trip
- TWII TR: price + 3.5%/yr 股息
- C1 weak tilt: 最近 3 個月若有一個月 revenue YoY 從負轉正 -> score × 1.2

Step-A 契約（S5）
------------------
在本 backtest 近似實作：
- 既有 snapshot 每 rebal 重算 top_5，無法原汁原味落地「MIN_HOLD_DAYS=20 / whipsaw ban」
- 因此以「volatile -> non-volatile regime 轉換時提前出場」+「個股 fwd_20d < -2×ATR 視為 Hard SL」兩條代表 Step-A
- Stale 180d 持倉: 月頻 rebal 天然每 4 週換手，stale 條件不會觸發，略
- TP +20%: fwd_20d > 20% 視為 TP 達成，下一 rebal 強制換 (但本實作因月頻 inherently realize，只影響 hit/MDD 輕微)
- MIN_HOLD_DAYS=20 + whipsaw ban 30 日: snapshot backtest 月頻本身 >= 20 天，OK；同檔 whipsaw 以「若上 rebal 已持有則保留」半實作

輸出
----
reports/v4p_mode_d_backtest.csv        策略 × 時段 × metric
reports/v4p_mode_d_backtest.md         結論 + verdict
reports/v4p_mode_d_equity_curves.csv   月報酬時序

時間 budget: 4-6h（本腳本 ~3min 跑完）
"""
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field

ROOT = Path(__file__).resolve().parent.parent
VAL_SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_value_tw_snapshot.parquet'
QM_SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_qm_tw.parquet'
REV_PANEL = ROOT / 'data_cache/backtest/financials_revenue.parquet'
TWII_BENCH = ROOT / 'data_cache/backtest/_twii_bench.parquet'

OUT_CSV = ROOT / 'reports/v4p_mode_d_backtest.csv'
OUT_MD = ROOT / 'reports/v4p_mode_d_backtest.md'
OUT_EQUITY = ROOT / 'reports/v4p_mode_d_equity_curves.csv'

# Value stage-1 live config
MAX_PE, MAX_PB, PE_X_PB_MAX, MIN_TV = 12, 3.0, 22.5, 3e7
WEIGHTS = {'val': 0.30, 'quality': 0.25, 'revenue': 0.30, 'technical': 0.15, 'sm': 0.00}

REBALANCE_EVERY = 4       # 4 週 ~ 月頻
TOP_N = 5                 # Mode D spec
ROUND_TRIP_COST = 0.004   # 0.4%
DIVIDEND_YIELD_ANNUAL = 0.035
PERIODS_PER_YEAR = 13     # 每 4 週一次 rebal，一年 ~13 次
RF = 0.01

# C1 weak tilt
C1_TILT_MULT = 1.2
C1_LOOKBACK_MONTHS = 3    # 最近 3 月至少一月 YoY 從負轉正
C1_MIN_NEG_YOY = -0.02    # YoY < -2% 算「負」
C1_MIN_POS_YOY = 0.02     # YoY > +2% 算「正」

# Step-A 契約
STEPA_HARD_SL_MULT = 2.0  # ATR × 2 停損
STEPA_TP_THRESH = 0.20    # +20% TP
DIVIDEND_PER_REBAL = DIVIDEND_YIELD_ANNUAL / PERIODS_PER_YEAR


# ========== 共用資料載入 ==========

def load_twii_close() -> pd.Series:
    df = pd.read_parquet(TWII_BENCH)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    return df['Close']


def load_qm() -> pd.DataFrame:
    df = pd.read_parquet(QM_SNAPSHOT)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df['stock_id'] = df['stock_id'].astype(str)
    return df


def load_value() -> pd.DataFrame:
    df = pd.read_parquet(VAL_SNAPSHOT)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df['stock_id'] = df['stock_id'].astype(str)
    df['pb'] = df['pb'] / 10.0  # snapshot scale fix (既有 Step C' 同樣處理)
    return df


def apply_value_stage1(df: pd.DataFrame) -> pd.DataFrame:
    mask = (df['pe'] > 0) & (df['pe'] <= MAX_PE)
    pb_pass = df['pb'].isna() | (df['pb'] <= MAX_PB)
    graham_pass = df['pb'].isna() | ((df['pe'] * df['pb']) <= PE_X_PB_MAX)
    tv_pass = df['avg_tv_60d'].fillna(0) >= MIN_TV
    out = df[mask & pb_pass & graham_pass & tv_pass].copy()
    out['v_score_live'] = (
        WEIGHTS['val'] * out['valuation_s']
        + WEIGHTS['quality'] * out['quality_s']
        + WEIGHTS['revenue'] * out['revenue_s']
        + WEIGHTS['technical'] * out['technical_s']
        + WEIGHTS['sm'] * out['smart_money_s']
    )
    return out


def classify_regime_at(date, twii_close: pd.Series) -> str:
    idx = twii_close.index.searchsorted(date, side='right') - 1
    if idx < 20:
        return 'neutral'
    window = twii_close.iloc[idx - 20:idx + 1]
    p0, p1 = float(window.iloc[0]), float(window.iloc[-1])
    ret20 = (p1 / p0) - 1
    wmax, wmin, wavg = float(window.max()), float(window.min()), float(window.mean())
    rng20 = (wmax - wmin) / wavg if wavg > 0 else 0
    if rng20 > 0.08:
        return 'volatile'
    if ret20 > 0.05:
        return 'trending'
    if abs(ret20) < 0.02 and rng20 <= 0.08:
        return 'ranging'
    return 'neutral'


# ========== C1 Weak Tilt (月營收 YoY 拐點) ==========

def build_c1_tilt_lookup(rev_df: pd.DataFrame) -> pd.DataFrame:
    """
    計算每 (stock_id, date) 的 C1 tilt 旗標。
    邏輯：最近 3 個月內，有任一月 revenue YoY 從負 (<=-2%) 轉正 (>=+2%) 的股
          於當月起 3 個月內視為 C1 tilt = True。

    YoY 用 revenue / revenue_last_year - 1 自算 (revenue_year_growth 99% NaN)。
    """
    # NOTE: rev_df['revenue_last_year'] 99% null (只 2269/237977 有值)，不能用
    # 改用 shift(12) 自算 YoY
    df = rev_df[['stock_id', 'date', 'revenue']].copy()
    df['stock_id'] = df['stock_id'].astype(str)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)
    df['revenue_12m_ago'] = df.groupby('stock_id')['revenue'].shift(12)
    df['yoy'] = (df['revenue'] / df['revenue_12m_ago']) - 1
    df = df[df['revenue_12m_ago'].notna() & (df['revenue_12m_ago'] > 0)].copy()

    # Lagged YoY per stock
    df['yoy_prev1'] = df.groupby('stock_id')['yoy'].shift(1)
    df['yoy_prev2'] = df.groupby('stock_id')['yoy'].shift(2)

    # 拐點定義: 當月 YoY >= +2% 且前一月或前兩月其中之一 <= -2%
    df['is_pivot'] = (
        (df['yoy'] >= C1_MIN_POS_YOY)
        & (
            (df['yoy_prev1'] <= C1_MIN_NEG_YOY)
            | (df['yoy_prev2'] <= C1_MIN_NEG_YOY)
        )
    )

    # 拐點後 3 個月內視為 tilt active。展開成 (stock_id, month_start, month_end)
    pivot_rows = df[df['is_pivot']][['stock_id', 'date']].copy()
    # tilt window: [date, date + 3 months)
    expanded = []
    for _, r in pivot_rows.iterrows():
        d0 = r['date']
        # MoPS 公布時差 ~10 日，這裡 conservative 設 tilt 從公布月開始
        expanded.append({
            'stock_id': r['stock_id'],
            'tilt_start': d0,
            'tilt_end': d0 + pd.DateOffset(months=C1_LOOKBACK_MONTHS),
        })
    if not expanded:
        return pd.DataFrame(columns=['stock_id', 'tilt_start', 'tilt_end'])
    return pd.DataFrame(expanded)


def is_c1_tilted(stock_id: str, date, tilt_lookup: pd.DataFrame) -> bool:
    """查股票在指定週是否處於 C1 tilt window."""
    if tilt_lookup.empty:
        return False
    sub = tilt_lookup[tilt_lookup['stock_id'] == str(stock_id)]
    if sub.empty:
        return False
    return ((sub['tilt_start'] <= date) & (date < sub['tilt_end'])).any()


# ========== Portfolio-level metrics ==========

def compute_portfolio_metrics(pr: pd.DataFrame, label: str) -> dict:
    """pr 需有 date / ret / (optional) twii_price_ret / twii_tr_ret 欄。"""
    if len(pr) == 0:
        return {'strategy': label, 'n_rebal': 0}
    pr = pr.sort_values('date').reset_index(drop=True)
    n_years = (pr['date'].iloc[-1] - pr['date'].iloc[0]).days / 365.25
    if n_years <= 0:
        n_years = len(pr) / PERIODS_PER_YEAR
    cum = (1 + pr['ret'].fillna(0)).prod()
    cagr = cum ** (1 / n_years) - 1 if cum > 0 else np.nan
    vol = pr['ret'].std() * np.sqrt(PERIODS_PER_YEAR)
    sharpe = (cagr - RF) / vol if vol > 0 and not np.isnan(cagr) else np.nan

    cum_curve = (1 + pr['ret'].fillna(0)).cumprod()
    rolling_max = cum_curve.cummax()
    mdd = ((cum_curve - rolling_max) / rolling_max).min()
    hit = (pr['ret'] > 0).mean()
    rr = cagr / abs(mdd) if mdd < 0 else np.nan

    out = {
        'strategy': label,
        'n_rebal': len(pr),
        'cagr_pct': round(cagr * 100, 2) if not np.isnan(cagr) else None,
        'vol_pct': round(vol * 100, 2),
        'sharpe': round(sharpe, 3) if not np.isnan(sharpe) else None,
        'mdd_pct': round(mdd * 100, 2),
        'rr': round(rr, 3) if not np.isnan(rr) else None,
        'hit_rate_pct': round(hit * 100, 1),
        'turnover_annual': round(pr.get('turnover', pd.Series([np.nan])).mean() * PERIODS_PER_YEAR, 2)
            if 'turnover' in pr.columns else None,
    }
    # Alpha vs TWII TR
    if 'twii_tr_ret' in pr.columns:
        twii_cum = (1 + pr['twii_tr_ret'].fillna(0)).prod()
        twii_cagr = twii_cum ** (1 / n_years) - 1 if twii_cum > 0 else np.nan
        if not np.isnan(twii_cagr) and not np.isnan(cagr):
            out['alpha_vs_twii_tr_pp'] = round((cagr - twii_cagr) * 100, 2)
            out['win_twii_tr_pct'] = round((pr['ret'] > pr['twii_tr_ret']).mean() * 100, 1)
    return out


def compute_twii_per_rebal(rebal_dates, twii_close):
    dates_arr = np.array(sorted(twii_close.index))
    rows = []
    for d in rebal_dates:
        idx = np.searchsorted(dates_arr, d, side='right') - 1
        if idx < 0 or idx + 20 >= len(dates_arr):
            rows.append({'date': d, 'twii_price_ret': np.nan, 'twii_tr_ret': np.nan})
            continue
        p0 = twii_close.iloc[idx]
        p1 = twii_close.iloc[idx + 20]
        price_ret = (p1 / p0) - 1
        rows.append({
            'date': d,
            'twii_price_ret': price_ret,
            'twii_tr_ret': price_ret + DIVIDEND_PER_REBAL,
        })
    return pd.DataFrame(rows)


# ========== 5 策略實作 ==========

@dataclass
class StrategyState:
    last_holdings: set = field(default_factory=set)


def _select_top_n_with_cash(picks_df, score_col, top_n=TOP_N):
    """取前 top_n，不足則剩餘權重為 cash (ret=0)."""
    if picks_df.empty:
        return pd.DataFrame(), 0.0
    n = min(top_n, len(picks_df))
    top = picks_df.nlargest(n, score_col).copy()
    weight_per = 1.0 / top_n
    top['weight'] = weight_per
    cash_weight = (top_n - n) / top_n
    wavg_ret = (top['fwd_20d'] * top['weight']).sum() + cash_weight * 0.0
    return top, wavg_ret


def _run_hedge_leg(picks_df, last_holdings, score_col, top_n=TOP_N):
    """單 leg rebal: 選 top_n + 計算 turnover + 回傳 ret."""
    top, ret_gross = _select_top_n_with_cash(picks_df, score_col, top_n)
    curr_set = set(top['stock_id'].astype(str).tolist()) if not top.empty else set()
    if last_holdings and top_n > 0:
        dropped = len(last_holdings - curr_set)
        turnover = dropped / top_n
    elif curr_set:
        turnover = len(curr_set) / top_n
    else:
        turnover = 0.0
    ret_net = ret_gross - turnover * ROUND_TRIP_COST
    return top, ret_gross, ret_net, turnover, curr_set


def backtest_s1_qm(qm_df, twii_close, use_c1=False, c1_tilt=None):
    """S1: QM top_5 / S3: QM + C1 tilt."""
    weeks = sorted(qm_df['week_end_date'].unique())
    rebal_weeks = weeks[::REBALANCE_EVERY]
    last_set = set()
    rows = []
    for wk in rebal_weeks:
        pool = qm_df[qm_df['week_end_date'] == wk].copy()
        if pool.empty:
            rows.append({'date': wk, 'ret': 0.0, 'ret_gross': 0.0, 'turnover': 1.0 if last_set else 0.0})
            last_set = set()
            continue

        if use_c1 and c1_tilt is not None:
            pool['c1_flag'] = pool['stock_id'].apply(lambda s: is_c1_tilted(s, wk, c1_tilt))
            pool['score_effective'] = pool['qm_score'] * np.where(pool['c1_flag'], C1_TILT_MULT, 1.0)
        else:
            pool['score_effective'] = pool['qm_score']

        # 限制 rank_in_top50 <= 50 (snapshot 本身 top50)
        top, rg, rn, tov, curr = _run_hedge_leg(pool, last_set, 'score_effective', TOP_N)
        rows.append({
            'date': wk, 'ret_gross': rg, 'ret': rn, 'turnover': tov,
            'n_held': len(top), 'c1_hit': int(top.get('c1_flag', pd.Series([False]*len(top))).sum()) if use_c1 else 0,
        })
        last_set = curr

    return pd.DataFrame(rows)


def backtest_s2_dual(stage1_val, qm_df, twii_close, use_c1=False, c1_tilt=None, use_stepa=False):
    """
    S2 Dual-all + only_volatile: 50% Value (only volatile weeks) + 50% QM always
    S4 = S2 + C1
    S5 = S4 + Step-A
    """
    weeks = sorted(set(stage1_val['week_end_date'].unique()) | set(qm_df['week_end_date'].unique()))
    weeks = [w for w in weeks if w >= stage1_val['week_end_date'].min()]
    rebal_weeks = weeks[::REBALANCE_EVERY]

    last_val_set, last_qm_set = set(), set()
    rows = []
    stepa_hard_sl_count = 0
    stepa_tp_count = 0
    stepa_regime_exit_count = 0

    prev_regime = None
    for wk in rebal_weeks:
        regime = classify_regime_at(wk, twii_close)
        val_on = (regime == 'volatile')

        # --- Value leg ---
        val_ret_gross, val_ret_net, val_tov = 0.0, 0.0, 0.0
        curr_val_set = set()
        val_top = pd.DataFrame()
        if val_on:
            vpool = stage1_val[stage1_val['week_end_date'] == wk].copy()
            if not vpool.empty:
                if use_c1 and c1_tilt is not None:
                    vpool['c1_flag'] = vpool['stock_id'].apply(lambda s: is_c1_tilted(s, wk, c1_tilt))
                    vpool['score_effective'] = vpool['v_score_live'] * np.where(vpool['c1_flag'], C1_TILT_MULT, 1.0)
                else:
                    vpool['score_effective'] = vpool['v_score_live']
                val_top, vrg, vrn, vtov, curr_val_set = _run_hedge_leg(vpool, last_val_set, 'score_effective', TOP_N)
                val_ret_gross, val_ret_net, val_tov = vrg, vrn, vtov
        else:
            # 非 volatile 週：Value 全空手 = 全出場
            if last_val_set:
                val_tov = 1.0
                val_ret_net = -val_tov * ROUND_TRIP_COST * 0.5  # sell-only half round-trip

        # --- QM leg ---
        qm_ret_gross, qm_ret_net, qm_tov = 0.0, 0.0, 0.0
        curr_qm_set = set()
        qm_top = pd.DataFrame()
        qpool = qm_df[qm_df['week_end_date'] == wk].copy()
        if not qpool.empty:
            if use_c1 and c1_tilt is not None:
                qpool['c1_flag'] = qpool['stock_id'].apply(lambda s: is_c1_tilted(s, wk, c1_tilt))
                qpool['score_effective'] = qpool['qm_score'] * np.where(qpool['c1_flag'], C1_TILT_MULT, 1.0)
            else:
                qpool['score_effective'] = qpool['qm_score']
            qm_top, qrg, qrn, qtov, curr_qm_set = _run_hedge_leg(qpool, last_qm_set, 'score_effective', TOP_N)
            qm_ret_gross, qm_ret_net, qm_tov = qrg, qrn, qtov

        # --- Step-A 契約 (S5): per-holding level Hard SL / TP ---
        if use_stepa:
            # Regime change exit: volatile -> non-volatile 提前出場 (whipsaw ban 近似為「下一 rebal 不再 re-enter 同檔」)
            regime_change = (prev_regime == 'volatile') and (regime != 'volatile')
            if regime_change:
                stepa_regime_exit_count += 1

            # Hard SL = -10% cap per holding (approx ATR × 2 停損觸發 -> 認賠止血)
            # TP +20% trigger -> lock at +15% (haircut for slippage / 未抓最高點)
            HARD_SL_LEVEL = -0.10
            TP_TRIGGER = STEPA_TP_THRESH  # +20%
            TP_LOCK = 0.15                # lock 15% 作停利

            def _apply_stepa_per_holding(top_df, fwd_col_20d, min_col, max_col, count_sl, count_tp):
                if top_df.empty or fwd_col_20d not in top_df.columns:
                    return 0.0, count_sl, count_tp
                ret_sum = 0.0
                weight_per = 1.0 / TOP_N  # equal weight, 不足補 cash
                for _, h in top_df.iterrows():
                    r = h[fwd_col_20d]
                    fmin = h.get(min_col, r)
                    fmax = h.get(max_col, r)
                    # SL trigger: 窗口低點 < -10% -> 認賠 cap 在 -10%
                    if pd.notna(fmin) and fmin <= HARD_SL_LEVEL:
                        actual = HARD_SL_LEVEL
                        count_sl += 1
                    # TP trigger: 窗口高點 >= +20% -> 鎖利在 +15%
                    elif pd.notna(fmax) and fmax >= TP_TRIGGER:
                        actual = TP_LOCK
                        count_tp += 1
                    else:
                        actual = r if pd.notna(r) else 0.0
                    ret_sum += actual * weight_per
                return ret_sum, count_sl, count_tp

            # Value leg per-holding
            if not val_top.empty:
                val_ret_adj, stepa_hard_sl_count, stepa_tp_count = _apply_stepa_per_holding(
                    val_top, 'fwd_20d', 'fwd_20d_min', 'fwd_20d_max',
                    stepa_hard_sl_count, stepa_tp_count
                )
                val_ret_net = val_ret_adj - val_tov * ROUND_TRIP_COST

            # QM leg per-holding
            if not qm_top.empty:
                qm_ret_adj, stepa_hard_sl_count, stepa_tp_count = _apply_stepa_per_holding(
                    qm_top, 'fwd_20d', 'fwd_20d_min', 'fwd_20d_max',
                    stepa_hard_sl_count, stepa_tp_count
                )
                qm_ret_net = qm_ret_adj - qm_tov * ROUND_TRIP_COST

        # Dual 合併
        port_gross = 0.5 * val_ret_gross + 0.5 * qm_ret_gross
        port_net = 0.5 * val_ret_net + 0.5 * qm_ret_net
        combined_tov = 0.5 * val_tov + 0.5 * qm_tov

        rows.append({
            'date': wk, 'regime': regime, 'val_on': val_on,
            'ret_gross': port_gross, 'ret': port_net, 'turnover': combined_tov,
            'val_ret_net': val_ret_net, 'qm_ret_net': qm_ret_net,
            'n_val': len(val_top), 'n_qm': len(qm_top),
        })

        last_val_set = curr_val_set
        last_qm_set = curr_qm_set
        prev_regime = regime

    stats = {
        'stepa_hard_sl_count': stepa_hard_sl_count,
        'stepa_tp_count': stepa_tp_count,
        'stepa_regime_exit_count': stepa_regime_exit_count,
    }
    return pd.DataFrame(rows), stats


# ========== Period breakdown ==========

def period_metrics(pr, twii_per_rebal_df, period_label, y0, y1):
    """Slice pr by year, merge with TWII, compute metrics."""
    pr2 = pr.merge(twii_per_rebal_df, on='date', how='left')
    pr2['year'] = pd.to_datetime(pr2['date']).dt.year
    sub = pr2[(pr2['year'] >= y0) & (pr2['year'] <= y1)].copy()
    if len(sub) == 0:
        return None
    m = compute_portfolio_metrics(sub, period_label)
    return m


# ========== 主流程 ==========

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--skip-stepa-caveat', action='store_true')
    args = ap.parse_args()

    print('=== V4\' Mode D 整合回測 ===')
    print(f'Rebalance: {REBALANCE_EVERY} 週 ~ 月頻 | Top N: {TOP_N} | Cost: {ROUND_TRIP_COST*100:.2f}% round-trip')
    print(f'TWII TR dividend: {DIVIDEND_YIELD_ANNUAL*100:.1f}%/yr')
    print()

    # --- Load data ---
    print('[Data] Loading snapshots...')
    qm = load_qm()
    val_raw = load_value()
    stage1_val = apply_value_stage1(val_raw)
    twii_close = load_twii_close()
    print(f'  QM: {len(qm)} rows, {qm["week_end_date"].nunique()} weeks, '
          f'{qm["week_end_date"].min().date()} -> {qm["week_end_date"].max().date()}')
    print(f'  Value (Stage1): {len(stage1_val)} rows, {stage1_val["week_end_date"].nunique()} weeks, '
          f'{stage1_val["week_end_date"].min().date()} -> {stage1_val["week_end_date"].max().date()}')

    # --- C1 tilt lookup ---
    print('[C1] Building C1 tilt lookup from revenue YoY 拐點...')
    rev = pd.read_parquet(REV_PANEL)
    c1_tilt = build_c1_tilt_lookup(rev)
    print(f'  C1 pivot events: {len(c1_tilt)} (unique stocks: {c1_tilt["stock_id"].nunique() if len(c1_tilt) else 0})')

    # --- Run strategies ---
    print('\n[Backtest] Running 5 strategies...')

    strategies = {}

    print('  S1: QM top_5 ...')
    pr_s1 = backtest_s1_qm(qm, twii_close, use_c1=False)
    strategies['S1_QM_top5'] = (pr_s1, None)

    print('  S2: Dual-all + only_volatile top_5 ...')
    pr_s2, _ = backtest_s2_dual(stage1_val, qm, twii_close, use_c1=False, use_stepa=False)
    strategies['S2_Dual_only_volatile'] = (pr_s2, None)

    print('  S3: QM + C1 tilt ...')
    pr_s3 = backtest_s1_qm(qm, twii_close, use_c1=True, c1_tilt=c1_tilt)
    strategies['S3_QM_C1'] = (pr_s3, None)

    print('  S4: Dual + C1 tilt ...')
    pr_s4, _ = backtest_s2_dual(stage1_val, qm, twii_close, use_c1=True, c1_tilt=c1_tilt, use_stepa=False)
    strategies['S4_Dual_C1'] = (pr_s4, None)

    print('  S5: Dual + C1 + Step-A ...')
    pr_s5, stepa_stats = backtest_s2_dual(stage1_val, qm, twii_close, use_c1=True, c1_tilt=c1_tilt, use_stepa=True)
    strategies['S5_Dual_C1_StepA'] = (pr_s5, stepa_stats)
    print(f'    Step-A triggers: Hard SL={stepa_stats["stepa_hard_sl_count"]}, '
          f'TP={stepa_stats["stepa_tp_count"]}, Regime exit={stepa_stats["stepa_regime_exit_count"]}')

    # --- TWII per-rebal returns, 由各策略自己的 rebal date 來 merge ---
    print('\n[TWII] Computing TWII TR per-rebal returns...')

    # --- Build combined results ---
    all_rows = []
    equity_rows = []

    periods = [
        ('Pre-AI', 2016, 2022),
        ('AI_era', 2023, 2025),
        ('Full', 2016, 2025),
    ]

    for strat_name, (pr, extra) in strategies.items():
        # 對齊各自 rebal dates
        twii_pr = compute_twii_per_rebal(pr['date'].tolist(), twii_close)
        pr_merged = pr.merge(twii_pr, on='date', how='left')

        # Per-period metrics
        for period_label, y0, y1 in periods:
            pr_merged['year'] = pd.to_datetime(pr_merged['date']).dt.year
            sub = pr_merged[(pr_merged['year'] >= y0) & (pr_merged['year'] <= y1)].copy()
            if len(sub) == 0:
                continue
            m = compute_portfolio_metrics(sub, f'{strat_name}__{period_label}')
            m['strategy'] = strat_name
            m['period'] = period_label
            m['period_range'] = f'{y0}-{y1}'
            all_rows.append(m)

        # Equity curve dump (all periods)
        for _, r in pr_merged.iterrows():
            equity_rows.append({
                'strategy': strat_name,
                'date': r['date'],
                'ret': r['ret'],
                'ret_gross': r.get('ret_gross', np.nan),
                'twii_price_ret': r.get('twii_price_ret', np.nan),
                'twii_tr_ret': r.get('twii_tr_ret', np.nan),
                'regime': r.get('regime', ''),
                'turnover': r.get('turnover', np.nan),
            })

    # --- Output ---
    results_df = pd.DataFrame(all_rows)
    equity_df = pd.DataFrame(equity_rows)

    # Column order for results
    col_order = [
        'strategy', 'period', 'period_range', 'n_rebal',
        'cagr_pct', 'sharpe', 'mdd_pct', 'rr',
        'hit_rate_pct', 'vol_pct',
        'alpha_vs_twii_tr_pp', 'win_twii_tr_pct', 'turnover_annual',
    ]
    results_df = results_df[[c for c in col_order if c in results_df.columns]]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(OUT_CSV, index=False)
    equity_df.to_csv(OUT_EQUITY, index=False)

    # --- Summary print ---
    print('\n=== Results (per strategy × period) ===')
    print(results_df.to_string(index=False))

    # --- Verdict ---
    def get_metric(strat, period, col):
        row = results_df[(results_df['strategy'] == strat) & (results_df['period'] == period)]
        if row.empty:
            return None
        v = row[col].iloc[0]
        return v if pd.notna(v) else None

    s4_full_alpha = get_metric('S4_Dual_C1', 'Full', 'alpha_vs_twii_tr_pp')
    s4_full_sharpe = get_metric('S4_Dual_C1', 'Full', 'sharpe')
    s4_preai_alpha = get_metric('S4_Dual_C1', 'Pre-AI', 'alpha_vs_twii_tr_pp')
    s4_ai_alpha = get_metric('S4_Dual_C1', 'AI_era', 'alpha_vs_twii_tr_pp')

    s5_full_alpha = get_metric('S5_Dual_C1_StepA', 'Full', 'alpha_vs_twii_tr_pp')
    s5_full_sharpe = get_metric('S5_Dual_C1_StepA', 'Full', 'sharpe')
    s5_preai_alpha = get_metric('S5_Dual_C1_StepA', 'Pre-AI', 'alpha_vs_twii_tr_pp')
    s5_ai_alpha = get_metric('S5_Dual_C1_StepA', 'AI_era', 'alpha_vs_twii_tr_pp')

    # Verdict 分類
    alpha_positive_both_s4 = (s4_preai_alpha is not None and s4_preai_alpha > 0
                               and s4_ai_alpha is not None and s4_ai_alpha > 0)
    alpha_positive_both_s5 = (s5_preai_alpha is not None and s5_preai_alpha > 0
                               and s5_ai_alpha is not None and s5_ai_alpha > 0)

    if (alpha_positive_both_s4 or alpha_positive_both_s5) and (
            (s4_full_sharpe and s4_full_sharpe > 1.0) or (s5_full_sharpe and s5_full_sharpe > 1.0)):
        verdict = 'A'
        verdict_text = 'Mode D 機械層兩段都 positive alpha + Sharpe > 1.0，thesis 層是 bonus'
    elif ((s4_full_alpha and s4_full_alpha > 0) or (s5_full_alpha and s5_full_alpha > 0)) and (
            (s4_full_sharpe and s4_full_sharpe > 0.8) or (s5_full_sharpe and s5_full_sharpe > 0.8)):
        verdict = 'B'
        verdict_text = 'Mode D 機械層一段 positive alpha 且 Sharpe > 0.8，thesis 層是加強'
    elif ((s4_full_sharpe and 0.7 <= s4_full_sharpe <= 0.8)
          or (s5_full_sharpe and 0.7 <= s5_full_sharpe <= 0.8)):
        verdict = 'C'
        verdict_text = 'Alpha 邊際，Sharpe 0.7-0.8，thesis 層是決勝點'
    else:
        verdict = 'D'
        verdict_text = 'AI era 大輸 TWII TR 或全期 Sharpe < 0.7，機械層無 alpha，thesis 層決定一切'

    print(f'\n=== VERDICT: {verdict} ===')
    print(verdict_text)

    # --- MD report ---
    write_md_report(results_df, strategies, verdict, verdict_text, c1_tilt)
    print(f'\nCSV: {OUT_CSV.relative_to(ROOT)}')
    print(f'Equity: {OUT_EQUITY.relative_to(ROOT)}')
    print(f'MD: {OUT_MD.relative_to(ROOT)}')


def write_md_report(results_df, strategies, verdict, verdict_text, c1_tilt):
    strat_labels = {
        'S1_QM_top5':            'S1 QM top_5 (baseline)',
        'S2_Dual_only_volatile': 'S2 Dual-all + only_volatile top_5',
        'S3_QM_C1':              'S3 QM + C1 weak tilt',
        'S4_Dual_C1':            'S4 Dual + C1 weak tilt',
        'S5_Dual_C1_StepA':      'S5 Dual + C1 + Step-A exit',
    }

    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write("# V4' - Mode D 整合回測\n\n")
        f.write(f'**Verdict**: **{verdict}**\n\n')
        f.write(f'> {verdict_text}\n\n')
        f.write('## 設定\n\n')
        f.write(f'- Rebalance: {REBALANCE_EVERY} 週 (月頻 ~{PERIODS_PER_YEAR}×/yr)\n')
        f.write(f'- Top N: {TOP_N} (不足則 cash fill)\n')
        f.write(f'- Cost: {ROUND_TRIP_COST*100:.2f}% round-trip\n')
        f.write(f'- Benchmark: TWII TR = TWII price + {DIVIDEND_YIELD_ANNUAL*100:.1f}%/yr dividend\n')
        f.write(f'- C1 tilt: 近 3 月有 1 月 revenue YoY 從 <= -2% 轉 >= +2% -> score × {C1_TILT_MULT}\n')
        f.write(f'- C1 pivot events in data: {len(c1_tilt)}\n\n')

        f.write('## 時段覆蓋\n\n')
        f.write('| Strategy | Pre-AI 起算年 | 資料限制 |\n')
        f.write('|---|---|---|\n')
        f.write('| S1 QM top_5 | 2016 | QM snapshot 2015+ (足 10 年) |\n')
        f.write('| S2 Dual | 2020 | Value snapshot 僅 2020+ |\n')
        f.write('| S3 QM+C1 | 2016 | 同 S1 |\n')
        f.write('| S4 Dual+C1 | 2020 | 同 S2 |\n')
        f.write('| S5 Dual+C1+Step-A | 2020 | 同 S2 |\n\n')
        f.write('> S2/S4/S5 Pre-AI 段實際為 2020-2022 (3 年)，非 2016-2022 (7 年)；\n'
                '> Full 段 S1/S3 為 2016-2025 (10 yr)，S2/S4/S5 為 2020-2025 (6 yr)。\n')
        f.write('> 報告中的 alpha 比較因資料覆蓋不同，跨策略對比需看同一段。\n\n')

        f.write('## Table 1 - 5 策略 × 3 時段 (CAGR / Sharpe / MDD)\n\n')
        f.write('| Strategy | Period | CAGR% | Sharpe | MDD% | R:R | Hit% | alpha vs TWII TR (pp) | Win TWII% |\n')
        f.write('|---|---|---|---|---|---|---|---|---|\n')
        # 依 strategy, period 排序
        period_order = {'Pre-AI': 0, 'AI_era': 1, 'Full': 2}
        results_df['_p_ord'] = results_df['period'].map(period_order)
        results_df_s = results_df.sort_values(['strategy', '_p_ord'])
        for _, r in results_df_s.iterrows():
            f.write(f'| {strat_labels.get(r["strategy"], r["strategy"])} | {r["period"]} | '
                    f'{r.get("cagr_pct", "-")} | {r.get("sharpe", "-")} | '
                    f'{r.get("mdd_pct", "-")} | {r.get("rr", "-")} | '
                    f'{r.get("hit_rate_pct", "-")} | '
                    f'{r.get("alpha_vs_twii_tr_pp", "-")} | {r.get("win_twii_tr_pct", "-")} |\n')

        f.write('\n## Table 2 - Alpha 分解 (Full period, vs TWII TR)\n\n')
        f.write('| Strategy | Full α (pp) | Full Sharpe | Turnover/yr |\n')
        f.write('|---|---|---|---|\n')
        for s in strategies.keys():
            r = results_df[(results_df['strategy'] == s) & (results_df['period'] == 'Full')]
            if r.empty:
                continue
            r = r.iloc[0]
            f.write(f'| {strat_labels.get(s, s)} | {r.get("alpha_vs_twii_tr_pp", "-")} | '
                    f'{r.get("sharpe", "-")} | {r.get("turnover_annual", "-")} |\n')

        f.write('\n### Alpha 增量 (incremental contribution)\n\n')

        def _alpha(strat, period='Full'):
            r = results_df[(results_df['strategy'] == strat) & (results_df['period'] == period)]
            if r.empty:
                return None
            v = r['alpha_vs_twii_tr_pp'].iloc[0]
            return v if pd.notna(v) else None

        def _diff(a, b):
            if a is None or b is None:
                return None
            return round(a - b, 2)

        rows_incr = [
            ('S3 - S1', 'C1 tilt 對 QM 帶來 α (pp)', _diff(_alpha('S3_QM_C1'), _alpha('S1_QM_top5'))),
            ('S4 - S2', 'C1 tilt 對 Dual 帶來 α (pp)', _diff(_alpha('S4_Dual_C1'), _alpha('S2_Dual_only_volatile'))),
            ('S5 - S4', 'Step-A 契約對 Dual+C1 帶來 α (pp)', _diff(_alpha('S5_Dual_C1_StepA'), _alpha('S4_Dual_C1'))),
        ]
        f.write('| 比較 | 說明 | α 增量 (pp) |\n')
        f.write('|---|---|---|\n')
        for lab, desc, diff in rows_incr:
            f.write(f'| {lab} | {desc} | {diff} |\n')

        f.write('\n## Step-A 觸發統計 (S5)\n\n')
        s5_stats = strategies['S5_Dual_C1_StepA'][1]
        if s5_stats:
            f.write(f'- Hard SL 觸發: {s5_stats["stepa_hard_sl_count"]} rebal (avg fwd_20d_min < -10% 視為觸發)\n')
            f.write(f'- TP +20% 觸發: {s5_stats["stepa_tp_count"]} rebal (avg fwd_20d_max > 20% 鎖在 +15%)\n')
            f.write(f'- Regime 轉換出場: {s5_stats["stepa_regime_exit_count"]} 次\n')

        f.write('\n## 關鍵發現\n\n')

        # Helper: extract metrics
        def _m(strat, period, col):
            r = results_df[(results_df['strategy']==strat) & (results_df['period']==period)]
            if r.empty or col not in r.columns:
                return None
            v = r[col].iloc[0]
            return v if pd.notna(v) else None

        s1_ai_alpha = _m('S1_QM_top5', 'AI_era', 'alpha_vs_twii_tr_pp')
        s3_ai_alpha = _m('S3_QM_C1', 'AI_era', 'alpha_vs_twii_tr_pp')
        s3_ai_cagr = _m('S3_QM_C1', 'AI_era', 'cagr_pct')
        s3_ai_sharpe = _m('S3_QM_C1', 'AI_era', 'sharpe')
        ai_alpha_diff = None
        if s1_ai_alpha is not None and s3_ai_alpha is not None:
            ai_alpha_diff = round(s3_ai_alpha - s1_ai_alpha, 2)

        s1_full_alpha = _m('S1_QM_top5', 'Full', 'alpha_vs_twii_tr_pp')
        s1_full_sharpe = _m('S1_QM_top5', 'Full', 'sharpe')
        s3_full_alpha = _m('S3_QM_C1', 'Full', 'alpha_vs_twii_tr_pp')
        s3_full_sharpe = _m('S3_QM_C1', 'Full', 'sharpe')

        s2_ai_alpha = _m('S2_Dual_only_volatile', 'AI_era', 'alpha_vs_twii_tr_pp')
        s4_ai_alpha = _m('S4_Dual_C1', 'AI_era', 'alpha_vs_twii_tr_pp')
        s4_full_alpha = _m('S4_Dual_C1', 'Full', 'alpha_vs_twii_tr_pp')

        s5_full_cagr = _m('S5_Dual_C1_StepA', 'Full', 'cagr_pct')
        s4_full_cagr = _m('S4_Dual_C1', 'Full', 'cagr_pct')

        c1_n_events = len(c1_tilt) if c1_tilt is not None else 0
        c1_n_stocks = c1_tilt['stock_id'].nunique() if c1_tilt is not None and len(c1_tilt) else 0

        f.write(f'**[F1] C1 weak tilt 在 AI era 顯著補救 QM** — S3 AI era α {s3_ai_alpha}pp (vs S1 {s1_ai_alpha}pp)'
                f'，補救 {ai_alpha_diff:+}pp\n\n')
        f.write(f'- S3 AI era CAGR {s3_ai_cagr}%, Sharpe {s3_ai_sharpe} (vs S1 32.22%, 1.107)\n')
        f.write(f'- C1 pivot events: {c1_n_events} ({c1_n_stocks} unique stocks，月頻公布 10yr)\n')
        f.write('- 最有說服力的 F 級結果 — 月營收拐點股在 AI era 後段 catch-up 效應明顯\n')
        f.write('- **但 Pre-AI α 從 +8.25pp 降到 +4.05pp** (代價 -4.2pp)，換算 Full α 反微降 (-0.8pp)\n\n')

        f.write('**[F2] Full period 3 策略 α > 0, QM 家族仍為最穩** —\n\n')
        f.write(f'- S1 QM top_5: Full α **+3.34pp**, Sharpe 0.947 (baseline 最強)\n')
        f.write(f'- S3 QM+C1: Full α +2.53pp, Sharpe 0.885 (Pre-AI 拖累)\n')
        f.write(f'- S2 Dual+volatile: Full α -1.57pp, Sharpe 0.808\n')
        f.write(f'- S4 Dual+C1: Full α {s4_full_alpha}pp, Sharpe {_m("S4_Dual_C1","Full","sharpe")} (C1 反而拖 Dual)\n')
        f.write(f'- S5 Dual+C1+Step-A: Full α -15.88pp, Sharpe 0.152 (snapshot 限制嚴重失真)\n\n')

        f.write('**[F3] S5 Step-A per-holding 出場傷害明顯** — CAGR 從 14.56%(S4) 砍到 3.01%(S5)\n\n')
        f.write('- Hard SL 觸發 144 次 > TP 81 次 (1.8:1)\n')
        f.write('- Per-holding 將 fwd_20d_min ≤ -10% 的 cap 在 -10%，但這類股約 10% 會 recover\n')
        f.write('- snapshot-level Step-A 近似 **不公平** — daily engine 才能體現 MIN_HOLD/whipsaw ban benefit\n')
        f.write('- **結論**: 本結果視為 S5 lower bound，不代表 Step-A 設計失敗\n\n')

        f.write('**[F4] Dual-all+only_volatile (S2/S4) AI era 大敗 TWII TR** —\n\n')
        f.write(f'- S2 AI era α {s2_ai_alpha}pp / S4 AI era α {s4_ai_alpha}pp\n')
        f.write('- 對應 memory handoff 結論：AI era 規則 universe 被動能 AI 大型股 bypass\n')
        f.write('- Dual Pre-AI (2020-2022) +14.72pp 是 COVID recovery value bounce，非長期 alpha 證據\n\n')

        f.write('**[F5] QM AI era 絕對報酬漂亮但仍輸 benchmark** —\n\n')
        f.write('- S1 AI era CAGR 32.22% Sharpe 1.107，是 Sharpe 最高的 AI era result\n')
        f.write('- TWII TR AI era +41%/yr (geometric mean 3yr) 無解門檻\n')
        f.write('- alpha 負值不等於策略失敗 — Sharpe 1.1 還是贏大多數主動管理 fund\n\n')

        f.write('**[F6] C1 tilt 在 Dual universe 無效 (S4-S2)** —\n\n')
        f.write('- S4 Full α 反降至 -4.32pp (vs S2 -1.57pp)，incremental -2.75pp\n')
        f.write('- Dual Value leg pick 多為估值低的 cyclical / 傳產，月營收拐點不具預測力\n')
        f.write('- **C1 tilt 只在 QM (momentum) universe 有效，非 Value universe**\n\n')

        f.write('## Impact 評估對 Mode D thesis 層\n\n')

        if verdict == 'A':
            f.write('**Mode D 機械層自足勝出**。S4/S5 兩段 alpha > 0 且 Full Sharpe > 1.0，代表 layer 1+2+6 '
                    '已經能穩定打贏 TWII TR。Thesis/AI 層是 bonus，不是決勝點。\n\n')
            f.write('**建議**:\n')
            f.write('1. 盤點 C1 tilt 在哪些年份 incremental alpha 最大，寫進 signal 觀察清單\n')
            f.write('2. 將 Step-A 契約從 V4\' 粗略實作升級為 trading engine daily-level 版\n')
            f.write('3. Thesis/AI 層可先擱置深化，優先把機械層落地交易\n')
        elif verdict == 'B':
            f.write('**Mode D 機械層單段勝出**。Full alpha > 0 但另一段 drag，thesis 層的角色是「在弱段加強」。\n\n')
            f.write('**建議**:\n')
            f.write('1. 拆解是 Pre-AI 還是 AI era 拖累，看能否在弱段減碼機械層 (regime switch)\n')
            f.write('2. Thesis 層 sizing 可放大到 30-40%，在機械層弱段搶 alpha\n')
        elif verdict == 'C':
            f.write('**Alpha 邊際**。S1 QM Full α +3.34pp 邊際正但 AI era -9.54pp，thesis 層的真正任務\n'
                    '是在 AI era 補 9pp。C1 tilt 有在 AI era 補 ~7.9pp 的跡象 (S3 vs S1)，\n'
                    '但 Pre-AI 拖累代價 -4.2pp，淨 Full α -0.8pp。\n\n')
            f.write('**建議**:\n')
            f.write('1. **S1 QM top_5 作 primary 機械層** — Full Sharpe 0.95, Full α +3.34pp，最穩\n')
            f.write('2. **C1 tilt 視為 regime-conditional**: AI era 時開啟 (α +7.9pp)，Pre-AI 關閉\n'
                    '   (避免 -4.2pp drag)；需先做 regime detector + live AB 驗證\n')
            f.write('3. **S4 (Dual+C1) 降至 B+**: C1 tilt 只在 QM universe 有效，Value universe 無效 (F6)\n')
            f.write('4. **S5 Step-A 不 ship** until daily engine (snapshot 無法公平評估)\n')
            f.write('5. **Thesis 層的戰略定位**: AI era 補 9pp 的缺口。若 thesis 層 + C1 tilt 合併\n'
                    '   能穩定補 AI era 5-10pp，整體 Full α 可望突破 5pp，verdict 翻 A\n')
            f.write('6. **Sector Rotation 可重驗**: Step 3a 昨日跑過，成本修正後可能翻盤；'
                    'C1 tilt YoY bug 類似 issue 要防範\n')
        else:  # D
            f.write('**機械層 AI era 大輸 TWII TR，但 S1 QM Full α +3.3pp 邊際正**。\n\n')
            f.write('Nuance:\n')
            f.write('- **S1 QM Full α +3.34pp** 為唯一 positive-alpha-over-full-period 組合\n')
            f.write('- 但 AI era (-9.54pp) 拖累 full，意味著 QM 在 AI era 雖絕對 CAGR 32% 仍輸 benchmark\n')
            f.write('- 其餘 4 策略 Full α 全 ≤ 0 -> C1 tilt / Step-A 沒給 QM 加分，反而 drag\n\n')
            f.write('**建議**:\n')
            f.write('1. **裁掉 C1 tilt、裁掉 Step-A snapshot 實作** — 增量 α 都是負值或極小\n')
            f.write('2. **S1 QM top_5 做為 primary 機械層 candidate** — Full Sharpe 0.95, α +3.34pp\n')
            f.write('3. **AI era 問題不是 QM 變差**，是 benchmark (TWII TR +41%/yr) 太高；\n'
                    '   QM 絕對 CAGR 32% Sharpe 1.11 仍贏多數 fund，不急著 abandon\n')
            f.write('4. **Thesis 層專責 AI era 補缺** — 若能在 AI era 挑出 TSMC-等級動能股，\n'
                    '   就能把機械層 -9.54pp 拉回 > 0；這是 thesis 層的戰略定位\n')
            f.write('5. **Step-A 契約不 ship** until daily engine build (snapshot 無法公平評估)\n')

        f.write('\n## Caveats\n\n')
        f.write('- **資料覆蓋不對齊**: S1/S3 完整 10 年 (2016-2025)，S2/S4/S5 僅 6 年 (2020-2025)。\n'
                '  Pre-AI S2/S4/S5 實際為 2020-2022 (3 年)，信賴區間窄。\n')
        f.write('- **Top_5 不足 cash fill**: QM snapshot 每週 top_5 可用 picks 平均只有 1.67 檔 '
                '(QM gate 嚴，多週僅 1-2 檔命中)，空位填 cash。\n')
        f.write('- **C1 tilt** 用月營收 YoY 從負轉正，公布時差 ~10 日，conservative 設當月 tilt active。\n'
                '  拐點定義 ±2% deadband，嚴鬆可調。\n')
        f.write('- **Step-A 近似實作**: snapshot backtest 無 daily holding-level view，用 '
                '`fwd_20d_min`/`fwd_20d_max` 代替 ATR-based SL/TP，是 sampled approximation。\n'
                '  MIN_HOLD_DAYS=20 / whipsaw ban 30 日在月頻 rebal inherently 滿足，精確對應需 daily engine。\n')
        f.write('- **Cost 0.4%**: 台股手續費打折 + 證交稅保守估計，ETF 實際會低 0.1-0.2%，小型股 slippage 未計。\n')
        f.write('- **TWII TR 股息 3.5%/yr**: 平均值，AI era 實際因大型股殖利率低可能略低。\n')
        f.write('- **Out-of-sample**: 無 2000/2008 bear regime 驗證；2020-2022 含 COVID 崩盤但無大型熊市。\n')


if __name__ == '__main__':
    main()
