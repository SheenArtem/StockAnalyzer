"""
Whale Picks Phase 2 — IC backtest pipeline (per docs/whale_picks_spec.md v0.2).

Implements stages 3-6 + 9 of the 9-stage council pipeline:
  Stage 3 — Preprocess (winsorize + standardize)
  Stage 4 — Univariate selection (CAR rank IC, P@K, path-MAE)
  Stage 5 — Decile + monotonicity kill test
  Stage 6 — BH-FDR alpha=0.10
  Stage 9 — Output report (markdown + CSV)

Stages 7 (walk-forward) and 8 (portfolio sim) deferred to v2 after MVP results.

Usage:
    python tools/whale_picks_phase2.py --start 2021-01-01 --end 2025-12-31 \
        --output-dir reports/whale_picks_phase2_v1
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import stats

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
CACHE = REPO / "data_cache" / "backtest"


# ============================================================
# Data loaders
# ============================================================

def load_indicators(start: str, end: str) -> pd.DataFrame:
    """Load value_sim_indicators (price + tech), filter by date range."""
    df = pd.read_parquet(CACHE / "value_sim_indicators.parquet")
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] >= start) & (df['date'] <= end)].copy()
    log.info("Indicators: %d rows, %d sids, %s ~ %s",
             len(df), df['stock_id'].nunique(), df['date'].min().date(), df['date'].max().date())
    return df


def load_fwd_returns(start: str, end: str) -> pd.DataFrame:
    df = pd.read_parquet(CACHE / "value_sim_fwd_returns.parquet")
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] >= start) & (df['date'] <= end)].copy()
    log.info("Fwd returns: %d rows, %d sids", len(df), df['stock_id'].nunique())
    return df


def load_smart_money(start: str, end: str) -> pd.DataFrame:
    df = pd.read_parquet(CACHE / "smart_money_scores.parquet")
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df = df[(df['week_end_date'] >= start) & (df['week_end_date'] <= end)].copy()
    df = df.rename(columns={'week_end_date': 'date'})
    log.info("Smart money: %d rows, %d sids", len(df), df['stock_id'].nunique())
    return df


def load_quality(start: str, end: str) -> pd.DataFrame:
    df = pd.read_parquet(CACHE / "quality_scores.parquet")
    df['date'] = pd.to_datetime(df['date'])
    df_start = (pd.Timestamp(start) - pd.DateOffset(years=2)).strftime('%Y-%m-%d')
    df = df[(df['date'] >= df_start) & (df['date'] <= end)].copy()
    # F-Score Δ (4Q diff = YoY change)
    df = df.sort_values(['stock_id', 'date'])
    df['f_score_4q_delta'] = df.groupby('stock_id')['f_score'].diff(4)
    df['f_score_1q_delta'] = df.groupby('stock_id')['f_score'].diff(1)
    log.info("Quality scores: %d rows, %d sids", len(df), df['stock_id'].nunique())
    return df


def load_universe_industry() -> pd.DataFrame:
    """Load PIT universe with industry tag — 含已下市股票避免 survivor bias。

    2026-05-16 修：原版用 universe_tw.parquet（2127 檔 survivor only），
    backtest 2021-2025 miss 掉 1483 檔當時在世後來下市的股票，是 survivor bias。
    改用 universe_tw_pit.parquet（3610 檔含下市），OHLCV 缺資料期間自然 drop out。

    PIT universe 由 tdcc_universe_download.py 產出（TDCC OpenData 1-1 證券基本資料主檔）
    後再用 tools/build_pit_universe.py 合併 industry_category。
    """
    pit_path = CACHE / "universe_tw_pit.parquet"
    if pit_path.exists():
        df = pd.read_parquet(pit_path)
        return df[['stock_id', 'industry_category']].drop_duplicates()
    # Fallback: survivor-only universe (legacy, survivorship-biased)
    log.warning("universe_tw_pit.parquet not found — fallback to survivor-only universe_tw.parquet (survivorship bias)")
    df = pd.read_parquet(CACHE / "universe_tw.parquet")
    return df[['stock_id', 'industry_category']].drop_duplicates()


def load_financials_panel(start: str, end: str) -> pd.DataFrame:
    """Load quarterly financials, compute ROE/ROA/GM/OpMargin/Debt ratio as wide table.

    Returns: stock_id, date (quarter end), roe, roa, gross_margin, op_margin, debt_ratio, eps
    """
    df_start = (pd.Timestamp(start) - pd.DateOffset(years=2)).strftime('%Y-%m-%d')

    inc = pd.read_parquet(CACHE / "financials_income.parquet")
    inc['date'] = pd.to_datetime(inc['date'])
    inc = inc[(inc['date'] >= df_start) & (inc['date'] <= end)]
    inc = inc[inc['type'].isin(['Revenue', 'GrossProfit', 'OperatingIncome',
                                 'NetIncome', 'IncomeAfterTaxes', 'EPS', 'InterestExpense'])]

    bal = pd.read_parquet(CACHE / "financials_balance.parquet")
    bal['date'] = pd.to_datetime(bal['date'])
    bal = bal[(bal['date'] >= df_start) & (bal['date'] <= end)]
    bal = bal[bal['type'].isin(['TotalAssets', 'Equity', 'Liabilities'])]

    cf = pd.read_parquet(CACHE / "financials_cashflow.parquet")
    cf['date'] = pd.to_datetime(cf['date'])
    cf = cf[(cf['date'] >= df_start) & (cf['date'] <= end)]
    cf = cf[cf['type'].isin(['CashFlowsFromOperatingActivities',
                              'AcquisitionOfPropertyPlantAndEquipment',
                              'CashFlowsFromInvestingActivities', 'Depreciation'])]

    # Pivot
    inc_w = inc.pivot_table(index=['stock_id', 'date'], columns='type', values='value', aggfunc='first').reset_index()
    bal_w = bal.pivot_table(index=['stock_id', 'date'], columns='type', values='value', aggfunc='first').reset_index()
    cf_w = cf.pivot_table(index=['stock_id', 'date'], columns='type', values='value', aggfunc='first').reset_index()

    fin = inc_w.merge(bal_w, on=['stock_id', 'date'], how='outer')
    fin = fin.merge(cf_w, on=['stock_id', 'date'], how='outer')
    # NetIncome fallback to IncomeAfterTaxes
    if 'NetIncome' not in fin.columns:
        fin['NetIncome'] = fin.get('IncomeAfterTaxes', pd.NA)
    fin['NetIncome'] = fin['NetIncome'].fillna(fin.get('IncomeAfterTaxes', pd.NA))

    # 4Q rolling sum for income items (TTM)
    fin = fin.sort_values(['stock_id', 'date'])
    for col in ['Revenue', 'GrossProfit', 'OperatingIncome', 'NetIncome',
                'CashFlowsFromOperatingActivities', 'AcquisitionOfPropertyPlantAndEquipment',
                'InterestExpense']:
        if col in fin.columns:
            fin[f'{col}_ttm'] = fin.groupby('stock_id')[col].transform(lambda s: s.rolling(4, min_periods=2).sum())

    # Ratios
    fin['roe'] = fin.get('NetIncome_ttm', pd.NA) / fin.get('Equity', pd.NA).replace(0, np.nan)
    fin['roa'] = fin.get('NetIncome_ttm', pd.NA) / fin.get('TotalAssets', pd.NA).replace(0, np.nan)
    fin['gross_margin'] = fin.get('GrossProfit_ttm', pd.NA) / fin.get('Revenue_ttm', pd.NA).replace(0, np.nan)
    fin['op_margin'] = fin.get('OperatingIncome_ttm', pd.NA) / fin.get('Revenue_ttm', pd.NA).replace(0, np.nan)
    fin['debt_ratio'] = fin.get('Liabilities', pd.NA) / fin.get('TotalAssets', pd.NA).replace(0, np.nan)

    # v9 NEW: Cash flow + FCF features
    def _col(name):
        return fin[name] if name in fin.columns else pd.Series(np.nan, index=fin.index)
    cfo_ttm = _col('CashFlowsFromOperatingActivities_ttm')
    capex_ttm = _col('AcquisitionOfPropertyPlantAndEquipment_ttm')
    rev_ttm = _col('Revenue_ttm').replace(0, np.nan)
    ni_ttm = _col('NetIncome_ttm').replace(0, np.nan)
    total_assets = _col('TotalAssets').replace(0, np.nan)
    op_inc_ttm = _col('OperatingIncome_ttm')
    int_exp_ttm = _col('InterestExpense_ttm')

    fin['cfo_to_revenue'] = cfo_ttm / rev_ttm
    # cfo_to_ni — undefined for loss-makers (NI ≤ 0)。
    # 2026-05-16 修：原 cfo_ttm / ni_ttm 在 NI<0 時 sign-flip（23% universe rows），
    # cash-conversion ratio 對虧損公司語義上 undefined → 用 NaN 排除而非 abs() 灌正
    fin['cfo_to_ni'] = np.where(ni_ttm > 0, cfo_ttm / ni_ttm, np.nan)
    fin['fcf_ttm'] = cfo_ttm + capex_ttm  # capex 已是負數 → 加法
    fin['fcf_to_revenue'] = fin['fcf_ttm'] / rev_ttm
    fin['capex_intensity'] = capex_ttm.abs() / total_assets
    fin['interest_coverage'] = op_inc_ttm / int_exp_ttm.abs().replace(0, np.nan)

    # 4Q Δ on margins + new ratios (改善方向 signal)
    for col in ['gross_margin', 'op_margin', 'roe', 'cfo_to_revenue', 'fcf_to_revenue']:
        fin[f'{col}_4q_delta'] = fin.groupby('stock_id')[col].diff(4)

    # EPS YoY — 用 abs(denominator) 避免「虧轉盈」被算成負值
    # 2026-05-16 修：pct_change(4) 在 EPS_yago < 0 時翻轉符號，turnaround stock 反成最大負分
    # 例：2025-Q4 EPS +0.76 vs 2024-Q4 EPS -0.15 → pct_change=-6.07（誤判）
    #     正確：(0.76 - (-0.15)) / |-0.15| = +6.07 ✅
    if 'EPS' in fin.columns:
        eps_yago = fin.groupby('stock_id')['EPS'].shift(4)
        fin['eps_yoy'] = (fin['EPS'] - eps_yago) / eps_yago.abs().replace(0, np.nan)

    keep_cols = ['stock_id', 'date',
                 'roe', 'roa', 'gross_margin', 'op_margin', 'debt_ratio',
                 'gross_margin_4q_delta', 'op_margin_4q_delta', 'roe_4q_delta', 'eps_yoy',
                 # v9 new
                 'cfo_to_revenue', 'cfo_to_ni', 'fcf_to_revenue', 'capex_intensity',
                 'interest_coverage', 'cfo_to_revenue_4q_delta', 'fcf_to_revenue_4q_delta']
    fin = fin[[c for c in keep_cols if c in fin.columns]]
    log.info("Financials: %d rows, %d sids", len(fin), fin['stock_id'].nunique())
    return fin


def load_revenue(start: str, end: str) -> pd.DataFrame:
    df = pd.read_parquet(CACHE / "revenue_scores_monthly.parquet")
    df['date'] = pd.to_datetime(df['date'])
    # Pull 1 year earlier for Δ computation lookback
    df_start = (pd.Timestamp(start) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
    df = df[(df['date'] >= df_start) & (df['date'] <= end)].copy()
    # Compute rolling Δ per stock
    df = df.sort_values(['stock_id', 'date'])
    df['revenue_score_3m_delta'] = df.groupby('stock_id')['revenue_score'].diff(3)
    df['revenue_score_6m_delta'] = df.groupby('stock_id')['revenue_score'].diff(6)
    log.info("Revenue scores: %d rows, %d sids", len(df), df['stock_id'].nunique())
    return df


# ============================================================
# Stage 1 — Feature engineering (hypothesis features)
# ============================================================

def _per_stock_features(group: pd.DataFrame) -> pd.DataFrame:
    """Compute per-stock features that need rolling windows.

    group has columns: date, Close, High, Low, Volume, avg_tv_60d, rsi_14, rvol_20, low52w_prox
    """
    g = group.sort_values('date').copy()
    close = g['Close']
    high = g['High']
    low = g['Low']
    volume = g['Volume']

    # ---- Snapshot features (v1) ----
    max_high_252 = high.rolling(252, min_periods=126).max()
    g['dist_52w_high'] = 1.0 - close / max_high_252
    ma60 = close.rolling(60, min_periods=30).mean()
    ma240 = close.rolling(240, min_periods=120).mean()
    g['close_to_ma60'] = close / ma60 - 1.0
    g['close_to_ma240'] = close / ma240 - 1.0

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    atr20 = tr.rolling(20, min_periods=10).mean()
    atr60 = tr.rolling(60, min_periods=30).mean()
    g['atr_ratio_20_60'] = atr20 / atr60

    # ---- Δ / dynamic features (v2 expansion) ----
    # Volume compression: 60d avg vol / 252d avg vol (量縮 ratio < 1)
    vol_60 = volume.rolling(60, min_periods=30).mean()
    vol_252 = volume.rolling(252, min_periods=126).mean()
    g['vol_compression_60_252'] = vol_60 / vol_252  # < 1 = 量縮

    # Price stability: std of pct_change 60d / 252d (價穩 ratio < 1)
    ret_1d = close.pct_change()
    std_60 = ret_1d.rolling(60, min_periods=30).std()
    std_252 = ret_1d.rolling(252, min_periods=126).std()
    g['price_stability_60_252'] = std_60 / std_252  # < 1 = 價穩

    # Volume-price divergence: volume z-score over 60d - price z-score over 60d
    # Whale accumulation = high volume z + low price z
    vol_z = (volume - volume.rolling(60, min_periods=30).mean()) / volume.rolling(60, min_periods=30).std()
    price_z = (close - close.rolling(60, min_periods=30).mean()) / close.rolling(60, min_periods=30).std()
    g['vol_price_divergence'] = vol_z - price_z

    # MA60 slope over 20d (trend acceleration; >0 = MA60 turning up = bottoming)
    g['ma60_slope_20d'] = ma60 / ma60.shift(20) - 1.0

    # Body strength: |close-open| / (high-low) over 20d mean = bullish candle strength
    body = (close - close.shift()).abs()
    daily_range = (high - low).clip(lower=0.001)
    body_ratio = body / daily_range
    g['body_strength_20d'] = body_ratio.rolling(20, min_periods=10).mean()

    # Pct days closing in upper half of daily range over 20d (accumulation proxy)
    upper_half = ((close - low) / daily_range) >= 0.5
    g['upper_half_close_20d_pct'] = upper_half.rolling(20, min_periods=10).mean().astype(float)

    # Volume spike but small price move (large block detection proxy)
    # = max(volume / 20d avg) over last 20d when |price change| < 1%
    vol_20 = volume.rolling(20, min_periods=10).mean()
    vol_spike = volume / vol_20
    flat_day = (close.pct_change().abs() < 0.01)
    g['stealth_volume_20d'] = (vol_spike * flat_day.astype(float)).rolling(20, min_periods=10).max()

    # ---- Forward-window targets ----
    fwd_max_120 = close.shift(-1).rolling(120, min_periods=60).max()
    g['fwd_120d_max'] = fwd_max_120 / close - 1.0
    fwd_max_180 = close.shift(-1).rolling(180, min_periods=90).max()
    g['fwd_180d_max'] = fwd_max_180 / close - 1.0
    fwd_max_60 = close.shift(-1).rolling(60, min_periods=30).max()
    g['fwd_60d_max_computed'] = fwd_max_60 / close - 1.0

    fwd_min_60 = close.shift(-1).rolling(60, min_periods=30).min()
    g['path_mae_60d'] = fwd_min_60 / close - 1.0

    return g


def _add_chip_deltas(sm: pd.DataFrame) -> pd.DataFrame:
    """Per-stock 4w / 8w Δ on smart_money chip features."""
    sm = sm.sort_values(['stock_id', 'date']).copy()
    for col in ['foreign_pct', 'total_pct', 'trust_pct', 'dealer_pct']:
        if col in sm.columns:
            sm[f'{col}_4w_delta'] = sm.groupby('stock_id')[col].diff(4)
            sm[f'{col}_8w_delta'] = sm.groupby('stock_id')[col].diff(8)
    for col in ['foreign_net_5d', 'trust_net_5d', 'dealer_net_5d', 'total_net_5d']:
        if col in sm.columns:
            # 4w cumulative net buying (rolling sum over 4 weekly bars)
            sm[f'{col}_4w_sum'] = sm.groupby('stock_id')[col].transform(lambda s: s.rolling(4, min_periods=2).sum())
            # 4w z-score vs trailing 52w
            def _z52(s):
                m = s.rolling(52, min_periods=26).mean()
                sd = s.rolling(52, min_periods=26).std()
                return (s - m) / sd
            sm[f'{col}_52w_z'] = sm.groupby('stock_id')[col].transform(_z52)
    return sm


def add_sector_features(feat: pd.DataFrame, universe_industry: pd.DataFrame) -> pd.DataFrame:
    """Add sector-rotation features:
      - sector_return_60d (industry mean fwd return... wait NO use realized!)
      - stock_rs_in_sector_60d (stock 60d return - sector 60d return)
      - sector_momentum_rank (rank of sector by 60d return across all sectors)
    Note: uses BACKWARD 60d realized return, not forward (to avoid look-ahead).
    """
    feat = feat.merge(universe_industry, on='stock_id', how='left')

    # Compute realized 60d return per stock (close-to-close)
    feat = feat.sort_values(['stock_id', 'date'])
    feat['ret_60d_realized'] = feat.groupby('stock_id')['Close'].transform(lambda s: s.pct_change(60))

    # Sector mean realized 60d return (per date × sector)
    sector_grp = feat.groupby(['date', 'industry_category'])['ret_60d_realized'].mean().reset_index()
    sector_grp = sector_grp.rename(columns={'ret_60d_realized': 'sector_return_60d'})
    feat = feat.merge(sector_grp, on=['date', 'industry_category'], how='left')

    # Stock relative strength in sector (RS)
    feat['stock_rs_in_sector_60d'] = feat['ret_60d_realized'] - feat['sector_return_60d']

    # Sector momentum rank (sector_return_60d rank across all sectors per date)
    feat['sector_momentum_rank'] = feat.groupby('date')['sector_return_60d'].rank(pct=True)

    # 3-month realized sector return for slower trend
    feat['ret_120d_realized'] = feat.groupby('stock_id')['Close'].transform(lambda s: s.pct_change(120))
    sector_grp_120 = feat.groupby(['date', 'industry_category'])['ret_120d_realized'].mean().reset_index()
    sector_grp_120 = sector_grp_120.rename(columns={'ret_120d_realized': 'sector_return_120d'})
    feat = feat.merge(sector_grp_120, on=['date', 'industry_category'], how='left')

    return feat


def build_feature_panel(
    indicators: pd.DataFrame,
    smart_money: pd.DataFrame,
    fwd_returns: pd.DataFrame,
    quality: pd.DataFrame,
    revenue: pd.DataFrame = None,
    financials: pd.DataFrame = None,
    universe_industry: pd.DataFrame = None,
) -> pd.DataFrame:
    """Stage 1+2 — Build feature panel.

    Output: long-format DataFrame with columns:
      stock_id, date, <features>, <targets>
    """
    log.info("Stage 1 — Computing per-stock features (this may take 1-3 min for 2000 sids)...")
    # Manual groupby loop to keep stock_id + date columns reliably
    parts = []
    for sid, grp in indicators.groupby('stock_id', sort=False):
        out = _per_stock_features(grp)
        out['stock_id'] = sid
        parts.append(out)
    feat = pd.concat(parts, ignore_index=True)
    log.info("  done per-stock features, total rows: %d", len(feat))

    # Compute features from raw cols
    feat['turnover_log'] = np.log(feat['avg_tv_60d'].clip(lower=1))
    feat['low52w_prox_adj'] = feat['low52w_prox']  # close / 52w_min, already provided

    # Merge smart_money (weekly, asof match)
    # merge_asof requires both DataFrames sorted by `on` key (date)
    log.info("Stage 2 — Computing chip Δ features + merging smart_money (asof weekly)...")
    sm = smart_money[['stock_id', 'date', 'foreign_pct', 'total_pct', 'trust_pct', 'dealer_pct',
                       'foreign_net_5d', 'trust_net_5d', 'dealer_net_5d', 'total_net_5d',
                       'avg_tv_60d', 'smart_money_score']].copy()
    sm = _add_chip_deltas(sm)
    sm = sm.sort_values('date').reset_index(drop=True)
    feat = feat.sort_values('date').reset_index(drop=True)
    feat = pd.merge_asof(
        feat, sm,
        on='date', by='stock_id',
        direction='backward', tolerance=pd.Timedelta('10 days'),
        suffixes=('', '_sm'),
    )

    # foreign_net_5d normalized by avg_tv_60d
    feat['foreign_net_pressure'] = feat['foreign_net_5d'] / feat['avg_tv_60d'].clip(lower=1)
    feat['trust_net_pressure'] = feat['trust_net_5d'] / feat['avg_tv_60d'].clip(lower=1)
    feat['total_net_pressure'] = feat['total_net_5d'] / feat['avg_tv_60d'].clip(lower=1)

    # Merge quality (quarterly, asof) — PIT-aware: 加 45 天 publication delay
    # 2026-05-16 修：原邏輯 date=quarter_end 直接 backward merge → 3/31 收盤就用了 5/15 才公開
    # 的 Q1 報，整個 fundamental feature 含 45 天 look-ahead leak。FinMind 法定揭露 deadline:
    # Q1 5/15 / Q2 8/14 / Q3 11/14 / Q4 3/31 → 季末 + 45-92 天，保守取 +45d。
    log.info("Stage 2 — Merging quality scores (asof, +45d publication delay)...")
    q = quality[['stock_id', 'date', 'f_score', 'z_score', 'f_score_4q_delta', 'f_score_1q_delta']].copy()
    q['date'] = pd.to_datetime(q['date']) + pd.Timedelta(days=45)
    q = q.sort_values('date').reset_index(drop=True)
    feat = feat.sort_values('date').reset_index(drop=True)
    feat = pd.merge_asof(
        feat, q,
        on='date', by='stock_id',
        direction='backward', tolerance=pd.Timedelta('120 days'),
        suffixes=('', '_q'),
    )

    # Merge revenue (monthly, asof) — PIT-aware: +10 days publication delay
    # 2026-05-22 修：cache date = 公告月初 (e.g., 4/01 代表 3 月營收，法定 4/10 才公告).
    # 沒加 delay → rebal date 落在公告月 day-1~day-10 期間會看到未公開資料 (e.g., M11
    # rebal 在 4/9 週五會 backward asof 到 4/01 March revenue, 但 4/10 才公告 → leak).
    if revenue is not None and not revenue.empty:
        log.info("Stage 2 — Merging revenue scores (asof, +10d publication delay)...")
        rv = revenue[['stock_id', 'date', 'revenue_score',
                      'revenue_score_3m_delta', 'revenue_score_6m_delta']].copy()
        rv['date'] = pd.to_datetime(rv['date']) + pd.Timedelta(days=10)
        rv = rv.sort_values('date').reset_index(drop=True)
        feat = feat.sort_values('date').reset_index(drop=True)
        feat = pd.merge_asof(
            feat, rv,
            on='date', by='stock_id',
            direction='backward', tolerance=pd.Timedelta('45 days'),
            suffixes=('', '_rv'),
        )

    # Merge financials (quarterly, asof) — PIT-aware: 加 45 天 publication delay
    # 2026-05-16 修：詳見上方 quality merge 註解
    if financials is not None and not financials.empty:
        log.info("Stage 2 — Merging financials (asof quarterly, +45d publication delay)...")
        fn = financials.copy()
        fn['date'] = pd.to_datetime(fn['date']) + pd.Timedelta(days=45)
        fn = fn.sort_values('date').reset_index(drop=True)
        feat = feat.sort_values('date').reset_index(drop=True)
        feat = pd.merge_asof(
            feat, fn,
            on='date', by='stock_id',
            direction='backward', tolerance=pd.Timedelta('120 days'),
            suffixes=('', '_fn'),
        )

    # Sector rotation features (need industry + Close column)
    if universe_industry is not None and not universe_industry.empty:
        log.info("Stage 2 — Adding sector rotation features...")
        feat = add_sector_features(feat, universe_industry)

    # Merge fwd_returns (exact join on stock_id+date)
    log.info("Stage 2 — Merging fwd_returns...")
    fr = fwd_returns[['stock_id', 'date', 'fwd_5d', 'fwd_10d', 'fwd_20d', 'fwd_60d',
                       'fwd_120d', 'fwd_60d_max', 'fwd_60d_min']].copy()
    feat = feat.merge(fr, on=['stock_id', 'date'], how='left', suffixes=('', '_fr'))

    log.info("Feature panel: %d rows, %d cols", len(feat), len(feat.columns))
    return feat


# ============================================================
# Stage 3 — Preprocess (winsorize + standardize)
# ============================================================

def winsorize_standardize(df: pd.DataFrame, feature_cols: List[str], lower: float = 0.01, upper: float = 0.99, industry_neutral: bool = False) -> pd.DataFrame:
    """Winsorize each feature within its date cross-section (optionally per industry), then z-standardize."""
    out = df.copy()
    group_keys = ['date', 'industry_category'] if (industry_neutral and 'industry_category' in out.columns) else ['date']
    log.info("Stage 3 — Winsorize [%.2f, %.2f] + standardize by %s...", lower, upper, '+'.join(group_keys))
    for col in feature_cols:
        if col not in out.columns:
            log.warning("  feature missing: %s", col)
            continue
        # Winsorize + z-score
        def _ws_z(s: pd.Series) -> pd.Series:
            s = s.copy()
            valid = s.dropna()
            if len(valid) < (10 if industry_neutral else 30):
                return pd.Series(np.nan, index=s.index)
            lo, hi = valid.quantile([lower, upper])
            s = s.clip(lo, hi)
            m, sd = s.mean(), s.std()
            return (s - m) / sd if sd > 0 else s * 0
        out[col] = out.groupby(group_keys)[col].transform(_ws_z)
    return out


# ============================================================
# Stage 4 — Univariate selection
# ============================================================

@dataclass
class FeatureResult:
    feature: str
    n: int
    ic_60d_car: float
    ic_120d_car: float
    p_k_10_hit_30_60d: float
    p_k_20_hit_30_60d: float
    base_rate_hit_30_60d: float
    path_mae_top10_mean: float
    decile_q1_car_60d: float
    decile_q10_car_60d: float
    decile_spread_60d: float
    decile_monotonicity: float
    p_value_ic_60d: float


def univariate_ic(df: pd.DataFrame, feature: str, target: str) -> Tuple[float, float, int]:
    """Cross-sectional rank IC: average daily Spearman correlation."""
    daily = []
    for date, grp in df.groupby('date'):
        valid = grp[[feature, target]].dropna()
        if len(valid) < 30:
            continue
        rho, _ = stats.spearmanr(valid[feature], valid[target])
        if pd.notna(rho):
            daily.append(rho)
    if not daily:
        return np.nan, np.nan, 0
    ic = np.mean(daily)
    # IC's t-test (one-sample t-test against 0)
    t_stat, p = stats.ttest_1samp(daily, 0.0)
    return ic, p, len(daily)


def decile_analysis(df: pd.DataFrame, feature: str, target: str) -> Tuple[List[float], float]:
    """Cross-sectional decile spread + monotonicity (Spearman of decile means)."""
    decile_means = []
    for date, grp in df.groupby('date'):
        valid = grp[[feature, target]].dropna()
        if len(valid) < 30:
            continue
        try:
            valid['decile'] = pd.qcut(valid[feature], 10, labels=False, duplicates='drop')
            means = valid.groupby('decile', observed=True)[target].mean()
            if len(means) == 10:
                decile_means.append(means.values)
        except (ValueError, TypeError):
            continue
    if not decile_means:
        return [np.nan] * 10, np.nan
    avg_deciles = np.mean(decile_means, axis=0)
    # Monotonicity: Spearman of decile rank vs decile mean
    mono, _ = stats.spearmanr(range(10), avg_deciles)
    return list(avg_deciles), mono


def precision_at_k(df: pd.DataFrame, feature: str, hit_target: str, K: int) -> float:
    """Per date: take top-K by feature, compute hit rate. Average over dates."""
    hit_rates = []
    for date, grp in df.groupby('date'):
        valid = grp[[feature, hit_target]].dropna()
        if len(valid) < K:
            continue
        top = valid.nlargest(K, feature)
        hit_rates.append(top[hit_target].mean())
    return float(np.mean(hit_rates)) if hit_rates else np.nan


def path_mae_top10(df: pd.DataFrame, feature: str) -> float:
    """Mean of path_mae_60d for top-10 by feature each date."""
    if 'path_mae_60d' not in df.columns:
        return np.nan
    rows = []
    for date, grp in df.groupby('date'):
        valid = grp[[feature, 'path_mae_60d']].dropna()
        if len(valid) < 10:
            continue
        top = valid.nlargest(10, feature)
        rows.append(top['path_mae_60d'].mean())
    return float(np.mean(rows)) if rows else np.nan


def stage4_5_evaluate(df: pd.DataFrame, features: List[str]) -> pd.DataFrame:
    """Run stages 4+5 for all features. Returns ranked result table."""
    log.info("Stage 4+5 — Univariate IC + Decile for %d features", len(features))
    results = []
    for f in features:
        log.info("  feature: %s", f)
        # IC vs CAR_60d
        ic_60, p_60, n_60 = univariate_ic(df, f, 'fwd_60d')
        # IC vs CAR_120d
        ic_120, p_120, _ = univariate_ic(df, f, 'fwd_120d')
        # P@K
        p_k_10 = precision_at_k(df, f, 'hit_30_60d', 10)
        p_k_20 = precision_at_k(df, f, 'hit_30_60d', 20)
        # Path MAE
        mae = path_mae_top10(df, f)
        # Decile
        deciles, mono = decile_analysis(df, f, 'fwd_60d')
        results.append(FeatureResult(
            feature=f,
            n=n_60,
            ic_60d_car=ic_60,
            ic_120d_car=ic_120,
            p_k_10_hit_30_60d=p_k_10,
            p_k_20_hit_30_60d=p_k_20,
            base_rate_hit_30_60d=df['hit_30_60d'].mean(),
            path_mae_top10_mean=mae,
            decile_q1_car_60d=deciles[0] if deciles else np.nan,
            decile_q10_car_60d=deciles[9] if deciles else np.nan,
            decile_spread_60d=(deciles[9] - deciles[0]) if deciles else np.nan,
            decile_monotonicity=mono,
            p_value_ic_60d=p_60,
        ))
    return pd.DataFrame([r.__dict__ for r in results])


# ============================================================
# Stage 7 — Walk-forward + cross-regime
# ============================================================

def walk_forward_ic(df: pd.DataFrame, feature: str, target: str,
                     train_months: int = 24, test_months: int = 6,
                     embargo_months: int = 2) -> List[Dict]:
    """Rolling walk-forward IC.

    For each test window, compute IC over the test period.
    Returns list of dicts: {train_start, train_end, test_start, test_end, ic_test}
    Embargo: skip embargo_months between train end and test start to prevent leak.
    """
    df = df.sort_values('date')
    min_d = df['date'].min()
    max_d = df['date'].max()
    results = []
    current_test_start = min_d + pd.DateOffset(months=train_months + embargo_months)
    while current_test_start + pd.DateOffset(months=test_months) <= max_d:
        train_start = current_test_start - pd.DateOffset(months=train_months + embargo_months)
        train_end = current_test_start - pd.DateOffset(months=embargo_months)
        test_end = current_test_start + pd.DateOffset(months=test_months)
        test_df = df[(df['date'] >= current_test_start) & (df['date'] < test_end)]
        if len(test_df) == 0:
            current_test_start += pd.DateOffset(months=test_months)
            continue
        ic_test, _, _ = univariate_ic(test_df, feature, target)
        results.append({
            'train_start': train_start.date(),
            'train_end': train_end.date(),
            'test_start': current_test_start.date(),
            'test_end': test_end.date(),
            'ic_test': ic_test,
            'n_test_rows': len(test_df),
        })
        current_test_start += pd.DateOffset(months=test_months)
    return results


def leave_one_year_out_ic(df: pd.DataFrame, feature: str, target: str) -> Dict[int, float]:
    """LOOY: for each year, compute IC after excluding that year."""
    df = df.copy()
    df['year'] = df['date'].dt.year
    years = sorted(df['year'].unique())
    results = {}
    for excluded_year in years:
        sub = df[df['year'] != excluded_year]
        ic, _, _ = univariate_ic(sub, feature, target)
        results[excluded_year] = ic
    return results


def cross_regime_ic(df: pd.DataFrame, feature: str, target: str, regimes: Dict[int, str]) -> Dict[str, float]:
    """Split by year-regime mapping and compute IC per regime."""
    df = df.copy()
    df['year'] = df['date'].dt.year
    df['regime'] = df['year'].map(regimes)
    results = {}
    for regime in df['regime'].dropna().unique():
        sub = df[df['regime'] == regime]
        ic, _, _ = univariate_ic(sub, feature, target)
        results[regime] = ic
    return results


# ============================================================
# Stage 8 — Portfolio simulator
# ============================================================

def build_walkforward_composite(
    feat: pd.DataFrame,
    candidate_features: List[str],
    train_months: int = 18,
    embargo_months: int = 2,
    target: str = 'fwd_60d',
    min_abs_ic: float = 0.03,
) -> pd.Series:
    """Compute walk-forward composite_wf_score for each row.

    For each date t, use rolling train_months trailing window (ending embargo_months before t)
    to compute univariate IC per feature. Use signed IC as weights. Apply to current cross-section.

    Returns a Series indexed same as feat with composite_wf_score (NaN before warmup).
    """
    df = feat.sort_values('date').copy()
    df = df.reset_index(drop=True)
    dates_sorted = pd.DatetimeIndex(sorted(df['date'].unique()))
    composite = pd.Series(np.nan, index=df.index, dtype=float)
    warmup = pd.DateOffset(months=train_months + embargo_months)

    # Pre-group by date for fast iteration
    date_to_indices = df.groupby('date').indices

    for cur_date in dates_sorted:
        train_end = cur_date - pd.DateOffset(months=embargo_months)
        train_start = train_end - pd.DateOffset(months=train_months)
        if train_start < dates_sorted[0]:
            continue
        train_df = df[(df['date'] >= train_start) & (df['date'] < train_end)]
        if len(train_df) < 1000:
            continue

        # Compute IC per feature on train window
        weights = {}
        for f in candidate_features:
            if f not in train_df.columns:
                continue
            ic, p, _ = univariate_ic(train_df, f, target)
            if pd.isna(ic) or abs(ic) < min_abs_ic:
                continue
            weights[f] = float(np.sign(ic) * abs(ic))
        if len(weights) < 3:
            continue

        # Apply weights to current cross-section
        if cur_date not in date_to_indices:
            continue
        cur_idx = date_to_indices[cur_date]
        cur_slice = df.loc[cur_idx]
        score = pd.Series(0.0, index=cur_idx)
        n_valid = pd.Series(0, index=cur_idx)
        for f, w in weights.items():
            v = cur_slice[f].fillna(0.0)
            score = score + w * v
            n_valid = n_valid + cur_slice[f].notna().astype(int)
        min_n = max(3, len(weights) // 2)
        score = score.where(n_valid >= min_n, np.nan)
        composite.loc[cur_idx] = score.values

    return composite


def portfolio_topk_returns(df: pd.DataFrame, feature: str, K: int, holding_period: str = 'M') -> pd.Series:
    """Simulate top-K equal weight portfolio rebalanced each date (already filtered to rebalance dates).

    Each rebalance date: pick top-K by feature, hold until next rebalance, return = mean fwd return.
    Returns a pd.Series indexed by rebalance date with portfolio return.
    """
    # Map holding period to fwd column
    fwd_col = {'M': 'fwd_20d', 'Q': 'fwd_60d', 'W': 'fwd_5d', '2W': 'fwd_10d',
                '60d': 'fwd_60d', '120d': 'fwd_120d'}.get(holding_period, 'fwd_20d')
    if fwd_col not in df.columns:
        log.warning("No %s in df, falling back to fwd_60d", fwd_col)
        fwd_col = 'fwd_60d'

    period_returns = []
    for date, grp in df.groupby('date'):
        valid = grp[[feature, fwd_col]].dropna()
        if len(valid) < K:
            continue
        top = valid.nlargest(K, feature)
        period_returns.append({'date': date, 'ret': top[fwd_col].mean()})

    if not period_returns:
        return pd.Series(dtype=float)
    out = pd.DataFrame(period_returns).set_index('date')['ret']
    return out


def portfolio_metrics(returns: pd.Series, freq: int = 12) -> Dict[str, float]:
    """Compute Sharpe / CAGR / MDD / volatility from period returns.

    returns: pd.Series of period returns (e.g. monthly).
    freq: annualization factor (12 monthly, 252 daily).
    """
    if len(returns) < 6:
        return {}
    # Convert to equity curve
    equity = (1 + returns).cumprod()
    n_periods = len(returns)
    cagr = equity.iloc[-1] ** (freq / n_periods) - 1.0
    annual_vol = returns.std() * np.sqrt(freq)
    sharpe = (returns.mean() * freq) / annual_vol if annual_vol > 0 else np.nan
    # MDD
    peak = equity.expanding().max()
    drawdown = equity / peak - 1.0
    mdd = drawdown.min()
    return {
        'n_periods': n_periods,
        'total_return': equity.iloc[-1] - 1.0,
        'cagr': cagr,
        'annual_vol': annual_vol,
        'sharpe': sharpe,
        'mdd': mdd,
        'win_rate': (returns > 0).mean(),
    }


def load_twii_baseline(start: str, end: str) -> pd.Series:
    """Load TWII close prices for baseline B&H computation."""
    fpath = CACHE / "_twii_for_audit.parquet"
    if not fpath.exists():
        log.warning("TWII baseline not found at %s", fpath)
        return pd.Series(dtype=float)
    df = pd.read_parquet(fpath)
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] >= start) & (df['date'] <= end)].copy()
    return df.set_index('date')['Close'].sort_index()


def twii_period_returns(twii: pd.Series, rebalance_dates: pd.DatetimeIndex) -> pd.Series:
    """Resample TWII to rebalance period returns."""
    # For each rebalance date, period_return = TWII at next rebalance / TWII at this rebalance - 1
    s = twii.reindex(rebalance_dates, method='ffill')
    return s.pct_change().dropna()


# ============================================================
# Stage 6 — BH-FDR multiple comparison correction
# ============================================================

def benjamini_hochberg(p_values: np.ndarray, alpha: float = 0.10) -> np.ndarray:
    """BH-FDR: return boolean array of rejected nulls (significant)."""
    n = len(p_values)
    sorted_idx = np.argsort(p_values)
    sorted_p = p_values[sorted_idx]
    # critical: i/n * alpha
    critical = np.arange(1, n + 1) / n * alpha
    below = sorted_p <= critical
    if not below.any():
        return np.zeros(n, dtype=bool)
    largest_below = np.max(np.where(below)[0])
    reject = np.zeros(n, dtype=bool)
    reject[sorted_idx[:largest_below + 1]] = True
    return reject


# ============================================================
# Stage 9 — Output report
# ============================================================

def generate_report_v2(
    results: pd.DataFrame,
    stage7_df: pd.DataFrame,
    stage8_df: pd.DataFrame,
    out_dir: Path,
    start: str,
    end: str,
    n_sids: int,
    kill_passed: pd.Series,
    fdr_passed: pd.Series,
    twii_metrics: dict,
) -> str:
    """Extended report with Stage 7 + Stage 8."""
    md = []
    md.append(f"# Whale Picks Phase 2 — IC Backtest Report (v2 with Stage 7+8)")
    md.append(f"\n**Universe**: TW {n_sids} stocks / **Period**: {start} ~ {end} / **Pipeline**: stages 3-8 (full minus extensions)")
    md.append(f"\n**Methodology**: per docs/whale_picks_spec.md v0.2 council verdict")

    md.append("\n## Stage 4+5+6 — Univariate selection + Decile kill + FDR\n")
    md.append("| Feature | N | IC_60d | IC_120d | p_value | FDR ✓ | P@10 | P@20 | base | Q10-Q1 | Mono | Kill ✓ |")
    md.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in results.iterrows():
        md.append(f"| {r['feature']} | {r['n']} | {r['ic_60d_car']:.4f} | {r['ic_120d_car']:.4f} | {r['p_value_ic_60d']:.4f} | {'✓' if fdr_passed.iloc[i] else '✗'} | {r['p_k_10_hit_30_60d']:.3f} | {r['p_k_20_hit_30_60d']:.3f} | {r['base_rate_hit_30_60d']:.3f} | {r['decile_spread_60d']:.4f} | {r['decile_monotonicity']:.3f} | {'✓' if kill_passed.iloc[i] else '✗'} |")

    md.append("\n## Stage 7 — Walk-forward + LOOY + Cross-regime (top features)\n")
    md.append("| Feature | WF IC mean | WF pos% | N wins | LOOY min | LOOY max | LOOY range | Bull IC | Bear IC | Sideways IC |")
    md.append("|---|---|---|---|---|---|---|---|---|---|")
    for _, r in stage7_df.iterrows():
        md.append(f"| {r['feature']} | {r['wf_ic_mean']:.4f} | {r['wf_ic_pos_pct']:.2f} | {r['wf_n_windows']} | {r['looy_min_ic']:.4f} | {r['looy_max_ic']:.4f} | {r['looy_range']:.4f} | {r['regime_bull_ic']:.4f} | {r['regime_bear_ic']:.4f} | {r['regime_sideways_ic']:.4f} |")

    md.append("\n## Stage 8 — Portfolio simulator (top-K equal weight monthly, fwd_20d hold)\n")
    if twii_metrics:
        md.append(f"\n**B&H TWII baseline**: CAGR {twii_metrics.get('cagr', np.nan):.2%} / Sharpe {twii_metrics.get('sharpe', np.nan):.2f} / MDD {twii_metrics.get('mdd', np.nan):.2%}")
    md.append("\n| Strategy | N periods | Total ret | CAGR | Vol | Sharpe | MDD | Win rate |")
    md.append("|---|---|---|---|---|---|---|---|")
    for _, r in stage8_df.iterrows():
        if pd.isna(r.get('n_periods', np.nan)):
            continue
        md.append(f"| {r['strategy']} | {int(r['n_periods'])} | {r.get('total_return', np.nan):.2%} | {r.get('cagr', np.nan):.2%} | {r.get('annual_vol', np.nan):.2%} | {r.get('sharpe', np.nan):.2f} | {r.get('mdd', np.nan):.2%} | {r.get('win_rate', np.nan):.2%} |")

    md.append("\n## Kill criteria recap")
    md.append("- Decile monotonicity |Spearman| ≥ 0.5 ✓")
    md.append("- Decile spread (Q10-Q1) sign matches IC sign ✓")
    md.append("- |IC_60d| ≥ 0.03")
    md.append("- BH-FDR alpha=0.10 ✓")

    md.append("\n## Final Verdict")
    passed_kill_fdr = (kill_passed & fdr_passed).sum()
    # SOP-10 hard gate: portfolio P&L must beat B&H
    bh_cagr = twii_metrics.get('cagr', 0)
    promoted_strategies = stage8_df[
        (stage8_df['strategy'] != 'B&H TWII')
        & (stage8_df['cagr'].fillna(-99) > bh_cagr)
        & (stage8_df['sharpe'].fillna(-99) > 0.3)
    ]
    md.append(f"\n- Univariate kill + FDR passed: **{passed_kill_fdr}/{len(results)} features**")
    md.append(f"- Stage 8 portfolio sim beat B&H (CAGR > {bh_cagr:.2%}) AND Sharpe > 0.3: **{len(promoted_strategies)}/{len(stage8_df)-1} strategies**")
    if len(promoted_strategies) > 0:
        md.append(f"\n**Phase 2 Verdict: PROMISING** — {len(promoted_strategies)} strategies pass SOP-10 portfolio gate")
        md.append("\nWinners:")
        for _, r in promoted_strategies.iterrows():
            md.append(f"- {r['strategy']}: CAGR {r['cagr']:.2%} / Sharpe {r['sharpe']:.2f} / MDD {r['mdd']:.2%}")
    else:
        md.append(f"\n**Phase 2 Verdict: D 級 — informational tier only**")
        md.append("- No strategy passes SOP-10 portfolio gate (must beat B&H CAGR AND Sharpe > 0.3)")
        md.append("- Per SPEC §8.8: 心理預期警告 set 對 — 「方法論驗證 negative，回到既有 QM/Value 框架」")

    md.append("\n## Caveats")
    md.append("- Stage 8 uses fwd_20d hold-to-month-end as simple proxy. Realistic exit (trailing stop / take profit) not modeled.")
    md.append("- Survivorship: universe_tw 1972 excludes 下市 stocks → over-states alpha.")
    md.append("- 4/5 years bullish (2021/2023/2024/2025 vs 2022) → cross-regime split essential to detect time-period dependency.")
    md.append("- TDCC 集中度 Δ features absent (deferred to Phase 3 per SPEC §5).")

    report_text = "\n".join(md)
    (out_dir / "report_v2.md").write_text(report_text, encoding='utf-8')
    log.info("Report v2 saved: %s", out_dir / "report_v2.md")
    return report_text


def generate_report(results: pd.DataFrame, out_dir: Path, start: str, end: str, n_sids: int, kill_passed: pd.Series, fdr_passed: pd.Series) -> str:
    """Markdown report."""
    md = []
    md.append(f"# Whale Picks Phase 2 — IC Backtest Report")
    md.append(f"\n**Universe**: TW {n_sids} stocks / **Period**: {start} ~ {end} / **Pipeline**: stages 3-6+9 (MVP)")
    md.append(f"\n**Methodology**: per docs/whale_picks_spec.md v0.2 council verdict")
    md.append("\n## Stage 4+5+6 Combined Verdict\n")
    md.append("| Feature | N | IC_60d | IC_120d | p_value | FDR ✓ | P@10 | P@20 | base | Q10-Q1 | Mono | Kill ✓ |")
    md.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in results.iterrows():
        md.append(f"| {r['feature']} | {r['n']} | {r['ic_60d_car']:.4f} | {r['ic_120d_car']:.4f} | {r['p_value_ic_60d']:.4f} | {'✓' if fdr_passed.iloc[i] else '✗'} | {r['p_k_10_hit_30_60d']:.3f} | {r['p_k_20_hit_30_60d']:.3f} | {r['base_rate_hit_30_60d']:.3f} | {r['decile_spread_60d']:.4f} | {r['decile_monotonicity']:.3f} | {'✓' if kill_passed.iloc[i] else '✗'} |")

    md.append("\n## Kill criteria")
    md.append("- Decile monotonicity |Spearman| ≥ 0.5 ✓")
    md.append("- Decile spread (Q10-Q1) sign matches IC sign ✓")
    md.append("- |IC_60d| ≥ 0.03")
    md.append("- p_value ≤ 0.10 AND BH-FDR alpha=0.10 ✓")

    md.append("\n## Verdict")
    passed = (kill_passed & fdr_passed).sum()
    md.append(f"\n**{passed}/{len(results)} features pass full pipeline (kill + FDR)**")
    if passed == 0:
        md.append("\n**Phase 2 MVP Verdict: D 級** — No features survive kill + FDR. Per SPEC §8.8 心理預期警告，'這輪 council 產出的可能是「方法論驗證 negative」'.")
    elif passed < 3:
        md.append(f"\n**Phase 2 MVP Verdict: PARTIAL** — {passed} features marginally significant. Need Stage 7 walk-forward + Stage 8 portfolio sim before promotion.")
    else:
        md.append(f"\n**Phase 2 MVP Verdict: PROMISING** — {passed} features pass. Proceed to Stage 7 walk-forward + Stage 8 portfolio sim.")

    md.append("\n## Top features by P@10 (user intent: 下週看哪幾檔)")
    top_pk = results.nlargest(5, 'p_k_10_hit_30_60d')[['feature', 'p_k_10_hit_30_60d', 'base_rate_hit_30_60d', 'ic_60d_car']]
    md.append("\n| Feature | P@10 | Base | IC_60d | Lift |")
    md.append("|---|---|---|---|---|")
    for _, r in top_pk.iterrows():
        lift = (r['p_k_10_hit_30_60d'] / r['base_rate_hit_30_60d']) if r['base_rate_hit_30_60d'] > 0 else np.nan
        md.append(f"| {r['feature']} | {r['p_k_10_hit_30_60d']:.3f} | {r['base_rate_hit_30_60d']:.3f} | {r['ic_60d_car']:.4f} | {lift:.2f}x |")

    md.append("\n## Caveats")
    md.append("- Stage 7 walk-forward not run (MVP scope). v0.2 SPEC requires this before promotion.")
    md.append("- Stage 8 portfolio simulator not run. Per SOP-10 必須 portfolio P&L > B&H baseline 才升 informational tier validated.")
    md.append("- Survivorship: features may show alpha because survivors are over-represented in universe_tw. Should verify with delisted-included universe.")
    md.append("- Sample skew: 2021-2025 includes 2022 bear but 4/5 years bullish. Cross-regime split + 2015-2020 backfill needed (SOP-13).")

    report_text = "\n".join(md)
    (out_dir / "report.md").write_text(report_text, encoding='utf-8')
    log.info("Report saved: %s", out_dir / "report.md")
    return report_text


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Whale Picks Phase 2 — IC backtest pipeline")
    parser.add_argument('--start', default='2021-01-01')
    parser.add_argument('--end', default='2025-12-31')
    parser.add_argument('--output-dir', default='reports/whale_picks_phase2_v1')
    parser.add_argument('--rebalance-freq', default='M', help='Rebalance frequency: D / W / 2W (bi-weekly) / M / Q')
    parser.add_argument('--smoke', action='store_true', help='Smoke mode: limit to 50 sids')
    parser.add_argument('--industry-neutral', action='store_true', help='Stage 3 standardize by date+industry')
    parser.add_argument('--k-grid', action='store_true', help='Stage 8 iterate K=[5,10,15,20,30,50] for composite_parsi')
    parser.add_argument('--liquidity-filter', action='store_true', help='Apply avg_tv_60d >= 100M TWD filter (matches production)')
    parser.add_argument('--min-avg-tv', type=float, default=1e8, help='Min avg_tv_60d threshold (default 100M TWD)')
    args = parser.parse_args()

    out_dir = REPO / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load all panels
    indicators = load_indicators(args.start, args.end)
    fwd_returns = load_fwd_returns(args.start, args.end)
    smart_money = load_smart_money(args.start, args.end)
    quality = load_quality(args.start, args.end)
    revenue = load_revenue(args.start, args.end)
    financials = load_financials_panel(args.start, args.end)
    universe_industry = load_universe_industry()

    if args.smoke:
        sids = sorted(indicators['stock_id'].unique())[:50]
        indicators = indicators[indicators['stock_id'].isin(sids)]
        fwd_returns = fwd_returns[fwd_returns['stock_id'].isin(sids)]
        smart_money = smart_money[smart_money['stock_id'].isin(sids)]
        quality = quality[quality['stock_id'].isin(sids)]
        revenue = revenue[revenue['stock_id'].isin(sids)]
        financials = financials[financials['stock_id'].isin(sids)]
        log.info("Smoke mode — restricted to %d sids", len(sids))

    # Stage 1+2 — feature panel
    feat = build_feature_panel(indicators, smart_money, fwd_returns, quality,
                                revenue, financials, universe_industry)

    # Build hit event: +30% within 60d (using fwd_60d_max if present, else computed)
    feat['hit_30_60d'] = (feat['fwd_60d_max'] >= 0.30).astype(float)
    feat.loc[feat['fwd_60d_max'].isna(), 'hit_30_60d'] = np.nan
    feat['hit_30_120d'] = (feat['fwd_120d_max'] >= 0.30).astype(float)
    feat.loc[feat['fwd_120d_max'].isna(), 'hit_30_120d'] = np.nan

    # Rebalance dates: filter to chosen frequency
    # 2026-05-16 加 '2W' (bi-weekly): 用日序對 14 取整 → 每 14 個交易日換一次
    # 2026-05-22 加 M15/M11/MIXED: rebal timing 實驗 (sell-the-news vs early-data)
    if args.rebalance_freq == 'M':
        feat['_period'] = feat['date'].dt.to_period('M')
    elif args.rebalance_freq == 'Q':
        feat['_period'] = feat['date'].dt.to_period('Q')
    elif args.rebalance_freq == 'W':
        feat['_period'] = feat['date'].dt.to_period('W')
    elif args.rebalance_freq == '2W':
        # Bi-weekly: 用每年第 N 週除以 2 取整作為 period
        weeks = feat['date'].dt.isocalendar().week
        years = feat['date'].dt.isocalendar().year
        feat['_period'] = years.astype(str) + '-' + (weeks // 2).astype(str).str.zfill(2)
    elif args.rebalance_freq == 'M15':
        # Mid-15 each month: keep last trading row on or before day 15
        feat = feat[feat['date'].dt.day <= 15].copy()
        feat['_period'] = feat['date'].dt.to_period('M')
    elif args.rebalance_freq == 'M11':
        # Day-11 each month (revenue-disclosure aware, 上月營收 10 號公告)
        feat = feat[feat['date'].dt.day <= 11].copy()
        feat['_period'] = feat['date'].dt.to_period('M')
    elif args.rebalance_freq == 'MIXED':
        # 季報月 (3/5/8/11) → month-end (avoid sell-the-news)
        # 非季報月 → mid-15 (capture monthly revenue freshness)
        quarter_months = {3, 5, 8, 11}
        is_q = feat['date'].dt.month.isin(quarter_months)
        keep_mask = is_q | (~is_q & (feat['date'].dt.day <= 15))
        feat = feat[keep_mask].copy()
        feat['_period'] = feat['date'].dt.to_period('M')
    else:
        feat['_period'] = feat['date']
    feat = feat.sort_values(['stock_id', 'date'])
    feat = feat.groupby(['stock_id', '_period']).tail(1).drop(columns=['_period']).reset_index(drop=True)
    log.info("After %s rebalance filter: %d rows", args.rebalance_freq, len(feat))

    # v13: liquidity filter (apply BEFORE standardization to exclude noise sids)
    if args.liquidity_filter and 'avg_tv_60d' in feat.columns:
        before_n = len(feat)
        feat = feat[feat['avg_tv_60d'] >= args.min_avg_tv].copy()
        log.info("Liquidity filter (avg_tv_60d >= %.0fM TWD): %d -> %d rows (-%.1f%%)",
                 args.min_avg_tv / 1e6, before_n, len(feat),
                 100 * (before_n - len(feat)) / before_n)

    # Feature list (v2 — expanded with Δ features)
    FEATURES = [
        # ---- v1 snapshot features ----
        'low52w_prox_adj',     # close / 52w_min - 1
        'dist_52w_high',       # 1 - close / 52w_max
        'rsi_14',
        'rvol_20',
        'turnover_log',        # log(avg_tv_60d) — size proxy
        'close_to_ma60',
        'close_to_ma240',
        'atr_ratio_20_60',
        'foreign_pct',
        'total_pct',
        'foreign_net_pressure',
        'trust_net_pressure',
        'total_net_pressure',
        'f_score',
        'z_score',
        # ---- v2 Δ / dynamic features ----
        # Chip Δ — 主力動態 fingerprint
        'foreign_pct_4w_delta',
        'foreign_pct_8w_delta',
        'total_pct_4w_delta',
        'total_pct_8w_delta',
        'trust_pct_4w_delta',
        'foreign_net_5d_4w_sum',
        'foreign_net_5d_52w_z',
        'trust_net_5d_4w_sum',
        'trust_net_5d_52w_z',
        # Price/Volume dynamic
        'vol_compression_60_252',  # 量縮 ratio (< 1 better)
        'price_stability_60_252',  # 價穩 ratio
        'vol_price_divergence',    # vol_z - price_z (whale accumulation)
        'ma60_slope_20d',
        'body_strength_20d',
        'upper_half_close_20d_pct',
        'stealth_volume_20d',
        # Revenue Δ
        'revenue_score',
        'revenue_score_3m_delta',
        'revenue_score_6m_delta',
        # ---- v7 fundamental details ----
        'roe',
        'roa',
        'gross_margin',
        'op_margin',
        'debt_ratio',
        'gross_margin_4q_delta',
        'op_margin_4q_delta',
        'roe_4q_delta',
        'eps_yoy',
        # ---- v7 sector rotation ----
        'sector_return_60d',
        'sector_return_120d',
        'stock_rs_in_sector_60d',
        'sector_momentum_rank',
        # ---- v9 cash flow + F-score Δ ----
        'cfo_to_revenue',
        'cfo_to_ni',
        'fcf_to_revenue',
        'capex_intensity',
        'interest_coverage',
        'cfo_to_revenue_4q_delta',
        'fcf_to_revenue_4q_delta',
        'f_score_4q_delta',
        'f_score_1q_delta',
    ]

    # Stage 3 preprocess
    feat = winsorize_standardize(feat, FEATURES, industry_neutral=args.industry_neutral)

    # Filter: keep only rows where targets exist
    feat = feat.dropna(subset=['fwd_60d', 'fwd_120d', 'hit_30_60d']).reset_index(drop=True)
    log.info("After target filter: %d rows", len(feat))

    # Stage 4+5
    results = stage4_5_evaluate(feat, FEATURES)
    results.to_csv(out_dir / "univariate_results.csv", index=False)

    # Stage 5 kill test
    kill_passed = (
        (results['decile_monotonicity'].abs() >= 0.5)
        & (np.sign(results['decile_spread_60d']) == np.sign(results['ic_60d_car']))
        & (results['ic_60d_car'].abs() >= 0.03)
    )
    # Stage 6 FDR
    fdr_passed = pd.Series(benjamini_hochberg(results['p_value_ic_60d'].fillna(1.0).values, alpha=0.10), index=results.index)

    # ---- Parsimonious fixed-weight composite (v5 iteration — no look-ahead, theory-based) ----
    log.info("Building parsimonious fixed composite (5 features, equal signed weights)...")
    PARSIMONIOUS_SPEC = {
        'f_score': +1.0,                   # quality
        'turnover_log': -1.0,              # 小型股優勢
        'dist_52w_high': -1.0,             # 距高近=動能 (距遠扣分)
        'stealth_volume_20d': +1.0,        # 量縮中爆量大單 (whale accumulation proxy)
        'revenue_score_6m_delta': +1.0,    # 營收改善方向
        'eps_yoy': +1.0,                   # v7 新 strong signal IC +0.082 p<1e-17
        'f_score_4q_delta': +1.0,          # v9 新 F-Score YoY 改善 IC +0.061
        'capex_intensity': -1.0,           # v9 新 Capex 重 = 資本黑洞 IC -0.041
    }
    avail = [f for f in PARSIMONIOUS_SPEC.keys() if f in feat.columns]
    log.info("  parsimonious features: %s", avail)
    feat['composite_parsi'] = 0.0
    n_valid = pd.Series(0, index=feat.index)
    for f in avail:
        w = PARSIMONIOUS_SPEC[f]
        v = feat[f].fillna(0.0)
        feat['composite_parsi'] = feat['composite_parsi'] + w * v
        n_valid = n_valid + feat[f].notna().astype(int)
    feat.loc[n_valid < max(3, len(avail) // 2), 'composite_parsi'] = np.nan
    log.info("  composite_parsi coverage: %d / %d rows",
             feat['composite_parsi'].notna().sum(), len(feat))

    # ---- Walk-forward composite (v4 iteration — honest OOS) ----
    log.info("Building walk-forward composite (rolling 18mo train weights)...")
    # Candidate pool: features with |IC| >= 0.025 (broader than 0.03 threshold to allow exploration)
    candidate_pool = results[results['ic_60d_car'].abs() >= 0.025]['feature'].tolist()
    log.info("  candidate pool: %d features", len(candidate_pool))
    feat['composite_wf_score'] = build_walkforward_composite(
        feat, candidate_pool, train_months=18, embargo_months=2,
        target='fwd_60d', min_abs_ic=0.03
    )
    log.info("  composite_wf_score coverage: %d / %d rows",
             feat['composite_wf_score'].notna().sum(), len(feat))

    # ---- Composite scoring (v3 iteration) ----
    # Build composite from top features that PASSED at least kill OR FDR criteria
    log.info("Building composite score from top features...")
    # Take features with significant IC (p < 0.01) and |IC| >= 0.03
    sig = results[(results['p_value_ic_60d'] < 0.01) & (results['ic_60d_car'].abs() >= 0.03)].copy()
    if len(sig) >= 3:
        # Composite = signed-sum normalized by |IC|
        weights = {row['feature']: float(np.sign(row['ic_60d_car']) * row['ic_60d_car'].__abs__()) for _, row in sig.iterrows()}
        log.info("  composite weights: %s", {k: f'{v:+.3f}' for k, v in weights.items()})
        # Build composite_score in feat
        feat['composite_score'] = 0.0
        feat['_composite_n'] = 0
        for f, w in weights.items():
            if f in feat.columns:
                # Already standardized in stage 3 (mean 0 std 1 per date), so weighted sum is valid
                v = feat[f].fillna(0.0)
                feat['composite_score'] = feat['composite_score'] + w * v
                feat['_composite_n'] = feat['_composite_n'] + feat[f].notna().astype(int)
        # Require at least half of features non-null per row
        min_n = max(3, len(weights) // 2)
        feat.loc[feat['_composite_n'] < min_n, 'composite_score'] = np.nan
        feat = feat.drop(columns=['_composite_n'])
        log.info("  composite_score coverage: %d / %d rows", feat['composite_score'].notna().sum(), len(feat))

        # Evaluate IS composite + walk-forward composite + parsimonious
        composite_results = stage4_5_evaluate(feat, ['composite_score', 'composite_wf_score', 'composite_parsi'])
        results = pd.concat([results, composite_results], ignore_index=True)
        results.to_csv(out_dir / "univariate_results.csv", index=False)
        # Re-compute kill + FDR with composite included
        kill_passed = (
            (results['decile_monotonicity'].abs() >= 0.5)
            & (np.sign(results['decile_spread_60d']) == np.sign(results['ic_60d_car']))
            & (results['ic_60d_car'].abs() >= 0.03)
        )
        fdr_passed = pd.Series(benjamini_hochberg(results['p_value_ic_60d'].fillna(1.0).values, alpha=0.10), index=results.index)

    # Stage 7 — walk-forward + cross-regime for top features by |IC| or P@10
    log.info("Stage 7 — Walk-forward + cross-regime on top features")
    # Pick top features by |IC| and top P@10 (union of top 4 each) + composite if exists
    top_ic = results.reindex(results['ic_60d_car'].abs().sort_values(ascending=False).index).head(4)['feature'].tolist()
    top_pk = results.nlargest(4, 'p_k_10_hit_30_60d')['feature'].tolist()
    extra = []
    if 'composite_score' in feat.columns:
        extra.append('composite_score')
    if 'composite_wf_score' in feat.columns:
        extra.append('composite_wf_score')
    if 'composite_parsi' in feat.columns:
        extra.append('composite_parsi')
    stage7_features = list(dict.fromkeys(top_ic + top_pk + extra))
    log.info("  features: %s", stage7_features)

    REGIMES = {2021: 'bull', 2022: 'bear', 2023: 'sideways', 2024: 'bull', 2025: 'sideways'}

    stage7_rows = []
    for f in stage7_features:
        log.info("  walk-forward: %s", f)
        wf = walk_forward_ic(feat, f, 'fwd_60d', train_months=18, test_months=6, embargo_months=2)
        looy = leave_one_year_out_ic(feat, f, 'fwd_60d')
        cr = cross_regime_ic(feat, f, 'fwd_60d', REGIMES)
        wf_ic_mean = np.mean([w['ic_test'] for w in wf if pd.notna(w['ic_test'])])
        wf_ic_pos_pct = float(np.mean([w['ic_test'] > 0 for w in wf if pd.notna(w['ic_test'])])) if wf else np.nan
        looy_min = min(looy.values()) if looy else np.nan
        looy_max = max(looy.values()) if looy else np.nan
        stage7_rows.append({
            'feature': f,
            'wf_ic_mean': wf_ic_mean,
            'wf_ic_pos_pct': wf_ic_pos_pct,
            'wf_n_windows': len(wf),
            'looy_min_ic': looy_min,
            'looy_max_ic': looy_max,
            'looy_range': looy_max - looy_min if pd.notna(looy_max) else np.nan,
            'regime_bull_ic': cr.get('bull', np.nan),
            'regime_bear_ic': cr.get('bear', np.nan),
            'regime_sideways_ic': cr.get('sideways', np.nan),
        })
    stage7_df = pd.DataFrame(stage7_rows)
    stage7_df.to_csv(out_dir / "stage7_walkforward.csv", index=False)

    # Stage 8 — Portfolio simulator
    log.info("Stage 8 — Portfolio simulator")
    twii = load_twii_baseline(args.start, args.end)
    rebalance_dates = sorted(feat['date'].unique())
    # B&H baseline
    twii_rets = twii_period_returns(twii, pd.DatetimeIndex(rebalance_dates)) if not twii.empty else pd.Series(dtype=float)
    twii_metrics = portfolio_metrics(twii_rets, freq=12) if not twii_rets.empty else {}

    stage8_rows = []
    stage8_rows.append({'strategy': 'B&H TWII', **twii_metrics})
    # Annualization factor for current rebalance freq
    freq_factor = {'M': 12, 'Q': 4, 'W': 52, '2W': 26, 'D': 252,
                   'M15': 12, 'M11': 12, 'MIXED': 12}.get(args.rebalance_freq, 12)
    hold_period = 'M' if args.rebalance_freq in ('M15', 'M11', 'MIXED') else args.rebalance_freq
    for K in [10, 20]:
        for f in stage7_features:
            rets = portfolio_topk_returns(feat, f, K, holding_period=hold_period)
            if rets.empty:
                continue
            m = portfolio_metrics(rets, freq=freq_factor)
            stage8_rows.append({
                'strategy': f'top-{K} {f}',
                **m,
            })
    # v11: K-grid for composite_parsi specifically
    if args.k_grid and 'composite_parsi' in feat.columns:
        log.info("Stage 8 K-grid for composite_parsi: K=[5,15,25,30,50]")
        for K in [5, 15, 25, 30, 50]:
            rets = portfolio_topk_returns(feat, 'composite_parsi', K, holding_period=hold_period)
            if rets.empty:
                continue
            m = portfolio_metrics(rets, freq=freq_factor)
            stage8_rows.append({
                'strategy': f'top-{K} composite_parsi (K-grid)',
                **m,
            })
    # 2026-05-16 加：K-grid for composite_score (production primary)
    if args.k_grid and 'composite_score' in feat.columns:
        log.info("Stage 8 K-grid for composite_score: K=[5,15,25,30,50]")
        for K in [5, 15, 25, 30, 50]:
            rets = portfolio_topk_returns(feat, 'composite_score', K, holding_period=hold_period)
            if rets.empty:
                continue
            m = portfolio_metrics(rets, freq=freq_factor)
            stage8_rows.append({
                'strategy': f'top-{K} composite_score (K-grid)',
                **m,
            })
    stage8_df = pd.DataFrame(stage8_rows)
    stage8_df.to_csv(out_dir / "stage8_portfolio.csv", index=False)

    # Stage 9 report (now extended)
    n_sids = feat['stock_id'].nunique()
    generate_report_v2(results, stage7_df, stage8_df, out_dir, args.start, args.end, n_sids, kill_passed, fdr_passed, twii_metrics)
    log.info("Done. Outputs: %s", out_dir)


if __name__ == "__main__":
    main()
