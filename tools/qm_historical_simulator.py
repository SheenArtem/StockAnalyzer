"""
QM Historical Simulator
========================
每週模擬跑一次 QM scan，記錄「哪些股票被選出」及其 forward return / 特徵，
產出 trade_journal_qm_tw_<mode>.parquet 供 VF-6 驗證使用。

用法:
    python tools/qm_historical_simulator.py --mode mixed      # 現行混合版
    python tools/qm_historical_simulator.py --mode pure_right # 純右側（趨勢確認）
    python tools/qm_historical_simulator.py --mode pure_left  # 純左側（週MA支撐接刀）
    python tools/qm_historical_simulator.py --start 2021-01-01 --end 2025-12-31 --debug

三模式定義（VF-6 QM 左右側混合矛盾驗證）：
    mixed:      趨勢確認（trend_score >= 6）AND 接近週 MA 支撐（±5%）
    pure_right: 只要 trend_score >= 6，不看 MA 支撐
    pure_left:  只要接近週 MA 支撐（±5%），不看 trend_score

    進場條件 proxy 說明：
    - "trigger >= 3" 以 trend_score >= 6 近似（live trend_score 0-10，>= 6 對應 Scenario A/強 B）
    - "接近支撐" 以 close 在 min(weekly_ma20, weekly_ma60) ±5% 帶內近似
      （live Scenario B action_plan entry: support * 0.98 ~ support * 1.02，放寬至 ±5% 避免過濾過多）

    每週仍在 top300 universe 中取 top50（按 QM score = F50+B30+T20），
    三模式的差異在於 top50 pool 之後再套用進場條件 gate。

輸出:
    data_cache/backtest/trade_journal_qm_tw_mixed.parquet
    data_cache/backtest/trade_journal_qm_tw_pure_right.parquet
    data_cache/backtest/trade_journal_qm_tw_pure_left.parquet
    data_cache/backtest/trade_journal_qm_tw.parquet  (mixed 的 alias，避免 G1/G2 腳本爆)
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_PATH = DATA_DIR / "trade_journal_qm_tw.parquet"  # legacy alias (mixed mode)

# Mode-specific output paths
OUT_PATHS = {
    'mixed':      DATA_DIR / "trade_journal_qm_tw_mixed.parquet",
    'pure_right': DATA_DIR / "trade_journal_qm_tw_pure_right.parquet",
    'pure_left':  DATA_DIR / "trade_journal_qm_tw_pure_left.parquet",
}

# Proxy thresholds (documented in module docstring)
TREND_SCORE_RIGHT_THRESHOLD = 6   # trend_score >= 6  →  "trigger >= 3" proxy
SUPPORT_BAND_PCT = 0.05           # ±5% around weekly MA  →  "接近支撐" proxy

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("qm_sim")

HORIZONS = [5, 10, 20, 40, 60]
MAX_DRAWDOWN_HORIZONS = [20, 40]   # for fwd_Nd_max / fwd_Nd_min
QM_TOP_N = 50
MIN_AVG_TV = 5e8          # 60d 均成交值門檻 (5 億)
MIN_PRICE = 10.0          # 最低股價


# ================================================================
# Data Loading
# ================================================================

def load_ohlcv():
    """Load OHLCV parquet."""
    logger.info("Loading OHLCV...")
    df = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet")
    df['date'] = pd.to_datetime(df['date'])
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    logger.info("  OHLCV: %d rows, %d stocks", len(df), df['stock_id'].nunique())
    return df


def load_quality_scores():
    """Load precomputed quarterly quality scores."""
    logger.info("Loading quality_scores...")
    qs = pd.read_parquet(DATA_DIR / "quality_scores.parquet")
    qs['date'] = pd.to_datetime(qs['date'])
    logger.info("  quality_scores: %d rows, %d stocks", len(qs), qs['stock_id'].nunique())
    return qs


def load_top300():
    """Load top300 universe (static list used as quarterly approximation)."""
    with open(DATA_DIR / "top300_universe.json") as f:
        raw = json.load(f)
    # Filter out ETF (start with '00'), special shares (non-4-digit / letter suffix),
    # warrants (6-digit), leaving only regular 4-digit non-zero-prefix stock_ids.
    universe = [s for s in raw if s.isdigit() and len(s) == 4 and not s.startswith('0')]
    logger.info("  top300 universe (after ETF/special filter): %d stocks", len(universe))
    return universe


# ================================================================
# Precompute: ATR%, daily MAs, weekly MAs, 52w high proximity
# ================================================================

def precompute_indicators(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    For every stock-date compute:
      - atr_pct (14d)
      - daily_ma20, daily_ma60
      - ma20_slope (5d), ma60_slope (5d), ma120 (for MA alignment check)
      - 52w_high_prox
      - wma20, wma60 (weekly)
      - avg_tv_60d (Close*Volume 60d mean)

    Returns a DataFrame indexed by (stock_id, date).
    """
    logger.info("Precomputing indicators for %d stocks...", ohlcv['stock_id'].nunique())
    results = []
    stocks = ohlcv['stock_id'].unique()
    total = len(stocks)

    for i, sid in enumerate(stocks):
        if (i + 1) % 200 == 0:
            logger.info("  [%d/%d] precompute...", i + 1, total)

        sdf = ohlcv[ohlcv['stock_id'] == sid].sort_values('date').copy()
        if len(sdf) < 20:
            continue

        sdf = sdf.set_index('date')

        # --- ATR 14d ---
        prev_close = sdf['Close'].shift(1)
        tr = pd.concat([
            sdf['High'] - sdf['Low'],
            (sdf['High'] - prev_close).abs(),
            (sdf['Low'] - prev_close).abs(),
        ], axis=1).max(axis=1)
        sdf['atr'] = tr.ewm(com=13, min_periods=5).mean()  # Wilder smoothing
        sdf['atr_pct'] = sdf['atr'] / sdf['Close'] * 100

        # --- Daily MAs ---
        sdf['daily_ma20'] = sdf['Close'].rolling(20, min_periods=10).mean()
        sdf['daily_ma60'] = sdf['Close'].rolling(60, min_periods=30).mean()
        sdf['daily_ma120'] = sdf['Close'].rolling(120, min_periods=60).mean()

        # --- MA slopes (5d % change) ---
        sdf['ma20_slope'] = sdf['daily_ma20'].pct_change(5)
        sdf['ma60_slope'] = sdf['daily_ma60'].pct_change(5)

        # --- 52-week high proximity ---
        sdf['hi52w'] = sdf['High'].rolling(252, min_periods=120).max()
        sdf['hi52w_prox'] = sdf['Close'] / sdf['hi52w']

        # --- 60d avg trading value ---
        sdf['tv'] = sdf['Close'] * sdf['Volume']
        sdf['avg_tv_60d'] = sdf['tv'].rolling(60, min_periods=20).mean()

        # --- Weekly MA20 / MA60 ---
        weekly = sdf[['Close', 'High', 'Low', 'Open', 'Volume']].resample('W-FRI').agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum',
        }).dropna(subset=['Close'])
        weekly['wma20'] = weekly['Close'].rolling(20, min_periods=10).mean()
        weekly['wma60'] = weekly['Close'].rolling(60, min_periods=30).mean()

        # Forward-fill weekly MA back to daily index
        daily_wma = weekly[['wma20', 'wma60']].reindex(sdf.index, method='ffill')
        sdf['weekly_ma20'] = daily_wma['wma20']
        sdf['weekly_ma60'] = daily_wma['wma60']

        sdf = sdf.reset_index()
        sdf['stock_id'] = sid
        cols = ['stock_id', 'date', 'Close', 'Volume',
                'atr_pct', 'daily_ma20', 'daily_ma60', 'daily_ma120',
                'ma20_slope', 'ma60_slope', 'hi52w_prox',
                'avg_tv_60d', 'weekly_ma20', 'weekly_ma60']
        results.append(sdf[cols])

    df_ind = pd.concat(results, ignore_index=True)
    logger.info("  Indicators done: %d rows", len(df_ind))
    return df_ind


# ================================================================
# Precompute: Forward Returns (per stock, one pass)
# ================================================================

def precompute_forward_returns(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    For every stock-date compute fwd_Nd, fwd_Nd_max, fwd_Nd_min.
    Returns DataFrame with [stock_id, date, fwd_5d, fwd_10d, fwd_20d, fwd_40d, fwd_60d,
                             fwd_20d_max, fwd_20d_min, fwd_40d_max, fwd_40d_min].
    """
    logger.info("Computing forward returns...")
    results = []
    stocks = ohlcv['stock_id'].unique()
    total = len(stocks)

    for i, sid in enumerate(stocks):
        if (i + 1) % 200 == 0:
            logger.info("  [%d/%d] forward returns...", i + 1, total)

        sdf = ohlcv[ohlcv['stock_id'] == sid].sort_values('date').copy().reset_index(drop=True)
        close = sdf['Close'].values
        high = sdf['High'].values
        low = sdf['Low'].values
        n = len(close)

        rows = {'stock_id': sdf['stock_id'], 'date': sdf['date']}
        for h in HORIZONS:
            fwd = np.full(n, np.nan)
            for j in range(n - h):
                if close[j] > 0 and not np.isnan(close[j]):
                    fwd[j] = close[j + h] / close[j] - 1
            rows[f'fwd_{h}d'] = fwd

        for h in MAX_DRAWDOWN_HORIZONS:
            fwd_max = np.full(n, np.nan)
            fwd_min = np.full(n, np.nan)
            for j in range(n - h):
                if close[j] > 0 and not np.isnan(close[j]):
                    future_high = np.nanmax(high[j + 1: j + h + 1])
                    future_low = np.nanmin(low[j + 1: j + h + 1])
                    fwd_max[j] = future_high / close[j] - 1
                    fwd_min[j] = future_low / close[j] - 1
            rows[f'fwd_{h}d_max'] = fwd_max
            rows[f'fwd_{h}d_min'] = fwd_min

        results.append(pd.DataFrame(rows))

    df_fwd = pd.concat(results, ignore_index=True)
    logger.info("  Forward returns done: %d rows", len(df_fwd))
    return df_fwd


# ================================================================
# Compute Trend Score (for a single stock on a single date)
# ================================================================

def compute_trend_score(row: pd.Series) -> float:
    """
    Compute trend_score (0-10) from precomputed indicator row.

    Components (sum to 10 max):
      +2  Close > daily_ma20 (price above 20d MA)
      +2  daily_ma20 > daily_ma60 (20d > 60d)
      +1  daily_ma60 > daily_ma120 (60d > 120d, full bull stack)
      +2  weekly Close > weekly_ma20 (price above weekly MA20)
      +1  weekly_ma20 > weekly_ma60
      +1  52w high proximity > 80%
      +1  ma20_slope > 0 AND ma60_slope > 0 (both MAs rising)

    Score is normalized to 0-10.
    """
    score = 0.0

    c = row['Close']
    ma20 = row['daily_ma20']
    ma60 = row['daily_ma60']
    ma120 = row['daily_ma120']
    wma20 = row['weekly_ma20']
    wma60 = row['weekly_ma60']
    hi52w_prox = row['hi52w_prox']
    ma20_slope = row['ma20_slope']
    ma60_slope = row['ma60_slope']

    if pd.notna(ma20) and pd.notna(c):
        if c > ma20:
            score += 2.0

    if pd.notna(ma20) and pd.notna(ma60):
        if ma20 > ma60:
            score += 2.0

    if pd.notna(ma60) and pd.notna(ma120):
        if ma60 > ma120:
            score += 1.0

    if pd.notna(wma20) and pd.notna(c):
        if c > wma20:
            score += 2.0

    if pd.notna(wma20) and pd.notna(wma60):
        if wma20 > wma60:
            score += 1.0

    if pd.notna(hi52w_prox):
        if hi52w_prox > 0.80:
            score += 1.0

    if pd.notna(ma20_slope) and pd.notna(ma60_slope):
        if ma20_slope > 0 and ma60_slope > 0:
            score += 1.0

    return round(score, 2)


# ================================================================
# Regime: Rule-based (TAIEX proxy via equal-weight top300)
# ================================================================

def build_regime_series(ohlcv: pd.DataFrame, universe: list) -> pd.Series:
    """
    Build daily regime series using equal-weight top300 as TAIEX proxy.

    Rules:
      - trending:  20d return > 5%
      - volatile:  20d high-low range / avg_price > 8%
      - ranging:   otherwise with 60d Sharpe-like < 1.5 or abs(20d ret) < 2%
      - neutral:   default

    Returns pd.Series indexed by date, values in {trending, volatile, ranging, neutral}.
    """
    logger.info("Building regime series...")
    univ_set = set(universe)
    proxy = ohlcv[ohlcv['stock_id'].isin(univ_set)].copy()

    # Equal-weight daily close index
    daily_avg = proxy.groupby('date')['Close'].mean().sort_index()

    # 20d return
    ret20 = daily_avg.pct_change(20)
    # 20d range: (max - min) / mean of last 20 closes
    rolling_max = daily_avg.rolling(20, min_periods=10).max()
    rolling_min = daily_avg.rolling(20, min_periods=10).min()
    rolling_avg = daily_avg.rolling(20, min_periods=10).mean()
    range20 = (rolling_max - rolling_min) / rolling_avg.replace(0, np.nan)

    # 60d Sharpe-like: mean daily return / std daily return * sqrt(60)
    daily_ret = daily_avg.pct_change()
    sharpe60 = (
        daily_ret.rolling(60, min_periods=30).mean() /
        daily_ret.rolling(60, min_periods=30).std().replace(0, np.nan) *
        np.sqrt(60)
    )

    regime = pd.Series('neutral', index=daily_avg.index)
    regime[ret20 > 0.05] = 'trending'
    regime[range20 > 0.08] = 'volatile'
    # Override trending only if strongly so; volatile can override trending
    regime[(ret20 > 0.05) & (range20 <= 0.08)] = 'trending'
    regime[(range20 > 0.08)] = 'volatile'
    regime[(ret20.abs() < 0.02) & (range20 <= 0.08)] = 'ranging'

    logger.info("  Regime distribution:\n%s", regime.value_counts().to_string())
    return regime


# ================================================================
# Market Cap Tier
# ================================================================

def build_market_cap_tier(ohlcv: pd.DataFrame, universe: list) -> pd.DataFrame:
    """
    Approximate market cap tier as of each scan date using Close * Volume proxy.
    Since actual shares outstanding data isn't available, use trailing 60d avg trading value
    as a liquidity proxy for ranking (large / mid / small within universe).

    Returns DataFrame [stock_id, date, market_cap_tier].
    """
    # Use avg_tv_60d as proxy; tier = top 30% = large, 30-70% = mid, bottom 30% = small
    # This is computed per scan date but we compute it globally here
    # (actual market cap rank is stable enough within a quarter)
    # Simpler: just bucket by 4-digit prefix / range which correlates to size in TW
    # Best approximation: use OHLCV Close as of each week
    # Since we don't have shares outstanding, we label by percentile of avg_tv_60d across the universe
    # at each scan date --- this is done inline during the weekly loop, return empty here.
    return pd.DataFrame()


# ================================================================
# Mode Gate Helpers (VF-6)
# ================================================================

def _gate_right_side(snap: pd.DataFrame) -> pd.Series:
    """
    Pure-right-side gate: trend_score >= TREND_SCORE_RIGHT_THRESHOLD.

    Proxy for "trigger >= 3" in live momentum_screener.py / analysis_engine.py.
    trend_score 0-10 computed by compute_trend_score(); >= 6 corresponds to
    strong weekly structure (Close > wMA20 > wMA60, daily MA stack bullish).

    Returns boolean mask (same index as snap).
    """
    return snap['trend_score'] >= TREND_SCORE_RIGHT_THRESHOLD


def _gate_support_proximity(snap: pd.DataFrame) -> pd.Series:
    """
    Pure-left-side gate: close is within SUPPORT_BAND_PCT of nearest daily MA (月線/季線).

    Proxy for Scenario B action_plan in analysis_engine.py `_generate_action_plan`:
      support_candidates = [m for m in [ma20, ma60] if m > 0]  # daily MA20, daily MA60
      entry = min(support_candidates) * 0.98 ~ * 1.02
    Widened to SUPPORT_BAND_PCT (±5%) for weekly-snapshot simulation
    (weekly close may not hit the tight ±2% intraday).

    "Near support" = close is between support * (1 - band) and support * (1 + band),
    where support = min(daily_ma20, daily_ma60).

    Note: 月線 = daily MA20, 季線 = daily MA60 in TW trading convention.
    The live code uses these daily MAs for Scenario B entry, NOT the weekly MAs.

    Returns boolean mask (same index as snap).
    """
    dma20 = snap['daily_ma20']
    dma60 = snap['daily_ma60']

    # Support level = lowest available daily MA (most conservative level)
    support = dma20.copy()
    has_both = dma20.notna() & dma60.notna()
    support[has_both] = snap.loc[has_both, ['daily_ma20', 'daily_ma60']].min(axis=1)
    support[~dma20.notna() & dma60.notna()] = dma60[~dma20.notna() & dma60.notna()]

    close = snap['Close']
    lower = support * (1.0 - SUPPORT_BAND_PCT)
    upper = support * (1.0 + SUPPORT_BAND_PCT)

    near = close.between(lower, upper) & support.notna() & (support > 0)
    return near


def apply_mode_gate(snap: pd.DataFrame, mode: str) -> pd.DataFrame:
    """
    Apply VF-6 mode gate to candidate snapshot.

    Args:
        snap: per-week candidate DataFrame with trend_score, Close, weekly_ma20/60
        mode: 'mixed' | 'pure_right' | 'pure_left'

    Returns:
        Filtered DataFrame (may be empty).
    """
    if mode == 'pure_right':
        mask = _gate_right_side(snap)
    elif mode == 'pure_left':
        mask = _gate_support_proximity(snap)
    elif mode == 'mixed':
        mask = _gate_right_side(snap) & _gate_support_proximity(snap)
    else:
        raise ValueError(f"Unknown mode: {mode!r}. Must be mixed | pure_right | pure_left")
    return snap[mask].copy()


# ================================================================
# Main Simulation Loop
# ================================================================

def run_simulation(
    ohlcv: pd.DataFrame,
    indicators: pd.DataFrame,
    fwd_returns: pd.DataFrame,
    quality_scores: pd.DataFrame,
    universe: list,
    regime_series: pd.Series,
    start_date: str,
    end_date: str,
    debug: bool = False,
    mode: str = 'mixed',
) -> pd.DataFrame:
    """
    For each weekly scan date, simulate QM Stage 1 + Stage 2 and record picks.

    Args:
        mode: 'mixed' | 'pure_right' | 'pure_left'
              Controls which VF-6 entry gate is applied after top50 selection.
              See module docstring for exact definitions.
    """
    # --- Build weekly scan dates from actual trading days ---
    ref_dates = ohlcv[ohlcv['stock_id'] == '2330'].sort_values('date')['date']
    ref_series = pd.Series(ref_dates.values, index=pd.DatetimeIndex(ref_dates.values))
    weekly = ref_series.resample('W-FRI').last().dropna()
    weekly = weekly[(weekly >= start_date) & (weekly <= end_date)]
    scan_dates = weekly.values
    logger.info("Weekly scan dates: %d weeks (%s to %s)", len(scan_dates),
                pd.Timestamp(scan_dates[0]).date(), pd.Timestamp(scan_dates[-1]).date())

    if debug:
        scan_dates = scan_dates[:10]
        logger.info("DEBUG mode: only processing %d weeks", len(scan_dates))

    univ_set = set(universe)

    # Merge indicators with forward returns once (big join)
    ind_fwd = indicators.merge(
        fwd_returns.drop(columns=['Close', 'Volume'], errors='ignore'),
        on=['stock_id', 'date'],
        how='left',
    )

    # Sort quality_scores for PIT lookup (merge_asof per stock)
    qs_sorted = quality_scores.sort_values(['stock_id', 'date'])

    all_picks = []
    stats_weeks_processed = 0
    stats_weeks_no_picks = 0

    for week_end in scan_dates:
        d = pd.Timestamp(week_end)

        # ---- Stage 1: Universe filter ----
        # 1a. top300 universe (static for this simulation)
        # 1b. Stock must have data on date d
        # 1c. 60d avg trading value >= MIN_AVG_TV
        # 1d. Close >= MIN_PRICE
        # 1e. ETF/special already excluded in load_top300()

        snap = ind_fwd[ind_fwd['date'] == d].copy()
        snap = snap[snap['stock_id'].isin(univ_set)]
        snap = snap[snap['avg_tv_60d'] >= MIN_AVG_TV]
        snap = snap[snap['Close'] >= MIN_PRICE]
        snap = snap[snap['Close'].notna() & (snap['Close'] > 0)]

        if snap.empty:
            stats_weeks_no_picks += 1
            continue

        # ---- PIT quality score lookup ----
        # For date d, find the most recent quarter end <= d for each stock
        qs_pit = (
            qs_sorted[qs_sorted['date'] <= d]
            .groupby('stock_id')
            .last()
            .reset_index()
        )
        snap = snap.merge(
            qs_pit[['stock_id', 'f_score', 'quality_score', 'combined_score']],
            on='stock_id',
            how='left',
        )
        snap.rename(columns={'quality_score': 'body_score', 'combined_score': '_combined'}, inplace=True)

        # ---- Stage 2: Compute trend_score ----
        snap['trend_score'] = snap.apply(compute_trend_score, axis=1)

        # Normalize scores to 0-100 for fair percentile ranking
        # f_score: 0-9 -> *100/9 -> ~0-100
        snap['f_score_norm'] = snap['f_score'] / 9.0 * 100
        # body_score: already 0-100
        snap['body_score_norm'] = snap['body_score']
        # trend_score: 0-10 -> *10 -> 0-100
        snap['trend_score_norm'] = snap['trend_score'] * 10.0

        # QM composite: F50 + Body30 + Trend20
        snap['qm_score'] = (
            snap['f_score_norm'] * 0.50 +
            snap['body_score_norm'] * 0.30 +
            snap['trend_score_norm'] * 0.20
        )

        # Market cap tier (by avg_tv_60d percentile as proxy)
        snap['tv_pct'] = snap['avg_tv_60d'].rank(pct=True)
        snap['market_cap_tier'] = 'small'
        snap.loc[snap['tv_pct'] >= 0.30, 'market_cap_tier'] = 'mid'
        snap.loc[snap['tv_pct'] >= 0.70, 'market_cap_tier'] = 'large'

        # ---- Top 50 (pool, before mode gate) ----
        # Drop rows where qm_score is NaN (no quality score available)
        snap = snap.dropna(subset=['qm_score'])
        top50_pool = snap.nlargest(QM_TOP_N, 'qm_score').reset_index(drop=True)

        if top50_pool.empty:
            stats_weeks_no_picks += 1
            continue

        # ---- VF-6 Mode Gate ----
        # Apply entry condition gate AFTER ranking (gate on top50 pool, not pre-filter,
        # so rank_in_top50 reflects QM rank without the gate, enabling overlap analysis).
        top50_pool['rank_in_top50'] = top50_pool.index + 1
        top50 = apply_mode_gate(top50_pool, mode)

        if top50.empty:
            stats_weeks_no_picks += 1
            continue

        # ---- Regime ----
        regime_val = regime_series.get(d, 'neutral')

        # ---- Assemble pick records ----
        picks = top50[['stock_id', 'Close', 'atr_pct',
                        'weekly_ma20', 'weekly_ma60',
                        'daily_ma20', 'daily_ma60',
                        'f_score', 'body_score', 'trend_score', 'qm_score',
                        'market_cap_tier',
                        'rank_in_top50',
                        'fwd_5d', 'fwd_10d', 'fwd_20d', 'fwd_40d', 'fwd_60d',
                        'fwd_20d_max', 'fwd_20d_min', 'fwd_40d_max', 'fwd_40d_min',
                        ]].copy()
        picks.rename(columns={'Close': 'entry_price'}, inplace=True)
        picks['week_end_date'] = d
        picks['regime'] = regime_val
        picks['mode'] = mode

        # Add stock_name and industry from ohlcv metadata (as-is, may be garbled)
        meta = ohlcv[ohlcv['date'] == d][['stock_id', 'stock_name', 'industry']].drop_duplicates('stock_id')
        picks = picks.merge(meta, on='stock_id', how='left')

        all_picks.append(picks)
        stats_weeks_processed += 1

        if stats_weeks_processed % 20 == 0:
            logger.info("  Processed %d weeks, %d total picks so far",
                        stats_weeks_processed, sum(len(p) for p in all_picks))

    if not all_picks:
        logger.error("No picks generated!")
        return pd.DataFrame()

    journal = pd.concat(all_picks, ignore_index=True)

    # Reorder columns
    col_order = [
        'week_end_date', 'stock_id', 'stock_name', 'rank_in_top50',
        'entry_price', 'atr_pct',
        'weekly_ma20', 'weekly_ma60', 'daily_ma20', 'daily_ma60',
        'f_score', 'body_score', 'trend_score', 'qm_score',
        'market_cap_tier', 'industry', 'regime', 'mode',
        'fwd_5d', 'fwd_10d', 'fwd_20d', 'fwd_40d', 'fwd_60d',
        'fwd_20d_max', 'fwd_20d_min', 'fwd_40d_max', 'fwd_40d_min',
    ]
    col_order = [c for c in col_order if c in journal.columns]
    journal = journal[col_order]

    logger.info("Simulation complete: %d weeks, %d picks, %d weeks with no picks",
                stats_weeks_processed, len(journal), stats_weeks_no_picks)
    return journal


# ================================================================
# Sanity Check
# ================================================================

def sanity_check(journal: pd.DataFrame, ohlcv: pd.DataFrame, n_sample: int = 20):
    """
    Randomly sample N picks and verify:
    - entry_price matches OHLCV Close on week_end_date
    - fwd_20d matches 20d-forward Close
    - f_score / body_score not NaN
    """
    logger.info("Running sanity check on %d random picks...", n_sample)
    rng = np.random.default_rng(42)
    check_rows = journal.dropna(subset=['fwd_20d']).sample(
        min(n_sample, len(journal)), random_state=42
    )

    ohlcv_idx = ohlcv.set_index(['stock_id', 'date'])['Close']
    fail_count = 0

    for _, row in check_rows.iterrows():
        sid = row['stock_id']
        d = pd.Timestamp(row['week_end_date'])
        entry = row['entry_price']
        fwd20 = row['fwd_20d']

        # Verify entry_price
        try:
            actual_close = ohlcv_idx.loc[(sid, d)]
            if abs(actual_close - entry) > 0.01:
                logger.warning("  FAIL entry_price: %s @%s expected=%.2f got=%.2f",
                               sid, d.date(), actual_close, entry)
                fail_count += 1
        except KeyError:
            logger.warning("  SKIP: (%s, %s) not found in OHLCV", sid, d.date())
            continue

        # Verify fwd_20d
        stock_data = ohlcv[ohlcv['stock_id'] == sid].sort_values('date').reset_index(drop=True)
        idx_list = stock_data[stock_data['date'] == d].index.tolist()
        if idx_list:
            idx0 = idx_list[0]
            if idx0 + 20 < len(stock_data):
                expected_close = stock_data.iloc[idx0 + 20]['Close']
                if actual_close > 0:
                    expected_ret = expected_close / actual_close - 1
                    if abs(expected_ret - fwd20) > 0.0001:
                        logger.warning(
                            "  FAIL fwd_20d: %s @%s expected=%.4f got=%.4f",
                            sid, d.date(), expected_ret, fwd20
                        )
                        fail_count += 1

        # Verify scores not NaN
        if pd.isna(row['f_score']) or pd.isna(row['body_score']):
            logger.warning("  FAIL scores NaN: %s @%s f=%.1f body=%.1f",
                           sid, d.date(), row['f_score'], row['body_score'])
            fail_count += 1

    if fail_count == 0:
        logger.info("  Sanity check PASSED: 0 failures in %d checks", n_sample)
    else:
        logger.warning("  Sanity check: %d failures in %d checks", fail_count, n_sample)


# ================================================================
# Print Summary Statistics
# ================================================================

def print_summary(journal: pd.DataFrame):
    """Print key statistics for the generated journal."""
    logger.info("=" * 60)
    logger.info("TRADE JOURNAL SUMMARY")
    logger.info("=" * 60)
    logger.info("Total weeks: %d", journal['week_end_date'].nunique())
    logger.info("Total picks: %d", len(journal))
    logger.info("Unique stocks picked: %d", journal['stock_id'].nunique())
    logger.info("Date range: %s to %s",
                journal['week_end_date'].min().date(),
                journal['week_end_date'].max().date())
    logger.info("")

    # Forward return distribution
    for h in [5, 10, 20, 40, 60]:
        col = f'fwd_{h}d'
        if col not in journal.columns:
            continue
        sub = journal[col].dropna()
        if len(sub) == 0:
            continue
        mean_r = sub.mean() * 100
        med_r = sub.median() * 100
        wr = (sub > 0).mean() * 100
        logger.info("fwd_%2dd: n=%5d mean=%+.2f%% median=%+.2f%% win_rate=%.1f%%",
                    h, len(sub), mean_r, med_r, wr)

    logger.info("")

    # Regime distribution
    logger.info("Regime distribution:")
    regime_dist = journal['regime'].value_counts()
    for r, cnt in regime_dist.items():
        logger.info("  %-12s: %5d picks", r, cnt)

    logger.info("")

    # QM edge check
    fwd20 = journal['fwd_20d'].dropna()
    if len(fwd20) > 0:
        mean20 = fwd20.mean() * 100
        wr20 = (fwd20 > 0).mean() * 100
        if mean20 > 1.0 and wr20 > 52.0:
            logger.info("Edge check PASS: fwd_20d mean=%.2f%% win_rate=%.1f%% (>1%% and >52%%)",
                        mean20, wr20)
        else:
            logger.warning("Edge check WARN: fwd_20d mean=%.2f%% win_rate=%.1f%% (expected >1%% and >52%%)",
                           mean20, wr20)

    # f_score / body_score NaN check
    fscore_nan_rate = journal['f_score'].isna().mean()
    body_nan_rate = journal['body_score'].isna().mean()
    logger.info("")
    logger.info("Data quality: f_score NaN=%.1f%%  body_score NaN=%.1f%%",
                fscore_nan_rate * 100, body_nan_rate * 100)

    # Score distribution
    logger.info("")
    logger.info("QM score stats:")
    logger.info("  f_score:    min=%.1f  mean=%.1f  max=%.1f",
                journal['f_score'].min(), journal['f_score'].mean(), journal['f_score'].max())
    logger.info("  body_score: min=%.1f  mean=%.1f  max=%.1f",
                journal['body_score'].min(), journal['body_score'].mean(), journal['body_score'].max())
    logger.info("  trend_score: min=%.1f  mean=%.1f  max=%.1f",
                journal['trend_score'].min(), journal['trend_score'].mean(), journal['trend_score'].max())
    logger.info("  qm_score: min=%.1f  mean=%.1f  max=%.1f",
                journal['qm_score'].min(), journal['qm_score'].mean(), journal['qm_score'].max())


# ================================================================
# VF-6 Sanity Report
# ================================================================

def _write_vf6_sanity_report(journals: dict, args) -> None:
    """
    Produce VF-6 sanity report:
    1. Trade journal row counts + weekly average picks per mode
    2. Weekly picks overlap rates (mixed∩right, mixed∩left, right∩left)
    3. fwd_20d / fwd_40d mean + win_rate per mode
    4. Regime distribution per mode

    Prints to stdout and saves reports/vf6_sanity.md.
    """
    import io

    REPORTS_DIR = _ROOT / "reports"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_md = REPORTS_DIR / "vf6_sanity.md"

    lines = []

    def emit(s=''):
        lines.append(s)
        logger.info(s)

    emit("# VF-6 QM 左右側混合矛盾 — Sanity Report")
    emit(f"生成時間: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
    emit(f"模擬區間: {args.start} ~ {args.end}")
    emit(f"Entry gate 參數: trend_score >= {TREND_SCORE_RIGHT_THRESHOLD} (right-side proxy), "
         f"support ±{SUPPORT_BAND_PCT*100:.0f}% (left-side proxy)")
    emit()

    # ---- 1. Row counts ----
    emit("## 1. Trade Journal 筆數")
    emit()
    emit("| 模式 | 總筆數 | 週數 | 每週平均 picks |")
    emit("|------|--------|------|----------------|")
    for mode, jnl in journals.items():
        n_weeks = jnl['week_end_date'].nunique()
        avg_picks = len(jnl) / n_weeks if n_weeks > 0 else 0
        emit(f"| {mode} | {len(jnl):,} | {n_weeks} | {avg_picks:.1f} |")
    emit()

    # ---- 2. Overlap analysis ----
    emit("## 2. 每週 Picks 交集率")
    emit()
    emit("方法: 對每週計算兩版的 stock_id 交集 / 聯集，取所有週的中位數。")
    emit()

    mode_names = list(journals.keys())
    pairs = [
        ('mixed', 'pure_right'),
        ('mixed', 'pure_left'),
        ('pure_right', 'pure_left'),
    ]

    emit("| 比較 | 中位數交集率 (Jaccard) | 中位數 A∩B/|A| | 中位數 A∩B/|B| | 解讀 |")
    emit("|------|------------------------|----------------|----------------|------|")

    overlap_summary = {}

    for ma, mb in pairs:
        if ma not in journals or mb not in journals:
            continue
        ja = journals[ma]
        jb = journals[mb]
        all_weeks = sorted(set(ja['week_end_date'].unique()) | set(jb['week_end_date'].unique()))

        jaccards, pct_a, pct_b = [], [], []
        for w in all_weeks:
            a_ids = set(ja.loc[ja['week_end_date'] == w, 'stock_id'])
            b_ids = set(jb.loc[jb['week_end_date'] == w, 'stock_id'])
            if not a_ids and not b_ids:
                continue
            inter = len(a_ids & b_ids)
            union = len(a_ids | b_ids)
            jaccards.append(inter / union if union > 0 else 0)
            pct_a.append(inter / len(a_ids) if a_ids else 0)
            pct_b.append(inter / len(b_ids) if b_ids else 0)

        med_j = float(np.median(jaccards)) if jaccards else 0
        med_a = float(np.median(pct_a)) if pct_a else 0
        med_b = float(np.median(pct_b)) if pct_b else 0
        overlap_summary[(ma, mb)] = (med_j, med_a, med_b)

        # Interpretation
        # Note: mixed ⊆ pure_right by construction (AND gate), so med_a for mixed/right is
        # trivially 100%. The meaningful metric is med_b = what % of right is also in mixed.
        if ma == 'mixed' and mb == 'pure_right':
            # med_b = |mixed ∩ right| / |right| = coverage of right by mixed
            if med_b > 0.80:
                interp = "mixed 覆蓋 >80% 右側池 — 支撐條件幾乎不起過濾，VF-6 假說不成立"
            elif med_b > 0.40:
                interp = f"mixed 覆蓋 {med_b:.0%} 右側池 — 支撐條件有中等過濾效果"
            else:
                interp = f"mixed 僅覆蓋 {med_b:.0%} 右側池 — 支撐條件大幅縮減可選股，踏空風險高 (VF-6)"
        elif ma == 'mixed' and mb == 'pure_left':
            # med_b = |mixed ∩ left| / |left| = coverage of left by mixed
            if med_b > 0.80:
                interp = "mixed 覆蓋 >80% 左側池 — 趨勢條件幾乎不起過濾"
            elif med_b > 0.40:
                interp = f"mixed 覆蓋 {med_b:.0%} 左側池 — 趨勢條件有中等過濾效果"
            else:
                interp = f"mixed 僅覆蓋 {med_b:.0%} 左側池 — 兩條件交集極小，同時成立極難 (VF-6)"
        else:
            if med_j < 0.10:
                interp = "右側與左側池幾乎不重疊 — 兩策略截然不同"
            elif med_j < 0.30:
                interp = "右側與左側有少量重疊 — 部分股票兩條件同時滿足"
            else:
                interp = "右側與左側重疊偏高 — 兩條件可能互相包含"

        emit(f"| {ma} ∩ {mb} | {med_j:.1%} | {med_a:.1%} | {med_b:.1%} | {interp} |")

    emit()

    # ---- 3. Forward return comparison ----
    emit("## 3. Forward Return 粗略對比")
    emit()
    emit("| 模式 | fwd_20d mean | fwd_20d win% | fwd_40d mean | fwd_40d win% | N (20d) |")
    emit("|------|-------------|--------------|-------------|--------------|---------|")

    best_20d = None
    best_20d_mode = None

    for mode, jnl in journals.items():
        f20 = jnl['fwd_20d'].dropna()
        f40 = jnl['fwd_40d'].dropna()
        mean20 = f20.mean() * 100 if len(f20) > 0 else float('nan')
        wr20 = (f20 > 0).mean() * 100 if len(f20) > 0 else float('nan')
        mean40 = f40.mean() * 100 if len(f40) > 0 else float('nan')
        wr40 = (f40 > 0).mean() * 100 if len(f40) > 0 else float('nan')
        emit(f"| {mode} | {mean20:+.2f}% | {wr20:.1f}% | {mean40:+.2f}% | {wr40:.1f}% | {len(f20):,} |")

        if best_20d is None or mean20 > best_20d:
            best_20d = mean20
            best_20d_mode = mode

    emit()
    if best_20d_mode:
        emit(f"粗略最佳 fwd_20d: **{best_20d_mode}** ({best_20d:+.2f}%)")
    emit()

    # ---- 4. Regime distribution ----
    emit("## 4. Regime 分布")
    emit()

    all_regimes = set()
    for jnl in journals.values():
        all_regimes.update(jnl['regime'].unique())
    all_regimes = sorted(all_regimes)

    header = "| 模式 | " + " | ".join(all_regimes) + " |"
    sep = "|------|" + "--------|" * len(all_regimes)
    emit(header)
    emit(sep)
    for mode, jnl in journals.items():
        counts = jnl['regime'].value_counts()
        total = len(jnl)
        cells = [f"{counts.get(r, 0)/total:.1%}" for r in all_regimes]
        emit(f"| {mode} | " + " | ".join(cells) + " |")
    emit()

    # ---- 5. Summary / VF-6 direction ----
    emit("## 5. VF-6 初步方向判斷")
    emit()
    if 'mixed' in journals and 'pure_right' in journals:
        # mixed ⊆ pure_right by construction, so:
        #   med_b_mr = |mixed ∩ right| / |right| = what % of right-side pool makes it into mixed
        #   med_b_ml = |mixed ∩ left|  / |left|  = what % of left-side pool makes it into mixed
        _, _, med_b_mr = overlap_summary.get(('mixed', 'pure_right'), (0, 0, 0))
        _, _, med_b_ml = overlap_summary.get(('mixed', 'pure_left'), (0, 0, 0))
        if med_b_mr < 0.30:
            # Mixed covers <30% of right-side: support gate is severely restrictive
            conclusion = (
                f"支撐條件僅讓 {med_b_mr:.0%} 的右側候選進入 mixed。"
                "混合版大幅踏空右側強勢股。"
                "VF-6 假說「兩條件同時成立機率太低」**初步成立**。"
                "建議直接採用純右側（移除 MA 支撐條件）。"
            )
        elif med_b_mr > 0.80:
            conclusion = (
                f"mixed 覆蓋 {med_b_mr:.0%} 的右側候選，支撐條件幾乎不起過濾作用。"
                "VF-6 假說「混合版實為純右側偽裝」**初步成立**。"
                "建議移除 Scenario B MA 支撐條件，直接用純右側。"
            )
        else:
            conclusion = (
                f"mixed 覆蓋 {med_b_mr:.0%} 右側池、{med_b_ml:.0%} 左側池。"
                "兩條件均有中等過濾效果，無極端踏空或冗餘。"
                "需看 fwd_20d/40d 報酬差異再決定方向。"
            )
        emit(conclusion)
    emit()
    emit("---")
    emit("注意：fwd_20d/40d 只是粗略對比，顯著性須 VF-6 ic-validator 驗證。")

    report_text = "\n".join(lines)
    out_md.write_text(report_text, encoding='utf-8')
    logger.info("VF-6 sanity report saved: %s", out_md)


# ================================================================
# Main
# ================================================================

def main():
    parser = argparse.ArgumentParser(description='QM Historical Simulator (VF-6 三模式)')
    parser.add_argument('--start', default='2021-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default='2025-12-31', help='End date (YYYY-MM-DD, OOS holdout: exclude last 3 months)')
    parser.add_argument('--debug', action='store_true', help='Only process 10 weeks for sanity check')
    parser.add_argument(
        '--mode',
        choices=['mixed', 'pure_right', 'pure_left', 'all'],
        default='mixed',
        help=(
            'Entry gate mode for VF-6 validation:\n'
            '  mixed      = trend_score >= 6 AND 接近週MA支撐 ±5%%\n'
            '  pure_right = trend_score >= 6 only (no support check)\n'
            '  pure_left  = 接近週MA支撐 ±5%% only (no trend check)\n'
            '  all        = run all three modes sequentially'
        ),
    )
    args = parser.parse_args()

    modes_to_run = ['mixed', 'pure_right', 'pure_left'] if args.mode == 'all' else [args.mode]

    logger.info("QM Historical Simulator: %s to %s  mode=%s%s",
                args.start, args.end, args.mode, " [DEBUG]" if args.debug else "")

    # --- Load (once, shared across modes) ---
    ohlcv = load_ohlcv()
    quality_scores = load_quality_scores()
    universe = load_top300()

    univ_set = set(universe)
    ohlcv_univ = ohlcv[ohlcv['stock_id'].isin(univ_set)].copy()
    logger.info("Universe OHLCV: %d stocks, %d rows", ohlcv_univ['stock_id'].nunique(), len(ohlcv_univ))

    lookback_start = (pd.Timestamp(args.start) - pd.Timedelta(days=400)).strftime('%Y-%m-%d')
    ohlcv_hist = ohlcv_univ[ohlcv_univ['date'] >= lookback_start].copy()

    # --- Precompute (once) ---
    indicators = precompute_indicators(ohlcv_hist)
    fwd_returns = precompute_forward_returns(ohlcv_univ)
    regime_series = build_regime_series(ohlcv_hist, universe)

    journals = {}

    for mode in modes_to_run:
        logger.info("=" * 60)
        logger.info("Running mode: %s", mode)
        logger.info("=" * 60)

        journal = run_simulation(
            ohlcv=ohlcv_hist,
            indicators=indicators,
            fwd_returns=fwd_returns,
            quality_scores=quality_scores,
            universe=universe,
            regime_series=regime_series,
            start_date=args.start,
            end_date=args.end,
            debug=args.debug,
            mode=mode,
        )

        if journal.empty:
            logger.error("No data generated for mode=%s. Skipping.", mode)
            continue

        journals[mode] = journal

        sanity_check(journal, ohlcv_univ)
        print_summary(journal)

        if not args.debug:
            out_path = OUT_PATHS[mode]
            out_path.parent.mkdir(parents=True, exist_ok=True)
            journal.to_parquet(out_path, index=False)
            logger.info("Saved: %s (%d rows)", out_path, len(journal))

            # mixed mode: also write the legacy alias (OUT_PATH) so G1/G2 scripts don't break
            if mode == 'mixed':
                journal.to_parquet(OUT_PATH, index=False)
                logger.info("Saved alias: %s (%d rows)", OUT_PATH, len(journal))
        else:
            logger.info("[DEBUG] skipping save for mode=%s", mode)

    # --- Sanity Report (VF-6) ---
    if len(journals) >= 2 and not args.debug:
        _write_vf6_sanity_report(journals, args)

    logger.info("Done.")


if __name__ == '__main__':
    main()
