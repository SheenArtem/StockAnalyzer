"""
QM 品質選股逐層驗證

第 1 輪: 品質門檻效果
第 2 輪: trend_score 最佳門檻
第 3 輪: 各維度條件 IC
第 4 輪: 綜合權重最佳化

用法:
    python tools/qm_validation.py --round 1
    python tools/qm_validation.py --round all
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("qm_val")

HORIZONS = [5, 10, 20, 40, 60]


# ================================================================
# Data Loading
# ================================================================

def load_ohlcv():
    """Load OHLCV parquet (1972 stocks, 15 years)."""
    df = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet")
    df['date'] = pd.to_datetime(df['date'])
    # Normalize stock_id from yf_ticker
    df['stock_id'] = df['yf_ticker'].str.replace('.TW', '').str.replace('.TWO', '')
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def load_financials():
    """Load financial statement parquets."""
    data = {}
    for name in ['income', 'balance', 'cashflow', 'revenue']:
        path = DATA_DIR / f"financials_{name}.parquet"
        if path.exists():
            data[name] = pd.read_parquet(path)
            data[name]['date'] = pd.to_datetime(data[name]['date'])
            logger.info(f"  {name}: {len(data[name])} rows, {data[name]['stock_id'].nunique()} stocks")
    return data


def load_quality_scores():
    """Load precomputed historical quality scores."""
    path = DATA_DIR / "quality_scores.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    df['date'] = pd.to_datetime(df['date'])
    logger.info(f"  quality_scores: {len(df)} rows, {df['stock_id'].nunique()} stocks")
    return df


def compute_forward_returns(ohlcv):
    """Add forward return columns to OHLCV dataframe."""
    logger.info("Computing forward returns...")
    results = []
    for sid, gdf in ohlcv.groupby('stock_id'):
        gdf = gdf.sort_values('date').copy()
        for h in HORIZONS:
            gdf[f'fwd_{h}d'] = gdf['Close'].shift(-h) / gdf['Close'] - 1
        results.append(gdf)
    return pd.concat(results, ignore_index=True)


# ================================================================
# Round 1: Quality Gate Effect
# ================================================================

def build_quality_flag(financials, ohlcv_dates):
    """
    For each (stock_id, quarter), determine if it passes quality gate.
    Returns: DataFrame with [stock_id, quarter_end, passes_quality]

    Quality gate: ROE > 0, net_margin > 0, debt_to_equity < 200, revenue_yoy > -20
    """
    logger.info("Building quality flags from financial data...")

    income = financials.get('income')
    balance = financials.get('balance')
    revenue = financials.get('revenue')

    if income is None or balance is None:
        logger.error("Missing income or balance data")
        return pd.DataFrame()

    # --- ROE and net_margin from income + balance ---
    # Income: type='Revenue', 'IncomeAfterTaxes', 'OperatingIncome'
    # Balance: type='Equity', 'Liabilities'
    net_income_df = income[income['type'] == 'IncomeAfterTaxes'][['stock_id', 'date', 'value']].copy()
    net_income_df.rename(columns={'value': 'net_income'}, inplace=True)
    net_income_df['net_income'] = pd.to_numeric(net_income_df['net_income'], errors='coerce')

    revenue_df_inc = income[income['type'] == 'Revenue'][['stock_id', 'date', 'value']].copy()
    revenue_df_inc.rename(columns={'value': 'revenue_val'}, inplace=True)
    revenue_df_inc['revenue_val'] = pd.to_numeric(revenue_df_inc['revenue_val'], errors='coerce')

    equity_df = balance[balance['type'] == 'Equity'][['stock_id', 'date', 'value']].copy()
    equity_df.rename(columns={'value': 'equity'}, inplace=True)
    equity_df['equity'] = pd.to_numeric(equity_df['equity'], errors='coerce')

    liabilities_df = balance[balance['type'] == 'Liabilities'][['stock_id', 'date', 'value']].copy()
    liabilities_df.rename(columns={'value': 'liabilities'}, inplace=True)
    liabilities_df['liabilities'] = pd.to_numeric(liabilities_df['liabilities'], errors='coerce')

    # Merge
    qf = net_income_df.merge(revenue_df_inc, on=['stock_id', 'date'], how='outer')
    qf = qf.merge(equity_df, on=['stock_id', 'date'], how='outer')
    qf = qf.merge(liabilities_df, on=['stock_id', 'date'], how='outer')

    # Compute metrics
    qf['roe'] = np.where(qf['equity'] > 0, qf['net_income'] / qf['equity'] * 100, np.nan)
    qf['net_margin'] = np.where(qf['revenue_val'] > 0, qf['net_income'] / qf['revenue_val'] * 100, np.nan)
    qf['debt_to_equity'] = np.where(qf['equity'] > 0, qf['liabilities'] / qf['equity'] * 100, np.nan)

    # --- Revenue YoY from monthly revenue ---
    rev_yoy = pd.DataFrame()
    if revenue is not None and not revenue.empty:
        rev = revenue.copy()
        rev['revenue_amount'] = pd.to_numeric(rev.get('revenue', rev.get('value', 0)), errors='coerce')
        if 'revenue' in rev.columns:
            rev['revenue_amount'] = pd.to_numeric(rev['revenue'], errors='coerce')
        # Group by stock_id + quarter, compute YoY
        rev['quarter'] = rev['date'].dt.to_period('Q')
        rev['year'] = rev['date'].dt.year
        rev['month'] = rev['date'].dt.month

        # Use the revenue column directly
        for col in ['revenue', 'revenue_amount', 'value']:
            if col in rev.columns:
                rev['rev_val'] = pd.to_numeric(rev[col], errors='coerce')
                break

        # Quarterly sum
        q_rev = rev.groupby(['stock_id', 'quarter'])['rev_val'].sum().reset_index()
        q_rev['quarter_str'] = q_rev['quarter'].astype(str)
        q_rev['year'] = q_rev['quarter'].dt.year
        q_rev['q_num'] = q_rev['quarter'].dt.quarter

        # YoY: same quarter last year
        q_rev_prev = q_rev.copy()
        q_rev_prev['year'] = q_rev_prev['year'] + 1
        q_rev_prev.rename(columns={'rev_val': 'rev_val_prev'}, inplace=True)

        q_merged = q_rev.merge(q_rev_prev[['stock_id', 'year', 'q_num', 'rev_val_prev']],
                                on=['stock_id', 'year', 'q_num'], how='left')
        q_merged['rev_yoy'] = np.where(
            q_merged['rev_val_prev'] > 0,
            (q_merged['rev_val'] / q_merged['rev_val_prev'] - 1) * 100,
            np.nan
        )
        # Convert quarter back to date (end of quarter)
        q_merged['date'] = q_merged['quarter'].dt.to_timestamp(how='end')
        rev_yoy = q_merged[['stock_id', 'date', 'rev_yoy']].dropna(subset=['rev_yoy'])

    # Merge revenue YoY
    if not rev_yoy.empty:
        qf = qf.merge(rev_yoy, on=['stock_id', 'date'], how='left')
    else:
        qf['rev_yoy'] = np.nan

    # Quality flag
    qf['passes_quality'] = True
    qf.loc[qf['roe'].notna() & (qf['roe'] <= 0), 'passes_quality'] = False
    qf.loc[qf['net_margin'].notna() & (qf['net_margin'] <= 0), 'passes_quality'] = False
    qf.loc[qf['debt_to_equity'].notna() & (qf['debt_to_equity'] > 200), 'passes_quality'] = False
    qf.loc[qf['rev_yoy'].notna() & (qf['rev_yoy'] < -20), 'passes_quality'] = False

    logger.info(f"  Quality flags: {len(qf)} rows, pass rate: {qf['passes_quality'].mean():.1%}")
    return qf[['stock_id', 'date', 'passes_quality', 'roe', 'net_margin', 'debt_to_equity', 'rev_yoy']]


def round1_quality_gate(ohlcv, financials):
    """
    第 1 輪: 品質門檻效果

    對每個歷史時點，把股票分成「通過品質門檻」和「未通過」兩組，
    比較兩組的前瞻報酬差異。
    """
    logger.info("=" * 60)
    logger.info("Round 1: Quality Gate Effect")
    logger.info("=" * 60)

    qf = build_quality_flag(financials, ohlcv['date'].unique())
    if qf.empty:
        logger.error("No quality flags built")
        return None

    # Map quality flag to OHLCV: for each trading day, use the most recent quarter's quality flag
    qf_sorted = qf.sort_values('date')
    ohlcv_sorted = ohlcv.sort_values('date')

    # For each stock, forward-fill quality flag from quarterly to daily
    merged = pd.merge_asof(
        ohlcv_sorted.sort_values('date'),
        qf_sorted[['stock_id', 'date', 'passes_quality']].sort_values('date'),
        on='date',
        by='stock_id',
        direction='backward',
    )

    # Only use dates where we have quality flags (2020+)
    merged = merged[merged['passes_quality'].notna()].copy()
    logger.info(f"  Merged: {len(merged)} rows with quality flags")

    # Compare forward returns
    results = []
    for h in HORIZONS:
        col = f'fwd_{h}d'
        if col not in merged.columns:
            continue

        pass_group = merged[merged['passes_quality'] == True][col].dropna()
        fail_group = merged[merged['passes_quality'] == False][col].dropna()

        if len(pass_group) < 100 or len(fail_group) < 100:
            continue

        pass_mean = pass_group.mean() * 100
        fail_mean = fail_group.mean() * 100
        diff = pass_mean - fail_mean

        # T-test
        t_stat, p_val = stats.ttest_ind(pass_group, fail_group, equal_var=False)

        # Win rate
        pass_wr = (pass_group > 0).mean() * 100
        fail_wr = (fail_group > 0).mean() * 100

        results.append({
            'horizon': f'{h}d',
            'pass_n': len(pass_group),
            'fail_n': len(fail_group),
            'pass_mean_ret': round(pass_mean, 3),
            'fail_mean_ret': round(fail_mean, 3),
            'diff': round(diff, 3),
            'pass_win_rate': round(pass_wr, 1),
            'fail_win_rate': round(fail_wr, 1),
            't_stat': round(t_stat, 2),
            'p_value': round(p_val, 4),
            'significant': p_val < 0.05,
        })

        logger.info(f"  {h}d: pass={pass_mean:+.3f}% (n={len(pass_group)}) vs "
                     f"fail={fail_mean:+.3f}% (n={len(fail_group)}) | "
                     f"diff={diff:+.3f}% | p={p_val:.4f} {'***' if p_val < 0.01 else '**' if p_val < 0.05 else 'ns'}")

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUT_DIR / "qm_round1_quality_gate.csv", index=False)
    logger.info(f"  Saved to reports/qm_round1_quality_gate.csv")
    return df_results


# ================================================================
# Round 2: Trend Score Threshold
# ================================================================

def compute_trend_score_daily(ohlcv):
    """
    Compute simplified weekly trend score for each stock on each date.
    Uses weekly MA20/MA60 + weekly close position.
    """
    logger.info("Computing weekly trend scores...")
    from technical_analysis import calculate_all_indicators

    results = []
    stocks = ohlcv['stock_id'].unique()
    total = len(stocks)

    for i, sid in enumerate(stocks):
        if (i + 1) % 100 == 0:
            logger.info(f"  [{i+1}/{total}] trend scores...")

        sdf = ohlcv[ohlcv['stock_id'] == sid].sort_values('date').copy()
        if len(sdf) < 60:
            continue

        sdf = sdf.set_index('date')

        # Resample to weekly
        weekly = sdf.resample('W-FRI').agg({
            'Open': 'first', 'High': 'max', 'Low': 'min',
            'Close': 'last', 'Volume': 'sum',
        }).dropna(subset=['Close'])

        if len(weekly) < 20:
            continue

        # Calculate weekly MA20 and MA60
        weekly['MA20'] = weekly['Close'].rolling(20).mean()
        weekly['MA60'] = weekly['Close'].rolling(60, min_periods=30).mean()

        # Simple trend score: Close > MA20 > MA60 = +2, Close > MA20 = +1, etc.
        def _trend(row):
            c, m20, m60 = row['Close'], row.get('MA20'), row.get('MA60')
            if pd.isna(m20):
                return 0
            score = 0
            if m60 is not None and not pd.isna(m60):
                if c > m20 and m20 > m60:
                    score = 2
                elif c > m20:
                    score = 1
                elif c < m20 and m20 < m60:
                    score = -2
                else:
                    score = 0
            else:
                score = 1 if c > m20 else -1
            return score

        weekly['trend_score'] = weekly.apply(_trend, axis=1)

        # Forward-fill weekly trend to daily
        daily_trend = weekly[['trend_score']].resample('D').ffill()
        daily_trend = daily_trend.reindex(sdf.index, method='ffill')
        sdf['trend_score'] = daily_trend['trend_score']
        sdf = sdf.reset_index()
        results.append(sdf[['stock_id', 'date', 'trend_score']])

    if not results:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)


def round2_trend_threshold(ohlcv, quality_flags=None):
    """
    第 2 輪: trend_score 最佳門檻

    在通過品質門檻的股票中，比較不同 trend 門檻的前瞻報酬。
    """
    logger.info("=" * 60)
    logger.info("Round 2: Trend Score Threshold")
    logger.info("=" * 60)

    trend_df = compute_trend_score_daily(ohlcv)
    if trend_df.empty:
        logger.error("No trend scores computed")
        return None

    merged = ohlcv.merge(trend_df, on=['stock_id', 'date'], how='left')

    # Apply quality filter if available
    if quality_flags is not None and not quality_flags.empty:
        qf_sorted = quality_flags.sort_values('date')
        merged = pd.merge_asof(
            merged.sort_values('date'),
            qf_sorted[['stock_id', 'date', 'passes_quality']].sort_values('date'),
            on='date', by='stock_id', direction='backward',
        )
        before = len(merged)
        merged = merged[merged['passes_quality'] == True].copy()
        logger.info(f"  After quality filter: {len(merged)}/{before}")

    merged = merged[merged['trend_score'].notna()].copy()

    results = []
    for threshold in [-1, 0, 1, 2, 3]:
        for h in HORIZONS:
            col = f'fwd_{h}d'
            if col not in merged.columns:
                continue

            subset = merged[merged['trend_score'] >= threshold][col].dropna()
            rest = merged[merged['trend_score'] < threshold][col].dropna()

            if len(subset) < 100:
                continue

            mean_ret = subset.mean() * 100
            rest_mean = rest.mean() * 100 if len(rest) > 100 else np.nan
            win_rate = (subset > 0).mean() * 100
            sharpe = subset.mean() / subset.std() * np.sqrt(252 / h) if subset.std() > 0 else 0

            results.append({
                'threshold': f'>={threshold}',
                'horizon': f'{h}d',
                'n': len(subset),
                'mean_ret': round(mean_ret, 3),
                'rest_mean_ret': round(rest_mean, 3) if not np.isnan(rest_mean) else None,
                'diff': round(mean_ret - rest_mean, 3) if not np.isnan(rest_mean) else None,
                'win_rate': round(win_rate, 1),
                'sharpe': round(sharpe, 3),
            })

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUT_DIR / "qm_round2_trend_threshold.csv", index=False)
    logger.info(f"  Saved to reports/qm_round2_trend_threshold.csv")

    # Print summary for key horizons
    for h in [20, 40, 60]:
        logger.info(f"\n  --- {h}d horizon ---")
        sub = df_results[df_results['horizon'] == f'{h}d']
        for _, r in sub.iterrows():
            diff_str = f"diff={r['diff']:+.3f}%" if r['diff'] is not None else ""
            logger.info(f"  trend{r['threshold']}: ret={r['mean_ret']:+.3f}% wr={r['win_rate']}% "
                         f"sharpe={r['sharpe']} n={r['n']} {diff_str}")

    return df_results


# ================================================================
# Round 3: Conditional IC per Dimension
# ================================================================

def compute_rvol_lowatr(ohlcv):
    """Compute rvol_lowatr for each stock-date."""
    logger.info("Computing rvol_lowatr...")
    results = []
    stocks = ohlcv['stock_id'].unique()
    total = len(stocks)

    for i, sid in enumerate(stocks):
        if (i + 1) % 200 == 0:
            logger.info(f"  [{i+1}/{total}] rvol_lowatr...")

        sdf = ohlcv[ohlcv['stock_id'] == sid].sort_values('date').copy()
        if len(sdf) < 252:
            continue

        # RVOL = Volume / 20d MA
        sdf['vol_ma20'] = sdf['Volume'].rolling(20).mean()
        sdf['rvol'] = sdf['Volume'] / sdf['vol_ma20']

        # ATR_pct
        prev_close = sdf['Close'].shift(1)
        tr = pd.concat([
            sdf['High'] - sdf['Low'],
            (sdf['High'] - prev_close).abs(),
            (sdf['Low'] - prev_close).abs(),
        ], axis=1).max(axis=1)
        sdf['atr'] = tr.rolling(14).mean()
        sdf['atr_pct'] = sdf['atr'] / sdf['Close'] * 100

        # Z-scores (252d rolling)
        for col, zcol in [('rvol', 'rvol_z'), ('atr_pct', 'atr_pct_z')]:
            rm = sdf[col].rolling(252, min_periods=60).mean()
            rs = sdf[col].rolling(252, min_periods=60).std()
            sdf[zcol] = (sdf[col] - rm) / rs.replace(0, np.nan)

        sdf['rvol_lowatr'] = sdf['rvol_z'] - sdf['atr_pct_z']
        results.append(sdf[['stock_id', 'date', 'rvol_lowatr']].dropna(subset=['rvol_lowatr']))

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def compute_trigger_proxy(ohlcv):
    """
    Simplified trigger_score proxy for backtesting.
    Uses MACD signal + RSI mean-reversion + Supertrend direction.
    """
    logger.info("Computing trigger score proxy...")
    results = []
    stocks = ohlcv['stock_id'].unique()
    total = len(stocks)

    for i, sid in enumerate(stocks):
        if (i + 1) % 200 == 0:
            logger.info(f"  [{i+1}/{total}] trigger proxy...")

        sdf = ohlcv[ohlcv['stock_id'] == sid].sort_values('date').copy()
        if len(sdf) < 60:
            continue

        close = sdf['Close']

        # MACD
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        macd_hist = macd - signal

        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))

        # Simple trigger proxy: MACD histogram z-score + RSI deviation
        macd_z = (macd_hist - macd_hist.rolling(60).mean()) / macd_hist.rolling(60).std().replace(0, np.nan)
        rsi_dev = (rsi - 50) / 50  # -1 to +1

        sdf['trigger_proxy'] = (macd_z * 0.6 + rsi_dev * 0.4).clip(-3, 3)
        results.append(sdf[['stock_id', 'date', 'trigger_proxy']].dropna(subset=['trigger_proxy']))

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def round3_conditional_ic(ohlcv, quality_flags=None, quality_scores=None, trend_threshold=1):
    """
    第 3 輪: 各維度條件 IC

    在通過品質門檻 + trend >= threshold 的子集內，
    計算 trigger / rvol_lowatr / trend / quality 的截面 IC。
    """
    logger.info("=" * 60)
    logger.info(f"Round 3: Conditional IC (quality gate + trend>={trend_threshold})")
    logger.info("=" * 60)

    # Compute indicators
    rvol_df = compute_rvol_lowatr(ohlcv)
    trigger_df = compute_trigger_proxy(ohlcv)
    trend_df = compute_trend_score_daily(ohlcv)

    # Merge all
    merged = ohlcv.copy()
    if not rvol_df.empty:
        merged = merged.merge(rvol_df, on=['stock_id', 'date'], how='left')
    if not trigger_df.empty:
        merged = merged.merge(trigger_df, on=['stock_id', 'date'], how='left')
    if not trend_df.empty:
        merged = merged.merge(trend_df, on=['stock_id', 'date'], how='left')

    # Merge quality scores (quarterly, forward-fill to daily)
    if quality_scores is not None and not quality_scores.empty:
        qs_sorted = quality_scores.sort_values('date')
        merged = pd.merge_asof(
            merged.sort_values('date'),
            qs_sorted[['stock_id', 'date', 'quality_score', 'combined_score', 'f_score']].sort_values('date'),
            on='date', by='stock_id', direction='backward',
        )

    # Apply quality filter
    if quality_flags is not None and not quality_flags.empty:
        qf_sorted = quality_flags.sort_values('date')
        merged = pd.merge_asof(
            merged.sort_values('date'),
            qf_sorted[['stock_id', 'date', 'passes_quality']].sort_values('date'),
            on='date', by='stock_id', direction='backward',
        )
        merged = merged[merged['passes_quality'] == True].copy()

    # Apply trend filter
    merged = merged[merged['trend_score'].notna() & (merged['trend_score'] >= trend_threshold)].copy()
    logger.info(f"  After filters: {len(merged)} rows, {merged['stock_id'].nunique()} stocks")

    # Compute cross-sectional IC per day
    indicators = ['rvol_lowatr', 'trigger_proxy', 'trend_score', 'quality_score', 'combined_score', 'f_score']
    indicators = [c for c in indicators if c in merged.columns]
    results = []

    for indicator in indicators:
        for h in HORIZONS:
            fwd_col = f'fwd_{h}d'
            if fwd_col not in merged.columns or indicator not in merged.columns:
                continue

            # Daily cross-sectional Spearman IC
            daily_ic = []
            for date, gdf in merged.groupby('date'):
                sub = gdf[[indicator, fwd_col]].dropna()
                if len(sub) < 20:  # need at least 20 stocks for meaningful IC
                    continue
                ic, _ = stats.spearmanr(sub[indicator], sub[fwd_col])
                if not np.isnan(ic):
                    daily_ic.append(ic)

            if len(daily_ic) < 30:
                continue

            ic_arr = np.array(daily_ic)
            mean_ic = ic_arr.mean()
            std_ic = ic_arr.std()
            ic_ir = mean_ic / std_ic if std_ic > 0 else 0
            positive_rate = (ic_arr > 0).mean()
            t_stat = mean_ic / (std_ic / np.sqrt(len(ic_arr))) if std_ic > 0 else 0

            results.append({
                'indicator': indicator,
                'horizon': f'{h}d',
                'mean_IC': round(mean_ic, 4),
                'IC_IR': round(ic_ir, 3),
                'positive_rate': round(positive_rate * 100, 1),
                't_stat': round(t_stat, 2),
                'significant': abs(t_stat) > 1.96,
                'n_days': len(daily_ic),
            })

            sig = '***' if abs(t_stat) > 2.58 else '**' if abs(t_stat) > 1.96 else 'ns'
            logger.info(f"  {indicator:20s} {h:2d}d: IC={mean_ic:+.4f} IR={ic_ir:+.3f} "
                         f"pos={positive_rate*100:.0f}% t={t_stat:.2f} {sig}")

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUT_DIR / "qm_round3_conditional_ic.csv", index=False)
    logger.info(f"  Saved to reports/qm_round3_conditional_ic.csv")
    return df_results


# ================================================================
# Round 4: Composite Weight Optimization
# ================================================================

def round4_weight_optimization(ohlcv, quality_flags=None, quality_scores=None, trend_threshold=1):
    """
    第 4 輪: 綜合權重最佳化

    格點搜尋不同權重組合，計算 Top-N portfolio 的 Sharpe / 勝率。
    """
    logger.info("=" * 60)
    logger.info("Round 4: Weight Optimization (with REAL quality scores)")
    logger.info("=" * 60)

    # Compute all indicators
    rvol_df = compute_rvol_lowatr(ohlcv)
    trigger_df = compute_trigger_proxy(ohlcv)
    trend_df = compute_trend_score_daily(ohlcv)

    merged = ohlcv.copy()
    if not rvol_df.empty:
        merged = merged.merge(rvol_df, on=['stock_id', 'date'], how='left')
    if not trigger_df.empty:
        merged = merged.merge(trigger_df, on=['stock_id', 'date'], how='left')
    if not trend_df.empty:
        merged = merged.merge(trend_df, on=['stock_id', 'date'], how='left')

    # Merge quality scores
    if quality_scores is not None and not quality_scores.empty:
        qs_sorted = quality_scores.sort_values('date')
        merged = pd.merge_asof(
            merged.sort_values('date'),
            qs_sorted[['stock_id', 'date', 'combined_score']].sort_values('date'),
            on='date', by='stock_id', direction='backward',
        )
        merged = merged.rename(columns={'combined_score': 'quality_score_val'})

    # Apply filters
    if quality_flags is not None and not quality_flags.empty:
        qf_sorted = quality_flags.sort_values('date')
        merged = pd.merge_asof(
            merged.sort_values('date'),
            qf_sorted[['stock_id', 'date', 'passes_quality']].sort_values('date'),
            on='date', by='stock_id', direction='backward',
        )
        merged = merged[merged['passes_quality'] == True].copy()

    merged = merged[merged['trend_score'].notna() & (merged['trend_score'] >= trend_threshold)].copy()

    # Percentile rank within each day's cross-section
    indicators = ['rvol_lowatr', 'trigger_proxy', 'trend_score']
    if 'quality_score_val' in merged.columns:
        indicators.append('quality_score_val')
    for ind in indicators:
        merged[f'{ind}_pct'] = merged.groupby('date')[ind].rank(pct=True) * 100

    # Rename quality_score_val_pct to quality_pct for consistency
    if 'quality_score_val_pct' in merged.columns:
        merged['quality_pct'] = merged['quality_score_val_pct']
    else:
        logger.warning("No historical quality scores — using constant 50 (bug warning)")
        merged['quality_pct'] = 50

    logger.info(f"  Quality score coverage: {merged['quality_pct'].notna().mean():.1%}")

    # Grid search
    TOP_N = 20
    results = []
    weight_grid = []

    # Generate weight combinations (step 10%, sum to 100%)
    for w_rvol in range(0, 70, 10):
        for w_trig in range(0, 70, 10):
            for w_trend in range(0, 70, 10):
                if w_rvol + w_trig + w_trend > 100:
                    continue
                w_qual = 100 - w_rvol - w_trig - w_trend
                if w_qual < 0:
                    continue
                weight_grid.append((w_rvol, w_trig, w_qual, w_trend))

    for h in [20, 40, 60]:
        fwd_col = f'fwd_{h}d'
        if fwd_col not in merged.columns:
            continue

        for w_rvol, w_trig, w_qual, w_trend in weight_grid:
            # Composite score
            merged['composite'] = (
                merged['rvol_lowatr_pct'] * w_rvol / 100 +
                merged['trigger_proxy_pct'] * w_trig / 100 +
                merged['quality_pct'] * w_qual / 100 +
                merged['trend_score_pct'] * w_trend / 100
            )

            # For each day, pick top N by composite, compute average forward return
            daily_returns = []
            for date, gdf in merged.groupby('date'):
                sub = gdf[['composite', fwd_col]].dropna()
                if len(sub) < TOP_N:
                    continue
                top = sub.nlargest(TOP_N, 'composite')
                daily_returns.append(top[fwd_col].mean())

            if len(daily_returns) < 30:
                continue

            arr = np.array(daily_returns)
            mean_ret = arr.mean() * 100
            win_rate = (arr > 0).mean() * 100
            sharpe = arr.mean() / arr.std() * np.sqrt(252 / h) if arr.std() > 0 else 0

            results.append({
                'horizon': f'{h}d',
                'w_rvol': w_rvol,
                'w_trigger': w_trig,
                'w_quality': w_qual,
                'w_trend': w_trend,
                'mean_ret': round(mean_ret, 3),
                'win_rate': round(win_rate, 1),
                'sharpe': round(sharpe, 3),
                'n_days': len(daily_returns),
            })

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUT_DIR / "qm_round4_weights.csv", index=False)

    # Print best weights per horizon
    for h in [20, 40, 60]:
        sub = df_results[df_results['horizon'] == f'{h}d']
        if sub.empty:
            continue
        best = sub.loc[sub['sharpe'].idxmax()]
        logger.info(f"\n  Best {h}d (Sharpe={best['sharpe']}):")
        logger.info(f"    rvol={best['w_rvol']}% trigger={best['w_trigger']}% "
                     f"quality={best['w_quality']}% trend={best['w_trend']}%")
        logger.info(f"    ret={best['mean_ret']:+.3f}% wr={best['win_rate']}%")

    logger.info(f"\n  Saved to reports/qm_round4_weights.csv")
    return df_results


# ================================================================
# Main
# ================================================================

def main():
    parser = argparse.ArgumentParser(description='QM Validation')
    parser.add_argument('--round', default='all', help='Which round: 1, 2, 3, 4, or all')
    parser.add_argument('--since', default='2022-01-01', help='Start date for analysis')
    parser.add_argument('--sample', type=int, default=None, help='Sample N stocks')
    args = parser.parse_args()

    # Load data
    logger.info("Loading data...")
    ohlcv = load_ohlcv()
    ohlcv = ohlcv[ohlcv['date'] >= args.since].copy()

    if args.sample:
        rng = np.random.default_rng(42)
        tickers = ohlcv['stock_id'].unique()
        chosen = rng.choice(tickers, min(args.sample, len(tickers)), replace=False)
        ohlcv = ohlcv[ohlcv['stock_id'].isin(chosen)].copy()

    # Load top300 universe
    with open(DATA_DIR / "top300_universe.json") as f:
        top300 = set(json.load(f))
    ohlcv = ohlcv[ohlcv['stock_id'].isin(top300)].copy()
    logger.info(f"  OHLCV: {ohlcv['stock_id'].nunique()} stocks, {ohlcv['date'].nunique()} days")

    # Forward returns
    ohlcv = compute_forward_returns(ohlcv)

    # Load financials + quality scores
    financials = load_financials()
    quality_scores = load_quality_scores()

    run_round = args.round

    # Round 1
    quality_flags = None
    if run_round in ('1', 'all'):
        r1 = round1_quality_gate(ohlcv, financials)
        quality_flags = build_quality_flag(financials, ohlcv['date'].unique())

    # Round 2
    if run_round in ('2', 'all'):
        if quality_flags is None:
            quality_flags = build_quality_flag(financials, ohlcv['date'].unique())
        round2_trend_threshold(ohlcv, quality_flags)

    # Round 3
    if run_round in ('3', 'all'):
        if quality_flags is None:
            quality_flags = build_quality_flag(financials, ohlcv['date'].unique())
        round3_conditional_ic(ohlcv, quality_flags, quality_scores)

    # Round 4
    if run_round in ('4', 'all'):
        if quality_flags is None:
            quality_flags = build_quality_flag(financials, ohlcv['date'].unique())
        round4_weight_optimization(ohlcv, quality_flags, quality_scores)

    logger.info("\nAll done!")


if __name__ == '__main__':
    main()
