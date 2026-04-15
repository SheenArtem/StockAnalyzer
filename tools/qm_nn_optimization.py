"""
Round 5: 類神經網路權重最佳化

用 MLP 學習特徵 → 前瞻報酬的映射，比較 NN 與線性加權的效果。

特徵: rvol_lowatr_pct, trigger_proxy_pct, trend_score_pct, quality_score_pct, f_score_pct
目標: forward returns (5d/10d/20d/40d/60d)

流程:
1. Walk-forward split: 2022-2023 訓練, 2024-2026 測試
2. 訓練 MLP (4-5 → 32 → 16 → 1)
3. 評估 test set portfolio performance (Top 20 stocks per day by predicted return)
4. 萃取特徵重要性（permutation importance）
5. 比較: NN vs 最佳線性權重 vs 當前權重
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.inspection import permutation_importance

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("qm_nn")

HORIZONS = [5, 10, 20, 40, 60]
FEATURES = ['rvol_lowatr_pct', 'trigger_proxy_pct', 'trend_score_pct', 'quality_score_pct', 'f_score_pct']


def load_features():
    """Load all features + forward returns from qm_validation pipeline."""
    from tools.qm_validation import (
        load_ohlcv, compute_forward_returns, compute_rvol_lowatr,
        compute_trigger_proxy, compute_trend_score_daily,
        load_financials, build_quality_flag, load_quality_scores,
    )
    import json

    logger.info("Loading OHLCV...")
    ohlcv = load_ohlcv()
    ohlcv = ohlcv[ohlcv['date'] >= '2022-01-01'].copy()

    # Restrict to top 300 universe
    with open(DATA_DIR / "top300_universe.json") as f:
        top300 = set(json.load(f))
    ohlcv = ohlcv[ohlcv['stock_id'].isin(top300)].copy()
    logger.info(f"  OHLCV: {ohlcv['stock_id'].nunique()} stocks, {ohlcv['date'].nunique()} days")

    ohlcv = compute_forward_returns(ohlcv)

    # Compute indicators
    logger.info("Computing indicators...")
    rvol_df = compute_rvol_lowatr(ohlcv)
    trigger_df = compute_trigger_proxy(ohlcv)
    trend_df = compute_trend_score_daily(ohlcv)

    merged = ohlcv.copy()
    merged = merged.merge(rvol_df, on=['stock_id', 'date'], how='left')
    merged = merged.merge(trigger_df, on=['stock_id', 'date'], how='left')
    merged = merged.merge(trend_df, on=['stock_id', 'date'], how='left')

    # Quality scores
    qs = load_quality_scores()
    qs_sorted = qs.sort_values('date')
    merged = pd.merge_asof(
        merged.sort_values('date'),
        qs_sorted[['stock_id', 'date', 'quality_score', 'combined_score', 'f_score']].sort_values('date'),
        on='date', by='stock_id', direction='backward',
    )

    # Quality gate filter
    financials = load_financials()
    qf = build_quality_flag(financials, ohlcv['date'].unique())
    merged = pd.merge_asof(
        merged.sort_values('date'),
        qf[['stock_id', 'date', 'passes_quality']].sort_values('date'),
        on='date', by='stock_id', direction='backward',
    )
    merged = merged[merged['passes_quality'] == True].copy()

    # Trend filter
    merged = merged[merged['trend_score'].notna() & (merged['trend_score'] >= 1)].copy()

    # Compute daily cross-sectional percentile ranks
    indicators = ['rvol_lowatr', 'trigger_proxy', 'trend_score', 'quality_score', 'f_score']
    for ind in indicators:
        if ind in merged.columns:
            merged[f'{ind}_pct'] = merged.groupby('date')[ind].rank(pct=True) * 100

    logger.info(f"  Merged: {len(merged)} rows, {merged['stock_id'].nunique()} stocks")
    return merged


def train_nn(df, horizon, test_start='2024-07-01'):
    """Train MLP on feature -> forward return."""
    fwd_col = f'fwd_{horizon}d'
    cols = FEATURES + [fwd_col]

    # Clean
    data = df[['stock_id', 'date'] + cols].dropna()
    train = data[data['date'] < test_start].copy()
    test = data[data['date'] >= test_start].copy()

    X_train = train[FEATURES].values
    y_train = train[fwd_col].values
    X_test = test[FEATURES].values
    y_test = test[fwd_col].values

    logger.info(f"  [{horizon}d] Train: {len(X_train)}, Test: {len(X_test)}")

    # Standardize
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    # MLP
    mlp = MLPRegressor(
        hidden_layer_sizes=(32, 16),
        activation='relu',
        solver='adam',
        max_iter=200,
        early_stopping=True,
        validation_fraction=0.15,
        random_state=42,
        alpha=0.01,  # L2
    )
    mlp.fit(X_train_s, y_train)

    # Test predictions
    train_score = mlp.score(X_train_s, y_train)
    test_score = mlp.score(X_test_s, y_test)

    # IC on test set
    preds = mlp.predict(X_test_s)
    test['pred'] = preds

    # Daily IC
    daily_ic = []
    for date, gdf in test.groupby('date'):
        if len(gdf) < 20:
            continue
        ic, _ = stats.spearmanr(gdf['pred'], gdf[fwd_col])
        if not np.isnan(ic):
            daily_ic.append(ic)

    mean_ic = np.mean(daily_ic) if daily_ic else 0
    ic_ir = mean_ic / np.std(daily_ic) if len(daily_ic) > 1 and np.std(daily_ic) > 0 else 0

    # Top-20 portfolio return on test set
    portfolio_returns = []
    for date, gdf in test.groupby('date'):
        if len(gdf) < 20:
            continue
        top20 = gdf.nlargest(20, 'pred')
        portfolio_returns.append(top20[fwd_col].mean())

    if portfolio_returns:
        mean_ret = np.mean(portfolio_returns) * 100
        win_rate = (np.array(portfolio_returns) > 0).mean() * 100
        sharpe = np.mean(portfolio_returns) / np.std(portfolio_returns) * np.sqrt(252/horizon) if np.std(portfolio_returns) > 0 else 0
    else:
        mean_ret = win_rate = sharpe = 0

    # Permutation importance
    importances = permutation_importance(mlp, X_test_s, y_test, n_repeats=3, random_state=42, n_jobs=1)
    feat_imp = dict(zip(FEATURES, importances.importances_mean))

    return {
        'horizon': f'{horizon}d',
        'train_r2': round(train_score, 4),
        'test_r2': round(test_score, 4),
        'mean_ic': round(mean_ic, 4),
        'ic_ir': round(ic_ir, 3),
        'portfolio_ret': round(mean_ret, 3),
        'portfolio_win': round(win_rate, 1),
        'portfolio_sharpe': round(sharpe, 3),
        'feat_imp': {k: round(v, 5) for k, v in feat_imp.items()},
        'n_test_days': len(daily_ic),
    }, mlp, scaler


def compare_with_linear(df, horizon, test_start='2024-07-01'):
    """Compare NN with a few baseline linear combinations."""
    fwd_col = f'fwd_{horizon}d'
    test = df[df['date'] >= test_start].copy()

    baselines = {
        'Current (rvol30/trig25/qual25/trend20)': {'rvol_lowatr_pct': 0.30, 'trigger_proxy_pct': 0.25, 'quality_score_pct': 0.25, 'trend_score_pct': 0.20, 'f_score_pct': 0},
        'F-Score Only': {'rvol_lowatr_pct': 0, 'trigger_proxy_pct': 0, 'quality_score_pct': 0, 'trend_score_pct': 0, 'f_score_pct': 1.0},
        'Quality Only': {'rvol_lowatr_pct': 0, 'trigger_proxy_pct': 0, 'quality_score_pct': 1.0, 'trend_score_pct': 0, 'f_score_pct': 0},
        'Trend Only': {'rvol_lowatr_pct': 0, 'trigger_proxy_pct': 0, 'quality_score_pct': 0, 'trend_score_pct': 1.0, 'f_score_pct': 0},
        'F50/Qual30/Trend20': {'rvol_lowatr_pct': 0, 'trigger_proxy_pct': 0, 'quality_score_pct': 0.3, 'trend_score_pct': 0.2, 'f_score_pct': 0.5},
    }

    results = []
    for name, weights in baselines.items():
        test['score'] = sum(test[f] * w for f, w in weights.items())
        portfolio_returns = []
        for date, gdf in test.groupby('date'):
            sub = gdf[['score', fwd_col]].dropna()
            if len(sub) < 20:
                continue
            top20 = sub.nlargest(20, 'score')
            portfolio_returns.append(top20[fwd_col].mean())

        if portfolio_returns:
            mean_ret = np.mean(portfolio_returns) * 100
            win = (np.array(portfolio_returns) > 0).mean() * 100
            sharpe = np.mean(portfolio_returns) / np.std(portfolio_returns) * np.sqrt(252/horizon) if np.std(portfolio_returns) > 0 else 0
        else:
            mean_ret = win = sharpe = 0

        results.append({
            'name': name,
            'horizon': f'{horizon}d',
            'ret': round(mean_ret, 3),
            'win': round(win, 1),
            'sharpe': round(sharpe, 3),
            'n_days': len(portfolio_returns),
        })

    return results


def main():
    logger.info("=" * 60)
    logger.info("Round 5: Neural Network Weight Optimization")
    logger.info("=" * 60)

    df = load_features()

    all_nn_results = []
    all_baseline_results = []

    for h in [20, 40, 60]:
        logger.info(f"\n--- Horizon {h}d ---")
        nn_res, mlp, scaler = train_nn(df, h)
        all_nn_results.append(nn_res)

        logger.info(f"  NN train R²: {nn_res['train_r2']}, test R²: {nn_res['test_r2']}")
        logger.info(f"  NN IC: {nn_res['mean_ic']}, IR: {nn_res['ic_ir']}")
        logger.info(f"  NN portfolio: ret={nn_res['portfolio_ret']}% win={nn_res['portfolio_win']}% sharpe={nn_res['portfolio_sharpe']}")
        logger.info(f"  Feature importance:")
        for feat, imp in sorted(nn_res['feat_imp'].items(), key=lambda x: -x[1]):
            logger.info(f"    {feat:30s}: {imp:+.5f}")

        baselines = compare_with_linear(df, h)
        all_baseline_results.extend(baselines)
        logger.info(f"  Baselines:")
        for b in baselines:
            logger.info(f"    {b['name']:45s} ret={b['ret']:+.2f}% win={b['win']:.0f}% sharpe={b['sharpe']:+.3f}")

    # Save results
    pd.DataFrame(all_nn_results).to_csv(OUT_DIR / "qm_round5_nn.csv", index=False)
    pd.DataFrame(all_baseline_results).to_csv(OUT_DIR / "qm_round5_baselines.csv", index=False)
    logger.info("\nSaved to reports/qm_round5_nn.csv + qm_round5_baselines.csv")


if __name__ == '__main__':
    main()
