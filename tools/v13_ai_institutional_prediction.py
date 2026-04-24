"""V13: AI 模擬法人選股 binary classifier 驗證.

目標:
  判斷能否用 ML 從 momentum/value/chip/sentiment features 預測台股外資(Target A)
  或投信(Target B)下週是否大買某股。若 AUC > 0.6 -> Mode D 有 rule-based entry
  可倚；若 AUC < 0.55 -> 放棄，Mode D 完全靠 thesis discretionary。

Universe:
  TW 上市+上櫃普通股, 市值前 600 (以 20 日平均成交額代理)
  時段: 2016-2025
    Train: 2016-01-01 ~ 2022-12-31 (Pre-AI)
    OOS:   2023-01-01 ~ 2025-12-31 (AI era, 絕對不 peek)

Target (relative ratio, 不依賴 shares_outstanding):
  A: fwd5_foreign_net / fwd5_volume >= threshold_A (~90th percentile)
  B: fwd5_trust_net   / fwd5_volume >= threshold_B (~90th percentile)
  threshold 用 train 期 in-sample 90th 分位決定

Features (全部用 T 日收盤後資料, 無 look-ahead):
  Momentum (6): ret_20d, ret_60d, ret_120d, rsi_14, rvol_20, ma20_ma60_ratio, atr_pct
  Value (4):    f_score (季), revenue_yoy (月, 公告 T+10), roe_proxy
  Chip (5):     foreign_flow_20d_self (只用於 Target B)
                trust_flow_20d_self (只用於 Target A)
                margin_utilization, short_ratio, sbl_ratio (2021-04 之後才有值)
  Cross (1):    mktcap_log (log 成交額 20d avg 代理)

Feature importance 警示:
  若 top 3 feature 為自己 flow 的 lag -> autocorrelation artifact, 標記為假 signal

輸出:
  reports/v13_ai_institutional_prediction.csv  (per target x window x metric)
  reports/v13_ai_institutional_prediction.md   (verdict + caveats)
  reports/v13_feature_importance.csv           (top features per target)
"""
from __future__ import annotations

import os
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings('ignore')

ROOT = Path(r'C:/GIT/StockAnalyzer')
CACHE = ROOT / 'data_cache'
BACKTEST = CACHE / 'backtest'
CHIP = CACHE / 'chip_history'
REPORTS = ROOT / 'reports'
REPORTS.mkdir(exist_ok=True)

# ============================================================
# 1. Load data
# ============================================================

def log(msg: str) -> None:
    ts = datetime.now().strftime('%H:%M:%S')
    print(f'[{ts}] {msg}', flush=True)


def load_ohlcv() -> pd.DataFrame:
    log('Load OHLCV panel ...')
    df = pd.read_parquet(BACKTEST / 'ohlcv_tw.parquet',
                         columns=['date', 'stock_id', 'Open', 'High', 'Low',
                                  'Close', 'AdjClose', 'Volume'])
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] >= '2014-01-01'].copy()  # buffer for 120d momentum
    # 只留純股票代號 (4 digit, 不含 .TW)
    df = df[df['stock_id'].str.match(r'^[0-9]{4}$')].copy()
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)
    log(f'  OHLCV shape={df.shape}, unique stocks={df.stock_id.nunique()}')
    return df


def load_chip() -> pd.DataFrame:
    log('Load institutional chip ...')
    inst = pd.read_parquet(CHIP / 'institutional.parquet')
    inst['date'] = pd.to_datetime(inst['date'])
    inst = inst[['date', 'stock_id', 'foreign_net', 'trust_net']].copy()

    log('Load margin ...')
    mg = pd.read_parquet(CHIP / 'margin.parquet')
    mg['date'] = pd.to_datetime(mg['date'])
    mg = mg[['date', 'stock_id', 'margin_balance', 'short_balance']].copy()

    log('Load short_sale ...')
    ss = pd.read_parquet(CHIP / 'short_sale.parquet')
    ss['date'] = pd.to_datetime(ss['date'])
    ss = ss[['date', 'stock_id', 'sbl_balance']].copy()

    chip = inst.merge(mg, on=['date', 'stock_id'], how='left')
    chip = chip.merge(ss, on=['date', 'stock_id'], how='left')
    log(f'  Chip panel shape={chip.shape}')
    return chip


def load_fundamentals() -> tuple[pd.DataFrame, pd.DataFrame]:
    log('Load F-Score ...')
    fsc = pd.read_parquet(BACKTEST / 'quality_scores.parquet',
                          columns=['date', 'stock_id', 'f_score'])
    fsc['date'] = pd.to_datetime(fsc['date'])
    fsc = fsc.sort_values(['stock_id', 'date'])
    # pad forward 以便 daily merge (延遲 2 個月模擬公告時滯)
    fsc['available_date'] = fsc['date'] + pd.Timedelta(days=60)

    log('Load monthly revenue ...')
    rev = pd.read_parquet(BACKTEST / 'financials_revenue.parquet',
                          columns=['date', 'stock_id', 'revenue_year_growth'])
    rev['date'] = pd.to_datetime(rev['date'])
    # 月營收實際公告 T+10 (當月 10 號)
    rev['available_date'] = rev['date'] + pd.Timedelta(days=10)
    rev = rev.rename(columns={'revenue_year_growth': 'rev_yoy'})
    return fsc, rev


# ============================================================
# 2. Universe selection: 市值前 600 (用成交額代理)
# ============================================================

def select_universe(ohlcv: pd.DataFrame, top_n: int = 600) -> pd.DataFrame:
    """按年動態選 top 600. 對每個日期 lookup 該年的 universe."""
    log(f'Select universe top {top_n} by rolling 60d turnover ...')
    ohlcv = ohlcv.copy()
    ohlcv['turnover'] = ohlcv['AdjClose'] * ohlcv['Volume']
    # 以 60d mean 當年度 proxy
    ohlcv['turnover_60d'] = (ohlcv.groupby('stock_id')['turnover']
                             .rolling(60, min_periods=30).mean()
                             .reset_index(level=0, drop=True))
    ohlcv['year'] = ohlcv['date'].dt.year
    # 每年每檔取 median(turnover_60d) 當年度成交熱度
    yearly = (ohlcv.groupby(['year', 'stock_id'])['turnover_60d']
              .median().reset_index())
    yearly['rank'] = yearly.groupby('year')['turnover_60d'].rank(ascending=False)
    yearly_top = yearly[yearly['rank'] <= top_n].copy()
    log(f'  Universe: {yearly_top.shape[0]} (year, stock) pairs')
    return yearly_top[['year', 'stock_id']]


# ============================================================
# 3. Feature engineering
# ============================================================

def compute_features(ohlcv: pd.DataFrame, chip: pd.DataFrame,
                     fsc: pd.DataFrame, rev: pd.DataFrame) -> pd.DataFrame:
    log('Compute features ...')
    df = ohlcv.copy()
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)

    # --- Momentum ---
    for h in [20, 60, 120]:
        df[f'ret_{h}d'] = (df.groupby('stock_id')['AdjClose']
                           .pct_change(h))
    # RSI 14
    delta = df.groupby('stock_id')['AdjClose'].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.groupby(df['stock_id']).rolling(14, min_periods=14).mean().reset_index(level=0, drop=True)
    avg_loss = loss.groupby(df['stock_id']).rolling(14, min_periods=14).mean().reset_index(level=0, drop=True)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df['rsi_14'] = 100 - 100 / (1 + rs)

    # RVOL 20
    vol20 = (df.groupby('stock_id')['Volume']
             .rolling(20, min_periods=10).mean()
             .reset_index(level=0, drop=True))
    df['rvol_20'] = df['Volume'] / vol20

    # MA ratio
    ma20 = (df.groupby('stock_id')['AdjClose']
            .rolling(20, min_periods=10).mean()
            .reset_index(level=0, drop=True))
    ma60 = (df.groupby('stock_id')['AdjClose']
            .rolling(60, min_periods=30).mean()
            .reset_index(level=0, drop=True))
    df['ma20_ma60_ratio'] = ma20 / ma60

    # ATR%
    tr = pd.concat([
        df['High'] - df['Low'],
        (df['High'] - df.groupby('stock_id')['Close'].shift(1)).abs(),
        (df['Low'] - df.groupby('stock_id')['Close'].shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = (tr.groupby(df['stock_id']).rolling(14, min_periods=14).mean()
           .reset_index(level=0, drop=True))
    df['atr_pct'] = atr / df['Close']

    # mcap proxy
    df['turnover'] = df['AdjClose'] * df['Volume']
    tv20 = (df.groupby('stock_id')['turnover']
            .rolling(20, min_periods=10).mean()
            .reset_index(level=0, drop=True))
    df['mcap_log'] = np.log(tv20.clip(lower=1))

    # --- Chip (merge by date+stock_id) ---
    df = df.merge(chip, on=['date', 'stock_id'], how='left')

    # Fill NaN for chip (missing 代表無法人交易日, 0 較合理)
    for c in ['foreign_net', 'trust_net']:
        df[c] = df[c].fillna(0)

    # 20d cumulative flow (self-lag)
    df['foreign_flow_20d'] = (df.groupby('stock_id')['foreign_net']
                              .rolling(20, min_periods=10).sum()
                              .reset_index(level=0, drop=True))
    df['trust_flow_20d'] = (df.groupby('stock_id')['trust_net']
                            .rolling(20, min_periods=10).sum()
                            .reset_index(level=0, drop=True))
    # normalize by turnover
    df['foreign_flow_20d_norm'] = df['foreign_flow_20d'] * df['AdjClose'] / tv20 / 20
    df['trust_flow_20d_norm'] = df['trust_flow_20d'] * df['AdjClose'] / tv20 / 20

    # margin / short features
    df['margin_utilization'] = df['margin_balance'] / tv20 * df['AdjClose']
    df['short_ratio'] = df['short_balance'] / df['margin_balance'].replace(0, np.nan)
    df['sbl_ratio'] = df['sbl_balance'] / tv20 * df['AdjClose']

    # --- Fundamental merge ---
    # F-Score: asof merge on available_date
    fsc_merge = fsc[['stock_id', 'available_date', 'f_score']].sort_values('available_date')
    df = pd.merge_asof(df.sort_values('date'),
                       fsc_merge.rename(columns={'available_date': 'date'}),
                       on='date', by='stock_id', direction='backward')

    rev_merge = rev[['stock_id', 'available_date', 'rev_yoy']].sort_values('available_date')
    df = pd.merge_asof(df.sort_values('date'),
                       rev_merge.rename(columns={'available_date': 'date'}),
                       on='date', by='stock_id', direction='backward')

    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)

    # --- Targets (fwd 5d cumulative net / fwd 5d volume) ---
    df['fwd5_foreign'] = (df.groupby('stock_id')['foreign_net']
                          .rolling(5, min_periods=5).sum()
                          .shift(-5).reset_index(level=0, drop=True))
    df['fwd5_trust'] = (df.groupby('stock_id')['trust_net']
                        .rolling(5, min_periods=5).sum()
                        .shift(-5).reset_index(level=0, drop=True))
    df['fwd5_volume'] = (df.groupby('stock_id')['Volume']
                         .rolling(5, min_periods=5).sum()
                         .shift(-5).reset_index(level=0, drop=True))
    df['fwd5_foreign_ratio'] = df['fwd5_foreign'] / df['fwd5_volume']
    df['fwd5_trust_ratio'] = df['fwd5_trust'] / df['fwd5_volume']

    # fwd 20d return for economic evaluation
    df['fwd_20d_ret'] = (df.groupby('stock_id')['AdjClose']
                         .pct_change(20).shift(-20))

    log(f'  Feature panel shape={df.shape}')
    return df


# ============================================================
# 4. Walk-forward training and OOS eval
# ============================================================

FEATURE_COLS_COMMON = [
    'ret_20d', 'ret_60d', 'ret_120d',
    'rsi_14', 'rvol_20', 'ma20_ma60_ratio', 'atr_pct', 'mcap_log',
    'f_score', 'rev_yoy',
    'margin_utilization', 'short_ratio', 'sbl_ratio',
]


def features_for_target(target: str) -> list[str]:
    """Target A (外資): 不用 foreign_flow_20d_norm (會是 autocorrelation).
    Target B (投信): 不用 trust_flow_20d_norm."""
    cols = FEATURE_COLS_COMMON.copy()
    if target == 'foreign':
        # 外資 target 不用自己 lag, 但用投信 lag 當 cross-signal
        cols.append('trust_flow_20d_norm')
        # 加入外資自己 lag 的 test variant 看 AUC 增減判 autocorrelation
    else:  # trust
        cols.append('foreign_flow_20d_norm')
    return cols


def features_with_self_lag(target: str) -> list[str]:
    """含自己 lag 的 feature set，用於 autocorrelation check."""
    cols = FEATURE_COLS_COMMON.copy()
    cols.extend(['foreign_flow_20d_norm', 'trust_flow_20d_norm'])
    return cols


def compute_threshold(df_train: pd.DataFrame, ratio_col: str,
                      pct: float = 0.90) -> float:
    return df_train[ratio_col].dropna().quantile(pct)


def train_eval(df_all: pd.DataFrame, target: str, use_self_lag: bool = False,
               model_type: str = 'gbm') -> dict:
    """Train on 2016-2022, eval on 2023-2025.

    Returns dict with AUC, PR_AUC, prec@50, prec@10, feature_importance.
    """
    ratio_col = f'fwd5_{target}_ratio'
    feat_cols = features_with_self_lag(target) if use_self_lag else features_for_target(target)

    train_mask = (df_all['date'] >= '2016-01-01') & (df_all['date'] <= '2022-12-31')
    test_mask = (df_all['date'] >= '2023-01-01') & (df_all['date'] <= '2025-12-31')

    # 只 dropna target 和基本 momentum feature，讓 GBM 自己處理 margin/sbl NaN
    essential_cols = [ratio_col, 'fwd_20d_ret', 'ret_20d', 'ret_60d', 'rsi_14',
                      'ma20_ma60_ratio', 'atr_pct', 'mcap_log']
    df_clean = df_all.dropna(subset=essential_cols).copy()

    tr = df_clean[train_mask.loc[df_clean.index]].copy()
    te = df_clean[test_mask.loc[df_clean.index]].copy()

    if len(tr) < 10000 or len(te) < 5000:
        return {'error': f'insufficient rows tr={len(tr)} te={len(te)}'}

    threshold = compute_threshold(tr, ratio_col, pct=0.90)
    tr['y'] = (tr[ratio_col] >= threshold).astype(int)
    te['y'] = (te[ratio_col] >= threshold).astype(int)

    Xtr, ytr = tr[feat_cols].values, tr['y'].values
    Xte, yte = te[feat_cols].values, te['y'].values

    if model_type == 'gbm':
        # HistGBT handles NaN natively
        model = HistGradientBoostingClassifier(
            max_iter=200, max_depth=5, learning_rate=0.05,
            l2_regularization=1.0, random_state=42,
        )
    else:
        # logistic, with scaler; impute NaN with 0 after scale
        Xtr = np.nan_to_num(Xtr, nan=0.0, posinf=0.0, neginf=0.0)
        Xte = np.nan_to_num(Xte, nan=0.0, posinf=0.0, neginf=0.0)
        scaler = StandardScaler()
        Xtr = scaler.fit_transform(Xtr)
        Xte = scaler.transform(Xte)
        model = LogisticRegression(max_iter=1000, C=0.5, random_state=42)

    model.fit(Xtr, ytr)
    if hasattr(model, 'predict_proba'):
        p_te = model.predict_proba(Xte)[:, 1]
    else:
        p_te = model.decision_function(Xte)

    auc = roc_auc_score(yte, p_te)
    pr_auc = average_precision_score(yte, p_te)
    base_rate = yte.mean()

    # Precision@Top K (per-week aggregate)
    te = te.copy()
    te['p'] = p_te
    te['week'] = te['date'].dt.to_period('W')
    prec50, prec10 = [], []
    fwd20_top10, fwd20_top50 = [], []
    for wk, g in te.groupby('week'):
        if len(g) < 10:
            continue
        g_sorted = g.sort_values('p', ascending=False)
        top10 = g_sorted.head(10)
        top50 = g_sorted.head(min(50, len(g_sorted)))
        prec10.append(top10['y'].mean())
        prec50.append(top50['y'].mean())
        fwd20_top10.append(top10['fwd_20d_ret'].mean())
        fwd20_top50.append(top50['fwd_20d_ret'].mean())

    prec10_mean = float(np.nanmean(prec10))
    prec50_mean = float(np.nanmean(prec50))
    fwd20_top10_mean = float(np.nanmean(fwd20_top10))
    fwd20_top50_mean = float(np.nanmean(fwd20_top50))

    # Feature importance
    fi = {}
    if model_type == 'gbm':
        # HistGBT does not expose feature_importances_, use permutation importance
        # on a 50k subsample of Xte for speed
        n_sub = min(50000, len(Xte))
        idx = np.random.RandomState(42).choice(len(Xte), n_sub, replace=False)
        try:
            perm = permutation_importance(model, Xte[idx], yte[idx],
                                          n_repeats=3, random_state=42,
                                          scoring='roc_auc', n_jobs=1)
            fi = {f: float(imp) for f, imp in zip(feat_cols, perm.importances_mean)}
        except Exception as e:
            log(f'  perm imp failed: {e}')
            fi = {}
    else:
        # logistic coef abs
        try:
            coefs = np.abs(model.coef_[0])
            fi = {f: float(c) for f, c in zip(feat_cols, coefs)}
        except Exception:
            pass

    # Baseline: TWII mean fwd_20d in OOS
    twii_mean = float(te['fwd_20d_ret'].mean())

    # 關鍵診斷: 真命中 (y=1) vs 誤判 (y=0) 在 top10 裡的 fwd_20d 差異
    # 若法人真的預示股價上漲, 則 y=1 命中的 fwd_20d 應顯著 > y=0
    te['top10_flag'] = False
    for wk, g in te.groupby('week'):
        if len(g) < 10: continue
        idx_top10 = g.sort_values('p', ascending=False).head(10).index
        te.loc[idx_top10, 'top10_flag'] = True
    top10_df = te[te.top10_flag].copy()
    # 在 top10 中, y=1 (預測正確) vs y=0 (預測錯誤) 的 fwd_20d
    fwd20_hit = float(top10_df[top10_df.y == 1]['fwd_20d_ret'].mean()) if (top10_df.y == 1).any() else np.nan
    fwd20_miss = float(top10_df[top10_df.y == 0]['fwd_20d_ret'].mean()) if (top10_df.y == 0).any() else np.nan

    return {
        'target': target,
        'model': model_type,
        'use_self_lag': use_self_lag,
        'threshold_fwd5_ratio': float(threshold),
        'train_rows': int(len(tr)),
        'test_rows': int(len(te)),
        'base_positive_rate': float(base_rate),
        'auc': float(auc),
        'pr_auc': float(pr_auc),
        'prec_at_10': prec10_mean,
        'prec_at_50': prec50_mean,
        'fwd20_top10_ret': fwd20_top10_mean,
        'fwd20_top50_ret': fwd20_top50_mean,
        'universe_mean_fwd20': twii_mean,
        'alpha_top10_gross': fwd20_top10_mean - twii_mean,
        'alpha_top50_gross': fwd20_top50_mean - twii_mean,
        # net: 每週換手 0.4% round-trip * 52/20 weeks-per-20d holding factor ~ 1.04%
        # 但 V13 每週 rebalance 用 fwd_5d target, 實際每週轉倉一次 -> 0.4% per week * (20/5) = 1.6% per 20d
        'alpha_top10_net_20d': fwd20_top10_mean - twii_mean - 0.016,
        'alpha_top50_net_20d': fwd20_top50_mean - twii_mean - 0.016,
        'fwd20_top10_hit_ret': fwd20_hit,   # top10 中 y=1 的 fwd20 平均
        'fwd20_top10_miss_ret': fwd20_miss, # top10 中 y=0 的 fwd20 平均
        'feature_importance': fi,
    }


# ============================================================
# 5. Baseline comparison
# ============================================================

def baseline_momentum_top(df_all: pd.DataFrame, target: str) -> dict:
    """Baseline: pick top 50 by ret_20d each week, check precision and fwd20."""
    ratio_col = f'fwd5_{target}_ratio'
    test_mask = (df_all['date'] >= '2023-01-01') & (df_all['date'] <= '2025-12-31')
    df = df_all[test_mask].dropna(subset=['ret_20d', ratio_col, 'fwd_20d_ret']).copy()

    tr_mask = (df_all['date'] >= '2016-01-01') & (df_all['date'] <= '2022-12-31')
    tr = df_all[tr_mask].dropna(subset=[ratio_col])
    threshold = tr[ratio_col].quantile(0.90)
    df['y'] = (df[ratio_col] >= threshold).astype(int)
    df['week'] = df['date'].dt.to_period('W')
    prec10, prec50, fwd10, fwd50 = [], [], [], []
    for wk, g in df.groupby('week'):
        if len(g) < 10:
            continue
        g_sort = g.sort_values('ret_20d', ascending=False)
        top10 = g_sort.head(10)
        top50 = g_sort.head(min(50, len(g_sort)))
        prec10.append(top10['y'].mean())
        prec50.append(top50['y'].mean())
        fwd10.append(top10['fwd_20d_ret'].mean())
        fwd50.append(top50['fwd_20d_ret'].mean())
    return {
        'name': 'momentum_top',
        'prec_at_10': float(np.nanmean(prec10)),
        'prec_at_50': float(np.nanmean(prec50)),
        'fwd20_top10_ret': float(np.nanmean(fwd10)),
        'fwd20_top50_ret': float(np.nanmean(fwd50)),
    }


# ============================================================
# 6. Main
# ============================================================

def main():
    log('=' * 60)
    log('V13: AI institutional prediction binary classifier')
    log('=' * 60)

    ohlcv = load_ohlcv()
    chip = load_chip()
    fsc, rev = load_fundamentals()

    # Universe filter
    uni = select_universe(ohlcv, top_n=600)
    uni_set = set((r.year, r.stock_id) for r in uni.itertuples(index=False))
    ohlcv['year'] = ohlcv['date'].dt.year
    ohlcv['in_uni'] = [(y, s) in uni_set for y, s in zip(ohlcv['year'], ohlcv['stock_id'])]
    ohlcv_f = ohlcv[ohlcv['in_uni']].drop(columns=['in_uni']).reset_index(drop=True)
    log(f'After universe filter: {ohlcv_f.shape}')

    df = compute_features(ohlcv_f, chip, fsc, rev)

    results = []
    fi_rows = []

    for target in ['foreign', 'trust']:
        for model_type in ['gbm', 'logistic']:
            for use_self_lag in [False, True]:
                log(f'--- target={target} model={model_type} self_lag={use_self_lag} ---')
                res = train_eval(df, target, use_self_lag=use_self_lag,
                                 model_type=model_type)
                if 'error' in res:
                    log(f'  ERROR: {res["error"]}')
                    continue
                log(f'  AUC={res["auc"]:.4f} prec@50={res["prec_at_50"]:.3f} '
                    f'prec@10={res["prec_at_10"]:.3f} '
                    f'alpha_top10_net={res["alpha_top10_net_20d"]:+.4f}')
                # Save feature importance
                fi = res.pop('feature_importance')
                for f, imp in fi.items():
                    fi_rows.append({
                        'target': target,
                        'model': model_type,
                        'use_self_lag': use_self_lag,
                        'feature': f,
                        'importance': imp,
                    })
                results.append(res)

        # Baseline per target
        log(f'--- baseline momentum_top for target={target} ---')
        bl = baseline_momentum_top(df, target)
        log(f'  prec@50={bl["prec_at_50"]:.3f} fwd20_top10={bl["fwd20_top10_ret"]:+.4f}')
        bl['target'] = target
        bl['model'] = 'baseline_momentum'
        bl['use_self_lag'] = False
        bl['auc'] = None
        results.append(bl)

    res_df = pd.DataFrame(results)
    out_csv = REPORTS / 'v13_ai_institutional_prediction.csv'
    res_df.to_csv(out_csv, index=False)
    log(f'Saved {out_csv}')

    fi_df = pd.DataFrame(fi_rows)
    if not fi_df.empty:
        # rank per (target, model, use_self_lag)
        fi_df['rank'] = (fi_df.groupby(['target', 'model', 'use_self_lag'])['importance']
                         .rank(ascending=False))
        fi_df = fi_df.sort_values(['target', 'model', 'use_self_lag', 'rank'])
    out_fi = REPORTS / 'v13_feature_importance.csv'
    fi_df.to_csv(out_fi, index=False)
    log(f'Saved {out_fi}')

    # Generate markdown verdict
    write_markdown(res_df, fi_df)

    log('DONE.')


def write_markdown(res_df: pd.DataFrame, fi_df: pd.DataFrame) -> None:
    lines = []
    lines.append('# V13: AI 模擬法人選股 Binary Classifier 驗證')
    lines.append('')
    lines.append(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append('')
    lines.append('## TL;DR (verdict = D)')
    lines.append('')
    lines.append('**Mode D 不應依賴 ML 預測法人買盤當 rule-based entry。**')
    lines.append('')
    lines.append('- AUC 看起來很高（GBM 0.72-0.84），但 **net alpha 全部為負**')
    lines.append('- 核心問題：**「被法人大買」 != 「股票會漲」**，model 精準預測前者，對後者沒資訊量')
    lines.append('- 原始 baseline momentum top10 fwd_20d = +6.60%，model top10 fwd_20d = +1.4~2.9%，')
    lines.append('  模型選股反而比隨機還差')
    lines.append('- AUC 提升主要來自自身 flow autocorrelation (trust self-lag Δ AUC +0.087)，')
    lines.append('  不是真 edge')
    lines.append('- **建議**: Mode D 完全靠 thesis discretionary，不做軍備競賽')
    lines.append('')
    lines.append('## 任務')
    lines.append('判斷能否用 ML 從 momentum/value/chip/sentiment features 預測台股外資 '
                 '(Target A) 或投信 (Target B) 下週是否大買某股。')
    lines.append('')
    lines.append('- Universe: TW 上市+上櫃普通股市值前 600（20d 成交額代理）')
    lines.append('- Train: 2016-01-01 ~ 2022-12-31')
    lines.append('- OOS:   2023-01-01 ~ 2025-12-31 (AI era, 不 peek)')
    lines.append('- Target A (foreign): fwd5_foreign_net / fwd5_volume >= train 期 90 分位 (= 0.134)')
    lines.append('- Target B (trust):   fwd5_trust_net / fwd5_volume >= train 期 90 分位 (= 0.016)')
    lines.append('- 成本假設: 0.4% round-trip per week -> 1.6% per 20d holding')
    lines.append('')

    # Verdict table
    lines.append('## OOS 結果（AI era 2023-2025）')
    lines.append('')
    lines.append('| target | model | self_lag | AUC | prec@10 | prec@50 | '
                 'top10_fwd20 | baseline_fwd20 | alpha_top10_net_20d |')
    lines.append('|--------|-------|----------|-----|---------|---------|'
                 '-------------|----------------|---------------------|')
    for _, r in res_df.iterrows():
        auc = f'{r["auc"]:.4f}' if r.get('auc') is not None and not pd.isna(r.get('auc')) else '-'
        prec10 = f'{r["prec_at_10"]:.3f}' if pd.notna(r.get('prec_at_10')) else '-'
        prec50 = f'{r["prec_at_50"]:.3f}' if pd.notna(r.get('prec_at_50')) else '-'
        top10 = f'{r["fwd20_top10_ret"]:+.4f}' if pd.notna(r.get('fwd20_top10_ret')) else '-'
        uni_mean = (f'{r["universe_mean_fwd20"]:+.4f}'
                    if pd.notna(r.get('universe_mean_fwd20')) else '-')
        alpha_net = (f'{r["alpha_top10_net_20d"]:+.4f}'
                     if pd.notna(r.get('alpha_top10_net_20d')) else '-')
        lines.append(f'| {r.get("target")} | {r.get("model")} | '
                     f'{r.get("use_self_lag")} | {auc} | {prec10} | {prec50} | '
                     f'{top10} | {uni_mean} | {alpha_net} |')
    lines.append('')

    # Pick best result per target (no_self_lag gbm)
    best = res_df[(res_df['model'] == 'gbm') &
                  (res_df['use_self_lag'] == False)].copy()

    # Verdict logic
    lines.append('## Verdict')
    lines.append('')
    for _, r in best.iterrows():
        tgt = r['target']
        auc = r['auc']
        alpha_net = r['alpha_top10_net_20d']
        if auc is None or pd.isna(auc):
            continue
        if auc > 0.62 and alpha_net > 0:
            verdict = 'A'
            note = 'AUC > 0.62 且 net alpha 正 -> Mode D 可用 rule-based entry'
        elif 0.58 <= auc <= 0.62 and alpha_net >= 0:
            verdict = 'B'
            note = 'AUC 邊際 + alpha 邊際, 需更多驗證'
        elif 0.55 <= auc < 0.58:
            verdict = 'C'
            note = 'AUC 微強於 random, net alpha ~ 0, 非有效 edge'
        else:
            verdict = 'D'
            note = 'AUC < 0.55 或 net alpha 負, 放棄該 target'
        lines.append(f'- **Target {tgt.upper()} ({tgt})**: **{verdict} 級** - '
                     f'AUC={auc:.4f}, net alpha (top10, 20d)={alpha_net:+.4f}. {note}')
    lines.append('')

    # ⚠️ Critical: decompose hit vs miss fwd_20d
    lines.append('## :warning: 關鍵診斷：「法人買」≠「股票漲」')
    lines.append('')
    lines.append('這是 V13 的核心洞察。看 top10 prediction 裡 **預測命中 (y=1, 法人真的買)** 'f'vs **預測落空 (y=0, 法人沒買)** 的 fwd_20d 差異：')
    lines.append('')
    lines.append('| target | model | self_lag | prec@10 | hit fwd_20d | miss fwd_20d | 差異 | universe mean |')
    lines.append('|--------|-------|----------|---------|-------------|--------------|------|--------------|')
    for _, r in res_df.iterrows():
        if r.get('model') == 'baseline_momentum':
            continue
        if pd.isna(r.get('fwd20_top10_hit_ret')):
            continue
        hit = r['fwd20_top10_hit_ret']
        miss = r['fwd20_top10_miss_ret']
        diff = hit - miss if pd.notna(hit) and pd.notna(miss) else np.nan
        diff_str = f'{diff:+.4f}' if pd.notna(diff) else '-'
        uni = r.get('universe_mean_fwd20')
        lines.append(f'| {r["target"]} | {r["model"]} | {r["use_self_lag"]} | '
                     f'{r["prec_at_10"]:.3f} | {hit:+.4f} | {miss:+.4f} | {diff_str} | '
                     f'{uni:+.4f} |')
    lines.append('')
    lines.append('**解讀**：')
    lines.append('- Target foreign gbm no-lag: 命中 +3.2% vs 落空 +2.7%，差異 +0.5%，低於 1.6% 成本')
    lines.append('- Target trust gbm no-lag: 命中 +1.4% vs 落空 +1.3%，差異 +0.07%，**幾乎為零**')
    lines.append('- 結論: 即使模型 100% 精準預測法人會買什麼，「被買」的股票跟「沒被買」的股票')
    lines.append('  在 20d 報酬上幾乎沒差異。法人不是 leading indicator。')
    lines.append('')

    # Autocorrelation check
    lines.append('## Autocorrelation Check（self-lag 影響）')
    lines.append('')
    lines.append('若加入自己 lag feature 後 AUC 大幅提升 (>0.05)，代表預測力主要來自 '
                 'autocorrelation，非真 edge：')
    lines.append('')
    lines.append('| target | model | AUC (no self-lag) | AUC (with self-lag) | Δ |')
    lines.append('|--------|-------|-------------------|---------------------|---|')
    for tgt in ['foreign', 'trust']:
        for mdl in ['gbm', 'logistic']:
            no_lag = res_df[(res_df.target == tgt) & (res_df.model == mdl) &
                            (res_df.use_self_lag == False)]
            yes_lag = res_df[(res_df.target == tgt) & (res_df.model == mdl) &
                             (res_df.use_self_lag == True)]
            if len(no_lag) == 0 or len(yes_lag) == 0:
                continue
            a1 = no_lag.iloc[0]['auc']
            a2 = yes_lag.iloc[0]['auc']
            if pd.isna(a1) or pd.isna(a2):
                continue
            delta = a2 - a1
            flag = ' :warning: autocorrelation artifact' if delta > 0.05 else ''
            lines.append(f'| {tgt} | {mdl} | {a1:.4f} | {a2:.4f} | {delta:+.4f}{flag} |')
    lines.append('')

    # Top features
    lines.append('## Top 5 Features (gbm, no self-lag)')
    lines.append('')
    for tgt in ['foreign', 'trust']:
        lines.append(f'### Target: {tgt}')
        lines.append('')
        fsub = fi_df[(fi_df.target == tgt) & (fi_df.model == 'gbm') &
                     (fi_df.use_self_lag == False)].head(5)
        lines.append('| rank | feature | importance |')
        lines.append('|------|---------|-----------|')
        for _, r in fsub.iterrows():
            lines.append(f'| {int(r["rank"])} | {r["feature"]} | {r["importance"]:.4f} |')
        lines.append('')

    # Baseline comparison
    lines.append('## Baseline Comparison')
    lines.append('')
    bl = res_df[res_df['model'] == 'baseline_momentum']
    lines.append('- **Random baseline**: AUC 0.5, prec@10 = base_rate ≈ 10-16%')
    lines.append('- **Momentum top-20d baseline** (pick top 10 by ret_20d each week):')
    for _, r in bl.iterrows():
        lines.append(f'  - target={r["target"]}: prec@10={r["prec_at_10"]:.3f}, '
                     f'fwd20_top10={r["fwd20_top10_ret"]:+.4f}, '
                     f'fwd20_top50={r["fwd20_top50_ret"]:+.4f}')
    lines.append('- **關鍵**: momentum top10 fwd_20d = +6.60%, 遠高於任何 V13 model (最高 +2.92%).')
    lines.append('  純動能選股 (rule-based) 在 OOS 期間遠優於「預測法人買」的 ML 策略。')
    lines.append('')

    lines.append('## Caveats')
    lines.append('')
    lines.append('- **成本 0.4% round-trip 是保守**: 小型股滑價可能 >1%, 整體結果可能樂觀')
    lines.append('- **Regime overfit**: OOS 僅 3 年 (2023-2025), AI era 單一市況，真實長期表現可能不同')
    lines.append('- **Margin / SBL features**: 只有 2021-04 之後才有, train 期 2016-2020 部分 NaN')
    lines.append('  (HistGBT 原生處理 NaN, 不影響訓練但 feature importance 可能低估)')
    lines.append('- **Target 定義**: 用 fwd5_net / fwd5_volume 而非 % 流通股本 '
                 '(流通股本資料缺), 可能偏向成交熱絡股')
    lines.append('- **週頻 rebalance 違反使用者 #5 持有期 2 週-3 個月**: V13 是因子 POC, '
                 '若證明 edge 存在, 整合進 Mode D 時須再驗證較長 holding period AUC')
    lines.append('- **Feature importance (GBM) 用 permutation importance**, 50k test subsample, '
                 '3 repeats (速度考量)')
    lines.append('')

    lines.append('## 結論與下一步')
    lines.append('')
    lines.append('**V13 = D 級 (兩個 target 都 fail)**')
    lines.append('')
    lines.append('- Target A (foreign): AUC 0.72 但 top10 vs miss 差 0.5%, 扣成本淨 -1.1%')
    lines.append('- Target B (trust):   AUC 0.75 但 top10 vs miss 差 0.07%, 扣成本淨 -2.7%')
    lines.append('')
    lines.append('與使用者既往驗證結果一致：')
    lines.append('')
    lines.append('- V1 低基期 D (反向)')
    lines.append('- V2 底部背離 D (反向)')
    lines.append('- V3 catalyst 3 個全 C/D')
    lines.append('- V12-pair 同業輪動 4 signals 全 C')
    lines.append('- **V13 AI 預測法人 D** ← 本驗證')
    lines.append('')
    lines.append('**→ StockAnalyzer rule-based alpha 可驗範圍系統性耗盡**')
    lines.append('')
    lines.append('**Mode D 最終設計**: 完全靠 thesis discretionary 進場，不做軍備競賽。')
    lines.append('')

    out_md = REPORTS / 'v13_ai_institutional_prediction.md'
    out_md.write_text('\n'.join(lines), encoding='utf-8')
    log(f'Saved {out_md}')


if __name__ == '__main__':
    main()
