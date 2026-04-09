# filename: ml_signal.py
# XGBoost 訊號分類模型 — 結合規則引擎與機器學習的 ensemble 評分

import os
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# 嘗試載入 ML 依賴，若未安裝則降級運作
_ML_AVAILABLE = False
try:
    import xgboost as xgb
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
    import joblib
    _ML_AVAILABLE = True
except ImportError:
    logger.warning(
        "xgboost / scikit-learn / joblib not installed. "
        "MLSignalClassifier will return default values. "
        "Install with: pip install xgboost scikit-learn joblib"
    )

# tqdm 為非必要依賴
try:
    from tqdm import tqdm as _tqdm
    _HAS_TQDM = True
except ImportError:
    _HAS_TQDM = False


# ---------------------------------------------------------------------------
# 特徵欄位定義 (供外部參考)
# ---------------------------------------------------------------------------
FEATURE_COLUMNS = [
    'feat_bias',              # 乖離率
    'feat_close_vs_ma20',     # 收盤 / MA20 比值
    'feat_close_vs_ma60',     # 收盤 / MA60 比值
    'feat_rsi',               # RSI(14)
    'feat_k',                 # KD-K
    'feat_d',                 # KD-D
    'feat_macd_hist',         # MACD 柱狀體
    'feat_efi_norm',          # 歸一化 EFI_EMA13
    'feat_rvol',              # 相對量
    'feat_obv_chg_rate',      # OBV 5日變化率
    'feat_atr_ratio',         # ATR / Close
    'feat_bb_width',          # 布林帶寬 / Close
    'feat_adx',               # ADX
    'feat_plus_di',           # +DI
    'feat_minus_di',          # -DI
    'feat_supertrend_dir',    # Supertrend 方向 (1/-1)
    'feat_squeeze_on',        # Squeeze 壓縮中 (0/1)
]


def _clip_feature(series: pd.Series, n_std: float = 5.0) -> pd.Series:
    """
    以簡易 clip 方式處理極端值，避免 StandardScaler 在時間序列上的 data leakage。
    將值限制在 [-n_std * std, +n_std * std] 區間。
    """
    mean = series.mean()
    std = series.std()
    if std == 0 or pd.isna(std):
        return series
    lower = mean - n_std * std
    upper = mean + n_std * std
    return series.clip(lower=lower, upper=upper)


class MLSignalClassifier:
    """XGBoost-based signal classifier for stock trading.

    結合技術指標特徵訓練二分類模型，預測未來 N 日是否有正報酬，
    並與規則引擎分數進行 ensemble 加權。
    """

    def __init__(self, model_dir: str = 'ml_models'):
        """Initialize classifier.

        Args:
            model_dir: 模型儲存目錄，若不存在會自動建立。
        """
        self.model_dir = model_dir
        self.model = None
        self.feature_columns = FEATURE_COLUMNS.copy()

        # 建立模型目錄
        if not os.path.exists(self.model_dir):
            try:
                os.makedirs(self.model_dir, exist_ok=True)
                logger.info("Created model directory: %s", self.model_dir)
            except OSError as e:
                logger.error("Failed to create model directory %s: %s", self.model_dir, e)

    # ------------------------------------------------------------------
    # 特徵工程
    # ------------------------------------------------------------------
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and normalize features from a DataFrame with technical indicators.

        Args:
            df: DataFrame output from ``calculate_all_indicators``.

        Returns:
            DataFrame of features (one row per trading day), indexed same as input.
        """
        if df is None or df.empty:
            logger.warning("prepare_features: empty input DataFrame")
            return pd.DataFrame(columns=self.feature_columns)

        features = pd.DataFrame(index=df.index)

        # --- Price features ---
        features['feat_bias'] = df.get('BIAS', pd.Series(0.0, index=df.index))

        ma20 = df.get('MA20', pd.Series(np.nan, index=df.index))
        ma60 = df.get('MA60', pd.Series(np.nan, index=df.index))
        close = df['Close']

        features['feat_close_vs_ma20'] = close / ma20.replace(0, np.nan)
        features['feat_close_vs_ma60'] = close / ma60.replace(0, np.nan)

        # --- Momentum ---
        features['feat_rsi'] = df.get('RSI', pd.Series(50.0, index=df.index))
        features['feat_k'] = df.get('K', pd.Series(50.0, index=df.index))
        features['feat_d'] = df.get('D', pd.Series(50.0, index=df.index))
        features['feat_macd_hist'] = df.get('Hist', pd.Series(0.0, index=df.index))

        # EFI_EMA13 歸一化：除以 20 日成交量均值以消除股本差異
        efi = df.get('EFI_EMA13', pd.Series(0.0, index=df.index))
        vol_ma20 = df['Volume'].rolling(window=20).mean().replace(0, np.nan)
        features['feat_efi_norm'] = efi / vol_ma20

        # --- Volume ---
        features['feat_rvol'] = df.get('RVOL', pd.Series(1.0, index=df.index))

        obv = df.get('OBV', pd.Series(0.0, index=df.index))
        obv_lag5 = obv.shift(5).replace(0, np.nan)
        features['feat_obv_chg_rate'] = (obv - obv_lag5) / obv_lag5.abs()

        # --- Volatility ---
        atr = df.get('ATR', pd.Series(0.0, index=df.index))
        features['feat_atr_ratio'] = atr / close.replace(0, np.nan)

        bb_up = df.get('BB_Up', pd.Series(np.nan, index=df.index))
        bb_lo = df.get('BB_Lo', pd.Series(np.nan, index=df.index))
        features['feat_bb_width'] = (bb_up - bb_lo) / close.replace(0, np.nan)

        # --- Trend ---
        features['feat_adx'] = df.get('ADX', pd.Series(0.0, index=df.index))
        features['feat_plus_di'] = df.get('+DI', pd.Series(0.0, index=df.index))
        features['feat_minus_di'] = df.get('-DI', pd.Series(0.0, index=df.index))
        features['feat_supertrend_dir'] = df.get('Supertrend_Dir', pd.Series(0.0, index=df.index))

        # --- Pattern ---
        squeeze = df.get('Squeeze_On', pd.Series(False, index=df.index))
        features['feat_squeeze_on'] = squeeze.astype(float)

        # 對連續型特徵做 clip 處理（排除方向型 0/1 欄位）
        skip_clip = {'feat_supertrend_dir', 'feat_squeeze_on'}
        for col in features.columns:
            if col not in skip_clip:
                features[col] = _clip_feature(features[col])

        # 確保欄位順序一致
        for col in self.feature_columns:
            if col not in features.columns:
                features[col] = 0.0
        features = features[self.feature_columns]

        return features

    # ------------------------------------------------------------------
    # 標籤生成
    # ------------------------------------------------------------------
    def prepare_labels(self, df: pd.DataFrame, forward_days: int = 5,
                       threshold_pct: float = 2.0) -> pd.Series:
        """Create classification labels based on forward returns.

        Args:
            df: DataFrame with 'Close' column.
            forward_days: 向前看的交易天數。
            threshold_pct: 報酬門檻百分比。

        Returns:
            Series of labels (1 = positive signal, 0 = negative/neutral).
        """
        if df is None or df.empty or 'Close' not in df.columns:
            return pd.Series(dtype=int)

        close = df['Close']

        # 計算未來 forward_days 日內的最大收盤價
        max_forward_close = close.shift(-1).rolling(window=forward_days, min_periods=1).max().shift(-forward_days + 1)

        # 未來最大報酬率
        forward_return = (max_forward_close - close) / close * 100.0

        # 標籤: 1 = 未來有超過 threshold 的上漲空間
        labels = (forward_return > threshold_pct).astype(int)
        labels.name = 'label'

        return labels

    # ------------------------------------------------------------------
    # 訓練
    # ------------------------------------------------------------------
    def train(self, df: pd.DataFrame, forward_days: int = 5,
              threshold_pct: float = 2.0, test_ratio: float = 0.2) -> dict:
        """Train XGBoost classifier on historical data.

        使用 rolling-window 切分（前段訓練、後段測試），避免時間序列 lookahead bias。

        Args:
            df: DataFrame with technical indicators (from calculate_all_indicators).
            forward_days: 標籤用前瞻天數。
            threshold_pct: 報酬門檻 %。
            test_ratio: 測試集比例（取最後 N% 資料）。

        Returns:
            dict with accuracy, precision, recall, f1, feature_importance, test_predictions.
        """
        default_result = {
            'accuracy': 0.0, 'precision': 0.0, 'recall': 0.0, 'f1': 0.0,
            'feature_importance': {}, 'test_predictions': np.array([])
        }

        if not _ML_AVAILABLE:
            logger.warning("ML libraries not available, skipping training")
            return default_result

        # 準備特徵與標籤
        features = self.prepare_features(df)
        labels = self.prepare_labels(df, forward_days=forward_days, threshold_pct=threshold_pct)

        if features.empty or labels.empty:
            logger.warning("Cannot train: empty features or labels")
            return default_result

        # 對齊索引，移除 NaN 行
        combined = features.join(labels, how='inner').dropna()
        if len(combined) < 50:
            logger.warning("Insufficient data for training (%d rows). Need at least 50.", len(combined))
            return default_result

        X = combined[self.feature_columns]
        y = combined['label']

        # 時間序列切分 — 不做 shuffle
        split_idx = int(len(X) * (1 - test_ratio))
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

        logger.info(
            "Training XGBoost: %d train / %d test samples, label-1 ratio: %.1f%%",
            len(X_train), len(X_test), y_train.mean() * 100
        )

        # XGBoost 參數
        params = {
            'n_estimators': 200,
            'max_depth': 5,
            'learning_rate': 0.05,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_weight': 5,
            'use_label_encoder': False,
            'eval_metric': 'logloss',
            'random_state': 42,
        }

        model = xgb.XGBClassifier(**params)

        # 使用 eval_set 監控測試集指標
        if _HAS_TQDM:
            # 在 verbose_eval 的回合裡印出進度
            logger.info("Training started (200 rounds)...")
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )

        self.model = model

        # 測試集預測
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        acc = accuracy_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)

        # 特徵重要性
        importance_raw = model.feature_importances_
        feat_imp = dict(zip(self.feature_columns, importance_raw.tolist()))
        # 降序排列
        feat_imp = dict(sorted(feat_imp.items(), key=lambda x: x[1], reverse=True))

        logger.info(
            "Training complete -- Acc: %.3f  Prec: %.3f  Rec: %.3f  F1: %.3f",
            acc, prec, rec, f1
        )

        return {
            'accuracy': acc,
            'precision': prec,
            'recall': rec,
            'f1': f1,
            'feature_importance': feat_imp,
            'test_predictions': y_prob,
        }

    # ------------------------------------------------------------------
    # 預測
    # ------------------------------------------------------------------
    def predict(self, df: pd.DataFrame) -> pd.Series:
        """Predict probability of positive return for each trading day.

        Args:
            df: DataFrame with technical indicators.

        Returns:
            Series of probabilities (0.0 to 1.0), indexed same as input.
        """
        if not _ML_AVAILABLE or self.model is None:
            logger.warning("predict: model not available, returning 0.5 for all rows")
            return pd.Series(0.5, index=df.index, name='ml_prob')

        features = self.prepare_features(df)
        if features.empty:
            return pd.Series(0.5, index=df.index, name='ml_prob')

        # XGBoost 原生支援 NaN，直接預測
        proba = self.model.predict_proba(features)[:, 1]
        return pd.Series(proba, index=features.index, name='ml_prob')

    # ------------------------------------------------------------------
    # 評分轉換
    # ------------------------------------------------------------------
    def get_ml_score(self, df: pd.DataFrame) -> float:
        """Convert latest-day probability to a score on [-5, +5] scale.

        Args:
            df: DataFrame with technical indicators.

        Returns:
            float score for the latest trading day.
        """
        proba_series = self.predict(df)
        if proba_series.empty:
            return 0.0

        prob = proba_series.iloc[-1]

        # 機率 → 離散分數對照表
        if prob > 0.7:
            return 5.0
        elif prob > 0.6:
            return 3.0
        elif prob > 0.5:
            return 1.0
        elif prob < 0.3:
            return -5.0
        elif prob < 0.4:
            return -3.0
        elif prob < 0.5:
            return -1.0
        else:
            # prob == 0.5 exactly
            return 0.0

    # ------------------------------------------------------------------
    # Ensemble
    # ------------------------------------------------------------------
    @staticmethod
    def ensemble_score(rule_score: float, ml_score: float,
                       rule_weight: float = 0.6, ml_weight: float = 0.4) -> float:
        """Combine rule-based and ML scores with weighted average.

        Args:
            rule_score: 規則引擎分數 (from analysis_engine).
            ml_score: ML 模型分數 (from get_ml_score).
            rule_weight: 規則引擎權重。
            ml_weight: ML 模型權重。

        Returns:
            加權後的 ensemble 分數，限制在 [-10, +10]。
        """
        final = rule_score * rule_weight + ml_score * ml_weight
        return float(np.clip(final, -10.0, 10.0))

    # ------------------------------------------------------------------
    # 特徵重要性
    # ------------------------------------------------------------------
    def get_feature_importance(self) -> dict:
        """Return feature importance dict sorted descending.

        Returns:
            dict of {feature_name: importance_score}. Empty if model not trained.
        """
        if not _ML_AVAILABLE or self.model is None:
            logger.warning("get_feature_importance: model not available")
            return {}

        importance_raw = self.model.feature_importances_
        feat_imp = dict(zip(self.feature_columns, importance_raw.tolist()))
        return dict(sorted(feat_imp.items(), key=lambda x: x[1], reverse=True))

    # ------------------------------------------------------------------
    # 模型持久化
    # ------------------------------------------------------------------
    def save_model(self, ticker: str) -> bool:
        """Save trained model to disk.

        Args:
            ticker: 股票代號 (e.g. '2330' or 'AAPL').

        Returns:
            True if saved successfully.
        """
        if not _ML_AVAILABLE:
            logger.warning("save_model: joblib not available")
            return False

        if self.model is None:
            logger.warning("save_model: no trained model to save")
            return False

        safe_ticker = ticker.replace('.', '_').replace('/', '_')
        path = os.path.join(self.model_dir, f"{safe_ticker}_model.joblib")
        try:
            joblib.dump(self.model, path)
            logger.info("Model saved to %s", path)
            return True
        except Exception as e:
            logger.error("Failed to save model: %s", e)
            return False

    def load_model(self, ticker: str) -> bool:
        """Load a previously saved model from disk.

        Args:
            ticker: 股票代號。

        Returns:
            True if loaded successfully.
        """
        if not _ML_AVAILABLE:
            logger.warning("load_model: joblib not available")
            return False

        safe_ticker = ticker.replace('.', '_').replace('/', '_')
        path = os.path.join(self.model_dir, f"{safe_ticker}_model.joblib")
        if not os.path.exists(path):
            logger.info("No saved model found at %s", path)
            return False

        try:
            self.model = joblib.load(path)
            logger.info("Model loaded from %s", path)
            return True
        except Exception as e:
            logger.error("Failed to load model: %s", e)
            return False


# ======================================================================
# __main__ 測試區塊 — 使用合成資料驗證流程
# ======================================================================
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

    print("=" * 60)
    print("MLSignalClassifier -- synthetic data test")
    print("=" * 60)

    if not _ML_AVAILABLE:
        print("[SKIP] xgboost / scikit-learn not installed. Cannot run test.")
        raise SystemExit(0)

    np.random.seed(42)
    n_days = 500

    # --- 生成合成股價 ---
    dates = pd.bdate_range(start='2023-01-01', periods=n_days)
    close = 100 + np.cumsum(np.random.randn(n_days) * 0.5)
    close = np.maximum(close, 10)  # 確保正數

    df_synth = pd.DataFrame({
        'Open':   close + np.random.randn(n_days) * 0.3,
        'High':   close + abs(np.random.randn(n_days) * 1.0),
        'Low':    close - abs(np.random.randn(n_days) * 1.0),
        'Close':  close,
        'Volume': np.random.randint(1000, 50000, n_days).astype(float),
    }, index=dates)

    # --- 手動計算技術指標（簡化版，模擬 calculate_all_indicators 輸出）---
    c = df_synth['Close']
    df_synth['MA5']  = c.rolling(5).mean()
    df_synth['MA10'] = c.rolling(10).mean()
    df_synth['MA20'] = c.rolling(20).mean()
    df_synth['MA60'] = c.rolling(60).mean()

    std20 = c.rolling(20).std()
    df_synth['BB_Up'] = df_synth['MA20'] + 2 * std20
    df_synth['BB_Lo'] = df_synth['MA20'] - 2 * std20
    df_synth['BIAS'] = (c - df_synth['MA20']) / df_synth['MA20'] * 100

    delta = c.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df_synth['RSI'] = 100 - (100 / (1 + rs))

    low_min = df_synth['Low'].rolling(9).min()
    high_max = df_synth['High'].rolling(9).max()
    rsv = (c - low_min) / (high_max - low_min).replace(0, np.nan) * 100
    df_synth['K'] = rsv.ewm(com=2).mean()
    df_synth['D'] = df_synth['K'].ewm(com=2).mean()

    exp12 = c.ewm(span=12, adjust=False).mean()
    exp26 = c.ewm(span=26, adjust=False).mean()
    df_synth['MACD'] = exp12 - exp26
    df_synth['Signal'] = df_synth['MACD'].ewm(span=9, adjust=False).mean()
    df_synth['Hist'] = df_synth['MACD'] - df_synth['Signal']

    df_synth['OBV'] = (np.sign(c.diff()) * df_synth['Volume']).fillna(0).cumsum()

    prev_close = c.shift(1)
    hl = df_synth['High'] - df_synth['Low']
    hpc = abs(df_synth['High'] - prev_close)
    lpc = abs(df_synth['Low'] - prev_close)
    tr = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    df_synth['ATR'] = tr.rolling(14).mean()

    up = df_synth['High'].diff()
    down = -df_synth['Low'].diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr_smooth = tr.rolling(14).mean().replace(0, np.nan)
    df_synth['+DI'] = 100 * pd.Series(plus_dm, index=dates).rolling(14).mean() / tr_smooth
    df_synth['-DI'] = 100 * pd.Series(minus_dm, index=dates).rolling(14).mean() / tr_smooth
    dx = 100 * abs(df_synth['+DI'] - df_synth['-DI']) / (df_synth['+DI'] + df_synth['-DI']).replace(0, np.nan)
    df_synth['ADX'] = dx.rolling(14).mean()

    change = c.diff()
    df_synth['EFI'] = change * df_synth['Volume']
    df_synth['EFI_EMA13'] = df_synth['EFI'].ewm(span=13, adjust=False).mean()

    vol_ma20 = df_synth['Volume'].rolling(20).mean()
    df_synth['RVOL'] = df_synth['Volume'] / vol_ma20.replace(0, np.nan)

    df_synth['Supertrend_Dir'] = np.where(c > df_synth['MA20'], 1.0, -1.0)
    df_synth['Squeeze_On'] = (df_synth['BB_Up'] - df_synth['BB_Lo']) < (df_synth['ATR'] * 3)

    # --- 訓練模型 ---
    clf = MLSignalClassifier(model_dir='ml_models_test')
    result = clf.train(df_synth, forward_days=5, threshold_pct=2.0, test_ratio=0.2)

    print(f"\n--- Training Results ---")
    print(f"Accuracy:  {result['accuracy']:.3f}")
    print(f"Precision: {result['precision']:.3f}")
    print(f"Recall:    {result['recall']:.3f}")
    print(f"F1 Score:  {result['f1']:.3f}")

    print(f"\n--- Feature Importance (top 10) ---")
    for i, (feat, imp) in enumerate(result['feature_importance'].items()):
        if i >= 10:
            break
        print(f"  {feat:30s} {imp:.4f}")

    # --- ML score ---
    ml_score = clf.get_ml_score(df_synth)
    print(f"\n--- ML Score (latest day): {ml_score:+.1f} ---")

    # --- Ensemble ---
    rule_score_example = 3.5
    ensemble = clf.ensemble_score(rule_score_example, ml_score)
    print(f"Rule score: {rule_score_example}, ML score: {ml_score}")
    print(f"Ensemble score (0.6 * rule + 0.4 * ml): {ensemble:.2f}")

    # --- 儲存/載入 ---
    clf.save_model('TEST_SYNTH')
    clf2 = MLSignalClassifier(model_dir='ml_models_test')
    loaded = clf2.load_model('TEST_SYNTH')
    print(f"\nModel save/load round-trip: {'OK' if loaded else 'FAILED'}")

    if loaded:
        ml_score2 = clf2.get_ml_score(df_synth)
        print(f"Loaded model ML score: {ml_score2:+.1f} (should match {ml_score:+.1f})")

    # 清理測試目錄
    import shutil
    if os.path.exists('ml_models_test'):
        shutil.rmtree('ml_models_test')
        print("Test model directory cleaned up.")

    print("\n" + "=" * 60)
    print("Test complete.")
    print("=" * 60)
