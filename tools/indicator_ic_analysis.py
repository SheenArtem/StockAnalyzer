"""
Phase 2a: 技術指標 IC 驗證（含 MC 顯著性）

讀取 data_cache/backtest/ohlcv_tw.parquet，對 14 個技術指標計算訊號，
評估其對 1/5/10/20 日 forward return 的預測力（截面 Spearman IC）。

每個 (指標, horizon, universe, regime) 組合輸出：
- mean_IC
- IC_IR (mean/std)
- 勝率 (positive_rate)
- t-stat p-value
- Block bootstrap 95% CI
- n_days

輸出: reports/indicator_ic_matrix.csv

用法:
    python tools/indicator_ic_analysis.py                 # 全量 1972 檔
    python tools/indicator_ic_analysis.py --sample 200    # 測試: 隨機 200 檔
    python tools/indicator_ic_analysis.py --since 2020    # 限縮期間
"""
import argparse
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

PARQUET_PATH = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "indicator_ic_matrix.csv"
OUT_DAILY_IC = OUT_DIR / "indicator_ic_daily.parquet"

HORIZONS = [1, 5, 10, 20, 40, 60]
MIN_CROSS_SECTION = 30          # 每日至少 N 檔股票才算 IC
MIN_HISTORY = 200               # 每檔至少 N 日資料才納入
BOOTSTRAP_N = 1000              # block bootstrap 抽樣次數
BOOTSTRAP_BLOCK = 20            # block size (保留 IC 時序自相關)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("ic")


# ============================================================
# 1. 載入 + 指標計算
# ============================================================
def load_ohlcv(sample=None, since=None):
    logger.info(f"Loading {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    df['date'] = pd.to_datetime(df['date'])
    if since:
        df = df[df['date'] >= since].copy()

    # 欄位轉 numeric（防 pyarrow 型別飄）
    for col in ['Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 過濾最低歷史
    counts = df.groupby('yf_ticker').size()
    keep = counts[counts >= MIN_HISTORY].index
    df = df[df['yf_ticker'].isin(keep)].copy()

    if sample:
        rng = np.random.default_rng(42)
        tickers = df['yf_ticker'].unique()
        chosen = rng.choice(tickers, min(sample, len(tickers)), replace=False)
        df = df[df['yf_ticker'].isin(chosen)].copy()

    df = df.sort_values(['yf_ticker', 'date']).reset_index(drop=True)
    logger.info(f"Loaded {len(df):,} rows, {df['yf_ticker'].nunique()} tickers, "
                f"date range {df['date'].min().date()} ~ {df['date'].max().date()}")
    return df


def _compute_one_ticker(sub):
    """對單一 ticker 計算 14 個訊號。輸入必須按 date 排序。"""
    from ta.trend import MACD, ADXIndicator, EMAIndicator, SMAIndicator
    from ta.momentum import RSIIndicator, StochasticOscillator
    from ta.volatility import BollingerBands, AverageTrueRange
    from ta.volume import OnBalanceVolumeIndicator, ForceIndexIndicator

    close = sub['Close']
    high = sub['High']
    low = sub['Low']
    volume = sub['Volume']

    try:
        # ---- 趨勢 ----
        ma20 = SMAIndicator(close, n=20).sma_indicator()
        ma60 = SMAIndicator(close, n=60).sma_indicator()
        sub['sig_ma20_dev'] = close / ma20 - 1          # % 離 MA20
        sub['sig_ma_align'] = ma20 / ma60 - 1           # 多頭排列強度

        # Supertrend 簡化版：HL2 ± ATR*3
        atr = AverageTrueRange(high, low, close, n=14).average_true_range()
        hl2 = (high + low) / 2
        st_upper = hl2 + 3 * atr
        st_lower = hl2 - 3 * atr
        st_dir = pd.Series(np.where(close > st_upper.shift(1), 1,
                           np.where(close < st_lower.shift(1), -1, 0)),
                           index=close.index).replace(0, method='ffill').fillna(0)
        sub['sig_supertrend'] = st_dir.astype(float)

        # ADX + DMI 方向
        adx_ind = ADXIndicator(high, low, close, n=14)
        adx = adx_ind.adx()
        di_plus = adx_ind.adx_pos()
        di_minus = adx_ind.adx_neg()
        sub['sig_adx_dir'] = (di_plus - di_minus) * (adx / 50.0)  # 方向 × 強度

        # VWAP（每日 rolling，不是 intraday；用 20 日 volume-weighted）
        vwap20 = (close * volume).rolling(20).sum() / volume.rolling(20).sum()
        sub['sig_vwap_dev'] = close / vwap20 - 1

        # ---- 動能 ----
        macd_ind = MACD(close)
        macd_hist = macd_ind.macd_diff()
        sub['sig_macd_hist'] = macd_hist / close        # 標準化

        rsi = RSIIndicator(close, n=14).rsi()
        sub['sig_rsi_dev'] = rsi - 50

        stoch = StochasticOscillator(high, low, close, n=14, d_n=3)
        k = stoch.stoch()
        d = stoch.stoch_signal()
        sub['sig_kd_diff'] = k - d

        # EFI (Force Index)
        efi = ForceIndexIndicator(close, volume, n=13).force_index()
        sub['sig_efi'] = efi / (close * volume).rolling(20).mean().replace(0, np.nan)

        # TD Sequential Setup (簡化：連續 N 天 close > close[4] 計數)
        buy_setup = _td_setup(close, direction='buy')
        sell_setup = _td_setup(close, direction='sell')
        sub['sig_td_setup'] = sell_setup - buy_setup    # +: bearish setup, -: bullish

        # ---- 量能 ----
        rvol = volume / volume.rolling(20).mean()
        sub['sig_rvol_log'] = np.log(rvol.clip(lower=0.01))

        obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
        sub['sig_obv_mom'] = obv.pct_change(20)

        # ---- 波動 ----
        bb = BollingerBands(close, n=20, ndev=2)
        bb_upper = bb.bollinger_hband()
        bb_lower = bb.bollinger_lband()
        bb_mid = bb.bollinger_mavg()
        bb_width = bb_upper - bb_lower
        sub['sig_bb_pos'] = (close - bb_mid) / bb_width.replace(0, np.nan)

        sub['sig_atr_pct'] = atr / close

        # Squeeze Momentum (簡化 BB inside KC)
        kc_upper = bb_mid + 1.5 * atr
        kc_lower = bb_mid - 1.5 * atr
        squeeze_on = ((bb_upper < kc_upper) & (bb_lower > kc_lower)).astype(float)
        sub['sig_squeeze'] = squeeze_on
    except Exception as e:
        logger.warning(f"Indicator calc failed for {sub['yf_ticker'].iloc[0]}: {e}")
        for col in SIGNAL_COLS:
            if col not in sub.columns:
                sub[col] = np.nan

    return sub


def _td_setup(close, direction='buy', max_count=9):
    """TD Sequential setup 計數。buy = close < close.shift(4) 連續天數。"""
    if direction == 'buy':
        cond = close < close.shift(4)
    else:
        cond = close > close.shift(4)
    count = cond.groupby((~cond).cumsum()).cumcount() + 1
    count = count.where(cond, 0).clip(upper=max_count)
    return count.fillna(0)


SIGNAL_COLS = [
    'sig_ma20_dev', 'sig_ma_align', 'sig_supertrend', 'sig_adx_dir', 'sig_vwap_dev',
    'sig_macd_hist', 'sig_rsi_dev', 'sig_kd_diff', 'sig_efi', 'sig_td_setup',
    'sig_rvol_log', 'sig_obv_mom',
    'sig_bb_pos', 'sig_atr_pct', 'sig_squeeze',
]
SIGNAL_LABELS = {
    'sig_ma20_dev': 'MA20 偏離',
    'sig_ma_align': 'MA 排列',
    'sig_supertrend': 'Supertrend',
    'sig_adx_dir': 'ADX×方向',
    'sig_vwap_dev': 'VWAP 偏離',
    'sig_macd_hist': 'MACD Hist',
    'sig_rsi_dev': 'RSI 偏離',
    'sig_kd_diff': 'KD 差',
    'sig_efi': 'EFI',
    'sig_td_setup': 'TD Setup',
    'sig_rvol_log': 'log(RVOL)',
    'sig_obv_mom': 'OBV 動能',
    'sig_bb_pos': 'BB %B',
    'sig_atr_pct': 'ATR%',
    'sig_squeeze': 'Squeeze',
}


def compute_all_indicators(df):
    logger.info("Computing indicators for all tickers...")
    t0 = time.time()
    df = df.groupby('yf_ticker', group_keys=False).apply(_compute_one_ticker)
    logger.info(f"Indicators done in {time.time()-t0:.1f}s")
    return df


# ============================================================
# 2. Forward returns
# ============================================================
def add_fwd_returns(df):
    logger.info("Computing forward returns...")
    df = df.sort_values(['yf_ticker', 'date']).reset_index(drop=True)
    for h in HORIZONS:
        df[f'fwd_{h}d'] = df.groupby('yf_ticker')['AdjClose'].pct_change(h).shift(-h)
    return df


# ============================================================
# 3. Regime detection (大盤 index HMM 簡化版)
# ============================================================
def add_regime(df, index_ticker='^TWII'):
    """
    簡化 regime: 用市場整體加權日報酬（用樣本中所有股票均值 proxy TAIEX）
    三分類: trending / ranging / volatile
    """
    logger.info("Detecting market regime...")
    # 用全市場日均報酬 proxy
    daily = df.groupby('date')['AdjClose'].apply(
        lambda s: s.pct_change().mean()
    ).rename('mkt_ret').reset_index()
    daily['abs_ret'] = daily['mkt_ret'].abs()
    daily['vol_20d'] = daily['mkt_ret'].rolling(20).std()
    daily['trend_20d'] = daily['mkt_ret'].rolling(20).mean()

    # 簡化分類規則：
    #   volatile: vol > 75th percentile
    #   trending: |trend| > mean + 0.5 std
    #   else: ranging
    vol_75 = daily['vol_20d'].quantile(0.75)
    trend_abs = daily['trend_20d'].abs()
    trend_thresh = trend_abs.mean() + 0.5 * trend_abs.std()

    def classify(row):
        if pd.isna(row['vol_20d']):
            return 'unknown'
        if row['vol_20d'] > vol_75:
            return 'volatile'
        if abs(row['trend_20d']) > trend_thresh:
            return 'trending'
        return 'ranging'

    daily['regime'] = daily.apply(classify, axis=1)
    df = df.merge(daily[['date', 'regime']], on='date', how='left')

    logger.info(f"Regime distribution:")
    print(df.drop_duplicates('date')['regime'].value_counts().to_string())
    return df


# ============================================================
# 4. Universe tagging
# ============================================================
def add_universe_flags(df):
    """加 in_momentum_universe 旗標（當日市值 proxy + 流動性 + 上市 3y）。"""
    logger.info("Tagging momentum universe...")
    # 市值 proxy = Close × 20 日均量 (沒有 shares_outstanding)
    # 流動性 = 20 日均成交額 (Close × Volume)
    df['amount'] = df['Close'] * df['Volume']
    df['avg_amount_20d'] = df.groupby('yf_ticker')['amount'].transform(
        lambda s: s.rolling(20).mean()
    )
    # 上市 >= 750 日 (3 年)
    df['listing_days'] = df.groupby('yf_ticker').cumcount()
    df['in_momentum_universe'] = (
        (df['avg_amount_20d'] >= 3e7)   # 3000 萬
        & (df['listing_days'] >= 750)
    )
    return df


# ============================================================
# 5. Cross-sectional IC + 統計
# ============================================================
def compute_daily_ic(df, signal_col, return_col, universe_filter=None, regime=None):
    """每日截面 Spearman IC 時序。"""
    x = df
    if universe_filter is not None:
        x = x[x[universe_filter] == True]
    if regime is not None:
        x = x[x['regime'] == regime]
    x = x.dropna(subset=[signal_col, return_col])

    ics = []
    dates = []
    for date, g in x.groupby('date'):
        if len(g) < MIN_CROSS_SECTION:
            continue
        try:
            ic, _ = stats.spearmanr(g[signal_col], g[return_col])
            if not np.isnan(ic):
                ics.append(ic)
                dates.append(date)
        except Exception:
            continue
    return pd.Series(ics, index=dates, name='ic')


def block_bootstrap_ci(ic_series, n_boot=BOOTSTRAP_N, block_size=BOOTSTRAP_BLOCK):
    """Block bootstrap 保留自相關 → mean IC 的 95% CI。"""
    arr = ic_series.values
    n = len(arr)
    if n < block_size * 2:
        return (np.nan, np.nan)
    rng = np.random.default_rng(42)
    n_blocks = n // block_size
    boot_means = []
    for _ in range(n_boot):
        starts = rng.integers(0, n - block_size, n_blocks)
        sample = np.concatenate([arr[s:s+block_size] for s in starts])
        boot_means.append(sample.mean())
    return (float(np.percentile(boot_means, 2.5)), float(np.percentile(boot_means, 97.5)))


def summarize_ic(ic_series):
    """對一個 daily IC 時序輸出 mean / std / IR / p-value / CI / 勝率。"""
    if len(ic_series) == 0:
        return dict(mean=np.nan, std=np.nan, ir=np.nan, p=np.nan,
                    ci_low=np.nan, ci_high=np.nan, win_rate=np.nan, n=0)
    arr = ic_series.dropna().values
    n = len(arr)
    if n < 20:
        return dict(mean=arr.mean() if n > 0 else np.nan, std=np.nan, ir=np.nan,
                    p=np.nan, ci_low=np.nan, ci_high=np.nan, win_rate=np.nan, n=n)
    m = arr.mean()
    s = arr.std(ddof=1)
    ir = m / s if s > 0 else np.nan
    # t-stat for mean != 0
    t_stat = m * np.sqrt(n) / s if s > 0 else 0
    p = 2 * (1 - stats.t.cdf(abs(t_stat), df=n - 1))
    win_rate = (arr > 0).mean() * 100
    ci_low, ci_high = block_bootstrap_ci(ic_series)
    return dict(mean=float(m), std=float(s), ir=float(ir), p=float(p),
                ci_low=ci_low, ci_high=ci_high, win_rate=float(win_rate), n=int(n))


# ============================================================
# 6. Main
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', type=int, default=None,
                        help='Random sample N tickers (for testing)')
    parser.add_argument('--since', type=str, default=None,
                        help='Start date YYYY-MM-DD')
    args = parser.parse_args()

    t0 = time.time()
    df = load_ohlcv(sample=args.sample, since=args.since)

    df = compute_all_indicators(df)
    df = add_fwd_returns(df)
    df = add_regime(df)
    df = add_universe_flags(df)

    # 儲存 daily IC series（for 後續 combo analysis）
    daily_ic_rows = []

    results = []
    scopes = [
        ('all', None),
        ('momentum', 'in_momentum_universe'),
    ]
    regimes = [None, 'trending', 'ranging', 'volatile']

    logger.info("Running IC matrix...")
    t_ic = time.time()
    for sig in SIGNAL_COLS:
        for h in HORIZONS:
            for scope_name, scope_col in scopes:
                for reg in regimes:
                    ic = compute_daily_ic(
                        df, sig, f'fwd_{h}d',
                        universe_filter=scope_col, regime=reg,
                    )
                    stats_dict = summarize_ic(ic)
                    results.append({
                        'indicator': sig,
                        'label': SIGNAL_LABELS[sig],
                        'horizon': h,
                        'universe': scope_name,
                        'regime': reg if reg else 'all',
                        **stats_dict,
                    })
                    # Save daily IC for overall scope (all, no regime)
                    if scope_name == 'all' and reg is None and len(ic) > 0:
                        tmp = ic.reset_index()
                        tmp.columns = ['date', 'ic']
                        tmp['indicator'] = sig
                        tmp['horizon'] = h
                        daily_ic_rows.append(tmp)

    logger.info(f"IC computation done in {time.time()-t_ic:.1f}s")

    # Output matrix
    out_df = pd.DataFrame(results)
    out_df = out_df.sort_values(['universe', 'regime', 'horizon', 'mean'],
                                ascending=[True, True, True, False])
    out_df.to_csv(OUT_CSV, index=False, encoding='utf-8-sig')
    logger.info(f"Matrix saved: {OUT_CSV} ({len(out_df)} rows)")

    if daily_ic_rows:
        daily_ic_df = pd.concat(daily_ic_rows, ignore_index=True)
        daily_ic_df.to_parquet(OUT_DAILY_IC)
        logger.info(f"Daily IC saved: {OUT_DAILY_IC}")

    # Print top significant signals
    sig_df = out_df[
        (out_df['universe'] == 'all') & (out_df['regime'] == 'all')
    ].copy()
    sig_df['sig'] = (sig_df['p'] < 0.05) & (sig_df['ci_low'] * sig_df['ci_high'] > 0)
    print("\n" + "=" * 90)
    print("  IC Matrix — universe=all, regime=all")
    print("=" * 90)
    print(f"  {'Indicator':<20} {'H':>3} {'mean_IC':>8} {'IR':>7} "
          f"{'95% CI':<18} {'p':>8} {'win%':>6} {'n':>5} {'sig':>4}")
    print("  " + "-" * 86)
    for _, r in sig_df.iterrows():
        sig_flag = "*" if r['sig'] else "."
        ci_str = f"[{r['ci_low']:+.3f}, {r['ci_high']:+.3f}]" if pd.notna(r['ci_low']) else ""
        # 中文標籤的顯示寬度處理（每個中文視為 2 字元）
        label = r['label']
        label_width = sum(2 if ord(c) > 127 else 1 for c in label)
        pad = max(0, 20 - label_width)
        print(f"  {label}{' ' * pad} {r['horizon']:>3d} "
              f"{r['mean']:>+8.4f} {r['ir']:>+7.3f} {ci_str:<18} "
              f"{r['p']:>8.4f} {r['win_rate']:>5.1f}% "
              f"{r['n']:>5d}  {sig_flag}")
    print("=" * 90)
    print(f"\nTotal time: {(time.time()-t0)/60:.1f} min")


if __name__ == '__main__':
    main()
