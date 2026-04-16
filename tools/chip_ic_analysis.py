"""
C2-b: 籌碼因子 IC 驗證

讀取 data_cache/chip_history/ 三個 parquet + ohlcv_tw.parquet，
對 19 個籌碼因子計算截面 Spearman IC vs forward return。

因子清單:
  - inst_foreign_{5,10,20}d       外資累計買賣超 / 20d avg volume
  - inst_trust_{5,10,20}d         投信累計買賣超 / 20d avg volume
  - inst_total_{5,10,20}d         三大法人合計 / 20d avg volume
  - inst_foreign_trust_sync       外資+投信同步 (+1/-1/0)
  - margin_chg_{5,10,20}d         融資餘額 N 日變動 / 20d avg volume
  - short_chg_{5,10,20}d          融券餘額 N 日變動 / 20d avg volume
  - margin_short_ratio            券資比 (short_balance / margin_balance)
  - sbl_chg_{5,10,20}d            借券餘額 N 日變動 / 20d avg volume

輸出:
  reports/chip_ic_matrix.csv       IC 總表
  reports/chip_ic_daily.parquet    每日截面 IC（供後續 combo analysis）

用法:
    python tools/chip_ic_analysis.py                 # 全量
    python tools/chip_ic_analysis.py --sample 200    # 測試
    python tools/chip_ic_analysis.py --since 2022    # 限縮期間
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

OHLCV_PATH = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
CHIP_DIR = _ROOT / "data_cache" / "chip_history"
INST_PATH = CHIP_DIR / "institutional.parquet"
MARGIN_PATH = CHIP_DIR / "margin.parquet"
SHORT_PATH = CHIP_DIR / "short_sale.parquet"

OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "chip_ic_matrix.csv"
OUT_DAILY_IC = OUT_DIR / "chip_ic_daily.parquet"

HORIZONS = [5, 10, 20, 40, 60]
MIN_CROSS_SECTION = 30
MIN_HISTORY = 200
BOOTSTRAP_N = 1000
BOOTSTRAP_BLOCK = 20

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("chip_ic")


# ============================================================
# 1. Load data
# ============================================================
def load_ohlcv(sample=None, since=None):
    """Load OHLCV and prepare stock_id + volume for normalization."""
    logger.info(f"Loading OHLCV: {OHLCV_PATH}")
    df = pd.read_parquet(OHLCV_PATH)
    df['date'] = pd.to_datetime(df['date'])
    if since:
        df = df[df['date'] >= since].copy()

    for col in ['Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Filter minimum history
    counts = df.groupby('stock_id').size()
    keep = counts[counts >= MIN_HISTORY].index
    df = df[df['stock_id'].isin(keep)].copy()

    if sample:
        rng = np.random.default_rng(42)
        ids = df['stock_id'].unique()
        chosen = rng.choice(ids, min(sample, len(ids)), replace=False)
        df = df[df['stock_id'].isin(chosen)].copy()

    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)
    logger.info(f"OHLCV: {len(df):,} rows, {df['stock_id'].nunique()} stocks, "
                f"{df['date'].min().date()} ~ {df['date'].max().date()}")
    return df


def load_chip_data():
    """Load all 3 chip parquets."""
    logger.info("Loading chip data...")

    inst = pd.read_parquet(INST_PATH)
    inst['date'] = pd.to_datetime(inst['date'])
    for c in ['foreign_net', 'trust_net', 'dealer_net', 'total_net']:
        inst[c] = pd.to_numeric(inst[c], errors='coerce')
    logger.info(f"  institutional: {len(inst):,} rows, {inst['stock_id'].nunique()} stocks")

    margin = pd.read_parquet(MARGIN_PATH)
    margin['date'] = pd.to_datetime(margin['date'])
    for c in ['margin_balance', 'short_balance']:
        margin[c] = pd.to_numeric(margin[c], errors='coerce')
    logger.info(f"  margin: {len(margin):,} rows, {margin['stock_id'].nunique()} stocks")

    short = pd.read_parquet(SHORT_PATH)
    short['date'] = pd.to_datetime(short['date'])
    short['sbl_balance'] = pd.to_numeric(short['sbl_balance'], errors='coerce')
    logger.info(f"  short_sale: {len(short):,} rows, {short['stock_id'].nunique()} stocks")

    return inst, margin, short


# ============================================================
# 2. Compute chip signals
# ============================================================
SIGNAL_COLS = []
SIGNAL_LABELS = {}

# Institutional signals
for entity, label_prefix in [('foreign', '外資'), ('trust', '投信'), ('total', '法人合計')]:
    for w in [5, 10, 20]:
        col = f'inst_{entity}_{w}d'
        SIGNAL_COLS.append(col)
        SIGNAL_LABELS[col] = f'{label_prefix} {w}D'
SIGNAL_COLS.append('inst_foreign_trust_sync')
SIGNAL_LABELS['inst_foreign_trust_sync'] = '外資投信同步'

# Margin signals
for w in [5, 10, 20]:
    col = f'margin_chg_{w}d'
    SIGNAL_COLS.append(col)
    SIGNAL_LABELS[col] = f'融資變動 {w}D'
for w in [5, 10, 20]:
    col = f'short_chg_{w}d'
    SIGNAL_COLS.append(col)
    SIGNAL_LABELS[col] = f'融券變動 {w}D'
SIGNAL_COLS.append('margin_short_ratio')
SIGNAL_LABELS['margin_short_ratio'] = '券資比'

# SBL signals
for w in [5, 10, 20]:
    col = f'sbl_chg_{w}d'
    SIGNAL_COLS.append(col)
    SIGNAL_LABELS[col] = f'借券變動 {w}D'


def compute_chip_signals(ohlcv, inst, margin, short):
    """
    Merge chip data onto ohlcv panel, compute all 19 signals.
    All flow-based signals are normalized by 20d average volume.
    """
    logger.info("Computing chip signals...")
    t0 = time.time()

    # Restrict chip data to stocks present in ohlcv
    valid_ids = set(ohlcv['stock_id'].unique())
    inst = inst[inst['stock_id'].isin(valid_ids)].copy()
    margin = margin[margin['stock_id'].isin(valid_ids)].copy()
    short = short[short['stock_id'].isin(valid_ids)].copy()

    # Start with ohlcv base (stock_id, date, AdjClose, Volume)
    df = ohlcv[['stock_id', 'date', 'AdjClose', 'Volume']].copy()

    # 20d average volume for normalization
    df = df.sort_values(['stock_id', 'date'])
    df['vol_20d_avg'] = df.groupby('stock_id')['Volume'].transform(
        lambda s: s.rolling(20, min_periods=10).mean()
    )
    # Avoid division by zero
    df['vol_20d_avg'] = df['vol_20d_avg'].replace(0, np.nan)

    # --- Merge institutional ---
    inst_cols = ['date', 'stock_id', 'foreign_net', 'trust_net', 'total_net']
    df = df.merge(inst[inst_cols], on=['stock_id', 'date'], how='left')

    # Rolling sums for institutional
    df = df.sort_values(['stock_id', 'date'])
    for entity, src_col in [('foreign', 'foreign_net'), ('trust', 'trust_net'), ('total', 'total_net')]:
        for w in [5, 10, 20]:
            sig_col = f'inst_{entity}_{w}d'
            raw = df.groupby('stock_id')[src_col].transform(
                lambda s: s.rolling(w, min_periods=1).sum()
            )
            # Normalize by 20d avg volume
            df[sig_col] = raw / df['vol_20d_avg']

    # Foreign + Trust sync signal
    # Use 5d rolling sums for sync detection
    f5 = df.groupby('stock_id')['foreign_net'].transform(
        lambda s: s.rolling(5, min_periods=1).sum()
    )
    t5 = df.groupby('stock_id')['trust_net'].transform(
        lambda s: s.rolling(5, min_periods=1).sum()
    )
    df['inst_foreign_trust_sync'] = np.where(
        (f5 > 0) & (t5 > 0), 1.0,
        np.where((f5 < 0) & (t5 < 0), -1.0, 0.0)
    )

    # --- Merge margin ---
    margin_cols = ['date', 'stock_id', 'margin_balance', 'short_balance']
    df = df.merge(margin[margin_cols], on=['stock_id', 'date'], how='left')

    # Margin balance change
    for w in [5, 10, 20]:
        raw = df.groupby('stock_id')['margin_balance'].transform(
            lambda s: s.diff(w)
        )
        df[f'margin_chg_{w}d'] = raw / df['vol_20d_avg']

    # Short balance change
    for w in [5, 10, 20]:
        raw = df.groupby('stock_id')['short_balance'].transform(
            lambda s: s.diff(w)
        )
        df[f'short_chg_{w}d'] = raw / df['vol_20d_avg']

    # Margin/short ratio (cap at 999 to handle near-zero margin)
    df['margin_short_ratio'] = (
        df['short_balance'] / df['margin_balance'].replace(0, np.nan)
    ).clip(upper=999)

    # --- Merge SBL ---
    sbl_cols = ['date', 'stock_id', 'sbl_balance']
    df = df.merge(short[sbl_cols], on=['stock_id', 'date'], how='left')

    # SBL balance change
    for w in [5, 10, 20]:
        raw = df.groupby('stock_id')['sbl_balance'].transform(
            lambda s: s.diff(w)
        )
        df[f'sbl_chg_{w}d'] = raw / df['vol_20d_avg']

    elapsed = time.time() - t0
    logger.info(f"Chip signals computed in {elapsed:.1f}s")

    # Log signal coverage
    for col in SIGNAL_COLS:
        valid_pct = df[col].notna().mean() * 100
        logger.info(f"  {col}: {valid_pct:.1f}% valid")

    return df


# ============================================================
# 3. Forward returns
# ============================================================
def add_fwd_returns(df):
    logger.info("Computing forward returns...")
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)
    for h in HORIZONS:
        df[f'fwd_{h}d'] = df.groupby('stock_id')['AdjClose'].pct_change(h).shift(-h)
    return df


# ============================================================
# 4. Cross-sectional IC + statistics (reuse indicator_ic_analysis pattern)
# ============================================================
def compute_daily_ic(df, signal_col, return_col):
    """Cross-sectional Spearman IC per day."""
    x = df.dropna(subset=[signal_col, return_col])

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
    """Block bootstrap preserving autocorrelation -> 95% CI for mean IC."""
    arr = ic_series.values
    n = len(arr)
    if n < block_size * 2:
        return (np.nan, np.nan)
    rng = np.random.default_rng(42)
    n_blocks = n // block_size
    boot_means = []
    for _ in range(n_boot):
        starts = rng.integers(0, n - block_size, n_blocks)
        sample = np.concatenate([arr[s:s + block_size] for s in starts])
        boot_means.append(sample.mean())
    return (float(np.percentile(boot_means, 2.5)), float(np.percentile(boot_means, 97.5)))


def summarize_ic(ic_series):
    """Summary stats for a daily IC series."""
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
    t_stat = m * np.sqrt(n) / s if s > 0 else 0
    p = 2 * (1 - stats.t.cdf(abs(t_stat), df=n - 1))
    win_rate = (arr > 0).mean() * 100
    ci_low, ci_high = block_bootstrap_ci(ic_series)
    return dict(mean=float(m), std=float(s), ir=float(ir), p=float(p),
                ci_low=ci_low, ci_high=ci_high, win_rate=float(win_rate), n=int(n))


# ============================================================
# 5. Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="C2-b: Chip factor IC analysis")
    parser.add_argument('--sample', type=int, default=None,
                        help='Random sample N stocks (for testing)')
    parser.add_argument('--since', type=str, default=None,
                        help='Start date YYYY or YYYY-MM-DD')
    args = parser.parse_args()

    t0 = time.time()

    # Load data
    ohlcv = load_ohlcv(sample=args.sample, since=args.since)
    inst, margin, short = load_chip_data()

    # Compute signals
    df = compute_chip_signals(ohlcv, inst, margin, short)
    df = add_fwd_returns(df)

    # IC computation
    daily_ic_rows = []
    results = []

    logger.info(f"Running IC matrix: {len(SIGNAL_COLS)} signals x {len(HORIZONS)} horizons...")
    t_ic = time.time()

    for sig in SIGNAL_COLS:
        for h in HORIZONS:
            ret_col = f'fwd_{h}d'
            ic = compute_daily_ic(df, sig, ret_col)
            stats_dict = summarize_ic(ic)
            results.append({
                'indicator': sig,
                'label': SIGNAL_LABELS[sig],
                'horizon': h,
                **stats_dict,
            })
            # Save daily IC
            if len(ic) > 0:
                tmp = ic.reset_index()
                tmp.columns = ['date', 'ic']
                tmp['indicator'] = sig
                tmp['horizon'] = h
                daily_ic_rows.append(tmp)

        # Progress
        done = SIGNAL_COLS.index(sig) + 1
        logger.info(f"  [{done}/{len(SIGNAL_COLS)}] {sig} done")

    logger.info(f"IC computation done in {time.time() - t_ic:.1f}s")

    # Output matrix
    out_df = pd.DataFrame(results)
    out_df = out_df.sort_values(['horizon', 'ir'], ascending=[True, False])
    out_df.to_csv(OUT_CSV, index=False, encoding='utf-8-sig')
    logger.info(f"Matrix saved: {OUT_CSV} ({len(out_df)} rows)")

    # Output daily IC
    if daily_ic_rows:
        daily_ic_df = pd.concat(daily_ic_rows, ignore_index=True)
        daily_ic_df.to_parquet(OUT_DAILY_IC)
        logger.info(f"Daily IC saved: {OUT_DAILY_IC}")

    # Print summary
    print("\n" + "=" * 100)
    print("  Chip Factor IC Matrix")
    print("=" * 100)
    print(f"  {'Factor':<28} {'H':>3} {'mean_IC':>8} {'IR':>7} "
          f"{'95% CI':<20} {'p':>10} {'win%':>6} {'n':>5} {'sig':>4}")
    print("  " + "-" * 96)

    for _, r in out_df.iterrows():
        sig_flag = "*" if (r['p'] < 0.05 and pd.notna(r['ci_low'])
                          and r['ci_low'] * r['ci_high'] > 0) else "."
        ci_str = (f"[{r['ci_low']:+.4f}, {r['ci_high']:+.4f}]"
                  if pd.notna(r['ci_low']) else "")
        label = r['label']
        label_width = sum(2 if ord(c) > 127 else 1 for c in label)
        pad = max(0, 28 - label_width)
        print(f"  {label}{' ' * pad} {r['horizon']:>3d} "
              f"{r['mean']:>+8.4f} {r['ir']:>+7.3f} {ci_str:<20} "
              f"{r['p']:>10.2e} {r['win_rate']:>5.1f}% "
              f"{r['n']:>5d}  {sig_flag}")
    print("=" * 100)

    # Top 5 per horizon
    print("\n  Top 5 IC_IR per horizon:")
    print("  " + "-" * 60)
    for h in HORIZONS:
        h_df = out_df[out_df['horizon'] == h].nlargest(5, 'ir')
        top_str = ", ".join(
            f"{r['label']}({r['ir']:+.3f})" for _, r in h_df.iterrows()
        )
        print(f"  {h:>2}d: {top_str}")

    print(f"\nTotal time: {(time.time() - t0) / 60:.1f} min")


if __name__ == '__main__':
    main()
