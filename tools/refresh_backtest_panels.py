"""Refresh ohlcv_tw.parquet + value_sim_indicators.parquet + value_sim_fwd_returns.parquet.

These 3 files are the core data feed for whale_picks_screener + IC validation tools.
Without a daily refresh job they go stale (last manual refresh on 2026-04-19 left
value_sim_indicators at 2026-04-13 while daily {sid}_price.csv was current to 5/21).

Pipeline:
  1. Read all data_cache/{stock_id}_price.csv (per-stock daily refresh by scanner)
  2. Concat -> ohlcv_tw.parquet
  3. precompute_indicators(ohlcv) -> value_sim_indicators.parquet (~5-10 min)
  4. precompute_forward_returns(ohlcv) -> value_sim_fwd_returns.parquet (~2 min)

Usage:
  python tools/refresh_backtest_panels.py            # full refresh
  python tools/refresh_backtest_panels.py --no-fwd   # skip fwd_returns (faster, OK for live screening)

Schedule via run_scanner.bat (weekly Sun) or run_scanner_weekly.bat. Each whale_picks_screener
run after this completes will pick up fresh data automatically.
"""
from __future__ import annotations
import argparse
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

CACHE_DIR = REPO / "data_cache"
BACKTEST_DIR = CACHE_DIR / "backtest"
OHLCV_PATH = BACKTEST_DIR / "ohlcv_tw.parquet"
IND_PATH = BACKTEST_DIR / "value_sim_indicators.parquet"
FWD_PATH = BACKTEST_DIR / "value_sim_fwd_returns.parquet"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger("refresh_backtest_panels")


_TW_TICKER_RE = re.compile(r'^\d{4,6}[A-Z]?$')


def aggregate_csv_to_parquet() -> pd.DataFrame:
    """Read all data_cache/{tw_id}_price.csv, concat, write ohlcv_tw.parquet."""
    log.info("Scanning %s for *_price.csv...", CACHE_DIR)
    csv_files = list(CACHE_DIR.glob("*_price.csv"))
    # Filter to TW tickers only (US tickers like AAPL/IEX/ULTA share same dir)
    tw_csv = [f for f in csv_files if _TW_TICKER_RE.match(f.stem.replace('_price', ''))]
    log.info("Found %d TW *_price.csv (out of %d total CSV files)", len(tw_csv), len(csv_files))

    frames = []
    t0 = time.time()
    dropped_badclose = 0
    for i, f in enumerate(tw_csv):
        if (i + 1) % 200 == 0:
            log.info("  [%d/%d] aggregating...", i + 1, len(tw_csv))
        sid = f.stem.replace('_price', '')
        try:
            df = pd.read_csv(f)
            # First column is unnamed date index
            df = df.rename(columns={df.columns[0]: 'date'})
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            df['stock_id'] = sid
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            # 防呆 (2026-06-07): 歷史 CSV 殘留 yfinance 無成交日填充列 (H=L=C=V=0)
            # 與還原殘渣 — Close NaN/<0.01 一律擋在 panel 之外 (TWSE 最低 tick 0.01,
            # 更低=物理不可能)，避免 ATR%/fwd_return 假極端值 (panel 曾累積 12,823 列
            # ≤0 + 121 列微正值, 見 reports/rvol_atr_factor_validation.md)
            if 'Close' in df.columns:
                before = len(df)
                df = df[df['Close'].notna() & (df['Close'] >= 0.01)]
                dropped_badclose += before - len(df)
                # 防呆 2 (2026-06-07): 單日尖刺回落 = yfinance 單位錯置 (元/分 100x 或
                # 10x 小數位移; 實測 1752/3114/8027O 三例, 曾害 fwd_5d 假 +10,000%)。
                # 漲跌停 ±10% 下「單日 >5x 且次日反向 >5x」物理不可能; 減資/恢復交易
                # 為階梯型不回落, 不誤殺。首尾列 ratio=NaN 比較為 False 自然跳過。
                c = df['Close'].reset_index(drop=True)
                df = df.reset_index(drop=True)
                r_in = c / c.shift(1)
                r_out = c.shift(-1) / c
                spike = ((r_in > 5) & (r_out < 0.2)) | ((r_in < 0.2) & (r_out > 5))
                if spike.any():
                    dropped_badclose += int(spike.sum())
                    df = df[~spike]
            cols = ['stock_id', 'date', 'Open', 'High', 'Low', 'Close', 'Volume']
            keep = [c for c in cols if c in df.columns]
            frames.append(df[keep])
        except Exception as e:
            log.warning("  skip %s: %s", sid, e)
    if dropped_badclose:
        log.warning("Dropped %d bad-Close rows (NaN/<=0) during aggregation", dropped_badclose)

    out = pd.concat(frames, ignore_index=True)
    log.info("Aggregated: %d rows, %d stocks, date range %s -> %s, took %.1fs",
             len(out), out['stock_id'].nunique(),
             out['date'].min().date(), out['date'].max().date(),
             time.time() - t0)

    OHLCV_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OHLCV_PATH, index=False)
    log.info("Saved: %s", OHLCV_PATH)
    return out


def refresh_indicators(ohlcv: pd.DataFrame, fwd: bool = True) -> None:
    """Run precompute_indicators + precompute_forward_returns from value_historical_simulator."""
    from tools.value_historical_simulator import precompute_indicators, precompute_forward_returns

    t0 = time.time()
    log.info("Precomputing indicators (RSI / RVOL / 52w-low / avg_tv) for %d stocks...",
             ohlcv['stock_id'].nunique())
    ind = precompute_indicators(ohlcv)
    ind.to_parquet(IND_PATH, index=False)
    log.info("Saved %s: %d rows, took %.1fs", IND_PATH, len(ind), time.time() - t0)

    if fwd:
        t1 = time.time()
        log.info("Precomputing forward returns (fwd_5d / fwd_20d / fwd_60d / fwd_120d / max-min)...")
        f = precompute_forward_returns(ohlcv)
        f.to_parquet(FWD_PATH, index=False)
        log.info("Saved %s: %d rows, took %.1fs", FWD_PATH, len(f), time.time() - t1)
    else:
        log.info("Skipping fwd_returns refresh (--no-fwd)")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--no-fwd', action='store_true',
                    help='Skip fwd_returns refresh (~2 min savings; OK for live screening)')
    ap.add_argument('--no-aggregate', action='store_true',
                    help='Skip ohlcv_tw aggregation, just recompute indicators from existing parquet')
    args = ap.parse_args()

    log.info("=== Refresh backtest panels (ohlcv_tw + indicators + fwd_returns) ===")
    t0 = time.time()

    if args.no_aggregate:
        log.info("Loading existing %s (skip aggregation)...", OHLCV_PATH)
        ohlcv = pd.read_parquet(OHLCV_PATH)
        ohlcv['date'] = pd.to_datetime(ohlcv['date'])
    else:
        ohlcv = aggregate_csv_to_parquet()

    refresh_indicators(ohlcv, fwd=not args.no_fwd)

    log.info("=== All done, total %.1fs ===", time.time() - t0)


if __name__ == "__main__":
    main()
