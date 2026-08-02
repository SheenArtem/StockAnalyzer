"""Refresh ohlcv_tw.parquet + value_sim_indicators.parquet + value_sim_fwd_returns.parquet.

These 3 files feed the market scan, valuation, and IC validation tools.
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

Schedule via run_scanner.bat or run_scanner_weekly.bat. Downstream consumers
pick up the refreshed panels automatically.
"""
from __future__ import annotations
import argparse
import logging
import math
import os
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


# 台股代號判別統一在 tools/tw_universe（原本這裡與 build_tw_breadth 各有一份，已漂移）
from tools.tw_universe import TW_TICKER_RE as _TW_TICKER_RE
MIN_MARKET_ROWS = 500
MIN_MARKET_COVERAGE_RATIO = 0.80


def _atomic_to_parquet(frame: pd.DataFrame, path: Path) -> None:
    """Write a complete parquet beside the target, then atomically promote it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _in_market_baseline(dates: pd.Series, frame: pd.DataFrame,
                        cutoff: pd.Timestamp, history_days: int = 180):
    """近期視窗**之前**的「每日在市檔數」中位數；不足資料回 None。

    這是絕對門檻的基準。**不可用磁碟上的 CSV 檔數**：下市股的 `*_price.csv` 永久保留，
    所以那個分母只會單向變大 —— 2026-08-02 實測磁碟 2,064 檔 vs 實際每日有量
    1,920~1,944，門檻算成 ceil(2064×0.80)=1,652，餘裕只剩 15% 且逐年縮小，是會自己
    走向硬失敗的設計（2026-08-02 code review）。

    改用面板自身歷史則自我維護：下市股停止長出新 bar，於是「近期有量的檔數」自然就是
    在市檔數。基準取近期視窗之前的歷史，才抓得到「整個近期視窗一起縮水」。
    """
    window = (dates < cutoff) & (dates >= cutoff - pd.Timedelta(days=history_days))
    if not window.any():
        return None
    hist = frame.loc[window, ['stock_id', 'Volume']].copy()
    hist['date'] = dates[window]
    hist = hist[pd.to_numeric(hist['Volume'], errors='coerce').fillna(0).gt(0)]
    if hist.empty:
        return None
    per_day = hist.groupby('date')['stock_id'].nunique()
    if len(per_day) < 20:            # 歷史太短，中位數不可靠
        return None
    return int(per_day.median())


def report_coverage_gaps(frame: pd.DataFrame, min_ratio: float = 0.90,
                         min_run: int = 3, severe_ratio: float = 0.50) -> dict:
    """掃**全歷史**，找出「連續多日在市檔數明顯低於長期水準」的區間，log 出來。

    這是純資訊性的（不刪任何列），存在的理由是這類缺口在現行檢查下**完全隱形**：
    `drop_unhealthy_recent_market_dates` 只看最近 45 天，而歷史上的抓取斷層就這樣
    留在 panel 裡沒人知道。

    實例（2026-08-02 查出）：2026-04-13~04-29 共 13 個交易日，panel 只有約 1,700 檔
    而非 1,965（少約 260 檔），2026-04-30 一次全部回來 —— 是抓取斷層不是市場事件
    （292 檔離開者中 193 檔在 5 月回歸）。跨這個窗的橫斷面研究（breadth / 等權 /
    排序 / IC）都是在縮小的 universe 上算的。

    註：同期另有 99 支減資舊股別（`*O`）自 2026-04-08 永久停止交易，那是合理變動，
    會被 252 日中位數基準吸收掉，不算缺口。

    另外單獨回報「單日重度不足」（低於 `severe_ratio`）：那類只有一天所以構不成
    `min_run`，但嚴重度更高 —— panel 有 11 個真實交易日只存了 33~38% 的橫斷面，
    連 2330 都沒有（yfinance 端缺資料，官方 MI_INDEX 證實那些日子有 1,100~1,300 檔
    成交）。

    回傳 {'gaps': [[Timestamp, ...], ...], 'severe_days': [Timestamp, ...]}。
    """
    empty = {'gaps': [], 'severe_days': []}
    if frame.empty or not {'date', 'stock_id'} <= set(frame.columns):
        return empty
    counts = frame.groupby(pd.to_datetime(frame['date']).dt.normalize())[
        'stock_id'].nunique().sort_index()

    # 兩種檢查需要兩種基準（2026-08-02 實測比較後定案）：
    #  - 持續斷層：用長期（252 日）中位數，缺口再長也拉不動它 -> 2026-04 那 13 天全中。
    #    但它在覆蓋率成長期偏低（2006 約 200 檔 -> 2016 約 1,620 檔），單日檢查會失靈。
    #  - 單日重度不足：用短期（21 日）中位數貼近當下水準，單一天拉不動中位數。
    #    改用它之後 11 個已知部分橫斷面日全中（長期基準只中 9 個）。
    long_base = counts.shift(1).rolling(252, min_periods=60).median()
    short_base = counts.shift(1).rolling(21, min_periods=10).median()
    ratio = (counts / long_base).dropna()
    baseline = long_base
    thin = ratio < min_ratio

    gaps, run = [], []
    for d, is_thin in thin.items():
        if is_thin:
            run.append(d)
        elif run:
            if len(run) >= min_run:
                gaps.append(run)
            run = []
    if len(run) >= min_run:
        gaps.append(run)

    for g in gaps:
        lo, hi = g[0], g[-1]
        obs = counts.loc[lo:hi]
        log.warning("Panel coverage gap: %s..%s (%d sessions), stocks %d~%d vs "
                    "trailing-252d median %.0f (%.0f%%)",
                    lo.date(), hi.date(), len(g), obs.min(), obs.max(),
                    baseline.loc[hi], ratio.loc[hi] * 100)
    if not gaps:
        log.info("Panel coverage: no sustained gap (>=%d sessions below %.0f%% of the "
                 "trailing-252d median)", min_run, min_ratio * 100)

    short_ratio = (counts / short_base).dropna()
    severe = sorted(short_ratio.index[short_ratio < severe_ratio])
    if severe:
        log.warning("Panel coverage: %d single session(s) below %.0f%% of the "
                    "trailing-21d median -- partial cross-sections, cross-sectional "
                    "stats on these dates are not comparable: %s",
                    len(severe), severe_ratio * 100,
                    ', '.join(f"{d.date()}({counts[d]}/{short_base[d]:.0f})"
                              for d in severe[:14]))
    return {'gaps': gaps, 'severe_days': severe}


def drop_unhealthy_recent_market_dates(
        frame: pd.DataFrame, lookback_days: int = 45,
        expected_stock_count: int | None = None) -> tuple[pd.DataFrame, list[pd.Timestamp]]:
    """Drop market-wide zero-volume and incomplete recent dates.

    This is a second boundary behind ``refresh_universe_prices`` because old
    per-stock CSVs may already contain a Yahoo holiday placeholder or a
    rate-limited partial day.

    `expected_stock_count` 為絕對門檻的分母。**不傳則由面板歷史自行推導**
    （見 `_in_market_baseline`）—— production 走這條；明確傳值只用於測試或特殊情況。
    """
    if frame.empty or not {'date', 'stock_id', 'Volume'} <= set(frame.columns):
        return frame, []
    dates = pd.to_datetime(frame['date']).dt.normalize()
    cutoff = dates.max() - pd.Timedelta(days=lookback_days)
    recent = frame.loc[dates >= cutoff, ['stock_id', 'Volume']].copy()
    recent['date'] = dates[dates >= cutoff]
    recent['positive'] = pd.to_numeric(
        recent['Volume'], errors='coerce').fillna(0).gt(0)
    stats = recent.groupby('date').agg(
        rows=('stock_id', 'size'), positive_volume=('positive', 'sum')).sort_index()
    if stats.empty:
        return frame, []
    reference = int(stats['positive_volume'].max())
    if expected_stock_count is None:
        expected_stock_count = _in_market_baseline(dates, frame, cutoff)
        if expected_stock_count:
            log.info("In-market baseline from panel history: %d stocks/day (median)",
                     expected_stock_count)
    expected_floor = (
        math.ceil(expected_stock_count * MIN_MARKET_COVERAGE_RATIO)
        if expected_stock_count else 0)
    threshold = max(MIN_MARKET_ROWS, expected_floor,
                    math.ceil(reference * MIN_MARKET_COVERAGE_RATIO))
    if not stats['positive_volume'].ge(threshold).any():
        raise RuntimeError(
            "no healthy recent market date during panel aggregation "
            f"(threshold={threshold}, max_positive={reference}, "
            f"expected_stocks={expected_stock_count})")
    bad_dates = list(stats.index[stats['positive_volume'] < threshold])
    if not bad_dates:
        return frame, []
    keep = ~dates.isin(bad_dates)
    dropped = int((~keep).sum())
    log.warning(
        "Dropped %d rows across %d unhealthy recent market dates "
        "(positive-volume threshold=%d): %s",
        dropped, len(bad_dates), threshold,
        ', '.join(str(d.date()) for d in bad_dates))
    return frame.loc[keep].copy(), bad_dates


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
    # 全歷史覆蓋率稽核（純 log，不刪列）—— 歷史抓取斷層在只看近 45 天的檢查下隱形。
    report_coverage_gaps(out)
    # 不再傳 len(tw_csv)：磁碟含永久保留的下市股 CSV，分母只會單向變大。
    # 交給 _in_market_baseline 由面板歷史推導每日在市檔數。
    out, _bad_market_dates = drop_unhealthy_recent_market_dates(out)
    log.info("Aggregated: %d rows, %d stocks, date range %s -> %s, took %.1fs",
             len(out), out['stock_id'].nunique(),
             out['date'].min().date(), out['date'].max().date(),
             time.time() - t0)

    _atomic_to_parquet(out, OHLCV_PATH)
    log.info("Saved: %s", OHLCV_PATH)
    return out


def refresh_indicators(ohlcv: pd.DataFrame, fwd: bool = True) -> None:
    """Run precompute_indicators + precompute_forward_returns from value_historical_simulator."""
    from tools.value_historical_simulator import precompute_indicators, precompute_forward_returns

    t0 = time.time()
    log.info("Precomputing indicators (RSI / RVOL / 52w-low / avg_tv) for %d stocks...",
             ohlcv['stock_id'].nunique())
    ind = precompute_indicators(ohlcv)
    _atomic_to_parquet(ind, IND_PATH)
    log.info("Saved %s: %d rows, took %.1fs", IND_PATH, len(ind), time.time() - t0)

    if fwd:
        t1 = time.time()
        log.info("Precomputing forward returns (fwd_5d / fwd_20d / fwd_60d / fwd_120d / max-min)...")
        f = precompute_forward_returns(ohlcv)
        _atomic_to_parquet(f, FWD_PATH)
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
