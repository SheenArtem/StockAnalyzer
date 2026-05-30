#!/usr/bin/env python
"""
refresh_universe_prices.py -- 全市場 price CSV 日更 (standalone, yfinance batch)

背景：2026-05-23 commit 56dcc6c 停掉 QM/Value 全市場掃描省 CPU/LLM 後，
per-stock data_cache/{sid}_price.csv 的日更連帶停擺（refresh 原本「搭」在
scanner_job.py 的 stage-2 candidate 迭代裡，不是獨立 job）。本工具把「純價格
刷新」抽出來：不跑 QM/Value 評分、不呼叫任何 LLM，只把 data_cache 既有的每檔
{sid}_price.csv 增量更新到最新交易日。

為何用 yfinance 批次而非 load_and_resample(FinMind)：
  FinMind free tier 600 req/hr，cache_manager 在 580/600 會 hard-sleep 到下個
  整點（實測 pausing 2096s）。全市場 ~2549 檔 per-stock FinMind 要分 ~5 個
  小時跑。yfinance 無額度、批次下載（threads）~2549 檔約 1-2 分鐘，且實測
  .TW / .TWO 都能正確回 5 日增量。故這裡走 yfinance 批次，FinMind 留給
  load_and_resample 的個股/盤中即時路徑。

下游受益（這些原本都因 CSV 凍結而吃舊價）：
  - tools/build_tw_breadth.py        -> 市場廣度 macro panel
  - tools/refresh_backtest_panels.py -> ohlcv_tw.parquet -> Whale Picks production
  - 個股技術分析 / 估值 panel

CSV 格式沿用 cache_manager：DatetimeIndex(無名) + [Open,High,Low,Close,Volume,Adj Close]。

執行：python tools/refresh_universe_prices.py [--limit N] [--chunk 160] [--lookback-days 30]
"""
import sys
import time
import logging
import argparse
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
CACHE = REPO / "data_cache"
# cache_manager 的 price CSV 欄位順序（Volume 在 Adj Close 之前）
COLS = ["Open", "High", "Low", "Close", "Volume", "Adj Close"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("refresh_universe_prices")


def _yf_batch(sids, suffix, start_date, chunk):
    """yfinance 批次下載一組 sid（加 suffix），回 {sid: DataFrame}。
    只收有資料的；空的（如 .TW 抓不到的 TPEX 股）留給上層改 .TWO 重試。"""
    import yfinance as yf
    out = {}
    for i in range(0, len(sids), chunk):
        batch = sids[i:i + chunk]
        tickers = [f"{s}{suffix}" for s in batch]
        try:
            df = yf.download(tickers, start=start_date, interval="1d",
                             progress=False, auto_adjust=False, threads=True,
                             group_by="ticker")
        except Exception as e:
            log.warning("yf batch fail [%s..%s] %s: %s",
                        batch[0], batch[-1], suffix, repr(e)[:80])
            continue
        if df is None or df.empty:
            continue
        multi = isinstance(df.columns, pd.MultiIndex)
        for s in batch:
            tk = f"{s}{suffix}"
            try:
                sub = df[tk] if multi else df
                sub = sub.dropna(how="all")
            except Exception:
                continue
            if sub is not None and len(sub):
                # yfinance 偶有 tz-aware index；cache CSV 為 tz-naive
                if getattr(sub.index, "tz", None) is not None:
                    sub.index = sub.index.tz_localize(None)
                out[s] = sub
    return out


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=0, help="只刷前 N 檔（debug 用）")
    ap.add_argument("--chunk", type=int, default=160, help="yfinance 批次大小")
    ap.add_argument("--lookback-days", type=int, default=30,
                    help="批次下載起始回看天數（重疊列會 dedupe）")
    args = ap.parse_args()

    files = sorted(CACHE.glob("*_price.csv"))
    sids = [f.name[:-len("_price.csv")] for f in files
            if f.name[:-len("_price.csv")].isdigit()]
    if args.limit:
        sids = sids[:args.limit]
    log.info("Refreshing %d TW price CSVs via yfinance batch (chunk=%d)...",
             len(sids), args.chunk)

    start_date = (pd.Timestamp.now().normalize()
                  - pd.Timedelta(days=args.lookback_days)).strftime("%Y-%m-%d")
    t0 = time.time()

    # 1. 先全部試 .TW
    data = _yf_batch(sids, ".TW", start_date, args.chunk)
    log.info("  .TW batch: %d/%d got data (%.0fs)", len(data), len(sids), time.time() - t0)
    # 2. .TW 抓不到的改 .TWO 重試（TPEX 上櫃）
    missing = [s for s in sids if s not in data]
    if missing:
        data2 = _yf_batch(missing, ".TWO", start_date, args.chunk)
        data.update(data2)
        log.info("  .TWO retry: +%d/%d (%.0fs)", len(data2), len(missing), time.time() - t0)

    # 3. 合併進每檔 CSV（沿用 cache_manager 格式）
    ok = fail = skipped = 0
    newest_seen = ""
    for sid in sids:
        new = data.get(sid)
        if new is None or new.empty:
            skipped += 1
            continue
        try:
            path = CACHE / f"{sid}_price.csv"
            cached = pd.read_csv(path, index_col=0, parse_dates=True)
            new = new.reindex(columns=COLS)          # 對齊欄位順序
            merged = pd.concat([cached, new])
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
            merged.to_csv(path)                       # 同 cm.save_cache (df.to_csv)
            ok += 1
            last = str(merged.index.max())[:10]
            if last > newest_seen:
                newest_seen = last
        except Exception as e:                        # 單檔失敗不可中斷整批
            fail += 1
            log.warning("merge fail %s: %s", sid, repr(e)[:100])

    log.info("Done: %d merged / %d skipped(no yf data) / %d fail in %.0fs",
             ok, skipped, fail, time.time() - t0)
    log.info("Newest date reached: %s", newest_seen)


if __name__ == "__main__":
    main()
