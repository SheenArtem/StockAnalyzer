"""
backfill_fundamentals.py - VF-L1a 擴大 TW universe 基本面回補

對 universe 中每檔股票，呼叫 cache_manager.get_cached_fundamentals 5 類資料：
  financial_statement / balance_sheet / cash_flows_statement / month_revenue / dividend
全部走 MOPS REST API（無 rate limit），fallback 才用 FinMind。

用法:
  python tools/backfill_fundamentals.py                             # auto (MOPS primary)
  python tools/backfill_fundamentals.py --source finmind            # FinMind-only (安全模式)
  python tools/backfill_fundamentals.py --workers 8                 # 自訂 worker 數
  python tools/backfill_fundamentals.py --limit 100                 # 測試前 100 檔
  python tools/backfill_fundamentals.py --universe path/to/list     # 自訂 universe

--source finmind:
  略過 MOPS，直接走 FinMind（600/hr 限制），適用於 IP 被 MOPS ban 或 MOPS 掛機。
  寫入 data_cache/fundamental_cache/ 同路徑，週一 scan 可直接讀到。
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# 加入專案根目錄到 sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import mops_fetcher
from cache_manager import get_cached_fundamentals, get_finmind_loader

DEFAULT_UNIVERSE = ROOT / "data_cache" / "vfl1a_universe.txt"
CACHE_DIR = ROOT / "data_cache" / "fundamental_cache"

# (cache_key, mops_fetcher_callable, finmind_method, freshness)
CATEGORIES = [
    ("financial_statement",   mops_fetcher.fetch_financial_statement_income,   "taiwan_stock_financial_statement",   "quarterly"),
    ("balance_sheet",         mops_fetcher.fetch_financial_statement_balance,  "taiwan_stock_balance_sheet",         "quarterly"),
    ("cash_flows_statement",  mops_fetcher.fetch_financial_statement_cashflow, "taiwan_stock_cash_flows_statement",  "quarterly"),
    ("month_revenue",         mops_fetcher.fetch_monthly_revenue,              "taiwan_stock_month_revenue",         "monthly"),
    ("dividend",              mops_fetcher.fetch_dividend,                     "taiwan_stock_dividend",              "annual"),
]


def _all_cached(stock_id: str) -> bool:
    """5 個 parquet 都存在就算 done。"""
    for key, *_ in CATEGORIES:
        if not (CACHE_DIR / f"{key}_{stock_id}.parquet").exists():
            return False
    return True


def _backfill_one(dl, stock_id: str, source: str = "auto") -> dict:
    """回補單一股票 5 類資料，回傳 {category: ok|fail|skip}。

    source:
      'auto'    -> get_cached_fundamentals (MOPS primary + FinMind fallback)
      'finmind' -> 直接走 FinMind（mops_fetcher=None 跳過 MOPS 段）
    """
    out = {}
    for key, fetcher, finmind_method, freshness in CATEGORIES:
        path = CACHE_DIR / f"{key}_{stock_id}.parquet"
        if path.exists() and path.stat().st_size > 0:
            out[key] = "skip"
            continue
        try:
            effective_fetcher = None if source == "finmind" else fetcher
            df = get_cached_fundamentals(
                dl, key, stock_id,
                mops_fetcher=effective_fetcher,
                finmind_method=finmind_method,
                freshness=freshness,
            )
            out[key] = "ok" if (df is not None and not df.empty) else "empty"
        except Exception as e:
            logging.warning("%s %s failed: %s", stock_id, key, e)
            out[key] = "fail"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default=str(DEFAULT_UNIVERSE),
                    help="股票清單檔（一行一個 stock_id）")
    ap.add_argument("--workers", type=int, default=4,
                    help="並行 worker 數（MOPS 用 4；FinMind 強制改 1 避免爆配額）")
    ap.add_argument("--limit", type=int, default=0,
                    help="只跑前 N 檔（測試用，0=全跑）")
    ap.add_argument("--progress-every", type=int, default=25)
    ap.add_argument("--source", default="auto", choices=["auto", "finmind"],
                    help="auto=MOPS primary + FinMind fallback; "
                         "finmind=強制純 FinMind (IP 被 MOPS ban 時使用)")
    args = ap.parse_args()

    # FinMind 模式自動降 worker=1（避免爆 600/hr）
    if args.source == "finmind" and args.workers > 1:
        print(f"[INFO] --source=finmind 強制 workers=1 (原 {args.workers})")
        args.workers = 1

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    universe_path = Path(args.universe)
    if not universe_path.exists():
        print(f"[ERROR] Universe file not found: {universe_path}")
        sys.exit(1)

    stock_ids = [s.strip() for s in universe_path.read_text().splitlines() if s.strip()]
    if args.limit > 0:
        stock_ids = stock_ids[:args.limit]

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # 跳過已完成的
    todo = [sid for sid in stock_ids if not _all_cached(sid)]
    print(f"[INFO] Universe: {len(stock_ids)}, already done: {len(stock_ids) - len(todo)}, TODO: {len(todo)}")
    if not todo:
        print("[INFO] Nothing to do. Exiting.")
        return

    dl = get_finmind_loader()  # 共享 loader（thread-safe）

    t0 = time.time()
    done = 0
    stats = {"ok": 0, "skip": 0, "empty": 0, "fail": 0}
    failed_stocks = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        future_to_sid = {ex.submit(_backfill_one, dl, sid, args.source): sid for sid in todo}
        for fut in as_completed(future_to_sid):
            sid = future_to_sid[fut]
            try:
                res = fut.result()
                for k, v in res.items():
                    stats[v] = stats.get(v, 0) + 1
                if any(v == "fail" for v in res.values()):
                    failed_stocks.append(sid)
            except Exception as e:
                logging.error("%s crashed: %s", sid, e)
                failed_stocks.append(sid)
            done += 1
            if done % args.progress_every == 0 or done == len(todo):
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(todo) - done) / rate if rate > 0 else 0
                print(
                    f"[{done}/{len(todo)}] elapsed={elapsed:.0f}s rate={rate:.2f}/s "
                    f"eta={eta/60:.1f}min stats={stats}"
                )

    elapsed = time.time() - t0
    print(f"\n[DONE] {len(todo)} stocks in {elapsed/60:.1f}min")
    print(f"  Per-category counts: {stats}")
    print(f"  Failed stocks: {len(failed_stocks)}")
    if failed_stocks[:20]:
        print(f"  Sample failed: {failed_stocks[:20]}")

    # 寫失敗清單供 retry
    if failed_stocks:
        fail_path = ROOT / "data_cache" / "vfl1a_failed.txt"
        fail_path.write_text("\n".join(failed_stocks))
        print(f"  Failed list written to {fail_path}")


if __name__ == "__main__":
    main()
