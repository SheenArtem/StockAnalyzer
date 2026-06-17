"""
vfvc_backfill_monthly_rev.py
============================
VF-VC P3-a: 補回在 snapshot 有但 live cache 幾乎無資料的股票 monthly revenue
(1101/1102/1303/... 水泥大型股)。目標：把 monthly revenue 回填完整供 VF-VC 1m YoY 使用。

2026-04-21 RF-1 重構：
---------------------
原版只寫 data_cache/backtest/financials_revenue.parquet，導致 live scanner
讀 data_cache/fundamental_cache/month_revenue_*.parquet 時看不到新資料 →
觸發 MOPS 重抓 → WAF ban（VF-VC 事件）。

新版：
  1. per-stock 寫入 data_cache/fundamental_cache/month_revenue_{sid}.parquet
     （這是 cache_manager.get_cached_fundamentals() 的 live 路徑）
  2. 全部 backfill 完後自動呼叫 aggregate_fundamental_cache.py --category revenue
     聚合到 data_cache/backtest/financials_revenue.parquet（simulator 用）

規則：任何 backfill tool 只能寫 fundamental_cache/，絕對禁止直接寫 backtest/。
詳見 feedback_unified_cache.md。

用法: python tools/vfvc_backfill_monthly_rev.py --universe data_cache/vfvc_missing_monthly_rev.txt
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("vfvc_bf")

LIVE_CACHE_DIR = ROOT / "data_cache" / "fundamental_cache"
FINMIND_CACHE_DIR = ROOT / "data_cache" / "finmind_cache"

# get_monthly_revenue (USE_MOPS=false, 預設) 走 cache_manager.get_finmind_cached 讀
# finmind_cache/，與 bulk-update 寫的 fundamental_cache/ 是兩條路。bulk-update 後必須
# 同步一份到 finmind_cache，否則排程更新餵不到 get_monthly_revenue（AI 報告 / value_screener /
# position_monitor 的營收來源）。2026-06-17 修「路徑分裂」根因。
_FINMIND_SCHEMA = ['date', 'stock_id', 'country', 'revenue',
                   'revenue_month', 'revenue_year']


def sync_fundamental_to_finmind_cache() -> int:
    """把 fundamental_cache/month_revenue_*.parquet 同步到 finmind_cache/。

    用「營收月」union (concat 後 drop_duplicates by revenue_year+month)，既補上新月、
    又不退化 finmind_cache 既有的完整歷史。schema 取 FinMind 原始子集。
    """
    FINMIND_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    synced, failed = 0, 0
    for fp in LIVE_CACHE_DIR.glob("month_revenue_*.parquet"):
        try:
            fdf = pd.read_parquet(fp)
            if fdf is None or fdf.empty or 'revenue' not in fdf.columns:
                continue
            sub = fdf[[c for c in _FINMIND_SCHEMA if c in fdf.columns]].copy()
            target = FINMIND_CACHE_DIR / fp.name
            if target.exists():
                old = pd.read_parquet(target)
                if old is not None and not old.empty:
                    old_sub = old[[c for c in _FINMIND_SCHEMA if c in old.columns]].copy()
                    sub = pd.concat([old_sub, sub], ignore_index=True)
            if 'revenue_year' in sub.columns and 'revenue_month' in sub.columns:
                sub = (sub.sort_values(['revenue_year', 'revenue_month'])
                          .drop_duplicates(subset=['revenue_year', 'revenue_month'],
                                           keep='last'))
            elif 'date' in sub.columns:
                sub = sub.sort_values('date').drop_duplicates(subset='date', keep='last')
            # date 欄統一成 FinMind 原生字串格式，避免 concat 混入 Timestamp/str
            # 造成 object dtype pyarrow 寫入失敗 (2881/2882 金控股案例)
            if 'date' in sub.columns:
                sub['date'] = pd.to_datetime(sub['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            sub.to_parquet(target)
            synced += 1
        except Exception as e:
            failed += 1
            logger.warning("sync finmind_cache failed %s: %s", fp.name, e)
    logger.info("Synced fundamental_cache -> finmind_cache: %d ok / %d fail", synced, failed)
    return synced


def run_bulk_update():
    """Cache 三層 Layer 2: 用 mopsfin bulk CSV 一次更新全市場最新月營收。

    特性:
    - 2 個 HTTP request (上市 + 上櫃, ~1954 stocks)
    - 僅補最新月 (公告月後 10-15 日更新可用), 歷史 backfill 仍走 FinMind
    - 不消耗 FinMind 600 req/hr, 不打 MOPS 個股 API (避 WAF)
    - merge_into_existing_cache 按期數比對, 已有不覆寫
    """
    from mops_bulk_fetcher import fetch_bulk_monthly_revenue, merge_into_existing_cache

    logger.info("=== BULK UPDATE mode (Cache Layer 2) ===")
    df = fetch_bulk_monthly_revenue(include_otc=True)
    if df.empty:
        logger.error("Bulk fetch returned empty, abort.")
        return False
    logger.info("Bulk fetched: %d rows / %d unique stocks", len(df), df['stock_id'].nunique())
    logger.info("Date range: %s ~ %s", df['date'].min(), df['date'].max())

    stats = merge_into_existing_cache(df, dry_run=False)
    logger.info("Merge stats: %s", stats)

    # 跑 aggregate 同步 backtest/financials_revenue.parquet
    logger.info("Running aggregate_fundamental_cache.py --category revenue ...")
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "aggregate_fundamental_cache.py"),
         "--category", "revenue"],
        cwd=str(ROOT),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.error("Aggregate FAILED (rc=%d):\n%s", result.returncode, result.stderr)
        return False
    logger.info("Aggregate OK")
    for line in result.stdout.splitlines()[-6:]:
        logger.info("  %s", line)

    # 修「路徑分裂」: 同步到 finmind_cache (get_monthly_revenue 實際讀取路徑)
    logger.info("Syncing fundamental_cache -> finmind_cache ...")
    sync_fundamental_to_finmind_cache()
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="data_cache/vfvc_missing_monthly_rev.txt")
    ap.add_argument("--start", default="2015-01-01")
    # end 預設不設限 (抓到 FinMind 最新月)。原硬編碼 "2026-04-30" 是過期日期，
    # 任何在該日後跑的 backfill 會把 2026-04 之後 (含 2026-04 營收本身，其
    # FinMind date=2026-05-01) 全切掉 → 全市場月營收破洞根因之一 (2026-06-17)。
    ap.add_argument("--end", default=None,
                    help="抓取截止 (FinMind date 欄, 公告月)；預設 None=抓到最新")
    ap.add_argument("--skip-aggregate", action="store_true",
                    help="不自動跑 aggregate（僅測試用）")
    ap.add_argument("--bulk-update", action="store_true",
                    help="走 Cache Layer 2 全市場 bulk 更新最新月（Cache 三層架構）；"
                         "不需 universe 檔，跳過 per-stock FinMind 路徑")
    args = ap.parse_args()

    if args.bulk_update:
        ok = run_bulk_update()
        sys.exit(0 if ok else 1)

    stocks = [l.strip() for l in open(args.universe) if l.strip()]
    logger.info("Universe: %d stocks", len(stocks))

    from cache_manager import get_finmind_loader
    dl = get_finmind_loader()
    logger.info("FinMind loaded, has_token=%s", dl.has_token)

    LIVE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    call_count = 0
    hour_start = time.time()
    fail_stocks = []
    ok_stocks = []

    for i, sid in enumerate(stocks):
        # Rate-limit: 600/hr = 1 req / 6 sec, leave buffer
        if call_count >= 580:
            wait = 3600 - (time.time() - hour_start) + 10
            if wait > 0:
                logger.warning("Rate limit, sleep %.0fs...", wait)
                time.sleep(wait)
            call_count = 0
            hour_start = time.time()

        try:
            rev_kwargs = dict(stock_id=sid, start_date=args.start)
            if args.end:
                rev_kwargs["end_date"] = args.end
            raw = dl.taiwan_stock_month_revenue(**rev_kwargs)
            call_count += 1
            time.sleep(1.2)  # Throttle to ~1/sec

            if raw is None or raw.empty:
                fail_stocks.append(sid)
                logger.warning("[%s] FinMind empty", sid)
                continue

            if 'revenue' not in raw.columns:
                fail_stocks.append(sid)
                continue
            raw = raw.copy()
            raw['revenue'] = pd.to_numeric(raw['revenue'], errors='coerce')
            raw = raw.dropna(subset=['revenue'])
            if raw.empty:
                fail_stocks.append(sid)
                continue

            # Schema 對齊 live cache（cache_manager 寫入 FinMind 原始 schema）
            keep_cols = ['date', 'stock_id', 'country', 'revenue',
                         'revenue_month', 'revenue_year', 'revenue_last_year',
                         'revenue_year_growth', 'revenue_last_month',
                         'revenue_month_growth']
            present = [c for c in keep_cols if c in raw.columns]
            out_df = raw[present].copy()

            # 寫 per-stock live cache（這是 RF-1 的關鍵改動：從 backtest 改寫到 fundamental_cache）
            # merge 既有 cache (不覆寫): 防單次 FinMind 回傳不完整 (缺月) 時抹掉已有歷史。
            # 用 (revenue_year, revenue_month) 去重 keep last (新值覆蓋同期、舊獨有期保留)。
            live_path = LIVE_CACHE_DIR / f"month_revenue_{sid}.parquet"
            if live_path.exists():
                try:
                    old_df = pd.read_parquet(live_path)
                    if old_df is not None and not old_df.empty:
                        out_df = pd.concat([old_df, out_df], ignore_index=True)
                except Exception as e:
                    logger.warning("[%s] read existing cache failed, overwrite: %s", sid, e)
            if 'revenue_year' in out_df.columns and 'revenue_month' in out_df.columns:
                out_df = (out_df.sort_values(['revenue_year', 'revenue_month'])
                          .drop_duplicates(subset=['revenue_year', 'revenue_month'], keep='last')
                          .reset_index(drop=True))
            out_df.to_parquet(live_path)
            ok_stocks.append(sid)

        except Exception as e:
            fail_stocks.append(sid)
            logger.warning("[%s] error: %s", sid, e)

        if (i + 1) % 25 == 0:
            logger.info("[%d/%d] %.1fmin elapsed, ok=%d fail=%d",
                        i + 1, len(stocks), (time.time() - t0) / 60,
                        len(ok_stocks), len(fail_stocks))

    logger.info("Backfill done: %d ok / %d fail / %.1fmin",
                len(ok_stocks), len(fail_stocks), (time.time() - t0) / 60)

    if fail_stocks:
        logger.warning("Failed (%d): %s", len(fail_stocks), fail_stocks[:20])

    # ================================================================
    # RF-1 鐵則：per-stock 寫完後，呼叫 aggregate 聚合到 backtest/
    # 確保 backfill 不會再造成 live 與 backtest 資料不一致
    # ================================================================
    if not ok_stocks:
        logger.warning("No stocks backfilled, skip sync/aggregate")
        return

    # 同步 fundamental_cache -> finmind_cache (get_monthly_revenue 實際讀取路徑)。
    # per-stock backfill 只寫 fundamental_cache，不同步則 AI 報告 / value_screener /
    # position_monitor (走 get_monthly_revenue 讀 finmind_cache) 看不到新資料
    # (2026-06-17 路徑分裂修)。不受 --skip-aggregate 影響 (那只跳過 backtest/ 聚合)。
    logger.info("Syncing fundamental_cache -> finmind_cache ...")
    sync_fundamental_to_finmind_cache()

    if args.skip_aggregate:
        logger.warning("--skip-aggregate 啟用，未聚合 backtest/financials_revenue.parquet")
        return

    logger.info("Running aggregate_fundamental_cache.py --category revenue ...")
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "aggregate_fundamental_cache.py"),
         "--category", "revenue"],
        cwd=str(ROOT),
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Aggregate OK")
        # print tail of aggregate output
        for line in result.stdout.splitlines()[-8:]:
            logger.info("  %s", line)
    else:
        logger.error("Aggregate FAILED (rc=%d):\n%s", result.returncode, result.stderr)
        sys.exit(result.returncode)


if __name__ == "__main__":
    main()
