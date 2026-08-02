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
import math
import os
import subprocess
import sys
import time
import uuid
from datetime import date
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
AGGREGATE_REVENUE_PATH = ROOT / "data_cache" / "backtest" / "financials_revenue.parquet"

REQUIRED_BULK_MARKETS = frozenset({"SII", "OTC"})
MIN_BULK_LATEST_STOCKS = 1000
MIN_BULK_COVERAGE_RATIO = 0.80
MIN_BULK_MARKET_STOCKS = {"SII": 500, "OTC": 300}

# get_monthly_revenue (USE_MOPS=false, 預設) 走 cache_manager.get_finmind_cached 讀
# finmind_cache/，與 bulk-update 寫的 fundamental_cache/ 是兩條路。bulk-update 後必須
# 同步一份到 finmind_cache，否則排程更新餵不到 get_monthly_revenue（AI 報告 / value_screener /
# position_monitor 的營收來源）。2026-06-17 修「路徑分裂」根因。
_FINMIND_SCHEMA = ['date', 'stock_id', 'country', 'revenue',
                   'revenue_month', 'revenue_year']


class BulkRevenueSafetyError(RuntimeError):
    """Raised when a bulk update is incomplete or unsafe to publish."""


def _atomic_write_parquet(frame: pd.DataFrame, path: Path) -> None:
    """Serialize beside ``path`` and replace only after a complete write."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp.parquet"
    try:
        frame.to_parquet(temp, index=False)
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _period_summary(frame: pd.DataFrame, label: str) -> tuple[int, int]:
    """Return (latest YYYYMM, unique-stock count) for an existing panel."""
    required = {'stock_id', 'revenue_year', 'revenue_month'}
    missing = required - set(frame.columns)
    if missing:
        raise BulkRevenueSafetyError(
            f"{label} missing period columns: {sorted(missing)}")
    year = pd.to_numeric(frame['revenue_year'], errors='coerce')
    month = pd.to_numeric(frame['revenue_month'], errors='coerce')
    stock_id = frame['stock_id'].astype(str).str.strip()
    valid = year.notna() & month.between(1, 12) & stock_id.ne('')
    if not valid.any():
        raise BulkRevenueSafetyError(f"{label} has no valid stock/period rows")
    periods = year * 100 + month
    latest = int(periods[valid].max())
    latest_count = int(stock_id[valid & periods.eq(latest)].nunique())
    return latest, latest_count


def expected_revenue_period(asof: date | None = None) -> int:
    """Latest revenue month whose statutory 10th-day deadline has arrived."""
    asof = asof or date.today()
    current = pd.Period(asof, freq='M')
    expected = current - (1 if asof.day >= 10 else 2)
    return int(expected.year * 100 + expected.month)


def validate_bulk_cross_section(
    bulk_df: pd.DataFrame,
    existing_df: pd.DataFrame | None,
    min_ratio: float = MIN_BULK_COVERAGE_RATIO,
    min_absolute: int = MIN_BULK_LATEST_STOCKS,
) -> dict[str, int]:
    """Validate both markets and latest-period coverage before any writes."""
    if not 0 < min_ratio <= 1:
        raise ValueError("bulk coverage ratio must be in (0, 1]")
    if min_absolute <= 0:
        raise ValueError("bulk absolute coverage minimum must be positive")
    required = {
        'date', 'stock_id', 'revenue', 'revenue_year', 'revenue_month',
        '_source_market',
    }
    missing = required - set(bulk_df.columns)
    if bulk_df.empty or missing:
        raise BulkRevenueSafetyError(
            f"bulk payload empty or missing columns: {sorted(missing)}")

    frame = bulk_df.copy()
    raw_stock_id = frame['stock_id']
    frame['stock_id'] = raw_stock_id.astype(str).str.strip()
    frame['_source_market'] = frame['_source_market'].astype(str).str.upper().str.strip()
    dates = pd.to_datetime(frame['date'], errors='coerce')
    year = pd.to_numeric(frame['revenue_year'], errors='coerce')
    month = pd.to_numeric(frame['revenue_month'], errors='coerce')
    revenue = pd.to_numeric(frame['revenue'], errors='coerce')
    invalid = (
        raw_stock_id.isna() | frame['stock_id'].eq('') | dates.isna()
        | ~year.between(1900, 2100) | year.mod(1).ne(0)
        | ~month.between(1, 12) | month.mod(1).ne(0) | revenue.isna()
    )
    if invalid.any():
        raise BulkRevenueSafetyError(
            f"bulk payload contains {int(invalid.sum())} invalid stock/period/revenue rows")
    frame['_period'] = year.astype(int) * 100 + month.astype(int)
    canonical_dates = pd.Series(
        (pd.PeriodIndex.from_fields(
            year=year.astype(int), month=month.astype(int), freq='M'
        ) + 1).to_timestamp(),
        index=frame.index,
    )
    if dates.dt.normalize().ne(canonical_dates).any():
        raise BulkRevenueSafetyError(
            "bulk raw date must be the first day after its revenue month")
    if frame.duplicated(['stock_id', '_period']).any():
        raise BulkRevenueSafetyError("bulk payload contains duplicate stock/period rows")

    markets = set(frame['_source_market'].unique())
    missing_markets = REQUIRED_BULK_MARKETS - markets
    if missing_markets:
        raise BulkRevenueSafetyError(
            f"bulk payload missing required markets: {sorted(missing_markets)}")
    unknown_markets = markets - REQUIRED_BULK_MARKETS
    if unknown_markets:
        raise BulkRevenueSafetyError(
            f"bulk payload contains unknown markets: {sorted(unknown_markets)}")

    latest_period = int(frame['_period'].max())
    latest = frame[frame['_period'].eq(latest_period)]
    for market in sorted(REQUIRED_BULK_MARKETS):
        market_frame = frame[frame['_source_market'].eq(market)]
        market_latest = int(market_frame['_period'].max())
        if market_latest != latest_period:
            raise BulkRevenueSafetyError(
                f"{market} latest period {market_latest} does not match {latest_period}")
        market_count = int(
            latest.loc[latest['_source_market'].eq(market), 'stock_id'].nunique())
        market_minimum = MIN_BULK_MARKET_STOCKS[market]
        if market_count < market_minimum:
            raise BulkRevenueSafetyError(
                f"{market} latest-period coverage collapsed: {market_count} stocks, "
                f"required at least {market_minimum}")

    latest_count = int(latest['stock_id'].nunique())
    existing_period = 0
    existing_count = 0
    if existing_df is not None:
        existing_period, existing_count = _period_summary(
            existing_df, "existing revenue aggregate")
        if latest_period < existing_period:
            raise BulkRevenueSafetyError(
                f"bulk latest period {latest_period} is older than existing {existing_period}")
    required_count = max(
        min_absolute,
        math.ceil(existing_count * min_ratio) if existing_count else 0,
    )
    if latest_count < required_count:
        raise BulkRevenueSafetyError(
            f"bulk latest-period coverage collapsed for {latest_period}: "
            f"{latest_count} stocks, required at least {required_count} "
            f"from existing reference {existing_count}")
    return {
        'latest_period': latest_period,
        'latest_count': latest_count,
        'existing_period': existing_period,
        'existing_count': existing_count,
        'required_count': required_count,
    }


def _load_existing_revenue_aggregate() -> pd.DataFrame | None:
    if not AGGREGATE_REVENUE_PATH.exists():
        return None
    try:
        return pd.read_parquet(
            AGGREGATE_REVENUE_PATH,
            columns=['stock_id', 'revenue_year', 'revenue_month'],
        )
    except Exception as exc:
        raise BulkRevenueSafetyError(
            f"cannot validate existing revenue aggregate: {exc}") from exc


def merge_bulk_into_existing_cache(
    bulk_df: pd.DataFrame,
    cache_dir: str | Path | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Merge a validated bulk payload with atomic per-stock replacements."""
    cache_path = Path(cache_dir) if cache_dir is not None else LIVE_CACHE_DIR
    cache_path.mkdir(parents=True, exist_ok=True)
    stats = {
        'written': 0,
        'skipped_already_exists': 0,
        'new_files': 0,
        'append_to_existing': 0,
        'errors': 0,
    }
    bulk_clean = bulk_df.drop(columns=['_source_market'], errors='ignore').copy()
    bulk_clean['stock_id'] = bulk_clean['stock_id'].astype(str)

    for sid, group in bulk_clean.groupby('stock_id'):
        sid = str(sid)
        target = cache_path / f'month_revenue_{sid}.parquet'
        try:
            if target.exists():
                existing = pd.read_parquet(target)
                required = {'revenue_year', 'revenue_month'}
                missing = required - set(existing.columns)
                if missing:
                    raise BulkRevenueSafetyError(
                        f"existing cache missing period columns: {sorted(missing)}")
                existing_periods = set(zip(
                    pd.to_numeric(existing['revenue_year'], errors='raise').astype(int),
                    pd.to_numeric(existing['revenue_month'], errors='raise').astype(int),
                ))
                new_rows = group[~group.apply(
                    lambda row: (
                        int(row['revenue_year']), int(row['revenue_month'])
                    ) in existing_periods,
                    axis=1,
                )]
                if new_rows.empty:
                    stats['skipped_already_exists'] += 1
                    continue
                stats['append_to_existing'] += 1
                if dry_run:
                    continue
                merged = pd.concat([existing, new_rows], ignore_index=True)
                if 'date' in merged.columns:
                    merged['date'] = pd.to_datetime(merged['date'], errors='coerce')
                    merged = merged.sort_values('date').reset_index(drop=True)
                _atomic_write_parquet(merged, target)
            else:
                stats['new_files'] += 1
                if dry_run:
                    continue
                new_frame = group.sort_values('date').reset_index(drop=True)
                _atomic_write_parquet(new_frame, target)
            stats['written'] += 1
        except Exception as exc:
            stats['errors'] += 1
            logger.warning("merge %s failed: %s", sid, exc)
    return stats


def sync_fundamental_to_finmind_cache(raise_on_error: bool = False) -> int:
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
            _atomic_write_parquet(sub, target)
            synced += 1
        except Exception as e:
            failed += 1
            logger.warning("sync finmind_cache failed %s: %s", fp.name, e)
    logger.info("Synced fundamental_cache -> finmind_cache: %d ok / %d fail", synced, failed)
    if failed and raise_on_error:
        raise BulkRevenueSafetyError(
            f"fundamental -> FinMind cache sync failed for {failed} files")
    return synced


def run_bulk_update() -> bool:
    """Cache 三層 Layer 2: 用 mopsfin bulk CSV 一次更新全市場最新月營收。

    特性:
    - 2 個 HTTP request (上市 + 上櫃, ~1954 stocks)
    - 僅補最新月 (公告月後 10-15 日更新可用), 歷史 backfill 仍走 FinMind
    - 不消耗 FinMind 600 req/hr, 不打 MOPS 個股 API (避 WAF)
    - atomic per-stock merge 按期數比對, 已有不覆寫
    """
    from mops_bulk_fetcher import fetch_bulk_monthly_revenue

    logger.info("=== BULK UPDATE mode (Cache Layer 2) ===")
    try:
        df = fetch_bulk_monthly_revenue(include_otc=True)
        existing = _load_existing_revenue_aggregate()
        coverage = validate_bulk_cross_section(df, existing)
        required_period = expected_revenue_period()
        if coverage['latest_period'] < required_period:
            raise BulkRevenueSafetyError(
                f"bulk latest period {coverage['latest_period']} is stale; "
                f"expected at least {required_period}")
    except Exception as exc:
        logger.error("Bulk validation failed, abort: %s", exc)
        return False
    logger.info("Bulk fetched: %d rows / %d unique stocks", len(df), df['stock_id'].nunique())
    logger.info("Date range: %s ~ %s", df['date'].min(), df['date'].max())
    logger.info("Latest-period coverage: %s", coverage)

    stats = merge_bulk_into_existing_cache(df, dry_run=False)
    logger.info("Merge stats: %s", stats)
    if stats['errors']:
        logger.error(
            "Bulk merge failed for %d stocks; aggregate and derived outputs remain unchanged",
            stats['errors'],
        )
        return False

    # Sync first so a cache-path split cannot be published into aggregate/derived.
    logger.info("Syncing fundamental_cache -> finmind_cache ...")
    try:
        sync_fundamental_to_finmind_cache(raise_on_error=True)
    except Exception as exc:
        logger.error("Cache sync failed; aggregate and derived outputs remain unchanged: %s", exc)
        return False

    # 跑 aggregate 同步 backtest/financials_revenue.parquet
    logger.info("Running aggregate_fundamental_cache.py --category revenue ...")
    try:
        result = subprocess.run(
            [sys.executable, str(ROOT / "tools" / "aggregate_fundamental_cache.py"),
             "--category", "revenue"],
            cwd=str(ROOT),
            capture_output=True, text=True,
        )
    except Exception as exc:
        logger.error("Aggregate launch FAILED: %s", exc)
        return False
    if result.returncode != 0:
        logger.error("Aggregate FAILED (rc=%d):\n%s", result.returncode, result.stderr)
        return False
    logger.info("Aggregate OK")
    for line in result.stdout.splitlines()[-6:]:
        logger.info("  %s", line)

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
            _atomic_write_parquet(out_df, live_path)
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
        if fail_stocks:
            # 有工作清單卻一檔都沒成功 = 硬失敗（FinMind 額度爆、token 失效、
            # 網路全斷都長這樣）。舊版在此 `return` → exit 0，排程只會看到成功
            # （2026-08-02 code review）。
            logger.error("Backfill FAILED: 0 ok / %d fail -- 全數失敗，不做 sync/aggregate",
                         len(fail_stocks))
            sys.exit(1)
        logger.warning("No stocks to backfill (work list empty), skip sync/aggregate")
        return

    # 同步 fundamental_cache -> finmind_cache (get_monthly_revenue 實際讀取路徑)。
    # per-stock backfill 只寫 fundamental_cache，不同步則 AI 報告 / value_screener /
    # position_monitor (走 get_monthly_revenue 讀 finmind_cache) 看不到新資料
    # (2026-06-17 路徑分裂修)。不受 --skip-aggregate 影響 (那只跳過 backtest/ 聚合)。
    # raise_on_error=True 與 --bulk-update 那條路徑（:366）一致：sync 失敗代表
    # fundamental_cache 與 finmind_cache 分歧，而那正是 RF-1 鐵則要防的事，不可靜默。
    logger.info("Syncing fundamental_cache -> finmind_cache ...")
    sync_fundamental_to_finmind_cache(raise_on_error=True)

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
