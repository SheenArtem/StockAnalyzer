"""
Phase 3: 對 Phase 2 過濾出的 qualified stocks 補抓缺失 panel 資料。

緣由 (2026-05-23): Phase 1 把 ohlcv_tw.parquet 擴充到 1958 stocks，Phase 2 算 historical
avg_tv_60d 識別出 1848 stocks 曾過 10M TWD 門檻 (真實會進候選池)。Phase 3 對這 1848 中
142 unique missing 補 financial / revenue / smart_money 等 panel。

設計:
  - 讀 `_phase3_targets.json` 拿 per-panel target list
  - 走 fetch_financial_history.py 的同樣 FinMind rate-limited 流程
  - 寫 per-stock live cache (fundamental_cache/)，最後跑 aggregate_fundamental_cache.py
  - smart_money 沒在 financial tool 範圍 → 走 TaiwanStockInstitutionalInvestorsBuySell 單獨抓

Output:
  - data_cache/fundamental_cache/{cache_key}_{sid}.parquet (per stock, per panel)
  - data_cache/backtest/_phase3_fetch.log (run log)
  - Aggregate 之後 financials_*.parquet + revenue + smart_money 都會更新

Usage:
    python tools/whale_picks_backfill_panels.py                  # 全跑
    python tools/whale_picks_backfill_panels.py --sample 5       # 測試 5 檔
    python tools/whale_picks_backfill_panels.py --skip-smart-money  # 只補 financials/revenue
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("phase3_panels")

OUT_DIR = REPO / "data_cache" / "backtest"
LIVE_CACHE_DIR = REPO / "data_cache" / "fundamental_cache"
LIVE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
TARGETS_PATH = OUT_DIR / "_phase3_targets.json"
LOG_PATH = OUT_DIR / "_phase3_fetch.log"

# Map: panel parquet file → (FinMind dataset, fundamental_cache key)
# fundamental_cache key 對齊 cache_manager.get_cached_fundamentals()
PANEL_MAP = {
    'financials_income.parquet':   ('TaiwanStockFinancialStatements', 'financial_statement'),
    'financials_balance.parquet':  ('TaiwanStockBalanceSheet',        'balance_sheet'),
    'financials_cashflow.parquet': ('TaiwanStockCashFlowsStatement',  'cash_flows_statement'),
    'financials_revenue.parquet':  ('TaiwanStockMonthRevenue',        'month_revenue'),
    'revenue_scores_monthly.parquet': ('TaiwanStockMonthRevenue',     'month_revenue'),  # same source
    # quality_scores: derived from financials, no separate fetch
}

# smart_money goes through different API
SMART_MONEY_PANEL = 'smart_money_scores.parquet'

START_DATE = '2015-01-01'  # 跟 OHLCV 對齊
FINMIND_RATE_LIMIT = 600
HOURLY_PAUSE = 65
BATCH_PAUSE = 0.5


def load_targets() -> dict:
    if not TARGETS_PATH.exists():
        raise FileNotFoundError(f"{TARGETS_PATH} missing — run audit first")
    with open(TARGETS_PATH, 'r') as f:
        return json.load(f)


def fetch_one_panel(dl, sid: str, panel_file: str, start_date: str) -> bool:
    """Fetch one panel for one stock. Return True on success."""
    dataset_info = PANEL_MAP.get(panel_file)
    if dataset_info is None:
        logger.warning("  %s: no FinMind mapping for %s, skip", sid, panel_file)
        return False
    dataset, cache_key = dataset_info
    try:
        if dataset == 'TaiwanStockFinancialStatements':
            df = dl.taiwan_stock_financial_statement(stock_id=sid, start_date=start_date)
        elif dataset == 'TaiwanStockBalanceSheet':
            df = dl.taiwan_stock_balance_sheet(stock_id=sid, start_date=start_date)
        elif dataset == 'TaiwanStockCashFlowsStatement':
            df = dl.taiwan_stock_cash_flows_statement(stock_id=sid, start_date=start_date)
        elif dataset == 'TaiwanStockMonthRevenue':
            df = dl.taiwan_stock_month_revenue(stock_id=sid, start_date=start_date)
        else:
            return False

        if df is None or df.empty:
            return False
        df = df.copy()
        df['stock_id'] = sid
        live_path = LIVE_CACHE_DIR / f"{cache_key}_{sid}.parquet"
        df.to_parquet(live_path)
        return True
    except Exception as e:
        logger.warning("  %s/%s: %s", sid, panel_file, str(e)[:80])
        return False


def fetch_smart_money(dl, sid: str, start_date: str) -> bool:
    """Fetch institutional investor buy/sell for one stock."""
    try:
        # 2026-05-23 修：method 是 taiwan_stock_institutional_investors（沒 _buy_sell）
        df = dl.taiwan_stock_institutional_investors(
            stock_id=sid, start_date=start_date)
        if df is None or df.empty:
            return False
        df = df.copy()
        df['stock_id'] = sid
        live_path = LIVE_CACHE_DIR / f"institutional_{sid}.parquet"
        df.to_parquet(live_path)
        return True
    except Exception as e:
        logger.warning("  %s/smart_money: %s", sid, str(e)[:80])
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--sample', type=int, default=None, help='Test mode: limit to N stocks')
    p.add_argument('--start', default=START_DATE)
    p.add_argument('--skip-smart-money', action='store_true', help='Skip institutional fetch')
    p.add_argument('--only-critical', action='store_true', help='Only fetch critical (no sparse)')
    p.add_argument('--skip-aggregate', action='store_true', help='Skip post-aggregate step')
    args = p.parse_args()

    targets = load_targets()
    logger.info("Targets loaded:")
    logger.info("  critical missing: %d", len(targets['critical_missing']))
    logger.info("  sparse 2018-2020: %d", len(targets['sparse_2018_2020']))
    logger.info("  total unique: %d", len(targets['all_to_fetch']))

    # Build per-panel work list
    # For critical: use critical_per_panel
    # For sparse: use per_panel
    # 2026-05-23: skip quality_scores.parquet (derived in Phase 4 from financials, not fetched directly)
    SKIP_PANELS = {'quality_scores.parquet'}
    work_list = []  # list of (stock_id, panel_file)
    for panel, sids in targets.get('critical_per_panel', {}).items():
        if panel in SKIP_PANELS:
            continue
        for sid in sids:
            work_list.append((sid, panel, 'critical'))
    if not args.only_critical:
        for panel, sids in targets.get('per_panel', {}).items():
            if panel in SKIP_PANELS:
                continue
            for sid in sids:
                work_list.append((sid, panel, 'sparse'))

    # Dedup work_list by (sid, dataset) — same dataset shared across panels
    # 2026-05-23 修：原本 dedup by (sid, panel)，導致同源 dataset (e.g. TaiwanStockMonthRevenue)
    # 被 financials_revenue.parquet + revenue_scores_monthly.parquet 各打一次浪費 quota
    def _dataset_for_panel(panel):
        info = PANEL_MAP.get(panel)
        return info[0] if info else panel  # for smart_money_scores → use panel as key
    seen = set()
    deduped = []
    for sid, panel, tag in work_list:
        dataset = _dataset_for_panel(panel)
        key = (sid, dataset)
        if key not in seen:
            seen.add(key)
            deduped.append((sid, panel, tag))
    work_list = deduped
    logger.info("After dedup by dataset: %d unique fetches", len(work_list))

    if args.sample:
        work_list = work_list[:args.sample]

    logger.info("Total panel-fetches planned: %d", len(work_list))

    from cache_manager import get_finmind_loader
    dl = get_finmind_loader()

    # Separate smart_money work
    sm_targets = [w for w in work_list if w[1] == SMART_MONEY_PANEL]
    panel_targets = [w for w in work_list if w[1] != SMART_MONEY_PANEL]

    if args.skip_smart_money:
        sm_targets = []
        logger.info("  --skip-smart-money: skipping %d institutional fetches", len(sm_targets))

    # Fetch panel data
    call_count = 0
    hour_start = time.time()
    success = 0
    failed = []
    t0 = time.time()

    logger.info("=" * 60)
    logger.info("Fetching %d panel records...", len(panel_targets))
    for i, (sid, panel, tag) in enumerate(panel_targets):
        # Rate limit
        elapsed = time.time() - hour_start
        if elapsed < 3600 and call_count >= FINMIND_RATE_LIMIT - 10:
            wait = 3600 - elapsed + 5
            logger.warning("Rate limit approaching (%d calls in %.0fs), waiting %.0fs...",
                          call_count, elapsed, wait)
            time.sleep(wait)
            call_count = 0
            hour_start = time.time()
        elif elapsed >= 3600:
            call_count = 0
            hour_start = time.time()

        if (i + 1) % 25 == 0 or i < 5:
            logger.info("[%d/%d] %s -> %s (%s, calls=%d)",
                       i + 1, len(panel_targets), sid, panel, tag, call_count)

        ok = fetch_one_panel(dl, sid, panel, args.start)
        if ok:
            success += 1
        else:
            failed.append((sid, panel))
        call_count += 1
        time.sleep(BATCH_PAUSE)

    # Fetch smart_money
    if sm_targets:
        logger.info("=" * 60)
        logger.info("Fetching %d institutional records...", len(sm_targets))
        for i, (sid, panel, tag) in enumerate(sm_targets):
            elapsed = time.time() - hour_start
            if elapsed < 3600 and call_count >= FINMIND_RATE_LIMIT - 10:
                wait = 3600 - elapsed + 5
                logger.warning("Rate limit approaching (%d calls in %.0fs), waiting %.0fs",
                              call_count, elapsed, wait)
                time.sleep(wait)
                call_count = 0
                hour_start = time.time()
            elif elapsed >= 3600:
                call_count = 0
                hour_start = time.time()

            if (i + 1) % 25 == 0 or i < 3:
                logger.info("[%d/%d] %s -> smart_money (%s, calls=%d)",
                           i + 1, len(sm_targets), sid, tag, call_count)

            ok = fetch_smart_money(dl, sid, args.start)
            if ok:
                success += 1
            else:
                failed.append((sid, panel))
            call_count += 1
            time.sleep(BATCH_PAUSE)

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info("Phase 3 fetch done in %.1f min", elapsed / 60)
    logger.info("  Success: %d / %d", success, len(panel_targets) + len(sm_targets))
    logger.info("  Failed:  %d", len(failed))

    # Log to file
    summary = {
        'phase': 'phase3_panel_backfill',
        'run_at': datetime.now().isoformat(timespec='seconds'),
        'start_date': args.start,
        'panel_fetches_attempted': len(panel_targets),
        'smart_money_attempted': len(sm_targets),
        'success_count': success,
        'failed_pairs': [{'sid': s, 'panel': p} for s, p in failed],
        'elapsed_min': round(elapsed / 60, 2),
    }
    LOG_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info("Log: %s", LOG_PATH)

    if not args.skip_aggregate:
        logger.info("=" * 60)
        logger.info("Running aggregate_fundamental_cache.py to merge fundamental_cache → backtest/")
        result = subprocess.run(
            [sys.executable, str(REPO / "tools" / "aggregate_fundamental_cache.py")],
            cwd=str(REPO),
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            logger.info("Aggregate OK")
            for line in result.stdout.splitlines()[-15:]:
                logger.info("  %s", line)
        else:
            logger.error("Aggregate FAILED (rc=%d):\n%s", result.returncode, result.stderr)


if __name__ == '__main__':
    main()
