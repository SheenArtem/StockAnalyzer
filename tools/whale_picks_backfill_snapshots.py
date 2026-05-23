"""
Whale Picks snapshot backfill — 一次性工具

緣由 (2026-05-23): 2026-05-16~2026-05-21 期間 daily scheduler 沒跑 refresh_backtest_panels
(該 hook v2026.05.22.4 才上線), 那 6 天的 snapshot 是 stale 的 (composite_score 排名
還停留在 5/15 M15 rebal 之前)。今天 5/22 first refresh 後，UI 顯示 5/22 vs 5/21 diff
出現假性「7 進 7 出」(其實是 5/15 M15 rebal 的延遲顯示)。

本 script 用今日已 fresh 的 raw cache (ohlcv_tw / smart_money / quality / revenue /
financials) 對那 6 天逐一重算 composite_score，覆寫 dated parquet。

**不會動到** `latest.parquet` 跟 `data/latest/whale_picks_top20.json` —
只重寫 `data/whale_picks/{YYYY-MM-DD}.parquet`。

Usage:
    python tools/whale_picks_backfill_snapshots.py --start 2026-05-16 --end 2026-05-21
    python tools/whale_picks_backfill_snapshots.py --dates 2026-05-16,2026-05-17
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("whale_picks_backfill")

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from tools.whale_picks_screener import score_universe, attach_metadata, apply_hard_exclusions, OUT_DIR


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        # 只跑 weekday (台股交易日近似; 真實節假日 ohlcv 自然會少筆,但 snapshot 仍可寫)
        if d.weekday() < 5:
            yield d
        d = d + timedelta(days=1)


def backfill_one(asof: date) -> dict:
    """Recompute and overwrite the dated snapshot for asof. Returns summary stats."""
    fp = OUT_DIR / f"{asof.isoformat()}.parquet"
    old_rows = None
    if fp.exists():
        try:
            old_rows = len(pd.read_parquet(fp, columns=['stock_id']))
        except Exception:
            pass

    scored = score_universe(asof)
    enriched = attach_metadata(scored)
    enriched = apply_hard_exclusions(enriched)

    enriched.to_parquet(fp, index=False)
    new_rows = len(enriched)
    valid_score = int(enriched['composite_score'].notna().sum())

    log.info("[%s] wrote %s (old_rows=%s -> new_rows=%d, valid_composite_score=%d)",
             asof.isoformat(), fp, old_rows, new_rows, valid_score)
    return {
        'date': asof.isoformat(),
        'old_rows': old_rows,
        'new_rows': new_rows,
        'valid_score': valid_score,
        'path': str(fp),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--start', help='ISO date YYYY-MM-DD (inclusive)')
    p.add_argument('--end', help='ISO date YYYY-MM-DD (inclusive)')
    p.add_argument('--dates', help='Comma-separated ISO dates (overrides --start/--end)')
    args = p.parse_args()

    if args.dates:
        dates = [date.fromisoformat(s.strip()) for s in args.dates.split(',') if s.strip()]
    elif args.start and args.end:
        s = date.fromisoformat(args.start)
        e = date.fromisoformat(args.end)
        dates = list(_daterange(s, e))
    else:
        p.error("Need --dates OR (--start AND --end)")

    log.info("Backfill plan: %d dates: %s", len(dates), [d.isoformat() for d in dates])

    results = []
    for d in dates:
        try:
            r = backfill_one(d)
            results.append(r)
        except Exception as ex:
            log.error("[%s] backfill FAILED: %s", d.isoformat(), ex)
            results.append({'date': d.isoformat(), 'error': str(ex)})

    log.info("Summary:")
    for r in results:
        if 'error' in r:
            log.info("  %s: ERROR %s", r['date'], r['error'])
        else:
            log.info("  %s: rows %s -> %d, valid_score=%d",
                     r['date'], r.get('old_rows'), r['new_rows'], r['valid_score'])


if __name__ == '__main__':
    main()
