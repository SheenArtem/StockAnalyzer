"""
Refresh data_cache/TAIEX_price.parquet (FinMind TaiwanStockPrice, stock_id=TAIEX).

System 2 / System 3 daily checks (crash early-warning) read this file as their
only TAIEX source. Root cause 2026-07-06: the parquet was a one-shot manual
fetch during System 2 development (last row 2026-05-08) with NO scheduled
producer -- both checks silently evaluated a frozen snapshot for ~2 months
while reporting green. This tool resume-appends from FinMind and is wired into
run_taifex_signals_afterclose.bat BEFORE the System 2 stage.

Usage:
  python tools/refresh_taiex_price.py           # resume append from last saved date
  python tools/refresh_taiex_price.py --full    # refetch entire history (1999-01-05+)

Exit codes:
  0 = updated or legitimately nothing to do (weekend / holiday / not yet published)
  1 = fetch failed, or data still stale beyond STALE_LIMIT_DAYS (fail loud)
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OUT_PATH = ROOT / "data_cache" / "TAIEX_price.parquet"
FULL_START = "1999-01-01"

# TW market longest closure = Lunar New Year (~9-11 calendar days).
# Beyond this with no new data = producer/source is broken, not a holiday.
STALE_LIMIT_DAYS = 12

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("refresh_taiex")


def _has_weekday(start: date, end: date) -> bool:
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            return True
        cur += timedelta(days=1)
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Resume-append TAIEX daily bars from FinMind")
    ap.add_argument("--full", action="store_true", help="refetch entire history instead of resume")
    args = ap.parse_args()

    from cache_manager import get_finmind_loader

    today = date.today()
    existing = None
    start_iso = FULL_START

    if not args.full and OUT_PATH.exists():
        existing = pd.read_parquet(OUT_PATH)
        last_iso = str(existing["date"].max())
        last = date.fromisoformat(last_iso)
        if last >= today:
            logger.info("Already up-to-date (last=%s)", last_iso)
            return 0
        start = last + timedelta(days=1)
        if not _has_weekday(start, today):
            logger.info("No weekday in %s ~ %s (weekend window), nothing to fetch (last=%s)",
                        start, today, last_iso)
            return 0
        start_iso = start.isoformat()
        logger.info("Resume: last saved %s, fetching %s ~ %s", last_iso, start_iso, today)
    else:
        logger.info("Full fetch: %s ~ %s", start_iso, today)

    try:
        dl = get_finmind_loader()
        df_new = dl.taiwan_stock_daily(stock_id="TAIEX",
                                       start_date=start_iso,
                                       end_date=today.isoformat())
    except Exception as e:
        logger.error("FinMind TAIEX fetch failed: %s", e)
        return 1

    if df_new is None or df_new.empty:
        if existing is not None:
            age = (today - date.fromisoformat(str(existing["date"].max()))).days
            if age > STALE_LIMIT_DAYS:
                logger.error("No new rows AND last bar is %d days old (> %d) -- "
                             "FinMind TAIEX source looks broken, System 2/3 inputs are stale",
                             age, STALE_LIMIT_DAYS)
                return 1
            logger.info("No new rows (holiday or today's bar not yet published); last=%s (%d days old)",
                        existing["date"].max(), age)
            return 0
        logger.error("Full fetch returned empty -- check FinMind token/quota")
        return 1

    if existing is not None:
        # Keep the established schema; guard against upstream column drift
        df_new = df_new.reindex(columns=existing.columns)
        combined = pd.concat([existing, df_new], ignore_index=True)
    else:
        combined = df_new

    combined = (combined.drop_duplicates(subset=["date"], keep="last")
                        .sort_values("date").reset_index(drop=True))
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUT_PATH, index=False)
    logger.info("Saved %s: +%d new rows, %d total, last=%s",
                OUT_PATH.name, len(df_new), len(combined), combined["date"].max())
    return 0


if __name__ == "__main__":
    sys.exit(main())
