"""
append_today_pcr_fgi.py -- 每日 archiver stage (BL-5 Part 2)

由 run_taifex_signals_afterclose.bat 每日 14:35 / 15:30 / 16:30 觸發，
incremental update:
  - data/sentiment/pcr_history.parquet (PCR 從 TAIFEX 抓今月 + dedupe append)
  - data/sentiment/fgi_history.parquet (重算全部 6 component scores 覆寫)

Dedup 規則：以 date index 為準，新 row 蓋舊 row（capture 同日後續修正）。

設計：
  - PCR: 只抓今月（1-2s），跟 existing 合併保留歷史
  - FGI: 全跑（~3s），覆寫所有 row（component 邏輯穩定，重算等價）
"""
from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Reuse backfill module's fetch functions
from tools.backfill_pcr_fgi_history import (  # noqa: E402
    PCR_OUT,
    FGI_OUT,
    TAIFEX_HEADERS,
    _fetch_pcr_one_month,
    build_fgi_history,
)

import requests  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("append_today_pcr_fgi")


def append_pcr_today() -> int:
    """Fetch this month's PCR from TAIFEX and merge with existing parquet.

    Returns
    -------
    int : number of new rows added (post-dedup)
    """
    if not PCR_OUT.exists():
        logger.warning("PCR_OUT not found: %s -- run backfill first", PCR_OUT)
        return 0

    existing = pd.read_parquet(PCR_OUT)
    last_date = existing.index.max()
    logger.info("PCR existing: %d rows, last_date=%s", len(existing), last_date.date())

    today = date.today()
    session = requests.Session()
    session.headers.update(TAIFEX_HEADERS)

    new_df = _fetch_pcr_one_month(session, today.year, today.month)
    if new_df.empty:
        logger.info("PCR: no rows from TAIFEX for %d-%02d", today.year, today.month)
        return 0

    # Merge: new rows shadow existing (capture same-day correction)
    merged = pd.concat([existing, new_df])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()

    n_added = len(merged) - len(existing)
    if n_added == 0:
        logger.info("PCR: no new rows after dedup (already up to date)")
        return 0

    merged.to_parquet(PCR_OUT)
    logger.info("PCR saved: %d rows (was %d, +%d new)",
                len(merged), len(existing), n_added)
    return n_added


def append_fgi_today() -> int:
    """Recompute full FGI history (cheap ~3s) and overwrite parquet.

    FGI uses ^TWII full history + 5 components; component logic is stable,
    so recomputing all rows is equivalent to incremental but simpler.

    Returns
    -------
    int : number of rows in resulting parquet (informational)
    """
    if not PCR_OUT.exists():
        logger.warning("PCR_OUT missing -- FGI's pcr_score component will be NaN where pcr=NaN")
        pcr_df = None
    else:
        pcr_df = pd.read_parquet(PCR_OUT)

    fgi_df = build_fgi_history(pcr_df)
    n_rows = len(fgi_df)
    logger.info("FGI saved: %d rows (latest=%s, score=%.1f)",
                n_rows, fgi_df.index.max().date(),
                float(fgi_df["score"].iloc[-1]) if not pd.isna(fgi_df["score"].iloc[-1]) else float("nan"))
    return n_rows


def main():
    logger.info("=== Daily PCR + FGI append ===")
    try:
        pcr_added = append_pcr_today()
    except Exception as e:
        logger.error("PCR append failed: %s", e)
        pcr_added = -1

    try:
        fgi_total = append_fgi_today()
    except Exception as e:
        logger.error("FGI append failed: %s", e)
        fgi_total = -1

    if pcr_added >= 0 and fgi_total >= 0:
        logger.info("=== Done: PCR +%d new rows; FGI %d total rows ===", pcr_added, fgi_total)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
