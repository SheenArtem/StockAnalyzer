"""Daily ATM PUT premium archiver.

Wraps `taifex_data.TAIFEXData.get_atm_put_premium()` and appends today's row
to `data/sentiment/atm_put_premium.parquet` (deduped by data_date).

Purpose: accumulate historical baseline so atm_put_pct / put_skew can be
z-scored or threshold-monitored later (>= 30 trading days needed for
meaningful baseline).

Usage (scanner stage / manual):
    python tools/fetch_atm_put_premium.py

Best-effort: failures do not affect scanner exit code.
"""
import logging
import sys
from datetime import date as _date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import pandas as pd

from taifex_data import TAIFEXData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("atm_put_archive")

ARCHIVE_PATH = REPO / "data" / "sentiment" / "atm_put_premium.parquet"


def _normalize(row: dict) -> dict:
    """Coerce data_date to ISO string for parquet stable schema."""
    d = row.get("data_date")
    if isinstance(d, _date):
        row["data_date"] = d.isoformat()
    elif d is None:
        row["data_date"] = ""
    return row


def main() -> int:
    td = TAIFEXData()
    r = td.get_atm_put_premium()

    if not r.get("data_date") or not r.get("atm_strike"):
        log.warning("ATM PUT premium fetch returned empty (atm_strike=%s, date=%s); skip archive",
                    r.get("atm_strike"), r.get("data_date"))
        return 0  # best-effort: don't fail scanner

    r = _normalize(dict(r))
    today_str = r["data_date"]
    log.info("Fetched: date=%s ref=%.0f ATM=%d close=%.1f pct=%.3f%% OTM5=%d skew=%.3f",
             today_str, r["reference"], r["atm_strike"], r["atm_put_close"],
             r["atm_put_pct"], r["otm5_strike"], r["put_skew"])

    ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if ARCHIVE_PATH.exists():
        try:
            existing = pd.read_parquet(ARCHIVE_PATH)
        except Exception as e:
            log.warning("Existing archive read failed (%s), starting fresh", e)
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    new_row = pd.DataFrame([r])
    if not existing.empty:
        # Dedupe by data_date — re-runs same day are idempotent (overwrite latest)
        merged = pd.concat([existing[existing["data_date"] != today_str], new_row],
                           ignore_index=True)
    else:
        merged = new_row
    merged = merged.sort_values("data_date").reset_index(drop=True)
    merged.to_parquet(ARCHIVE_PATH, index=False)

    log.info("Archive: %d rows total -> %s", len(merged), ARCHIVE_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
