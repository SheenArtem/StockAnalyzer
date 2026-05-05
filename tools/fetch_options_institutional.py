"""Daily TXO institutional buy/sell OI archiver — 三大法人選擇權持倉.

Wraps `taifex_data.TAIFEXData.get_options_institutional()` and appends today's row
to `data/sentiment/options_institutional.parquet` (deduped by data_date).

Purpose: accumulate baseline for inst_pc_oi_skew z-score (>= 30 trading days)
and for IC validation against forward 5/10/20d returns.

Reads:
  foreign/trust/dealer × call/put net OI (口) + 3 derived totals

Usage:
    python tools/fetch_options_institutional.py

Best-effort: failures do not affect scanner exit.
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
log = logging.getLogger("options_inst_archive")

ARCHIVE_PATH = REPO / "data" / "sentiment" / "options_institutional.parquet"


def _normalize(row: dict) -> dict:
    d = row.get("data_date")
    if isinstance(d, _date):
        row["data_date"] = d.isoformat()
    elif d is None:
        row["data_date"] = ""
    return row


def main() -> int:
    td = TAIFEXData()
    r = td.get_options_institutional()

    if not r.get("data_date"):
        log.warning("TXO institutional fetch returned empty (date=%s); skip archive",
                    r.get("data_date"))
        return 0  # best-effort

    r = _normalize(dict(r))
    today_str = r["data_date"]
    log.info(
        "Fetched: date=%s foreign C/P=%d/%d trust C/P=%d/%d dealer C/P=%d/%d skew=%d",
        today_str,
        r["foreign_call_net"], r["foreign_put_net"],
        r["trust_call_net"], r["trust_put_net"],
        r["dealer_call_net"], r["dealer_put_net"],
        r["inst_pc_oi_skew"],
    )

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
