"""Daily MTX/TXF (mini/major futures) OI ratio archiver — 散戶倉位 proxy.

Wraps `taifex_data.TAIFEXData.get_minifutures_oi_ratio()` and appends today's row
to `data/sentiment/minifutures_ratio.parquet` (deduped by data_date).

Purpose: accumulate baseline so mtx_txf_ratio can be z-scored later
(>= 30 trading days). MTX 是散戶為主、TXF 法人為主 — ratio 高 = 散戶倉位過大
= 反向訊號。

Usage:
    python tools/fetch_minifutures_ratio.py

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
log = logging.getLogger("minifutures_archive")

ARCHIVE_PATH = REPO / "data" / "sentiment" / "minifutures_ratio.parquet"


def _normalize(row: dict) -> dict:
    d = row.get("data_date")
    if isinstance(d, _date):
        row["data_date"] = d.isoformat()
    elif d is None:
        row["data_date"] = ""
    return row


def main() -> int:
    td = TAIFEXData()
    r = td.get_minifutures_oi_ratio()

    if not r.get("data_date") or not r.get("txf_oi"):
        log.warning("MTX/TXF ratio fetch returned empty (txf_oi=%s, date=%s); skip archive",
                    r.get("txf_oi"), r.get("data_date"))
        return 0  # best-effort

    r = _normalize(dict(r))
    today_str = r["data_date"]
    log.info("Fetched: date=%s near=%s TX_OI=%d MTX_OI=%d ratio=%.4f",
             today_str, r["near_month"], r["txf_oi"], r["mtx_oi"], r["mtx_txf_ratio"])

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
