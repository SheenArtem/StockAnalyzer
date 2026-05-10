"""
SBL History Backfill -- Expand from 33 to ~2000 stocks.

Dataset: FinMind taiwan_daily_short_sale_balances (per-stock, returns full history
         from ~2016 in one API call).  1 req per stock = simple quota math.

Output: data_cache/{stock_id}_sbl_chip.csv
Schema: date (index), 借券賣出餘額, 借券賣出, 借券還券, 借券調整
        (same as chip_analysis.py cache_key={stock_id}_sbl written via CacheManager)

Quota: FinMind 600 req/hr free tier.
       Sleep 7.2 s between requests => ~500 req/hr (83% utilisation, safe margin).
       Night-1: first 500 stocks of remaining universe.
       Night-2/3: subsequent batches.

Checkpoint: data_cache/sbl_backfill_progress.json
            Flushes every CHECKPOINT_EVERY stocks.  Resumes automatically on re-run.

Usage:
    python tools/sbl_history_backfill.py                    # run batch-1 (up to 500 stocks)
    python tools/sbl_history_backfill.py --batch-size 300   # smaller batch
    python tools/sbl_history_backfill.py --dry-run          # print plan, no API calls
    python tools/sbl_history_backfill.py --verify           # check 5 sample stocks only
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = _ROOT / "data_cache"
PROGRESS_FILE = CACHE_DIR / "sbl_backfill_progress.json"
UNIVERSE_PATH = CACHE_DIR / "backtest" / "universe_tw.parquet"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SLEEP_BETWEEN_REQS = 7.2        # seconds -> ~500 req/hr (quota safe)
DEFAULT_BATCH_SIZE  = 500       # stocks per night
CHECKPOINT_EVERY    = 50        # flush progress every N stocks
SBL_START_DATE      = "2016-01-01"  # FinMind data goes back to 2016

# Rate guard (mirrors chip_history_dl.py)
_req_count  = 0
_hour_start = time.time()
FINMIND_MAX_PER_HOUR = 560      # hard ceiling (600 - 40 buffer)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sbl_backfill")

# ---------------------------------------------------------------------------
# Column mapping: FinMind -> Chinese cache schema
# ---------------------------------------------------------------------------
COL_MAP = {
    "SBLShortSalesCurrentDayBalance": "借券賣出餘額",
    "SBLShortSalesShortSales":        "借券賣出",
    "SBLShortSalesReturns":           "借券還券",
    "SBLShortSalesAdjustments":       "借券調整",
}


# ---------------------------------------------------------------------------
# Universe helpers
# ---------------------------------------------------------------------------
def load_universe() -> list[str]:
    """
    Return sorted list of TW stock IDs to backfill.
    Source: data_cache/backtest/universe_tw.parquet (2127 stocks, TWSE + TPEX).
    Fallback: scan data_cache/*_price.csv for 4-digit numeric IDs.
    """
    if UNIVERSE_PATH.exists():
        u = pd.read_parquet(UNIVERSE_PATH)
        ids = sorted(u["stock_id"].astype(str).unique().tolist())
        logger.info("Universe loaded from parquet: %d stocks", len(ids))
        return ids

    # Fallback: price cache scan
    logger.warning("universe_tw.parquet not found; falling back to price cache scan")
    ids = []
    for f in CACHE_DIR.glob("*_price.csv"):
        sid = f.name.replace("_price.csv", "")
        if re.match(r"^\d{4}$", sid):  # 4-digit TW stocks only
            ids.append(sid)
    ids = sorted(set(ids))
    logger.info("Fallback universe: %d stocks from price cache", len(ids))
    return ids


def already_done() -> set[str]:
    """IDs that already have a valid sbl_chip.csv (non-empty file)."""
    done = set()
    for f in CACHE_DIR.glob("*_sbl_chip.csv"):
        sid = f.name.replace("_sbl_chip.csv", "")
        if f.stat().st_size > 50:   # >50 bytes -> has at least header + 1 row
            done.add(sid)
    return done


# ---------------------------------------------------------------------------
# Progress checkpoint
# ---------------------------------------------------------------------------
def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            with PROGRESS_FILE.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    return {"completed": [], "failed": [], "empty": [], "batches": []}


def save_progress(prog: dict) -> None:
    tmp = PROGRESS_FILE.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(prog, fh, ensure_ascii=False, indent=2)
    tmp.replace(PROGRESS_FILE)


# ---------------------------------------------------------------------------
# FinMind rate guard
# ---------------------------------------------------------------------------
def _rate_check() -> None:
    global _req_count, _hour_start
    elapsed = time.time() - _hour_start
    if elapsed >= 3600:
        _req_count  = 0
        _hour_start = time.time()
        return
    if _req_count >= FINMIND_MAX_PER_HOUR:
        wait = 3600 - elapsed + 15
        logger.warning(
            "Quota guard: %d req in %.0f s. Pausing %.0f s...",
            _req_count, elapsed, wait,
        )
        time.sleep(wait)
        _req_count  = 0
        _hour_start = time.time()


# ---------------------------------------------------------------------------
# Fetch + write one stock
# ---------------------------------------------------------------------------
def fetch_and_write(dl, stock_id: str, start_date: str = SBL_START_DATE) -> str:
    """
    Fetch SBL history for stock_id from FinMind and write to cache CSV.

    Returns: "ok" | "empty" | "error:<msg>"
    """
    global _req_count
    _rate_check()

    try:
        df_raw = dl.taiwan_daily_short_sale_balances(
            stock_id=stock_id,
            start_date=start_date,
        )
        _req_count += 1
    except Exception as e:
        _req_count += 1
        msg = str(e)[:120]
        logger.warning("[%s] FinMind call failed: %s", stock_id, msg)
        return f"error:{msg}"

    if df_raw is None or df_raw.empty:
        logger.debug("[%s] Empty response (no SBL trading)", stock_id)
        return "empty"

    if "date" not in df_raw.columns:
        logger.warning("[%s] 'date' column missing in response", stock_id)
        return "error:no_date_column"

    # Build the output frame in the same schema as chip_analysis.py
    df_raw["date"] = pd.to_datetime(df_raw["date"])
    df_raw = df_raw.set_index("date").sort_index()

    avail = [c for c in COL_MAP if c in df_raw.columns]
    if not avail:
        logger.warning("[%s] None of the SBL columns found: %s", stock_id, df_raw.columns.tolist())
        return "error:no_sbl_columns"

    df_out = df_raw[avail].copy()
    df_out.rename(columns={c: COL_MAP[c] for c in avail}, inplace=True)

    # Write (same atomic approach as CacheManager.save_cache)
    out_path = CACHE_DIR / f"{stock_id}_sbl_chip.csv"
    tmp_path = out_path.with_suffix(".csv.tmp")
    try:
        df_out.to_csv(tmp_path, encoding="utf-8")
        tmp_path.replace(out_path)
    except Exception as e:
        logger.error("[%s] Write failed: %s", stock_id, e)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return f"error:write:{e}"

    logger.info("[%s] OK — %d rows, %s ~ %s",
                stock_id, len(df_out),
                df_out.index.min().strftime("%Y-%m-%d") if len(df_out) else "N/A",
                df_out.index.max().strftime("%Y-%m-%d") if len(df_out) else "N/A")
    return "ok"


# ---------------------------------------------------------------------------
# Verify mode (sample 5 stocks)
# ---------------------------------------------------------------------------
VERIFY_STOCKS = ["2330", "0050", "2454", "6669", "3675"]  # large/mid/small/tpex

def run_verify(dl) -> None:
    logger.info("=== VERIFY MODE: checking %d sample stocks ===", len(VERIFY_STOCKS))
    for sid in VERIFY_STOCKS:
        out_path = CACHE_DIR / f"{sid}_sbl_chip.csv"
        if out_path.exists():
            df = pd.read_csv(out_path, index_col=0, parse_dates=True, encoding="utf-8")
            logger.info(
                "[%s] cache: %d rows, %s ~ %s, cols=%s",
                sid, len(df),
                str(df.index.min())[:10] if len(df) else "N/A",
                str(df.index.max())[:10] if len(df) else "N/A",
                df.columns.tolist(),
            )
        else:
            result = fetch_and_write(dl, sid)
            logger.info("[%s] fetch result: %s", sid, result)
            time.sleep(SLEEP_BETWEEN_REQS)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="SBL history backfill for 1500+ TW stocks")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Max stocks per run (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without making API calls")
    parser.add_argument("--verify", action="store_true",
                        help="Verify 5 sample stocks only")
    parser.add_argument("--start-date", default=SBL_START_DATE,
                        help=f"Earliest date to fetch (default: {SBL_START_DATE})")
    args = parser.parse_args()

    # ---- Init FinMind ----
    try:
        from FinMind.data import DataLoader
        dl = DataLoader()
        logger.info("FinMind DataLoader initialised")
    except Exception as e:
        logger.error("Cannot import FinMind: %s", e)
        sys.exit(1)

    if args.verify:
        run_verify(dl)
        return

    # ---- Universe ----
    universe   = load_universe()
    done_files = already_done()
    prog       = load_progress()

    # Merge already-done from files + progress json
    done_set = done_files | set(prog.get("completed", [])) | set(prog.get("empty", []))
    remaining = [sid for sid in universe if sid not in done_set]

    logger.info(
        "Universe: %d | Already done: %d | Remaining: %d",
        len(universe), len(done_set), len(remaining),
    )

    # ---- Batch selection ----
    batch = remaining[: args.batch_size]
    logger.info("This run: %d stocks (batch_size=%d)", len(batch), args.batch_size)

    if args.dry_run:
        est_min = len(batch) * SLEEP_BETWEEN_REQS / 60
        logger.info("DRY-RUN -- no API calls. Estimated time: %.0f min", est_min)
        logger.info("First 20 to fetch: %s", batch[:20])
        nights_left = max(0, (len(remaining) - len(batch)) / args.batch_size)
        logger.info("After this batch, remaining: %d (%.1f more nights)",
                    len(remaining) - len(batch), nights_left)
        return

    if not batch:
        logger.info("All stocks done. Nothing to fetch.")
        return

    # ---- Run ----
    t0 = time.time()
    ok_count    = 0
    empty_count = 0
    fail_count  = 0
    batch_started = datetime.now().isoformat()

    for i, stock_id in enumerate(batch, 1):
        result = fetch_and_write(dl, stock_id, start_date=args.start_date)

        if result == "ok":
            ok_count += 1
            prog["completed"].append(stock_id)
        elif result == "empty":
            empty_count += 1
            prog["empty"].append(stock_id)
        else:
            fail_count += 1
            prog["failed"].append(stock_id)
            logger.warning("[%s] failed: %s", stock_id, result)

        # Checkpoint
        if i % CHECKPOINT_EVERY == 0:
            save_progress(prog)
            elapsed_m = (time.time() - t0) / 60
            logger.info(
                "Checkpoint %d/%d | ok=%d empty=%d fail=%d | elapsed=%.1f min",
                i, len(batch), ok_count, empty_count, fail_count, elapsed_m,
            )

        # Rate-limited sleep (skip after last item)
        if i < len(batch):
            time.sleep(SLEEP_BETWEEN_REQS)

    # ---- Final checkpoint ----
    elapsed_total = time.time() - t0
    prog["batches"].append({
        "started":     batch_started,
        "finished":    datetime.now().isoformat(),
        "batch_count": len(batch),
        "ok":          ok_count,
        "empty":       empty_count,
        "failed":      fail_count,
        "elapsed_min": round(elapsed_total / 60, 1),
    })
    save_progress(prog)

    # ---- Summary ----
    total_done  = len(done_set) + ok_count + empty_count
    still_left  = len(universe) - total_done
    nights_left = max(0, still_left / args.batch_size)

    print()
    print("=" * 60)
    print(f"SBL Backfill Night Summary")
    print("=" * 60)
    print(f"  Batch size     : {len(batch)}")
    print(f"  OK (written)   : {ok_count}")
    print(f"  Empty (no SBL) : {empty_count}")
    print(f"  Failed         : {fail_count}")
    print(f"  Elapsed        : {elapsed_total/60:.1f} min")
    print(f"  Quota used     : {_req_count} req this hour")
    print(f"  Quota hit?     : {'YES - check logs' if _req_count >= FINMIND_MAX_PER_HOUR else 'No'}")
    print(f"  Total done now : {total_done} / {len(universe)}")
    print(f"  Still remaining: {still_left}")
    print(f"  Est nights left: {nights_left:.1f}")
    print(f"  Progress file  : {PROGRESS_FILE}")
    print("=" * 60)

    if fail_count > 0:
        failed_ids = [sid for sid in batch
                      if sid not in set(prog["completed"]) and sid not in set(prog["empty"])]
        logger.warning("Failed stocks (%d): %s", fail_count, failed_ids[:20])


if __name__ == "__main__":
    main()
