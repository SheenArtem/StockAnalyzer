"""
TDCC Portal Historical Scraper -- 51-week backfill
===================================================
Background
----------
TDCC OpenAPI getOD.ashx?id=1-5 returns only the current week's data.
The web portal https://www.tdcc.com.tw/portal/zh/smWeb/qryStock exposes
a dropdown of the last 51 weeks, but delivers per-stock results only
(one stock -> 17 rows / levels per request).  There is no bulk-download
endpoint for historical dates.

This script scrapes the portal per-stock x per-week and assembles the
same parquet schema used by the OpenAPI fetcher (tdcc_shareholding.py).

Scale reality
-------------
  3,971 stocks x 47 missing weeks = ~187 K requests
  At 1.5 s / request -> ~78 hours wall-clock (not feasible in one run)

Use --top-n <N> to limit to the N largest stocks by shares (level 17)
from the most recent existing cache week.  Recommended for IC validation
bootstrap:

    --top-n 500   -> ~23 K req -> ~10 h
    --top-n 100   -> ~4.7 K req -> ~2 h

Run multiple incremental sessions; progress is checkpointed per week.

Storage
-------
  Scratch (per-week CSV):  data_cache/tdcc/1-5/scratch/<date>_partial.csv
  Final parquet:           data_cache/tdcc/1-5/<date>.parquet

  Scratch files survive crashes and are removed after final parquet is
  written successfully.

Usage
-----
  python tools/tdcc_portal_scraper.py                  # all stocks, all missing weeks
  python tools/tdcc_portal_scraper.py --top-n 500      # top 500 by market size
  python tools/tdcc_portal_scraper.py --date 20260417  # single week only
  python tools/tdcc_portal_scraper.py --force          # overwrite existing parquets

Anti-scrape notes
-----------------
  - SYNCHRONIZER_TOKEN rotates every response; always read from latest HTML.
  - JSESSIONID persists in requests.Session; reuse the session object.
  - Sleep 1-2 s per stock request; re-GET the portal page every 200 requests
    to renew session / token freshness.
  - 3 retries per request, backoff 5 s.  If all retries fail the stock-week
    is marked 'partial' and skipped on next run (use --retry-partial to redo).
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
import urllib3
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
OUT_DIR = Path("C:/GIT/StockAnalyzer/data_cache/tdcc/1-5")
SCRATCH_DIR = OUT_DIR / "scratch"
SLEEP_BETWEEN_STOCKS = 1.5   # seconds
SESSION_REFRESH_EVERY = 200  # refresh GET every N stocks
MAX_RETRIES = 3
RETRY_BACKOFF = 5            # seconds

# Table column positions in portal response (positional, not by header text)
# Portal table: col0=level_num, col1=range_label, col2=people_count, col3=shares, col4=pct
COL_LEVEL = 0
COL_PEOPLE = 2
COL_SHARES = 3
COL_PCT = 4

# Same FIELD_MAP as tdcc_shareholding.py for parquet compatibility
DERIVED_FIELD_MAP = {
    "is_retail": lambda lv: 1 <= lv <= 5,
    "is_large":  lambda lv: 11 <= lv <= 15,
    "is_whale":  lambda lv: lv == 15,
    "is_total":  lambda lv: lv == 17,
}


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    })
    return s


def portal_get(session: requests.Session) -> dict:
    """GET the portal page; return {token, uri, fir_date, dates:[str,...]}."""
    resp = session.get(BASE_URL, verify=False, timeout=30)
    resp.raise_for_status()
    text = resp.text

    token_m = re.search(r'name="SYNCHRONIZER_TOKEN"\s+value="([^"]+)"', text)
    uri_m   = re.search(r'name="SYNCHRONIZER_URI"\s+value="([^"]+)"', text)
    fir_m   = re.search(r'name="firDate"\s+value="(\d+)"', text)
    dates   = re.findall(r'<option\s+value="(\d{8})"', text)

    if not token_m or not dates:
        raise RuntimeError("Cannot parse SYNCHRONIZER_TOKEN or date list from portal page")

    return {
        "token":    token_m.group(1),
        "uri":      uri_m.group(1) if uri_m else "/portal/zh/smWeb/qryStock",
        "fir_date": fir_m.group(1) if fir_m else dates[0],
        "dates":    dates,
    }


def _refresh_token_from_html(html_text: str, current: dict) -> dict:
    """Extract updated token from response HTML."""
    token_m = re.search(r'name="SYNCHRONIZER_TOKEN"\s+value="([^"]+)"', html_text)
    if token_m:
        current = dict(current)
        current["token"] = token_m.group(1)
    return current


def fetch_stock_week(
    session: requests.Session,
    ctx: dict,
    stock_id: str,
    sca_date: str,
) -> tuple[list[dict], dict]:
    """POST for one stock + date.  Returns (rows, updated_ctx).

    rows: list of dicts with keys level/people_count/shares/pct
    Updated ctx has latest token from the response.
    On all retries fail returns ([], ctx) and logs a warning.
    """
    payload = {
        "SYNCHRONIZER_TOKEN": ctx["token"],
        "SYNCHRONIZER_URI":   ctx["uri"],
        "method":             "submit",
        "firDate":            ctx["fir_date"],
        "scaDate":            sca_date,
        "sqlMethod":          "StockNo",
        "stockNo":            stock_id,
        "stockName":          "",
    }

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(BASE_URL, data=payload, verify=False, timeout=30)
            resp.raise_for_status()

            ctx = _refresh_token_from_html(resp.text, ctx)
            payload["SYNCHRONIZER_TOKEN"] = ctx["token"]

            soup = BeautifulSoup(resp.content, "html.parser")
            tables = soup.find_all("table")
            if len(tables) < 2:
                # No data table -- may be invalid stock_id or no data for this date
                return [], ctx

            rows_out = []
            for tr in tables[1].find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if not cells or len(cells) < 5:
                    continue
                try:
                    level = int(cells[COL_LEVEL])
                    # people_count can be empty for level 16 (adjustment row)
                    people_str = cells[COL_PEOPLE].replace(",", "").strip()
                    people_count = int(people_str) if people_str else 0
                    shares_str = cells[COL_SHARES].replace(",", "").strip()
                    shares = int(shares_str) if shares_str else 0
                    pct = float(cells[COL_PCT].replace(",", "").strip())
                except (ValueError, IndexError):
                    continue
                rows_out.append({
                    "level":        level,
                    "people_count": people_count,
                    "shares":       shares,
                    "pct":          pct,
                })
            return rows_out, ctx

        except Exception as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF)

    print(f"  [WARN] {stock_id} {sca_date}: all {MAX_RETRIES} retries failed: {last_exc!r}",
          flush=True)
    return [], ctx


# ---------------------------------------------------------------------------
# Scratch file management
# ---------------------------------------------------------------------------

def scratch_path(date: str) -> Path:
    return SCRATCH_DIR / f"{date}_partial.csv"


def load_scratch(date: str) -> dict[str, list[dict]]:
    """Load existing scratch CSV; return {stock_id: [row_dicts]}."""
    p = scratch_path(date)
    if not p.exists():
        return {}
    result: dict[str, list[dict]] = {}
    with open(p, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = row["stock_id"]
            result.setdefault(sid, []).append(row)
    return result


def append_scratch(date: str, stock_id: str, rows: list[dict]) -> None:
    """Append rows for one stock to the scratch CSV."""
    p = scratch_path(date)
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    write_header = not p.exists()
    with open(p, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "stock_id", "data_date", "level", "people_count", "shares", "pct"
        ])
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({
                "stock_id":    stock_id,
                "data_date":   date,
                "level":       row["level"],
                "people_count": row["people_count"],
                "shares":      row["shares"],
                "pct":         row["pct"],
            })


def mark_failed_scratch(date: str, stock_id: str) -> None:
    """Write a sentinel row so resume knows this stock was attempted (and failed)."""
    p = scratch_path(date)
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    write_header = not p.exists()
    with open(p, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "stock_id", "data_date", "level", "people_count", "shares", "pct"
        ])
        if write_header:
            writer.writeheader()
        # sentinel: level=-1 marks failure
        writer.writerow({
            "stock_id": stock_id, "data_date": date,
            "level": -1, "people_count": 0, "shares": 0, "pct": 0.0,
        })


def scratch_to_parquet(date: str, out_path: Path) -> pd.DataFrame:
    """Assemble scratch CSV -> parquet with derived columns."""
    p = scratch_path(date)
    if not p.exists():
        raise FileNotFoundError(f"Scratch file missing: {p}")

    df = pd.read_csv(p, dtype={"stock_id": str, "data_date": str})
    # Drop sentinel rows (failed stocks)
    df = df[df["level"] != -1].copy()
    if df.empty:
        raise ValueError(f"Scratch for {date} contains no valid rows")

    df["level"]        = df["level"].astype(int)
    df["people_count"] = df["people_count"].astype(int)
    df["shares"]       = df["shares"].astype(int)
    df["pct"]          = df["pct"].astype(float)
    df["stock_id"]     = df["stock_id"].str.strip()
    df["data_date"]    = df["data_date"].astype(str).str.strip()

    for col, fn in DERIVED_FIELD_MAP.items():
        df[col] = df["level"].apply(fn)

    df["download_ts"] = datetime.now().isoformat(timespec="seconds")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    return df


# ---------------------------------------------------------------------------
# Stock universe helpers
# ---------------------------------------------------------------------------

def load_stock_ids_from_cache() -> list[str]:
    """Return ordered list of stock_ids from the most recent existing parquet.

    Ordered by descending total shares (level 17) so top-n picks the largest.
    Falls back to reading the TDCC universe download if no existing parquet.
    """
    existing = sorted(OUT_DIR.glob("????????.parquet"))
    if existing:
        df = pd.read_parquet(existing[-1])
        # Sort by total shares descending
        totals = (
            df[df["level"] == 17]
            .sort_values("shares", ascending=False)
        )
        return totals["stock_id"].tolist()

    # Fallback: TDCC universe download
    universe_path = Path("C:/GIT/StockAnalyzer/data_cache/backtest/universe_tw_full.parquet")
    if universe_path.exists():
        uf = pd.read_parquet(universe_path)
        common = uf[uf.get("is_common_stock", pd.Series(True, index=uf.index))]
        return common["stock_id"].tolist()

    raise RuntimeError(
        "Cannot find existing TDCC parquet or universe file to get stock list"
    )


# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape_date(
    date: str,
    stock_ids: list[str],
    force: bool = False,
    retry_partial: bool = False,
) -> dict:
    """Scrape all stock_ids for one historical date.

    Returns stats dict with keys: scraped, skipped, failed, total_rows.
    """
    out_path = OUT_DIR / f"{date}.parquet"
    if out_path.exists() and not force:
        print(f"[tdcc-portal] {date}: parquet exists, skipping (use --force to overwrite)",
              flush=True)
        return {"scraped": 0, "skipped": len(stock_ids), "failed": 0, "total_rows": 0}

    # Load resume state from scratch
    scratch = load_scratch(date)
    already_done = set(scratch.keys())

    pending = [s for s in stock_ids if s not in already_done]
    if not pending and already_done:
        print(f"[tdcc-portal] {date}: all {len(already_done)} stocks in scratch, assembling parquet",
              flush=True)
        df = scratch_to_parquet(date, out_path)
        scratch_path(date).unlink(missing_ok=True)
        print(f"[tdcc-portal] {date}: done, {len(df):,} rows, {len(df['stock_id'].nunique() if True else 0)} stocks",
              flush=True)
        return {"scraped": 0, "skipped": len(already_done), "failed": 0, "total_rows": len(df)}

    print(f"[tdcc-portal] {date}: {len(pending)} stocks to fetch "
          f"({len(already_done)} already in scratch)", flush=True)

    session = make_session()
    ctx = portal_get(session)
    if date not in ctx["dates"]:
        print(f"[tdcc-portal] {date}: NOT in portal dropdown -- skipping", flush=True)
        return {"scraped": 0, "skipped": 0, "failed": 0, "total_rows": 0}

    stats = {"scraped": 0, "skipped": len(already_done), "failed": 0, "total_rows": 0}
    refresh_counter = 0

    for i, stock_id in enumerate(pending):
        # Periodic session refresh
        if refresh_counter >= SESSION_REFRESH_EVERY:
            print(f"[tdcc-portal] {date}: refreshing session at stock {i}/{len(pending)}",
                  flush=True)
            try:
                session = make_session()
                ctx = portal_get(session)
            except Exception as exc:
                print(f"[tdcc-portal] {date}: session refresh failed: {exc!r}; continuing",
                      flush=True)
            refresh_counter = 0

        rows, ctx = fetch_stock_week(session, ctx, stock_id, date)
        refresh_counter += 1

        if rows:
            append_scratch(date, stock_id, rows)
            stats["scraped"] += 1
            stats["total_rows"] += len(rows)
        else:
            # Could be: stock not listed on this date, or fetch failure
            mark_failed_scratch(date, stock_id)
            stats["failed"] += 1

        # Progress log every 100 stocks
        if (i + 1) % 100 == 0:
            pct = (i + 1) / len(pending) * 100
            print(f"[tdcc-portal] {date}: {i+1}/{len(pending)} ({pct:.0f}%) "
                  f"ok={stats['scraped']} fail={stats['failed']}", flush=True)

        time.sleep(SLEEP_BETWEEN_STOCKS)

    # Assemble parquet
    print(f"[tdcc-portal] {date}: assembling parquet...", flush=True)
    try:
        df = scratch_to_parquet(date, out_path)
        scratch_path(date).unlink(missing_ok=True)
        print(f"[tdcc-portal] {date}: saved {out_path} ({len(df):,} rows, "
              f"{df['stock_id'].nunique()} stocks)", flush=True)
        stats["total_rows"] = len(df)
    except Exception as exc:
        print(f"[tdcc-portal] {date}: parquet assembly failed: {exc!r} "
              f"-- scratch preserved at {scratch_path(date)}", flush=True)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="TDCC portal historical scraper (51-week backfill)"
    )
    parser.add_argument(
        "--top-n", type=int, default=0,
        help="Limit to top N stocks by total shares (0 = all ~3,971 stocks)"
    )
    parser.add_argument(
        "--date", type=str, default="",
        help="Scrape only a specific date (YYYYMMDD).  Default: all missing dates."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-scrape even if parquet already exists"
    )
    parser.add_argument(
        "--retry-partial", action="store_true",
        help="Re-scrape stocks that failed in previous runs (sentinel rows)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print plan (dates + stock count) without scraping"
    )
    args = parser.parse_args(argv)

    # --- Get available dates from portal ---
    print("[tdcc-portal] Fetching date list from portal...", flush=True)
    session = make_session()
    try:
        ctx = portal_get(session)
    except Exception as exc:
        print(f"[tdcc-portal] ERROR: cannot reach portal: {exc!r}", flush=True)
        return 1

    all_dates: list[str] = ctx["dates"]
    print(f"[tdcc-portal] Portal has {len(all_dates)} dates: "
          f"{all_dates[-1]} .. {all_dates[0]}", flush=True)

    # --- Determine which dates to process ---
    if args.date:
        if args.date not in all_dates:
            print(f"[tdcc-portal] ERROR: {args.date} not in portal date list", flush=True)
            return 1
        target_dates = [args.date]
    else:
        existing = {p.stem for p in OUT_DIR.glob("????????.parquet")}
        if args.force:
            target_dates = all_dates
        else:
            target_dates = [d for d in all_dates if d not in existing]

    if not target_dates:
        print("[tdcc-portal] Nothing to do -- all dates already cached.", flush=True)
        return 0

    print(f"[tdcc-portal] Dates to scrape: {len(target_dates)} "
          f"({target_dates[-1]} .. {target_dates[0]})", flush=True)

    # --- Stock universe ---
    try:
        stock_ids = load_stock_ids_from_cache()
    except Exception as exc:
        print(f"[tdcc-portal] ERROR loading stock universe: {exc!r}", flush=True)
        return 1

    if args.top_n > 0:
        stock_ids = stock_ids[: args.top_n]
        print(f"[tdcc-portal] Limited to top {len(stock_ids)} stocks by total shares",
              flush=True)
    else:
        print(f"[tdcc-portal] Full universe: {len(stock_ids)} stocks", flush=True)

    # --- Estimate ---
    n_req = len(target_dates) * len(stock_ids)
    eta_h = n_req * SLEEP_BETWEEN_STOCKS / 3600
    print(f"[tdcc-portal] Estimated requests: {n_req:,}  "
          f"ETA: {eta_h:.1f} h at {SLEEP_BETWEEN_STOCKS}s/req", flush=True)

    if args.dry_run:
        print("[tdcc-portal] --dry-run mode: exiting without scraping.", flush=True)
        return 0

    # --- Scrape ---
    total_stats = {"scraped": 0, "skipped": 0, "failed": 0, "total_rows": 0}
    for date in sorted(target_dates):
        s = scrape_date(date, stock_ids, force=args.force,
                        retry_partial=args.retry_partial)
        for k in total_stats:
            total_stats[k] += s.get(k, 0)

    print("\n[tdcc-portal] === Backfill complete ===", flush=True)
    print(f"  Dates processed:     {len(target_dates)}", flush=True)
    print(f"  Stocks scraped:      {total_stats['scraped']:,}", flush=True)
    print(f"  Stocks skipped:      {total_stats['skipped']:,}", flush=True)
    print(f"  Stocks failed:       {total_stats['failed']:,}", flush=True)
    print(f"  Total rows written:  {total_stats['total_rows']:,}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
