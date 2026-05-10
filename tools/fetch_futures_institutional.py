"""
Backfill TAIFEX 三大法人期貨 (TXF) 未平倉淨額 from FinMind.

FinMind dataset: TaiwanFuturesInstitutionalInvestors, data_id=TXF
Output: data/sentiment/futures_institutional.parquet

Schema:
  data_date (str ISO)
  foreign_long_oi / foreign_short_oi / foreign_net_oi
  trust_long_oi   / trust_short_oi   / trust_net_oi
  dealer_long_oi  / dealer_short_oi  / dealer_net_oi
  inst_total_net_oi (= foreign + trust + dealer)

Usage:
    # 全歷史 bulk (2007-04-01 ~ today)
    python tools/fetch_futures_institutional.py

    # 指定範圍
    python tools/fetch_futures_institutional.py --start-date 2015-01-01 --end-date 2020-12-31

    # 強制重抓 (忽略 resume)
    python tools/fetch_futures_institutional.py --no-resume
"""
import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Dict

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

OUT_PATH = REPO / "data" / "sentiment" / "futures_institutional.parquet"
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"

# TX 期貨三大法人 FinMind 最早歷史實測 2018-06 (data_id=TX)
DEFAULT_START = date(2018, 6, 1)

CHUNK_YEARS = 4  # 分段大小（若 single bulk 失敗時）

_INST_MAP = {
    "外資": "foreign",
    "投信": "trust",
    "自營商": "dealer",
}

LOG_PATH = REPO / "fut_inst_backfill.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fut_inst")


# ============================================================
# Token
# ============================================================

def _read_finmind_token() -> str:
    tok = os.environ.get("FINMIND_TOKEN", "") or os.environ.get("FINMIND_API_TOKEN", "")
    if tok:
        return tok
    env_path = REPO / "local" / ".env"
    if not env_path.exists():
        return ""
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if "FINMIND" in line and "=" in line:
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


# ============================================================
# FinMind fetch (single chunk)
# ============================================================

def _fetch_chunk(token: str, start: date, end: date) -> List[Dict]:
    """Fetch one date range from FinMind TaiwanFuturesInstitutionalInvestors TXF.

    Returns list of row dicts matching OUT_PATH schema.
    """
    params = {
        "dataset": "TaiwanFuturesInstitutionalInvestors",
        "data_id": "TX",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "token": token,
    }
    log.info("FinMind fetch TXF fut-inst %s ~ %s", start, end)
    try:
        r = requests.get(FINMIND_API, params=params, timeout=120, verify=False)
        r.raise_for_status()
    except requests.RequestException as e:
        log.error("FinMind request failed: %s", e)
        return []

    js = r.json()
    raw = js.get("data", [])
    if not raw:
        log.warning("FinMind empty for TXF %s ~ %s; status=%s msg=%s",
                    start, end, js.get("status"), js.get("msg"))
        return []
    log.info("FinMind raw rows: %d", len(raw))

    # Pivot: date -> inst_key -> (long_oi, short_oi)
    by_date: Dict[str, Dict[str, tuple]] = {}
    for row in raw:
        d_iso = row.get("date")
        inst_key = _INST_MAP.get(row.get("institutional_investors", ""))
        if not d_iso or not inst_key:
            continue
        try:
            long_oi = int(row.get("long_open_interest_balance_volume", 0) or 0)
            short_oi = int(row.get("short_open_interest_balance_volume", 0) or 0)
        except (TypeError, ValueError):
            continue
        by_date.setdefault(d_iso, {})[inst_key] = (long_oi, short_oi)

    out = []
    for d_iso in sorted(by_date.keys()):
        parsed = by_date[d_iso]
        # Require all 3 institutions; warn and skip if incomplete
        if len(parsed) < 3:
            log.debug("[%s] incomplete (%d/3 institutions), skip", d_iso, len(parsed))
            continue
        f_long, f_short = parsed.get("foreign", (0, 0))
        t_long, t_short = parsed.get("trust", (0, 0))
        d_long, d_short = parsed.get("dealer", (0, 0))
        f_net = f_long - f_short
        t_net = t_long - t_short
        d_net = d_long - d_short
        out.append({
            "data_date": d_iso,
            "foreign_long_oi": f_long,
            "foreign_short_oi": f_short,
            "foreign_net_oi": f_net,
            "trust_long_oi": t_long,
            "trust_short_oi": t_short,
            "trust_net_oi": t_net,
            "dealer_long_oi": d_long,
            "dealer_short_oi": d_short,
            "dealer_net_oi": d_net,
            "inst_total_net_oi": f_net + t_net + d_net,
        })
    return out


# ============================================================
# Main fetch logic (single bulk, fallback to chunks)
# ============================================================

def fetch_futures_inst(start: date, end: date, token: str) -> pd.DataFrame:
    """Fetch full range; fall back to 4-yr chunks on empty/error."""
    rows = _fetch_chunk(token, start, end)
    if rows:
        log.info("Single bulk call succeeded: %d rows", len(rows))
        return pd.DataFrame(rows)

    # Fallback: chunked by CHUNK_YEARS years
    log.warning("Single bulk returned empty — switching to chunked fetch (%d-yr chunks)", CHUNK_YEARS)
    all_rows: List[Dict] = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = date(
            min(chunk_start.year + CHUNK_YEARS - 1, end.year),
            12,
            31,
        )
        if chunk_end > end:
            chunk_end = end
        rows = _fetch_chunk(token, chunk_start, chunk_end)
        all_rows.extend(rows)
        log.info("Chunk %s ~ %s: %d rows accumulated", chunk_start, chunk_end, len(all_rows))
        chunk_start = date(chunk_start.year + CHUNK_YEARS, 1, 1)
        if chunk_start <= end:
            time.sleep(5)

    if not all_rows:
        log.error("All chunks returned empty — check FinMind token quota")
        return pd.DataFrame()
    return pd.DataFrame(all_rows)


# ============================================================
# Save with dedup + sort
# ============================================================

def save_parquet(df: pd.DataFrame, existing_dates: set) -> pd.DataFrame:
    """Merge df into existing parquet (if any), dedup by data_date, sort."""
    if OUT_PATH.exists() and existing_dates:
        old_df = pd.read_parquet(OUT_PATH)
        combined = pd.concat([old_df, df], ignore_index=True)
    else:
        combined = df.copy()

    combined = combined.drop_duplicates(subset=["data_date"], keep="last")
    combined = combined.sort_values("data_date").reset_index(drop=True)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUT_PATH, index=False)
    log.info("Saved %s: %d rows", OUT_PATH, len(combined))
    return combined


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Fetch TAIFEX futures institutional investors (TXF) from FinMind")
    parser.add_argument("--start-date", default=DEFAULT_START.isoformat(),
                        help="Start date ISO (default: %(default)s, earliest FinMind TX data)")
    parser.add_argument("--end-date", default=date.today().isoformat(),
                        help="End date ISO (default: today)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore existing parquet; re-fetch entire range")
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)

    token = _read_finmind_token()
    if not token:
        log.error("FINMIND_TOKEN not found; set env var or add to local/.env")
        sys.exit(1)

    # Resume: skip dates already in parquet
    existing_dates: set = set()
    fetch_start = start
    if not args.no_resume and OUT_PATH.exists():
        try:
            existing_df = pd.read_parquet(OUT_PATH)
            existing_dates = set(existing_df["data_date"].tolist())
            if existing_dates:
                latest = max(existing_dates)
                log.info("Resume: %d existing dates, latest=%s", len(existing_dates), latest)
                # Fetch only from day after latest
                next_day = (date.fromisoformat(latest) + timedelta(days=1))
                if next_day > end:
                    log.info("Already up-to-date through %s, nothing to fetch", latest)
                    _print_summary(existing_df)
                    return
                fetch_start = next_day
        except Exception as e:
            log.warning("Could not read existing parquet (%s), will re-fetch", e)

    log.info("Fetching TXF futures inst: %s ~ %s", fetch_start, end)
    df_new = fetch_futures_inst(fetch_start, end, token)

    if df_new.empty:
        log.error("No data fetched; aborting without touching parquet")
        sys.exit(1)

    df_final = save_parquet(df_new, existing_dates)
    _print_summary(df_final)


def _print_summary(df: pd.DataFrame):
    print("\n=== futures_institutional.parquet summary ===")
    print(f"Total rows : {len(df)}")
    print(f"Date range : {df['data_date'].min()} ~ {df['data_date'].max()}")
    print("\nLast 5 rows:")
    print(df.tail(5).to_string(index=False))
    print("\nStat summary (net OI columns):")
    net_cols = ["foreign_net_oi", "trust_net_oi", "dealer_net_oi", "inst_total_net_oi"]
    print(df[net_cols].describe().to_string())


if __name__ == "__main__":
    main()
