"""
Backfill TAIFEX three sentiment signals' full history.

Targets (same schema as `tools/fetch_*` daily archivers, deduped by data_date):
  1. data/sentiment/atm_put_premium.parquet      → TAIFEX, 2009-12-15+
  2. data/sentiment/minifutures_ratio.parquet    → TAIFEX, 2009-12-15+
  3. data/sentiment/options_institutional.parquet → FinMind, 2018-12+

Data sources:
  - atm:     TAIFEX dlOptDataDown  (TXO 選擇權每日 OHLC + OI by strike + CP)
  - minifut: TAIFEX dlFutDataDown × 2  (TX / MTX 期貨每日 OHLC + OI by month)
  - inst:    FinMind TaiwanOptionInstitutionalInvestors  (三大法人 TXO 買賣權)
             ⚠️ TAIFEX callsAndPutsDate 端點只保留 ~2 年歷史 (2023-05+)，
             所以 inst 改用 FinMind 才能回到 2018-12。FinMind 與 TAIFEX 數值
             已交叉驗證 (2026-05-08 一致)。

Reference price for ATM (atm_put 用): bulk-fetch ^TWII history once via yfinance.

Resume: 自動檢查 target parquet 已存在的 data_date，跳過已 backfill 的日期。

Usage:
    # 全歷史 backfill (預設 2009-12-15 起；inst 從 FinMind 起點 2018-12)
    python tools/backfill_taifex_signals_history.py

    # 指定範圍
    python tools/backfill_taifex_signals_history.py \\
        --start-date 2020-01-01 --end-date 2025-12-31

    # 只跑某項
    python tools/backfill_taifex_signals_history.py --signals atm

    # 強制 redo (忽略 resume)
    python tools/backfill_taifex_signals_history.py --no-resume

預估時間: atm + minifut 全歷史 ~4000 weekdays × ~3 req/day × 1s = ~3.5 小時；
          inst 走 FinMind 單次 bulk call (~10 秒)。
"""
import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests
import urllib3
import yfinance as yf
from bs4 import BeautifulSoup

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

# ============================================================
# Config
# ============================================================
TAIFEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.taifex.com.tw/",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 1.0  # 跟 backfill_pcr_fgi_history.py 一致
MAX_RETRIES = 3
FLUSH_EVERY_DAYS = 20
TOP_OI_N = 5

ATM_PATH = REPO / "data" / "sentiment" / "atm_put_premium.parquet"
MINIFUT_PATH = REPO / "data" / "sentiment" / "minifutures_ratio.parquet"
INST_PATH = REPO / "data" / "sentiment" / "options_institutional.parquet"

# TXO 上市 2009-12-15
DEFAULT_START = date(2009, 12, 15)

# FinMind TaiwanOptionInstitutionalInvestors 的歷史起點 (實測 2018-12 開始)
INST_FINMIND_START = date(2018, 12, 1)

# FinMind API
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"

LOG_PATH = REPO / "taifex_backfill.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("taifex_backfill")


# ============================================================
# HTTP helpers (with retry)
# ============================================================
def _post_with_retry(session: requests.Session, url: str, payload: dict) -> Optional[requests.Response]:
    for attempt in range(MAX_RETRIES):
        try:
            r = session.post(url, data=payload, timeout=REQUEST_TIMEOUT, verify=False)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                log.debug("POST %s payload=%s failed after %d retries: %s",
                          url, payload, MAX_RETRIES, e)
                return None
            time.sleep(2 ** attempt)
    return None


def _get_with_retry(session: requests.Session, url: str, params: dict) -> Optional[requests.Response]:
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT, verify=False)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                log.debug("GET %s params=%s failed after %d retries: %s",
                          url, params, MAX_RETRIES, e)
                return None
            time.sleep(2 ** attempt)
    return None


# ============================================================
# Parsers (per-date, ported from taifex_data.py)
# ============================================================
def fetch_atm_put_for_date(
    session: requests.Session, target_date: date, reference_price: float
) -> Optional[Dict]:
    """Fetch ATM PUT premium for a specific date.

    Returns row dict matching atm_put_premium.parquet schema, or None.
    """
    if reference_price <= 0:
        return None

    date_str = target_date.strftime("%Y/%m/%d")
    url = "https://www.taifex.com.tw/cht/3/dlOptDataDown"
    payload = {
        "down_type": "1",
        "commodity_id": "TXO",
        "queryStartDate": date_str,
        "queryEndDate": date_str,
    }
    resp = _post_with_retry(session, url, payload)
    if resp is None:
        return None
    lines = resp.text.strip().split("\n")
    if len(lines) < 10:
        return None

    put_records = []  # (month_str, strike, close, oi)
    for line in lines[1:]:
        fields = line.split(",")
        if len(fields) < 18:
            continue
        if fields[4].strip() != "賣權":
            continue
        month_str = fields[2].strip()
        if "W" in month_str:
            continue
        sess = fields[17].strip()
        if sess and sess != "一般":
            continue
        try:
            strike = float(fields[3].strip())
            close_str = fields[8].strip()
            if close_str in ("-", ""):
                continue
            close_f = float(close_str)
            oi_str = fields[11].strip()
            oi_int = int(oi_str) if oi_str and oi_str != "-" else 0
            put_records.append((month_str, strike, close_f, oi_int))
        except (ValueError, IndexError):
            continue

    if not put_records:
        return None

    near_month = sorted(set(r[0] for r in put_records))[0]
    near_records = [r for r in put_records if r[0] == near_month]
    if not near_records:
        return None

    atm_record = min(near_records, key=lambda r: abs(r[1] - reference_price))
    otm5_target = reference_price * 0.95
    otm5_record = min(near_records, key=lambda r: abs(r[1] - otm5_target))

    atm_pct = (atm_record[2] / reference_price) * 100 if reference_price > 0 else 0.0
    put_skew = (otm5_record[2] / atm_record[2]) if atm_record[2] > 0 else 0.0

    near_with_oi = [(r[1], r[3]) for r in near_records if r[3] > 0]
    near_with_oi.sort(key=lambda x: x[1], reverse=True)
    top_strikes = [(int(s), int(oi)) for s, oi in near_with_oi[:TOP_OI_N]]

    row = {
        "data_date": target_date.isoformat(),
        "near_month": near_month,
        "reference": round(reference_price, 2),
        "atm_strike": int(atm_record[1]),
        "atm_put_close": round(atm_record[2], 2),
        "atm_put_pct": round(atm_pct, 3),
        "otm5_strike": int(otm5_record[1]),
        "otm5_put_close": round(otm5_record[2], 2),
        "put_skew": round(put_skew, 3),
    }
    for i in range(TOP_OI_N):
        if i < len(top_strikes):
            row[f"top_oi_strike_{i+1}"] = top_strikes[i][0]
            row[f"top_oi_oi_{i+1}"] = top_strikes[i][1]
        else:
            row[f"top_oi_strike_{i+1}"] = 0
            row[f"top_oi_oi_{i+1}"] = 0
    return row


def _fetch_futures_near_oi(
    session: requests.Session, commodity_id: str, target_date: date
) -> Tuple[Optional[str], Optional[int]]:
    """Fetch near-month OI for TX/MTX on date. Returns (near_month, oi)."""
    date_str = target_date.strftime("%Y/%m/%d")
    url = "https://www.taifex.com.tw/cht/3/dlFutDataDown"
    payload = {
        "down_type": "1",
        "commodity_id": commodity_id,
        "queryStartDate": date_str,
        "queryEndDate": date_str,
    }
    resp = _post_with_retry(session, url, payload)
    if resp is None:
        return None, None
    lines = resp.text.strip().split("\n")
    if len(lines) < 3:
        return None, None
    records = []
    for line in lines[1:]:
        fields = line.split(",")
        if len(fields) < 18:
            continue
        month_str = fields[2].strip()
        if "W" in month_str:
            continue
        sess = fields[17].strip()
        if sess and sess != "一般":
            continue
        try:
            oi_str = fields[11].strip()
            if not oi_str or oi_str == "-":
                continue
            records.append((month_str, int(oi_str)))
        except (ValueError, IndexError):
            continue
    if not records:
        return None, None
    near = sorted(set(r[0] for r in records))[0]
    near_oi = sum(oi for m, oi in records if m == near)
    return near, near_oi


def fetch_minifut_for_date(session: requests.Session, target_date: date) -> Optional[Dict]:
    """Fetch MTX/TX near-month OI ratio for a date."""
    tx_month, tx_oi = _fetch_futures_near_oi(session, "TX", target_date)
    time.sleep(SLEEP_BETWEEN_REQUESTS)  # 兩個 fut 端點間也 sleep
    mtx_month, mtx_oi = _fetch_futures_near_oi(session, "MTX", target_date)

    if not tx_oi or not mtx_oi:
        return None
    if tx_month != mtx_month:
        log.debug("[%s] MTX/TX near month mismatch TX=%s MTX=%s; using TX",
                  target_date, tx_month, mtx_month)

    ratio = mtx_oi / tx_oi if tx_oi > 0 else 0.0
    return {
        "data_date": target_date.isoformat(),
        "near_month": tx_month,
        "txf_oi": int(tx_oi),
        "mtx_oi": int(mtx_oi),
        "mtx_txf_ratio": round(ratio, 4),
    }


# ============================================================
# FinMind: 三大法人 TXO 買賣權 (bulk fetch)
# ============================================================
_FINMIND_INST_MAP = {
    "外資": "foreign",
    "投信": "trust",
    "自營商": "dealer",
}


def _read_finmind_token() -> str:
    import os
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


def fetch_options_inst_bulk_finmind(start_date: date, end_date: date) -> list:
    """Fetch 三大法人 TXO 買賣權未平倉淨額 from FinMind (single bulk call).

    FinMind dataset: TaiwanOptionInstitutionalInvestors, data_id=TXO.
    Returns list of row dicts matching options_institutional.parquet schema.

    Schema mapping:
      net_oi = long_open_interest_balance_volume - short_open_interest_balance_volume
      foreign_call_net = net_oi where institutional_investors=外資 & call_put=買權
      ... (3 institutions × 2 CP = 6 net values per date)
    """
    token = _read_finmind_token()
    if not token:
        log.error("FINMIND token not found in env or local/.env; cannot backfill inst")
        return []

    params = {
        "dataset": "TaiwanOptionInstitutionalInvestors",
        "data_id": "TXO",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "token": token,
    }
    log.info("FinMind fetch: TaiwanOptionInstitutionalInvestors %s ~ %s",
             start_date, end_date)
    try:
        r = requests.get(FINMIND_API, params=params, timeout=120, verify=False)
        r.raise_for_status()
    except requests.RequestException as e:
        log.error("FinMind fetch failed: %s", e)
        return []
    js = r.json()
    raw = js.get("data", [])
    if not raw:
        log.warning("FinMind returned empty for inst (%s ~ %s); status=%s msg=%s",
                    start_date, end_date, js.get("status"), js.get("msg"))
        return []
    log.info("FinMind raw rows: %d", len(raw))

    # Pivot: date -> (inst, cp) -> net
    by_date: Dict[str, Dict[Tuple[str, str], int]] = {}
    for row in raw:
        d_iso = row.get("date")
        inst_key = _FINMIND_INST_MAP.get(row.get("institutional_investors", ""))
        cp_str = row.get("call_put", "")
        cp_key = "call" if cp_str == "買權" else ("put" if cp_str == "賣權" else None)
        if not d_iso or not inst_key or not cp_key:
            continue
        try:
            long_oi = int(row.get("long_open_interest_balance_volume", 0))
            short_oi = int(row.get("short_open_interest_balance_volume", 0))
        except (TypeError, ValueError):
            continue
        net = long_oi - short_oi
        by_date.setdefault(d_iso, {})[(inst_key, cp_key)] = net

    out = []
    for d_iso in sorted(by_date.keys()):
        parsed = by_date[d_iso]
        if len(parsed) < 6:
            log.debug("[%s] FinMind incomplete (%d/6 inst-cp combos), skip", d_iso, len(parsed))
            continue
        fc = parsed.get(("foreign", "call"), 0)
        fp = parsed.get(("foreign", "put"), 0)
        tc = parsed.get(("trust", "call"), 0)
        tp = parsed.get(("trust", "put"), 0)
        dc = parsed.get(("dealer", "call"), 0)
        dp = parsed.get(("dealer", "put"), 0)
        call_total = fc + tc + dc
        put_total = fp + tp + dp
        out.append({
            "data_date": d_iso,
            "foreign_call_net": fc,
            "foreign_put_net": fp,
            "trust_call_net": tc,
            "trust_put_net": tp,
            "dealer_call_net": dc,
            "dealer_put_net": dp,
            "inst_call_net_total": call_total,
            "inst_put_net_total": put_total,
            "inst_pc_oi_skew": put_total - call_total,
        })
    return out


# ============================================================
# Reference price (TWII) - bulk fetch once
# ============================================================
def load_twii_history(start_date: date, end_date: date) -> Dict[str, float]:
    """Bulk-fetch ^TWII close history via yfinance. Returns {iso_date: close}."""
    try:
        twii = yf.Ticker("^TWII")
        hist = twii.history(
            start=start_date.isoformat(),
            end=(end_date + timedelta(days=1)).isoformat(),
            auto_adjust=False,
        )
        if hist.empty:
            log.warning("^TWII history fetch returned empty")
            return {}
        return {idx.date().isoformat(): float(close) for idx, close in zip(hist.index, hist["Close"])}
    except Exception as e:
        log.error("^TWII history fetch failed: %s", e, exc_info=True)
        return {}


# ============================================================
# Resume / incremental flush helpers
# ============================================================
def get_existing_dates(parquet_path: Path) -> set:
    if not parquet_path.exists():
        return set()
    try:
        df = pd.read_parquet(parquet_path)
        if df.empty or "data_date" not in df.columns:
            return set()
        return set(df["data_date"].astype(str))
    except Exception as e:
        log.warning("Read %s failed (%s); treating as empty", parquet_path.name, e)
        return set()


def append_rows(parquet_path: Path, new_rows: list) -> None:
    if not new_rows:
        return
    new_df = pd.DataFrame(new_rows)
    if parquet_path.exists():
        try:
            existing = pd.read_parquet(parquet_path)
            new_dates = set(new_df["data_date"])
            existing = existing[~existing["data_date"].isin(new_dates)]
            merged = pd.concat([existing, new_df], ignore_index=True)
        except Exception as e:
            log.warning("Read %s failed (%s); overwriting", parquet_path.name, e)
            merged = new_df
    else:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        merged = new_df
    merged = merged.sort_values("data_date").reset_index(drop=True)
    merged.to_parquet(parquet_path, index=False)


# ============================================================
# Main
# ============================================================
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", type=str, default=None,
                        help="ISO date, default 2009-12-15 (TXO 上市)")
    parser.add_argument("--end-date", type=str, default=None, help="ISO date, default today")
    parser.add_argument("--signals", type=str, default="atm,minifut,inst",
                        help="comma-separated subset of {atm,minifut,inst}")
    parser.add_argument("--no-resume", action="store_true",
                        help="ignore existing parquet, refetch all dates in range")
    parser.add_argument("--sleep-secs", type=float, default=SLEEP_BETWEEN_REQUESTS)
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start_date) if args.start_date else DEFAULT_START
    end_date = date.fromisoformat(args.end_date) if args.end_date else date.today()
    signals = set(s.strip() for s in args.signals.split(",") if s.strip())
    valid_signals = {"atm", "minifut", "inst"}
    if not signals.issubset(valid_signals):
        log.error("Invalid signals: %s (allowed: %s)", signals - valid_signals, valid_signals)
        return 2

    sleep_secs = max(0.1, args.sleep_secs)

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.headers.update(TAIFEX_HEADERS)

    # Resume: build skip-set per signal
    if args.no_resume:
        skip_atm, skip_minifut, skip_inst = set(), set(), set()
    else:
        skip_atm = get_existing_dates(ATM_PATH) if "atm" in signals else set()
        skip_minifut = get_existing_dates(MINIFUT_PATH) if "minifut" in signals else set()
        skip_inst = get_existing_dates(INST_PATH) if "inst" in signals else set()

    log.info("Backfill window: %s -> %s | signals=%s | sleep=%.1fs",
             start_date, end_date, sorted(signals), sleep_secs)
    log.info("Existing dates to skip: atm=%d / minifut=%d / inst=%d",
             len(skip_atm), len(skip_minifut), len(skip_inst))

    # ----- inst via FinMind (bulk, single call) -----
    if "inst" in signals:
        inst_start = max(start_date, INST_FINMIND_START)
        if inst_start > end_date:
            log.info("inst: window before FinMind start (%s); skip", INST_FINMIND_START)
        else:
            inst_rows = fetch_options_inst_bulk_finmind(inst_start, end_date)
            inst_rows_new = [r for r in inst_rows if r["data_date"] not in skip_inst]
            log.info("inst: FinMind returned %d rows total, %d new (after skip)",
                     len(inst_rows), len(inst_rows_new))
            if inst_rows_new:
                append_rows(INST_PATH, inst_rows_new)
                log.info("inst: appended %d rows -> %s", len(inst_rows_new), INST_PATH)

    # ----- atm + minifut via TAIFEX, weekday loop -----
    if not (signals & {"atm", "minifut"}):
        log.info("No TAIFEX signals selected (only inst); done.")
        return 0

    # Bulk-fetch TWII history (only needed for atm)
    twii_close = {}
    if "atm" in signals:
        log.info("Loading ^TWII history for ATM reference price...")
        twii_close = load_twii_history(start_date, end_date)
        log.info("^TWII history: %d trading days", len(twii_close))
        if not twii_close:
            log.error("^TWII history empty -- atm backfill will skip all")

    atm_buf, minifut_buf = [], []
    cur = start_date
    weekday_count = 0
    processed_atm = processed_minifut = 0
    added_atm = added_minifut = 0

    try:
        while cur <= end_date:
            if cur.weekday() >= 5:  # 跳過週六(5)/日(6)
                cur += timedelta(days=1)
                continue

            weekday_count += 1
            date_iso = cur.isoformat()

            try:
                if "atm" in signals and date_iso not in skip_atm:
                    ref = twii_close.get(date_iso, 0.0)
                    if ref > 0:
                        row = fetch_atm_put_for_date(session, cur, ref)
                        processed_atm += 1
                        if row:
                            atm_buf.append(row)
                            added_atm += 1
                    time.sleep(sleep_secs)

                if "minifut" in signals and date_iso not in skip_minifut:
                    row = fetch_minifut_for_date(session, cur)
                    processed_minifut += 1
                    if row:
                        minifut_buf.append(row)
                        added_minifut += 1
                    time.sleep(sleep_secs)

            except Exception as e:
                log.warning("[%s] error: %s", cur, e, exc_info=True)

            # Periodic flush + progress log
            if weekday_count % FLUSH_EVERY_DAYS == 0:
                if atm_buf:
                    append_rows(ATM_PATH, atm_buf)
                    atm_buf = []
                if minifut_buf:
                    append_rows(MINIFUT_PATH, minifut_buf)
                    minifut_buf = []
                log.info("[PROGRESS] %s | weekday=%d | added atm=%d minifut=%d "
                         "(processed atm=%d minifut=%d)",
                         cur, weekday_count, added_atm, added_minifut,
                         processed_atm, processed_minifut)

            cur += timedelta(days=1)

    except KeyboardInterrupt:
        log.warning("Interrupted by user; flushing buffered rows before exit")

    # Final flush
    if atm_buf:
        append_rows(ATM_PATH, atm_buf)
    if minifut_buf:
        append_rows(MINIFUT_PATH, minifut_buf)

    log.info("Backfill complete: %d weekdays scanned | added atm=%d minifut=%d",
             weekday_count, added_atm, added_minifut)

    # Final status
    for label, path in [("atm", ATM_PATH), ("minifut", MINIFUT_PATH), ("inst", INST_PATH)]:
        if label not in signals:
            continue
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty and "data_date" in df.columns:
                log.info("  %s: %d rows | %s -> %s", label, len(df),
                         df["data_date"].min(), df["data_date"].max())
    return 0


if __name__ == "__main__":
    sys.exit(main())
