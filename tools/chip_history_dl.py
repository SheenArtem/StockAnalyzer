"""
C2-a: Bulk download TW chip (institutional/margin/day_trading/shareholding/short_sale)
history for IC backtest.

Date range: 2021-04-16 ~ 2026-04-15 (5 years)
Universe:   data_cache/backtest/universe_tw.parquet (same as OHLCV backtest)
Output:     data_cache/chip_history/*.parquet (long format)

Endpoint strategy per dataset:
  institutional  -- TWSE T86 ALL (batch/date) + TPEX batch  [~1250 trading days x 1 API call]
  margin         -- TWSE MI_MARGN ALL (batch/date) [TWSE only]; TPEX stocks -> FinMind fallback
  day_trading    -- FinMind per-stock (no TWSE/TPEX all-market batch exists)
  shareholding   -- FinMind per-stock (no TWSE/TPEX batch exists)
  short_sale     -- TWSE TWT93U (batch/date) [TWSE only]; TPEX stocks -> FinMind fallback

Usage:
    python tools/chip_history_dl.py                             # all datasets, full 5yr
    python tools/chip_history_dl.py --dataset institutional     # single dataset
    python tools/chip_history_dl.py --test                      # 10 stocks x 1 month
    python tools/chip_history_dl.py --resume                    # resume from last saved date
    python tools/chip_history_dl.py --start-date 2023-01-01 --end-date 2023-12-31
"""
import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# ============================================================
# Config
# ============================================================
OUTPUT_DIR = _ROOT / "data_cache" / "chip_history"
UNIVERSE_PATH = _ROOT / "data_cache" / "backtest" / "universe_tw.parquet"

DEFAULT_START = "2021-04-16"
# 2026-05-02: changed from hardcoded "2026-04-15" to dynamic (today) so
# scanner cron --resume picks up the latest trading day automatically.
# Backtest reproducibility: pass explicit --end-date when needed.
DEFAULT_END   = datetime.now().strftime("%Y-%m-%d")

# TWSE throttle: already enforced inside TWSEOpenData._throttle(),
# but we add extra sleep between date-loop iterations to be safe.
TWSE_EXTRA_SLEEP = 0.5   # seconds after each _fetch_json call in our own loops
FINMIND_SLEEP    = 1.2   # seconds between FinMind per-stock calls (600 req/hr = 0.6s/req, we use 2x margin)

FLUSH_EVERY_N_DAYS = 20  # flush parquet every ~1 month of trading days

ALL_DATASETS = ["institutional", "margin", "day_trading", "shareholding", "short_sale"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("chip_dl")


# ============================================================
# Trading calendar helpers
# ============================================================
def _generate_trading_dates(start: str, end: str) -> list:
    """
    Return list of weekday dates between start and end (inclusive), oldest first.
    Does not filter actual holidays -- TWSE will simply return no data for those days.
    """
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    dates = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:  # Mon-Fri
            dates.append(cur)
        cur += timedelta(days=1)
    return dates


# ============================================================
# Parquet I/O helpers
# ============================================================
def _parquet_path(dataset: str) -> Path:
    return OUTPUT_DIR / f"{dataset}.parquet"


def _flush(dataset: str, rows: list, key_cols: list) -> None:
    """Append rows to existing parquet, dedup on key_cols, sort, save."""
    if not rows:
        return
    new_df = pd.DataFrame(rows)
    path = _parquet_path(dataset)
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=key_cols, keep="last")
    else:
        combined = new_df
    combined = combined.sort_values(key_cols).reset_index(drop=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path, engine="pyarrow", compression="snappy")
    size_mb = path.stat().st_size / 1e6
    logger.info(
        "[%s] Flushed %d new rows -> parquet total %d rows (%.1f MB)",
        dataset, len(new_df), len(combined), size_mb,
    )


def _get_last_saved_date(dataset: str) -> str | None:
    """Return the last date string in the saved parquet, or None if not exists."""
    path = _parquet_path(dataset)
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["date"])
        if df.empty:
            return None
        last = pd.to_datetime(df["date"]).max()
        return last.strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning("[%s] Could not read last date: %s", dataset, e)
        return None


# ============================================================
# TWSE batch helpers (institutional / margin / short_sale)
# ============================================================
def _twse_date(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _tpex_date(dt: datetime) -> str:
    return f"{dt.year - 1911}/{dt.strftime('%m/%d')}"


def _make_session():
    """Reuse a single requests.Session with proper headers."""
    import requests, urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s = requests.Session()
    s.verify = False
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, */*",
    })
    return s


_session = None

def _get_session():
    global _session
    if _session is None:
        _session = _make_session()
    return _session


def _fetch_json(url: str, params: dict = None, timeout: int = 15) -> dict | None:
    """Simple fetch with 3 retries; no throttle (caller is responsible)."""
    s = _get_session()
    for attempt in range(3):
        try:
            r = s.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                logger.warning("fetch failed %s params=%s: %s", url, params, e)
    return None


# ---- institutional (T86 + TPEX) ----

def _fetch_institutional_twse_one_day(dt: datetime) -> list:
    """
    Fetch TWSE T86 ALL for one date.
    Returns list of dicts: {date, stock_id, foreign_net, trust_net, dealer_net, total_net, market}.
    """
    date_str = _twse_date(dt)
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    params = {"date": date_str, "selectType": "ALL", "response": "json"}
    time.sleep(_TWSE_REQUEST_INTERVAL)
    data = _fetch_json(url, params)
    if not data or data.get("stat") != "OK":
        return []

    fields = data.get("fields", [])
    rows = data.get("data", [])
    if not fields or not rows:
        return []

    field_map = {f.strip(): i for i, f in enumerate(fields)}
    records = []
    for row in rows:
        if len(row) < 2:
            continue
        sid = str(row[0]).strip()
        if not sid.isdigit() or len(sid) != 4:
            continue
        try:
            foreign_net = 0
            for key in ["外陸資買賣超股數(不含外資自營商)", "外資及陸資買賣超股數", "外資買賣超股數"]:
                if key in field_map:
                    foreign_net = _safe_int(row[field_map[key]])
                    break
            for key in ["外資自營商買賣超股數"]:
                if key in field_map:
                    foreign_net += _safe_int(row[field_map[key]])
                    break
            trust_net = 0
            for key in ["投信買賣超股數"]:
                if key in field_map:
                    trust_net = _safe_int(row[field_map[key]])
                    break
            dealer_net = 0
            for key in ["自營商買賣超股數"]:
                if key in field_map:
                    dealer_net = _safe_int(row[field_map[key]])
                    break
            if dealer_net == 0:
                d1 = d2 = 0
                for key in ["自營商(自行)買賣超股數"]:
                    if key in field_map:
                        d1 = _safe_int(row[field_map[key]])
                        break
                for key in ["自營商(避險)買賣超股數"]:
                    if key in field_map:
                        d2 = _safe_int(row[field_map[key]])
                        break
                dealer_net = d1 + d2
            total_net = 0
            for key in ["三大法人買賣超股數"]:
                if key in field_map:
                    total_net = _safe_int(row[field_map[key]])
                    break
            if total_net == 0:
                total_net = foreign_net + trust_net + dealer_net
            records.append({
                "date": dt.strftime("%Y-%m-%d"),
                "stock_id": sid,
                "foreign_net": foreign_net,
                "trust_net": trust_net,
                "dealer_net": dealer_net,
                "total_net": total_net,
                "market": "twse",
            })
        except Exception:
            continue
    return records


def _fetch_institutional_tpex_one_day(dt: datetime) -> list:
    """
    Fetch TPEX all-market institutional for one date.
    Returns list of dicts with same schema as TWSE.
    """
    roc_date = _tpex_date(dt)
    url = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
    params = {"l": "zh-tw", "o": "json", "se": "EW", "t": "D", "d": roc_date, "s": "0,asc,0"}
    time.sleep(_TWSE_REQUEST_INTERVAL)
    data = _fetch_json(url, params)
    if not data:
        return []

    rows = data.get("aaData", [])
    if not rows:
        tables = data.get("tables", [])
        if tables and isinstance(tables[0], dict):
            rows = tables[0].get("data", [])
    if not rows:
        return []

    is_new_fmt = len(rows[0]) >= 24 if rows else False
    records = []
    for row in rows:
        if len(row) < 15:
            continue
        sid = str(row[0]).strip()
        if not sid.isdigit() or len(sid) != 4:
            continue
        try:
            if is_new_fmt:
                foreign_net = _safe_int(row[10])
                trust_net   = _safe_int(row[13])
                dealer_net  = _safe_int(row[22])
                total_net   = _safe_int(row[23])
            else:
                foreign_net  = _safe_int(row[4])
                trust_net    = _safe_int(row[7])
                dealer_self  = _safe_int(row[10])
                dealer_hedge = _safe_int(row[13])
                dealer_net   = dealer_self + dealer_hedge
                total_net    = _safe_int(row[14]) if len(row) > 14 else (foreign_net + trust_net + dealer_net)
            records.append({
                "date": dt.strftime("%Y-%m-%d"),
                "stock_id": sid,
                "foreign_net": foreign_net,
                "trust_net": trust_net,
                "dealer_net": dealer_net,
                "total_net": total_net,
                "market": "tpex",
            })
        except Exception:
            continue
    return records


# ---- margin (MI_MARGN TWSE ALL) ----
# Fields: 代號 名稱 | [融資]買進 賣出 現金償還 前日餘額 今日餘額 次一營業日限額
#                  | [融券]買進 賣出 現券償還 前日餘額 今日餘額 次一營業日限額 | 資券互抵 註記
_MARGIN_FIELD_IDX = {
    "margin_buy": 2, "margin_sell": 3, "margin_balance": 6,
    "short_buy":  8, "short_sell":  9, "short_balance": 12,
}

def _fetch_margin_twse_one_day(dt: datetime) -> list:
    """Fetch TWSE MI_MARGN ALL for one date."""
    date_str = _twse_date(dt)
    url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
    params = {"date": date_str, "selectType": "ALL", "response": "json"}
    time.sleep(_TWSE_REQUEST_INTERVAL)
    data = _fetch_json(url, params)
    if not data or data.get("stat") != "OK":
        return []

    tables = data.get("tables", [])
    # table[0] = market summary (統計, 3 rows), table[1] = per-stock (彙總, 1000+ rows)
    # Select the table with the most data rows -- that is always the per-stock table.
    target = None
    if tables:
        target = max(tables, key=lambda t: len(t.get("data", [])))
    if target is None or not target.get("data"):
        return []

    fields = target.get("fields", [])
    rows   = target.get("data", [])
    if not rows:
        return []

    # Build field_map for robust column lookup
    field_map = {f.strip(): i for i, f in enumerate(fields)}

    records = []
    for row in rows:
        if len(row) < 13:
            continue
        sid = str(row[0]).strip()
        if not sid.isdigit() or len(sid) != 4:
            continue
        try:
            # Margin (融資): cols 2-7; Short (融券): cols 8-13
            # Use field_map if available for robustness, else positional fallback
            def _get(keys, fallback_idx):
                for k in keys:
                    if k in field_map:
                        return _safe_int(row[field_map[k]])
                return _safe_int(row[fallback_idx]) if len(row) > fallback_idx else 0

            records.append({
                "date": dt.strftime("%Y-%m-%d"),
                "stock_id": sid,
                "margin_buy":     _get(["融資買進", "資買進"], 2),
                "margin_sell":    _get(["融資賣出", "資賣出"], 3),
                "margin_balance": _get(["融資今日餘額", "融資餘額", "資餘額"], 6),
                "short_buy":      _get(["融券買進", "券買進"], 8),
                "short_sell":     _get(["融券賣出", "券賣出"], 9),
                "short_balance":  _get(["融券今日餘額", "融券餘額", "券餘額"], 12),
                "market": "twse",
            })
        except Exception:
            continue
    return records


# ---- short_sale (TWT93U TWSE ALL) ----
# Fields: 代號 名稱 | [融券]前日餘額 賣出 買進 現券 今日餘額 次一營業日限額
#                  | [借券賣出]前日餘額 當日賣出 當日還券 當日調整 當日餘額 次一營業日可限額 備註

def _fetch_shortsale_twse_one_day(dt: datetime) -> list:
    """Fetch TWSE TWT93U (all-market short sale + SBL) for one date."""
    date_str = _twse_date(dt)
    url = "https://www.twse.com.tw/exchangeReport/TWT93U"
    params = {"date": date_str, "response": "json", "type": "MS"}
    time.sleep(_TWSE_REQUEST_INTERVAL)
    data = _fetch_json(url, params)
    if not data or data.get("stat") != "OK":
        return []

    fields = data.get("fields", [])
    rows   = data.get("data", [])
    if not rows:
        return []

    field_map = {f.strip(): i for i, f in enumerate(fields)}

    records = []
    for row in rows:
        if len(row) < 13:
            continue
        sid = str(row[0]).strip()
        if not sid.isdigit() or len(sid) != 4:
            continue
        try:
            def _get(keys, fallback_idx):
                for k in keys:
                    if k in field_map:
                        return _safe_int(row[field_map[k]])
                return _safe_int(row[fallback_idx]) if len(row) > fallback_idx else 0

            records.append({
                "date": dt.strftime("%Y-%m-%d"),
                "stock_id": sid,
                # SBL borrowing (借券賣出)
                "sbl_balance":   _get(["當日餘額"], 12),
                "sbl_sell":      _get(["當日賣出"], 9),
                "sbl_return":    _get(["當日還券"], 10),
                "sbl_adjust":    _get(["當日調整"], 11),
                # Margin short sale (融券賣出) - also in this table
                "ms_sell":       _get(["賣出"], 3),
                "ms_buy":        _get(["買進"], 4),
                "ms_balance":    _get(["今日餘額"], 6),
                "market": "twse",
            })
        except Exception:
            continue
    return records


# ============================================================
# FinMind per-stock helpers
# ============================================================
_finmind_call_count = 0
_finmind_hour_start = time.time()
_FINMIND_MAX_PER_HOUR = 580  # leave 20 buffer

def _finmind_rate_check():
    """Auto-pause if approaching 580 req/hr."""
    global _finmind_call_count, _finmind_hour_start
    elapsed = time.time() - _finmind_hour_start
    if elapsed >= 3600:
        _finmind_call_count = 0
        _finmind_hour_start = time.time()
    if _finmind_call_count >= _FINMIND_MAX_PER_HOUR:
        wait = 3600 - elapsed + 10
        logger.warning("FinMind rate limit reached (%d req). Pausing %.0f sec...",
                       _finmind_call_count, wait)
        time.sleep(wait)
        _finmind_call_count = 0
        _finmind_hour_start = time.time()


def _finmind_call(dl, method_name: str, **kwargs) -> pd.DataFrame:
    """Wrapper for FinMind API call with rate tracking and sleep."""
    global _finmind_call_count
    _finmind_rate_check()
    try:
        method = getattr(dl, method_name)
        df = method(**kwargs)
        _finmind_call_count += 1
        time.sleep(FINMIND_SLEEP)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        _finmind_call_count += 1
        logger.warning("FinMind %s(%s) failed: %s", method_name, kwargs.get("stock_id"), e)
        time.sleep(FINMIND_SLEEP)
        return pd.DataFrame()


def _safe_int(val) -> int:
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    cleaned = str(val).replace(",", "").replace(" ", "").strip()
    if cleaned in ("", "--", "N/A", "N\\A"):
        return 0
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0


# ============================================================
# Per-dataset download orchestrators
# ============================================================

_TWSE_REQUEST_INTERVAL = 3.0  # seconds (match twse_api.py)


def download_institutional(dates: list, universe: pd.DataFrame, buffer_cap: int = FLUSH_EVERY_N_DAYS) -> dict:
    """
    Download institutional (三大法人) via TWSE T86 ALL + TPEX batch per date.
    Returns stats dict.
    """
    dataset = "institutional"
    key_cols = ["date", "stock_id"]
    buffer = []
    ok_days = 0
    fail_days = 0
    fallback_count = 0  # not applicable for institutional (both are batch)
    t0 = time.time()

    for i, dt in enumerate(dates):
        day_records = []

        # TWSE batch
        twse_recs = _fetch_institutional_twse_one_day(dt)
        day_records.extend(twse_recs)

        # TPEX batch
        tpex_recs = _fetch_institutional_tpex_one_day(dt)
        day_records.extend(tpex_recs)

        if day_records:
            buffer.extend(day_records)
            ok_days += 1
        else:
            logger.warning("[%s] No data for %s", dataset, dt.strftime("%Y-%m-%d"))
            fail_days += 1

        if (i + 1) % buffer_cap == 0 and buffer:
            _flush(dataset, buffer, key_cols)
            buffer = []

    if buffer:
        _flush(dataset, buffer, key_cols)

    elapsed = time.time() - t0
    return {"ok_days": ok_days, "fail_days": fail_days, "fallback": fallback_count,
            "elapsed_min": elapsed / 60}


def download_margin(dates: list, universe: pd.DataFrame, tpex_ids: set, dl,
                    buffer_cap: int = FLUSH_EVERY_N_DAYS) -> dict:
    """
    Download margin trading (融資融券).
    TWSE stocks: MI_MARGN ALL batch per date.
    TPEX stocks: FinMind per-stock.
    """
    dataset = "margin"
    key_cols = ["date", "stock_id"]
    buffer = []
    ok_days = 0
    fail_days = 0
    fallback_count = 0
    t0 = time.time()

    # Phase 1: TWSE batch by date
    logger.info("[margin] Phase 1/2: TWSE batch (%d dates)", len(dates))
    for i, dt in enumerate(dates):
        recs = _fetch_margin_twse_one_day(dt)
        if recs:
            buffer.extend(recs)
            ok_days += 1
        else:
            logger.warning("[margin] No TWSE data for %s", dt.strftime("%Y-%m-%d"))
            fail_days += 1
        if (i + 1) % buffer_cap == 0 and buffer:
            _flush(dataset, buffer, key_cols)
            buffer = []

    if buffer:
        _flush(dataset, buffer, key_cols)
        buffer = []

    # Phase 2: TPEX stocks via FinMind
    logger.info("[margin] Phase 2/2: TPEX stocks via FinMind (%d stocks)", len(tpex_ids))
    start_str = dates[0].strftime("%Y-%m-%d")
    end_str   = dates[-1].strftime("%Y-%m-%d")

    for j, sid in enumerate(sorted(tpex_ids)):
        raw = _finmind_call(dl, "taiwan_stock_margin_purchase_short_sale",
                            stock_id=sid, start_date=start_str, end_date=end_str)
        if raw.empty:
            logger.debug("[margin] TPEX FinMind empty for %s", sid)
            fallback_count += 1
            continue
        if "date" not in raw.columns:
            continue
        keep = {
            "MarginPurchaseBuy": "margin_buy",
            "MarginPurchaseSell": "margin_sell",
            "MarginPurchaseTodayBalance": "margin_balance",
            "ShortSaleBuy": "short_buy",
            "ShortSaleSell": "short_sell",
            "ShortSaleTodayBalance": "short_balance",
        }
        recs = []
        for _, row in raw.iterrows():
            rec = {"date": str(row["date"])[:10], "stock_id": sid, "market": "tpex"}
            for src, dst in keep.items():
                rec[dst] = int(row[src]) if src in raw.columns and pd.notna(row[src]) else 0
            recs.append(rec)
        buffer.extend(recs)
        fallback_count += 1

        if (j + 1) % buffer_cap == 0 and buffer:
            _flush(dataset, buffer, key_cols)
            buffer = []

    if buffer:
        _flush(dataset, buffer, key_cols)

    elapsed = time.time() - t0
    return {"ok_days": ok_days, "fail_days": fail_days, "fallback": fallback_count,
            "elapsed_min": elapsed / 60}


def download_day_trading(dates: list, all_ids: list, dl,
                         buffer_cap: int = FLUSH_EVERY_N_DAYS) -> dict:
    """
    Download day trading (當沖) via FinMind per-stock.
    No TWSE/TPEX all-market batch exists for this dataset.
    """
    dataset = "day_trading"
    key_cols = ["date", "stock_id"]
    buffer = []
    ok_count = 0
    fail_count = 0
    start_str = dates[0].strftime("%Y-%m-%d")
    end_str   = dates[-1].strftime("%Y-%m-%d")
    t0 = time.time()

    for j, sid in enumerate(all_ids):
        raw = _finmind_call(dl, "taiwan_stock_day_trading",
                            stock_id=sid, start_date=start_str, end_date=end_str)
        if raw.empty:
            fail_count += 1
            continue
        if "date" not in raw.columns:
            fail_count += 1
            continue
        recs = []
        for _, row in raw.iterrows():
            recs.append({
                "date": str(row["date"])[:10],
                "stock_id": sid,
                "dt_volume":  int(row["Volume"])     if "Volume"     in raw.columns and pd.notna(row["Volume"])     else 0,
                "dt_buy":     int(row["BuyAmount"])  if "BuyAmount"  in raw.columns and pd.notna(row["BuyAmount"])  else 0,
                "dt_sell":    int(row["SellAmount"]) if "SellAmount" in raw.columns and pd.notna(row["SellAmount"]) else 0,
            })
        buffer.extend(recs)
        ok_count += 1

        if (j + 1) % (buffer_cap * 5) == 0 and buffer:  # flush every ~100 stocks
            _flush(dataset, buffer, key_cols)
            buffer = []
            logger.info("[day_trading] Progress: %d/%d stocks", j + 1, len(all_ids))

    if buffer:
        _flush(dataset, buffer, key_cols)

    elapsed = time.time() - t0
    return {"ok_stocks": ok_count, "fail_stocks": fail_count, "elapsed_min": elapsed / 60}


def download_shareholding(dates: list, all_ids: list, dl,
                          buffer_cap: int = FLUSH_EVERY_N_DAYS) -> dict:
    """
    Download foreign shareholding ratio (外資持股%) via FinMind per-stock.
    """
    dataset = "shareholding"
    key_cols = ["date", "stock_id"]
    buffer = []
    ok_count = 0
    fail_count = 0
    start_str = dates[0].strftime("%Y-%m-%d")
    end_str   = dates[-1].strftime("%Y-%m-%d")
    t0 = time.time()

    for j, sid in enumerate(all_ids):
        raw = _finmind_call(dl, "taiwan_stock_shareholding",
                            stock_id=sid, start_date=start_str, end_date=end_str)
        if raw.empty:
            fail_count += 1
            continue
        if "date" not in raw.columns:
            fail_count += 1
            continue
        recs = []
        for _, row in raw.iterrows():
            ratio = float(row["ForeignInvestmentSharesRatio"]) if "ForeignInvestmentSharesRatio" in raw.columns and pd.notna(row["ForeignInvestmentSharesRatio"]) else float("nan")
            issued = int(row["NumberOfSharesIssued"]) if "NumberOfSharesIssued" in raw.columns and pd.notna(row["NumberOfSharesIssued"]) else 0
            recs.append({
                "date": str(row["date"])[:10],
                "stock_id": sid,
                "foreign_holding_ratio": ratio,
                "shares_issued": issued,
            })
        buffer.extend(recs)
        ok_count += 1

        if (j + 1) % (buffer_cap * 5) == 0 and buffer:
            _flush(dataset, buffer, key_cols)
            buffer = []
            logger.info("[shareholding] Progress: %d/%d stocks", j + 1, len(all_ids))

    if buffer:
        _flush(dataset, buffer, key_cols)

    elapsed = time.time() - t0
    return {"ok_stocks": ok_count, "fail_stocks": fail_count, "elapsed_min": elapsed / 60}


def download_short_sale(dates: list, universe: pd.DataFrame, tpex_ids: set, dl,
                        buffer_cap: int = FLUSH_EVERY_N_DAYS) -> dict:
    """
    Download short sale / SBL (借券賣出).
    TWSE stocks: TWT93U batch per date.
    TPEX stocks: FinMind per-stock (taiwan_daily_short_sale_balances).
    """
    dataset = "short_sale"
    key_cols = ["date", "stock_id"]
    buffer = []
    ok_days = 0
    fail_days = 0
    fallback_count = 0
    t0 = time.time()

    # Phase 1: TWSE batch by date
    logger.info("[short_sale] Phase 1/2: TWSE TWT93U batch (%d dates)", len(dates))
    for i, dt in enumerate(dates):
        recs = _fetch_shortsale_twse_one_day(dt)
        if recs:
            buffer.extend(recs)
            ok_days += 1
        else:
            logger.warning("[short_sale] No TWSE data for %s", dt.strftime("%Y-%m-%d"))
            fail_days += 1
        if (i + 1) % buffer_cap == 0 and buffer:
            _flush(dataset, buffer, key_cols)
            buffer = []

    if buffer:
        _flush(dataset, buffer, key_cols)
        buffer = []

    # Phase 2: TPEX stocks via FinMind
    logger.info("[short_sale] Phase 2/2: TPEX stocks via FinMind (%d stocks)", len(tpex_ids))
    start_str = dates[0].strftime("%Y-%m-%d")
    end_str   = dates[-1].strftime("%Y-%m-%d")

    for j, sid in enumerate(sorted(tpex_ids)):
        raw = _finmind_call(dl, "taiwan_daily_short_sale_balances",
                            stock_id=sid, start_date=start_str, end_date=end_str)
        if raw.empty:
            fallback_count += 1
            continue
        if "date" not in raw.columns:
            fallback_count += 1
            continue
        recs = []
        for _, row in raw.iterrows():
            recs.append({
                "date": str(row["date"])[:10],
                "stock_id": sid,
                "sbl_balance": int(row["SBLShortSalesCurrentDayBalance"]) if "SBLShortSalesCurrentDayBalance" in raw.columns and pd.notna(row["SBLShortSalesCurrentDayBalance"]) else 0,
                "sbl_sell":    int(row["SBLShortSalesShortSales"])         if "SBLShortSalesShortSales"         in raw.columns and pd.notna(row["SBLShortSalesShortSales"])         else 0,
                "sbl_return":  int(row["SBLShortSalesReturns"])            if "SBLShortSalesReturns"            in raw.columns and pd.notna(row["SBLShortSalesReturns"])            else 0,
                "sbl_adjust":  int(row["SBLShortSalesAdjustments"])        if "SBLShortSalesAdjustments"        in raw.columns and pd.notna(row["SBLShortSalesAdjustments"])        else 0,
                "ms_sell":     int(row["MarginShortSalesShortSales"])       if "MarginShortSalesShortSales"       in raw.columns and pd.notna(row["MarginShortSalesShortSales"])       else 0,
                "ms_buy":      int(row["MarginShortSalesShortCovering"])    if "MarginShortSalesShortCovering"    in raw.columns and pd.notna(row["MarginShortSalesShortCovering"])    else 0,
                "ms_balance":  int(row["MarginShortSalesCurrentDayBalance"]) if "MarginShortSalesCurrentDayBalance" in raw.columns and pd.notna(row["MarginShortSalesCurrentDayBalance"]) else 0,
                "market": "tpex",
            })
        buffer.extend(recs)
        fallback_count += 1

        if (j + 1) % buffer_cap == 0 and buffer:
            _flush(dataset, buffer, key_cols)
            buffer = []

    if buffer:
        _flush(dataset, buffer, key_cols)

    elapsed = time.time() - t0
    return {"ok_days": ok_days, "fail_days": fail_days, "fallback": fallback_count,
            "elapsed_min": elapsed / 60}


# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="C2-a: Bulk download TW chip history for IC backtest")
    parser.add_argument("--dataset", default="all",
                        choices=ALL_DATASETS + ["all"],
                        help="Which dataset to download (default: all)")
    parser.add_argument("--start-date", default=DEFAULT_START, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date",   default=DEFAULT_END,   help="End date YYYY-MM-DD")
    parser.add_argument("--test",   action="store_true",
                        help="Test mode: 10 stocks x 1 month (April 2024)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last saved date per dataset")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ----- Universe -----
    if not UNIVERSE_PATH.exists():
        logger.error("Universe file not found: %s", UNIVERSE_PATH)
        logger.error("Run tools/backtest_dl_ohlcv.py first to generate universe_tw.parquet")
        return 1

    universe = pd.read_parquet(UNIVERSE_PATH)
    logger.info("Universe: %d stocks (%d TWSE, %d TPEX)",
                len(universe),
                (universe["type"] == "twse").sum(),
                (universe["type"] == "tpex").sum())

    twse_ids = set(universe.loc[universe["type"] == "twse", "stock_id"].astype(str))
    tpex_ids = set(universe.loc[universe["type"] == "tpex", "stock_id"].astype(str))
    all_ids  = list(universe["stock_id"].astype(str))

    # ----- Test mode -----
    if args.test:
        test_stocks = ["2330", "2454", "2317", "2412", "3034",  # TWSE
                       "3661", "5269", "6415", "6770", "3016"]  # mix TWSE/TPEX
        universe = universe[universe["stock_id"].isin(test_stocks)].copy()
        twse_ids = set(universe.loc[universe["type"] == "twse", "stock_id"].astype(str))
        tpex_ids = set(universe.loc[universe["type"] == "tpex", "stock_id"].astype(str))
        all_ids  = list(universe["stock_id"].astype(str))
        args.start_date = "2024-04-01"
        args.end_date   = "2024-04-30"
        logger.info("TEST MODE: %d stocks, %s ~ %s", len(universe), args.start_date, args.end_date)

    # ----- Which datasets to run -----
    datasets_to_run = ALL_DATASETS if args.dataset == "all" else [args.dataset]

    # ----- FinMind loader (lazy init, only if needed) -----
    needs_finmind = any(d in datasets_to_run for d in
                        ["margin", "day_trading", "shareholding", "short_sale"])
    dl = None
    if needs_finmind:
        from cache_manager import get_finmind_loader
        dl = get_finmind_loader()
        logger.info("FinMind loader initialized (has_token=%s)", dl.has_token)

    # ----- Run each dataset -----
    all_stats = {}
    for dataset in datasets_to_run:
        start_date = args.start_date
        if args.resume:
            last = _get_last_saved_date(dataset)
            if last and last >= args.end_date:
                logger.info("[%s] Already complete (last=%s), skipping", dataset, last)
                continue
            if last:
                # resume from day after last saved
                next_day = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info("[%s] Resuming from %s (last saved: %s)", dataset, next_day, last)
                start_date = next_day

        dates = _generate_trading_dates(start_date, args.end_date)
        logger.info("[%s] %d trading days (%s ~ %s)", dataset, len(dates), dates[0].strftime("%Y-%m-%d"), dates[-1].strftime("%Y-%m-%d"))

        if dataset == "institutional":
            stats = download_institutional(dates, universe)
        elif dataset == "margin":
            stats = download_margin(dates, universe, tpex_ids, dl)
        elif dataset == "day_trading":
            stats = download_day_trading(dates, all_ids, dl)
        elif dataset == "shareholding":
            stats = download_shareholding(dates, all_ids, dl)
        elif dataset == "short_sale":
            stats = download_short_sale(dates, universe, tpex_ids, dl)

        all_stats[dataset] = stats
        logger.info("[%s] Done: %s", dataset, stats)

    # ----- Summary -----
    logger.info("=== SUMMARY ===")
    for ds, st in all_stats.items():
        path = _parquet_path(ds)
        if path.exists():
            df = pd.read_parquet(path)
            size_mb = path.stat().st_size / 1e6
            logger.info(
                "  %-20s rows=%7d  stocks=%4d  %.1f MB  stats=%s",
                ds, len(df), df["stock_id"].nunique(), size_mb, st,
            )
        else:
            logger.info("  %-20s (no parquet produced)  stats=%s", ds, st)

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
