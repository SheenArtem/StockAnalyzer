"""
fetch_tw_lei_panel.py -- NDC 景氣領先指標 7 分項歷史 panel 建立

資料來源：index.ndc.gov.tw AngularJS SPA 後端 JSON API
  POST /n/json/data/eco/index   -- 6 個分項 + 2 個綜合指數
  POST /n/json/data/eco/signal  -- 製造業營業氣候測驗點 (SR0056，2013-07+)

一次各呼叫一次即可，無需下載 PDF / Camelot。

7 個領先分項 (code -> 欄位名稱 -> 來源端點 -> 起始月)：
  SR0007 外銷訂單動向指數     leading_export_order_idx   eco/index  2000-01
  SR0008 貨幣總計數 M1B       leading_m1b                eco/index  1982-01
  SR0009 股價指數             leading_stock_price_idx    eco/index  1967-01
  SR0052 受僱員工淨進入率     leading_employee_entry_rt  eco/index  1980-01
  SR0012 建築物開工樓地板面積 leading_building_area      eco/index  2009-01
  SR0022 半導體設備進口值     leading_semi_equip_import  eco/index  2001-01
  SR0056 製造業營業氣候測驗點 leading_mfg_climate        eco/signal 2013-07

另含領先指標綜合指數 (SR0001) 及不含趨勢 (SR0051)，均來自 eco/index。

輸出：data/macro/tw_lei_panel.parquet
schema:
  date (index, datetime64[ns] YYYY-MM-01)
  lei_composite          -- 領先指標綜合指數
  lei_ex_trend           -- 領先指標不含趨勢指數
  leading_export_order_idx
  leading_m1b
  leading_stock_price_idx
  leading_employee_entry_rt
  leading_building_area
  leading_semi_equip_import
  leading_mfg_climate    -- 2013-07 起有資料

執行：
  python tools/fetch_tw_lei_panel.py
  python tools/fetch_tw_lei_panel.py --from-year 2010
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "macro" / "tw_lei_panel.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

NDC_PAGE   = "https://index.ndc.gov.tw/n/zh_tw/data/eco/indicators_table2"
NDC_INDEX  = "https://index.ndc.gov.tw/n/json/data/eco/index"
NDC_SIGNAL = "https://index.ndc.gov.tw/n/json/data/eco/signal"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Indicator ID -> output column name (confirmed from live API 2026-05-09)
# IDs from eco/index endpoint:
INDEX_IDS: dict[str, str] = {
    "33":  "lei_composite",             # SR0001 領先指標綜合指數
    "34":  "lei_ex_trend",              # SR0051 領先指標不含趨勢
    "35":  "leading_export_order_idx",  # SR0007 外銷訂單動向指數
    "36":  "leading_m1b",               # SR0008 貨幣總計數 M1B
    "37":  "leading_stock_price_idx",   # SR0009 股價指數
    "38":  "leading_employee_entry_rt", # SR0052 工業及服務業受僱員工淨進入率
    "39":  "leading_building_area",     # SR0012 建築物開工樓地板面積
    "277": "leading_semi_equip_import", # SR0022 半導體設備進口值
}

# IDs from eco/signal endpoint (SR0056 not in eco/index):
SIGNAL_IDS: dict[str, str] = {
    "7": "leading_mfg_climate",         # SR0056 製造業營業氣候測驗點 (2013-07+)
}

# Final column order for output
COLUMN_ORDER = [
    "lei_composite",
    "lei_ex_trend",
    "leading_export_order_idx",
    "leading_m1b",
    "leading_stock_price_idx",
    "leading_employee_entry_rt",
    "leading_building_area",
    "leading_semi_equip_import",
    "leading_mfg_climate",
]


def _get_csrf_and_session() -> tuple[requests.Session, str]:
    """建立 session，從 NDC 頁面取得 CSRF token."""
    session = requests.Session()
    session.verify = False
    r = session.get(NDC_PAGE, headers={"User-Agent": UA}, timeout=20, verify=False)
    r.raise_for_status()
    m = re.search(r'csrf-token.*?content="([^"]+)"', r.text)
    if not m:
        raise RuntimeError("Cannot extract CSRF token from NDC page")
    token = m.group(1)
    logger.info("CSRF OK: %s...", token[:12])
    return session, token


def _post_api(session: requests.Session, csrf_token: str, api_url: str) -> dict:
    """POST 到 NDC JSON API，回傳解析後 dict."""
    post_headers = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRF-TOKEN": csrf_token,
        "Content-Type": "application/json",
        "Referer": NDC_PAGE,
        "Origin": "https://index.ndc.gov.tw",
    }
    logger.info("POST %s ...", api_url)
    r = session.post(api_url, headers=post_headers, timeout=30)
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "application/json" not in ct:
        raise RuntimeError(f"Expected JSON, got: {ct}")
    return r.json()


def parse_line_series(data_points: list[dict]) -> pd.Series:
    """將 API data 陣列 [{x:'YYYYMM', y:float|null}, ...] 轉成 pd.Series."""
    rows = []
    for pt in data_points:
        x_str = pt.get("x", "")
        y_val = pt.get("y")
        if len(x_str) == 6 and y_val is not None:
            try:
                yr = int(x_str[:4])
                mo = int(x_str[4:])
                rows.append((pd.Timestamp(year=yr, month=mo, day=1), float(y_val)))
            except (ValueError, TypeError):
                pass
    if not rows:
        return pd.Series(dtype=float)
    idx, vals = zip(*rows)
    return pd.Series(vals, index=pd.DatetimeIndex(idx), dtype=float)


def extract_series(raw: dict, id_map: dict[str, str]) -> dict[str, pd.Series]:
    """從 raw JSON 中按 id_map 提取各序列，回傳 {col_name: Series}."""
    lines = raw.get("line", {})
    result: dict[str, pd.Series] = {}
    for str_id, col_name in id_map.items():
        item = lines.get(str_id)
        if item is None:
            logger.warning("ID %s (%s) not found in API response", str_id, col_name)
            continue
        dp = item.get("data", [])
        s = parse_line_series(dp)
        if s.empty:
            logger.warning("ID %s (%s) has no non-null data", str_id, col_name)
            continue
        result[col_name] = s
        logger.info("  %-32s  %d rows  %s ~ %s",
                    col_name, len(s),
                    s.index.min().strftime("%Y-%m"),
                    s.index.max().strftime("%Y-%m"))
    return result


def build_panel(from_year: int | None = None) -> pd.DataFrame:
    """呼叫兩個 NDC API 端點，合併成 DataFrame."""
    logger.info("取得 NDC CSRF token ...")
    session, csrf_token = _get_csrf_and_session()

    # First call: eco/index (6 components + 2 composites)
    raw_index = _post_api(session, csrf_token, NDC_INDEX)
    time.sleep(1)

    # Second call: eco/signal (SR0056 製造業營業氣候測驗點)
    raw_signal = _post_api(session, csrf_token, NDC_SIGNAL)

    logger.info("eco/index latest_date: %s", raw_index.get("latest_date", "N/A"))

    series_dict: dict[str, pd.Series] = {}
    series_dict.update(extract_series(raw_index, INDEX_IDS))
    series_dict.update(extract_series(raw_signal, SIGNAL_IDS))

    df = pd.DataFrame(series_dict)
    df.index.name = "date"
    df = df.sort_index()

    if from_year is not None:
        df = df[df.index.year >= from_year]

    # Ensure column order
    df = df[[c for c in COLUMN_ORDER if c in df.columns]]

    return df


def validate_and_print(df: pd.DataFrame) -> None:
    """打印最近 12 個月的 7 分項數值，並輸出 shape / date range."""
    sys.stdout.buffer.write(b"\n=== tw_lei_panel validation ===\n")
    sys.stdout.buffer.write(
        (f"Shape: {df.shape}  "
         f"Date range: {df.index.min().strftime('%Y-%m')} ~ "
         f"{df.index.max().strftime('%Y-%m')}\n").encode("utf-8")
    )
    sys.stdout.buffer.write(b"\nNon-null counts per column:\n")
    for col in df.columns:
        nn = df[col].notna().sum()
        first = df[col].dropna().index.min().strftime("%Y-%m") if nn > 0 else "N/A"
        sys.stdout.buffer.write(f"  {col:<35} {nn:>5} rows  starts {first}\n".encode("utf-8"))

    sys.stdout.buffer.write(b"\nLast 12 months:\n")
    out = df.tail(12).to_string()
    sys.stdout.buffer.write(out.encode("utf-8"))
    sys.stdout.buffer.write(b"\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch NDC TW LEI 7-component panel")
    parser.add_argument("--from-year", type=int, default=None,
                        help="Filter to rows >= YYYY (default: all history)")
    args = parser.parse_args()

    df = build_panel(from_year=args.from_year)

    if df.empty:
        logger.error("Empty panel - aborting")
        sys.exit(1)

    validate_and_print(df)

    df.to_parquet(OUT, index=True)
    logger.info("Saved -> %s", OUT)


if __name__ == "__main__":
    main()
