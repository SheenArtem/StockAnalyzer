"""
TDCC OpenData id=1-1 證券基本資料主檔 下載器
================================================
下載 TDCC 完整證券清單（含下市/終止/暫停），修正 universe_tw.parquet
的 survivorship bias。

來源: https://openapi.tdcc.com.tw/v1/opendata/1-1
格式: JSON (~50 MB, 14萬筆以上)
產出: data_cache/backtest/universe_tw_full.parquet

欄位（中文 → 正規化）:
    證券代號     → stock_id
    證券名稱     → name
    市場別       → market           (上市/上櫃/興櫃/公開發行 等)
    股務單位     → transfer_agent
    停止過戶領回 → suspend_transfer
    證券狀態     → status           (正常/終止/暫停 等)
    無面無實體   → dematerialized
    歸戶領回     → account_recovery
    限制部分帳簿劃撥功能註記 → book_entry_restriction
    發行幣別     → currency
    股票面額     → par_value

衍生欄位:
    is_common_stock  — 四碼數字且非 ETF/權證 → True
    is_etf           — 00xx / 006xxx / 00xxx
    is_warrant       — 6xxxx / 7xxxx

使用:
    python tools/tdcc_universe_download.py
    python tools/tdcc_universe_download.py --force   # 強制重抓（忽略當日快取）

Notes:
    - TDCC SSL 憑證常有 chain 問題，用 verify=False（一般台灣公部門慣例）
    - Rate limit 嚴，失敗等 30 秒重試，最多 3 次
    - response 含 Content-Disposition，自動解析資料日期存入 parquet metadata
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib3
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# 關閉 InsecureRequestWarning（TDCC 用 verify=False）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TDCC_URL = "https://openapi.tdcc.com.tw/v1/opendata/1-1"
OUT_PATH = Path("data_cache/backtest/universe_tw_full.parquet")
RAW_JSON_PATH = Path("data_cache/backtest/_tdcc_1_1_raw.json")

# 中文欄位 → 英文欄位映射（含實測 2026-04 的欄位名；首欄有 BOM \ufeff）
FIELD_MAP = {
    "證券代號": "stock_id",
    "\ufeff證券代號": "stock_id",  # 首個 key 常帶 BOM
    "證券名稱": "name",
    "市場別": "market",
    "股務單位": "transfer_agent",
    "停止過戶領回": "suspend_transfer",
    "證券狀態": "status",
    "無面無實體": "dematerialized",
    "歸戶領回": "account_recovery",
    "限制部分帳簿劃撥功能註記": "book_entry_restriction",
    "發行幣別": "currency",
    "股票面額": "par_value",
    "每股面額(元)": "par_value",  # 實測欄位名
    "更新日期": "update_date",
}


def download_tdcc(
    url: str = TDCC_URL,
    max_retries: int = 3,
    backoff_seconds: int = 30,
    timeout: int = 180,
) -> tuple[list[dict], str | None]:
    """
    下載 TDCC OpenData 1-1，回傳 (records, data_date)。

    data_date 來自 Content-Disposition: filename=TDCC_OD_1-1_YYYYMMDD_HHMMSS.json
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; StockAnalyzer/1.0)",
        "Accept": "application/json",
    })

    last_err: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[tdcc] 第 {attempt}/{max_retries} 次嘗試下載 {url} ...", flush=True)
            t0 = time.time()
            resp = session.get(url, verify=False, timeout=timeout, stream=False)
            resp.raise_for_status()

            # 解析資料日期（從 Content-Disposition）
            cd = resp.headers.get("Content-Disposition", "")
            m = re.search(r"TDCC_OD_1-1_(\d{8})_\d+\.json", cd)
            data_date = m.group(1) if m else None

            size_mb = len(resp.content) / (1024 * 1024)
            elapsed = time.time() - t0
            print(
                f"[tdcc] 下載成功 {size_mb:.2f} MB / {elapsed:.1f}s / data_date={data_date}",
                flush=True,
            )

            records = resp.json()
            if not isinstance(records, list):
                raise ValueError(f"預期 JSON array，實際型別 {type(records).__name__}")
            return records, data_date

        except Exception as exc:  # noqa: BLE001 — retry on all network/parse errors
            last_err = exc
            print(f"[tdcc] 失敗: {exc!r}", flush=True)
            if attempt < max_retries:
                print(f"[tdcc] 等待 {backoff_seconds}s 後重試 ...", flush=True)
                time.sleep(backoff_seconds)

    raise RuntimeError(f"TDCC 下載失敗（{max_retries} 次重試）: {last_err!r}")


def classify_security(stock_id: str) -> dict:
    """
    依證券代號推斷類型（台股慣例，實測對照 TDCC 1-1）:
        普通股: 四碼純數字且首碼 1-9 (1101/2330/...)
        ETF:    以 00 開頭的純數字（0050/00878/006208）
        權證:   六碼以上純數字（03xxxx/7xxxxx/8xxxxx），或字尾帶字母 (U/P/...)
        其他:   興櫃、KY、海外發行等 → 全部 False

    TDCC 裡還存在很多非股票代號（荷銀鴻運 0001 這種已終止基金/海外商品），
    所以首碼 0 一律不視為普通股。
    """
    sid = str(stock_id).strip().lstrip("\ufeff") if stock_id else ""
    is_pure_digit = sid.isdigit()
    # 四碼純數字且首碼 1-9 = 普通股
    is_common = is_pure_digit and len(sid) == 4 and sid[0] != "0"
    # ETF: 以 00 開頭純數字（涵蓋 0050 / 006208 / 00878）
    is_etf = is_pure_digit and len(sid) >= 4 and sid.startswith("00")
    # 權證: 六碼以上純數字 或 帶字母（排除已歸類 ETF 的 00xxxx）
    is_warrant = (is_pure_digit and len(sid) >= 6 and not sid.startswith("00")) or (
        len(sid) >= 5 and not is_pure_digit and sid[:4].isdigit()
    )
    return {
        "is_common_stock": is_common,
        "is_etf": is_etf,
        "is_warrant": is_warrant,
    }


def to_dataframe(records: list[dict]) -> pd.DataFrame:
    """將 TDCC JSON records 轉為正規化 DataFrame。"""
    df = pd.DataFrame(records)
    # 去除欄位名首的 BOM
    df.columns = [c.lstrip("\ufeff") for c in df.columns]
    # 中文欄位改英文；未出現的欄位忽略
    rename_map = {k: v for k, v in FIELD_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # stock_id 正規化為字串，保留前導 0（雖然台股代號通常無前導 0，但保險起見）
    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].astype(str).str.strip()

    # 其他欄位去除首尾空白
    for col in ["name", "market", "status", "currency", "transfer_agent"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # 面額轉 float（可能是字串或 None）
    if "par_value" in df.columns:
        df["par_value"] = pd.to_numeric(df["par_value"], errors="coerce")

    # 衍生類型旗標
    cls = df["stock_id"].apply(classify_security).apply(pd.Series)
    df = pd.concat([df, cls], axis=1)

    return df


def save_parquet(df: pd.DataFrame, out_path: Path, data_date: str | None) -> None:
    """存 parquet（附 data_date、download_ts 到 DataFrame 欄位）。"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # 把 data_date 寫進欄位方便後續查詢（parquet metadata 麻煩）
    df = df.copy()
    df["data_date"] = data_date or "unknown"
    df["download_ts"] = datetime.now().isoformat(timespec="seconds")
    df.to_parquet(out_path, index=False)
    print(f"[tdcc] 已存檔 {out_path} ({len(df)} rows)", flush=True)


def summarize(df: pd.DataFrame, old_universe_path: Path) -> None:
    """印統計報告並與既有 universe_tw.parquet 比對。"""
    print("\n" + "=" * 60)
    print("TDCC Universe 統計")
    print("=" * 60)
    print(f"總筆數: {len(df):,}")
    if "status" in df.columns:
        print("\n[status 分布]")
        print(df["status"].value_counts(dropna=False).to_string())
    if "market" in df.columns:
        print("\n[market 分布]")
        print(df["market"].value_counts(dropna=False).head(20).to_string())
    print("\n[類型旗標]")
    print(f"  is_common_stock: {int(df['is_common_stock'].sum()):,}")
    print(f"  is_etf:          {int(df['is_etf'].sum()):,}")
    print(f"  is_warrant:      {int(df['is_warrant'].sum()):,}")

    # 抽樣終止上市股票 5 檔，檢查是否在舊 universe 缺席
    terminated_keywords = ["終止", "下市", "停止"]
    status_col = df.get("status", pd.Series([""] * len(df)))
    mask_term = status_col.astype(str).str.contains("|".join(terminated_keywords), na=False)
    # 只看四碼普通股，抽樣才有意義
    mask_term = mask_term & df["is_common_stock"]
    terminated = df[mask_term]
    print(f"\n[終止/下市普通股總數] {len(terminated):,}")

    if old_universe_path.exists():
        old = pd.read_parquet(old_universe_path)
        old_ids = set(old["stock_id"].astype(str))
        new_common_ids = set(df[df["is_common_stock"]]["stock_id"])

        print(f"\n[比對 舊 {old_universe_path.name}]")
        print(f"  舊 universe 筆數:             {len(old_ids):,}")
        print(f"  新 universe_tw_full 普通股數: {len(new_common_ids):,}")

        # 舊是否為新子集
        missing_from_new = old_ids - new_common_ids
        extra_in_new = new_common_ids - old_ids
        print(f"  舊 - 新 (舊有新沒有):          {len(missing_from_new):,}")
        if missing_from_new:
            sample = sorted(missing_from_new)[:5]
            print(f"    前 5 檔: {sample}")
        print(f"  新 - 舊 (新多出來，多為下市): {len(extra_in_new):,}")

        # 抽 5 檔終止股，檢查是否不在舊 universe
        if len(terminated) >= 5:
            sample_term = terminated.sample(min(5, len(terminated)), random_state=42)
            print("\n[抽樣 5 檔終止/下市股確認不在舊 universe]")
            for _, row in sample_term.iterrows():
                sid = row["stock_id"]
                in_old = sid in old_ids
                print(
                    f"  {sid:<8} {row.get('name', ''):<20} "
                    f"status={row.get('status', '')} market={row.get('market', '')} "
                    f"in_old_universe={in_old}"
                )
    else:
        print(f"\n[警告] 找不到舊 universe: {old_universe_path}")

    print("=" * 60 + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TDCC OpenData 1-1 Universe Downloader")
    parser.add_argument("--force", action="store_true", help="強制重抓（忽略當日已存在的輸出）")
    parser.add_argument("--save-raw-json", action="store_true", help="額外存一份原始 JSON 作備份")
    parser.add_argument("--out", default=str(OUT_PATH), help="輸出 parquet 路徑")
    args = parser.parse_args(argv)

    out_path = Path(args.out)
    old_universe = Path("data_cache/backtest/universe_tw.parquet")

    # 若當日已抓過且非 --force，提示跳過
    if out_path.exists() and not args.force:
        mtime = datetime.fromtimestamp(out_path.stat().st_mtime)
        if mtime.date() == datetime.now().date():
            print(
                f"[tdcc] {out_path} 今日已更新（{mtime}），"
                f"略過下載。使用 --force 強制重抓。",
                flush=True,
            )
            # 仍印出 summary 方便驗證
            df = pd.read_parquet(out_path)
            summarize(df, old_universe)
            return 0

    records, data_date = download_tdcc()
    print(f"[tdcc] parse 記錄數: {len(records):,}", flush=True)

    if args.save_raw_json:
        RAW_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
        print(f"[tdcc] 原始 JSON 備份存 {RAW_JSON_PATH}", flush=True)

    df = to_dataframe(records)
    save_parquet(df, out_path, data_date)
    summarize(df, old_universe)
    return 0


if __name__ == "__main__":
    sys.exit(main())
