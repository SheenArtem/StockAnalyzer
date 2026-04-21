"""
TDCC OpenData id=1-5 集保戶股權分散表 下載器
================================================
週更單一 snapshot：全市場 3,956 檔股票 × 17 個持股分級（1-15 持股級距 + 16 差異 + 17 合計）。

來源: https://openapi.tdcc.com.tw/v1/opendata/1-5
更新: 每週六凌晨（對應前一週五收盤狀態）
歷史: ⚠️ API 不支援歷史日期參數，只給最新一週。歷史靠本檔排程累積。
產出: data_cache/tdcc/1-5/<data_date>.parquet

原始欄位（中文）:
    證券代號          → stock_id (有 trailing space 要 strip)
    持股分級          → level (int 1-17)
    人數              → people_count (int)
    股數              → shares (int)
    占集保庫存數比例% → pct (float)
    資料日期          → data_date (str YYYYMMDD)

衍生欄位（在下載時就算好，省分析時 groupby 成本）:
    is_retail   — level 1-5    (<20 張，散戶)
    is_large    — level 11-15  (>200 張，大戶含機構)
    is_whale    — level 15     (>1000 張，巨鯨/機構/家族信託)
    is_total    — level 17     (合計，用來計算占比)

持股分級定義（TDCC 標準）:
    1:  1-999 股      (零股族)
    2:  1千-5千
    3:  5千-1萬
    4:  1萬-1.5萬
    5:  1.5萬-2萬
    6:  2萬-3萬
    7:  3萬-4萬
    8:  4萬-5萬
    9:  5萬-10萬
    10: 10萬-20萬
    11: 20萬-40萬
    12: 40萬-60萬
    13: 60萬-80萬
    14: 80萬-100萬
    15: >100萬 (超過 1,000 張)
    16: 差異數調整
    17: 合計

使用:
    python tools/tdcc_shareholding.py
    python tools/tdcc_shareholding.py --force   # 已存在也重抓
"""
from __future__ import annotations

import argparse
import re
import sys
import time
import urllib3
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TDCC_URL = "https://openapi.tdcc.com.tw/v1/opendata/1-5"
OUT_DIR = Path("data_cache/tdcc/1-5")

# 原始中文欄位 → 正規化英文欄位
# 注意：TDCC 回的 JSON 第一個 key 常帶 \ufeff BOM，要先做 strip
FIELD_MAP = {
    "證券代號": "stock_id",
    "持股分級": "level",
    "人數": "people_count",
    "股數": "shares",
    "占集保庫存數比例%": "pct",
    "資料日期": "data_date",
}


def download_tdcc_1_5(
    url: str = TDCC_URL,
    max_retries: int = 3,
    backoff_seconds: int = 30,
    timeout: int = 180,
) -> tuple[list[dict], str | None]:
    """下載 TDCC OpenData 1-5，回傳 (records, data_date)。"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; StockAnalyzer/1.0)",
        "Accept": "application/json",
    })

    last_err: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[tdcc-1-5] 第 {attempt}/{max_retries} 次嘗試下載 {url} ...", flush=True)
            t0 = time.time()
            resp = session.get(url, verify=False, timeout=timeout)
            resp.raise_for_status()

            cd = resp.headers.get("Content-Disposition", "")
            m = re.search(r"TDCC_OD_1-5_(\d{8})_\d+\.json", cd)
            data_date = m.group(1) if m else None

            size_mb = len(resp.content) / (1024 * 1024)
            elapsed = time.time() - t0
            print(
                f"[tdcc-1-5] 下載成功 {size_mb:.2f} MB / {elapsed:.1f}s / data_date={data_date}",
                flush=True,
            )

            records = resp.json()
            if not isinstance(records, list):
                raise ValueError(f"預期 JSON array，實際型別 {type(records).__name__}")
            return records, data_date

        except Exception as exc:  # noqa: BLE001
            last_err = exc
            print(f"[tdcc-1-5] 失敗: {exc!r}", flush=True)
            if attempt < max_retries:
                print(f"[tdcc-1-5] 等待 {backoff_seconds}s 後重試 ...", flush=True)
                time.sleep(backoff_seconds)

    raise RuntimeError(f"TDCC 1-5 下載失敗（{max_retries} 次重試）: {last_err!r}")


def to_dataframe(records: list[dict]) -> pd.DataFrame:
    """將 raw JSON 轉正規化 DataFrame，含衍生欄位。"""
    df = pd.DataFrame(records)
    df.columns = [c.lstrip("\ufeff") for c in df.columns]

    rename_map = {k: v for k, v in FIELD_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # 檢查必要欄位
    required = {"stock_id", "level", "people_count", "shares", "pct", "data_date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"缺少必要欄位: {missing}，實際欄位: {list(df.columns)}")

    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["level"] = df["level"].astype(int)
    df["people_count"] = df["people_count"].astype(int)
    df["shares"] = df["shares"].astype(int)
    df["pct"] = df["pct"].astype(float)
    df["data_date"] = df["data_date"].astype(str).str.strip()

    # 衍生欄位
    df["is_retail"] = df["level"].between(1, 5)
    df["is_large"] = df["level"].between(11, 15)
    df["is_whale"] = df["level"] == 15
    df["is_total"] = df["level"] == 17

    return df


def save_parquet(df: pd.DataFrame, out_path: Path) -> None:
    """存 parquet。"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["download_ts"] = datetime.now().isoformat(timespec="seconds")
    df.to_parquet(out_path, index=False)
    print(f"[tdcc-1-5] 已存檔 {out_path} ({len(df):,} rows)", flush=True)


def summarize(df: pd.DataFrame) -> None:
    """印資料摘要 + 抽樣 2330/0050 驗證資料品質。"""
    print("\n" + "=" * 60)
    print("TDCC 1-5 集保股權分散表 統計")
    print("=" * 60)
    print(f"總筆數:       {len(df):,}")
    print(f"涵蓋證券數:   {df['stock_id'].nunique():,}")
    print(f"資料日期:     {df['data_date'].iloc[0]}")
    print(f"持股分級範圍: {sorted(df['level'].unique().tolist())}")

    # 全市場等級統計
    lvl_summary = df.groupby("level", as_index=False).agg(
        total_people=("people_count", "sum"),
        total_shares=("shares", "sum"),
    )
    print("\n[全市場各分級總計]")
    print(lvl_summary.to_string(index=False))

    # 抽樣 2330 驗證
    print("\n[2330 台積電 verify]")
    tsmc = df[df["stock_id"] == "2330"].sort_values("level")
    if tsmc.empty:
        print("  ❌ 找不到 2330，檢查 stock_id strip 邏輯！")
    else:
        total_people = tsmc[tsmc["is_total"]]["people_count"].iloc[0]
        total_shares = tsmc[tsmc["is_total"]]["shares"].iloc[0]
        retail_shares = tsmc[tsmc["is_retail"]]["shares"].sum()
        large_shares = tsmc[tsmc["is_large"]]["shares"].sum()
        whale_shares = tsmc[tsmc["is_whale"]]["shares"].sum()
        print(f"  總持股人數: {total_people:,}")
        print(f"  總股數:     {total_shares:,}")
        print(f"  散戶股數占比 (level 1-5):  {retail_shares/total_shares*100:.2f}%")
        print(f"  大戶股數占比 (level 11-15): {large_shares/total_shares*100:.2f}%")
        print(f"  巨鯨股數占比 (level 15):   {whale_shares/total_shares*100:.2f}%")

    print("=" * 60 + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TDCC 1-5 集保股權分散表下載器")
    parser.add_argument("--force", action="store_true", help="強制重抓（忽略既有檔）")
    parser.add_argument("--out-dir", default=str(OUT_DIR), help="輸出目錄")
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)

    # 先探 API 拿 data_date，若已抓過且非 --force 就 skip
    records, data_date = download_tdcc_1_5()
    if not data_date:
        data_date = datetime.now().strftime("%Y%m%d")
        print(f"[tdcc-1-5] 警告：無法從 header 解析 data_date，用 今天 {data_date}", flush=True)

    out_path = out_dir / f"{data_date}.parquet"
    if out_path.exists() and not args.force:
        print(f"[tdcc-1-5] {out_path} 已存在，略過（用 --force 強制重抓）", flush=True)
        df = pd.read_parquet(out_path)
        summarize(df)
        return 0

    df = to_dataframe(records)
    save_parquet(df, out_path)
    summarize(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
