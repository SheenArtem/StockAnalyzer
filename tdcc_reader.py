"""
TDCC 集保股權分散表讀取 helper
===============================
讀取 `tools/tdcc_shareholding.py` 產生的 `data_cache/tdcc/1-5/<date>.parquet`，
提供個股大戶/散戶/巨鯨分布計算。

使用場景:
    - 個股頁面 UI 顯示股權結構
    - AI 報告 prompt 提供股權集中度
    - 未來累積 13 週後做 IC 驗證

歷史資料限制:
    TDCC OpenAPI 不給歷史，要累積時間才有週變動。剛上線時只有單一 snapshot。
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

TDCC_1_5_DIR = Path("data_cache/tdcc/1-5")

# TDCC 持股分級定義（1-15 級距，16 差異，17 合計）
LEVEL_LABELS = {
    1: "1-999 股 (零股)",
    2: "1-5 張",
    3: "5-10 張",
    4: "10-15 張",
    5: "15-20 張",
    6: "20-30 張",
    7: "30-40 張",
    8: "40-50 張",
    9: "50-100 張",
    10: "100-200 張",
    11: "200-400 張",
    12: "400-600 張",
    13: "600-800 張",
    14: "800-1000 張",
    15: ">1000 張",
    16: "差異數調整",
    17: "合計",
}


def get_latest_snapshot_path() -> Optional[Path]:
    """找最新 1-5 snapshot 檔案（按檔名排序，命名 YYYYMMDD.parquet）。"""
    if not TDCC_1_5_DIR.exists():
        return None
    files = sorted(TDCC_1_5_DIR.glob("*.parquet"))
    return files[-1] if files else None


def list_snapshot_dates() -> list[str]:
    """回傳所有已下載的 snapshot 日期（YYYYMMDD）排序升冪。"""
    if not TDCC_1_5_DIR.exists():
        return []
    return sorted(p.stem for p in TDCC_1_5_DIR.glob("*.parquet"))


def load_stock_distribution(
    stock_id: str,
    snapshot_path: Optional[Path] = None,
) -> Optional[pd.DataFrame]:
    """讀取單檔 17 級分布。

    Returns:
        DataFrame with columns [level, level_label, people_count, shares, pct,
        is_retail, is_large, is_whale, is_total, data_date]
        若找不到資料或檔案不存在回 None。
    """
    if snapshot_path is None:
        snapshot_path = get_latest_snapshot_path()
    if snapshot_path is None or not snapshot_path.exists():
        return None

    df = pd.read_parquet(snapshot_path)
    stock_df = df[df["stock_id"] == str(stock_id).strip()].copy()
    if stock_df.empty:
        return None

    stock_df = stock_df.sort_values("level").reset_index(drop=True)
    stock_df["level_label"] = stock_df["level"].map(LEVEL_LABELS)
    return stock_df[[
        "level", "level_label", "people_count", "shares", "pct",
        "is_retail", "is_large", "is_whale", "is_total", "data_date",
    ]]


def compute_summary(
    stock_id: str,
    snapshot_path: Optional[Path] = None,
) -> Optional[dict]:
    """計算單檔大戶/散戶/巨鯨摘要。

    Returns dict with:
        stock_id, data_date
        total_people, total_shares
        retail_people, retail_shares, retail_people_pct, retail_shares_pct
        large_people, large_shares, large_people_pct, large_shares_pct
        whale_people, whale_shares, whale_people_pct, whale_shares_pct
    """
    df = load_stock_distribution(stock_id, snapshot_path)
    if df is None or df.empty:
        return None

    total_row = df[df["is_total"]]
    if total_row.empty:
        return None

    total_people = int(total_row["people_count"].iloc[0])
    total_shares = int(total_row["shares"].iloc[0])
    if total_shares == 0:
        return None

    retail = df[df["is_retail"]]
    large = df[df["is_large"]]
    whale = df[df["is_whale"]]

    retail_people = int(retail["people_count"].sum())
    retail_shares = int(retail["shares"].sum())
    large_people = int(large["people_count"].sum())
    large_shares = int(large["shares"].sum())
    whale_people = int(whale["people_count"].sum())
    whale_shares = int(whale["shares"].sum())

    return {
        "stock_id": str(stock_id),
        "data_date": df["data_date"].iloc[0],
        "total_people": total_people,
        "total_shares": total_shares,
        "retail_people": retail_people,
        "retail_shares": retail_shares,
        "retail_people_pct": retail_people / total_people * 100 if total_people else 0,
        "retail_shares_pct": retail_shares / total_shares * 100,
        "large_people": large_people,
        "large_shares": large_shares,
        "large_people_pct": large_people / total_people * 100 if total_people else 0,
        "large_shares_pct": large_shares / total_shares * 100,
        "whale_people": whale_people,
        "whale_shares": whale_shares,
        "whale_people_pct": whale_people / total_people * 100 if total_people else 0,
        "whale_shares_pct": whale_shares / total_shares * 100,
    }


def large_pct_trend(stock_id: str, weeks: int = 4) -> list:
    """近 N 週大戶(>200張) 股數占比序列 [(data_date, pct), ...] 舊→新。

    趨勢比單週水位有資訊量（大戶加碼/出貨方向）；不足 2 週由呼叫端判斷。
    """
    files = sorted(TDCC_1_5_DIR.glob("*.parquet"))[-weeks:]
    out = []
    for p in files:
        s = compute_summary(stock_id, snapshot_path=p)
        if s:
            out.append((s["data_date"], s["large_shares_pct"]))
    return out


def format_shareholding_for_prompt(stock_id: str) -> str:
    """為 AI 報告 prompt 產生簡潔的股權結構描述字串。"""
    s = compute_summary(stock_id)
    if s is None:
        return ""
    txt = (
        f"【集保股權分散 (TDCC {s['data_date']})】\n"
        f"- 總持股人數: {s['total_people']:,}\n"
        f"- 散戶(<20張) 股數占比: {s['retail_shares_pct']:.2f}% (人數 {s['retail_people']:,})\n"
        f"- 大戶(>200張) 股數占比: {s['large_shares_pct']:.2f}% (人數 {s['large_people']:,})\n"
        f"- 巨鯨(>1000張) 股數占比: {s['whale_shares_pct']:.2f}% (人數 {s['whale_people']:,})\n"
    )
    trend = large_pct_trend(stock_id, weeks=4)
    if len(trend) >= 2:
        seq = " -> ".join(f"{pct:.2f}%" for _, pct in trend)
        delta = trend[-1][1] - trend[0][1]
        txt += (f"- 大戶占比近 {len(trend)} 週趨勢 ({trend[0][0]} -> {trend[-1][0]}): "
                f"{seq} (Δ {delta:+.2f}pp)\n")
    return txt


if __name__ == "__main__":
    # 簡易驗證
    for sid in ["2330", "0050", "2317"]:
        s = compute_summary(sid)
        if s:
            print(f"\n{sid}:")
            for k, v in s.items():
                if isinstance(v, float):
                    print(f"  {k}: {v:.2f}")
                else:
                    print(f"  {k}: {v:,}" if isinstance(v, int) else f"  {k}: {v}")
        else:
            print(f"{sid}: 無資料")
