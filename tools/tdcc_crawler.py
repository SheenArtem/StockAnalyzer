"""
TDCC OpenData 全面抓取器（Phase 2 週更 skeleton）
====================================================
目前只實作 1-1 證券基本資料主檔（delegated to tdcc_universe_download.main()）。
Phase 2 會擴充到其他常用 dataset（持股分散表、董監持股、融資券等），並由
Windows Task Scheduler 週更。

TDCC API 目錄參考: https://openapi.tdcc.com.tw/

常用 dataset（待補）:
    1-1  證券基本資料主檔       — universe 黑名單修正（本檔 DONE）
    1-5  集保戶股權分散表       — 大小戶分布（週更）
    1-6  集保戶股權分散表依證券 — 個股持股人數分布
    6-*  興櫃/公開發行相關      — 視需求補

設計原則:
    - 每個 dataset 一個 fetch_<id> 函式，輸出統一放 data_cache/tdcc/<id>/<date>.parquet
    - 共用 rate-limit/retry/verify=False 策略
    - 呼叫端（scheduler / Windows Task）傳 --datasets 1-1,1-5 決定抓哪些

目前此檔僅作為 future scope 的 entry point，避免散落多個 ad-hoc 腳本。

使用:
    python tools/tdcc_crawler.py --datasets 1-1
    python tools/tdcc_crawler.py --datasets 1-1,1-5    # (1-5 未實作會 skip)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 復用各 dataset 的下載邏輯
from tdcc_universe_download import main as run_universe_1_1
from tdcc_shareholding import main as run_shareholding_1_5

TDCC_CACHE_ROOT = Path("data_cache/tdcc")

# dataset id → (description, handler fn or None)
# 註：1-6 經實測是「債券」而非股票集保分散表（欄位：債券代號/債券餘額/公司債類別），
# 故不做 1-6。若未來 TDCC 新增股票月頻分散表再補。
DATASETS: dict[str, tuple[str, callable | None]] = {
    "1-1": ("證券基本資料主檔", run_universe_1_1),
    "1-5": ("集保戶股權分散表（週更）", run_shareholding_1_5),
}


def run_dataset(ds_id: str, force: bool = False) -> int:
    """跑單一 dataset；未實作就印 warning 並回 0。"""
    if ds_id not in DATASETS:
        print(f"[tdcc] 未知 dataset id={ds_id}，略過")
        return 0
    desc, handler = DATASETS[ds_id]
    if handler is None:
        print(f"[tdcc] dataset {ds_id} ({desc}) 尚未實作，略過 (TODO Phase 2)")
        return 0
    print(f"[tdcc] === 執行 dataset {ds_id} ({desc}) ===")
    argv = ["--force"] if force else []
    return handler(argv)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TDCC OpenData crawler (multi-dataset)")
    parser.add_argument(
        "--datasets",
        default="1-1",
        help="逗號分隔的 dataset id（預設 1-1）。例: --datasets 1-1,1-5",
    )
    parser.add_argument("--force", action="store_true", help="強制重抓")
    args = parser.parse_args(argv)

    ds_list = [x.strip() for x in args.datasets.split(",") if x.strip()]
    rc = 0
    for ds in ds_list:
        code = run_dataset(ds, force=args.force)
        rc = rc or code
    return rc


if __name__ == "__main__":
    sys.exit(main())
