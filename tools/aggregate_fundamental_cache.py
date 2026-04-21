"""
aggregate_fundamental_cache.py
==============================
把 VF-L1a backfill 產生的 per-stock parquet（data_cache/fundamental_cache/）
匯總成歷史回測用的聚合檔（data_cache/backtest/financials_*.parquet）。

⚠️ RF-1 鐵則（2026-04-21）：
  **本工具是唯一被允許寫入 data_cache/backtest/financials_*.parquet 的檔案。**
  任何 backfill tool 只能寫 fundamental_cache/{cache_key}_{sid}.parquet（per-stock），
  然後在結束時呼叫本工具聚合到 backtest/。絕對禁止 backfill 直接寫 backtest/。

  背景：VF-VC P3-a 事件（2026-04-20）就是因 backfill 直接寫 backtest/ 而 live cache stale，
  導致 scanner 重抓觸發 MOPS WAF ban。本規則確保 live + backtest 永遠一致。
  詳見 memory/feedback_unified_cache.md。

- 輸入目錄: data_cache/fundamental_cache/
  檔名模式: {cache_key}_{stock_id}.parquet
  cache_key in: financial_statement / balance_sheet / cash_flows_statement
                / month_revenue / dividend

- 輸出：覆蓋 data_cache/backtest/financials_{income,balance,cashflow,revenue}.parquet
  schema 與舊聚合檔一致（date, stock_id, type, value, origin_name）

用法:
  python tools/aggregate_fundamental_cache.py           # 全部 4 類
  python tools/aggregate_fundamental_cache.py --category income
  python tools/aggregate_fundamental_cache.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

FUND_CACHE_DIR = ROOT / "data_cache" / "fundamental_cache"
BACKTEST_DIR = ROOT / "data_cache" / "backtest"

# (cache_key in fundamental_cache, output filename in backtest/)
MAPPINGS = [
    ("financial_statement",   "financials_income.parquet"),
    ("balance_sheet",         "financials_balance.parquet"),
    ("cash_flows_statement",  "financials_cashflow.parquet"),
    ("month_revenue",         "financials_revenue.parquet"),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("aggregate")


def aggregate_category(cache_key: str, out_file: str, dry_run: bool = False) -> None:
    """Read all {cache_key}_*.parquet and concat into one aggregated parquet."""
    pattern = f"{cache_key}_*.parquet"
    files = sorted(FUND_CACHE_DIR.glob(pattern))
    logger.info("Aggregating %s: %d files -> %s", cache_key, len(files), out_file)

    if not files:
        logger.warning("No files matching %s, skipping", pattern)
        return

    t0 = time.time()
    frames = []
    skipped = 0
    for i, f in enumerate(files):
        try:
            df = pd.read_parquet(f)
            if df.empty:
                skipped += 1
                continue
            # Schema sanity check
            if "stock_id" not in df.columns:
                logger.warning("Skipping %s: no stock_id column", f.name)
                skipped += 1
                continue
            # RF-1 schema normalization：fundamental_cache 歷史上混用 str vs datetime64,
            # pyarrow concat 會報 ArrowTypeError，統一轉 str 對齊 FinMind 原始格式。
            if "date" in df.columns and df["date"].dtype != "object":
                df = df.copy()
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            frames.append(df)
        except Exception as e:
            logger.warning("Skipping %s: %s", f.name, e)
            skipped += 1
        if (i + 1) % 500 == 0:
            logger.info("  read %d/%d files...", i + 1, len(files))

    if not frames:
        logger.warning("No valid frames for %s", cache_key)
        return

    combined = pd.concat(frames, ignore_index=True)
    elapsed = time.time() - t0

    out_path = BACKTEST_DIR / out_file
    logger.info("  combined: %d rows, %d unique stocks, %.1fs elapsed",
                len(combined), combined["stock_id"].nunique(), elapsed)
    logger.info("  skipped:  %d files (empty / invalid)", skipped)

    if dry_run:
        logger.info("  [dry-run] would write %s (%d rows)", out_path, len(combined))
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # Backup existing file before overwrite
        if out_path.exists():
            backup = out_path.with_suffix(".parquet.bak")
            out_path.replace(backup)
            logger.info("  backed up existing -> %s", backup.name)
        combined.to_parquet(out_path)
        size_mb = out_path.stat().st_size / (1024 * 1024)
        logger.info("  written: %s (%.1f MB)", out_path, size_mb)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--category", choices=["income", "balance", "cashflow", "revenue", "all"],
                    default="all")
    ap.add_argument("--dry-run", action="store_true",
                    help="只報告不寫檔")
    args = ap.parse_args()

    cat_map = {
        "income":   [("financial_statement",   "financials_income.parquet")],
        "balance":  [("balance_sheet",         "financials_balance.parquet")],
        "cashflow": [("cash_flows_statement",  "financials_cashflow.parquet")],
        "revenue":  [("month_revenue",         "financials_revenue.parquet")],
        "all":      MAPPINGS,
    }
    work = cat_map[args.category]

    if not FUND_CACHE_DIR.exists():
        logger.error("fundamental_cache dir not found: %s", FUND_CACHE_DIR)
        sys.exit(1)

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)

    for cache_key, out_file in work:
        aggregate_category(cache_key, out_file, dry_run=args.dry_run)

    logger.info("Done.")


if __name__ == "__main__":
    main()
