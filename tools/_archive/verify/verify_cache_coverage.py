"""
verify_cache_coverage.py - 通用 cache 覆蓋率驗證 + 重抓清單生成

對任何「universe × 分類」的批量抓資料作業（FinMind / MOPS / yfinance / 自製爬蟲）
提供統一的覆蓋率檢查：
  1. 掃 cache 目錄，判斷每檔股票是否齊全
  2. 分類標記：complete / partial / missing
  3. 產出重抓清單
  4. 支援 empty marker（避免真空資料反覆重抓）

用法範例:

  # 驗證 VF-L1a 基本面 backfill 覆蓋
  python tools/verify_cache_coverage.py \\
      --universe data_cache/vfl1a_universe.txt \\
      --cache-dir data_cache/fundamental_cache \\
      --pattern "{category}_{sid}.parquet" \\
      --categories financial_statement,balance_sheet,cash_flows_statement,month_revenue,dividend \\
      --out-prefix data_cache/vfl1a

  # 驗證 price cache（檔名模式不同）
  python tools/verify_cache_coverage.py \\
      --universe some_list.txt \\
      --cache-dir data_cache \\
      --pattern "{sid}_{category}.csv" \\
      --categories price,inst,margin,day_trading,shareholding

  # 標註已確認無資料的股票（下次跳過）
  python tools/verify_cache_coverage.py ... --mark-empty <sid>

輸出檔案:
  {out-prefix}_complete.txt  — 所有分類齊全
  {out-prefix}_partial.txt   — 缺 1-2 類（可能真空，低優先重抓）
  {out-prefix}_missing.txt   — 缺 3+ 類（建議重抓）
  {out-prefix}_report.json   — 詳細報告（每檔的缺失分類）

Empty markers:
  {cache-dir}/_empty_markers/{sid}.json — 該檔已確認整批真空，跳過
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path


def _load_universe(path: Path) -> list[str]:
    return [s.strip() for s in path.read_text().splitlines() if s.strip()]


def _load_empty_markers(empty_dir: Path) -> set[str]:
    """讀取 empty marker 目錄，回傳已標記「真空」的 stock_id 集合。"""
    if not empty_dir.exists():
        return set()
    markers = set()
    for f in empty_dir.glob("*.json"):
        markers.add(f.stem)
    return markers


def _check_stock_coverage(
    cache_dir: Path,
    pattern: str,
    categories: list[str],
    sid: str,
) -> tuple[list[str], list[str]]:
    """回傳 (present_categories, missing_categories)。"""
    present = []
    missing = []
    for cat in categories:
        filename = pattern.format(category=cat, sid=sid)
        p = cache_dir / filename
        if p.exists() and p.stat().st_size > 0:
            present.append(cat)
        else:
            missing.append(cat)
    return present, missing


def _write_list(path: Path, items: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(items) + ("\n" if items else ""))


def _mark_empty(empty_dir: Path, sid: str, reason: str = "verified empty") -> None:
    empty_dir.mkdir(parents=True, exist_ok=True)
    marker = empty_dir / f"{sid}.json"
    marker.write_text(json.dumps({
        "stock_id": sid,
        "marked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "reason": reason,
    }, ensure_ascii=False, indent=2))
    print(f"[OK] Marked {sid} as empty -> {marker}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--universe", required=True, type=Path,
                    help="股票清單檔（一行一個 stock_id）")
    ap.add_argument("--cache-dir", required=True, type=Path,
                    help="cache 目錄")
    ap.add_argument("--pattern", required=True,
                    help='檔名模板，使用 {category} 和 {sid}，例 "{category}_{sid}.parquet"')
    ap.add_argument("--categories", required=True,
                    help="預期分類（逗號分隔），例 financial_statement,balance_sheet,dividend")
    ap.add_argument("--out-prefix", default=None,
                    help="輸出檔案前綴（未指定則用 cache-dir 同目錄 + 'verify'）")
    ap.add_argument("--partial-threshold", type=int, default=None,
                    help="缺 N 類以上算 missing，其他算 partial（預設 = len(categories)/2 取整）")
    ap.add_argument("--mark-empty", metavar="SID", default=None,
                    help="把指定股票標註為 empty marker（跳過未來重抓），然後退出")
    ap.add_argument("--empty-dir", type=Path, default=None,
                    help="empty marker 目錄（預設 cache-dir/_empty_markers）")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    categories = [c.strip() for c in args.categories.split(",") if c.strip()]
    if not categories:
        print("[ERROR] No categories specified")
        sys.exit(1)

    empty_dir = args.empty_dir or (args.cache_dir / "_empty_markers")

    # Early exit: mark-empty mode
    if args.mark_empty:
        _mark_empty(empty_dir, args.mark_empty)
        return

    if not args.universe.exists():
        print(f"[ERROR] Universe file not found: {args.universe}")
        sys.exit(1)
    if not args.cache_dir.exists():
        print(f"[ERROR] Cache dir not found: {args.cache_dir}")
        sys.exit(1)

    # 驗證 pattern 合法
    test = args.pattern.format(category="cat", sid="sid")  # noqa: F841
    if "{category}" not in args.pattern or "{sid}" not in args.pattern:
        print(f"[ERROR] Pattern must contain {{category}} and {{sid}}: {args.pattern}")
        sys.exit(1)

    universe = _load_universe(args.universe)
    empty_markers = _load_empty_markers(empty_dir)
    n_cats = len(categories)

    # 預設 partial threshold: 缺超過一半視為 missing
    if args.partial_threshold is None:
        args.partial_threshold = (n_cats // 2) + 1  # e.g., 5 類 -> 缺 3+ 算 missing

    out_prefix = args.out_prefix or str(args.cache_dir.parent / "verify")

    complete, partial, missing = [], [], []
    empty_skipped = []
    detail_report = {}  # sid -> {"present": [...], "missing": [...]}
    missing_cat_counter = Counter()

    for sid in universe:
        if sid in empty_markers:
            empty_skipped.append(sid)
            continue

        present, miss = _check_stock_coverage(
            args.cache_dir, args.pattern, categories, sid,
        )
        detail_report[sid] = {"present": present, "missing": miss}
        for m in miss:
            missing_cat_counter[m] += 1

        if len(miss) == 0:
            complete.append(sid)
        elif len(miss) >= args.partial_threshold:
            missing.append(sid)
        else:
            partial.append(sid)

        if args.verbose and miss:
            print(f"  {sid}: present={present}, missing={miss}")

    # Write output files
    _write_list(Path(f"{out_prefix}_complete.txt"), complete)
    _write_list(Path(f"{out_prefix}_partial.txt"), partial)
    _write_list(Path(f"{out_prefix}_missing.txt"), missing)

    report_path = Path(f"{out_prefix}_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({
        "universe_size": len(universe),
        "categories": categories,
        "partial_threshold": args.partial_threshold,
        "summary": {
            "complete": len(complete),
            "partial": len(partial),
            "missing": len(missing),
            "empty_skipped": len(empty_skipped),
        },
        "missing_by_category": dict(missing_cat_counter),
        "detail": detail_report,
    }, ensure_ascii=False, indent=2))

    # Print summary
    print("=" * 60)
    print(f"Universe:           {len(universe)}")
    print(f"Categories:         {n_cats} ({', '.join(categories)})")
    print(f"Empty markers:      {len(empty_skipped)} (skipped)")
    print(f"  Complete:         {len(complete)} ({100*len(complete)/max(len(universe),1):.1f}%)")
    print(f"  Partial (<{args.partial_threshold} miss): {len(partial)} ({100*len(partial)/max(len(universe),1):.1f}%)")
    print(f"  Missing (>={args.partial_threshold} miss): {len(missing)} ({100*len(missing)/max(len(universe),1):.1f}%)")
    print()
    print("Missing-by-category (候選 retry 分類):")
    for cat, cnt in missing_cat_counter.most_common():
        print(f"  {cat:<30} {cnt}")
    print()
    print(f"Output files:")
    print(f"  {out_prefix}_complete.txt")
    print(f"  {out_prefix}_partial.txt")
    print(f"  {out_prefix}_missing.txt")
    print(f"  {out_prefix}_report.json")
    if missing:
        print(f"\n下次可以：")
        print(f"  python tools/backfill_fundamentals.py --universe {out_prefix}_missing.txt --source finmind")


if __name__ == "__main__":
    main()
