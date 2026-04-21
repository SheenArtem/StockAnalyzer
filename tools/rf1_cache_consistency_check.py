"""
rf1_cache_consistency_check.py
==============================
RF-1 一致性檢查：掃描 data_cache/fundamental_cache/ 與 data_cache/backtest/financials_*.parquet
的「最新資料日期」是否一致。偵測 VF-VC P3-a 類型的 drift（backfill 只寫一邊）。

邏輯：
  - 對每個 (stock_id, category) 抓 fundamental_cache 的 latest date
  - 對同樣 (stock_id, category) 抓 backtest/financials_*.parquet 的 latest date
  - 差距超過 --threshold-days（預設 45 天）就算 inconsistent

執行方式：
  python tools/rf1_cache_consistency_check.py                    # 全掃描
  python tools/rf1_cache_consistency_check.py --threshold-days 30
  python tools/rf1_cache_consistency_check.py --category revenue
  python tools/rf1_cache_consistency_check.py --fix              # 自動跑 aggregate 修復

輸出:
  - stdout: 彙總 + 前 20 檔 drift 列表
  - data_cache/rf1_drift_report.txt: 完整 drift 清單（每行 `stock_id category live_date bt_date gap_days`）

用途：
  - Scheduled scanner 前跑一次，及早發現 drift
  - Backfill 工具完成後 validate
  - Debug：VF-VC 類事件重現時定位範圍
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LIVE_DIR = ROOT / "data_cache" / "fundamental_cache"
BT_DIR = ROOT / "data_cache" / "backtest"

# (cache_key, backtest_filename)
MAPPINGS = {
    "income":    ("financial_statement",   "financials_income.parquet"),
    "balance":   ("balance_sheet",         "financials_balance.parquet"),
    "cashflow":  ("cash_flows_statement",  "financials_cashflow.parquet"),
    "revenue":   ("month_revenue",         "financials_revenue.parquet"),
}


def check_category(category: str, threshold_days: int) -> list[dict]:
    """Return list of {stock_id, live_date, bt_date, gap_days} for drift rows."""
    cache_key, bt_name = MAPPINGS[category]
    bt_path = BT_DIR / bt_name

    if not bt_path.exists():
        print(f"[WARN] {bt_path} 不存在，skip")
        return []

    # Load backtest aggregated
    bt = pd.read_parquet(bt_path)
    if 'date' not in bt.columns or 'stock_id' not in bt.columns:
        print(f"[WARN] {bt_name} schema 異常（無 date/stock_id），skip")
        return []
    bt['date'] = pd.to_datetime(bt['date'], errors='coerce')
    bt_latest = bt.dropna(subset=['date']).groupby('stock_id')['date'].max().to_dict()

    # Scan live per-stock
    drifts = []
    live_pattern = f"{cache_key}_*.parquet"
    files = sorted(LIVE_DIR.glob(live_pattern))
    print(f"[{category}] Scanning {len(files)} live files vs backtest ({len(bt_latest)} stocks)...")

    for f in files:
        sid = f.stem.replace(f"{cache_key}_", "")
        try:
            df_live = pd.read_parquet(f)
        except Exception:
            continue
        if df_live.empty or 'date' not in df_live.columns:
            continue
        df_live['date'] = pd.to_datetime(df_live['date'], errors='coerce')
        live_latest = df_live['date'].max()
        if pd.isna(live_latest):
            continue

        bt_latest_for_sid = bt_latest.get(sid)
        if bt_latest_for_sid is None:
            drifts.append({
                'category': category, 'stock_id': sid,
                'live_date': live_latest.strftime('%Y-%m-%d'),
                'bt_date': 'MISSING',
                'gap_days': 9999,
            })
            continue

        gap = abs((live_latest - bt_latest_for_sid).days)
        if gap > threshold_days:
            drifts.append({
                'category': category, 'stock_id': sid,
                'live_date': live_latest.strftime('%Y-%m-%d'),
                'bt_date': bt_latest_for_sid.strftime('%Y-%m-%d'),
                'gap_days': gap,
            })
    return drifts


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--category", choices=list(MAPPINGS) + ["all"], default="all")
    ap.add_argument("--threshold-days", type=int, default=45,
                    help="容忍 drift 天數（預設 45，超過就視為不一致）")
    ap.add_argument("--fix", action="store_true",
                    help="發現 drift 時自動跑 aggregate 修復")
    args = ap.parse_args()

    cats = list(MAPPINGS) if args.category == "all" else [args.category]
    all_drifts = []
    for c in cats:
        all_drifts.extend(check_category(c, args.threshold_days))

    print()
    print("=" * 70)
    print(f"RF-1 Consistency Check Summary (threshold={args.threshold_days}d)")
    print("=" * 70)
    by_cat = {}
    for d in all_drifts:
        by_cat.setdefault(d['category'], 0)
        by_cat[d['category']] += 1
    for c in cats:
        print(f"  {c:10s} drift count: {by_cat.get(c, 0)}")
    print(f"  TOTAL drift stocks: {len(all_drifts)}")

    if all_drifts:
        # Sort by gap_days desc
        sorted_drifts = sorted(all_drifts, key=lambda x: -x['gap_days'])
        print(f"\nTop 20 worst drifts:")
        print(f"  {'cat':10s} {'stock_id':10s} {'live_date':12s} {'bt_date':12s} {'gap_days':>10s}")
        for d in sorted_drifts[:20]:
            print(f"  {d['category']:10s} {d['stock_id']:10s} {d['live_date']:12s} "
                  f"{d['bt_date']:12s} {d['gap_days']:>10}")

        # Write full report
        report_path = ROOT / "data_cache" / "rf1_drift_report.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"RF-1 drift report (threshold={args.threshold_days}d)\n")
            f.write(f"Total: {len(all_drifts)} drift rows\n\n")
            for d in sorted_drifts:
                f.write(f"{d['stock_id']}\t{d['category']}\t{d['live_date']}\t"
                        f"{d['bt_date']}\t{d['gap_days']}\n")
        print(f"\nFull report: {report_path}")

        if args.fix:
            print("\n--fix 啟用，執行 aggregate_fundamental_cache.py...")
            subprocess.run([sys.executable, str(ROOT / "tools" / "aggregate_fundamental_cache.py")],
                           cwd=str(ROOT), check=True)
            print("Aggregate done. 建議重跑 consistency check 驗證。")
    else:
        print("\n[OK] All categories consistent!")

    return 0 if not all_drifts else 1


if __name__ == "__main__":
    sys.exit(main())
