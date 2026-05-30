#!/usr/bin/env python
"""
refresh_foreign_holding.py -- 外資持股比 (ForeignHoldingRatio) 增量刷新 (23 檔)

build_systemic_chip_panel 的 Group A foreign_holding 只用 TW0050_FIXED_UNIVERSE
(23 檔) 的 data_cache/{sid}_shareholding_chip.csv。這些 CSV 原由 ChipAnalyzer 在
QM/Value 掃描中順手寫入，2026-05-23 掃描停掉 (56dcc6c) 後停更（實測 14/23 卡在
4/10，超過 aggregate_foreign_holding 的 ffill limit=30 -> foreign_holding 只剩
~9 檔有效）。本工具用 FinMind taiwan_stock_shareholding 增量刷這 23 檔。

僅 ~23 FinMind calls；外資持股比約週更，建議掛 run_tdcc_weekly.bat
（與 chip_history_dl margin/short_sale 同週更節奏）。

執行：python tools/refresh_foreign_holding.py
"""
import sys
import logging
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools"))
CACHE = REPO / "data_cache"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("refresh_foreign_holding")
logging.getLogger("FinMind").setLevel(logging.WARNING)


def main():
    from build_systemic_chip_panel import TW0050_FIXED_UNIVERSE
    from cache_manager import get_finmind_loader
    dl = get_finmind_loader()
    if dl is None:
        log.error("No FinMind loader (token missing?)")
        return

    today_str = pd.Timestamp.now().normalize().strftime("%Y-%m-%d")
    ok = fail = skipped = 0
    for sid in TW0050_FIXED_UNIVERSE:
        path = CACHE / f"{sid}_shareholding_chip.csv"
        start = "2026-01-01"
        existing = None
        if path.exists():
            try:
                existing = pd.read_csv(path, index_col=0)
                existing.index = pd.to_datetime(existing.index, errors='coerce')
                existing = existing[~existing.index.isna()]
                if len(existing):
                    start = (existing.index.max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            except Exception:
                existing = None
        if start > today_str:
            skipped += 1  # 已是最新
            continue
        try:
            raw = dl.taiwan_stock_shareholding(stock_id=sid, start_date=start,
                                               end_date=today_str)
        except Exception as e:
            fail += 1
            log.warning("fetch fail %s: %s", sid, repr(e)[:100])
            continue
        if raw is None or raw.empty or "ForeignInvestmentSharesRatio" not in raw.columns:
            skipped += 1  # 區間內無新資料（非失敗）
            continue
        new = raw[["date", "ForeignInvestmentSharesRatio"]].copy()
        new["date"] = pd.to_datetime(new["date"])
        new = new.set_index("date").rename(
            columns={"ForeignInvestmentSharesRatio": "ForeignHoldingRatio"})
        new = pd.to_numeric(new["ForeignHoldingRatio"], errors="coerce").dropna().to_frame()
        if new.empty:
            skipped += 1
            continue
        if existing is not None and "ForeignHoldingRatio" in existing.columns and len(existing):
            merged = pd.concat([existing[["ForeignHoldingRatio"]], new])
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        else:
            merged = new.sort_index()
        merged.index.name = "date"
        merged.to_csv(path)
        ok += 1
        log.info("  %s -> last %s (+%d rows)", sid, str(merged.index.max())[:10], len(new))

    log.info("Done: %d updated / %d skipped(fresh|no-new) / %d fail (of %d)",
             ok, skipped, fail, len(TW0050_FIXED_UNIVERSE))


if __name__ == "__main__":
    main()
