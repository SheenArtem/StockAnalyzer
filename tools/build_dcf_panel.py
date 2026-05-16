"""
build_dcf_panel.py -- 批量計算 DCF panel 並聚合為 parquet (2026-05-16 加)

Input sources (依優先序):
  --tickers 2330,2454,...     CLI 指定
  --from-strong (default)     讀 data/latest/strong_stocks_daily.json
  --from-file path.txt        每行一個 ticker

Output:
  data_cache/dcf_panels/*.json     individual cached panels (compute_panel 寫入)
  data/dcf_mos_panel.parquet       聚合 [stock_id, industry, sector_key, fy_end,
                                    spot, base_mos, bull_mos, bear_mos, wacc,
                                    fcf_base_bn, updated_at]
  stdout                            top-N 低估 (Base MOS > +20%) + top-N 高估

執行：python tools/build_dcf_panel.py
       python tools/build_dcf_panel.py --tickers 2330,2454,1101
       python tools/build_dcf_panel.py --from-file my_watchlist.txt

⚠️ FinMind 600 req/hr。每檔約 5 calls (cold) → 100 檔 ~500 calls 約 50min
   若已 cache → 1 call/檔 (spot refresh) → 100 檔約 1.5min
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools.dcf_calculator import compute_panel  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_PATH = REPO / "data" / "dcf_mos_panel.parquet"
STRONG_STOCKS_PATH = REPO / "data" / "latest" / "strong_stocks_daily.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _load_strong_stocks() -> list[str]:
    if not STRONG_STOCKS_PATH.exists():
        logger.warning("strong_stocks_daily.json not found at %s", STRONG_STOCKS_PATH)
        return []
    with open(STRONG_STOCKS_PATH, encoding="utf-8") as f:
        d = json.load(f)
    ids = [s["stock_id"] for s in d.get("twse_top", []) + d.get("tpex_top", [])]
    return ids


def _load_file(path: Path) -> list[str]:
    with open(path, encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]


def _panel_to_row(panel: dict) -> dict | None:
    if not panel.get("ok"):
        return None
    sc = {s["scenario"]: s for s in panel["scenarios"]}
    return {
        "stock_id": panel["stock_id"],
        "industry": panel.get("industry", ""),
        "sector_key": panel.get("sector_key", "default"),
        "fy_end": panel["fy_end"],
        "spot": panel["spot"],
        "bull_mos": sc.get("Bull", {}).get("MOS_vs_spot"),
        "base_mos": sc.get("Base", {}).get("MOS_vs_spot"),
        "bear_mos": sc.get("Bear", {}).get("MOS_vs_spot"),
        "bull_fair": sc.get("Bull", {}).get("FairValue_per_share"),
        "base_fair": sc.get("Base", {}).get("FairValue_per_share"),
        "bear_fair": sc.get("Bear", {}).get("FairValue_per_share"),
        "wacc": panel["wacc"]["WACC"],
        "fcf_base_bn": panel["fcf_base_bn"],
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def main():
    ap = argparse.ArgumentParser(description="批量算 DCF panel 聚合 parquet")
    ap.add_argument("--tickers", default=None, help="comma-sep 直接指定 e.g. 2330,2454")
    ap.add_argument("--from-strong", action="store_true",
                    help="(default) 讀 strong_stocks_daily.json")
    ap.add_argument("--from-file", default=None, help="從檔案讀 (每行一個 ticker)")
    ap.add_argument("--top-n", type=int, default=10, help="stdout 顯示前 N 個低估/高估 (default 10)")
    args = ap.parse_args()

    # 決定 tickers
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    elif args.from_file:
        tickers = _load_file(Path(args.from_file))
    else:
        tickers = _load_strong_stocks()
    if not tickers:
        logger.error("無 ticker 可跑")
        sys.exit(1)
    logger.info("批量算 DCF：%d 檔", len(tickers))

    rows, n_ok, n_skip = [], 0, 0
    t_start = time.time()
    for i, stk in enumerate(tickers, 1):
        try:
            panel = compute_panel(stk)
            if not panel.get("ok"):
                logger.info("[%d/%d] %s SKIP: %s", i, len(tickers), stk, panel.get("reason"))
                n_skip += 1
                continue
            row = _panel_to_row(panel)
            if row:
                rows.append(row)
                n_ok += 1
                logger.info("[%d/%d] %s OK sector=%s base_mos=%+.1f%%",
                            i, len(tickers), stk, row["sector_key"], row["base_mos"]*100)
        except Exception as e:
            logger.warning("[%d/%d] %s FAIL: %s", i, len(tickers), stk, e)
            n_skip += 1

    if not rows:
        logger.error("沒有任何成功 panel，退出")
        sys.exit(1)

    df = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    elapsed = time.time() - t_start
    logger.info("完成：%d OK / %d skip / 寫 %s (%.1fs)", n_ok, n_skip, OUT_PATH, elapsed)

    # stdout 顯示 top-N
    top_n = args.top_n
    df_und = df.sort_values("base_mos", ascending=False).head(top_n)
    df_ovr = df.sort_values("base_mos", ascending=True).head(top_n)

    def _disp(d: pd.DataFrame, title: str):
        print(f"\n=== {title} (top {len(d)}) ===")
        print(f"{'stock':<8} {'sector':<11} {'spot':>8} {'base_fair':>10} {'base_mos':>9} {'bull_mos':>9}")
        for _, r in d.iterrows():
            print(f"{r['stock_id']:<8} {r['sector_key']:<11} {r['spot']:>8.1f} "
                  f"{r['base_fair']:>10.1f} {r['base_mos']*100:>+8.1f}% {r['bull_mos']*100:>+8.1f}%")

    _disp(df_und, "Base MOS 最大（低估）")
    _disp(df_ovr, "Base MOS 最小（高估）")


if __name__ == "__main__":
    main()
