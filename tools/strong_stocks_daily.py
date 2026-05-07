"""
強勢股日報 — enrich + 分桶 + 嚴格日期對齊

讀 qm_result.json (or momentum_result.json)，補欄位後分桶輸出。

** 嚴格日期對齊 (schema v2) **：
  ref_date = OHLCV cache consensus latest date (top-N tickers 必須一致)。
  所有籌碼 fetcher 都用這個 ref_date 查詢，不會發生「price 是 D-1, 籌碼是 D」
  的時間錯位 (Robustness First)。OHLCV 不齊 → return 2 fail loud。

新增欄位（非破壞性）:
  - volume_ratio_5d:           今日量 / 前 5 日均量
  - is_abnormal_volume:        volume_ratio_5d >= 1.9
  - change_pct_5d:             近 5 日漲幅 %
  - inst_net_buy_today_shares: ref_date 法人合計買賣超 (張)
  - primary_sector:            sector 3 層 fallback (manual / YT / TV)
  - margin_net_today_shares:   ref_date 融資增減 (張) = 融資買進 - 融資賣出
  - day_trade_pct:             ref_date 當沖比 (%) = 當沖量 / 總成交量
  - sbl_sell_today_shares:     ref_date 借券賣出 (張，新空頭力道)

輸出 schema v2: data/latest/strong_stocks_daily.json
  - ref_date 欄位記錄所有資料對齊的交易日
  - 上市 Top 15 + 上櫃 Top 15 (按 trigger_score 排序)

Usage:
  python tools/strong_stocks_daily.py
  python tools/strong_stocks_daily.py --skip-inst    # 跳過法人 (debug)
  python tools/strong_stocks_daily.py --skip-chip    # 跳過融資/當沖/借券 (debug)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logger = logging.getLogger(__name__)

QM_PATH = REPO / "data" / "latest" / "qm_result.json"
MOMENTUM_PATH = REPO / "data" / "latest" / "momentum_result.json"
SECTOR_MANUAL_PATH = REPO / "data" / "sector_tags_manual.json"
SECTOR_DYNAMIC_PATH = REPO / "data" / "sector_tags_dynamic.parquet"
OUT_PATH = REPO / "data" / "latest" / "strong_stocks_daily.json"
PRICE_CACHE_DIR = REPO / "data_cache"

DEFAULT_TOP_N = 15
DEFAULT_ABNORMAL_VOL_THRESHOLD = 1.9
DYNAMIC_TAG_LOOKBACK_DAYS = 90  # 近 90 日 YT tag 才算數，避免過時題材


# TV industry (英文) → PDF 範本風格中文短籤. 用於 manual + YT 都缺漏時 fallback.
TV_INDUSTRY_CN = {
    "Semiconductors": "半導體",
    "Electronic Components": "電子零組件",
    "Electronic Equipment/Instruments": "電子設備",
    "Electronic Production Equipment": "半導體設備",
    "Computer Communications": "網通設備",
    "Computer Peripherals": "電腦周邊",
    "Computer Processing Hardware": "電腦硬體",
    "Telecommunications Equipment": "通訊設備",
    "Industrial Machinery": "工業機械",
    "Electrical Products": "電力設備",
    "Miscellaneous Manufacturing": "綜合製造",
    "Auto Parts: OEM": "車用零組件",
    "Motor Vehicles": "整車",
    "Chemicals: Specialty": "特化",
    "Chemicals: Major Diversified": "石化",
    "Pharmaceuticals: Major": "製藥",
    "Pharmaceuticals: Other": "製藥",
    "Biotechnology": "生技",
    "Medical Specialties": "醫材",
    "Steel": "鋼鐵",
    "Aluminum": "鋁",
    "Containers/Packaging": "塑化",
    "Building Products": "建材",
    "Construction Materials": "建材",
    "Major Banks": "金融",
    "Regional Banks": "金融",
    "Investment Banks/Brokers": "金融",
    "Life/Health Insurance": "保險",
    "Property/Casualty Insurance": "保險",
    "Apparel/Footwear": "紡織成衣",
    "Textiles": "紡織",
    "Food: Major Diversified": "食品",
    "Restaurants": "餐飲",
    "Air Freight/Couriers": "航空",
    "Marine Shipping": "航運",
    "Real Estate Development": "建設",
}


# ============================================================
# Sector resolver - 3-layer fallback
#   L1: sector_tags_manual.json (137 主流主題, 最準)
#   L2: sector_tags_dynamic.parquet (YT 即時題材, 過去 90d 出現次數最多 tag)
#   L3: TradingView industry → 中文 mapping (粗分類兜底)
# ============================================================
def _load_l1_manual() -> dict[str, str]:
    if not SECTOR_MANUAL_PATH.exists():
        return {}
    with SECTOR_MANUAL_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    idx: dict[str, str] = {}
    for theme in data.get("themes", []):
        name_zh = theme.get("theme_name_zh", "")
        for tier_key in ("tier1", "tier2"):
            for stock in theme.get(tier_key, []):
                t = str(stock.get("ticker", "")).strip()
                if t and t not in idx:
                    idx[t] = name_zh
    return idx


def _normalize_yt_tag(tag: str) -> str | None:
    """'其他 (CPU 連接器)' -> 'CPU 連接器'; '其他' -> None (太泛); 其他原樣回傳."""
    s = str(tag).strip()
    if not s or s == "其他":
        return None
    if s.startswith("其他"):
        # 抽括號內容，例: '其他 (CPU 連接器)' 或 '其他: 半導體微污染防治AMC'
        for sep in ("(", "（", ":", "：", " "):
            if sep in s:
                inner = s.split(sep, 1)[1].strip()
                inner = inner.rstrip(")）").strip()
                if inner:
                    return inner
        return None
    return s


def _load_l2_yt_dynamic() -> dict[str, str]:
    """ticker -> 出現最多次的 normalized tag (近 90 日)."""
    if not SECTOR_DYNAMIC_PATH.exists():
        return {}
    try:
        df = pd.read_parquet(SECTOR_DYNAMIC_PATH)
        df["date"] = pd.to_datetime(df["date"])
        cutoff = df["date"].max() - pd.Timedelta(days=DYNAMIC_TAG_LOOKBACK_DAYS)
        df = df[df["date"] >= cutoff]
    except Exception as e:
        logger.warning("YT sector_tags_dynamic load failed: %s", e)
        return {}

    from collections import Counter
    out: dict[str, str] = {}
    for ticker, group in df.groupby("ticker"):
        counter: Counter = Counter()
        for tags in group["tags"]:
            if tags is None:
                continue
            for t in tags:
                norm = _normalize_yt_tag(t)
                if norm:
                    counter[norm] += 1
        if counter:
            out[str(ticker)] = counter.most_common(1)[0][0]
    return out


def _load_l3_tv_industry() -> dict[str, str]:
    """ticker -> TV industry → 中文短籤. 透過 peer_comparison 共用 fetcher."""
    try:
        from peer_comparison import _fetch_tv_industry_map
        tv = _fetch_tv_industry_map()
    except Exception as e:
        logger.warning("TV industry map load failed: %s", e)
        return {}
    if tv is None or tv.empty:
        return {}
    out: dict[str, str] = {}
    for sid, row in tv.iterrows():
        ind = row.get("industry", "")
        zh = TV_INDUSTRY_CN.get(ind)
        if zh:
            out[str(sid)] = zh
    return out


def load_sector_index() -> dict[str, str]:
    """ticker -> primary sector name (zh). 3-layer fallback: manual > YT > TV industry."""
    l1 = _load_l1_manual()
    l2 = _load_l2_yt_dynamic()
    l3 = _load_l3_tv_industry()
    # Merge with priority: l1 wins, then l2, then l3
    merged: dict[str, str] = {}
    merged.update(l3)
    for k, v in l2.items():
        merged[k] = v
    for k, v in l1.items():
        merged[k] = v
    logger.info(
        "Sector index: L1 manual=%d, L2 YT=%d, L3 TV=%d, merged=%d",
        len(l1), len(l2), len(l3), len(merged),
    )
    return merged


# ============================================================
# Volume ratio + 5-day change from OHLCV cache
# ============================================================
def enrich_from_price_cache(stock_id: str) -> dict[str, Any]:
    """從 data_cache/{sid}_price.csv 算 volume_ratio_5d / change_pct_5d。
    缺資料給 None，不爆 exception。"""
    out = {"volume_ratio_5d": None, "change_pct_5d": None}
    fp = PRICE_CACHE_DIR / f"{stock_id}_price.csv"
    if not fp.exists():
        return out
    try:
        df = pd.read_csv(fp, index_col=0, parse_dates=True)
        if len(df) < 7:
            return out
        # Volume ratio: today / mean(prev 5 days)
        today_vol = float(df["Volume"].iloc[-1])
        prev5_vol = df["Volume"].iloc[-6:-1]
        avg5 = float(prev5_vol.mean()) if len(prev5_vol) > 0 else 0
        if avg5 > 0:
            out["volume_ratio_5d"] = round(today_vol / avg5, 2)
        # 5-day change: close[-1] / close[-6] - 1 (近 5 日漲幅)
        c_now = float(df["Close"].iloc[-1])
        c_5ago = float(df["Close"].iloc[-6])
        if c_5ago > 0:
            out["change_pct_5d"] = round((c_now / c_5ago - 1) * 100, 1)
    except Exception as e:
        logger.debug("enrich_from_price_cache(%s) failed: %s", stock_id, e)
    return out


# ============================================================
# Reference date (strict alignment with OHLCV cache)
# ------------------------------------------------------------
# All fetchers query the exact `ref_date` derived from OHLCV cache consensus
# of the top-N tickers. Mismatch between OHLCV cache and chip data → fail loud.
# ============================================================
def detect_ref_date(top_records: list[dict]) -> tuple[str | None, list[tuple[str, str]]]:
    """Return (consensus_date, mismatches).
    consensus_date = OHLCV cache 最新日期 across top-N (most-common).
    mismatches = list of (ticker, its_date) where date != consensus."""
    from collections import Counter
    detail: list[tuple[str, str]] = []
    for r in top_records:
        sid = str(r.get("stock_id"))
        fp = PRICE_CACHE_DIR / f"{sid}_price.csv"
        if not fp.exists():
            continue
        try:
            df = pd.read_csv(fp, index_col=0, parse_dates=True)
            if df.empty:
                continue
            d = df.index[-1].strftime("%Y-%m-%d")
            detail.append((sid, d))
        except Exception:
            continue
    if not detail:
        return None, []
    consensus = Counter(d for _, d in detail).most_common(1)[0][0]
    mismatches = [(sid, d) for sid, d in detail if d != consensus]
    return consensus, mismatches


# ============================================================
# Chip data for a specific date
# ------------------------------------------------------------
# Strategy: TWSE batch helpers from chip_history_dl + TPEX FinMind fallback.
# All fetchers take explicit ref_date (str, YYYY-MM-DD) — no "today's latest"
# magic, to avoid the data-misalignment trap (OHLCV from D-1 + chip from D).
# ============================================================
def fetch_margin_for_date(tpex_tickers: set[str], ref_date: str) -> dict[str, int]:
    """ticker -> 當日融資增減 (張) = margin_buy - margin_sell, for ref_date.
    TWSE batch + TPEX FinMind. 空回傳 → ref_date 資料未公布 (caller 處理)."""
    from datetime import datetime, timedelta
    out: dict[str, int] = {}
    dt = datetime.strptime(ref_date, "%Y-%m-%d")

    try:
        from tools.chip_history_dl import _fetch_margin_twse_one_day
        recs = _fetch_margin_twse_one_day(dt)
        for r in recs:
            sid = str(r.get("stock_id"))
            out[sid] = int(r.get("margin_buy", 0)) - int(r.get("margin_sell", 0))
    except Exception as e:
        logger.warning("margin TWSE fetch failed for %s: %s", ref_date, e)

    if tpex_tickers:
        try:
            from FinMind.data import DataLoader
            dl = DataLoader()
            for sid in tpex_tickers:
                try:
                    df = dl.taiwan_stock_margin_purchase_short_sale(
                        stock_id=sid, start_date=ref_date, end_date=ref_date)
                    if df is not None and not df.empty:
                        row = df.iloc[0]
                        out[sid] = int(row.get("MarginPurchaseBuy", 0)) - int(row.get("MarginPurchaseSell", 0))
                except Exception as e:
                    logger.debug("TPEX margin %s on %s failed: %s", sid, ref_date, e)
        except Exception as e:
            logger.warning("FinMind margin fallback init failed: %s", e)
    return out


def fetch_sbl_for_date(tpex_tickers: set[str], ref_date: str) -> dict[str, int]:
    """ticker -> 當日借券賣出 (張) for ref_date."""
    from datetime import datetime
    out: dict[str, int] = {}
    dt = datetime.strptime(ref_date, "%Y-%m-%d")

    try:
        from tools.chip_history_dl import _fetch_shortsale_twse_one_day
        recs = _fetch_shortsale_twse_one_day(dt)
        for r in recs:
            sid = str(r.get("stock_id"))
            # TWSE TWT93U sbl_sell 單位「股」÷1000 → 張
            out[sid] = int(r.get("sbl_sell", 0)) // 1000
    except Exception as e:
        logger.warning("sbl TWSE fetch failed for %s: %s", ref_date, e)

    if tpex_tickers:
        try:
            from FinMind.data import DataLoader
            dl = DataLoader()
            for sid in tpex_tickers:
                try:
                    df = dl.taiwan_stock_securities_lending(
                        stock_id=sid, start_date=ref_date, end_date=ref_date)
                    if df is not None and not df.empty:
                        row = df.iloc[0]
                        for col in ("sell", "transaction_volume", "volume"):
                            if col in row.index:
                                out[sid] = int(row.get(col, 0)) // 1000
                                break
                except Exception as e:
                    logger.debug("TPEX sbl %s on %s failed: %s", sid, ref_date, e)
        except Exception as e:
            logger.warning("FinMind sbl fallback init failed: %s", e)
    return out


def fetch_day_trade_for_date(tickers: list[str], ref_date: str) -> dict[str, float]:
    """ticker -> ref_date 當沖比 (%) = DayTrading.Volume / OHLCV.Volume.
    FinMind 逐檔；嚴格只取 ref_date，未公布就 N/A (不 fallback 前一日)。"""
    out: dict[str, float] = {}
    if not tickers:
        return out
    try:
        from FinMind.data import DataLoader
        dl = DataLoader()
    except Exception as e:
        logger.warning("FinMind day_trade init failed: %s", e)
        return out

    def _price_volume_on(sid: str, date_str: str) -> int | None:
        fp = PRICE_CACHE_DIR / f"{sid}_price.csv"
        if not fp.exists():
            return None
        try:
            pdf = pd.read_csv(fp, index_col=0, parse_dates=True)
            row = pdf.loc[pdf.index <= date_str].tail(1)
            if row.empty:
                return None
            return int(row["Volume"].iloc[0])
        except Exception:
            return None

    import time
    for sid in tickers:
        try:
            df = dl.taiwan_stock_day_trading(
                stock_id=sid, start_date=ref_date, end_date=ref_date)
            if df is None or df.empty:
                continue
            row = df.iloc[0]
            dt_vol = float(row.get("Volume", 0))
            if dt_vol <= 0:
                # ref_date 當沖資料還沒公布
                continue
            total_vol = _price_volume_on(sid, ref_date)
            if total_vol and total_vol > 0:
                out[sid] = round(dt_vol / total_vol * 100, 1)
        except Exception as e:
            logger.debug("day_trade %s on %s failed: %s", sid, ref_date, e)
        time.sleep(1.2)  # FinMind 600/hr safe interval
    return out


# ============================================================
# Institutional for a specific date
# ============================================================
def fetch_inst_for_date(ref_date: str) -> dict[str, int]:
    """ticker -> ref_date 三大法人合計買賣超 (張). TWSE T86 + TPEX 各一個 batch call."""
    from datetime import datetime
    dt = datetime.strptime(ref_date, "%Y-%m-%d")
    out: dict[str, int] = {}

    try:
        from tools.chip_history_dl import (
            _fetch_institutional_twse_one_day,
            _fetch_institutional_tpex_one_day,
        )
    except Exception as e:
        logger.warning("chip_history_dl import failed: %s", e)
        return out

    for fetcher, label in (
        (_fetch_institutional_twse_one_day, "TWSE T86"),
        (_fetch_institutional_tpex_one_day, "TPEX 3insti"),
    ):
        try:
            recs = fetcher(dt)
            for r in recs:
                sid = str(r.get("stock_id"))
                # total_net 單位「股」÷1000 → 張
                out[sid] = round(int(r.get("total_net", 0)) / 1000)
        except Exception as e:
            logger.warning("inst %s fetch failed for %s: %s", label, ref_date, e)
    logger.info("fetch_inst_for_date(%s): %d tickers", ref_date, len(out))
    return out


# ============================================================
# Enrich + bucket
# ============================================================
def enrich_record(
    rec: dict[str, Any],
    sector_idx: dict[str, str],
    inst_today: dict[str, int],
    margin_today: dict[str, int],
    sbl_today: dict[str, int],
    day_trade_today: dict[str, float],
    abnormal_vol_threshold: float,
) -> dict[str, Any]:
    """In-place enrich + return same dict."""
    sid = str(rec.get("stock_id", ""))
    # 1. Sector
    rec["primary_sector"] = sector_idx.get(sid, "")
    # 2. Volume ratio + 5d change
    px = enrich_from_price_cache(sid)
    rec["volume_ratio_5d"] = px["volume_ratio_5d"]
    rec["change_pct_5d"] = px["change_pct_5d"]
    rec["is_abnormal_volume"] = (
        rec["volume_ratio_5d"] is not None
        and rec["volume_ratio_5d"] >= abnormal_vol_threshold
    )
    # 3. Institutional today (張)
    rec["inst_net_buy_today_shares"] = inst_today.get(sid)
    # 4. Chip - margin 增減 (張)
    rec["margin_net_today_shares"] = margin_today.get(sid)
    # 5. Chip - 借券賣出 (張)
    rec["sbl_sell_today_shares"] = sbl_today.get(sid)
    # 6. Chip - 當沖比 (%)
    rec["day_trade_pct"] = day_trade_today.get(sid)
    return rec


def bucket_and_topn(
    records: list[dict[str, Any]], top_n: int
) -> dict[str, list[dict[str, Any]]]:
    """按 market 分桶 + 各取 Top N (按 trigger_score 排序，保留原 momentum_result 排序穩定性)."""
    buckets: dict[str, list[dict[str, Any]]] = {"twse": [], "tpex": []}
    for r in records:
        m = r.get("market", "twse")
        if m in buckets:
            buckets[m].append(r)
    for m in buckets:
        buckets[m].sort(
            key=lambda x: (x.get("trigger_score", 0), x.get("change_pct", 0)),
            reverse=True,
        )
        buckets[m] = buckets[m][:top_n]
    return buckets


# ============================================================
# Main
# ============================================================
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--top-n", type=int, default=DEFAULT_TOP_N,
        help=f"Top N per market (default {DEFAULT_TOP_N})",
    )
    parser.add_argument(
        "--abnormal-vol-threshold", type=float, default=DEFAULT_ABNORMAL_VOL_THRESHOLD,
        help=f"volume_ratio_5d threshold for is_abnormal_volume (default {DEFAULT_ABNORMAL_VOL_THRESHOLD})",
    )
    parser.add_argument(
        "--skip-inst", action="store_true",
        help="Skip institutional fetch (debug / offline mode)",
    )
    parser.add_argument(
        "--skip-chip", action="store_true",
        help="Skip margin / day_trade / sbl fetch (debug / offline mode)",
    )
    parser.add_argument(
        "--input", type=Path, default=None,
        help="Input scan result JSON. Default: qm_result.json if exists, else momentum_result.json.",
    )
    parser.add_argument(
        "--output", type=Path, default=OUT_PATH,
        help="Output strong_stocks_daily.json path",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Resolve default input: prefer qm_result.json (BAT 主鏈產出), fallback momentum.
    if args.input is None:
        if QM_PATH.exists():
            args.input = QM_PATH
        elif MOMENTUM_PATH.exists():
            args.input = MOMENTUM_PATH
        else:
            print(f"[ERROR] Neither {QM_PATH} nor {MOMENTUM_PATH} found. Run scanner first.",
                  file=sys.stderr)
            return 1
    elif not args.input.exists():
        print(f"[ERROR] {args.input} not found.", file=sys.stderr)
        return 1

    with args.input.open("r", encoding="utf-8") as f:
        momentum = json.load(f)

    results = momentum.get("results", [])
    if not results:
        print("[ERROR] momentum_result.json has no results", file=sys.stderr)
        return 1

    print(f"[INFO] Loaded {len(results)} momentum results (scan_date={momentum.get('scan_date')})")

    sector_idx = load_sector_index()

    # ----- Strict date alignment: detect ref_date from OHLCV cache consensus -----
    prelim_buckets = bucket_and_topn(results, args.top_n)
    target_tickers = [
        r["stock_id"] for r in prelim_buckets["twse"] + prelim_buckets["tpex"]
    ]
    tpex_targets = {r["stock_id"] for r in prelim_buckets["tpex"]}

    ref_date, mismatches = detect_ref_date(prelim_buckets["twse"] + prelim_buckets["tpex"])
    if ref_date is None:
        print("[ERROR] Cannot determine ref_date from OHLCV cache (no top-N tickers have cache).",
              file=sys.stderr)
        return 1
    print(f"[INFO] ref_date = {ref_date} (OHLCV cache consensus across top {len(target_tickers)})")
    if mismatches:
        print(f"[ERROR] OHLCV ref_date mismatch ({len(mismatches)}/{len(target_tickers)} tickers off):",
              file=sys.stderr)
        for sid, d in mismatches[:10]:
            print(f"  {sid}: {d} (expected {ref_date})", file=sys.stderr)
        print("[ERROR] Refusing to proceed - run scanner first to refresh OHLCV cache, "
              "or pass --allow-mismatch (not implemented).", file=sys.stderr)
        return 2

    # ----- Fetch chip data strictly for ref_date -----
    inst_today = {} if args.skip_inst else fetch_inst_for_date(ref_date)
    margin_today: dict[str, int] = {}
    sbl_today: dict[str, int] = {}
    day_trade_today: dict[str, float] = {}
    if not args.skip_chip:
        print(f"[INFO] Fetching chip data for {len(target_tickers)} target tickers on {ref_date}...")
        margin_today = fetch_margin_for_date(tpex_targets, ref_date)
        sbl_today = fetch_sbl_for_date(tpex_targets, ref_date)
        day_trade_today = fetch_day_trade_for_date(target_tickers, ref_date)
        print(
            f"[INFO] Chip coverage on {ref_date}: "
            f"inst={sum(1 for t in target_tickers if t in inst_today)}/{len(target_tickers)}, "
            f"margin={sum(1 for t in target_tickers if t in margin_today)}/{len(target_tickers)}, "
            f"sbl={sum(1 for t in target_tickers if t in sbl_today)}/{len(target_tickers)}, "
            f"day_trade={sum(1 for t in target_tickers if t in day_trade_today)}/{len(target_tickers)}"
        )

    for rec in results:
        enrich_record(
            rec, sector_idx, inst_today,
            margin_today, sbl_today, day_trade_today,
            args.abnormal_vol_threshold,
        )

    buckets = bucket_and_topn(results, args.top_n)

    out = {
        "schema_version": 2,  # 2 = ref_date alignment enforced
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scan_date": momentum.get("scan_date"),
        "scan_time": momentum.get("scan_time"),
        "ref_date": ref_date,  # 所有資料對齊的交易日 (OHLCV cache consensus)
        "abnormal_vol_threshold": args.abnormal_vol_threshold,
        "top_n_per_market": args.top_n,
        "twse_top": buckets["twse"],
        "tpex_top": buckets["tpex"],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(
        f"[OK] Wrote {args.output} "
        f"(twse={len(buckets['twse'])}, tpex={len(buckets['tpex'])}, "
        f"sector_covered={sum(1 for r in results if r.get('primary_sector'))}/{len(results)}, "
        f"inst_covered={sum(1 for r in results if r.get('inst_net_buy_today_shares') is not None)}/{len(results)})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
