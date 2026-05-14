"""
強勢股週報 scorer (Phase 1, 2026-05-14)

設計：與日報 (`strong_stocks_daily.py`) 平行的「週度視角」scanner。
不重用 TechnicalAnalyzer (其 trigger_score 全部 daily-signal-based)，而是自寫
透明的 5 信號 + pct_rank universe-wide scoring。

⚠️ Informational tier:
  - 週度 scoring 未經 IC 驗證 → 不接 paper_trade / 出場邏輯
  - 累積 3-6 個月 (>= 13 週數據) 後跑 IC 驗證
  - Output JSON 標 informational_tier=true

排程: 週日 12:00 (詳 run_scanner_weekly.bat)
資料對齊: 本週週五收盤 (last trading day of the ISO week containing the scan date - 7d)

Scoring (universe-wide pct_rank, 0-100):
  30% weekly_change_pct rank      (本週漲幅)
  20% volume_ratio_5w rank        (本週均量 / 前 4 週均量)
  20% change_pct_13w rank         (13 週累積漲幅)
  15% is_52w_high binary          (本週高點 == 52 週新高)
  15% above_ma20w binary          (週 K 收 > MA20W)

Top 30 enrich (display only, not in score):
  - 5-day institutional aggregate (張)
  - 5-day margin net aggregate (張)
  - 5-day sbl sell aggregate (張)
  - primary_sector (3-layer fallback: manual / YT / TV industry)

Output: data/latest/strong_stocks_weekly.json

Usage:
  python tools/strong_stocks_weekly_screener.py
  python tools/strong_stocks_weekly_screener.py --top-n 20
  python tools/strong_stocks_weekly_screener.py --week 2026-W19   # 補跑指定週次
  python tools/strong_stocks_weekly_screener.py --skip-chip       # 跳過 5d 籌碼 enrich (debug)
  python tools/strong_stocks_weekly_screener.py --dry-run         # 不寫檔
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

logger = logging.getLogger(__name__)

OUT_PATH = REPO / "data" / "latest" / "strong_stocks_weekly.json"
PRICE_CACHE_DIR = REPO / "data_cache"

DEFAULT_TOP_N = 15
MIN_UNIVERSE_SIZE = 100  # health check 門檻 (本週數據過少 → 退出 fail loud)
MIN_WEEKS_HISTORY = 53   # 至少 53 週才能算 52 週新高 + 13 週累積

# Scoring weights (sum to 100)
W_WEEKLY_CHANGE = 30
W_VOL_RATIO_5W = 20
W_CHANGE_13W = 20
W_52W_HIGH = 15
W_ABOVE_MA20W = 15


# ============================================================
# Week window
# ============================================================
def resolve_week_window(anchor: datetime | None = None) -> tuple[datetime, datetime, str]:
    """從 scan 日期 (預設今日) 推算「上一個完整交易週」的週一/週五/ISO 週次標籤.

    Returns: (week_start_monday, week_end_friday, week_label "2026-W19")

    範例:
      anchor = 2026-05-17 (週日) → 上週是 5/12(Mon) - 5/16(Fri), label 2026-W20
      anchor = 2026-05-16 (週六) → 同上
      anchor = 2026-05-18 (週一) → 仍是上週 (本週才剛開始)
    """
    if anchor is None:
        anchor = datetime.now()
    # 找出 anchor 當週或之前的最近一個週五
    # anchor.weekday(): Mon=0..Sun=6, Fri=4
    days_since_friday = (anchor.weekday() - 4) % 7
    # 若 anchor 本身是週五，days_since_friday=0, 但可能盤未收 → 退到上週五
    if days_since_friday == 0 and anchor.hour < 14:
        days_since_friday = 7
    last_friday = (anchor - timedelta(days=days_since_friday)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    last_monday = last_friday - timedelta(days=4)
    iso_year, iso_week, _ = last_friday.isocalendar()
    label = f"{iso_year}-W{iso_week:02d}"
    return last_monday, last_friday, label


def parse_week_label(label: str) -> tuple[datetime, datetime, str]:
    """'2026-W19' -> (Mon, Fri, label) of that ISO week."""
    year_str, week_str = label.split("-W")
    iso_year, iso_week = int(year_str), int(week_str)
    # ISO week to Monday: use fromisocalendar (py 3.8+)
    monday = datetime.fromisocalendar(iso_year, iso_week, 1)
    friday = monday + timedelta(days=4)
    return monday, friday, label


# ============================================================
# Weekly signals from price cache
# ============================================================
def compute_weekly_signals(
    df: pd.DataFrame, week_end: datetime,
) -> dict[str, Any] | None:
    """從 daily OHLCV 算本週 5 個信號. df index 必須是 DatetimeIndex.

    Returns dict 或 None (data 不足).
    """
    if df.empty or "Close" not in df.columns or "Volume" not in df.columns:
        return None

    # 只看 week_end 當天或之前的資料 (避免 lookahead)
    df = df[df.index <= pd.Timestamp(week_end)].copy()
    if len(df) < MIN_WEEKS_HISTORY * 5:  # 概估 53 週 * 5 個交易日
        return None

    # Resample 日 → 週 K (週五對齊 -> W-FRI freq)
    week_df = df.resample("W-FRI").agg({
        "Open": "first", "High": "max", "Low": "min",
        "Close": "last", "Volume": "sum",
    }).dropna()
    if len(week_df) < MIN_WEEKS_HISTORY:
        return None

    # 把 week_end 對齊到 resample 後最近一個週五（可能差 0-4 天）
    last_week = week_df.iloc[-1]
    if len(week_df) < 2:
        return None
    prev_week = week_df.iloc[-2]

    weekly_change_pct = (last_week["Close"] / prev_week["Close"] - 1) * 100

    # 5w volume ratio: this week / mean(prev 4 weeks)
    prev4_vol_mean = week_df["Volume"].iloc[-5:-1].mean()
    if prev4_vol_mean <= 0:
        return None
    volume_ratio_5w = last_week["Volume"] / prev4_vol_mean

    # 13w cumulative return
    if len(week_df) < 14:
        return None
    close_13w_ago = week_df["Close"].iloc[-14]
    if close_13w_ago <= 0:
        return None
    change_pct_13w = (last_week["Close"] / close_13w_ago - 1) * 100

    # 52w new high (本週 high 是否等於 52 週最高)
    last_52w = week_df.iloc[-52:]
    is_52w_high = bool(last_week["High"] >= last_52w["High"].max() * 0.999)  # 允許 0.1% 誤差

    # MA20W (週 K MA20)
    ma20w = week_df["Close"].rolling(20).mean().iloc[-1]
    above_ma20w = bool(last_week["Close"] > ma20w) if pd.notna(ma20w) else False

    return {
        "weekly_change_pct": round(float(weekly_change_pct), 2),
        "volume_ratio_5w": round(float(volume_ratio_5w), 2),
        "change_pct_13w": round(float(change_pct_13w), 2),
        "is_52w_high": is_52w_high,
        "above_ma20w": above_ma20w,
        "week_close": round(float(last_week["Close"]), 2),
        "week_high": round(float(last_week["High"]), 2),
        "week_low": round(float(last_week["Low"]), 2),
        "week_volume": int(last_week["Volume"]),
    }


# ============================================================
# Scoring (universe-wide pct_rank)
# ============================================================
def score_universe(df: pd.DataFrame) -> pd.DataFrame:
    """加 weekly_trigger_score 與 score_breakdown 欄位.

    輸入: DataFrame，含 weekly_change_pct / volume_ratio_5w / change_pct_13w /
                       is_52w_high / above_ma20w
    輸出: 同 df + 6 個欄位
    """
    df = df.copy()

    # pct_rank: rank(pct=True) * 100, NaN 保持 NaN (不計分)
    df["rank_weekly_change"] = df["weekly_change_pct"].rank(pct=True) * 100
    df["rank_volume_ratio"] = df["volume_ratio_5w"].rank(pct=True) * 100
    df["rank_change_13w"] = df["change_pct_13w"].rank(pct=True) * 100

    # binary signals (0/1)
    df["bin_52w_high"] = df["is_52w_high"].astype(int)
    df["bin_above_ma20w"] = df["above_ma20w"].astype(int)

    # Weighted sum (handle NaN: fillna 0 in ranks but mark in breakdown)
    df["weekly_trigger_score"] = (
        df["rank_weekly_change"].fillna(0) * (W_WEEKLY_CHANGE / 100)
        + df["rank_volume_ratio"].fillna(0) * (W_VOL_RATIO_5W / 100)
        + df["rank_change_13w"].fillna(0) * (W_CHANGE_13W / 100)
        + df["bin_52w_high"] * W_52W_HIGH
        + df["bin_above_ma20w"] * W_ABOVE_MA20W
    ).round(1)

    return df


# ============================================================
# 5-day chip aggregate
# ============================================================
def fetch_5day_inst_aggregate(
    week_start: datetime, week_end: datetime,
) -> dict[str, int]:
    """5 個交易日累計三大法人 (張). 跨 TWSE+TPEX. Skip 缺資料日."""
    from tools.strong_stocks_daily import fetch_inst_for_date
    agg: dict[str, int] = {}
    cur = week_start
    days_fetched = 0
    while cur <= week_end:
        if cur.weekday() < 5:  # Mon-Fri only
            try:
                day_data = fetch_inst_for_date(cur.strftime("%Y-%m-%d"))
                for sid, net in day_data.items():
                    agg[sid] = agg.get(sid, 0) + net
                days_fetched += 1
            except Exception as e:
                logger.warning("inst fetch failed for %s: %s", cur, e)
        cur += timedelta(days=1)
    logger.info("5-day inst aggregate: %d tickers across %d trading days",
                len(agg), days_fetched)
    return agg


def fetch_5day_margin_aggregate(
    tpex_tickers: set[str], week_start: datetime, week_end: datetime,
) -> dict[str, int]:
    """5 日累計融資增減 (張)."""
    from tools.strong_stocks_daily import fetch_margin_for_date
    agg: dict[str, int] = {}
    cur = week_start
    while cur <= week_end:
        if cur.weekday() < 5:
            try:
                day = fetch_margin_for_date(tpex_tickers, cur.strftime("%Y-%m-%d"))
                for sid, net in day.items():
                    agg[sid] = agg.get(sid, 0) + net
            except Exception as e:
                logger.warning("margin fetch failed for %s: %s", cur, e)
        cur += timedelta(days=1)
    return agg


def fetch_5day_sbl_aggregate(
    tpex_tickers: set[str], week_start: datetime, week_end: datetime,
) -> dict[str, int]:
    """5 日累計借券賣出 (張)."""
    from tools.strong_stocks_daily import fetch_sbl_for_date
    agg: dict[str, int] = {}
    cur = week_start
    while cur <= week_end:
        if cur.weekday() < 5:
            try:
                day = fetch_sbl_for_date(tpex_tickers, cur.strftime("%Y-%m-%d"))
                for sid, n in day.items():
                    agg[sid] = agg.get(sid, 0) + n
            except Exception as e:
                logger.warning("sbl fetch failed for %s: %s", cur, e)
        cur += timedelta(days=1)
    return agg


# ============================================================
# Universe loader
# ============================================================
def load_universe() -> pd.DataFrame:
    """Reuse momentum_screener Stage 1 pre-filter → ~300-500 TW stocks.

    Returns DataFrame with: stock_id, name, market, close, change_pct, trading_value
    """
    from momentum_screener import MomentumScreener
    screener = MomentumScreener(config=None)
    df = screener.run_stage1_only(market='tw')
    if df is None or df.empty:
        raise RuntimeError("Universe empty - check TWSE/TPEX API + TradingView availability")
    return df


# ============================================================
# Main
# ============================================================
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--week", type=str, default=None,
                        help="ISO week label (e.g. 2026-W19), default: 上一個完整交易週")
    parser.add_argument("--skip-chip", action="store_true",
                        help="跳過 5-day 籌碼 enrich (debug)")
    parser.add_argument("--dry-run", action="store_true",
                        help="跑完印 Top 30 但不寫檔")
    parser.add_argument("--output", type=Path, default=OUT_PATH)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.week:
        week_start, week_end, week_label = parse_week_label(args.week)
    else:
        week_start, week_end, week_label = resolve_week_window()

    print(f"=== Strong Stocks Weekly Screener ===")
    print(f"  Week:       {week_label}")
    print(f"  Week start: {week_start.strftime('%Y-%m-%d')} (Mon)")
    print(f"  Week end:   {week_end.strftime('%Y-%m-%d')} (Fri)")
    print()

    # 1. Universe (~300-500)
    print("[Stage 1] Loading universe (momentum_screener stage 1 filter)...")
    universe_df = load_universe()
    print(f"  Universe: {len(universe_df)} stocks ({(universe_df['market']=='twse').sum()} TWSE / "
          f"{(universe_df['market']=='tpex').sum()} TPEX)")

    if len(universe_df) < MIN_UNIVERSE_SIZE:
        print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"  [ALERT] Universe too small: {len(universe_df)} (expected >= {MIN_UNIVERSE_SIZE})")
        print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return 3

    # 2. Per-ticker weekly signals
    print(f"[Stage 2] Computing weekly signals for {len(universe_df)} stocks...")
    rows = []
    records = universe_df.to_dict("records")  # avoid pandas itertuples name-shadow gotcha
    for i, row in enumerate(records):
        sid = str(row.get("stock_id", "")).strip()
        if not sid:
            continue
        fp = PRICE_CACHE_DIR / f"{sid}_price.csv"
        if not fp.exists():
            continue
        try:
            df = pd.read_csv(fp, index_col=0, parse_dates=True)
        except Exception:
            continue
        signals = compute_weekly_signals(df, week_end)
        if signals is None:
            continue
        rows.append({
            "stock_id": sid,
            "name": str(row.get("stock_name", "") or row.get("name", "") or ""),
            "market": row.get("market", "twse"),
            "change_pct_day": float(row.get("change_pct", 0) or 0),
            "trading_value": float(row.get("trading_value", 0) or 0),
            **signals,
        })
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(records)}] {len(rows)} valid")

    if len(rows) < MIN_UNIVERSE_SIZE:
        print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"  [ALERT] Valid signals too few: {len(rows)} (expected >= {MIN_UNIVERSE_SIZE})")
        print(f"  Likely cause: OHLCV cache stale or week_end too recent for resample")
        print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return 3

    sig_df = pd.DataFrame(rows)
    print(f"  Valid signals: {len(sig_df)} stocks (filtered out {len(universe_df)-len(sig_df)} for insufficient history)")

    # 3. Score universe-wide
    print("[Stage 3] Scoring universe-wide (pct_rank)...")
    sig_df = score_universe(sig_df)

    # 4. Split by market + Top N each
    twse_top = sig_df[sig_df["market"] == "twse"].nlargest(args.top_n, "weekly_trigger_score").to_dict("records")
    tpex_top = sig_df[sig_df["market"] == "tpex"].nlargest(args.top_n, "weekly_trigger_score").to_dict("records")
    print(f"  TWSE Top {args.top_n}: {[r['stock_id'] for r in twse_top[:5]]} ...")
    print(f"  TPEX Top {args.top_n}: {[r['stock_id'] for r in tpex_top[:5]]} ...")

    top_ids = {r["stock_id"] for r in twse_top + tpex_top}
    tpex_ids = {r["stock_id"] for r in tpex_top}

    # 5. Sector enrich
    print("[Stage 4] Sector enrich (3-layer fallback)...")
    from tools.strong_stocks_daily import load_sector_index
    sector_idx = load_sector_index()
    for r in twse_top + tpex_top:
        r["primary_sector"] = sector_idx.get(r["stock_id"], "")

    # 6. 5-day chip aggregate (skip if --skip-chip)
    inst_5d: dict[str, int] = {}
    margin_5d: dict[str, int] = {}
    sbl_5d: dict[str, int] = {}
    if not args.skip_chip:
        print("[Stage 5] 5-day institutional aggregate (TWSE+TPEX batch x 5 days)...")
        inst_5d = fetch_5day_inst_aggregate(week_start, week_end)
        print(f"  Institutional: {len(inst_5d)} tickers aggregated")

        print("[Stage 6] 5-day margin aggregate...")
        margin_5d = fetch_5day_margin_aggregate(tpex_ids, week_start, week_end)
        print(f"  Margin: {len(margin_5d)} tickers aggregated")

        print("[Stage 7] 5-day SBL aggregate...")
        sbl_5d = fetch_5day_sbl_aggregate(tpex_ids, week_start, week_end)
        print(f"  SBL: {len(sbl_5d)} tickers aggregated")
    else:
        print("[Stage 5-7] Skip chip (--skip-chip)")

    # 7. Attach chip to top 30
    for r in twse_top + tpex_top:
        sid = r["stock_id"]
        r["inst_net_5d_shares"] = inst_5d.get(sid)
        r["margin_net_5d_shares"] = margin_5d.get(sid)
        r["sbl_sell_5d_shares"] = sbl_5d.get(sid)

    # 8. Score breakdown for transparency
    for r in twse_top + tpex_top:
        r["score_breakdown"] = {
            "weekly_change_rank": round(r.get("rank_weekly_change", 0) or 0, 1),
            "volume_ratio_rank": round(r.get("rank_volume_ratio", 0) or 0, 1),
            "momentum_13w_rank": round(r.get("rank_change_13w", 0) or 0, 1),
            "is_52w_high_pts": W_52W_HIGH if r.get("is_52w_high") else 0,
            "above_ma20w_pts": W_ABOVE_MA20W if r.get("above_ma20w") else 0,
        }
        # Cleanup raw rank columns from output
        for k in ("rank_weekly_change", "rank_volume_ratio", "rank_change_13w",
                  "bin_52w_high", "bin_above_ma20w"):
            r.pop(k, None)

    # 9. Build output
    out = {
        "schema_version": 1,
        "report_type": "weekly",
        "scan_date": datetime.now().strftime("%Y-%m-%d"),
        "week_label": week_label,
        "week_start": week_start.strftime("%Y-%m-%d"),
        "week_end": week_end.strftime("%Y-%m-%d"),
        "ref_date": week_end.strftime("%Y-%m-%d"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "informational_tier": True,
        "informational_caveat": (
            "週度 scoring 尚未經 IC 驗證，僅供盤勢回顧探索；"
            "不接 paper_trade / step_a_engine 出場邏輯。"
            "累積 3-6 個月後將補 IC 驗證。"
        ),
        "scoring_weights": {
            "weekly_change_pct_rank": W_WEEKLY_CHANGE,
            "volume_ratio_5w_rank": W_VOL_RATIO_5W,
            "change_pct_13w_rank": W_CHANGE_13W,
            "is_52w_high_pts": W_52W_HIGH,
            "above_ma20w_pts": W_ABOVE_MA20W,
        },
        "universe_size": len(sig_df),
        "twse_top": twse_top,
        "tpex_top": tpex_top,
    }

    # 10. Write
    if args.dry_run:
        print("\n=== Top 5 TWSE (dry-run) ===")
        for r in twse_top[:5]:
            print(f"  {r['stock_id']} {r['name']:8s} | score={r['weekly_trigger_score']:5.1f} "
                  f"chg5d={r['weekly_change_pct']:+5.1f}% vol5w={r['volume_ratio_5w']:.2f}x "
                  f"52wH={r['is_52w_high']} MA20W={r['above_ma20w']}")
        print(f"\n[DRY-RUN] Not writing {args.output}")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] Wrote {args.output}")
    print(f"  twse_top={len(twse_top)}, tpex_top={len(tpex_top)}, "
          f"universe={len(sig_df)}, informational_tier=true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
