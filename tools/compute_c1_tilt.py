"""
Mode D Layer 2 C1 tilt detector: 月營收 YoY 拐點 (近期從負轉正)

V3 實證 verdict: C 級 signal (alpha +0.9~2.0pp 兩段顯著，hit rate 52-55% right-skew)
→ 適合當 regime-conditional weak tilt，不適合獨立 entry signal。

Regime-conditional 設計 (based on V4' finding):
- AI era (TWII 近 12 月總報酬 > 20%): C1 tilt ON × 1.2 boost
- Pre-AI / bear / sideway: C1 tilt OFF

輸出:
  data/c1_tilt_flags.parquet: ticker × is_ai_era × c1_tilt_on × yoy_3m / yoy_1m
  (每月營收公布後執行更新，snapshot 用於 scanner lookup)

CLI:
  python tools/compute_c1_tilt.py              # 重算並寫檔
  python tools/compute_c1_tilt.py --check      # 只印統計不寫
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
REVENUE_PATH = REPO / "data_cache" / "backtest" / "financials_revenue.parquet"
TWII_CACHE = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
OUT_PATH = REPO / "data" / "c1_tilt_flags.parquet"


def detect_ai_era(ohlcv_path: Path = TWII_CACHE) -> tuple[bool, float]:
    """
    AI era detector: 近 12 個月台股 top 300 equal-weight 總報酬 > +20% → AI era ON.

    使用 top-300 equal-weight 代理 (同 market_regime_logger.py 做法)。
    ohlcv_tw.parquet 無 ^TWII index，需用成分股聚合。
    """
    if not ohlcv_path.exists():
        return False, 0.0

    df = pd.read_parquet(ohlcv_path)
    df["date"] = pd.to_datetime(df["date"])

    latest_date = df["date"].max()
    past_target = latest_date - pd.Timedelta(days=365)

    # 找最接近 past_target 且實際有資料的日期
    snap_today = df[df["date"] == latest_date].set_index("stock_id")["Close"]
    # 找 past_target 附近有資料的日期 (±5 天)
    past_window = df[(df["date"] >= past_target - pd.Timedelta(days=5)) &
                     (df["date"] <= past_target + pd.Timedelta(days=5))]
    if past_window.empty:
        return False, 0.0
    past_date = past_window["date"].min()
    snap_past = df[df["date"] == past_date].set_index("stock_id")["Close"]

    # Equal-weight 報酬 (取 top 300 以流動性 / 市值先後; 這裡簡單取 intersection 前 300)
    common = snap_today.index.intersection(snap_past.index)
    if len(common) < 100:
        return False, 0.0

    # 用近期成交量估 top 300
    recent = df[df["date"] > latest_date - pd.Timedelta(days=30)]
    avg_volume = recent.groupby("stock_id")["Volume"].mean()
    top300 = avg_volume.loc[avg_volume.index.isin(common)].nlargest(300).index

    ret_pct = (snap_today.loc[top300] / snap_past.loc[top300] - 1).mean() * 100

    return bool(ret_pct > 20.0), float(ret_pct)


def compute_c1_tilt(revenue_df: pd.DataFrame) -> pd.DataFrame:
    """
    對每個 ticker 計算 C1 tilt flag。

    C1 定義 (from V3 catalyst spec):
    - 近 3 個月的月營收 YoY 從負轉正
    - YoY(T-2) < -2% AND (YoY(T) > +2% OR YoY(T-1) > +2%)
    - ±2% dead-band 避免雜訊

    輸入 revenue_df cols: stock_id, date (月初), revenue
    """
    # revenue_year_growth 99% NaN，自算 YoY (lag 12)
    revenue_df = revenue_df.sort_values(["stock_id", "date"]).copy()
    revenue_df["revenue_yoy"] = (
        revenue_df.groupby("stock_id")["revenue"]
        .transform(lambda s: (s / s.shift(12) - 1) * 100)
    )
    # Outlier clip: 基期極小 (重組/停牌復工) 會產出 +inf / 數千 % YoY，視為離群剔除
    revenue_df.loc[revenue_df["revenue_yoy"].abs() > 500, "revenue_yoy"] = pd.NA
    import numpy as np
    revenue_df.loc[~np.isfinite(revenue_df["revenue_yoy"]), "revenue_yoy"] = pd.NA

    # 取每 ticker 最近 3 期
    latest = revenue_df.groupby("stock_id").tail(3).copy()
    latest["period_rank"] = latest.groupby("stock_id").cumcount()  # 0 = oldest of 3, 2 = newest

    # Pivot 成 ticker × period matrix
    wide = latest.pivot_table(
        index="stock_id", columns="period_rank", values="revenue_yoy", aggfunc="first"
    ).rename(columns={0: "yoy_m2", 1: "yoy_m1", 2: "yoy_m0"})

    # Flag: yoy_m2 < -2 AND (yoy_m0 > +2 OR yoy_m1 > +2)
    DEAD_BAND = 2.0
    wide["c1_tilt_on"] = (
        (wide["yoy_m2"] < -DEAD_BAND) &
        ((wide["yoy_m0"] > DEAD_BAND) | (wide["yoy_m1"] > DEAD_BAND))
    )

    wide = wide.reset_index()
    return wide[["stock_id", "yoy_m2", "yoy_m1", "yoy_m0", "c1_tilt_on"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="print stats only, don't write")
    args = ap.parse_args()

    # 1. AI era detector
    is_ai_era, ret_12m = detect_ai_era()
    print(f"TWII 12-month return: {ret_12m:+.1f}% → AI era: {is_ai_era}", file=sys.stderr)

    # 2. Load revenue and compute C1 tilt
    if not REVENUE_PATH.exists():
        print(f"ERROR: {REVENUE_PATH} not found", file=sys.stderr)
        sys.exit(1)

    rev = pd.read_parquet(REVENUE_PATH)
    print(f"Revenue panel: {len(rev)} rows, {rev['stock_id'].nunique()} tickers", file=sys.stderr)

    c1_df = compute_c1_tilt(rev)
    c1_df["is_ai_era"] = is_ai_era
    c1_df["twii_ret_12m"] = round(ret_12m, 2)
    c1_df["computed_at"] = datetime.now().isoformat(timespec="seconds")

    # 3. Stats
    total = len(c1_df)
    tilt_on = c1_df["c1_tilt_on"].sum()
    pct = tilt_on / total * 100 if total else 0
    print(f"C1 tilt ON: {tilt_on}/{total} tickers ({pct:.1f}%)", file=sys.stderr)

    if tilt_on > 0:
        sample = c1_df[c1_df["c1_tilt_on"]].sort_values("yoy_m0", ascending=False).head(10)
        print("\nTop 10 C1 tilt ON by latest YoY:", file=sys.stderr)
        for _, row in sample.iterrows():
            print(f"  {row['stock_id']}: yoy_m2={row['yoy_m2']:+.1f}% / "
                  f"yoy_m1={row['yoy_m1']:+.1f}% / yoy_m0={row['yoy_m0']:+.1f}%", file=sys.stderr)

    if args.check:
        return

    # 4. Write
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    c1_df.to_parquet(OUT_PATH, index=False)
    print(f"\nWritten: {OUT_PATH} ({OUT_PATH.stat().st_size/1024:.1f} KB)", file=sys.stderr)


if __name__ == "__main__":
    main()
