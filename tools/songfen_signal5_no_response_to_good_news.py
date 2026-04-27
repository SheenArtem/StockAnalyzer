"""
宋分擇時 Signal #5 -- 「好消息股價不推」 event study (出場訊號)

訊號論點 (宋分框架):
  正面 news + EPS beat 但股價平盤/下跌 = 最危險的 de-rate 前哨。
  「市場拒絕好消息」是機構在出貨/不再願意承接的訊號。
  本研究用大盤 proxy: TWII 大漲日當天個股不漲。

實作三條獨立 signal (在大盤利多日 t 當天判讀個股):
  S1 不漲           : 個股當日 ret = Close/PrevClose - 1 <= 0 (大盤大漲但個股翻黑)
  S2 盤中走弱       : (Open/PrevClose - 1) >= +1.0% AND (Close/PrevClose - 1) <= +0.3%
                     (從強拉回, 開高走低)
  S3 T+1 跌破       : t+1 收盤 <= 過去 20 日 low (隔日破底, 失守支撐)
                     entry_date 改為 t+1 close (避免 look-ahead bias, 學 #1 教訓)

Forward 報酬:
  S1/S2:  fwd_h = Close_{t+h} / Close_t - 1 (trader 在 t 收盤後決定減碼)
  S3:     fwd_h = Close_{t+1+h} / Close_{t+1} - 1 (避免吃到 breakdown 當日跌幅)
  CAR = stock_ret - twii_ret

判級 (出場訊號版, 期望 forward return 負):
  A: CAR_5d & CAR_10d 平均 < -2% AND |t| > 2 AND regime 不反向 -> 強烈減碼
  B: CAR < -1% AND |t| > 1.5
  C: CAR < 0 但 t 弱
  D: CAR >= 0 OR t 反向 -> 歸檔

⚠ 方向跟 #1 進場訊號相反 (進場期望 +CAR, 出場期望 -CAR)。

Regime split:
  - bull / bear  by TWII Close vs 200-day MA
  - vol regime   by TWII 20-day realized vol top/bottom 33%

Baseline 對照組:
  同一利多日「沒篩股」(所有 active 個股) 平均 forward return。

過濾:
  - 排除日均成交量 < 200K 股
  - 排除股價 < 5 元
  - 不過濾財報日 (簡化, 用大盤 proxy)

CLI:
  python tools/songfen_signal5_no_response_to_good_news.py \\
      --start 2015-01-05 --end 2026-04-21 --twii-threshold 1.0
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
OHLCV = ROOT / "data_cache/backtest/ohlcv_tw.parquet"
TWII_BENCH = ROOT / "data_cache/backtest/_twii_bench.parquet"
OUT_CSV = ROOT / "reports/songfen_signal5_event_study.csv"
OUT_MD = ROOT / "reports/songfen_signal5_event_study.md"

FWD_HORIZONS = [1, 5, 10, 20]
MIN_PRICE = 5.0
MIN_AVG_VOL = 200_000
LIQUIDITY_LOOKBACK = 20


def log(msg: str) -> None:
    print(f"[{pd.Timestamp.now():%H:%M:%S}] {msg}", flush=True)


# ------------------------------------------------------------------ data load


def load_twii() -> pd.DataFrame:
    df = pd.read_parquet(TWII_BENCH)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    out = pd.DataFrame(
        {
            "twii_close": df["Close"].astype(float),
            "twii_open": df["Open"].astype(float),
        }
    )
    out["twii_ret"] = out["twii_close"].pct_change()
    out["twii_ma200"] = out["twii_close"].rolling(200, min_periods=120).mean()
    out["regime_bull"] = (out["twii_close"] >= out["twii_ma200"]).astype("Int8")
    out["twii_rv20"] = out["twii_ret"].rolling(20, min_periods=15).std()
    return out


def load_ohlcv() -> pd.DataFrame:
    df = pd.read_parquet(
        OHLCV, columns=["stock_id", "date", "Open", "Close", "High", "Low", "Volume"]
    )
    df["date"] = pd.to_datetime(df["date"])
    df["stock_id"] = df["stock_id"].astype(str)
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    df = df.dropna(subset=["Close", "Open"])
    return df


# ------------------------------------------------------------------ signal construction


def build_panel(ohlcv: pd.DataFrame, twii: pd.DataFrame) -> pd.DataFrame:
    """Build panel with all per-stock signals + forward returns vectorized."""
    log(f"Build panel: {ohlcv['stock_id'].nunique()} stocks, {len(ohlcv):,} rows")

    g = ohlcv.groupby("stock_id", sort=False)

    ohlcv["prev_close"] = g["Close"].shift(1)
    ohlcv["stock_ret"] = ohlcv["Close"] / ohlcv["prev_close"] - 1
    ohlcv["open_gap"] = ohlcv["Open"] / ohlcv["prev_close"] - 1

    # 20-day rolling low (incl current day) for S3 (t+1 close vs 20d low at t)
    ohlcv["low20_incl_t"] = g["Low"].transform(
        lambda s: s.rolling(20, min_periods=15).min()
    )
    # 20-day avg volume (lookback excluding today)
    ohlcv["avg_vol20"] = g["Volume"].transform(
        lambda s: s.shift(1).rolling(LIQUIDITY_LOOKBACK, min_periods=15).mean()
    )

    # Forward closes via shift(-h) per stock
    for h in FWD_HORIZONS:
        ohlcv[f"close_t{h}"] = g["Close"].shift(-h)
        ohlcv[f"fwd_{h}d"] = ohlcv[f"close_t{h}"] / ohlcv["Close"] - 1

    # Merge TWII
    panel = ohlcv.merge(
        twii[["twii_ret", "regime_bull", "twii_rv20"]],
        left_on="date",
        right_index=True,
        how="left",
    )

    # Liquidity / price filter
    panel["liquid"] = (
        (panel["Close"] >= MIN_PRICE) & (panel["avg_vol20"] >= MIN_AVG_VOL)
    ).astype(bool)

    log(f"Panel built: {len(panel):,} rows, liquid rows: {panel['liquid'].sum():,}")
    return panel


def detect_signals(panel: pd.DataFrame, twii_threshold: float) -> pd.DataFrame:
    """
    Vectorized signal detection (mirror of #1 for good-news days).
    Returns long-format trigger DataFrame with one row per (date, stock_id, signal).
    """
    thr = twii_threshold / 100.0
    good_day = panel["twii_ret"] >= thr  # ⚠ 鏡像: 利多日 = TWII >= +1.0%
    base = panel["liquid"] & good_day & panel["prev_close"].notna()

    log(f"Good TWII days (ret >= {twii_threshold}%): {good_day.sum():,} stock-rows")
    log(f"After liquidity filter: {base.sum():,} stock-rows")

    triggers = []

    # S1: 不漲 stock_ret <= 0 (大盤大漲但個股翻黑/平)
    s1_mask = base & (panel["stock_ret"] <= 0)
    if s1_mask.any():
        df1 = panel.loc[s1_mask, ["date", "stock_id"]].copy()
        df1["signal"] = "S1_no_response"
        triggers.append(df1)
        log(f"S1 (no response) triggers: {len(df1):,}")

    # S2: 開高 +1% 但收盤 <= +0.3% (從強拉回)
    s2_mask = (
        base
        & (panel["open_gap"] >= 0.01)
        & (panel["stock_ret"] <= 0.003)
    )
    if s2_mask.any():
        df2 = panel.loc[s2_mask, ["date", "stock_id"]].copy()
        df2["signal"] = "S2_intraday_fade"
        triggers.append(df2)
        log(f"S2 (intraday fade) triggers: {len(df2):,}")

    # S3: t+1 收盤 <= 過去 20 日 low (隔日破底, 失守支撐)
    # 訊號確認在 t+1 收盤, entry_date = t+1 (避免 look-ahead bias)
    s3_mask = (
        base
        & panel["close_t1"].notna()
        & panel["low20_incl_t"].notna()
        & (panel["close_t1"] <= panel["low20_incl_t"])
    )
    if s3_mask.any():
        df3 = panel.loc[s3_mask, ["date", "stock_id"]].copy()
        df3["signal"] = "S3_t1_breakdown"
        df3["_shift_days"] = 1
        triggers.append(df3)
        log(f"S3 (t+1 breakdown) triggers: {len(df3):,}")

    if not triggers:
        return pd.DataFrame(
            columns=["date", "stock_id", "signal", "_shift_days"]
        )
    out = pd.concat(triggers, ignore_index=True)
    if "_shift_days" not in out.columns:
        out["_shift_days"] = 0
    out["_shift_days"] = out["_shift_days"].fillna(0).astype(int)
    return out


def attach_returns(triggers: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    """
    Merge forward returns + regime onto each trigger.

    對 _shift_days==0 (S1/S2): fwd_h 直接從 panel 讀取 (基準 = t close)
    對 _shift_days==1 (S3):    重算 fwd_h, 基準改為 t+1 close
    """
    no_shift = triggers[triggers["_shift_days"] == 0].copy()
    cols = ["date", "stock_id"] + [f"fwd_{h}d" for h in FWD_HORIZONS] + [
        "twii_ret",
        "regime_bull",
        "twii_rv20",
        "Close",
    ]
    look = panel[cols]
    out_noshift = no_shift.merge(look, on=["date", "stock_id"], how="left")

    # Shifted (S3, entry_date = t+1)
    shifted = triggers[triggers["_shift_days"] == 1].copy()
    if not shifted.empty:
        panel2 = panel.sort_values(["stock_id", "date"]).copy()
        panel2["_idx"] = panel2.groupby("stock_id").cumcount()
        merge_keys = panel2[["stock_id", "date", "_idx", "Close"]]
        s3 = shifted.merge(merge_keys, on=["stock_id", "date"], how="left")
        per_stock = {sid: g.set_index("_idx")["Close"].to_dict()
                     for sid, g in merge_keys.groupby("stock_id")}

        def lookup_close(stock_id, idx):
            if pd.isna(idx):
                return np.nan
            return per_stock.get(stock_id, {}).get(int(idx), np.nan)

        s3["_entry_close"] = [
            lookup_close(sid, (i + 1) if not pd.isna(i) else np.nan)
            for sid, i in zip(s3["stock_id"].values, s3["_idx"].values)
        ]
        for h in FWD_HORIZONS:
            offset = h + 1
            future_closes = [
                lookup_close(sid, (i + offset) if not pd.isna(i) else np.nan)
                for sid, i in zip(s3["stock_id"].values, s3["_idx"].values)
            ]
            s3[f"fwd_{h}d"] = pd.Series(future_closes, index=s3.index) / s3["_entry_close"] - 1

        s3 = s3.merge(panel[["date", "stock_id", "twii_ret",
                              "regime_bull", "twii_rv20"]],
                       on=["date", "stock_id"], how="left")
        s3 = s3.drop(columns=["_idx", "_entry_close"])
        out_shifted = s3[out_noshift.columns.intersection(s3.columns).tolist()
                         + [c for c in s3.columns if c not in out_noshift.columns]]
    else:
        out_shifted = pd.DataFrame(columns=out_noshift.columns)

    combined = pd.concat([out_noshift, out_shifted], ignore_index=True, sort=False)
    return combined


def attach_car(events: pd.DataFrame, panel: pd.DataFrame, twii: pd.DataFrame) -> pd.DataFrame:
    """
    Compute CAR_h = stock_fwd_h - twii_fwd_h.
    對 S3 (_shift_days==1): TWII fwd 從 t+1..t+1+h 對齊 (entry_pos shift +1)
    """
    cal = twii.sort_index()
    cal_idx = cal.index
    pos_map = pd.Series(np.arange(len(cal_idx)), index=cal_idx)
    twii_close = cal["twii_close"]

    events = events.copy()
    events["t_pos"] = events["date"].map(pos_map)
    if "_shift_days" not in events.columns:
        events["_shift_days"] = 0
    events["_shift_days"] = events["_shift_days"].fillna(0).astype(int)
    events["entry_pos"] = events["t_pos"] + events["_shift_days"]

    n_cal = len(cal_idx)

    for h in FWD_HORIZONS:
        future_pos = events["entry_pos"] + h
        valid_future = (future_pos < n_cal) & events["entry_pos"].notna()
        valid_entry = events["entry_pos"].notna() & (events["entry_pos"] < n_cal)
        future_close = pd.Series(np.nan, index=events.index)
        if valid_future.any():
            future_close.loc[valid_future] = twii_close.values[
                future_pos[valid_future].astype(int)
            ]
        base_close = pd.Series(np.nan, index=events.index)
        if valid_entry.any():
            base_close.loc[valid_entry] = twii_close.values[
                events.loc[valid_entry, "entry_pos"].astype(int)
            ]
        twii_fwd = future_close / base_close - 1
        events[f"twii_fwd_{h}d"] = twii_fwd
        events[f"car_{h}d"] = events[f"fwd_{h}d"] - twii_fwd

    events = events.drop(columns=["t_pos", "entry_pos"])
    return events


# ------------------------------------------------------------------ stats


def stat_block(values: pd.Series) -> dict:
    v = values.dropna()
    n = len(v)
    if n == 0:
        return {"n": 0, "mean": np.nan, "median": np.nan, "t_stat": np.nan, "p": np.nan}
    mean = v.mean()
    median = v.median()
    if n >= 2 and v.std(ddof=1) > 0:
        t_stat, p = stats.ttest_1samp(v, 0.0)
    else:
        t_stat, p = np.nan, np.nan
    return {"n": n, "mean": mean, "median": median, "t_stat": t_stat, "p": p}


def vol_regime_label(events: pd.DataFrame) -> pd.Series:
    rv = events["twii_rv20"]
    q33 = rv.quantile(1 / 3)
    q66 = rv.quantile(2 / 3)
    label = pd.Series("vol_mid", index=events.index, dtype=object)
    label[rv <= q33] = "vol_low"
    label[rv >= q66] = "vol_high"
    return label


def grade_signal(stats_full: dict) -> str:
    """
    出場訊號版 (期望 CAR < 0):
      A: CAR_5d & CAR_10d mean < -2% AND |t|>2 AND regime 不反向
      B: CAR < -1% AND |t|>1.5
      C: CAR < 0 但 t 弱
      D: CAR >= 0 OR t 反向
    """
    car5 = stats_full.get("car_5d", {})
    car10 = stats_full.get("car_10d", {})
    if not car5.get("n") or not car10.get("n"):
        return "D"
    m5, t5 = car5["mean"], car5["t_stat"]
    m10, t10 = car10["mean"], car10["t_stat"]
    if (m5 >= 0) or (m10 >= 0):
        return "D"
    if (m5 < -0.02) and (m10 < -0.02) and (abs(t5) > 2) and (abs(t10) > 2):
        return "A"
    if (m5 < -0.01) and (m10 < -0.01) and (abs(t5) > 1.5) and (abs(t10) > 1.5):
        return "B"
    return "C"


def stats_for_subset(subset: pd.DataFrame) -> dict:
    out = {}
    for h in FWD_HORIZONS:
        out[f"fwd_{h}d"] = stat_block(subset[f"fwd_{h}d"])
        out[f"car_{h}d"] = stat_block(subset[f"car_{h}d"])
    return out


# ------------------------------------------------------------------ baseline


def baseline_per_good_day(panel: pd.DataFrame, twii_threshold: float) -> pd.DataFrame:
    """Baseline = 利多日當天 ALL liquid stocks 的 forward returns (沒篩 signal)."""
    thr = twii_threshold / 100.0
    base = panel["liquid"] & (panel["twii_ret"] >= thr) & panel["prev_close"].notna()
    sub = panel.loc[base].copy()
    return sub


# ------------------------------------------------------------------ reporting


def fmt_pct(x: float) -> str:
    if pd.isna(x):
        return "n/a"
    return f"{x*100:+.2f}%"


def fmt_t(x: float) -> str:
    if pd.isna(x):
        return "n/a"
    return f"{x:+.2f}"


def write_report(
    overall: dict,
    regime: dict,
    baseline_stats: dict,
    triggers_df: pd.DataFrame,
    args: argparse.Namespace,
) -> str:
    lines = []
    lines.append("# Signal #5 「好消息股價不推」 Event Study (出場訊號)")
    lines.append("")
    lines.append(f"- 期間: {args.start} ~ {args.end}")
    lines.append(f"- TWII 利多門檻: {args.twii_threshold}% (close-to-close)")
    lines.append(f"- 樣本過濾: 收盤價 >= {MIN_PRICE} 元、20 日均量 >= {MIN_AVG_VOL:,} 股")
    lines.append(f"- Forward horizons: {FWD_HORIZONS}")
    lines.append(f"- 總 trigger 樣本數: {len(triggers_df):,}")
    lines.append("")
    lines.append("⚠ **方向跟 #1 進場訊號相反**: 出場訊號期望 forward CAR < 0 才有 alpha。")
    lines.append("")

    # baseline
    lines.append("## Baseline 對照組 (利多日所有 liquid 個股, 沒篩 signal)")
    lines.append("")
    lines.append("| Horizon | n | mean fwd | mean CAR | t-stat (CAR) |")
    lines.append("|---|---:|---:|---:|---:|")
    for h in FWD_HORIZONS:
        f_ = baseline_stats[f"fwd_{h}d"]
        c_ = baseline_stats[f"car_{h}d"]
        lines.append(
            f"| {h}d | {f_['n']:,} | {fmt_pct(f_['mean'])} | {fmt_pct(c_['mean'])} | {fmt_t(c_['t_stat'])} |"
        )
    lines.append("")

    # per-signal aggregate
    lines.append("## 各訊號彙整 (Full sample)")
    lines.append("")
    lines.append(
        "| Signal | n | fwd_5d | CAR_5d (t) | fwd_10d | CAR_10d (t) | fwd_20d | CAR_20d (t) | Grade |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    for sig, blocks in overall.items():
        n = blocks["fwd_5d"]["n"]
        lines.append(
            f"| {sig} | {n:,} "
            f"| {fmt_pct(blocks['fwd_5d']['mean'])} "
            f"| {fmt_pct(blocks['car_5d']['mean'])} ({fmt_t(blocks['car_5d']['t_stat'])}) "
            f"| {fmt_pct(blocks['fwd_10d']['mean'])} "
            f"| {fmt_pct(blocks['car_10d']['mean'])} ({fmt_t(blocks['car_10d']['t_stat'])}) "
            f"| {fmt_pct(blocks['fwd_20d']['mean'])} "
            f"| {fmt_pct(blocks['car_20d']['mean'])} ({fmt_t(blocks['car_20d']['t_stat'])}) "
            f"| {blocks['grade']} |"
        )
    lines.append("")

    # regime breakdown
    lines.append("## Regime Breakdown (CAR_10d mean / t-stat / n)")
    lines.append("")
    lines.append(
        "| Signal | Bull | Bear | Vol Low | Vol Mid | Vol High |"
    )
    lines.append("|---|---|---|---|---|---|")
    for sig, by in regime.items():
        cells = []
        for key in ["bull", "bear", "vol_low", "vol_mid", "vol_high"]:
            blk = by.get(key, {}).get("car_10d", {})
            if not blk or blk.get("n", 0) == 0:
                cells.append("n/a")
            elif blk["n"] < 50:
                cells.append(f"{fmt_pct(blk['mean'])} ({fmt_t(blk['t_stat'])}) n={blk['n']} [unsamp]")
            else:
                cells.append(f"{fmt_pct(blk['mean'])} ({fmt_t(blk['t_stat'])}) n={blk['n']}")
        lines.append(f"| {sig} | " + " | ".join(cells) + " |")
    lines.append("")

    # final verdict (出場訊號版)
    lines.append("## 最終判級 (含 regime 反向檢查)")
    lines.append("")
    for sig, blocks in overall.items():
        base = blocks["grade"]
        regime_block = regime.get(sig, {})
        bull_car = regime_block.get("bull", {}).get("car_10d", {})
        bear_car = regime_block.get("bear", {}).get("car_10d", {})
        notes = []
        # 出場訊號: bull regime 應該 CAR < 0; bear regime 應該 CAR < 0 (兩種 regime 都該負)
        if bull_car and bull_car.get("n", 0) >= 50 and bull_car.get("mean", 0) >= 0:
            notes.append("bull regime CAR>=0 (出場訊號失靈)")
            base = "D"
        if bear_car and bear_car.get("n", 0) >= 50 and bear_car.get("mean", 0) > 0:
            notes.append("bear regime CAR>0 (regime 反向)")
            if base == "A":
                base = "B"
        baseline_car10 = baseline_stats["car_10d"]["mean"]
        if blocks["car_10d"]["mean"] is not None and not np.isnan(blocks["car_10d"]["mean"]):
            if baseline_car10 is not None and not np.isnan(baseline_car10):
                edge = blocks["car_10d"]["mean"] - baseline_car10
                edge_note = f"vs baseline CAR_10d edge = {fmt_pct(edge)}"
                notes.append(edge_note)
                # 出場訊號: edge 應該負 (signal 樣本比 baseline 更弱)
                if blocks["car_10d"]["mean"] < 0 and edge >= 0:
                    notes.append("baseline 已涵蓋此弱勢, 無 alpha")
                    base = "D"
        lines.append(f"- **{sig}**: {base} ({'; '.join(notes) if notes else 'no flags'})")
    lines.append("")

    # methodology footer
    lines.append("## 方法說明")
    lines.append("")
    lines.append(
        "- TWII 利多日: 當日 close-to-close 報酬 >= 門檻; 個股 forward 取 (Close_t+h / Close_t - 1)。"
    )
    lines.append(
        "- CAR = stock_fwd_h - twii_fwd_h; 以同 horizon 區間 cumulative return 對齊。"
    )
    lines.append(
        "- S3 entry_date = t+1 (避免 look-ahead bias, 學 Signal #1 教訓)。"
    )
    lines.append(
        "- Bull/Bear 以 TWII Close vs 200d MA 切; Vol Low/Mid/High 以樣本 TWII 20d realized vol 33/66 quantile 切。"
    )
    lines.append(
        "- Grade A 需 CAR_5d 與 CAR_10d 平均 <-2% 且 |t|>2、regime 不反向; B 是 <-1%、|t|>1.5; C 為負但 t 弱; D 為正或反向。"
    )
    lines.append(
        "- 警告: 本研究只用大盤 proxy, 未引入 stock-level news/EPS beat sentiment; 若有 alpha, 後續可加上 EPS event window 細分。"
    )
    lines.append("")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    return str(OUT_MD)


# ------------------------------------------------------------------ main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2015-01-05")
    ap.add_argument("--end", default="2026-04-21")
    ap.add_argument("--twii-threshold", type=float, default=1.0,
                    help="TWII close-to-close return %% threshold for good-day events")
    args = ap.parse_args()

    log("== Songfen Signal #5 event study (no response to good news, 出場訊號) ==")
    log(f"args: {vars(args)}")

    twii = load_twii()
    twii = twii.loc[(twii.index >= args.start) & (twii.index <= args.end)]
    log(f"TWII rows: {len(twii):,}")

    ohlcv = load_ohlcv()
    ohlcv = ohlcv.loc[
        (ohlcv["date"] >= args.start) & (ohlcv["date"] <= args.end)
    ].copy()
    log(f"OHLCV rows in range: {len(ohlcv):,}, stocks: {ohlcv['stock_id'].nunique()}")

    panel = build_panel(ohlcv, twii)

    triggers = detect_signals(panel, args.twii_threshold)
    log(f"Total triggers across all signals: {len(triggers):,}")
    if triggers.empty:
        log("No triggers found, exiting.")
        sys.exit(0)

    events = attach_returns(triggers, panel)
    events = attach_car(events, panel, twii)

    events["vol_regime"] = vol_regime_label(events)
    events["regime_label"] = np.where(events["regime_bull"] == 1, "bull", "bear")

    # Save raw CSV
    keep_cols = [
        "date", "stock_id", "signal",
        "fwd_1d", "fwd_5d", "fwd_10d", "fwd_20d",
        "car_1d", "car_5d", "car_10d", "car_20d",
        "regime_label", "vol_regime",
    ]
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    events[keep_cols].to_csv(OUT_CSV, index=False)
    log(f"Wrote raw events: {OUT_CSV} ({len(events):,} rows)")

    # Aggregate stats per signal
    overall = {}
    for sig in events["signal"].unique():
        sub = events[events["signal"] == sig]
        blk = stats_for_subset(sub)
        blk["grade"] = grade_signal(blk)
        overall[sig] = blk
        log(f"{sig}: n={len(sub):,}, "
            f"CAR_5d={fmt_pct(blk['car_5d']['mean'])} (t={fmt_t(blk['car_5d']['t_stat'])}), "
            f"CAR_10d={fmt_pct(blk['car_10d']['mean'])} (t={fmt_t(blk['car_10d']['t_stat'])}), "
            f"grade={blk['grade']}")

    # Regime breakdown
    regime = {}
    for sig in events["signal"].unique():
        sub = events[events["signal"] == sig]
        regime[sig] = {
            "bull": stats_for_subset(sub[sub["regime_label"] == "bull"]),
            "bear": stats_for_subset(sub[sub["regime_label"] == "bear"]),
            "vol_low": stats_for_subset(sub[sub["vol_regime"] == "vol_low"]),
            "vol_mid": stats_for_subset(sub[sub["vol_regime"] == "vol_mid"]),
            "vol_high": stats_for_subset(sub[sub["vol_regime"] == "vol_high"]),
        }

    # Baseline
    bsub = baseline_per_good_day(panel, args.twii_threshold).copy()
    log(f"Baseline subset rows (all liquid stocks on good days): {len(bsub):,}")
    bsub["signal"] = "BASELINE"
    bsub_events = bsub[["date", "stock_id"] + [f"fwd_{h}d" for h in FWD_HORIZONS]
                       + ["twii_ret", "regime_bull", "twii_rv20"]].copy()
    bsub_events["signal"] = "BASELINE"
    bsub_events["_shift_days"] = 0
    bsub_events = attach_car(bsub_events, panel, twii)
    baseline_stats = stats_for_subset(bsub_events)
    log(f"Baseline CAR_5d mean = {fmt_pct(baseline_stats['car_5d']['mean'])} "
        f"(n={baseline_stats['car_5d']['n']:,}), "
        f"CAR_10d = {fmt_pct(baseline_stats['car_10d']['mean'])}")

    # Report
    out_md = write_report(overall, regime, baseline_stats, triggers, args)
    log(f"Wrote report: {out_md}")

    # Print summary
    print("\n== SUMMARY ==")
    for sig, blk in overall.items():
        print(f"{sig}: grade={blk['grade']}, "
              f"CAR_5d={fmt_pct(blk['car_5d']['mean'])} (t={fmt_t(blk['car_5d']['t_stat'])}, n={blk['car_5d']['n']:,}), "
              f"CAR_10d={fmt_pct(blk['car_10d']['mean'])} (t={fmt_t(blk['car_10d']['t_stat'])}), "
              f"CAR_20d={fmt_pct(blk['car_20d']['mean'])} (t={fmt_t(blk['car_20d']['t_stat'])})")
    print(f"BASELINE: CAR_5d={fmt_pct(baseline_stats['car_5d']['mean'])} "
          f"(n={baseline_stats['car_5d']['n']:,}), "
          f"CAR_10d={fmt_pct(baseline_stats['car_10d']['mean'])}, "
          f"CAR_20d={fmt_pct(baseline_stats['car_20d']['mean'])}")


if __name__ == "__main__":
    main()
