"""
宋分擇時 Signal #1 — 「對利空不反應」 event study

訊號論點（宋分框架）：
  真正強勢股對大盤利空不會跌，盤中拉回、隔日創高，是機構在承接的訊號。

實作三條獨立 signal（在大盤利空日 t 當天判讀個股）：
  S1 收紅           : 個股當日 ret = Close/PrevClose - 1 >= 0
  S2 盤中拉回       : (Open/PrevClose - 1) <= -1.0% 且 (Close/PrevClose - 1) >= -0.3%
  S3 T+1 創新高     : t+1 收盤 >= 包含 t 的過去 20 日 high
                     ⚠ 訊號確認在 t+1 收盤，trader 可行動最早是 t+2 開盤；
                     為避免 look-ahead bias，S3 forward 報酬以 t+1 為基準
                     （fwd_h = Close_{t+1+h} / Close_{t+1} - 1）。

Forward 報酬：
  S1/S2:  fwd_h = Close_{t+h} / Close_t - 1（trader 可在 t 收盤後決定）
  S3:     fwd_h = Close_{t+1+h} / Close_{t+1} - 1（避免吃到 breakout 當日漲幅）
  CAR = stock_ret - twii_ret，TWII 取同樣的時間窗對齊。

Regime split：
  - bull / bear  by TWII Close vs 200-day MA
  - vol regime   by TWII 20-day realized vol top/bottom 33%

Baseline 對照組：
  同一利空日「沒篩股」（所有 active 個股）平均 forward return。

過濾：
  - 排除日均成交量 < 200K 股
  - 排除股價 < 5 元
  - 不過濾財報日（簡化）

CLI:
  python tools/songfen_signal1_unrespond_to_negative.py \\
      --start 2015-01-05 --end 2026-04-13 --twii-threshold -1.0
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
OUT_CSV = ROOT / "reports/songfen_signal1_event_study.csv"
OUT_MD = ROOT / "reports/songfen_signal1_event_study.md"

FWD_HORIZONS = [1, 5, 10, 20]
MIN_PRICE = 5.0
MIN_AVG_VOL = 200_000  # 過去 20 日均量門檻
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
    # 200-day MA -> bull/bear regime
    out["twii_ma200"] = out["twii_close"].rolling(200, min_periods=120).mean()
    out["regime_bull"] = (out["twii_close"] >= out["twii_ma200"]).astype("Int8")
    # 20-day realized vol -> vol regime split (top 33% / mid / bottom 33%)
    out["twii_rv20"] = out["twii_ret"].rolling(20, min_periods=15).std()
    return out


def load_ohlcv() -> pd.DataFrame:
    df = pd.read_parquet(
        OHLCV, columns=["stock_id", "date", "Open", "Close", "High", "Volume"]
    )
    df["date"] = pd.to_datetime(df["date"])
    df["stock_id"] = df["stock_id"].astype(str)
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    df = df.dropna(subset=["Close", "Open"])
    return df


# ------------------------------------------------------------------ signal construction


def build_panel(ohlcv: pd.DataFrame, twii: pd.DataFrame) -> pd.DataFrame:
    """
    Build panel with all per-stock signals + forward returns vectorized.
    Output one row per (stock_id, date).
    """
    log(f"Build panel: {ohlcv['stock_id'].nunique()} stocks, {len(ohlcv):,} rows")

    g = ohlcv.groupby("stock_id", sort=False)

    ohlcv["prev_close"] = g["Close"].shift(1)
    ohlcv["stock_ret"] = ohlcv["Close"] / ohlcv["prev_close"] - 1
    ohlcv["open_gap"] = ohlcv["Open"] / ohlcv["prev_close"] - 1

    # 20-day rolling high (excl current day) — for S3 (next-day breakout, evaluated at t+1)
    ohlcv["high20_excl"] = g["High"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=15).max()
    )
    # 20-day avg volume (lookback excluding today)
    ohlcv["avg_vol20"] = g["Volume"].transform(
        lambda s: s.shift(1).rolling(LIQUIDITY_LOOKBACK, min_periods=15).mean()
    )

    # Forward closes via shift(-h) per stock
    for h in FWD_HORIZONS:
        ohlcv[f"close_t{h}"] = g["Close"].shift(-h)
        ohlcv[f"fwd_{h}d"] = ohlcv[f"close_t{h}"] / ohlcv["Close"] - 1

    # Forward HIGH for breakout check (t+1 close vs t (current) high20_excl_for_next)
    # S3: at day t+1, close >= rolling 20d high observed at t
    # We'll evaluate using close_t1 vs high20 measured AT t (which already excludes t)
    # Use high including day t up to t (since t+1 break-out wants close_t+1 >= max(High[t-19..t]))
    ohlcv["high20_incl_t"] = g["High"].transform(
        lambda s: s.rolling(20, min_periods=15).max()
    )

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
    Vectorized signal detection.
    Returns long-format trigger DataFrame with one row per (date, stock_id, signal).
    """
    thr = twii_threshold / 100.0
    bad_day = panel["twii_ret"] <= thr
    base = panel["liquid"] & bad_day & panel["prev_close"].notna()

    log(f"Bad TWII days (ret <= {twii_threshold}%): {bad_day.sum():,} stock-rows")
    log(f"After liquidity filter: {base.sum():,} stock-rows")

    triggers = []

    # S1: 收紅 stock_ret >= 0
    s1_mask = base & (panel["stock_ret"] >= 0)
    if s1_mask.any():
        df1 = panel.loc[s1_mask, ["date", "stock_id"]].copy()
        df1["signal"] = "S1_close_up"
        triggers.append(df1)
        log(f"S1 (close up) triggers: {len(df1):,}")

    # S2: 開低 -1% 但收盤 >= -0.3%
    s2_mask = (
        base
        & (panel["open_gap"] <= -0.01)
        & (panel["stock_ret"] >= -0.003)
    )
    if s2_mask.any():
        df2 = panel.loc[s2_mask, ["date", "stock_id"]].copy()
        df2["signal"] = "S2_intraday_recovery"
        triggers.append(df2)
        log(f"S2 (intraday recovery) triggers: {len(df2):,}")

    # S3: 「t+1 創新高」— 事件日 = t（利空日），以 t+1 收盤 >= rolling 20d high(t) 判定。
    # ⚠ 訊號確認時點是 t+1 收盤，因此 trader 進場最早在 t+2 開盤。為避免 look-ahead bias
    # （把 t→t+1 的突破日漲幅當成「未來報酬」），S3 的 entry_date 改為 t+1，後續 forward
    # return 從 t+1 close 起算。
    s3_mask = (
        base
        & panel["close_t1"].notna()
        & panel["high20_incl_t"].notna()
        & (panel["close_t1"] >= panel["high20_incl_t"])
    )
    if s3_mask.any():
        df3 = panel.loc[s3_mask, ["date", "stock_id"]].copy()
        df3["signal"] = "S3_t1_breakout"
        # S3 將 event_date 整體推後 1 個 trading day。為了維持與 panel join 的 key
        # 一致，先在這裡標一個欄位 _shift_days，在後續 attach_returns 時處理。
        df3["_shift_days"] = 1
        triggers.append(df3)
        log(f"S3 (t+1 breakout) triggers: {len(df3):,}")

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

    對 _shift_days==0 的 signal（S1/S2），fwd_h 直接從 panel 的預算欄位讀取（基準 = t close）。
    對 _shift_days==1 的 signal（S3），需要重算 fwd_h，基準改為 t+1 close。
    """
    # 沒有 shift 的部分（S1/S2）
    no_shift = triggers[triggers["_shift_days"] == 0].copy()
    cols = ["date", "stock_id"] + [f"fwd_{h}d" for h in FWD_HORIZONS] + [
        "twii_ret",
        "regime_bull",
        "twii_rv20",
        "Close",
    ]
    look = panel[cols]
    out_noshift = no_shift.merge(look, on=["date", "stock_id"], how="left")

    # Shifted 部分（S3, entry_date = t+1）
    shifted = triggers[triggers["_shift_days"] == 1].copy()
    if not shifted.empty:
        # 對每個 stock 取 t+1 的 close 當作 entry。我們已在 panel 算過 close_t1 / close_t5...
        # close_t{h} 是相對 t 的後 h 天 close。S3 entry = t+1 close = close_t1。
        # 因此 fwd_h(S3) = close_t{h+1} / close_t1 - 1。
        ext_cols = ["date", "stock_id", "Close", "close_t1", "close_t5", "close_t10",
                    "close_t20", "twii_ret", "regime_bull", "twii_rv20"]
        # 需要 close_t{h+1}，h in [1,5,10,20] -> 需要 close_t2/t6/t11/t21，panel 沒算到
        # 直接 join 出 panel 完整的 close_t1.. close_t21
        # 最直接方法：merge 整個 stock series，然後依 date+stock_id+offset 查 future close
        sub = shifted.merge(panel[["date", "stock_id", "Close", "close_t1"]],
                            on=["date", "stock_id"], how="left")
        # build per-(stock, date) full close series lookup
        panel_idx = panel.set_index(["stock_id", "date"]).sort_index()
        # 為了 vectorize，用 trading-day position lookup
        cal = panel.sort_values("date")["date"].drop_duplicates().reset_index(drop=True)
        cal_pos = pd.Series(np.arange(len(cal)), index=pd.to_datetime(cal.values))
        # 對每個 stock 取其 (date -> Close) 與該 stock 的 trading day index
        # 實作: 對 panel 加一欄 _trade_idx (per stock 的相對 index)，再 lookup 偏移
        panel2 = panel.sort_values(["stock_id", "date"]).copy()
        panel2["_idx"] = panel2.groupby("stock_id").cumcount()
        # 我們要取 (stock_id, date) -> _idx；S3 entry 在 t+1 = _idx+1，後 h 天 = _idx+1+h
        merge_keys = panel2[["stock_id", "date", "_idx", "Close"]]
        s3 = shifted.merge(merge_keys, on=["stock_id", "date"], how="left")
        # 為每個 stock 建立 idx -> Close 對照
        # 用一個 dict-of-arrays 加速
        per_stock = {sid: g.set_index("_idx")["Close"].to_dict()
                     for sid, g in merge_keys.groupby("stock_id")}

        def lookup_close(stock_id, idx):
            if pd.isna(idx):
                return np.nan
            return per_stock.get(stock_id, {}).get(int(idx), np.nan)

        # entry_close = close at idx+1
        s3["_entry_close"] = [
            lookup_close(sid, (i + 1) if not pd.isna(i) else np.nan)
            for sid, i in zip(s3["stock_id"].values, s3["_idx"].values)
        ]
        for h in FWD_HORIZONS:
            offset = h + 1  # 從 t+1 起算 h 天 -> idx + 1 + h
            future_closes = [
                lookup_close(sid, (i + offset) if not pd.isna(i) else np.nan)
                for sid, i in zip(s3["stock_id"].values, s3["_idx"].values)
            ]
            s3[f"fwd_{h}d"] = pd.Series(future_closes, index=s3.index) / s3["_entry_close"] - 1

        # 補上 regime / twii 欄位（regime label 仍以 t 為主，因為利空日是 t）
        s3 = s3.merge(panel[["date", "stock_id", "twii_ret",
                              "regime_bull", "twii_rv20"]],
                       on=["date", "stock_id"], how="left")
        s3 = s3.drop(columns=["_idx", "_entry_close"])
        # 對齊 columns 順序
        out_shifted = s3[out_noshift.columns.intersection(s3.columns).tolist()
                         + [c for c in s3.columns if c not in out_noshift.columns]]
    else:
        out_shifted = pd.DataFrame(columns=out_noshift.columns)

    # combine
    combined = pd.concat([out_noshift, out_shifted], ignore_index=True, sort=False)
    return combined


def attach_car(events: pd.DataFrame, panel: pd.DataFrame, twii: pd.DataFrame) -> pd.DataFrame:
    """
    Compute CAR_h = stock_fwd_h - twii_fwd_h for each event.

    對於 _shift_days==0 (S1/S2): TWII fwd 取 t..t+h 區間
    對於 _shift_days==1 (S3):   TWII fwd 取 t+1..t+1+h 區間（與 stock entry 對齊）
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
    """tag vol regime based on TWII 20-day rv tertiles (computed over event day samples)."""
    rv = events["twii_rv20"]
    q33 = rv.quantile(1 / 3)
    q66 = rv.quantile(2 / 3)
    label = pd.Series("vol_mid", index=events.index, dtype=object)
    label[rv <= q33] = "vol_low"
    label[rv >= q66] = "vol_high"
    return label


def grade_signal(stats_full: dict) -> str:
    """
    A: CAR_5d & CAR_10d mean > 2% AND |t| > 2 AND regime 不反向 (左 caller 處理)
    B: CAR > 1% AND |t| > 1.5
    C: CAR > 0 但 t 弱
    D: CAR <= 0 OR t 反向
    回傳的是 "before regime check" tier，最終 grade 由 caller 補 regime check
    """
    car5 = stats_full.get("car_5d", {})
    car10 = stats_full.get("car_10d", {})
    if not car5.get("n") or not car10.get("n"):
        return "D"
    m5, t5 = car5["mean"], car5["t_stat"]
    m10, t10 = car10["mean"], car10["t_stat"]
    if (m5 <= 0) or (m10 <= 0):
        return "D"
    if (m5 > 0.02) and (m10 > 0.02) and (abs(t5) > 2) and (abs(t10) > 2):
        return "A"
    if (m5 > 0.01) and (m10 > 0.01) and (abs(t5) > 1.5) and (abs(t10) > 1.5):
        return "B"
    return "C"


def stats_for_subset(subset: pd.DataFrame) -> dict:
    out = {}
    for h in FWD_HORIZONS:
        out[f"fwd_{h}d"] = stat_block(subset[f"fwd_{h}d"])
        out[f"car_{h}d"] = stat_block(subset[f"car_{h}d"])
    return out


# ------------------------------------------------------------------ baseline (counterfactual)


def baseline_per_bad_day(panel: pd.DataFrame, twii_threshold: float) -> pd.DataFrame:
    """
    Baseline = 利空日當天 ALL liquid stocks 的 forward returns（沒篩 signal）。
    用同樣的 fwd / car 計算口徑。
    """
    thr = twii_threshold / 100.0
    base = panel["liquid"] & (panel["twii_ret"] <= thr) & panel["prev_close"].notna()
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
    lines.append("# Signal #1 「對利空不反應」 Event Study")
    lines.append("")
    lines.append(f"- 期間: {args.start} ~ {args.end}")
    lines.append(f"- TWII 利空門檻: {args.twii_threshold}% (close-to-close)")
    lines.append(f"- 樣本過濾: 收盤價 >= {MIN_PRICE} 元、20 日均量 >= {MIN_AVG_VOL:,} 股")
    lines.append(f"- Forward horizons: {FWD_HORIZONS}")
    lines.append(f"- 總 trigger 樣本數: {len(triggers_df):,}")
    lines.append("")

    # baseline
    lines.append("## Baseline 對照組（利空日所有 liquid 個股，沒篩 signal）")
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
    lines.append("## Regime Breakdown（CAR_10d mean / t-stat / n）")
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

    # final verdict
    lines.append("## 最終判級（含 regime 反向檢查）")
    lines.append("")
    for sig, blocks in overall.items():
        base = blocks["grade"]
        regime_block = regime.get(sig, {})
        bull_car = regime_block.get("bull", {}).get("car_10d", {})
        bear_car = regime_block.get("bear", {}).get("car_10d", {})
        notes = []
        if bull_car and bull_car.get("n", 0) >= 50 and bull_car.get("mean", 0) <= 0:
            notes.append("bull regime CAR<=0")
            base = "D"
        if bear_car and bear_car.get("n", 0) >= 50 and bear_car.get("mean", 0) < 0:
            notes.append("bear regime CAR<0 (regime 反向)")
            if base == "A":
                base = "B"
        baseline_car10 = baseline_stats["car_10d"]["mean"]
        if blocks["car_10d"]["mean"] is not None and not np.isnan(blocks["car_10d"]["mean"]):
            if baseline_car10 is not None and not np.isnan(baseline_car10):
                edge = blocks["car_10d"]["mean"] - baseline_car10
                edge_note = f"vs baseline CAR_10d edge = {fmt_pct(edge)}"
                notes.append(edge_note)
                if blocks["car_10d"]["mean"] > 0 and edge <= 0:
                    notes.append("baseline 已涵蓋此報酬，無 alpha")
                    base = "D"
        lines.append(f"- **{sig}**: {base} ({'; '.join(notes) if notes else 'no flags'})")
    lines.append("")

    # methodology footer
    lines.append("## 方法說明")
    lines.append("")
    lines.append(
        "- TWII 利空日：當日 close-to-close 報酬 <= 門檻；個股 forward 取 (Close_t+h / Close_t - 1)。"
    )
    lines.append(
        "- CAR = stock_fwd_h - twii_fwd_h；以同 horizon 區間 cumulative return 對齊。"
    )
    lines.append(
        "- Bull/Bear 以 TWII Close vs 200d MA 切；Vol Low/Mid/High 以樣本 TWII 20d realized vol 33/66 quantile 切。"
    )
    lines.append(
        "- Grade A 需 CAR_5d 與 CAR_10d 平均 >2% 且 |t|>2、regime 不反向；B 是 >1%、|t|>1.5；C 為正但 t 弱；D 為負或反向。"
    )
    lines.append(
        "- 警告：本研究只用大盤 proxy，未引入 news sentiment；若有 alpha，後續可加上 stock-level 利空新聞細分。"
    )
    lines.append("")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    return str(OUT_MD)


# ------------------------------------------------------------------ main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2015-01-05")
    ap.add_argument("--end", default="2026-04-13")
    ap.add_argument("--twii-threshold", type=float, default=-1.0,
                    help="TWII close-to-close return %% threshold for bad-day events")
    args = ap.parse_args()

    log("== Songfen Signal #1 event study ==")
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

    # vol regime tag for events
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
    bsub = baseline_per_bad_day(panel, args.twii_threshold).copy()
    # Need to compute CAR for baseline too
    log(f"Baseline subset rows (all liquid stocks on bad days): {len(bsub):,}")
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
