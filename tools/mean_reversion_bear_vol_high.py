"""
Contrarian Mean-Reversion 訊號驗證 (bear + vol_high regime)
============================================================

來源: 宋分擇時 #5 「好消息股價不推」 event study (D 歸檔) 的副產物。
發現: bear + vol_high regime 三 signal CAR_10d +0.78 ~ +1.43%, t > 5 → 完全反向。

意涵:
  在 bear + vol_high regime 下, 大盤大漲 (TWII +1%+) 但個股「沒跟漲」,
  這些跟不上的弱勢股反而會 mean-revert 反彈。
  → contrarian buy signal (跟宋分原意完全相反)。

驗證設計 (4 stage):
  Stage 1: Confirm regime-conditional alpha (含 baseline 對照)
  Stage 2: Walk-forward stability (rolling IS/OOS windows)
  Stage 3: Cross-regime sanity (bull/bear × vol_low/mid/high 全 grid)
  Stage 4: Operationalization (signal frequency, tx cost, conflict check)

判級:
  A: bear+vol_high CAR_10d > +1% AND walk-forward sign-hit ≥ 60% AND
     cross-regime 對比清晰 AND 訊號頻率 > 10% days
  B: CAR > +0.5% but walk-forward 邊際, paper trade 累積後再決定
  C: aggregate 強但 walk-forward 不穩 OR 頻率太低 → watchlist 不上線
  D: aggregate 強但 walk-forward sign-hit < 60% OR cross-regime noise → 假象

Reuse:
  - reports/songfen_signal5_event_study.csv (169,560 events, 已含 regime label)
  - data_cache/backtest/ohlcv_tw.parquet (regime-conditional baseline)
  - data_cache/backtest/_twii_bench.parquet (TWII regime label)

CLI:
  python tools/mean_reversion_bear_vol_high.py \\
      --csv reports/songfen_signal5_event_study.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "reports/songfen_signal5_event_study.csv"
OHLCV = ROOT / "data_cache/backtest/ohlcv_tw.parquet"
TWII_BENCH = ROOT / "data_cache/backtest/_twii_bench.parquet"
OUT_CSV = ROOT / "reports/mean_reversion_bear_vol_high.csv"
OUT_MD = ROOT / "reports/mean_reversion_bear_vol_high.md"

FWD_HORIZONS = [1, 5, 10, 20]
TWII_THRESHOLD = 0.01  # +1.0% close-to-close
MIN_PRICE = 5.0
MIN_AVG_VOL = 200_000
LIQUIDITY_LOOKBACK = 20


def log(msg: str) -> None:
    print(f"[{pd.Timestamp.now():%H:%M:%S}] {msg}", flush=True)


# ---------------------------------------------------------------------- helpers


def t_stat(values: np.ndarray) -> float:
    v = values[~np.isnan(values)]
    if len(v) < 2:
        return np.nan
    se = v.std(ddof=1) / np.sqrt(len(v))
    if se == 0:
        return np.nan
    return v.mean() / se


def fmt_pct(x: float) -> str:
    if pd.isna(x):
        return "nan"
    return f"{x * 100:+.2f}%"


# ------------------------------------------------------------------ data loaders


def load_triggers(csv_path: Path) -> pd.DataFrame:
    log(f"Loading trigger CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])
    log(f"  Loaded {len(df):,} trigger rows")
    log(f"  Date range: {df['date'].min().date()} ~ {df['date'].max().date()}")
    return df


def load_twii() -> pd.DataFrame:
    df = pd.read_parquet(TWII_BENCH)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    out = pd.DataFrame(
        {
            "twii_close": df["Close"].astype(float),
        }
    )
    out["twii_ret"] = out["twii_close"].pct_change()
    out["twii_ma200"] = out["twii_close"].rolling(200, min_periods=120).mean()
    out["regime_bull"] = (out["twii_close"] >= out["twii_ma200"]).astype("Int8")
    out["twii_rv20"] = out["twii_ret"].rolling(20, min_periods=15).std()
    return out


def label_regimes(twii: pd.DataFrame) -> pd.DataFrame:
    """同 signal5 study: 用 sample 內 33/66 quantile 切 vol regime"""
    valid_rv = twii["twii_rv20"].dropna()
    q33 = valid_rv.quantile(1 / 3)
    q66 = valid_rv.quantile(2 / 3)
    twii = twii.copy()
    twii["regime_label"] = np.where(
        twii["regime_bull"] == 1, "bull",
        np.where(twii["regime_bull"] == 0, "bear", "unknown"),
    )
    twii["vol_regime"] = pd.cut(
        twii["twii_rv20"],
        bins=[-np.inf, q33, q66, np.inf],
        labels=["vol_low", "vol_mid", "vol_high"],
    ).astype(str)
    log(f"  Vol quantile thresholds: q33={q33:.4f}, q66={q66:.4f}")
    return twii


def load_baseline(twii: pd.DataFrame) -> pd.DataFrame:
    """
    從 OHLCV 建構同 regime cell 下「未觸發任何 signal」的全市場 baseline。
    過濾條件跟 signal5 一致 (price >= 5, avg_vol20 >= 200K)。
    """
    log("Loading OHLCV for baseline...")
    df = pd.read_parquet(
        OHLCV,
        columns=["stock_id", "date", "Open", "Close", "High", "Low", "Volume"],
    )
    df["date"] = pd.to_datetime(df["date"])
    df["stock_id"] = df["stock_id"].astype(str)
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    log(f"  OHLCV rows: {len(df):,}, stocks: {df['stock_id'].nunique()}")

    g = df.groupby("stock_id", sort=False)
    df["prev_close"] = g["Close"].shift(1)
    df["stock_ret"] = df["Close"] / df["prev_close"] - 1
    df["avg_vol20"] = g["Volume"].transform(
        lambda s: s.shift(1).rolling(LIQUIDITY_LOOKBACK, min_periods=15).mean()
    )

    # Forward returns
    for h in FWD_HORIZONS:
        df[f"close_t{h}"] = g["Close"].shift(-h)
        df[f"fwd_{h}d"] = df[f"close_t{h}"] / df["Close"] - 1

    # Merge TWII regime
    df = df.merge(
        twii[["twii_ret", "regime_label", "vol_regime"]],
        left_on="date",
        right_index=True,
        how="left",
    )

    # liquid + good_day filter (same as trigger universe)
    df["liquid"] = (df["Close"] >= MIN_PRICE) & (df["avg_vol20"] >= MIN_AVG_VOL)
    df["good_day"] = df["twii_ret"] >= TWII_THRESHOLD
    df["base_universe"] = df["liquid"] & df["good_day"] & df["prev_close"].notna()

    base = df.loc[df["base_universe"]].copy()
    log(f"  Baseline universe (liquid + good_day): {len(base):,} stock-rows")

    # CAR vs TWII (compute on baseline)
    twii_fwd_map = {}
    twii_close = twii["twii_close"]
    for h in FWD_HORIZONS:
        twii_fwd_map[h] = (twii_close.shift(-h) / twii_close - 1).rename(
            f"twii_fwd_{h}d"
        )
    twii_fwd_df = pd.concat(twii_fwd_map.values(), axis=1)
    base = base.merge(twii_fwd_df, left_on="date", right_index=True, how="left")
    for h in FWD_HORIZONS:
        base[f"car_{h}d"] = base[f"fwd_{h}d"] - base[f"twii_fwd_{h}d"]

    return base[["date", "stock_id", "regime_label", "vol_regime"]
                + [f"fwd_{h}d" for h in FWD_HORIZONS]
                + [f"car_{h}d" for h in FWD_HORIZONS]]


# ----------------------------------------------------------- analysis stages


def stage1_alpha(
    triggers: pd.DataFrame, baseline: pd.DataFrame
) -> tuple[pd.DataFrame, dict]:
    """Stage 1: bear + vol_high alpha vs same-regime baseline."""
    log("=" * 70)
    log("STAGE 1: regime-conditional alpha (bear + vol_high)")
    log("=" * 70)

    cell = (triggers["regime_label"] == "bear") & (
        triggers["vol_regime"] == "vol_high"
    )
    sub = triggers.loc[cell].copy()
    log(f"  bear+vol_high triggers: {len(sub):,}")

    base_cell = (baseline["regime_label"] == "bear") & (
        baseline["vol_regime"] == "vol_high"
    )
    base_sub = baseline.loc[base_cell].copy()
    log(f"  bear+vol_high baseline (liquid + good_day, no signal filter): {len(base_sub):,}")

    rows = []
    summary = {}

    # Per-signal stats
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        ssub = sub[sub["signal"] == sig]
        n = len(ssub)
        for h in FWD_HORIZONS:
            fwd = ssub[f"fwd_{h}d"].dropna().values
            car = ssub[f"car_{h}d"].dropna().values
            rows.append(
                {
                    "stage": "1_alpha",
                    "signal": sig,
                    "horizon": h,
                    "n": n,
                    "mean_fwd": np.nanmean(fwd) if len(fwd) else np.nan,
                    "mean_car": np.nanmean(car) if len(car) else np.nan,
                    "t_car": t_stat(car),
                    "median_car": np.nanmedian(car) if len(car) else np.nan,
                    "win_rate": (car > 0).mean() if len(car) else np.nan,
                }
            )
        summary[sig] = {
            "n": n,
            "car_5d": ssub["car_5d"].mean(),
            "t_5d": t_stat(ssub["car_5d"].values),
            "car_10d": ssub["car_10d"].mean(),
            "t_10d": t_stat(ssub["car_10d"].values),
            "car_20d": ssub["car_20d"].mean(),
            "t_20d": t_stat(ssub["car_20d"].values),
            "win10d": (ssub["car_10d"] > 0).mean(),
        }

    # Baseline (same regime, no signal filter)
    for h in FWD_HORIZONS:
        fwd = base_sub[f"fwd_{h}d"].dropna().values
        car = base_sub[f"car_{h}d"].dropna().values
        rows.append(
            {
                "stage": "1_alpha",
                "signal": "BASELINE_bear_volhigh",
                "horizon": h,
                "n": len(base_sub),
                "mean_fwd": np.nanmean(fwd) if len(fwd) else np.nan,
                "mean_car": np.nanmean(car) if len(car) else np.nan,
                "t_car": t_stat(car),
                "median_car": np.nanmedian(car) if len(car) else np.nan,
                "win_rate": (car > 0).mean() if len(car) else np.nan,
            }
        )
    summary["BASELINE"] = {
        "n": len(base_sub),
        "car_5d": base_sub["car_5d"].mean(),
        "car_10d": base_sub["car_10d"].mean(),
        "car_20d": base_sub["car_20d"].mean(),
        "t_10d": t_stat(base_sub["car_10d"].values),
    }

    log("--- Stage 1 conclusions ---")
    for sig, s in summary.items():
        if sig == "BASELINE":
            log(
                f"  BASELINE bear+volhigh n={s['n']:,}  "
                f"car_10d={fmt_pct(s['car_10d'])} t={s['t_10d']:.2f}"
            )
        else:
            log(
                f"  {sig:<20s} n={s['n']:>6,}  "
                f"car_5d={fmt_pct(s['car_5d'])} (t={s['t_5d']:+.2f})  "
                f"car_10d={fmt_pct(s['car_10d'])} (t={s['t_10d']:+.2f})  "
                f"car_20d={fmt_pct(s['car_20d'])} (t={s['t_20d']:+.2f})  "
                f"win10d={s['win10d']:.0%}"
            )

    # Edge over baseline
    log("--- Edge over BASELINE (bear+vol_high cell, CAR_10d) ---")
    base_car10 = summary["BASELINE"]["car_10d"]
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        edge = summary[sig]["car_10d"] - base_car10
        log(
            f"  {sig}: trigger={fmt_pct(summary[sig]['car_10d'])} "
            f"baseline={fmt_pct(base_car10)} edge={fmt_pct(edge)}"
        )

    return pd.DataFrame(rows), summary


def stage2_walkforward(triggers: pd.DataFrame) -> pd.DataFrame:
    """
    Stage 2: walk-forward windows, OOS sign-hit on bear+vol_high.

    Window 設計 (rolling 3yr IS / 1yr OOS):
      IS-1: 2015-2017 → OOS-1: 2018
      IS-2: 2016-2018 → OOS-2: 2019
      ...
      IS-7: 2021-2023 → OOS-7: 2024
      (2025-2026 不夠完整 1yr OOS, 留作 final hold-out)
    """
    log("=" * 70)
    log("STAGE 2: walk-forward stability (bear + vol_high only)")
    log("=" * 70)

    cell = (triggers["regime_label"] == "bear") & (
        triggers["vol_regime"] == "vol_high"
    )
    sub = triggers.loc[cell].copy()
    sub["year"] = sub["date"].dt.year

    rows = []
    windows = []
    # 製作 windows: IS = 3yr, OOS = next 1yr
    for start in range(2015, 2024):
        is_years = list(range(start, start + 3))
        oos_year = start + 3
        if oos_year > 2025:
            continue
        windows.append((is_years, oos_year))

    log(f"  Built {len(windows)} walk-forward windows")
    sign_hit_per_signal: dict[str, list[int]] = {
        s: [] for s in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]
    }

    for is_years, oos_year in windows:
        for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
            is_part = sub[(sub["signal"] == sig) & sub["year"].isin(is_years)]
            oos_part = sub[(sub["signal"] == sig) & (sub["year"] == oos_year)]
            is_car = is_part["car_10d"].dropna().values
            oos_car = oos_part["car_10d"].dropna().values
            row = {
                "stage": "2_walkforward",
                "signal": sig,
                "is_window": f"{is_years[0]}-{is_years[-1]}",
                "oos_year": oos_year,
                "is_n": len(is_car),
                "is_car10": np.nanmean(is_car) if len(is_car) else np.nan,
                "is_t": t_stat(is_car),
                "oos_n": len(oos_car),
                "oos_car10": np.nanmean(oos_car) if len(oos_car) else np.nan,
                "oos_t": t_stat(oos_car),
                "oos_sign_positive": int(np.nanmean(oos_car) > 0)
                if len(oos_car)
                else 0,
            }
            rows.append(row)
            if len(oos_car) > 0:
                sign_hit_per_signal[sig].append(int(np.nanmean(oos_car) > 0))

    log("--- OOS sign-hit by signal ---")
    summary = {}
    for sig, hits in sign_hit_per_signal.items():
        n_win = len(hits)
        rate = sum(hits) / n_win if n_win else np.nan
        summary[sig] = {"n_windows": n_win, "sign_hit": rate, "hits": hits}
        log(f"  {sig}: {sum(hits)}/{n_win} OOS years positive = {rate:.0%}")

    return pd.DataFrame(rows), summary


def stage3_cross_regime(triggers: pd.DataFrame) -> pd.DataFrame:
    """Stage 3: 5 regime cells × 3 signals = 15 cells, full grid CAR_10d."""
    log("=" * 70)
    log("STAGE 3: cross-regime sanity grid (CAR_10d)")
    log("=" * 70)

    cells = [
        ("bull", "vol_low"),
        ("bull", "vol_mid"),
        ("bull", "vol_high"),
        ("bear", "vol_low"),
        ("bear", "vol_mid"),
        ("bear", "vol_high"),
    ]

    rows = []
    log(f"  {'Signal':<22} | {'cell':<18} | {'n':>7} | {'CAR_10d':>9} | {'t':>7}")
    log(f"  {'-' * 22} | {'-' * 18} | {'-' * 7} | {'-' * 9} | {'-' * 7}")
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        for reg, vol in cells:
            mask = (
                (triggers["signal"] == sig)
                & (triggers["regime_label"] == reg)
                & (triggers["vol_regime"] == vol)
            )
            sub = triggers.loc[mask]
            car10 = sub["car_10d"].dropna().values
            n = len(car10)
            mean = np.nanmean(car10) if n else np.nan
            t = t_stat(car10) if n else np.nan
            rows.append(
                {
                    "stage": "3_cross_regime",
                    "signal": sig,
                    "regime": reg,
                    "vol_regime": vol,
                    "n": n,
                    "car_10d_mean": mean,
                    "t_car_10d": t,
                }
            )
            log(
                f"  {sig:<22} | {reg + '+' + vol:<18} | {n:>7,} | "
                f"{fmt_pct(mean):>9} | {t:>+7.2f}"
            )

    return pd.DataFrame(rows)


def stage4b_covid_strip_and_edge(
    triggers: pd.DataFrame, baseline: pd.DataFrame
) -> tuple[pd.DataFrame, dict]:
    """
    Stage 4b: COVID-strip robustness + edge-vs-baseline (核心 kill test)

    動機:
      Stage 1 baseline (bear+vol_high, 全市場無篩股) CAR_10d=+1.14% (t=41) 已是 mean-revert,
      訊號 incremental edge 才是真實 alpha。
      Stage 2 walk-forward 只有 4 window 有資料, 且 2020 COVID outlier 量級遠超其他年.
    """
    log("=" * 70)
    log("STAGE 4b: COVID-strip + edge vs same-regime baseline")
    log("=" * 70)

    cell = (triggers["regime_label"] == "bear") & (
        triggers["vol_regime"] == "vol_high"
    )
    sub_full = triggers.loc[cell].copy()
    sub_full["year"] = sub_full["date"].dt.year
    base_cell = (baseline["regime_label"] == "bear") & (
        baseline["vol_regime"] == "vol_high"
    )
    base_full = baseline.loc[base_cell].copy()
    base_full["year"] = base_full["date"].dt.year

    rows = []
    summary = {}

    log("--- Excluding 2020 (COVID outlier) ---")
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        ssub = sub_full[(sub_full["signal"] == sig) & (sub_full["year"] != 2020)]
        bsub = base_full[base_full["year"] != 2020]
        n = len(ssub)
        car10 = ssub["car_10d"].mean()
        t10 = t_stat(ssub["car_10d"].values)
        base_car10 = bsub["car_10d"].mean()
        edge = car10 - base_car10
        rows.append(
            {
                "stage": "4b_covid_strip",
                "signal": sig,
                "scope": "exclude_2020",
                "n": int(n),
                "trigger_car10": car10,
                "baseline_car10": base_car10,
                "edge_over_baseline": edge,
                "t_trigger": t10,
            }
        )
        summary[sig] = {
            "n_ex2020": int(n),
            "car10_ex2020": car10,
            "t_ex2020": t10,
            "base_car10_ex2020": base_car10,
            "edge_ex2020": edge,
        }
        log(
            f"  {sig:<22} n={n:>6,}  trigger={fmt_pct(car10)} "
            f"baseline={fmt_pct(base_car10)} edge={fmt_pct(edge)} t_trigger={t10:+.2f}"
        )

    log("--- Per-year edge vs baseline (CAR_10d) ---")
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        for yr in sorted(sub_full["year"].unique()):
            tsub = sub_full[(sub_full["signal"] == sig) & (sub_full["year"] == yr)]
            bsub_yr = base_full[base_full["year"] == yr]
            if len(tsub) < 30 or len(bsub_yr) < 30:
                continue
            t_car10 = tsub["car_10d"].mean()
            b_car10 = bsub_yr["car_10d"].mean()
            rows.append(
                {
                    "stage": "4b_covid_strip",
                    "signal": sig,
                    "scope": f"year_{yr}",
                    "n": int(len(tsub)),
                    "trigger_car10": t_car10,
                    "baseline_car10": b_car10,
                    "edge_over_baseline": t_car10 - b_car10,
                    "t_trigger": t_stat(tsub["car_10d"].values),
                }
            )

    return pd.DataFrame(rows), summary


def stage4_operationalization(
    triggers: pd.DataFrame, twii: pd.DataFrame
) -> pd.DataFrame:
    """Stage 4: regime detection feasibility, signal frequency, tx cost robustness."""
    log("=" * 70)
    log("STAGE 4: operationalization assessment")
    log("=" * 70)

    # 4a. Regime frequency: % of trading days in bear+vol_high
    twii_clean = twii.dropna(subset=["regime_label", "vol_regime"])
    n_total = len(twii_clean)
    bear_volhigh_days = (
        (twii_clean["regime_label"] == "bear")
        & (twii_clean["vol_regime"] == "vol_high")
    ).sum()
    bear_days = (twii_clean["regime_label"] == "bear").sum()
    volhigh_days = (twii_clean["vol_regime"] == "vol_high").sum()

    pct_bvh = bear_volhigh_days / n_total
    pct_bear = bear_days / n_total
    pct_volhigh = volhigh_days / n_total
    log(f"  Total trading days in TWII: {n_total:,}")
    log(f"  bear days: {bear_days:,} ({pct_bear:.1%})")
    log(f"  vol_high days: {volhigh_days:,} ({pct_volhigh:.1%})")
    log(f"  bear+vol_high days: {bear_volhigh_days:,} ({pct_bvh:.1%})")

    # 4b. Trigger frequency on those days
    cell = (triggers["regime_label"] == "bear") & (
        triggers["vol_regime"] == "vol_high"
    )
    sub = triggers.loc[cell]
    triggers_per_signal = sub.groupby("signal").size()
    unique_dates = sub["date"].nunique()
    log(f"  bear+vol_high active trigger dates: {unique_dates:,}")
    log("  triggers per signal:")
    for sig, n in triggers_per_signal.items():
        per_day = n / unique_dates if unique_dates else np.nan
        log(f"    {sig}: {n:,} triggers, ~{per_day:.1f} stocks/day")

    # 4c. Tx-cost robustness: CAR_10d - 0.2% (round-trip)
    log("  Tx-cost robustness (round-trip 0.2%):")
    rows = []
    rows.append(
        {
            "stage": "4_op",
            "metric": "regime_freq_total_days",
            "signal": "_global",
            "value": int(n_total),
        }
    )
    rows.append(
        {
            "stage": "4_op",
            "metric": "regime_freq_bear_pct",
            "signal": "_global",
            "value": pct_bear,
        }
    )
    rows.append(
        {
            "stage": "4_op",
            "metric": "regime_freq_volhigh_pct",
            "signal": "_global",
            "value": pct_volhigh,
        }
    )
    rows.append(
        {
            "stage": "4_op",
            "metric": "regime_freq_bear_volhigh_pct",
            "signal": "_global",
            "value": pct_bvh,
        }
    )
    rows.append(
        {
            "stage": "4_op",
            "metric": "active_trigger_dates",
            "signal": "_global",
            "value": int(unique_dates),
        }
    )

    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        ssub = sub[sub["signal"] == sig]
        n = len(ssub)
        if n == 0:
            continue
        car10 = ssub["car_10d"].mean()
        car10_net = car10 - 0.002  # 0.2% round-trip
        rows.append(
            {
                "stage": "4_op",
                "metric": "trigger_count_bear_volhigh",
                "signal": sig,
                "value": int(n),
            }
        )
        rows.append(
            {
                "stage": "4_op",
                "metric": "car_10d_gross",
                "signal": sig,
                "value": car10,
            }
        )
        rows.append(
            {
                "stage": "4_op",
                "metric": "car_10d_net_after_tx",
                "signal": sig,
                "value": car10_net,
            }
        )
        rows.append(
            {
                "stage": "4_op",
                "metric": "stocks_per_active_day",
                "signal": sig,
                "value": n / unique_dates if unique_dates else np.nan,
            }
        )
        log(
            f"    {sig}: gross={fmt_pct(car10)} → net={fmt_pct(car10_net)}"
        )

    # 4d. Year-by-year occurrence
    twii_clean = twii_clean.copy()
    twii_clean["year"] = twii_clean.index.year
    twii_clean["is_bvh"] = (
        (twii_clean["regime_label"] == "bear")
        & (twii_clean["vol_regime"] == "vol_high")
    ).astype(int)
    bvh_by_year = twii_clean.groupby("year")["is_bvh"].sum()
    log("  bear+vol_high days per year:")
    for y, d in bvh_by_year.items():
        log(f"    {y}: {d} days")
        rows.append(
            {
                "stage": "4_op",
                "metric": f"bvh_days_year",
                "signal": str(y),
                "value": int(d),
            }
        )

    return pd.DataFrame(rows)


# -------------------------------------------------------------- grading + report


def grade(
    stage1_summary: dict, stage2_summary: dict, stage4b_summary: dict
) -> tuple[str, str]:
    """
    Apply grading criteria (含 baseline edge + COVID-strip 雙 kill test).

    A: edge_ex2020 > +1% AND OOS sign-hit >= 60% AND t_ex2020 > 3
    B: edge_ex2020 > +0.5% AND OOS sign-hit >= 60% AND t_ex2020 > 2
    C: edge gross > 0 但 strip 後或 walk-forward 邊際
    D: edge_ex2020 <= 0 OR OOS sign-hit < 60% AND COVID outlier 主導
    """
    sigs = ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]
    grades = {}
    for sig in sigs:
        car10_full = stage1_summary[sig]["car_10d"]
        t10_full = stage1_summary[sig]["t_10d"]
        sign_hit = stage2_summary[sig]["sign_hit"]

        edge_ex2020 = stage4b_summary[sig]["edge_ex2020"]
        car10_ex2020 = stage4b_summary[sig]["car10_ex2020"]
        t_ex2020 = stage4b_summary[sig]["t_ex2020"]

        # 雙條件: 必須 (a) edge_ex2020 > 0 (b) walk-forward sign-hit OK
        if (
            edge_ex2020 > 0.01
            and sign_hit >= 0.6
            and t_ex2020 > 3
        ):
            grades[sig] = "A"
        elif (
            edge_ex2020 > 0.005
            and sign_hit >= 0.6
            and t_ex2020 > 2
        ):
            grades[sig] = "B"
        elif edge_ex2020 > 0.003 and sign_hit >= 0.6:
            # C: 邊際 alpha (0.3% ~ 0.5% edge), walk-forward 還行
            grades[sig] = "C"
        else:
            # edge 反向 / 太小 / walk-forward 不穩 → D 多重比較陷阱
            grades[sig] = "D"

    # Aggregate grade = best signal
    order = {"A": 4, "B": 3, "C": 2, "D": 1}
    best = max(grades.values(), key=lambda g: order[g])
    return best, str(grades)


def write_report(
    stage1_df: pd.DataFrame,
    stage1_summary: dict,
    stage2_df: pd.DataFrame,
    stage2_summary: dict,
    stage3_df: pd.DataFrame,
    stage4_df: pd.DataFrame,
    stage4b_df: pd.DataFrame,
    stage4b_summary: dict,
    overall_grade: str,
    per_sig_grades: str,
) -> None:
    lines = []
    lines.append("# Contrarian Mean-Reversion 訊號驗證 (bear + vol_high)")
    lines.append("")
    lines.append(f"- Overall grade: **{overall_grade}**")
    lines.append(f"- Per-signal grades: {per_sig_grades}")
    lines.append(
        "- 來源: 宋分擇時 #5 「好消息股價不推」event study 副產物 — bear+vol_high regime 反向發現"
    )
    lines.append(
        "- 訊號: 大盤 +1% 利多日, 個股「沒跟漲」(S1/S2/S3), 在 bear+vol_high regime 反而 mean-revert 反彈"
    )
    lines.append("")

    # ---- Stage 1
    lines.append("## Stage 1: bear+vol_high regime alpha vs baseline")
    lines.append("")
    lines.append("| Signal | n | CAR_5d | t | CAR_10d | t | CAR_20d | t | win_10d |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        s = stage1_summary[sig]
        lines.append(
            f"| {sig} | {s['n']:,} | {fmt_pct(s['car_5d'])} | {s['t_5d']:+.2f} | "
            f"{fmt_pct(s['car_10d'])} | {s['t_10d']:+.2f} | "
            f"{fmt_pct(s['car_20d'])} | {s['t_20d']:+.2f} | {s['win10d']:.0%} |"
        )
    b = stage1_summary["BASELINE"]
    lines.append(
        f"| BASELINE_bear_volhigh | {b['n']:,} | {fmt_pct(b['car_5d'])} | -- | "
        f"{fmt_pct(b['car_10d'])} | {b['t_10d']:+.2f} | "
        f"{fmt_pct(b['car_20d'])} | -- | -- |"
    )
    lines.append("")
    base_car10 = stage1_summary["BASELINE"]["car_10d"]
    lines.append("**Edge over baseline (CAR_10d, same regime cell):**")
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        edge = stage1_summary[sig]["car_10d"] - base_car10
        lines.append(
            f"- {sig}: trigger {fmt_pct(stage1_summary[sig]['car_10d'])} − baseline "
            f"{fmt_pct(base_car10)} = **{fmt_pct(edge)}**"
        )
    lines.append("")

    # ---- Stage 2
    lines.append("## Stage 2: walk-forward stability (rolling IS=3yr / OOS=1yr)")
    lines.append("")
    lines.append("| Signal | OOS years positive | sign-hit | verdict |")
    lines.append("|---|---|---:|---|")
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        s = stage2_summary[sig]
        verdict = (
            "穩定" if s["sign_hit"] >= 0.6 else "邊際" if s["sign_hit"] >= 0.5 else "不穩"
        )
        hits_str = ", ".join(["+" if h else "-" for h in s["hits"]])
        lines.append(
            f"| {sig} | {hits_str} | {s['sign_hit']:.0%} | {verdict} |"
        )
    lines.append("")
    lines.append("Detail (per-window OOS CAR_10d):")
    lines.append("")
    lines.append("| Signal | IS window | OOS year | OOS n | OOS CAR_10d | OOS t |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for _, r in stage2_df.iterrows():
        lines.append(
            f"| {r['signal']} | {r['is_window']} | {r['oos_year']} | "
            f"{int(r['oos_n']) if not pd.isna(r['oos_n']) else 0} | "
            f"{fmt_pct(r['oos_car10'])} | "
            f"{r['oos_t']:+.2f}" if not pd.isna(r['oos_t']) else "nan"
            f" |"
        )
    lines.append("")

    # ---- Stage 3
    lines.append("## Stage 3: cross-regime sanity (CAR_10d grid)")
    lines.append("")
    lines.append("| Signal | Cell | n | CAR_10d | t |")
    lines.append("|---|---|---:|---:|---:|")
    for _, r in stage3_df.iterrows():
        lines.append(
            f"| {r['signal']} | {r['regime']}+{r['vol_regime']} | {r['n']:,} | "
            f"{fmt_pct(r['car_10d_mean'])} | {r['t_car_10d']:+.2f} |"
        )
    lines.append("")

    # ---- Stage 4
    lines.append("## Stage 4: operationalization")
    lines.append("")
    bvh_pct = stage4_df.loc[
        stage4_df["metric"] == "regime_freq_bear_volhigh_pct", "value"
    ].iloc[0]
    n_active = stage4_df.loc[
        stage4_df["metric"] == "active_trigger_dates", "value"
    ].iloc[0]
    n_total = stage4_df.loc[
        stage4_df["metric"] == "regime_freq_total_days", "value"
    ].iloc[0]
    lines.append(
        f"- bear+vol_high regime 占比: **{bvh_pct:.1%}** of {int(n_total):,} trading days"
    )
    lines.append(f"- bear+vol_high 中有觸發訊號的活躍日: {int(n_active):,} 日")
    lines.append("- Regime detection: 兩個 daily TWII 指標, 收盤後即可算")
    lines.append("  - bear: TWII Close < 200d MA")
    lines.append("  - vol_high: TWII 20d realized vol > sample 66 quantile")
    lines.append("")
    lines.append("**Tx cost robustness (round-trip 0.2%):**")
    lines.append("")
    lines.append("| Signal | n | CAR_10d gross | CAR_10d net | stocks/active day |")
    lines.append("|---|---:|---:|---:|---:|")
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        sub = stage4_df[stage4_df["signal"] == sig]
        if sub.empty:
            continue
        n = sub.loc[sub["metric"] == "trigger_count_bear_volhigh", "value"]
        gross = sub.loc[sub["metric"] == "car_10d_gross", "value"]
        net = sub.loc[sub["metric"] == "car_10d_net_after_tx", "value"]
        per_day = sub.loc[sub["metric"] == "stocks_per_active_day", "value"]
        if n.empty:
            continue
        lines.append(
            f"| {sig} | {int(n.iloc[0]):,} | {fmt_pct(gross.iloc[0])} | "
            f"{fmt_pct(net.iloc[0])} | {per_day.iloc[0]:.1f} |"
        )
    lines.append("")

    # bear+vol_high days per year (sanity for sparse-clustering risk)
    lines.append("**bear+vol_high days per year (concentration risk check):**")
    lines.append("")
    lines.append("| Year | days |")
    lines.append("|---|---:|")
    yr_rows = stage4_df[stage4_df["metric"] == "bvh_days_year"]
    for _, r in yr_rows.iterrows():
        lines.append(f"| {r['signal']} | {int(r['value'])} |")
    lines.append("")

    # ---- Stage 4b kill test
    lines.append("## Stage 4b: COVID-strip + edge-vs-baseline (核心 kill test)")
    lines.append("")
    lines.append(
        "Stage 1 baseline (bear+vol_high 整個市場無篩股) CAR_10d = +1.14% (t=41.39, n=83k) — "
        "**整體市場本身就 mean-revert**, 訊號 incremental edge 才是真實 alpha。"
    )
    lines.append("")
    lines.append("**Excluding 2020 (COVID outlier):**")
    lines.append("")
    lines.append("| Signal | n (ex 2020) | Trigger CAR_10d | Baseline CAR_10d | **Edge** | t (trigger) |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for sig in ["S1_no_response", "S2_intraday_fade", "S3_t1_breakdown"]:
        s = stage4b_summary[sig]
        lines.append(
            f"| {sig} | {s['n_ex2020']:,} | {fmt_pct(s['car10_ex2020'])} | "
            f"{fmt_pct(s['base_car10_ex2020'])} | **{fmt_pct(s['edge_ex2020'])}** | "
            f"{s['t_ex2020']:+.2f} |"
        )
    lines.append("")
    lines.append("**Per-year edge over same-regime baseline (CAR_10d, n>=30):**")
    lines.append("")
    lines.append("| Signal | Year | n | Trigger | Baseline | Edge |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    yr_only = stage4b_df[stage4b_df["scope"].str.startswith("year_")]
    for _, r in yr_only.iterrows():
        yr = r["scope"].replace("year_", "")
        lines.append(
            f"| {r['signal']} | {yr} | {int(r['n'])} | "
            f"{fmt_pct(r['trigger_car10'])} | {fmt_pct(r['baseline_car10'])} | "
            f"{fmt_pct(r['edge_over_baseline'])} |"
        )
    lines.append("")

    # ---- Final verdict
    lines.append("## Final verdict")
    lines.append("")
    lines.append(f"**Overall grade: {overall_grade}**  ({per_sig_grades})")
    lines.append("")
    lines.append("### 核心問題")
    lines.append("")
    lines.append(
        "1. **Baseline 已 mean-revert**: bear+vol_high 整個市場 CAR_10d=+1.14% (t=41), "
        "訊號 incremental alpha 嚴重縮水。"
    )
    lines.append(
        "2. **2020 COVID outlier 主導 walk-forward**: 4 個有資料的 OOS year 中 (2018/2020/2022/2025), "
        "2020 量級遠超其他 (S1 +2.73%, S3 +12.94%). 去掉 COVID 後 edge 大幅下降。"
    )
    lines.append(
        "3. **Walk-forward window 數不足**: 8 個 windows 中 4 個 (2019/2021/2023/2024) bear+vol_high 樣本=0, "
        "實際只有 4 個 OOS observation, 統計力極弱。"
    )
    lines.append(
        "4. **2025 OOS 反向**: 最近期 OOS year, S1 (-0.29%, t=-1.82) 與 S2 (-1.14%, t=-3.27) 反向, "
        "與 2020 COVID dominant 訊號形成衝突。"
    )
    lines.append(
        "5. **Cross-regime 顯示真正 conditioning 是 vol_high 不是 bear**: bull+vol_high 也是 "
        "+1.0%~1.4% (t > 6), 不是 bear-specific. 訊號被誤標為 bear+vol_high。"
    )
    lines.append("")
    lines.append("### 操作化建議")
    lines.append("")
    if overall_grade in ("A", "B"):
        lines.append("- 上線形式: paper trade engine 累積 N=200 樣本後再評估")
        lines.append(
            "- Trigger_score 可考慮加 contrarian 加分項 (限 bear+vol_high regime, 量級小)"
        )
    elif overall_grade == "C":
        lines.append("- **不上線 trigger_score 或 position_monitor**")
        lines.append(
            "- 列入 Mode D / Thesis Panel watchlist (regime breadth 標記為「弱勢股反彈候選」, 不直接下單)"
        )
        lines.append("- 等更多 bear+vol_high samples (2025+) 累積後重驗")
    else:  # D
        lines.append("- **不上線**: trigger_score / position_monitor / paper trade 全部不採納")
        lines.append("- 歸類為 multiple-comparison false positive (15 cells × 3 signals = 45 tests)")
        lines.append(
            "- 真正的訊號可能就只是「TWII 大漲日 vol_high regime 大盤本身會 mean-revert」, "
            "個股「不跟漲」的 incremental edge 約 0~+0.3% 量級, 無 actionable alpha"
        )
    lines.append("")
    lines.append("### Multiple-comparison adjustment 建議 (給未來 SOP)")
    lines.append("")
    lines.append(
        "本研究 cell 數量 6 regime × 3 signal = 18 tests, Bonferroni 門檻為 t ≈ 2.86 (α=0.05/18). "
        "本研究多數 t > 5, 統計上仍顯著, 但問題不在 t 不夠大, 而在 **(a) 與 baseline edge 太小** "
        "**(b) walk-forward 樣本被 COVID 主導**. SOP 建議:"
    )
    lines.append("")
    lines.append("1. Cross-regime grid 看到 outlier cell 時, 先比 same-cell baseline (no signal filter), 算 incremental edge")
    lines.append("2. Walk-forward 至少要 5+ OOS years 都正才算穩, 4 windows 不夠")
    lines.append("3. 訊號量級若高度依賴單一極端年 (COVID 2020), 自動扣分")
    lines.append("4. 多重比較不能只看 t-stat, 要 cross-validate 訊號是否在 conceptually 不該出現的 cell 也出現 (如 bull+vol_high 也正)")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    log(f"Report written: {OUT_MD}")


# ----------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=str(DEFAULT_CSV))
    args = parser.parse_args()

    triggers = load_triggers(Path(args.csv))
    twii = load_twii()
    twii_labeled = label_regimes(twii)
    baseline = load_baseline(twii_labeled)

    # Stage 1
    s1_df, s1_sum = stage1_alpha(triggers, baseline)

    # Stage 2
    s2_df, s2_sum = stage2_walkforward(triggers)

    # Stage 3
    s3_df = stage3_cross_regime(triggers)

    # Stage 4
    s4_df = stage4_operationalization(triggers, twii_labeled)

    # Stage 4b: COVID strip + edge vs baseline
    s4b_df, s4b_sum = stage4b_covid_strip_and_edge(triggers, baseline)

    # Grade
    overall, per_sig = grade(s1_sum, s2_sum, s4b_sum)
    log("=" * 70)
    log(f"FINAL GRADE: {overall}")
    log(f"Per-signal grades: {per_sig}")
    log("=" * 70)

    # Combine all stage CSVs
    all_df = pd.concat(
        [s1_df, s2_df, s3_df, s4_df, s4b_df], ignore_index=True, sort=False
    )
    all_df.to_csv(OUT_CSV, index=False)
    log(f"CSV written: {OUT_CSV}")

    # Report
    write_report(
        s1_df, s1_sum, s2_df, s2_sum, s3_df, s4_df, s4b_df, s4b_sum, overall, per_sig
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
