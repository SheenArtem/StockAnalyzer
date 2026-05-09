"""
System 3 Phase 3.1 - Daily panel + forward 21d MDD target (Short-Horizon Crash Alarm).

User need (2026-05-09): 1-week to 1-month ahead crash warning for hedge / reduce.

Targets (binary labels per day):
  fwd_5d_mdd_5pct      = forward 5d MDD <= -5% (1-week sharp drop)
  fwd_5d_mdd_10pct     = forward 5d MDD <= -10%
  fwd_21d_mdd_10pct    = forward 21d MDD <= -10% (primary, 1-month correction)
  fwd_21d_mdd_15pct    = forward 21d MDD <= -15%

Features (per day, all lagged i.e. as-of close):
  TAIEX-internal (Track A, 1999+):
    rv10, rv20, ma_dist_60, ma_dist_20, vol_ratio_20d, rsi14,
    ret_5d, ret_20d, range_5d, gap_open
  US session (Track B, 2002+):
    vix_level, vix_5d_chg, vix_term (vix/vix3m), move_level, move_5d_chg,
    spx_5d_ret, spx_20d_ret
  Chip (Track C, 2015+):
    foreign_5d_z, inst_5d_z

Output:
  reports/system3_panel.parquet   (one row per TAIEX trading day)
  reports/system3_panel_summary.md
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TAIEX_PATH = ROOT / "data_cache" / "TAIEX_price.parquet"
INST_PATH = ROOT / "data_cache" / "chip_history" / "institutional.parquet"
FRED_DIR = ROOT / "data_cache" / "fred"
OUT_PARQUET = ROOT / "reports" / "system3_panel.parquet"
OUT_MD = ROOT / "reports" / "system3_panel_summary.md"


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def load_taiex() -> pd.DataFrame:
    df = pd.read_parquet(TAIEX_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df = df.rename(columns={"max": "high", "min": "low", "Trading_Volume": "volume"})
    return df.set_index("date")[["open", "high", "low", "close", "volume"]]


def build_taiex_features(taiex: pd.DataFrame) -> pd.DataFrame:
    close = taiex["close"]
    log_ret = np.log(close).diff()
    f = pd.DataFrame(index=taiex.index)
    f["close"] = close
    f["volume"] = taiex["volume"]
    f["ret_5d"] = close.pct_change(5)
    f["ret_20d"] = close.pct_change(20)
    f["rv_10d"] = log_ret.rolling(10).std() * np.sqrt(252)
    f["rv_20d"] = log_ret.rolling(20).std() * np.sqrt(252)
    f["vol_ratio_20d"] = taiex["volume"] / taiex["volume"].rolling(20).mean()
    f["rsi14"] = compute_rsi(close, 14)
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    f["ma_dist_20"] = (close - ma20) / ma20
    f["ma_dist_60"] = (close - ma60) / ma60
    rng = (taiex["high"] - taiex["low"]) / close
    f["range_5d_avg"] = rng.rolling(5).mean()
    f["gap_open"] = (taiex["open"] - close.shift(1)) / close.shift(1)
    return f


def build_us_features() -> pd.DataFrame:
    """VIX / VIX3M / MOVE level + 5d change."""
    vix = pd.read_parquet(FRED_DIR / "vix.parquet")["vix"]
    vix3m = pd.read_parquet(FRED_DIR / "vix3m.parquet")["vix3m"]
    move = pd.read_parquet(FRED_DIR / "move.parquet")["move"]
    df = pd.DataFrame()
    df["vix_level"] = vix
    df["vix_5d_chg"] = vix.pct_change(5)
    df["vix3m_level"] = vix3m
    df["vix_term"] = (vix / vix3m).reindex(vix.index)
    df["move_level"] = move
    df["move_5d_chg"] = move.pct_change(5)
    df.index = pd.to_datetime(df.index)
    return df


def build_chip_features() -> pd.DataFrame:
    inst = pd.read_parquet(INST_PATH)
    inst["date"] = pd.to_datetime(inst["date"])
    agg = inst.groupby("date")[["foreign_net", "trust_net", "dealer_net"]].sum()
    agg["total_net"] = agg.sum(axis=1)
    df = pd.DataFrame(index=agg.index)
    df["foreign_5d_sum"] = agg["foreign_net"].rolling(5).sum()
    fmean = agg["foreign_net"].rolling(60).mean() * 5
    fstd = agg["foreign_net"].rolling(60).std() * np.sqrt(5)
    df["foreign_5d_z"] = (df["foreign_5d_sum"] - fmean) / fstd
    df["inst_5d_sum"] = agg["total_net"].rolling(5).sum()
    imean = agg["total_net"].rolling(60).mean() * 5
    istd = agg["total_net"].rolling(60).std() * np.sqrt(5)
    df["inst_5d_z"] = (df["inst_5d_sum"] - imean) / istd
    return df[["foreign_5d_z", "inst_5d_z"]]


def build_targets(close: pd.Series, fwd_window: int) -> pd.Series:
    """Forward MDD = min(close[t+1..t+W]) / close[t] - 1."""
    fwd_min = close.shift(-fwd_window).rolling(fwd_window, min_periods=1).min()
    # rolling over shifted window would re-include earlier; use direct loop or cumulative method
    # Better: use close[t+1..t+W] explicitly via reverse rolling
    n = len(close)
    out = pd.Series(np.nan, index=close.index, dtype=float)
    vals = close.to_numpy(dtype=float)
    for i in range(n - 1):
        end = min(i + 1 + fwd_window, n)
        if end > i + 1:
            window_min = vals[i + 1:end].min()
            out.iloc[i] = window_min / vals[i] - 1.0
    return out


def main() -> None:
    taiex = load_taiex()
    print(f"[INFO] TAIEX {taiex.index[0].date()} -> {taiex.index[-1].date()} ({len(taiex)} days)")

    feat_taiex = build_taiex_features(taiex)
    feat_us = build_us_features()
    feat_chip = build_chip_features()

    # Targets
    close = taiex["close"]
    targets = pd.DataFrame(index=taiex.index)
    targets["fwd_5d_mdd"] = build_targets(close, 5)
    targets["fwd_21d_mdd"] = build_targets(close, 21)
    targets["fwd_5d_mdd_5pct"] = (targets["fwd_5d_mdd"] <= -0.05).astype(int)
    targets["fwd_5d_mdd_10pct"] = (targets["fwd_5d_mdd"] <= -0.10).astype(int)
    targets["fwd_21d_mdd_10pct"] = (targets["fwd_21d_mdd"] <= -0.10).astype(int)
    targets["fwd_21d_mdd_15pct"] = (targets["fwd_21d_mdd"] <= -0.15).astype(int)

    # Merge: TAIEX index is canonical
    panel = feat_taiex.join(feat_us, how="left").join(feat_chip, how="left").join(targets, how="left")
    # Forward-fill US (non-TW trading days have no US data; use prior close)
    us_cols = ["vix_level", "vix_5d_chg", "vix3m_level", "vix_term", "move_level", "move_5d_chg"]
    for c in us_cols:
        panel[c] = panel[c].ffill()

    # Drop rows where target unavailable (last 21 days)
    panel = panel.dropna(subset=["fwd_21d_mdd"])
    panel.to_parquet(OUT_PARQUET)

    feat_cols = [c for c in panel.columns if c not in ["close", "volume", "fwd_5d_mdd", "fwd_21d_mdd"]
                 and not c.endswith("_pct")]

    L = []
    L.append("# System 3 Daily Panel Summary")
    L.append("")
    L.append(f"**Date range**: {panel.index[0].date()} -> {panel.index[-1].date()}")
    L.append(f"**Trading days**: {len(panel)}")
    L.append(f"**Total columns**: {len(panel.columns)}")
    L.append("")
    L.append("## Target label baselines")
    L.append("")
    L.append("| Target | N positive | Baseline % |")
    L.append("|---|---|---|")
    for col in ["fwd_5d_mdd_5pct", "fwd_5d_mdd_10pct", "fwd_21d_mdd_10pct", "fwd_21d_mdd_15pct"]:
        n = int(panel[col].sum())
        p = 100 * n / len(panel)
        L.append(f"| {col} | {n} | {p:.1f}% |")
    L.append("")
    L.append("## Feature coverage")
    L.append("")
    L.append("| Feature | Coverage % | First valid |")
    L.append("|---|---|---|")
    for col in panel.columns:
        if col in ["close", "volume"] or col.startswith("fwd_"):
            continue
        nn = panel[col].notna().sum()
        cov = 100 * nn / len(panel)
        first = panel[col].dropna().index.min() if nn > 0 else None
        first_str = str(first.date()) if first is not None else "-"
        L.append(f"| {col} | {cov:.0f}% | {first_str} |")
    L.append("")
    L.append("## Class balance by epoch")
    L.append("")
    L.append("| Epoch | Days | fwd_21d_mdd_10pct rate | fwd_5d_mdd_5pct rate |")
    L.append("|---|---|---|---|")
    for label, mask in [
        ("1999-2007", panel.index < pd.Timestamp("2008-01-01")),
        ("2008-2014", (panel.index >= pd.Timestamp("2008-01-01")) & (panel.index < pd.Timestamp("2015-01-01"))),
        ("2015-2026", panel.index >= pd.Timestamp("2015-01-01")),
    ]:
        sub = panel[mask]
        if len(sub):
            r1 = 100 * sub["fwd_21d_mdd_10pct"].mean()
            r2 = 100 * sub["fwd_5d_mdd_5pct"].mean()
            L.append(f"| {label} | {len(sub)} | {r1:.1f}% | {r2:.1f}% |")
    L.append("")

    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")

    print(f"[OK] panel {panel.shape} -> {OUT_PARQUET.name}")
    print(f"[OK] summary -> {OUT_MD.name}")
    print()
    print("Target baselines:")
    for col in ["fwd_5d_mdd_5pct", "fwd_5d_mdd_10pct", "fwd_21d_mdd_10pct", "fwd_21d_mdd_15pct"]:
        n = int(panel[col].sum())
        p = 100 * n / len(panel)
        print(f"  {col}: {n} ({p:.1f}%)")


if __name__ == "__main__":
    main()
