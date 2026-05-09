"""
System 2 Phase 2.2 - Internal reflexivity features at -5% trigger.

Per spec (route B = pure internal, no cross-asset due to FRED HY OAS 3yr only).
Computes features at each event's trigger_date using TAIEX OHLCV (1999+) and
chip aggregates (2015+).

Feature catalog:
  Price/Volume (full history 1999+):
    velocity_5d, velocity_20d, vol_ratio_20d, rsi14,
    ma_dist_20, ma_dist_60, rv_10d, rv_20d, range_5d, gap_open
  Chip (2015+; nullable for earlier events):
    foreign_5d_sum, foreign_20d_sum, foreign_5d_z,
    trust_5d_sum, dealer_5d_sum,
    inst_total_5d_z

Outputs:
  reports/system2_features.parquet     (per-event feature matrix)
  reports/system2_features_summary.md  (coverage + per-class medians)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TAIEX_PATH = ROOT / "data_cache" / "TAIEX_price.parquet"
INST_PATH = ROOT / "data_cache" / "chip_history" / "institutional.parquet"
EVENTS_PATH = ROOT / "reports" / "system2_events.parquet"
OUT_PARQUET = ROOT / "reports" / "system2_features.parquet"
OUT_MD = ROOT / "reports" / "system2_features_summary.md"


def load_taiex() -> pd.DataFrame:
    df = pd.read_parquet(TAIEX_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df = df.rename(columns={"max": "high", "min": "low", "Trading_Volume": "volume"})
    return df.set_index("date")[["open", "high", "low", "close", "volume"]]


def load_chip_daily() -> pd.DataFrame:
    df = pd.read_parquet(INST_PATH)
    df["date"] = pd.to_datetime(df["date"])
    agg = df.groupby("date")[["foreign_net", "trust_net", "dealer_net"]].sum()
    agg["total_net"] = agg[["foreign_net", "trust_net", "dealer_net"]].sum(axis=1)
    return agg


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def compute_price_features(taiex: pd.DataFrame) -> pd.DataFrame:
    close = taiex["close"]
    log_ret = np.log(close).diff()

    f = pd.DataFrame(index=taiex.index)
    f["close"] = close
    f["velocity_5d"] = close.pct_change(5)
    f["velocity_20d"] = close.pct_change(20)
    f["vol_ratio_20d"] = taiex["volume"] / taiex["volume"].rolling(20).mean()
    f["rsi14"] = compute_rsi(close, 14)
    f["ma20"] = close.rolling(20).mean()
    f["ma60"] = close.rolling(60).mean()
    f["ma_dist_20"] = (close - f["ma20"]) / f["ma20"]
    f["ma_dist_60"] = (close - f["ma60"]) / f["ma60"]
    f["rv_10d"] = log_ret.rolling(10).std() * np.sqrt(252)
    f["rv_20d"] = log_ret.rolling(20).std() * np.sqrt(252)
    rng = (taiex["high"] - taiex["low"]) / close
    f["range_5d_avg"] = rng.rolling(5).mean()
    f["gap_open"] = (taiex["open"] - close.shift(1)) / close.shift(1)
    return f


def compute_chip_features(chip: pd.DataFrame) -> pd.DataFrame:
    f = pd.DataFrame(index=chip.index)
    f["foreign_5d_sum"] = chip["foreign_net"].rolling(5).sum()
    f["foreign_20d_sum"] = chip["foreign_net"].rolling(20).sum()
    foreign_60d_mean = chip["foreign_net"].rolling(60).mean() * 5
    foreign_60d_std = chip["foreign_net"].rolling(60).std() * np.sqrt(5)
    f["foreign_5d_z"] = (f["foreign_5d_sum"] - foreign_60d_mean) / foreign_60d_std
    f["trust_5d_sum"] = chip["trust_net"].rolling(5).sum()
    f["dealer_5d_sum"] = chip["dealer_net"].rolling(5).sum()
    f["inst_total_5d_sum"] = chip["total_net"].rolling(5).sum()
    inst_60d_mean = chip["total_net"].rolling(60).mean() * 5
    inst_60d_std = chip["total_net"].rolling(60).std() * np.sqrt(5)
    f["inst_total_5d_z"] = (f["inst_total_5d_sum"] - inst_60d_mean) / inst_60d_std
    return f


def main() -> None:
    events = pd.read_parquet(EVENTS_PATH)
    events["trigger_date"] = pd.to_datetime(events["trigger_date"])

    taiex = load_taiex()
    price_feat = compute_price_features(taiex)
    chip_daily = load_chip_daily()
    chip_feat = compute_chip_features(chip_daily)

    # Snap each trigger_date to the latest taiex date <= trigger_date
    feature_rows = []
    for _, ev in events.iterrows():
        td = ev["trigger_date"]
        # Find the trading date that matches exactly (events come from TAIEX so should be exact)
        if td not in price_feat.index:
            # Fall back to nearest prior
            avail = price_feat.index[price_feat.index <= td]
            if len(avail) == 0:
                continue
            td = avail[-1]
        row = {"event_id": int(ev["event_id"]), "trigger_date": td, "class": ev["class"]}
        for col in price_feat.columns:
            if col == "close":
                continue
            row[col] = price_feat.at[td, col] if td in price_feat.index else np.nan
        # Chip features
        if td in chip_feat.index:
            for col in chip_feat.columns:
                row[col] = chip_feat.at[td, col]
        else:
            for col in chip_feat.columns:
                row[col] = np.nan
        feature_rows.append(row)

    fdf = pd.DataFrame(feature_rows)

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    fdf.to_parquet(OUT_PARQUET, index=False)

    # Coverage summary
    feature_cols = [c for c in fdf.columns if c not in ("event_id", "trigger_date", "class")]

    lines = [
        "# System 2 Features Summary",
        "",
        f"**Events**: {len(fdf)}",
        f"**Features**: {len(feature_cols)}",
        "",
        "## Coverage (non-null %)",
        "",
        "| Feature | Coverage | First valid | A_small med | B_medium med | C_crash med |",
        "|---|---|---|---|---|---|",
    ]
    for col in feature_cols:
        nn = fdf[col].notna().sum()
        cov = 100 * nn / len(fdf)
        first = fdf.loc[fdf[col].notna(), "trigger_date"].min()
        first_str = str(first.date()) if pd.notna(first) else "-"
        med_a = fdf.loc[fdf["class"] == "A_small", col].median()
        med_b = fdf.loc[fdf["class"] == "B_medium", col].median()
        med_c = fdf.loc[fdf["class"] == "C_crash", col].median()
        def fmt(v):
            if pd.isna(v):
                return "-"
            if abs(v) >= 1e6:
                return f"{v:.2e}"
            return f"{v:.4f}"
        lines.append(
            f"| {col} | {cov:.0f}% ({nn}/{len(fdf)}) | {first_str} | {fmt(med_a)} | {fmt(med_b)} | {fmt(med_c)} |"
        )

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[OK] {len(fdf)} events x {len(feature_cols)} features -> {OUT_PARQUET.name}")
    print(f"[OK] summary -> {OUT_MD.name}")


if __name__ == "__main__":
    main()
