"""Re-evaluate cached events parquet with the updated verdict logic."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import tools.v3_layer3_catalyst_ic as M

OUT_EV = _ROOT / "reports" / "v3_layer3_catalyst_events.parquet"
OHLCV = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"

def main():
    print("Loading events ...")
    ev = pd.read_parquet(OUT_EV)
    ev["effective_date"] = pd.to_datetime(ev["effective_date"])

    print("Loading OHLCV for baseline ...")
    df = pd.read_parquet(OHLCV, columns=["stock_id", "date", "Close", "AdjClose", "Volume"])
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)].copy()
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)

    # Recompute fwd (same as main)
    print("Computing fwd ...")
    fd = df[["stock_id", "date", "AdjClose"]].copy()
    fd = fd.sort_values(["stock_id", "date"])
    for h in M.HORIZONS:
        fd[f"fwd_{h}"] = fd.groupby("stock_id")["AdjClose"].shift(-h) / fd["AdjClose"] - 1.0
    fd = fd.drop(columns=["AdjClose"])

    print("Building universe ...")
    uv = df[["stock_id", "date", "Close", "Volume"]].copy()
    uv["tv"] = uv["Close"] * uv["Volume"]
    uv["tv60"] = uv.groupby("stock_id")["tv"].transform(
        lambda s: s.rolling(60, min_periods=30).mean()
    )
    uv["rk"] = uv.groupby("date")["tv60"].rank(ascending=False, method="first")
    uv = uv.loc[uv["rk"] <= M.UNIV_TOP_N, ["date", "stock_id"]].drop_duplicates()

    print("Building baseline ...")
    baseline = fd.merge(uv, on=["date", "stock_id"], how="inner")
    baseline["regime"] = baseline["date"].apply(M._regime)
    baseline = baseline[baseline["regime"] != "other"]

    print("Evaluating ...")
    result = M.evaluate(ev, baseline)
    verdicts = {}
    for sig in result["signal"].unique():
        verdicts[sig] = M.verdict(result[result["signal"] == sig])
    result["verdict"] = result["signal"].map(verdicts)
    result = result.sort_values(["signal", "regime", "horizon"])
    result.to_csv(M.OUT_CSV, index=False, float_format="%.6f")
    print(f"Wrote {M.OUT_CSV}")

    total_ev = len(ev)
    M.write_md(result, verdicts, total_ev, total_ev)
    print(f"Wrote {M.OUT_MD}")
    print("Verdicts:", verdicts)

if __name__ == "__main__":
    main()
