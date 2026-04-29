"""
宋分擇時 #3 — 三風險同步不惡化 (大盤擇時)

訊號論點：「市場只要不再變更壞就會反彈，不需要等利多」。
三條件 20 日 Δ 全部不惡化 → 進場大盤訊號:
  1. HY spread 20d Δ <= 0 (信用利差未擴大) -- HYG 反向 proxy: HYG_pct_20d >= 0
  2. (VIX3M - VIX) 20d Δ >= 0 (期限結構 contango 加深)
  3. 10Y 殖利率 20d Δ <= 0 (不再創新高)

驗證方式 (event study):
  日 t 訊號觸發 → 比較 TWII fwd_5d/10d/20d 報酬
  vs unconditional baseline (所有日)
  Regime split: bull/bear by TWII vs MA200

Output: reports/songfen_signal3_three_risk_sync.{csv,md}
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
DATA_DIR = _ROOT / "data_cache" / "backtest"
FRED_DIR = _ROOT / "data_cache" / "fred"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "songfen_signal3_three_risk_sync.csv"
OUT_MD = OUT_DIR / "songfen_signal3_three_risk_sync.md"

LOOKBACK = 20
HORIZONS = [5, 10, 20]


def load_fred():
    parts = []
    for label in ["tnx", "vix", "vix3m", "hyg"]:
        df = pd.read_parquet(FRED_DIR / f"{label}.parquet")
        parts.append(df)
    return pd.concat(parts, axis=1).sort_index()


def load_twii():
    df = pd.read_parquet(DATA_DIR / "_twii_bench.parquet")
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = pd.DataFrame({"twii_close": df["Close"].astype(float)})
    df["twii_ret"] = df["twii_close"].pct_change()
    df["twii_ma200"] = df["twii_close"].rolling(200, min_periods=120).mean()
    df["regime"] = np.where(df["twii_close"] >= df["twii_ma200"], "bull", "bear")
    return df


def build_signal(fred):
    """3 conditions of "not worsening" 20d delta."""
    f = fred.copy()
    # C1: HYG 20d pct change >= 0 (HY spread NOT widening)
    f["hyg_pct_20"] = f["hyg"].pct_change(LOOKBACK)
    f["c1"] = f["hyg_pct_20"] >= 0

    # C2: (VIX3M - VIX) 20d Δ >= 0 (contango deepening = panic receding)
    f["term"] = f["vix3m"] - f["vix"]
    f["term_diff_20"] = f["term"] - f["term"].shift(LOOKBACK)
    f["c2"] = f["term_diff_20"] >= 0

    # C3: 10Y yield 20d Δ <= 0 (yield NOT rising)
    f["tnx_diff_20"] = f["tnx"] - f["tnx"].shift(LOOKBACK)
    f["c3"] = f["tnx_diff_20"] <= 0

    f["signal"] = f["c1"] & f["c2"] & f["c3"]
    return f


def event_study(twii, signal_df):
    # Align signal_df to TWII trading days (US session date might differ; use forward-fill 2 days)
    s = signal_df[["signal", "c1", "c2", "c3", "hyg_pct_20", "term_diff_20", "tnx_diff_20"]].copy()
    # Reindex to twii dates with forward-fill (1 day max gap typical for time-zone)
    s = s.reindex(twii.index).ffill(limit=2)
    merged = twii.join(s)

    for h in HORIZONS:
        merged[f"twii_fwd_{h}d"] = merged["twii_close"].shift(-h) / merged["twii_close"] - 1

    return merged


def summarize(merged):
    rows = []
    for label, mask in [
        ("all", merged["signal"].notna()),
        ("signal=True", merged["signal"] == True),
        ("signal=False", merged["signal"] == False),
        ("c1_only", merged["c1"] == True),
        ("c2_only", merged["c2"] == True),
        ("c3_only", merged["c3"] == True),
        ("regime_bull & signal", (merged["regime"] == "bull") & (merged["signal"] == True)),
        ("regime_bear & signal", (merged["regime"] == "bear") & (merged["signal"] == True)),
    ]:
        sub = merged[mask].copy()
        n = len(sub)
        if n < 30:
            continue
        row = {"group": label, "n": n}
        for h in HORIZONS:
            col = f"twii_fwd_{h}d"
            ser = sub[col].dropna()
            if len(ser) < 10:
                row[f"fwd_{h}d_mean"] = np.nan
                row[f"fwd_{h}d_t"] = np.nan
                continue
            row[f"fwd_{h}d_mean"] = ser.mean()
            row[f"fwd_{h}d_median"] = ser.median()
            row[f"fwd_{h}d_t"] = ser.mean() / (ser.std(ddof=1) / np.sqrt(len(ser))) if ser.std(ddof=1) > 0 else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def grade(mean_20d, t_20d, baseline_mean):
    if pd.isna(mean_20d) or pd.isna(t_20d):
        return "n/a"
    edge = mean_20d - baseline_mean
    if edge > 0.005 and t_20d > 2:
        return "A"
    if edge > 0.003 and t_20d > 1.5:
        return "B"
    if edge > 0.001:
        return "C"
    return "D"


def main():
    fred = load_fred()
    twii = load_twii()
    print(f"FRED: {fred.index[0].date()} ~ {fred.index[-1].date()} ({len(fred):,} rows)")
    print(f"TWII: {twii.index[0].date()} ~ {twii.index[-1].date()} ({len(twii):,} rows)")

    signal_df = build_signal(fred)
    merged = event_study(twii, signal_df)

    # Restrict to where we have all data (HYG starts 2007-04, VIX3M 2006-07, so signal from 2007+)
    merged = merged[merged.index >= "2007-05-01"].copy()
    print(f"Joined panel after data trim: {len(merged):,} rows from {merged.index[0].date()}")

    summary = summarize(merged)
    summary.to_csv(OUT_CSV, index=False)
    print()
    print(summary.to_string(index=False))

    # Grading using "all" as baseline
    base = summary[summary["group"] == "all"].iloc[0]
    sig_row = summary[summary["group"] == "signal=True"].iloc[0]
    base_mean_20 = base["fwd_20d_mean"]
    sig_mean_20 = sig_row["fwd_20d_mean"]
    sig_t_20 = sig_row["fwd_20d_t"]
    g = grade(sig_mean_20, sig_t_20, base_mean_20)
    edge = sig_mean_20 - base_mean_20
    print(f"\nBaseline TWII 20d mean = {base_mean_20:+.4f}")
    print(f"Signal=True 20d mean   = {sig_mean_20:+.4f}")
    print(f"Edge = {edge:+.4f} (t={sig_t_20:+.2f}) → Grade {g}")

    md = f"""# 宋分擇時 #3 三風險同步 Event Study

**Date**: 2026-04-29
**Universe**: TWII 2007-05 ~ 2026-04 (HYG/VIX3M data limit)
**Signal**: HYG 20d ≥ 0 AND (VIX3M-VIX) 20d Δ ≥ 0 AND 10Y 20d Δ ≤ 0

## Result Table

| group | n | fwd_5d | fwd_10d | fwd_20d | t_20d |
|---|---:|---:|---:|---:|---:|
"""
    for _, r in summary.iterrows():
        md += (f"| {r['group']} | {r['n']:.0f} | "
               f"{r.get('fwd_5d_mean', 0):+.4f} | "
               f"{r.get('fwd_10d_mean', 0):+.4f} | "
               f"{r.get('fwd_20d_mean', 0):+.4f} | "
               f"{r.get('fwd_20d_t', 0):+.2f} |\n")
    md += f"""
## Verdict

Edge over baseline = {edge:+.4f} (t={sig_t_20:+.2f}) → **Grade {g}**

Grading rule:
- A: edge > 0.5% AND t > 2
- B: edge > 0.3% AND t > 1.5
- C: edge > 0.1%
- D: 否則
"""
    OUT_MD.write_text(md, encoding="utf-8")
    print(f"\n=== Verdict: {g} ===")


if __name__ == "__main__":
    main()
