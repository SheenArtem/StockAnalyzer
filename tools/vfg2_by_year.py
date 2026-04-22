"""
VF-G2 TP ladder by-year breakdown (10.5yr trade_journal).

Answers: does the 3-stage TP ladder save capital in bear years (2015/2018/2022)
that 5yr mean-pooled grade D analysis missed?

For each year, compute:
  baseline TP: tp1=0.15/tp2=0.25/tp3=0.4/floor=0.7/ceil=1.6
  best TP (from full grid): tp1=0.3/tp2=0.4/tp3=0.6/floor=0.9/ceil=2.0
  SL-only (no TP): SL hit -> sl_pct else fwd_40d
  pure-hold: fwd_40d

Metrics: mean, Sharpe, win_rate, sl_rate.

Output: reports/vfg2_by_year.csv + console summary.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"c:\GIT\StockAnalyzer")
JOURNAL = ROOT / "data_cache" / "backtest" / "trade_journal_qm_tw.parquet"
OUT = ROOT / "reports" / "vfg2_by_year.csv"

STOP_MULT = 3.0
STOP_CEIL = 0.14
STOP_FLOOR = 0.05
MIN_SL_GAP_ATR_MULT = 1.5
DEFAULT_MIN_SL_GAP = 0.03
ATR_PCT_MEDIAN = 2.5

BASELINE = dict(tp1=0.15, tp2=0.25, tp3=0.40, floor=0.7, ceil=1.6)
BEST = dict(tp1=0.30, tp2=0.40, tp3=0.60, floor=0.9, ceil=2.0)


def compute_sl_pct_vec(df):
    entry = df["entry_price"].values
    atr = df["atr_pct"].values
    ma20 = df["weekly_ma20"].values

    stop_pct = np.clip(atr / 100.0 * STOP_MULT, STOP_FLOOR, STOP_CEIL)
    hard_stop_price = entry * (1 - stop_pct)
    gap = atr * MIN_SL_GAP_ATR_MULT / 100.0
    min_gap = np.maximum(DEFAULT_MIN_SL_GAP, gap)

    ma20_valid = np.isfinite(ma20) & (ma20 > 0) & (ma20 < entry)
    ma20_gap = np.where(ma20_valid, (entry - ma20) / entry, np.nan)
    use_ma20 = ma20_valid & (ma20 > hard_stop_price) & (ma20_gap >= min_gap)
    sl_price = np.where(use_ma20, ma20, hard_stop_price)
    return (sl_price / entry) - 1.0


def simulate_tp(df, tp1, tp2, tp3, floor, ceil):
    atr = df["atr_pct"].values
    fwd = df["fwd_40d"].values
    fwd_max = df["fwd_40d_max"].values
    fwd_min = df["fwd_40d_min"].values

    sl_pct = compute_sl_pct_vec(df)
    hit_sl = fwd_min <= sl_pct
    tp_scale = np.clip(atr / ATR_PCT_MEDIAN, floor, ceil)

    t1s, t2s, t3s = tp1 * tp_scale, tp2 * tp_scale, tp3 * tp_scale
    hit_tp1 = fwd_max >= t1s
    hit_tp2 = fwd_max >= t2s
    hit_tp3 = fwd_max >= t3s

    blend = (
        np.where(hit_tp1, t1s, fwd) +
        np.where(hit_tp2, t2s, fwd) +
        np.where(hit_tp3, t3s, fwd)
    ) / 3.0
    realized = np.where(hit_sl, sl_pct, blend)
    return realized, hit_sl


def stats(x, label):
    x = np.asarray(x)
    x = x[np.isfinite(x)]
    if len(x) == 0:
        return dict(strategy=label, n=0, mean=np.nan, std=np.nan, sharpe=np.nan, win=np.nan)
    mean = float(np.mean(x))
    std = float(np.std(x))
    sh = mean / std if std > 0 else np.nan
    win = float((x > 0).sum()) / len(x)
    return dict(strategy=label, n=len(x), mean=mean, std=std, sharpe=sh, win=win)


def run_year(df_year):
    sl_pct = compute_sl_pct_vec(df_year)
    fwd = df_year["fwd_40d"].values
    fwd_min = df_year["fwd_40d_min"].values
    hit_sl = fwd_min <= sl_pct

    base_r, base_sl = simulate_tp(df_year, **BASELINE)
    best_r, best_sl = simulate_tp(df_year, **BEST)
    sl_only = np.where(hit_sl, sl_pct, fwd)
    pure = fwd

    rows = [
        stats(base_r, "baseline_TP"),
        stats(best_r, "best_TP"),
        stats(sl_only, "SL_only"),
        stats(pure, "pure_hold"),
    ]
    for r in rows:
        r["sl_rate"] = float(hit_sl.sum()) / len(hit_sl)
    return rows


def main():
    df = pd.read_parquet(JOURNAL)
    df = df.dropna(subset=["fwd_40d", "fwd_40d_min", "fwd_40d_max", "entry_price", "atr_pct"])
    df["year"] = pd.to_datetime(df["week_end_date"]).dt.year

    all_rows = []
    print(f"{'Year':6} {'Strat':14} {'N':>5} {'Mean':>8} {'Sharpe':>7} {'Win':>6} {'SL':>6}")
    print("-" * 60)
    for year in sorted(df["year"].unique()):
        sub = df[df["year"] == year]
        rows = run_year(sub)
        for r in rows:
            r["year"] = int(year)
            all_rows.append(r)
            print(f"{year:6} {r['strategy']:14} {r['n']:>5} "
                  f"{r['mean']*100:>7.2f}% {r['sharpe']:>7.3f} {r['win']*100:>5.1f}% "
                  f"{r['sl_rate']*100:>5.1f}%")
        print("-" * 60)

    # Also: full-sample
    rows = run_year(df)
    for r in rows:
        r["year"] = 9999
        all_rows.append(r)
        print(f"{'ALL':6} {r['strategy']:14} {r['n']:>5} "
              f"{r['mean']*100:>7.2f}% {r['sharpe']:>7.3f} {r['win']*100:>5.1f}% "
              f"{r['sl_rate']*100:>5.1f}%")

    out = pd.DataFrame(all_rows)[["year", "strategy", "n", "mean", "std", "sharpe", "win", "sl_rate"]]
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(f"\n-> {OUT}")

    # --- Bear year summary ---
    print("\n=== BEAR YEAR DELTA (TP vs pure-hold) ===")
    for year in [2015, 2018, 2022]:
        sub = out[out["year"] == year].set_index("strategy")
        if year not in out["year"].values:
            continue
        ph = sub.loc["pure_hold"]
        for strat in ["baseline_TP", "best_TP", "SL_only"]:
            if strat not in sub.index:
                continue
            s = sub.loc[strat]
            dm = (s["mean"] - ph["mean"]) * 100
            ds = s["sharpe"] - ph["sharpe"]
            verdict = "SAVES" if dm > 0.5 else ("neutral" if abs(dm) <= 0.5 else "HURTS")
            print(f"  {year} {strat:12} vs pure_hold: mean {dm:+.2f}pp  Sharpe {ds:+.3f}  [{verdict}]")


if __name__ == "__main__":
    main()
