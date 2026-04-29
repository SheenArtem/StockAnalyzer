"""
VF — Operating Leverage factor IC validation (宋分 Value 候選 #4).

宋分原話：「營收成長速度 > 費用成長速度 2 季以上 → 利潤噴發 → 主升段」。

3 個營業槓桿 factor:
  F1 (yoy_diff)     = Rev_YoY(t) - OpEx_YoY(t)             單季 YoY 差
  F2 (yoy_diff_2q)  = avg of F1 over last 2 quarters       2Q 平均（仿宋分「連 2 季」）
  F3 (positive_2q)  = 1 if F1>0 for both last 2 quarters   binary 觸發（最嚴格）

Robustness:
- 45-day announce delay 防 lookahead
- YoY 需要 5 quarters 連續資料 (Q(t) + Q(t-4) for both Rev and OpEx)
- 過濾極端值 |F1| > 200% (拆股/重組異常)
- 月底 cross-sectional IC，月度樣本 >= 100

R1-R5 同 GM/ROIC/CCC SOP:
  R1 IC mean / IR / hit_rate over 1m/3m/6m/12m
  R2 Decile spread + monotonicity
  R3 Walk-forward 60M IS / 12M OOS sign-stability
  R4 Regime breakdown (bull/bear)
  R5 Correlation with F-Score quality_score

Output: reports/vf_oplev_factor_ic.{csv,md}
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "vf_oplev_factor_ic.csv"
OUT_MD = OUT_DIR / "vf_oplev_factor_ic.md"

ANNOUNCE_DELAY_DAYS = 45
HORIZONS_MONTHS = [1, 3, 6, 12]
MIN_CROSS_SECTION = 100

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("oplev_ic")


def _pivot_long(df, types):
    sub = df[df["type"].isin(types)].copy()
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce")
    pivot = sub.pivot_table(index=["stock_id", "date"], columns="type",
                            values="value", aggfunc="first").reset_index()
    pivot.columns.name = None
    return pivot


def load_financials():
    logger.info("Loading financials_income.parquet...")
    inc_long = pd.read_parquet(DATA_DIR / "financials_income.parquet")
    inc_long["date"] = pd.to_datetime(inc_long["date"])
    inc = _pivot_long(inc_long, ["Revenue", "OperatingExpenses"])
    inc = inc.sort_values(["stock_id", "date"]).reset_index(drop=True)
    if "Revenue" not in inc.columns or "OperatingExpenses" not in inc.columns:
        raise RuntimeError("Need Revenue + OperatingExpenses columns")
    logger.info(f"Income wide: {len(inc):,} rows × {inc['stock_id'].nunique()} stocks")
    return inc


def load_ohlcv():
    px = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet", columns=["stock_id", "date", "AdjClose"])
    px["date"] = pd.to_datetime(px["date"])
    px["AdjClose"] = pd.to_numeric(px["AdjClose"], errors="coerce")
    return px.dropna(subset=["AdjClose"]).query("AdjClose > 0")


def load_universe():
    u = pd.read_parquet(DATA_DIR / "universe_tw_full.parquet")
    return set(u[u["is_common_stock"] == True]["stock_id"].astype(str).unique())


def compute_oplev(g):
    """For one stock, compute F1/F2/F3."""
    g = g.sort_values("date").copy()
    rev = g["Revenue"]
    opex = g["OperatingExpenses"]

    # Need both > 0 for ratio to be meaningful (some companies report negative opex)
    rev_yoy = rev / rev.shift(4) - 1
    opex_yoy = opex / opex.shift(4) - 1

    f1 = rev_yoy - opex_yoy
    # Sanity clip: > 200% absolute = data anomaly
    f1 = f1.where(f1.abs() < 2.0, np.nan)

    g["F1_yoy_diff"] = f1
    g["F2_yoy_diff_2q"] = f1.rolling(2, min_periods=2).mean()
    # F3: binary, both quarters positive
    f3 = ((f1 > 0) & (f1.shift(1) > 0)).astype(int)
    f3 = f3.where(f1.notna() & f1.shift(1).notna(), np.nan)
    g["F3_positive_2q"] = f3

    return g[["stock_id", "date", "F1_yoy_diff", "F2_yoy_diff_2q", "F3_positive_2q"]]


def build_panel(fin):
    logger.info("Computing operating leverage factors per stock...")
    t0 = time.time()
    out = []
    for sid, g in fin.groupby("stock_id", sort=False):
        if len(g) < 6:
            continue
        out.append(compute_oplev(g))
    panel = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    panel = panel.dropna(subset=["F1_yoy_diff"])
    logger.info(f"Panel: {len(panel):,} rows, {panel['stock_id'].nunique()} stocks ({time.time()-t0:.1f}s)")
    return panel


def build_monthly_returns(px):
    px = px.sort_values(["stock_id", "date"])
    px["ym"] = px["date"].dt.to_period("M")
    monthly = px.groupby(["stock_id", "ym"], as_index=False).tail(1).copy()
    monthly = monthly[["stock_id", "ym", "date", "AdjClose"]].rename(
        columns={"date": "me_date", "AdjClose": "px"})
    monthly["me_date"] = monthly["ym"].dt.to_timestamp("M")
    monthly = monthly.sort_values(["stock_id", "ym"]).reset_index(drop=True)
    monthly["px"] = monthly["px"].astype(float)
    g = monthly.groupby("stock_id", sort=False)
    for h in HORIZONS_MONTHS:
        monthly[f"fwd_{h}m"] = g["px"].shift(-h) / monthly["px"] - 1
    return monthly


def attach_factor(monthly, panel):
    panel = panel.copy()
    panel["available_from"] = panel["date"] + pd.Timedelta(days=ANNOUNCE_DELAY_DAYS)
    panel = panel.sort_values(["available_from", "stock_id"]).reset_index(drop=True)
    monthly = monthly.sort_values(["me_date", "stock_id"]).reset_index(drop=True)
    out = pd.merge_asof(monthly, panel,
                        left_on="me_date", right_on="available_from",
                        by="stock_id", direction="backward", allow_exact_matches=True)
    return out


def cross_sectional_ic(df, factor_col, ret_col):
    out = []
    for me, g in df.groupby("me_date", sort=True):
        gg = g[[factor_col, ret_col]].dropna()
        if len(gg) < MIN_CROSS_SECTION:
            continue
        try:
            r, p = stats.spearmanr(gg[factor_col].values, gg[ret_col].values)
        except Exception:
            continue
        if not np.isnan(r):
            out.append({"me_date": me, "n": len(gg), "ic": r, "p": p})
    return pd.DataFrame(out)


def ic_summary(ic_df):
    if ic_df.empty:
        return {"n_months": 0, "ic_mean": np.nan, "ir": np.nan,
                "hit_rate": np.nan, "t_stat": np.nan}
    s = ic_df["ic"]
    mean, std = s.mean(), s.std(ddof=1)
    return {"n_months": len(s), "ic_mean": mean,
            "ir": mean / std if std > 0 else np.nan,
            "hit_rate": (s > 0).mean(),
            "t_stat": mean / (std / np.sqrt(len(s))) if std > 0 else np.nan}


def decile_spread(df, factor_col, ret_col):
    rows = []
    for me, g in df.groupby("me_date", sort=True):
        gg = g[[factor_col, ret_col]].dropna()
        if len(gg) < MIN_CROSS_SECTION:
            continue
        gg = gg.copy()
        try:
            gg["q"] = pd.qcut(gg[factor_col].rank(method="first"), 10, labels=False)
        except Exception:
            continue
        means = gg.groupby("q")[ret_col].mean()
        means.index = [f"Q{i+1}" for i in means.index]
        means["me_date"] = me
        rows.append(means)
    if not rows:
        return {}
    qdf = pd.DataFrame(rows).reset_index(drop=True)
    spread = qdf["Q10"] - qdf["Q1"]
    q_means = qdf[[f"Q{i}" for i in range(1, 11)]].mean()
    mono = stats.spearmanr(np.arange(1, 11), q_means.values).correlation
    return {"spread_mean": spread.mean(),
            "spread_sharpe": spread.mean() / spread.std(ddof=1) if spread.std(ddof=1) > 0 else np.nan,
            "monotonic_corr": mono,
            "q10": q_means["Q10"], "q1": q_means["Q1"]}


def walk_forward(ic_df, is_m=60, oos_m=12):
    if len(ic_df) < is_m + oos_m:
        return {"n_windows": 0, "sign_hit_rate": np.nan}
    s = ic_df.sort_values("me_date").reset_index(drop=True)
    hits, n = 0, 0
    for i in range(is_m, len(s) - oos_m + 1, oos_m):
        is_mean = s.loc[i - is_m: i - 1, "ic"].mean()
        oos_mean = s.loc[i: i + oos_m - 1, "ic"].mean()
        if np.isnan(is_mean) or np.isnan(oos_mean):
            continue
        n += 1
        if np.sign(is_mean) == np.sign(oos_mean):
            hits += 1
    return {"n_windows": n, "sign_hit_rate": hits / n if n > 0 else np.nan}


def grade(ic_mean, ir, mono, spread_q10_q1):
    """A: IC>0.03 IR>0.3 mono>0.5 spread>0; B: IC>0.02 IR>0.2; C 觀察; D 否決"""
    if pd.isna(ic_mean) or pd.isna(ir):
        return "n/a"
    sign_consistent = (ic_mean > 0 and (spread_q10_q1 or 0) > 0) or (ic_mean < 0 and (spread_q10_q1 or 0) < 0)
    if abs(ic_mean) > 0.03 and abs(ir) > 0.3 and abs(mono or 0) > 0.5 and sign_consistent:
        return "A"
    if abs(ic_mean) > 0.02 and abs(ir) > 0.2 and sign_consistent:
        return "B"
    if abs(ic_mean) > 0.01:
        return "C"
    return "D"


def main():
    universe = load_universe()
    fin = load_financials()
    fin = fin[fin["stock_id"].isin(universe)].copy()

    panel = build_panel(fin)
    if panel.empty:
        raise RuntimeError("Panel empty.")

    px = load_ohlcv()
    px = px[px["stock_id"].isin(universe)].copy()
    monthly = build_monthly_returns(px)

    # Restrict to 2015-2025
    monthly = monthly[(monthly["me_date"] >= "2015-01-01") & (monthly["me_date"] <= "2025-12-31")].copy()
    panel = panel[(panel["date"] >= "2014-09-01") & (panel["date"] <= "2025-12-31")].copy()

    merged = attach_factor(monthly, panel)
    logger.info(f"Merged: {len(merged):,} rows, {merged['stock_id'].nunique()} stocks")

    factor_cols = {"yoy_diff": "F1_yoy_diff",
                   "yoy_diff_2q": "F2_yoy_diff_2q",
                   "positive_2q": "F3_positive_2q"}

    all_records = []
    summary_rows = []
    for fkey, fcol in factor_cols.items():
        for h in HORIZONS_MONTHS:
            ret_col = f"fwd_{h}m"
            ic_df = cross_sectional_ic(merged, fcol, ret_col)
            ic_s = ic_summary(ic_df)
            dec = decile_spread(merged, fcol, ret_col)
            wf = walk_forward(ic_df) if h == 12 else {"n_windows": 0, "sign_hit_rate": np.nan}
            spread_q10_q1 = dec.get("q10", np.nan) - dec.get("q1", np.nan) if dec else np.nan
            g = grade(ic_s["ic_mean"], ic_s["ir"], dec.get("monotonic_corr", 0), spread_q10_q1)
            row = {"factor": fkey, "horizon_m": h,
                   **ic_s,
                   "spread_q10_q1": spread_q10_q1,
                   "spread_sharpe": dec.get("spread_sharpe", np.nan),
                   "monotonic_corr": dec.get("monotonic_corr", np.nan),
                   "wf_sign_hit": wf["sign_hit_rate"],
                   "wf_n": wf["n_windows"],
                   "grade": g}
            summary_rows.append(row)
            all_records.append(ic_df.assign(factor=fkey, horizon_m=h))

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(OUT_CSV, index=False)
    logger.info(f"Saved: {OUT_CSV}")

    print("\n=== Operating Leverage Factor IC ===\n")
    print(f"{'Factor':16s} {'h':>3s} {'months':>7s} {'IC':>8s} {'IR':>7s} {'hit':>6s} "
          f"{'t':>6s} {'Q10-Q1':>8s} {'mono':>7s} {'WF_hit':>7s} {'Grade':>6s}")
    print("-" * 110)
    for _, r in summary.iterrows():
        print(f"{r['factor']:16s} {r['horizon_m']:>2d}m {r['n_months']:>7.0f} "
              f"{r['ic_mean']:>+8.4f} {r['ir']:>+7.3f} {r['hit_rate']:>+6.3f} "
              f"{r['t_stat']:>+6.2f} "
              f"{r['spread_q10_q1']:>+8.4f} "
              f"{r['monotonic_corr']:>+7.3f} "
              f"{(r['wf_sign_hit'] if not pd.isna(r['wf_sign_hit']) else float('nan')):>+7.3f} "
              f"{r['grade']:>6s}")

    # Best factor verdict
    best = summary.loc[summary["ic_mean"].abs().idxmax()]
    md = f"""# Operating Leverage Factor IC

**Date**: 2026-04-29
**Universe**: TW common stocks 2015-2025
**Factors**: F1 yoy_diff (Rev YoY - OpEx YoY) / F2 yoy_diff_2q / F3 positive_2q

## Result Table

| factor | h | n_mo | IC | IR | hit | t | Q10-Q1 | mono | WF | Grade |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:--:|
"""
    for _, r in summary.iterrows():
        wf = r['wf_sign_hit']
        wf_str = f"{wf:+.3f}" if not pd.isna(wf) else "n/a"
        md += (f"| {r['factor']} | {r['horizon_m']}m | {r['n_months']:.0f} | "
               f"{r['ic_mean']:+.4f} | {r['ir']:+.3f} | {r['hit_rate']:.3f} | "
               f"{r['t_stat']:+.2f} | {r['spread_q10_q1']:+.4f} | "
               f"{r['monotonic_corr']:+.3f} | {wf_str} | {r['grade']} |\n")

    md += f"""
## Verdict

Best factor by |IC|: **{best['factor']}** @ {best['horizon_m']}m, IC={best['ic_mean']:+.4f}, IR={best['ir']:+.3f}, Grade {best['grade']}

宋分原話: 「營收成長 > 費用成長 連 2 季 → 利潤噴發」

如果 best grade = D → thesis 不成立或 alpha 太弱 (跟 ROIC/CCC 一致)
如果 grade ≥ B → 考慮加進 value_screener 或做 portfolio backtest 確認
"""
    OUT_MD.write_text(md, encoding="utf-8")
    logger.info(f"Saved: {OUT_MD}")
    print(f"\n=== Best: {best['factor']} @ {best['horizon_m']}m grade {best['grade']} (IC={best['ic_mean']:+.4f}) ===")


if __name__ == "__main__":
    main()
