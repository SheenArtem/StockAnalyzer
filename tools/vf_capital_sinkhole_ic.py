"""
VF — Capital Sinkhole hard-filter validation (宋分 Value 候選 #5).

宋分原話：「資本黑洞——CAPEX > 營收 25% + FCF<0 + ROIC<WACC + 連 3 年 → 排除」

資料限制：
  - AcquisitionOfPropertyPlantAndEquipment (CFI 細項) 只有 211 stocks，無法做 CAPEX 直驗
  - 改用 PP&E book value YoY growth 當 capital deployment proxy (74k rows × 2118 stocks)
  - WACC 不算 (拼湊困難)

3 個 capital sinkhole 訊號（hard filter，不是 alpha factor）：
  F1 (ppe_yoy_growth)  = PP&E(t) / PP&E(t-4) - 1                 PP&E 增速
  F2 (cfo_to_revenue)  = TTM CFO / TTM Revenue                    現金流產出能力
  F3 (sinkhole)        = (F1 > 0.20) AND (F2 < 0)                 binary trap signal
                        即「PP&E 1 年增超 20% 但 CFO/Rev < 0」

驗證方式（同 trap_pockets pattern, not IC）：
  比較 F3=1 的 pocket vs full universe 的 fwd return
  如果 alpha @ 12m < -3% AND 5/6 年穩定負 → 落地 hard filter
  否則歸檔

資料來源:
  fundamental_cache/financials_balance.parquet  (PP&E)
  fundamental_cache/financials_cashflow.parquet (CFO)
  fundamental_cache/financials_income.parquet   (Revenue)
  fundamental_cache/ohlcv_tw.parquet             (forward returns)
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

OUT_CSV = OUT_DIR / "vf_capital_sinkhole.csv"
OUT_MD = OUT_DIR / "vf_capital_sinkhole.md"

ANNOUNCE_DELAY_DAYS = 45
HORIZONS_MONTHS = [3, 6, 12]
MIN_CROSS_SECTION = 100

PPE_GROWTH_THR = 0.20    # PP&E YoY growth > 20%
CFO_RATIO_THR = 0.0      # TTM CFO/Revenue < 0

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("sinkhole")


def _pivot_long(df, types):
    sub = df[df["type"].isin(types)].copy()
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce")
    pivot = sub.pivot_table(index=["stock_id", "date"], columns="type",
                            values="value", aggfunc="first").reset_index()
    pivot.columns.name = None
    return pivot


def load_data():
    logger.info("Loading balance / cashflow / income...")
    bal = pd.read_parquet(DATA_DIR / "financials_balance.parquet")
    bal["date"] = pd.to_datetime(bal["date"])
    bal_w = _pivot_long(bal, ["PropertyPlantAndEquipment"])
    cf = pd.read_parquet(DATA_DIR / "financials_cashflow.parquet")
    cf["date"] = pd.to_datetime(cf["date"])
    cf_w = _pivot_long(cf, ["CashFlowsFromOperatingActivities"])
    inc = pd.read_parquet(DATA_DIR / "financials_income.parquet")
    inc["date"] = pd.to_datetime(inc["date"])
    inc_w = _pivot_long(inc, ["Revenue"])

    merged = bal_w.merge(cf_w, on=["stock_id", "date"], how="outer")
    merged = merged.merge(inc_w, on=["stock_id", "date"], how="outer")
    merged = merged.sort_values(["stock_id", "date"]).reset_index(drop=True)
    logger.info(f"Merged fundamentals: {len(merged):,} rows × {merged['stock_id'].nunique()} stocks")
    return merged


def load_ohlcv():
    px = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet", columns=["stock_id", "date", "AdjClose"])
    px["date"] = pd.to_datetime(px["date"])
    px["AdjClose"] = pd.to_numeric(px["AdjClose"], errors="coerce")
    return px.dropna(subset=["AdjClose"]).query("AdjClose > 0")


def load_universe():
    u = pd.read_parquet(DATA_DIR / "universe_tw_full.parquet")
    return set(u[u["is_common_stock"] == True]["stock_id"].astype(str).unique())


def compute_signals(g):
    g = g.sort_values("date").copy()
    ppe = g["PropertyPlantAndEquipment"]
    cfo = g["CashFlowsFromOperatingActivities"]
    rev = g["Revenue"]

    # F1: PP&E YoY growth (need 5 quarters)
    g["F1_ppe_yoy"] = ppe / ppe.shift(4) - 1
    g.loc[g["F1_ppe_yoy"].abs() > 5, "F1_ppe_yoy"] = np.nan  # sanity clip

    # F2: TTM CFO / TTM Revenue
    cfo_ttm = cfo.rolling(4, min_periods=4).sum()
    rev_ttm = rev.rolling(4, min_periods=4).sum()
    g["F2_cfo_ratio"] = np.where(rev_ttm > 0, cfo_ttm / rev_ttm, np.nan)
    g.loc[g["F2_cfo_ratio"].abs() > 5, "F2_cfo_ratio"] = np.nan

    # F3: sinkhole binary
    g["F3_sinkhole"] = ((g["F1_ppe_yoy"] > PPE_GROWTH_THR) &
                       (g["F2_cfo_ratio"] < CFO_RATIO_THR)).astype(int)
    # NaN out where any input is missing
    g.loc[g["F1_ppe_yoy"].isna() | g["F2_cfo_ratio"].isna(), "F3_sinkhole"] = np.nan

    return g[["stock_id", "date", "F1_ppe_yoy", "F2_cfo_ratio", "F3_sinkhole"]]


def build_panel(fin):
    logger.info("Computing capital sinkhole signals per stock...")
    t0 = time.time()
    out = []
    for sid, g in fin.groupby("stock_id", sort=False):
        if len(g) < 6:
            continue
        out.append(compute_signals(g))
    panel = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    panel = panel.dropna(subset=["F3_sinkhole"])
    logger.info(f"Panel: {len(panel):,} rows ({time.time()-t0:.1f}s)")
    n_sinkhole = (panel["F3_sinkhole"] == 1).sum()
    logger.info(f"  F3_sinkhole=1: {n_sinkhole:,} ({n_sinkhole/len(panel)*100:.1f}%)")
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


def evaluate_pocket(merged, signal_col, threshold):
    """For binary or threshold signal, compare pocket fwd return vs universe."""
    rows = []
    for h in HORIZONS_MONTHS:
        ret = f"fwd_{h}m"
        univ = merged[ret].dropna().mean()
        univ_std = merged[ret].dropna().std(ddof=1)
        if isinstance(threshold, (int, float)) and signal_col == "F3_sinkhole":
            mask = merged[signal_col] == threshold
        else:
            mask = merged[signal_col] >= threshold
        pocket = merged.loc[mask & merged[ret].notna(), ret]
        if len(pocket) < 50:
            rows.append({"signal": signal_col, "threshold": threshold, "horizon_m": h,
                         "n": len(pocket), "pocket_mean": np.nan, "univ_mean": univ,
                         "alpha": np.nan, "snr": np.nan})
            continue
        pmean = pocket.mean()
        alpha = pmean - univ
        snr = alpha / (univ_std / np.sqrt(len(pocket))) if univ_std > 0 else np.nan
        rows.append({"signal": signal_col, "threshold": threshold, "horizon_m": h,
                     "n": len(pocket), "pocket_mean": pmean, "univ_mean": univ,
                     "alpha": alpha, "snr": snr})
    return rows


def yearly_breakdown(merged, sinkhole_col="F3_sinkhole"):
    merged = merged.copy()
    merged["year"] = merged["me_date"].dt.year
    rows = []
    for yr in sorted(merged["year"].dropna().unique()):
        for h in HORIZONS_MONTHS:
            ret = f"fwd_{h}m"
            yr_df = merged[merged["year"] == yr]
            mask = yr_df[sinkhole_col] == 1
            pocket = yr_df.loc[mask & yr_df[ret].notna(), ret]
            univ = yr_df[ret].dropna().mean()
            if len(pocket) < 30:
                continue
            rows.append({"year": int(yr), "horizon_m": h, "n": len(pocket),
                         "pocket_mean": pocket.mean(), "univ_mean": univ,
                         "alpha": pocket.mean() - univ})
    return pd.DataFrame(rows)


def main():
    universe = load_universe()
    fin = load_data()
    fin = fin[fin["stock_id"].isin(universe)].copy()

    panel = build_panel(fin)
    if panel.empty:
        raise RuntimeError("Panel empty.")

    px = load_ohlcv()
    px = px[px["stock_id"].isin(universe)].copy()
    monthly = build_monthly_returns(px)
    monthly = monthly[(monthly["me_date"] >= "2015-01-01") &
                      (monthly["me_date"] <= "2025-12-31")].copy()
    panel = panel[(panel["date"] >= "2014-09-01") &
                  (panel["date"] <= "2025-12-31")].copy()

    merged = attach_factor(monthly, panel)
    logger.info(f"Merged: {len(merged):,} rows")

    # Evaluate F3 binary trap
    rows_f3 = evaluate_pocket(merged, "F3_sinkhole", 1)
    rows_neg_f3 = evaluate_pocket(merged, "F3_sinkhole", 0)  # universe minus sinkhole

    print(f"\n=== Capital Sinkhole Pocket Analysis ===\n")
    print(f"{'Signal':25s} {'thr':>5s} {'h':>3s} {'n':>7s} {'pocket':>10s} {'univ':>10s} {'alpha':>10s} {'snr':>8s}")
    print("-" * 95)
    for r in rows_f3 + rows_neg_f3:
        print(f"{r['signal']:25s} {r['threshold']:>5} {r['horizon_m']:>2}m {r['n']:>7} "
              f"{r['pocket_mean']:>+10.4f} {r['univ_mean']:>+10.4f} "
              f"{r['alpha']:>+10.4f} {r['snr']:>+8.2f}")

    # Yearly breakdown
    yr_df = yearly_breakdown(merged, "F3_sinkhole")
    print("\n=== Yearly breakdown for F3=1 sinkhole pocket ===")
    print(f"{'Year':>5s} {'h':>3s} {'n':>6s} {'pocket':>10s} {'univ':>10s} {'alpha':>10s}")
    for _, r in yr_df.iterrows():
        print(f"{r['year']:>5} {r['horizon_m']:>2}m {r['n']:>6} "
              f"{r['pocket_mean']:>+10.4f} {r['univ_mean']:>+10.4f} "
              f"{r['alpha']:>+10.4f}")

    # Save
    summary = pd.DataFrame(rows_f3 + rows_neg_f3)
    summary.to_csv(OUT_CSV, index=False)
    yr_df.to_csv(OUT_DIR / "vf_capital_sinkhole_yearly.csv", index=False)
    logger.info(f"Saved: {OUT_CSV}")

    # Verdict
    f3_12m = next((r for r in rows_f3 if r["horizon_m"] == 12), None)
    if f3_12m and not pd.isna(f3_12m["alpha"]):
        years_neg = (yr_df[yr_df["horizon_m"] == 12]["alpha"] < 0).sum()
        years_total = len(yr_df[yr_df["horizon_m"] == 12])
        verdict = "LAND" if (f3_12m["alpha"] <= -0.03 and abs(f3_12m["snr"]) >= 2.0
                              and years_neg >= 5) else \
                  "WEAK" if (f3_12m["alpha"] <= -0.01) else "PLAIN"
        print(f"\n=== Verdict: F3 sinkhole 12m alpha={f3_12m['alpha']:+.4f} "
              f"snr={f3_12m['snr']:+.2f} years_neg={years_neg}/{years_total} -> {verdict} ===")

    md = f"""# Capital Sinkhole Hard Filter Validation

**Date**: 2026-04-29
**Method**: pocket alpha analysis (not IC factor)
**Limitation**: AcquisitionOfPPE 只 211 stocks 用 PP&E book value YoY growth proxy

## Signals

- F1 (PP&E YoY growth) > {PPE_GROWTH_THR*100}%
- F2 (TTM CFO / TTM Revenue) < {CFO_RATIO_THR}
- F3 (sinkhole binary): F1 AND F2 both fire → 「擴張中燒錢」

## Pocket Analysis Table

| signal | thr | h | n | pocket | univ | alpha | snr |
|---|---:|---:|---:|---:|---:|---:|---:|
"""
    for r in rows_f3 + rows_neg_f3:
        md += (f"| {r['signal']} | {r['threshold']} | {r['horizon_m']}m | {r['n']} | "
               f"{r['pocket_mean']:+.4f} | {r['univ_mean']:+.4f} | "
               f"{r['alpha']:+.4f} | {r['snr']:+.2f} |\n")
    md += "\n## Yearly breakdown (F3=1)\n\n"
    md += "| Year | h | n | pocket | univ | alpha |\n|---:|---:|---:|---:|---:|---:|\n"
    for _, r in yr_df.iterrows():
        md += (f"| {r['year']} | {r['horizon_m']}m | {r['n']} | "
               f"{r['pocket_mean']:+.4f} | {r['univ_mean']:+.4f} | {r['alpha']:+.4f} |\n")

    if f3_12m and not pd.isna(f3_12m["alpha"]):
        years_neg = (yr_df[yr_df["horizon_m"] == 12]["alpha"] < 0).sum()
        years_total = len(yr_df[yr_df["horizon_m"] == 12])
        md += f"""
## Verdict

F3 sinkhole pocket @ 12m: alpha={f3_12m['alpha']:+.4f} snr={f3_12m['snr']:+.2f} years_neg={years_neg}/{years_total}

落地門檻：alpha <= -3% AND |snr| >= 2.0 AND years_neg >= 5
"""
    OUT_MD.write_text(md, encoding="utf-8")


if __name__ == "__main__":
    main()
