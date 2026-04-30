"""
VF — DuPont 三因子 IC validation (docs/project_review_and_roadmap.md 「下一輪 4 件待開工」#2).

DuPont 拆解 ROE = NetMargin x AssetTurnover x EquityMultiplier
  F1 (NM)  Net Margin       = IncomeAfterTaxes(TTM) / Revenue(TTM)
  F2 (AT)  Asset Turnover   = Revenue(TTM) / AvgTotalAssets
  F3 (EM)  Equity Multiplier = TotalAssets(t) / Equity(t)
  F4 (ROE) NetMargin x AssetTurnover x EquityMultiplier  (sanity check)

設計目的: DuPont 傳統用法是 quality filter (砍偽價值股: 高 ROE 但靠高負債撐),
不一定是 alpha factor。但仍跑 IC 確認三個分量是否有 cross-section alpha。

Robustness:
- 45-day announce delay
- TTM revenue / IncomeAfterTaxes (4q rolling sum)
- AvgTotalAssets = (TotalAssets(t) + TotalAssets(t-4)) / 2  (避免 quarterly noise)
- 異常過濾:
    |F1| > 1            (淨利率 > 100%)
    F2 < 0 or > 5       (資產周轉率)
    F3 < 0 or > 20      (權益乘數)
- Min cross-section: 100 stocks per month

R1-R6:
  R1 IC mean / IR / hit_rate over horizons 1m/3m/6m/12m
  R2 Decile spread (Q10-Q1) Sharpe + monotonicity (sign 必須與 IC 一致)
  R3 Walk-forward (60M IS -> 12M OOS) sign-stability
  R4 Regime breakdown (TWII 200d MA bull/bear)
  R5 Correlation with F-Score quality_score
  R6 Quality filter: high-ROE x low-EM vs high-ROE x high-EM forward return

Output:
  reports/vf_dupont_factor_ic.csv
  reports/vf_dupont_factor_ic.md
CLI: python tools/vf_dupont_factor_ic.py
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))

DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "vf_dupont_factor_ic.csv"
OUT_MD = OUT_DIR / "vf_dupont_factor_ic.md"

ANNOUNCE_DELAY_DAYS = 45
HORIZONS_MONTHS = [1, 3, 6, 12]
MIN_CROSS_SECTION = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dupont_ic")


# ============================================================
# Loaders
# ============================================================
def _pivot_long(df: pd.DataFrame, types: list[str]) -> pd.DataFrame:
    sub = df[df["type"].isin(types)].copy()
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce")
    pivot = sub.pivot_table(
        index=["stock_id", "date"], columns="type", values="value", aggfunc="first"
    ).reset_index()
    pivot.columns.name = None
    return pivot


def load_financials() -> pd.DataFrame:
    logger.info("Loading financials_income.parquet ...")
    inc_long = pd.read_parquet(DATA_DIR / "financials_income.parquet")
    inc_long["date"] = pd.to_datetime(inc_long["date"])

    inc = _pivot_long(inc_long, ["Revenue", "IncomeAfterTaxes"])
    inc = inc.sort_values(["stock_id", "date"]).reset_index(drop=True)

    if "Revenue" not in inc.columns:
        raise RuntimeError("Revenue missing in income.")
    if "IncomeAfterTaxes" not in inc.columns:
        raise RuntimeError("IncomeAfterTaxes missing in income.")

    logger.info(
        f"Income wide: {len(inc):,} rows, {inc['stock_id'].nunique()} stocks, "
        f"{inc['date'].min().date()} ~ {inc['date'].max().date()}"
    )
    return inc


def load_balance() -> pd.DataFrame:
    logger.info("Loading financials_balance.parquet ...")
    bal_long = pd.read_parquet(DATA_DIR / "financials_balance.parquet")
    bal_long["date"] = pd.to_datetime(bal_long["date"])

    bal = _pivot_long(bal_long, ["TotalAssets", "Equity"])
    bal = bal.sort_values(["stock_id", "date"]).reset_index(drop=True)

    if "TotalAssets" not in bal.columns:
        raise RuntimeError("TotalAssets missing in balance.")
    if "Equity" not in bal.columns:
        raise RuntimeError("Equity missing in balance.")

    logger.info(
        f"Balance wide: {len(bal):,} rows, {bal['stock_id'].nunique()} stocks, "
        f"{bal['date'].min().date()} ~ {bal['date'].max().date()}"
    )
    return bal


def load_ohlcv() -> pd.DataFrame:
    logger.info("Loading ohlcv_tw.parquet ...")
    px = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet")
    px["date"] = pd.to_datetime(px["date"])
    px = px[["stock_id", "date", "AdjClose"]].copy()
    px["AdjClose"] = pd.to_numeric(px["AdjClose"], errors="coerce")
    px = px.dropna(subset=["AdjClose"])
    px = px[px["AdjClose"] > 0]
    logger.info(
        f"OHLCV: {len(px):,} rows, {px['stock_id'].nunique()} stocks, "
        f"{px['date'].min().date()} ~ {px['date'].max().date()}"
    )
    return px


def load_universe_common() -> set[str]:
    u = pd.read_parquet(DATA_DIR / "universe_tw_full.parquet")
    keep = u[u["is_common_stock"] == True]["stock_id"].astype(str).unique()  # noqa: E712
    logger.info(f"Universe (common stock): {len(keep)} tickers")
    return set(keep)


def load_quality() -> pd.DataFrame:
    q = pd.read_parquet(DATA_DIR / "quality_scores.parquet")
    q["date"] = pd.to_datetime(q["date"])
    return q[["stock_id", "date", "f_score", "quality_score"]]


# ============================================================
# DuPont per-stock computation
# ============================================================
def compute_dupont_per_stock(g: pd.DataFrame) -> pd.DataFrame:
    """
    Input: 單一 stock 的季資料 (Revenue, IncomeAfterTaxes, TotalAssets, Equity)
    Output: 加上 F1_nm / F2_at / F3_em / F4_roe
    """
    g = g.sort_values("date").copy()

    # TTM
    g["Rev_TTM"] = g["Revenue"].rolling(4, min_periods=4).sum()
    g["NI_TTM"] = g["IncomeAfterTaxes"].rolling(4, min_periods=4).sum()

    # AvgTotalAssets: (TotalAssets(t) + TotalAssets(t-4)) / 2
    g["AvgTA"] = (g["TotalAssets"] + g["TotalAssets"].shift(4)) / 2.0

    # F1: Net Margin (TTM)
    with np.errstate(divide="ignore", invalid="ignore"):
        g["F1_nm"] = np.where(g["Rev_TTM"] > 0, g["NI_TTM"] / g["Rev_TTM"], np.nan)
    # filter |F1| > 1
    g.loc[g["F1_nm"].abs() > 1.0, "F1_nm"] = np.nan

    # F2: Asset Turnover (TTM rev / avg TA)
    with np.errstate(divide="ignore", invalid="ignore"):
        g["F2_at"] = np.where(g["AvgTA"] > 0, g["Rev_TTM"] / g["AvgTA"], np.nan)
    g.loc[(g["F2_at"] < 0) | (g["F2_at"] > 5), "F2_at"] = np.nan

    # F3: Equity Multiplier (point-in-time)
    with np.errstate(divide="ignore", invalid="ignore"):
        g["F3_em"] = np.where(g["Equity"] > 0, g["TotalAssets"] / g["Equity"], np.nan)
    g.loc[(g["F3_em"] < 0) | (g["F3_em"] > 20), "F3_em"] = np.nan

    # F4: synthesized ROE (DuPont identity)
    g["F4_roe"] = g["F1_nm"] * g["F2_at"] * g["F3_em"]

    return g[
        [
            "stock_id",
            "date",
            "Rev_TTM",
            "NI_TTM",
            "AvgTA",
            "F1_nm",
            "F2_at",
            "F3_em",
            "F4_roe",
        ]
    ]


def build_dupont_panel(fin_inc: pd.DataFrame, fin_bal: pd.DataFrame) -> pd.DataFrame:
    logger.info("Merging income + balance ...")
    fin = pd.merge(fin_inc, fin_bal, on=["stock_id", "date"], how="inner")
    logger.info(f"Merged income+balance: {len(fin):,} rows, {fin['stock_id'].nunique()} stocks")

    logger.info("Computing DuPont factors per stock ...")
    t0 = time.time()
    out = []
    n_total = fin["stock_id"].nunique()
    n_done = 0
    for sid, g in fin.groupby("stock_id", sort=False):
        if len(g) < 5:
            continue
        out.append(compute_dupont_per_stock(g))
        n_done += 1
        if n_done % 500 == 0:
            logger.info(f"  computed {n_done}/{n_total} stocks ...")
    panel = pd.concat(out, ignore_index=True) if out else pd.DataFrame()

    # require at least one factor
    panel = panel.dropna(subset=["F1_nm", "F2_at", "F3_em"], how="all")
    logger.info(
        f"DuPont panel: {len(panel):,} rows, {panel['stock_id'].nunique()} stocks "
        f"({time.time()-t0:.1f}s)"
    )

    # Coverage check
    for col in ["F1_nm", "F2_at", "F3_em", "F4_roe"]:
        nn = panel[col].notna().sum()
        logger.info(f"  {col}: {nn:,} non-null ({100*nn/len(panel):.1f}%)")

    return panel


# ============================================================
# Forward returns (monthly)
# ============================================================
def build_monthly_returns(px: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building monthly forward returns ...")
    px = px.sort_values(["stock_id", "date"])
    px["ym"] = px["date"].dt.to_period("M")
    monthly = px.groupby(["stock_id", "ym"], as_index=False).tail(1).copy()
    monthly = monthly[["stock_id", "ym", "date", "AdjClose"]].rename(
        columns={"date": "me_date", "AdjClose": "px"}
    )
    monthly["me_date"] = monthly["ym"].dt.to_timestamp("M")
    monthly = monthly.sort_values(["stock_id", "ym"]).reset_index(drop=True)

    monthly["px"] = monthly["px"].astype(float)
    g = monthly.groupby("stock_id", sort=False)
    for h in HORIZONS_MONTHS:
        monthly[f"fwd_{h}m"] = g["px"].shift(-h) / monthly["px"] - 1
    return monthly


def attach_factor_to_monthly(
    monthly: pd.DataFrame, panel: pd.DataFrame
) -> pd.DataFrame:
    logger.info(f"Merging DuPont factors with {ANNOUNCE_DELAY_DAYS}-day announce delay ...")
    panel = panel.copy()
    panel["available_from"] = panel["date"] + pd.Timedelta(days=ANNOUNCE_DELAY_DAYS)
    panel = panel.sort_values(["available_from", "stock_id"]).reset_index(drop=True)
    monthly = monthly.sort_values(["me_date", "stock_id"]).reset_index(drop=True)

    out = pd.merge_asof(
        monthly,
        panel[
            [
                "stock_id",
                "available_from",
                "F1_nm",
                "F2_at",
                "F3_em",
                "F4_roe",
            ]
        ],
        left_on="me_date",
        right_on="available_from",
        by="stock_id",
        direction="backward",
        allow_exact_matches=True,
    )
    return out


# ============================================================
# IC / IR / decile / regime
# ============================================================
def cross_sectional_ic(
    df: pd.DataFrame, factor_col: str, ret_col: str
) -> pd.DataFrame:
    out = []
    for me, g in df.groupby("me_date", sort=True):
        gg = g[[factor_col, ret_col]].dropna()
        if len(gg) < MIN_CROSS_SECTION:
            continue
        try:
            r, p = stats.spearmanr(gg[factor_col].values, gg[ret_col].values)
        except Exception:
            continue
        if np.isnan(r):
            continue
        out.append({"me_date": me, "n": len(gg), "ic": r, "p": p})
    return pd.DataFrame(out)


def ic_summary(ic_df: pd.DataFrame) -> dict:
    if ic_df.empty:
        return {
            "n_months": 0,
            "ic_mean": np.nan,
            "ic_std": np.nan,
            "ir": np.nan,
            "hit_rate": np.nan,
            "t_stat": np.nan,
        }
    s = ic_df["ic"]
    mean = s.mean()
    std = s.std(ddof=1)
    return {
        "n_months": len(s),
        "ic_mean": mean,
        "ic_std": std,
        "ir": mean / std if std > 0 else np.nan,
        "hit_rate": (s > 0).mean(),
        "t_stat": mean / (std / np.sqrt(len(s))) if std > 0 else np.nan,
    }


def decile_spread(df: pd.DataFrame, factor_col: str, ret_col: str) -> dict:
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
        return {
            "spread_mean": np.nan,
            "spread_std": np.nan,
            "spread_sharpe": np.nan,
            "monotonic_corr": np.nan,
            "q10_mean": np.nan,
            "q1_mean": np.nan,
            "q_means": {},
        }
    qdf = pd.DataFrame(rows).reset_index(drop=True)
    spread = qdf["Q10"] - qdf["Q1"]
    q_means = qdf[[f"Q{i}" for i in range(1, 11)]].mean()
    mono = stats.spearmanr(np.arange(1, 11), q_means.values).correlation
    return {
        "spread_mean": spread.mean(),
        "spread_std": spread.std(ddof=1),
        "spread_sharpe": (
            spread.mean() / spread.std(ddof=1) if spread.std(ddof=1) > 0 else np.nan
        ),
        "monotonic_corr": mono,
        "q10_mean": q_means["Q10"],
        "q1_mean": q_means["Q1"],
        "q_means": q_means.to_dict(),
    }


def walk_forward_sign_stability(
    ic_df: pd.DataFrame, is_months: int = 60, oos_months: int = 12
) -> dict:
    if len(ic_df) < is_months + oos_months:
        return {"n_windows": 0, "sign_hit_rate": np.nan, "all_oos_means": []}
    s = ic_df.sort_values("me_date").reset_index(drop=True)
    hits = 0
    n = 0
    oos_means = []
    for i in range(is_months, len(s) - oos_months + 1, oos_months):
        is_mean = s.loc[i - is_months : i - 1, "ic"].mean()
        oos_mean = s.loc[i : i + oos_months - 1, "ic"].mean()
        if np.isnan(is_mean) or np.isnan(oos_mean):
            continue
        n += 1
        oos_means.append(oos_mean)
        if np.sign(is_mean) == np.sign(oos_mean):
            hits += 1
    return {
        "n_windows": n,
        "sign_hit_rate": hits / n if n > 0 else np.nan,
        "all_oos_means": oos_means,
    }


def leave_one_year_out(ic_df: pd.DataFrame) -> dict:
    """
    SOP4: leave-one-year-out check — IC mean 對單一極端年的依賴。
    """
    if ic_df.empty:
        return {"loo_min": np.nan, "loo_max": np.nan, "all_year_signs": []}
    df = ic_df.copy()
    df["year"] = pd.to_datetime(df["me_date"]).dt.year
    full_mean = df["ic"].mean()
    years = sorted(df["year"].unique())
    loo_means = []
    for y in years:
        m = df[df["year"] != y]["ic"].mean()
        loo_means.append({"year_excluded": y, "ic_mean_loo": m})
    loo_df = pd.DataFrame(loo_means)
    sign_full = np.sign(full_mean)
    sign_loo_min = np.sign(loo_df["ic_mean_loo"].min())
    sign_loo_max = np.sign(loo_df["ic_mean_loo"].max())
    return {
        "full_mean": full_mean,
        "loo_min": loo_df["ic_mean_loo"].min(),
        "loo_max": loo_df["ic_mean_loo"].max(),
        "all_signs_consistent": (sign_loo_min == sign_loo_max == sign_full),
        "loo_table": loo_df.to_dict("records"),
    }


def regime_breakdown(
    df: pd.DataFrame, twii: pd.DataFrame, factor_col: str, ret_col: str
) -> dict:
    df = df.merge(twii[["me_date", "regime"]], on="me_date", how="left")
    out = {}
    for r, sub in df.groupby("regime"):
        ic_d = cross_sectional_ic(sub, factor_col, ret_col)
        s = ic_summary(ic_d)
        s["regime"] = r
        out[r] = s
    return out


def build_twii_regime() -> pd.DataFrame:
    p = DATA_DIR / "_twii_bench.parquet"
    if not p.exists():
        logger.warning(f"TWII bench not found: {p}")
        return pd.DataFrame(columns=["me_date", "regime"])
    raw = pd.read_parquet(p)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    twii = raw.reset_index().rename(columns={"Date": "date"})
    twii["date"] = pd.to_datetime(twii["date"])
    twii = twii.sort_values("date").reset_index(drop=True)
    cl_col = "Close"
    twii["ma200"] = twii[cl_col].rolling(200, min_periods=100).mean()
    twii["regime"] = np.where(twii[cl_col] >= twii["ma200"], "bull", "bear")
    twii["ym"] = twii["date"].dt.to_period("M")
    me = twii.groupby("ym").tail(1).copy()
    me["me_date"] = me["ym"].dt.to_timestamp("M")
    return me[["me_date", "regime"]]


# ============================================================
# R6: DuPont as quality filter
# ============================================================
def quality_filter_grid(
    merged: pd.DataFrame, ret_col: str = "fwd_6m"
) -> dict:
    """
    在每個月底:
      1. 計算 ROE = F4_roe (=F1*F2*F3) 三分位 (low/mid/high)
      2. 計算 EM = F3_em 三分位 (low/mid/high)
      3. 把 universe 切成 3x3 grid, 計算每 cell 的 forward return mean / Sharpe

    重點比較:
      A. high-ROE x low-EM (低槓桿真價值)  -> 應該是最好
      B. high-ROE x high-EM (高槓桿撐 ROE) -> 應該明顯較差
      A - B spread 是 DuPont quality filter 的真正 alpha
    """
    cells = {}  # (roe_t, em_t) -> [monthly mean returns]
    n_months_used = 0
    for me, g in merged.groupby("me_date", sort=True):
        gg = g[["F4_roe", "F3_em", ret_col]].dropna()
        if len(gg) < MIN_CROSS_SECTION:
            continue
        try:
            gg = gg.copy()
            gg["roe_t"] = pd.qcut(
                gg["F4_roe"].rank(method="first"),
                3,
                labels=["low", "mid", "high"],
            )
            gg["em_t"] = pd.qcut(
                gg["F3_em"].rank(method="first"),
                3,
                labels=["low", "mid", "high"],
            )
        except Exception:
            continue
        for (rt, et), sub in gg.groupby(["roe_t", "em_t"], observed=False):
            key = (str(rt), str(et))
            cells.setdefault(key, []).append(sub[ret_col].mean())
        n_months_used += 1

    cell_stats = {}
    for k, vs in cells.items():
        vs = np.array(vs, dtype=float)
        vs = vs[~np.isnan(vs)]
        if len(vs) == 0:
            continue
        m = vs.mean()
        sd = vs.std(ddof=1) if len(vs) > 1 else np.nan
        cell_stats[k] = {
            "n_months": len(vs),
            "mean_ret": m,
            "std_ret": sd,
            "sharpe": m / sd if sd and sd > 0 else np.nan,
        }
    return {"n_months_used": n_months_used, "cells": cell_stats}


# ============================================================
# Driver
# ============================================================
def run(start: str, end: str) -> dict:
    universe = load_universe_common()
    fin_inc = load_financials()
    fin_bal = load_balance()
    fin_inc = fin_inc[fin_inc["stock_id"].isin(universe)].copy()
    fin_bal = fin_bal[fin_bal["stock_id"].isin(universe)].copy()

    panel = build_dupont_panel(fin_inc, fin_bal)
    if panel.empty:
        raise RuntimeError("DuPont panel empty.")

    panel = panel[
        (panel["date"] >= pd.Timestamp(start) - pd.Timedelta(days=400))
        & (panel["date"] <= pd.Timestamp(end))
    ].copy()

    px = load_ohlcv()
    px = px[px["stock_id"].isin(universe)].copy()
    monthly = build_monthly_returns(px)
    monthly = monthly[
        (monthly["me_date"] >= pd.Timestamp(start))
        & (monthly["me_date"] <= pd.Timestamp(end))
    ].copy()

    merged = attach_factor_to_monthly(monthly, panel)
    logger.info(
        f"Merged grid: {len(merged):,} rows, {merged['stock_id'].nunique()} stocks, "
        f"{merged['me_date'].min()} ~ {merged['me_date'].max()}"
    )

    twii = build_twii_regime()

    factor_cols = {
        "nm": "F1_nm",
        "at": "F2_at",
        "em": "F3_em",
        "roe": "F4_roe",
    }

    all_ic_records = []
    summary = {}

    for fkey, fcol in factor_cols.items():
        logger.info(f"==== Factor {fkey} ({fcol}) ====")
        per_factor = {}
        for h in HORIZONS_MONTHS:
            ret_col = f"fwd_{h}m"
            ic_df = cross_sectional_ic(merged, fcol, ret_col)
            if not ic_df.empty:
                ic_df = ic_df.assign(factor=fkey, horizon_m=h)
                all_ic_records.append(ic_df)
            stats_h = ic_summary(ic_df)
            spread_h = decile_spread(merged, fcol, ret_col)
            wf_h = walk_forward_sign_stability(ic_df)
            loo_h = leave_one_year_out(ic_df)
            per_factor[f"h{h}m"] = {
                "ic": stats_h,
                "decile": spread_h,
                "walk_forward": wf_h,
                "loo": loo_h,
            }
            wf_str = (
                f"{wf_h['sign_hit_rate']:.2f}"
                if wf_h["sign_hit_rate"] is not None
                and not (isinstance(wf_h["sign_hit_rate"], float) and np.isnan(wf_h["sign_hit_rate"]))
                else "NA"
            )
            logger.info(
                f"  h={h}m: n={stats_h['n_months']}, IC={stats_h['ic_mean']:+.4f} "
                f"IR={stats_h['ir']:+.3f} hit={stats_h['hit_rate']:.2f} | "
                f"spread Sharpe={spread_h['spread_sharpe']:+.3f} mono={spread_h['monotonic_corr']:+.3f} | "
                f"WF sign-hit={wf_str} | LOO consistent={loo_h.get('all_signs_consistent')}"
            )

        regime_h6 = regime_breakdown(merged, twii, fcol, "fwd_6m")
        per_factor["regime_h6m"] = regime_h6
        if regime_h6:
            for r, st in regime_h6.items():
                logger.info(
                    f"  regime={r}: n={st['n_months']}, IC={st['ic_mean']:+.4f} "
                    f"IR={st['ir']:+.3f}"
                )
        summary[fkey] = per_factor

    # R5 corr with F-Score
    quality = load_quality()
    quality["available_from"] = quality["date"] + pd.Timedelta(days=ANNOUNCE_DELAY_DAYS)
    quality = quality.sort_values(["available_from", "stock_id"])
    merged_q = pd.merge_asof(
        merged.sort_values(["me_date", "stock_id"]),
        quality[["stock_id", "available_from", "f_score", "quality_score"]],
        left_on="me_date",
        right_on="available_from",
        by="stock_id",
        direction="backward",
        allow_exact_matches=True,
    )
    fscore_corr = {}
    for fkey, fcol in factor_cols.items():
        sub = merged_q[[fcol, "quality_score", "f_score"]].dropna()
        if len(sub) < 1000:
            fscore_corr[fkey] = {"n": len(sub), "corr_quality": np.nan, "corr_fscore": np.nan}
            continue
        cq = stats.spearmanr(sub[fcol], sub["quality_score"]).correlation
        cf = stats.spearmanr(sub[fcol], sub["f_score"]).correlation
        fscore_corr[fkey] = {
            "n": len(sub),
            "corr_quality": cq,
            "corr_fscore": cf,
        }
        logger.info(
            f"R5 {fkey}: rho(factor, quality_score)={cq:+.3f}, rho(factor, f_score)={cf:+.3f}, n={len(sub)}"
        )
    summary["_fscore_corr"] = fscore_corr

    # Inter-factor (NM/AT/EM 三者間相關性)
    inter = {}
    pairs = [("nm", "at"), ("nm", "em"), ("at", "em"), ("nm", "roe"), ("at", "roe"), ("em", "roe")]
    for a, b in pairs:
        ca, cb = factor_cols[a], factor_cols[b]
        sub = merged[[ca, cb]].dropna()
        if len(sub) < 1000:
            continue
        rho = stats.spearmanr(sub[ca], sub[cb]).correlation
        inter[f"{a}_vs_{b}"] = {"n": len(sub), "rho": rho}
        logger.info(f"R5 inter-factor rho({a}, {b}) = {rho:+.3f}, n={len(sub)}")
    summary["_inter_factor"] = inter

    # R6 quality filter (high ROE x low EM vs high ROE x high EM)
    logger.info("==== R6 Quality filter (3x3 ROE x EM grid, h=6m) ====")
    qf = quality_filter_grid(merged, ret_col="fwd_6m")
    summary["_quality_filter"] = qf
    cells = qf["cells"]
    for k, v in sorted(cells.items()):
        logger.info(
            f"  ROE={k[0]:>4} x EM={k[1]:>4}: n_m={v['n_months']}, "
            f"mean_ret={v['mean_ret']*100:+.3f}%, sharpe={v['sharpe']:+.3f}"
        )
    a = cells.get(("high", "low"))
    b = cells.get(("high", "high"))
    if a and b:
        spread = a["mean_ret"] - b["mean_ret"]
        logger.info(
            f"  R6 KEY: high-ROE x low-EM mean = {a['mean_ret']*100:+.3f}%, "
            f"high-ROE x high-EM mean = {b['mean_ret']*100:+.3f}%, "
            f"spread = {spread*100:+.3f}% (per 6m)"
        )
        summary["_r6_summary"] = {
            "high_roe_low_em_mean": a["mean_ret"],
            "high_roe_high_em_mean": b["mean_ret"],
            "spread": spread,
        }

    if all_ic_records:
        raw = pd.concat(all_ic_records, ignore_index=True)
        raw.to_csv(OUT_CSV, index=False)
        logger.info(f"Wrote {OUT_CSV}")

    return summary


# ============================================================
# Markdown report
# ============================================================
def grade(ic_mean, ir, mono=None, sharpe=None) -> str:
    if np.isnan(ic_mean) or np.isnan(ir):
        return "D"

    inconsistent = False
    if mono is not None and not np.isnan(mono) and sharpe is not None and not np.isnan(sharpe):
        if np.sign(ic_mean) != np.sign(sharpe):
            inconsistent = True
        if np.sign(ic_mean) > 0 and mono < 0:
            inconsistent = True
        if np.sign(ic_mean) < 0 and mono > 0:
            inconsistent = True

    if inconsistent:
        return "C*"

    abs_ic = abs(ic_mean)
    abs_ir = abs(ir)
    if abs_ic >= 0.03 and abs_ir >= 0.3:
        return "A"
    if abs_ic >= 0.02 and abs_ir >= 0.2:
        return "B"
    if abs_ic >= 0.01:
        return "C"
    return "D"


def render_report(summary: dict, start: str, end: str) -> str:
    lines = []
    lines.append("# VF — DuPont 三因子 IC validation")
    lines.append("")
    lines.append(f"- Window: {start} ~ {end}")
    lines.append("- Universe: 普通股 (universe_tw_full.is_common_stock)")
    lines.append(f"- Announce delay: {ANNOUNCE_DELAY_DAYS} 天")
    lines.append(f"- Min cross-section: {MIN_CROSS_SECTION} stocks per month")
    lines.append("")
    lines.append("## 因子定義")
    lines.append("")
    lines.append("- **F1 (nm)**  Net Margin = IncomeAfterTaxes(TTM) / Revenue(TTM)")
    lines.append("- **F2 (at)**  Asset Turnover = Revenue(TTM) / AvgTotalAssets")
    lines.append("- **F3 (em)**  Equity Multiplier = TotalAssets(t) / Equity(t)")
    lines.append("- **F4 (roe)** synthesized = F1 x F2 x F3 (DuPont identity sanity check)")
    lines.append("")
    lines.append("AvgTotalAssets = (TotalAssets(t) + TotalAssets(t-4)) / 2")
    lines.append("")

    lines.append("## R1-R3 一覽")
    lines.append("")
    lines.append("| Factor | Horizon | n | IC | IR | hit | Sp Sharpe | Mono | WF sign-hit | LOO | Grade |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for fkey in ["nm", "at", "em", "roe"]:
        if fkey not in summary:
            continue
        for h in HORIZONS_MONTHS:
            block = summary[fkey].get(f"h{h}m")
            if not block:
                continue
            ic = block["ic"]
            sp = block["decile"]
            wf = block["walk_forward"]
            loo = block["loo"]
            g = grade(ic["ic_mean"], ic["ir"], sp.get("monotonic_corr"), sp.get("spread_sharpe"))
            wf_s = wf["sign_hit_rate"]
            wf_str = (
                f"{wf_s:.2f}" if wf_s is not None and not (isinstance(wf_s, float) and np.isnan(wf_s)) else "NA"
            )
            loo_str = "Y" if loo.get("all_signs_consistent") else "N"
            lines.append(
                f"| {fkey} | {h}m | {ic['n_months']} | "
                f"{ic['ic_mean']:+.4f} | {ic['ir']:+.3f} | "
                f"{ic['hit_rate']:.2f} | "
                f"{sp['spread_sharpe']:+.3f} | "
                f"{sp['monotonic_corr']:+.3f} | "
                f"{wf_str} | "
                f"{loo_str} | "
                f"{g} |"
            )
    lines.append("")

    # R4 regime
    lines.append("## R4 Regime breakdown (TWII 200d MA, h=6m)")
    lines.append("")
    lines.append("| Factor | Regime | n | IC | IR | hit |")
    lines.append("|---|---|---|---|---|---|")
    for fkey in ["nm", "at", "em", "roe"]:
        if fkey not in summary:
            continue
        rg = summary[fkey].get("regime_h6m", {})
        for r in ["bull", "bear"]:
            st = rg.get(r)
            if not st:
                continue
            lines.append(
                f"| {fkey} | {r} | {st['n_months']} | "
                f"{st['ic_mean']:+.4f} | {st['ir']:+.3f} | {st['hit_rate']:.2f} |"
            )
    lines.append("")

    # R5
    lines.append("## R5 與 F-Score 相關性 (Spearman)")
    lines.append("")
    fc = summary.get("_fscore_corr", {})
    lines.append("| Factor | n | rho(factor, quality_score) | rho(factor, f_score) | 增量價值 |")
    lines.append("|---|---|---|---|---|")
    for fkey in ["nm", "at", "em", "roe"]:
        d = fc.get(fkey)
        if not d:
            continue
        cq = d.get("corr_quality")
        cf = d.get("corr_fscore")
        cq_s = f"{cq:+.3f}" if cq is not None and not (isinstance(cq, float) and np.isnan(cq)) else "NA"
        cf_s = f"{cf:+.3f}" if cf is not None and not (isinstance(cf, float) and np.isnan(cf)) else "NA"
        if cq is None or (isinstance(cq, float) and np.isnan(cq)):
            inc = "NA"
        elif abs(cq) < 0.3:
            inc = "高(獨立)"
        elif abs(cq) < 0.5:
            inc = "中"
        else:
            inc = "低(共線)"
        lines.append(f"| {fkey} | {d['n']} | {cq_s} | {cf_s} | {inc} |")
    lines.append("")

    # Inter-factor
    inter = summary.get("_inter_factor", {})
    if inter:
        lines.append("## Inter-factor 相關性 (DuPont 三分量間)")
        lines.append("")
        lines.append("| pair | n | rho |")
        lines.append("|---|---|---|")
        for k, v in inter.items():
            lines.append(f"| {k} | {v['n']} | {v['rho']:+.3f} |")
        lines.append("")

    # Decile breakdown for h=6m
    lines.append("## Decile Q1-Q10 平均月報酬 (h=6m)")
    lines.append("")
    lines.append("| Factor | " + " | ".join([f"Q{i}" for i in range(1, 11)]) + " |")
    lines.append("|" + "|".join(["---"] * 11) + "|")
    for fkey in ["nm", "at", "em", "roe"]:
        if fkey not in summary:
            continue
        sp = summary[fkey].get("h6m", {}).get("decile", {})
        qm = sp.get("q_means", {})
        if not qm:
            continue
        cells = [
            f"{qm.get(f'Q{i}', np.nan)*100:+.2f}%"
            if not np.isnan(qm.get(f"Q{i}", np.nan)) else "NA"
            for i in range(1, 11)
        ]
        lines.append(f"| {fkey} | " + " | ".join(cells) + " |")
    lines.append("")

    # R6 quality filter
    lines.append("## R6 Quality filter: ROE x EM 3x3 grid (forward 6m return)")
    lines.append("")
    qf = summary.get("_quality_filter", {})
    cells = qf.get("cells", {})
    if cells:
        lines.append(f"- N months used: {qf['n_months_used']}")
        lines.append("")
        lines.append("| ROE \\ EM | low | mid | high |")
        lines.append("|---|---|---|---|")
        for rt in ["low", "mid", "high"]:
            row = [f"**{rt}**"]
            for et in ["low", "mid", "high"]:
                v = cells.get((rt, et))
                if v:
                    row.append(f"{v['mean_ret']*100:+.2f}% (Sh {v['sharpe']:+.2f})")
                else:
                    row.append("NA")
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")

        r6 = summary.get("_r6_summary", {})
        if r6:
            lines.append("### R6 關鍵比較")
            lines.append("")
            lines.append(
                f"- **high-ROE x low-EM** (低槓桿真價值): "
                f"{r6['high_roe_low_em_mean']*100:+.3f}% per 6m"
            )
            lines.append(
                f"- **high-ROE x high-EM** (高槓桿撐 ROE): "
                f"{r6['high_roe_high_em_mean']*100:+.3f}% per 6m"
            )
            lines.append(
                f"- **Spread (low-EM minus high-EM)**: "
                f"{r6['spread']*100:+.3f}% per 6m "
                f"-> {'YES quality filter alpha' if r6['spread'] > 0 else 'NO quality filter alpha (反直覺)'}"
            )
            lines.append("")

    lines.append("## Grading 圖例")
    lines.append("")
    lines.append("- **A**: |IC|>=0.03 且 |IR|>=0.3 + decile/IC 同向 + monotonicity 同向")
    lines.append("- **B**: |IC| 0.02-0.03, |IR| 0.2-0.3 + 方向一致")
    lines.append("- **C**: |IC| 0.01-0.02 觀察")
    lines.append("- **C\\***: IC 數字 OK 但 decile spread / monotonicity 反向 (假象)")
    lines.append("- **D**: |IC|<0.01 或反向")
    lines.append("")

    return "\n".join(lines) + "\n"


# ============================================================
# CLI
# ============================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2015-01-01")
    p.add_argument("--end", default="2025-12-31")
    args = p.parse_args()

    summary = run(args.start, args.end)

    md = render_report(summary, args.start, args.end)
    OUT_MD.write_text(md, encoding="utf-8")
    logger.info(f"Wrote {OUT_MD}")


if __name__ == "__main__":
    main()
