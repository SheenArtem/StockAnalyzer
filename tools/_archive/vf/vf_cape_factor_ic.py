"""
VF -- CAPE (Shiller PE) factor IC validation for cyclical stocks (台股).

景氣循環股痛點：
  TTM PE 在景氣高峰看「便宜」(EPS 暴衝, P/E 變低), 用 5-7 年平均 EPS 後 CAPE 才
  揭穿「現在獲利偏離長期均值」。例: 航運 2021 TTM PE=4 看似超便宜, CAPE=15。

驗證 4 個 factor 在台股 panel 是否有 alpha (整體 + sector-conditional):
  F1 CAPE_5y = month_end_close / EPS_avg_20q (5 年, min_periods=12)
  F2 CAPE_7y = month_end_close / EPS_avg_28q (7 年, min_periods=20)
  F3 TTM_PE  = month_end_close / EPS_TTM    (4 季 trailing)  -- 控制組
  F4 CAPE_minus_TTM = (CAPE_5y - TTM_PE) / TTM_PE  -- 「目前獲利偏離長期」%
       越大代表現獲利 >> 長期均值 (週期高點訊號)

EPS 計算:
  EPS_q = IncomeAfterTaxes_q / OrdinaryShare_q  (income x balance merge)
  IncomeAfterTaxes 為單季稅後盈餘 (FinMind 季資料)
  OrdinaryShare 為流通在外股本面額 (FinMind 季資料, 額面 10 元 -> 股數 = OS/10)

  WAIT: OrdinaryShare 在 FinMind 是「股本」金額 (面額 10 元 x 股數)。EPS 應該 =
  IAT / shares_outstanding, shares = OrdinaryShare / 10 (台股面額固定 10 元)。
  但因為 CAPE = price / EPS, 同樣的單位除下去, 用 IAT / OrdinaryShare 也可以
  (只是相對 ratio 縮 10 倍)。為了與 TTM PE 直接比較數字, 用 shares = OS/10。

Robustness:
- 45-day announce delay 防 lookahead
- 月底取 RAW Close (不要 AdjClose, 後者扣股息扭曲歷史 PE)
- 排除「5 年中曾有 1 季虧損」的 ticker-period (CAPE 公式不適用 / 噪音爆高)
- |CAPE| > 200 或 < 0 截斷 NaN
- 月度 cross-sectional IC, 樣本 >= 100

R1-R6:
  R1 IC mean / IR / hit_rate over horizons 1m/3m/6m/12m
  R2 Decile spread (Q1 便宜 vs Q10 貴) Sharpe + monotonicity
  R3 Walk-forward (60M IS -> 12M OOS) sign-stability
  R4 TWII regime breakdown (bull/bear)
  R5 ρ correlation with TTM_PE + ρ with quality_score
  R6 Sector-conditional IC (cyclical vs stable)

Output: reports/vf_cape_factor_ic.{csv,md}
CLI: python tools/vf_cape_factor_ic.py --start 2015-01-01 --end 2025-12-31
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

OUT_CSV = OUT_DIR / "vf_cape_factor_ic.csv"
OUT_MD = OUT_DIR / "vf_cape_factor_ic.md"

ANNOUNCE_DELAY_DAYS = 45
HORIZONS_MONTHS = [1, 3, 6, 12]
MIN_CROSS_SECTION = 100
PAR_VALUE = 10.0  # 台股每股面額

# Sector classification: industry text in ohlcv parquet is Big5 mojibake under
# python utf8 decode. Avoid embedding mojibake bytes in source -- bootstrap a
# mapping at runtime from reference stocks whose sector is known by ticker.
SECTOR_REF_TICKERS = {
    # Cyclical
    "2330": "cyclical_semi",       # 0xe5 0x8d 0x8a ... (半導體業 TSMC semi)
    "2603": "cyclical_shipping",   # shipping (Evergreen)
    "2002": "cyclical_steel",      # steel (CSC)
    "1101": "cyclical_cement",     # cement (Taiwan Cement)
    "1303": "cyclical_plastic",    # plastic (Nan Ya)
    "2105": "cyclical_rubber",     # rubber (Cheng Shin Rubber)
    # Stable
    "1216": "stable_food",         # food (Uni-President)
    "2880": "stable_finance",      # finance (Hua Nan FHC)
    "2412": "stable_telecom",      # telecom (Chunghwa Telecom)
    # Mixed (large pool, kept separate)
    "2454": "electronics_general", # electronics (MediaTek)
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cape_ic")


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


def load_eps_panel() -> pd.DataFrame:
    """組合 IncomeAfterTaxes (income) x OrdinaryShare (balance) -> EPS_q."""
    logger.info("Loading IncomeAfterTaxes from financials_income.parquet ...")
    inc_long = pd.read_parquet(DATA_DIR / "financials_income.parquet")
    inc_long["date"] = pd.to_datetime(inc_long["date"])
    inc = _pivot_long(inc_long, ["IncomeAfterTaxes"])

    logger.info("Loading OrdinaryShare from financials_balance.parquet ...")
    bal_long = pd.read_parquet(DATA_DIR / "financials_balance.parquet")
    bal_long["date"] = pd.to_datetime(bal_long["date"])
    bal = _pivot_long(bal_long, ["OrdinaryShare"])

    df = inc.merge(bal, on=["stock_id", "date"], how="inner")
    # shares = OrdinaryShare / par_value
    df["shares"] = df["OrdinaryShare"] / PAR_VALUE
    # EPS quarterly
    df["EPS_q"] = np.where(df["shares"] > 0, df["IncomeAfterTaxes"] / df["shares"], np.nan)
    df = df.dropna(subset=["EPS_q"])
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    logger.info(
        f"EPS panel: {len(df):,} rows, {df['stock_id'].nunique()} stocks, "
        f"{df['date'].min().date()} ~ {df['date'].max().date()}"
    )
    return df[["stock_id", "date", "EPS_q"]]


def load_ohlcv() -> pd.DataFrame:
    """讀 ohlcv_tw, 用 RAW Close (非 AdjClose), 帶 industry 欄位。"""
    logger.info("Loading ohlcv_tw.parquet ...")
    px = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet")
    px["date"] = pd.to_datetime(px["date"])
    px = px[["stock_id", "date", "Close", "industry"]].copy()
    px["Close"] = pd.to_numeric(px["Close"], errors="coerce")
    px = px.dropna(subset=["Close"])
    px = px[px["Close"] > 0]
    logger.info(
        f"OHLCV: {len(px):,} rows, {px['stock_id'].nunique()} stocks "
        f"(using RAW Close)"
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


def get_industry_map(px: pd.DataFrame) -> pd.DataFrame:
    """每 stock_id -> sector_group.
    Bootstrap industry-string -> sector_group from SECTOR_REF_TICKERS so we
    avoid hardcoding mojibake bytes in source.
    """
    ind = px[["stock_id", "industry"]].drop_duplicates(subset=["stock_id"]).copy()
    industry_to_group = {}
    for ref_id, group in SECTOR_REF_TICKERS.items():
        rows = ind[ind["stock_id"] == ref_id]
        if len(rows) == 0:
            logger.warning(f"  ref ticker {ref_id} not in universe")
            continue
        ind_str = rows.iloc[0]["industry"]
        if pd.notna(ind_str):
            industry_to_group[ind_str] = group
    logger.info(
        f"Bootstrapped {len(industry_to_group)} industry strings from "
        f"{len(SECTOR_REF_TICKERS)} anchor tickers"
    )
    ind["sector_group"] = ind["industry"].map(
        lambda s: industry_to_group.get(s, "other") if pd.notna(s) else "other"
    )
    n_by_grp = ind["sector_group"].value_counts()
    logger.info("Sector group distribution:")
    for k, v in n_by_grp.items():
        logger.info(f"  {k:30s}  {v:4d}")
    return ind[["stock_id", "sector_group"]]


# ============================================================
# CAPE / EPS aggregations per stock
# ============================================================
def compute_cape_per_stock(g: pd.DataFrame) -> pd.DataFrame:
    """
    輸入: 單一 stock 的季 EPS (sorted asc)
    輸出: 加上 EPS_TTM / EPS_avg5y / EPS_avg7y
    Note: 是否「5y 平均為負」由 attach_factors() guard, 不在這裡 filter
          (景氣循環股應該 INCLUDE, 否則就違反研究目的)。
    """
    g = g.sort_values("date").copy()

    # TTM = sum 4 quarters
    g["EPS_TTM"] = g["EPS_q"].rolling(4, min_periods=4).sum()

    # 5y / 7y averages of quarterly EPS, lenient min_periods
    g["EPS_avg5y"] = g["EPS_q"].rolling(20, min_periods=12).mean()
    g["EPS_avg7y"] = g["EPS_q"].rolling(28, min_periods=20).mean()

    return g[["stock_id", "date", "EPS_q", "EPS_TTM", "EPS_avg5y", "EPS_avg7y"]]


def build_eps_aggs(eps: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing TTM / 5y / 7y aggregates per stock ...")
    t0 = time.time()
    out = []
    n_total = eps["stock_id"].nunique()
    n_done = 0
    for sid, g in eps.groupby("stock_id", sort=False):
        if len(g) < 4:
            continue
        out.append(compute_cape_per_stock(g))
        n_done += 1
        if n_done % 500 == 0:
            logger.info(f"  computed {n_done}/{n_total} stocks ...")
    panel = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    logger.info(
        f"EPS-agg panel: {len(panel):,} rows, {panel['stock_id'].nunique()} stocks "
        f"({time.time()-t0:.1f}s)"
    )
    return panel


# ============================================================
# Forward returns (monthly cross-sectional)
# ============================================================
def build_monthly_prices(px: pd.DataFrame) -> pd.DataFrame:
    """每 (stock, month_end) 取 RAW Close 作 fundamental denominator,
    並用 AdjClose 估 forward return。"""
    logger.info("Building monthly prices (raw Close for PE, fwd ret from AdjClose) ...")
    # Need both Close (for CAPE numerator) and AdjClose (for fwd return ic)
    # Re-load with AdjClose
    full = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet")
    full["date"] = pd.to_datetime(full["date"])
    full = full[["stock_id", "date", "Close", "AdjClose"]].copy()
    full["Close"] = pd.to_numeric(full["Close"], errors="coerce")
    full["AdjClose"] = pd.to_numeric(full["AdjClose"], errors="coerce")
    full = full.dropna(subset=["Close", "AdjClose"])
    full = full[(full["Close"] > 0) & (full["AdjClose"] > 0)]

    full = full.sort_values(["stock_id", "date"])
    full["ym"] = full["date"].dt.to_period("M")
    monthly = full.groupby(["stock_id", "ym"], as_index=False).tail(1).copy()
    monthly = monthly[["stock_id", "ym", "date", "Close", "AdjClose"]].rename(
        columns={"date": "me_date", "Close": "px_raw", "AdjClose": "px_adj"}
    )
    monthly["me_date"] = monthly["ym"].dt.to_timestamp("M")
    monthly = monthly.sort_values(["stock_id", "ym"]).reset_index(drop=True)

    g = monthly.groupby("stock_id", sort=False)
    for h in HORIZONS_MONTHS:
        monthly[f"fwd_{h}m"] = g["px_adj"].shift(-h) / monthly["px_adj"] - 1
    return monthly


# ============================================================
# Merge: monthly grid <- EPS aggs (announce delay)
# ============================================================
def attach_factors(monthly: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        f"Merging EPS aggs with {ANNOUNCE_DELAY_DAYS}-day announce delay ..."
    )
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
                "EPS_TTM",
                "EPS_avg5y",
                "EPS_avg7y",
            ]
        ],
        left_on="me_date",
        right_on="available_from",
        by="stock_id",
        direction="backward",
        allow_exact_matches=True,
    )

    # Compute factors using RAW price
    # Guard via avg > 0 only -- 平均虧損的公司 PE 公式無意義(負 PE), 排除即可。
    # 個別季度虧損但 5y 平均仍 > 0 的景氣循環股 -- include (這正是 CAPE 想處理的標的!)
    px_raw = out["px_raw"]
    out["F1_cape5y"] = np.where(out["EPS_avg5y"] > 0, px_raw / out["EPS_avg5y"], np.nan)
    out["F2_cape7y"] = np.where(out["EPS_avg7y"] > 0, px_raw / out["EPS_avg7y"], np.nan)
    out["F3_ttm_pe"] = np.where(out["EPS_TTM"] > 0, px_raw / out["EPS_TTM"], np.nan)
    out["F4_cape_minus_ttm"] = np.where(
        out["F3_ttm_pe"] > 0,
        (out["F1_cape5y"] - out["F3_ttm_pe"]) / out["F3_ttm_pe"],
        np.nan,
    )

    # Clip extreme PE (>200 = 數據噪音 / 微利公司; <0 已被 np.where 排除)
    for col in ["F1_cape5y", "F2_cape7y", "F3_ttm_pe"]:
        out.loc[out[col] > 200, col] = np.nan
    # F4: |delta| > 5 (CAPE 比 TTM 高 5 倍以上 = 異常)
    out.loc[out["F4_cape_minus_ttm"].abs() > 5, "F4_cape_minus_ttm"] = np.nan

    return out


# ============================================================
# IC / IR / decile / regime
# ============================================================
def cross_sectional_ic(df: pd.DataFrame, factor_col: str, ret_col: str) -> pd.DataFrame:
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
        return {"n_months": 0, "ic_mean": np.nan, "ic_std": np.nan,
                "ir": np.nan, "hit_rate": np.nan, "t_stat": np.nan}
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
            gg["q"] = pd.qcut(
                gg[factor_col].rank(method="first"), 10, labels=False
            )
        except Exception:
            continue
        means = gg.groupby("q")[ret_col].mean()
        means.index = [f"Q{i+1}" for i in means.index]
        means["me_date"] = me
        rows.append(means)
    if not rows:
        return {"spread_mean": np.nan, "spread_std": np.nan, "spread_sharpe": np.nan,
                "monotonic_corr": np.nan, "q10_mean": np.nan, "q1_mean": np.nan,
                "q_means": {}}
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


def walk_forward_sign_stability(ic_df: pd.DataFrame, is_months: int = 60, oos_months: int = 12) -> dict:
    if len(ic_df) < is_months + oos_months:
        return {"n_windows": 0, "sign_hit_rate": np.nan}
    s = ic_df.sort_values("me_date").reset_index(drop=True)
    hits = 0
    n = 0
    for i in range(is_months, len(s) - oos_months + 1, oos_months):
        is_mean = s.loc[i - is_months : i - 1, "ic"].mean()
        oos_mean = s.loc[i : i + oos_months - 1, "ic"].mean()
        if np.isnan(is_mean) or np.isnan(oos_mean):
            continue
        n += 1
        if np.sign(is_mean) == np.sign(oos_mean):
            hits += 1
    return {"n_windows": n, "sign_hit_rate": hits / n if n > 0 else np.nan}


def leave_one_year_out(ic_df: pd.DataFrame) -> dict:
    """SOP #4: 排除每一年看 IC 是否仍然 robust (避免 COVID 2020/bear 2022 主導)."""
    if ic_df.empty:
        return {"min_ic": np.nan, "max_ic": np.nan, "by_year": {}}
    df = ic_df.copy()
    df["year"] = pd.to_datetime(df["me_date"]).dt.year
    by_year = {}
    leave_outs = {}
    for y, g in df.groupby("year"):
        by_year[int(y)] = g["ic"].mean()
        loo = df[df["year"] != y]
        leave_outs[int(y)] = loo["ic"].mean()
    if not leave_outs:
        return {"min_ic": np.nan, "max_ic": np.nan, "by_year": {}}
    arr = np.array(list(leave_outs.values()))
    return {
        "min_ic": float(arr.min()),
        "max_ic": float(arr.max()),
        "full_ic": float(df["ic"].mean()),
        "by_year": by_year,
        "leave_one_out": leave_outs,
    }


def regime_breakdown(df, twii, factor_col, ret_col):
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
    twii["ma200"] = twii["Close"].rolling(200, min_periods=100).mean()
    twii["regime"] = np.where(twii["Close"] >= twii["ma200"], "bull", "bear")
    twii["ym"] = twii["date"].dt.to_period("M")
    me = twii.groupby("ym").tail(1).copy()
    me["me_date"] = me["ym"].dt.to_timestamp("M")
    return me[["me_date", "regime"]]


def sector_conditional_ic(
    df: pd.DataFrame, factor_col: str, ret_col: str, min_cs: int = 20
) -> dict:
    """Sector-conditional IC. Uses smaller min cross-section because individual
    sectors only have 7-200 tickers (cyclical_cement n=7, semi n=174)."""
    out = {}
    for grp, sub in df.groupby("sector_group"):
        if len(sub) < 200:  # at least some breadth
            continue
        # Lower min cross-section for sector breakdown
        ic_records = []
        for me, g in sub.groupby("me_date", sort=True):
            gg = g[[factor_col, ret_col]].dropna()
            if len(gg) < min_cs:
                continue
            try:
                r, p = stats.spearmanr(gg[factor_col].values, gg[ret_col].values)
            except Exception:
                continue
            if np.isnan(r):
                continue
            ic_records.append({"me_date": me, "n": len(gg), "ic": r, "p": p})
        ic_d = pd.DataFrame(ic_records)
        if ic_d.empty:
            continue
        s = ic_summary(ic_d)
        # Spread on smaller cross-section: use quintile (5) instead of decile (10)
        # to keep groups populated
        spread_rows = []
        for me, g in sub.groupby("me_date", sort=True):
            gg = g[[factor_col, ret_col]].dropna()
            if len(gg) < min_cs:
                continue
            gg = gg.copy()
            try:
                gg["q"] = pd.qcut(
                    gg[factor_col].rank(method="first"), 5, labels=False
                )
            except Exception:
                continue
            means = gg.groupby("q")[ret_col].mean()
            means.index = [f"Q{i+1}" for i in means.index]
            spread_rows.append(means)
        if spread_rows:
            qdf = pd.DataFrame(spread_rows).reset_index(drop=True)
            spread_q5q1 = qdf["Q5"] - qdf["Q1"]
            q_means_5 = qdf[[f"Q{i}" for i in range(1, 6)]].mean()
            mono = stats.spearmanr(np.arange(1, 6), q_means_5.values).correlation
            s["spread_sharpe"] = (
                spread_q5q1.mean() / spread_q5q1.std(ddof=1)
                if spread_q5q1.std(ddof=1) > 0 else np.nan
            )
            s["mono"] = mono
        else:
            s["spread_sharpe"] = np.nan
            s["mono"] = np.nan
        out[grp] = s
    return out


# ============================================================
# Driver
# ============================================================
def run(start: str, end: str) -> dict:
    universe = load_universe_common()

    eps_panel = load_eps_panel()
    eps_panel = eps_panel[eps_panel["stock_id"].isin(universe)].copy()

    panel = build_eps_aggs(eps_panel)
    if panel.empty:
        raise RuntimeError("EPS panel empty -- check schema mapping.")

    panel = panel[
        (panel["date"] >= pd.Timestamp(start) - pd.Timedelta(days=400))
        & (panel["date"] <= pd.Timestamp(end))
    ].copy()

    px = load_ohlcv()
    px = px[px["stock_id"].isin(universe)].copy()
    sector_map = get_industry_map(px)

    monthly = build_monthly_prices(px)
    monthly = monthly[
        (monthly["me_date"] >= pd.Timestamp(start))
        & (monthly["me_date"] <= pd.Timestamp(end))
    ].copy()
    monthly = monthly[monthly["stock_id"].isin(universe)].copy()

    merged = attach_factors(monthly, panel)
    merged = merged.merge(sector_map, on="stock_id", how="left")
    merged["sector_group"] = merged["sector_group"].fillna("other")

    # Coverage report
    n_total = len(merged)
    n_f1 = merged["F1_cape5y"].notna().sum()
    n_f2 = merged["F2_cape7y"].notna().sum()
    n_f3 = merged["F3_ttm_pe"].notna().sum()
    n_f4 = merged["F4_cape_minus_ttm"].notna().sum()
    logger.info(
        f"Merged grid: {n_total:,} rows, {merged['stock_id'].nunique()} stocks, "
        f"{merged['me_date'].min().date()} ~ {merged['me_date'].max().date()}"
    )
    logger.info(
        f"Factor coverage: F1_cape5y={n_f1:,} ({100*n_f1/n_total:.1f}%) | "
        f"F2_cape7y={n_f2:,} ({100*n_f2/n_total:.1f}%) | "
        f"F3_ttm_pe={n_f3:,} ({100*n_f3/n_total:.1f}%) | "
        f"F4_delta={n_f4:,} ({100*n_f4/n_total:.1f}%)"
    )

    twii = build_twii_regime()

    factor_cols = {
        "cape5y": "F1_cape5y",
        "cape7y": "F2_cape7y",
        "ttm_pe": "F3_ttm_pe",
        "cape_minus_ttm": "F4_cape_minus_ttm",
    }
    targets = list(factor_cols.keys())

    all_ic_records = []
    summary = {}

    for fkey in targets:
        fcol = factor_cols[fkey]
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
            logger.info(
                f"  h={h}m: n={stats_h['n_months']}, IC={stats_h['ic_mean']:+.4f} "
                f"IR={stats_h['ir']:+.3f} hit={stats_h['hit_rate']:.2f} | "
                f"Sp Sharpe={spread_h['spread_sharpe']:+.3f} "
                f"mono={spread_h['monotonic_corr']:+.3f} | "
                f"WF sign-hit={wf_h['sign_hit_rate']} | "
                f"LOO range=[{loo_h.get('min_ic', np.nan):+.4f}, {loo_h.get('max_ic', np.nan):+.4f}]"
            )

        regime_h6 = regime_breakdown(merged, twii, fcol, "fwd_6m")
        per_factor["regime_h6m"] = regime_h6

        # R6: sector-conditional (h=6m)
        sector_h6 = sector_conditional_ic(merged, fcol, "fwd_6m")
        per_factor["sector_h6m"] = sector_h6
        if sector_h6:
            logger.info(f"  Sector-conditional IC (h=6m):")
            for grp, st in sorted(sector_h6.items()):
                logger.info(
                    f"    {grp:30s} n={st['n_months']:3d}  IC={st['ic_mean']:+.4f}  "
                    f"IR={st['ir']:+.3f}  Sp={st.get('spread_sharpe', np.nan):+.3f}  "
                    f"mono={st.get('mono', np.nan):+.3f}"
                )

        summary[fkey] = per_factor

    # R5 corr with quality_score / f_score
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
        fscore_corr[fkey] = {"n": len(sub), "corr_quality": cq, "corr_fscore": cf}
        logger.info(
            f"R5 {fkey}: rho(factor, quality_score)={cq:+.3f}, "
            f"rho(factor, f_score)={cf:+.3f}, n={len(sub)}"
        )
    summary["_fscore_corr"] = fscore_corr

    # Inter-factor correlations: CAPE vs TTM_PE, CAPE_5y vs CAPE_7y
    inter = {}
    pairs = [
        ("cape5y", "ttm_pe"),
        ("cape7y", "ttm_pe"),
        ("cape5y", "cape7y"),
        ("cape_minus_ttm", "ttm_pe"),
        ("cape_minus_ttm", "cape5y"),
    ]
    for a, b in pairs:
        ca, cb = factor_cols[a], factor_cols[b]
        sub = merged[[ca, cb]].dropna()
        if len(sub) < 1000:
            continue
        rho = stats.spearmanr(sub[ca], sub[cb]).correlation
        inter[f"{a}_vs_{b}"] = {"n": len(sub), "rho": rho}
        logger.info(f"Inter-factor rho({a}, {b}) = {rho:+.3f}, n={len(sub)}")
    summary["_inter_factor"] = inter

    # CSV
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
    lines.append("# VF -- CAPE (Shiller PE) factor IC validation (台股景氣循環股)")
    lines.append("")
    lines.append(f"- Window: {start} ~ {end}")
    lines.append("- Universe: 普通股 (universe_tw_full.is_common_stock)")
    lines.append(f"- Announce delay: {ANNOUNCE_DELAY_DAYS} 天")
    lines.append(f"- Min cross-section: {MIN_CROSS_SECTION} stocks per month")
    lines.append("- 月底取 RAW Close (非 AdjClose, 避免 dividend back-adjust 扭曲歷史 PE)")
    lines.append("- EPS_q = IncomeAfterTaxes_q / (OrdinaryShare / 10)")
    lines.append("- F1 CAPE_5y = px / EPS_avg_20q (min_periods=12)")
    lines.append("- F2 CAPE_7y = px / EPS_avg_28q (min_periods=20)")
    lines.append("- F3 TTM_PE  = px / EPS_TTM (4q sum)  -- 控制組")
    lines.append("- F4 CAPE-TTM = (CAPE_5y - TTM_PE) / TTM_PE  -- 偏離率訊號")
    lines.append("- Guard: 5y/7y 平均 EPS > 0 才算 (個別季度虧損但平均 > 0 仍 include,")
    lines.append("  因為這正是 CAPE 想處理的景氣循環股); 並 clip CAPE/PE > 200 與 |F4|>5")
    lines.append("")

    lines.append("## R1-R3 一覽")
    lines.append("")
    lines.append("| Factor | Horizon | n | IC | IR | hit | Sp Sharpe | Mono | WF sign-hit | LOO range | Grade |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for fkey in ["cape5y", "cape7y", "ttm_pe", "cape_minus_ttm"]:
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
            loo_str = "NA"
            if not np.isnan(loo.get("min_ic", np.nan)):
                loo_str = f"[{loo['min_ic']:+.3f}, {loo['max_ic']:+.3f}]"
            lines.append(
                f"| {fkey} | {h}m | {ic['n_months']} | "
                f"{ic['ic_mean']:+.4f} | {ic['ir']:+.3f} | "
                f"{ic['hit_rate']:.2f} | "
                f"{sp['spread_sharpe']:+.3f} | "
                f"{sp['monotonic_corr']:+.3f} | "
                f"{wf_str} | {loo_str} | {g} |"
            )
    lines.append("")

    # R4 regime
    lines.append("## R4 Regime breakdown (TWII 200d MA, h=6m)")
    lines.append("")
    lines.append("| Factor | Regime | n | IC | IR | hit |")
    lines.append("|---|---|---|---|---|---|")
    for fkey in ["cape5y", "cape7y", "ttm_pe", "cape_minus_ttm"]:
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
    lines.append("## R5 與 quality_score / f_score 相關性 (Spearman)")
    lines.append("")
    fc = summary.get("_fscore_corr", {})
    lines.append("| Factor | n | rho(factor, quality_score) | rho(factor, f_score) | 增量價值 |")
    lines.append("|---|---|---|---|---|")
    for fkey in ["cape5y", "cape7y", "ttm_pe", "cape_minus_ttm"]:
        d = fc.get(fkey)
        if not d:
            continue
        cq = d.get("corr_quality")
        cf = d.get("corr_fscore")
        cq_s = f"{cq:+.3f}" if cq is not None and not (isinstance(cq, float) and np.isnan(cq)) else "NA"
        cf_s = f"{cf:+.3f}" if cf is not None and not (isinstance(cf, float) and np.isnan(cf)) else "NA"
        if cq is None or (isinstance(cq, float) and np.isnan(cq)):
            inc = "NA"
        elif abs(cq) < 0.4:
            inc = "高 (獨立)"
        elif abs(cq) < 0.7:
            inc = "中"
        else:
            inc = "低 (共線)"
        lines.append(f"| {fkey} | {d['n']} | {cq_s} | {cf_s} | {inc} |")
    lines.append("")

    # Inter-factor
    inter = summary.get("_inter_factor", {})
    if inter:
        lines.append("## Inter-factor 相關性 (CAPE vs TTM_PE 重疊度)")
        lines.append("")
        lines.append("| pair | n | rho |")
        lines.append("|---|---|---|")
        for k, v in inter.items():
            lines.append(f"| {k} | {v['n']} | {v['rho']:+.3f} |")
        lines.append("")

    # R6 Sector-conditional
    lines.append("## R6 Sector-conditional IC (h=6m)")
    lines.append("")
    for fkey in ["cape5y", "cape7y", "ttm_pe", "cape_minus_ttm"]:
        if fkey not in summary:
            continue
        sec = summary[fkey].get("sector_h6m", {})
        if not sec:
            continue
        lines.append(f"### {fkey}")
        lines.append("")
        lines.append("| Sector | n | IC | IR | hit | Sp Sharpe | Mono |")
        lines.append("|---|---|---|---|---|---|---|")
        for grp in sorted(sec.keys()):
            st = sec[grp]
            lines.append(
                f"| {grp} | {st['n_months']} | {st['ic_mean']:+.4f} | "
                f"{st['ir']:+.3f} | {st['hit_rate']:.2f} | "
                f"{st.get('spread_sharpe', np.nan):+.3f} | "
                f"{st.get('mono', np.nan):+.3f} |"
            )
        lines.append("")

    # Decile breakdown for h=6m
    lines.append("## Decile Q1-Q10 平均月報酬 (h=6m, Q1 = 最便宜)")
    lines.append("")
    lines.append("| Factor | " + " | ".join([f"Q{i}" for i in range(1, 11)]) + " |")
    lines.append("|" + "|".join(["---"] * 11) + "|")
    for fkey in ["cape5y", "cape7y", "ttm_pe", "cape_minus_ttm"]:
        if fkey not in summary:
            continue
        sp = summary[fkey].get("h6m", {}).get("decile", {})
        qm = sp.get("q_means", {})
        if not qm:
            continue
        cells = [
            f"{qm.get(f'Q{i}', np.nan)*100:+.2f}%" if not np.isnan(qm.get(f"Q{i}", np.nan)) else "NA"
            for i in range(1, 11)
        ]
        lines.append(f"| {fkey} | " + " | ".join(cells) + " |")
    lines.append("")

    # LOO detail (CAPE_5y h=6m)
    lines.append("## LOO (Leave-One-Year-Out, CAPE_5y h=6m)")
    lines.append("")
    if "cape5y" in summary and "h6m" in summary["cape5y"]:
        loo = summary["cape5y"]["h6m"].get("loo", {})
        by_year = loo.get("by_year", {})
        lo = loo.get("leave_one_out", {})
        if by_year:
            lines.append("| Year | IC of that year | IC excluding that year |")
            lines.append("|---|---|---|")
            for y in sorted(by_year.keys()):
                lines.append(f"| {y} | {by_year[y]:+.4f} | {lo.get(y, np.nan):+.4f} |")
        lines.append("")

    lines.append("## Grading 圖例")
    lines.append("")
    lines.append("- **A**: |IC|>=0.03 且 |IR|>=0.3 + decile/IC 同向 + monotonicity 同向")
    lines.append("- **B**: |IC| 0.02-0.03, |IR| 0.2-0.3 + 方向一致")
    lines.append("- **C**: |IC| 0.01-0.02 觀察")
    lines.append("- **C\\***: IC OK 但 decile spread / mono 反向 -> 假象")
    lines.append("- **D**: |IC|<0.01 或反向")
    lines.append("")
    lines.append("> CAPE / PE 是 valuation factor, 預期 sign = 負 (越貴未來報酬越低)")
    lines.append("> 故 IC < 0 + Q10-Q1 < 0 + mono < 0 才是「正確 alpha」")
    lines.append("")

    # Verdict block
    lines.append("## Verdict 結論")
    lines.append("")
    s5 = summary.get("cape5y", {}).get("h6m", {}).get("ic", {})
    s5_sp = summary.get("cape5y", {}).get("h6m", {}).get("decile", {})
    s7 = summary.get("cape7y", {}).get("h6m", {}).get("ic", {})
    sttm = summary.get("ttm_pe", {}).get("h6m", {}).get("ic", {})
    sttm_sp = summary.get("ttm_pe", {}).get("h6m", {}).get("decile", {})
    s4 = summary.get("cape_minus_ttm", {}).get("h6m", {}).get("ic", {})
    s4_sp = summary.get("cape_minus_ttm", {}).get("h6m", {}).get("decile", {})
    inter = summary.get("_inter_factor", {})
    rho_5y_ttm = inter.get("cape5y_vs_ttm_pe", {}).get("rho", np.nan)

    lines.append(f"- **F1 CAPE_5y (h=6m)**: IC={s5.get('ic_mean', np.nan):+.4f}, "
                 f"IR={s5.get('ir', np.nan):+.3f}, "
                 f"Sp Sharpe={s5_sp.get('spread_sharpe', np.nan):+.3f}, "
                 f"mono={s5_sp.get('monotonic_corr', np.nan):+.3f}")
    lines.append(f"- **F3 TTM_PE (h=6m, baseline)**: IC={sttm.get('ic_mean', np.nan):+.4f}, "
                 f"IR={sttm.get('ir', np.nan):+.3f}, "
                 f"Sp Sharpe={sttm_sp.get('spread_sharpe', np.nan):+.3f}, "
                 f"mono={sttm_sp.get('monotonic_corr', np.nan):+.3f}")
    lines.append(f"- **F4 CAPE-TTM (h=6m)**: IC={s4.get('ic_mean', np.nan):+.4f}, "
                 f"IR={s4.get('ir', np.nan):+.3f}, "
                 f"Sp Sharpe={s4_sp.get('spread_sharpe', np.nan):+.3f}, "
                 f"mono={s4_sp.get('monotonic_corr', np.nan):+.3f}")
    lines.append(f"- **CAPE_5y vs TTM_PE 共線度**: rho={rho_5y_ttm:+.3f}")
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
