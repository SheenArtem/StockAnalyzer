"""
VF — CCC factor IC validation (宋分 Value 候選 #2).

驗證三個 Cash Conversion Cycle 因子在台股 panel 是否有 alpha：
  F1 (yoy)    = -(CCC_TTM(t) - CCC_TTM(t-4))   # 縮短 = 正分
  F2 (ma_yoy) = -(MA4_CCC(t) - MA4_CCC(t-4))   # 4 季平滑後 yoy
  F3 (level)  = -CCC_TTM(t)                    # 低 CCC = 正分（控制組）

CCC = DSO + DIO - DPO (天數)
  DSO = AR / Revenue × 365
  DIO = Inventories / COGS × 365
  DPO = AP / COGS × 365
TTM 版本: 用 4 季滾動平均 AR/Inv/AP 和 4 季 SUM 的 Revenue/COGS（季度資料 TTM 標準作法）

⚠️ 新 SOP（從 ROIC 假象學到）：
  IC sign 必須與 decile spread sign 一致 + monotonicity ≥ +0.5
  否則無論 IC 多顯著，直接降 D。

Robustness:
- 45-day announce delay 防 lookahead
- TTM 需要連續 4 季資料
- 排除 |CCC|>365 天 (合理範圍)
- 排除負 Revenue/COGS（資料錯）
- 月底 cross-sectional IC，要求月度樣本 >= 100

5 輪驗證 (R1-R5)：
  R1 IC mean / IR / hit_rate over horizons 1m/3m/6m/12m
  R2 Decile spread (Q10-Q1) Sharpe & monotonicity (NEW SOP)
  R3 Walk-forward (60M IS -> 12M OOS) sign-stability
  R4 Regime breakdown (TWII 200d MA bull/bear)
  R5 Correlation with F-Score quality_score / f_score

Output: reports/vf_ccc_factor_ic.{csv,md}
CLI: python tools/vf_ccc_factor_ic.py --start 2015-01-01 --end 2025-12-31 --factor all
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

OUT_CSV = OUT_DIR / "vf_ccc_factor_ic.csv"
OUT_MD = OUT_DIR / "vf_ccc_factor_ic.md"

ANNOUNCE_DELAY_DAYS = 45
HORIZONS_MONTHS = [1, 3, 6, 12]
MIN_CROSS_SECTION = 100
CCC_ABS_CAP = 365  # |CCC| > 365 天視為 outlier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ccc_ic")


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
    logger.info("Loading financials_income / financials_balance ...")
    inc_long = pd.read_parquet(DATA_DIR / "financials_income.parquet")
    bal_long = pd.read_parquet(DATA_DIR / "financials_balance.parquet")
    inc_long["date"] = pd.to_datetime(inc_long["date"])
    bal_long["date"] = pd.to_datetime(bal_long["date"])

    inc = _pivot_long(inc_long, ["Revenue", "CostOfGoodsSold"])
    bal = _pivot_long(
        bal_long, ["AccountsReceivableNet", "Inventories", "AccountsPayable"]
    )
    wide = inc.merge(bal, on=["stock_id", "date"], how="outer")
    wide = wide.sort_values(["stock_id", "date"]).reset_index(drop=True)

    needed = [
        "Revenue",
        "CostOfGoodsSold",
        "AccountsReceivableNet",
        "Inventories",
        "AccountsPayable",
    ]
    missing = [c for c in needed if c not in wide.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    logger.info(
        f"Financials wide: {len(wide):,} rows, "
        f"{wide['stock_id'].nunique()} stocks, "
        f"{wide['date'].min().date()} ~ {wide['date'].max().date()}"
    )
    return wide


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
# CCC computation (per stock) — vectorized
# ============================================================
def compute_ccc_per_stock(g: pd.DataFrame) -> pd.DataFrame:
    """
    輸入: 單一 stock 的季資料 (sorted by date asc)
    輸出: 加上 DSO_TTM / DIO_TTM / DPO_TTM / CCC_TTM / F1_yoy / F2_ma_yoy / F3_level

    TTM rule:
      - flow (Revenue, COGS): rolling 4Q sum
      - stock (AR, Inv, AP):   rolling 4Q mean (季底快照→平均當作期間平均)
    """
    g = g.sort_values("date").copy()

    # Flow TTM (sum 4 quarters)
    rev_ttm = g["Revenue"].rolling(4, min_periods=4).sum()
    cogs_ttm = g["CostOfGoodsSold"].rolling(4, min_periods=4).sum()

    # Stock TTM (avg of 4 quarter-end balances)
    ar_avg = g["AccountsReceivableNet"].rolling(4, min_periods=4).mean()
    inv_avg = g["Inventories"].rolling(4, min_periods=4).mean()
    ap_avg = g["AccountsPayable"].rolling(4, min_periods=4).mean()

    with np.errstate(divide="ignore", invalid="ignore"):
        dso = np.where(rev_ttm > 0, ar_avg / rev_ttm * 365.0, np.nan)
        dio = np.where(cogs_ttm > 0, inv_avg / cogs_ttm * 365.0, np.nan)
        dpo = np.where(cogs_ttm > 0, ap_avg / cogs_ttm * 365.0, np.nan)

    g["DSO_TTM"] = dso
    g["DIO_TTM"] = dio
    g["DPO_TTM"] = dpo
    g["CCC_TTM"] = g["DSO_TTM"] + g["DIO_TTM"] - g["DPO_TTM"]

    # Outlier guard: 合理範圍
    g.loc[g["CCC_TTM"].abs() > CCC_ABS_CAP, "CCC_TTM"] = np.nan
    # 也 guard 各個 component（防單一極端搞垮 CCC）
    for col in ["DSO_TTM", "DIO_TTM", "DPO_TTM"]:
        g.loc[g[col].abs() > CCC_ABS_CAP, col] = np.nan

    # Re-compose CCC (任一 component NaN 則整個 NaN)
    g["CCC_TTM"] = g["DSO_TTM"] + g["DIO_TTM"] - g["DPO_TTM"]

    # Factors — 注意正方向（縮短 = 利多 = 因子值高）
    g["F1_yoy"] = -(g["CCC_TTM"] - g["CCC_TTM"].shift(4))

    # MA4 平滑後再做 yoy Δ
    ma4 = g["CCC_TTM"].rolling(4, min_periods=4).mean()
    g["F2_ma_yoy"] = -(ma4 - ma4.shift(4))

    g["F3_level"] = -g["CCC_TTM"]

    return g[
        [
            "stock_id",
            "date",
            "CCC_TTM",
            "DSO_TTM",
            "DIO_TTM",
            "DPO_TTM",
            "F1_yoy",
            "F2_ma_yoy",
            "F3_level",
        ]
    ]


def build_ccc_panel(fin: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing CCC + factors per stock ...")
    t0 = time.time()
    out = []
    n_stocks = fin["stock_id"].nunique()
    for i, (sid, g) in enumerate(fin.groupby("stock_id", sort=False), 1):
        if len(g) < 8:
            continue
        out.append(compute_ccc_per_stock(g))
        if i % 500 == 0:
            logger.info(f"  ..processed {i}/{n_stocks} stocks")
    panel = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    panel = panel.dropna(subset=["CCC_TTM"])
    logger.info(
        f"CCC panel: {len(panel):,} rows, {panel['stock_id'].nunique()} stocks "
        f"({time.time()-t0:.1f}s)"
    )
    # Sanity: CCC summary
    desc = panel["CCC_TTM"].describe(
        percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]
    )
    logger.info(f"CCC distribution:\n{desc}")
    return panel


# ============================================================
# Forward returns (monthly cross-sectional)
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


# ============================================================
# Merge factor onto monthly grid with announce delay
# ============================================================
def attach_factor_to_monthly(
    monthly: pd.DataFrame, panel: pd.DataFrame
) -> pd.DataFrame:
    logger.info(
        f"Merging CCC factors with {ANNOUNCE_DELAY_DAYS}-day announce delay ..."
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
                "CCC_TTM",
                "DSO_TTM",
                "DIO_TTM",
                "DPO_TTM",
                "F1_yoy",
                "F2_ma_yoy",
                "F3_level",
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
    return {
        "n_windows": n,
        "sign_hit_rate": hits / n if n > 0 else np.nan,
    }


def regime_breakdown(
    df: pd.DataFrame, twii: pd.DataFrame, factor_col: str, ret_col: str
) -> dict:
    df = df.merge(twii[["me_date", "regime"]], on="me_date", how="left")
    out = {}
    for r, sub in df.groupby("regime"):
        ic_d = cross_sectional_ic(sub, factor_col, ret_col)
        sp = decile_spread(sub, factor_col, ret_col)
        s = ic_summary(ic_d)
        s["regime"] = r
        s["spread_sharpe"] = sp["spread_sharpe"]
        s["mono"] = sp["monotonic_corr"]
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
# Driver
# ============================================================
def run(start: str, end: str, factors: list[str]) -> dict:
    universe = load_universe_common()
    fin = load_financials()
    fin = fin[fin["stock_id"].isin(universe)].copy()
    # 排除負 Revenue / 負 COGS（資料錯）
    before = len(fin)
    fin = fin[
        (fin["Revenue"].fillna(0) >= 0) & (fin["CostOfGoodsSold"].fillna(0) >= 0)
    ].copy()
    logger.info(f"Removed {before - len(fin):,} rows with neg Revenue/COGS")

    panel = build_ccc_panel(fin)
    if panel.empty:
        raise RuntimeError("CCC panel empty — check schema mapping.")

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
        f"Merged grid: {len(merged):,} rows, "
        f"{merged['stock_id'].nunique()} stocks, "
        f"{merged['me_date'].min()} ~ {merged['me_date'].max()}"
    )

    twii = build_twii_regime()

    factor_cols = {
        "yoy": "F1_yoy",
        "ma_yoy": "F2_ma_yoy",
        "level": "F3_level",
    }
    if "all" in factors:
        targets = list(factor_cols)
    else:
        targets = factors

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
            per_factor[f"h{h}m"] = {
                "ic": stats_h,
                "decile": spread_h,
                "walk_forward": wf_h,
            }
            logger.info(
                f"  h={h}m: n={stats_h['n_months']}, IC={stats_h['ic_mean']:+.4f} "
                f"IR={stats_h['ir']:+.3f} hit={stats_h['hit_rate']:.2f} | "
                f"spread Sharpe={spread_h['spread_sharpe']:+.3f} "
                f"mono={spread_h['monotonic_corr']:+.3f} | "
                f"WF sign-hit={wf_h['sign_hit_rate']}"
            )

        regime_h6 = regime_breakdown(merged, twii, fcol, "fwd_6m")
        per_factor["regime_h6m"] = regime_h6
        if regime_h6:
            for r, st in regime_h6.items():
                logger.info(
                    f"  regime={r}: n={st['n_months']}, IC={st['ic_mean']:+.4f} "
                    f"IR={st['ir']:+.3f} sp_sharpe={st.get('spread_sharpe', float('nan')):+.3f}"
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
        if fkey not in targets:
            continue
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
            f"R5 {fkey}: ρ(factor, quality_score)={cq:+.3f}, ρ(factor, f_score)={cf:+.3f}, n={len(sub)}"
        )
    summary["_fscore_corr"] = fscore_corr

    if all_ic_records:
        raw = pd.concat(all_ic_records, ignore_index=True)
        raw.to_csv(OUT_CSV, index=False)
        logger.info(f"Wrote {OUT_CSV}")

    return summary


# ============================================================
# Markdown report
# ============================================================
def grade(ic_mean: float, ir: float, mono: float = None, sharpe: float = None) -> str:
    """
    新 SOP: IC sign 必須 = decile spread sign + monotonicity ≥ +0.5 才能 A/B
    否則無論 IC 多顯著，最多 C* (假象警告)
    """
    if np.isnan(ic_mean) or np.isnan(ir):
        return "D"

    inconsistent = False
    if mono is not None and not np.isnan(mono) and sharpe is not None and not np.isnan(sharpe):
        if np.sign(ic_mean) != np.sign(sharpe):
            inconsistent = True
        if np.sign(ic_mean) > 0 and mono < 0.3:
            inconsistent = True
        if np.sign(ic_mean) < 0 and mono > -0.3:
            inconsistent = True
        if abs(mono) < 0.3:
            inconsistent = True

    if inconsistent:
        return "C*"

    if abs(ic_mean) >= 0.03 and abs(ir) >= 0.3 and abs(mono) >= 0.5:
        return "A"
    if abs(ic_mean) >= 0.02 and abs(ir) >= 0.2 and abs(mono) >= 0.3:
        return "B"
    if abs(ic_mean) >= 0.01:
        return "C"
    return "D"


def render_report(summary: dict, start: str, end: str) -> str:
    lines = []
    lines.append("# VF — CCC factor IC validation (宋分 Value 候選 #2)")
    lines.append("")
    lines.append(f"- Window: {start} ~ {end}")
    lines.append("- Universe: 普通股（universe_tw_full.is_common_stock）")
    lines.append(f"- Announce delay: {ANNOUNCE_DELAY_DAYS} 天")
    lines.append(f"- Min cross-section: {MIN_CROSS_SECTION} stocks per month")
    lines.append("")
    lines.append("**因子定義**")
    lines.append("- CCC = DSO + DIO − DPO （天）")
    lines.append("- DSO = AR / Revenue × 365；DIO = Inv / COGS × 365；DPO = AP / COGS × 365")
    lines.append("- TTM: flow (Rev/COGS) sum 4Q ; stock (AR/Inv/AP) avg 4Q")
    lines.append("- F1_yoy = −(CCC_TTM_t − CCC_TTM_{t-4})  （縮短 = 正分）")
    lines.append("- F2_ma_yoy = −(MA4_CCC_t − MA4_CCC_{t-4})（4 季平滑後 yoy）")
    lines.append("- F3_level = −CCC_TTM_t（控制組：低 CCC = 正分）")
    lines.append(f"- Outlier: |CCC| > {CCC_ABS_CAP} 天 視為 NaN")
    lines.append("")
    lines.append("⚠️ **新 SOP**: IC sign 必須 = decile spread sign 且 |mono| ≥ 0.3，否則直接 C*（假象警告）；A 級需 |mono| ≥ 0.5")
    lines.append("")

    # Summary table
    lines.append("## R1-R3 一覽")
    lines.append("")
    lines.append("| Factor | Horizon | n | IC | IR | hit | Sp Sharpe | Mono | WF sign-hit | Grade |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for fkey in ["yoy", "ma_yoy", "level"]:
        if fkey not in summary:
            continue
        for h in HORIZONS_MONTHS:
            block = summary[fkey].get(f"h{h}m")
            if not block:
                continue
            ic = block["ic"]
            sp = block["decile"]
            wf = block["walk_forward"]
            g = grade(ic["ic_mean"], ic["ir"], sp.get("monotonic_corr"), sp.get("spread_sharpe"))
            wf_s = (
                f"{wf['sign_hit_rate']:.2f}"
                if wf["sign_hit_rate"] is not None
                and not (isinstance(wf["sign_hit_rate"], float) and np.isnan(wf["sign_hit_rate"]))
                else "NA"
            )
            lines.append(
                f"| {fkey} | {h}m | {ic['n_months']} | "
                f"{ic['ic_mean']:+.4f} | {ic['ir']:+.3f} | "
                f"{ic['hit_rate']:.2f} | "
                f"{sp['spread_sharpe']:+.3f} | "
                f"{sp['monotonic_corr']:+.3f} | "
                f"{wf_s} | "
                f"{g} |"
            )
    lines.append("")

    # R4 regime
    lines.append("## R4 Regime breakdown (TWII 200d MA, h=6m)")
    lines.append("")
    lines.append("| Factor | Regime | n | IC | IR | hit | Sp Sharpe | Mono |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for fkey in ["yoy", "ma_yoy", "level"]:
        if fkey not in summary:
            continue
        rg = summary[fkey].get("regime_h6m", {})
        for r in ["bull", "bear"]:
            st = rg.get(r)
            if not st:
                continue
            lines.append(
                f"| {fkey} | {r} | {st['n_months']} | "
                f"{st['ic_mean']:+.4f} | {st['ir']:+.3f} | {st['hit_rate']:.2f} | "
                f"{st.get('spread_sharpe', float('nan')):+.3f} | "
                f"{st.get('mono', float('nan')):+.3f} |"
            )
    lines.append("")

    # R5
    lines.append("## R5 與 F-Score 的相關性 (Spearman)")
    lines.append("")
    fc = summary.get("_fscore_corr", {})
    lines.append("| Factor | n | ρ(factor, quality_score) | ρ(factor, f_score) | 增量價值 |")
    lines.append("|---|---|---|---|---|")
    for fkey in ["yoy", "ma_yoy", "level"]:
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
            inc = "高（獨立）"
        elif abs(cq) < 0.7:
            inc = "中"
        else:
            inc = "低（共線）"
        lines.append(f"| {fkey} | {d['n']} | {cq_s} | {cf_s} | {inc} |")
    lines.append("")

    # Decile breakdown for h=6m
    lines.append("## Decile Q1-Q10 平均月報酬 (h=6m)")
    lines.append("")
    lines.append("Q1 = 因子值最低 (CCC 增加最多 / level 最高)；Q10 = 因子值最高 (CCC 縮短最多 / level 最低)")
    lines.append("")
    lines.append("| Factor | " + " | ".join([f"Q{i}" for i in range(1, 11)]) + " |")
    lines.append("|" + "|".join(["---"] * 11) + "|")
    for fkey in ["yoy", "ma_yoy", "level"]:
        if fkey not in summary:
            continue
        sp = summary[fkey].get("h6m", {}).get("decile", {})
        qm = sp.get("q_means", {})
        if not qm:
            continue
        cells = [
            f"{qm.get(f'Q{i}', np.nan)*100:+.2f}%"
            if not np.isnan(qm.get(f"Q{i}", np.nan))
            else "NA"
            for i in range(1, 11)
        ]
        lines.append(f"| {fkey} | " + " | ".join(cells) + " |")
    lines.append("")

    # Grading legend
    lines.append("## Grading 圖例 (新 SOP)")
    lines.append("")
    lines.append("- **A**: |IC|≥0.03 且 |IR|≥0.3 且 IC=spread 同向 且 |mono|≥0.5")
    lines.append("- **B**: |IC|≥0.02 且 |IR|≥0.2 且 IC=spread 同向 且 |mono|≥0.3")
    lines.append("- **C**: |IC|≥0.01 觀察")
    lines.append("- **C\\***: IC 看似 OK 但 decile spread / monotonicity 反向 → 不可上線（IC 假象）")
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
    p.add_argument(
        "--factor",
        default="all",
        help="yoy / ma_yoy / level / all",
    )
    args = p.parse_args()

    factors = [args.factor] if args.factor != "all" else ["all"]
    summary = run(args.start, args.end, factors)

    md = render_report(summary, args.start, args.end)
    OUT_MD.write_text(md, encoding="utf-8")
    logger.info(f"Wrote {OUT_MD}")


if __name__ == "__main__":
    main()
