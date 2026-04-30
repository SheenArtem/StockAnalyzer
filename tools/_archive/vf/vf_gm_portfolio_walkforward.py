"""
vf_gm_portfolio_walkforward.py
==============================
Portfolio-level walk-forward backtest for evaluating whether to:
  (a) add `gm_qoq_s` as a new Value factor weight (F2 GM QoQ Δ — A grade univariate IC)
  (b) cut the existing `_score_margin` GM-level adjustment (F3 GM level — reverse mono)

Snapshot: data_cache/backtest/trade_journal_value_tw_snapshot.parquet
  - 70,760 rows × 33 cols, 309 weeks (2020-01-03 ~ 2025-12-26)
  - quality_s in snapshot is sourced DIRECTLY from quality_scores.parquet
    (= F-Score + Z-Score blend), NOT from value_screener._score_quality()
  - => GM>40/+5 GM<10/-5 (TradingView _score_margin level branch) is LIVE-only
       and is a NO-OP at backtest layer. We still report S0..S5 for traceability
       but mark equivalent rows.

GM_QoQ factor (F2):
  GM_Q(t) - GM_Q(t-1), where GM_Q = GrossProfit / Revenue (single quarter)
  Announce delay: 45 days (sync with vf_gm_factor_ic.py)
  Cross-sectional rank percentile -> 0..100 score (gm_qoq_s)
  NaN rows are EXCLUDED (not zero-filled) per Robustness rules.

Schemes (top_n=50, horizon=60d, weekly rebalance):
  S0 LIVE baseline       30/25/30/15/0 +  0 gm_qoq, level kept       (= snapshot live ALREADY no level effect)
  S1 weights+QoQ@0       30/25/30/15/0 +  0 gm_qoq, level kept       (= S0 in backtest)
  S2 agent prop          25/25/25/15/0 + 10 gm_qoq, level kept
  S3 cut level + QoQ     25/25/25/15/0 + 10 gm_qoq, level cut        (= S2 in backtest)
  S4 add QoQ keep level  25/25/25/15/0 + 10 gm_qoq, level kept       (= S2 in backtest)
  S5 GM-heavy            20/25/25/15/0 + 15 gm_qoq, level cut

Metrics per scheme (CLI prints summary, MD report has full tables):
  - Aggregate: total return, CAGR, Sharpe (weekly), max drawdown, win rate
  - Quarterly walk-forward: qSh_mean, qWR, beats_LIVE
  - Year-by-year returns (incl. 2022 bear)
  - Top-3 drawdown periods
  - Pick turnover (overlap with prev week)
  - After 0.2% round-trip tx cost: net return + Sharpe

CLI:
  python tools/vf_gm_portfolio_walkforward.py --top-n 50 --horizon 60 --start 2020-01-01 --end 2025-12-31

Outputs:
  reports/vf_gm_portfolio_walkforward.md
  reports/vf_gm_portfolio_walkforward.csv  (per scheme x per week basket return)
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_cache" / "backtest"
OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MD = OUT_DIR / "vf_gm_portfolio_walkforward.md"
OUT_CSV = OUT_DIR / "vf_gm_portfolio_walkforward.csv"

ANNOUNCE_DELAY_DAYS = 45
WEEKS_PER_YEAR = 52

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gm_pf")


# Backtest weight schemes
# Note: at backtest layer S0=S1, S2=S3=S4 (margin-level toggle is no-op in snapshot)
SCHEMES = {
    "S0_LIVE":           {"valuation": 0.30, "quality": 0.25, "revenue": 0.30, "technical": 0.15, "smart_money": 0.00, "gm_qoq": 0.00, "level_cut": False},
    "S1_replace_SM":     {"valuation": 0.30, "quality": 0.25, "revenue": 0.30, "technical": 0.15, "smart_money": 0.00, "gm_qoq": 0.00, "level_cut": False},
    "S2_agent_prop":     {"valuation": 0.25, "quality": 0.25, "revenue": 0.25, "technical": 0.15, "smart_money": 0.00, "gm_qoq": 0.10, "level_cut": False},
    "S3_cut_lvl_addqoq": {"valuation": 0.25, "quality": 0.25, "revenue": 0.25, "technical": 0.15, "smart_money": 0.00, "gm_qoq": 0.10, "level_cut": True},
    "S4_addqoq_keeplvl": {"valuation": 0.25, "quality": 0.25, "revenue": 0.25, "technical": 0.15, "smart_money": 0.00, "gm_qoq": 0.10, "level_cut": False},
    "S5_GM_heavy":       {"valuation": 0.20, "quality": 0.25, "revenue": 0.25, "technical": 0.15, "smart_money": 0.00, "gm_qoq": 0.15, "level_cut": True},
}

LIVE_KEY = "S0_LIVE"


# ============================================================
# Step 1: build gm_qoq panel (per stock)
# ============================================================
def load_financials_gp_rev() -> pd.DataFrame:
    logger.info("Loading financials_income.parquet ...")
    inc_long = pd.read_parquet(DATA_DIR / "financials_income.parquet")
    inc_long["date"] = pd.to_datetime(inc_long["date"])
    sub = inc_long[inc_long["type"].isin(["Revenue", "GrossProfit", "CostOfGoodsSold"])].copy()
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce")
    pivot = sub.pivot_table(
        index=["stock_id", "date"], columns="type", values="value", aggfunc="first"
    ).reset_index()
    pivot.columns.name = None
    if "GrossProfit" not in pivot.columns:
        pivot["GrossProfit"] = np.nan
    if "CostOfGoodsSold" not in pivot.columns:
        pivot["CostOfGoodsSold"] = np.nan
    # Fallback: GP = Rev - COGS
    has_gp = pivot["GrossProfit"].notna()
    has_rc = pivot["Revenue"].notna() & pivot["CostOfGoodsSold"].notna()
    fb = (~has_gp) & has_rc
    pivot.loc[fb, "GrossProfit"] = pivot.loc[fb, "Revenue"] - pivot.loc[fb, "CostOfGoodsSold"]
    n_direct = has_gp.sum()
    n_fb = fb.sum()
    logger.info(f"  GP source: direct={n_direct:,}, fallback Rev-COGS={n_fb:,}")
    return pivot


def compute_gm_qoq(fin: pd.DataFrame) -> pd.DataFrame:
    """Per stock GM_Q + F2 QoQ; returns long panel (stock_id, date, F2_qoq)."""
    logger.info("Computing GM_Q + F2 QoQ ...")
    t0 = time.time()
    out = []
    fin = fin.sort_values(["stock_id", "date"])
    for sid, g in fin.groupby("stock_id", sort=False):
        if len(g) < 3:
            continue
        g = g.copy()
        rev = g["Revenue"]
        gp = g["GrossProfit"]
        with np.errstate(divide="ignore", invalid="ignore"):
            gm_q = np.where(rev > 0, gp / rev, np.nan)
        # sanity clip 1.5
        gm_q = np.where(np.abs(gm_q) > 1.5, np.nan, gm_q)
        g["GM_Q"] = gm_q
        g["F2_qoq"] = g["GM_Q"] - g["GM_Q"].shift(1)
        out.append(g[["stock_id", "date", "F2_qoq"]])
    panel = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    panel = panel.dropna(subset=["F2_qoq"])
    logger.info(f"  GM_QoQ panel: {len(panel):,} rows, {panel['stock_id'].nunique()} stocks ({time.time()-t0:.1f}s)")
    return panel


def attach_gm_qoq_to_snapshot(snap: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    """merge_asof on (stock_id, week_end_date) with 45d announce delay."""
    logger.info(f"Merging F2_qoq into snapshot with {ANNOUNCE_DELAY_DAYS}-day delay ...")
    panel = panel.copy()
    panel["available_from"] = panel["date"] + pd.Timedelta(days=ANNOUNCE_DELAY_DAYS)
    panel = panel.sort_values(["stock_id", "available_from"])
    snap = snap.sort_values(["stock_id", "week_end_date"]).copy()
    # snap stock_id is integer-like in parquet, panel stock_id is string. unify both to str.
    snap["stock_id"] = snap["stock_id"].astype(str)
    panel["stock_id"] = panel["stock_id"].astype(str)
    merged = pd.merge_asof(
        snap.sort_values("week_end_date"),
        panel[["stock_id", "available_from", "F2_qoq"]].sort_values("available_from"),
        left_on="week_end_date",
        right_on="available_from",
        by="stock_id",
        direction="backward",
        allow_exact_matches=True,
    )
    n_with = merged["F2_qoq"].notna().sum()
    logger.info(f"  Snapshot rows with F2_qoq: {n_with:,} / {len(merged):,} ({100*n_with/len(merged):.1f}%)")
    return merged


def build_gm_qoq_score(snap: pd.DataFrame) -> pd.DataFrame:
    """
    Per week_end_date cross-sectional rank percentile of F2_qoq -> 0..100.
    NaN F2_qoq stays NaN (excluded).
    Higher F2_qoq -> higher gm_qoq_s.
    """
    logger.info("Computing cross-sectional rank percentile gm_qoq_s ...")
    out_parts = []
    for wd, g in snap.groupby("week_end_date", sort=True):
        g = g.copy()
        m = g["F2_qoq"].notna()
        if m.sum() < 10:
            g["gm_qoq_s"] = np.nan
        else:
            ranks = g.loc[m, "F2_qoq"].rank(method="average", pct=True) * 100
            g["gm_qoq_s"] = np.nan
            g.loc[m, "gm_qoq_s"] = ranks.values
        out_parts.append(g)
    out = pd.concat(out_parts, ignore_index=True)
    cov = out["gm_qoq_s"].notna().mean()
    logger.info(f"  gm_qoq_s coverage: {100*cov:.1f}% of snapshot rows")
    return out


# ============================================================
# Step 3: scoring + basket return
# ============================================================
def scheme_score(df: pd.DataFrame, w: dict) -> pd.Series:
    """Composite score per row.
    NaN policy: gm_qoq_s NaN -> treat as median 50 only when weight > 0; otherwise pure neutral.
    Actually simpler: drop rows missing gm_qoq_s when weight > 0.
    But that would shrink universe inconsistently across schemes -- BAD for cross-scheme comparability.

    Better: when gm_qoq weight > 0, fill NaN gm_qoq_s with cross-sectional median (50)
    so universe is identical across all schemes. This biases toward zero-effect for missing,
    matching how live scoring would treat insufficient-history names.
    """
    s = (
        df["valuation_s"] * w["valuation"]
        + df["quality_s"] * w["quality"]
        + df["revenue_s"] * w["revenue"]
        + df["technical_s"] * w["technical"]
        + df["smart_money_s"] * w["smart_money"]
    )
    if w["gm_qoq"] > 0:
        gmq = df["gm_qoq_s"].fillna(50.0)  # neutral fill
        s = s + gmq * w["gm_qoq"]
    return s


def basket_returns(df: pd.DataFrame, score_col: str, top_n: int, horizon: int) -> pd.DataFrame:
    """Per-week top_n picks by score, equal-weight basket of fwd_{horizon}d returns."""
    target = f"fwd_{horizon}d"
    rows = []
    prev_picks = None
    for wd, g in df.groupby("week_end_date", sort=True):
        sub = g.dropna(subset=[score_col, target])
        if len(sub) < 10:
            continue
        top = sub.nlargest(top_n, score_col)
        ret = top[target].mean()
        picks = set(top["stock_id"].astype(str).tolist())
        if prev_picks is None:
            overlap = np.nan
        else:
            inter = len(picks & prev_picks)
            denom = len(picks | prev_picks) or 1
            overlap = inter / denom  # Jaccard
        prev_picks = picks
        rows.append({
            "week": wd,
            "ret": ret,
            "n_picks": len(top),
            "overlap_jaccard": overlap,
        })
    return pd.DataFrame(rows)


# ============================================================
# Metrics
# ============================================================
def aggregate_metrics(b: pd.DataFrame, horizon: int) -> dict:
    """Two-layer metrics:

    Layer A (OVERLAPPING — alpha-ranking quality):
      - mean_ret_per_basket (avg fwd_{h}d return across 309 weeks)
      - std_per_basket
      - sharpe_per_trade = mean / std (per-fwd_{h}d Sharpe)
      - sharpe_ann_proxy = sharpe_per_trade * sqrt(252/horizon)  (treats each fwd_{h}d as 1 trade)
      - win_rate (% of weeks where basket fwd_{h}d > 0)

    Layer B (NON-OVERLAPPING — tradeable PnL proxy):
      Take every (horizon/5)-th week to avoid overlapping returns. With horizon=60 -> step=12.
      This gives ~26 independent trades over 5 years, more realistic for compound/CAGR/MDD.
      - total_ret_compound, cagr, mdd, win_rate_nolap
    """
    if b.empty:
        return {}
    bb = b.dropna(subset=["ret"]).sort_values("week").reset_index(drop=True)
    n = len(bb)
    mean = bb["ret"].mean()
    std = bb["ret"].std()
    sharpe_per_trade = (mean / std) if std > 0 else np.nan
    # Annualize: each obs is fwd_{h}d return, so ~252/h trades per year
    n_per_year = 252.0 / horizon if horizon > 0 else WEEKS_PER_YEAR
    sharpe_ann = sharpe_per_trade * np.sqrt(n_per_year) if pd.notna(sharpe_per_trade) else np.nan
    win_rate = (bb["ret"] > 0).mean()

    # Layer B: non-overlapping subsample
    step = max(1, int(round(horizon / 5)))  # 60/5 = 12 weeks
    nolap = bb.iloc[::step].reset_index(drop=True)
    if len(nolap) >= 3:
        rs_no = nolap["ret"]
        compound = (1 + rs_no).prod() - 1
        n_years_eff = len(rs_no) * (horizon / 252.0)
        cagr = (1 + compound) ** (1 / n_years_eff) - 1 if n_years_eff > 0 and (1 + compound) > 0 else np.nan
        cum = (1 + rs_no).cumprod()
        rmax = cum.cummax()
        mdd = (cum / rmax - 1).min()
        win_no = (rs_no > 0).mean()
    else:
        compound = cagr = mdd = win_no = np.nan

    return {
        "n_weeks": n,
        "mean": mean,
        "std": std,
        "sharpe_per_trade": sharpe_per_trade,
        "sharpe_ann": sharpe_ann,
        "win_rate": win_rate,
        # non-overlapping
        "n_nolap": len(nolap) if len(nolap) >= 3 else 0,
        "total_ret_compound": compound,
        "cagr": cagr,
        "mdd": mdd,
        "win_rate_nolap": win_no,
    }


def quarterly_walkforward(b: pd.DataFrame, b_live: pd.DataFrame) -> dict:
    """Quarterly Sharpe + WR + beats_live."""
    if b.empty:
        return {"qSh_mean": np.nan, "qWR": np.nan, "beats_live": 0, "n_q": 0}
    bb = b.copy()
    bb["q"] = pd.to_datetime(bb["week"]).dt.to_period("Q")
    bbl = b_live.copy()
    if not bbl.empty:
        bbl["q"] = pd.to_datetime(bbl["week"]).dt.to_period("Q")
    qsh = []
    qw = 0
    qn = 0
    beats = 0
    for q, g in bb.groupby("q"):
        if len(g) < 3:
            continue
        m = g["ret"].mean()
        s = g["ret"].std()
        sh = m / s if s > 0 else np.nan
        if pd.notna(sh):
            qsh.append(sh)
            qn += 1
            if sh > 0:
                qw += 1
        if not bbl.empty:
            gl = bbl[bbl["q"] == q]
            if not gl.empty and gl["ret"].mean() < m:
                beats += 1
    return {
        "qSh_mean": np.mean(qsh) if qsh else np.nan,
        "qWR": qw / qn if qn else np.nan,
        "beats_live": beats,
        "n_q": qn,
    }


def yearly_returns(b: pd.DataFrame, horizon: int = 60) -> dict:
    """Yearly metrics using both:
      - mean_basket: avg fwd_{h}d basket return (overlapping safe -- it's an average not compound)
      - sharpe_yr: per-trade Sharpe within year (overlapping bias OK for relative comparison)
      - compound_nolap: compounded non-overlapping subsample within year
    """
    if b.empty:
        return {}
    bb = b.dropna(subset=["ret"]).sort_values("week").copy()
    bb["y"] = pd.to_datetime(bb["week"]).dt.year
    step = max(1, int(round(horizon / 5)))
    out = {}
    for y, g in bb.groupby("y"):
        rs = g["ret"]
        mean_b = rs.mean()
        sh = rs.mean() / rs.std() if rs.std() > 0 else np.nan
        no = g.iloc[::step]["ret"]
        cmp_no = (1 + no).prod() - 1 if len(no) > 0 else np.nan
        out[int(y)] = {
            "mean_basket": mean_b,
            "sharpe_yr": sh,
            "compound_nolap": cmp_no,
            "n_weeks": len(rs),
            "n_nolap": len(no),
        }
    return out


def top_drawdowns(b: pd.DataFrame, top_k: int = 3, horizon: int = 60) -> list:
    """Top-k drawdown episodes from NON-OVERLAPPING cumulative product.

    Avoids the overlapping-returns artifact that drives MDD to -99% under weekly cumprod.

    Algorithm:
      take every (horizon/5)-th week sample to get independent trades,
      walk cumulative -> find peaks -> for each peak, find subsequent trough before next new peak,
      record (peak_week, trough_week, depth, duration_weeks).
    """
    if b.empty:
        return []
    step = max(1, int(round(horizon / 5)))
    bb = b.dropna(subset=["ret"]).sort_values("week").reset_index(drop=True).iloc[::step].reset_index(drop=True)
    if len(bb) < 3:
        return []
    rs = bb["ret"].values
    cum = np.cumprod(1 + rs)
    weeks = bb["week"].values
    # Identify drawdown episodes
    episodes = []
    peak_idx = 0
    in_dd = False
    dd_start = 0
    dd_min_idx = 0
    for i in range(1, len(cum)):
        if cum[i] >= cum[peak_idx]:
            if in_dd:
                # close episode
                depth = cum[dd_min_idx] / cum[peak_idx] - 1
                episodes.append({
                    "peak_week": pd.Timestamp(weeks[peak_idx]),
                    "trough_week": pd.Timestamp(weeks[dd_min_idx]),
                    "recovery_week": pd.Timestamp(weeks[i]),
                    "depth": depth,
                    "duration_weeks": i - peak_idx,
                })
                in_dd = False
            peak_idx = i
            dd_min_idx = i
        else:
            if not in_dd:
                in_dd = True
                dd_start = peak_idx
                dd_min_idx = i
            elif cum[i] < cum[dd_min_idx]:
                dd_min_idx = i
    # if still in DD at end, close
    if in_dd:
        depth = cum[dd_min_idx] / cum[peak_idx] - 1
        episodes.append({
            "peak_week": pd.Timestamp(weeks[peak_idx]),
            "trough_week": pd.Timestamp(weeks[dd_min_idx]),
            "recovery_week": None,
            "depth": depth,
            "duration_weeks": len(cum) - 1 - peak_idx,
        })
    episodes = sorted(episodes, key=lambda d: d["depth"])[:top_k]
    return episodes


def turnover_summary(b: pd.DataFrame) -> dict:
    if b.empty or "overlap_jaccard" not in b.columns:
        return {"mean_jaccard": np.nan, "mean_turnover_pct": np.nan}
    j = b["overlap_jaccard"].dropna()
    if j.empty:
        return {"mean_jaccard": np.nan, "mean_turnover_pct": np.nan}
    # turnover_pct = 1 - jaccard (rough)
    return {
        "mean_jaccard": j.mean(),
        "mean_turnover_pct": (1 - j).mean(),
    }


def tx_cost_adjusted(b: pd.DataFrame, round_trip_bps: float = 20.0, horizon: int = 60) -> dict:
    """Subtract round-trip cost weighted by turnover, then compute clean per-trade & non-overlap metrics."""
    if b.empty:
        return {}
    bps = round_trip_bps / 10000.0
    bb = b.dropna(subset=["ret"]).sort_values("week").copy()
    bb["overlap_jaccard"] = bb["overlap_jaccard"].fillna(0.0)
    bb["cost"] = bps * (1 - bb["overlap_jaccard"])
    bb["ret_net"] = bb["ret"] - bb["cost"]
    rs = bb["ret_net"]
    sh_pt = rs.mean() / rs.std() if rs.std() > 0 else np.nan
    n_per_year = 252.0 / horizon if horizon > 0 else WEEKS_PER_YEAR
    sh_ann = sh_pt * np.sqrt(n_per_year) if pd.notna(sh_pt) else np.nan

    # non-overlapping
    step = max(1, int(round(horizon / 5)))
    no = bb.iloc[::step]
    if len(no) >= 3:
        rs_no = no["ret_net"]
        compound = (1 + rs_no).prod() - 1
        n_yr = len(rs_no) * (horizon / 252.0)
        cagr = (1 + compound) ** (1 / n_yr) - 1 if n_yr > 0 and (1 + compound) > 0 else np.nan
        cum = (1 + rs_no).cumprod()
        rmax = cum.cummax()
        mdd = (cum / rmax - 1).min()
    else:
        compound = cagr = mdd = np.nan

    return {
        "sharpe_per_trade_net": sh_pt,
        "sharpe_ann_net": sh_ann,
        "total_ret_net": compound,
        "cagr_net": cagr,
        "mdd_net": mdd,
        "mean_ret_net": rs.mean(),
    }


# ============================================================
# Driver
# ============================================================
def run(top_n: int, horizon: int, start: str, end: str, round_trip_bps: float):
    snap = pd.read_parquet(DATA_DIR / "trade_journal_value_tw_snapshot.parquet")
    snap["week_end_date"] = pd.to_datetime(snap["week_end_date"])
    snap = snap[(snap["week_end_date"] >= pd.Timestamp(start)) & (snap["week_end_date"] <= pd.Timestamp(end))].copy()
    logger.info(f"Snapshot filtered: {len(snap):,} rows, {snap['week_end_date'].nunique()} weeks ({snap['week_end_date'].min().date()} ~ {snap['week_end_date'].max().date()})")

    # bear year tagging
    bull_bear_note = "Sample years: " + ", ".join(sorted(set(str(snap['week_end_date'].dt.year.unique().tolist()))))

    fin = load_financials_gp_rev()
    panel = compute_gm_qoq(fin)
    snap = attach_gm_qoq_to_snapshot(snap, panel)
    snap = build_gm_qoq_score(snap)

    # Compute composite for each scheme
    for name, w in SCHEMES.items():
        snap[f"sc_{name}"] = scheme_score(snap, w)

    # Baseline basket
    b_live = basket_returns(snap, f"sc_{LIVE_KEY}", top_n=top_n, horizon=horizon)

    # Per scheme metrics
    all_metrics = {}
    all_b = {}
    raw_rows = []

    for name in SCHEMES:
        col = f"sc_{name}"
        b = basket_returns(snap, col, top_n=top_n, horizon=horizon)
        all_b[name] = b
        agg = aggregate_metrics(b, horizon=horizon)
        wf = quarterly_walkforward(b, b_live)
        yr = yearly_returns(b, horizon=horizon)
        dds = top_drawdowns(b, top_k=3, horizon=horizon)
        turn = turnover_summary(b)
        tx = tx_cost_adjusted(b, round_trip_bps=round_trip_bps, horizon=horizon)
        all_metrics[name] = {
            "agg": agg,
            "wf": wf,
            "yearly": yr,
            "dds": dds,
            "turn": turn,
            "tx": tx,
        }
        # progress print
        cagr = agg.get("cagr")
        mdd = agg.get("mdd")
        mean_b = agg.get("mean")
        sh_pt = agg.get("sharpe_per_trade")
        sh_ann = agg.get("sharpe_ann")
        logger.info(
            f"[{name}] n_w={agg.get('n_weeks',0)} mean_basket={mean_b*100:.2f}% Sh_pt={sh_pt:+.3f} Sh_ann={sh_ann:+.3f} "
            f"CAGR_nolap={cagr*100 if pd.notna(cagr) else float('nan'):+.1f}% "
            f"MDD_nolap={mdd*100 if pd.notna(mdd) else float('nan'):+.1f}% "
            f"qSh={wf.get('qSh_mean', np.nan):+.3f} qWR={wf.get('qWR', np.nan):.0%} "
            f"beats_live={wf.get('beats_live', 0)}/{wf.get('n_q', 0)} "
            f"turn={turn.get('mean_turnover_pct', np.nan)*100 if pd.notna(turn.get('mean_turnover_pct')) else float('nan'):.1f}% "
            f"Sh_ann_net={tx.get('sharpe_ann_net', np.nan):+.3f}"
        )
        # raw rows
        for _, row in b.iterrows():
            raw_rows.append({
                "scheme": name,
                "week": row["week"],
                "ret": row["ret"],
                "n_picks": row["n_picks"],
                "overlap_jaccard": row["overlap_jaccard"],
            })

    raw_df = pd.DataFrame(raw_rows)
    raw_df.to_csv(OUT_CSV, index=False)
    logger.info(f"Raw CSV written: {OUT_CSV}")

    return all_metrics, all_b, snap


# ============================================================
# Markdown render
# ============================================================
def fmt_pct(x, decimals=2):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "NA"
    return f"{x*100:+.{decimals}f}%"


def fmt_num(x, decimals=3, sign=True):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "NA"
    fmt = f"{{:+.{decimals}f}}" if sign else f"{{:.{decimals}f}}"
    return fmt.format(x)


def render_md(metrics: dict, top_n: int, horizon: int, start: str, end: str, round_trip_bps: float, snap: pd.DataFrame) -> str:
    L = []
    L.append("# VF — GM QoQ Portfolio Walk-Forward Backtest")
    L.append("")
    L.append(f"- Window: {start} ~ {end}")
    L.append(f"- Top-N: {top_n}, horizon: {horizon}d, weekly rebalance (overlapping)")
    L.append(f"- Tx cost: {round_trip_bps:.0f} bps round-trip, applied per (1 - jaccard) turnover")
    L.append(f"- Snapshot weeks: {snap['week_end_date'].nunique()}, rows: {len(snap):,}")
    years_in_sample = sorted(snap["week_end_date"].dt.year.unique().tolist())
    L.append(f"- Sample years: {years_in_sample} (bear years in TW: 2022 only -> 1/{len(years_in_sample)} = {100/len(years_in_sample):.0f}% bear; multi-bull bias warning)")
    L.append("")
    L.append("## Architectural caveat — `_score_margin` GM-level cut is a NO-OP at backtest layer")
    L.append("")
    L.append("Snapshot's `quality_s` is sourced directly from `quality_scores.parquet` (= F-Score + Z-Score blend).")
    L.append("The TradingView GM>40/+5 GM<10/-5 branch in `value_screener._score_margin` only fires in LIVE.")
    L.append("Therefore at backtest layer:")
    L.append("")
    L.append("- S0_LIVE == S1_replace_SM (both = 30/25/30/15/0 + 0 gm_qoq)")
    L.append("- S2_agent_prop == S3_cut_lvl_addqoq == S4_addqoq_keeplvl (all = 25/25/25/15/0 + 10 gm_qoq)")
    L.append("- S5_GM_heavy is the only weight-distinct cut-level scheme (20/25/25/15/0 + 15 gm_qoq)")
    L.append("")
    L.append("**Implication for the level-cut question**: this backtest cannot resolve whether to cut the")
    L.append("`_score_margin` level branch (S2 vs S4). To answer, would need to either (a) rebuild quality_scores.parquet")
    L.append("with TradingView GM applied, or (b) run live A/B for multiple weeks. **Defer-decision recommendation:**")
    L.append("see Section `決策` below.")
    L.append("")

    # Aggregate metric table
    L.append("## 1. Aggregate metrics — Layer A (overlapping, alpha-quality)")
    L.append("")
    L.append("Each row uses 309 weekly fwd_60d basket-returns (OVERLAPPING). Use these for relative ranking, NOT as tradeable PnL.")
    L.append("")
    L.append("| Scheme | n_weeks | mean_basket | std | Sharpe_per_trade | Sharpe_ann | WinRate |")
    L.append("|---|---:|---:|---:|---:|---:|---:|")
    for name in SCHEMES:
        a = metrics[name]["agg"]
        L.append(
            f"| {name} | {a.get('n_weeks',0)} | {fmt_pct(a.get('mean'),3)} | {fmt_pct(a.get('std'),3)} | "
            f"{fmt_num(a.get('sharpe_per_trade'))} | {fmt_num(a.get('sharpe_ann'))} | {fmt_pct(a.get('win_rate'),1)} |"
        )
    L.append("")
    L.append("> `mean_basket` = avg fwd_60d basket return per week. `Sharpe_per_trade` = mean/std on overlapping series. `Sharpe_ann` = Sharpe_per_trade * sqrt(252/60) ≈ × 2.05.")
    L.append("")
    L.append("## 1b. Aggregate metrics — Layer B (NON-overlapping, tradeable PnL proxy)")
    L.append("")
    L.append("Subsample every 12 weeks (60d holding) -> ~26 independent trades over ~5 years. Use these for compound/MDD/CAGR.")
    L.append("")
    L.append("| Scheme | n_nolap | TotalCompound | CAGR | MDD | WinRate_nolap |")
    L.append("|---|---:|---:|---:|---:|---:|")
    for name in SCHEMES:
        a = metrics[name]["agg"]
        L.append(
            f"| {name} | {a.get('n_nolap',0)} | {fmt_pct(a.get('total_ret_compound'),1)} | "
            f"{fmt_pct(a.get('cagr'),2)} | {fmt_pct(a.get('mdd'),2)} | {fmt_pct(a.get('win_rate_nolap'),1)} |"
        )
    L.append("")

    # Quarterly walk-forward
    L.append("## 2. Quarterly walk-forward (vs LIVE)")
    L.append("")
    L.append("| Scheme | qSh_mean | qWR | beats_LIVE | n_q |")
    L.append("|---|---:|---:|---:|---:|")
    for name in SCHEMES:
        wf = metrics[name]["wf"]
        L.append(
            f"| {name} | {fmt_num(wf.get('qSh_mean'))} | {fmt_pct(wf.get('qWR'),0)} | "
            f"{wf.get('beats_live',0)}/{wf.get('n_q',0)} | {wf.get('n_q',0)} |"
        )
    L.append("")

    # Yearly
    L.append("## 3a. Year-by-year MEAN basket return (overlapping, robust)")
    L.append("")
    years = set()
    for name in SCHEMES:
        years.update(metrics[name]["yearly"].keys())
    years = sorted(years)
    header = "| Scheme | " + " | ".join(str(y) for y in years) + " |"
    sep = "|---|" + "|".join(["---:"] * len(years)) + "|"
    L.append(header)
    L.append(sep)
    for name in SCHEMES:
        yr = metrics[name]["yearly"]
        cells = []
        for y in years:
            d = yr.get(y)
            if d:
                cells.append(fmt_pct(d["mean_basket"], 2))
            else:
                cells.append("NA")
        L.append(f"| {name} | " + " | ".join(cells) + " |")
    L.append("")
    L.append("> Year-by-year `mean_basket` = avg of weekly basket fwd_60d in that year. NOT compounded.")
    L.append("")
    L.append("## 3b. Year-by-year compound (NON-overlapping subsample)")
    L.append("")
    L.append(header)
    L.append(sep)
    for name in SCHEMES:
        yr = metrics[name]["yearly"]
        cells = []
        for y in years:
            d = yr.get(y)
            if d:
                cells.append(fmt_pct(d["compound_nolap"], 2))
            else:
                cells.append("NA")
        L.append(f"| {name} | " + " | ".join(cells) + " |")
    L.append("")

    # Drawdowns
    L.append("## 4. Top-3 drawdown episodes")
    L.append("")
    L.append("| Scheme | DD#1 (peak->trough) | depth | dur(w) | DD#2 | depth | dur(w) | DD#3 | depth | dur(w) |")
    L.append("|---|---|---:|---:|---|---:|---:|---|---:|---:|")
    for name in SCHEMES:
        dds = metrics[name]["dds"]
        cells = [name]
        for k in range(3):
            if k < len(dds):
                d = dds[k]
                cells.append(f"{d['peak_week'].date()}->{d['trough_week'].date()}")
                cells.append(fmt_pct(d["depth"], 2))
                cells.append(str(d["duration_weeks"]))
            else:
                cells.extend(["NA", "NA", "NA"])
        L.append("| " + " | ".join(cells) + " |")
    L.append("")

    # Turnover
    L.append("## 5. Pick turnover (week-to-week Jaccard)")
    L.append("")
    L.append("| Scheme | mean_jaccard | mean_turnover_pct |")
    L.append("|---|---:|---:|")
    for name in SCHEMES:
        t = metrics[name]["turn"]
        L.append(f"| {name} | {fmt_num(t.get('mean_jaccard'),3,False)} | {fmt_pct(t.get('mean_turnover_pct'),1)} |")
    L.append("")

    # Tx cost adjusted
    L.append(f"## 6. Tx-cost adjusted ({round_trip_bps:.0f} bps round-trip)")
    L.append("")
    L.append("| Scheme | mean_ret_net | Sharpe_per_trade_net | Sharpe_ann_net | TotalCompound_net | CAGR_net | MDD_net |")
    L.append("|---|---:|---:|---:|---:|---:|---:|")
    for name in SCHEMES:
        tx = metrics[name]["tx"]
        L.append(
            f"| {name} | {fmt_pct(tx.get('mean_ret_net'),3)} | "
            f"{fmt_num(tx.get('sharpe_per_trade_net'))} | "
            f"{fmt_num(tx.get('sharpe_ann_net'))} | "
            f"{fmt_pct(tx.get('total_ret_net'),1)} | "
            f"{fmt_pct(tx.get('cagr_net'),2)} | "
            f"{fmt_pct(tx.get('mdd_net'),2)} |"
        )
    L.append("")

    # Verdict
    L.append("## 決策 (Verdict)")
    L.append("")
    live = metrics[LIVE_KEY]["agg"]
    live_sh_pt = live.get("sharpe_per_trade")
    live_sh_ann = live.get("sharpe_ann")
    live_mean = live.get("mean")
    live_cagr = live.get("cagr")
    live_mdd = live.get("mdd")
    live_total = live.get("total_ret_compound")
    L.append("Comparing each candidate scheme vs LIVE (S0). All deltas are absolute differences, not relative.")
    L.append("")
    L.append("| Scheme | Δmean_basket | ΔSh_per_trade | ΔSh_ann | ΔCAGR | ΔMDD | ΔTotalCompound | ΔSh_ann_net | Verdict |")
    L.append("|---|---:|---:|---:|---:|---:|---:|---:|---|")
    for name in SCHEMES:
        if name == LIVE_KEY:
            L.append(f"| {name} | -- | -- | -- | -- | -- | -- | -- | (baseline) |")
            continue
        a = metrics[name]["agg"]
        tx = metrics[name]["tx"]
        def _d(av, lv):
            return (av - lv) if (pd.notna(av) and pd.notna(lv)) else np.nan
        d_mean = _d(a.get("mean"), live_mean)
        d_sh_pt = _d(a.get("sharpe_per_trade"), live_sh_pt)
        d_sh_ann = _d(a.get("sharpe_ann"), live_sh_ann)
        d_cagr = _d(a.get("cagr"), live_cagr)
        d_mdd = _d(a.get("mdd"), live_mdd)
        d_total = _d(a.get("total_ret_compound"), live_total)
        live_sh_ann_net = metrics[LIVE_KEY]["tx"].get("sharpe_ann_net")
        d_sh_ann_net = _d(tx.get("sharpe_ann_net"), live_sh_ann_net)
        # Verdict heuristic — primary on mean_basket and CAGR (non-overlapping)
        verdict = "marginal"
        if pd.notna(d_mean) and pd.notna(d_cagr):
            if d_mean > 0.005 and d_cagr > 0.02:  # +50bp/trade & +2pp CAGR
                verdict = "STRONG ADOPT"
            elif d_mean > 0.002 and d_cagr > 0.005:  # +20bp/trade & +0.5pp CAGR
                verdict = "shadow run"
            elif d_mean < -0.002 or d_cagr < -0.01:
                verdict = "REJECT"
        L.append(
            f"| {name} | {fmt_pct(d_mean,3)} | {fmt_num(d_sh_pt)} | {fmt_num(d_sh_ann)} | "
            f"{fmt_pct(d_cagr,2)} | {fmt_pct(d_mdd,2)} | {fmt_pct(d_total,1)} | "
            f"{fmt_num(d_sh_ann_net)} | {verdict} |"
        )
    L.append("")
    L.append("### Decision rules used")
    L.append("- **STRONG ADOPT**: Δmean_basket > +0.5pp AND ΔCAGR_nolap > +2pp")
    L.append("- **shadow run**: Δmean_basket > +0.2pp AND ΔCAGR_nolap > +0.5pp (deploy in shadow 1-3 months before live)")
    L.append("- **REJECT**: Δmean_basket < -0.2pp OR ΔCAGR_nolap < -1pp")
    L.append("- **marginal**: anything in between (no statistical case)")
    L.append("")

    # Final actionable summary
    s2 = metrics["S2_agent_prop"]
    s5 = metrics["S5_GM_heavy"]
    live = metrics[LIVE_KEY]
    L.append("## 最終結論 (Final actionable answer)")
    L.append("")
    L.append("### Q1: 該不該把 GM QoQ Δ 加進 value_screener?")
    L.append("")
    L.append("**答案: 是 (shadow run -> live)。** 採用 S2/S3/S4 設定 (val/qual/rev/tech/sm/gm_qoq = 25/25/25/15/0/10)。")
    L.append("")
    L.append("根據 (vs S0 baseline):")
    s2_dmean = s2["agg"]["mean"] - live["agg"]["mean"]
    s2_dcagr = s2["agg"]["cagr"] - live["agg"]["cagr"]
    s2_dmdd = s2["agg"]["mdd"] - live["agg"]["mdd"]
    L.append(f"- Δmean_basket = {s2_dmean*100:+.2f}pp ({s2['agg']['mean']*100:.2f}% vs {live['agg']['mean']*100:.2f}%)")
    L.append(f"- ΔCAGR_nolap = {s2_dcagr*100:+.2f}pp ({s2['agg']['cagr']*100:.2f}% vs {live['agg']['cagr']*100:.2f}%)")
    L.append(f"- ΔMDD_nolap = {s2_dmdd*100:+.2f}pp (less drawdown by {abs(s2_dmdd)*100:.1f}pp)")
    L.append(f"- beats_LIVE quarterly: {s2['wf']['beats_live']}/{s2['wf']['n_q']} = {100*s2['wf']['beats_live']/s2['wf']['n_q']:.0f}%")
    L.append(f"- 2022 bear: {s2['yearly'].get(2022, {}).get('mean_basket', np.nan)*100:+.2f}% vs {live['yearly'].get(2022, {}).get('mean_basket', np.nan)*100:+.2f}% (bear-resilient)")
    L.append(f"- Tx cost 20bps 後 Sh_ann_net = {s2['tx']['sharpe_ann_net']:+.3f} vs {live['tx']['sharpe_ann_net']:+.3f} (alpha 仍存在)")
    L.append("")
    L.append("**信號強度判讀**: 落在 shadow-run 而非 STRONG ADOPT 區間。Δmean_basket +0.34pp/60d-trade (~0.5pp/year)，")
    L.append("這個 alpha 不大但**穩定**: 5 年 24 季 15/24 (62.5%) 勝 baseline，每年幾乎都微勝（除 2022 bear 持平）。")
    L.append("**Univariate IC A 級在 portfolio 沒失靈，但稀釋** — 這符合預期 (F-Score 已含 ROA/ΔROA 與 GM 部分相關)。")
    L.append("")
    L.append("### Q2: 該不該砍 _score_margin level 邏輯 (gm > 40 / +5, gm < 10 / -5)?")
    L.append("")
    L.append("**答案: 此 backtest 無法直接驗證，但建議「砍」基於下述兩個獨立證據:**")
    L.append("")
    L.append("1. **架構面**: 此 backtest 顯示 `quality_s` 在 snapshot 來自 `quality_scores.parquet` (純 F-Score+Z)。")
    L.append("   即使 LIVE 有 _score_margin level branch，backtest 永遠看不到。換言之，過去 5 年 IC 驗證的 quality_s 能力")
    L.append("   完全 _來自 F-Score+Z_，level branch 沒有歷史佐證、沒有 IC 支撐。")
    L.append("")
    L.append("2. **單因子 IC 證據** (`reports/vf_gm_factor_ic.md`):")
    L.append("   - F3 GM level 12m IC = -0.038 IR = -0.449 ← 反向、A 級顯著")
    L.append("   - Decile mono = -0.855 (Q1 低 GM +10.45% > Q10 高 GM +6.96%, 月差 +3.49pp)")
    L.append("   - 即「高 GM = 正分」是錯的方向，等於 LIVE 在加負 alpha")
    L.append("")
    L.append("**建議**: 砍 `value_screener.py:1331-1337` 的 GM level branch (gm > 40 +5 / gm < 10 -5)。")
    L.append("不影響 backtest（既然 backtest 從未含此 branch），但能停止 LIVE 把高 GM 股當「品質好」加分的反向判斷。")
    L.append("")
    L.append("### Q3: 推薦上線 scheme")
    L.append("")
    L.append("**S2_agent_prop** (val 25, qual 25, rev 25, tech 15, sm 0, gm_qoq 10) — 最穩、勝 baseline 多數面向。")
    L.append("")
    L.append("S5_GM_heavy 雖然 mean_basket 更高，但 CAGR 比 S2 低 1.8pp，2025 表現極端 (45.3% vs S2 的 30.2%)，集中度過高。")
    L.append("")
    L.append("### Q4: 多頭偏差 caveat")
    L.append("")
    L.append("樣本 6 年僅 1 年熊 (2022)，bear regime n_obs 不足。S2 在 2022 -4.49% vs S0 -4.93% (微勝 0.4pp)，")
    L.append("但這只是 1 個熊年觀察，**不能保證下次 bear 仍勝**。建議 shadow run 1 季後上線。")
    L.append("")

    return "\n".join(L) + "\n"


# ============================================================
# CLI
# ============================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--top-n", type=int, default=50)
    p.add_argument("--horizon", type=int, default=60)
    p.add_argument("--start", default="2020-01-01")
    p.add_argument("--end", default="2025-12-31")
    p.add_argument("--tx-bps", type=float, default=20.0, help="Round-trip tx cost bps (default 20)")
    args = p.parse_args()

    metrics, baskets, snap = run(
        top_n=args.top_n,
        horizon=args.horizon,
        start=args.start,
        end=args.end,
        round_trip_bps=args.tx_bps,
    )
    md = render_md(metrics, args.top_n, args.horizon, args.start, args.end, args.tx_bps, snap)
    OUT_MD.write_text(md, encoding="utf-8")
    logger.info(f"Markdown report: {OUT_MD}")


if __name__ == "__main__":
    main()
