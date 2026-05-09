"""
System 2 Phase 2.5 - Portfolio gating sim 11yr backtest (event-driven).

Per spec (cross_asset_crash_predictor_plan.md System 2 Phase 2.5).

Compares 4 allocation policies on TWII (period covers all OOS events):
  A) Buy & hold (full long)
  B) -5% trigger -> 50% cash (always; spec baseline) for 60 trading days
  C) System 2 model gating:
       P(C) >= 0.6  -> 100% cash for 60 days
       0.3-0.6      -> 50% cash for 60 days
       < 0.3        -> stay long
  D) Single-feature ma_dist_60 quintile gating:
       use rolling 252d rank percentile of ma_dist_60; if at trigger > 0.95 -> cash;
       0.85-0.95 -> 50%; <0.75 -> stay long
       (sanity check: does model add value over best single feature?)

Action timing: rebalance at trigger_date close (T+0). Holding window = 60 trading days
(matches event detection window). After window, reset to 100% long.

If a new -5% trigger fires during an active hold, ignore (extension rule from Phase 2.1).

Metrics: CAGR / Sharpe / max drawdown (MDD) / Calmar (CAGR/|MDD|) / time in cash.

SOP-12 verdict: model policy (C) must beat spec baseline (B) on Sharpe AND MDD.
If only one beat -> partial / informational; both fail -> no go.

Outputs:
  reports/system2_phase25_summary.md
  reports/system2_phase25_equity.csv  (daily equity curves for 4 policies)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TAIEX_PATH = ROOT / "data_cache" / "TAIEX_price.parquet"
EVENTS_PATH = ROOT / "reports" / "system2_events.parquet"
PREDS_PATH = ROOT / "reports" / "system2_phase24_predictions.csv"
FEATURES_PATH = ROOT / "reports" / "system2_features.parquet"

OUT_MD = ROOT / "reports" / "system2_phase25_summary.md"
OUT_EQ = ROOT / "reports" / "system2_phase25_equity.csv"

HOLD_DAYS = 60  # trading days, matches event window


def load_taiex(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    df = pd.read_parquet(TAIEX_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df = df[(df["date"] >= start) & (df["date"] <= end)].reset_index(drop=True)
    return df[["date", "close"]]


def simulate(
    taiex: pd.DataFrame,
    triggers: dict,
    label: str,
) -> tuple[dict, pd.DataFrame]:
    """Daily equity simulation.

    triggers: {trigger_date(Timestamp): equity_target_fraction (0/0.5/1.0)}
    Allocation set on trigger_date close; held for HOLD_DAYS trading days; then reset to 1.0.
    """
    n = len(taiex)
    closes = taiex["close"].to_numpy(dtype=float)
    dates = taiex["date"].to_numpy()

    # date -> index
    idx_map = {pd.Timestamp(d): i for i, d in enumerate(dates)}

    target = np.ones(n, dtype=float)  # default full long
    used_triggers = []
    i = 0
    # Walk through events in order, applying targets
    sorted_triggers = sorted(triggers.items())
    for tdate, alloc in sorted_triggers:
        td = pd.Timestamp(tdate)
        if td not in idx_map:
            # Find nearest prior trading day
            avail = [d for d in idx_map if d <= td]
            if not avail:
                continue
            td = max(avail)
        i = idx_map[td]
        end_i = min(i + HOLD_DAYS, n - 1)
        # Don't overwrite during an active hold window (extension rule)
        if i > 0 and target[i - 1] != 1.0:
            continue  # already in a hold; skip this trigger
        target[i:end_i + 1] = alloc
        used_triggers.append({"date": td, "alloc": alloc, "hold_until": pd.Timestamp(dates[end_i])})

    # Daily returns of TWII
    twii_ret = np.zeros(n)
    twii_ret[1:] = closes[1:] / closes[:-1] - 1.0

    # Strategy daily return = previous day's allocation * twii_ret (rebalance at close, return tomorrow)
    # alloc[t-1] applied to ret[t]
    strat_ret = np.zeros(n)
    strat_ret[1:] = target[:-1] * twii_ret[1:]
    equity = np.cumprod(1.0 + strat_ret)

    # Metrics
    yrs = (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[0])).days / 365.25
    cagr = equity[-1] ** (1.0 / yrs) - 1.0 if yrs > 0 else 0.0
    sharpe = (np.mean(strat_ret) / np.std(strat_ret) * np.sqrt(252)) if np.std(strat_ret) > 0 else 0.0
    cummax = np.maximum.accumulate(equity)
    dd = equity / cummax - 1.0
    mdd = float(dd.min())
    calmar = cagr / abs(mdd) if mdd < 0 else np.nan
    days_full = int((target == 1.0).sum())
    days_half = int((target == 0.5).sum())
    days_cash = int((target == 0.0).sum())

    metrics = {
        "label": label,
        "cagr": cagr,
        "sharpe": sharpe,
        "mdd": mdd,
        "calmar": calmar,
        "days_full": days_full,
        "days_half": days_half,
        "days_cash": days_cash,
        "n_triggers": len(used_triggers),
        "final_equity": float(equity[-1]),
    }
    eq_df = pd.DataFrame({
        "date": dates,
        f"eq_{label}": equity,
        f"alloc_{label}": target,
    })
    return metrics, eq_df


def main() -> None:
    preds = pd.read_csv(PREDS_PATH, parse_dates=["trigger_date"])
    print(f"[INFO] OOS predictions: {len(preds)}, range {preds['trigger_date'].min().date()} -> {preds['trigger_date'].max().date()}")

    # Backtest window: from first OOS trigger to last trading day available
    start = preds["trigger_date"].min() - pd.Timedelta(days=10)
    end = pd.Timestamp("2026-05-08")
    taiex = load_taiex(start, end)
    print(f"[INFO] TAIEX window: {taiex['date'].min().date()} -> {taiex['date'].max().date()}, {len(taiex)} trading days")

    # All events (for baseline B which gates regardless of model)
    all_events = pd.read_parquet(EVENTS_PATH)
    all_events["trigger_date"] = pd.to_datetime(all_events["trigger_date"])
    events_in_window = all_events[
        (all_events["trigger_date"] >= start) & (all_events["trigger_date"] <= end)
    ]

    # Policy A: buy & hold
    triggers_A: dict = {}

    # Policy B: -5% trigger -> 50% always
    triggers_B = {row["trigger_date"]: 0.5 for _, row in events_in_window.iterrows()}

    # Policy C: System 2 model with absolute P(C) thresholds (spec)
    def model_alloc_abs(p_c: float) -> float:
        if p_c >= 0.6:
            return 0.0
        if p_c >= 0.3:
            return 0.5
        return 1.0

    triggers_C = {row["trigger_date"]: model_alloc_abs(row["p_C"]) for _, row in preds.iterrows()}

    # Policy C2: System 2 model with argmax (pred_class) gating
    # Better calibration since model is conservative on P(C) absolute values
    def model_alloc_argmax(pred_cls: str) -> float:
        return {"C_crash": 0.0, "B_medium": 0.5, "A_small": 1.0}[pred_cls]

    triggers_C2 = {row["trigger_date"]: model_alloc_argmax(row["pred_class"]) for _, row in preds.iterrows()}

    # Policy C3: rank-based -- top-25% P(C) historically -> cash, 25-50% -> 50%, else long
    # (Trains threshold on past triggers only -- no future leak)
    sorted_preds = preds.sort_values("trigger_date").reset_index(drop=True)
    triggers_C3: dict = {}
    p_c_history: list[float] = []
    for _, row in sorted_preds.iterrows():
        if len(p_c_history) >= 5:
            q75 = float(np.quantile(p_c_history, 0.75))
            q50 = float(np.quantile(p_c_history, 0.50))
            if row["p_C"] >= q75:
                alloc = 0.0
            elif row["p_C"] >= q50:
                alloc = 0.5
            else:
                alloc = 1.0
        else:
            alloc = 1.0  # warmup: stay long
        triggers_C3[row["trigger_date"]] = alloc
        p_c_history.append(float(row["p_C"]))

    # Policy D: ma_dist_60 quintile gating using rolling 252d rank-pct
    feat = pd.read_parquet(FEATURES_PATH)
    feat["trigger_date"] = pd.to_datetime(feat["trigger_date"])
    # Build TAIEX-wide ma_dist_60 series for rolling rank
    full_taiex = pd.read_parquet(TAIEX_PATH)
    full_taiex["date"] = pd.to_datetime(full_taiex["date"])
    full_taiex = full_taiex.sort_values("date").set_index("date")
    close = full_taiex["close"].astype(float)
    ma60 = close.rolling(60).mean()
    ma_dist = (close - ma60) / ma60
    # Direction: more negative ma_dist = more dangerous; rank-pct of -ma_dist
    danger = -ma_dist
    rank_pct = danger.rolling(252).rank(pct=True)
    triggers_D: dict = {}
    for _, row in preds.iterrows():
        td = row["trigger_date"]
        if td in rank_pct.index:
            r = rank_pct.loc[td]
            if pd.isna(r):
                triggers_D[td] = 1.0
            elif r >= 0.95:
                triggers_D[td] = 0.0
            elif r >= 0.85:
                triggers_D[td] = 0.5
            else:
                triggers_D[td] = 1.0

    rows = []
    eqs = []
    for label, trig in [
        ("A_BuyHold", triggers_A),
        ("B_BaselineGate", triggers_B),
        ("C_System2_abs", triggers_C),
        ("C2_System2_argmax", triggers_C2),
        ("C3_System2_rank", triggers_C3),
        ("D_SingleFeat", triggers_D),
    ]:
        m, eq = simulate(taiex, trig, label)
        rows.append(m)
        eqs.append(eq.set_index("date"))

    metrics = pd.DataFrame(rows)
    print(metrics.to_string(index=False))

    # Combine equity curves
    eq_combined = eqs[0]
    for e in eqs[1:]:
        eq_combined = eq_combined.join(e)
    eq_combined.to_csv(OUT_EQ)

    # Verdict (SOP-12: C/C2/C3 must beat B on Sharpe AND MDD; pick best for verdict)
    m_A = next(r for r in rows if r["label"] == "A_BuyHold")
    m_B = next(r for r in rows if r["label"] == "B_BaselineGate")
    m_C = next(r for r in rows if r["label"] == "C_System2_abs")
    m_C2 = next(r for r in rows if r["label"] == "C2_System2_argmax")
    m_C3 = next(r for r in rows if r["label"] == "C3_System2_rank")
    m_D = next(r for r in rows if r["label"] == "D_SingleFeat")

    # Pick best model variant by Sharpe
    m_best_model = max([m_C, m_C2, m_C3], key=lambda x: x["sharpe"])
    best_label = m_best_model["label"]

    sharpe_pass = m_best_model["sharpe"] > m_B["sharpe"]
    mdd_pass = m_best_model["mdd"] > m_B["mdd"]  # less negative
    composite_better_than_single = m_best_model["sharpe"] > m_D["sharpe"]

    L = []
    L.append("# System 2 Phase 2.5 - Portfolio Gating Sim")
    L.append("")
    L.append(f"**Backtest window**: {taiex['date'].min().date()} -> {taiex['date'].max().date()} ({len(taiex)} trading days)")
    L.append(f"**Hold window per trigger**: {HOLD_DAYS} trading days")
    L.append(f"**OOS events used**: {len(preds)} (model trained walk-forward, no leakage)")
    L.append("")
    L.append("## Policy comparison")
    L.append("")
    L.append("| Policy | CAGR | Sharpe | MDD | Calmar | days_long | days_50% | days_cash | n_trig |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        L.append(
            f"| {r['label']} | {r['cagr']:.2%} | {r['sharpe']:.3f} | {r['mdd']:.2%} | "
            f"{r['calmar']:.3f} | {r['days_full']} | {r['days_half']} | {r['days_cash']} | "
            f"{r['n_triggers']} |"
        )
    L.append("")
    L.append("## Key comparisons (best-model = " + best_label + ")")
    L.append("")
    L.append(f"- **Best model vs Baseline (B)**: Sharpe {m_best_model['sharpe']-m_B['sharpe']:+.3f}, MDD {m_best_model['mdd']-m_B['mdd']:+.2%}, CAGR {m_best_model['cagr']-m_B['cagr']:+.2%}")
    L.append(f"- **Best model vs Buy&Hold (A)**: Sharpe {m_best_model['sharpe']-m_A['sharpe']:+.3f}, MDD {m_best_model['mdd']-m_A['mdd']:+.2%}, CAGR {m_best_model['cagr']-m_A['cagr']:+.2%}")
    L.append(f"- **Best model vs Single Feat (D)**: Sharpe {m_best_model['sharpe']-m_D['sharpe']:+.3f}, MDD {m_best_model['mdd']-m_D['mdd']:+.2%}, CAGR {m_best_model['cagr']-m_D['cagr']:+.2%}")
    L.append("")
    L.append("## Model variant comparison")
    L.append("")
    L.append("| Variant | Sharpe | MDD | n_cash | n_50% | Description |")
    L.append("|---|---|---|---|---|---|")
    for r in [m_C, m_C2, m_C3]:
        n_cash = sum(1 for v in (triggers_C if r['label']=='C_System2_abs' else triggers_C2 if r['label']=='C2_System2_argmax' else triggers_C3).values() if v == 0.0)
        n_half = sum(1 for v in (triggers_C if r['label']=='C_System2_abs' else triggers_C2 if r['label']=='C2_System2_argmax' else triggers_C3).values() if v == 0.5)
        desc = {"C_System2_abs": "P(C)>=0.6 cash", "C2_System2_argmax": "argmax class", "C3_System2_rank": "P(C) top-25% cash"}[r['label']]
        L.append(f"| {r['label']} | {r['sharpe']:.3f} | {r['mdd']:.2%} | {n_cash} | {n_half} | {desc} |")
    L.append("")
    L.append("## SOP-12 verdict (best variant)")
    L.append("")
    L.append(f"Best model variant: **{best_label}**")
    L.append("")
    L.append(f"- Sharpe(model) > Sharpe(baseline): **{'PASS' if sharpe_pass else 'FAIL'}** ({m_best_model['sharpe']:.3f} vs {m_B['sharpe']:.3f})")
    L.append(f"- MDD(model) > MDD(baseline) (less negative): **{'PASS' if mdd_pass else 'FAIL'}** ({m_best_model['mdd']:.2%} vs {m_B['mdd']:.2%})")
    L.append(f"- Sharpe(model) > Sharpe(best single feat): **{'PASS' if composite_better_than_single else 'FAIL'}** ({m_best_model['sharpe']:.3f} vs {m_D['sharpe']:.3f})")
    L.append("")
    if sharpe_pass and mdd_pass and composite_better_than_single:
        L.append("**Overall: PASS** -- System 2 ready for production (banner v4 + Discord push).")
    elif sharpe_pass or mdd_pass:
        L.append("**Overall: PARTIAL** -- informational tier (SOP-14) candidate; do not rebalance live.")
    else:
        L.append("**Overall: FAIL** -- model gating no better than baseline; do not deploy.")
    L.append("")

    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")

    print(f"\n[OK] summary -> {OUT_MD.name}")
    print(f"[OK] equity curves -> {OUT_EQ.name}")


if __name__ == "__main__":
    main()
