"""
System 3 Phase 3.4 - Portfolio gating sim (1w-1mo crash predictor).

Phase 3.3 logistic L2 composite AUC 0.679 < single ma_dist_60 rank 0.714 (FAIL on
SOP-12 composite-beat-single). Phase 3.3b rank composite AUC 0.731 > single 0.699
(PASS). This phase tests in real-world portfolio context.

Policies (all 11+ year backtest):
  A) Buy & hold TAIEX
  B) Always 50% hedged (cost-of-capital baseline)
  C1) Composite (Phase 3.3b rank-weighted) top-5% threshold -> 0% equity for 21 days
       top-10% (excluding top-5%) -> 50% for 21 days
       else stay long
  C2) Single ma_dist_60 rolling rank: rank>=0.95 -> cash, 0.85-0.95 -> 50%, else long
  C3) AND combo: composite>=top-10% AND ma_dist_60_rank>=0.85 -> cash,
                  composite>=top-20% OR ma_dist_60_rank>=0.85 -> 50%, else long
  C4) OR combo: composite>=top-5% OR ma_dist_60_rank>=0.95 -> cash,
                composite>=top-15% OR ma_dist_60_rank>=0.85 -> 50%, else long

Hold window: 21 trading days (matches target). After hold, reset to long
unless signal still red (re-evaluate at end of hold).

Outputs:
  reports/system3_phase34_portfolio.md
  reports/system3_phase34_equity.csv
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PANEL_PATH = ROOT / "reports" / "system3_panel.parquet"
COMPOSITE_PRED_PATH = ROOT / "reports" / "system3_phase33b_predictions.parquet"
OUT_MD = ROOT / "reports" / "system3_phase34_portfolio.md"
OUT_EQ = ROOT / "reports" / "system3_phase34_equity.csv"

HOLD_DAYS = 21


def simulate_daily_gated(
    close: pd.Series,
    target_alloc: pd.Series,
    label: str,
) -> tuple[dict, pd.DataFrame]:
    """Daily simulation. target_alloc[t-1] -> applied to close[t-1]->close[t] return."""
    df = pd.concat([close.rename("close"), target_alloc.rename("alloc")], axis=1).dropna()
    df = df.sort_index()
    rets = df["close"].pct_change().fillna(0).to_numpy()
    alloc = df["alloc"].to_numpy()
    strat = np.zeros(len(df))
    strat[1:] = alloc[:-1] * rets[1:]
    eq = np.cumprod(1.0 + strat)

    yrs = (df.index[-1] - df.index[0]).days / 365.25
    cagr = eq[-1] ** (1 / yrs) - 1 if yrs > 0 else 0
    sharpe = (np.mean(strat) / np.std(strat) * np.sqrt(252)) if np.std(strat) > 0 else 0
    cummax = np.maximum.accumulate(eq)
    dd = eq / cummax - 1
    mdd = float(dd.min())
    calmar = cagr / abs(mdd) if mdd < 0 else np.nan
    days_full = int((alloc == 1.0).sum())
    days_half = int((alloc == 0.5).sum())
    days_cash = int((alloc == 0.0).sum())
    n_alloc_change = int((np.diff(alloc) != 0).sum())

    metrics = {
        "label": label,
        "cagr": cagr,
        "sharpe": sharpe,
        "mdd": mdd,
        "calmar": calmar,
        "days_full": days_full,
        "days_half": days_half,
        "days_cash": days_cash,
        "n_alloc_change": n_alloc_change,
        "final_equity": float(eq[-1]),
    }
    eq_df = pd.DataFrame({
        "date": df.index,
        f"eq_{label}": eq,
        f"alloc_{label}": alloc,
    })
    return metrics, eq_df


def trigger_to_alloc(
    score: pd.Series,
    threshold_cash: float,
    threshold_half: float,
    score_index: pd.Index,
    hold_days: int = HOLD_DAYS,
) -> pd.Series:
    """Convert a daily score series to allocation series (0/0.5/1.0).

    Trigger allocation = max-state hit during prior `hold_days`.
    Re-trigger renews hold window.
    """
    s = score.reindex(score_index).fillna(method="ffill")
    n = len(s)
    target = np.ones(n, dtype=float)
    sval = s.to_numpy()
    hold_until = -1
    state = 1.0
    for i in range(n):
        if pd.isna(sval[i]):
            target[i] = 1.0
            continue
        # Determine new state from today's score
        if sval[i] >= threshold_cash:
            new_state = 0.0
        elif sval[i] >= threshold_half:
            new_state = 0.5
        else:
            new_state = 1.0
        # If still in active hold, only escalate (lower allocation), never relax early
        if i <= hold_until:
            target[i] = min(state, new_state)  # take more conservative
            if new_state < state:
                state = new_state
                hold_until = i + hold_days
        else:
            state = new_state
            target[i] = state
            if new_state < 1.0:
                hold_until = i + hold_days
    return pd.Series(target, index=score_index)


def main() -> None:
    panel = pd.read_parquet(PANEL_PATH)
    panel.index = pd.to_datetime(panel.index)
    composite = pd.read_parquet(COMPOSITE_PRED_PATH)["composite"]

    # Backtest window: composite OOS range
    start = composite.index[0]
    end = panel.index[-1]
    bt = panel.loc[start:end]
    close = bt["close"]
    print(f"[INFO] Backtest {start.date()} -> {end.date()}, {len(bt)} days")

    # Single ma_dist_60 rolling rank
    md_rank = (-panel["ma_dist_60"]).rolling(252).rank(pct=True).reindex(bt.index)

    # Composite quantile thresholds (use full OOS range for stable quantile)
    comp_q95 = float(composite.quantile(0.95))
    comp_q90 = float(composite.quantile(0.90))
    comp_q85 = float(composite.quantile(0.85))
    comp_q80 = float(composite.quantile(0.80))
    print(f"[INFO] Composite thresholds: q95={comp_q95:.3f}, q90={comp_q90:.3f}, q85={comp_q85:.3f}")

    rows = []
    eqs = []

    # Policy A: Buy & hold
    alloc_A = pd.Series(1.0, index=bt.index)
    m, eq = simulate_daily_gated(close, alloc_A, "A_BuyHold")
    rows.append(m)
    eqs.append(eq.set_index("date"))

    # Policy B: always 50%
    alloc_B = pd.Series(0.5, index=bt.index)
    m, eq = simulate_daily_gated(close, alloc_B, "B_Always50")
    rows.append(m)
    eqs.append(eq.set_index("date"))

    # Policy C1: composite top-5% cash, top-10% half
    alloc_C1 = trigger_to_alloc(composite, threshold_cash=comp_q95, threshold_half=comp_q90, score_index=bt.index)
    m, eq = simulate_daily_gated(close, alloc_C1, "C1_CompositeTop5_10")
    rows.append(m)
    eqs.append(eq.set_index("date"))

    # Policy C2: ma_dist_60 rank>=0.95 cash, 0.85 half
    alloc_C2 = trigger_to_alloc(md_rank, threshold_cash=0.95, threshold_half=0.85, score_index=bt.index)
    m, eq = simulate_daily_gated(close, alloc_C2, "C2_MdDist_Rank")
    rows.append(m)
    eqs.append(eq.set_index("date"))

    # Policy C3: AND combo (most strict)
    md_rank_align = md_rank.reindex(bt.index)
    comp_align = composite.reindex(bt.index).fillna(method="ffill")
    score_C3 = pd.Series(0.0, index=bt.index)
    score_C3.loc[(comp_align >= comp_q90) & (md_rank_align >= 0.85)] = 2.0
    score_C3.loc[((comp_align >= comp_q80) | (md_rank_align >= 0.85)) & (score_C3 < 2.0)] = 1.0
    alloc_C3 = trigger_to_alloc(score_C3, threshold_cash=2.0, threshold_half=1.0, score_index=bt.index)
    m, eq = simulate_daily_gated(close, alloc_C3, "C3_AND_combo")
    rows.append(m)
    eqs.append(eq.set_index("date"))

    # Policy C4: OR combo (sensitive)
    score_C4 = pd.Series(0.0, index=bt.index)
    score_C4.loc[(comp_align >= comp_q95) | (md_rank_align >= 0.95)] = 2.0
    score_C4.loc[((comp_align >= comp_q85) | (md_rank_align >= 0.85)) & (score_C4 < 2.0)] = 1.0
    alloc_C4 = trigger_to_alloc(score_C4, threshold_cash=2.0, threshold_half=1.0, score_index=bt.index)
    m, eq = simulate_daily_gated(close, alloc_C4, "C4_OR_combo")
    rows.append(m)
    eqs.append(eq.set_index("date"))

    metrics = pd.DataFrame(rows)
    print()
    print(metrics[["label", "cagr", "sharpe", "mdd", "calmar", "days_full", "days_half", "days_cash"]].to_string(index=False))

    # Combine equity
    eq_combined = eqs[0]
    for e in eqs[1:]:
        eq_combined = eq_combined.join(e, rsuffix="_dup")
        eq_combined = eq_combined.loc[:, ~eq_combined.columns.str.endswith("_dup")]
    eq_combined.to_csv(OUT_EQ)

    # Verdict
    m_A = next(r for r in rows if r["label"] == "A_BuyHold")
    m_B = next(r for r in rows if r["label"] == "B_Always50")
    best_policy = max(rows[2:], key=lambda r: r["sharpe"])

    L = []
    L.append("# System 3 Phase 3.4 - Portfolio Gating Sim (1w-1mo)")
    L.append("")
    L.append(f"**Backtest**: {bt.index[0].date()} -> {bt.index[-1].date()} ({len(bt)} days)")
    L.append(f"**Hold window per trigger**: {HOLD_DAYS} trading days")
    L.append("")
    L.append("## Policy comparison")
    L.append("")
    L.append("| Policy | CAGR | Sharpe | MDD | Calmar | days_full | days_50% | days_cash |")
    L.append("|---|---|---|---|---|---|---|---|")
    for r in rows:
        L.append(
            f"| {r['label']} | {r['cagr']:.2%} | {r['sharpe']:.3f} | {r['mdd']:.2%} | "
            f"{r['calmar']:.3f} | {r['days_full']} | {r['days_half']} | {r['days_cash']} |"
        )
    L.append("")
    L.append(f"## Best gated policy: **{best_policy['label']}**")
    L.append("")
    L.append(f"- vs Buy & Hold: Sharpe {best_policy['sharpe'] - m_A['sharpe']:+.3f} / MDD {best_policy['mdd'] - m_A['mdd']:+.2%} / CAGR {best_policy['cagr'] - m_A['cagr']:+.2%}")
    L.append(f"- vs Always 50%: Sharpe {best_policy['sharpe'] - m_B['sharpe']:+.3f} / MDD {best_policy['mdd'] - m_B['mdd']:+.2%} / CAGR {best_policy['cagr'] - m_B['cagr']:+.2%}")
    L.append("")
    L.append("## SOP-12 verdict")
    L.append("")
    L.append(f"- best policy Sharpe > B&H Sharpe: **{'PASS' if best_policy['sharpe'] > m_A['sharpe'] else 'FAIL'}**")
    L.append(f"- best policy MDD > B&H MDD (less neg): **{'PASS' if best_policy['mdd'] > m_A['mdd'] else 'FAIL'}**")
    # Composite-vs-single check
    m_C1 = next(r for r in rows if r["label"] == "C1_CompositeTop5_10")
    m_C2 = next(r for r in rows if r["label"] == "C2_MdDist_Rank")
    composite_beat_single = m_C1["sharpe"] > m_C2["sharpe"]
    L.append(f"- composite (C1) Sharpe > single (C2) Sharpe: **{'PASS' if composite_beat_single else 'FAIL'}** ({m_C1['sharpe']:.3f} vs {m_C2['sharpe']:.3f})")
    L.append("")

    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"\n[OK] summary -> {OUT_MD.name}")


if __name__ == "__main__":
    main()
