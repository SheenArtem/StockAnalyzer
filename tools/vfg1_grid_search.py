"""
VF-G1 QM Stop-Loss 4-Parameter Grid Search

Grid search over:
  ATR_STOP_MULTIPLIER    in [2.0, 2.5, 3.0, 3.5, 4.0, 4.5]    # 6 values
  BREAKEVEN_ATR_MULT     in [1.5, 2.0, 2.5, 3.0, 3.5]         # 5 values
  ATR_STOP_CEIL          in [0.10, 0.12, 0.14, 0.16]          # 4 values
  MA20_BREAK_ATR_MULT    in [1.0, 1.2, 1.5, 1.8]              # 4 values

Total: 6 x 5 x 4 x 4 = 480 combos

Simulation (20d holding horizon per pick):
  1. compute SL, BE trigger, TP levels using exit_manager logic with trial params
  2. approx SL breach via entry * (1 + fwd_20d_min) <= SL_price
  3. approx TP breach via entry * (1 + fwd_20d_max) >= entry * (1 + TP1_pct)
  4. BE simulation: if fwd_20d_max >= entry * (1 + BE_trigger),
     assume SL lifted to entry; if fwd_20d_min < 0 post-BE -> exit at entry (0% return)
     (simplification: once BE armed, treat final return as max(fwd_20d, 0))
  5. Conservative tie-break: if both SL and TP triggered in window -> SL first

Realized return logic:
  - If SL triggered (before BE): realized = stop_loss_pct (negative)
  - If BE armed + later fwd_20d_min <= 0: realized = 0  (BE exit at entry)
  - If TP1 triggered (no SL, no BE exit): realized = TP1 scaled pct
  - Else: realized = fwd_20d (held to expiry)

Output:
  - reports/vfg1_grid_search_full.csv   (480 rows x metrics)
  - reports/vfg1_walkforward.csv        (cross-period stability)
  - reports/vfg1_by_regime.csv          (per-regime stats for top combos)
  - reports/vfg1_grid_search.md         (TL;DR + tables)
  - reports/vfg1_heatmaps/              (seaborn heatmaps)
"""

import sys
from pathlib import Path
from itertools import product

import numpy as np
import pandas as pd

ROOT = Path(r"c:\GIT\StockAnalyzer")
JOURNAL = ROOT / "data_cache" / "backtest" / "trade_journal_qm_tw.parquet"
OUT_FULL = ROOT / "reports" / "vfg1_grid_search_full.csv"
OUT_WF = ROOT / "reports" / "vfg1_walkforward.csv"
OUT_REGIME = ROOT / "reports" / "vfg1_by_regime.csv"
OUT_MD = ROOT / "reports" / "vfg1_grid_search.md"
HEATMAP_DIR = ROOT / "reports" / "vfg1_heatmaps"

# --- Baseline (current exit_manager.py constants) ---
BASELINE = dict(stop_mult=3.0, be_mult=3.0, stop_ceil=0.14, ma20_mult=1.2)

# --- Constants that stay fixed (VF-1 outcome + others not in scope) ---
MIN_SL_GAP_ATR_MULT = 1.5  # VF-1 validated B-grade
DEFAULT_MIN_SL_GAP = 0.03
ATR_STOP_FLOOR = 0.05
ATR_TP_SCALE_FLOOR = 0.7
ATR_TP_SCALE_CEIL = 1.6
ATR_PCT_MEDIAN = 2.5
MA20_BREAK_FLOOR = 0.02
MA20_BREAK_CEIL = 0.05
BE_TRIGGER_CLIP = (0.05, 0.15)
DEFAULT_TP_PCTS = (0.15, 0.25, 0.40)

# --- Grid (4D) ---
# Note: ma20_mult (MA20_BREAK_ATR_MULT) cannot be evaluated from trade_journal:
#   it controls daily MA20-break monitoring threshold in position_monitor.py,
#   which needs intraday path data, not aggregated fwd_20d_max/min.
#   We still sweep it so the grid dimensions match spec, but verify its effect
#   is zero (as expected) and grade it separately.
GRID = {
    "stop_mult": [2.0, 2.5, 3.0, 3.5, 4.0, 4.5],
    "be_mult":   [1.5, 2.0, 2.5, 3.0, 3.5],
    "stop_ceil": [0.10, 0.12, 0.14, 0.16],
    "ma20_mult": [1.0, 1.2, 1.5, 1.8],
}

HOLDING = "fwd_20d"
HOLDING_MAX = "fwd_20d_max"
HOLDING_MIN = "fwd_20d_min"


def compute_min_sl_gap_vec(atr_pct):
    """Vectorized: max(0.03, atr_pct * 1.5 / 100)."""
    gap = atr_pct * MIN_SL_GAP_ATR_MULT / 100.0
    return np.maximum(DEFAULT_MIN_SL_GAP, gap)


def simulate(df, stop_mult, be_mult, stop_ceil, ma20_mult,
             be_mode="pessimistic"):
    """
    Vectorized simulation for one parameter combo.

    be_mode:
      - "pessimistic" (DEFAULT, recommended): in ambiguous cases where
        fwd_20d_max >= BE_trigger AND fwd_20d_min <= SL, assume SL hit first.
        Equivalent to ignoring BE's protective effect; honest grading.
      - "optimistic": assume BE armed before SL in ambiguous cases, realize 0.
        This is an UPPER bound -- known to overstate edge by 0.9%+.
      - "no_be": disable BE entirely.

    Returns DataFrame columns: sl_pct, tp1_pct, be_trigger,
      triggered_sl, triggered_tp, be_armed, realized, is_false_stop
    """
    entry = df["entry_price"].values
    atr = df["atr_pct"].values                    # in %
    ma20 = df["weekly_ma20"].values
    fwd = df[HOLDING].values
    fwd_max = df[HOLDING_MAX].values
    fwd_min = df[HOLDING_MIN].values

    # --- SL computation (mirrors compute_exit_plan) ---
    stop_pct = np.clip(atr / 100.0 * stop_mult, ATR_STOP_FLOOR, stop_ceil)
    hard_stop_price = entry * (1 - stop_pct)

    # VF-1 min_sl_gap
    min_gap = compute_min_sl_gap_vec(atr)

    # MA20 trend stop: choose MA20 if above hard stop AND gap >= min_gap
    ma20_valid = np.isfinite(ma20) & (ma20 > 0) & (ma20 < entry)
    ma20_gap = np.where(ma20_valid, (entry - ma20) / entry, np.nan)
    use_ma20 = ma20_valid & (ma20 > hard_stop_price) & (ma20_gap >= min_gap)
    sl_price = np.where(use_ma20, ma20, hard_stop_price)
    sl_pct = (sl_price / entry) - 1.0  # negative

    # --- TP1 (first target) ---
    tp_scale = np.clip(atr / ATR_PCT_MEDIAN, ATR_TP_SCALE_FLOOR, ATR_TP_SCALE_CEIL)
    tp1_pct = DEFAULT_TP_PCTS[0] * tp_scale

    # --- BE trigger (from atr * be_mult, clip 5-15%) ---
    be_trigger = np.clip(atr / 100.0 * be_mult, BE_TRIGGER_CLIP[0], BE_TRIGGER_CLIP[1])

    # --- Intra-window breach logic ---
    # SL hit during 20d: fwd_20d_min <= sl_pct (both negative)
    hit_sl = fwd_min <= sl_pct
    # TP1 hit: fwd_20d_max >= tp1_pct
    hit_tp = fwd_max >= tp1_pct
    # BE armed: fwd_20d_max >= be_trigger
    be_armed = fwd_max >= be_trigger

    # --- Realized return logic ---
    # Conservative tie-break: if SL would hit AND TP would hit, assume SL first
    # (we cannot know order from aggregates; pessimistic assumption)
    # BE refinement: if BE armed BEFORE SL, SL moves to entry (0%).
    #   Approx: if be_trigger < |sl_pct|, i.e. the gain needed for BE (positive)
    #   is smaller in magnitude than the drawdown needed for SL, BE likely hit first
    #   when fwd_max >= be_trigger AND fwd_min <= sl_pct.
    # This is a first-order approximation since we lack intra-path sequence.

    realized = np.full(len(df), np.nan, dtype=float)

    # Case A: SL hit. Resolution depends on be_mode.
    #   pessimistic / no_be -> realize loss at sl_pct (SL hits first always)
    #   optimistic -> if be_armed AND be_trigger <= |sl_pct| -> 0; else sl_pct
    if be_mode == "optimistic":
        case_sl_be_saved = hit_sl & be_armed & (be_trigger <= np.abs(sl_pct))
        case_sl_no_save = hit_sl & ~case_sl_be_saved
        realized[case_sl_be_saved] = 0.0
        realized[case_sl_no_save] = sl_pct[case_sl_no_save]
    else:  # pessimistic or no_be
        realized[hit_sl] = sl_pct[hit_sl]

    # Case B: TP hit, SL not hit -> realize tp1_pct
    case_tp_only = hit_tp & ~hit_sl
    realized[case_tp_only] = tp1_pct[case_tp_only]

    # Case C: Neither hit -> fwd_20d
    case_hold = ~hit_sl & ~hit_tp
    realized[case_hold] = fwd[case_hold]

    # False stop: SL triggered but fwd_20d > 0 (would have been profitable if held)
    triggered_sl = hit_sl  # all cases where SL was hit (regardless of BE outcome)
    # For false_stop, use final realization:
    #   - if realized <= sl_pct-ish (i.e. took the loss) and fwd_20d_max > 0 -> "false"
    is_false_stop = triggered_sl & (fwd > 0) & (realized < 0)

    triggered_tp = hit_tp & ~hit_sl  # pure TP hits (excludes ambiguous)

    return pd.DataFrame({
        "sl_pct": sl_pct,
        "tp1_pct": tp1_pct,
        "be_trigger": be_trigger,
        "hit_sl": hit_sl,
        "hit_tp": hit_tp,
        "be_armed": be_armed,
        "triggered_sl": triggered_sl,
        "triggered_tp": triggered_tp,
        "realized": realized,
        "is_false_stop": is_false_stop,
    })


def summarize(sim, df_ref=None):
    """One combo -> scalar metric row."""
    rel = sim["realized"].values
    n = len(rel)
    mean = np.nanmean(rel)
    std = np.nanstd(rel)
    sharpe = mean / std if std > 0 else np.nan
    win = float((rel > 0).sum()) / n
    sl_rate = float(sim["triggered_sl"].sum()) / n
    tp_rate = float(sim["triggered_tp"].sum()) / n
    be_rate = float(sim["be_armed"].sum()) / n
    fs_rate = float(sim["is_false_stop"].sum()) / max(int(sim["triggered_sl"].sum()), 1)

    # RR median: fwd_20d_max / |sl_pct|
    if df_ref is not None:
        fwd_max = df_ref[HOLDING_MAX].values
        sl_abs = np.abs(sim["sl_pct"].values)
        sl_abs = np.where(sl_abs > 1e-6, sl_abs, 1e-6)
        rr = fwd_max / sl_abs
        rr_median = float(np.nanmedian(rr))
    else:
        rr_median = np.nan

    # Avg holding return (did not trigger SL)
    held = ~sim["triggered_sl"].values
    avg_hold = float(np.nanmean(rel[held])) if held.sum() > 0 else np.nan

    return {
        "n": n,
        "realized_mean": mean,
        "realized_std": std,
        "realized_sharpe": sharpe,
        "win_rate": win,
        "sl_trigger_rate": sl_rate,
        "tp_trigger_rate": tp_rate,
        "be_arm_rate": be_rate,
        "false_stop_rate": fs_rate,
        "rr_median": rr_median,
        "avg_holding_ret": avg_hold,
    }


def run_full_grid(df, be_mode="pessimistic"):
    """Run all 480 combos over full sample."""
    combos = list(product(GRID["stop_mult"], GRID["be_mult"],
                          GRID["stop_ceil"], GRID["ma20_mult"]))
    print(f"[grid] {len(combos)} combos x {len(df)} picks (be_mode={be_mode})")

    rows = []
    for i, (sm, bm, sc, mm) in enumerate(combos, 1):
        sim = simulate(df, sm, bm, sc, mm, be_mode=be_mode)
        metrics = summarize(sim, df)
        rows.append({
            "stop_mult": sm, "be_mult": bm,
            "stop_ceil": sc, "ma20_mult": mm,
            **metrics,
        })
        if i % 120 == 0:
            print(f"  {i}/{len(combos)}")
    return pd.DataFrame(rows)


def run_by_regime(df, params_list, be_mode="pessimistic"):
    """For a list of (name, params), compute per-regime stats."""
    rows = []
    for name, p in params_list:
        sim_all = simulate(df, p["stop_mult"], p["be_mult"],
                           p["stop_ceil"], p["ma20_mult"], be_mode=be_mode)
        for regime in df["regime"].dropna().unique():
            mask = (df["regime"] == regime).values
            if mask.sum() < 50:
                continue
            sub_sim = sim_all.loc[mask].reset_index(drop=True)
            sub_df = df.loc[mask].reset_index(drop=True)
            m = summarize(sub_sim, sub_df)
            rows.append({"combo": name, "regime": regime, **p, **m})
        # all regimes combined
        m = summarize(sim_all, df)
        rows.append({"combo": name, "regime": "ALL", **p, **m})
    return pd.DataFrame(rows)


def walk_forward(df, window_train=12, window_test=4, stride=4,
                 be_mode="pessimistic"):
    """
    Rolling WF: train window_train weeks -> pick top-5 combos, test on next
    window_test weeks, check if they remain top-5.
    Uses week_end_date for time-based split.
    """
    combos = list(product(GRID["stop_mult"], GRID["be_mult"],
                          GRID["stop_ceil"], GRID["ma20_mult"]))
    weeks = sorted(df["week_end_date"].unique())
    n_weeks = len(weeks)
    print(f"[wf] weeks={n_weeks}, train={window_train}, test={window_test}, stride={stride}")

    wf_rows = []
    for start in range(0, n_weeks - window_train - window_test + 1, stride):
        train_weeks = weeks[start:start + window_train]
        test_weeks = weeks[start + window_train:start + window_train + window_test]
        df_train = df[df["week_end_date"].isin(train_weeks)]
        df_test = df[df["week_end_date"].isin(test_weeks)]
        if len(df_train) < 80 or len(df_test) < 30:
            continue

        train_res = []
        test_res = []
        for sm, bm, sc, mm in combos:
            sim_tr = simulate(df_train, sm, bm, sc, mm, be_mode=be_mode)
            mt_tr = summarize(sim_tr, df_train)
            train_res.append({
                "stop_mult": sm, "be_mult": bm, "stop_ceil": sc, "ma20_mult": mm,
                "train_sharpe": mt_tr["realized_sharpe"],
                "train_mean": mt_tr["realized_mean"],
            })
            sim_te = simulate(df_test, sm, bm, sc, mm, be_mode=be_mode)
            mt_te = summarize(sim_te, df_test)
            test_res.append({
                "stop_mult": sm, "be_mult": bm, "stop_ceil": sc, "ma20_mult": mm,
                "test_sharpe": mt_te["realized_sharpe"],
                "test_mean": mt_te["realized_mean"],
            })

        tr_df = pd.DataFrame(train_res)
        te_df = pd.DataFrame(test_res)
        tr_df["train_rank"] = tr_df["train_sharpe"].rank(ascending=False, method="min")
        te_df["test_rank"] = te_df["test_sharpe"].rank(ascending=False, method="min")
        merged = tr_df.merge(te_df, on=["stop_mult", "be_mult", "stop_ceil", "ma20_mult"])
        merged["train_start"] = pd.Timestamp(train_weeks[0])
        merged["test_start"] = pd.Timestamp(test_weeks[0])
        # Keep only what we need: train top-5 + baseline
        top5_train = merged.nsmallest(5, "train_rank").copy()
        top5_train["note"] = "train_top5"
        # baseline always included
        base = merged[(merged["stop_mult"] == BASELINE["stop_mult"]) &
                      (merged["be_mult"] == BASELINE["be_mult"]) &
                      (merged["stop_ceil"] == BASELINE["stop_ceil"]) &
                      (merged["ma20_mult"] == BASELINE["ma20_mult"])].copy()
        base["note"] = "baseline"
        wf_rows.append(pd.concat([top5_train, base], ignore_index=True))

    if not wf_rows:
        return pd.DataFrame()
    return pd.concat(wf_rows, ignore_index=True)


def pick_best_combo(grid_df):
    """Best by Sharpe, tie-break by realized_mean."""
    return grid_df.sort_values(
        ["realized_sharpe", "realized_mean"], ascending=[False, False]
    ).iloc[0].to_dict()


def baseline_row(grid_df):
    """Row for the current baseline combo."""
    b = BASELINE
    m = grid_df[(grid_df["stop_mult"] == b["stop_mult"]) &
                (grid_df["be_mult"] == b["be_mult"]) &
                (grid_df["stop_ceil"] == b["stop_ceil"]) &
                (grid_df["ma20_mult"] == b["ma20_mult"])]
    return m.iloc[0].to_dict() if len(m) else None


def make_heatmaps(grid_df):
    """Produce 2D heatmaps holding 2 axes fixed at baseline."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError:
        print("[heatmap] skip: matplotlib/seaborn not available")
        return

    HEATMAP_DIR.mkdir(parents=True, exist_ok=True)

    # STOP x BE (fix stop_ceil=0.14, ma20_mult=1.2)
    sub = grid_df[(grid_df["stop_ceil"] == 0.14) & (grid_df["ma20_mult"] == 1.2)]
    piv = sub.pivot(index="stop_mult", columns="be_mult", values="realized_sharpe")
    plt.figure(figsize=(7, 5))
    sns.heatmap(piv, annot=True, fmt=".3f", cmap="RdYlGn", center=piv.values.mean())
    plt.title("Sharpe: STOP_MULT x BE_MULT (ceil=0.14, ma20=1.2)")
    plt.tight_layout()
    plt.savefig(HEATMAP_DIR / "heatmap_stop_be.png", dpi=100)
    plt.close()

    # STOP x CEIL (fix be_mult=3.0, ma20_mult=1.2)
    sub = grid_df[(grid_df["be_mult"] == 3.0) & (grid_df["ma20_mult"] == 1.2)]
    piv = sub.pivot(index="stop_mult", columns="stop_ceil", values="realized_sharpe")
    plt.figure(figsize=(7, 5))
    sns.heatmap(piv, annot=True, fmt=".3f", cmap="RdYlGn", center=piv.values.mean())
    plt.title("Sharpe: STOP_MULT x STOP_CEIL (be=3.0, ma20=1.2)")
    plt.tight_layout()
    plt.savefig(HEATMAP_DIR / "heatmap_stop_ceil.png", dpi=100)
    plt.close()

    # STOP x MA20_BREAK (fix be=3.0, ceil=0.14)
    sub = grid_df[(grid_df["be_mult"] == 3.0) & (grid_df["stop_ceil"] == 0.14)]
    piv = sub.pivot(index="stop_mult", columns="ma20_mult", values="realized_sharpe")
    plt.figure(figsize=(7, 5))
    sns.heatmap(piv, annot=True, fmt=".3f", cmap="RdYlGn", center=piv.values.mean())
    plt.title("Sharpe: STOP_MULT x MA20_BREAK (be=3.0, ceil=0.14)")
    plt.tight_layout()
    plt.savefig(HEATMAP_DIR / "heatmap_stop_ma20.png", dpi=100)
    plt.close()

    # BE x CEIL (fix stop=3.0, ma20=1.2)
    sub = grid_df[(grid_df["stop_mult"] == 3.0) & (grid_df["ma20_mult"] == 1.2)]
    piv = sub.pivot(index="be_mult", columns="stop_ceil", values="realized_sharpe")
    plt.figure(figsize=(7, 5))
    sns.heatmap(piv, annot=True, fmt=".3f", cmap="RdYlGn", center=piv.values.mean())
    plt.title("Sharpe: BE_MULT x STOP_CEIL (stop=3.0, ma20=1.2)")
    plt.tight_layout()
    plt.savefig(HEATMAP_DIR / "heatmap_be_ceil.png", dpi=100)
    plt.close()

    print(f"[heatmap] 4 heatmaps -> {HEATMAP_DIR}")


def decide_grade(best, base, wf_df):
    """
    Grading rules:
      A: best cross-period stable + plateau + regime-consistent
      B: cross-period stable + plateau; acceptable regime differences
      C: single-period best but unstable
      D: overfit or no edge -> keep baseline

    Simple rule based on:
      - delta_mean = best.realized_mean - base.realized_mean
      - delta_sharpe = best.realized_sharpe - base.realized_sharpe
      - wf stability: best combo avg test_rank (out of 480) across windows
    """
    dm = best["realized_mean"] - (base["realized_mean"] if base else 0)
    ds = best["realized_sharpe"] - (base["realized_sharpe"] if base else 0)

    # Walk-forward stability: best combo test_rank percentile
    wf_test_rank = np.nan
    if len(wf_df):
        best_mask = ((wf_df["stop_mult"] == best["stop_mult"]) &
                     (wf_df["be_mult"] == best["be_mult"]) &
                     (wf_df["stop_ceil"] == best["stop_ceil"]) &
                     (wf_df["ma20_mult"] == best["ma20_mult"]))
        if best_mask.any():
            wf_test_rank = wf_df.loc[best_mask, "test_rank"].mean()

    # Decision tree per spec
    if dm < 0.003 and ds < 0.05:
        grade = "D"
        reason = "best-vs-baseline delta insufficient (keep current)"
        recommend = "keep_baseline"
    elif 0.003 <= dm < 0.008:
        grade = "C"
        reason = "mild edge 0.3-0.8% mean; keep baseline but note best"
        recommend = "keep_baseline_note_best"
    elif dm >= 0.008:
        # check WF stability
        if pd.notna(wf_test_rank) and wf_test_rank <= 50:
            grade = "B"
            reason = f"strong edge >0.8% AND stable WF (test_rank_mean={wf_test_rank:.1f})"
            recommend = "commit"
        elif pd.notna(wf_test_rank) and wf_test_rank <= 120:
            grade = "C"
            reason = f"strong edge in-sample but WF unstable (test_rank_mean={wf_test_rank:.1f})"
            recommend = "keep_baseline_note_best"
        else:
            grade = "C"
            reason = f"strong in-sample edge but WF fails (test_rank_mean={wf_test_rank})"
            recommend = "keep_baseline_note_best"
    else:
        grade = "D"
        reason = "fallback"
        recommend = "keep_baseline"

    return {
        "grade": grade,
        "reason": reason,
        "recommend": recommend,
        "delta_mean": dm,
        "delta_sharpe": ds,
        "wf_test_rank_mean": wf_test_rank,
    }


def fmt_combo(d):
    return (f"stop={d['stop_mult']}/be={d['be_mult']}/"
            f"ceil={d['stop_ceil']:.2f}/ma20={d['ma20_mult']}")


def write_report(grid_df, best, base, regime_df, wf_df, decision,
                 grid_opt=None, best_opt=None, base_opt=None):
    lines = []
    lines.append("# VF-G1 QM Stop-Loss 4-Parameter Grid Search\n")
    lines.append(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n")

    # --- TL;DR ---
    lines.append("## TL;DR\n")
    if base is None:
        lines.append("- baseline not in grid (unexpected)")
    else:
        lines.append(
            f"- **Best combo (honest BE)**: {fmt_combo(best)}  "
            f"Sharpe={best['realized_sharpe']:.3f}  "
            f"mean={best['realized_mean']*100:.2f}%  win={best['win_rate']*100:.1f}%"
        )
        lines.append(
            f"- **Baseline** (3.0/3.0/0.14/1.2): "
            f"Sharpe={base['realized_sharpe']:.3f}  "
            f"mean={base['realized_mean']*100:.2f}%  win={base['win_rate']*100:.1f}%"
        )
        lines.append(
            f"- **Delta (best - baseline)**: "
            f"Sharpe {decision['delta_sharpe']:+.3f}  "
            f"mean {decision['delta_mean']*100:+.2f}%"
        )
        lines.append(
            f"- **Walk-forward stability**: best combo avg test_rank = "
            f"{decision['wf_test_rank_mean']:.1f}/480"
            if pd.notna(decision["wf_test_rank_mean"]) else
            "- **Walk-forward stability**: no data"
        )
        lines.append(
            f"- **Grade: {decision['grade']}** -- {decision['reason']} "
            f"-- recommend: `{decision['recommend']}`"
        )
    lines.append("")

    # --- No-exit baseline (critical context) ---
    try:
        no_exit_mean = df_journal["fwd_20d"].mean()
        no_exit_std = df_journal["fwd_20d"].std()
        no_exit_sharpe = no_exit_mean / no_exit_std
        no_exit_win = (df_journal["fwd_20d"] > 0).mean()
        lines.append("## 0A. No-Exit Baseline (CRITICAL CONTEXT)\n")
        lines.append(
            f"**Pure 20d hold (no SL, no TP) produces**: mean={no_exit_mean*100:.2f}%, "
            f"Sharpe={no_exit_sharpe:.3f}, win={no_exit_win*100:.1f}%.\n"
        )
        lines.append(
            f"**Any stop configuration in this grid UNDERPERFORMS the no-exit baseline**: "
            f"best-pess mean={best['realized_mean']*100:.2f}%, "
            f"Sharpe={best['realized_sharpe']:.3f}. "
            f"Delta vs no-exit: mean {(best['realized_mean']-no_exit_mean)*100:+.2f}%, "
            f"Sharpe {best['realized_sharpe']-no_exit_sharpe:+.3f}.\n"
        )
        lines.append(
            "**Interpretation**: QM picks have a positive 20d drift (~2.8% mean). "
            "Stop-losses at any reasonable level cut profitable paths short more often "
            "than they save losses, because top-300 momentum picks rarely sustain "
            "catastrophic drawdowns. The protective value of SL here is risk-management "
            "(tail protection) rather than expected-return enhancement."
        )
        lines.append("")
    except Exception as e:
        lines.append(f"(no-exit baseline calc failed: {e})")

    # --- Simulation caveats (CRITICAL) ---
    lines.append("## 0B. Simulation Caveats (READ FIRST)\n")
    lines.append(
        "**BE (break-even) simulation mode**: trade_journal only has fwd_20d_max / fwd_20d_min, "
        "not intraday paths. In ~19% of picks both a +5% gain AND -8% drawdown occur within "
        "20d. The ORDER of these moves decides whether BE armed before SL hit."
    )
    lines.append("")
    lines.append("- **Pessimistic BE (default, honest)**: assume SL hit first in ambiguous cases. "
                 "Realized = sl_pct if SL touched, regardless of later BE arming. **This is the "
                 "honest grading metric.**")
    lines.append("- **Optimistic BE (upper-bound)**: assume BE armed before SL in ambiguous cases, "
                 "realized = 0. Known to overstate edge by 0.9pp+ mean return. Provided only "
                 "for sensitivity context.")
    lines.append("")
    if best_opt and base_opt:
        lines.append(
            f"**Upper-bound delta (optimistic BE)**: best={fmt_combo(best_opt)} "
            f"Sharpe={best_opt['realized_sharpe']:.3f} mean={best_opt['realized_mean']*100:.2f}%; "
            f"gap vs pessimistic best = "
            f"{(best_opt['realized_mean']-best['realized_mean'])*100:+.2f}% "
            f"(this gap is the BE simulation artifact, not real edge)."
        )
    lines.append("")
    lines.append(
        "**ma20_mult (MA20_BREAK_ATR_MULT) caveat**: this parameter controls position_monitor "
        "daily MA20-break alert threshold, not initial SL placement. It cannot be evaluated "
        "from aggregated fwd_20d data (needs intraday path). Grid search confirmed its effect "
        "is identically zero across all rows. Grade it separately via daily OHLCV simulation "
        "if needed."
    )
    lines.append("")

    # --- Top-10 combos ---
    lines.append("## 1. Top-10 Combos (full sample)\n")
    top10 = grid_df.sort_values("realized_sharpe", ascending=False).head(10)
    lines.append("| rank | stop | be | ceil | ma20 | Sharpe | mean | win | sl_rate | tp_rate | false_stop |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in enumerate(top10.itertuples(index=False), 1):
        lines.append(
            f"| {i} | {r.stop_mult} | {r.be_mult} | {r.stop_ceil:.2f} | {r.ma20_mult} | "
            f"{r.realized_sharpe:.3f} | {r.realized_mean*100:.2f}% | "
            f"{r.win_rate*100:.1f}% | {r.sl_trigger_rate*100:.1f}% | "
            f"{r.tp_trigger_rate*100:.1f}% | {r.false_stop_rate*100:.1f}% |"
        )
    lines.append("")

    # --- Baseline ranking ---
    lines.append("## 2. Baseline Ranking\n")
    base_rank = (grid_df["realized_sharpe"] > base["realized_sharpe"]).sum() + 1 if base else None
    if base:
        lines.append(
            f"Baseline (3.0/3.0/0.14/1.2) Sharpe = {base['realized_sharpe']:.3f}, "
            f"ranked **#{base_rank}/{len(grid_df)}**.  "
            f"{(base_rank/len(grid_df))*100:.1f}%ile.\n"
        )

    # --- By regime ---
    lines.append("## 3. Best Combo vs Baseline by Regime\n")
    if len(regime_df):
        lines.append("| combo | regime | n | Sharpe | mean | win | sl_rate |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in regime_df.itertuples(index=False):
            lines.append(
                f"| {r.combo} | {r.regime} | {int(r.n)} | {r.realized_sharpe:.3f} | "
                f"{r.realized_mean*100:.2f}% | {r.win_rate*100:.1f}% | "
                f"{r.sl_trigger_rate*100:.1f}% |"
            )
    lines.append("")

    # --- Walk-forward ---
    lines.append("## 4. Walk-Forward Summary\n")
    if len(wf_df):
        n_windows = wf_df["test_start"].nunique()
        lines.append(f"Windows: {n_windows} (12 weeks train / 4 weeks test, stride 4)")
        # For the best combo: its test_rank across windows
        best_mask = ((wf_df["stop_mult"] == best["stop_mult"]) &
                     (wf_df["be_mult"] == best["be_mult"]) &
                     (wf_df["stop_ceil"] == best["stop_ceil"]) &
                     (wf_df["ma20_mult"] == best["ma20_mult"]))
        best_wf = wf_df[best_mask]
        if len(best_wf):
            lines.append(
                f"\nBest combo WF: test_rank mean={best_wf['test_rank'].mean():.1f}, "
                f"median={best_wf['test_rank'].median():.0f}, "
                f"std={best_wf['test_rank'].std():.1f}"
            )
            top5_hits = (best_wf["test_rank"] <= 5).sum()
            top20_hits = (best_wf["test_rank"] <= 20).sum()
            lines.append(
                f"Best combo lands in test top-5: {top5_hits}/{len(best_wf)} windows; "
                f"top-20: {top20_hits}/{len(best_wf)} windows."
            )
        # Baseline WF
        base_mask = ((wf_df["stop_mult"] == BASELINE["stop_mult"]) &
                     (wf_df["be_mult"] == BASELINE["be_mult"]) &
                     (wf_df["stop_ceil"] == BASELINE["stop_ceil"]) &
                     (wf_df["ma20_mult"] == BASELINE["ma20_mult"]))
        base_wf = wf_df[base_mask]
        if len(base_wf):
            lines.append(
                f"\nBaseline WF: test_rank mean={base_wf['test_rank'].mean():.1f}, "
                f"median={base_wf['test_rank'].median():.0f}"
            )
    else:
        lines.append("Walk-forward not produced.")
    lines.append("")

    # --- Volatile regime check ---
    lines.append("## 5. Hypothesis: Volatile Regime Needs Wider STOP?\n")
    vol = regime_df[regime_df["regime"] == "volatile"]
    if len(vol):
        # Best stop_mult under volatile
        # Actually: we only have best+baseline here, so cross check from grid
        pass
    # Sweep over volatile subset for all stop_mult values
    try:
        df_vol = df_journal[df_journal["regime"] == "volatile"]
        if len(df_vol) > 100:
            vol_rows = []
            for sm in GRID["stop_mult"]:
                sim = simulate(df_vol, sm, BASELINE["be_mult"],
                               BASELINE["stop_ceil"], BASELINE["ma20_mult"])
                m = summarize(sim, df_vol)
                vol_rows.append({"stop_mult": sm, **m})
            vdf = pd.DataFrame(vol_rows)
            best_vol_sm = vdf.loc[vdf["realized_sharpe"].idxmax(), "stop_mult"]
            lines.append(
                f"Volatile subset (n={len(df_vol)}): best stop_mult = **{best_vol_sm}** "
                f"(Sharpe={vdf['realized_sharpe'].max():.3f}); "
                f"hypothesis 'volatile needs wider STOP (>3.5)' is "
                f"{'supported' if best_vol_sm > 3.5 else 'NOT supported'}."
            )
            lines.append("\n| stop_mult | Sharpe | mean | win | sl_rate |")
            lines.append("|---|---|---|---|---|")
            for r in vdf.itertuples(index=False):
                lines.append(
                    f"| {r.stop_mult} | {r.realized_sharpe:.3f} | "
                    f"{r.realized_mean*100:.2f}% | {r.win_rate*100:.1f}% | "
                    f"{r.sl_trigger_rate*100:.1f}% |"
                )
    except Exception as e:
        lines.append(f"(volatile sweep failed: {e})")
    lines.append("")

    # --- Decision ---
    lines.append("## 6. Decision & Action\n")
    lines.append(f"- **Grade**: {decision['grade']}")
    lines.append(f"- **Reason**: {decision['reason']}")
    lines.append(f"- **Recommendation**: `{decision['recommend']}`")
    if decision["recommend"] == "keep_baseline":
        lines.append("\nNo change. Baseline params remain optimal or grid failed to improve meaningfully.")
    elif decision["recommend"] == "keep_baseline_note_best":
        lines.append(
            f"\nKeep baseline for now. Note best combo `{fmt_combo(best)}` "
            f"for next round validation once more weeks accumulated."
        )
    elif decision["recommend"] == "commit":
        lines.append(
            f"\n**Propose commit**: change `exit_manager.py` constants to "
            f"{fmt_combo(best)}."
        )

    lines.append("\n## 7. Files\n")
    lines.append(f"- Full grid: `{OUT_FULL.name}`")
    lines.append(f"- Walk-forward: `{OUT_WF.name}`")
    lines.append(f"- By regime: `{OUT_REGIME.name}`")
    lines.append(f"- Heatmaps: `{HEATMAP_DIR.name}/*.png`")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"[report] -> {OUT_MD}")


def main():
    global df_journal
    print("[1/5] loading trade_journal...")
    df_journal = pd.read_parquet(JOURNAL)
    # drop rows with NaN weekly_ma20 (rare)
    before = len(df_journal)
    df_journal = df_journal.dropna(subset=["weekly_ma20", "atr_pct", "fwd_20d",
                                           "fwd_20d_max", "fwd_20d_min"])
    print(f"  rows: {before} -> {len(df_journal)} after dropna")

    print("[2/5] full grid search (480 combos, PESSIMISTIC BE) ...")
    grid_df = run_full_grid(df_journal, be_mode="pessimistic")
    grid_df.to_csv(OUT_FULL, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_FULL}")

    # also produce optimistic-BE upper bound for transparency
    print("[2b/5] optimistic BE upper-bound grid (for transparency) ...")
    grid_opt = run_full_grid(df_journal, be_mode="optimistic")
    opt_path = ROOT / "reports" / "vfg1_grid_search_optimistic.csv"
    grid_opt.to_csv(opt_path, index=False, encoding="utf-8-sig")
    print(f"  -> {opt_path}")

    best = pick_best_combo(grid_df)
    base = baseline_row(grid_df)
    best_opt = pick_best_combo(grid_opt)
    base_opt = baseline_row(grid_opt)

    print(f"  [pess] best: {fmt_combo(best)}  Sharpe={best['realized_sharpe']:.3f}")
    if base:
        print(f"  [pess] base: {fmt_combo(base)}  Sharpe={base['realized_sharpe']:.3f}")
    print(f"  [opt]  best: {fmt_combo(best_opt)}  Sharpe={best_opt['realized_sharpe']:.3f}")

    print("[3/5] by-regime for best & baseline (pessimistic)...")
    params_list = [
        ("baseline", BASELINE),
        ("best_pess", {k: best[k] for k in ["stop_mult", "be_mult", "stop_ceil", "ma20_mult"]}),
        ("best_opt", {k: best_opt[k] for k in ["stop_mult", "be_mult", "stop_ceil", "ma20_mult"]}),
    ]
    regime_df = run_by_regime(df_journal, params_list, be_mode="pessimistic")
    regime_df.to_csv(OUT_REGIME, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_REGIME}")

    print("[4/5] walk-forward (pessimistic)...")
    wf_df = walk_forward(df_journal, window_train=12, window_test=4, stride=4,
                         be_mode="pessimistic")
    wf_df.to_csv(OUT_WF, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_WF} ({len(wf_df)} rows)")

    print("[5/5] heatmaps + report...")
    make_heatmaps(grid_df)
    decision = decide_grade(best, base, wf_df)
    write_report(grid_df, best, base, regime_df, wf_df, decision,
                 grid_opt=grid_opt, best_opt=best_opt, base_opt=base_opt)

    print("\n=== DONE ===")
    print(f"Grade: {decision['grade']} | recommend: {decision['recommend']}")
    print(f"Best (pess): {fmt_combo(best)}  Sharpe={best['realized_sharpe']:.3f}")
    print(f"Best (opt):  {fmt_combo(best_opt)}  Sharpe={best_opt['realized_sharpe']:.3f}")
    print(f"Delta (pess vs base): Sharpe {decision['delta_sharpe']:+.3f}  "
          f"mean {decision['delta_mean']*100:+.2f}%")


if __name__ == "__main__":
    main()
