"""
VF-G3 Part 1: REGIME_EXIT_MULT validation

Validate 8 regime-specific multipliers in exit_manager.py:
  REGIME_EXIT_MULT = {
      'trending': (0.85, 1.20),  # SL tighten, TP loose
      'ranging':  (1.00, 1.00),
      'volatile': (1.20, 0.80),  # SL loose, TP tighten
      'neutral':  (1.00, 1.00),
  }

Four versions compared:
  V1 current:      REGIME_EXIT_MULT (8 values)
  V2 all 1.0:      single (1.0, 1.0) -- cut regime overlay
  V3 trending-only:trending=(0.85,1.20), others=(1,1)
  V4 volatile-only:volatile=(1.20,0.80), others=(1,1)

Then Test B: per-regime grid over SL/TP multipliers (5x4=20 per regime).
Then Test C: Walk-forward stability (12w train / 4w test, stride 4).

Baseline stop params (from VF-G1, unchanged):
  stop_mult=3.0, be_mult=3.0, stop_ceil=0.14, ma20_mult=1.2
  min_sl_gap = max(3%, atr% * 1.5 / 100)  # VF-1 B-grade

Decision rule (per spec):
  - V1 vs V2 delta_mean < 0.3% AND delta_sharpe < 0.05 -> cut REGIME_EXIT_MULT
  - V1 > V2 by >0.5% mean AND WF stable                -> keep/optimize
  - V1 > V2 but WF unstable                            -> keep as D (not recommended)

BE mode: pessimistic (SL-first in ambiguous cases) -- consistent with VF-G1.
"""

import sys
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"c:\GIT\StockAnalyzer")
JOURNAL = ROOT / "data_cache" / "backtest" / "trade_journal_qm_tw.parquet"
OUT_MD = ROOT / "reports" / "vfg3_part1_regime_exit_mult.md"
OUT_REGIME = ROOT / "reports" / "vfg3_part1_by_regime.csv"
OUT_WF = ROOT / "reports" / "vfg3_part1_walkforward.csv"
OUT_VERSIONS = ROOT / "reports" / "vfg3_part1_versions.csv"

# --- Fixed (VF-G1 baseline) ---
STOP_MULT = 3.0
BE_MULT = 3.0
STOP_CEIL = 0.14
MIN_SL_GAP_ATR_MULT = 1.5
DEFAULT_MIN_SL_GAP = 0.03
ATR_STOP_FLOOR = 0.05
ATR_TP_SCALE_FLOOR = 0.7
ATR_TP_SCALE_CEIL = 1.6
ATR_PCT_MEDIAN = 2.5
BE_TRIGGER_CLIP = (0.05, 0.15)
DEFAULT_TP_PCTS = (0.15, 0.25, 0.40)

# --- REGIME_EXIT_MULT versions ---
V1_CURRENT = {
    'trending': (0.85, 1.20),
    'ranging':  (1.00, 1.00),
    'volatile': (1.20, 0.80),
    'neutral':  (1.00, 1.00),
}
V2_ALL_ONE = {
    'trending': (1.00, 1.00),
    'ranging':  (1.00, 1.00),
    'volatile': (1.00, 1.00),
    'neutral':  (1.00, 1.00),
}
V3_TRENDING_ONLY = {
    'trending': (0.85, 1.20),
    'ranging':  (1.00, 1.00),
    'volatile': (1.00, 1.00),
    'neutral':  (1.00, 1.00),
}
V4_VOLATILE_ONLY = {
    'trending': (1.00, 1.00),
    'ranging':  (1.00, 1.00),
    'volatile': (1.20, 0.80),
    'neutral':  (1.00, 1.00),
}

VERSIONS = {
    "V1_current":       V1_CURRENT,
    "V2_all_one":       V2_ALL_ONE,
    "V3_trending_only": V3_TRENDING_ONLY,
    "V4_volatile_only": V4_VOLATILE_ONLY,
}

# --- Per-regime grid for Test B ---
SL_MULT_GRID = [0.7, 0.85, 1.0, 1.15, 1.3]
TP_MULT_GRID = [0.8, 1.0, 1.2, 1.4]
REGIMES = ["trending", "ranging", "volatile", "neutral"]


def compute_min_sl_gap_vec(atr_pct):
    gap = atr_pct * MIN_SL_GAP_ATR_MULT / 100.0
    return np.maximum(DEFAULT_MIN_SL_GAP, gap)


def simulate_with_mult_map(df, mult_map):
    """
    Simulate exit with per-regime (sl_mult, tp_mult) from mult_map.

    mult_map: dict regime -> (sl_mult, tp_mult). Unknown regime -> (1,1).

    Logic mirrors compute_exit_plan + VF-G1 simulate:
      1. base stop_pct = clip(atr/100 * 3.0, 0.05, 0.14)
      2. stop_pct_adj = clip(stop_pct * sl_mult, 0.05, 0.14)
      3. MA20 trend stop overrides if valid and gap >= min_sl_gap
      4. base tp_scale = clip(atr / 2.5, 0.7, 1.6)
      5. tp_scale_adj = clip(tp_scale * tp_mult, 0.7, 1.6)
      6. BE trigger = clip(atr/100 * 3.0, 0.05, 0.15)   (unaffected by regime)
      7. Pessimistic BE: SL-first in ambiguous cases
    """
    entry = df["entry_price"].values
    atr = df["atr_pct"].values
    ma20 = df["weekly_ma20"].values
    fwd = df["fwd_20d"].values
    fwd_max = df["fwd_20d_max"].values
    fwd_min = df["fwd_20d_min"].values
    regime = df["regime"].fillna("neutral").values

    sl_mult_arr = np.ones(len(df))
    tp_mult_arr = np.ones(len(df))
    for r, (sl, tp) in mult_map.items():
        mask = regime == r
        sl_mult_arr[mask] = sl
        tp_mult_arr[mask] = tp

    # Base stop_pct
    base_stop = np.clip(atr / 100.0 * STOP_MULT, ATR_STOP_FLOOR, STOP_CEIL)
    stop_pct = np.clip(base_stop * sl_mult_arr, ATR_STOP_FLOOR, STOP_CEIL)
    hard_stop_price = entry * (1 - stop_pct)

    # MA20 override
    min_gap = compute_min_sl_gap_vec(atr)
    ma20_valid = np.isfinite(ma20) & (ma20 > 0) & (ma20 < entry)
    ma20_gap = np.where(ma20_valid, (entry - ma20) / entry, np.nan)
    use_ma20 = ma20_valid & (ma20 > hard_stop_price) & (ma20_gap >= min_gap)
    sl_price = np.where(use_ma20, ma20, hard_stop_price)
    sl_pct = (sl_price / entry) - 1.0

    # TP1
    base_tp_scale = np.clip(atr / ATR_PCT_MEDIAN, ATR_TP_SCALE_FLOOR, ATR_TP_SCALE_CEIL)
    tp_scale = np.clip(base_tp_scale * tp_mult_arr, ATR_TP_SCALE_FLOOR, ATR_TP_SCALE_CEIL)
    tp1_pct = DEFAULT_TP_PCTS[0] * tp_scale

    # BE trigger (not regime-adjusted)
    be_trigger = np.clip(atr / 100.0 * BE_MULT, BE_TRIGGER_CLIP[0], BE_TRIGGER_CLIP[1])

    hit_sl = fwd_min <= sl_pct
    hit_tp = fwd_max >= tp1_pct
    be_armed = fwd_max >= be_trigger

    realized = np.full(len(df), np.nan, dtype=float)
    # Pessimistic: SL first in ambiguous cases
    realized[hit_sl] = sl_pct[hit_sl]
    case_tp_only = hit_tp & ~hit_sl
    realized[case_tp_only] = tp1_pct[case_tp_only]
    case_hold = ~hit_sl & ~hit_tp
    realized[case_hold] = fwd[case_hold]

    triggered_sl = hit_sl
    triggered_tp = hit_tp & ~hit_sl
    is_false_stop = triggered_sl & (fwd > 0) & (realized < 0)

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
    rel = sim["realized"].values
    n = len(rel)
    mean = float(np.nanmean(rel))
    std = float(np.nanstd(rel))
    sharpe = mean / std if std > 0 else np.nan
    win = float((rel > 0).sum()) / n
    sl_rate = float(sim["triggered_sl"].sum()) / n
    tp_rate = float(sim["triggered_tp"].sum()) / n
    be_rate = float(sim["be_armed"].sum()) / n
    fs_rate = float(sim["is_false_stop"].sum()) / max(int(sim["triggered_sl"].sum()), 1)

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
    }


def run_test_a(df):
    """V1/V2/V3/V4 overall + per-regime."""
    rows = []
    for name, mult_map in VERSIONS.items():
        sim = simulate_with_mult_map(df, mult_map)

        # Overall
        m = summarize(sim, df)
        rows.append({"version": name, "regime": "ALL", **m})

        # Per-regime
        for r in REGIMES:
            mask = (df["regime"] == r).values
            if mask.sum() < 50:
                continue
            sub = sim.loc[mask].reset_index(drop=True)
            sub_df = df.loc[mask].reset_index(drop=True)
            mr = summarize(sub, sub_df)
            rows.append({"version": name, "regime": r, **mr})

    return pd.DataFrame(rows)


def run_test_b(df):
    """
    Per-regime grid: for each regime, sweep SL x TP mult, other regimes kept at (1,1).
    Output: best per regime + baseline (1,1) + current V1 value per regime.
    """
    rows = []
    for r in REGIMES:
        rmask = (df["regime"] == r).values
        if rmask.sum() < 100:
            continue
        for sl_m, tp_m in product(SL_MULT_GRID, TP_MULT_GRID):
            # Only adjust target regime; others (1,1)
            mmap = {rr: ((sl_m, tp_m) if rr == r else (1.0, 1.0)) for rr in REGIMES}
            sim = simulate_with_mult_map(df, mmap)
            sub = sim.loc[rmask].reset_index(drop=True)
            sub_df = df.loc[rmask].reset_index(drop=True)
            m = summarize(sub, sub_df)
            rows.append({
                "regime": r,
                "sl_mult": sl_m,
                "tp_mult": tp_m,
                **m,
            })
    return pd.DataFrame(rows)


def run_walk_forward(df, window_train=12, window_test=4, stride=4):
    """
    WF: rolling train/test windows. For each window:
      - compute V1 (current) mean/sharpe on train and test
      - compute V2 (all 1.0) mean/sharpe on train and test
      - compute best per-regime V1 rank on train vs test
    """
    weeks = sorted(df["week_end_date"].unique())
    n_weeks = len(weeks)
    print(f"[wf] weeks={n_weeks}, train={window_train}, test={window_test}, stride={stride}")

    rows = []
    for start in range(0, n_weeks - window_train - window_test + 1, stride):
        train_weeks = weeks[start:start + window_train]
        test_weeks = weeks[start + window_train:start + window_train + window_test]
        df_train = df[df["week_end_date"].isin(train_weeks)]
        df_test = df[df["week_end_date"].isin(test_weeks)]
        if len(df_train) < 200 or len(df_test) < 100:
            continue

        train_start = pd.Timestamp(train_weeks[0])
        test_start = pd.Timestamp(test_weeks[0])

        for name, mmap in VERSIONS.items():
            sim_tr = simulate_with_mult_map(df_train, mmap)
            mtr = summarize(sim_tr, df_train)
            sim_te = simulate_with_mult_map(df_test, mmap)
            mte = summarize(sim_te, df_test)
            rows.append({
                "version": name,
                "train_start": train_start,
                "test_start": test_start,
                "train_n": len(df_train),
                "test_n": len(df_test),
                "train_mean": mtr["realized_mean"],
                "train_sharpe": mtr["realized_sharpe"],
                "test_mean": mte["realized_mean"],
                "test_sharpe": mte["realized_sharpe"],
            })
    return pd.DataFrame(rows)


def decide_grade(versions_df, wf_df):
    """
    V1 vs V2 on overall:
      delta_mean < 0.3% AND delta_sharpe < 0.05 -> CUT (grade D)
      delta_mean >= 0.5% AND WF stable          -> keep/optimize (B/C)
      else                                       -> keep as D
    """
    v1 = versions_df[(versions_df["version"] == "V1_current") &
                     (versions_df["regime"] == "ALL")].iloc[0]
    v2 = versions_df[(versions_df["version"] == "V2_all_one") &
                     (versions_df["regime"] == "ALL")].iloc[0]

    dm = v1["realized_mean"] - v2["realized_mean"]
    ds = v1["realized_sharpe"] - v2["realized_sharpe"]

    # WF: count of windows where V1 beats V2 on test
    wf_stable = False
    v1_wins_count = 0
    total_windows = 0
    if len(wf_df):
        wf_pivot = wf_df.pivot_table(
            index="test_start", columns="version",
            values=["test_mean", "test_sharpe"], aggfunc="first"
        )
        if ("test_mean", "V1_current") in wf_pivot.columns and \
           ("test_mean", "V2_all_one") in wf_pivot.columns:
            v1_wins = wf_pivot[("test_mean", "V1_current")] > wf_pivot[("test_mean", "V2_all_one")]
            v1_wins_count = int(v1_wins.sum())
            total_windows = int(len(v1_wins))
            wf_stable = v1_wins_count >= total_windows * 0.6  # V1 wins >=60% windows

    if abs(dm) < 0.003 and abs(ds) < 0.05:
        grade = "D"
        reason = (f"V1 vs V2 delta negligible (mean {dm*100:+.2f}%, Sharpe {ds:+.3f}). "
                  f"No regime-specific edge detected.")
        recommend = "cut_regime_exit_mult"
    elif dm >= 0.005 and wf_stable:
        grade = "B"
        reason = (f"V1 meaningful edge over V2 (mean {dm*100:+.2f}%, Sharpe {ds:+.3f}) "
                  f"AND WF stable (V1 wins {v1_wins_count}/{total_windows} windows).")
        recommend = "keep_or_optimize"
    elif dm >= 0.005:
        grade = "D"
        reason = (f"V1 in-sample edge ({dm*100:+.2f}%) but WF unstable "
                  f"(V1 wins {v1_wins_count}/{total_windows} windows). Likely overfit.")
        recommend = "cut_regime_exit_mult"
    else:
        grade = "D"
        reason = (f"V1 vs V2 small/negative delta (mean {dm*100:+.2f}%). "
                  f"REGIME_EXIT_MULT not justified.")
        recommend = "cut_regime_exit_mult"

    return {
        "grade": grade,
        "reason": reason,
        "recommend": recommend,
        "delta_mean": dm,
        "delta_sharpe": ds,
        "wf_v1_wins": v1_wins_count,
        "wf_total_windows": total_windows,
        "wf_stable": wf_stable,
    }


def write_report(df, versions_df, regime_grid_df, wf_df, decision):
    lines = []
    lines.append("# VF-G3 Part 1: REGIME_EXIT_MULT 8-Multiplier Validation\n")
    lines.append(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n")

    # --- TL;DR ---
    lines.append("## TL;DR\n")
    v1 = versions_df[(versions_df["version"] == "V1_current") &
                     (versions_df["regime"] == "ALL")].iloc[0]
    v2 = versions_df[(versions_df["version"] == "V2_all_one") &
                     (versions_df["regime"] == "ALL")].iloc[0]
    v3 = versions_df[(versions_df["version"] == "V3_trending_only") &
                     (versions_df["regime"] == "ALL")].iloc[0]
    v4 = versions_df[(versions_df["version"] == "V4_volatile_only") &
                     (versions_df["regime"] == "ALL")].iloc[0]

    lines.append(
        f"- **V1 (current)**:  mean={v1['realized_mean']*100:.2f}% "
        f"Sharpe={v1['realized_sharpe']:.3f}  win={v1['win_rate']*100:.1f}%  "
        f"sl_rate={v1['sl_trigger_rate']*100:.1f}%"
    )
    lines.append(
        f"- **V2 (all 1.0)**:  mean={v2['realized_mean']*100:.2f}% "
        f"Sharpe={v2['realized_sharpe']:.3f}  win={v2['win_rate']*100:.1f}%  "
        f"sl_rate={v2['sl_trigger_rate']*100:.1f}%"
    )
    lines.append(
        f"- **V3 (trending only)**: mean={v3['realized_mean']*100:.2f}% "
        f"Sharpe={v3['realized_sharpe']:.3f}"
    )
    lines.append(
        f"- **V4 (volatile only)**: mean={v4['realized_mean']*100:.2f}% "
        f"Sharpe={v4['realized_sharpe']:.3f}"
    )
    lines.append(
        f"- **V1 vs V2 delta**: mean {decision['delta_mean']*100:+.3f}%  "
        f"Sharpe {decision['delta_sharpe']:+.4f}"
    )
    if decision["wf_total_windows"]:
        lines.append(
            f"- **Walk-forward**: V1 beats V2 on test in "
            f"{decision['wf_v1_wins']}/{decision['wf_total_windows']} windows "
            f"({decision['wf_v1_wins']/decision['wf_total_windows']*100:.0f}%)"
        )
    lines.append(f"- **Grade: {decision['grade']}** -- {decision['reason']}")
    lines.append(f"- **Recommendation**: `{decision['recommend']}`")
    lines.append("")

    # --- Context: no-exit baseline ---
    try:
        no_exit_mean = df["fwd_20d"].mean()
        no_exit_sharpe = no_exit_mean / df["fwd_20d"].std()
        lines.append("## 0. Context (no-exit baseline)\n")
        lines.append(
            f"Pure 20d hold: mean={no_exit_mean*100:.2f}%, Sharpe={no_exit_sharpe:.3f}. "
            f"Any stop config (V1/V2/V3/V4) underperforms no-exit (consistent with VF-G1 finding: "
            f"stop-loss is risk control, not alpha)."
        )
        lines.append("")
    except Exception as e:
        lines.append(f"(no-exit baseline calc failed: {e})\n")

    # --- 1. Test A: four versions overall + by regime ---
    lines.append("## 1. Test A: V1/V2/V3/V4 Comparison\n")
    lines.append("### 1.1 Overall (full sample)\n")
    lines.append("| version | n | mean | Sharpe | win | sl_rate | tp_rate | false_stop |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for name in ["V1_current", "V2_all_one", "V3_trending_only", "V4_volatile_only"]:
        r = versions_df[(versions_df["version"] == name) &
                        (versions_df["regime"] == "ALL")].iloc[0]
        lines.append(
            f"| {name} | {int(r['n'])} | {r['realized_mean']*100:.2f}% | "
            f"{r['realized_sharpe']:.3f} | {r['win_rate']*100:.1f}% | "
            f"{r['sl_trigger_rate']*100:.1f}% | {r['tp_trigger_rate']*100:.1f}% | "
            f"{r['false_stop_rate']*100:.1f}% |"
        )
    lines.append("")

    lines.append("### 1.2 Per-regime breakdown (V1 vs V2)\n")
    lines.append("| regime | version | n | mean | Sharpe | win | sl_rate |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in REGIMES:
        for name in ["V1_current", "V2_all_one"]:
            row = versions_df[(versions_df["version"] == name) &
                              (versions_df["regime"] == r)]
            if len(row):
                rr = row.iloc[0]
                lines.append(
                    f"| {r} | {name} | {int(rr['n'])} | "
                    f"{rr['realized_mean']*100:.2f}% | {rr['realized_sharpe']:.3f} | "
                    f"{rr['win_rate']*100:.1f}% | {rr['sl_trigger_rate']*100:.1f}% |"
                )
    lines.append("")

    # Delta per regime
    lines.append("### 1.3 Per-regime V1 - V2 delta\n")
    lines.append("| regime | delta_mean | delta_sharpe | verdict |")
    lines.append("|---|---|---|---|")
    for r in REGIMES:
        v1r = versions_df[(versions_df["version"] == "V1_current") &
                          (versions_df["regime"] == r)]
        v2r = versions_df[(versions_df["version"] == "V2_all_one") &
                          (versions_df["regime"] == r)]
        if len(v1r) and len(v2r):
            dmr = v1r.iloc[0]["realized_mean"] - v2r.iloc[0]["realized_mean"]
            dsr = v1r.iloc[0]["realized_sharpe"] - v2r.iloc[0]["realized_sharpe"]
            if abs(dmr) < 0.002:
                verdict = "neutral (within noise)"
            elif dmr > 0.005:
                verdict = "V1 edge"
            elif dmr < -0.005:
                verdict = "V1 hurts"
            else:
                verdict = "marginal"
            lines.append(
                f"| {r} | {dmr*100:+.3f}% | {dsr:+.4f} | {verdict} |"
            )
    lines.append("")

    # --- 2. Test B: Per-regime grid best ---
    lines.append("## 2. Test B: Per-regime (sl_mult x tp_mult) grid search\n")
    if len(regime_grid_df):
        lines.append(f"Grid per regime: {len(SL_MULT_GRID)} SL x {len(TP_MULT_GRID)} TP = "
                     f"{len(SL_MULT_GRID)*len(TP_MULT_GRID)} combos.\n")
        lines.append("### Best per-regime (by Sharpe) vs V1 current vs V2 baseline\n")
        lines.append("| regime | best_sl | best_tp | best_mean | best_Sharpe | V1_sl | V1_tp | V1_mean | V1_Sharpe | baseline(1,1)_mean | baseline_Sharpe |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for r in REGIMES:
            sub = regime_grid_df[regime_grid_df["regime"] == r]
            if not len(sub):
                continue
            best = sub.sort_values(["realized_sharpe", "realized_mean"],
                                   ascending=False).iloc[0]
            # V1 current in this regime
            v1_sl, v1_tp = V1_CURRENT[r]
            v1_row = sub[(np.isclose(sub["sl_mult"], v1_sl)) &
                         (np.isclose(sub["tp_mult"], v1_tp))]
            base_row = sub[(np.isclose(sub["sl_mult"], 1.0)) &
                           (np.isclose(sub["tp_mult"], 1.0))]
            v1_mean = v1_row.iloc[0]["realized_mean"] if len(v1_row) else np.nan
            v1_sh = v1_row.iloc[0]["realized_sharpe"] if len(v1_row) else np.nan
            b_mean = base_row.iloc[0]["realized_mean"] if len(base_row) else np.nan
            b_sh = base_row.iloc[0]["realized_sharpe"] if len(base_row) else np.nan
            lines.append(
                f"| {r} | {best['sl_mult']} | {best['tp_mult']} | "
                f"{best['realized_mean']*100:.2f}% | {best['realized_sharpe']:.3f} | "
                f"{v1_sl} | {v1_tp} | {v1_mean*100:.2f}% | {v1_sh:.3f} | "
                f"{b_mean*100:.2f}% | {b_sh:.3f} |"
            )
        lines.append("")

        lines.append("### Per-regime interpretation\n")
        for r in REGIMES:
            sub = regime_grid_df[regime_grid_df["regime"] == r]
            if not len(sub):
                continue
            sh_range = sub["realized_sharpe"].max() - sub["realized_sharpe"].min()
            mn_range = (sub["realized_mean"].max() - sub["realized_mean"].min()) * 100
            lines.append(
                f"- **{r}** (n={int(sub['n'].iloc[0])}): "
                f"Sharpe range across 20 combos = {sh_range:.3f}; "
                f"mean range = {mn_range:.2f}pp. "
                f"{'FLAT -- no combo dominates' if sh_range < 0.02 else 'has some structure'}"
            )
        lines.append("")

    # --- 3. Walk-forward ---
    lines.append("## 3. Test C: Walk-forward stability\n")
    if len(wf_df):
        wf_pivot = wf_df.pivot_table(
            index="test_start", columns="version",
            values=["test_mean", "test_sharpe"], aggfunc="first"
        )
        n_win = len(wf_pivot)
        lines.append(f"Windows: {n_win} (12w train / 4w test, stride 4).\n")

        lines.append("### Version mean/Sharpe across WF test windows\n")
        lines.append("| version | test_mean avg | test_mean median | test_Sharpe avg | test_Sharpe median |")
        lines.append("|---|---|---|---|---|")
        for name in VERSIONS.keys():
            if ("test_mean", name) not in wf_pivot.columns:
                continue
            mn = wf_pivot[("test_mean", name)].dropna()
            sh = wf_pivot[("test_sharpe", name)].dropna()
            lines.append(
                f"| {name} | {mn.mean()*100:.2f}% | {mn.median()*100:.2f}% | "
                f"{sh.mean():.3f} | {sh.median():.3f} |"
            )
        lines.append("")

        # V1 vs V2 pairwise
        if ("test_mean", "V1_current") in wf_pivot.columns and \
           ("test_mean", "V2_all_one") in wf_pivot.columns:
            diff_mean = wf_pivot[("test_mean", "V1_current")] - wf_pivot[("test_mean", "V2_all_one")]
            diff_sh = wf_pivot[("test_sharpe", "V1_current")] - wf_pivot[("test_sharpe", "V2_all_one")]
            lines.append(
                f"**V1 - V2 diff across {n_win} windows**: "
                f"mean_diff avg = {diff_mean.mean()*100:+.3f}%, "
                f"median = {diff_mean.median()*100:+.3f}%; "
                f"Sharpe_diff avg = {diff_sh.mean():+.4f}, "
                f"median = {diff_sh.median():+.4f}."
            )
            lines.append(
                f"V1 beats V2 on test_mean in {int((diff_mean > 0).sum())}/{n_win} windows.\n"
            )
    else:
        lines.append("Walk-forward not produced.\n")

    # --- 4. Decision & action ---
    lines.append("## 4. Decision & Action\n")
    lines.append(f"- **Grade**: {decision['grade']}")
    lines.append(f"- **Reason**: {decision['reason']}")
    lines.append(f"- **Recommendation**: `{decision['recommend']}`\n")

    if decision["recommend"] == "cut_regime_exit_mult":
        lines.append("### Proposed code diff (DO NOT apply automatically)\n")
        lines.append("**File: `exit_manager.py`**\n")
        lines.append("1. Delete REGIME_EXIT_MULT constant (lines 62-72).\n")
        lines.append("2. Remove Phase 4 regime overlay in `compute_exit_plan`:\n")
        lines.append("```python")
        lines.append("# DELETE (lines 117-118, 131-132, 156-157):")
        lines.append("#   sl_mult, tp_mult = REGIME_EXIT_MULT.get(regime, (1.0, 1.0))")
        lines.append("#   stop_pct = np.clip(stop_pct * sl_mult, ATR_STOP_FLOOR, ATR_STOP_CEIL)")
        lines.append("#   tp_scale = np.clip(tp_scale * tp_mult, ATR_TP_SCALE_FLOOR, ATR_TP_SCALE_CEIL)")
        lines.append("")
        lines.append("# Keep return keys but hardcode regime_sl_mult=1.0, regime_tp_mult=1.0")
        lines.append("# so callers relying on these fields still work.")
        lines.append("```")
        lines.append("3. Optionally keep `regime` parameter in signature for forward-compat; ")
        lines.append("   or remove entirely after grep confirms no caller relies on it.\n")
        lines.append("**Rationale**: per-regime multipliers add code complexity (12 mult applications ")
        lines.append("across compute_exit_plan + tests) without measurable edge. Consistent with VF-G1 ")
        lines.append("finding that the stop-loss parameter space is a plateau and tail-risk is the ")
        lines.append("dominant role of SL, not regime-adaptive alpha.\n")
    elif decision["recommend"] == "keep_or_optimize":
        lines.append("Keep REGIME_EXIT_MULT. Consider tuning to best per-regime values from Test B.\n")

    lines.append("## 5. Files\n")
    lines.append(f"- Versions table: `{OUT_VERSIONS.name}`")
    lines.append(f"- Per-regime grid: `{OUT_REGIME.name}`")
    lines.append(f"- Walk-forward:   `{OUT_WF.name}`")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"[report] -> {OUT_MD}")


def main():
    print("[1/4] loading trade_journal...")
    df = pd.read_parquet(JOURNAL)
    before = len(df)
    df = df.dropna(subset=["weekly_ma20", "atr_pct", "fwd_20d",
                           "fwd_20d_max", "fwd_20d_min", "regime"])
    print(f"  rows: {before} -> {len(df)} after dropna")

    print("[2/4] Test A: V1/V2/V3/V4 + per-regime ...")
    versions_df = run_test_a(df)
    versions_df.to_csv(OUT_VERSIONS, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_VERSIONS}")

    print("[3/4] Test B: per-regime (sl,tp) grid ...")
    regime_grid_df = run_test_b(df)
    regime_grid_df.to_csv(OUT_REGIME, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_REGIME}")

    print("[4/4] Test C: walk-forward ...")
    wf_df = run_walk_forward(df, window_train=12, window_test=4, stride=4)
    wf_df.to_csv(OUT_WF, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_WF} ({len(wf_df)} rows)")

    decision = decide_grade(versions_df, wf_df)
    write_report(df, versions_df, regime_grid_df, wf_df, decision)

    print("\n=== DONE ===")
    print(f"Grade: {decision['grade']} | recommend: {decision['recommend']}")
    print(f"V1 vs V2 delta: mean {decision['delta_mean']*100:+.3f}%, "
          f"Sharpe {decision['delta_sharpe']:+.4f}")
    if decision["wf_total_windows"]:
        print(f"WF: V1 wins {decision['wf_v1_wins']}/{decision['wf_total_windows']} windows")


if __name__ == "__main__":
    main()
