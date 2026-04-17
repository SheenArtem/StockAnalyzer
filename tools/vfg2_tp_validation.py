"""
VF-G2 QM Take-Profit Grid Search (DEFAULT_TP_PCTS + ATR_TP_SCALE_FLOOR/CEIL)

Scope:
  Validate the 3-stage TP ladder + ATR tp_scale clip bounds in exit_manager.py.
  DO NOT change SL parameters -- they stay at VF-G1 baseline (stop_mult=3.0,
  stop_ceil=0.14, min_sl_gap_atr_mult=1.5).

Grid:
  TP1_PCT in [0.08, 0.12, 0.15, 0.18, 0.22, 0.30]              # first stage
  TP2_PCT in [0.18, 0.22, 0.25, 0.30, 0.40]                    # must > TP1
  TP3_PCT in [0.30, 0.40, 0.50, 0.60]                          # must > TP2
  ATR_TP_SCALE_CEIL  in [1.2, 1.4, 1.6, 1.8, 2.0]
  ATR_TP_SCALE_FLOOR in [0.5, 0.7, 0.9]

  Filter TP1 < TP2 < TP3. With 6*5*4 = 120 ladder combos,
  ~70 survive the monotonicity filter, times 5*3 = 15 scale combos
  = ~1050 valid combos.

Simulation (40d horizon, pessimistic path-aware logic):
  1. Compute SL price from entry / atr (baseline VF-G1 params).
  2. Compute TP1/TP2/TP3 prices = entry * (1 + base_pct * tp_scale).
  3. Compute tp_scale = clip(atr_pct / 2.5, floor, ceil) per pick.
  4. Position = 3 tranches (1/3 each).
  5. Pessimistic: SL hit -> ENTIRE position stops out at sl_pct (no partial exit
     even if TP1 also touched in-window). This is the honest grading.
  6. If SL not hit:
       - 1/3 at TP1 if fwd_40d_max >= TP1_price, else that 1/3 exits at fwd_40d
       - 1/3 at TP2 if fwd_40d_max >= TP2_price, else exits at fwd_40d
       - 1/3 at TP3 if fwd_40d_max >= TP3_price, else exits at fwd_40d
     blended_return = mean of three tranche realized returns.
  7. Pure-hold baseline: realized = fwd_40d (no SL, no TP).
  8. "SL-only" counterfactual: realized = sl_pct if SL hit else fwd_40d.
  9. "Pure hold (no SL)" counterfactual: realized = fwd_40d always.

Why fwd_40d over fwd_20d:
  TP3 = 40% may need >20d. QM holding period is 20-60d. Using 40d gives
  enough runway to evaluate whether the longer TP stages are touchable.

Outputs:
  reports/vfg2_tp_grid_full.csv     -- all valid combos
  reports/vfg2_walkforward.csv       -- WF stability for best combo
  reports/vfg2_by_regime.csv         -- per-regime for baseline + best
  reports/vfg2_tp_grid_search.md     -- TL;DR + tables + decision

Decision rules:
  A: best stable across WF + regime-consistent AND plateau (delta > 0.5% mean)
  B: best stable AND > baseline by 0.3-0.5% mean
  C: best in-sample but WF unstable
  D: delta < 0.3% mean OR pure-hold beats all -> keep baseline
"""

import sys
from pathlib import Path
from itertools import product

import numpy as np
import pandas as pd

ROOT = Path(r"c:\GIT\StockAnalyzer")
JOURNAL = ROOT / "data_cache" / "backtest" / "trade_journal_qm_tw.parquet"
OUT_FULL = ROOT / "reports" / "vfg2_tp_grid_full.csv"
OUT_WF = ROOT / "reports" / "vfg2_walkforward.csv"
OUT_REGIME = ROOT / "reports" / "vfg2_by_regime.csv"
OUT_MD = ROOT / "reports" / "vfg2_tp_grid_search.md"

# --- SL params held at VF-G1 baseline (NOT tuned here) ---
STOP_MULT = 3.0
STOP_CEIL = 0.14
STOP_FLOOR = 0.05
MIN_SL_GAP_ATR_MULT = 1.5
DEFAULT_MIN_SL_GAP = 0.03
ATR_PCT_MEDIAN = 2.5

# --- Current production values (baseline V1) ---
BASELINE_TP = dict(
    tp1=0.15, tp2=0.25, tp3=0.40,
    tp_scale_floor=0.7, tp_scale_ceil=1.6,
)

# --- TP Grid ---
GRID = {
    "tp1": [0.08, 0.12, 0.15, 0.18, 0.22, 0.30],
    "tp2": [0.18, 0.22, 0.25, 0.30, 0.40],
    "tp3": [0.30, 0.40, 0.50, 0.60],
    "tp_scale_ceil": [1.2, 1.4, 1.6, 1.8, 2.0],
    "tp_scale_floor": [0.5, 0.7, 0.9],
}

# --- Holding horizon (40d since TP3 >= 30%) ---
HOLDING = "fwd_40d"
HOLDING_MAX = "fwd_40d_max"
HOLDING_MIN = "fwd_40d_min"


def compute_min_sl_gap_vec(atr_pct):
    gap = atr_pct * MIN_SL_GAP_ATR_MULT / 100.0
    return np.maximum(DEFAULT_MIN_SL_GAP, gap)


def compute_sl_pct_vec(df):
    """Return SL pct (negative) using VF-G1 baseline logic."""
    entry = df["entry_price"].values
    atr = df["atr_pct"].values
    ma20 = df["weekly_ma20"].values

    stop_pct = np.clip(atr / 100.0 * STOP_MULT, STOP_FLOOR, STOP_CEIL)
    hard_stop_price = entry * (1 - stop_pct)
    min_gap = compute_min_sl_gap_vec(atr)

    ma20_valid = np.isfinite(ma20) & (ma20 > 0) & (ma20 < entry)
    ma20_gap = np.where(ma20_valid, (entry - ma20) / entry, np.nan)
    use_ma20 = ma20_valid & (ma20 > hard_stop_price) & (ma20_gap >= min_gap)
    sl_price = np.where(use_ma20, ma20, hard_stop_price)
    sl_pct = (sl_price / entry) - 1.0
    return sl_pct


def simulate(df, tp1, tp2, tp3, scale_floor, scale_ceil):
    """
    Vectorized simulation for one TP combo.

    Returns dict of metrics plus per-pick realized return.
    """
    atr = df["atr_pct"].values
    fwd = df[HOLDING].values
    fwd_max = df[HOLDING_MAX].values
    fwd_min = df[HOLDING_MIN].values

    sl_pct = compute_sl_pct_vec(df)  # negative
    hit_sl = fwd_min <= sl_pct

    # Per-pick TP scale (vectorized clip)
    tp_scale = np.clip(atr / ATR_PCT_MEDIAN, scale_floor, scale_ceil)

    # Scaled TP pcts (positive)
    tp1_scaled = tp1 * tp_scale
    tp2_scaled = tp2 * tp_scale
    tp3_scaled = tp3 * tp_scale

    # Did each TP level touch within 40d?
    hit_tp1 = fwd_max >= tp1_scaled
    hit_tp2 = fwd_max >= tp2_scaled
    hit_tp3 = fwd_max >= tp3_scaled

    # --- Pessimistic: if SL hit, WHOLE position exits at sl_pct ---
    # (we can't know whether TP1 fired before SL; the conservative call is SL first)
    realized = np.where(
        hit_sl,
        sl_pct,
        # No SL: blend of 3 tranches
        (
            (np.where(hit_tp1, tp1_scaled, fwd) +
             np.where(hit_tp2, tp2_scaled, fwd) +
             np.where(hit_tp3, tp3_scaled, fwd)) / 3.0
        )
    )

    n = len(df)
    mean = float(np.nanmean(realized))
    std = float(np.nanstd(realized))
    sharpe = mean / std if std > 0 else np.nan
    win = float((realized > 0).sum()) / n

    sl_rate = float(hit_sl.sum()) / n
    tp1_rate = float((hit_tp1 & ~hit_sl).sum()) / n  # pure TP1 (no SL conflict)
    tp2_rate = float((hit_tp2 & ~hit_sl).sum()) / n
    tp3_rate = float((hit_tp3 & ~hit_sl).sum()) / n

    # Avg holding return for picks that didn't trigger SL or any TP
    neither = (~hit_sl) & (~hit_tp1)
    avg_hold = float(np.nanmean(fwd[neither])) if neither.sum() > 0 else np.nan

    # R:R
    sl_abs = np.abs(sl_pct)
    sl_abs = np.where(sl_abs > 1e-6, sl_abs, 1e-6)
    rr = fwd_max / sl_abs
    rr_median = float(np.nanmedian(rr))

    return {
        "n": n,
        "realized_mean": mean,
        "realized_std": std,
        "realized_sharpe": sharpe,
        "win_rate": win,
        "sl_trigger_rate": sl_rate,
        "tp1_trigger_rate": tp1_rate,
        "tp2_trigger_rate": tp2_rate,
        "tp3_trigger_rate": tp3_rate,
        "avg_holding_ret_no_tp": avg_hold,
        "rr_median": rr_median,
        "_realized": realized,   # not serialized unless needed
    }, realized


def valid_combos():
    combos = []
    for tp1, tp2, tp3, ceil, floor in product(
        GRID["tp1"], GRID["tp2"], GRID["tp3"],
        GRID["tp_scale_ceil"], GRID["tp_scale_floor"],
    ):
        if tp1 >= tp2 or tp2 >= tp3:
            continue
        if floor >= ceil:
            continue
        combos.append((tp1, tp2, tp3, floor, ceil))
    return combos


def run_full_grid(df):
    combos = valid_combos()
    print(f"[grid] {len(combos)} valid combos x {len(df)} picks")
    rows = []
    for i, (tp1, tp2, tp3, floor, ceil) in enumerate(combos, 1):
        m, _ = simulate(df, tp1, tp2, tp3, floor, ceil)
        m.pop("_realized", None)
        rows.append({
            "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "tp_scale_floor": floor, "tp_scale_ceil": ceil,
            **m,
        })
        if i % 200 == 0:
            print(f"  {i}/{len(combos)}")
    return pd.DataFrame(rows)


def pick_best(grid_df):
    return grid_df.sort_values(
        ["realized_sharpe", "realized_mean"], ascending=[False, False]
    ).iloc[0].to_dict()


def baseline_row(grid_df):
    b = BASELINE_TP
    m = grid_df[
        (grid_df["tp1"] == b["tp1"]) &
        (grid_df["tp2"] == b["tp2"]) &
        (grid_df["tp3"] == b["tp3"]) &
        (grid_df["tp_scale_floor"] == b["tp_scale_floor"]) &
        (grid_df["tp_scale_ceil"] == b["tp_scale_ceil"])
    ]
    return m.iloc[0].to_dict() if len(m) else None


def counterfactual_stats(df):
    """Pure-hold + SL-only baselines for context."""
    sl_pct = compute_sl_pct_vec(df)
    fwd = df[HOLDING].values
    fwd_min = df[HOLDING_MIN].values

    # pure hold
    ph = {
        "realized_mean": float(np.nanmean(fwd)),
        "realized_std": float(np.nanstd(fwd)),
        "win_rate": float((fwd > 0).sum()) / len(fwd),
    }
    ph["realized_sharpe"] = ph["realized_mean"] / ph["realized_std"] if ph["realized_std"] > 0 else np.nan

    # SL-only (no TP)
    hit_sl = fwd_min <= sl_pct
    sl_only = np.where(hit_sl, sl_pct, fwd)
    so = {
        "realized_mean": float(np.nanmean(sl_only)),
        "realized_std": float(np.nanstd(sl_only)),
        "sl_rate": float(hit_sl.sum()) / len(sl_only),
        "win_rate": float((sl_only > 0).sum()) / len(sl_only),
    }
    so["realized_sharpe"] = so["realized_mean"] / so["realized_std"] if so["realized_std"] > 0 else np.nan

    return ph, so


def run_by_regime(df, params_list):
    rows = []
    for name, p in params_list:
        m_all, rel = simulate(
            df, p["tp1"], p["tp2"], p["tp3"],
            p["tp_scale_floor"], p["tp_scale_ceil"],
        )
        m_all.pop("_realized", None)
        rows.append({"combo": name, "regime": "ALL", **p, **m_all})

        for regime in df["regime"].dropna().unique():
            mask = (df["regime"] == regime).values
            if mask.sum() < 50:
                continue
            sub = df.loc[mask].reset_index(drop=True)
            m, _ = simulate(
                sub, p["tp1"], p["tp2"], p["tp3"],
                p["tp_scale_floor"], p["tp_scale_ceil"],
            )
            m.pop("_realized", None)
            rows.append({"combo": name, "regime": regime, **p, **m})
    return pd.DataFrame(rows)


def walk_forward(df, window_train=12, window_test=4, stride=4):
    combos = valid_combos()
    weeks = sorted(df["week_end_date"].unique())
    n_weeks = len(weeks)
    print(f"[wf] weeks={n_weeks}, train={window_train}, test={window_test}, stride={stride}, combos={len(combos)}")

    wf_rows = []
    win_count = 0
    for start in range(0, n_weeks - window_train - window_test + 1, stride):
        train_weeks = weeks[start:start + window_train]
        test_weeks = weeks[start + window_train:start + window_train + window_test]
        df_train = df[df["week_end_date"].isin(train_weeks)]
        df_test = df[df["week_end_date"].isin(test_weeks)]
        if len(df_train) < 200 or len(df_test) < 100:
            continue

        train_rows = []
        test_rows = []
        for tp1, tp2, tp3, floor, ceil in combos:
            m_tr, _ = simulate(df_train, tp1, tp2, tp3, floor, ceil)
            train_rows.append({
                "tp1": tp1, "tp2": tp2, "tp3": tp3,
                "tp_scale_floor": floor, "tp_scale_ceil": ceil,
                "train_sharpe": m_tr["realized_sharpe"],
                "train_mean": m_tr["realized_mean"],
            })
            m_te, _ = simulate(df_test, tp1, tp2, tp3, floor, ceil)
            test_rows.append({
                "tp1": tp1, "tp2": tp2, "tp3": tp3,
                "tp_scale_floor": floor, "tp_scale_ceil": ceil,
                "test_sharpe": m_te["realized_sharpe"],
                "test_mean": m_te["realized_mean"],
            })

        tr_df = pd.DataFrame(train_rows)
        te_df = pd.DataFrame(test_rows)
        tr_df["train_rank"] = tr_df["train_sharpe"].rank(ascending=False, method="min")
        te_df["test_rank"] = te_df["test_sharpe"].rank(ascending=False, method="min")
        merged = tr_df.merge(
            te_df,
            on=["tp1", "tp2", "tp3", "tp_scale_floor", "tp_scale_ceil"],
        )
        merged["train_start"] = pd.Timestamp(train_weeks[0])
        merged["test_start"] = pd.Timestamp(test_weeks[0])

        top5_train = merged.nsmallest(5, "train_rank").copy()
        top5_train["note"] = "train_top5"
        base_mask = (
            (merged["tp1"] == BASELINE_TP["tp1"]) &
            (merged["tp2"] == BASELINE_TP["tp2"]) &
            (merged["tp3"] == BASELINE_TP["tp3"]) &
            (merged["tp_scale_floor"] == BASELINE_TP["tp_scale_floor"]) &
            (merged["tp_scale_ceil"] == BASELINE_TP["tp_scale_ceil"])
        )
        base_row = merged[base_mask].copy()
        base_row["note"] = "baseline"
        wf_rows.append(pd.concat([top5_train, base_row], ignore_index=True))
        win_count += 1

    print(f"  windows: {win_count}")
    if not wf_rows:
        return pd.DataFrame()
    return pd.concat(wf_rows, ignore_index=True)


def fmt_combo(d):
    return (f"tp1={d['tp1']}/tp2={d['tp2']}/tp3={d['tp3']}/"
            f"floor={d['tp_scale_floor']}/ceil={d['tp_scale_ceil']}")


def decide_grade(best, base, wf_df, ph_stats):
    """
    D: pure-hold beats best OR delta_mean < 0.3% vs baseline
    C: delta 0.3-0.5% mean, either in-sample or WF unstable
    B: delta > 0.5% mean AND WF stable (best combo avg test_rank <= 50)
    A: delta > 0.8% mean AND WF very stable (test_rank <= 20) AND plateau
    """
    dm = best["realized_mean"] - (base["realized_mean"] if base else 0)
    ds = best["realized_sharpe"] - (base["realized_sharpe"] if base else 0)
    ph_dm = best["realized_mean"] - ph_stats["realized_mean"]
    ph_ds = best["realized_sharpe"] - ph_stats["realized_sharpe"]

    # WF stability
    wf_test_rank = np.nan
    if len(wf_df):
        best_mask = (
            (wf_df["tp1"] == best["tp1"]) &
            (wf_df["tp2"] == best["tp2"]) &
            (wf_df["tp3"] == best["tp3"]) &
            (wf_df["tp_scale_floor"] == best["tp_scale_floor"]) &
            (wf_df["tp_scale_ceil"] == best["tp_scale_ceil"])
        )
        if best_mask.any():
            wf_test_rank = wf_df.loc[best_mask, "test_rank"].mean()

    # Rule 1: pure-hold dominates
    if ph_dm < 0 or ph_ds < -0.02:
        return dict(
            grade="D",
            reason=(
                f"pure-hold beats best TP: mean delta {ph_dm*100:+.2f}%, "
                f"Sharpe {ph_ds:+.3f}. TP ladder destroys edge."
            ),
            recommend="consider_removing_tp_or_keep_baseline",
            delta_mean=dm, delta_sharpe=ds,
            ph_delta_mean=ph_dm, ph_delta_sharpe=ph_ds,
            wf_test_rank_mean=wf_test_rank,
        )

    # Rule 2: delta vs baseline
    if dm < 0.003 and ds < 0.05:
        grade = "D"
        reason = f"best-vs-baseline delta insufficient (mean {dm*100:+.2f}%, Sharpe {ds:+.3f})"
        recommend = "keep_baseline"
    elif 0.003 <= dm < 0.005:
        grade = "C"
        reason = f"mild edge {dm*100:.2f}% mean; note but don't commit"
        recommend = "keep_baseline_note_best"
    elif 0.005 <= dm < 0.008:
        if pd.notna(wf_test_rank) and wf_test_rank <= 50:
            grade = "B"
            reason = f"edge {dm*100:.2f}% mean AND WF stable (test_rank_mean={wf_test_rank:.1f})"
            recommend = "commit"
        else:
            grade = "C"
            reason = f"mid edge {dm*100:.2f}% mean but WF unstable (test_rank_mean={wf_test_rank})"
            recommend = "keep_baseline_note_best"
    elif dm >= 0.008:
        if pd.notna(wf_test_rank) and wf_test_rank <= 20:
            grade = "A"
            reason = f"strong edge {dm*100:.2f}% AND very stable (test_rank_mean={wf_test_rank:.1f})"
            recommend = "commit"
        elif pd.notna(wf_test_rank) and wf_test_rank <= 80:
            grade = "B"
            reason = f"strong edge {dm*100:.2f}% AND stable (test_rank_mean={wf_test_rank:.1f})"
            recommend = "commit"
        else:
            grade = "C"
            reason = f"strong in-sample but WF fails (test_rank_mean={wf_test_rank})"
            recommend = "keep_baseline_note_best"
    else:
        grade = "D"
        reason = "fallback"
        recommend = "keep_baseline"

    return dict(
        grade=grade, reason=reason, recommend=recommend,
        delta_mean=dm, delta_sharpe=ds,
        ph_delta_mean=ph_dm, ph_delta_sharpe=ph_ds,
        wf_test_rank_mean=wf_test_rank,
    )


def write_report(grid_df, best, base, regime_df, wf_df, decision, ph_stats, so_stats, df):
    lines = []
    lines.append("# VF-G2 QM Take-Profit Grid Search\n")
    lines.append(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"Sample: {len(df)} picks, {df['week_end_date'].nunique()} weeks "
                 f"({df['week_end_date'].min():%Y-%m-%d} to {df['week_end_date'].max():%Y-%m-%d})")
    lines.append(f"Horizon: {HOLDING}  (fwd_40d_max/min used for TP/SL touch detection)")
    lines.append("")

    # --- TL;DR ---
    lines.append("## TL;DR\n")
    lines.append(
        f"- **Best combo**: {fmt_combo(best)}  "
        f"Sharpe={best['realized_sharpe']:.3f}  "
        f"mean={best['realized_mean']*100:.2f}%  win={best['win_rate']*100:.1f}%"
    )
    if base:
        lines.append(
            f"- **Baseline (V1 current)**: {fmt_combo(BASELINE_TP)}  "
            f"Sharpe={base['realized_sharpe']:.3f}  "
            f"mean={base['realized_mean']*100:.2f}%  win={base['win_rate']*100:.1f}%"
        )
    lines.append(
        f"- **Pure-hold (no SL/TP, fwd_40d)**: "
        f"Sharpe={ph_stats['realized_sharpe']:.3f}  "
        f"mean={ph_stats['realized_mean']*100:.2f}%  win={ph_stats['win_rate']*100:.1f}%"
    )
    lines.append(
        f"- **SL-only (VF-G1 baseline, no TP)**: "
        f"Sharpe={so_stats['realized_sharpe']:.3f}  "
        f"mean={so_stats['realized_mean']*100:.2f}%  win={so_stats['win_rate']*100:.1f}%"
    )
    lines.append(
        f"- **Delta (best vs baseline)**: "
        f"Sharpe {decision['delta_sharpe']:+.3f}  "
        f"mean {decision['delta_mean']*100:+.2f}%"
    )
    lines.append(
        f"- **Delta (best vs pure-hold)**: "
        f"Sharpe {decision['ph_delta_sharpe']:+.3f}  "
        f"mean {decision['ph_delta_mean']*100:+.2f}%"
    )
    if pd.notna(decision["wf_test_rank_mean"]):
        lines.append(
            f"- **Walk-forward stability**: best combo avg test_rank = "
            f"{decision['wf_test_rank_mean']:.1f}/{len(valid_combos())}"
        )
    lines.append(
        f"- **Grade: {decision['grade']}** -- {decision['reason']} "
        f"-- recommend: `{decision['recommend']}`"
    )
    lines.append("")

    # --- Critical context ---
    lines.append("## 0. Critical Context\n")
    lines.append(
        "**VF-G1 finding**: 4D SL grid was D-grade; pure-hold mean=2.84% beat any SL. "
        "This VF-G2 test asks the SAME question for TP: does adding 3-stage TP ladder "
        "help or hurt vs pure-hold + SL-only?"
    )
    lines.append("")
    lines.append(
        "**Pessimistic simulation**: if SL triggers within 40d (fwd_40d_min <= sl_pct), "
        "the ENTIRE 3-tranche position exits at sl_pct. We cannot know intraday order "
        "between TP1 and SL, so we assume SL first (same as VF-G1). This UNDERSTATES "
        "the TP ladder benefit slightly, but is the honest grading."
    )
    lines.append("")
    lines.append(
        "**TP blending (if SL not hit)**: each of 3 tranches realizes independently. "
        "Tranche i pays min(tp_i_scaled, fwd_40d_max); if not reached, it pays fwd_40d "
        "(close-of-period)."
    )
    lines.append("")

    # --- Top-10 combos ---
    lines.append("## 1. Top-10 Combos (full sample, sorted by Sharpe)\n")
    top10 = grid_df.sort_values("realized_sharpe", ascending=False).head(10)
    lines.append("| rank | tp1 | tp2 | tp3 | floor | ceil | Sharpe | mean | win | sl_rate | tp1_rate | tp3_rate |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in enumerate(top10.itertuples(index=False), 1):
        lines.append(
            f"| {i} | {r.tp1} | {r.tp2} | {r.tp3} | {r.tp_scale_floor} | {r.tp_scale_ceil} | "
            f"{r.realized_sharpe:.3f} | {r.realized_mean*100:.2f}% | "
            f"{r.win_rate*100:.1f}% | {r.sl_trigger_rate*100:.1f}% | "
            f"{r.tp1_trigger_rate*100:.1f}% | {r.tp3_trigger_rate*100:.1f}% |"
        )
    lines.append("")

    # --- Baseline ranking ---
    lines.append("## 2. Baseline Ranking\n")
    if base:
        base_rank = (grid_df["realized_sharpe"] > base["realized_sharpe"]).sum() + 1
        lines.append(
            f"Baseline V1 ({fmt_combo(BASELINE_TP)}) Sharpe = {base['realized_sharpe']:.3f}, "
            f"ranked **#{base_rank}/{len(grid_df)}** "
            f"({(base_rank/len(grid_df))*100:.1f}%ile)."
        )
    lines.append("")

    # --- Pure-hold vs best vs baseline table ---
    lines.append("## 3. V1 vs V2 vs V3 (No-TP) vs Pure-Hold\n")
    lines.append("| version | desc | Sharpe | mean | win | sl_rate |")
    lines.append("|---|---|---|---|---|---|")
    if base:
        lines.append(f"| V1 baseline | {fmt_combo(BASELINE_TP)} | "
                     f"{base['realized_sharpe']:.3f} | {base['realized_mean']*100:.2f}% | "
                     f"{base['win_rate']*100:.1f}% | {base['sl_trigger_rate']*100:.1f}% |")
    lines.append(f"| V2 best grid | {fmt_combo(best)} | "
                 f"{best['realized_sharpe']:.3f} | {best['realized_mean']*100:.2f}% | "
                 f"{best['win_rate']*100:.1f}% | {best['sl_trigger_rate']*100:.1f}% |")
    lines.append(f"| V3 SL-only (no TP) | SL at VF-G1 baseline, hold fwd_40d | "
                 f"{so_stats['realized_sharpe']:.3f} | {so_stats['realized_mean']*100:.2f}% | "
                 f"{so_stats['win_rate']*100:.1f}% | {so_stats['sl_rate']*100:.1f}% |")
    lines.append(f"| V4 pure-hold | no SL, no TP, hold fwd_40d | "
                 f"{ph_stats['realized_sharpe']:.3f} | {ph_stats['realized_mean']*100:.2f}% | "
                 f"{ph_stats['win_rate']*100:.1f}% | - |")
    lines.append("")

    # --- By regime ---
    lines.append("## 4. Best Combo vs Baseline by Regime\n")
    if len(regime_df):
        lines.append("| combo | regime | n | Sharpe | mean | win | sl_rate | tp1_rate | tp3_rate |")
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for r in regime_df.itertuples(index=False):
            lines.append(
                f"| {r.combo} | {r.regime} | {int(r.n)} | {r.realized_sharpe:.3f} | "
                f"{r.realized_mean*100:.2f}% | {r.win_rate*100:.1f}% | "
                f"{r.sl_trigger_rate*100:.1f}% | {r.tp1_trigger_rate*100:.1f}% | "
                f"{r.tp3_trigger_rate*100:.1f}% |"
            )
    lines.append("")

    # --- Walk-forward ---
    lines.append("## 5. Walk-Forward Summary\n")
    if len(wf_df):
        n_windows = wf_df["test_start"].nunique()
        lines.append(f"Windows: {n_windows} (12 weeks train / 4 weeks test, stride 4)")
        best_mask = (
            (wf_df["tp1"] == best["tp1"]) &
            (wf_df["tp2"] == best["tp2"]) &
            (wf_df["tp3"] == best["tp3"]) &
            (wf_df["tp_scale_floor"] == best["tp_scale_floor"]) &
            (wf_df["tp_scale_ceil"] == best["tp_scale_ceil"])
        )
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
        base_mask = (
            (wf_df["tp1"] == BASELINE_TP["tp1"]) &
            (wf_df["tp2"] == BASELINE_TP["tp2"]) &
            (wf_df["tp3"] == BASELINE_TP["tp3"]) &
            (wf_df["tp_scale_floor"] == BASELINE_TP["tp_scale_floor"]) &
            (wf_df["tp_scale_ceil"] == BASELINE_TP["tp_scale_ceil"])
        )
        base_wf = wf_df[base_mask]
        if len(base_wf):
            lines.append(
                f"\nBaseline WF: test_rank mean={base_wf['test_rank'].mean():.1f}, "
                f"median={base_wf['test_rank'].median():.0f}, "
                f"std={base_wf['test_rank'].std():.1f}"
            )
    else:
        lines.append("Walk-forward not produced.")
    lines.append("")

    # --- TP scale sensitivity ---
    lines.append("## 6. TP Scale Floor/Ceil Sensitivity (baseline tp1/tp2/tp3)\n")
    sub = grid_df[
        (grid_df["tp1"] == BASELINE_TP["tp1"]) &
        (grid_df["tp2"] == BASELINE_TP["tp2"]) &
        (grid_df["tp3"] == BASELINE_TP["tp3"])
    ].sort_values(["tp_scale_floor", "tp_scale_ceil"])
    if len(sub):
        lines.append("| floor | ceil | Sharpe | mean | win | tp1_rate |")
        lines.append("|---|---|---|---|---|---|")
        for r in sub.itertuples(index=False):
            lines.append(
                f"| {r.tp_scale_floor} | {r.tp_scale_ceil} | "
                f"{r.realized_sharpe:.3f} | {r.realized_mean*100:.2f}% | "
                f"{r.win_rate*100:.1f}% | {r.tp1_trigger_rate*100:.1f}% |"
            )
    lines.append("")

    # --- Decision ---
    lines.append("## 7. Decision & Action\n")
    lines.append(f"- **Grade**: {decision['grade']}")
    lines.append(f"- **Reason**: {decision['reason']}")
    lines.append(f"- **Recommendation**: `{decision['recommend']}`")
    lines.append("")

    if decision["recommend"] == "keep_baseline":
        lines.append("No change proposed. Current `DEFAULT_TP_PCTS = (0.15, 0.25, 0.40)` "
                     "and ATR scale [0.7, 1.6] remain optimal or grid failed to improve meaningfully.")
    elif decision["recommend"] == "keep_baseline_note_best":
        lines.append(
            f"Keep V1 baseline. Note V2 best `{fmt_combo(best)}` for revalidation "
            f"after more weeks accumulated."
        )
    elif decision["recommend"].startswith("consider_removing"):
        lines.append(
            "**Pure-hold beats best TP ladder**. Options:\n"
            "1. Remove 3-stage TP entirely; hold until 40d close or trailing stop.\n"
            "2. Replace with single-stage 40% target + trailing.\n"
            "3. Keep baseline for psychological/risk-mgmt reasons but acknowledge no alpha.\n"
            "\n**Recommended code change** (exit_manager.py):\n"
            "```python\n"
            "# If removing TP entirely:\n"
            "DEFAULT_TP_PCTS = ()  # empty tuple disables staged TP\n"
            "# TP block in compute_exit_plan() should early-return empty tp_levels\n"
            "```\n"
        )
    elif decision["recommend"] == "commit":
        diff_lines = [
            "**Propose exit_manager.py diff**:",
            "```python",
            f"# BEFORE: DEFAULT_TP_PCTS = {tuple(BASELINE_TP[k] for k in ['tp1','tp2','tp3'])}",
            f"DEFAULT_TP_PCTS = ({best['tp1']}, {best['tp2']}, {best['tp3']})",
            "",
            f"# BEFORE: ATR_TP_SCALE_FLOOR = {BASELINE_TP['tp_scale_floor']}  /  ATR_TP_SCALE_CEIL = {BASELINE_TP['tp_scale_ceil']}",
            f"ATR_TP_SCALE_FLOOR = {best['tp_scale_floor']}",
            f"ATR_TP_SCALE_CEIL  = {best['tp_scale_ceil']}",
            "```",
        ]
        lines.extend(diff_lines)

    lines.append("")
    lines.append("## 8. Files\n")
    lines.append(f"- Full grid: `{OUT_FULL.name}`")
    lines.append(f"- Walk-forward: `{OUT_WF.name}`")
    lines.append(f"- By regime: `{OUT_REGIME.name}`")
    lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"[report] -> {OUT_MD}")


def main():
    print("[1/5] loading trade_journal...")
    df = pd.read_parquet(JOURNAL)
    before = len(df)
    df = df.dropna(subset=["weekly_ma20", "atr_pct", "fwd_40d",
                           "fwd_40d_max", "fwd_40d_min"]).reset_index(drop=True)
    print(f"  rows: {before} -> {len(df)} after dropna")

    print("[2/5] full grid search...")
    grid_df = run_full_grid(df)
    OUT_FULL.parent.mkdir(parents=True, exist_ok=True)
    grid_df.to_csv(OUT_FULL, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_FULL}  ({len(grid_df)} rows)")

    best = pick_best(grid_df)
    base = baseline_row(grid_df)
    print(f"  best: {fmt_combo(best)}  Sharpe={best['realized_sharpe']:.3f}")
    if base:
        print(f"  base: {fmt_combo(BASELINE_TP)}  Sharpe={base['realized_sharpe']:.3f}")

    print("[3/5] counterfactuals (pure-hold, SL-only)...")
    ph_stats, so_stats = counterfactual_stats(df)
    print(f"  pure-hold: Sharpe={ph_stats['realized_sharpe']:.3f}, mean={ph_stats['realized_mean']*100:.2f}%")
    print(f"  SL-only:   Sharpe={so_stats['realized_sharpe']:.3f}, mean={so_stats['realized_mean']*100:.2f}%")

    print("[4/5] by-regime (baseline + best)...")
    params_list = [
        ("baseline", BASELINE_TP),
        ("best", {k: best[k] for k in ["tp1", "tp2", "tp3", "tp_scale_floor", "tp_scale_ceil"]}),
    ]
    regime_df = run_by_regime(df, params_list)
    regime_df.to_csv(OUT_REGIME, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_REGIME}")

    print("[5/5] walk-forward...")
    wf_df = walk_forward(df, window_train=12, window_test=4, stride=4)
    wf_df.to_csv(OUT_WF, index=False, encoding="utf-8-sig")
    print(f"  -> {OUT_WF} ({len(wf_df)} rows)")

    decision = decide_grade(best, base, wf_df, ph_stats)
    write_report(grid_df, best, base, regime_df, wf_df, decision, ph_stats, so_stats, df)

    print("\n=== DONE ===")
    print(f"Grade: {decision['grade']} | recommend: {decision['recommend']}")
    print(f"Best: {fmt_combo(best)}  Sharpe={best['realized_sharpe']:.3f}  mean={best['realized_mean']*100:.2f}%")
    if base:
        print(f"Base: Sharpe={base['realized_sharpe']:.3f}  mean={base['realized_mean']*100:.2f}%")
    print(f"Pure-hold: Sharpe={ph_stats['realized_sharpe']:.3f}  mean={ph_stats['realized_mean']*100:.2f}%")
    print(f"Delta (best vs base):  mean {decision['delta_mean']*100:+.2f}%  Sharpe {decision['delta_sharpe']:+.3f}")
    print(f"Delta (best vs pure):  mean {decision['ph_delta_mean']*100:+.2f}%  Sharpe {decision['ph_delta_sharpe']:+.3f}")


if __name__ == "__main__":
    main()
