"""
System 3 Phase 3.3b - Rank-based composite (B+E v3 style).

Phase 3.3 logistic L2 raw-feature composite scored AUC 0.679 < single ma_dist_60
rank 0.714 -- failed SOP-12. This phase tests rank-percentile composite (each
feature's rolling-252d rank, then weighted-mean by univariate lift).

Hypothesis: rank-percentile scoring matches the conditional context (recent
regime) better than raw values, especially for level features (vix, move).

Outputs:
  reports/system3_phase33b_predictions.parquet
  reports/system3_phase33b_summary.md
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, log_loss

ROOT = Path(__file__).resolve().parents[1]
PANEL_PATH = ROOT / "reports" / "system3_panel.parquet"
OUT_PRED = ROOT / "reports" / "system3_phase33b_predictions.parquet"
OUT_MD = ROOT / "reports" / "system3_phase33b_summary.md"

# Univariate-lift-derived weights (from Phase 3.2 results)
# rebalanced to sum=1
RAW_LIFTS = {
    "ma_dist_60": 2.85,    # high lift, stable
    "rv_20d": 2.15,
    "move_level": 2.46,
    "vix_term": 2.58,
}

# Direction: True = high rank (above 50%) more dangerous; False = low rank dangerous
DIRECTION_HIGH_DANGER = {
    "ma_dist_60": False,    # low ma_dist (below MA60) = danger
    "rv_20d": True,
    "move_level": True,
    "vix_term": True,
}

WEIGHTS = {k: v / sum(RAW_LIFTS.values()) for k, v in RAW_LIFTS.items()}
FEATURES = list(WEIGHTS.keys())
TARGET = "fwd_21d_mdd_10pct"
ROLLING = 252
MIN_TRAIN = 1000


def compute_composite(panel: pd.DataFrame) -> pd.Series:
    """Per-day composite = weighted mean of rolling-252d rank percentiles."""
    valid = panel[FEATURES + [TARGET]].notna().all(axis=1)
    df = panel[valid].sort_index()
    composite = pd.Series(0.0, index=df.index)
    weight_sum = 0.0
    for feat in FEATURES:
        s = df[feat]
        rank = s.rolling(ROLLING).rank(pct=True)
        if not DIRECTION_HIGH_DANGER[feat]:
            rank = 1.0 - rank
        composite = composite + WEIGHTS[feat] * rank
        weight_sum += WEIGHTS[feat]
    composite = composite / weight_sum
    return composite.dropna()


def main() -> None:
    panel = pd.read_parquet(PANEL_PATH)
    panel.index = pd.to_datetime(panel.index)

    composite = compute_composite(panel)
    print(f"[INFO] composite N={len(composite)}, range {composite.index[0].date()} -> {composite.index[-1].date()}")

    # Align with target
    target_aligned = panel.loc[composite.index, TARGET].astype(int)
    df_eval = pd.DataFrame({"composite": composite, "true": target_aligned}).dropna()
    df_eval = df_eval.iloc[MIN_TRAIN:]  # match phase33 OOS window
    print(f"[INFO] OOS evaluation N={len(df_eval)} from {df_eval.index[0].date()} to {df_eval.index[-1].date()}")

    df_eval.to_parquet(OUT_PRED)

    y = df_eval["true"].to_numpy(dtype=int)
    p = df_eval["composite"].to_numpy(dtype=float)
    auc = roc_auc_score(y, p)
    base = float(y.mean())

    # Lift @ thresholds
    metrics = {"auc": float(auc), "n": len(df_eval), "n_pos": int(y.sum()), "baseline": base}
    for top_pct in [0.05, 0.10, 0.20]:
        thr = np.quantile(p, 1 - top_pct)
        mask = p >= thr
        metrics[f"lift_top{int(top_pct * 100)}"] = float(y[mask].mean() / base) if base > 0 else np.nan
        metrics[f"precision_top{int(top_pct * 100)}"] = float(y[mask].mean())

    # Per-epoch
    per_ep = {}
    for ep, start, end in [
        ("2011-2014", "2011-01-01", "2015-01-01"),
        ("2015-2019", "2015-01-01", "2020-01-01"),
        ("2020-2026", "2020-01-01", "2027-01-01"),
    ]:
        mask = (df_eval.index >= pd.Timestamp(start)) & (df_eval.index < pd.Timestamp(end))
        sub = df_eval[mask]
        if len(sub) > 50 and sub["true"].sum() > 5:
            per_ep[ep] = float(roc_auc_score(sub["true"], sub["composite"]))
        else:
            per_ep[ep] = np.nan

    # Compare to single ma_dist_60 rank baseline
    md_rank = (-panel["ma_dist_60"]).rolling(252).rank(pct=True)
    md_aligned = md_rank.loc[df_eval.index].dropna()
    common_idx = df_eval.index.intersection(md_aligned.index)
    y_md = df_eval.loc[common_idx, "true"].to_numpy(dtype=int)
    p_md = md_aligned.loc[common_idx].to_numpy(dtype=float)
    md_auc = float(roc_auc_score(y_md, p_md))
    md_lift10 = float(y_md[p_md >= np.quantile(p_md, 0.90)].mean() / y_md.mean()) if y_md.mean() > 0 else np.nan

    L = []
    L.append("# System 3 Phase 3.3b - Rank Composite vs Single Feature")
    L.append("")
    L.append(f"**Composite**: weighted-mean of rolling-{ROLLING}d rank-pct of {FEATURES}")
    L.append(f"**Weights** (lift-derived, normalized): {WEIGHTS}")
    L.append(f"**Target**: {TARGET}")
    L.append(f"**OOS N**: {metrics['n']} ({metrics['n_pos']} positive, {metrics['baseline']:.1%} base)")
    L.append("")
    L.append("## Composite vs Single ma_dist_60 rank (SOP-12 critical)")
    L.append("")
    L.append("| Metric | Composite | Single ma_dist_60 rank | Δ |")
    L.append("|---|---|---|---|")
    L.append(f"| AUC | {metrics['auc']:.3f} | {md_auc:.3f} | {metrics['auc'] - md_auc:+.3f} |")
    L.append(f"| Lift top-10% | {metrics['lift_top10']:.2f} | {md_lift10:.2f} | {metrics['lift_top10'] - md_lift10:+.2f} |")
    L.append(f"| Lift top-5% | {metrics['lift_top5']:.2f} | (n/a) | - |")
    L.append(f"| Precision top-10% | {metrics['precision_top10']:.2%} | (n/a) | - |")
    L.append(f"| Precision top-5% | {metrics['precision_top5']:.2%} | (n/a) | - |")
    L.append("")
    L.append("## Per-epoch composite AUC")
    L.append("")
    L.append("| Epoch | Composite AUC |")
    L.append("|---|---|")
    for ep, a in per_ep.items():
        L.append(f"| {ep} | {a:.3f} |")
    L.append("")

    auc_pass = metrics["auc"] >= 0.60
    beat_single = metrics["auc"] > md_auc
    lift_pass = metrics["lift_top10"] >= 1.5

    L.append("## SOP-12 verdict")
    L.append("")
    L.append(f"- AUC >= 0.60: **{'PASS' if auc_pass else 'FAIL'}** ({metrics['auc']:.3f})")
    L.append(f"- Composite > best-single (ma_dist_60 rank): **{'PASS' if beat_single else 'FAIL'}** ({metrics['auc']:.3f} vs {md_auc:.3f})")
    L.append(f"- Lift top-10% >= 1.5: **{'PASS' if lift_pass else 'FAIL'}** ({metrics['lift_top10']:.2f})")
    L.append("")
    if auc_pass and beat_single and lift_pass:
        L.append("**Overall: PASS** -- rank composite earns its keep.")
    else:
        L.append("**Overall: PARTIAL/FAIL** -- consider single-feature gating instead.")
    L.append("")

    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")

    print(f"[OK] composite metrics -> {OUT_MD.name}")
    print()
    print(f"Composite: AUC={metrics['auc']:.3f}, lift10={metrics['lift_top10']:.2f}, lift5={metrics['lift_top5']:.2f}, prec10={metrics['precision_top10']:.2%}")
    print(f"Single ma_dist_60 rank: AUC={md_auc:.3f}, lift10={md_lift10:.2f}")
    print(f"Per-epoch composite AUC: {per_ep}")


if __name__ == "__main__":
    main()
