"""
System 2 Phase 2.3 - Per-feature univariate lift / AUC vs Class C.

For each feature, evaluate ability to separate Class C (deep crash) from
Class A+B (small/medium reversal):
  - AUC vs binary label (1=C_crash, 0=else)
  - Lift @ top-20% (P(C | top-quintile by danger) / baseline P(C))
  - Class median + spread monotonicity check
  - N coverage

Filter rule (spec): keep features with abs(AUC-0.5) >= 0.10 AND lift >= 1.3.

Outputs:
  reports/system2_univariate.csv
  reports/system2_univariate_summary.md
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FEAT_PATH = ROOT / "reports" / "system2_features.parquet"
OUT_CSV = ROOT / "reports" / "system2_univariate.csv"
OUT_MD = ROOT / "reports" / "system2_univariate_summary.md"

# Feature direction: +1 means "higher = more dangerous" (more crash-like)
# -1 means "lower = more dangerous"
# 0 means undefined / two-sided (skip directional AUC)
DIRECTION = {
    "velocity_5d":       -1,   # more negative = bigger drop = more danger
    "velocity_20d":      -1,
    "vol_ratio_20d":     +1,   # higher volume = panic
    "rsi14":             -1,   # lower RSI = oversold = more danger
    "ma_dist_20":        -1,   # more below MA = danger
    "ma_dist_60":        -1,
    "rv_10d":            +1,   # higher vol = danger
    "rv_20d":            +1,
    "range_5d_avg":      +1,   # bigger range = panic
    "gap_open":          -1,   # gap down = danger
    "foreign_5d_sum":    -1,   # more negative foreign net = panic selling
    "foreign_20d_sum":   -1,
    "foreign_5d_z":      -1,   # more negative z = extreme outflow
    "trust_5d_sum":      -1,
    "dealer_5d_sum":     -1,
    "inst_total_5d_sum": -1,
    "inst_total_5d_z":   -1,
}

EXCLUDE = {"event_id", "trigger_date", "class", "ma20", "ma60", "close"}


def auc_score(scores: np.ndarray, labels: np.ndarray) -> float:
    """Compute AUC where higher score should mean label=1.

    Uses Mann-Whitney U formulation; handles ties.
    """
    pos_scores = scores[labels == 1]
    neg_scores = scores[labels == 0]
    if len(pos_scores) == 0 or len(neg_scores) == 0:
        return np.nan
    n_pos = len(pos_scores)
    n_neg = len(neg_scores)
    # Rank-based AUC
    all_scores = np.concatenate([pos_scores, neg_scores])
    ranks = pd.Series(all_scores).rank(method="average").values
    rank_sum_pos = ranks[:n_pos].sum()
    auc = (rank_sum_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)
    return float(auc)


def lift_top_quintile(scores: np.ndarray, labels: np.ndarray, top_pct: float = 0.20) -> float:
    if len(scores) == 0:
        return np.nan
    baseline = labels.mean()
    if baseline == 0:
        return np.nan
    threshold = np.quantile(scores, 1 - top_pct)
    top_mask = scores >= threshold
    if top_mask.sum() == 0:
        return np.nan
    top_rate = labels[top_mask].mean()
    return top_rate / baseline


def evaluate_feature(feat_col: pd.Series, labels: pd.Series, direction: int) -> dict:
    valid = feat_col.notna() & labels.notna()
    fv = feat_col[valid].to_numpy(dtype=float)
    lv = labels[valid].to_numpy(dtype=int)
    n = len(fv)
    n_pos = int(lv.sum())
    if n < 10 or n_pos < 3:
        return {"n": n, "n_pos": n_pos, "auc": np.nan, "auc_aligned": np.nan, "lift_q5": np.nan}

    score = direction * fv  # so larger score = predicted danger
    auc = auc_score(score, lv)
    lift = lift_top_quintile(score, lv)
    return {
        "n": n,
        "n_pos": n_pos,
        "baseline": lv.mean(),
        "auc": auc_score(fv, lv),    # raw AUC (no direction)
        "auc_aligned": auc,           # direction-aligned AUC (>0.5 = good)
        "lift_q5": lift,
    }


def class_medians(feat_col: pd.Series, classes: pd.Series) -> dict:
    out = {}
    for cls in ["A_small", "B_medium", "C_crash"]:
        v = feat_col[classes == cls]
        out[cls] = float(v.median()) if v.notna().any() else np.nan
    return out


def is_monotone(values: dict, direction: int) -> str:
    a = values["A_small"]
    b = values["B_medium"]
    c = values["C_crash"]
    if any(pd.isna([a, b, c])):
        return "?"
    if direction > 0:
        return "Y" if a < b < c else "N"
    if direction < 0:
        return "Y" if a > b > c else "N"
    return "?"


def main() -> None:
    fdf = pd.read_parquet(FEAT_PATH)
    labels_c = (fdf["class"] == "C_crash").astype(int)

    feature_cols = [c for c in fdf.columns if c not in EXCLUDE]

    rows = []
    for col in feature_cols:
        d = DIRECTION.get(col, 0)
        if d == 0:
            continue
        stats = evaluate_feature(fdf[col], labels_c, d)
        meds = class_medians(fdf[col], fdf["class"])
        rows.append({
            "feature": col,
            "direction": d,
            **stats,
            "med_A": meds["A_small"],
            "med_B": meds["B_medium"],
            "med_C": meds["C_crash"],
            "monotone": is_monotone(meds, d),
        })

    res = pd.DataFrame(rows)
    res = res.sort_values("auc_aligned", ascending=False).reset_index(drop=True)

    # Apply filter
    res["pass_filter"] = (
        (res["auc_aligned"] >= 0.60) & (res["lift_q5"] >= 1.3)
    )

    res.to_csv(OUT_CSV, index=False)

    # Build markdown summary
    lines = [
        "# System 2 Phase 2.3 - Univariate Feature Evaluation",
        "",
        f"**Target**: Class C (deep crash, drawdown <= -20%) vs Class A+B",
        f"**Total events**: {len(fdf)}",
        f"**Class C**: {int(labels_c.sum())} / {len(labels_c)} (baseline = {labels_c.mean():.1%})",
        "",
        "## Feature ranking by direction-aligned AUC",
        "",
        "Filter: `auc_aligned >= 0.60 AND lift_q5 >= 1.3`",
        "",
        "| Feature | Dir | N | AUC | Lift@20% | Mono | med_A | med_B | med_C | PASS |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]

    def fmt(v, p=4):
        if pd.isna(v):
            return "-"
        if abs(v) >= 1e6:
            return f"{v:.2e}"
        return f"{v:.{p}f}"

    for _, r in res.iterrows():
        lines.append(
            f"| {r['feature']} | {'+' if r['direction']>0 else '-'} | {r['n']} | "
            f"{fmt(r['auc_aligned'], 3)} | {fmt(r['lift_q5'], 2)} | {r['monotone']} | "
            f"{fmt(r['med_A'])} | {fmt(r['med_B'])} | {fmt(r['med_C'])} | "
            f"{'PASS' if r['pass_filter'] else ''} |"
        )

    pass_n = int(res["pass_filter"].sum())
    lines += [
        "",
        f"## Summary: {pass_n} / {len(res)} features pass filter",
        "",
    ]

    # Group by full-history vs limited-history
    full_history = res[res["n"] == len(fdf)]
    limited_history = res[res["n"] < len(fdf)]
    lines += [
        f"- Full-history features (1999+): {len(full_history)}, pass = {int(full_history['pass_filter'].sum())}",
        f"- Limited-history features (2015+, N={limited_history['n'].iloc[0] if len(limited_history) else 0}): {len(limited_history)}, pass = {int(limited_history['pass_filter'].sum())}",
        "",
        "## Top 5 features (by AUC)",
        "",
    ]
    for _, r in res.head(5).iterrows():
        lines.append(
            f"- **{r['feature']}**: AUC={r['auc_aligned']:.3f}, lift={r['lift_q5']:.2f}, "
            f"N={r['n']}, mono={r['monotone']}, "
            f"med A/B/C = {fmt(r['med_A'])} / {fmt(r['med_B'])} / {fmt(r['med_C'])}"
        )

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[OK] {len(res)} features evaluated -> {OUT_CSV.name}")
    print(f"[OK] {pass_n} pass filter (auc>=0.60 AND lift>=1.3)")
    print(f"[OK] summary -> {OUT_MD.name}")
    print()
    print("Top 8 by AUC:")
    print(res[["feature", "n", "auc_aligned", "lift_q5", "monotone", "pass_filter"]].head(8).to_string(index=False))


if __name__ == "__main__":
    main()
