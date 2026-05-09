"""
System 3 Phase 3.2 - Univariate AUC + lift on daily panel.

For each feature, evaluate vs fwd_21d_mdd_10pct (primary) and fwd_5d_mdd_5pct (secondary):
  - AUC vs binary label
  - Lift @ top-10% (P(crash | top-decile by danger) / baseline)
  - Direction monotonicity check
  - Per-epoch lift (1999-2007 / 2008-2014 / 2015-2026) -- regime stability check

Filter rule: pass if AUC>=0.60 AND lift_top10>=1.5 AND lift stable across all 3 epochs.

Outputs:
  reports/system3_univariate.csv
  reports/system3_univariate_summary.md
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PANEL_PATH = ROOT / "reports" / "system3_panel.parquet"
OUT_CSV = ROOT / "reports" / "system3_univariate.csv"
OUT_MD = ROOT / "reports" / "system3_univariate_summary.md"

# Feature direction: +1 = high value is dangerous; -1 = low value is dangerous
DIRECTION = {
    "ret_5d":         -1,   # more negative ret = recent drop
    "ret_20d":        -1,
    "rv_10d":         +1,   # high vol = stress
    "rv_20d":         +1,
    "vol_ratio_20d":  +1,   # volume spike
    "rsi14":          -1,   # low rsi = oversold (could mean continuation)
    "ma_dist_20":     -1,   # below MA20 = weak
    "ma_dist_60":     -1,   # below MA60 = weak
    "range_5d_avg":   +1,   # wide range = panic
    "gap_open":       -1,   # gap down
    "vix_level":      +1,
    "vix_5d_chg":     +1,
    "vix_term":       +1,   # VIX > VIX3M = curve inverted = acute fear
    "vix3m_level":    +1,
    "move_level":     +1,
    "move_5d_chg":    +1,
    "foreign_5d_z":   -1,   # more negative foreign net = panic outflow
    "inst_5d_z":      -1,
}

EXCLUDE = {"close", "volume", "fwd_5d_mdd", "fwd_21d_mdd",
           "fwd_5d_mdd_5pct", "fwd_5d_mdd_10pct", "fwd_21d_mdd_10pct", "fwd_21d_mdd_15pct"}

EPOCHS = {
    "1999-2007": (pd.Timestamp("1999-01-01"), pd.Timestamp("2008-01-01")),
    "2008-2014": (pd.Timestamp("2008-01-01"), pd.Timestamp("2015-01-01")),
    "2015-2026": (pd.Timestamp("2015-01-01"), pd.Timestamp("2027-01-01")),
}


def auc(scores: np.ndarray, labels: np.ndarray) -> float:
    pos = scores[labels == 1]
    neg = scores[labels == 0]
    if len(pos) == 0 or len(neg) == 0:
        return np.nan
    all_s = np.concatenate([pos, neg])
    ranks = pd.Series(all_s).rank(method="average").to_numpy()
    rank_pos_sum = ranks[: len(pos)].sum()
    return float((rank_pos_sum - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def lift_top(scores: np.ndarray, labels: np.ndarray, top_pct: float) -> float:
    if len(scores) == 0:
        return np.nan
    base = labels.mean()
    if base == 0:
        return np.nan
    thr = np.quantile(scores, 1 - top_pct)
    mask = scores >= thr
    if mask.sum() == 0:
        return np.nan
    return float(labels[mask].mean() / base)


def evaluate(panel: pd.DataFrame, feat: str, target: str, direction: int) -> dict:
    valid = panel[feat].notna() & panel[target].notna()
    s = panel.loc[valid, feat].to_numpy(dtype=float)
    y = panel.loc[valid, target].to_numpy(dtype=int)
    n = len(s)
    n_pos = int(y.sum())
    if n < 200 or n_pos < 20:
        return {"feature": feat, "target": target, "n": n, "n_pos": n_pos,
                "auc": np.nan, "lift_top10": np.nan, "lift_top5": np.nan}

    score = direction * s
    return {
        "feature": feat,
        "target": target,
        "direction": direction,
        "n": n,
        "n_pos": n_pos,
        "baseline": float(y.mean()),
        "auc": auc(score, y),
        "lift_top10": lift_top(score, y, 0.10),
        "lift_top5": lift_top(score, y, 0.05),
    }


def evaluate_per_epoch(panel: pd.DataFrame, feat: str, target: str, direction: int) -> dict:
    out = {}
    for ep, (start, end) in EPOCHS.items():
        mask = (panel.index >= start) & (panel.index < end)
        sub = panel[mask]
        if sub[feat].notna().sum() < 100 or sub[target].sum() < 5:
            out[f"lift10_{ep}"] = np.nan
            continue
        valid = sub[feat].notna() & sub[target].notna()
        s = sub.loc[valid, feat].to_numpy(dtype=float)
        y = sub.loc[valid, target].to_numpy(dtype=int)
        out[f"lift10_{ep}"] = lift_top(direction * s, y, 0.10)
    return out


def main() -> None:
    panel = pd.read_parquet(PANEL_PATH)
    feature_cols = [c for c in panel.columns if c not in EXCLUDE]

    primary = "fwd_21d_mdd_10pct"
    secondary = "fwd_5d_mdd_5pct"

    rows = []
    for feat in feature_cols:
        d = DIRECTION.get(feat, 0)
        if d == 0:
            continue
        # Primary
        r1 = evaluate(panel, feat, primary, d)
        per_epoch = evaluate_per_epoch(panel, feat, primary, d)
        r1.update(per_epoch)
        # Stability check: lift10 across 3 epochs
        lifts = [v for k, v in per_epoch.items() if not pd.isna(v)]
        if len(lifts) >= 2:
            r1["lift_min_epoch"] = float(min(lifts))
            r1["lift_max_epoch"] = float(max(lifts))
            r1["lift_stable"] = "Y" if r1["lift_min_epoch"] >= 1.0 else "N"
        else:
            r1["lift_min_epoch"] = np.nan
            r1["lift_max_epoch"] = np.nan
            r1["lift_stable"] = "?"
        # Secondary AUC for context
        r2 = evaluate(panel, feat, secondary, d)
        r1["auc_5d_5pct"] = r2.get("auc", np.nan)
        r1["lift10_5d_5pct"] = r2.get("lift_top10", np.nan)
        rows.append(r1)

    res = pd.DataFrame(rows).sort_values("auc", ascending=False).reset_index(drop=True)
    res["pass_filter"] = (
        (res["auc"] >= 0.60) & (res["lift_top10"] >= 1.5) & (res["lift_stable"] == "Y")
    )
    res.to_csv(OUT_CSV, index=False)

    L = []
    L.append("# System 3 Phase 3.2 - Univariate Filter (1w-1mo crash predictor)")
    L.append("")
    L.append(f"**Primary target**: forward 21d MDD <= -10% (baseline {res['baseline'].iloc[0] * 100 if len(res) else 0:.1f}%)")
    L.append(f"**Secondary target**: forward 5d MDD <= -5%")
    L.append(f"**Filter**: AUC >= 0.60 AND lift_top10 >= 1.5 AND stable across 3 epochs")
    L.append("")
    L.append("## Feature ranking (sorted by AUC vs primary target)")
    L.append("")
    L.append("| Feature | Dir | N | AUC | Lift10 | Lift5 | Stable | 99-07 | 08-14 | 15-26 | AUC(5d5%) | PASS |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    def fmt(v, p=3):
        if pd.isna(v):
            return "-"
        return f"{v:.{p}f}"
    for _, r in res.iterrows():
        L.append(
            f"| {r['feature']} | {'+' if r.get('direction', 0) > 0 else '-'} | {r['n']} | "
            f"{fmt(r['auc'])} | {fmt(r['lift_top10'], 2)} | {fmt(r['lift_top5'], 2)} | {r['lift_stable']} | "
            f"{fmt(r.get('lift10_1999-2007'), 2)} | {fmt(r.get('lift10_2008-2014'), 2)} | {fmt(r.get('lift10_2015-2026'), 2)} | "
            f"{fmt(r['auc_5d_5pct'])} | {'PASS' if r['pass_filter'] else ''} |"
        )

    pass_n = int(res["pass_filter"].sum())
    L += [
        "",
        f"## Summary: {pass_n} / {len(res)} features pass filter",
        "",
        "Top 5 by AUC:",
        "",
    ]
    for _, r in res.head(5).iterrows():
        L.append(f"- **{r['feature']}** AUC={r['auc']:.3f} lift10={r['lift_top10']:.2f} (epochs: 99-07={fmt(r.get('lift10_1999-2007'), 2)} / 08-14={fmt(r.get('lift10_2008-2014'), 2)} / 15-26={fmt(r.get('lift10_2015-2026'), 2)})")

    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")

    print(f"[OK] {len(res)} features evaluated -> {OUT_CSV.name}")
    print(f"[OK] {pass_n} pass filter")
    print(f"[OK] summary -> {OUT_MD.name}")
    print()
    print("Top 8 by AUC:")
    print(res[["feature", "n", "auc", "lift_top10", "lift_min_epoch", "lift_stable", "pass_filter"]].head(8).to_string(index=False))


if __name__ == "__main__":
    main()
