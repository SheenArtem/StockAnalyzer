"""
System 3 Phase 3.3 - Composite walk-forward CV (1w-1mo crash predictor).

Trains logistic L2 on 4-feature Track B set:
  vix_term + move_level + ma_dist_60 + rv_20d
Target: forward 21d MDD <= -10%

Walk-forward: expanding window, min_train = 1000 trading days (~4 years).
Re-fit every fit_freq days for compute efficiency.

Baselines (SOP-12):
  - stratified prior (train base rate)
  - majority class (always negative)
  - best-single feature ma_dist_60 quintile
  - B+E composite (read from existing pcr/fgi history if compatible)

Outputs:
  reports/system3_phase33_predictions.parquet
  reports/system3_phase33_summary.md
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, log_loss

ROOT = Path(__file__).resolve().parents[1]
PANEL_PATH = ROOT / "reports" / "system3_panel.parquet"
OUT_PRED = ROOT / "reports" / "system3_phase33_predictions.parquet"
OUT_MD = ROOT / "reports" / "system3_phase33_summary.md"

FEATURES = ["vix_term", "move_level", "ma_dist_60", "rv_20d"]
TARGET = "fwd_21d_mdd_10pct"
MIN_TRAIN = 1000        # ~4 years
FIT_FREQ = 21           # re-fit monthly
RNG = np.random.default_rng(20260509)


def fit_predict_walkforward(panel: pd.DataFrame) -> pd.DataFrame:
    valid = panel[FEATURES + [TARGET]].notna().all(axis=1)
    df = panel[valid].sort_index()
    print(f"[INFO] Walk-forward N={len(df)} from {df.index[0].date()} to {df.index[-1].date()}")

    X_all = df[FEATURES].to_numpy(dtype=float)
    y_all = df[TARGET].to_numpy(dtype=int)
    dates = df.index

    rows = []
    n = len(df)
    model = None
    scaler = None
    last_fit = -1
    for i in range(MIN_TRAIN, n):
        if model is None or (i - last_fit) >= FIT_FREQ:
            X_train = X_all[:i]
            y_train = y_all[:i]
            scaler = StandardScaler().fit(X_train)
            Xt = scaler.transform(X_train)
            # class_weight balanced helps with rare positive class
            model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000,
                                       class_weight="balanced")
            model.fit(Xt, y_train)
            last_fit = i

        x_test = X_all[i:i + 1]
        xs = scaler.transform(x_test)
        p = float(model.predict_proba(xs)[0][list(model.classes_).index(1)])
        rows.append({
            "date": dates[i],
            "true": int(y_all[i]),
            "p_crash": p,
        })

    return pd.DataFrame(rows).set_index("date")


def evaluate(preds: pd.DataFrame) -> dict:
    y = preds["true"].to_numpy(dtype=int)
    p = preds["p_crash"].to_numpy(dtype=float)
    auc = roc_auc_score(y, p)
    p_clip = np.clip(p, 1e-6, 1 - 1e-6)
    ll = log_loss(y, p_clip, labels=[0, 1])

    base = float(y.mean())
    # Lift @ multiple thresholds
    out = {
        "n": len(preds),
        "n_pos": int(y.sum()),
        "baseline": base,
        "auc": float(auc),
        "log_loss": float(ll),
    }
    for top_pct in [0.05, 0.10, 0.20]:
        thr = np.quantile(p, 1 - top_pct)
        mask = p >= thr
        if mask.sum() > 0:
            r = float(y[mask].mean() / base) if base > 0 else np.nan
            out[f"lift_top{int(top_pct * 100)}"] = r
            out[f"precision_top{int(top_pct * 100)}"] = float(y[mask].mean())
        else:
            out[f"lift_top{int(top_pct * 100)}"] = np.nan
            out[f"precision_top{int(top_pct * 100)}"] = np.nan

    # Stability across epochs
    for epoch_label, start, end in [
        ("2011-2014", "2011-01-01", "2015-01-01"),
        ("2015-2019", "2015-01-01", "2020-01-01"),
        ("2020-2026", "2020-01-01", "2027-01-01"),
    ]:
        mask = (preds.index >= pd.Timestamp(start)) & (preds.index < pd.Timestamp(end))
        sub = preds[mask]
        if len(sub) > 50 and sub["true"].sum() > 5:
            try:
                a = roc_auc_score(sub["true"], sub["p_crash"])
                out[f"auc_{epoch_label}"] = float(a)
            except Exception:
                out[f"auc_{epoch_label}"] = np.nan
        else:
            out[f"auc_{epoch_label}"] = np.nan
    return out


def baseline_single_feature(panel: pd.DataFrame) -> pd.DataFrame:
    """ma_dist_60 rolling 252d rank as baseline single-feature score."""
    valid = panel[FEATURES + [TARGET]].notna().all(axis=1)
    df = panel[valid].sort_index()
    md = df["ma_dist_60"]
    # higher rank of -ma_dist = more dangerous
    danger_rank = (-md).rolling(252).rank(pct=True)
    out = pd.DataFrame({
        "date": df.index,
        "true": df[TARGET].astype(int).to_numpy(),
        "p_crash": danger_rank.to_numpy(),
    }).set_index("date").dropna()
    out = out.loc[out.index >= df.index[MIN_TRAIN]]
    return out


def main() -> None:
    panel = pd.read_parquet(PANEL_PATH)
    panel.index = pd.to_datetime(panel.index)

    # Model
    preds = fit_predict_walkforward(panel)
    preds.to_parquet(OUT_PRED)
    metrics_model = evaluate(preds)

    # Baseline single-feat
    base_preds = baseline_single_feature(panel)
    metrics_single = evaluate(base_preds)

    L = []
    L.append("# System 3 Phase 3.3 - Walk-Forward Composite (1w-1mo)")
    L.append("")
    L.append(f"**Features**: {', '.join(FEATURES)}")
    L.append(f"**Target**: {TARGET}")
    L.append(f"**Walk-forward**: expanding window, min_train={MIN_TRAIN}, refit every {FIT_FREQ} days")
    L.append(f"**OOS predictions**: {metrics_model['n']} (positive: {metrics_model['n_pos']}, baseline: {metrics_model['baseline']:.1%})")
    L.append("")
    L.append("## Model vs single-feature baseline")
    L.append("")
    L.append("| Metric | Logistic Composite | Single ma_dist_60 rank | Better |")
    L.append("|---|---|---|---|")
    for k in ["auc", "log_loss", "lift_top5", "lift_top10", "lift_top20",
              "precision_top5", "precision_top10", "precision_top20"]:
        m = metrics_model.get(k, np.nan)
        s = metrics_single.get(k, np.nan)
        if pd.isna(m) or pd.isna(s):
            L.append(f"| {k} | {m:.3f} | {s:.3f} | - |")
            continue
        if k == "log_loss":
            better = "model" if m < s else "single"
        else:
            better = "model" if m > s else "single"
        marker = "✓" if better == "model" else "✗"
        L.append(f"| {k} | {m:.3f} | {s:.3f} | {marker} {better} |")
    L.append("")
    L.append("## Stability across epochs (composite AUC)")
    L.append("")
    L.append("| Epoch | Composite AUC | Single AUC |")
    L.append("|---|---|---|")
    for ep in ["2011-2014", "2015-2019", "2020-2026"]:
        m = metrics_model.get(f"auc_{ep}", np.nan)
        s = metrics_single.get(f"auc_{ep}", np.nan)
        L.append(f"| {ep} | {m:.3f} | {s:.3f} |")
    L.append("")

    # SOP-12 verdict
    auc_pass = metrics_model["auc"] > 0.60
    composite_better = metrics_model["auc"] > metrics_single["auc"]
    lift_pass = metrics_model.get("lift_top10", 0) > 1.5

    L.append("## SOP-12 verdict")
    L.append("")
    L.append(f"- AUC >= 0.60: **{'PASS' if auc_pass else 'FAIL'}** ({metrics_model['auc']:.3f})")
    L.append(f"- Composite > best-single: **{'PASS' if composite_better else 'FAIL'}** ({metrics_model['auc']:.3f} vs {metrics_single['auc']:.3f})")
    L.append(f"- Lift top-10% >= 1.5: **{'PASS' if lift_pass else 'FAIL'}** ({metrics_model.get('lift_top10'):.2f})")
    L.append("")
    if auc_pass and composite_better and lift_pass:
        L.append("**Overall: PASS** -- proceed to Phase 3.4 portfolio gating sim.")
    else:
        L.append("**Overall: PARTIAL/FAIL** -- review.")
    L.append("")

    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")

    print(f"[OK] {len(preds)} OOS predictions -> {OUT_PRED.name}")
    print(f"[OK] summary -> {OUT_MD.name}")
    print()
    print(f"Composite: AUC={metrics_model['auc']:.3f}, lift10={metrics_model.get('lift_top10'):.2f}, lift5={metrics_model.get('lift_top5'):.2f}, log_loss={metrics_model['log_loss']:.3f}")
    print(f"Single (ma_dist_60 rank): AUC={metrics_single['auc']:.3f}, lift10={metrics_single.get('lift_top10'):.2f}")
    print()
    print(f"Per-epoch composite AUC: 2011-14={metrics_model.get('auc_2011-2014'):.3f}, 2015-19={metrics_model.get('auc_2015-2019'):.3f}, 2020-26={metrics_model.get('auc_2020-2026'):.3f}")


if __name__ == "__main__":
    main()
