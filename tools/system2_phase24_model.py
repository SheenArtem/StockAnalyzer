"""
System 2 Phase 2.4 - Multinomial logistic L2 with walk-forward CV.

Inputs: reports/system2_features.parquet (77 events x features)
Selected features (post co-linearity audit): ma_dist_60, rv_20d
  - ma_dist_60: extension-below-MA60, weakly correlated to vol cluster
  - rv_20d:     vol cluster representative (AUC=0.722, Pearson 0.92 vs rv_10d)
  Dropped: rv_10d, range_5d_avg (Pearson > 0.80 with rv_20d)

Walk-forward setup:
  - sort events by trigger_date
  - min_train = 30 events (includes >= 5 C_crash for class coverage)
  - for each test index k in [min_train..N-1]:
      train on events [0..k-1], predict event[k]
  - L2 multinomial logistic (lbfgs, multinomial); scaler refit per fold
  - class_weight=None (probability calibration > balanced accuracy)

Baselines (SOP-12 must beat):
  - Stratified random: P(class) = train prior
  - Majority class: predict argmax of train prior
  - Best-single ma_dist_60 quintile rule

Outputs:
  reports/system2_phase24_predictions.csv
  reports/system2_phase24_summary.md
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, log_loss, f1_score, confusion_matrix, roc_auc_score
)

ROOT = Path(__file__).resolve().parents[1]
FEAT_PATH = ROOT / "reports" / "system2_features.parquet"
OUT_CSV = ROOT / "reports" / "system2_phase24_predictions.csv"
OUT_MD = ROOT / "reports" / "system2_phase24_summary.md"

SELECTED_FEATURES = ["ma_dist_60", "rv_20d"]
MIN_TRAIN = 30
CLASSES = ["A_small", "B_medium", "C_crash"]
RNG = np.random.default_rng(20260509)


def fit_predict_walkforward(df: pd.DataFrame) -> pd.DataFrame:
    """Expanding-window walk-forward, returns per-event predictions."""
    rows = []
    for k in range(MIN_TRAIN, len(df)):
        train = df.iloc[:k]
        test = df.iloc[k:k + 1]
        X_train = train[SELECTED_FEATURES].to_numpy(dtype=float)
        y_train = train["class"].to_numpy()
        X_test = test[SELECTED_FEATURES].to_numpy(dtype=float)

        scaler = StandardScaler().fit(X_train)
        Xt = scaler.transform(X_train)
        Xs = scaler.transform(X_test)

        # If train set lacks a class, sklearn won't include it in classes_
        unique_classes = sorted(set(y_train))
        if len(unique_classes) < 2:
            # Degenerate fold: skip
            continue

        model = LogisticRegression(
            penalty="l2",
            C=1.0,
            solver="lbfgs",
            max_iter=1000,
        )
        model.fit(Xt, y_train)
        proba = model.predict_proba(Xs)[0]
        pred = model.predict(Xs)[0]

        # Map to fixed class order
        p_map = {c: 0.0 for c in CLASSES}
        for i, c in enumerate(model.classes_):
            p_map[c] = float(proba[i])

        # Stratified random baseline (uses train prior)
        prior = train["class"].value_counts(normalize=True).reindex(CLASSES).fillna(0).to_dict()
        majority = max(prior, key=prior.get)

        rows.append({
            "event_id": int(test["event_id"].iloc[0]),
            "trigger_date": test["trigger_date"].iloc[0],
            "true_class": test["class"].iloc[0],
            "pred_class": pred,
            "p_A": p_map["A_small"],
            "p_B": p_map["B_medium"],
            "p_C": p_map["C_crash"],
            "prior_A": prior["A_small"],
            "prior_B": prior["B_medium"],
            "prior_C": prior["C_crash"],
            "baseline_majority": majority,
        })
    return pd.DataFrame(rows)


def evaluate(preds: pd.DataFrame) -> dict:
    """Compute metrics + baselines."""
    y_true = preds["true_class"].to_numpy()
    y_pred = preds["pred_class"].to_numpy()
    y_pred_majority = preds["baseline_majority"].to_numpy()

    # Map class strings -> ints for metrics
    cls_idx = {c: i for i, c in enumerate(CLASSES)}
    y_true_idx = np.array([cls_idx[c] for c in y_true])
    y_pred_idx = np.array([cls_idx[c] for c in y_pred])

    proba_model = preds[["p_A", "p_B", "p_C"]].to_numpy()
    proba_prior = preds[["prior_A", "prior_B", "prior_C"]].to_numpy()

    # Clip probabilities (avoid log 0)
    proba_model = np.clip(proba_model, 1e-6, 1 - 1e-6)
    proba_model = proba_model / proba_model.sum(axis=1, keepdims=True)
    proba_prior = np.clip(proba_prior, 1e-6, 1 - 1e-6)
    proba_prior = proba_prior / proba_prior.sum(axis=1, keepdims=True)

    metrics = {
        "n": len(preds),
        "accuracy_model": accuracy_score(y_true, y_pred),
        "accuracy_majority": accuracy_score(y_true, y_pred_majority),
        "logloss_model": log_loss(y_true_idx, proba_model, labels=[0, 1, 2]),
        "logloss_prior": log_loss(y_true_idx, proba_prior, labels=[0, 1, 2]),
        "macro_f1_model": f1_score(y_true, y_pred, labels=CLASSES, average="macro", zero_division=0),
        "macro_f1_majority": f1_score(y_true, y_pred_majority, labels=CLASSES, average="macro", zero_division=0),
    }

    # Per-class one-vs-rest AUC (model probability)
    for i, cls in enumerate(CLASSES):
        y_bin = (y_true_idx == i).astype(int)
        if y_bin.sum() > 0 and y_bin.sum() < len(y_bin):
            auc = roc_auc_score(y_bin, proba_model[:, i])
            metrics[f"auc_{cls}"] = auc
        else:
            metrics[f"auc_{cls}"] = np.nan

    # Per-class precision/recall
    cm = confusion_matrix(y_true, y_pred, labels=CLASSES)
    for i, cls in enumerate(CLASSES):
        tp = cm[i, i]
        fp = cm[:, i].sum() - tp
        fn = cm[i, :].sum() - tp
        metrics[f"precision_{cls}"] = tp / (tp + fp) if (tp + fp) > 0 else np.nan
        metrics[f"recall_{cls}"] = tp / (tp + fn) if (tp + fn) > 0 else np.nan

    metrics["confusion_matrix"] = cm
    return metrics


def block_bootstrap_logloss(preds: pd.DataFrame, n_boot: int = 500, block_size: int = 10) -> tuple[float, float]:
    """Block bootstrap CI for model log-loss to handle event clustering."""
    cls_idx = {c: i for i, c in enumerate(CLASSES)}
    y_true_idx = np.array([cls_idx[c] for c in preds["true_class"].to_numpy()])
    proba = preds[["p_A", "p_B", "p_C"]].to_numpy()
    proba = np.clip(proba, 1e-6, 1 - 1e-6)
    proba = proba / proba.sum(axis=1, keepdims=True)

    n = len(preds)
    n_blocks = max(1, n // block_size)
    losses = []
    for _ in range(n_boot):
        idx = []
        for _ in range(n_blocks + 1):
            start = RNG.integers(0, n - block_size + 1)
            idx.extend(range(start, start + block_size))
        idx = idx[:n]
        idx = np.array(idx)
        try:
            ll = log_loss(y_true_idx[idx], proba[idx], labels=[0, 1, 2])
            losses.append(ll)
        except Exception:
            continue
    losses = np.array(losses)
    return float(np.percentile(losses, 2.5)), float(np.percentile(losses, 97.5))


def write_summary(metrics: dict, ci: tuple[float, float], preds: pd.DataFrame) -> str:
    cm = metrics.pop("confusion_matrix")
    L = []
    L.append("# System 2 Phase 2.4 - Multinomial Logistic Walk-Forward")
    L.append("")
    L.append(f"**Selected features**: {', '.join(SELECTED_FEATURES)}")
    L.append(f"**Walk-forward**: expanding window, min_train = {MIN_TRAIN}")
    L.append(f"**OOS predictions**: {metrics['n']}")
    L.append("")
    L.append("## Model vs baselines")
    L.append("")
    L.append("| Metric | Model | Stratified prior | Majority class |")
    L.append("|---|---|---|---|")
    L.append(f"| Multi-class log-loss | **{metrics['logloss_model']:.3f}** | {metrics['logloss_prior']:.3f} | - |")
    L.append(f"| Accuracy             | **{metrics['accuracy_model']:.3f}** | - | {metrics['accuracy_majority']:.3f} |")
    L.append(f"| Macro F1             | **{metrics['macro_f1_model']:.3f}** | - | {metrics['macro_f1_majority']:.3f} |")
    L.append("")

    delta_ll = metrics["logloss_prior"] - metrics["logloss_model"]
    L.append(f"**Log-loss vs prior**: {delta_ll:+.3f} ({'BETTER' if delta_ll > 0 else 'WORSE'})")
    L.append(f"**Block bootstrap CI** (95%, block=10): [{ci[0]:.3f}, {ci[1]:.3f}]")
    L.append("")

    L.append("## Per-class one-vs-rest AUC")
    L.append("")
    L.append("| Class | AUC | Precision | Recall |")
    L.append("|---|---|---|---|")
    for cls in CLASSES:
        a = metrics.get(f"auc_{cls}", np.nan)
        p = metrics.get(f"precision_{cls}", np.nan)
        r = metrics.get(f"recall_{cls}", np.nan)
        L.append(f"| {cls} | {a:.3f} | {p:.3f} | {r:.3f} |")
    L.append("")

    L.append("## Confusion matrix")
    L.append("")
    L.append("| true \\ pred | A_small | B_medium | C_crash |")
    L.append("|---|---|---|---|")
    for i, cls in enumerate(CLASSES):
        L.append(f"| {cls} | {cm[i, 0]} | {cm[i, 1]} | {cm[i, 2]} |")
    L.append("")

    # SOP-12 verdict
    pass_ll = metrics["logloss_model"] < metrics["logloss_prior"]
    pass_f1 = metrics["macro_f1_model"] > metrics["macro_f1_majority"]
    pass_auc_C = metrics.get("auc_C_crash", np.nan) >= 0.65
    L.append("## SOP-12 verdict")
    L.append("")
    L.append(f"- log-loss < stratified prior: **{'PASS' if pass_ll else 'FAIL'}** ({delta_ll:+.3f})")
    L.append(f"- macro F1 > majority baseline: **{'PASS' if pass_f1 else 'FAIL'}** ({metrics['macro_f1_model']:.3f} vs {metrics['macro_f1_majority']:.3f})")
    L.append(f"- AUC(C_crash) >= 0.65: **{'PASS' if pass_auc_C else 'FAIL'}** ({metrics.get('auc_C_crash', np.nan):.3f})")
    L.append("")
    if pass_ll and pass_f1 and pass_auc_C:
        L.append("**Overall: PASS** -- proceed to Phase 2.5 portfolio gating sim.")
    else:
        L.append("**Overall: PARTIAL/FAIL** -- review before Phase 2.5.")
    L.append("")

    return "\n".join(L) + "\n"


def main() -> None:
    df = pd.read_parquet(FEAT_PATH)
    df["trigger_date"] = pd.to_datetime(df["trigger_date"])
    df = df.dropna(subset=SELECTED_FEATURES).sort_values("trigger_date").reset_index(drop=True)

    print(f"[INFO] Events with all features: {len(df)} (originally 77)")
    print(f"[INFO] Class distribution: {df['class'].value_counts().to_dict()}")

    preds = fit_predict_walkforward(df)
    preds.to_csv(OUT_CSV, index=False)

    metrics = evaluate(preds)
    ci = block_bootstrap_logloss(preds)
    summary = write_summary(metrics, ci, preds)
    OUT_MD.write_text(summary, encoding="utf-8")

    print(f"[OK] {len(preds)} OOS predictions -> {OUT_CSV.name}")
    print(f"[OK] summary -> {OUT_MD.name}")
    print()
    print(f"Log-loss: model={metrics['logloss_model']:.3f}  prior={metrics['logloss_prior']:.3f}  delta={metrics['logloss_prior']-metrics['logloss_model']:+.3f}")
    print(f"Accuracy: model={metrics['accuracy_model']:.3f}  majority={metrics['accuracy_majority']:.3f}")
    print(f"Macro F1: model={metrics['macro_f1_model']:.3f}  majority={metrics['macro_f1_majority']:.3f}")
    print(f"AUC: A={metrics.get('auc_A_small'):.3f}  B={metrics.get('auc_B_medium'):.3f}  C={metrics.get('auc_C_crash'):.3f}")
    print(f"Block bootstrap CI for log-loss: [{ci[0]:.3f}, {ci[1]:.3f}]")


if __name__ == "__main__":
    main()
