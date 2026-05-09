# System 2 Phase 2.4 - Multinomial Logistic Walk-Forward

**Selected features**: ma_dist_60, rv_20d
**Walk-forward**: expanding window, min_train = 30
**OOS predictions**: 46

## Model vs baselines

| Metric | Model | Stratified prior | Majority class |
|---|---|---|---|
| Multi-class log-loss | **0.947** | 1.106 | - |
| Accuracy             | **0.478** | - | 0.326 |
| Macro F1             | **0.425** | - | 0.303 |

**Log-loss vs prior**: +0.159 (BETTER)
**Block bootstrap CI** (95%, block=10): [0.886, 1.024]

## Per-class one-vs-rest AUC

| Class | AUC | Precision | Recall |
|---|---|---|---|
| A_small | 0.705 | 0.516 | 0.762 |
| B_medium | 0.581 | 0.300 | 0.188 |
| C_crash | 0.733 | 0.600 | 0.333 |

## Confusion matrix

| true \ pred | A_small | B_medium | C_crash |
|---|---|---|---|
| A_small | 16 | 5 | 0 |
| B_medium | 11 | 3 | 2 |
| C_crash | 4 | 2 | 3 |

## SOP-12 verdict

- log-loss < stratified prior: **PASS** (+0.159)
- macro F1 > majority baseline: **PASS** (0.425 vs 0.303)
- AUC(C_crash) >= 0.65: **PASS** (0.733)

**Overall: PASS** -- proceed to Phase 2.5 portfolio gating sim.

