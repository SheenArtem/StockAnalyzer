# System 3 Phase 3.3 - Walk-Forward Composite (1w-1mo)

**Features**: vix_term, move_level, ma_dist_60, rv_20d
**Target**: fwd_21d_mdd_10pct
**Walk-forward**: expanding window, min_train=1000, refit every 21 days
**OOS predictions**: 3868 (positive: 171, baseline: 4.4%)

## Model vs single-feature baseline

| Metric | Logistic Composite | Single ma_dist_60 rank | Better |
|---|---|---|---|
| auc | 0.679 | 0.714 | ✗ single |
| log_loss | 0.487 | 1.111 | ✓ model |
| lift_top5 | 2.798 | 3.884 | ✗ single |
| lift_top10 | 2.338 | 3.353 | ✗ single |
| lift_top20 | 2.367 | 2.416 | ✗ single |
| precision_top5 | 0.124 | 0.172 | ✗ single |
| precision_top10 | 0.103 | 0.148 | ✗ single |
| precision_top20 | 0.105 | 0.107 | ✗ single |

## Stability across epochs (composite AUC)

| Epoch | Composite AUC | Single AUC |
|---|---|---|
| 2011-2014 | 0.840 | 0.704 |
| 2015-2019 | 0.462 | 0.817 |
| 2020-2026 | 0.638 | 0.681 |

## SOP-12 verdict

- AUC >= 0.60: **PASS** (0.679)
- Composite > best-single: **FAIL** (0.679 vs 0.714)
- Lift top-10% >= 1.5: **PASS** (2.34)

**Overall: PARTIAL/FAIL** -- review.

