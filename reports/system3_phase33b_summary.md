# System 3 Phase 3.3b - Rank Composite vs Single Feature

**Composite**: weighted-mean of rolling-252d rank-pct of ['ma_dist_60', 'rv_20d', 'move_level', 'vix_term']
**Weights** (lift-derived, normalized): {'ma_dist_60': 0.28386454183266935, 'rv_20d': 0.21414342629482072, 'move_level': 0.2450199203187251, 'vix_term': 0.2569721115537849}
**Target**: fwd_21d_mdd_10pct
**OOS N**: 3617 (158 positive, 4.4% base)

## Composite vs Single ma_dist_60 rank (SOP-12 critical)

| Metric | Composite | Single ma_dist_60 rank | Δ |
|---|---|---|---|
| AUC | 0.731 | 0.699 | +0.032 |
| Lift top-10% | 1.96 | 3.32 | -1.36 |
| Lift top-5% | 2.53 | (n/a) | - |
| Precision top-10% | 8.56% | (n/a) | - |
| Precision top-5% | 11.05% | (n/a) | - |

## Per-epoch composite AUC

| Epoch | Composite AUC |
|---|---|
| 2011-2014 | 0.812 |
| 2015-2019 | 0.622 |
| 2020-2026 | 0.738 |

## SOP-12 verdict

- AUC >= 0.60: **PASS** (0.731)
- Composite > best-single (ma_dist_60 rank): **PASS** (0.731 vs 0.699)
- Lift top-10% >= 1.5: **PASS** (1.96)

**Overall: PASS** -- rank composite earns its keep.

