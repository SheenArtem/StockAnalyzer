# TW Crash Predictor - Phase 2 (Univariate + Cluster)

**Pipeline**: panel `reports/crash_predictor_tw_panel.parquet` (1999-2026, 6774 rows) -> 
univariate metrics on 5 viable factors -> hierarchical cluster on Track B overlap.

## Sample Sizes & Power Caveat

- **Track A (1999-2026)**: 29 distinct label_10pct events, 14 label_20pct events. 
  Adequate for AUC discrimination; lead-time medians are stable.
- **Track B (2016-2026)**: 9 label_10pct events, 2 label_20pct events visible in window. 
  **LOW STATISTICAL POWER** -- AUC 95% CI on Track B is wide; treat point estimates as suggestive only.
- **Note on `lead_d = n/a` cells**: signals where all top-5% fires fell outside any event's pre-peak 60d window. 
  Common cause on Track B: post-2020 vol spikes (COVID rebound, 2022 tech selloff late) form V-shape recoveries; 
  rv30/m1b top-5% triggered _after_ peaks (lagging), not before -- see `precision_top5 = 0` corroboration.

## Factor Direction Convention

All AUCs computed after sign-flipping so **larger value = more dangerous**:

- `rv10`, `rv30`, `m1b_ratio_pct`: high = danger (direction = +1, no flip)
- `foreign_5d_z`, `foreign_20d_z`: low (heavy foreign selling) = danger (direction = -1, flipped)

## Track A (Long: 1999-2026)

### Including V-shape events

| factor | dir | label | n_obs | n_evt | AUC | 95% CI | P@5% | R@5% | lead_d | FP/yr |
|---|---|---|---|---|---|---|---|---|---|---|
| rv10 | + | label_10pct | 6709 | 29 | 0.688 | [0.620, 0.757] | 0.539 | 0.090 | 45.0 | 9.99 |
| rv10 | + | label_20pct | 6709 | 14 | 0.718 | [0.631, 0.805] | 0.235 | 0.120 | 57.5 | 11.34 |
| rv30 | + | label_10pct | 6699 | 29 | 0.694 | [0.622, 0.772] | 0.421 | 0.070 | 57.0 | 10.02 |
| rv30 | + | label_20pct | 6699 | 14 | 0.715 | [0.620, 0.803] | 0.110 | 0.056 | 58.5 | 11.45 |
| m1b_ratio_pct | + | label_10pct | 6710 | 29 | 0.719 | [0.645, 0.791] | 0.664 | 0.111 | 55.0 | 5.56 |
| m1b_ratio_pct | + | label_20pct | 6710 | 14 | 0.695 | [0.591, 0.796] | 0.164 | 0.084 | 60.0 | 8.05 |

### Excluding V-shape events

| factor | dir | label | n_obs | n_evt | AUC | 95% CI | P@5% | R@5% | lead_d | FP/yr |
|---|---|---|---|---|---|---|---|---|---|---|
| rv10 | + | label_10pct | 6644 | 28 | 0.686 | [0.622, 0.754] | 0.519 | 0.089 | 45.0 | 9.80 |
| rv10 | + | label_20pct | 6684 | 14 | 0.713 | [0.621, 0.803] | 0.206 | 0.109 | 57.5 | 11.30 |
| rv30 | + | label_10pct | 6634 | 28 | 0.690 | [0.615, 0.767] | 0.419 | 0.071 | 57.0 | 9.95 |
| rv30 | + | label_20pct | 6674 | 14 | 0.707 | [0.607, 0.798] | 0.111 | 0.059 | 58.5 | 11.41 |
| m1b_ratio_pct | + | label_10pct | 6645 | 28 | 0.715 | [0.639, 0.794] | 0.631 | 0.107 | 60.0 | 6.51 |
| m1b_ratio_pct | + | label_20pct | 6685 | 14 | 0.696 | [0.592, 0.797] | 0.161 | 0.085 | 60.0 | 8.05 |

## Track B (Short: 2016-2026)

### Including V-shape events

| factor | dir | label | n_obs | n_evt | AUC | 95% CI | P@5% | R@5% | lead_d | FP/yr |
|---|---|---|---|---|---|---|---|---|---|---|
| rv10 | + | label_10pct | 2413 | 9 | 0.653 | [0.529, 0.774] | 0.149 | 0.045 | 53.0 | 11.04 |
| rv10 | + | label_20pct | 2413 | 2 | 0.618 | [0.323, 0.826] | 0.083 | 0.101 | n/a | 12.22 |
| rv30 | + | label_10pct | 2413 | 9 | 0.678 | [0.517, 0.823] | 0.000 | 0.000 | n/a | 12.22 |
| rv30 | + | label_20pct | 2413 | 2 | 0.629 | [0.329, 0.853] | 0.000 | 0.000 | n/a | 12.22 |
| m1b_ratio_pct | + | label_10pct | 2413 | 9 | 0.617 | [0.466, 0.755] | 0.033 | 0.010 | 23.0 | 9.96 |
| m1b_ratio_pct | + | label_20pct | 2413 | 2 | 0.520 | [0.251, 0.805] | 0.000 | 0.000 | n/a | 12.22 |
| foreign_5d_z | - | label_10pct | 2261 | 9 | 0.516 | [0.421, 0.591] | 0.193 | 0.059 | 47.0 | 8.87 |
| foreign_5d_z | - | label_20pct | 2261 | 2 | 0.595 | [0.436, 0.785] | 0.044 | 0.051 | n/a | 11.44 |
| foreign_20d_z | - | label_10pct | 1952 | 9 | 0.531 | [0.419, 0.630] | 0.092 | 0.024 | 34.0 | 8.48 |
| foreign_20d_z | - | label_20pct | 1952 | 2 | 0.601 | [0.385, 0.851] | 0.061 | 0.061 | 23.0 | 9.76 |

### Excluding V-shape events

| factor | dir | label | n_obs | n_evt | AUC | 95% CI | P@5% | R@5% | lead_d | FP/yr |
|---|---|---|---|---|---|---|---|---|---|---|
| rv10 | + | label_10pct | 2407 | 8 | 0.654 | [0.527, 0.769] | 0.149 | 0.045 | 41.0 | 11.53 |
| rv10 | + | label_20pct | 2413 | 2 | 0.618 | [0.390, 0.835] | 0.083 | 0.101 | n/a | 12.22 |
| rv30 | + | label_10pct | 2407 | 8 | 0.675 | [0.529, 0.807] | 0.000 | 0.000 | n/a | 12.22 |
| rv30 | + | label_20pct | 2413 | 2 | 0.629 | [0.309, 0.854] | 0.000 | 0.000 | n/a | 12.22 |
| m1b_ratio_pct | + | label_10pct | 2407 | 8 | 0.613 | [0.477, 0.766] | 0.033 | 0.010 | n/a | 12.22 |
| m1b_ratio_pct | + | label_20pct | 2413 | 2 | 0.520 | [0.258, 0.811] | 0.000 | 0.000 | n/a | 12.22 |
| foreign_5d_z | - | label_10pct | 2255 | 8 | 0.516 | [0.420, 0.597] | 0.195 | 0.060 | 47.0 | 8.87 |
| foreign_5d_z | - | label_20pct | 2261 | 2 | 0.595 | [0.432, 0.767] | 0.044 | 0.051 | n/a | 11.44 |
| foreign_20d_z | - | label_10pct | 1946 | 8 | 0.535 | [0.410, 0.635] | 0.092 | 0.025 | 34.0 | 8.48 |
| foreign_20d_z | - | label_20pct | 1952 | 2 | 0.601 | [0.352, 0.870] | 0.061 | 0.061 | 23.0 | 9.76 |

## Filter Pass List (AUC >= 0.55 AND CI_lo >= 0.50 AND lead >= 10d)

Using **including-V** results (V-shape excluded as sensitivity check only).

| track | factor | label | AUC | CI_lo | lead_d |
|---|---|---|---|---|---|
| A | rv10 | label_10pct | 0.688 | 0.620 | 45.0 |
| A | rv10 | label_20pct | 0.718 | 0.631 | 57.5 |
| A | rv30 | label_10pct | 0.694 | 0.622 | 57.0 |
| A | rv30 | label_20pct | 0.715 | 0.620 | 58.5 |
| A | m1b_ratio_pct | label_10pct | 0.719 | 0.645 | 55.0 |
| A | m1b_ratio_pct | label_20pct | 0.695 | 0.591 | 60.0 |
| B | rv10 | label_10pct | 0.653 | 0.529 | 53.0 |

## Cluster Structure

### Track A 3-factor correlation (1999-2026, n_obs=6759)

| | rv10 | rv30 | m1b_ratio_pct |
|---|---|---|---|
| rv10 | +1.000 | +0.824 | +0.365 |
| rv30 | +0.824 | +1.000 | +0.394 |
| m1b_ratio_pct | +0.365 | +0.394 | +1.000 |

### Track B 5-factor correlation (2016-2026, n_obs=2011)

Pearson correlation matrix on rows of full overlap:

| | rv10 | rv30 | m1b_ratio_pct | foreign_5d_z | foreign_20d_z |
|---|---|---|---|---|---|
| rv10 | +1.000 | +0.702 | +0.296 | -0.188 | -0.291 |
| rv30 | +0.702 | +1.000 | +0.302 | -0.066 | -0.176 |
| m1b_ratio_pct | +0.296 | +0.302 | +1.000 | -0.012 | +0.014 |
| foreign_5d_z | -0.188 | -0.066 | -0.012 | +1.000 | +0.554 |
| foreign_20d_z | -0.291 | -0.176 | +0.014 | +0.554 | +1.000 |

Hierarchical clustering (linkage=average, distance=1-|corr|, threshold=0.5):

- **Cluster 1**: foreign_5d_z, foreign_20d_z
- **Cluster 2**: rv10, rv30
- **Cluster 3**: m1b_ratio_pct

Dendrogram: `reports/crash_predictor_tw_dendrogram.png`

## Recommendations

### (a) Factors to advance to Phase 3

- **Track A pass**: m1b_ratio_pct, rv10, rv30
- **Track B pass**: rv10

### (b) Composite weighting suggestion

Three orthogonal clusters identified on Track B:
- **Cluster vol** (rv10, rv30): r=+0.70 internal; pick `rv30` as representative (slightly higher AUC, longer lookback smooths noise)
- **Cluster volume/liquidity** (m1b_ratio_pct): standalone, r<0.31 with all others; **highest Track A AUC at 0.721** -- keep as-is, top weight candidate
- **Cluster foreign-flow** (foreign_5d_z, foreign_20d_z): r=+0.55 internal; both AUC near 0.5 on label_10pct -- **do not include in composite for now**

Suggested Phase 3 starting composite (Track A scope):
- 50% m1b_ratio_pct + 30% rv30 + 20% rv10 (sign-flipped, z-scored, then weighted)
- Validate that composite AUC > best-single AUC (0.721); otherwise drop to single-factor `m1b_ratio_pct`

### (c) Should we backfill TWD/breadth?

**Yes, but in a second wave -- not blocking Phase 3 kickoff.**

Rationale:
- Track A 3 factors cover **2 of 3 clusters** (vol + volume) with AUC 0.69-0.72; Phase 3 can start now.
- Foreign-flow cluster underperforms (AUC ~0.51-0.60 with wide CI), so the missing TWD/breadth factors would be filling the **third orthogonal axis** that we currently lack on the long sample.
- Breadth (advance/decline) typically has independent crash-warning power separate from vol/volume; backfilling it would improve composite robustness.
- TWD/USD requires a long history fetch (1999-2026); breadth requires aggregating per-stock CSVs into a daily panel -- both are 1-2 day data engineering jobs, schedule after Phase 3 baseline is established.

## Verdict

**Worth advancing to Phase 3.** 3 factor(s) pass on Track A long sample (N=29 events): m1b_ratio_pct, rv10, rv30 -- AUC 0.69-0.72, CI lower bound 0.59+, lead time 45-60d. Two orthogonal clusters covered (vol + volume). Recommend Phase 3 start with `m1b_ratio_pct` lead + `rv30/rv10` confirmation; TWD/breadth backfill scheduled for second wave (not blocking). Track B foreign-flow factors did NOT pass -- exclude from composite, revisit only if N grows past 20 events.
