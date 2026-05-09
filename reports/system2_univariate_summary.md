# System 2 Phase 2.3 - Univariate Feature Evaluation

**Target**: Class C (deep crash, drawdown <= -20%) vs Class A+B
**Total events**: 77
**Class C**: 20 / 77 (baseline = 26.0%)

## Feature ranking by direction-aligned AUC

Filter: `auc_aligned >= 0.60 AND lift_q5 >= 1.3`

| Feature | Dir | N | AUC | Lift@20% | Mono | med_A | med_B | med_C | PASS |
|---|---|---|---|---|---|---|---|---|---|
| ma_dist_60 | - | 76 | 0.731 | 2.38 | Y | -0.0110 | -0.0137 | -0.0411 | PASS |
| rv_20d | + | 76 | 0.722 | 1.90 | Y | 0.1582 | 0.1965 | 0.2365 | PASS |
| rv_10d | + | 77 | 0.662 | 1.68 | Y | 0.1588 | 0.2134 | 0.2383 | PASS |
| range_5d_avg | + | 77 | 0.660 | 1.68 | Y | 0.0126 | 0.0160 | 0.0189 | PASS |
| ma_dist_20 | - | 76 | 0.572 | 1.66 | N | -0.0304 | -0.0264 | -0.0307 |  |
| velocity_20d | - | 76 | 0.548 | 1.19 | N | -0.0308 | -0.0342 | -0.0317 |  |
| velocity_5d | - | 77 | 0.547 | 1.20 | N | -0.0332 | -0.0278 | -0.0358 |  |
| rsi14 | - | 77 | 0.521 | 0.96 | N | 39.0633 | 42.1167 | 40.2688 |  |
| vol_ratio_20d | + | 76 | 0.481 | 0.71 | N | 0.9752 | 1.0484 | 0.9732 |  |
| dealer_5d_sum | - | 26 | 0.400 | 0.00 | N | -9.16e+07 | -7.80e+07 | -8.73e+07 |  |
| trust_5d_sum | - | 26 | 0.400 | 0.87 | N | 5.42e+06 | -5.94e+06 | 7.68e+07 |  |
| foreign_5d_z | - | 26 | 0.390 | 0.00 | N | -2.3144 | -1.3455 | -0.7786 |  |
| gap_open | - | 77 | 0.377 | 0.72 | N | -0.0030 | -0.0031 | -0.0009 |  |
| inst_total_5d_z | - | 26 | 0.371 | 0.00 | N | -2.2143 | -1.3803 | -0.1690 |  |
| foreign_20d_sum | - | 26 | 0.343 | 0.00 | N | -1.63e+09 | -1.12e+09 | -5.26e+08 |  |
| foreign_5d_sum | - | 26 | 0.333 | 0.00 | N | -7.21e+08 | -7.01e+08 | -3.38e+08 |  |
| inst_total_5d_sum | - | 26 | 0.305 | 0.00 | N | -1.03e+09 | -7.63e+08 | -4.64e+08 |  |

## Summary: 4 / 17 features pass filter

- Full-history features (1999+): 5, pass = 2
- Limited-history features (2015+, N=76): 12, pass = 2

## Top 5 features (by AUC)

- **ma_dist_60**: AUC=0.731, lift=2.38, N=76, mono=Y, med A/B/C = -0.0110 / -0.0137 / -0.0411
- **rv_20d**: AUC=0.722, lift=1.90, N=76, mono=Y, med A/B/C = 0.1582 / 0.1965 / 0.2365
- **rv_10d**: AUC=0.662, lift=1.68, N=77, mono=Y, med A/B/C = 0.1588 / 0.2134 / 0.2383
- **range_5d_avg**: AUC=0.660, lift=1.68, N=77, mono=Y, med A/B/C = 0.0126 / 0.0160 / 0.0189
- **ma_dist_20**: AUC=0.572, lift=1.66, N=76, mono=N, med A/B/C = -0.0304 / -0.0264 / -0.0307
