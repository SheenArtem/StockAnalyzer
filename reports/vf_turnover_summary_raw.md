# VF-Turnover Validation Summary — Decision

Generated: 2026-04-22 22:24

## Best-horizon IR per pool (factor=turnover_20d)

| Pool | Horizon | Mean IC | IC IR | t-stat | Grade |
|---|---|---|---|---|---|
| QM (turnover_20d) | fwd_10d | -0.0338 | -0.079 | -1.64 | D (noise) |
| QM (turnover_5d) | fwd_5d | -0.0481 | -0.112 | -2.34 | C (weak) |
| Value (turnover_20d) | fwd_60d | -0.0384 | -0.230 | -4.04 | C (weak) |
| Value (turnover_5d) | fwd_40d | -0.0347 | -0.198 | -3.49 | C (weak) |

## RVOL Overlap (turnover_20d vs RVOL)

| Pool | Pair | Spearman rho | Overlap? |
|---|---|---|---|
| QM | turnover_20d_vs_rvol_20_calc | -0.017 | no |
| QM | turnover_5d_vs_rvol_20_calc | -0.032 | no |
| Value | turnover_20d_vs_rvol_20 | -0.139 | no |
| Value | turnover_5d_vs_rvol_20 | +0.088 | no |
| Value | turnover_20d_vs_rvol_20_calc | +0.281 | no |

## Top-Bot Quantile Spread Annualized (turnover_20d, fwd_40d)

| Pool | Bins | Low ann | High ann | High - Low ann |
|---|---|---|---|---|
| QM | 5 | +12.15% | +31.66% | +19.51% |
| Value | 10 | +6.25% | +34.77% | +28.53% |

## Regime Cut (turnover_20d, fwd_40d)

| Pool | Regime | N obs | Mean IC | IC IR | Grade |
|---|---|---|---|---|---|
| QM | bear | 709 | +0.1144 | +0.240 | C (weak) |
| QM | bull | 3821 | -0.0308 | -0.078 | D (noise) |
| QM | volatile | 307 | -0.0634 | -0.206 | C (weak) |
| Value | bear | 10690 | +0.0059 | +0.035 | D (noise) |
| Value | bull | 52598 | -0.0765 | -0.476 | B (tradable) |
| Value | volatile | 7472 | +0.1335 | +0.706 | A (strong) |

## Decision Framework

**Required for B+落地**：

1. Pooled IC IR >= 0.3 OR
2. Decile spread annualized > 3% with monotonic trend OR
3. Regime-conditional IC IR >= 0.3 with t-stat >= 2

AND:

4. RVOL overlap (Spearman) < 0.7

See individual reports for full details:

- `reports/vf_turnover_ic_qm.md`
- `reports/vf_turnover_ic_value.md`
