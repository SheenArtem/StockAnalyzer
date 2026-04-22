# VF-Value-ex2 EDGAR IC Validation Summary

Generated: 2026-04-22 15:42

Sample: 52,062 (ticker, quarter) observations
        1512 unique tickers x 37 quarters
        Quarter range: 2015-12-31 ~ 2024-12-31

## F-Score Distribution

| F-Score | Count | Pct |
|---|---|---|
| 0 | 125 | 0.2% |
| 1 | 701 | 1.3% |
| 2 | 2,215 | 4.3% |
| 3 | 5,339 | 10.3% |
| 4 | 9,665 | 18.6% |
| 5 | 12,016 | 23.1% |
| 6 | 10,734 | 20.6% |
| 7 | 7,167 | 13.8% |
| 8 | 3,376 | 6.5% |
| 9 | 724 | 1.4% |

Historical incidence: F>=8 = 7.9%, F>=7 = 21.6%, F<=5 = 57.7%

## IC Summary (Spearman, f_score vs forward return)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N quarters | Grade |
|---|---|---|---|---|---|---|---|
| ret_3m | -0.0061 | 0.0766 | -0.080 | -0.49 | 45.9% | 37 | D (noise) |
| ret_6m | -0.0078 | 0.0779 | -0.101 | -0.61 | 51.4% | 37 | D (noise) |
| ret_12m | -0.0169 | 0.0622 | -0.272 | -1.65 | 37.8% | 37 | C (weak) |

## Group Annualized Returns

### ret_3m

| Group | Ann. Return |
|---|---|
| Top decile (F>=8) | +17.15% |
| Bot decile (F<=3) | +24.39% |
| **Top - Bot spread** | **-7.24%** |
| F>=8 | +17.15% |
| F>=7 | +18.29% |
| F<=5 | +26.82% |
| **F>=8 alpha vs F<=5** | **-9.67%** |
| F>=7 alpha vs F<=5 | -8.53% |

### ret_6m

| Group | Ann. Return |
|---|---|
| Top decile (F>=8) | +14.28% |
| Bot decile (F<=3) | +24.89% |
| **Top - Bot spread** | **-10.61%** |
| F>=8 | +14.28% |
| F>=7 | +16.04% |
| F<=5 | +24.41% |
| **F>=8 alpha vs F<=5** | **-10.13%** |
| F>=7 alpha vs F<=5 | -8.38% |

### ret_12m

| Group | Ann. Return |
|---|---|
| Top decile (F>=8) | +14.55% |
| Bot decile (F<=3) | +27.62% |
| **Top - Bot spread** | **-13.08%** |
| F>=8 | +14.55% |
| F>=7 | +16.46% |
| F<=5 | +24.66% |
| **F>=8 alpha vs F<=5** | **-10.11%** |
| F>=7 alpha vs F<=5 | -8.19% |

## By Regime

### IC IR by regime (3m / 6m / 12m)

| Regime | N obs | IC IR 3m | IC IR 6m | IC IR 12m |
|---|---|---|---|---|
| bear | 5,523 | +0.110 | -0.070 | +0.074 |
| bull | 39,662 | +0.023 | +0.242 | +0.211 |
| ranged | 1,236 | +nan | +nan | +nan |
| volatile | 5,641 | -2.620 | -1.381 | -0.657 |

### F>=8 alpha vs F<=5 (annualized, 6m horizon)

| Regime | N | F>=8 ann | F<=5 ann | Alpha |
|---|---|---|---|---|
| bear | 5,523 | +10.19% | +6.80% | **+3.39%** |
| bull | 39,662 | +12.39% | +38.10% | **-25.71%** |
| ranged | 1,236 | +50.72% | +62.85% | **-12.14%** |
| volatile | 5,641 | +19.16% | +20.54% | **-1.38%** |

## Conclusion

- Best IC horizon: **ret_12m** with IR = **-0.272** (grade C (weak))
- **F>=8 alpha vs F<=5 (annualized, ret_12m)**: **-10.11%**
- F>=7 alpha vs F<=5 (annualized, ret_12m): -8.19%
- Top-Bot decile spread (annualized, ret_12m): -13.08%
