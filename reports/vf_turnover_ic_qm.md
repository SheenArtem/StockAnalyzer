# VF-Turnover IC Validation — QM Pool (TW)

Generated: 2026-04-22 22:23

**Factor**: turnover = mean(Volume over W trading days) / Shares_Outstanding * 100

**Universe**: trade_journal_qm_tw_mixed.parquet (QM 動能池, 2015-07 ~ 2025-12)

**Grading**: |IR|>=0.5 A / >=0.3 B / >=0.1 C / <0.1 D

**Overlap test**: rho_spearman(turnover, RVOL) > 0.7 => duplicate factor

## Coverage

- Total rows: 4,923
- turnover_20d coverage: 4,670 (94.9%)
- turnover_5d coverage:  4,670 (94.9%)
- Unique stocks: 205
- Unique weeks: 538
- turnover_20d mean: 1.755%
- turnover_20d median: 0.753%
- turnover_20d p95: 7.038%

## RVOL Overlap Check

(rvol_20_calc = mean(vol last 20d) / mean(vol prior 60d), computed on-the-fly)

| Pair | N | Spearman rho | Pearson rho | Overlap? |
|---|---|---|---|---|
| turnover_20d_vs_rvol_20_calc | 4,670 | -0.017 | +0.116 | no |
| turnover_5d_vs_rvol_20_calc | 4,670 | -0.032 | +0.068 | no |

## Factor: turnover_5d

### IC Summary (Spearman, cross-section per week)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Weeks | Grade |
|---|---|---|---|---|---|---|---|
| fwd_5d | -0.0481 | 0.4285 | -0.112 | -2.34 | 44.0% | 432 | C (weak) |
| fwd_10d | -0.0466 | 0.4237 | -0.110 | -2.29 | 44.4% | 432 | C (weak) |
| fwd_20d | -0.0414 | 0.4195 | -0.099 | -2.05 | 42.1% | 432 | D (noise) |
| fwd_40d | -0.0359 | 0.4185 | -0.086 | -1.78 | 43.5% | 432 | D (noise) |
| fwd_60d | -0.0141 | 0.4035 | -0.035 | -0.72 | 46.8% | 432 | D (noise) |

### Quintile Spread (QM top-50 => n_bins=5, per week then time-avg)

#### fwd_20d

| Q | Mean Ret (fwd_20d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| Q1 | +0.0096 | +12.84% | 432 | 1,045 |
| Q2 | +0.0114 | +15.39% | 432 | 792 |
| Q3 | +0.0128 | +17.32% | 432 | 803 |
| Q4 | +0.0151 | +20.74% | 432 | 792 |
| Q5 | +0.0129 | +17.57% | 432 | 962 |

- **Q5 - Q1 spread (fwd_20d)**: +0.0033 per period, **+4.74% annualized**

#### fwd_40d

| Q | Mean Ret (fwd_40d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| Q1 | +0.0193 | +12.81% | 432 | 1,045 |
| Q2 | +0.0204 | +13.54% | 432 | 792 |
| Q3 | +0.0171 | +11.26% | 432 | 803 |
| Q4 | +0.0404 | +28.33% | 432 | 792 |
| Q5 | +0.0390 | +27.26% | 432 | 962 |

- **Q5 - Q1 spread (fwd_40d)**: +0.0197 per period, **+14.44% annualized**

#### fwd_60d

| Q | Mean Ret (fwd_60d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| Q1 | +0.0247 | +10.78% | 432 | 1,045 |
| Q2 | +0.0269 | +11.81% | 432 | 792 |
| Q3 | +0.0179 | +7.75% | 432 | 803 |
| Q4 | +0.0707 | +33.25% | 432 | 792 |
| Q5 | +0.0668 | +31.20% | 432 | 962 |

- **Q5 - Q1 spread (fwd_60d)**: +0.0421 per period, **+20.42% annualized**

## Factor: turnover_20d

### IC Summary (Spearman, cross-section per week)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Weeks | Grade |
|---|---|---|---|---|---|---|---|
| fwd_5d | -0.0269 | 0.4286 | -0.063 | -1.30 | 45.4% | 432 | D (noise) |
| fwd_10d | -0.0338 | 0.4266 | -0.079 | -1.64 | 45.4% | 432 | D (noise) |
| fwd_20d | -0.0280 | 0.4179 | -0.067 | -1.39 | 43.8% | 432 | D (noise) |
| fwd_40d | -0.0178 | 0.4119 | -0.043 | -0.90 | 43.3% | 432 | D (noise) |
| fwd_60d | -0.0030 | 0.4044 | -0.007 | -0.15 | 47.0% | 432 | D (noise) |

### Quintile Spread (QM top-50 => n_bins=5, per week then time-avg)

#### fwd_20d

| Q | Mean Ret (fwd_20d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| Q1 | +0.0097 | +12.92% | 432 | 1,045 |
| Q2 | +0.0089 | +11.80% | 432 | 792 |
| Q3 | +0.0089 | +11.77% | 432 | 803 |
| Q4 | +0.0155 | +21.32% | 432 | 792 |
| Q5 | +0.0166 | +23.12% | 432 | 962 |

- **Q5 - Q1 spread (fwd_20d)**: +0.0070 per period, **+10.20% annualized**

#### fwd_40d

| Q | Mean Ret (fwd_40d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| Q1 | +0.0184 | +12.15% | 432 | 1,045 |
| Q2 | +0.0183 | +12.07% | 432 | 792 |
| Q3 | +0.0142 | +9.30% | 432 | 803 |
| Q4 | +0.0371 | +25.77% | 432 | 792 |
| Q5 | +0.0446 | +31.66% | 432 | 962 |

- **Q5 - Q1 spread (fwd_40d)**: +0.0263 per period, **+19.51% annualized**

#### fwd_60d

| Q | Mean Ret (fwd_60d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| Q1 | +0.0235 | +10.27% | 432 | 1,045 |
| Q2 | +0.0227 | +9.90% | 432 | 792 |
| Q3 | +0.0226 | +9.84% | 432 | 803 |
| Q4 | +0.0652 | +30.36% | 432 | 792 |
| Q5 | +0.0726 | +34.24% | 432 | 962 |

- **Q5 - Q1 spread (fwd_60d)**: +0.0491 per period, **+23.97% annualized**

## Regime Cut (turnover_20d)

| Regime | N Obs | Horizon | Mean IC | IC IR | t-stat | Grade |
|---|---|---|---|---|---|---|
| bear | 709 | fwd_20d | +0.1278 | +0.250 | +2.10 | C (weak) |
| bear | 709 | fwd_40d | +0.1144 | +0.240 | +2.02 | C (weak) |
| bear | 709 | fwd_60d | +0.1572 | +0.351 | +2.96 | B (tradable) |
| bull | 3821 | fwd_20d | -0.0578 | -0.149 | -2.71 | C (weak) |
| bull | 3821 | fwd_40d | -0.0308 | -0.078 | -1.43 | D (noise) |
| bull | 3821 | fwd_60d | -0.0221 | -0.056 | -1.02 | D (noise) |
| volatile | 307 | fwd_20d | +0.0106 | +0.027 | +0.13 | D (noise) |
| volatile | 307 | fwd_40d | -0.0634 | -0.206 | -0.97 | C (weak) |
| volatile | 307 | fwd_60d | -0.1067 | -0.407 | -1.91 | B (tradable) |
