# VF-Turnover IC Validation — Value Pool (TW)

Generated: 2026-04-22 22:24

**Factor**: turnover = mean(Volume over W trading days) / Shares_Outstanding * 100

**Universe**: trade_journal_value_tw_snapshot.parquet (Value 價值池, 2020-01 ~ 2025-12)

**Grading**: |IR|>=0.5 A / >=0.3 B / >=0.1 C / <0.1 D

**Overlap test**: rho_spearman(turnover, RVOL) > 0.7 => duplicate factor

## Coverage

- Total rows: 70,760
- turnover_20d coverage: 68,090 (96.2%)
- Unique stocks: 857
- Unique weeks: 309
- turnover_20d mean: 3.846%
- turnover_20d median: 1.723%
- turnover_20d p95: 13.731%

## RVOL Overlap Check

(rvol_20 = native column from Value journal; rvol_20_calc = our formula)

| Pair | N | Spearman rho | Pearson rho | Overlap? |
|---|---|---|---|---|
| turnover_20d_vs_rvol_20 | 68,090 | -0.139 | -0.022 | no |
| turnover_5d_vs_rvol_20 | 68,090 | +0.088 | +0.082 | no |
| turnover_20d_vs_rvol_20_calc | 68,039 | +0.281 | +0.143 | no |

## Factor: turnover_5d

### IC Summary (Spearman, cross-section per week)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Weeks | Grade |
|---|---|---|---|---|---|---|---|
| fwd_5d | -0.0346 | 0.2027 | -0.171 | -3.00 | 41.7% | 309 | C (weak) |
| fwd_10d | -0.0290 | 0.2053 | -0.141 | -2.48 | 43.0% | 309 | C (weak) |
| fwd_20d | -0.0345 | 0.2004 | -0.172 | -3.02 | 44.7% | 309 | C (weak) |
| fwd_40d | -0.0347 | 0.1750 | -0.198 | -3.49 | 41.4% | 309 | C (weak) |
| fwd_60d | -0.0303 | 0.1636 | -0.185 | -3.26 | 44.7% | 309 | C (weak) |

### Decile Spread (per week, then time-avg)

#### fwd_20d

| D | Mean Ret (fwd_20d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| D1 | +0.0031 | +3.94% | 309 | 6,950 |
| D2 | +0.0078 | +10.27% | 309 | 6,795 |
| D3 | +0.0079 | +10.41% | 309 | 6,763 |
| D4 | +0.0116 | +15.62% | 309 | 6,792 |
| D5 | +0.0109 | +14.67% | 309 | 6,816 |
| D6 | +0.0135 | +18.42% | 309 | 6,739 |
| D7 | +0.0137 | +18.71% | 309 | 6,756 |
| D8 | +0.0142 | +19.51% | 309 | 6,799 |
| D9 | +0.0173 | +24.14% | 309 | 6,762 |
| D10 | +0.0259 | +38.09% | 309 | 6,918 |

- **D10 - D1 spread (fwd_20d)**: +0.0229 per period, **+34.15% annualized**

#### fwd_40d

| D | Mean Ret (fwd_40d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| D1 | +0.0099 | +6.43% | 309 | 6,950 |
| D2 | +0.0196 | +12.97% | 309 | 6,795 |
| D3 | +0.0190 | +12.58% | 309 | 6,763 |
| D4 | +0.0203 | +13.52% | 309 | 6,792 |
| D5 | +0.0237 | +15.92% | 309 | 6,816 |
| D6 | +0.0289 | +19.66% | 309 | 6,739 |
| D7 | +0.0335 | +23.08% | 309 | 6,756 |
| D8 | +0.0325 | +22.29% | 309 | 6,799 |
| D9 | +0.0370 | +25.74% | 309 | 6,762 |
| D10 | +0.0485 | +34.78% | 309 | 6,917 |

- **D10 - D1 spread (fwd_40d)**: +0.0386 per period, **+28.35% annualized**

#### fwd_60d

| D | Mean Ret (fwd_60d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| D1 | +0.0178 | +7.71% | 309 | 6,950 |
| D2 | +0.0329 | +14.56% | 309 | 6,795 |
| D3 | +0.0312 | +13.79% | 309 | 6,763 |
| D4 | +0.0361 | +16.07% | 309 | 6,792 |
| D5 | +0.0439 | +19.77% | 309 | 6,816 |
| D6 | +0.0460 | +20.79% | 309 | 6,739 |
| D7 | +0.0549 | +25.17% | 309 | 6,756 |
| D8 | +0.0515 | +23.47% | 309 | 6,799 |
| D9 | +0.0587 | +27.07% | 309 | 6,762 |
| D10 | +0.0759 | +35.97% | 309 | 6,918 |

- **D10 - D1 spread (fwd_60d)**: +0.0581 per period, **+28.26% annualized**

## Factor: turnover_20d

### IC Summary (Spearman, cross-section per week)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Weeks | Grade |
|---|---|---|---|---|---|---|---|
| fwd_5d | -0.0328 | 0.2132 | -0.154 | -2.70 | 43.0% | 309 | C (weak) |
| fwd_10d | -0.0307 | 0.2133 | -0.144 | -2.53 | 45.0% | 309 | C (weak) |
| fwd_20d | -0.0354 | 0.2049 | -0.173 | -3.04 | 42.7% | 309 | C (weak) |
| fwd_40d | -0.0389 | 0.1781 | -0.218 | -3.84 | 39.8% | 309 | C (weak) |
| fwd_60d | -0.0384 | 0.1669 | -0.230 | -4.04 | 42.1% | 309 | C (weak) |

### Decile Spread (per week, then time-avg)

#### fwd_20d

| D | Mean Ret (fwd_20d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| D1 | +0.0030 | +3.89% | 309 | 6,950 |
| D2 | +0.0078 | +10.29% | 309 | 6,795 |
| D3 | +0.0071 | +9.37% | 309 | 6,763 |
| D4 | +0.0117 | +15.81% | 309 | 6,792 |
| D5 | +0.0109 | +14.62% | 309 | 6,816 |
| D6 | +0.0107 | +14.39% | 309 | 6,739 |
| D7 | +0.0186 | +26.19% | 309 | 6,756 |
| D8 | +0.0133 | +18.17% | 309 | 6,799 |
| D9 | +0.0169 | +23.45% | 309 | 6,762 |
| D10 | +0.0257 | +37.65% | 309 | 6,918 |

- **D10 - D1 spread (fwd_20d)**: +0.0226 per period, **+33.75% annualized**

#### fwd_40d

| D | Mean Ret (fwd_40d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| D1 | +0.0097 | +6.25% | 309 | 6,950 |
| D2 | +0.0210 | +13.98% | 309 | 6,795 |
| D3 | +0.0169 | +11.13% | 309 | 6,763 |
| D4 | +0.0258 | +17.37% | 309 | 6,792 |
| D5 | +0.0227 | +15.17% | 309 | 6,816 |
| D6 | +0.0260 | +17.57% | 309 | 6,739 |
| D7 | +0.0373 | +25.98% | 309 | 6,756 |
| D8 | +0.0304 | +20.76% | 309 | 6,799 |
| D9 | +0.0347 | +23.98% | 309 | 6,762 |
| D10 | +0.0485 | +34.77% | 309 | 6,917 |

- **D10 - D1 spread (fwd_40d)**: +0.0388 per period, **+28.53% annualized**

#### fwd_60d

| D | Mean Ret (fwd_60d) | Annualized | N Weeks | N Stocks-Total |
|---|---|---|---|---|
| D1 | +0.0173 | +7.48% | 309 | 6,950 |
| D2 | +0.0342 | +15.17% | 309 | 6,795 |
| D3 | +0.0308 | +13.57% | 309 | 6,763 |
| D4 | +0.0430 | +19.33% | 309 | 6,792 |
| D5 | +0.0388 | +17.35% | 309 | 6,816 |
| D6 | +0.0480 | +21.74% | 309 | 6,739 |
| D7 | +0.0557 | +25.57% | 309 | 6,756 |
| D8 | +0.0535 | +24.47% | 309 | 6,799 |
| D9 | +0.0534 | +24.44% | 309 | 6,762 |
| D10 | +0.0744 | +35.19% | 309 | 6,918 |

- **D10 - D1 spread (fwd_60d)**: +0.0571 per period, **+27.71% annualized**

## Regime Cut (turnover_20d)

| Regime | N Obs | Horizon | Mean IC | IC IR | t-stat | Grade |
|---|---|---|---|---|---|---|
| bear | 10690 | fwd_20d | -0.0073 | -0.033 | -0.25 | D (noise) |
| bear | 10690 | fwd_40d | +0.0059 | +0.035 | +0.26 | D (noise) |
| bear | 10690 | fwd_60d | +0.0242 | +0.166 | +1.26 | C (weak) |
| bull | 52598 | fwd_20d | -0.0677 | -0.369 | -5.47 | B (tradable) |
| bull | 52598 | fwd_40d | -0.0765 | -0.476 | -7.05 | B (tradable) |
| bull | 52598 | fwd_60d | -0.0733 | -0.473 | -7.00 | B (tradable) |
| volatile | 7472 | fwd_20d | +0.1297 | +0.549 | +3.15 | A (strong) |
| volatile | 7472 | fwd_40d | +0.1335 | +0.706 | +4.06 | A (strong) |
| volatile | 7472 | fwd_60d | +0.0854 | +0.449 | +2.58 | B (tradable) |
