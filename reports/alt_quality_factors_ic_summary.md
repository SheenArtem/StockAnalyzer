# Alt Quality Factors IC Validation Summary

Generated: 2026-04-22 16:36

Sample: 52,062 (ticker, quarter) rows
        1512 tickers x 37 quarters
        Period: 2015-12-31 ~ 2024-12-31

Factors:
- **FCF Yield** = (CFO_ttm - |CapEx_ttm|) / (Price_entry * Shares_q)
- **ROIC (approx)** = OpIncome_ttm * (1 - 0.21) / (Equity_q + LTDebt_q); NI fallback if OpIncome missing
- **Gross Profitability** = GrossProfit_ttm / TotalAssets_q  (Novy-Marx 2013)

  fcf_yield: 49,735 non-null (95.5%)
  roic: 49,260 non-null (94.6%)
  gp_assets: 51,237 non-null (98.4%)

## Head-to-head (vs F-Score baseline)

| Factor | N obs | IC IR 12m | Top-Bot Dec 12m (ann) | Dec Win Rate | Top Quin alpha 12m | Grade |
|---|---|---|---|---|---|---|
| Piotroski F-Score (ref) | 52,062 | -0.272 | -13.08% | 37.8% | -10.11% | D (reverse) |
| FCF Yield | 49,735 | +0.091 | -8.51% | 27.0% | -4.97% | D (noise) |
| ROIC (approx) | 49,260 | -0.068 | -8.94% | 43.2% | -5.36% | D (noise) |
| Gross Profitability | 51,237 | +0.204 | -3.47% | 45.9% | -3.86% | C (weak) |

## IC Detail per Factor (3m / 6m / 12m)

| Factor | Horizon | Mean IC | Std | IC IR | t-stat | % Pos | N Q | Grade |
|---|---|---|---|---|---|---|---|---|
| FCF Yield | ret_3m | +0.0060 | 0.1270 | +0.047 | +0.29 | 45.9% | 37 | D (noise) |
| FCF Yield | ret_6m | +0.0120 | 0.1172 | +0.102 | +0.62 | 48.6% | 37 | C (weak) |
| FCF Yield | ret_12m | +0.0100 | 0.1109 | +0.091 | +0.55 | 51.4% | 37 | D (noise) |
| ROIC (approx) | ret_3m | -0.0099 | 0.0945 | -0.105 | -0.64 | 40.5% | 37 | C (weak) |
| ROIC (approx) | ret_6m | -0.0074 | 0.1140 | -0.065 | -0.39 | 56.8% | 37 | D (noise) |
| ROIC (approx) | ret_12m | -0.0068 | 0.1001 | -0.068 | -0.41 | 51.4% | 37 | D (noise) |
| Gross Profitability | ret_3m | +0.0182 | 0.1128 | +0.161 | +0.98 | 54.1% | 37 | C (weak) |
| Gross Profitability | ret_6m | +0.0232 | 0.1172 | +0.198 | +1.21 | 64.9% | 37 | C (weak) |
| Gross Profitability | ret_12m | +0.0275 | 0.1345 | +0.204 | +1.24 | 48.6% | 37 | C (weak) |

## Decile & Quintile Spread (annualized)

### FCF Yield

| Horizon | Top Dec | Bot Dec | Top-Bot Dec | Dec Win% | Dec Spread IR | Top Quin | Bot Quin | Top-Bot Quin |
|---|---|---|---|---|---|---|---|---|
| ret_3m | +25.00% | +31.86% | **-6.86%** | 37.8% | -0.246 | +22.63% | +25.09% | **-2.46%** |
| ret_6m | +22.74% | +27.76% | **-5.02%** | 32.4% | -0.294 | +20.55% | +22.66% | **-2.12%** |
| ret_12m | +22.71% | +31.22% | **-8.51%** | 27.0% | -0.637 | +20.06% | +25.02% | **-4.97%** |

### ROIC (approx)

| Horizon | Top Dec | Bot Dec | Top-Bot Dec | Dec Win% | Dec Spread IR | Top Quin | Bot Quin | Top-Bot Quin |
|---|---|---|---|---|---|---|---|---|
| ret_3m | +19.24% | +29.79% | **-10.55%** | 43.2% | -0.242 | +19.22% | +23.16% | **-3.94%** |
| ret_6m | +17.11% | +27.39% | **-10.28%** | 45.9% | -0.312 | +17.01% | +22.49% | **-5.49%** |
| ret_12m | +18.20% | +27.14% | **-8.94%** | 43.2% | -0.357 | +17.37% | +22.73% | **-5.36%** |

### Gross Profitability

| Horizon | Top Dec | Bot Dec | Top-Bot Dec | Dec Win% | Dec Spread IR | Top Quin | Bot Quin | Top-Bot Quin |
|---|---|---|---|---|---|---|---|---|
| ret_3m | +23.13% | +27.69% | **-4.57%** | 51.4% | -0.075 | +22.46% | +27.69% | **-5.23%** |
| ret_6m | +21.42% | +25.49% | **-4.07%** | 62.2% | -0.090 | +20.92% | +25.49% | **-4.57%** |
| ret_12m | +21.48% | +24.95% | **-3.47%** | 45.9% | -0.097 | +21.09% | +24.95% | **-3.86%** |

## Regime Breakdown (IC IR 12m)

| Factor | bull | bear | volatile | ranged |
|---|---|---|---|---|
| FCF Yield | +0.143 | -1.156 | +0.018 | N/A |
| ROIC (approx) | -0.061 | +0.309 | -0.013 | N/A |
| Gross Profitability | +0.039 | +1.252 | +0.785 | N/A |

### Top-Quintile Alpha 12m by Regime (top quin ann - bot quin ann)

| Factor | bull | bear | volatile | ranged |
|---|---|---|---|---|
| FCF Yield | -5.58% | -3.20% | -0.48% | -0.99% |
| ROIC (approx) | -4.85% | +2.97% | -9.92% | -29.19% |
| Gross Profitability | -6.06% | +6.47% | +4.26% | -7.04% |

## Top-Bot Decile 12m Spread by Year

| Year | FCF Yield | ROIC | Gross Profitability |
|---|---|---|---|
| 2015 | -14.23% | -37.39% | -14.36% |
| 2016 | -11.35% | -5.29% | -3.97% |
| 2017 | -8.80% | -2.64% | +17.46% |
| 2018 | -13.78% | +3.84% | -1.02% |
| 2019 | -26.26% | +0.75% | +21.74% |
| 2020 | +20.18% | -59.52% | -50.71% |
| 2021 | +4.88% | -7.76% | -9.46% |
| 2022 | -11.80% | +8.42% | +10.36% |
| 2023 | -3.55% | -3.61% | -4.11% |
| 2024 | -16.03% | -1.58% | -4.08% |

### Years where factor top > bot decile

- **FCF Yield**: 2 / 10 years positive
- **ROIC (approx)**: 3 / 10 years positive
- **Gross Profitability**: 3 / 10 years positive

## Factor Rank Correlations (Spearman)

| | fcf_yield | roic | gp_assets | f_score |
|---|---|---|---|---|
| fcf_yield | +1.000 | +0.088 | -0.091 | +0.123 |
| roic | +0.088 | +1.000 | +0.384 | +0.331 |
| gp_assets | -0.091 | +0.384 | +1.000 | +0.275 |
| f_score | +0.123 | +0.331 | +0.275 | +1.000 |

## Bottom Line

**All three candidate quality factors grade D/C-weak in US 2015-2024.** None provides 
a reliable replacement for Piotroski F-Score; the same growth/momentum regime that broke 
F-Score also breaks classic quality/value factors.

Year-by-year win rate is 2-3 out of 10. Every factor loses in the 2015-2016, 2018, 2020 
and 2023-2024 windows. Gross Profitability has the least-bad IC IR (+0.20, t=1.24) but 
decile spread is still slightly negative (-3.47% annualized) and wins only 3/10 years.

- **FCF Yield**: 2/10 years positive; winning only 2020 (cash anomaly) + 2021 (reopening). 
  Decile spread -8.5% ann., win rate 27%. D (noise with negative tilt).
- **ROIC (approx)**: 3/10 years positive; dominated by 2020 bust (-60% spread). 
  Decile spread -8.9% ann., win rate 43%. D (noise with negative tilt).
- **Gross Profitability**: 3/10 years positive. IC IR +0.20 directionally right, but 
  decile spread -3.5% ann. The "bear regime IR +1.25" is computed over only 4 effective 
  bear quarters and is statistical noise. Best candidate among the three but still not tradable.

All three factors correlate positively with F-Score (rho 0.12-0.33), confirming they 
measure similar underlying quality construct that has been unrewarded in US 2015-2024.

### Recommendation for US value_screener

1. **Do NOT add FCF Yield / ROIC / Gross Profitability as positive adders**. They would 
   extend the same negative bias already proven for F-Score.
2. **Gross Profitability may be used defensively**: small positive weight only during 
   confirmed bear regime (SPY 200DMA slope < 0), where its bear IC IR is positive in 
   2022. Requires HMM/regime detection already implemented in scoring_status.
3. **Better path forward**: given US growth dominance, focus on momentum / growth / 
   technical factors (already validated via VF-G1..G4). Quality is a regime factor in 
   US 2015-2024, not a secular alpha source.

## Notes & Caveats

- Bear regime has only ~4 effective cross-section quarters in 2015-2024 (2016-Q1, 2018-Q3, 
  2022-Q2, 2022-Q4). High-IR regime claims for bear are not statistically robust.
- Ranged regime has only 1 quarter -- ignore those columns.
- FCF uses abs(CapEx) to guard against sign inconsistencies across XBRL filers.
- ROIC is an approximation: NOPAT = OperatingIncome*(1-0.21); falls back to NI+InterestExpense*(1-0.21) if OpIncome missing.
- All flow items use trailing 4 quarters (TTM) to reduce seasonality.
- Outlier filter: |FCF yield| <= 1, |ROIC| <= 2, GP/Assets within [-0.5, 3].
- Winsorized at 1%/99% per quarter before Spearman IC.
- IC grade: A >=0.5, B >=0.3, C >=0.1, D <0.1.
