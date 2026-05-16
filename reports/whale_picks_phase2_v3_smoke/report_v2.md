# Whale Picks Phase 2 — IC Backtest Report (v2 with Stage 7+8)

**Universe**: TW 50 stocks / **Period**: 2022-01-01 ~ 2024-12-31 / **Pipeline**: stages 3-8 (full minus extensions)

**Methodology**: per docs/whale_picks_spec.md v0.2 council verdict

## Stage 4+5+6 — Univariate selection + Decile kill + FDR

| Feature | N | IC_60d | IC_120d | p_value | FDR ✓ | P@10 | P@20 | base | Q10-Q1 | Mono | Kill ✓ |
|---|---|---|---|---|---|---|---|---|---|---|---|
| low52w_prox_adj | 36 | 0.0442 | 0.0298 | 0.1656 | ✗ | 0.144 | 0.092 | 0.057 | 0.0329 | 0.915 | ✓ |
| dist_52w_high | 30 | -0.2432 | -0.3102 | 0.0000 | ✓ | 0.037 | 0.038 | 0.057 | -0.0852 | -0.903 | ✓ |
| rsi_14 | 36 | 0.0677 | 0.0643 | 0.0270 | ✓ | 0.108 | 0.079 | 0.057 | 0.0527 | 0.794 | ✓ |
| rvol_20 | 36 | -0.0136 | -0.0302 | 0.5733 | ✗ | 0.075 | 0.053 | 0.057 | -0.0113 | -0.030 | ✗ |
| turnover_log | 36 | -0.1834 | -0.2501 | 0.0000 | ✓ | 0.028 | 0.042 | 0.057 | -0.0663 | -0.903 | ✓ |
| close_to_ma60 | 35 | 0.0845 | 0.1016 | 0.0172 | ✓ | 0.120 | 0.080 | 0.057 | 0.0674 | 0.915 | ✓ |
| close_to_ma240 | 30 | 0.1792 | 0.2552 | 0.0006 | ✓ | 0.143 | 0.090 | 0.057 | 0.0791 | 0.891 | ✓ |
| atr_ratio_20_60 | 35 | -0.0292 | -0.0304 | 0.3832 | ✗ | 0.083 | 0.070 | 0.057 | 0.0206 | 0.018 | ✗ |
| foreign_pct | 35 | -0.0611 | -0.0367 | 0.1007 | ✗ | 0.051 | 0.051 | 0.057 | -0.0146 | -0.224 | ✗ |
| total_pct | 35 | -0.0712 | -0.0439 | 0.0524 | ✗ | 0.046 | 0.051 | 0.057 | -0.0127 | -0.285 | ✗ |
| foreign_net_pressure | 35 | -0.0615 | -0.0373 | 0.0973 | ✗ | 0.046 | 0.051 | 0.057 | -0.0157 | -0.261 | ✗ |
| trust_net_pressure | 35 | -0.0177 | -0.0388 | 0.5105 | ✗ | 0.026 | 0.049 | 0.057 | nan | nan | ✗ |
| total_net_pressure | 35 | -0.0707 | -0.0433 | 0.0525 | ✗ | 0.046 | 0.051 | 0.057 | -0.0117 | -0.236 | ✗ |
| f_score | 34 | 0.1406 | 0.1507 | 0.0001 | ✓ | 0.044 | 0.051 | 0.057 | nan | nan | ✗ |
| z_score | 34 | -0.0837 | -0.1163 | 0.0001 | ✓ | 0.029 | 0.041 | 0.057 | -0.0498 | -0.394 | ✗ |
| foreign_pct_4w_delta | 34 | -0.0310 | -0.0276 | 0.3407 | ✗ | 0.044 | 0.043 | 0.057 | -0.0095 | -0.285 | ✗ |
| foreign_pct_8w_delta | 33 | -0.0263 | -0.0448 | 0.3605 | ✗ | 0.058 | 0.055 | 0.057 | 0.0009 | 0.115 | ✗ |
| total_pct_4w_delta | 34 | -0.0315 | -0.0384 | 0.3539 | ✗ | 0.044 | 0.043 | 0.057 | -0.0177 | -0.236 | ✗ |
| total_pct_8w_delta | 33 | -0.0347 | -0.0521 | 0.2444 | ✗ | 0.052 | 0.058 | 0.057 | 0.0047 | 0.079 | ✗ |
| trust_pct_4w_delta | 34 | -0.0145 | -0.0050 | 0.6497 | ✗ | 0.026 | 0.040 | 0.057 | nan | nan | ✗ |
| foreign_net_5d_4w_sum | 35 | -0.0210 | 0.0356 | 0.5578 | ✗ | 0.049 | 0.054 | 0.057 | 0.0052 | 0.030 | ✗ |
| foreign_net_5d_52w_z | 29 | -0.0583 | -0.0491 | 0.1542 | ✗ | 0.048 | 0.041 | 0.057 | 0.0008 | -0.127 | ✗ |
| trust_net_5d_4w_sum | 35 | -0.0307 | -0.0253 | 0.3170 | ✗ | 0.034 | 0.050 | 0.057 | nan | nan | ✗ |
| trust_net_5d_52w_z | 8 | 0.0558 | 0.1486 | 0.1484 | ✗ | 0.038 | 0.050 | 0.057 | 0.0029 | 0.394 | ✗ |
| vol_compression_60_252 | 30 | -0.0546 | -0.0715 | 0.0762 | ✗ | 0.057 | 0.057 | 0.057 | -0.0364 | -0.624 | ✓ |
| price_stability_60_252 | 30 | -0.0651 | -0.0666 | 0.0475 | ✗ | 0.067 | 0.052 | 0.057 | -0.0212 | -0.855 | ✓ |
| vol_price_divergence | 35 | -0.0823 | -0.1012 | 0.0064 | ✓ | 0.023 | 0.040 | 0.057 | -0.0671 | -0.879 | ✓ |
| ma60_slope_20d | 34 | 0.0901 | 0.1332 | 0.0383 | ✗ | 0.112 | 0.072 | 0.057 | 0.0504 | 0.976 | ✓ |
| body_strength_20d | 36 | 0.0122 | 0.0252 | 0.6562 | ✗ | 0.083 | 0.069 | 0.057 | 0.0094 | -0.006 | ✗ |
| upper_half_close_20d_pct | 36 | 0.0748 | 0.0985 | 0.0115 | ✓ | 0.086 | 0.072 | 0.057 | nan | nan | ✗ |
| stealth_volume_20d | 35 | 0.0991 | 0.1429 | 0.0011 | ✓ | 0.089 | 0.071 | 0.057 | 0.0388 | 0.939 | ✓ |
| revenue_score | 36 | 0.0181 | 0.0680 | 0.5682 | ✗ | 0.050 | 0.065 | 0.057 | nan | nan | ✗ |
| revenue_score_3m_delta | 36 | -0.0155 | 0.0034 | 0.6109 | ✗ | 0.058 | 0.051 | 0.057 | nan | nan | ✗ |
| revenue_score_6m_delta | 36 | 0.0019 | 0.0724 | 0.9486 | ✗ | 0.053 | 0.057 | 0.057 | nan | nan | ✗ |

## Stage 7 — Walk-forward + LOOY + Cross-regime (top features)

| Feature | WF IC mean | WF pos% | N wins | LOOY min | LOOY max | LOOY range | Bull IC | Bear IC | Sideways IC |
|---|---|---|---|---|---|---|---|---|---|
| dist_52w_high | -0.3230 | 0.00 | 2 | -0.3177 | -0.1639 | 0.1538 | -0.3621 | 0.0549 | -0.2733 |
| turnover_log | -0.1595 | 0.00 | 2 | -0.1964 | -0.1686 | 0.0277 | -0.1575 | -0.2130 | -0.1797 |
| close_to_ma240 | 0.2801 | 1.00 | 2 | 0.1086 | 0.2676 | 0.1590 | 0.2503 | -0.1747 | 0.2850 |
| f_score | 0.1071 | 0.50 | 2 | 0.1180 | 0.1591 | 0.0411 | 0.1821 | 0.0963 | 0.1361 |
| low52w_prox_adj | 0.2103 | 1.00 | 2 | -0.0008 | 0.1247 | 0.1255 | 0.1152 | -0.1168 | 0.1342 |
| close_to_ma60 | 0.1682 | 1.00 | 2 | 0.0304 | 0.1446 | 0.1142 | 0.1884 | -0.0465 | 0.1008 |
| ma60_slope_20d | 0.2141 | 1.00 | 2 | 0.0212 | 0.1639 | 0.1427 | 0.2165 | -0.0869 | 0.1112 |

## Stage 8 — Portfolio simulator (top-K equal weight monthly, fwd_20d hold)


**B&H TWII baseline**: CAGR 9.51% / Sharpe 0.58 / MDD -26.81%

| Strategy | N periods | Total ret | CAGR | Vol | Sharpe | MDD | Win rate |
|---|---|---|---|---|---|---|---|
| B&H TWII | 35 | 30.33% | 9.51% | 18.57% | 0.58 | -26.81% | 60.00% |
| top-10 dist_52w_high | 30 | -44.25% | -20.84% | 16.17% | -1.35 | -45.91% | 40.00% |
| top-10 turnover_log | 36 | -35.42% | -13.56% | 12.13% | -1.14 | -38.08% | 44.44% |
| top-10 close_to_ma240 | 30 | 48.13% | 17.02% | 13.66% | 1.22 | -9.57% | 60.00% |
| top-10 f_score | 34 | 18.49% | 6.17% | 12.05% | 0.56 | -12.17% | 64.71% |
| top-10 low52w_prox_adj | 36 | 51.82% | 14.93% | 16.19% | 0.94 | -14.62% | 69.44% |
| top-10 close_to_ma60 | 35 | 33.66% | 10.46% | 13.12% | 0.82 | -13.30% | 54.29% |
| top-10 ma60_slope_20d | 34 | 32.35% | 10.40% | 14.04% | 0.77 | -10.64% | 58.82% |
| top-20 dist_52w_high | 30 | -32.08% | -14.34% | 14.00% | -1.03 | -33.59% | 40.00% |
| top-20 turnover_log | 36 | -29.32% | -10.92% | 12.09% | -0.89 | -32.34% | 36.11% |
| top-20 close_to_ma240 | 30 | 34.06% | 12.44% | 10.69% | 1.15 | -7.42% | 66.67% |
| top-20 f_score | 34 | 13.70% | 4.64% | 9.70% | 0.51 | -11.04% | 50.00% |
| top-20 low52w_prox_adj | 36 | 13.03% | 4.17% | 12.06% | 0.40 | -16.83% | 63.89% |
| top-20 close_to_ma60 | 35 | 24.32% | 7.75% | 10.08% | 0.79 | -9.86% | 57.14% |
| top-20 ma60_slope_20d | 34 | 19.29% | 6.42% | 10.12% | 0.67 | -9.91% | 61.76% |

## Kill criteria recap
- Decile monotonicity |Spearman| ≥ 0.5 ✓
- Decile spread (Q10-Q1) sign matches IC sign ✓
- |IC_60d| ≥ 0.03
- BH-FDR alpha=0.10 ✓

## Final Verdict

- Univariate kill + FDR passed: **7/34 features**
- Stage 8 portfolio sim beat B&H (CAGR > 9.51%) AND Sharpe > 0.3: **5/14 strategies**

**Phase 2 Verdict: PROMISING** — 5 strategies pass SOP-10 portfolio gate

Winners:
- top-10 close_to_ma240: CAGR 17.02% / Sharpe 1.22 / MDD -9.57%
- top-10 low52w_prox_adj: CAGR 14.93% / Sharpe 0.94 / MDD -14.62%
- top-10 close_to_ma60: CAGR 10.46% / Sharpe 0.82 / MDD -13.30%
- top-10 ma60_slope_20d: CAGR 10.40% / Sharpe 0.77 / MDD -10.64%
- top-20 close_to_ma240: CAGR 12.44% / Sharpe 1.15 / MDD -7.42%

## Caveats
- Stage 8 uses fwd_20d hold-to-month-end as simple proxy. Realistic exit (trailing stop / take profit) not modeled.
- Survivorship: universe_tw 1972 excludes 下市 stocks → over-states alpha.
- 4/5 years bullish (2021/2023/2024/2025 vs 2022) → cross-regime split essential to detect time-period dependency.
- TDCC 集中度 Δ features absent (deferred to Phase 3 per SPEC §5).