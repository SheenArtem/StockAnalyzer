# Whale Picks Phase 2 — IC Backtest Report (v2 with Stage 7+8)

**Universe**: TW 1943 stocks / **Period**: 2021-01-01 ~ 2025-12-31 / **Pipeline**: stages 3-8 (full minus extensions)

**Methodology**: per docs/whale_picks_spec.md v0.2 council verdict

## Stage 4+5+6 — Univariate selection + Decile kill + FDR

| Feature | N | IC_60d | IC_120d | p_value | FDR ✓ | P@10 | P@20 | base | Q10-Q1 | Mono | Kill ✓ |
|---|---|---|---|---|---|---|---|---|---|---|---|
| low52w_prox_adj | 57 | -0.0668 | -0.0703 | 0.0001 | ✓ | 0.354 | 0.368 | 0.173 | 0.0081 | 0.636 | ✗ |
| dist_52w_high | 51 | -0.0702 | -0.0847 | 0.0004 | ✓ | 0.288 | 0.283 | 0.173 | -0.0332 | -0.976 | ✓ |
| rsi_14 | 57 | -0.0124 | 0.0073 | 0.2250 | ✗ | 0.260 | 0.254 | 0.173 | 0.0093 | 0.806 | ✗ |
| rvol_20 | 57 | -0.0010 | -0.0017 | 0.8764 | ✗ | 0.240 | 0.248 | 0.173 | -0.0004 | 0.030 | ✗ |
| turnover_log | 57 | -0.0812 | -0.0952 | 0.0004 | ✓ | 0.204 | 0.260 | 0.173 | -0.0263 | -0.903 | ✓ |
| close_to_ma60 | 56 | -0.0097 | 0.0041 | 0.4908 | ✗ | 0.386 | 0.396 | 0.173 | 0.0216 | 0.939 | ✗ |
| close_to_ma240 | 51 | -0.0103 | -0.0017 | 0.5365 | ✗ | 0.373 | 0.394 | 0.173 | 0.0199 | 0.855 | ✗ |
| atr_ratio_20_60 | 56 | -0.0148 | -0.0132 | 0.0965 | ✗ | 0.286 | 0.289 | 0.173 | 0.0124 | 0.818 | ✗ |
| foreign_pct | 53 | -0.0064 | 0.0058 | 0.3375 | ✗ | 0.128 | 0.124 | 0.173 | 0.0017 | 0.164 | ✗ |
| total_pct | 53 | -0.0086 | 0.0015 | 0.2278 | ✗ | 0.121 | 0.122 | 0.173 | -0.0011 | -0.006 | ✗ |
| foreign_net_pressure | 53 | -0.0061 | 0.0061 | 0.3557 | ✗ | 0.128 | 0.123 | 0.173 | 0.0012 | 0.261 | ✗ |
| trust_net_pressure | 53 | -0.0074 | -0.0046 | 0.1270 | ✗ | 0.087 | 0.084 | 0.173 | nan | nan | ✗ |
| f_score | 55 | 0.0860 | 0.0760 | 0.0000 | ✓ | 0.160 | 0.195 | 0.173 | nan | nan | ✗ |
| z_score | 55 | -0.0040 | -0.0099 | 0.5310 | ✗ | 0.160 | 0.190 | 0.173 | -0.0047 | -0.236 | ✗ |

## Stage 7 — Walk-forward + LOOY + Cross-regime (top features)

| Feature | WF IC mean | WF pos% | N wins | LOOY min | LOOY max | LOOY range | Bull IC | Bear IC | Sideways IC |
|---|---|---|---|---|---|---|---|---|---|
| f_score | 0.0998 | 1.00 | 6 | 0.0790 | 0.0924 | 0.0133 | 0.0701 | 0.0727 | 0.1101 |
| turnover_log | -0.0258 | 0.50 | 6 | -0.1025 | -0.0670 | 0.0355 | -0.1210 | -0.1346 | -0.0053 |
| dist_52w_high | -0.0525 | 0.33 | 6 | -0.0823 | -0.0557 | 0.0265 | -0.0726 | -0.0617 | -0.0731 |
| low52w_prox_adj | -0.0358 | 0.50 | 6 | -0.0873 | -0.0555 | 0.0318 | -0.1040 | -0.0873 | -0.0125 |
| close_to_ma60 | -0.0131 | 0.33 | 6 | -0.0210 | 0.0041 | 0.0251 | -0.0331 | -0.0052 | 0.0135 |
| close_to_ma240 | -0.0088 | 0.33 | 6 | -0.0349 | 0.0046 | 0.0394 | -0.0584 | -0.0052 | 0.0280 |

## Stage 8 — Portfolio simulator (top-K equal weight monthly, fwd_20d hold)


**B&H TWII baseline**: CAGR 8.68% / Sharpe 0.64 / MDD -28.92%

| Strategy | N periods | Total ret | CAGR | Vol | Sharpe | MDD | Win rate |
|---|---|---|---|---|---|---|---|
| B&H TWII | 77 | 70.56% | 8.68% | 14.62% | 0.64 | -28.92% | 55.84% |
| top-10 f_score | 55 | 129.30% | 19.85% | 18.44% | 1.08 | -15.69% | 61.82% |
| top-10 turnover_log | 57 | 113.09% | 17.27% | 28.70% | 0.70 | -43.71% | 56.14% |
| top-10 dist_52w_high | 51 | 9.30% | 2.11% | 30.20% | 0.21 | -41.78% | 49.02% |
| top-10 low52w_prox_adj | 57 | 82.66% | 13.52% | 39.65% | 0.51 | -62.53% | 56.14% |
| top-10 close_to_ma60 | 56 | 67.41% | 11.67% | 46.57% | 0.46 | -47.29% | 50.00% |
| top-10 close_to_ma240 | 51 | 9.78% | 2.22% | 37.25% | 0.24 | -46.48% | 49.02% |
| top-20 f_score | 55 | 203.73% | 27.43% | 18.61% | 1.41 | -17.35% | 67.27% |
| top-20 turnover_log | 57 | 93.40% | 14.90% | 29.23% | 0.62 | -46.03% | 56.14% |
| top-20 dist_52w_high | 51 | 28.71% | 6.12% | 27.30% | 0.34 | -35.49% | 49.02% |
| top-20 low52w_prox_adj | 57 | 123.75% | 18.48% | 32.06% | 0.69 | -46.98% | 59.65% |
| top-20 close_to_ma60 | 56 | 91.28% | 14.91% | 39.35% | 0.54 | -37.76% | 58.93% |
| top-20 close_to_ma240 | 51 | 86.07% | 15.73% | 32.83% | 0.61 | -30.27% | 52.94% |

## Kill criteria recap
- Decile monotonicity |Spearman| ≥ 0.5 ✓
- Decile spread (Q10-Q1) sign matches IC sign ✓
- |IC_60d| ≥ 0.03
- BH-FDR alpha=0.10 ✓

## Final Verdict

- Univariate kill + FDR passed: **2/14 features**
- Stage 8 portfolio sim beat B&H (CAGR > 8.68%) AND Sharpe > 0.3: **9/12 strategies**

**Phase 2 Verdict: PROMISING** — 9 strategies pass SOP-10 portfolio gate

Winners:
- top-10 f_score: CAGR 19.85% / Sharpe 1.08 / MDD -15.69%
- top-10 turnover_log: CAGR 17.27% / Sharpe 0.70 / MDD -43.71%
- top-10 low52w_prox_adj: CAGR 13.52% / Sharpe 0.51 / MDD -62.53%
- top-10 close_to_ma60: CAGR 11.67% / Sharpe 0.46 / MDD -47.29%
- top-20 f_score: CAGR 27.43% / Sharpe 1.41 / MDD -17.35%
- top-20 turnover_log: CAGR 14.90% / Sharpe 0.62 / MDD -46.03%
- top-20 low52w_prox_adj: CAGR 18.48% / Sharpe 0.69 / MDD -46.98%
- top-20 close_to_ma60: CAGR 14.91% / Sharpe 0.54 / MDD -37.76%
- top-20 close_to_ma240: CAGR 15.73% / Sharpe 0.61 / MDD -30.27%

## Caveats
- Stage 8 uses fwd_20d hold-to-month-end as simple proxy. Realistic exit (trailing stop / take profit) not modeled.
- Survivorship: universe_tw 1972 excludes 下市 stocks → over-states alpha.
- 4/5 years bullish (2021/2023/2024/2025 vs 2022) → cross-regime split essential to detect time-period dependency.
- TDCC 集中度 Δ features absent (deferred to Phase 3 per SPEC §5).