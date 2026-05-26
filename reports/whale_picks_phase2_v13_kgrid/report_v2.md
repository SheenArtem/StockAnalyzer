# Whale Picks Phase 2 — IC Backtest Report (v2 with Stage 7+8)

**Universe**: TW 1749 stocks / **Period**: 2021-01-01 ~ 2025-12-31 / **Pipeline**: stages 3-8 (full minus extensions)

**Methodology**: per docs/whale_picks_spec.md v0.2 council verdict

## Stage 4+5+6 — Univariate selection + Decile kill + FDR

| Feature | N | IC_60d | IC_120d | p_value | FDR ✓ | P@10 | P@20 | base | Q10-Q1 | Mono | Kill ✓ |
|---|---|---|---|---|---|---|---|---|---|---|---|
| low52w_prox_adj | 57 | -0.0659 | -0.0713 | 0.0004 | ✓ | 0.398 | 0.359 | 0.184 | 0.0264 | 0.733 | ✗ |
| dist_52w_high | 51 | -0.0710 | -0.0905 | 0.0005 | ✓ | 0.261 | 0.262 | 0.184 | -0.0406 | -0.988 | ✓ |
| rsi_14 | 57 | -0.0110 | 0.0082 | 0.3784 | ✗ | 0.304 | 0.291 | 0.184 | 0.0143 | 0.830 | ✗ |
| rvol_20 | 57 | 0.0061 | 0.0103 | 0.4454 | ✗ | 0.247 | 0.225 | 0.184 | -0.0022 | 0.079 | ✗ |
| turnover_log | 57 | -0.0394 | -0.0465 | 0.0120 | ✓ | 0.235 | 0.260 | 0.184 | 0.0074 | -0.370 | ✗ |
| close_to_ma60 | 56 | -0.0091 | 0.0019 | 0.5615 | ✗ | 0.421 | 0.380 | 0.184 | 0.0275 | 0.915 | ✗ |
| close_to_ma240 | 51 | -0.0202 | -0.0080 | 0.3121 | ✗ | 0.404 | 0.395 | 0.184 | 0.0314 | 0.988 | ✗ |
| atr_ratio_20_60 | 56 | -0.0116 | -0.0086 | 0.3172 | ✗ | 0.339 | 0.318 | 0.184 | 0.0144 | 0.685 | ✗ |
| foreign_pct | 53 | 0.0024 | 0.0104 | 0.7089 | ✗ | 0.130 | 0.143 | 0.184 | 0.0020 | 0.455 | ✗ |
| total_pct | 53 | -0.0002 | 0.0053 | 0.9790 | ✗ | 0.128 | 0.138 | 0.184 | 0.0011 | 0.467 | ✗ |
| foreign_net_pressure | 53 | 0.0027 | 0.0107 | 0.6761 | ✗ | 0.123 | 0.140 | 0.184 | 0.0026 | 0.467 | ✗ |
| trust_net_pressure | 53 | -0.0002 | 0.0018 | 0.9768 | ✗ | 0.081 | 0.082 | 0.184 | nan | nan | ✗ |
| total_net_pressure | 53 | -0.0000 | 0.0055 | 0.9976 | ✗ | 0.132 | 0.135 | 0.184 | 0.0012 | 0.382 | ✗ |
| f_score | 57 | 0.0421 | 0.0499 | 0.0000 | ✓ | 0.214 | 0.211 | 0.184 | nan | nan | ✗ |
| z_score | 57 | -0.0383 | -0.0491 | 0.0002 | ✓ | 0.232 | 0.211 | 0.184 | -0.0121 | -0.782 | ✓ |
| foreign_pct_4w_delta | 52 | 0.0023 | 0.0032 | 0.7998 | ✗ | 0.125 | 0.138 | 0.184 | -0.0093 | 0.176 | ✗ |
| foreign_pct_8w_delta | 51 | -0.0066 | -0.0102 | 0.3127 | ✗ | 0.133 | 0.141 | 0.184 | -0.0019 | -0.103 | ✗ |
| total_pct_4w_delta | 52 | -0.0018 | -0.0015 | 0.8300 | ✗ | 0.127 | 0.142 | 0.184 | -0.0090 | -0.236 | ✗ |
| total_pct_8w_delta | 51 | -0.0069 | -0.0110 | 0.2423 | ✗ | 0.131 | 0.136 | 0.184 | -0.0046 | -0.139 | ✗ |
| trust_pct_4w_delta | 52 | -0.0088 | -0.0134 | 0.2804 | ✗ | 0.094 | 0.099 | 0.184 | nan | nan | ✗ |
| foreign_net_5d_4w_sum | 53 | 0.0040 | 0.0124 | 0.5821 | ✗ | 0.115 | 0.118 | 0.184 | 0.0102 | 0.576 | ✗ |
| foreign_net_5d_52w_z | 47 | 0.0076 | 0.0134 | 0.2331 | ✗ | 0.309 | 0.290 | 0.184 | 0.0072 | 0.261 | ✗ |
| trust_net_5d_4w_sum | 53 | -0.0043 | -0.0038 | 0.5187 | ✗ | 0.100 | 0.117 | 0.184 | nan | nan | ✗ |
| trust_net_5d_52w_z | 47 | -0.0081 | -0.0079 | 0.4099 | ✗ | 0.204 | 0.198 | 0.184 | -0.0040 | -0.382 | ✗ |
| vol_compression_60_252 | 51 | -0.0191 | -0.0006 | 0.2213 | ✗ | 0.290 | 0.295 | 0.184 | 0.0132 | 0.794 | ✗ |
| price_stability_60_252 | 51 | -0.0175 | -0.0002 | 0.2278 | ✗ | 0.257 | 0.258 | 0.184 | 0.0088 | 0.903 | ✗ |
| vol_price_divergence | 56 | -0.0109 | -0.0212 | 0.3179 | ✗ | 0.111 | 0.134 | 0.184 | -0.0261 | -0.964 | ✗ |
| ma60_slope_20d | 55 | -0.0050 | 0.0074 | 0.7480 | ✗ | 0.385 | 0.356 | 0.184 | 0.0261 | 0.867 | ✗ |
| body_strength_20d | 57 | -0.0186 | -0.0172 | 0.0699 | ✗ | 0.316 | 0.276 | 0.184 | -0.0090 | -0.636 | ✗ |
| upper_half_close_20d_pct | 57 | 0.0507 | 0.0597 | 0.0000 | ✓ | 0.118 | 0.124 | 0.184 | nan | nan | ✗ |
| stealth_volume_20d | 57 | 0.0261 | 0.0312 | 0.0012 | ✓ | 0.216 | 0.221 | 0.184 | 0.0072 | 0.818 | ✗ |
| revenue_score | 57 | 0.0298 | 0.0264 | 0.0001 | ✓ | 0.170 | 0.183 | 0.184 | nan | nan | ✗ |
| revenue_score_3m_delta | 57 | 0.0180 | 0.0207 | 0.0025 | ✓ | 0.167 | 0.175 | 0.184 | nan | nan | ✗ |
| revenue_score_6m_delta | 57 | 0.0313 | 0.0468 | 0.0000 | ✓ | 0.186 | 0.194 | 0.184 | nan | nan | ✗ |
| roe | 57 | 0.0269 | 0.0219 | 0.1281 | ✗ | 0.225 | 0.227 | 0.184 | 0.0033 | -0.382 | ✗ |
| roa | 57 | 0.0111 | 0.0045 | 0.5301 | ✗ | 0.223 | 0.205 | 0.184 | -0.0097 | -0.382 | ✗ |
| gross_margin | 57 | -0.0125 | -0.0203 | 0.2803 | ✗ | 0.205 | 0.192 | 0.184 | -0.0113 | -0.673 | ✗ |
| op_margin | 57 | 0.0236 | 0.0194 | 0.1245 | ✗ | 0.204 | 0.189 | 0.184 | -0.0116 | -0.224 | ✗ |
| debt_ratio | 57 | 0.0502 | 0.0605 | 0.0000 | ✓ | 0.037 | 0.042 | 0.184 | 0.0203 | 0.879 | ✓ |
| gross_margin_4q_delta | 57 | -0.0060 | -0.0169 | 0.5804 | ✗ | 0.237 | 0.231 | 0.184 | 0.0010 | 0.236 | ✗ |
| op_margin_4q_delta | 57 | -0.0239 | -0.0355 | 0.0290 | ✗ | 0.216 | 0.218 | 0.184 | -0.0048 | -0.115 | ✗ |
| roe_4q_delta | 57 | -0.0297 | -0.0442 | 0.0137 | ✓ | 0.246 | 0.228 | 0.184 | -0.0039 | -0.224 | ✗ |
| eps_yoy | 57 | 0.0011 | -0.0074 | 0.9251 | ✗ | 0.226 | 0.223 | 0.184 | 0.0091 | 0.709 | ✗ |
| sector_return_60d | 54 | 0.0150 | 0.0149 | 0.3787 | ✗ | 0.219 | 0.220 | 0.184 | 0.0259 | 0.939 | ✗ |
| sector_return_120d | 51 | -0.0161 | -0.0029 | 0.2833 | ✗ | 0.169 | 0.171 | 0.184 | -0.0276 | -0.127 | ✗ |
| stock_rs_in_sector_60d | 54 | -0.0142 | -0.0006 | 0.2682 | ✗ | 0.398 | 0.387 | 0.184 | 0.0158 | 0.939 | ✗ |
| sector_momentum_rank | 54 | 0.0150 | 0.0149 | 0.3787 | ✗ | 0.219 | 0.220 | 0.184 | 0.0259 | 0.939 | ✗ |
| cfo_to_revenue | 57 | 0.0128 | 0.0125 | 0.2095 | ✗ | 0.177 | 0.174 | 0.184 | -0.0035 | -0.103 | ✗ |
| cfo_to_ni | 57 | 0.0005 | 0.0030 | 0.9509 | ✗ | 0.182 | 0.196 | 0.184 | 0.0034 | 0.212 | ✗ |
| fcf_to_revenue | 57 | -0.0129 | -0.0294 | 0.4066 | ✗ | 0.265 | 0.232 | 0.184 | -0.0051 | -0.430 | ✗ |
| capex_intensity | 57 | -0.0319 | -0.0381 | 0.0217 | ✓ | 0.193 | 0.202 | 0.184 | -0.0107 | -0.600 | ✓ |
| interest_coverage | 0 | nan | nan | nan | ✗ | nan | nan | 0.184 | nan | nan | ✗ |
| cfo_to_revenue_4q_delta | 57 | -0.0038 | -0.0123 | 0.5742 | ✗ | 0.200 | 0.176 | 0.184 | -0.0089 | 0.394 | ✗ |
| fcf_to_revenue_4q_delta | 57 | -0.0140 | -0.0176 | 0.2174 | ✗ | 0.218 | 0.236 | 0.184 | -0.0160 | -0.030 | ✗ |
| f_score_4q_delta | 57 | 0.0077 | 0.0080 | 0.3472 | ✗ | 0.225 | 0.243 | 0.184 | nan | nan | ✗ |
| f_score_1q_delta | 57 | 0.0116 | 0.0256 | 0.0702 | ✗ | 0.218 | 0.215 | 0.184 | nan | nan | ✗ |
| composite_score | 57 | 0.1154 | 0.1400 | 0.0000 | ✓ | 0.111 | 0.118 | 0.184 | 0.0455 | 0.976 | ✓ |
| composite_wf_score | 57 | 0.0051 | -0.0015 | 0.3181 | ✗ | 0.198 | 0.183 | 0.184 | 0.0043 | 0.539 | ✗ |
| composite_parsi | 57 | 0.0565 | 0.0732 | 0.0001 | ✓ | 0.219 | 0.243 | 0.184 | 0.0425 | 0.964 | ✓ |

## Stage 7 — Walk-forward + LOOY + Cross-regime (top features)

| Feature | WF IC mean | WF pos% | N wins | LOOY min | LOOY max | LOOY range | Bull IC | Bear IC | Sideways IC |
|---|---|---|---|---|---|---|---|---|---|
| composite_score | 0.0913 | 1.00 | 6 | 0.1090 | 0.1261 | 0.0171 | 0.1259 | 0.1197 | 0.1010 |
| dist_52w_high | -0.0529 | 0.33 | 6 | -0.0851 | -0.0513 | 0.0337 | -0.0677 | -0.0611 | -0.0795 |
| low52w_prox_adj | -0.0358 | 0.33 | 6 | -0.0892 | -0.0505 | 0.0387 | -0.1118 | -0.0916 | 0.0014 |
| composite_parsi | 0.0396 | 0.83 | 6 | 0.0432 | 0.0717 | 0.0285 | 0.0547 | 0.0710 | 0.0503 |
| close_to_ma60 | -0.0128 | 0.50 | 6 | -0.0181 | 0.0052 | 0.0234 | -0.0349 | -0.0022 | 0.0153 |
| close_to_ma240 | -0.0190 | 0.17 | 6 | -0.0484 | -0.0024 | 0.0460 | -0.0783 | -0.0089 | 0.0232 |
| stock_rs_in_sector_60d | -0.0177 | 0.50 | 6 | -0.0219 | -0.0036 | 0.0183 | -0.0341 | -0.0091 | 0.0028 |
| composite_wf_score | 0.0047 | 0.67 | 6 | 0.0033 | 0.0091 | 0.0058 | 0.0067 | 0.0121 | -0.0006 |

## Stage 8 — Portfolio simulator (top-K equal weight monthly, fwd_20d hold)


**B&H TWII baseline**: CAGR 11.47% / Sharpe 0.73 / MDD -28.92%

| Strategy | N periods | Total ret | CAGR | Vol | Sharpe | MDD | Win rate |
|---|---|---|---|---|---|---|---|
| B&H TWII | 59 | 70.56% | 11.47% | 16.79% | 0.73 | -28.92% | 61.02% |
| top-10 composite_score | 57 | 153.68% | 21.65% | 13.61% | 1.52 | -9.02% | 71.93% |
| top-10 dist_52w_high | 51 | -8.52% | -2.07% | 35.15% | 0.10 | -37.31% | 43.14% |
| top-10 low52w_prox_adj | 57 | 191.46% | 25.26% | 40.28% | 0.76 | -51.73% | 59.65% |
| top-10 composite_parsi | 57 | 91.36% | 14.64% | 26.77% | 0.64 | -35.51% | 50.88% |
| top-10 close_to_ma60 | 56 | 158.76% | 22.60% | 44.26% | 0.68 | -47.73% | 57.14% |
| top-10 close_to_ma240 | 51 | 5.89% | 1.35% | 37.12% | 0.22 | -42.08% | 45.10% |
| top-10 stock_rs_in_sector_60d | 54 | 55.84% | 10.36% | 43.23% | 0.43 | -43.38% | 51.85% |
| top-10 composite_wf_score | 57 | 23.75% | 4.59% | 21.63% | 0.31 | -36.26% | 54.39% |
| top-20 composite_score | 57 | 131.32% | 19.31% | 12.70% | 1.46 | -12.68% | 63.16% |
| top-20 dist_52w_high | 51 | -3.86% | -0.92% | 30.21% | 0.11 | -41.59% | 37.25% |
| top-20 low52w_prox_adj | 57 | 93.05% | 14.85% | 33.51% | 0.58 | -51.99% | 57.89% |
| top-20 composite_parsi | 57 | 155.67% | 21.85% | 21.81% | 1.02 | -19.09% | 57.89% |
| top-20 close_to_ma60 | 56 | 44.81% | 8.26% | 39.04% | 0.39 | -42.05% | 58.93% |
| top-20 close_to_ma240 | 51 | 65.24% | 12.54% | 31.92% | 0.53 | -33.71% | 52.94% |
| top-20 stock_rs_in_sector_60d | 54 | 61.66% | 11.26% | 34.94% | 0.48 | -35.48% | 53.70% |
| top-20 composite_wf_score | 57 | 12.13% | 2.44% | 18.62% | 0.22 | -29.02% | 54.39% |
| top-5 composite_parsi (K-grid) | 57 | 113.59% | 17.32% | 28.65% | 0.70 | -38.72% | 54.39% |
| top-15 composite_parsi (K-grid) | 57 | 154.94% | 21.78% | 24.44% | 0.93 | -25.84% | 59.65% |
| top-25 composite_parsi (K-grid) | 57 | 184.04% | 24.58% | 19.88% | 1.21 | -15.18% | 57.89% |
| top-30 composite_parsi (K-grid) | 57 | 171.40% | 23.39% | 18.69% | 1.22 | -15.92% | 63.16% |
| top-50 composite_parsi (K-grid) | 57 | 110.03% | 16.91% | 18.99% | 0.92 | -21.43% | 57.89% |
| top-5 composite_score (K-grid) | 57 | 156.78% | 21.96% | 19.28% | 1.12 | -14.31% | 61.40% |
| top-15 composite_score (K-grid) | 57 | 137.29% | 19.95% | 13.64% | 1.41 | -11.54% | 64.91% |
| top-25 composite_score (K-grid) | 57 | 128.82% | 19.04% | 12.87% | 1.43 | -14.50% | 61.40% |
| top-30 composite_score (K-grid) | 57 | 129.44% | 19.10% | 12.56% | 1.46 | -15.29% | 63.16% |
| top-50 composite_score (K-grid) | 57 | 116.15% | 17.62% | 12.88% | 1.33 | -17.16% | 66.67% |

## Kill criteria recap
- Decile monotonicity |Spearman| ≥ 0.5 ✓
- Decile spread (Q10-Q1) sign matches IC sign ✓
- |IC_60d| ≥ 0.03
- BH-FDR alpha=0.10 ✓

## Final Verdict

- Univariate kill + FDR passed: **6/59 features**
- Stage 8 portfolio sim beat B&H (CAGR > 11.47%) AND Sharpe > 0.3: **18/26 strategies**

**Phase 2 Verdict: PROMISING** — 18 strategies pass SOP-10 portfolio gate

Winners:
- top-10 composite_score: CAGR 21.65% / Sharpe 1.52 / MDD -9.02%
- top-10 low52w_prox_adj: CAGR 25.26% / Sharpe 0.76 / MDD -51.73%
- top-10 composite_parsi: CAGR 14.64% / Sharpe 0.64 / MDD -35.51%
- top-10 close_to_ma60: CAGR 22.60% / Sharpe 0.68 / MDD -47.73%
- top-20 composite_score: CAGR 19.31% / Sharpe 1.46 / MDD -12.68%
- top-20 low52w_prox_adj: CAGR 14.85% / Sharpe 0.58 / MDD -51.99%
- top-20 composite_parsi: CAGR 21.85% / Sharpe 1.02 / MDD -19.09%
- top-20 close_to_ma240: CAGR 12.54% / Sharpe 0.53 / MDD -33.71%
- top-5 composite_parsi (K-grid): CAGR 17.32% / Sharpe 0.70 / MDD -38.72%
- top-15 composite_parsi (K-grid): CAGR 21.78% / Sharpe 0.93 / MDD -25.84%
- top-25 composite_parsi (K-grid): CAGR 24.58% / Sharpe 1.21 / MDD -15.18%
- top-30 composite_parsi (K-grid): CAGR 23.39% / Sharpe 1.22 / MDD -15.92%
- top-50 composite_parsi (K-grid): CAGR 16.91% / Sharpe 0.92 / MDD -21.43%
- top-5 composite_score (K-grid): CAGR 21.96% / Sharpe 1.12 / MDD -14.31%
- top-15 composite_score (K-grid): CAGR 19.95% / Sharpe 1.41 / MDD -11.54%
- top-25 composite_score (K-grid): CAGR 19.04% / Sharpe 1.43 / MDD -14.50%
- top-30 composite_score (K-grid): CAGR 19.10% / Sharpe 1.46 / MDD -15.29%
- top-50 composite_score (K-grid): CAGR 17.62% / Sharpe 1.33 / MDD -17.16%

## Caveats
- Stage 8 uses fwd_20d hold-to-month-end as simple proxy. Realistic exit (trailing stop / take profit) not modeled.
- Survivorship: universe_tw 1972 excludes 下市 stocks → over-states alpha.
- 4/5 years bullish (2021/2023/2024/2025 vs 2022) → cross-regime split essential to detect time-period dependency.
- TDCC 集中度 Δ features absent (deferred to Phase 3 per SPEC §5).