# Whale Picks Phase 2 — IC Backtest Report (v2 with Stage 7+8)

**Universe**: TW 1520 stocks / **Period**: 2024-01-01 ~ 2024-12-31 / **Pipeline**: stages 3-8 (full minus extensions)

**Methodology**: per docs/whale_picks_spec.md v0.2 council verdict

## Stage 4+5+6 — Univariate selection + Decile kill + FDR

| Feature | N | IC_60d | IC_120d | p_value | FDR ✓ | P@10 | P@20 | base | Q10-Q1 | Mono | Kill ✓ |
|---|---|---|---|---|---|---|---|---|---|---|---|
| low52w_prox_adj | 12 | -0.0699 | -0.1024 | 0.0159 | ✓ | 0.433 | 0.412 | 0.190 | 0.0003 | 0.552 | ✗ |
| dist_52w_high | 6 | -0.0039 | -0.0405 | 0.7434 | ✗ | 0.267 | 0.258 | 0.190 | -0.0175 | -0.164 | ✗ |
| rsi_14 | 12 | 0.0244 | 0.0184 | 0.2092 | ✗ | 0.333 | 0.317 | 0.190 | 0.0307 | 0.721 | ✗ |
| rvol_20 | 12 | 0.0223 | 0.0061 | 0.1064 | ✗ | 0.300 | 0.308 | 0.190 | 0.0148 | 0.648 | ✗ |
| turnover_log | 12 | -0.0556 | -0.0609 | 0.0016 | ✓ | 0.200 | 0.229 | 0.190 | -0.0238 | -0.782 | ✓ |
| close_to_ma60 | 10 | -0.0022 | -0.0153 | 0.9246 | ✗ | 0.380 | 0.415 | 0.190 | 0.0334 | 0.818 | ✗ |
| close_to_ma240 | 6 | -0.1218 | -0.1235 | 0.0007 | ✓ | 0.250 | 0.292 | 0.190 | -0.0291 | -0.806 | ✓ |
| atr_ratio_20_60 | 10 | -0.0087 | -0.0179 | 0.6464 | ✗ | 0.360 | 0.375 | 0.190 | 0.0239 | 0.600 | ✗ |
| foreign_pct | 11 | -0.0015 | -0.0155 | 0.8937 | ✗ | 0.264 | 0.209 | 0.190 | 0.0009 | 0.176 | ✗ |
| total_pct | 11 | -0.0009 | -0.0176 | 0.9413 | ✗ | 0.300 | 0.236 | 0.190 | 0.0032 | 0.333 | ✗ |
| foreign_net_pressure | 11 | -0.0011 | -0.0152 | 0.9183 | ✗ | 0.255 | 0.209 | 0.190 | -0.0002 | 0.188 | ✗ |
| trust_net_pressure | 11 | 0.0096 | 0.0033 | 0.4580 | ✗ | 0.136 | 0.123 | 0.190 | 0.0022 | 0.152 | ✗ |
| total_net_pressure | 11 | -0.0008 | -0.0175 | 0.9500 | ✗ | 0.282 | 0.232 | 0.190 | 0.0056 | 0.248 | ✗ |
| f_score | 12 | 0.0673 | 0.0899 | 0.0022 | ✓ | 0.233 | 0.254 | 0.190 | 0.0523 | 0.564 | ✓ |
| z_score | 12 | 0.0064 | 0.0107 | 0.6833 | ✗ | 0.183 | 0.237 | 0.190 | 0.0107 | -0.067 | ✗ |
| foreign_pct_4w_delta | 10 | 0.0080 | -0.0029 | 0.3337 | ✗ | 0.160 | 0.160 | 0.190 | -0.0003 | -0.297 | ✗ |
| foreign_pct_8w_delta | 10 | 0.0063 | -0.0028 | 0.5865 | ✗ | 0.130 | 0.165 | 0.190 | -0.0151 | 0.212 | ✗ |
| total_pct_4w_delta | 10 | 0.0044 | -0.0046 | 0.6007 | ✗ | 0.180 | 0.175 | 0.190 | -0.0009 | -0.115 | ✗ |
| total_pct_8w_delta | 10 | 0.0058 | -0.0010 | 0.6171 | ✗ | 0.190 | 0.170 | 0.190 | -0.0120 | 0.055 | ✗ |
| trust_pct_4w_delta | 10 | -0.0047 | -0.0087 | 0.6591 | ✗ | 0.110 | 0.115 | 0.190 | 0.0056 | -0.273 | ✗ |
| foreign_net_5d_4w_sum | 11 | 0.0071 | -0.0012 | 0.5676 | ✗ | 0.127 | 0.114 | 0.190 | 0.0164 | 0.261 | ✗ |
| foreign_net_5d_52w_z | 6 | -0.0240 | -0.0316 | 0.1044 | ✗ | 0.250 | 0.258 | 0.190 | -0.0100 | -0.297 | ✗ |
| trust_net_5d_4w_sum | 11 | -0.0069 | -0.0104 | 0.6210 | ✗ | 0.118 | 0.127 | 0.190 | -0.0056 | 0.067 | ✗ |
| trust_net_5d_52w_z | 6 | -0.0029 | 0.0052 | 0.8602 | ✗ | 0.217 | 0.192 | 0.190 | -0.0016 | -0.273 | ✗ |
| vol_compression_60_252 | 6 | -0.1096 | -0.1325 | 0.0000 | ✓ | 0.233 | 0.242 | 0.190 | -0.0292 | -0.770 | ✓ |
| price_stability_60_252 | 6 | -0.0524 | -0.0723 | 0.0061 | ✓ | 0.150 | 0.142 | 0.190 | -0.0173 | -0.891 | ✓ |
| vol_price_divergence | 10 | -0.0029 | 0.0007 | 0.9002 | ✗ | 0.210 | 0.195 | 0.190 | -0.0166 | -0.709 | ✗ |
| ma60_slope_20d | 9 | -0.0444 | -0.0704 | 0.1425 | ✗ | 0.400 | 0.328 | 0.190 | 0.0105 | 0.273 | ✗ |
| body_strength_20d | 11 | -0.0012 | 0.0013 | 0.8889 | ✗ | 0.382 | 0.341 | 0.190 | -0.0046 | 0.103 | ✗ |
| upper_half_close_20d_pct | 12 | 0.0389 | 0.0417 | 0.0042 | ✓ | 0.133 | 0.179 | 0.190 | 0.0166 | 0.842 | ✓ |
| stealth_volume_20d | 11 | 0.0174 | 0.0248 | 0.0611 | ✗ | 0.264 | 0.255 | 0.190 | 0.0026 | 0.370 | ✗ |
| revenue_score | 12 | 0.0057 | 0.0137 | 0.5364 | ✗ | 0.200 | 0.200 | 0.190 | 0.0018 | 0.515 | ✗ |
| revenue_score_3m_delta | 12 | -0.0063 | 0.0101 | 0.3416 | ✗ | 0.200 | 0.213 | 0.190 | 0.0111 | -0.042 | ✗ |
| revenue_score_6m_delta | 12 | 0.0149 | 0.0166 | 0.0551 | ✗ | 0.125 | 0.129 | 0.190 | -0.0034 | 0.200 | ✗ |
| roe | 12 | 0.0731 | 0.1059 | 0.0138 | ✓ | 0.258 | 0.238 | 0.190 | 0.0321 | 0.503 | ✓ |
| roa | 12 | 0.0708 | 0.1038 | 0.0211 | ✓ | 0.250 | 0.204 | 0.190 | 0.0246 | 0.539 | ✓ |
| gross_margin | 12 | 0.0407 | 0.0532 | 0.0018 | ✓ | 0.217 | 0.204 | 0.190 | 0.0336 | 0.721 | ✓ |
| op_margin | 12 | 0.0647 | 0.0931 | 0.0017 | ✓ | 0.208 | 0.225 | 0.190 | 0.0356 | 0.515 | ✓ |
| debt_ratio | 12 | 0.0043 | 0.0039 | 0.7477 | ✗ | 0.225 | 0.146 | 0.190 | -0.0203 | -0.030 | ✗ |
| gross_margin_4q_delta | 12 | 0.0065 | 0.0033 | 0.6429 | ✗ | 0.242 | 0.233 | 0.190 | -0.0183 | 0.079 | ✗ |
| op_margin_4q_delta | 12 | 0.0015 | 0.0019 | 0.9169 | ✗ | 0.275 | 0.225 | 0.190 | -0.0055 | 0.200 | ✗ |
| roe_4q_delta | 12 | -0.0050 | 0.0032 | 0.7209 | ✗ | 0.175 | 0.200 | 0.190 | -0.0066 | -0.261 | ✗ |
| eps_yoy | 12 | -0.0023 | 0.0069 | 0.9040 | ✗ | 0.267 | 0.279 | 0.190 | 0.0113 | 0.709 | ✗ |
| sector_return_60d | 9 | 0.0133 | 0.0518 | 0.6189 | ✗ | 0.144 | 0.144 | 0.190 | nan | nan | ✗ |
| sector_return_120d | 6 | 0.0031 | -0.0012 | 0.9178 | ✗ | 0.200 | 0.150 | 0.190 | nan | nan | ✗ |
| stock_rs_in_sector_60d | 9 | -0.0254 | -0.0449 | 0.3683 | ✗ | 0.400 | 0.356 | 0.190 | 0.0189 | 0.697 | ✗ |
| sector_momentum_rank | 9 | 0.0277 | 0.0337 | 0.0580 | ✗ | 0.200 | 0.161 | 0.190 | nan | nan | ✗ |
| cfo_to_revenue | 12 | 0.0706 | 0.0830 | 0.0000 | ✓ | 0.183 | 0.200 | 0.190 | 0.0241 | 0.879 | ✓ |
| cfo_to_ni | 12 | 0.0246 | 0.0265 | 0.0663 | ✗ | 0.300 | 0.279 | 0.190 | 0.0083 | 0.576 | ✗ |
| fcf_to_revenue | 12 | 0.0337 | 0.0388 | 0.2525 | ✗ | 0.283 | 0.221 | 0.190 | 0.0291 | 0.152 | ✗ |
| capex_intensity | 12 | 0.0410 | 0.0187 | 0.1815 | ✗ | 0.175 | 0.221 | 0.190 | -0.0116 | 0.345 | ✗ |
| interest_coverage | 0 | nan | nan | nan | ✗ | nan | nan | 0.190 | nan | nan | ✗ |
| cfo_to_revenue_4q_delta | 12 | 0.0145 | 0.0125 | 0.0763 | ✗ | 0.250 | 0.225 | 0.190 | 0.0101 | 0.539 | ✗ |
| fcf_to_revenue_4q_delta | 12 | 0.0252 | 0.0206 | 0.1756 | ✗ | 0.308 | 0.279 | 0.190 | 0.0861 | -0.079 | ✗ |
| f_score_4q_delta | 12 | 0.0168 | 0.0137 | 0.3387 | ✗ | 0.325 | 0.300 | 0.190 | 0.0189 | 0.733 | ✗ |
| f_score_1q_delta | 12 | -0.0017 | 0.0149 | 0.8793 | ✗ | 0.217 | 0.263 | 0.190 | 0.0098 | 0.139 | ✗ |
| composite_score | 12 | 0.1314 | 0.1576 | 0.0000 | ✓ | 0.258 | 0.217 | 0.190 | 0.0670 | 0.952 | ✓ |
| composite_wf_score | 0 | nan | nan | nan | ✗ | nan | nan | 0.190 | nan | nan | ✗ |
| composite_parsi | 12 | 0.0563 | 0.0731 | 0.0020 | ✓ | 0.250 | 0.238 | 0.190 | 0.0279 | 0.903 | ✓ |

## Stage 7 — Walk-forward + LOOY + Cross-regime (top features)

| Feature | WF IC mean | WF pos% | N wins | LOOY min | LOOY max | LOOY range | Bull IC | Bear IC | Sideways IC |
|---|---|---|---|---|---|---|---|---|---|
| composite_score | nan | nan | 0 | nan | nan | nan | 0.1314 | nan | nan |
| close_to_ma240 | nan | nan | 0 | nan | nan | nan | -0.1218 | nan | nan |
| vol_compression_60_252 | nan | nan | 0 | nan | nan | nan | -0.1096 | nan | nan |
| roe | nan | nan | 0 | nan | nan | nan | 0.0731 | nan | nan |
| low52w_prox_adj | nan | nan | 0 | nan | nan | nan | -0.0699 | nan | nan |
| ma60_slope_20d | nan | nan | 0 | nan | nan | nan | -0.0444 | nan | nan |
| stock_rs_in_sector_60d | nan | nan | 0 | nan | nan | nan | -0.0254 | nan | nan |
| body_strength_20d | nan | nan | 0 | nan | nan | nan | -0.0012 | nan | nan |
| composite_wf_score | nan | nan | 0 | nan | nan | nan | nan | nan | nan |
| composite_parsi | nan | nan | 0 | nan | nan | nan | 0.0563 | nan | nan |

## Stage 8 — Portfolio simulator (top-K equal weight monthly, fwd_20d hold)


**B&H TWII baseline**: CAGR 34.47% / Sharpe 1.89 / MDD -8.88%

| Strategy | N periods | Total ret | CAGR | Vol | Sharpe | MDD | Win rate |
|---|---|---|---|---|---|---|---|
| B&H TWII | 11 | 31.19% | 34.47% | 16.50% | 1.89 | -8.88% | 72.73% |
| top-10 composite_score | 12 | -1.11% | -1.11% | 18.92% | 0.03 | -23.78% | 50.00% |
| top-10 close_to_ma240 | 6 | -15.50% | -28.60% | 23.71% | -1.30 | -14.27% | 33.33% |
| top-10 vol_compression_60_252 | 6 | -16.60% | -30.44% | 23.69% | -1.41 | -11.90% | 16.67% |
| top-10 roe | 12 | 33.50% | 33.50% | 21.88% | 1.43 | -11.63% | 66.67% |
| top-10 low52w_prox_adj | 12 | 37.58% | 37.58% | 34.99% | 1.08 | -15.76% | 66.67% |
| top-10 ma60_slope_20d | 9 | -4.89% | -6.47% | 31.61% | -0.07 | -14.26% | 44.44% |
| top-10 stock_rs_in_sector_60d | 9 | 12.26% | 16.67% | 32.34% | 0.61 | -15.50% | 44.44% |
| top-10 body_strength_20d | 11 | 27.77% | 30.65% | 38.79% | 0.86 | -19.60% | 54.55% |
| top-10 composite_parsi | 12 | 47.50% | 47.50% | 25.16% | 1.68 | -10.99% | 66.67% |
| top-20 composite_score | 12 | 1.38% | 1.38% | 14.57% | 0.16 | -17.30% | 58.33% |
| top-20 close_to_ma240 | 6 | -16.04% | -29.51% | 11.73% | -2.89 | -15.91% | 16.67% |
| top-20 vol_compression_60_252 | 6 | -21.72% | -38.72% | 21.32% | -2.16 | -15.42% | 16.67% |
| top-20 roe | 12 | 20.92% | 20.92% | 14.20% | 1.41 | -5.39% | 66.67% |
| top-20 low52w_prox_adj | 12 | 31.17% | 31.17% | 23.95% | 1.25 | -9.93% | 41.67% |
| top-20 ma60_slope_20d | 9 | -21.43% | -27.50% | 25.66% | -1.12 | -28.52% | 33.33% |
| top-20 stock_rs_in_sector_60d | 9 | -8.26% | -10.86% | 22.76% | -0.41 | -19.71% | 33.33% |
| top-20 body_strength_20d | 11 | 24.50% | 27.00% | 29.37% | 0.95 | -12.13% | 54.55% |
| top-20 composite_parsi | 12 | 38.32% | 38.32% | 21.91% | 1.60 | -7.92% | 66.67% |

## Kill criteria recap
- Decile monotonicity |Spearman| ≥ 0.5 ✓
- Decile spread (Q10-Q1) sign matches IC sign ✓
- |IC_60d| ≥ 0.03
- BH-FDR alpha=0.10 ✓

## Final Verdict

- Univariate kill + FDR passed: **13/59 features**
- Stage 8 portfolio sim beat B&H (CAGR > 34.47%) AND Sharpe > 0.3: **3/18 strategies**

**Phase 2 Verdict: PROMISING** — 3 strategies pass SOP-10 portfolio gate

Winners:
- top-10 low52w_prox_adj: CAGR 37.58% / Sharpe 1.08 / MDD -15.76%
- top-10 composite_parsi: CAGR 47.50% / Sharpe 1.68 / MDD -10.99%
- top-20 composite_parsi: CAGR 38.32% / Sharpe 1.60 / MDD -7.92%

## Caveats
- Stage 8 uses fwd_20d hold-to-month-end as simple proxy. Realistic exit (trailing stop / take profit) not modeled.
- Survivorship: universe_tw 1972 excludes 下市 stocks → over-states alpha.
- 4/5 years bullish (2021/2023/2024/2025 vs 2022) → cross-regime split essential to detect time-period dependency.
- TDCC 集中度 Δ features absent (deferred to Phase 3 per SPEC §5).