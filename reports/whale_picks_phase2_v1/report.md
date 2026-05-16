# Whale Picks Phase 2 — IC Backtest Report

**Universe**: TW 1943 stocks / **Period**: 2021-01-01 ~ 2025-12-31 / **Pipeline**: stages 3-6+9 (MVP)

**Methodology**: per docs/whale_picks_spec.md v0.2 council verdict

## Stage 4+5+6 Combined Verdict

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

## Kill criteria
- Decile monotonicity |Spearman| ≥ 0.5 ✓
- Decile spread (Q10-Q1) sign matches IC sign ✓
- |IC_60d| ≥ 0.03
- p_value ≤ 0.10 AND BH-FDR alpha=0.10 ✓

## Verdict

**2/14 features pass full pipeline (kill + FDR)**

**Phase 2 MVP Verdict: PARTIAL** — 2 features marginally significant. Need Stage 7 walk-forward + Stage 8 portfolio sim before promotion.

## Top features by P@10 (user intent: 下週看哪幾檔)

| Feature | P@10 | Base | IC_60d | Lift |
|---|---|---|---|---|
| close_to_ma60 | 0.386 | 0.173 | -0.0097 | 2.24x |
| close_to_ma240 | 0.373 | 0.173 | -0.0103 | 2.16x |
| low52w_prox_adj | 0.354 | 0.173 | -0.0668 | 2.05x |
| dist_52w_high | 0.288 | 0.173 | -0.0702 | 1.67x |
| atr_ratio_20_60 | 0.286 | 0.173 | -0.0148 | 1.66x |

## Caveats
- Stage 7 walk-forward not run (MVP scope). v0.2 SPEC requires this before promotion.
- Stage 8 portfolio simulator not run. Per SOP-10 必須 portfolio P&L > B&H baseline 才升 informational tier validated.
- Survivorship: features may show alpha because survivors are over-represented in universe_tw. Should verify with delisted-included universe.
- Sample skew: 2021-2025 includes 2022 bear but 4/5 years bullish. Cross-regime split + 2015-2020 backfill needed (SOP-13).