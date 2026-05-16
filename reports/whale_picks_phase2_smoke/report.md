# Whale Picks Phase 2 — IC Backtest Report

**Universe**: TW 50 stocks / **Period**: 2022-01-01 ~ 2024-12-31 / **Pipeline**: stages 3-6+9 (MVP)

**Methodology**: per docs/whale_picks_spec.md v0.2 council verdict

## Stage 4+5+6 Combined Verdict

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
| total_pct | 35 | -0.0712 | -0.0439 | 0.0524 | ✓ | 0.046 | 0.051 | 0.057 | -0.0127 | -0.285 | ✗ |
| foreign_net_pressure | 35 | -0.0615 | -0.0373 | 0.0973 | ✗ | 0.046 | 0.051 | 0.057 | -0.0157 | -0.261 | ✗ |
| trust_net_pressure | 35 | -0.0177 | -0.0388 | 0.5105 | ✗ | 0.026 | 0.049 | 0.057 | nan | nan | ✗ |
| f_score | 34 | 0.1406 | 0.1507 | 0.0001 | ✓ | 0.044 | 0.051 | 0.057 | nan | nan | ✗ |
| z_score | 34 | -0.0837 | -0.1163 | 0.0001 | ✓ | 0.029 | 0.041 | 0.057 | -0.0498 | -0.394 | ✗ |

## Kill criteria
- Decile monotonicity |Spearman| ≥ 0.5 ✓
- Decile spread (Q10-Q1) sign matches IC sign ✓
- |IC_60d| ≥ 0.03
- p_value ≤ 0.10 AND BH-FDR alpha=0.10 ✓

## Verdict

**5/14 features pass full pipeline (kill + FDR)**

**Phase 2 MVP Verdict: PROMISING** — 5 features pass. Proceed to Stage 7 walk-forward + Stage 8 portfolio sim.

## Top features by P@10 (user intent: 下週看哪幾檔)

| Feature | P@10 | Base | IC_60d | Lift |
|---|---|---|---|---|
| low52w_prox_adj | 0.144 | 0.057 | 0.0442 | 2.56x |
| close_to_ma240 | 0.143 | 0.057 | 0.1792 | 2.54x |
| close_to_ma60 | 0.120 | 0.057 | 0.0845 | 2.12x |
| rsi_14 | 0.108 | 0.057 | 0.0677 | 1.92x |
| atr_ratio_20_60 | 0.083 | 0.057 | -0.0292 | 1.47x |

## Caveats
- Stage 7 walk-forward not run (MVP scope). v0.2 SPEC requires this before promotion.
- Stage 8 portfolio simulator not run. Per SOP-10 必須 portfolio P&L > B&H baseline 才升 informational tier validated.
- Survivorship: features may show alpha because survivors are over-represented in universe_tw. Should verify with delisted-included universe.
- Sample skew: 2021-2025 includes 2022 bear but 4/5 years bullish. Cross-regime split + 2015-2020 backfill needed (SOP-13).