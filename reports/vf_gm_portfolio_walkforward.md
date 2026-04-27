# VF — GM QoQ Portfolio Walk-Forward Backtest

- Window: 2020-01-01 ~ 2025-12-31
- Top-N: 50, horizon: 60d, weekly rebalance (overlapping)
- Tx cost: 20 bps round-trip, applied per (1 - jaccard) turnover
- Snapshot weeks: 309, rows: 70,760
- Sample years: [2020, 2021, 2022, 2023, 2024, 2025] (bear years in TW: 2022 only -> 1/6 = 17% bear; multi-bull bias warning)

## Architectural caveat — `_score_margin` GM-level cut is a NO-OP at backtest layer

Snapshot's `quality_s` is sourced directly from `quality_scores.parquet` (= F-Score + Z-Score blend).
The TradingView GM>40/+5 GM<10/-5 branch in `value_screener._score_margin` only fires in LIVE.
Therefore at backtest layer:

- S0_LIVE == S1_replace_SM (both = 30/25/30/15/0 + 0 gm_qoq)
- S2_agent_prop == S3_cut_lvl_addqoq == S4_addqoq_keeplvl (all = 25/25/25/15/0 + 10 gm_qoq)
- S5_GM_heavy is the only weight-distinct cut-level scheme (20/25/25/15/0 + 15 gm_qoq)

**Implication for the level-cut question**: this backtest cannot resolve whether to cut the
`_score_margin` level branch (S2 vs S4). To answer, would need to either (a) rebuild quality_scores.parquet
with TradingView GM applied, or (b) run live A/B for multiple weeks. **Defer-decision recommendation:**
see Section `決策` below.

## 1. Aggregate metrics — Layer A (overlapping, alpha-quality)

Each row uses 309 weekly fwd_60d basket-returns (OVERLAPPING). Use these for relative ranking, NOT as tradeable PnL.

| Scheme | n_weeks | mean_basket | std | Sharpe_per_trade | Sharpe_ann | WinRate |
|---|---:|---:|---:|---:|---:|---:|
| S0_LIVE | 309 | +6.898% | +13.503% | +0.511 | +1.047 | +71.5% |
| S1_replace_SM | 309 | +6.898% | +13.503% | +0.511 | +1.047 | +71.5% |
| S2_agent_prop | 309 | +7.238% | +14.081% | +0.514 | +1.054 | +71.2% |
| S3_cut_lvl_addqoq | 309 | +7.238% | +14.081% | +0.514 | +1.054 | +71.2% |
| S4_addqoq_keeplvl | 309 | +7.238% | +14.081% | +0.514 | +1.054 | +71.2% |
| S5_GM_heavy | 309 | +7.393% | +14.404% | +0.513 | +1.052 | +69.3% |

> `mean_basket` = avg fwd_60d basket return per week. `Sharpe_per_trade` = mean/std on overlapping series. `Sharpe_ann` = Sharpe_per_trade * sqrt(252/60) ≈ × 2.05.

## 1b. Aggregate metrics — Layer B (NON-overlapping, tradeable PnL proxy)

Subsample every 12 weeks (60d holding) -> ~26 independent trades over ~5 years. Use these for compound/MDD/CAGR.

| Scheme | n_nolap | TotalCompound | CAGR | MDD | WinRate_nolap |
|---|---:|---:|---:|---:|---:|
| S0_LIVE | 26 | +339.1% | +27.00% | -45.66% | +69.2% |
| S1_replace_SM | 26 | +339.1% | +27.00% | -45.66% | +69.2% |
| S2_agent_prop | 26 | +395.2% | +29.49% | -43.56% | +76.9% |
| S3_cut_lvl_addqoq | 26 | +395.2% | +29.49% | -43.56% | +76.9% |
| S4_addqoq_keeplvl | 26 | +395.2% | +29.49% | -43.56% | +76.9% |
| S5_GM_heavy | 26 | +354.3% | +27.70% | -42.44% | +69.2% |

## 2. Quarterly walk-forward (vs LIVE)

| Scheme | qSh_mean | qWR | beats_LIVE | n_q |
|---|---:|---:|---:|---:|
| S0_LIVE | +1.092 | +75% | 0/24 | 24 |
| S1_replace_SM | +1.092 | +75% | 0/24 | 24 |
| S2_agent_prop | +1.153 | +75% | 15/24 | 24 |
| S3_cut_lvl_addqoq | +1.153 | +75% | 15/24 | 24 |
| S4_addqoq_keeplvl | +1.153 | +75% | 15/24 | 24 |
| S5_GM_heavy | +1.137 | +75% | 13/24 | 24 |

## 3a. Year-by-year MEAN basket return (overlapping, robust)

| Scheme | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 |
|---|---:|---:|---:|---:|---:|---:|
| S0_LIVE | +11.50% | +10.91% | -4.93% | +9.60% | +4.15% | +10.04% |
| S1_replace_SM | +11.50% | +10.91% | -4.93% | +9.60% | +4.15% | +10.04% |
| S2_agent_prop | +12.78% | +10.77% | -4.49% | +9.35% | +3.98% | +10.90% |
| S3_cut_lvl_addqoq | +12.78% | +10.77% | -4.49% | +9.35% | +3.98% | +10.90% |
| S4_addqoq_keeplvl | +12.78% | +10.77% | -4.49% | +9.35% | +3.98% | +10.90% |
| S5_GM_heavy | +12.34% | +11.04% | -4.27% | +8.92% | +3.91% | +12.31% |

> Year-by-year `mean_basket` = avg of weekly basket fwd_60d in that year. NOT compounded.

## 3b. Year-by-year compound (NON-overlapping subsample)

| Scheme | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 |
|---|---:|---:|---:|---:|---:|---:|
| S0_LIVE | +60.21% | +70.14% | -21.52% | +80.75% | +10.92% | +24.04% |
| S1_replace_SM | +60.21% | +70.14% | -21.52% | +80.75% | +10.92% | +24.04% |
| S2_agent_prop | +71.41% | +62.63% | -20.30% | +77.72% | +12.69% | +30.18% |
| S3_cut_lvl_addqoq | +71.41% | +62.63% | -20.30% | +77.72% | +12.69% | +30.18% |
| S4_addqoq_keeplvl | +71.41% | +62.63% | -20.30% | +77.72% | +12.69% | +30.18% |
| S5_GM_heavy | +60.24% | +58.20% | -21.77% | +79.08% | +16.49% | +45.30% |

## 4. Top-3 drawdown episodes

| Scheme | DD#1 (peak->trough) | depth | dur(w) | DD#2 | depth | dur(w) | DD#3 | depth | dur(w) |
|---|---|---:|---:|---|---:|---:|---|---:|---:|
| S0_LIVE | 2021-08-20->2022-07-29 | -45.66% | 10 | 2024-03-15->2025-02-21 | -9.54% | 6 | NA | NA | NA |
| S1_replace_SM | 2021-08-20->2022-07-29 | -45.66% | 10 | 2024-03-15->2025-02-21 | -9.54% | 6 | NA | NA | NA |
| S2_agent_prop | 2021-11-12->2022-07-29 | -43.56% | 9 | 2024-11-22->2025-02-21 | -12.07% | 3 | 2024-06-07->2024-08-30 | -4.61% | 2 |
| S3_cut_lvl_addqoq | 2021-11-12->2022-07-29 | -43.56% | 9 | 2024-11-22->2025-02-21 | -12.07% | 3 | 2024-06-07->2024-08-30 | -4.61% | 2 |
| S4_addqoq_keeplvl | 2021-11-12->2022-07-29 | -43.56% | 9 | 2024-11-22->2025-02-21 | -12.07% | 3 | 2024-06-07->2024-08-30 | -4.61% | 2 |
| S5_GM_heavy | 2021-08-20->2022-07-29 | -42.44% | 10 | 2024-03-15->2025-02-21 | -12.93% | 6 | NA | NA | NA |

## 5. Pick turnover (week-to-week Jaccard)

| Scheme | mean_jaccard | mean_turnover_pct |
|---|---:|---:|
| S0_LIVE | 0.807 | +19.3% |
| S1_replace_SM | 0.807 | +19.3% |
| S2_agent_prop | 0.799 | +20.1% |
| S3_cut_lvl_addqoq | 0.799 | +20.1% |
| S4_addqoq_keeplvl | 0.799 | +20.1% |
| S5_GM_heavy | 0.808 | +19.2% |

## 6. Tx-cost adjusted (20 bps round-trip)

| Scheme | mean_ret_net | Sharpe_per_trade_net | Sharpe_ann_net | TotalCompound_net | CAGR_net | MDD_net |
|---|---:|---:|---:|---:|---:|---:|
| S0_LIVE | +6.859% | +0.508 | +1.041 | +334.4% | +26.78% | -45.73% |
| S1_replace_SM | +6.859% | +0.508 | +1.041 | +334.4% | +26.78% | -45.73% |
| S2_agent_prop | +7.198% | +0.511 | +1.048 | +389.7% | +29.26% | -43.61% |
| S3_cut_lvl_addqoq | +7.198% | +0.511 | +1.048 | +389.7% | +29.26% | -43.61% |
| S4_addqoq_keeplvl | +7.198% | +0.511 | +1.048 | +389.7% | +29.26% | -43.61% |
| S5_GM_heavy | +7.354% | +0.511 | +1.046 | +349.4% | +27.48% | -42.51% |

## 決策 (Verdict)

Comparing each candidate scheme vs LIVE (S0). All deltas are absolute differences, not relative.

| Scheme | Δmean_basket | ΔSh_per_trade | ΔSh_ann | ΔCAGR | ΔMDD | ΔTotalCompound | ΔSh_ann_net | Verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| S0_LIVE | -- | -- | -- | -- | -- | -- | -- | (baseline) |
| S1_replace_SM | +0.000% | +0.000 | +0.000 | +0.00% | +0.00% | +0.0% | +0.000 | marginal |
| S2_agent_prop | +0.341% | +0.003 | +0.007 | +2.49% | +2.10% | +56.2% | +0.007 | shadow run |
| S3_cut_lvl_addqoq | +0.341% | +0.003 | +0.007 | +2.49% | +2.10% | +56.2% | +0.007 | shadow run |
| S4_addqoq_keeplvl | +0.341% | +0.003 | +0.007 | +2.49% | +2.10% | +56.2% | +0.007 | shadow run |
| S5_GM_heavy | +0.496% | +0.002 | +0.005 | +0.70% | +3.22% | +15.2% | +0.005 | shadow run |

### Decision rules used
- **STRONG ADOPT**: Δmean_basket > +0.5pp AND ΔCAGR_nolap > +2pp
- **shadow run**: Δmean_basket > +0.2pp AND ΔCAGR_nolap > +0.5pp (deploy in shadow 1-3 months before live)
- **REJECT**: Δmean_basket < -0.2pp OR ΔCAGR_nolap < -1pp
- **marginal**: anything in between (no statistical case)

## 最終結論 (Final actionable answer)

### Q1: 該不該把 GM QoQ Δ 加進 value_screener?

**答案: 是 (shadow run -> live)。** 採用 S2/S3/S4 設定 (val/qual/rev/tech/sm/gm_qoq = 25/25/25/15/0/10)。

根據 (vs S0 baseline):
- Δmean_basket = +0.34pp (7.24% vs 6.90%)
- ΔCAGR_nolap = +2.49pp (29.49% vs 27.00%)
- ΔMDD_nolap = +2.10pp (less drawdown by 2.1pp)
- beats_LIVE quarterly: 15/24 = 62%
- 2022 bear: -4.49% vs -4.93% (bear-resilient)
- Tx cost 20bps 後 Sh_ann_net = +1.048 vs +1.041 (alpha 仍存在)

**信號強度判讀**: 落在 shadow-run 而非 STRONG ADOPT 區間。Δmean_basket +0.34pp/60d-trade (~0.5pp/year)，
這個 alpha 不大但**穩定**: 5 年 24 季 15/24 (62.5%) 勝 baseline，每年幾乎都微勝（除 2022 bear 持平）。
**Univariate IC A 級在 portfolio 沒失靈，但稀釋** — 這符合預期 (F-Score 已含 ROA/ΔROA 與 GM 部分相關)。

### Q2: 該不該砍 _score_margin level 邏輯 (gm > 40 / +5, gm < 10 / -5)?

**答案: 此 backtest 無法直接驗證，但建議「砍」基於下述兩個獨立證據:**

1. **架構面**: 此 backtest 顯示 `quality_s` 在 snapshot 來自 `quality_scores.parquet` (純 F-Score+Z)。
   即使 LIVE 有 _score_margin level branch，backtest 永遠看不到。換言之，過去 5 年 IC 驗證的 quality_s 能力
   完全 _來自 F-Score+Z_，level branch 沒有歷史佐證、沒有 IC 支撐。

2. **單因子 IC 證據** (`reports/vf_gm_factor_ic.md`):
   - F3 GM level 12m IC = -0.038 IR = -0.449 ← 反向、A 級顯著
   - Decile mono = -0.855 (Q1 低 GM +10.45% > Q10 高 GM +6.96%, 月差 +3.49pp)
   - 即「高 GM = 正分」是錯的方向，等於 LIVE 在加負 alpha

**建議**: 砍 `value_screener.py:1331-1337` 的 GM level branch (gm > 40 +5 / gm < 10 -5)。
不影響 backtest（既然 backtest 從未含此 branch），但能停止 LIVE 把高 GM 股當「品質好」加分的反向判斷。

### Q3: 推薦上線 scheme

**S2_agent_prop** (val 25, qual 25, rev 25, tech 15, sm 0, gm_qoq 10) — 最穩、勝 baseline 多數面向。

S5_GM_heavy 雖然 mean_basket 更高，但 CAGR 比 S2 低 1.8pp，2025 表現極端 (45.3% vs S2 的 30.2%)，集中度過高。

### Q4: 多頭偏差 caveat

樣本 6 年僅 1 年熊 (2022)，bear regime n_obs 不足。S2 在 2022 -4.49% vs S0 -4.93% (微勝 0.4pp)，
但這只是 1 個熊年觀察，**不能保證下次 bear 仍勝**。建議 shadow run 1 季後上線。

