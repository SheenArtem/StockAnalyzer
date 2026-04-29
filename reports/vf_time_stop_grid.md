# Time Stop Grid Validation

**Source**: tools/vf_time_stop_grid.py (extends vf_dual_portfolio_walkforward.py)

**Hypothesis**: 進場後盤整 N 天無突破無破停損 -> 釋放部位避免機會成本

**Baseline = PRE_POLICY** (b18758d revert 後 live 設定):
- MIN_HOLD = 20 trading days
- TP staged 1/3 at +10%
- Monthly rebalance (4w)
- Regime defer 1mo
- Whipsaw ban 30d (固定)

**Time Stop Grid**: hold_days {5,10,15,20,30} x progress_pct {0,1,2,3}% = 20 cells

Trigger condition: `days_held >= hold_days AND days_held >= MIN_HOLD AND cum_ret < progress_pct`
(MIN_HOLD floor 確保不違反 hard floor; cum_ret 計算到當週 close, 無 look-ahead)

## Baseline Performance (PRE_POLICY no Time Stop, Dual 50/50)

| Period | CAGR % | Sharpe | MDD % | Hit % | Years |
|---|---|---|---|---|---|
| IS_2020_2022 | 14.20 | 1.122 | -11.78 | 42.6 | 2.98 |
| OOS_2023 | 8.45 | 0.738 | -11.03 | 35.3 | 0.98 |
| OOS_2024 | 10.25 | 1.428 | -3.94 | 59.6 | 1.00 |
| OOS_2025 | 7.95 | 0.650 | -8.83 | 41.2 | 0.98 |
| BEAR_2022 | 14.05 | 1.198 | -7.89 | 29.4 | 0.98 |
| FULL_2020_2025 | 12.63 | 0.991 | -11.78 | 43.7 | 5.94 |

## Grid Search Results (Dual side, FULL_2020_2025)

Δ = (cell - baseline). Positive ΔCAGR/ΔSharpe = improvement.
Note: ΔMDD positive 也 = improvement (MDD 是負值, 趨近 0 = 改善).

| Cell | hold_d | prog% | CAGR % | Sharpe | MDD % | Hit % | ΔCAGR | ΔSharpe | ΔMDD |
|---|---|---|---|---|---|---|---|---|---|
| baseline | - | - | 12.63 | 0.991 | -11.78 | 43.7 | - | - | - |
| TS_h5_p0pct | 5 | 0% | 13.20 | 0.987 | -12.64 | 43.4 | +0.57 | -0.004 | -0.86 |
| TS_h5_p1pct | 5 | 1% | 11.35 | 0.865 | -14.82 | 41.7 | -1.28 | -0.126 | -3.04 |
| TS_h5_p2pct | 5 | 2% | 10.62 | 0.790 | -16.15 | 41.4 | -2.01 | -0.201 | -4.37 |
| TS_h5_p3pct | 5 | 3% | 10.20 | 0.759 | -17.09 | 41.7 | -2.43 | -0.232 | -5.31 |
| TS_h10_p0pct | 10 | 0% | 13.20 | 0.987 | -12.64 | 43.4 | +0.57 | -0.004 | -0.86 |
| TS_h10_p1pct | 10 | 1% | 11.35 | 0.865 | -14.82 | 41.7 | -1.28 | -0.126 | -3.04 |
| TS_h10_p2pct | 10 | 2% | 10.62 | 0.790 | -16.15 | 41.4 | -2.01 | -0.201 | -4.37 |
| TS_h10_p3pct | 10 | 3% | 10.20 | 0.759 | -17.09 | 41.7 | -2.43 | -0.232 | -5.31 |
| TS_h15_p0pct | 15 | 0% | 13.20 | 0.987 | -12.64 | 43.4 | +0.57 | -0.004 | -0.86 |
| TS_h15_p1pct | 15 | 1% | 11.35 | 0.865 | -14.82 | 41.7 | -1.28 | -0.126 | -3.04 |
| TS_h15_p2pct | 15 | 2% | 10.62 | 0.790 | -16.15 | 41.4 | -2.01 | -0.201 | -4.37 |
| TS_h15_p3pct | 15 | 3% | 10.20 | 0.759 | -17.09 | 41.7 | -2.43 | -0.232 | -5.31 |
| TS_h20_p0pct | 20 | 0% | 13.20 | 0.987 | -12.64 | 43.4 | +0.57 | -0.004 | -0.86 |
| TS_h20_p1pct | 20 | 1% | 11.35 | 0.865 | -14.82 | 41.7 | -1.28 | -0.126 | -3.04 |
| TS_h20_p2pct | 20 | 2% | 10.62 | 0.790 | -16.15 | 41.4 | -2.01 | -0.201 | -4.37 |
| TS_h20_p3pct | 20 | 3% | 10.20 | 0.759 | -17.09 | 41.7 | -2.43 | -0.232 | -5.31 |
| TS_h30_p0pct | 30 | 0% | 13.88 | 1.050 | -11.95 | 43.7 | +1.25 | +0.059 | -0.17 |
| TS_h30_p1pct | 30 | 1% | 12.78 | 0.992 | -11.95 | 43.4 | +0.15 | +0.001 | -0.17 |
| TS_h30_p2pct | 30 | 2% | 13.22 | 1.015 | -11.95 | 43.4 | +0.59 | +0.024 | -0.17 |
| TS_h30_p3pct | 30 | 3% | 13.49 | 1.024 | -11.95 | 43.4 | +0.86 | +0.033 | -0.17 |

## OOS Sharpe by Year (Dual side)

| Cell | OOS_2023 | OOS_2024 | OOS_2025 | BEAR_2022 |
|---|---|---|---|---|
| baseline_no_ts | 0.738 | 1.428 | 0.650 | 1.198 |
| TS_h5_p0pct | 1.026 | 1.373 | 0.595 | 1.279 |
| TS_h5_p1pct | 1.026 | 1.554 | 0.601 | 1.279 |
| TS_h5_p2pct | 1.026 | 1.505 | 0.472 | 1.290 |
| TS_h5_p3pct | 1.026 | 1.348 | 0.750 | 1.143 |
| TS_h10_p0pct | 1.026 | 1.373 | 0.595 | 1.279 |
| TS_h10_p1pct | 1.026 | 1.554 | 0.601 | 1.279 |
| TS_h10_p2pct | 1.026 | 1.505 | 0.472 | 1.290 |
| TS_h10_p3pct | 1.026 | 1.348 | 0.750 | 1.143 |
| TS_h15_p0pct | 1.026 | 1.373 | 0.595 | 1.279 |
| TS_h15_p1pct | 1.026 | 1.554 | 0.601 | 1.279 |
| TS_h15_p2pct | 1.026 | 1.505 | 0.472 | 1.290 |
| TS_h15_p3pct | 1.026 | 1.348 | 0.750 | 1.143 |
| TS_h20_p0pct | 1.026 | 1.373 | 0.595 | 1.279 |
| TS_h20_p1pct | 1.026 | 1.554 | 0.601 | 1.279 |
| TS_h20_p2pct | 1.026 | 1.505 | 0.472 | 1.290 |
| TS_h20_p3pct | 1.026 | 1.348 | 0.750 | 1.143 |
| TS_h30_p0pct | 0.840 | 1.350 | 0.589 | 1.183 |
| TS_h30_p1pct | 0.840 | 1.434 | 0.596 | 1.183 |
| TS_h30_p2pct | 0.840 | 1.523 | 0.467 | 1.194 |
| TS_h30_p3pct | 0.840 | 1.563 | 0.597 | 1.194 |

## 2022 Bear MDD vs Baseline (Dual side)

Smaller |MDD| = better. ΔMDD positive = improvement.

| Cell | CAGR_2022 % | MDD_2022 % | ΔCAGR | ΔMDD |
|---|---|---|---|---|
| baseline | 14.05 | -7.89 | - | - |
| TS_h5_p0pct | 15.01 | -7.54 | +0.96 | +0.35 |
| TS_h5_p1pct | 15.01 | -7.54 | +0.96 | +0.35 |
| TS_h5_p2pct | 15.12 | -7.54 | +1.07 | +0.35 |
| TS_h5_p3pct | 12.43 | -7.54 | -1.62 | +0.35 |
| TS_h10_p0pct | 15.01 | -7.54 | +0.96 | +0.35 |
| TS_h10_p1pct | 15.01 | -7.54 | +0.96 | +0.35 |
| TS_h10_p2pct | 15.12 | -7.54 | +1.07 | +0.35 |
| TS_h10_p3pct | 12.43 | -7.54 | -1.62 | +0.35 |
| TS_h15_p0pct | 15.01 | -7.54 | +0.96 | +0.35 |
| TS_h15_p1pct | 15.01 | -7.54 | +0.96 | +0.35 |
| TS_h15_p2pct | 15.12 | -7.54 | +1.07 | +0.35 |
| TS_h15_p3pct | 12.43 | -7.54 | -1.62 | +0.35 |
| TS_h20_p0pct | 15.01 | -7.54 | +0.96 | +0.35 |
| TS_h20_p1pct | 15.01 | -7.54 | +0.96 | +0.35 |
| TS_h20_p2pct | 15.12 | -7.54 | +1.07 | +0.35 |
| TS_h20_p3pct | 12.43 | -7.54 | -1.62 | +0.35 |
| TS_h30_p0pct | 13.95 | -7.99 | -0.10 | -0.10 |
| TS_h30_p1pct | 13.95 | -7.99 | -0.10 | -0.10 |
| TS_h30_p2pct | 14.05 | -7.99 | +0.00 | -0.10 |
| TS_h30_p3pct | 14.05 | -7.99 | +0.00 | -0.10 |

## Time Stop Exit Frequency (FULL_2020_2025, QM side)

Higher exits = more aggressive trim. Compare against rebal_swap_exits.

| Cell | TS exits | Rebal swap exits | Avg hold days | n_closed |
|---|---|---|---|---|
| baseline_no_ts | 0 | 231 | - | - |
| TS_h5_p0pct | 83 | 152 | 23.4 | 235 |
| TS_h5_p1pct | 165 | 71 | 22.8 | 236 |
| TS_h5_p2pct | 171 | 65 | 22.7 | 236 |
| TS_h5_p3pct | 180 | 57 | 22.4 | 237 |
| TS_h10_p0pct | 83 | 152 | 23.4 | 235 |
| TS_h10_p1pct | 165 | 71 | 22.8 | 236 |
| TS_h10_p2pct | 171 | 65 | 22.7 | 236 |
| TS_h10_p3pct | 180 | 57 | 22.4 | 237 |
| TS_h15_p0pct | 83 | 152 | 23.4 | 235 |
| TS_h15_p1pct | 165 | 71 | 22.8 | 236 |
| TS_h15_p2pct | 171 | 65 | 22.7 | 236 |
| TS_h15_p3pct | 180 | 57 | 22.4 | 237 |
| TS_h20_p0pct | 83 | 152 | 23.4 | 235 |
| TS_h20_p1pct | 165 | 71 | 22.8 | 236 |
| TS_h20_p2pct | 171 | 65 | 22.7 | 236 |
| TS_h20_p3pct | 180 | 57 | 22.4 | 237 |
| TS_h30_p0pct | 20 | 212 | 24.5 | 232 |
| TS_h30_p1pct | 24 | 208 | 24.1 | 232 |
| TS_h30_p2pct | 27 | 205 | 23.9 | 232 |
| TS_h30_p3pct | 28 | 204 | 23.9 | 232 |

## Leave-One-Out (Best cell: TS_h30_p0pct)

Best cell hold_days=30, progress_pct=0.00

Drop each year, compute Sharpe on remaining 5 years. If single year dominates -> Sharpe drops a lot when dropped.

| Drop year | Best cell Sharpe (5y) | Baseline Sharpe (5y) | Δ |
|---|---|---|---|
| 2020 | 1.149 | 1.131 | +0.018 |
| 2021 | 0.909 | 0.834 | +0.075 |
| 2022 | 1.015 | 0.940 | +0.075 |
| 2023 | 0.957 | 0.882 | +0.075 |
| 2024 | 1.139 | 1.056 | +0.083 |
| 2025 | 1.164 | 1.139 | +0.025 |

## Verdict

### Grading Rubric
- **A**: ΔCAGR > +1pp AND ΔSharpe > +0.1 AND ΔMDD <= 0 (FULL)
- **B**: ΔCAGR > +0.5pp BUT ΔMDD worsens -> shadow run
- **D 平原**: 全部 cell |ΔCAGR| < 0.5pp -> noise, keep PRE_POLICY
- **D 反向**: Time Stop 越嚴績效越糟

### Findings
- **Best cell ΔCAGR**: +1.25pp (TS_h30_p0pct)
- **Best cell ΔSharpe**: +0.059 (TS_h30_p0pct)
- **Worst cell ΔCAGR**: -2.43pp (TS_h5_p3pct)
- **Avg ΔCAGR across grid**: -0.89pp
- **Avg ΔSharpe across grid**: -0.107
- **Max |ΔCAGR|**: 2.43pp
- **A-grade cells**: 0
- **B-grade (trade-off) cells**: 7

### Grade: **B** -- Trade-off, OOS sign 不穩

B-grade cells (CAGR up but MDD worse OR Sharpe gap < +0.1):

| Cell | ΔCAGR | ΔSharpe | ΔMDD | OOS sign-stab |
|---|---|---|---|---|
| TS_h5_p0pct | +0.57 | -0.004 | -0.86 | 1/3 |
| TS_h10_p0pct | +0.57 | -0.004 | -0.86 | 1/3 |
| TS_h15_p0pct | +0.57 | -0.004 | -0.86 | 1/3 |
| TS_h20_p0pct | +0.57 | -0.004 | -0.86 | 1/3 |
| TS_h30_p0pct | +1.25 | +0.059 | -0.17 | 1/3 |
| TS_h30_p2pct | +0.59 | +0.024 | -0.17 | 2/3 |
| TS_h30_p3pct | +0.86 | +0.033 | -0.17 | 2/3 |

### Recommendation

B-grade cells with OOS-stable sign exist: ['TS_h30_p2pct', 'TS_h30_p3pct']
-> shadow run 6 mo, 看實際與 simulator 一致再決定

## Caveats

1. Time Stop 觸發判斷使用當週 fwd_5d 後的 cum_ret -> 等同 close-of-week 觸發, 無 look-ahead
2. Time Stop 觸發後 30d ban (跟 Whipsaw 同 mechanism) 避免立即重買
3. MIN_HOLD=20 為 hard floor, Time Stop 不會在 days_held<20 觸發
4. 不模擬交易成本 (台股 ~0.3% round-trip), Time Stop 多換手會有額外 drag
5. Baseline = PRE_POLICY (b18758d revert 後設定), 不是 post-policy baseline
6. Universe 限於 trade_journal_value snapshot (Stage 1 後 857 檔) + QM panel
7. cum_ret 計算用 fwd_5d 連乘, 不含 TP partial exit 後的真實組合報酬 (TP 機制與 baseline 一致)
