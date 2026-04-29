# Dual × position_monitor Rule 1/2/7/8 補驗

**Date**: 2026-04-29
**Universe**: trade_journal_value_tw_snapshot 309 weeks 2020-2025 + TWII 2015+

## Rule 1 — Hard exit timing

Hard stop proxy: weeks where fwd_5d_min < -8%

| Metric | Value |
|---|---:|
| n_hard_events | 2691 |
| Strategy A (hard exit @ -8% cap) mean ret | -0.0800 |
| Strategy B (wait to month-end fwd_20d) | -0.0699 |
| Diff (A - B) | -0.0101 |
| Win rate (A better) | 0.505 |

**結論**: 等到月末反而較佳 → Rule 1 priority 邏輯需重新評估（hard exit 過於敏感）

## Rule 2 — Rebalance frequency

 freq_weeks freq_label  n_periods  mean_period_ret     std    cagr  sharpe_ann     mdd  final_cum

          1     weekly        309          +0.0061 +0.0405 +0.3123     +1.0484 -0.4659    +5.0289

          2   biweekly        155          +0.0127 +0.0597 +0.3263     +1.0550 -0.4512    +5.3838

          4    monthly         78          +0.0231 +0.0904 +0.2756     +0.8898 -0.4922    +4.3088

         13  quarterly         24          +0.1115 +0.2199 +0.4216     +0.9912 -0.3883    +8.2538

**結論**: 比 Sharpe → Best = biweekly (Sharpe +1.055)

## Rule 7 — regime 切換立刻清 vs 延遲清

| Metric | Value |
|---|---:|
| n_transitions | 32 |
| mean_A_immediate | +0.0090 |
| mean_B_defer_1mo | +0.0216 |
| mean_C_defer_3mo | +0.0516 |
| diff_B_minus_A (defer 1mo cost) | +0.0126 |
| diff_C_minus_A (defer 3mo cost) | +0.0425 |

**結論**: 延遲 1 個月反而較佳 → Rule 7 維持「下月清倉」

## Rule 8 — 跳空 ±3% 日 fwd return

       group  n_days  fwd_5d_mean  fwd_5d_t  fwd_20d_mean  fwd_20d_t

    all_days    2720      +0.0027   +5.6871       +0.0109   +11.5678

non_gap_3pct    2713      +0.0026   +5.6300       +0.0108   +11.4220

 gap_up_3pct       2          NaN       NaN           NaN        NaN

 gap_dn_3pct       5      +0.0174   +0.5782       +0.0573    +2.5648

non_gap_1pct    2571      +0.0025   +5.2300       +0.0100   +10.4084

 gap_up_1pct      81      +0.0059   +2.1737       +0.0286    +6.2498

 gap_dn_1pct      68      +0.0065   +1.3167       +0.0239    +2.9778