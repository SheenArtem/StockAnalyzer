# Contrarian Mean-Reversion 訊號驗證 (bear + vol_high)

- Overall grade: **D**
- Per-signal grades: {'S1_no_response': 'D', 'S2_intraday_fade': 'D', 'S3_t1_breakdown': 'D'}
- 來源: 宋分擇時 #5 「好消息股價不推」event study 副產物 — bear+vol_high regime 反向發現
- 訊號: 大盤 +1% 利多日, 個股「沒跟漲」(S1/S2/S3), 在 bear+vol_high regime 反而 mean-revert 反彈

## Stage 1: bear+vol_high regime alpha vs baseline

| Signal | n | CAR_5d | t | CAR_10d | t | CAR_20d | t | win_10d |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| S1_no_response | 14,264 | +0.86% | +15.96 | +1.31% | +17.86 | +1.89% | +17.04 | 51% |
| S2_intraday_fade | 4,463 | +0.42% | +3.96 | +1.03% | +7.67 | +2.18% | +10.19 | 49% |
| S3_t1_breakdown | 1,176 | +2.17% | +10.03 | +2.38% | +7.78 | +2.89% | +7.52 | 54% |
| BASELINE_bear_volhigh | 83,027 | +0.71% | -- | +1.14% | +41.39 | +1.62% | -- | -- |

**Edge over baseline (CAR_10d, same regime cell):**
- S1_no_response: trigger +1.31% − baseline +1.14% = **+0.18%**
- S2_intraday_fade: trigger +1.03% − baseline +1.14% = **-0.11%**
- S3_t1_breakdown: trigger +2.38% − baseline +1.14% = **+1.25%**

## Stage 2: walk-forward stability (rolling IS=3yr / OOS=1yr)

| Signal | OOS years positive | sign-hit | verdict |
|---|---|---:|---|
| S1_no_response | +, +, +, - | 75% | 穩定 |
| S2_intraday_fade | +, +, +, - | 75% | 穩定 |
| S3_t1_breakdown | -, +, -, + | 50% | 邊際 |

Detail (per-window OOS CAR_10d):

| Signal | IS window | OOS year | OOS n | OOS CAR_10d | OOS t |
|---|---|---:|---:|---:|---:|
| S1_no_response | 2015-2017 | 2018 | 393 | +0.06% | +0.18
| S2_intraday_fade | 2015-2017 | 2018 | 128 | +0.08% | +0.12
| S3_t1_breakdown | 2015-2017 | 2018 | 46 | -0.63% | -0.63
nan |
nan |
nan |
| S1_no_response | 2017-2019 | 2020 | 3202 | +2.73% | +15.34
| S2_intraday_fade | 2017-2019 | 2020 | 1239 | +2.95% | +10.68
| S3_t1_breakdown | 2017-2019 | 2020 | 186 | +12.94% | +12.39
nan |
nan |
nan |
| S1_no_response | 2019-2021 | 2022 | 6325 | +0.63% | +6.23
| S2_intraday_fade | 2019-2021 | 2022 | 2132 | +0.02% | +0.11
| S3_t1_breakdown | 2019-2021 | 2022 | 796 | -0.07% | -0.24
nan |
nan |
nan |
nan |
nan |
nan |
| S1_no_response | 2022-2024 | 2025 | 2651 | -0.29% | -1.82
| S2_intraday_fade | 2022-2024 | 2025 | 593 | -1.14% | -3.27
| S3_t1_breakdown | 2022-2024 | 2025 | 111 | +3.20% | +3.03

## Stage 3: cross-regime sanity (CAR_10d grid)

| Signal | Cell | n | CAR_10d | t |
|---|---|---:|---:|---:|
| S1_no_response | bull+vol_low | 39,922 | -0.72% | -16.73 |
| S1_no_response | bull+vol_mid | 34,846 | -0.82% | -17.43 |
| S1_no_response | bull+vol_high | 24,616 | +1.01% | +17.31 |
| S1_no_response | bear+vol_low | 4,471 | -0.02% | -0.18 |
| S1_no_response | bear+vol_mid | 6,785 | +0.33% | +3.45 |
| S1_no_response | bear+vol_high | 14,263 | +1.31% | +17.86 |
| S2_intraday_fade | bull+vol_low | 7,499 | -0.56% | -4.72 |
| S2_intraday_fade | bull+vol_mid | 7,731 | -0.84% | -7.05 |
| S2_intraday_fade | bull+vol_high | 7,309 | +1.38% | +11.73 |
| S2_intraday_fade | bear+vol_low | 815 | +0.16% | +0.56 |
| S2_intraday_fade | bear+vol_mid | 2,302 | +0.53% | +3.13 |
| S2_intraday_fade | bear+vol_high | 4,463 | +1.03% | +7.67 |
| S3_t1_breakdown | bull+vol_low | 3,454 | -1.29% | -11.07 |
| S3_t1_breakdown | bull+vol_mid | 3,899 | -0.46% | -4.20 |
| S3_t1_breakdown | bull+vol_high | 2,909 | +1.04% | +6.30 |
| S3_t1_breakdown | bear+vol_low | 411 | -1.43% | -3.99 |
| S3_t1_breakdown | bear+vol_mid | 553 | +0.05% | +0.17 |
| S3_t1_breakdown | bear+vol_high | 1,176 | +2.38% | +7.78 |

## Stage 4: operationalization

- bear+vol_high regime 占比: **15.5%** of 2,741 trading days
- bear+vol_high 中有觸發訊號的活躍日: 69 日
- Regime detection: 兩個 daily TWII 指標, 收盤後即可算
  - bear: TWII Close < 200d MA
  - vol_high: TWII 20d realized vol > sample 66 quantile

**Tx cost robustness (round-trip 0.2%):**

| Signal | n | CAR_10d gross | CAR_10d net | stocks/active day |
|---|---:|---:|---:|---:|
| S1_no_response | 14,264 | +1.31% | +1.11% | 206.7 |
| S2_intraday_fade | 4,463 | +1.03% | +0.83% | 64.7 |
| S3_t1_breakdown | 1,176 | +2.38% | +2.18% | 17.0 |

**bear+vol_high days per year (concentration risk check):**

| Year | days |
|---|---:|
| 2015 | 62 |
| 2016 | 19 |
| 2017 | 0 |
| 2018 | 46 |
| 2019 | 16 |
| 2020 | 57 |
| 2021 | 3 |
| 2022 | 161 |
| 2023 | 4 |
| 2024 | 0 |
| 2025 | 56 |
| 2026 | 0 |

## Stage 4b: COVID-strip + edge-vs-baseline (核心 kill test)

Stage 1 baseline (bear+vol_high 整個市場無篩股) CAR_10d = +1.14% (t=41.39, n=83k) — **整體市場本身就 mean-revert**, 訊號 incremental edge 才是真實 alpha。

**Excluding 2020 (COVID outlier):**

| Signal | n (ex 2020) | Trigger CAR_10d | Baseline CAR_10d | **Edge** | t (trigger) |
|---|---:|---:|---:|---:|---:|
| S1_no_response | 11,062 | +0.90% | +0.71% | **+0.20%** | +11.42 |
| S2_intraday_fade | 3,224 | +0.29% | +0.71% | **-0.42%** | +1.95 |
| S3_t1_breakdown | 990 | +0.40% | +0.71% | **-0.31%** | +1.52 |

**Per-year edge over same-regime baseline (CAR_10d, n>=30):**

| Signal | Year | n | Trigger | Baseline | Edge |
|---|---:|---:|---:|---:|---:|
| S1_no_response | 2015 | 1533 | +4.08% | +2.50% | +1.58% |
| S1_no_response | 2016 | 160 | +3.17% | +1.07% | +2.10% |
| S1_no_response | 2018 | 393 | +0.06% | +1.07% | -1.01% |
| S1_no_response | 2020 | 3202 | +2.73% | +3.11% | -0.38% |
| S1_no_response | 2022 | 6325 | +0.63% | +0.59% | +0.04% |
| S1_no_response | 2025 | 2651 | -0.29% | -0.20% | -0.09% |
| S2_intraday_fade | 2015 | 342 | +4.42% | +2.50% | +1.93% |
| S2_intraday_fade | 2018 | 128 | +0.08% | +1.07% | -1.00% |
| S2_intraday_fade | 2020 | 1239 | +2.95% | +3.11% | -0.16% |
| S2_intraday_fade | 2022 | 2132 | +0.02% | +0.59% | -0.57% |
| S2_intraday_fade | 2025 | 593 | -1.14% | -0.20% | -0.94% |
| S3_t1_breakdown | 2015 | 35 | +3.56% | +2.50% | +1.06% |
| S3_t1_breakdown | 2018 | 46 | -0.63% | +1.07% | -1.71% |
| S3_t1_breakdown | 2020 | 186 | +12.94% | +3.11% | +9.83% |
| S3_t1_breakdown | 2022 | 796 | -0.07% | +0.59% | -0.66% |
| S3_t1_breakdown | 2025 | 111 | +3.20% | -0.20% | +3.40% |

## Final verdict

**Overall grade: D**  ({'S1_no_response': 'D', 'S2_intraday_fade': 'D', 'S3_t1_breakdown': 'D'})

### 核心問題

1. **Baseline 已 mean-revert**: bear+vol_high 整個市場 CAR_10d=+1.14% (t=41), 訊號 incremental alpha 嚴重縮水。
2. **2020 COVID outlier 主導 walk-forward**: 4 個有資料的 OOS year 中 (2018/2020/2022/2025), 2020 量級遠超其他 (S1 +2.73%, S3 +12.94%). 去掉 COVID 後 edge 大幅下降。
3. **Walk-forward window 數不足**: 8 個 windows 中 4 個 (2019/2021/2023/2024) bear+vol_high 樣本=0, 實際只有 4 個 OOS observation, 統計力極弱。
4. **2025 OOS 反向**: 最近期 OOS year, S1 (-0.29%, t=-1.82) 與 S2 (-1.14%, t=-3.27) 反向, 與 2020 COVID dominant 訊號形成衝突。
5. **Cross-regime 顯示真正 conditioning 是 vol_high 不是 bear**: bull+vol_high 也是 +1.0%~1.4% (t > 6), 不是 bear-specific. 訊號被誤標為 bear+vol_high。

### 操作化建議

- **不上線**: trigger_score / position_monitor / paper trade 全部不採納
- 歸類為 multiple-comparison false positive (15 cells × 3 signals = 45 tests)
- 真正的訊號可能就只是「TWII 大漲日 vol_high regime 大盤本身會 mean-revert」, 個股「不跟漲」的 incremental edge 約 0~+0.3% 量級, 無 actionable alpha

### Multiple-comparison adjustment 建議 (給未來 SOP)

本研究 cell 數量 6 regime × 3 signal = 18 tests, Bonferroni 門檻為 t ≈ 2.86 (α=0.05/18). 本研究多數 t > 5, 統計上仍顯著, 但問題不在 t 不夠大, 而在 **(a) 與 baseline edge 太小** **(b) walk-forward 樣本被 COVID 主導**. SOP 建議:

1. Cross-regime grid 看到 outlier cell 時, 先比 same-cell baseline (no signal filter), 算 incremental edge
2. Walk-forward 至少要 5+ OOS years 都正才算穩, 4 windows 不夠
3. 訊號量級若高度依賴單一極端年 (COVID 2020), 自動扣分
4. 多重比較不能只看 t-stat, 要 cross-validate 訊號是否在 conceptually 不該出現的 cell 也出現 (如 bull+vol_high 也正)