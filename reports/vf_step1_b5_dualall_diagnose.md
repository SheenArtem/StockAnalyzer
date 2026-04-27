# Step B.5 — Dual-all Leg Diagnose (2024-2025 focus)

**目的**：Council R2 共識 — 跑 Dual-all 分 Value/QM 雙腿年度分解，判斷 2024-2025 alpha 崩潰是 factor decay / regime shift / crowding。

**Spec**: Dual-all (no mcap filter) + only_volatile，top_20 each side，Value fwd_20d + QM fwd_20d，月頻 rebalance。

## VERDICT: **A - FACTOR_DECAY**

> Value 2yr Sharpe -0.28 < 0.3, QM 0.71 OK — Value factor decay，DELAY ship 重驗 Value

## 1. 2024-2025 焦點比較

| leg | 2024 Ret% | 2024 Sharpe | 2024 MDD% | 2025 Ret% | 2025 Sharpe | 2025 MDD% |
|---|---|---|---|---|---|---|
| **Value only** | 5.88 | 0.928 | -1.89 | -11.5 | -1.483 | -11.5 |
| **QM only** | 9.83 | 0.399 | -12.21 | 43.27 | 1.023 | -30.24 |
| **Combined 50/50** | 8.43 | 0.642 | -6.18 | 14.97 | 0.642 | -17.52 |

**TWII 對照**：
- 2024: TWII +29.02% / MDD -18.69%
- 2025: TWII +26.85% / MDD -26.71%

## 2. 全年度分解 (2016-2025, 3 legs)

### Annual Return %
      combined     qm    val
year                        
2016      6.91  13.89   0.00
2017     16.23  33.92   0.00
2018     -0.86  -8.30   8.23
2019     28.20  61.43   0.00
2020     22.72  18.92  23.00
2021     69.22  88.86  46.39
2022      5.33   4.33   4.82
2023     10.27  12.89   6.94
2024      8.43   9.83   5.88
2025     14.97  43.27 -11.50

### Annual Sharpe
      combined     qm    val
year                        
2016     0.989  1.078    NaN
2017     1.714  1.853    NaN
2018    -0.111 -0.314  1.227
2019     2.153  2.391    NaN
2020     1.165  0.519  1.648
2021     2.877  2.228  2.147
2022     0.188  0.106  0.190
2023     0.941  0.673  0.822
2024     0.642  0.399  0.928
2025     0.642  1.023 -1.483

### TWII 對照（全期）
 year  twii_ret_pct  twii_mdd_pct
 2015        -10.09        -25.70
 2016         14.04         -8.61
 2017         14.77         -4.60
 2018         -9.18        -15.77
 2019         25.57         -7.16
 2020         21.75        -28.72
 2021         22.26        -12.74
 2022        -22.62        -31.63
 2023         26.06         -7.69
 2024         29.02        -18.69
 2025         26.85        -26.71
 2026         28.13        -10.42


## 3. Step D 分流決定

⏸️ **Verdict A (VALUE FACTOR DECAY)**: Value 腿 2yr Sharpe < 0.3，QM 腿正常 → **DELAY ship**，回 brainstorm 重驗 Value factor（優先跑宋分毛利 Δ / ROIC IC）。
