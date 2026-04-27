# Step 3a — Sector Rotation Backtest

**Verdict**: **D_FAIL**

**Best config**: `mom12m_top5sec`

- Full 2016-2025 α_net vs TWII TR: **+0.61pp/yr**
- Pre-AI 2016-2022 α_net: +8.02pp/yr
- AI era 2023-2025 α_net: -17.70pp/yr

## 1. Grid results (6 configs × 3 periods)

        config           period  gross_cagr  net_cagr  twii_tr_cagr  alpha_net
 mom3m_top3sec Pre-AI 2016-2022       23.06     15.42         11.65       3.77
 mom3m_top3sec AI era 2023-2025       12.30      4.08         31.43     -27.36
 mom3m_top3sec   Full 2016-2025       19.73     11.90         17.25      -5.35
 mom3m_top5sec Pre-AI 2016-2022       19.86     15.64         11.65       3.99
 mom3m_top5sec AI era 2023-2025       13.15      9.96         31.43     -21.47
 mom3m_top5sec   Full 2016-2025       17.81     13.90         17.25      -3.35
 mom6m_top3sec Pre-AI 2016-2022       27.51     23.32         11.65      11.67
 mom6m_top3sec AI era 2023-2025        5.85      2.01         31.43     -29.42
 mom6m_top3sec   Full 2016-2025       20.58     16.50         17.25      -0.75
 mom6m_top5sec Pre-AI 2016-2022       20.81     18.00         11.65       6.35
 mom6m_top5sec AI era 2023-2025       14.58     12.04         31.43     -19.40
 mom6m_top5sec   Full 2016-2025       18.91     16.18         17.25      -1.07
mom12m_top3sec Pre-AI 2016-2022       21.13     17.78         11.65       6.12
mom12m_top3sec AI era 2023-2025       11.54      9.33         31.43     -22.10
mom12m_top3sec   Full 2016-2025       18.17     15.18         17.25      -2.07
mom12m_top5sec Pre-AI 2016-2022       21.99     19.67         11.65       8.02
mom12m_top5sec AI era 2023-2025       15.53     13.74         31.43     -17.70
mom12m_top5sec   Full 2016-2025       20.01     17.86         17.25       0.61

## 2. Best config annual breakdown (mom12m_top5sec)

 year  n_months  port_gross_pct  port_net_pct  twii_price_pct  twii_tr_pct  avg_turnover  alpha_net_vs_tr
 2015        12          -14.42        -16.08          -10.94        -8.00         0.391            -8.08
 2016        12            0.76         -0.53           10.98        14.89         0.271           -15.42
 2017        12           32.00         29.83           15.01        19.06         0.354            10.77
 2018        12          -10.74        -11.96           -8.60        -5.32         0.287            -6.64
 2019        12           34.84         32.06           23.33        27.64         0.449             4.42
 2020        12           46.07         39.54           22.80        27.10         1.041            12.44
 2021        12           90.25         89.48           23.66        27.99         0.095            61.49
 2022        12           -9.64        -11.47          -22.40       -19.57         0.428             8.10
 2023        12           45.23         43.32           26.83        31.25         0.298            12.07
 2024        12           11.48         10.39           28.47        32.94         0.208           -22.55
 2025        12           -4.76         -7.00           25.74        30.13         0.493           -37.13
 2026         4           15.94         15.09           29.84        31.27         0.459           -16.18

## 3. Pass/Fail gate

| Criterion | Target | Actual | Pass |
|---|---|---|---|
| Full 10yr α_net | > +3pp | +0.61pp | ❌ |
| AI era α_net | > +5pp | -17.70pp | ❌ |
| Pre-AI α_net | > 0 | +8.02pp | ✅ |

## 4. 下一步分流

❌ **Sector rotation 在台股也輸 0050**。AI era 結構性不可達結論被再次確認。

## 5. Caveats

- Sector = 目前 industry 分類（每股取 latest industry）— 若歷史中有產業調整會有輕微 look-ahead
- Sector return = equal-weight constituents（非市值加權，保留 alpha 但高 turnover 成本）
- Top N sectors 內每股 equal-weight，未做 sector 內二次 ranking
- 月頻 rebalance cost 0.4% round-trip × turnover
- Benchmark TWII price + 3.5%/yr dividend yield approximation
