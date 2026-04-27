# Step D0 — PEAD Event Study (TW 月營收)

**Verdict**: **B_WEAK_SIGNAL**

> Q5 1.526%, spread 1.892, IC 0.0435

**Event**: 月營收公告日（營收月 +10 天近似）

**Surprise**: YoY growth − rolling 12m median, 標準化除 rolling 12m std

**CAR**: Stock fwd return − TWII fwd return，horizons {5,20,40,60}d

**Quintile**: 每事件按 surprise 強度 cross-section 分 5 等分

## 1. Quintile × Horizon × Period

          period horizon  n_events  Q1_pct  Q2_pct  Q3_pct  Q4_pct  Q5_pct  spread_Q5_Q1  ic_spearman  q5_tstat  q5_pval
  Full 2016-2025      5d    189682  -0.130  -0.111   0.069   0.245   0.570         0.700       0.0340     17.36   0.0000
  Full 2016-2025     20d    189682  -0.365  -0.368   0.128   0.706   1.526         1.892       0.0435     22.31   0.0000
  Full 2016-2025     40d    189679  -0.976  -0.613   0.192   0.988   2.194         3.170       0.0496     21.00   0.0000
  Full 2016-2025     60d    189676  -1.337  -0.439   0.474   1.672   2.826         4.164       0.0547     21.36   0.0000
Pre-AI 2016-2022      5d    126246  -0.130  -0.055   0.073   0.267   0.520         0.650       0.0304     12.85   0.0000
Pre-AI 2016-2022     20d    126246  -0.251  -0.056   0.372   1.053   1.927         2.179       0.0517     22.84   0.0000
Pre-AI 2016-2022     40d    126246  -0.497   0.174   0.944   1.763   3.076         3.573       0.0586     23.81   0.0000
Pre-AI 2016-2022     60d    126246  -0.357   0.953   1.894   3.000   4.185         4.542       0.0620     25.74   0.0000
AI era 2023-2025      5d     63436  -0.146  -0.203   0.074   0.187   0.667         0.813       0.0413     11.89   0.0000
AI era 2023-2025     20d     63436  -0.641  -0.946  -0.342   0.044   0.690         1.331       0.0271      5.96   0.0000
AI era 2023-2025     40d     63433  -2.037  -2.004  -1.266  -0.665   0.443         2.480       0.0329      2.50   0.0123
AI era 2023-2025     60d     63430  -3.493  -2.926  -2.196  -1.221   0.136         3.629       0.0419      0.60   0.5490

## 2. Pass/Fail Gate

| Criterion | Target | Actual | Pass |
|---|---|---|---|
| Top quintile 20d CAR | > +2% | 1.526% | ❌ |
| Q5-Q1 spread | > +3pp | 1.892pp | ❌ |
| Spearman IC | > 0.03 | 0.0435 | ✅ |
| Q5 t-test p-value | < 0.05 | 0.0 | ✅ |
| AI era Q5 20d CAR | > 0 | 0.69% | ✅ |

## 3. 下一步分流

⚠️ **訊號存在但弱**。可試 top-decile（Q5 再切細）或改用 earnings surprise magnitude 加權。考慮不獨立當 factor，只做 overlay。

## 4. Caveats

- Announce date 用「營收月 + 10 天」近似，未用真實 MOPS 公告時間戳
- CAR 相對 TWII，未相對 sector/industry（未去除產業 beta）
- 無 size effect 控制（小型股 signal 可能較強但流動性差）
- Surprise baseline 用 12m rolling median + std，其他 baseline 未測試
- 未扣交易成本（event-driven trade，若每月全 Q5 進場 turnover 高）
