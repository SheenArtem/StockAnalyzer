# Signal #1 「對利空不反應」 Event Study

- 期間: 2015-01-05 ~ 2026-04-13
- TWII 利空門檻: -1.0% (close-to-close)
- 樣本過濾: 收盤價 >= 5.0 元、20 日均量 >= 200,000 股
- Forward horizons: [1, 5, 10, 20]
- 總 trigger 樣本數: 98,201

## Baseline 對照組（利空日所有 liquid 個股，沒篩 signal）

| Horizon | n | mean fwd | mean CAR | t-stat (CAR) |
|---|---:|---:|---:|---:|
| 1d | 325,082 | -0.15% | -0.20% | -42.57 |
| 5d | 325,078 | +0.23% | -0.23% | -22.48 |
| 10d | 321,553 | +0.41% | -0.29% | -19.81 |
| 20d | 319,135 | +1.57% | -0.15% | -6.91 |

## 各訊號彙整 (Full sample)

| Signal | n | fwd_5d | CAR_5d (t) | fwd_10d | CAR_10d (t) | fwd_20d | CAR_20d (t) | Grade |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| S1_close_up | 75,935 | +0.07% | -0.31% (-12.62) | +0.11% | -0.42% (-12.30) | +1.08% | -0.36% (-7.42) | D |
| S2_intraday_recovery | 12,447 | +0.26% | -0.39% (-6.18) | +0.34% | -0.51% (-5.76) | +1.67% | -0.10% (-0.80) | D |
| S3_t1_breakout | 9,769 | +0.78% | +0.32% (+3.60) | +1.22% | +0.69% (+5.50) | +1.76% | +0.55% (+3.11) | C |

## Regime Breakdown（CAR_10d mean / t-stat / n）

| Signal | Bull | Bear | Vol Low | Vol Mid | Vol High |
|---|---|---|---|---|---|
| S1_close_up | -0.48% (-10.95) n=47935 | -0.31% (-5.79) n=27255 | -0.60% (-10.99) n=26681 | -0.63% (-10.11) n=24506 | -0.02% (-0.29) n=24003 |
| S2_intraday_recovery | -0.42% (-3.30) n=7048 | -0.64% (-5.36) n=5242 | -0.39% (-1.96) n=2693 | -0.90% (-6.19) n=4648 | -0.22% (-1.59) n=4949 |
| S3_t1_breakout | +0.77% (+4.92) n=6743 | +0.50% (+2.47) n=2984 | +0.68% (+3.33) n=3421 | +0.31% (+1.37) n=2997 | +1.04% (+4.73) n=3309 |

## 最終判級（含 regime 反向檢查）

- **S1_close_up**: D (bull regime CAR<=0; bear regime CAR<0 (regime 反向); vs baseline CAR_10d edge = -0.13%)
- **S2_intraday_recovery**: D (bull regime CAR<=0; bear regime CAR<0 (regime 反向); vs baseline CAR_10d edge = -0.23%)
- **S3_t1_breakout**: C (vs baseline CAR_10d edge = +0.98%)

## 方法說明

- TWII 利空日：當日 close-to-close 報酬 <= 門檻；個股 forward 取 (Close_t+h / Close_t - 1)。
- CAR = stock_fwd_h - twii_fwd_h；以同 horizon 區間 cumulative return 對齊。
- Bull/Bear 以 TWII Close vs 200d MA 切；Vol Low/Mid/High 以樣本 TWII 20d realized vol 33/66 quantile 切。
- Grade A 需 CAR_5d 與 CAR_10d 平均 >2% 且 |t|>2、regime 不反向；B 是 >1%、|t|>1.5；C 為正但 t 弱；D 為負或反向。
- 警告：本研究只用大盤 proxy，未引入 news sentiment；若有 alpha，後續可加上 stock-level 利空新聞細分。
