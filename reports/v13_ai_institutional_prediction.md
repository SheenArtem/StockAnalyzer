# V13: AI 模擬法人選股 Binary Classifier 驗證

Generated: 2026-04-24 19:46

## TL;DR (verdict = D)

**Mode D 不應依賴 ML 預測法人買盤當 rule-based entry。**

- AUC 看起來很高（GBM 0.72-0.84），但 **net alpha 全部為負**
- 核心問題：**「被法人大買」 != 「股票會漲」**，model 精準預測前者，對後者沒資訊量
- 原始 baseline momentum top10 fwd_20d = +6.60%，model top10 fwd_20d = +1.4~2.9%，
  模型選股反而比隨機還差
- AUC 提升主要來自自身 flow autocorrelation (trust self-lag Δ AUC +0.087)，
  不是真 edge
- **建議**: Mode D 完全靠 thesis discretionary，不做軍備競賽

## 任務
判斷能否用 ML 從 momentum/value/chip/sentiment features 預測台股外資 (Target A) 或投信 (Target B) 下週是否大買某股。

- Universe: TW 上市+上櫃普通股市值前 600（20d 成交額代理）
- Train: 2016-01-01 ~ 2022-12-31
- OOS:   2023-01-01 ~ 2025-12-31 (AI era, 不 peek)
- Target A (foreign): fwd5_foreign_net / fwd5_volume >= train 期 90 分位 (= 0.134)
- Target B (trust):   fwd5_trust_net / fwd5_volume >= train 期 90 分位 (= 0.016)
- 成本假設: 0.4% round-trip per week -> 1.6% per 20d holding

## OOS 結果（AI era 2023-2025）

| target | model | self_lag | AUC | prec@10 | prec@50 | top10_fwd20 | baseline_fwd20 | alpha_top10_net_20d |
|--------|-------|----------|-----|---------|---------|-------------|----------------|---------------------|
| foreign | gbm | False | 0.7152 | 0.402 | 0.354 | +0.0292 | +0.0247 | -0.0114 |
| foreign | gbm | True | 0.7324 | 0.493 | 0.451 | +0.0212 | +0.0247 | -0.0194 |
| foreign | logistic | False | 0.6721 | 0.325 | 0.297 | -0.0009 | +0.0247 | -0.0416 |
| foreign | logistic | True | 0.7022 | 0.454 | 0.407 | +0.0107 | +0.0247 | -0.0300 |
| foreign | baseline_momentum | False | - | 0.023 | 0.040 | +0.0660 | - | - |
| trust | gbm | False | 0.7488 | 0.531 | 0.513 | +0.0138 | +0.0247 | -0.0269 |
| trust | gbm | True | 0.8357 | 0.703 | 0.677 | +0.0162 | +0.0247 | -0.0244 |
| trust | logistic | False | 0.7150 | 0.430 | 0.363 | +0.0081 | +0.0247 | -0.0326 |
| trust | logistic | True | 0.7509 | 0.707 | 0.662 | +0.0122 | +0.0247 | -0.0285 |
| trust | baseline_momentum | False | - | 0.048 | 0.093 | +0.0660 | - | - |

## Verdict

- **Target FOREIGN (foreign)**: **D 級** - AUC=0.7152, net alpha (top10, 20d)=-0.0114. AUC < 0.55 或 net alpha 負, 放棄該 target
- **Target TRUST (trust)**: **D 級** - AUC=0.7488, net alpha (top10, 20d)=-0.0269. AUC < 0.55 或 net alpha 負, 放棄該 target

## :warning: 關鍵診斷：「法人買」≠「股票漲」

這是 V13 的核心洞察。看 top10 prediction 裡 **預測命中 (y=1, 法人真的買)** vs **預測落空 (y=0, 法人沒買)** 的 fwd_20d 差異：

| target | model | self_lag | prec@10 | hit fwd_20d | miss fwd_20d | 差異 | universe mean |
|--------|-------|----------|---------|-------------|--------------|------|--------------|
| foreign | gbm | False | 0.402 | +0.0320 | +0.0274 | +0.0046 | +0.0247 |
| foreign | gbm | True | 0.493 | +0.0200 | +0.0225 | -0.0024 | +0.0247 |
| foreign | logistic | False | 0.325 | +0.0143 | -0.0083 | +0.0226 | +0.0247 |
| foreign | logistic | True | 0.454 | +0.0192 | +0.0036 | +0.0156 | +0.0247 |
| trust | gbm | False | 0.531 | +0.0141 | +0.0134 | +0.0007 | +0.0247 |
| trust | gbm | True | 0.703 | +0.0170 | +0.0144 | +0.0027 | +0.0247 |
| trust | logistic | False | 0.430 | +0.0046 | +0.0107 | -0.0061 | +0.0247 |
| trust | logistic | True | 0.707 | +0.0140 | +0.0078 | +0.0061 | +0.0247 |

**解讀**：
- Target foreign gbm no-lag: 命中 +3.2% vs 落空 +2.7%，差異 +0.5%，低於 1.6% 成本
- Target trust gbm no-lag: 命中 +1.4% vs 落空 +1.3%，差異 +0.07%，**幾乎為零**
- 結論: 即使模型 100% 精準預測法人會買什麼，「被買」的股票跟「沒被買」的股票
  在 20d 報酬上幾乎沒差異。法人不是 leading indicator。

## Autocorrelation Check（self-lag 影響）

若加入自己 lag feature 後 AUC 大幅提升 (>0.05)，代表預測力主要來自 autocorrelation，非真 edge：

| target | model | AUC (no self-lag) | AUC (with self-lag) | Δ |
|--------|-------|-------------------|---------------------|---|
| foreign | gbm | 0.7152 | 0.7324 | +0.0172 |
| foreign | logistic | 0.6721 | 0.7022 | +0.0301 |
| trust | gbm | 0.7488 | 0.8357 | +0.0870 :warning: autocorrelation artifact |
| trust | logistic | 0.7150 | 0.7509 | +0.0359 |

## Top 5 Features (gbm, no self-lag)

### Target: foreign

| rank | feature | importance |
|------|---------|-----------|
| 1 | atr_pct | 0.0652 |
| 2 | sbl_ratio | 0.0469 |
| 3 | trust_flow_20d_norm | 0.0180 |
| 4 | mcap_log | 0.0180 |
| 5 | rsi_14 | 0.0105 |

### Target: trust

| rank | feature | importance |
|------|---------|-----------|
| 1 | mcap_log | 0.0681 |
| 2 | margin_utilization | 0.0435 |
| 3 | foreign_flow_20d_norm | 0.0262 |
| 4 | sbl_ratio | 0.0231 |
| 5 | f_score | 0.0098 |

## Baseline Comparison

- **Random baseline**: AUC 0.5, prec@10 = base_rate ≈ 10-16%
- **Momentum top-20d baseline** (pick top 10 by ret_20d each week):
  - target=foreign: prec@10=0.023, fwd20_top10=+0.0660, fwd20_top50=+0.0518
  - target=trust: prec@10=0.048, fwd20_top10=+0.0660, fwd20_top50=+0.0518
- **關鍵**: momentum top10 fwd_20d = +6.60%, 遠高於任何 V13 model (最高 +2.92%).
  純動能選股 (rule-based) 在 OOS 期間遠優於「預測法人買」的 ML 策略。

## Caveats

- **成本 0.4% round-trip 是保守**: 小型股滑價可能 >1%, 整體結果可能樂觀
- **Regime overfit**: OOS 僅 3 年 (2023-2025), AI era 單一市況，真實長期表現可能不同
- **Margin / SBL features**: 只有 2021-04 之後才有, train 期 2016-2020 部分 NaN
  (HistGBT 原生處理 NaN, 不影響訓練但 feature importance 可能低估)
- **Target 定義**: 用 fwd5_net / fwd5_volume 而非 % 流通股本 (流通股本資料缺), 可能偏向成交熱絡股
- **週頻 rebalance 違反使用者 #5 持有期 2 週-3 個月**: V13 是因子 POC, 若證明 edge 存在, 整合進 Mode D 時須再驗證較長 holding period AUC
- **Feature importance (GBM) 用 permutation importance**, 50k test subsample, 3 repeats (速度考量)

## 結論與下一步

**V13 = D 級 (兩個 target 都 fail)**

- Target A (foreign): AUC 0.72 但 top10 vs miss 差 0.5%, 扣成本淨 -1.1%
- Target B (trust):   AUC 0.75 但 top10 vs miss 差 0.07%, 扣成本淨 -2.7%

與使用者既往驗證結果一致：

- V1 低基期 D (反向)
- V2 底部背離 D (反向)
- V3 catalyst 3 個全 C/D
- V12-pair 同業輪動 4 signals 全 C
- **V13 AI 預測法人 D** ← 本驗證

**→ StockAnalyzer rule-based alpha 可驗範圍系統性耗盡**

**Mode D 最終設計**: 完全靠 thesis discretionary 進場，不做軍備競賽。
