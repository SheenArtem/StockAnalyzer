# VF — ROIC factor IC validation

- Window: 2015-01-01 ~ 2025-12-31
- Universe: 普通股（universe_tw_full.is_common_stock）
- Announce delay: 45 天
- Min cross-section: 100 stocks per month
- ROIC = NOPAT_TTM / Avg_InvestedCapital
  - NOPAT = OperatingIncome_TTM × (1 - eff_tax) ; eff_tax∈[0,0.35]
  - IC = Equity + ShortDebt + LongDebt - Cash

## R1-R3 一覽

| Factor | Horizon | n | IC | IR | hit | Sp Sharpe | Mono | WF sign-hit | Grade |
|---|---|---|---|---|---|---|---|---|---|
| yoy | 1m | 107 | +0.0049 | +0.073 | 0.57 | +0.089 | +0.697 | 0.3333333333333333 | D |
| yoy | 3m | 107 | +0.0052 | +0.078 | 0.54 | +0.164 | +0.709 | 0.0 | D |
| yoy | 6m | 105 | -0.0043 | -0.062 | 0.55 | +0.203 | +0.382 | 0.3333333333333333 | C* |
| yoy | 12m | 99 | -0.0187 | -0.243 | 0.51 | +0.197 | -0.042 | 0.3333333333333333 | C* |
| slope | 1m | 86 | -0.0082 | -0.107 | 0.43 | -0.096 | -0.600 | 0.0 | D |
| slope | 3m | 86 | -0.0193 | -0.248 | 0.41 | -0.129 | -0.673 | 0.5 | D |
| slope | 6m | 84 | -0.0301 | -0.376 | 0.36 | -0.184 | -0.600 | 0.5 | D |
| slope | 12m | 78 | -0.0303 | -0.384 | 0.46 | -0.052 | -0.503 | 0.0 | D |
| level | 1m | 119 | +0.0382 | +0.388 | 0.67 | -0.105 | -0.588 | 1.0 | C* |
| level | 3m | 119 | +0.0422 | +0.420 | 0.65 | -0.173 | -0.709 | 0.75 | C* |
| level | 6m | 117 | +0.0377 | +0.354 | 0.63 | -0.262 | -0.721 | 0.75 | C* |
| level | 12m | 111 | +0.0290 | +0.254 | 0.50 | -0.277 | -0.527 | 0.25 | C* |

## R4 Regime breakdown (TWII 200d MA, h=6m)

| Factor | Regime | n | IC | IR | hit |
|---|---|---|---|---|---|
| yoy | bull | 83 | -0.0048 | -0.070 | 0.53 |
| yoy | bear | 22 | -0.0022 | -0.032 | 0.64 |
| slope | bull | 64 | -0.0214 | -0.269 | 0.41 |
| slope | bear | 20 | -0.0581 | -0.747 | 0.20 |
| level | bull | 93 | +0.0317 | +0.317 | 0.60 |
| level | bear | 24 | +0.0608 | +0.473 | 0.75 |

## R5 與 F-Score 的相關性 (Spearman)

| Factor | n | ρ(factor, quality_score) | ρ(factor, f_score) | 增量價值 |
|---|---|---|---|---|
| yoy | 172498 | +0.143 | +0.105 | 高（獨立） |
| slope | 132200 | +0.089 | +0.040 | 高（獨立） |
| level | 198261 | +0.482 | +0.330 | 中 |

## Decile Q1-Q10 平均月報酬 (h=6m)

| Factor | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 | Q10 |
|---|---|---|---|---|---|---|---|---|---|---|
| yoy | +6.32% | +8.00% | +8.37% | +8.52% | +8.64% | +7.88% | +8.47% | +8.88% | +9.80% | +7.62% |
| slope | +8.93% | +10.72% | +10.59% | +10.24% | +9.70% | +9.11% | +9.18% | +9.35% | +7.11% | +7.58% |
| level | +8.96% | +10.77% | +9.02% | +7.89% | +8.19% | +8.65% | +8.79% | +8.89% | +6.96% | +6.36% |

## Grading 圖例

- **A**: IC>=0.03 且 |IR|>=0.3 + decile/IC 同向 + monotonicity 同向
- **B**: IC 0.02-0.03, |IR| 0.2-0.3 + 方向一致
- **C**: IC 0.01-0.02 觀察
- **C\***: IC 數字 OK 但 decile spread / monotonicity 反向 → 不可上線（IC 假象）
- **D**: IC<0.01 或反向


## 結論與建議（Validator 判讀）

### 三因子最終評級

| Factor | 最終 grade | 說明 |
|---|---|---|
| F1 ROIC YoY Δ | **D** | 短期 IC 接近 0（+0.005），長期反向（-0.019）但 IR 弱；Decile 模糊；regime 雙邊接近零。**沒 alpha**。 |
| F2 ROIC 3yr slope | **D（反向）** | h=6m IC=-0.030 t=-3.44，**slope 越陡越差**（mean reversion）。Decile mono -0.6 一致負向。但 IR 0.38 強度仍未到可獨立上線（只能配合長空 short），台股做多池內無用。 |
| F3 ROIC level | **C\*（IC 假象，不可上線）** | IC +0.038 t=+4.23 看似 A 級，但 **decile spread Sharpe 全部負（-0.10 ~ -0.28）、mono -0.59 ~ -0.72**，Q1+8.96% 高於 Q10+6.36%。 IC 是「中段排序資訊」捕捉到的，不是真正可交易的多空 alpha。長 Q10 / 短 Q1 會虧錢。 |

### 為什麼 ROIC level 看起來 A 但實際 C\*

Decile 表 Q1=+8.96%, Q2=+10.77%(峰值), Q3=+9.02%, ..., Q9=+6.96%, Q10=+6.36%。
是 **倒 U 形 + 右尾衰退**：最低 ROIC（Q1 困境）和最高 ROIC（Q10 已被定價）兩端皆輸給中段 Q2-Q3。
Spearman IC 把全 panel 排序資訊壓成一個數字，會誤抓中段 Q2→Q10 的負斜率為「正相關」，但長/空交易只看 Q10-Q1 spread，得到反向。

這是典型 **「Quality 已被定價」+「困境股 mean reversion」雙重效應**，宋分論點「ROIC 高 = 複利好」在台股 panel 內已被市場吸收，不存在剩餘 alpha。

### 與 F-Score 的關係

- ρ(level, quality_score) = +0.48 — F-Score 已帶有 ROA、毛利、債務改善等 ROIC 近似訊號；ROIC level 增量低。
- ρ(yoy, quality_score) = +0.14、ρ(slope, quality_score) = +0.09 — 增量訊號獨立但因子本身無 alpha。

### Bear regime 警示

F3 level 在 bear regime（n=24）IC=+0.061 IR=+0.47，但 bear 月份僅 24 個樣本，置信度不足；**且 decile spread 在全期就反向**，bear 不能單獨救活這因子。

### 上線建議

**全部 D / C\*，不上線 value_screener。**

- 不調整 30/25/30/15/0 五面向權重。
- 不新增 ROIC 因子到 quality_score。
- 不建議再做「ROIC × XXX 互動」回測（基底因子無 alpha，互動更難穩）。

### 後續可探索方向（不在本輪）

1. **ROIC - WACC spread**：理論上更乾淨的 economic profit，但需 WACC 估算（台股缺 cost of debt 細節）。
2. **ROIC 變動方向 + 估值低**：Quality + Value 互動（高 ROIC + 低 PE 才買），需另案驗證。
3. **ROIC 長期穩定度（過去 5 年標準差）**：補捉「複利穩定度」而非斜率，可作為 quality filter 而非 alpha factor。
4. **產業中性化 ROIC**：金融/科技 ROIC 結構性差異大，去產業中位後再排序，看 IC 是否提升。

### 歸檔指引

依 SOP 將本次驗證結果寫入 `project_songfen_value_factors.md`，標記「ROIC YoY Δ / slope / level — D 級歸檔 2026-04-27」。
