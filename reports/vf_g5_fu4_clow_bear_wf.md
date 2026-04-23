# VF-G5 FU-4: C_low × TWII bear walk-forward (2026-04-23)

- Journal: 4923 picks; in bear clusters: 496
- Scenario proxy: A (trend_score≥9) / B (=8) / C_mid (=7) / C_low (<7, picks ≥6)

## TWII bear clusters (>50 days)

- **2015H2_China**: 2015-10-30 ~ 2016-03-02, picks n=40
- **2018H2_Fed_Trade**: 2018-10-04 ~ 2019-03-14, picks n=65
- **2020Q1_COVID**: 2020-03-09 ~ 2020-05-29, picks n=40
- **2022_FedHike**: 2022-04-07 ~ 2023-01-16, picks n=244
- **2025H1_Tariff**: 2025-03-07 ~ 2025-06-09, picks n=107

## T1: 每個 bear cluster × scenario fwd_60d mean

| cluster | scenario | n | mean_fwd60 | winrate_60 |
| --- | --- | --- | --- | --- |
| 2015H2_China | A | 14 | +7.74% | +85.71% |
| 2015H2_China | B | 3 | +5.29% | +100.00% |
| 2015H2_China | C_mid | 14 | -2.33% | +50.00% |
| 2015H2_China | C_low | 9 | -6.12% | +55.56% |
| 2018H2_Fed_Trade | A | 13 | +3.91% | +53.85% |
| 2018H2_Fed_Trade | B | 14 | +4.08% | +57.14% |
| 2018H2_Fed_Trade | C_mid | 22 | +13.57% | +59.09% |
| 2018H2_Fed_Trade | C_low | 16 | +7.29% | +75.00% |
| 2020Q1_COVID | A | 2 | +12.27% | +100.00% |
| 2020Q1_COVID | B | 16 | +17.28% | +62.50% |
| 2020Q1_COVID | C_mid | 11 | +27.37% | +63.64% |
| 2020Q1_COVID | C_low | 11 | +49.42% | +100.00% |
| 2022_FedHike | A | 53 | -7.84% | +33.96% |
| 2022_FedHike | B | 55 | -5.34% | +47.27% |
| 2022_FedHike | C_mid | 84 | -0.38% | +44.05% |
| 2022_FedHike | C_low | 52 | +6.32% | +59.62% |
| 2025H1_Tariff | A | 26 | -2.20% | +42.31% |
| 2025H1_Tariff | B | 16 | +2.40% | +37.50% |
| 2025H1_Tariff | C_mid | 39 | -4.63% | +25.64% |
| 2025H1_Tariff | C_low | 26 | +3.10% | +34.62% |

## T2: C_low pivot across clusters

| cluster | n | mean_fwd60 | winrate_60 |
| --- | --- | --- | --- |
| 2015H2_China | 9 | -6.12% | +55.56% |
| 2018H2_Fed_Trade | 16 | +7.29% | +75.00% |
| 2020Q1_COVID | 11 | +49.42% | +100.00% |
| 2022_FedHike | 52 | +6.32% | +59.62% |
| 2025H1_Tariff | 26 | +3.10% | +34.62% |

- **C_low 全 5 cluster 平均 fwd_60**: +12.00% (4/5 正)
- **排除 2022 後 4 cluster 平均**: +13.42% (3/4 正)

## T3: C_low 是否每個 cluster 都勝其他 scenario？

| cluster | A | B | C_mid | C_low |
| --- | --- | --- | --- | --- |
| 2015H2_China | +7.74% | +5.29% | -2.33% | -6.12% |
| 2018H2_Fed_Trade | +3.91% | +4.08% | +13.57% | +7.29% |
| 2020Q1_COVID | +12.27% | +17.28% | +27.37% | +49.42% |
| 2022_FedHike | -7.84% | -5.34% | -0.38% | +6.32% |
| 2025H1_Tariff | -2.20% | +2.40% | -4.63% | +3.10% |

- **C_low rank 1 (最佳) 的 cluster 數**: 3/5

## T4: Live 可行性 — bear regime 下每週 C_low picks 分佈

- Bear regime 週數: 103
- 每週 C_low picks 平均: 1.1 (median 1)
- 每週 C_low 占比平均: 22%
- 0 個 C_low picks 的週數: 39/103 (38%)

## 結論

### C_low × bear pattern robustness

- 5 個 bear cluster 平均 fwd_60: +12.00%
- 5 個 cluster 中 **4 個正**
- ✅ 排除 2022 後 4/4 正 (+3)，mean=+13.42% → pattern **robust across multiple bears**

### C_low 相對其他 scenario 的排名穩定性

- ✅ C_low 在 3/5 cluster 排名第 1 (≥60%)，dominant

### Live 可行性

- 每週平均 1.1 個，配置稀疏，需跨週累積
- 38% 週無 C_low picks，signal 不連續

### Outlier 敏感性（關鍵修正 — 自動判讀不夠謹慎）

「4/5 positive + avg +12%」表面 robust，但深入拆解發現 **2020 COVID 是 dominant driver**：

| 情境 | mean fwd_60 | pos clusters |
|---|---|---|
| 全 5 cluster | +12.00% | 4/5 |
| 排除 2022 | +13.42% | 3/4 |
| **排除 COVID** | **+2.65%** | 3/4 |
| **排除 COVID + 2022** | **+1.42%** | 2/3 |

COVID 單一 cluster (+49.42%, n=11) 將整體拉升 ~10pp。排除 COVID outlier 後 C_low bear **只剩 +1-3% fwd_60**，與其他 scenario 差距極小。**2015H2 C_low 甚至 -6.12% rank 4** 最差（A 反而 rank 1 +7.74%）。

### 最終決策 — 修正為不落地

❌ **不落地 live**：
1. COVID V-shape reversal 不是 typical bear，不適合代表「bear market 通用 pattern」
2. 排除 COVID 後 +2.65% 與 noise 難區分
3. 2015H2 China bear C_low rank 4 → cluster 間 inconsistency
4. 實務信號稀疏（每週 1.1 picks, 38% 週 0 picks）
5. VF-G4 regime filter (A 級 +78% Sharpe) 已覆蓋熊市防禦需求，邊際 alpha 重疊

### 教訓

- **outlier 敏感性檢查必要**：單一極端事件 (COVID) 可以把「robust pattern」→ 「弱 signal」
- Auto 判讀（mean + pos count）不足，必須**手動 leave-one-out 檢視**
- 與 VF-G5 Test 2 (top_10 fwd_40 被 6-10 超車) 同 pattern：**表面數字與實質 alpha 不一致**

### 與 VF-G4 regime filter 的關係

VF-G4 (only_volatile Sharpe +78%) 已是**策略層級** regime-aware filter：
- VF-G4 = **whole-strategy 級**（universe filter）
- FU-4 = **scenario 級**（picks selection overlay）
- FU-4 的邊際 alpha 被 VF-G4 吸收
- **熊市 alpha 主要靠 regime filter，不靠 picks selection 差異化**

### 關閉 FU-4

C_low × bear 歸檔為歷史 finding，無 actionable alpha。FU-4 結束，不再追。

## 產出

- `tools/vf_g5_fu4_clow_bear_wf.py`
- `reports/vf_g5_fu4_clow_bear_wf.md`
- `reports/vf_g5_fu4_cluster_scenario.csv`
- `reports/vf_g5_fu4_clow_pivot.csv`
- `reports/vf_g5_fu4_cluster_pivot.csv`
- `reports/vf_g5_fu4_weekly_clow_dist.csv`