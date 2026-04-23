# VF-G5 FU-3 Scenario × Regime 二維分析 (2026-04-23)

- Journal: 4923 picks × 538 weeks × 205 tickers
- Proxy: A (trend_score≥9) / B (=8) / C_mid (=7) / C_low (<7, picks ≥6)

## Baseline: Scenario × fwd return (no regime)

| scenario | n | mean_20d | win_20d | mean_40d | win_40d | mean_60d | win_60d |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A | 1230 | +1.22% | +50.81% | +2.07% | +51.30% | +2.72% | +49.19% |
| B | 1156 | +1.58% | +53.29% | +3.25% | +52.34% | +5.63% | +53.20% |
| C_low | 695 | +1.21% | +53.81% | +3.91% | +53.24% | +5.88% | +52.52% |
| C_mid | 1842 | +1.47% | +51.90% | +3.30% | +51.90% | +5.05% | +53.31% |

## Scenario × HMM regime

HMM regime (from trade_journal): trending / volatile / ranging / neutral

| scenario | regime | n | mean_20d | win_20d | mean_40d | win_40d | mean_60d | win_60d |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | neutral | 348 | +1.05% | +50.29% | +1.64% | +53.74% | +1.63% | +50.00% |
| A | ranging | 278 | +0.39% | +47.12% | +1.74% | +49.28% | +4.51% | +51.44% |
| A | trending | 242 | +0.16% | +47.11% | +1.32% | +48.35% | +1.29% | +47.11% |
| A | volatile | 362 | +2.72% | +56.63% | +3.23% | +52.49% | +3.34% | +48.07% |
| B | neutral | 354 | +1.28% | +51.69% | +1.46% | +50.28% | +3.55% | +49.72% |
| B | ranging | 292 | +2.79% | +57.53% | +6.22% | +55.82% | +10.96% | +59.93% |
| B | trending | 196 | -0.91% | +44.90% | +1.13% | +45.41% | +3.74% | +49.49% |
| B | volatile | 314 | +2.36% | +56.37% | +3.84% | +55.73% | +4.19% | +53.18% |
| C_low | neutral | 191 | +0.61% | +50.79% | +4.79% | +54.45% | +8.43% | +57.07% |
| C_low | ranging | 180 | -0.97% | +50.56% | -1.19% | +45.00% | -0.78% | +42.78% |
| C_low | trending | 83 | +0.50% | +48.19% | +1.07% | +45.78% | +1.58% | +49.40% |
| C_low | volatile | 241 | +3.55% | +60.58% | +8.00% | +61.00% | +10.31% | +57.26% |
| C_mid | neutral | 578 | +1.09% | +48.44% | +2.76% | +51.56% | +4.47% | +52.42% |
| C_mid | ranging | 522 | +1.78% | +54.79% | +2.84% | +50.96% | +5.48% | +52.68% |
| C_mid | trending | 219 | +0.20% | +45.66% | +4.12% | +52.51% | +4.83% | +57.53% |
| C_mid | volatile | 523 | +2.11% | +55.45% | +4.00% | +52.96% | +5.35% | +53.15% |

### A 在各 HMM regime fwd_60d mean

- neutral (n=348): fwd_60d = +1.63%, winrate = +50%
- ranging (n=278): fwd_60d = +4.51%, winrate = +51%
- trending (n=242): fwd_60d = +1.29%, winrate = +47%
- volatile (n=362): fwd_60d = +3.34%, winrate = +48%

## Scenario × TWII bull/bear (SMA200)

TWII bull = TWII close > 200-day SMA；bear = below

| scenario | twii_regime | n | mean_20d | win_20d | mean_40d | win_40d | mean_60d | win_60d |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | bear | 131 | -0.49% | +46.56% | -1.67% | +44.27% | -2.72% | +42.75% |
| A | bull | 1080 | +1.48% | +51.57% | +2.66% | +52.59% | +3.54% | +50.37% |
| A | unknown | 19 | -1.84% | +36.84% | -5.71% | +26.32% | -6.44% | +26.32% |
| B | bear | 129 | +0.05% | +53.49% | +1.08% | +55.04% | +1.40% | +55.04% |
| B | bull | 1007 | +1.89% | +53.82% | +3.74% | +52.73% | +6.48% | +53.72% |
| B | unknown | 20 | -3.72% | +25.00% | -7.03% | +15.00% | -9.83% | +15.00% |
| C_low | bear | 130 | +1.15% | +56.92% | +6.38% | +64.62% | +10.34% | +57.69% |
| C_low | bull | 559 | +1.31% | +53.49% | +3.44% | +50.81% | +4.96% | +51.34% |
| C_low | unknown | 6 | -6.71% | +16.67% | -5.46% | +33.33% | -5.21% | +50.00% |
| C_mid | bear | 244 | -0.09% | +51.23% | +0.02% | +45.90% | +1.93% | +45.49% |
| C_mid | bull | 1568 | +1.83% | +52.49% | +3.96% | +53.32% | +5.76% | +55.10% |
| C_mid | unknown | 30 | -4.50% | +26.67% | -4.93% | +26.67% | -6.70% | +23.33% |

### A 在 bull vs bear

- bear (n=131): fwd_60d = -2.72%, winrate = +43%
- bull (n=1080): fwd_60d = +3.54%, winrate = +50%
- unknown (n=19): fwd_60d = -6.44%, winrate = +26%

## A vs C avg 差距 (fwd_60d)

| regime split | A fwd_60d | C avg fwd_60d | Δ (A - C) |
| --- | --- | --- | --- |
| HMM / neutral | +1.63% | +5.45% | -3.83% |
| HMM / ranging | +4.51% | +3.87% | +0.64% |
| HMM / trending | +1.29% | +3.94% | -2.65% |
| HMM / volatile | +3.34% | +6.91% | -3.57% |
| TWII / bear | -2.72% | +4.85% | -7.57% |
| TWII / bull | +3.54% | +5.55% | -2.01% |
| TWII / unknown | -6.44% | -6.45% | +0.01% |

## Scenario × Year fwd_60d

| year | A_fwd60 | B_fwd60 | C_low_fwd60 | C_mid_fwd60 |
| --- | --- | --- | --- | --- |
| 2015 | -2.10% | -7.86% | -7.52% | -5.86% |
| 2016 | +3.53% | +2.03% | +2.61% | +2.63% |
| 2017 | +3.50% | +8.53% | +1.76% | +7.08% |
| 2018 | -1.83% | -5.34% | -5.99% | -2.41% |
| 2019 | -1.64% | +3.02% | +6.48% | +4.38% |
| 2020 | +8.20% | +8.81% | +14.19% | +9.43% |
| 2021 | +3.94% | +4.50% | +9.95% | +8.32% |
| 2022 | -6.49% | -7.72% | -1.84% | -4.52% |
| 2023 | +6.99% | +13.99% | +7.17% | +12.20% |
| 2024 | +4.97% | +5.99% | +14.03% | +2.67% |
| 2025 | +5.07% | +18.12% | +5.57% | +6.81% |

## 結論

### A 是否 regime-dependent?

- TWII bull A fwd_60d: +3.54% (n=1080)
- TWII bear A fwd_60d: -2.72% (n=131)
- **差距: +6.26%**

- TWII bull C_low fwd_60d: +4.96%
- TWII bear C_low fwd_60d: +10.34%

### 判讀

**1. A 是 TWII bull/bear dependent，不是 HMM regime dependent**
- TWII bull vs bear Δ = **+6.26pp**（> 5pp threshold，strong regime-dependent）
- HMM regime 各 Δ 只 -3.83 ~ +0.64pp，分辨力弱
- 符合 VF-G4 結論：TWII-based regime filter > HMM-based

**2. A 熊市不是防禦強，是真虧**
- A bear -2.72% + winrate 43% → 明確虧損
- 推測：A (trend_score ≥ 9) picks 是「已漲很多」的強勢股，熊市 mean-revert 最嚴重

**3. C_low 熊市 +10.34% 是意外發現（novel）**
- C_low (trend_score < 7) picks 熊市 +10.34% 遠勝牛市 +4.96%
- 推測：低動能 picks = 「沒過熱」= 熊市底部有反彈力道
- 但樣本 n=130 偏小，要確認 robustness 需 walk-forward

**4. By-year 熊市對比**
| 年 | A | B | C_low | C_mid |
|---|---|---|---|---|
| 2015 熊 | -2.10% | -7.86% | -7.52% | -5.86% | A 相對好 |
| 2018 熊 | -1.83% | -5.34% | -5.99% | -2.41% | A 相對好 |
| 2022 熊 | -6.49% | -7.72% | -1.84% | -4.52% | **C_low 最好** |

2022 年是 C_low bear outperform 的主要驅動（sample concentration concern）。

### 落地建議

**保守選項（推薦）**：記錄 flag，不動 live。
- 因為只 1 次 cross-sectional 分析，未 walk-forward，overfit 風險高
- VF-G4 regime filter (only_volatile A 級) 已部分覆蓋熊市防禦需求
- C_low bear +10.34% 主要來自 2022 單一年，樣本集中

**積極選項（需 walk-forward 才能落地）**：
- A: Scenario A regime gate：TWII bear 時 A 降級為 B（stop 2:1 + 不加倉）
- B: A × TWII bull 才允許進場；TWII bear 時空手
- C: 將「C_low in TWII bear」作為新的 entry pattern（底部反彈買點）

**建議 follow-up**：若未來擴樣本 / 擴 walk-forward 驗證框架時，Priority A > B > C。但當前無強動機動 live（VF-G4 已是 A 級解決方案）。

### Robustness check（2022 是否 dominant）

拆年分析顯示 **A bear 與 C_low bear 是 opposite robustness pattern**：

**A × bear by year**：
| Year | n | A fwd_60 |
|---|---|---|
| 2015 | 8 | +8.18% |
| 2016 | 8 | +6.44% |
| 2018 | 18 | -4.52% |
| 2019 | 7 | +11.31% |
| 2020 | 2 | +12.27% |
| **2022** | **57** | **-7.17%** |
| 2023 | 1 | -6.88% |
| 2025 | 30 | -2.67% |

- 移除 2022 後 A × bear = **+0.71%**（原 -2.72% 幾乎全部來自 2022）
- 2022 單一年驅動 → **A bear -2.72% 非 robust pattern**，是 2022 (Fed 升息 + Ukraine + 電子下行) 三重熊特殊年

**C_low × bear by year**：
| Year | n | C_low fwd_60 |
|---|---|---|
| 2015 | 7 | -9.51% |
| 2016 | 6 | +3.57% |
| 2018 | 15 | +21.43% |
| 2019 | 10 | +12.48% |
| 2020 | 11 | +49.42% |
| **2022** | **51** | **+3.30%** |
| 2023 | 4 | +37.53% |
| 2025 | 26 | +3.10% |

- 移除 2022 後 C_low × bear = **+14.88%**（原 +10.34% 去 2022 反而更強）
- **C_low bear pattern cross-year robust**，2018/2019/2020 都 +12~49%
- 2022 雖是最大樣本 (n=51) 反而表現最平（+3.30%）

### 修正結論

1. **A bear 虧損非 robust**：-2.72% 是 2022 single-year artifact；其他熊市年 A 表現 mixed（2015/2016/2019 正，2018 負），**取消「A bear regime filter」的落地理由**
2. **C_low bear +14.88% robust**：跨年一致（2018-2020 三年都大正），是 novel finding 值得 follow-up
3. **C_low in bear 可解釋為「契約反彈股」pattern**：弱動能但基本面過濾已過的 picks，熊市底部買反彈

### 修訂落地建議

**取消**「A × TWII bear filter」落地（non-robust，2022 artifact）
**flag 新 follow-up FU-4**：C_low × TWII bear 底部反彈 pattern 做 walk-forward 驗證
- 實務可行性：live top_20 中 C_low picks 數量？若占比 <20% 動作意義有限
- 需搭配「進場即預期熊市底部」策略，複雜度高

**本輪**：C_low bear finding 純歸檔，不動 live。

## 產出

- `tools/vf_g5_fu3_scenario_regime.py`
- `reports/vf_g5_fu3_scenario_regime.md`
- `reports/vf_g5_fu3_{baseline,hmm_regime,twii_regime,year_pivot}.csv`