# VF-Value-ex2 EDGAR IC 驗證 — Piotroski 原版 P/B Filter

Generated: 2026-04-22 16:09

## 驗證動機

前一輪驗證將 F-Score 當全市場 cross-section factor 使用，結論為 D 級反向（IC IR = -0.272，F>=8 alpha = -10.1%）。
Piotroski 2000 原論文的正確用法是：**先篩 low P/B (book-to-market top quintile)**，再在 value universe 內用 F-Score 區分贏家/輸家。本輪重驗此精神。

P/B 計算方式：
- book_value = StockholdersEquity（從 EDGAR XBRL 重新 extract，新增欄位）
- fallback: TotalAssets - Liabilities（StockholdersEquity 缺值時）
- market_cap = entry_date 的 AdjClose x SharesOutstanding_q
- 每個 quarter_end 截面排序，取 bottom K% (lowest P/B = value universe)
- 排除 book_value <= 0（負淨值股票）、P/B 為 NaN 的觀測

## Scenario 對比主表

| Scenario | N obs | Mean IC (12m) | IC IR (12m) | F>=8 alpha (12m) | F>=7 alpha (12m) | Top-Bot spread (12m) | Grade |
|---|---|---|---|---|---|---|---|
| Unfiltered (前一輪全市場) | 52,062 | -0.017 | -0.272 | -10.11% | -8.19% | -13.08% | D 反向 |
| P/B bottom 30% | 14,737 | -0.020 | -0.296 | -4.64% | -2.69% | -10.83% | D/C (仍反向) |
| P/B bottom 20% | 9,832 | -0.021 | -0.314 | -3.15% | -2.99% | -8.69% | D/C (仍反向) |

注意：P/B bottom 30% 的 P/B 截面分位數中位數約 1.43x book，20% 約 1.10x book。

## P/B bottom 30% 詳細結果

P/B quantile threshold: bottom 30% by cross-section P/B (mean cutoff ~1.46x book)
Sample: 14,737 obs across 37 quarters

### IC Summary (Spearman, f_score vs forward return)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N quarters | Grade |
|---|---|---|---|---|---|---|---|
| ret_3m | -0.0104 | 0.0842 | -0.124 | -0.75 | 51.4% | 37 | D (noise) |
| ret_6m | -0.0102 | 0.0823 | -0.124 | -0.76 | 51.4% | 37 | D (noise) |
| ret_12m | -0.0200 | 0.0676 | -0.296 | -1.80 | 37.8% | 37 | C (weak) |

### Group Annualized Returns (12m horizon)

| Group | Ann. Return |
|---|---|
| Top decile (F>=8) | +18.90% |
| Bot decile (F<=3) | +25.78% |
| Top - Bot spread | -10.83% |
| F>=8 | +18.90% |
| F>=7 | +20.85% |
| F<=5 | +23.55% |
| **F>=8 alpha vs F<=5** | **-4.64%** |
| F>=7 alpha vs F<=5 | -2.69% |

### F-Score Distribution (value subset, P/B bottom 30%)

| F-Score | Count | Pct |
|---|---|---|
| 0 | 30 | 0.2% |
| 1 | 213 | 1.4% |
| 2 | 833 | 5.7% |
| 3 | 2,130 | 14.5% |
| 4 | 3,481 | 23.6% |
| 5 | 3,735 | 25.3% |
| 6 | 2,531 | 17.2% |
| 7 | 1,255 | 8.5% |
| 8 | 440 | 3.0% |
| 9 | 89 | 0.6% |

F>=8: 3.6% | F>=7: 12.1% | F<=5: 70.7%

### Regime Breakdown (P/B bottom 30%)

| Regime | N obs | N quarters | IC IR 3m | IC IR 6m | IC IR 12m | F>=8 alpha 6m |
|---|---|---|---|---|---|---|
| bear | 1,548 | ~10 | -0.003 | -0.278 | -0.035 | +0.08% |
| bull | 11,252 | ~25 | +0.032 | +0.070 | -0.213 | -31.84% |
| ranged | 349 | ~3 | N/A | N/A | N/A | +11.45% |
| volatile | 1,588 | 4* | -1.007 | -0.977 | -3.175* | +18.59% |

*volatile 12m IC IR = -3.175 是假象：僅 4 個 quarter 有 ret_12m 資料，std 極小導致 IR 失控，不可採信。

## P/B bottom 20% 詳細結果

P/B quantile threshold: bottom 20% by cross-section P/B (mean cutoff ~1.10x book)
Sample: 9,832 obs across 37 quarters

### IC Summary (Spearman, f_score vs forward return)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N quarters | Grade |
|---|---|---|---|---|---|---|---|
| ret_3m | -0.0195 | 0.0881 | -0.221 | -1.35 | 51.4% | 37 | C (weak) |
| ret_6m | -0.0198 | 0.0807 | -0.246 | -1.50 | 32.4% | 37 | C (weak) |
| ret_12m | -0.0211 | 0.0671 | -0.314 | -1.91 | 40.5% | 37 | C (weak) |

### Group Annualized Returns (12m horizon)

| Group | Ann. Return |
|---|---|
| Top decile (F>=8) | +23.09% |
| Bot decile (F<=3) | +31.78% |
| Top - Bot spread | -8.69% |
| F>=8 | +23.09% |
| F>=7 | +23.25% |
| F<=5 | +26.24% |
| **F>=8 alpha vs F<=5** | **-3.15%** |
| F>=7 alpha vs F<=5 | -2.99% |

### F-Score Distribution (value subset, P/B bottom 20%)

| F-Score | Count | Pct |
|---|---|---|
| 0 | 21 | 0.2% |
| 1 | 159 | 1.6% |
| 2 | 633 | 6.4% |
| 3 | 1,524 | 15.5% |
| 4 | 2,399 | 24.4% |
| 5 | 2,449 | 24.9% |
| 6 | 1,595 | 16.2% |
| 7 | 742 | 7.5% |
| 8 | 264 | 2.7% |
| 9 | 46 | 0.5% |

F>=8: 3.2% | F>=7: 10.7% | F<=5: 73.1%

### Regime Breakdown (P/B bottom 20%)

| Regime | N obs | IC IR 3m | IC IR 6m | IC IR 12m | F>=8 alpha 6m |
|---|---|---|---|---|---|
| bear | 1,030 | -0.240 | -0.337 | -0.018 | +2.48% |
| bull | 7,512 | -0.059 | -0.082 | -0.231 | -27.99% |
| ranged | 231 | N/A | N/A | N/A | +15.80% |
| volatile | 1,059 | -0.744 | -0.683 | -0.779 | +29.70% |

## 核心結論

**D 級：Piotroski 在 US 近 10 年失效，即使用原版 low P/B filter 也不例外。**

- P/B filter 確實將 F>=8 alpha 的反向幅度從 -10.1% 收窄到 -3.2%（bottom 20%），方向改善但仍為負
- IC IR 在 P/B subset 內從 -0.272 變化到 -0.314（更負），主要因為 value universe 本身在 2015-2024 落後市場的 absolute drag 加大
- 兩個 threshold（30%/20%）結論一致：**低 P/B 反而是壞事（value trap），high F-Score 在 value trap 中仍然跑輸**
- t-stat 均未達統計顯著（|t| < 2.0），所有結論在統計上仍屬 noise 範疇

**建議：走選項 A 全砍，不在 value screener 中使用 F-Score 加分。**

### 唯一亮點：Bear regime 的微弱訊號

在 P/B bottom 20% + bear regime 中，F>=8 alpha 6m = +2.48%（正向），但：
- 樣本僅 1,030 obs，且 IC IR 3m/6m 仍為負（-0.240/-0.337）
- 12m IC IR 趨近 0（-0.018），方向有改善但未達顯著
- 結論：bear regime + low P/B + high F-Score 有微弱方向性，但不足以支持實盤使用

### 為什麼（不）有效的 Nuance 分析

**1. Value factor 近 10 年的結構性壓制**
- 2015-2021 以 growth/momentum 主導，low P/B 本身就是落後指標（factor beta 為負）
- 低 P/B 股票大量是 "value traps"：能源、金融、零售等 disrupted 產業
- 在這個 universe 內，F-Score 低分的公司（爛股）反而有高 beta，在牛市中表現更佳
- 換言之：F<=5 在 value universe 中 = 高 beta + 困境反彈潛力，反而跑贏 F>=8 的保守型

**2. F-Score 設計的時代侷限**
- Piotroski 2000 原始樣本是 1976-1996，價值投資主導的時代
- 2015+ 的市場，無形資產/網絡效應/平台壟斷不在 F-Score 9 個 GAAP 指標中
- 例：Amazon 2014-2020 F-Score 低（CFO < NI、ROA 邊際），但市場給極高估值
- 傳統 GAAP 財報越來越無法捕捉真實 competitive moat

**3. F-Score 在 low P/B universe 中的結構性問題**
- Low P/B 選出的公司本來就財務狀況偏差（否則市場不會給低估值）
- 在這個 universe 中，F>=8 的公司是「例外中的好學生」，但可能缺乏 upside catalyst
- F<=5 的公司處於困境但有反彈可能性（高 beta），牛市中的困境反彈往往跑贏

**4. Regime 非對稱性（最重要的 nuance）**
- Bull regime (n=11,252)：F>=8 alpha 6m = -31.84%，F-Score 完全失效，低分公司反彈更猛
- Bear regime (n=1,548)：F>=8 alpha 6m = +0.08%，幾乎中性，略有保護性
- Volatile regime：樣本太少（4 quarters），不可信
- 即使在 bear 中，F-Score 在 low P/B universe 的保護作用也微乎其微

**5. 實務建議（按優先級）**
1. 放棄在 US value screener 中使用 F-Score 作為加分項目（D 反向已二度驗證）
2. 若仍要做財務質量過濾，改用更現代的質量因子：FCF yield、ROIC、毛利率趨勢（3 年）
3. P/B filter 本身的有效性也值得重新評估（US low P/B 近 10 年 absolute 跑輸）
4. 考慮用 momentum + quality 替代傳統 value + F-Score 組合
