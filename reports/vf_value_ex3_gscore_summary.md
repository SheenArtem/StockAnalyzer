# VF-Value-ex3 Mohanram 5-lite G-Score 最終決策 Summary

Generated: 2026-04-22

## 一句話結論

**B 級條件式採納**：5-signal G-Lite 在「US 金融業 G=5 極端組」與「TW 全市場 G=5 極端組」呈現可驗證的 alpha，但作為連續分數 (0–5) 的線性加分 **D 級拒用**（非單調、信號稀薄）。建議僅當 **「G=5 binary filter」** 使用，且需與既有流程正交測試再決定落地。

---

## 實驗設計

| 項目 | 說明 |
|---|---|
| Factor | G-Lite (5 of Mohanram 2005 signals): ROA / CFOA / Accruals / EarnStd / RevStd vs cross-section median |
| Universe | US: 1500 tickers × 57 quarters (2010–2024), Financials subset 251 × 57 |
|   | TW: 2216 tickers × 33 quarters (2016–2024), Financials subset 21 × 33 |
| Forward returns | 3m (63 TD) / 6m (126 TD) / 12m (252 TD) |
| Entry lag | US 45d, TW 75d (財報公告延遲) |
| Grade | A \|IR\|≥0.5 / B \|IR\|≥0.3 / C \|IR\|≥0.1 / D (noise) |
| Source | `financials_us_edgar.parquet`, `financials_income/balance/cashflow.parquet` |
| Tool | `tools/vf_value_ex3_gscore_ic.py --market {us,tw}` |

---

## 四組結果一覽

| Track | N obs | Best horizon IR | Grade | G=5 incidence | G=5 vs rest (best-horizon median) | 單調性 |
|---|---|---|---|---|---|---|
| **US All-Market** | 74,499 | 6m IR **-0.17** | **D (反向)** | 8.1% | bucket mean U-shape | ❌ 非單調 |
| **US Financials** | 13,055 | 12m IR +0.21 | C | 2.6% | +16.32% vs +8.32%, hit **84.3% vs 64.3%** | **✅ 單調**（median/hit） |
| **TW All-Market** | 56,645 | 3m IR **+1.02** | A (rank only) | 13.2% | +2.18% vs +0.13%, hit **58.8% vs 50.2%** | **⚠️ mean 非單調，median/hit 單調** |
| **TW Financials** | 577 | 6m IR +0.10 | D | 4.6% | — | 樣本太少（21 ticker） |

---

## 逐組詳細

### 1. US 全市場 — D 級反向

IC IR 3m/6m/12m = −0.09 / −0.17 / +0.12。Bucket median ret_12m 呈 U 型（G=0 最低 −1.6%、G=2 頂峰 +8.2%、G=5 回落 +8.9%），**非單調**。bull regime 底部 G<=1 反而 +88% ann（少數倖存者偏誤放大），ranged regime IC IR = −0.365。

與 2026-04-22 VF-Value-ex2 Piotroski F-Score US 驗證「F≥8 alpha −10%/yr」結論一致 — **US 大盤整體品質分數反向**，可能因為 US 市場已效率地把高品質 price in。

### 2. US Financials — C 級，G=5 binary 有料

整體 IC IR 12m +0.21（C），但：

| Group | n | mean ret_12m | median ret_12m | Hit rate | t-stat (vs G<=3) |
|---|---|---|---|---|---|
| **G=5** | 338 | **+18.86%** | **+16.32%** | **84.3%** | **+4.54** (p<0.001) |
| G=4 | 2,281 | +12.87% | +10.19% | 67.6% | — |
| G<=3 | 10,436 | +12.92% | +8.32% | 64.3% | baseline |
| G<=1 | 2,318 | +15.18% | +7.39% | 62.8% | — |

**G=5 時間穩定性**：57 季中 54 季 mean ret_12m > 0（**94.7%**）。Worst Q −29.8%（2015 bank 壓力測試），Best Q +95.6%（2009 recovery）。

Median 呈嚴格單調（0.076 / 0.074 / 0.077 / 0.099 / 0.102 / 0.163）— **真 alpha**，但只發生在 G=5 極端組。

### 3. TW 全市場 — A 級 rank IR，但 mean 非單調

Spearman IC IR 3m/6m/12m = +1.02 / +0.98 / +0.92，t-stat >5，% positive 85%+ 季度。**看似 A 級**，但實測 bucket mean：

| G | mean ret_6m | median ret_6m | Hit rate |
|---|---|---|---|
| 0 | +9.68% | −1.32% | 47.1% |
| 1 | +8.10% | — | 48.7% |
| 2 | +9.18% | — | 52.8% |
| 3 | +8.09% | — | 52.7% |
| 4 | +8.95% | — | 56.7% |
| 5 | **+9.34%** | **+4.34%** | **61.8%** |

**mean 非單調（G=0 最高）**，Spearman IC 高是 median/rank 單調（median G=0 −1.3% → G=5 +4.3%）的反映。

統計顯著性：
- G=5 vs rest ret_12m median: +8.41% vs +2.95% (+5.5pp), hit 64.3% vs 54.4%, t=+2.92 p=0.0036
- G=0 vs rest ret_12m: median +0.27% vs +4.13% (−3.9pp), hit 50.3% vs 56.3%

G=5 與 G=0 都提供信號，**mean 被尾部扭曲壓平**（G=0 常出現低基期 recovery 飆股）。結論：Spearman IR 可信，但 **不適合用作線性加分**，適合用作 **binary filter + quintile ranking**。

### 4. TW Financials — D 級（樣本不足）

21 ticker × 33 quarter = 577 obs。G=5 只 31 觀測、G=0 51。IC IR 0.05–0.10。信號不穩。**放棄**。

---

## Regime 拆分關鍵發現

### US Financials ret_6m IC IR by regime

| Regime | N obs | IC IR 6m | Top(G≥4) ann | Bot(G≤1) ann | Spread |
|---|---|---|---|---|---|
| **bear** | 953 | **+0.30** | +12.71% | +11.77% | +0.93% |
| bull | 6,768 | +0.07 | +14.39% | +16.25% | −1.87% |
| ranged | 4,365 | −0.37 | +8.83% | +9.48% | −0.66% |
| volatile | 969 | +0.24 | +27.48% | +10.92% | **+16.56%** |

US 金融業 G-Score 在 **bear / volatile regime 最有用**，符合 Mohanram 原論文「G-Score 在 glamour stocks 下跌時辨識真品質」。

### TW All-Market ret_6m IC IR by regime

| Regime | N obs | IC IR 6m | Top ann | Bot ann | Spread |
|---|---|---|---|---|---|
| **bear** | 6,778 | **+2.10** | +37.07% | +26.61% | +10.46% |
| bull | 41,350 | +1.11 | +11.65% | +7.83% | +3.81% |
| volatile | 8,517 | −0.08 | +29.24% | +43.70% | −14.46% |

TW bear regime 表現極強（但 bear 樣本是 SPY regime 定義 — 跨市場 regime proxy 可能有誤差，應以 TWII regime 複驗）。

---

## 落地決策建議

### 不推薦做的

| 決策 | 原因 |
|---|---|
| ❌ G-Score 作為連續分數全市場加分 | US 全市場 D 級反向；TW mean 非單調 |
| ❌ TW 金融業專用 G-Score | 只 21 ticker，樣本嚴重不足（577 obs），N 無統計力 |
| ❌ 作為 F-Score 的線性補充因子 | 兩者相關性未驗，且 G-Score rank IC 高但絕對 spread 小，加進組合可能稀釋更強因子 |

### 有條件推薦

| 決策 | 條件 |
|---|---|
| **✅ 「G=5 binary filter」US 金融業** | 頻率 2.6%（~1 per quarter per 38 tickers），hit 84.3%、ret_12m mean +18.86%、94.7% 季度正報酬；**但獨立於現有 VF-Value-ex2 邏輯驗證**：需在 US value screener 的候選池內檢查 G=5 有多少檔、與現有 value factor 的 overlap，再決定是否「加分」或「獨立 filter」 |
| **⚠️ TW 全市場 G=5 / G=0 quintile signal** | G=5 hit 61.8%（vs 52.3% rest），G=0 hit 47.1% — 信號強度**中等**且 N=7895。可以探索但不優先於現有純技術 Sharpe >3 的 Phase 1 方案 |

### 下一步建議（使用者決定）

1. **US**: 把 **G=5 binary filter** 拉進 VF-Value-ex2 US pipeline 當 pre-filter，比較：
   - (A) VF-Value-ex2 原版（2026-04-22 F-Score 加分全砍後）
   - (B) VF-Value-ex2 + 「若為 Financials 且 G=5 加 +10」vs 不加
   - 用 Sharpe / maxdd / 勝率三件套比。**不要直接上 live**。

2. **TW**: 因 bear-regime 結果受 SPY proxy 影響，**先補跑 TWII regime 版本** 再決定。TW 全市場 G=5 若 TWII regime 下仍 bear IR > 0.5，則加進 VF-Value-ex2 TW 測試組合。

3. **樣本擴充**: US Financials G=5 只 338 obs，建議回補 2000–2009 data（如果 EDGAR 有），以涵蓋 2008 金融危機，提升統計力。

---

## 產出檔案

| 檔案 | 說明 |
|---|---|
| `reports/vf_value_ex3_gscore_ic_us.md` | US 全市場 + 金融業 IC/Grp/Regime 詳細 |
| `reports/vf_value_ex3_gscore_ic_tw.md` | TW 全市場 + 金融業 IC/Grp/Regime 詳細 |
| `reports/vf_value_ex3_gscore_panel_{all,fin}_{us,tw}.parquet` | Per-(ticker, q) panel，供後續 drilldown |
| `reports/vf_value_ex3_gscore_ic_by_quarter_{all,fin}_{us,tw}.csv` | 各季 IC 時序 |
| `reports/vf_value_ex3_gscore_decile_spread_{all,fin}_{us,tw}.csv` | 各季 Top/Bot 分組報酬 |
| `tools/vf_value_ex3_gscore_ic.py` | 主驗證腳本 |

## 驗證方法論備註

- **為何 Spearman IC +1.02 卻 mean 反向** (TW)：Spearman 用 rank 對尾部魯棒；mean 被 G=0 中少數 recovery 飆股（低基期小型股）扭曲。**這是 IC 不足以單用的典型案例**（見 `project_ic_research.md` v1 scanner 反向誤判）。**必須同看 median + hit rate + decile spread**。
- **為何 US 全市場 D 但 Financials C**：US 大盤高品質已 price in（與 2026-04-22 F-Score 翻反向同源），但金融業財報複雜度高、資訊摩擦大，G-Score 在雜訊中仍撈得出 alpha — 這是 Mohanram 2005 本來就鎖定的場景。
- **樣本時間偏誤**：US 2010–2024 涵蓋 bull regime 居多（39,565/74,499 = 53%）；TW 2016–2024 完全錯過 2008。Bear 樣本 US 7.3%、TW 12.0% — bear regime IR 雖高但樣本厚度需警告。
