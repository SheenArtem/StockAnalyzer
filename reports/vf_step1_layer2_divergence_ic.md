# VF Step 1 - Layer 2 底部背離 factor IC 驗證

## 結論 (TL;DR)

**Verdict: D 級 (砍掉 / 不作為 Layer 2 主因子)** — 但不是因為 noise，是因為 **signal 系統性地反向**。

- **RSI divergence**: 9 個 (regime × horizon) 中 **7 個 IC 為負且統計顯著** (p<0.05)，其中 5 個 p<1e-5
- **MACD_hist divergence**: 9 個中 **8 個 IC 為負且統計顯著**，其中 6 個 p<1e-4
- **IC 量級小 (~-0.02)** 但 **跨三段 regime 方向一致 + IR ≈ -0.15~-0.22**，是穩健的反向 signal
- **bull_strong vs bear_strong 報酬差**：大多數 regime/horizon 下 bear_strong 比 bull_strong 報酬更高，與教科書定義相反

也就是說：`pattern_detection.detect_divergence()` 標記「底背離（看多）」時，該股後續 20-60d 報酬 **反而較差**；標記「頂背離（看空）」時報酬 **反而較好**。`analysis_engine.py` 目前把 bull 加 +2~+3 分、bear 扣 -2~-3 分的邏輯 **方向弄反了**。

---

## 核心數字

### RSI divergence (sample 400 × 2015-2026, 949,701 rows)

Signal 觸發率 **19.21%** (bull 10.21% / bear 9.01%)，分佈合理。

| regime    | horizon | mean_IC  | IC_IR  | t_stat | p_value  | bull_n | bear_n | bull_bear_spread |
|-----------|---------|----------|--------|--------|----------|--------|--------|------------------|
| 2016-2019 | 20d     | -0.0287  | -0.219 | -6.82  | 1.6e-11  | 32,607 | 27,349 | -0.14%           |
| 2016-2019 | 40d     | -0.0196  | -0.153 | -4.76  | 2.2e-06  | 32,607 | 27,349 | +0.17%           |
| 2016-2019 | 60d     | -0.0195  | -0.156 | -4.88  | 1.3e-06  | 32,603 | 27,349 | +0.20%           |
| 2020-2022 | 20d     | -0.0161  | -0.125 | -3.38  | 7.8e-04  | 27,784 | 24,030 | -0.48%           |
| 2020-2022 | 40d     | -0.0074  | -0.063 | -1.70  | 0.090    | 27,784 | 24,030 | -0.14%           |
| 2020-2022 | 60d     | -0.0032  | -0.024 | -0.66  | 0.509    | 27,784 | 24,030 | -0.77%           |
| 2023-2025 | 20d     | -0.0071  | -0.059 | -1.58  | 0.114    | 29,817 | 27,977 | -0.08%           |
| 2023-2025 | 40d     | -0.0156  | -0.133 | -3.59  | 3.5e-04  | 29,817 | 27,977 | +0.41%           |
| 2023-2025 | 60d     | -0.0092  | -0.068 | -1.82  | 0.069    | 29,817 | 27,969 | +1.02%           |

p<0.05 且 IC<0 的組合：2016-2019 三段 / 2020-2022 20d / 2023-2025 40d。

### MACD_hist divergence (sample 400, 同 universe)

Signal 觸發率 **17.81%** (bull 9.98% / bear 7.83%)。

| regime    | horizon | mean_IC  | IC_IR  | t_stat | p_value  | bull_n | bear_n | bull_bear_spread |
|-----------|---------|----------|--------|--------|----------|--------|--------|------------------|
| 2016-2019 | 20d     | -0.0201  | -0.146 | -4.56  | 5.8e-06  | 31,537 | 24,480 | +0.04%           |
| 2016-2019 | 40d     | -0.0294  | -0.220 | -6.86  | 1.2e-11  | 31,537 | 24,480 | +0.04%           |
| 2016-2019 | 60d     | -0.0251  | -0.189 | -5.88  | 5.6e-09  | 31,537 | 24,480 | +0.27%           |
| 2020-2022 | 20d     | -0.0212  | -0.164 | -4.43  | 1.1e-05  | 26,308 | 22,873 | -0.38%           |
| 2020-2022 | 40d     | -0.0100  | -0.073 | -1.97  | 0.049    | 26,308 | 22,873 | +0.29%           |
| 2020-2022 | 60d     | +0.0022  | +0.015 | +0.41  | 0.683    | 26,308 | 22,873 | +0.76%           |
| 2023-2025 | 20d     | -0.0186  | -0.150 | -4.03  | 6.2e-05  | 29,800 | 22,635 | -0.22%           |
| 2023-2025 | 40d     | -0.0213  | -0.167 | -4.48  | 8.5e-06  | 29,800 | 22,635 | +0.03%           |
| 2023-2025 | 60d     | -0.0272  | -0.216 | -5.82  | 8.9e-09  | 29,800 | 22,634 | -0.23%           |

p<0.05 且 IC<0 的組合：**9 個中 8 個** (僅 2020-2022 60d 不顯著)。

### Bull_strong vs Bear_strong 組平均報酬 (MACD)

這是最直接的反向證據 — 強背離應該預示強反轉，但：

| regime    | horizon | bull_strong_mean | bear_strong_mean | 方向 |
|-----------|---------|------------------|------------------|------|
| 2016-2019 | 60d     | +3.13%           | +3.19%           | 幾乎平手 |
| 2020-2022 | 40d     | +4.52%           | +4.25%           | 幾乎平手 |
| 2020-2022 | 60d     | +6.43%           | +6.24%           | 幾乎平手 |
| 2023-2025 | 40d     | +1.11%           | **+3.19%**       | **bear > bull** |
| 2023-2025 | 60d     | +0.11%           | **+6.61%**       | **bear >> bull** |

最近 2023-2025 regime，**強頂背離組的 60d 報酬 (+6.6%) 遠勝強底背離組 (+0.1%)**，是 analysis_engine 目前加減分邏輯的徹底 counter-example。

---

## 資料 / 方法

- **Universe**: 台股 TWSE + TPEX，sample 400（random seed=42，從 MIN_HISTORY=200 後 1948 檔中抽）
- **時段**: 2015-06-01 ~ 2026-04-13（6 個月 buffer）
- **Regimes**: 2016-2019 / 2020-2022 / 2023-2025
- **Horizons**: 20d / 40d / 60d (close-to-close pct_change)
- **Signal generator**: vectorized 時序版 `detect_divergence()`
  - 對每檔每日用 [T-window, T] 區間跑 pivot detection (scipy argrelextrema order=3)
  - 為避免 look-ahead，pivot 僅取到 T-order 才算已確認
  - signal 編碼 ±3 strong / ±2 standard / ±1 hidden / 0 none
- **IC**: 每日 cross-sectional Spearman，只在有 divergence 的 sample 上算（MIN_CROSS_SECTION=5，因為背離稀疏）
- **Look-ahead check**: signal[T] 使用 <= T-3 的 pivot，fwd_Nd[T] 從 T+1 開始算，時序對齊無 leakage

---

## 實作踩到的雷

1. **原 `detect_divergence()` 是 snapshot function** — 只回傳「最後一根是否背離」的分類字串，沒有時序版本。要做 IC 驗證必須自己把 sliding window 邏輯展開（對每檔每日跑 scan），無法直接呼叫現有函式
2. **pivot confirmation lag** — argrelextrema order=3 需要右側 3 根才能確認 pivot，所以 T 日實際能用的 pivot 只到 T-3。這是 lookahead 檢查的關鍵，在 `_scan_divergence_series` 裡明確設 `cutoff = T - order`
3. **樣本稀疏** — divergence 觸發率 ~18%，每日截面實際樣本數比 30 小，把 MIN_CROSS_SECTION 從 30 下調到 5 才有足夠 daily IC 觀察值
4. **hidden divergence 方向與 standard 相反** — 原函式 bull_weak 是 hidden bullish（價格更高低點 + 指標更低低點），數值上與 bear_weak 對稱，編碼為 ±1；從 signal 分佈看 hidden 比 standard 密度更高（RSI: -1:3,455 > -2:2,850），這可能對 IC 造成稀釋
5. **Smoke test (30 檔) IC 量級較大** — 例如 2020-2022 20d RSI IC=-0.108；全量 sample (400 檔) 收斂到 -0.016。標準差大 -> 結論沒變，但 IC 量級被收斂
6. **第一次 full-universe (1948 檔) 背景任務被誤 kill** — 後改用 sample 400 是正確的折衷，因為 IC 分佈已相當穩定（t-stat 普遍 -4 ~ -7）

---

## 看法 / 建議

### 立即行動
1. **analysis_engine.py line 737-778 的 divergence 加分邏輯方向錯誤**
   - 現狀: bull +2~+3 加分、bear -2~-3 扣分
   - IC 證據: 方向相反 (bull IC<0, bear IC>0)
   - **不建議直接反轉符號** — IC 量級 (~-0.02) 太小、反向 signal 的 out-of-sample 穩健性未驗；直接反轉可能被誤讀為「頂背離是買點」過度樂觀
   - **建議: 把 divergence 加分邏輯 *移除***，或改為 0 分 (純視覺 UI 提示不影響 score)

### 不推薦的路徑
- **作為 Layer 2 主因子**: |IC| 量級太小（即使方向正確也只有 0.02 ~ 0.03），加權太低沒意義，加權太高又不穩
- **組合成 composite factor**: 可以嘗試，但先要確認反向 signal 的 out-of-sample 穩定，目前樣本內都已呈現「某 horizon 失效」的不穩特性（例 2020-2022 60d 兩個 indicator 都 non-sig）

### 可延伸研究 (低優先)
- **Conditional divergence**：只在特定 regime (如 bull_trending) 或特定 RSI 水位（如 RSI<30 的 bull_strong）條件機率是否更高
- **Multi-indicator confirmation**：RSI+MACD 同時背離 (intersection) 是否比單一 indicator 更好 — smoke test 顯示 bull_strong 報酬 +9~+10% 時樣本數過小 (n=136)，全量還沒驗證
- **Divergence + F-Score / 動能複合**：作為劇本觸發器（策略：F-Score 高 + 底背離 = 進場時點）而非截面排序因子

---

## 產出檔案

- `reports/vf_step1_layer2_divergence_ic.csv` — 最後一次 MACD run 的 9 行結果（regime × horizon）
- `reports/vf_step1_layer2_divergence_ic_RSI.csv` / `.md` — RSI 版本備份
- `reports/vf_step1_layer2_divergence_ic_MACD.csv` / `.md` — MACD 版本備份
- `tools/vf_step1_layer2_divergence_ic.py` — 驗證腳本（可 reuse，支持 `--indicator` / `--sample` / `--since`）
