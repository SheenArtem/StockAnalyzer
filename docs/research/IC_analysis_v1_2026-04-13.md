# 技術指標 IC 驗證報告 v1

| 欄位 | 值 |
|------|----|
| **版本** | v1 |
| **建立日期** | 2026-04-13 |
| **資料範圍** | 2011-04-14 ~ 2026-04-13（15 年） |
| **樣本** | 台股 1,951 檔（FinMind 現役清單，6.1M rows OHLCV） |
| **狀態** | 🔴 Superseded by v2 (2026-04-13) |
| **取代前版** | 無（首版） |
| **被取代於** | 2026-04-13（同日 Phase 2c Quantile 驗證推翻核心結論 ① ②） |
| **下次檢視** | 見 `IC_analysis_v2_2026-04-13.md` |

---

## ⛔ **本報告已被 v2 取代**

**2026-04-13 同日**的 Phase 2c Quantile 報酬驗證推翻了本報告的兩個核心結論：

1. **結論 ① (scanner 符號反向)** 是**錯誤建議**。實測 scanner 現行 Top-20 持 20 日報酬 +2.48%，D10 > D9 > ... > D1 單調遞增，absolute 報酬贏過所有「翻符號」組合。
2. **結論 ② (rvol_lowatr 是最強 alpha)** 部分誤導。它 IC 確實最高、Sharpe 最好 (6.07)，但絕對 Top-20 報酬 (+1.14%) 只有 scanner (+2.48%) 的一半。

**根本原因**：IC 測全排名線性相關，scanner 實際取 Top N。IC 低可能是中段非單調造成，不代表 Top bucket 不賺錢。v1 過度信任 IC，未做 quantile 驗證就下結論。

**建議閱讀順序**：
- 先看 `IC_analysis_v2_2026-04-13.md`（修正版）
- 本檔保留作為 IC 驗證方法論範例 + 失誤教訓

仍然有效的結論（v1 章節 ③-⑤）：指標冗餘、短期均值回歸、RVOL 是唯一正 IC 個別指標 — 這些在 v2 保留。

---

## ⚠️ 使用守則（原）

- 這份報告是 **in-sample 分析**，未做 walk-forward 驗證，實戰績效可能打折。
- 只用純技術面（OHLCV），**不含籌碼、基本面、消息面**。
- 未納入交易成本（台股約 0.5% 手續費+稅+滑價）。
- IC 測的是**線性截面相關**，與 scanner 取 Top N（右尾 bucket 報酬）不完全等價。
- **任何後續實證結果推翻本報告結論**，請建立 v2 並把本檔 `狀態` 改為 `🔴 Superseded by v2`。

---

## 1. 核心結論（Executive Summary）

### ① 現行 scanner 的觸發分數方向**符號相反**

| Combo | Horizon | IC | 勝率 | 解讀 |
|-------|---------|-----|------|------|
| 現行 3-group median (未反向) | 1d | **-0.037** | 34% | 買在訊號 → 賠錢 |
| 現行 3-group median (未反向) | 20d | -0.012 | 47% | 長期仍負 |
| 翻符號版 | 1d | +0.040 | 67% | 顯著獲利 |
| 翻符號版 | 20d | +0.027 | 60% | 顯著獲利 |

**推論**：台股過去 15 年呈現**強烈短期均值回歸**（漲多必跌），右側動能邏輯整體輸給逆向思維。

### ② 最強 Alpha 來源：`combo_rvol_lowatr` (RVOL - ATR%)

| Horizon | Universe | mean_IC | IR | 勝率 |
|---------|----------|---------|-----|------|
| 20d | all | **+0.066** | +0.52 | 71% |
| 10d | all | +0.059 | +0.46 | 69% |
| 20d | momentum (流動性過濾) | +0.049 | +0.31 | 63% |
| 10d | momentum | +0.043 | +0.26 | 60% |

**意義**：相對低波動的股票 + 今日放量 → 持 10-20 天有正超額報酬。學術上稱「低波動溢酬 × 爆量突破」因子。

### ③ 短期王者：`combo_meanrev_pure` (純反向均值回歸)

| Horizon | IC | 勝率 |
|---------|-----|------|
| 1d | **+0.060** | 75.5% |
| 5d | +0.055 | 73.3% |
| 10d | +0.037 | 67% |
| 20d | +0.033 | 64% |

**定義**：6 個 flipped 指標平均 = -(MA20偏離 + RSI偏離 + KD差 + EFI + BB%B + ATR%)
**侷限**：10 天後衰退，適合短線當沖、不適合 scanner 持倉型策略。

### ④ 唯一正 IC 的個別指標：`log(RVOL)`

| Regime | Horizon | IC |
|--------|---------|-----|
| volatile | 20d | +0.026 |
| trending | 10d | +0.020 |
| all | 20d | +0.018 |

**14 個個別指標中只有這一個穩定正 IC**，其他全是負 IC（均值回歸訊號）。

### ⑤ 指標高度冗餘

兩兩相關矩陣揭示以下 5 個指標是同一個訊號的變體（corr 0.78-0.93）：

- MA20 偏離 ↔ VWAP 偏離 (corr +0.93)
- MA20 偏離 ↔ BB %B (+0.89)
- RSI 偏離 ↔ BB %B (+0.88)
- MA20 偏離 ↔ RSI 偏離 (+0.88)
- MA20 偏離 ↔ EFI (+0.78)

**Scanner 目前等於 double-counting 同一訊號 4-5 次**，降低信噪比。

---

## 2. 建議改動（依優先序）

### 🟢 P1：trigger_score 符號翻轉（1 行改動）

```python
# analysis_engine.py，trigger_score 算完後加：
trigger_score *= -1
```

**預期效果**：
- Scanner 整體 IC 從 `+0.016`（原記錄）提升到 `+0.027`
- Top-N picks 勝率從 ~45% 提升到 57-60%

### 🟡 P2：新增 `rvol_lowatr` 作為主 factor

在 scanner 加入以下分數（rank-normalized）：

```python
rvol_rank = pct_rank(log(rvol_20d))       # 0-1
atr_rank = pct_rank(atr / close)          # 0-1
rvol_lowatr_score = (rvol_rank - atr_rank) * 10  # -10 ~ +10
# 加重權重，至少與 3-group median 相當
```

**預期效果**：IC 進一步提升到 `+0.04 ~ +0.05`。

### 🟡 P3：合併 5 個冗餘指標

將 MA20 偏離 / VWAP 偏離 / BB %B / RSI 偏離 / EFI 合併成單一 `mean_reversion_composite`，釋放權重空間。

### 🟡 P4：改為橫截面 rank normalize

目前 trigger_score 是絕對分數（如 +5.8 分）。改成「今日全市場 rank percentile」的相對分數，更符合 cross-sectional 邏輯，也與 IC 量測框架一致。

### 🔴 P5+（長期）：
- **Walk-forward OLS**（避免 in-sample overfit）
- **Regime-dependent combo**（不同 regime 換不同組合）
- **加籌碼面 factor**（Phase 2 只用純技術）
- **Quantile 報酬驗證**（確認頂部 decile 是否真賺錢）

---

## 3. 方法論

### 3.1 資料

- **來源**：yfinance（台股 .TW / .TWO suffix），curl_cffi Chrome 模擬繞 Cloudflare
- **欄位**：Open / High / Low / Close / AdjClose / Volume / Dividends / Splits
- **期間**：2011-04-14 ~ 2026-04-13（15 年）
- **宇宙**：FinMind taiwan_stock_info 現役清單 → 2,121 檔 → yfinance 成功抓到 1,972 檔 → IC 分析（需 ≥200 日歷史）留 1,951 檔
- **儲存**：`data_cache/backtest/ohlcv_tw.parquet`（128.7 MB）
- **已知限制**：倖存者偏誤（未回補下市股）、分割調整 2 筆手動修復

### 3.2 指標（14 個 → 15 個訊號）

| 類別 | 指標 | 訊號定義 |
|------|------|---------|
| 趨勢 | MA | `Close / MA20 - 1` |
| 趨勢 | MA 排列 | `MA20 / MA60 - 1` |
| 趨勢 | Supertrend | ATR-based 簡化版，±1 方向 |
| 趨勢 | ADX + DMI | `(DI+ - DI-) × (ADX/50)` |
| 趨勢 | VWAP | `Close / VWAP20 - 1` |
| 動能 | MACD | `macd_hist / Close` |
| 動能 | RSI | `RSI(14) - 50` |
| 動能 | KD | `K - D` |
| 動能 | EFI | `Force_Index / rolling_avg(CxV)` |
| 動能 | TD Setup | `sell_count - buy_count` |
| 量能 | RVOL | `log(volume / MA20(volume))` |
| 量能 | OBV 動能 | `OBV.pct_change(20)` |
| 波動 | BB %B | `(Close - BB_mid) / BB_width` |
| 波動 | ATR% | `ATR(14) / Close` |
| 波動 | Squeeze | BB inside KC = 1 else 0 |

### 3.3 IC 計算

**截面 Spearman IC**：
```
for each date t:
    x = signal values across all stocks on day t
    y = forward h-day return across same stocks
    IC_t = spearman_rank_corr(x, y)
time-series: {IC_t for t in all trading days}
```

**彙總統計**：
- `mean_IC` = 時序平均
- `IR` = mean_IC / std_IC（訊號穩定性）
- `win_rate` = (IC > 0).mean()

### 3.4 顯著性檢定

- **t-stat p-value**: `t = mean_IC × sqrt(n) / std_IC`，查 t-dist
- **Block bootstrap 95% CI**: 20 日 block × 1000 次抽樣，保留自相關
- **判定為顯著**: `p < 0.05 AND CI 不跨 0`

### 3.5 Regime 分類

用全市場日均報酬 proxy TAIEX：
- `volatile`: 20 日波動率 > 75 percentile
- `trending`: |20日趨勢| > mean + 0.5 std
- `ranging`: 其他
- `unknown`: 前 20 日資料不足

### 3.6 Universe 分層

- `all`: 全 1,951 檔
- `momentum`: 流動性過濾 (20 日均成交額 ≥ 3,000 萬 + 上市 ≥ 750 日)

---

## 4. 數字細節

### 4.1 Phase 2a 單指標 IC（480 組，296 顯著）

完整數據：`reports/indicator_ic_matrix.csv`
日 IC 時序：`reports/indicator_ic_daily.parquet`

頂部 10 筆（universe=all, regime=all）：

| 指標 | Horizon | mean_IC | IR | 95% CI | p |
|------|---------|---------|-----|--------|---|
| ATR% | 20d | -0.0789 | -0.42 | [-0.099, -0.058] | <0.0001 |
| ATR% | 10d | -0.0693 | -0.37 | [-0.085, -0.053] | <0.0001 |
| ATR% | 5d | -0.0605 | -0.32 | [-0.072, -0.049] | <0.0001 |
| KD 差 | 1d | -0.0506 | -0.68 | [-0.054, -0.047] | <0.0001 |
| EFI | 5d | -0.0480 | -0.55 | [-0.054, -0.043] | <0.0001 |
| ATR% | 1d | -0.0456 | -0.24 | [-0.050, -0.040] | <0.0001 |
| EFI | 1d | -0.0424 | -0.46 | [-0.045, -0.039] | <0.0001 |
| BB %B | 1d | -0.0410 | -0.45 | [-0.044, -0.037] | <0.0001 |
| EFI | 20d | -0.0392 | -0.46 | [-0.048, -0.031] | <0.0001 |
| MA20 偏離 | 1d | -0.0378 | -0.30 | [-0.042, -0.034] | <0.0001 |

### 4.2 Phase 2b 組合 IC（80 組）

完整數據：`reports/indicator_combo_ic.csv`

**Universe = all**（所有 horizon）：

| Combo | h=1d | h=5d | h=10d | h=20d |
|-------|------|------|-------|-------|
| **combo_rvol_lowatr** | +0.032 | +0.046 | **+0.059** | **+0.066** |
| combo_meanrev_pure | **+0.060** | +0.055 | +0.037 | +0.033 |
| combo_all_equal | +0.048 | +0.046 | +0.034 | +0.033 |
| combo_3group_median_flip | +0.040 | +0.038 | +0.028 | +0.027 |
| combo_rvol_meanrev | +0.035 | +0.033 | +0.029 | +0.026 |
| combo_counter_trend_rvol | +0.031 | +0.030 | +0.024 | +0.024 |
| combo_rvol_only | +0.001 | +0.006 | +0.018 | +0.018 |
| combo_squeeze_rvol | +0.003 | +0.009 | +0.018 | +0.018 |
| combo_momflip_volpos | +0.024 | +0.022 | +0.021 | +0.019 |
| **combo_3group_median_raw** (現行) | **-0.037** | **-0.032** | **-0.015** | **-0.012** |

### 4.3 Pairwise Correlation

完整矩陣：`reports/indicator_pairwise_corr.csv`

|corr| > 0.7 的配對（代表冗餘）：

| A | B | corr |
|---|---|------|
| MA20 偏離 | VWAP 偏離 | +0.93 |
| MA20 偏離 | BB %B | +0.89 |
| RSI 偏離 | BB %B | +0.88 |
| MA20 偏離 | RSI 偏離 | +0.88 |
| VWAP 偏離 | BB %B | +0.84 |
| ADX×方向 | RSI 偏離 | +0.83 |
| VWAP 偏離 | RSI 偏離 | +0.80 |
| MA20 偏離 | EFI | +0.78 |
| RSI 偏離 | EFI | +0.78 |
| MA20 偏離 | MACD Hist | +0.77 |
| VWAP 偏離 | MACD Hist | +0.75 |
| EFI | BB %B | +0.74 |
| MA20 偏離 | ADX×方向 | +0.74 |
| ADX×方向 | EFI | +0.71 |

### 4.4 OLS 最佳權重（僅供參考，有 in-sample overfit 風險）

完整數據：`reports/indicator_ols_weights.csv`

`h=20d, universe=all, in-sample IC=+0.023, n=910,686` 前 8 名權重：

| 指標 | OLS 權重 | 方向 |
|------|----------|------|
| MA20 偏離 | -0.0134 | 看空（符合 IC） |
| RSI 偏離 | +0.0077 | 看多（矛盾，受 multicolinearity 影響） |
| EFI | -0.0063 | 看空 |
| BB %B | +0.0056 | 看多（同 RSI） |
| VWAP 偏離 | +0.0047 | 看多 |
| MACD Hist | +0.0034 | 看多 |
| log(RVOL) | +0.0029 | 看多 ✓ |
| MA 排列 | +0.0028 | 看多 |

**警告**：上述高度相關指標的 OLS 係數方向會互相抵消、失真。決策依據應以 Phase 2a 的單指標 IC 為準。

---

## 5. 再現性（Reproducibility）

### 重跑資料下載
```bash
python tools/backtest_dl_ohlcv.py          # 全量 15y
python tools/backtest_dl_ohlcv.py --test   # 10 檔測試
python tools/backtest_dl_ohlcv.py --resume # 斷點續抓
python tools/backtest_dl_ohlcv.py --validate  # 連續性驗證
```

### 重跑 Phase 2a
```bash
python tools/indicator_ic_analysis.py              # 全量 13 分鐘
python tools/indicator_ic_analysis.py --sample 200 # 測試
```

### 重跑 Phase 2b
```bash
python tools/indicator_combo_analysis.py           # 全量 7 分鐘
python tools/indicator_combo_analysis.py --sample 300 --horizon 20
```

### 相依性
- Python 3.14
- pandas / numpy / scipy
- ta==0.5.25（old API `n=` 非 `window=`）
- pyarrow / snappy
- curl_cffi（yfinance Cloudflare bypass）

---

## 6. 變更紀錄

| 版本 | 日期 | 變更 |
|------|------|------|
| v1 | 2026-04-13 | 首版。Phase 2a + 2b 完成。樣本 1,951 檔 × 15 年。 |

---

## 7. 如何創建 v2

若未來實證結果推翻本報告結論（e.g. quantile 報酬驗證顯示 Top decile 是負的），流程：

1. 複製本檔為 `IC_analysis_v2_YYYY-MM-DD.md`
2. 把本檔（v1）frontmatter `狀態` 改為 `🔴 Superseded by v2`，`被取代於` 填新日期
3. v2 裡 `取代前版` 填 `v1 (2026-04-13)`
4. 在 v2 的「變更紀錄」章節詳述**為什麼推翻 v1**
