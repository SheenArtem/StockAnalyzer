# V12-pair: 台股同業 Pair Divergence leading signal IC 驗證

**日期**：2026-04-24
**腳本**：`tools/v12_pair_divergence_ic.py`
**原始 CSV**：`reports/v12_pair_divergence_ic.csv` (per pair × signal × horizon × segment 明細)
**聚合 CSV**：`reports/v12_pair_divergence_ic_agg.csv`
**Div rate 對比**：`reports/v12_pair_div_rate_by_segment.csv`

---

## 結論先寫

**全部 4 個 signal 在 pair-weighted aggregate 皆為 C 級（hit rate 跟 baseline 差不多）。** 無法用任何單一 leading signal 可靠預測 Pair Divergence regime。

使用者提出的 hypothesis「AI era 的 Pair Divergence 發生率更高」**被推翻**：AI era (2023-2025) 同業 pair 的 60d divergence 實際發生率 47.3%，Pre-AI (2021-2022) 47.9%，差異 -0.7% 無意義。**12 對 pair 中 AI era div rate 上升的只有 6 對（剛好一半），其中 2382/3231 廣達緯創 AI era div 較 pre +9.5pp，但 6515/6223 穎崴旺矽反向 -39.2pp，沒有系統性偏多。**

**最可用 signal 是 S1 券資比差**，但 uplift 也僅 +4.8pp（60d, AI era），邊際夠但不到可以當主要決策依據的程度；可當 **warning flag** 而非 **predictor**。

**對「同業輪動建議」feature 的 impact 判讀**：
- 不能做「看到 B 落後就建議買 B 補漲」— baseline 就有 47% 機率是 divergence 繼續擴大
- 建議改成「提示 pair 狀態 + 列出兩檔 signal 值，使用者自行判斷」而非主動推薦 buy
- 若要保留主動推薦，**必須** pair = convergence regime 才推，且必須有外部 anchor（題材 catalyst / 整體 momentum 轉換）加持
- 單看 chip signal 無法區分 regime → feature 應降級成「警告」或「提示」

---

## 數字細節

### 1. Signal-level verdict（pair-weighted, pooled across 12 pairs）

| Signal | Horizon | Pre-AI hit_div (B 弱→ 實際 div) | AI era hit_div | Pre-AI baseline | AI era baseline | Verdict |
|---|---|---|---|---|---|---|
| S1 券資比差 | 20d | 0.508 | 0.408 | 0.417 | 0.398 | **C** |
| S1 券資比差 | 60d | 0.582 | 0.521 | 0.476 | 0.473 | **C** |
| S2 借券餘額差 (z-score) | 20d | 0.430 | 0.407 | 0.412 | 0.397 | **C** |
| S2 借券餘額差 (z-score) | 60d | 0.510 | 0.499 | 0.473 | 0.472 | **C** |
| S3 RS gap 5d slope | 20d | 0.384 | 0.399 | 0.412 | 0.397 | **C** |
| S3 RS gap 5d slope | 60d | 0.468 | 0.473 | 0.473 | 0.473 | **C** |
| S4 外資 10d 差 | 20d | 0.395 | 0.405 | 0.411 | 0.397 | **C** |
| S4 外資 10d 差 | 60d | 0.474 | 0.464 | 0.473 | 0.473 | **C** |

（verdict A: 兩段 hit>60% / B: 一段>60% / C: ~baseline / D: <25% 明顯反向）

### 2. Uplift over baseline（signal 真正的 edge）

| Signal | Horizon | Pre-AI uplift | AI era uplift | 結論 |
|---|---|---|---|---|
| **S1 券資比差** | 20d | **+9.4%** | +1.0% | Pre-AI 有效但 AI era 失效 |
| **S1 券資比差** | 60d | **+10.8%** | **+4.8%** | 兩段都正，最可用 |
| S2 借券餘額差 | 20d | +1.8% | +1.0% | 無 edge |
| S2 借券餘額差 | 60d | +4.0% | +2.7% | 邊際 |
| S3 RS gap slope | 20d | -2.8% | +0.2% | 無 edge |
| S3 RS gap slope | 60d | -0.5% | 0.0% | 完全無 edge |
| S4 外資 10d 差 | 20d | -1.5% | +0.8% | 無 edge |
| S4 外資 10d 差 | 60d | +0.1% | -0.9% | 完全無 edge |

**解讀**：S1 券資比差在 Pre-AI +10.8% 看起來強，但 AI era 減半到 +4.8%。其他 3 signal 幾乎純噪音。

### 3. 個別 pair 可能藏 alpha（AI era 60d uplift > 5pp）

| Pair | Signal | hit_div | baseline | uplift |
|---|---|---|---|---|
| 3037 欣興 vs 8046 南電 | S2 借券差 | 0.771 | 0.614 | **+15.7%** |
| 3711 日月光 vs 2449 京元電 | S1 券資比差 | 0.472 | 0.306 | **+16.6%** |
| 2383 台光電 vs 6274 台燿 | S1 券資比差 | 0.593 | 0.488 | +10.5% |
| 2368 金像電 vs 3044 健鼎 | S1 券資比差 | 0.739 | 0.638 | +10.1% |
| 2382 廣達 vs 3231 緯創 | S4 外資差 | 0.400 | 0.317 | +8.3% |
| 6223 旺矽 vs 6510 中華精測 | S1 券資比差 | 0.713 | 0.637 | +7.6% |
| 3037 欣興 vs 3189 景碩 | S2 借券差 | 0.658 | 0.591 | +6.7% |
| 3037 欣興 vs 8046 南電 | S1 券資比差 | 0.635 | 0.615 | +2.0% (列此對比) |

**12 pair × 4 signal × 2 horizon = 96 cell，只有 10 cell uplift > 5%**，約 10%。且多數集中在 baseline 已經 60%+ 的 pair（B 長期弱的結構性 divergence，如 3037 欣興 vs 南電/景碩 / 6223 vs 6510 / 2368 vs 3044）。**Signal 只是追認既有結構弱勢，不是 leading。**

### 4. AI era 是否真的 divergence 更多？— 否定

每 pair 60d divergence 發生率比較（AI era 2023-2025 vs Pre-AI 2021-2022）：

| Pair | Pre-AI div rate | AI era div rate | delta |
|---|---|---|---|
| 6515 穎崴 vs 6223 旺矽 | 0.694 | 0.302 | **-39.2%** |
| 2383 台光電 vs 6274 台燿 | 0.655 | 0.488 | -16.7% |
| 3711 日月光 vs 2449 京元電 | 0.343 | 0.306 | -3.7% |
| 3008 大立光 vs 3406 玉晶光 | 0.423 | 0.388 | -3.5% |
| 3443 創意 vs 3661 世芯 | 0.428 | 0.388 | -4.0% |
| 3017 奇鋐 vs 3324 雙鴻 | 0.612 | 0.578 | -3.5% |
| 2368 金像電 vs 3044 健鼎 | 0.627 | 0.639 | +1.2% |
| 3037 欣興 vs 8046 南電 | 0.556 | 0.615 | +6.0% |
| 3037 欣興 vs 3189 景碩 | 0.435 | 0.593 | **+15.8%** |
| 6223 旺矽 vs 6510 中華精測 | 0.514 | 0.637 | +12.3% |
| 6488 環球晶 vs 5483 中美晶 | 0.242 | 0.423 | **+18.1%** |
| 2382 廣達 vs 3231 緯創 | 0.222 | 0.317 | +9.5% |

**Mean delta = -0.7%**，正 delta 6/12 pair。使用者提的廣達/緯創確實 AI era divergence +9.5%，但整體 pattern 不成立。

---

## 方法論備註

- **Look-ahead check**：signal 全用 T 日收盤後可得資料（法人買賣超 T+0 晚間更新、margin/sbl T+0 盤後），fwd return 從 T+h 算（h=20 或 60 交易日）
- **Regime 定義**：B fwd_ret - A fwd_ret > +3% convergence / < -3% divergence / 其餘 neutral
- **Signal direction 對齊**：
  - S1 = A_short_ratio - B_short_ratio（< 0 → B 券資比高 → B 弱）
  - S2 = A_sbl_z60 - B_sbl_z60（< 0 → B 借券異常增多 → B 弱）
  - S3 = (A-B) 20d_ret 的 5 日斜率（> 0 → gap 擴大中 → B 相對弱）
  - S4 = A_foreign_10d - B_foreign_10d（> 0 → 外資流入 A 多於 B → B 弱）
- **Data coverage caveat**：margin.parquet 與 short_sale.parquet 從 2021-04-16 開始，所以 "Pre-AI" 實際窗口只到 2021-04-16 ~ 2022-12-31（1.7 年 ~ 423 交易日/pair）。使用者原要求 2016-2022 無法達成（資料前 5 年沒覆蓋）。
- **Pair 選擇**：12 pair 涵蓋 9 個 AI era 主流題材，全 pair_divergence_suitable=true，pair 兩檔皆在 tier1 或 tier1/tier2 典型組合
- **Signal 拆分**：每 segment 按 signal 值三分位（bottom 1/3 = B weak, top 1/3 = B strong, middle = neutral），因此 b_weak 樣本數平均每 pair ~200-500（有統計意義）

---

## 建議行動

### 直接砍的

1. **不要做「買落後補漲」主動推薦**：baseline 47% 機率繼續弱，勝算低於擲骰子
2. S3 RS gap slope / S4 外資 10d 差：完全無 signal edge，不要拿來判斷 pair regime

### 可以保留但降級的

3. **同業輪動 feature 改為「pair 狀態顯示」**：
   - 顯示 pair 最近 20d/60d 兩檔的相對強弱（RS gap、券資比、外資差）
   - 標示 baseline divergence rate（告訴使用者「同業這種 pair 歷史上 47% 的時間是 gap 擴大而非收斂」）
   - **不給 buy signal**，讓使用者自行綜合判斷
4. **S1 券資比差** 當 warning flag：
   - 兩檔券資比差距拉大（B >> A）時，標示「B 有空方放空累積，需留意 late mover trap 風險」
   - 不當主推 signal，當風險警告
5. **個別 pair 例外**：3037 欣興 vs 南電/景碩、3711 日月光 vs 京元電、2368 金像電 vs 健鼎 這幾對 S1/S2 uplift 較高（10%+）可標示「此 pair 歷史上券資比差 signal 較有效」，但仍保守使用

### 根本不適合的

6. 若未來想做「pair convergence 判斷」，需另外找 catalyst anchor（題材營收確認 / 法說日 / 大戶進出 T-account），只看 chip 技術面不夠

---

## Verdict 總表

| 項目 | 結果 |
|---|---|
| S1 券資比差 | **C**（AI era uplift +4.8% 邊際，Pre-AI +10.8% 較強但 AI era 減半） |
| S2 借券差 | **C**（uplift < 3%） |
| S3 RS gap slope | **C**（uplift ~0%） |
| S4 外資 10d 差 | **C**（uplift ~0%） |
| AI era 的 Pair Divergence 發生率更高？ | **否**（delta mean -0.7%，6/12 pair 為正） |
| 同業輪動 buy signal feature | **建議降級為 warning/提示**，不做主動 recommend |

