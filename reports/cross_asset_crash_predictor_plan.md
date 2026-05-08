# Crash Prediction System — Two-Stage Plan (v2)

**Date drafted**: 2026-05-08
**Status**: 規劃中（user 拍板 「下次 session 動工」+ 「優先 System 2 後 System 1」）
**Dependency**: B+E informational dashboard 已完工（commits 272446e + 4bed849 + d612b7c + c963378）
**前一版**: 2026-05-08 早版只含 System 1 cross-asset stack；user reframe 後加 System 2 為優先

---

## User 真正的需求（reframe 自原對話）

> **「主要目標還是放在一個夠準的預測系統，甚至在崩盤的前段 5% 的時候能有更高度的肯定是小回檔還是大崩盤」**

拆解兩層：
1. **「夠準的預測系統」** — 要 leading predictor，不是 informational coincident（B+E 沒滿足）
2. **「-5% 時區分小回 vs 大崩」** — conditional classifier（事件中段分流）

技術上是 **兩個獨立系統**，原 spec 只解第 1 層（System 1），漏了第 2 層（System 2）。

修正後架構：

```
事件 timeline:    [t-60d 危險區開始]  ...  [t=0 -5% 觸發]  ...  [t+60d 結束]
                  ──────System 1 fire ─────▶
                                              ──────System 2 fire ─────▶
                  Leading predictor             Conditional classifier
                  「接下來 60d 可能 ≥10%」       「已 -5%，最終是小回或大崩」
```

兩者都 fire → 高度肯定 risk-off
僅 System 1 fire → 早期警告（System 2 在 -5% 觸發後再給）
僅 System 2 fire → System 1 漏抓但事件中段補救

---

## System 2: Drawdown Regime Classifier (優先做)

### 為什麼 System 2 比 System 1 容易（且應該先做）

| | System 1 | System 2 |
|---|---|---|
| 樣本量 | 29 events / 27 年 | **~100+ events**（每次 -5% 都算）|
| Imbalance | 19% baseline | **~40% baseline**（-5% 後進入大跌的比例） |
| Lead time 要求 | 必須 leading 60d 前 | 不需要（已在事件中）|
| Distinguishing 訊號 | 學界普遍 weak | **學界 well-studied**（reflexivity vs mean reversion 有 distinct fingerprints）|
| SOP-14 約束 | N≤30 informational only | **N>30 可能進 portfolio gating** |

### Phase 2.1: 事件偵測 + 三類 label（4-6h）

#### 事件偵測

對 ^TWII 1999-2026：
- 從 60d rolling high 算 drawdown
- 第一天觸 -5% 為 event start
- 之後 60d 內最低點 = trough
- final_drawdown = (trough - peak) / peak

如果一個 event 還在 60d window 內又進入新 -5% 事件 → 視為同一個 event 延伸（不重複計）。

預期：1999-2026 約 100-150 個 -5% events。

#### 三類 label

| Label | 條件 | 預期樣本 |
|---|---|---|
| **Class A 小回** | final_drawdown 在 [-5%, -10%) | ~60% (60-90 events) |
| **Class B 中度** | final_drawdown 在 [-10%, -20%) | ~25% (25-37 events) |
| **Class C 大崩** | final_drawdown ≤ -20% | ~15% (15-22 events) |

額外 label 欄：
- `time_to_trough`: -5% 觸發到 trough 的天數
- `recovery_speed`: trough 後 30d 回到 -5% 起點的比例

### Phase 2.2: Feature Engineering（3-4h）

關鍵 distinguishing features（學界 + 實務經驗）— 在 -5% 觸發日當下計算：

#### 跨資產 risk-off 同步度（最有分量）

| Feature | 大崩 fingerprint | 計算 |
|---|---|---|
| HY OAS spread Δ in -5% week | 跳升 ≥50bp | 從 FRED `BAMLH0A0HYM2` 算 5d Δ |
| MOVE index Δ | 同步飆升 ≥20% | yfinance `^MOVE` 5d % change |
| VIX term structure | 翹尾 (front > back) | yfinance `^VIX/^VIX3M` ratio |
| DXY Δ | risk-off 時 USD 走強 | FRED `DTWEXBGS` 5d Δ |

#### 內部 reflexivity 訊號

| Feature | 大崩 fingerprint | 計算 |
|---|---|---|
| 5d 跌幅速度 | 越快越大崩 | (current - peak) / time_elapsed |
| Volume vs prior 20d avg | 爆量 = panic | 當日成交量 / 20d avg |
| Breadth (跌停家數比) | extreme negative | 從 TWSE bulk 撈每日漲跌 |
| 三大法人 5d 累積買賣超 | derisk 加速 | 既有 chip data |
| 融資維持率 | 跌破危險線 → margin call cascade | 從 chip_history/margin |
| 大盤 RSI(14) | 越極端越可能反彈 | 從 close 算 |

#### 政策/外部催化（binary flags）

| Flag | 計算方法 |
|---|---|
| Fed FOMC week | 從 FRED 政策日歷對照 |
| 新台幣急貶 5d > 1.5% | TWD/USD 5d Δ |
| 全球 risk event proxy | VIX > 30 等 |

預期最終 features ~12-15 個（要過 univariate filter）。

### Phase 2.3: Univariate per-feature lift（2h）

對每個 feature 跑 conditional univariate：
- 給定 -5% 事件已觸發
- P(大崩 | feature in danger zone) vs P(大崩 | feature 不 in danger zone)
- AUC vs Class C label
- precision@top-20% / lead time

篩選 lift > 1.3 才入選（比 System 1 嚴格因樣本充足）。

### Phase 2.4: Model（2-3h）

樣本 ~100+ 跨 5 個 crisis regime → **logistic regression with L2** 是合理選擇（避免 GBM overfit）。

也試 **multinomial logistic** 直接預測三類（小回/中度/大崩）：
- 給 user **三類 probability distribution** 而非 binary
- 例：「-5% 觸發，目前 readings 顯示：小回 25% / 中度 35% / 大崩 40%」

訓練：
- Walk-forward expanding window（仿前面 cf）
- CPCV + embargo
- block bootstrap CI
- SOP-13 xcorr classification

### Phase 2.5: Portfolio gating sim（2-3h）

仿 `tools/audit_crash_predictor_expected_value.py`：
- baseline: -5% 觸發後一律減倉到 50%
- 用 System 2: P(大崩) ≥ 60% → 全現金 / 30-60% → 50% / <30% → 不調倉
- 對比 11 年 portfolio MDD / Sharpe / CAGR

**SOP-12**: 必須 multinomial 分流 outperform 單純 -5% 觸發減倉（否則 fallback 到 binary）。

### System 2 預期 verdict

| Verdict | 機率 | 內容 |
|---|---|---|
| **過 SOP-10+12** | 50-60% | banner v4 加「事件中段分流燈」+ Discord push -5% 後立即推 P(大崩) |
| **AUC > 0.7 但 portfolio fail** | 25-30% | 進 informational tier (SOP-14) — 顯三類機率但不接 rebalance |
| **AUC < 0.6 全 fail** | 10-15% | 封桃，但累積的 conditional sample (100+) 仍有研究價值 |

**比 System 1 樂觀很多** — 樣本充足 + 學界基礎穩 + framing 對。

---

## System 1: Cross-Asset Leading Predictor (後續)

延用前一版 spec 內容，**System 2 落地後再做**。

### Phase 1.1: FRED 資料撈取（5-7h）

| Series | FRED ID | 用途 |
|---|---|---|
| HY OAS | `BAMLH0A0HYM2` | 信用壓力 |
| 3M-10Y yield curve | `T10Y3M` | 12-18mo lead |
| 10Y Treasury | `DGS10` | 利率水位 |
| Fed Funds | `DFF` | 流動性 |
| SOFR | `SOFR` | repo |
| Trade-weighted USD | `DTWEXBGS` | FX 壓力 |

### Phase 1.2: SPX panel + dual target（4-5h）

- TWII forward 60d MDD ≥10%/≥20%（已有）
- SPX forward 60d MDD ≥10%/≥20%（新）
- Cross-asset features 對兩 target 跑 transfer learning

### Phase 1.3: Univariate AUC + cluster + composite（3-4h）

仿 B+E v3 calibration：strict-preceding lead time / xcorr lag classification / cluster z-score 平均

### Phase 1.4: Portfolio gating sim（3-4h）

仿 audit_crash_predictor_expected_value.py — best-single + composite vs B&H

System 1 預期 verdict（前版未變）：
- 30-40% 通過（真 leading alpha）
- 40-50% informational only
- 15-25% 封桃

---

## Integration: Banner v4 + Discord（System 1+2 都過後 2-3h）

### Banner row 結構

```
Row 0:  [System 1 leading]  [HMM regime]
Row 0.5: [System 2 conditional] (只在 -5% 觸發時 fire)
Row 1:   [B+E coincident]  (現有 row)
Row 2:   [4 columns 加權/FGI/SPX/CNN] (現有)
```

### Discord push triggers

- System 1 升黃/橘 → push 一次（轉燈時觸發）
- **System 2 fire when -5% 剛觸發** → push 一次「-5% 觸發，P(大崩) = X%」
- 兩者都 fire → 高優先 push（reaction emoji + @user）

### 文案守則

仍守 SOP-14：
- System 1: 「歷史此狀態下 60d 內 ≥10% 同期重合率 X%」
- System 2: 「-5% 觸發時 P(大崩) = 40%（baseline 15%）」— 用 absolute probability 不用 lift（-5% 後使用者要直接 actionable）

---

## 總工時 + 排程

| 階段 | 工時 | Cumulative |
|---|---|---|
| Phase 2.1 事件偵測 + label | 4-6h | 4-6h |
| Phase 2.2 Feature engineering | 3-4h | 7-10h |
| Phase 2.3 Univariate filter | 2h | 9-12h |
| Phase 2.4 Multinomial model | 2-3h | 11-15h |
| Phase 2.5 Portfolio gating sim | 2-3h | **13-18h（System 2 完成）** |
| Phase 1.1 FRED 撈取 | 5-7h | 18-25h |
| Phase 1.2 SPX panel + dual target | 4-5h | 22-30h |
| Phase 1.3 Univariate + cluster | 3-4h | 25-34h |
| Phase 1.4 Portfolio gating sim | 3-4h | **28-38h（System 1 完成）** |
| Integration banner v4 + Discord | 2-3h | **30-41h（兩系統整合）** |

實務排程建議：
- **Session 1（5-6h）**: Phase 2.1+2.2 — 事件偵測 + feature engineering 完成 → user 看 sample 確認 design
- **Session 2（5-6h）**: Phase 2.3+2.4+2.5 — System 2 上線 verdict
- **Session 3-4**: System 1（如果 user 仍要做）
- **Session 5**: Integration + Discord

---

## SOP 守則繼承（從 R3）

1. **SOP-10** Portfolio gating sim 強制 gate
2. **SOP-11** Strict-preceding lead-time（System 1 用；System 2 不需要因為已在事件中）
3. **SOP-12** Composite / multinomial must beat baseline
4. **SOP-13** xcorr lag classification（System 1 用）+ cash drag 鑑別
5. **SOP-14** N>30 才能進 portfolio gating
   - System 2 N≈100 → 過閘
   - System 1 N≈50 (transfer learning) → 邊緣，要嚴格驗證
6. **Council Portfolio Backtest Auditor** mandatory role
7. **不允許 GBM/XGBoost** 在 N<200 樣本（D R2 建議）
8. **strict-preceding 要求 fire 在 event_start 之前 + forward 5d return < -1%**（System 1）

---

## 重啟條件

下次 session 動工前確認：

1. ✅ B+E informational dashboard 在 banner 上跑一段時間（最少 1 週）user 確認沒被誤解
2. ✅ FRED API key 申請好（System 1 階段才需要，System 2 純用 yfinance + 既有 cache）
3. ✅ 沒有 active production bug 搶資源
4. ✅ Working tree clean

---

## 預期最終樣態（兩系統都過閘）

User 看 banner 時：

**情境 1 — 平靜時**:
```
Row 0: System 1 綠燈 (清晰) | HMM neutral
Row 0.5: (隱藏，未 fire)
Row 1: B+E 綠燈 (35) — 同期重合率 17%
```

**情境 2 — System 1 開始警示**:
```
Row 0: System 1 黃燈 — HY OAS Δ 升 / VIX 升 / 歷史此狀態下 60d 內 ≥10% 重合率 X%
Row 0.5: (隱藏)
Row 1: B+E 黃燈 (60) — 同期重合率 33%
Discord push: System 1 升黃燈，建議審視持倉
```

**情境 3 — 已 -5% 觸發**:
```
Row 0: System 1 橘燈 (確認 leading 在前)
Row 0.5: System 2 fire — 「-5% 觸發 P(小回 25% / 中度 35% / 大崩 40%) — 大崩 baseline 15%」
Row 1: B+E 橘燈 (75)
Discord push @user: 「-5% 觸發，conditional 大崩機率 40%（baseline 15%），建議減倉到 50%」
```

**情境 4 — 大崩確認**:
```
Row 0.5: System 2 fire — 「P(大崩 70%)」
Discord push: 「大崩 conditional 機率 70%，全現金建議」
```

跟 R3 的「informational lagging signal」相比，**有了 directional probability quantification**，使用者真有 actionable info。

---

## 跨 session handoff 必讀

1. `~/.claude/.../memory/project_validation_bias_warning.md` — SOP 1-14 全文
2. `reports/_history/2026_05_crash_predictor_closed/crash_predictor_methodology_audit.md` — R3 council
3. `reports/banner_risk_score_calibration_v2.md` — B+E v3 calibration
4. **本檔** — Two-stage spec
