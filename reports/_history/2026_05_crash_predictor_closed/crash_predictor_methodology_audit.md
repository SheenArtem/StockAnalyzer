# Crash Predictor — Methodology Audit (Council R3)

**Date**: 2026-05-08
**Trigger**: Phase 2 verdict (univariate AUC 0.69-0.72 / lead 55-60d 全綠) 通過 backtest 第三道閘門「B 機會成本檢驗」反向翻盤
**Outcome**: Crash predictor 整條線封桃；最高價值產出 = SOP-10~14 + Council Portfolio Backtest Auditor mandatory role
**Council**: 4 視角並行 R1+R2，全 Opus

---

## TL;DR

Phase 2 univariate AUC 0.72 + lead time 55-60d 在 portfolio simulator 上完全失靈：50/30/20 composite portfolio MDD -32.3% **比 B&H -31.6% 還差**，m1b alone 是最差 gating（-6pp CAGR / 0 改善 MDD / 540d cash 浪費）。

**Smoking gun**：Council R3 (G) 親自重跑 panel 發現 Phase 2 lead time 算式 anchor `peak_date = forward 60d window 極大值`，是 forward-looking 構造性偏誤 — 真 strict-preceding leading event 只 5/29 = **17%**，6/8 fire 的 lead 接近 60d 上界 = window 寬度本身被當 lead time。

**最終定位**：N=29 events / 27 年 / 5 個不同 crisis regime（dotcom/2008/2015/2020/2022）= regime mixing + 統計力不足，整個 crash predictor 系統本質是 **informational lagging signal**，不是 predict。BANNER 文案禁用「預警/預測/領先」。

---

## R1 — 4 視角觀點

### (F) AUC↔Portfolio 翻譯失靈專家

1. **AUC vs P&L 本質脫節** (conf 75)：AUC 衡量 score 對 binary outcome 的排序能力，不含 (a) 報酬幅度、(b) cash drag、(c) 訊號時序。Bailey/López de Prado 推 Deflated SR / PBO 正是這道翻譯鴻溝。AUC 0.72 + portfolio -0.6pp MDD 完全相容。
2. **Coincident 數學直觀** (conf 100)：AUC = P(score₊ > score₋) 沒有時間 t 的 lag/lead 概念。若 m1b 在 drawdown event 期間 (t∈[t₀, t₀+60d]) 內任一日高於非 event 日，AUC 就高 — 不論 t₀-30d 還是 t₀+30d 觸發。Section 4 m1b 540d cash 0 MDD lift = 訊號 fire 在崩跌中段，數學自洽。
3. **Phase 2 lead time metric 定義錯了** (conf 100)：「fire 日 → forward 60d window 內 peak day 距離」內建 lookahead — 只要 fire 落在 [peak−60d, peak] 任何一天都計為 lead，包含 peak 前一天 fire（實際是 coincident/lagging）。m1b 60d「lead」可能 80% 是事件中段假性 lead。
4. **正確 single-asset crash eval 三件套** (conf 75)：(i) Strict-preceding hit rate fire 必須在 peak 前 N 日 N≥5；(ii) Portfolio simulator MDD/Sharpe Δ vs B&H 為 primary metric，AUC 退為 sanity check；(iii) False-cash-drag day count 比 cash 天數 / drawdown 避開幅度。

**Overall Confidence: 80**

### (G) Lead Time 計算審計師（smoking gun 抓最深）

**算式拆解** (`crash_predictor_tw_phase2.py:156-201`)：
```
lead_days = peak_pos - earliest_fire_pos
window: [peak_date - 60d, peak_date)
peak_date: forward 60d window 內最高點 ← 錨點本身是 forward-looking 極大值
```

1. **anchor peak_date 本身已 forward-looking** (conf 100) — lead time 計的是「fire 到行情頂部的距離」不是「fire 到下跌的距離」
2. **panel 親自重跑驗證** (conf 90)：29 events 中：
   - 21/29 完全無 fire（recall 17%）
   - 8/29 有 fire，**3/8 fire 日 ≥ event_start**（fire 在事件啟動之後）
   - 6/8 fire 的 lead 50-60d **接近 window 上界** = fire 後 60d 才到頂部，不是預警下跌
   - **真 leading event ≤ 5/29 = 17%**（不是 csv 顯示的 recall_top5=11%）
3. **正確 lead time 定義** (conf 75)：
   - Anchor 改 `event_start_date`（label=1 首日）
   - 條件 `fire_date ≤ event_start - 5d` 才算 leading
4. **重算結論**：m1b 在 8/8 fire-cases 裡 fire ≥ event_start - 12d 之內，3 案 fire 在 event_start 之後 → 真實 leading event 比例 ≤ 17%

**Overall Confidence: 90**

### (H) Coincident vs Leading 訊號分類專家

1. **rv10 leading vs rv30 coincident** (conf 75)：rv10 半衰期短能在 vol regime 切換初期跨閾值；rv30 對 shock 反應有 ~3 週延遲，等它 z>1 時 drawdown 多半已展開
2. **m1b coincident** (conf 75)：分子（20d 成交金額）事件期間放大 + 分母（M1B 月供給）變化慢 = ratio 同步衝高，本質「現況熱度」非「預測」
3. **leading vs coincident 鑑別法** (conf 75)：cross-correlation lag 圖 + Δ vs level 較好；Granger 在 N=29 檢定力弱
4. **重審 5 因子** (conf 50-75)：
   - rv10 ✓ leading；rv30 ✗ coincident；m1b_ratio_pct ✗ coincident
   - foreign_5d_z 偏 leading；foreign_20d_z 偏 coincident
5. **下輪驗證候選**：rv10-rv30 spread / Δ rv10 z-score / vol-of-vol / m1b 月變化率 / foreign 5d Δz

**Overall Confidence: 65**

### (I) Portfolio Simulator SOP 起草人

**4 條 anti-pattern**：
- AP-1 AUC/IC high ≠ portfolio P&L (conf 100)
- AP-2 「lead 55-60d」是 event-window 平均，非 strict-preceding daily-gating (conf 100)
- AP-3 Univariate filter pass ≠ composite portfolio pass (conf 100)
- AP-4 Coincident vs Leading 沒分 (conf 75)
- AP-5 Track A pass 用 P@5%/R@5% 在 event windows 量，沒映射到 daily allocation cost (conf 100)

**SOP-10~13** + Council mandatory Portfolio Backtest Auditor role + reproducer 工程改進清單

**Overall Confidence: 95**

---

## R2 Critique 重要更新

### F R2：完全推翻 Phase 2 verdict
- G 的 5/29 重算把 F R1 的「理論質疑」升級為「實證共識」(conf 85)
- m1b 的「leading」標籤必須撤回，重跑 strict-preceding 才能下結論 (conf 90)
- **共識盲點**：N=29 underpowered (conf 85) — 4 視角都在「修對 metric 就能救」的框架內優化，但 27 年 29 events 拆 train/test 後每組 ≤15，strict-preceding 真陽性可能只剩 2-3 個，任何 metric 都 noise dominated → 應加 SOP「N<50 的 crash predictor 一律 advisory，不准單獨 gate live capital」(conf 75)

### G R2：metric 定義錯不是解讀錯（駁 F）
- 6/8 fire 卡 ~58-60d 不是巧合，是 metric **構造性偏誤** (conf 75)
- 必須重定義 anchor = event_start_date，F 提的 Time-Stratified AUC / P@k 是補充指標不是替代 (conf 50)
- **rv10 真 leading 還是 cash drag artifact?** (conf 50) — rv10 半衰期短 → fire 密度低 → cash 占比低 → portfolio MDD 自動受惠（**cash drag artifact**），與 leading 無關。必須拆 strict-preceding hit rate vs cash day count
- **致命盲點** (conf 100)：dotcom (流動性 driven) / 2008 (信用) / 2015 (China devaluation) / 2020 (pandemic) / 2022 (rate) 機制根本不同，**pooled N=29 算單一 hit rate 是 regime mixing**，即使 metric 修對統計 power 也不足

### H R2：撤回 HAR-RV 學界背書 + 重定位為 informational
- 服 G 質疑 — rv10 portfolio 表現好可能 cash drag artifact 不是 leading (conf 75)
- **撤回 HAR-RV 學界背書**：HAR-RV (Corsi 2009) 是 forecasting volatility，不是 crash predictor (conf 75)。rv10-rv30 spread 在學界沒有穩固 crash-predictor 文獻
- **完全同意系統重定位** (conf 100)：N=29 + 5/29 真 leading + 任一 candidate 過 strict-preceding 機率渺茫 = 從 **predict** → **inform**。AI 報告 BANNER 文案禁用「預警/預測/領先」(conf 100)，改用「同期風險指標：當前 readings 高於歷史 X%」

### I R2：SOP 精準化 + SOP-14 拍板
- **SOP-11 雙重 sanity** (conf 75)：`strict_lead_d ≥ 5d AND forward_5d_ret(fire_date) < -1%`，純 lead 條件不夠
- **Portfolio Backtest Auditor 升 gate role** (conf 75)：沒這 role 的 verdict 標 INVALID — missing portfolio gate
- **SOP-13 xcorr threshold + cash drag 鑑別** (conf 75)：portfolio sim 必 report cash_pct，cash_pct > 30% 標 `low_exposure_artifact` 不算 leading
- **SOP-14 N≤30 informational only** (conf 100)：「strict_fire_count ≤ 5 的 crash predictor 一律 informational tier，禁 banner / 禁 rebalance / 禁 hard rule」— **這條救我們很多次，是 R3 最高價值產出**

---

## Final Verdict

### 對 user 原問題的答案

> **「有辦法透過回測找出高可信度的多重因子嗎？目前猜測是需要至少五個以上的因子？」**

**沒有，至少在台股大盤回檔預警場景下沒有。** 三層理由：

1. **N=29 events / 27 年 / 5 個不同 crisis regime** — 統計力不足以驗任何 multi-factor model
2. **AUC + lead time 看起來漂亮的 metric 都是構造性偏誤** — 真 strict-preceding leading 比例 17%
3. **「找 5+ orthogonal 因子」對 rare-event prediction 在數學上無解** — 多因子搜尋只會放大 multiple testing 災難

### 三方案最終排序（B audit 實證 + R3 確認）

| 排名 | 方案 | 工時 | ROI | 結論 |
|---|---|---|---|---|
| 1 | **SOP-10~14 收進 user memory** | 1h | **長期最高** | 阻止下次重蹈，立刻做 |
| 2 | rv10-only Mode D throttle | 1-2h | **不確定**（H/G 質疑 cash drag artifact） | 暫緩，要先跑 strict-preceding 重算 |
| 3 | (c) Mode D conviction tier sizing | 6-8h | 樣本不足 | Park（等 paper trade ≥6 月） |
| 4 | (b) ATR 動態停損 | 4-6h | **負 ROI**（K=2.0/2.5/3.0 全輸固定 8%） | 不做 |
| 5 | (a) 50/30/20 composite Phase 3 | 8-12h | **負 ROI**（比 B&H 還差） | 封桃 |

---

## SOP-10~14（擴 `project_validation_bias_warning.md`）

### SOP-10 — Portfolio gating sim 強制 gate（擴 portfolio rotation rule）

任何 *market-state / regime / crash-warning / signal gating* 訊號上線前，必跑 daily-allocation portfolio sim（B&H + best-single-factor + composite 三欄齊全），輸出 CAGR / Sharpe / MDD 三欄齊全。**AUC / IC / lead-time 全綠不能上線**。

- **Why**: AUC ≠ portfolio P&L。`audit_crash_predictor_expected_value.py` §4 已示範：50/30/20 composite AUC 0.72 但 portfolio MDD 比 B&H 還差
- **How**: 仿 `tools/audit_crash_predictor_expected_value.py`，內建 5 strategy 對比 + B&H 非選擇性 baseline + best-single 必入表

### SOP-11 — Strict-preceding lead-time only

Lead-time metric 只認「signal fire bar < event peak bar」的 strict-preceding case。雙重 sanity：
```
strict_lead_d := event_start_date - first_fire_date
required: strict_lead_d ≥ 5  AND  forward_5d_ret(first_fire_date) < -1%
```

- **Why**: Phase 2 lead 算式 anchor = peak_date forward window 極大值是構造性偏誤；event-window-avg lead 跟「fire 必須在 peak 之前」混淆
- **How**: reproducer 加 `lead_d_strict` column；原 `lead_d` 註明 `event_window_avg`；fire 日對應 forward 5d return 必須負 1% 以上才算 leading（避免 fire 後反彈再崩的巧合）

### SOP-12 — Composite must beat best-single (portfolio metric)

Composite 上線必須 portfolio Sharpe > 任一 single-factor Sharpe，且 ≥1 component 通過 strict-preceding lead test。

- **Why**: Phase 2 50/30/20 composite Sharpe 0.81 輸給 rv10-only 0.98；composite 在 portfolio P&L 贏 best-single 但全是 coincident 訊號疊加 = 沒有預警價值
- **How**: gating sim 必含 N+1 row（N single + 1 composite）；verdict block 自動拒絕「composite 輸 best-single」

### SOP-13 — Cross-correlation lag classification + cash drag 鑑別

任何 warning signal 必跑 xcorr（signal vs forward returns / drawdown indicator）lag 圖：
- `lag < 3d` 視為 **coincident**（不可宣稱「預警」）
- `3-15d` 視為 **mixed**
- `>15d` 才標 **leading**

加 cash drag 鑑別：portfolio sim 必須 report `cash_pct`，**cash_pct > 30% 標 `low_exposure_artifact` 不算 leading**（rv10 portfolio 表現好可能是 cash drag artifact 不是 timing skill）。

- **Why**: 同步發生不是預測；cash drag 偽裝成 MDD 改善
- **How**: reproducer 內建 `xcorr(signal, peak_indicator, max_lag=60)` 表 + `cash_pct` 欄位；verdict 強制標 `coincident|mixed|leading`

### SOP-14 — N≤30 events 一律 informational tier（最重要）

```
if N_events < 30 OR strict_fire_count <= 5:
    deployment_tier = "informational_only"
    block: banner, rebalance, hard_rule, position_size
```

- **Why**: 27 年 29 events / 5 個不同 crisis regime 是 regime mixing；任何 crash predictor 在這個 N 下統計力幾乎為零；BANNER 紅燈會被使用者過度信任
- **How**: SOP-10 portfolio sim 出 N + strict_fire_count 後自動 gate；只能進 AI 報告 EDA 段「informational lagging signal」段落，禁進 banner 燈號 / Mode D rebalance / paper_trade 出場條件

### Council 模板新增 mandatory role: Portfolio Backtest Auditor

任何 policy-affecting council（含 signal/factor/regime/gating/rotation/exit-rule 主題）強制掛 Portfolio Backtest Auditor gate role。職責固定：
1. 確認 portfolio sim 跑過
2. 確認 baseline B&H 在表內
3. 確認 best-single 在表內
4. **AUC/IC-only verdict 一律否決升 D 假象**

沒這 role 的 verdict 標 `INVALID — missing portfolio gate`。例外：純 IC research / EDA 探索性 council 不強制掛（避免 over-rigid）。

### Reproducer 工程改進清單

`tools/crash_predictor_tw_phase2.py` 之後該加：
1. 內建 `run_portfolio_gating_sim()` block，仿 audit §4
2. `lead_d` 拆 `lead_d_event_window`（現值）+ `lead_d_strict_preceding`
3. verdict block hard rule：AUC pass + portfolio Sharpe fail = D 假象，禁推 Phase 3
4. 自動跑 xcorr lag table，lag<3 標 coincident

---

## 歷史完整輸出

### 本次調查產生的 reports（全歸檔到 `reports/_history/2026_05_crash_predictor_closed/`）
- `crash_predictor_tw_panel.parquet`（27 年 daily panel）
- `crash_predictor_tw_panel_summary.md`（事件清單）
- `crash_predictor_tw_phase2.md`（univariate AUC + cluster）
- `crash_predictor_tw_factor_metrics.csv`
- `crash_predictor_tw_factor_corr.csv`
- `crash_predictor_tw_dendrogram.png`
- `crash_predictor_b_opportunity_cost_audit.md`（B audit smoking gun §4）
- `audit_atr_vs_fixed_stop.csv`
- `audit_crash_predictor_expected_value.csv`
- `crash_predictor_methodology_audit.md`（本檔）

### Reproducer scripts（保留 `tools/` 可重用）
- `tools/build_crash_predictor_panel.py`（M1B 35d lag fix 已落地）
- `tools/crash_predictor_tw_phase2.py`（待加 portfolio gating sim + strict-preceding lead）
- `tools/audit_atr_vs_fixed_stop.py`
- `tools/audit_crash_predictor_expected_value.py`

### Council 流程
- R1 (5 視角) + R2 (critique) — 設計 + 訊號 + 模型 + UX → 開出 cluster composite + N-of-M 燈號 + Discord-primary UI
- B audit (chip-analyst) — 機會成本 + ATR vs 固定 + composite portfolio gating sim → 翻盤
- R3 (4 視角 + critique) — 方法學審 → SOP-10~14

---

## 關鍵教訓 (TL;DR for future Claude)

1. **AUC + lead time 漂亮不等於有 alpha** — 必跑 portfolio simulator
2. **Lead time anchor 不可用 forward-looking 極大值點** — 用 event_start_date + 5d buffer
3. **N<30 events 不要試圖建 predict 系統** — 定位 informational lagging signal
4. **Composite 須過「portfolio Sharpe > best-single」雙重 gate**，不能只看 AUC
5. **Council R1+R2 默認該掛 Portfolio Backtest Auditor mandatory role**

> 這是專案第二次踩「proxy ≠ portfolio simulator」坑（第一次 Dual cf1e2e0 翻盤）。SOP-10 必須是 hard rule 不能再漏。
