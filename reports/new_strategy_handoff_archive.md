---
name: 新策略研究 handoff 狀態（2026-04-24 session 尾）
description: 新 session 接手入口 — 使用者策略需求 + 已驗證結論 + 重大錯誤 flag + 下一步
type: project
originSessionId: d9dd0a72-b1b5-4577-8f52-cb2938f042b5
---
# ⚠️ New Session Must Read First

**接手前必讀三件事**：

1. **使用者目標**：超越 0050（10 年 net CAGR vs TWII TR 至少 +3pp）
2. **已驗證 6 個框架全 fail**（但 backtest 有設計錯誤，結論需重驗）
3. **⚠️ CRITICAL BUG**：所有既有 backtest 的 rebalance 頻率（月頻全換）**違反使用者 #5 持有期 2 週-3 個月需求**。Step A 契約定的 `MIN_HOLD_DAYS=20 + Whipsaw Ban 30 日` **從未實作到 backtest**。→ 既有 alpha 結論是「高頻暴力換股版本」，不是使用者真正要的策略

---

## 📋 使用者策略需求 11 條（從 `project_new_strategy_brainstorm.md` 整理）

### 核心目標
- **#5-revised**：年化 **40-80%**（原 80-120% 已下修）→ 實際現在面臨「能 beat 0050 就好」
- **使用者最終宣告**：「**超越 0050**」，並接受「一直做直到做到好」
- **使用者澄清**：接受挑戰 #2 原則（不追右側動能），**只要能達到目標就好**

### 策略 DNA 需求
| # | 原始需求 | 狀態 |
|---|---|---|
| #1 | 能知道牛/熊 regime | ✓ SMA200 + 4-regime + VF-G4 已備 |
| **#2** | ~~不追右側動能突破~~ | **2026-04-24 使用者鬆綁**：只要能達標即可 |
| #3 | 技術支持 + 產業趨勢 + 避免主力做線 | 部分滿足（產業 ✓ 技術背離待 IC 驗）|
| #4 / #4a | 流動性/市值前 600 / 法人認養股 | 未實作到 backtest（tv_top_25 proxy 失敗）|
| **#5** ⚠️ | **持有期 2 週 ~ 3 個月**，超過 3 個月要值得期待 | **backtest 完全沒遵守 — 是重大 bug** |
| #6 | 參考宋分策略 | factor 候選記錄在 `project_songfen_*.md` 未驗 |
| #7 / #7a | 股票資訊來源盤點 + MOPS 逐字稿 | MOPS 解禁中，待啟動 |
| #8 | LINE 群組資訊 | 8e 使用者決定 defer |
| #9 | 動態精選三檔策略 | Council 確認 violates #2，且 sample size 不足 |
| #10 | 一次操作幾檔 | top_n grid 驗過，tv_top_25 + top_20 原是 Step 1 champion 但 OOS 翻盤 |
| **P1** | **所有決策須正式回測支持** | 全域原則 |
| **P2** | 使用者 idea 也是候選需驗證 | 全域原則 |

### 報酬/風險 aspirational（使用者自認非硬規則）
- 目標 3 個月報酬 **20-30%**（= 80-120% 年化）
- 停損 **10-15%**（非硬性）

---

## 📊 已驗證 6 個策略 — 全部 FAIL AI era

**⚠️ 所有結論基於「高頻月頻全換」backtest，不符 #5 持有期需求，扣成本可能高估 1-3pp**

| # | 策略 | Full 10yr α_net | AI era 2023-2025 α_net | Verdict |
|---|---|---|---|---|
| 1 | Dual + tv_top_25 + only_volatile top_20 | (Step B OOS 翻盤) | - | Step B 死 |
| 2 | **Dual-all + only_volatile top_20** | **-4.96pp** | **-18.64pp** | D FAIL |
| 3 | PEAD long-only top quintile | 20d spread 1.89pp | 60d p=0.55 不顯著 | D FAIL |
| 4 | 券資比反轉 | IC -0.046 顯著 | **方向翻轉 +0.56pp** | D FAIL |
| 5 | Sector Rotation (mom12m_top5sec best) | **+0.61pp** | **-17.70pp** | D FAIL |
| 6 | Baseline A (Value top_5 only_volatile) | -4pp 左右 | - | D FAIL |

### Pattern
- Pre-AI 2016-2022：多個策略都有 α（Sector Rotation +8pp / Dual +0.87pp / 券資比反轉 -3.39pp 符預期）
- AI era 2023-2025：**全部失效或反向**
- 根源：**2330 權重 61.68% × 2024 +83%，0050 市值加權吃到；任何分散選股都輸**
- **Survivor Bias 的「結構性不可達」預言已 80% 成真**

---

## 🐛 Critical Bug：Backtest 不符 #5 持有期

### 錯誤
所有 backtest（`vf_step1_*.py` / `vf_step_c_*.py` / `vf_step_d0_*.py` / `vf_step_3a_*.py`）設計為：
```
每月 rebalance → top_N 全重算 → 不在新 top_N 就全砍補新
```
→ QM 側實測每月 turnover **80-117%**（幾乎全換），Value 側 32-51%。
→ 每檔平均持股 **<1 個月**，違反使用者 #5「**持有期 2 週 ~ 3 個月**」。

### Step A 契約（2026-04-24 寫好但沒落地）
`project_dual_position_monitor_contract.md` 第 4/5/7 條：
- **MIN_HOLD_DAYS = 20**（抗 whipsaw）
- **Whipsaw Ban 30 日**（hard-exited 股 30 日內禁入）
- **規則 7：regime 切換的舊持股下次 rebalance 一次清倉**

這些 **rules 只存在 memory，從沒在 backtest 實作**。

### 正確的 backtest 應實作
```python
for each month_end:
    for each existing position:
        if hold_days >= MIN_HOLD_DAYS:
            if any_hard_exit_condition:   # position_monitor 7 條
                exit()
            elif hold_days >= MAX_HOLD_DAYS:   # 60-90 天強制
                exit()
            else:
                keep()
        else:
            keep()  # 內不換

    for each empty slot:
        fill from top_N newcomers (not in Whipsaw Ban)
```

### 影響估算
| 指標 | 現在（錯）| 修正後（估）|
|---|---|---|
| 年換股次數 | 240-380 次 | ~60-80 次 |
| 成本 drag | 3-4pp/年 | **~1-1.5pp/年** |
| Dual-all net α | -4.96pp | **~-2.6pp** |
| Sector Rotation net α | +0.61pp | **~+2.6pp**（接近 +3pp gate 🔥）|

**潛在翻盤**：Sector Rotation 修正成本後可能 marginal pass +3pp gate。

---

## 🧩 Council 系統性缺陷

跑了 4 輪 council + 15+ perspectives 都沒抓到這 bug。原因：

1. **Anchor bias**：所有 agent 讀 `vf_value_portfolio_backtest.py` 看到 `REBALANCE_EVERY=4 (weeks)` 當 given，沒質疑
2. **缺 User Requirement Auditor perspective**：15 個 role 全是量化視角，沒人做需求對照
3. **Validation Architect 只驗 ex-post statistical gate**（α / Sharpe / MDD），沒 ex-ante operational gate（turnover 合理性）
4. **契約 ↔ backtest 一致性無檢查**：Step A 契約寫完即忘
5. **Step C' 實際計算了 turnover 但我只當 cost input**，沒 sanity check「117% 換手是否合理」

### New session 該做的預防
- **每輪 council 默認加 User Requirement Auditor perspective**（策略設計類問題必備）
- **Validation Architect gate 加第 6 條**：turnover + 持有期符合使用者需求
- **新 backtest 跑完輸出時必 print 持有期 / turnover / 年交易次數**，人工 sanity check
- **Step A 契約當 hard spec**，寫 `tools/vf_contract_compliance_check.py` 驗 backtest 是否 follow

---

## 🎯 Next Session 優先級

### 立即（必做）
1. **寫正確的 position-tracking backtest framework**（`tools/vf_position_tracked_backtest.py`）
   - 實作 MIN_HOLD_DAYS / MAX_HOLD_DAYS / Whipsaw Ban
   - 模擬 position_monitor 7 條 hard exit
   - 輸出年度 turnover / 持有期分布
   - **Contract compliance check built-in**
2. **用正確 framework 重跑 Dual-all / Sector Rotation**，看 net α 是否翻盤
3. **更新 Validation Architect gate 加 operational compliance**

### 中期（重跑完後決定）
4a. 若 Sector Rotation 修正後 net α > +3pp → 進 Core-Satellite 組合 backtest
4b. 若全部仍 fail → 接受 Survivor Bias 逃生結論，轉 0050 DCA + Dual-all MDD 保護
5. 若 4a 成功 → Step D live integration（加 tag 到 scanner_job）

### 長期（規模化）
6. 擴 US universe 當 diversification 保險（若台股 AI regime 延續）
7. Council 模板升級加 User Requirement Auditor
8. MEMORY.md pin 使用者 #5 需求到顯眼位置

---

## 📁 現有 backtest scripts（保留，重跑用）

| Script | 狀態 |
|---|---|
| `vf_step1_dual_mcap_grid.py` | ❌ 高頻 bug，結論作廢 |
| `vf_step1_followup_topn.py` | ❌ 同上 |
| `vf_step1_oos_killtest.py` | ❌ 同上 |
| `vf_step1_b5_dualall_diagnose.py` | ❌ 同上 |
| `vf_step_c_prime_net_alpha.py` | ❌ 同上 |
| `vf_step_d0_pead_event_study.py` | ✓ event study 架構對（event-driven 持有 20-60d），可保留用 |
| `vf_step_d0_margin_short_ic.py` | ⚠️ weekly IC 跑法 OK 但 portfolio 層未驗 |
| `vf_step_3a_sector_rotation.py` | ❌ 高頻 bug，結論作廢 |

## 📁 reports/（結論作廢但數字留底）
- `vf_step1_dual_mcap_grid.md`
- `vf_step1_followup_topn.md`
- `vf_step1_oos_killtest.md`
- `vf_step1_b5_dualall_diagnose.md`
- `vf_step_c_prime_net_alpha.md`
- `vf_step_d0_pead_event_study.md`
- `vf_step_d0_margin_short_ic.md`
- `vf_step_3a_sector_rotation.md`

---

## 🔗 關鍵 memory cross-ref

- `project_new_strategy_brainstorm.md` — 完整 10 條需求累積 + 8 個 step 結果
- `project_dual_position_monitor_contract.md` — Step A 契約（backtest 沒實作的 8 條規則）
- `project_songfen_value_factors.md` / `project_songfen_timing_signals.md` — 未驗 factor 候選
- `project_final_strategy_roadmap.md` — UI 整合方向（Dual 50/50 過渡版已 ship）
- `feedback_strategy_design_backtest_required.md` — P1 原則
- `feedback_robustness_first.md` — CLAUDE.md 最高原則
- `project_validation_bias_warning.md` — 歷史多頭偏差教訓

---

## 📝 本 session 最大教訓（寫進 feedback）

**教訓 1：契約 ↔ backtest 一致性必須用工具檢查**，不能靠人類記憶。memory 裡寫的 contract 不等於 backtest 實作的規則。

**教訓 2：Council perspective 不能全量化**。「使用者需求」需要獨立 auditor 角色。

**教訓 3：Turnover 數字 > 50% 是 red flag**。這次實測 QM 117% 月 turnover 就該觸發 sanity check。

**教訓 4：花 24h + 4 輪 council + 8 個 step 才發現設計 bug**。Handoff memory 應 frontload「critical bug / 未驗假設」而不是 frontload「已做什麼」。
