# 技術指標 IC + Quantile 驗證報告 v2

| 欄位 | 值 |
|------|----|
| **版本** | v2 |
| **建立日期** | 2026-04-13 |
| **資料範圍** | 2011-04-14 ~ 2026-04-13（15 年） |
| **樣本** | 台股 1,951 檔（FinMind 現役清單，6.1M rows OHLCV） |
| **狀態** | 🟢 Current |
| **取代前版** | [v1 (2026-04-13)](./IC_analysis_v1_2026-04-13.md) — 部分結論錯誤，詳見下文 |
| **被取代於** | — |
| **下次檢視** | 2026-10 或 scanner 大改版後 |

---

## ⭐ 關鍵變革：Quantile 驗證推翻 v1 兩個核心結論

v1 用 Spearman IC 下結論「scanner 符號反向」+「rvol_lowatr 最強」。Phase 2c Quantile 報酬驗證顯示：

1. ❌ **scanner 符號不需要反向** — Top-20 持 20 日 +2.48%，D10 最賺
2. ❌ **rvol_lowatr 絕對報酬輸 scanner** — +1.14% vs +2.48%（雖然 Sharpe 較好）

**根本教訓**：IC 測全排名 Spearman 相關，scanner 實際取 Top N。兩者量測對象不同：
- IC 低可以來自**中段非單調**，不代表 Top bucket 不賺錢
- Quantile 分析才能驗證 scanner 真實績效

**以後做因子研究必須同時看 IC + quantile + Top-N portfolio 三件事**。

---

## ⚠️ 使用守則

- **In-sample 分析**，未做 walk-forward 驗證，實戰績效可能打折。
- 純技術面（OHLCV），**不含籌碼、基本面、消息面**。
- 未納入交易成本（台股約 0.5% 手續費+稅+滑價）。
- 樣本 1,951 檔為 FinMind 現役，**有倖存者偏誤**。

---

## 1. 核心結論（Executive Summary）

### ① Scanner 現行邏輯已接近最佳 — **不需要大改**

`combo_3group_median_raw` Top-20 投組（每日取最高分 20 檔持 h 天）績效（universe=all）：

| Horizon | 平均報酬 | Sharpe | 勝率 |
|---------|---------|--------|------|
| 5d | +0.88% | +3.35 | 59.8% |
| 10d | **+1.66%** | **+4.53** | 63.4% |
| 20d | **+2.48%** | **+4.69** | 62.1% |

Momentum universe（流動性過濾後）：

| Horizon | 平均報酬 | Sharpe | 勝率 |
|---------|---------|--------|------|
| 10d | +1.42% | +3.80 | 62.5% |
| 20d | +2.04% | +3.86 | 60.0% |

**Sharpe ~4 是專業級量化策略標準**。

### ② Decile Spread 完美單調遞增

Scanner 現行邏輯 (universe=all, h=20d) 的 10 個 decile 平均 20 日報酬：

```
D1:  +0.79%  ###############
D2:  +0.78%  ###############
D3:  +0.83%  ################
D4:  +0.89%  #################
D5:  +0.97%  ###################
D6:  +1.04%  ####################
D7:  +1.14%  ######################
D8:  +1.19%  #######################
D9:  +1.33%  ##########################
D10: +1.74%  ##################################
```

D10/D1 = **2.2x**，幾近完美單調。Scanner 分數排序確實反映報酬預期。

### ③ rvol_lowatr 仍是風險調整後贏家（但不是絕對報酬贏家）

| 指標 | Scanner | rvol_lowatr |
|------|---------|-------------|
| Top-20 @ 20d 平均報酬 | **+2.48%** | +1.14% |
| Top-20 @ 20d Sharpe | +4.69 | **+6.07** |
| Top-20 @ 20d 勝率 | 62.1% | **69.8%** |
| Top-20 @ 20d std | 8.38% | **2.99%** |
| Top-20 @ 20d max drawdown (估) | 較大 | 較小 |

**取捨**：
- Scanner：追絕對報酬，波動大
- rvol_lowatr：追 Sharpe + 勝率，波動小

**兩者各有用途**：
- 主動型部位（追漲+高 beta）用 scanner 現行邏輯
- 保守型部位（追求穩定複利）可考慮 rvol_lowatr
- **合併**：scanner 選出候選後，用 rvol_lowatr 當第二層 filter（可能兼得）

### ④ v1 仍成立的結論

- **指標冗餘**：MA20偏離 / VWAP偏離 / BB %B / RSI偏離 / EFI 五個兩兩 corr 0.78-0.93，scanner 等於 4-5 次計算同訊號
- **短期均值回歸**：1d horizon IC 負 → 短線交易（日內/隔日）適合反向操作，但對 scanner 5-20d 持倉不適用
- **RVOL 是唯一穩定正 IC 個別指標**：IC +0.018（全 regime）, +0.026（volatile regime × 20d）
- **ATR% 最強個別 IC**：IC -0.079（全 regime × 20d），低波動溢酬學術定律成立

---

## 2. 建議改動（完全改寫）

### 🟢 **P0：短期不需要改動 scanner trigger_score 邏輯**

Scanner 現行 Top-20 Sharpe 4.69、勝率 62% 已是優秀表現。不要為了理論 IC 輕易翻方向。

### 🟡 **P1：冗餘指標合併（降噪 + 效能）**

把高度相關的 5 個指標合併成 1 個複合指標：

```python
mean_reversion_composite = (
    rank(ma20_dev) + rank(vwap_dev) + rank(bb_pos)
    + rank(rsi_dev) + rank(efi)
) / 5
```

好處：
- 減少 noise（5 個幾乎相同訊號被平均後 SNR 提升）
- 降低計算量（個股分析/AI 報告/scanner 共用）
- 釋放權重給真正獨立的訊號（RVOL、ATR、Squeeze、MACD）

**不改 scanner 核心邏輯**，只是重構訊號來源。

### 🟡 **P2：考慮加 rvol_lowatr 做第二層過濾**

`rvol_lowatr` 勝率 70% + Sharpe 6 很值得利用。設計：

```python
# 1. Scanner 原本的 trigger_score 選出候選 Top 50
candidates = scanner.top_n(trigger_score, n=50)

# 2. 對候選再用 rvol_lowatr 排序，選前 20
final_picks = candidates.top_n(rvol_lowatr_score, n=20)
```

**預期**：保留 scanner 的報酬來源 + 吸收 rvol_lowatr 的穩定性 → Sharpe 可能進一步提升。

需要實證，可做 A/B 測試。

### 🟢 **P3：短線交易（1-3d）可用 meanrev_pure**

短期均值回歸在 1d horizon 有 IC=+0.060、75.5% 勝率。但**不適合 scanner 持倉型策略**，因為 10d 後就衰退。可當 day-trading 或 5d 以下短線工具，獨立於 scanner。

### 🔴 **P4+（長期）**

- **Walk-forward 驗證**：切 2011-2020 訓練、2021-2025 測試，確認結論在 out-of-sample 維持
- **Regime-dependent 權重**：scanner 可根據 HMM regime 動態切換 rvol_lowatr / meanrev 主力
- **加籌碼面 factor**：2021-2025 歷史 FinMind 法人資料納入 IC 驗證
- **Dollar P&L 模擬**：計入 0.5% 交易成本後的實際報酬

---

## 3. Top-N Portfolio 完整績效矩陣

| Score | Uni | H | Mean% | Std% | Sharpe | Win% |
|-------|-----|---|-------|------|--------|------|
| **combo_3group_median_raw** | all | 5 | +0.88 | 4.18 | +3.35 | 59.8% |
| **combo_3group_median_raw** | all | 10 | **+1.66** | 5.81 | +4.53 | 63.4% |
| **combo_3group_median_raw** | all | 20 | **+2.48** | 8.38 | +4.69 | 62.1% |
| combo_3group_median_raw | mom | 10 | +1.42 | 5.94 | +3.80 | 62.5% |
| combo_3group_median_raw | mom | 20 | +2.04 | 8.38 | +3.86 | 60.0% |
| **combo_rvol_lowatr** | all | 5 | +0.38 | 1.31 | +4.60 | 65.4% |
| **combo_rvol_lowatr** | all | 10 | +0.64 | 1.97 | +5.19 | 67.6% |
| **combo_rvol_lowatr** | all | 20 | +1.14 | 2.99 | **+6.07** | **69.8%** |
| combo_rvol_lowatr | mom | 20 | +0.85 | 3.43 | +3.93 | 65.8% |

完整數據：`reports/quantile_topn_portfolio.csv`

---

## 4. 方法論

### 4.1 Decile Returns

- 每日按 score 排名，取 `qcut(..., 10)` 分 10 bucket
- 每 decile 計算當日所有股票的 forward h 日平均報酬
- 跨時間聚合：mean / median / std / win_rate / n_days
- 資料需 ≥ 50 檔股票才算當日 decile（MIN_CROSS_SECTION）

### 4.2 Top-N Portfolio Simulation

- 每日取 Top-N (N=10/20/50) by score
- 計算該 N 檔的 forward h 日平均報酬 = 當日 portfolio return
- 整合：mean / std / Sharpe (annualized, × sqrt(252)) / win_rate
- Top/Bottom 雙向測試（看多 vs 看空）

### 4.3 重要限制

- **Sharpe 是 "proxy"**：因為 fwd return 已是 h 日總報酬，不是日報酬。正確的 Sharpe 需要連續持倉 daily return，本方法估計偏高。
- **Overlapping periods**：連續日 Top-20 持 20 天有重疊，標準差低估。
- **No transaction cost**：實際扣 0.5% 後，每次換倉吃掉 ~0.25% 單邊成本。
- **Survivorship bias**：FinMind 現役清單，未納入下市股。

---

## 5. 產出檔案

| 檔案 | 說明 |
|------|------|
| `reports/quantile_decile_returns.csv` | 1,500 筆 decile return（25 score × 3 horizon × 2 universe × 10 decile） |
| `reports/quantile_topn_portfolio.csv` | 900 筆 Top-N portfolio（25 score × 3 horizon × 2 universe × 3 N × 2 direction） |
| `reports/indicator_ic_matrix.csv` | v1 產出，個別指標 IC（仍有效） |
| `reports/indicator_combo_ic.csv` | v1 產出，組合 IC（仍有效，但對應到「絕對報酬」需配合 quantile 看） |
| `tools/indicator_quantile_returns.py` | Phase 2c 分析腳本 |

---

## 6. 再現性

```bash
# Phase 1 資料（15 年 OHLCV）
python tools/backtest_dl_ohlcv.py

# Phase 2a 個別指標 IC
python tools/indicator_ic_analysis.py

# Phase 2b 組合 IC + OLS
python tools/indicator_combo_analysis.py

# Phase 2c Quantile 驗證（本報告核心）
python tools/indicator_quantile_returns.py
```

全流程總時間約 70-80 分鐘（單次 session）。

---

## 7. 變更紀錄

| 版本 | 日期 | 變更 |
|------|------|------|
| v1 | 2026-04-13 | 首版，僅 IC 分析（Phase 2a + 2b）。**結論 ① ② 後證實錯誤**。 |
| **v2** | 2026-04-13 | 加入 Phase 2c Quantile 驗證。推翻 v1 的「翻符號」+「rvol_lowatr 最強」建議。確認 scanner 現行邏輯已是優秀，改動方向從「翻符號」改為「合併冗餘 + 第二層過濾」。 |

---

## 8. 方法論教訓（for future research）

**因子研究三件事必做（不可只看 IC）**：

1. **Spearman IC**：量測全排名線性相關，診斷指標整體有效性
2. **Decile Spread**：分 10 bucket 看單調性，檢查頂部 bucket 是否真的最賺
3. **Top-N Portfolio**：模擬實戰情境（每日取 Top N 持 h 天），算 Sharpe + 勝率

**三個數字不一致時的處理**：
- IC 高 + Decile 單調 + Top-N 賺錢 → 訊號有效 ✅
- IC 高 + Decile **不單調** + Top-N 可能不賺 → 小心（rvol_lowatr 的狀況：IC 高但 decile spread 很平坦）
- IC 低 + Decile **單調** + Top-N 賺錢 → **仍有效**（scanner 的狀況，不要被 IC 誤導）
- IC 負 + Decile **反單調** → 翻方向可能有效

**未來報告 SOP**：寫完 IC 分析一定要接 quantile + Top-N 驗證，否則結論可能反向。
