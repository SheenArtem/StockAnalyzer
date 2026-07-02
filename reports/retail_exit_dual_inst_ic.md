# 散戶撤退 + 法人進駐 兩腿籌碼複合訊號 — IC 驗證

**Verdict: D 級 → 該歸檔（最多 informational 觀察，不可接 Whale 下單）**

驗證日期 2026-06-29｜腳本 `tools/retail_exit_dual_inst_validation.py`｜期間 2021-04-16 ~ 2026-06-26

---

## TL;DR（先講結論）

1. **全市場**：複合訊號**沒有 alpha**。composite_z 短 horizon IC 微弱正（5d +0.008 / 10d +0.006）但 20d 以後歸零（+0.0015，p=0.30）。decile **非單調**（monotonicity 20d = −0.19）。事件型框架 A（融資減幅 top30% ∩ 雙法人同買）forward return **全 horizon 負 spread**（20d −0.47%，t=−6.0）。
2. **品質池內（F-Score≥6, PIT）**：composite_z IC 全 horizon 正且 t>2（40d +0.009 t=4.5），表面像 B 級。**但這是假象** — 控制實驗顯示 Top-20 訊號組合 Sharpe 1.36 **輸給**「整個品質池等權」Sharpe 1.48，也只比品質池內隨機選 20 檔（Sharpe ~1.26）高一點點。好數字全部來自 **F-Score 品質 + 2021-26 多頭的 pool beta**，訊號本身零增量。
3. **複合 vs 單腿**：複合**沒有增量價值**。Leg 1（融資減幅）在全市場/品質池的 decile 都是**強烈反向**（monotonicity −0.96 / −0.93，融資減「越多」報酬越「差」），複合分數把這條反向腿拉進來反而稀釋。單獨 Leg 2（雙法人買強度）品質池 Top-20 Sharpe 1.24，也是 pool beta。
4. **A 級門檻**：全 5 項全部不過。IC 全 < 0.05、event spread 為負、Top-N IR 對 0050 **全為負**（−0.23 ~ −0.64）。
5. **這格（品質池內）過去沒測過，現在測了** — 結論是：把這個籌碼複合放進 QM/Whale 品質池，**不會比單純持有品質池更好**。籌碼 timing 在台股 mid-cap 依舊不是 alpha 源，與 [[project_archived_strategies]] / chip_ic_matrix sync 負 IC 的歷史結論一致。

---

## 訊號定義（實作）

- **Leg 1 散戶撤退**：`retail_exit_20d = -(margin_balance.diff(20) / vol_20d_avg)`（融資減幅，越大=散戶下車越多）。複用 `chip_ic_analysis.py` 的 diff/vol_20d_avg 算法。
- **Leg 2 法人進駐**：`dual_sync = (外資5d sum>0) AND (投信5d sum>0)`；`dual_buy_strength = (外資5d+投信5d)/vol_20d_avg`；嚴格變體 `dual_consec3 = 外資&投信連續≥3日皆淨買`。
- **複合 (B)**：`composite_z = z(retail_exit_20d) + z(dual_buy_strength)`（每日截面標準化，兩腿皆須有值）。
- **複合 (A)**：候選 = {retail_exit_20d 當日 pct rank ≥ 0.70} ∩ {dual_sync==1}，比候選 vs 非候選 forward return。

### Bias 控制（誠實標注）
- **Look-ahead**：F-Score 用 quarterly `quality_scores.parquet`，join 時加 **+45 日 publication lag** + `merge_asof(direction=backward)`，季底資料只在公布後才可見。
- **Volume==0 凍結列**：標準化分母排除（`Volume>0` 才入 20d 均量），避免冷門股 RVOL 分母失真（見 memory `project_v0_frozen_rows`）。
- **Close 毒列**：`Close>0` 過濾（此版 ohlcv_tw 已清，0 列）。
- **ETF**：排除 `00` 開頭。
- **⚠️ Survivor bias（未完全消除）**：ohlcv_tw 為 survivor-leaning universe（下市股票在有資料期間自然 drop out，但 universe 名單本身仍偏存活者）。多頭年 + survivor 會讓**所有**池子報酬虛高，包含 benchmark。這是為何要看 **IR / spread / 控制實驗**而非絕對報酬。

---

## 結果 1：IC Matrix（Spearman 截面）

| pool | signal | 5d | 10d | 20d | 40d | 60d |
|---|---|---|---|---|---|---|
| **all** | composite_z | +0.0082\*(t5.1) | +0.0063\*(t4.2) | +0.0015(t1.0) | +0.0017(t1.2) | +0.0022(t1.6) |
| all | retail_exit_20d | +0.0095\* | +0.0097\* | +0.0084\* | +0.0063\* | +0.0014 |
| all | dual_buy_strength | +0.0028 | +0.0004 | **−0.0049\*** | −0.0038\* | −0.0025 |
| **quality** | composite_z | +0.0083\*(t4.0) | +0.0062\*(t3.1) | +0.0048\*(t2.6) | +0.0090\*(t4.5) | +0.0077\*(t3.8) |
| quality | retail_exit_20d | −0.0007 | −0.0031 | −0.0028 | +0.0007 | **−0.0043\*** |
| quality | dual_buy_strength | +0.0055\* | +0.0044\* | −0.0007 | +0.0036 | +0.0046\* |

\* p<0.05。**無一個 IC ≥ +0.05（A 級門檻）**。品質池 composite_z 全正但 magnitude 全 < 0.01（雜訊等級，|IC|>0.02 門檻都不過）。

---

## 結果 2：Decile Spread + 單調性（關鍵打臉）

| pool | signal | h | D1 | D10 | spread | monotonicity |
|---|---|---|---|---|---|---|
| all | composite_z | 20 | +0.95% | +0.94% | **−0.02%** | −0.19（非單調）|
| all | retail_exit_20d | 20 | +1.76% | +0.92% | **−0.84%** | **−0.96（強烈反向）** |
| all | dual_buy_strength | 20 | +0.72% | +1.04% | +0.32% | +0.32（弱）|
| quality | composite_z | 60 | +4.16% | +4.44% | +0.28% | −0.22（非單調）|
| quality | retail_exit_20d | 60 | +5.81% | +3.98% | **−1.83%** | **−0.84（反向）** |
| quality | dual_buy_strength | 60 | +3.62% | +5.06% | +1.44% | −0.01（無結構）|

- **composite_z spread ≈ 0 且非單調** → 純 ranking-tilt 假象，沒有真實截面結構。
- **retail_exit_20d 單調性 −0.84~−0.96 = 融資減越多、報酬越差**。這條腿方向與假設**相反**：融資大減常伴隨股價已跌（散戶被洗出非主動撤退），是 coincident/落後訊號，不是 leading alpha。把它放進複合是**負貢獻**。

---

## 結果 3：事件框架 A（融資減幅 top30% ∩ 雙法人同買）

| pool | h | n_cand | μ候選 | μ其餘 | spread | t | win% |
|---|---|---|---|---|---|---|---|
| all | 20 | 24,752 | +0.68% | +1.15% | **−0.47%** | −6.0 | 46.3% |
| all | 60 | 24,096 | +2.26% | +3.01% | **−0.75%** | −5.1 | 46.5% |
| quality | 20 | 8,724 | +1.54% | +2.01% | **−0.47%** | −3.2 | 47.5% |
| quality | 60 | 8,449 | +4.71% | +4.99% | −0.28% | −1.1(ns) | 51.0% |

**AND 候選集 forward return 顯著低於非候選**（全市場全 horizon 負且顯著）。品質池內 spread 仍負，60d 不顯著。框架 A **明確失敗**：這個組合不是「健康換手」，反而是「弱勢股」標記。

---

## 結果 4：Top-20 月 rebalance vs 0050（IR 全負）

| pool | signal | CAGR | Sharpe | MDD | 0050 Sharpe | **IR vs 0050** |
|---|---|---|---|---|---|---|
| all | composite_z | +10.5% | 0.80 | −17.1% | 1.13 | **−0.62** |
| all | retail_exit_20d | +14.1% | 1.21 | −9.3% | 1.13 | −0.52 |
| quality | composite_z | +17.9% | 1.36 | −10.8% | 1.13 | **−0.40** |
| quality | retail_exit_20d | +19.7% | 1.82 | −8.2% | 1.13 | −0.34 |
| quality | dual_buy_strength | +20.1% | 1.24 | −9.3% | 1.13 | −0.23 |

0050 同期 CAGR +26.6% / Sharpe 1.10。**所有組合 IR 對 0050 為負 = 不贏大盤**。

### 控制實驗（拆穿品質池假象）— 最重要
品質池內，用 fwd_20d 月度：

| 組合 | CAGR | Sharpe | MDD |
|---|---|---|---|
| **整個品質池等權（pool beta）** | **+22.8%** | **1.48** | −11.8% |
| Top-20 by composite_z（訊號）| +17.9% | 1.36 | −10.8% |
| 品質池內隨機 20 檔（20 seeds 平均）| — | ~1.26 | — |

**訊號 Sharpe（1.36）< 池等權（1.48），只略高於隨機（1.26）。** 品質池那組「好看」的 Sharpe 完全是 F-Score 品質 gate + 多頭 beta，**訊號零增量、甚至負貢獻**（把池子報酬從 1.48 拉低到 1.36）。

---

## 結果 5：分年 regime（composite_z）

| pool | h | 2021 | 2022(空) | 2023 | 2024(盤) | 2025 | 2026 |
|---|---|---|---|---|---|---|---|
| all | 60d IC | +0.009 | **−0.015** | +0.011 | +0.000 | −0.000 | +0.039 |
| quality | 60d IC | +0.024 | **−0.016** | +0.015 | +0.003 | +0.010 | +0.045 |

**2022 空頭年 IC 翻負**（win 38~39%），2024 盤整年趨近 0。正 IC 集中在 2021/2023/2025/2026 多頭段。**Regime-dependent，非穩健 alpha**（與 dual-inst 舊結論 `vf_chip_dual_inst_ic.md` 的 2024 regime fail 一致）。

---

## 嚴格變體 dual_consec3（外資投信連續≥3日同買）

品質池 IC：5d +0.005(t3.0) / 20d +0.009(t5.2) / 60d +0.011(t5.7)。t 顯著但 **|IC| 全 < 0.012**，與 dual_buy_strength 同級，仍遠低於 0.05。更嚴格條件沒救回 magnitude。

---

## 回答你的 5 個問題

1. **全市場有 alpha 嗎？** 沒有。composite_z 20d+ IC 歸零、decile 非單調、事件框架 A spread 全負（20d −0.47% t=−6.0）、Top-20 IR vs 0050 = −0.62。
2. **品質池內有 alpha 嗎？（重點格）** 表面 IC 全正 t>2，**但是假的**。控制實驗證明 Top-20 訊號 Sharpe 1.36 < 池等權 1.48 ≈ 隨機 1.26。訊號在品質池內**零增量**，好數字全是 pool beta。**這格過去沒測過，現在判定：無 alpha。**
3. **複合 vs 單腿增量？** 無，且為負。Leg 1（融資減幅）decile 強烈反向（−0.84~−0.96），是落後訊號；複合把它拉進來稀釋了 Leg 2。單獨 Leg 2 也只是 pool beta。
4. **過 A 級嗎？** 全部不過：IC < 0.05、event spread 負、decile 非單調/反向、Top-N IR 對 0050 全負。是 ranking-tilt + pool-beta + regime 三重假象。
5. **Verdict**：**D 級，該歸檔**。不可接 Whale 下單訊號。若硬要保留，只能掛 informational 觀察名單（且須明示「在品質池內不優於單純持有品質池」），但建議直接歸檔，避免重蹈 `chip_ic_matrix` sync 負 IC / dual-inst 輸 0050 的覆轍。

---

## 產出檔案
- `reports/retail_exit_dual_inst_ic.md`（本檔）
- `reports/retail_exit_dual_inst_ic_matrix.csv`
- `reports/retail_exit_dual_inst_decile.csv`
- `reports/retail_exit_dual_inst_event.csv`
- `reports/retail_exit_dual_inst_yearly.csv`
- `reports/retail_exit_dual_inst_portfolio.csv`
- 腳本 `tools/retail_exit_dual_inst_validation.py`

## 方法論註記
本研究的價值在於**先做控制實驗（pool-beta / random-N baseline）**，這正是過去 sync（全市場負 IC 誤判）與 dual-inst（事件池輸 0050）兩份研究漏掉的一步。品質池內 IC 全正且顯著很容易讓人誤判為 B 級可上線 — 但加上「訊號 vs 池等權」對照後立刻現形：**截面 IC 顯著 ≠ portfolio 有增量**，因為品質池本身已經是強 selection，任何在其中再排序的弱訊號都會繼承 pool beta 卻無法超越它。
