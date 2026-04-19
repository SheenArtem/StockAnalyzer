# VF-VA / VF-VF Validation — Value P1 驗證結果

**驗證日期**: 2026-04-19
**Universe**: 2400 stocks (VF-L1a 擴充後)
**樣本**: 309 週 × 70,760 Stage-1-passed snapshot (2020-01 ~ 2025-12)
**Forward horizon**: 60 days

## 資料基礎
- Universe 2400 stocks (VF-L1a backfill, 2015-2026 quality_scores)
- Snapshot 70,760 rows across 309 weeks, 857 unique stocks
- Journal 15,450 picks (top 50 per week), 486 unique stocks

## VF-VA: 估值門檻驗證

### Layer 1 — Cross-sectional IC (IR 決策)
| Factor | IC_mean | IR | Grade |
|---|---|---|---|
| PE (lower=better) | 0.043 | **0.242** | **B** |
| PB (lower=better) | 0.024 | 0.153 | B |
| Graham ratio | 0.051 | **0.269** | **B** |

**所有 3 個估值因子均達 B 級 IR（0.1-0.3），即「有效但弱」的 alpha。**

### Layer 2 — Decile Spread (便宜 10% vs 最貴 10%)
| Factor | 便宜 | 昂貴 | Spread | WinPct |
|---|---|---|---|---|
| **PE** | **4.06%** | 0.64% | **+3.41%** | **63.1%** |
| PB | 5.10% | 5.14% | -0.04% | 51.1% |
| Graham | 4.48% | 2.82% | +1.66% | 57.6% |

**PE decile spread 最強（+3.41% per 60d, 63% 勝率）**，PB 單獨看沒用。

### Layer 3 — 固定門檻壓力測試
| Threshold | #Pass | PassRet | FailRet | Diff |
|---|---|---|---|---|
| **PE < 12** | 6,089 | **1.95%** | 1.18% | **+0.77%** ✓ |
| PE < 20 | 15,801 | 1.20% | 1.42% | -0.22% ✗ |
| PB < 3 | **只 79 檔** | 0.29% | 4.02% | -3.73% ✗ |
| PB < 1.5 | 只 12 檔 | -1.47% | 4.02% | -5.49% ✗ |

### VF-VA 結論
1. **PE 20 門檻無效 → 建議降到 PE < 12**（+0.77% 每 60d）
2. **PB 3 / PB 1.5 門檻絕對值無意義**（樣本太少、且 passers underperform）
3. **Graham 門檻 22.5 整體有 alpha，維持**
4. 修 value_screener 建議：
   - `score_valuation` 裡 PE<20 加 +5 → 改成 PE<12 加 +10
   - PB 門檻改用**百分位**排名（e.g., PB 最低 20% 加分），不用絕對值

---

## VF-VF: 5 面向權重驗證

### 測試 6 種權重方案 (snapshot fair re-ranking)
| Scheme | Weights (Val/Q/Rev/Tech/SM) | BasketRet | Sharpe | IC | IR |
|---|---|---|---|---|---|
| V1 current | 30/25/15/15/15 | 5.83% | 0.420 | 0.063 | 0.478 |
| V2 equal | 20/20/20/20/20 | 5.45% | 0.400 | 0.046 | 0.351 |
| **V3 val_heavy** | **50/20/10/10/10** | **6.05%** | **0.448** | **0.084** | **0.645** |
| V4 qm_like | 0/50/30/20/0 | 5.35% | 0.385 | 0.041 | 0.319 |
| V5 quality | 20/40/20/15/5 | 5.63% | 0.402 | 0.054 | 0.414 |
| V6 no_sm | 35/30/15/20/0 | 5.78% | 0.417 | 0.063 | 0.482 |

### VF-VF 結論
1. **V3 val_heavy (50/20/10/10/10) 全面勝出**：
   - BasketRet +0.22% per 60d vs V1 (~1.3% 年化)
   - **IR +0.167 vs V1**（0.645 vs 0.478，+35% alpha 穩定性）
   - Sharpe +0.028（小但正向）
2. **V4 qm_like 最差**：把 Value 當 QM 跑（無 valuation weight）→ Sharpe 0.385
3. **V2 equal 也弱**：5 等分不是答案
4. **V6 no_sm 和 V1 近乎持平** → 15% smart_money 未證明有顯著 alpha（等 VF-VE 進一步驗）

### 決策建議（待 walk-forward 確認）
**候選改法：V1 (30/25/15/15/15) → V3 (50/20/10/10/10)**
- Val 30→50 加重估值（既 IC 最強）
- Q 25→20 小幅減
- Rev 15→10 小幅減
- Tech 15→10 小幅減
- SM 15→10 小幅減

**信心等級**：B 級（IR 差異顯著，但 Sharpe 只小勝；需 walk-forward 驗跨期穩定性避免 overfit）

---

## 整體 Value 策略解讀

1. **Value 本質靠估值**（V3 win 印證傳統 Value 哲學）— 估值因子 IR 最強
2. **Quality (F-Score) 重要但不該過重** — V5 quality-heavy 反而不如 V3
3. **Revenue / Technical / SmartMoney 都是輔助** — 單獨都小貢獻
4. **PE < 12 嚴格門檻比 PE < 20 寬鬆門檻更有 alpha**

## 下一步（未做）
- [ ] Walk-forward (12m train / 3m test) 確認 V3 跨期穩定
- [ ] VF-VB: Value quality 門檻 F-Score ≥ 7 是否最佳
- [ ] VF-VC: Revenue 窗口 12m vs 4 quarter 對比
- [ ] VF-VD: Technical 5 層門檻集中管理
- [ ] VF-VE: SmartMoney IC 驗證（若 < 0.1 則刪 SM）

## 檔案
- 資料：`data_cache/backtest/trade_journal_value_tw_snapshot.parquet` (70,760 rows)
- 工具：`tools/value_historical_simulator.py` / `vf_va_validation.py` / `vf_vf_validation.py`
