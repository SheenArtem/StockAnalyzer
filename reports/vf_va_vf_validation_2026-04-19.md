# VF-V* Phase 1 驗證結果 — Value 選股因子 + 權重

**驗證日期**: 2026-04-19
**Universe**: 2400 stocks (VF-L1a 擴充後)
**Snapshot**: 70,760 rows × 309 weeks × 857 unique stocks (2020-2026)
**Forward horizon**: 60 days (除特別註明)

---

## VF-VA: 估值門檻驗證 (PE / PB / Graham)

### Layer 1 — Cross-sectional IC
| Factor | IC | IR | Grade |
|---|---|---|---|
| PE (lower=better) | 0.043 | **0.242** | **B** |
| PB (lower=better) | 0.024 | 0.153 | B |
| Graham ratio | 0.051 | **0.269** | **B** |

### Layer 2 — Decile Spread (cheap 10% vs expensive 10%)
- **PE +3.41% per 60d (winrate 63%)** ⭐ 最強
- PB: no spread
- Graham: +1.66%

### Layer 3 — Threshold (PE/PB)
- **PE < 12 pass ret 1.95% vs fail 1.18%** (+0.77%) ✓
- PE < 20 無效 (-0.22%)
- PB < 3 / 1.5 樣本太少，pass 反而 underperform

### VF-VA 結論
- ✅ PE / Graham 有 B 級 alpha，維持
- ⚠️ PE 門檻 20 → **12** (更嚴格更有 alpha)
- ❌ PB 絕對門檻無效 → 改用**百分位**加分
- 🎯 Action: 微調 `value_screener._score_valuation` 的 PE 門檻

---

## VF-VB: 體質門檻驗證 (F-Score / Z-Score)

### Layer 1 — IC
| Factor | IC | IR | Grade |
|---|---|---|---|
| **F-Score** | 0.097 | **0.892** | **A** ⭐ |
| Z-Score | -0.035 | **-0.271** | **B (反轉!)** |
| quality_score (combined) | 0.060 | 0.458 | A (弱) |

### Layer 2 — F-Score Threshold
| Threshold | PassRet | FailRet | Diff |
|---|---|---|---|
| F >= 5 | 6.09% | 1.22% | +4.87% |
| F >= 6 | 7.27% | 2.09% | +5.18% |
| **F >= 7** | **9.82%** | 2.79% | **+7.03%** ⭐ |
| **F >= 8** | **13.26%** | 3.77% | **+9.49%** ⭐⭐ |

### Layer 4 — F-Score Decile Spread
- **Top 10% F-Score: 10.53% per 60d**
- Bottom 10%: 1.07%
- **Spread +9.47% (winrate 79.6%)** ⭐⭐⭐ 超強

### Layer 3 — Z-Score Threshold
| Threshold | PassRet | FailRet | Diff |
|---|---|---|---|
| Z >= 1.8 | 4.66% | 3.77% | +0.89% |
| Z >= 2.6 | 3.66% | 4.12% | **-0.47%** |
| Z >= 3.0 | 2.96% | 4.18% | **-1.22%** |

### VF-VB 結論
- ✅ **F-Score 超強 A 級因子** (IR 0.892)，跟 QM 一樣
- ⚠️ **Z-Score 高分反而 underperform** (IR -0.271 B 反轉)
- 🎯 Action:
  - F-Score 門檻可從 ≥7 升到 ≥8（+9.49% alpha），但 Stage 2 樣本會少很多（僅 1663/70760 = 2.3%）
  - **Z-Score 加分邏輯錯誤**：目前「Z > 3 加分」應刪除或反轉
  - quality_score 整體 IR 0.458 OK，保留

---

## VF-VD: 技術轉折驗證 (RSI / RVOL / 52w)

### Layer 1 — IC
| Factor | IR | Grade | 解讀 |
|---|---|---|---|
| RSI 14 (lower=oversold) | **-0.225** | **B (rev)** | 超賣 → 未來 return LOWER |
| RVOL 20 (lower=量萎縮) | **-0.208** | **B (rev)** | 量萎縮 → 未來 return LOWER |
| low52w_prox (lower=近底部) | 0.165 | B | 矛盾訊號，decile 反向 |

### Layer 2 — Decile Spread
| Factor | Oversold ret | Normal ret | Spread |
|---|---|---|---|
| RSI | 2.81% | 6.51% | **-3.70%** |
| RVOL | 3.58% | 4.78% | -1.20% |
| 52w low | 1.43% | 7.38% | **-5.95%** |

### Layer 3 — Threshold
- **RSI < 30 超賣**: pass 3.83% vs fail 4.02% (-0.19%) — 無 alpha
- **RSI > 70 超買**: pass 7.33% vs fail 3.46% (+3.87%) — 反而有 alpha！
- **近 52w 低 <10%**: pass 1.19% vs fail 4.21% (-3.03%) — 近底部糟
- **近 52w 低 <20%**: pass 1.10% vs fail 4.54% (-3.45%) — 更糟

### VF-VD 結論 — ⚠️ 重大發現
**現行 Value 技術轉折加分邏輯全部反向**！
- `RSI < 30 加 +15` → **應刪除**（近底部反而糟）
- `RVOL < 0.5 加 +8` → **應刪除**（量萎縮反而糟）
- `近 52w 低 <10% 加 +12` → **應刪除或反轉**

真正 Value 選股**不該挑「看似超賣」的股票** — 那是價值陷阱！
Value = **「好公司 + 合理估值」**，不是 **「正在跌的股票」**。

🎯 Action: `value_screener._score_technical` 重寫方向，或技術轉折 15% 權重**重新分配到估值/體質**。

---

## VF-VF: 5 面向權重驗證

### 全樣本對比 (snapshot, 309 weeks)
| Scheme | Val/Q/R/T/SM | BasketRet | Sharpe | IR |
|---|---|---|---|---|
| V1 current | 30/25/15/15/15 | 5.83% | 0.420 | 0.478 |
| V2 equal | 20/20/20/20/20 | 5.45% | 0.400 | 0.351 |
| **V3 val_heavy** | **50/20/10/10/10** | **6.05%** | **0.448** | **0.645** |
| V4 qm_like | 0/50/30/20/0 | 5.35% | 0.385 | 0.319 |
| V5 quality | 20/40/20/15/5 | 5.63% | 0.402 | 0.414 |
| V6 no_sm | 35/30/15/20/0 | 5.78% | 0.417 | 0.482 |

### Walk-forward (12m train / 3m test, 19 slides)
| Scheme | Avg Ret | Avg Sharpe | Std Sharpe |
|---|---|---|---|
| V1 | 3.99% | 0.812 | 2.022 |
| **V3** | **4.36%** | **0.848** | 2.033 |
| V6 | 3.96% | 0.787 | 2.034 |

**V3 wins V1 in Sharpe: 11/19 slides = 57.9%**
**V3 wins V1 in basket_ret: 11/19 slides = 57.9%**

### VF-VF 結論
- 🟡 V3 prevail 在全樣本，但 walk-forward 只勝 **58%** slides → **MARGINAL**
- 平均優勢 (+0.37% per 3m) 太小，**不足以確信不是 overfit**
- 🎯 **暫維持 V1 (30/25/15/15/15) 不改權重**，等更多 universe/sample 再驗

---

## 跨 VF-V* 綜合行動清單（優先級排序）

### 🔴 P1 立即可落地（高信心）
1. **F-Score 單獨拆出作為高權重因子** — IR 0.892 A 級 (IC 1.37x of quality_score)
2. **刪除 Z-Score 加分邏輯**（目前 Z > 3 加分反 alpha）
3. **Value 技術轉折 15% 權重重新分配** — 已驗證反向，刪除或大幅降權

### 🟡 P2 需更多驗證
4. **PE 門檻從 20 改 12** — VF-VA 驗證有 +0.77% alpha，落地前先跑 walk-forward
5. **5 面向權重調整 V1 → V3** — walk-forward 只勝 58%，暫不動

### ⚪ P3 尚未驗證
6. **VF-VC 營收窗口**（12m vs 4 季）— 需擴充 simulator 資料
7. **VF-VE SmartMoney** — snapshot 用 placeholder 50，需 backfill 歷史 ETF / 法人資料
8. **VF-Value-ex1 DDM** — 動態折現率

---

## 數據檔案
- Snapshot: `data_cache/backtest/trade_journal_value_tw_snapshot.parquet`
- Journal: `data_cache/backtest/trade_journal_value_tw.parquet`
- Walk-forward CSV: `reports/vf_walkforward_result.csv`

## 工具
- `tools/value_historical_simulator.py` (with --save-snapshot)
- `tools/vf_va_validation.py` (估值)
- `tools/vf_vb_validation.py` (體質)
- `tools/vf_vd_validation.py` (技術)
- `tools/vf_vf_validation.py` (5 面向 overall)
- `tools/vf_walkforward.py` (跨期穩定性)
