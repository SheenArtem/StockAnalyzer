# DCF Base MOS IC 驗證報告

資料: 662 (stock, fy_end) panels；universe top300 扣金融/公用後 274 candidate，含 5 FY (['2019-12-31', '2020-12-31', '2021-12-31', '2022-12-31', '2023-12-31'])

---

## Phase 1: Cross-sectional IC (Pearson + Spearman)

| FY end | n | Pearson IC | Spearman IC | Mean Fwd 252d Ret |
|---|---:|---:|---:|---:|
| 2019-12-31 | 118 | +0.0487 | +0.1000 | +98.85% |
| 2020-12-31 | 133 | -0.1318 | -0.0388 | +9.79% |
| 2021-12-31 | 142 | -0.0070 | +0.1365 | -0.30% |
| 2022-12-31 | 134 | -0.0130 | -0.0274 | +59.77% |
| 2023-12-31 | 135 | +0.2245 | +0.0849 | -11.25% |

**Overall IC**: Pearson **+0.0243** (IR=+0.19, grade **C**) | Spearman **+0.0510** (IR=+0.64, grade **A**)

## Phase 2: Decile spread (Base MOS deciles → fwd 252d ret)

| Decile | n | Mean MOS | Mean Fwd Ret | Median Fwd Ret |
|---:|---:|---:|---:|---:|
| Q1 | 67 | -64.51% | +12.47% | -0.58% |
| Q2 | 66 | -38.76% | +19.51% | -3.16% |
| Q3 | 66 | -13.48% | +29.63% | -0.26% |
| Q4 | 66 | +16.83% | +42.98% | +7.81% |
| Q5 | 66 | +47.49% | +23.71% | +4.35% |
| Q6 | 66 | +86.29% | +33.94% | +2.31% |
| Q7 | 66 | +144.83% | +33.76% | +11.73% |
| Q8 | 66 | +221.04% | +32.44% | +4.75% |
| Q9 | 66 | +347.33% | +29.69% | -0.63% |
| Q10 | 67 | +753.85% | +35.28% | +8.58% |

**Q10 - Q1 spread**: +22.81% | **Monotonicity (Spearman)**: +0.600

✅ 正向單調 — Base MOS 高（低估）→ 報酬高（符合 DCF 理論）

## Phase 3: 方案 A 過濾回測

策略：每年 FY-end+90d 進場，equal-weight, hold 252d。
Baseline = 全 universe；Filtered = Base MOS > threshold。

| FY entry (FY+90d) | n_universe | Baseline ret (eq-weight) | MOS>-20% n / ret | MOS>+0% n / ret | MOS>+20% n / ret |
|---|---:|---:||---:||---:||---:|
| 2019-12-31 | 118 | +98.85% | 91 / +107.97% | 86 / +103.93% | 79 / +102.72% |
| 2020-12-31 | 133 | +9.79% | 102 / +12.95% | 92 / +10.63% | 79 / +8.80% |
| 2021-12-31 | 142 | -0.30% | 102 / -3.29% | 89 / -2.34% | 76 / +1.05% |
| 2022-12-31 | 134 | +59.77% | 113 / +63.31% | 107 / +64.97% | 99 / +55.25% |
| 2023-12-31 | 135 | -11.25% | 102 / -10.46% | 94 / -11.44% | 86 / -11.99% |

**Yearly summary (mean across 5 FY)**:

| Strategy | Mean Ret | Stdev | Sharpe-like | n_avg/yr |
|---|---:|---:|---:|---:|
| Baseline | +31.37% | 46.49% | +0.67 | 132 |
| MOS>-20% | +34.10% | 50.35% | +0.68 | 102 |
| MOS>+0% | +33.15% | 49.44% | +0.67 | 94 |
| MOS>+20% | +31.17% | 47.35% | +0.66 | 84 |

---
## 最終 Verdict

### 方案 B (軟加分 — composite_score 加 MOS 維度)

- Pearson IC = +0.0243 (IR=+0.19) → **C**
- Spearman IC = +0.0510 (IR=+0.64) → **A**
- Decile Q10-Q1 spread = +22.81%, monotonicity = +0.600
- **落地建議**：弱訊號 C 級，shadow 觀察 1-2 季再決定

### 方案 A (硬過濾 — 強勢股池 + MOS > threshold)

- 所有 threshold 都無法擊敗 baseline Sharpe (best gain=+0.00)
- **落地建議**：**不上線**，過濾後 risk-adjusted 報酬反而變差或無提升

### 觀察重點 (Multi-bull bias 警示)

- 2022 FY (bear year): Pearson IC = -0.0130, Mean Fwd Ret = +59.77%
  ⚠️ Bear year 方向與整體相反 — 警惕 multi-bull bias