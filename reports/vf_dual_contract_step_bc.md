# Dual × position_monitor 契約 Step B/C 驗證

**Date**: 2026-04-29
**Method**: targeted analyses on existing trade journals (full simulator deferred)

## Step C — Rule 6 dual-leg overlap

| Metric | Value |
|---|---:|
| Mean overlap % of top-20 | 5.2% |
| Max overlap % | 25.0% |
| Weeks with overlap >= 25% | 1 / 307 |
| Mean dual_naive return | +0.0229 |
| Mean Value 20-pick return | +0.0252 |
| Mean QM 20-pick return | +0.0206 |

**結論**: 重疊比例平均 5.2%，當前 dual_5050 backtest 把
重疊股各算一份（Value 50% + QM 50%），實際下單只下一份，Rule 6 所述。
若 cap 同股 5% 上限：權重變化但對等權平均報酬影響小（同股報酬一致）。
**主要影響**: 真實資金佔用比 < backtest 假設 → 真實年化報酬可能低估 0.3%
（每重疊一檔 backtest 多算 5% 曝險）。

## Step B1 — MIN_HOLD_DAYS dropout cost

| Hold scenario | Mean fwd return |
|---|---:|
| Exit @ 1mo (~20d) | +0.0426 |
| Force hold 2mo (~40d) | +0.0648 |
| Force hold 3mo (~60d) | +0.0939 |
| Cost of 40d vs 20d | +0.0222 |

**結論**: 強迫多持有掉榜股 1 個月的成本 = +2.22pp。
若為正，較長 MIN_HOLD 反而有利（dropout 後反彈）。

## Step B2 — TP 1/3 vs 1/2 split

| Strategy | Expected return |
|---|---:|
| No TP (hold to 60d) | +0.1955 |
| TP 1/3 at +10% | +0.1637 |
| TP 1/2 at +10% | +0.1478 |
| Diff (1/3 vs 1/2) | +0.0159 |
| n trades w/ TP1 hit | 2738 |

**結論**: TP 1/3 vs 1/2 期望值差 +1.59pp。
1/3 較佳: 留更多曝險享受 mean fwd_60d > +10%

## Step B3 — Whipsaw 30-day cooldown re-entry analysis

| Metric | Value |
|---|---:|
| Total re-entries within 30d | 258 |
| Mean re-entry fwd_20d | +0.0131 |
| Baseline (all top-20) fwd_20d | +0.0258 |
| Re-entry edge fwd_20d | -0.0128 |
| Mean re-entry fwd_60d | +0.0524 |
| Baseline fwd_60d | +0.0696 |
| Re-entry edge fwd_60d | -0.0172 |

**結論**: Re-entries 表現劣於 baseline → 30 天 ban 有保護作用，建議保留

## 落地建議

1. **Rule 6 (Step C)**: 確認 dual_5050 backtest 真實曝險低估，未來精算建議以 cap 5% per stock 重 simulate
2. **MIN_HOLD_DAYS (B1)**: 看 dropout cost 數據判斷是否從 20 降到 10 或維持
3. **TP split (B2)**: 看 1/3 vs 1/2 期望值差距決定
4. **Whipsaw 30d (B3)**: 看 re-entry 表現是否真的差於 baseline
