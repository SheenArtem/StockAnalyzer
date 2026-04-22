# VF-G2 QM Take-Profit Grid Search

Generated: 2026-04-21 20:22
Sample: 4923 picks, 538 weeks (2015-07-03 to 2025-12-26)
Horizon: fwd_40d  (fwd_40d_max/min used for TP/SL touch detection)

## TL;DR

- **Best combo**: tp1=0.3/tp2=0.4/tp3=0.6/floor=0.9/ceil=2.0  Sharpe=0.169  mean=2.64%  win=43.5%
- **Baseline (V1 current)**: tp1=0.15/tp2=0.25/tp3=0.4/floor=0.7/ceil=1.6  Sharpe=0.150  mean=1.98%  win=44.0%
- **Pure-hold (no SL/TP, fwd_40d)**: Sharpe=0.167  mean=3.07%  win=52.0%
- **SL-only (VF-G1 baseline, no TP)**: Sharpe=0.169  mean=2.81%  win=43.5%
- **Delta (best vs baseline)**: Sharpe +0.019  mean +0.67%
- **Delta (best vs pure-hold)**: Sharpe +0.002  mean -0.42%
- **Grade: D** -- pure-hold beats best TP: mean delta -0.42%, Sharpe +0.002. TP ladder destroys edge. -- recommend: `consider_removing_tp_or_keep_baseline`

## TL;DR Update (10.5yr + by-year breakdown, 2026-04-21)

**Core finding**: TP ladder 在 3 個空頭年救命 +0.5-1.4pp，但救命幾乎全來自 SL（不是 TP）。

| Year | Market | baseline_TP | best_TP | SL_only | pure_hold | Winner |
|---|---|---|---|---|---|---|
| 2015 | bear | -3.64% | -3.74% | -3.74% | -5.05% | **SL 救 +1.3pp** |
| 2018 | trade war | -1.02% | -1.04% | -1.03% | -2.20% | **SL 救 +1.2pp** |
| 2022 | Fed bear | -1.98% | -2.11% | -2.13% | -2.58% | **SL 救 +0.5pp** |
| 2023 | bull | +3.67% | +4.17% | +4.51% | **+6.36%** | pure-hold +2.7pp |
| 2024 | bull | +3.47% | +4.58% | +4.63% | **+5.77%** | pure-hold +2.3pp |
| 2025 | bull | +2.64% | +4.58% | +5.26% | +3.77% | SL_only +1.5pp |
| ALL | mixed | +1.98% | +2.64% | +2.81% | **+3.07%** | pure-hold |

**Key insight**: `SL_only` ≈ `baseline_TP` ≈ `best_TP` in bear years → 三階 TP ladder 對空頭年 **邊際貢獻 < 0.2pp**。救命主因是 **SL hit**，不是 TP。

- Bear year takeaway: **SL keeps edge, TP doesn't add value**
- Bull year takeaway: **TP ladder 吃掉 2-2.7pp**，pure-hold 完勝
- Net verdict: **可安全砍 TP**（全樣本 delta pure-hold -0.43pp）；SL 必須保留（已於 VF-G1 10.5yr 驗證）
- Bear regime alpha 仍要靠 **VF-G4 regime filter**（volatile-only Sharpe 0.208 > full 0.117），不是 TP

詳細 by-year + delta table：`reports/vfg2_by_year.csv`

## 0. Critical Context

**VF-G1 finding**: 4D SL grid was D-grade; pure-hold mean=2.84% beat any SL. This VF-G2 test asks the SAME question for TP: does adding 3-stage TP ladder help or hurt vs pure-hold + SL-only?

**Pessimistic simulation**: if SL triggers within 40d (fwd_40d_min <= sl_pct), the ENTIRE 3-tranche position exits at sl_pct. We cannot know intraday order between TP1 and SL, so we assume SL first (same as VF-G1). This UNDERSTATES the TP ladder benefit slightly, but is the honest grading.

**TP blending (if SL not hit)**: each of 3 tranches realizes independently. Tranche i pays min(tp_i_scaled, fwd_40d_max); if not reached, it pays fwd_40d (close-of-period).

## 1. Top-10 Combos (full sample, sorted by Sharpe)

| rank | tp1 | tp2 | tp3 | floor | ceil | Sharpe | mean | win | sl_rate | tp1_rate | tp3_rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.3 | 0.4 | 0.6 | 0.9 | 2.0 | 0.169 | 2.64% | 43.5% | 46.1% | 6.3% | 1.1% |
| 2 | 0.3 | 0.4 | 0.5 | 0.9 | 2.0 | 0.169 | 2.64% | 43.5% | 46.1% | 6.3% | 1.8% |
| 3 | 0.3 | 0.4 | 0.6 | 0.9 | 1.8 | 0.169 | 2.62% | 43.5% | 46.1% | 6.5% | 1.1% |
| 4 | 0.3 | 0.4 | 0.5 | 0.9 | 1.8 | 0.169 | 2.61% | 43.5% | 46.1% | 6.5% | 2.0% |
| 5 | 0.3 | 0.4 | 0.6 | 0.9 | 1.6 | 0.168 | 2.59% | 43.5% | 46.1% | 7.0% | 1.2% |
| 6 | 0.3 | 0.4 | 0.6 | 0.7 | 2.0 | 0.168 | 2.63% | 43.5% | 46.1% | 6.7% | 1.2% |
| 7 | 0.3 | 0.4 | 0.5 | 0.9 | 1.6 | 0.168 | 2.57% | 43.5% | 46.1% | 7.0% | 2.3% |
| 8 | 0.3 | 0.4 | 0.6 | 0.7 | 1.8 | 0.168 | 2.61% | 43.5% | 46.1% | 6.9% | 1.2% |
| 9 | 0.3 | 0.4 | 0.5 | 0.7 | 2.0 | 0.168 | 2.62% | 43.5% | 46.1% | 6.7% | 2.0% |
| 10 | 0.3 | 0.4 | 0.6 | 0.7 | 1.6 | 0.168 | 2.57% | 43.5% | 46.1% | 7.4% | 1.3% |

## 2. Baseline Ranking

Baseline V1 (tp1=0.15/tp2=0.25/tp3=0.4/floor=0.7/ceil=1.6) Sharpe = 0.150, ranked **#475/1125** (42.2%ile).

## 3. V1 vs V2 vs V3 (No-TP) vs Pure-Hold

| version | desc | Sharpe | mean | win | sl_rate |
|---|---|---|---|---|---|
| V1 baseline | tp1=0.15/tp2=0.25/tp3=0.4/floor=0.7/ceil=1.6 | 0.150 | 1.98% | 44.0% | 46.1% |
| V2 best grid | tp1=0.3/tp2=0.4/tp3=0.6/floor=0.9/ceil=2.0 | 0.169 | 2.64% | 43.5% | 46.1% |
| V3 SL-only (no TP) | SL at VF-G1 baseline, hold fwd_40d | 0.169 | 2.81% | 43.5% | 46.1% |
| V4 pure-hold | no SL, no TP, hold fwd_40d | 0.167 | 3.07% | 52.0% | - |

## 4. Best Combo vs Baseline by Regime

| combo | regime | n | Sharpe | mean | win | sl_rate | tp1_rate | tp3_rate |
|---|---|---|---|---|---|---|---|---|
| baseline | ALL | 4923 | 0.150 | 1.98% | 44.0% | 46.1% | 23.0% | 4.0% |
| baseline | neutral | 1471 | 0.124 | 1.54% | 44.3% | 46.8% | 21.5% | 3.3% |
| baseline | ranging | 1272 | 0.147 | 2.03% | 43.3% | 47.4% | 25.2% | 4.7% |
| baseline | volatile | 1440 | 0.216 | 2.98% | 47.3% | 41.4% | 24.1% | 4.0% |
| baseline | trending | 740 | 0.067 | 0.81% | 38.2% | 51.8% | 20.4% | 4.1% |
| best | ALL | 4923 | 0.169 | 2.64% | 43.5% | 46.1% | 6.3% | 1.1% |
| best | neutral | 1471 | 0.143 | 2.11% | 43.8% | 46.8% | 5.1% | 0.7% |
| best | ranging | 1272 | 0.169 | 2.79% | 42.5% | 47.4% | 7.7% | 1.6% |
| best | volatile | 1440 | 0.224 | 3.70% | 47.0% | 41.4% | 5.9% | 1.2% |
| best | trending | 740 | 0.100 | 1.40% | 37.7% | 51.8% | 7.2% | 0.7% |

## 5. Walk-Forward Summary

Walk-forward not produced.

## 6. TP Scale Floor/Ceil Sensitivity (baseline tp1/tp2/tp3)

| floor | ceil | Sharpe | mean | win | tp1_rate |
|---|---|---|---|---|---|
| 0.5 | 1.2 | 0.140 | 1.73% | 44.4% | 27.1% |
| 0.5 | 1.4 | 0.146 | 1.88% | 44.1% | 25.7% |
| 0.5 | 1.6 | 0.150 | 1.96% | 44.0% | 24.6% |
| 0.5 | 1.8 | 0.150 | 2.01% | 43.9% | 23.7% |
| 0.5 | 2.0 | 0.152 | 2.06% | 43.9% | 23.3% |
| 0.7 | 1.2 | 0.141 | 1.75% | 44.3% | 25.5% |
| 0.7 | 1.4 | 0.147 | 1.89% | 44.1% | 24.1% |
| 0.7 | 1.6 | 0.150 | 1.98% | 44.0% | 23.0% |
| 0.7 | 1.8 | 0.151 | 2.02% | 43.9% | 22.1% |
| 0.7 | 2.0 | 0.153 | 2.07% | 43.9% | 21.7% |
| 0.9 | 1.2 | 0.142 | 1.77% | 44.3% | 24.2% |
| 0.9 | 1.4 | 0.148 | 1.91% | 44.0% | 22.8% |
| 0.9 | 1.6 | 0.151 | 2.00% | 43.9% | 21.7% |
| 0.9 | 1.8 | 0.152 | 2.04% | 43.9% | 20.8% |
| 0.9 | 2.0 | 0.154 | 2.09% | 43.9% | 20.4% |

## 7. Decision & Action

- **Grade**: D
- **Reason**: pure-hold beats best TP: mean delta -0.42%, Sharpe +0.002. TP ladder destroys edge.
- **Recommendation**: `consider_removing_tp_or_keep_baseline`

**Pure-hold beats best TP ladder**. Options:
1. Remove 3-stage TP entirely; hold until 40d close or trailing stop.
2. Replace with single-stage 40% target + trailing.
3. Keep baseline for psychological/risk-mgmt reasons but acknowledge no alpha.

**Recommended code change** (exit_manager.py):
```python
# If removing TP entirely:
DEFAULT_TP_PCTS = ()  # empty tuple disables staged TP
# TP block in compute_exit_plan() should early-return empty tp_levels
```


## 8. Files

- Full grid: `vfg2_tp_grid_full.csv`
- Walk-forward: `vfg2_walkforward.csv`
- By regime: `vfg2_by_regime.csv`
