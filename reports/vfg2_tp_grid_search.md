# VF-G2 QM Take-Profit Grid Search

Generated: 2026-04-17 13:25
Sample: 12797 picks, 256 weeks (2021-01-08 to 2025-12-26)
Horizon: fwd_40d  (fwd_40d_max/min used for TP/SL touch detection)

## TL;DR

- **Best combo**: tp1=0.3/tp2=0.4/tp3=0.6/floor=0.9/ceil=2.0  Sharpe=0.208  mean=4.20%  win=43.9%
- **Baseline (V1 current)**: tp1=0.15/tp2=0.25/tp3=0.4/floor=0.7/ceil=1.6  Sharpe=0.190  mean=3.17%  win=44.9%
- **Pure-hold (no SL/TP, fwd_40d)**: Sharpe=0.246  mean=5.84%  win=54.1%
- **SL-only (VF-G1 baseline, no TP)**: Sharpe=0.203  mean=4.55%  win=43.9%
- **Delta (best vs baseline)**: Sharpe +0.018  mean +1.03%
- **Delta (best vs pure-hold)**: Sharpe -0.038  mean -1.64%
- **Walk-forward stability**: best combo avg test_rank = 669.2/1125
- **Grade: D** -- pure-hold beats best TP: mean delta -1.64%, Sharpe -0.038. TP ladder destroys edge. -- recommend: `consider_removing_tp_or_keep_baseline`

## 0. Critical Context

**VF-G1 finding**: 4D SL grid was D-grade; pure-hold mean=2.84% beat any SL. This VF-G2 test asks the SAME question for TP: does adding 3-stage TP ladder help or hurt vs pure-hold + SL-only?

**Pessimistic simulation**: if SL triggers within 40d (fwd_40d_min <= sl_pct), the ENTIRE 3-tranche position exits at sl_pct. We cannot know intraday order between TP1 and SL, so we assume SL first (same as VF-G1). This UNDERSTATES the TP ladder benefit slightly, but is the honest grading.

**TP blending (if SL not hit)**: each of 3 tranches realizes independently. Tranche i pays min(tp_i_scaled, fwd_40d_max); if not reached, it pays fwd_40d (close-of-period).

## 1. Top-10 Combos (full sample, sorted by Sharpe)

| rank | tp1 | tp2 | tp3 | floor | ceil | Sharpe | mean | win | sl_rate | tp1_rate | tp3_rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.3 | 0.4 | 0.6 | 0.9 | 2.0 | 0.208 | 4.20% | 43.9% | 46.3% | 8.5% | 1.4% |
| 2 | 0.3 | 0.4 | 0.6 | 0.9 | 1.8 | 0.208 | 4.15% | 43.9% | 46.3% | 9.1% | 1.5% |
| 3 | 0.3 | 0.4 | 0.5 | 0.9 | 2.0 | 0.208 | 4.18% | 43.9% | 46.3% | 8.5% | 2.4% |
| 4 | 0.3 | 0.4 | 0.5 | 0.9 | 1.8 | 0.208 | 4.13% | 43.9% | 46.3% | 9.1% | 2.6% |
| 5 | 0.3 | 0.4 | 0.6 | 0.9 | 1.6 | 0.208 | 4.08% | 43.9% | 46.3% | 10.3% | 1.8% |
| 6 | 0.3 | 0.4 | 0.6 | 0.7 | 2.0 | 0.208 | 4.19% | 43.9% | 46.3% | 8.7% | 1.4% |
| 7 | 0.3 | 0.4 | 0.6 | 0.7 | 1.8 | 0.208 | 4.14% | 43.9% | 46.3% | 9.3% | 1.5% |
| 8 | 0.3 | 0.4 | 0.6 | 0.5 | 2.0 | 0.208 | 4.18% | 43.9% | 46.3% | 8.7% | 1.5% |
| 9 | 0.3 | 0.4 | 0.5 | 0.7 | 2.0 | 0.208 | 4.17% | 43.9% | 46.3% | 8.7% | 2.4% |
| 10 | 0.3 | 0.4 | 0.6 | 0.5 | 1.8 | 0.208 | 4.14% | 43.9% | 46.3% | 9.3% | 1.5% |

## 2. Baseline Ranking

Baseline V1 (tp1=0.15/tp2=0.25/tp3=0.4/floor=0.7/ceil=1.6) Sharpe = 0.190, ranked **#452/1125** (40.2%ile).

## 3. V1 vs V2 vs V3 (No-TP) vs Pure-Hold

| version | desc | Sharpe | mean | win | sl_rate |
|---|---|---|---|---|---|
| V1 baseline | tp1=0.15/tp2=0.25/tp3=0.4/floor=0.7/ceil=1.6 | 0.190 | 3.17% | 44.9% | 46.3% |
| V2 best grid | tp1=0.3/tp2=0.4/tp3=0.6/floor=0.9/ceil=2.0 | 0.208 | 4.20% | 43.9% | 46.3% |
| V3 SL-only (no TP) | SL at VF-G1 baseline, hold fwd_40d | 0.203 | 4.55% | 43.9% | 46.3% |
| V4 pure-hold | no SL, no TP, hold fwd_40d | 0.246 | 5.84% | 54.1% | - |

## 4. Best Combo vs Baseline by Regime

| combo | regime | n | Sharpe | mean | win | sl_rate | tp1_rate | tp3_rate |
|---|---|---|---|---|---|---|---|---|
| baseline | ALL | 12797 | 0.190 | 3.17% | 44.9% | 46.3% | 29.1% | 5.7% |
| baseline | volatile | 5349 | 0.250 | 4.24% | 48.7% | 42.0% | 29.9% | 5.4% |
| baseline | neutral | 2900 | 0.198 | 3.34% | 45.0% | 46.8% | 30.3% | 6.6% |
| baseline | ranging | 2650 | 0.132 | 2.15% | 41.8% | 49.0% | 28.1% | 5.4% |
| baseline | trending | 1898 | 0.082 | 1.30% | 38.6% | 54.3% | 26.0% | 5.4% |
| best | ALL | 12797 | 0.208 | 4.20% | 43.9% | 46.3% | 8.5% | 1.4% |
| best | volatile | 5349 | 0.261 | 5.37% | 47.9% | 42.0% | 7.8% | 1.3% |
| best | neutral | 2900 | 0.219 | 4.51% | 43.6% | 46.8% | 10.1% | 1.9% |
| best | ranging | 2650 | 0.152 | 2.98% | 40.6% | 49.0% | 8.3% | 1.2% |
| best | trending | 1898 | 0.112 | 2.11% | 37.6% | 54.3% | 8.6% | 1.3% |

## 5. Walk-Forward Summary

Windows: 61 (12 weeks train / 4 weeks test, stride 4)

Best combo WF: test_rank mean=669.2, median=680, std=524.3
Best combo lands in test top-5: 0/4 windows; top-20: 0/4 windows.

Baseline WF: test_rank mean=400.9, median=404, std=167.7

## 6. TP Scale Floor/Ceil Sensitivity (baseline tp1/tp2/tp3)

| floor | ceil | Sharpe | mean | win | tp1_rate |
|---|---|---|---|---|---|
| 0.5 | 1.2 | 0.174 | 2.66% | 45.7% | 34.4% |
| 0.5 | 1.4 | 0.184 | 2.95% | 45.2% | 31.5% |
| 0.5 | 1.6 | 0.190 | 3.17% | 44.9% | 29.3% |
| 0.5 | 1.8 | 0.194 | 3.31% | 44.7% | 27.7% |
| 0.5 | 2.0 | 0.195 | 3.39% | 44.6% | 26.7% |
| 0.7 | 1.2 | 0.174 | 2.66% | 45.7% | 34.1% |
| 0.7 | 1.4 | 0.184 | 2.95% | 45.2% | 31.2% |
| 0.7 | 1.6 | 0.190 | 3.17% | 44.9% | 29.1% |
| 0.7 | 1.8 | 0.194 | 3.31% | 44.7% | 27.5% |
| 0.7 | 2.0 | 0.195 | 3.39% | 44.6% | 26.5% |
| 0.9 | 1.2 | 0.174 | 2.68% | 45.7% | 33.8% |
| 0.9 | 1.4 | 0.184 | 2.97% | 45.2% | 30.9% |
| 0.9 | 1.6 | 0.191 | 3.18% | 44.9% | 28.8% |
| 0.9 | 1.8 | 0.194 | 3.32% | 44.7% | 27.2% |
| 0.9 | 2.0 | 0.196 | 3.40% | 44.6% | 26.2% |

## 7. Decision & Action

- **Grade**: D
- **Reason**: pure-hold beats best TP: mean delta -1.64%, Sharpe -0.038. TP ladder destroys edge.
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
