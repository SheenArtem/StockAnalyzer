# VF-G3 Part 1: REGIME_EXIT_MULT 8-Multiplier Validation

Generated: 2026-04-17 13:18

## TL;DR

- **V1 (current)**:  mean=1.77% Sharpe=0.151  win=49.8%  sl_rate=31.2%
- **V2 (all 1.0)**:  mean=1.89% Sharpe=0.157  win=49.6%  sl_rate=31.5%
- **V3 (trending only)**: mean=1.91% Sharpe=0.159
- **V4 (volatile only)**: mean=1.75% Sharpe=0.149
- **V1 vs V2 delta**: mean -0.127%  Sharpe -0.0061
- **Walk-forward**: V1 beats V2 on test in 17/61 windows (28%)
- **Grade: D** -- V1 vs V2 delta negligible (mean -0.13%, Sharpe -0.006). No regime-specific edge detected.
- **Recommendation**: `cut_regime_exit_mult`

## 0. Context (no-exit baseline)

Pure 20d hold: mean=2.84%, Sharpe=0.182. Any stop config (V1/V2/V3/V4) underperforms no-exit (consistent with VF-G1 finding: stop-loss is risk control, not alpha).

## 1. Test A: V1/V2/V3/V4 Comparison

### 1.1 Overall (full sample)

| version | n | mean | Sharpe | win | sl_rate | tp_rate | false_stop |
|---|---|---|---|---|---|---|---|
| V1_current | 12797 | 1.77% | 0.151 | 49.8% | 31.2% | 20.2% | 10.9% |
| V2_all_one | 12797 | 1.89% | 0.157 | 49.6% | 31.5% | 17.5% | 10.9% |
| V3_trending_only | 12797 | 1.91% | 0.159 | 49.4% | 32.5% | 17.2% | 11.2% |
| V4_volatile_only | 12797 | 1.75% | 0.149 | 50.0% | 30.2% | 20.6% | 10.6% |

### 1.2 Per-regime breakdown (V1 vs V2)

| regime | version | n | mean | Sharpe | win | sl_rate |
|---|---|---|---|---|---|---|
| trending | V1_current | 1898 | 0.08% | 0.007 | 39.0% | 45.4% |
| trending | V2_all_one | 1898 | -0.03% | -0.002 | 40.7% | 38.8% |
| ranging | V1_current | 2650 | 1.56% | 0.131 | 49.4% | 32.3% |
| ranging | V2_all_one | 2650 | 1.56% | 0.131 | 49.4% | 32.3% |
| volatile | V1_current | 5349 | 2.49% | 0.218 | 54.3% | 25.0% |
| volatile | V2_all_one | 5349 | 2.84% | 0.233 | 53.3% | 28.0% |
| neutral | V1_current | 2900 | 1.72% | 0.141 | 49.0% | 32.4% |
| neutral | V2_all_one | 2900 | 1.72% | 0.141 | 49.0% | 32.4% |

### 1.3 Per-regime V1 - V2 delta

| regime | delta_mean | delta_sharpe | verdict |
|---|---|---|---|
| trending | +0.109% | +0.0095 | neutral (within noise) |
| ranging | +0.000% | +0.0000 | neutral (within noise) |
| volatile | -0.343% | -0.0146 | marginal |
| neutral | +0.000% | +0.0000 | neutral (within noise) |

## 2. Test B: Per-regime (sl_mult x tp_mult) grid search

Grid per regime: 5 SL x 4 TP = 20 combos.

### Best per-regime (by Sharpe) vs V1 current vs V2 baseline

| regime | best_sl | best_tp | best_mean | best_Sharpe | V1_sl | V1_tp | V1_mean | V1_Sharpe | baseline(1,1)_mean | baseline_Sharpe |
|---|---|---|---|---|---|---|---|---|---|---|
| trending | 0.7 | 1.4 | 0.14% | 0.013 | 0.85 | 1.2 | 0.08% | 0.007 | -0.03% | -0.002 |
| ranging | 0.7 | 1.4 | 1.75% | 0.155 | 1.0 | 1.0 | 1.56% | 0.131 | 1.56% | 0.131 |
| volatile | 1.0 | 1.4 | 2.91% | 0.236 | 1.2 | 0.8 | nan% | nan | 2.84% | 0.233 |
| neutral | 1.3 | 1.4 | 1.86% | 0.149 | 1.0 | 1.0 | 1.72% | 0.141 | 1.72% | 0.141 |

### Per-regime interpretation

- **trending** (n=1898): Sharpe range across 20 combos = 0.036; mean range = 0.40pp. has some structure
- **ranging** (n=2650): Sharpe range across 20 combos = 0.048; mean range = 0.54pp. has some structure
- **volatile** (n=5349): Sharpe range across 20 combos = 0.030; mean range = 0.72pp. has some structure
- **neutral** (n=2900): Sharpe range across 20 combos = 0.028; mean range = 0.60pp. has some structure

## 3. Test C: Walk-forward stability

Windows: 61 (12w train / 4w test, stride 4).

### Version mean/Sharpe across WF test windows

| version | test_mean avg | test_mean median | test_Sharpe avg | test_Sharpe median |
|---|---|---|---|---|
| V1_current | 1.56% | 2.11% | 0.106 | 0.168 |
| V2_all_one | 1.69% | 2.33% | 0.105 | 0.207 |
| V3_trending_only | 1.70% | 2.33% | 0.105 | 0.207 |
| V4_volatile_only | 1.54% | 2.11% | 0.107 | 0.168 |

**V1 - V2 diff across 61 windows**: mean_diff avg = -0.131%, median = -0.057%; Sharpe_diff avg = +0.0011, median = +0.0000.
V1 beats V2 on test_mean in 17/61 windows.

## 4. Decision & Action

- **Grade**: D
- **Reason**: V1 vs V2 delta negligible (mean -0.13%, Sharpe -0.006). No regime-specific edge detected.
- **Recommendation**: `cut_regime_exit_mult`

### Proposed code diff (DO NOT apply automatically)

**File: `exit_manager.py`**

1. Delete REGIME_EXIT_MULT constant (lines 62-72).

2. Remove Phase 4 regime overlay in `compute_exit_plan`:

```python
# DELETE (lines 117-118, 131-132, 156-157):
#   sl_mult, tp_mult = REGIME_EXIT_MULT.get(regime, (1.0, 1.0))
#   stop_pct = np.clip(stop_pct * sl_mult, ATR_STOP_FLOOR, ATR_STOP_CEIL)
#   tp_scale = np.clip(tp_scale * tp_mult, ATR_TP_SCALE_FLOOR, ATR_TP_SCALE_CEIL)

# Keep return keys but hardcode regime_sl_mult=1.0, regime_tp_mult=1.0
# so callers relying on these fields still work.
```
3. Optionally keep `regime` parameter in signature for forward-compat; 
   or remove entirely after grep confirms no caller relies on it.

**Rationale**: per-regime multipliers add code complexity (12 mult applications 
across compute_exit_plan + tests) without measurable edge. Consistent with VF-G1 
finding that the stop-loss parameter space is a plateau and tail-risk is the 
dominant role of SL, not regime-adaptive alpha.

## 5. Files

- Versions table: `vfg3_part1_versions.csv`
- Per-regime grid: `vfg3_part1_by_regime.csv`
- Walk-forward:   `vfg3_part1_walkforward.csv`