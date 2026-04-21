# VF-G3 Part 1: REGIME_EXIT_MULT 8-Multiplier Validation

Generated: 2026-04-21 17:16

## TL;DR

- **V1 (current)**:  mean=1.04% Sharpe=0.110  win=48.9%  sl_rate=32.1%
- **V2 (all 1.0)**:  mean=1.12% Sharpe=0.117  win=49.1%  sl_rate=32.1%
- **V3 (trending only)**: mean=1.11% Sharpe=0.116
- **V4 (volatile only)**: mean=1.04% Sharpe=0.111
- **V1 vs V2 delta**: mean -0.081%  Sharpe -0.0067
- **Grade: D** -- V1 vs V2 delta negligible (mean -0.08%, Sharpe -0.007). No regime-specific edge detected.
- **Recommendation**: `cut_regime_exit_mult`

## 0. Context (no-exit baseline)

Pure 20d hold: mean=1.40%, Sharpe=0.117. Any stop config (V1/V2/V3/V4) underperforms no-exit (consistent with VF-G1 finding: stop-loss is risk control, not alpha).

## 1. Test A: V1/V2/V3/V4 Comparison

### 1.1 Overall (full sample)

| version | n | mean | Sharpe | win | sl_rate | tp_rate | false_stop |
|---|---|---|---|---|---|---|---|
| V1_current | 4923 | 1.04% | 0.110 | 48.9% | 32.1% | 13.8% | 10.9% |
| V2_all_one | 4923 | 1.12% | 0.117 | 49.1% | 32.1% | 12.8% | 10.3% |
| V3_trending_only | 4923 | 1.11% | 0.116 | 48.9% | 32.8% | 12.3% | 10.8% |
| V4_volatile_only | 4923 | 1.04% | 0.111 | 49.2% | 31.4% | 14.3% | 10.3% |

### 1.2 Per-regime breakdown (V1 vs V2)

| regime | version | n | mean | Sharpe | win | sl_rate |
|---|---|---|---|---|---|---|
| trending | V1_current | 740 | -0.08% | -0.009 | 41.6% | 40.9% |
| trending | V2_all_one | 740 | -0.04% | -0.004 | 43.4% | 36.4% |
| ranging | V1_current | 1272 | 1.11% | 0.112 | 49.3% | 33.5% |
| ranging | V2_all_one | 1272 | 1.11% | 0.112 | 49.3% | 33.5% |
| volatile | V1_current | 1440 | 1.83% | 0.195 | 54.0% | 25.2% |
| volatile | V2_all_one | 1440 | 2.09% | 0.211 | 53.8% | 27.5% |
| neutral | V1_current | 1471 | 0.75% | 0.081 | 47.3% | 33.2% |
| neutral | V2_all_one | 1471 | 0.75% | 0.081 | 47.3% | 33.2% |

### 1.3 Per-regime V1 - V2 delta

| regime | delta_mean | delta_sharpe | verdict |
|---|---|---|---|
| trending | -0.043% | -0.0050 | neutral (within noise) |
| ranging | +0.000% | +0.0000 | neutral (within noise) |
| volatile | -0.255% | -0.0159 | marginal |
| neutral | +0.000% | +0.0000 | neutral (within noise) |

## 2. Test B: Per-regime (sl_mult x tp_mult) grid search

Grid per regime: 5 SL x 4 TP = 20 combos.

### Best per-regime (by Sharpe) vs V1 current vs V2 baseline

| regime | best_sl | best_tp | best_mean | best_Sharpe | V1_sl | V1_tp | V1_mean | V1_Sharpe | baseline(1,1)_mean | baseline_Sharpe |
|---|---|---|---|---|---|---|---|---|---|---|
| trending | 1.15 | 1.4 | 0.10% | 0.011 | 0.85 | 1.2 | -0.08% | -0.009 | -0.04% | -0.004 |
| ranging | 0.7 | 1.4 | 1.38% | 0.142 | 1.0 | 1.0 | 1.11% | 0.112 | 1.11% | 0.112 |
| volatile | 1.0 | 1.0 | 2.09% | 0.211 | 1.2 | 0.8 | nan% | nan | 2.09% | 0.211 |
| neutral | 0.7 | 1.4 | 0.91% | 0.100 | 1.0 | 1.0 | 0.75% | 0.081 | 0.75% | 0.081 |

### Per-regime interpretation

- **trending** (n=740): Sharpe range across 20 combos = 0.032; mean range = 0.26pp. has some structure
- **ranging** (n=1272): Sharpe range across 20 combos = 0.056; mean range = 0.57pp. has some structure
- **volatile** (n=1440): Sharpe range across 20 combos = 0.018; mean range = 0.40pp. FLAT -- no combo dominates
- **neutral** (n=1471): Sharpe range across 20 combos = 0.037; mean range = 0.35pp. has some structure

## 3. Test C: Walk-forward stability

Walk-forward not produced.

## 4. Decision & Action

- **Grade**: D
- **Reason**: V1 vs V2 delta negligible (mean -0.08%, Sharpe -0.007). No regime-specific edge detected.
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