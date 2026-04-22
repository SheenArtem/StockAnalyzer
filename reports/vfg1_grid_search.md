# VF-G1 QM Stop-Loss 4-Parameter Grid Search

Generated: 2026-04-21 20:25

## TL;DR

- **Best combo (honest BE)**: stop=2.0/be=1.5/ceil=0.10/ma20=1.0  Sharpe=0.127  mean=1.14%  win=46.5%
- **Baseline** (3.0/3.0/0.14/1.2): Sharpe=0.117  mean=1.12%  win=49.1%
- **Delta (best - baseline)**: Sharpe +0.010  mean +0.03%
- **Walk-forward stability**: best combo avg test_rank = 263.5/480
- **Grade: D** -- best-vs-baseline delta insufficient (keep current) -- recommend: `keep_baseline`

## 0A. No-Exit Baseline (CRITICAL CONTEXT)

**Pure 20d hold (no SL, no TP) produces**: mean=1.40%, Sharpe=0.117, win=52.2%.

**Any stop configuration in this grid UNDERPERFORMS the no-exit baseline**: best-pess mean=1.14%, Sharpe=0.127. Delta vs no-exit: mean -0.25%, Sharpe +0.010.

**Interpretation**: QM picks have a positive 20d drift (~2.8% mean). Stop-losses at any reasonable level cut profitable paths short more often than they save losses, because top-300 momentum picks rarely sustain catastrophic drawdowns. The protective value of SL here is risk-management (tail protection) rather than expected-return enhancement.

## 0B. Simulation Caveats (READ FIRST)

**BE (break-even) simulation mode**: trade_journal only has fwd_20d_max / fwd_20d_min, not intraday paths. In ~19% of picks both a +5% gain AND -8% drawdown occur within 20d. The ORDER of these moves decides whether BE armed before SL hit.

- **Pessimistic BE (default, honest)**: assume SL hit first in ambiguous cases. Realized = sl_pct if SL touched, regardless of later BE arming. **This is the honest grading metric.**
- **Optimistic BE (upper-bound)**: assume BE armed before SL in ambiguous cases, realized = 0. Known to overstate edge by 0.9pp+ mean return. Provided only for sensitivity context.

**Upper-bound delta (optimistic BE)**: best=stop=2.0/be=1.5/ceil=0.16/ma20=1.0 Sharpe=0.206 mean=1.79%; gap vs pessimistic best = +0.64% (this gap is the BE simulation artifact, not real edge).

**ma20_mult (MA20_BREAK_ATR_MULT) caveat**: this parameter controls position_monitor daily MA20-break alert threshold, not initial SL placement. It cannot be evaluated from aggregated fwd_20d data (needs intraday path). Grid search confirmed its effect is identically zero across all rows. Grade it separately via daily OHLCV simulation if needed.

## 1. Top-10 Combos (full sample)

| rank | stop | be | ceil | ma20 | Sharpe | mean | win | sl_rate | tp_rate | false_stop |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2.0 | 2.0 | 0.10 | 1.0 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 2 | 2.0 | 2.0 | 0.10 | 1.2 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 3 | 2.0 | 2.0 | 0.10 | 1.5 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 4 | 2.0 | 2.0 | 0.10 | 1.8 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 5 | 2.0 | 2.5 | 0.10 | 1.8 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 6 | 2.0 | 2.5 | 0.10 | 1.5 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 7 | 2.0 | 2.5 | 0.10 | 1.2 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 8 | 2.0 | 2.5 | 0.10 | 1.0 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 9 | 2.0 | 1.5 | 0.10 | 1.8 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |
| 10 | 2.0 | 1.5 | 0.10 | 1.5 | 0.127 | 1.14% | 46.5% | 40.5% | 12.5% | 14.5% |

## 2. Baseline Ranking

Baseline (3.0/3.0/0.14/1.2) Sharpe = 0.117, ranked **#221/480**.  46.0%ile.

## 3. Best Combo vs Baseline by Regime

| combo | regime | n | Sharpe | mean | win | sl_rate |
|---|---|---|---|---|---|---|
| baseline | neutral | 1471 | 0.081 | 0.75% | 47.3% | 33.2% |
| baseline | ranging | 1272 | 0.112 | 1.11% | 49.3% | 33.5% |
| baseline | volatile | 1440 | 0.211 | 2.09% | 53.8% | 27.5% |
| baseline | trending | 740 | -0.004 | -0.04% | 43.4% | 36.4% |
| baseline | ALL | 4923 | 0.117 | 1.12% | 49.1% | 32.1% |
| best_pess | neutral | 1471 | 0.094 | 0.83% | 45.1% | 41.7% |
| best_pess | ranging | 1272 | 0.122 | 1.14% | 46.4% | 42.0% |
| best_pess | volatile | 1440 | 0.217 | 2.05% | 51.2% | 35.8% |
| best_pess | trending | 740 | 0.003 | 0.02% | 40.4% | 45.0% |
| best_pess | ALL | 4923 | 0.127 | 1.14% | 46.5% | 40.5% |
| best_opt | neutral | 1471 | 0.094 | 0.83% | 45.3% | 41.3% |
| best_opt | ranging | 1272 | 0.123 | 1.16% | 46.7% | 40.7% |
| best_opt | volatile | 1440 | 0.215 | 2.05% | 51.4% | 34.7% |
| best_opt | trending | 740 | 0.002 | 0.02% | 40.5% | 44.7% |
| best_opt | ALL | 4923 | 0.127 | 1.15% | 46.7% | 39.7% |

## 4. Walk-Forward Summary

Windows: 79 (12 weeks train / 4 weeks test, stride 4)

Best combo WF: test_rank mean=263.5, median=381, std=181.2
Best combo lands in test top-5: 1/8 windows; top-20: 1/8 windows.

Baseline WF: test_rank mean=194.2, median=181

## 5. Hypothesis: Volatile Regime Needs Wider STOP?

Volatile subset (n=1440): best stop_mult = **2.0** (Sharpe=0.216); hypothesis 'volatile needs wider STOP (>3.5)' is NOT supported.

| stop_mult | Sharpe | mean | win | sl_rate |
|---|---|---|---|---|
| 2.0 | 0.216 | 2.06% | 51.4% | 34.7% |
| 2.5 | 0.209 | 2.05% | 52.8% | 30.8% |
| 3.0 | 0.211 | 2.09% | 53.8% | 27.5% |
| 3.5 | 0.205 | 2.05% | 53.9% | 26.0% |
| 4.0 | 0.199 | 2.00% | 53.9% | 25.3% |
| 4.5 | 0.197 | 1.99% | 54.0% | 24.3% |

## 6. Decision & Action

- **Grade**: D
- **Reason**: best-vs-baseline delta insufficient (keep current)
- **Recommendation**: `keep_baseline`

No change. Baseline params remain optimal or grid failed to improve meaningfully.

## 7. Files

- Full grid: `vfg1_grid_search_full.csv`
- Walk-forward: `vfg1_walkforward.csv`
- By regime: `vfg1_by_regime.csv`
- Heatmaps: `vfg1_heatmaps/*.png`