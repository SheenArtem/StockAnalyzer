# VF-G1 QM Stop-Loss 4-Parameter Grid Search

Generated: 2026-04-17 13:12

## TL;DR

- **Best combo (honest BE)**: stop=2.0/be=1.5/ceil=0.16/ma20=1.0  Sharpe=0.163  mean=1.86%  win=45.8%
- **Baseline** (3.0/3.0/0.14/1.2): Sharpe=0.157  mean=1.89%  win=49.6%
- **Delta (best - baseline)**: Sharpe +0.007  mean -0.03%
- **Walk-forward stability**: best combo avg test_rank = 274.3/480
- **Grade: D** -- best-vs-baseline delta insufficient (keep current) -- recommend: `keep_baseline`

## 0A. No-Exit Baseline (CRITICAL CONTEXT)

**Pure 20d hold (no SL, no TP) produces**: mean=2.84%, Sharpe=0.182, win=52.8%.

**Any stop configuration in this grid UNDERPERFORMS the no-exit baseline**: best-pess mean=1.86%, Sharpe=0.163. Delta vs no-exit: mean -0.98%, Sharpe -0.019.

**Interpretation**: QM picks have a positive 20d drift (~2.8% mean). Stop-losses at any reasonable level cut profitable paths short more often than they save losses, because top-300 momentum picks rarely sustain catastrophic drawdowns. The protective value of SL here is risk-management (tail protection) rather than expected-return enhancement.

## 0B. Simulation Caveats (READ FIRST)

**BE (break-even) simulation mode**: trade_journal only has fwd_20d_max / fwd_20d_min, not intraday paths. In ~19% of picks both a +5% gain AND -8% drawdown occur within 20d. The ORDER of these moves decides whether BE armed before SL hit.

- **Pessimistic BE (default, honest)**: assume SL hit first in ambiguous cases. Realized = sl_pct if SL touched, regardless of later BE arming. **This is the honest grading metric.**
- **Optimistic BE (upper-bound)**: assume BE armed before SL in ambiguous cases, realized = 0. Known to overstate edge by 0.9pp+ mean return. Provided only for sensitivity context.

**Upper-bound delta (optimistic BE)**: best=stop=2.0/be=1.5/ceil=0.14/ma20=1.0 Sharpe=0.272 mean=2.92%; gap vs pessimistic best = +1.06% (this gap is the BE simulation artifact, not real edge).

**ma20_mult (MA20_BREAK_ATR_MULT) caveat**: this parameter controls position_monitor daily MA20-break alert threshold, not initial SL placement. It cannot be evaluated from aggregated fwd_20d data (needs intraday path). Grid search confirmed its effect is identically zero across all rows. Grade it separately via daily OHLCV simulation if needed.

## 1. Top-10 Combos (full sample)

| rank | stop | be | ceil | ma20 | Sharpe | mean | win | sl_rate | tp_rate | false_stop |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2.0 | 2.0 | 0.16 | 1.0 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 2 | 2.0 | 2.0 | 0.16 | 1.2 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 3 | 2.0 | 2.0 | 0.16 | 1.5 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 4 | 2.0 | 2.0 | 0.16 | 1.8 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 5 | 2.0 | 3.0 | 0.16 | 1.0 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 6 | 2.0 | 3.0 | 0.16 | 1.2 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 7 | 2.0 | 3.0 | 0.16 | 1.5 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 8 | 2.0 | 3.0 | 0.16 | 1.8 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 9 | 2.0 | 2.5 | 0.16 | 1.8 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |
| 10 | 2.0 | 2.5 | 0.16 | 1.5 | 0.163 | 1.86% | 45.8% | 43.1% | 16.9% | 16.6% |

## 2. Baseline Ranking

Baseline (3.0/3.0/0.14/1.2) Sharpe = 0.157, ranked **#181/480**.  37.7%ile.

## 3. Best Combo vs Baseline by Regime

| combo | regime | n | Sharpe | mean | win | sl_rate |
|---|---|---|---|---|---|---|
| baseline | volatile | 5349 | 0.233 | 2.84% | 53.3% | 28.0% |
| baseline | neutral | 2900 | 0.141 | 1.72% | 49.0% | 32.4% |
| baseline | ranging | 2650 | 0.131 | 1.56% | 49.4% | 32.3% |
| baseline | trending | 1898 | -0.002 | -0.03% | 40.7% | 38.8% |
| baseline | ALL | 12797 | 0.157 | 1.89% | 49.6% | 31.5% |
| best_pess | volatile | 5349 | 0.234 | 2.73% | 49.7% | 38.5% |
| best_pess | neutral | 2900 | 0.144 | 1.64% | 44.6% | 44.9% |
| best_pess | ranging | 2650 | 0.151 | 1.69% | 46.0% | 43.4% |
| best_pess | trending | 1898 | 0.001 | 0.01% | 36.6% | 53.1% |
| best_pess | ALL | 12797 | 0.163 | 1.86% | 45.8% | 43.1% |
| best_opt | volatile | 5349 | 0.233 | 2.72% | 49.7% | 38.6% |
| best_opt | neutral | 2900 | 0.144 | 1.65% | 44.6% | 45.0% |
| best_opt | ranging | 2650 | 0.150 | 1.68% | 45.9% | 43.5% |
| best_opt | trending | 1898 | 0.001 | 0.01% | 36.6% | 53.1% |
| best_opt | ALL | 12797 | 0.163 | 1.86% | 45.8% | 43.2% |

## 4. Walk-Forward Summary

Windows: 61 (12 weeks train / 4 weeks test, stride 4)

Best combo WF: test_rank mean=274.3, median=261, std=138.9
Best combo lands in test top-5: 0/6 windows; top-20: 0/6 windows.

Baseline WF: test_rank mean=194.1, median=181

## 5. Hypothesis: Volatile Regime Needs Wider STOP?

Volatile subset (n=5349): best stop_mult = **2.0** (Sharpe=0.233); hypothesis 'volatile needs wider STOP (>3.5)' is NOT supported.

| stop_mult | Sharpe | mean | win | sl_rate |
|---|---|---|---|---|
| 2.0 | 0.233 | 2.72% | 49.7% | 38.6% |
| 2.5 | 0.230 | 2.77% | 52.0% | 32.4% |
| 3.0 | 0.233 | 2.84% | 53.3% | 28.0% |
| 3.5 | 0.230 | 2.82% | 53.7% | 25.5% |
| 4.0 | 0.227 | 2.80% | 53.8% | 24.3% |
| 4.5 | 0.226 | 2.79% | 54.0% | 23.6% |

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