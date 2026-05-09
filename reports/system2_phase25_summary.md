# System 2 Phase 2.5 - Portfolio Gating Sim

**Backtest window**: 2008-05-16 -> 2026-05-08 (4416 trading days)
**Hold window per trigger**: 60 trading days
**OOS events used**: 46 (model trained walk-forward, no leakage)

## Policy comparison

| Policy | CAGR | Sharpe | MDD | Calmar | days_long | days_50% | days_cash | n_trig |
|---|---|---|---|---|---|---|---|---|
| A_BuyHold | 8.76% | 0.557 | -56.00% | 0.156 | 4416 | 0 | 0 | 0 |
| B_BaselineGate | 5.88% | 0.491 | -48.86% | 0.120 | 2297 | 2119 | 0 | 35 |
| C_System2_abs | 8.87% | 0.598 | -44.27% | 0.200 | 3928 | 427 | 61 | 42 |
| C2_System2_argmax | 8.81% | 0.623 | -30.22% | 0.292 | 3700 | 533 | 183 | 41 |
| C3_System2_rank | 8.37% | 0.559 | -56.00% | 0.150 | 3989 | 244 | 183 | 42 |
| D_SingleFeat | 10.61% | 0.729 | -54.14% | 0.196 | 3501 | 488 | 427 | 39 |

## Key comparisons (best-model = C2_System2_argmax)

- **Best model vs Baseline (B)**: Sharpe +0.132, MDD +18.64%, CAGR +2.93%
- **Best model vs Buy&Hold (A)**: Sharpe +0.066, MDD +25.78%, CAGR +0.06%
- **Best model vs Single Feat (D)**: Sharpe -0.107, MDD +23.92%, CAGR -1.79%

## Model variant comparison

| Variant | Sharpe | MDD | n_cash | n_50% | Description |
|---|---|---|---|---|---|
| C_System2_abs | 0.598 | -44.27% | 3 | 7 | P(C)>=0.6 cash |
| C2_System2_argmax | 0.623 | -30.22% | 5 | 10 | argmax class |
| C3_System2_rank | 0.559 | -56.00% | 4 | 4 | P(C) top-25% cash |

## SOP-12 verdict (best variant)

Best model variant: **C2_System2_argmax**

- Sharpe(model) > Sharpe(baseline): **PASS** (0.623 vs 0.491)
- MDD(model) > MDD(baseline) (less negative): **PASS** (-30.22% vs -48.86%)
- Sharpe(model) > Sharpe(best single feat): **FAIL** (0.623 vs 0.729)

**Overall: PARTIAL** -- informational tier (SOP-14) candidate; do not rebalance live.

