# System 3 Phase 3.4 - Portfolio Gating Sim (1w-1mo)

**Backtest**: 2011-07-26 -> 2026-05-07 (3617 days)
**Hold window per trigger**: 21 trading days

## Policy comparison

| Policy | CAGR | Sharpe | MDD | Calmar | days_full | days_50% | days_cash |
|---|---|---|---|---|---|---|---|
| A_BuyHold | 11.15% | 0.723 | -31.63% | 0.352 | 3617 | 0 | 0 |
| B_Always50 | 5.80% | 0.723 | -16.98% | 0.342 | 0 | 3617 | 0 |
| C1_CompositeTop5_10 | 8.88% | 0.734 | -23.55% | 0.377 | 2829 | 326 | 462 |
| C2_MdDist_Rank | 10.42% | 0.898 | -19.53% | 0.534 | 2419 | 560 | 638 |
| C3_AND_combo | 8.40% | 0.801 | -19.88% | 0.423 | 2019 | 938 | 660 |
| C4_OR_combo | 9.60% | 0.881 | -20.08% | 0.478 | 2112 | 823 | 682 |

## Best gated policy: **C2_MdDist_Rank**

- vs Buy & Hold: Sharpe +0.175 / MDD +12.10% / CAGR -0.72%
- vs Always 50%: Sharpe +0.175 / MDD -2.55% / CAGR +4.62%

## SOP-12 verdict

- best policy Sharpe > B&H Sharpe: **PASS**
- best policy MDD > B&H MDD (less neg): **PASS**
- composite (C1) Sharpe > single (C2) Sharpe: **FAIL** (0.734 vs 0.898)

