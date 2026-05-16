# Whale Picks v15 — Rank-Trigger Backtest Report

**Period**: 2021-01-01 ~ 2025-12-31

**Config**: BUY rank<=20 / SELL rank>30 / Stop loss -15%

**Methodology**: 日頻 composite_parsi rank → 進入 top-20 BUY；掉出 top-30 或 -15% drawdown SELL；
industry-neutral standardize / liquidity filter ≥ 10M TWD / 純價格報酬 (與 v13 一致 fair comparison)

## Performance vs v13 monthly baseline

| Metric | v13 monthly | v15 rank-trigger | Δ |
|---|---|---|---|
| Total return | 254.76% | 385.17% | +130.41pp |
| CAGR | 30.55% | 38.83% | +8.28pp |
| Sharpe | 1.519 | 1.947 | +0.428 |
| MDD | -12.44% | -21.97% | -9.53pp |
| Annual vol | 18.88% | 17.67% | — |

## Trade statistics (v15 only)

- **Total positions**: 2,384
- **Unique stocks**: 1,019
- **Still holding (data end)**: 37
- **Avg holding days**: 22.9 (median 14.0)
- **Win rate per position**: 49.2%
- **Avg PnL per position**: +2.27%
- **Best position**: +193.5%
- **Worst position**: -30.1%
- **Exit reasons**: {'rank_out': 2223, 'stop_loss': 124}

## Verdict

_Auto-fill below based on metric deltas..._
⚠️ **CONDITIONAL**: v15 Sharpe +0.43 有改善但 MDD -9.53% 風險加大，看 user 對 turnover 容忍度