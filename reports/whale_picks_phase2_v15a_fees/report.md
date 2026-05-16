# Whale Picks v15 — Rank-Trigger Backtest Report

**Period**: 2021-01-01 ~ 2025-12-31

**Config**: BUY rank<=20 / SELL rank>30 / Stop loss -15%

**Methodology**: 日頻 composite_parsi rank → 進入 top-20 BUY；掉出 top-30 或 -15% drawdown SELL；
industry-neutral standardize / liquidity filter ≥ 10M TWD / 純價格報酬 (與 v13 一致 fair comparison)

## Performance vs v13 monthly baseline

| Metric | v13 monthly | v15 rank-trigger | Δ |
|---|---|---|---|
| Total return | 254.76% | 219.04% | -35.72pp |
| CAGR | 30.55% | 27.25% | -3.30pp |
| Sharpe | 1.519 | 1.216 | -0.303 |
| MDD | -12.44% | -34.10% | -21.66pp |
| Annual vol | 18.88% | 21.79% | — |

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
❌ **REJECT**: v15 Sharpe -0.30 不夠 + MDD -21.66%，保 v13 monthly