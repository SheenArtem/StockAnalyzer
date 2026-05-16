# Whale Picks v15 — Rank-Trigger Backtest Report

**Period**: 2021-01-01 ~ 2025-12-31

**Config**: BUY rank<=15 / SELL rank>25 / Stop loss -10%

**Methodology**: 日頻 composite_parsi rank → 進入 top-20 BUY；掉出 top-30 或 -15% drawdown SELL；
industry-neutral standardize / liquidity filter ≥ 10M TWD / 純價格報酬 (與 v13 一致 fair comparison)

## Performance vs v13 monthly baseline

| Metric | v13 monthly | v15 rank-trigger | Δ |
|---|---|---|---|
| Total return | 254.76% | 285.44% | +30.68pp |
| CAGR | 30.55% | 32.35% | +1.80pp |
| Sharpe | 1.519 | 1.374 | -0.145 |
| MDD | -12.44% | -31.66% | -19.22pp |
| Annual vol | 18.88% | 22.22% | — |

## Trade statistics (v15 only)

- **Total positions**: 1,953
- **Unique stocks**: 898
- **Still holding (data end)**: 30
- **Avg holding days**: 21.6 (median 13.0)
- **Win rate per position**: 49.7%
- **Avg PnL per position**: +2.44%
- **Best position**: +193.5%
- **Worst position**: -18.9%
- **Exit reasons**: {'rank_out': 1702, 'stop_loss': 221}

## Verdict

_Auto-fill below based on metric deltas..._
❌ **REJECT**: v15 Sharpe -0.14 不夠 + MDD -19.22%，保 v13 monthly