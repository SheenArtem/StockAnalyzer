# Value Portfolio Backtest (2020-2025)

**Date**: 2020-01-03 -> 2025-12-26

**Spec**: Top-5 equal-weight, rebalance every 4 weeks, Stage 1 (PE<=12 / PB<=3.0 / Graham<=22.5 / TV>=30M), weights {'val': 0.3, 'quality': 0.25, 'revenue': 0.3, 'technical': 0.15, 'sm': 0.0}

**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)

## Value Top-5

| Metric | Value |
|---|---|
| n_years | 5.98 |
| n_rebalances | 78 |
| cagr | 23.72 |
| vol_annual | 29.73 |
| sharpe | 0.764 |
| mdd | -31.73 |
| hit_rate | 37.2 |
| mean_ret_per_rebal | 2.91 |

## TWII Benchmark (aligned dates)

| Metric | TWII |
|---|---|
| n_years | 5.98 |
| cagr | 14.82 |
| vol_annual | 21.2 |
| sharpe | 0.652 |
| mdd | -34.7 |

## Alpha vs TWII

- **CAGR alpha**: +8.90 pp
- **Sharpe delta**: +0.112
- **MDD**: Value -31.73% vs TWII -34.7%

## Annual Breakdown

| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |
|---|---|---|---|---|
| 2020 | 25.50 | 33.59 | -8.09 | 28.6% |
| 2021 | 36.12 | 18.99 | +17.13 | 41.7% |
| 2022 | -23.85 | -23.97 | +0.12 | 23.1% |
| 2023 | 117.14 | 22.02 | +95.12 | 61.5% |
| 2024 | 13.51 | 19.22 | -5.71 | 30.8% |
| 2025 | 11.34 | 29.97 | -18.63 | 38.5% |


## Caveats

- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)
- `fwd_20d` 是 PIT-safe 但**不含股息再投入**
- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)
- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場
