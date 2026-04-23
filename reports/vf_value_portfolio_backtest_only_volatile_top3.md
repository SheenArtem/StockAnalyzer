# Value Portfolio Backtest (2020-2025)

**Date**: 2020-01-03 -> 2025-12-26

**Spec**: Top-3 equal-weight, rebalance every 4 weeks, Stage 1 (PE<=12 / PB<=3.0 / Graham<=22.5 / TV>=30M), weights {'val': 0.3, 'quality': 0.25, 'revenue': 0.3, 'technical': 0.15, 'sm': 0.0}

**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)

## Value Top-3

| Metric | Value |
|---|---|
| n_years | 5.98 |
| n_rebalances | 78 |
| cagr | 21.24 |
| vol_annual | 21.4 |
| sharpe | 0.946 |
| mdd | -17.04 |
| hit_rate | 19.2 |
| mean_ret_per_rebal | 1.65 |

## TWII Benchmark (aligned dates)

| Metric | TWII |
|---|---|
| n_years | 5.98 |
| cagr | 14.82 |
| vol_annual | 21.2 |
| sharpe | 0.652 |
| mdd | -34.7 |

## Alpha vs TWII

- **CAGR alpha**: +6.42 pp
- **Sharpe delta**: +0.294
- **MDD**: Value -17.04% vs TWII -34.7%

## Annual Breakdown

| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |
|---|---|---|---|---|
| 2020 | 41.48 | 33.59 | +7.89 | 28.6% |
| 2021 | 63.38 | 18.99 | +44.39 | 25.0% |
| 2022 | 2.49 | -23.97 | +26.46 | 15.4% |
| 2023 | 37.27 | 22.02 | +15.25 | 15.4% |
| 2024 | 2.55 | 19.22 | -16.67 | 23.1% |
| 2025 | -5.14 | 29.97 | -35.11 | 7.7% |


## Caveats

- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)
- `fwd_20d` 是 PIT-safe 但**不含股息再投入**
- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)
- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場
