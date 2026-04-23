# Value Portfolio Backtest (2020-2025)

**Date**: 2020-01-03 -> 2025-12-26

**Spec**: Top-3 equal-weight, rebalance every 4 weeks, Stage 1 (PE<=12 / PB<=3.0 / Graham<=22.5 / TV>=30M), weights {'val': 0.3, 'quality': 0.25, 'revenue': 0.3, 'technical': 0.15, 'sm': 0.0}

**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)

## Value Top-3

| Metric | Value |
|---|---|
| n_years | 5.98 |
| n_rebalances | 78 |
| cagr | nan |
| vol_annual | 29.0 |
| sharpe | nan |
| mdd | -31.05 |
| hit_rate | 23.1 |
| mean_ret_per_rebal | 1.11 |

## TWII Benchmark (aligned dates)

| Metric | TWII |
|---|---|
| n_years | 5.98 |
| cagr | 14.82 |
| vol_annual | 21.2 |
| sharpe | 0.652 |
| mdd | -34.7 |

## Alpha vs TWII

- **CAGR alpha**: +nan pp
- **Sharpe delta**: +nan
- **MDD**: Value -31.05% vs TWII -34.7%

## Annual Breakdown

| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |
|---|---|---|---|---|
| 2020 | 6.04 | 33.59 | -27.55 | 28.6% |
| 2021 | 33.91 | 18.99 | +14.92 | 25.0% |
| 2022 | -28.86 | -23.97 | -4.89 | 7.7% |
| 2023 | 32.83 | 22.02 | +10.81 | 30.8% |
| 2024 | -3.06 | 19.22 | -22.28 | 23.1% |
| 2025 | -0.26 | 29.97 | -30.23 | 23.1% |


## Caveats

- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)
- `fwd_20d` 是 PIT-safe 但**不含股息再投入**
- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)
- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場
