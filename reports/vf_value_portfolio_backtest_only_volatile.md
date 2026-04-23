# Value Portfolio Backtest (2020-2025)

**Date**: 2020-01-03 -> 2025-12-26

**Spec**: Top-20 equal-weight, rebalance every 4 weeks, Stage 1 (PE<=12 / PB<=3.0 / Graham<=22.5 / TV>=30M), weights {'val': 0.3, 'quality': 0.25, 'revenue': 0.3, 'technical': 0.15, 'sm': 0.0}

**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)

## Value Top-20

| Metric | Value |
|---|---|
| n_years | 5.98 |
| n_rebalances | 78 |
| cagr | 15.05 |
| vol_annual | 15.08 |
| sharpe | 0.932 |
| mdd | -12.79 |
| hit_rate | 17.9 |
| mean_ret_per_rebal | 1.16 |

## TWII Benchmark (aligned dates)

| Metric | TWII |
|---|---|
| n_years | 5.98 |
| cagr | 14.82 |
| vol_annual | 21.2 |
| sharpe | 0.652 |
| mdd | -34.7 |

## Alpha vs TWII

- **CAGR alpha**: +0.23 pp
- **Sharpe delta**: +0.280
- **MDD**: Value -12.79% vs TWII -34.7%

## Annual Breakdown

| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |
|---|---|---|---|---|
| 2020 | 35.65 | 33.59 | +2.06 | 28.6% |
| 2021 | 43.25 | 18.99 | +24.26 | 25.0% |
| 2022 | 14.22 | -23.97 | +38.19 | 23.1% |
| 2023 | 9.03 | 22.02 | -12.99 | 7.7% |
| 2024 | 1.39 | 19.22 | -17.83 | 15.4% |
| 2025 | -5.77 | 29.97 | -35.74 | 7.7% |


## Caveats

- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)
- `fwd_20d` 是 PIT-safe 但**不含股息再投入**
- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)
- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場
