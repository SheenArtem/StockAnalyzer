# Value Portfolio Backtest (2020-2025)

**Date**: 2020-01-03 -> 2025-12-26

**Spec**: Top-10 equal-weight, rebalance every 4 weeks, Stage 1 (PE<=12 / PB<=3.0 / Graham<=22.5 / TV>=30M), weights {'val': 0.3, 'quality': 0.25, 'revenue': 0.3, 'technical': 0.15, 'sm': 0.0}

**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)

## Value Top-10

| Metric | Value |
|---|---|
| n_years | 5.98 |
| n_rebalances | 78 |
| cagr | 21.09 |
| vol_annual | 25.39 |
| sharpe | 0.791 |
| mdd | -43.15 |
| hit_rate | 51.3 |
| mean_ret_per_rebal | 1.89 |

## TWII Benchmark (aligned dates)

| Metric | TWII |
|---|---|
| n_years | 5.98 |
| cagr | 14.82 |
| vol_annual | 21.2 |
| sharpe | 0.652 |
| mdd | -34.7 |

## Alpha vs TWII

- **CAGR alpha**: +6.27 pp
- **Sharpe delta**: +0.139
- **MDD**: Value -43.15% vs TWII -34.7%

## Annual Breakdown

| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |
|---|---|---|---|---|
| 2020 | 56.18 | 33.59 | +22.59 | 64.3% |
| 2021 | 39.96 | 18.99 | +20.97 | 41.7% |
| 2022 | -35.93 | -23.97 | -11.96 | 23.1% |
| 2023 | 82.08 | 22.02 | +60.06 | 84.6% |
| 2024 | 9.31 | 19.22 | -9.91 | 46.2% |
| 2025 | 12.67 | 29.97 | -17.30 | 46.2% |


## Caveats

- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)
- `fwd_20d` 是 PIT-safe 但**不含股息再投入**
- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)
- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場
