# Value Portfolio Backtest (2020-2025)

**Date**: 2020-01-03 -> 2025-12-26

**Spec**: Top-20 equal-weight, rebalance every 4 weeks, Stage 1 (PE<=12 / PB<=3.0 / Graham<=22.5 / TV>=30M), weights {'val': 0.3, 'quality': 0.25, 'revenue': 0.3, 'technical': 0.15, 'sm': 0.0}

**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)

## Value Top-20

| Metric | Value |
|---|---|
| n_years | 5.98 |
| n_rebalances | 78 |
| cagr | 12.55 |
| vol_annual | 27.82 |
| sharpe | 0.415 |
| mdd | -44.71 |
| hit_rate | 57.7 |
| mean_ret_per_rebal | 1.21 |

## TWII Benchmark (aligned dates)

| Metric | TWII |
|---|---|
| n_years | 5.98 |
| cagr | 14.82 |
| vol_annual | 21.2 |
| sharpe | 0.652 |
| mdd | -34.7 |

## Alpha vs TWII

- **CAGR alpha**: -2.27 pp
- **Sharpe delta**: -0.237
- **MDD**: Value -44.71% vs TWII -34.7%

## Annual Breakdown

| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |
|---|---|---|---|---|
| 2020 | 23.98 | 33.59 | -9.61 | 64.3% |
| 2021 | 45.75 | 18.99 | +26.76 | 66.7% |
| 2022 | -30.70 | -23.97 | -6.73 | 46.2% |
| 2023 | 64.32 | 22.02 | +42.30 | 61.5% |
| 2024 | 8.28 | 19.22 | -10.94 | 53.8% |
| 2025 | -8.99 | 29.97 | -38.96 | 53.8% |


## Caveats

- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)
- `fwd_20d` 是 PIT-safe 但**不含股息再投入**
- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)
- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場
