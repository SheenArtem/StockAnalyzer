# Value Portfolio Backtest (2020-2025)

**Date**: 2020-01-03 -> 2025-12-26

**Spec**: Top-5 equal-weight, rebalance every 4 weeks, Stage 1 (PE<=12 / PB<=3.0 / Graham<=22.5 / TV>=30M), weights {'val': 0.3, 'quality': 0.25, 'revenue': 0.3, 'technical': 0.15, 'sm': 0.0}

**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)

## Value Top-5

| Metric | Value |
|---|---|
| n_years | 5.98 |
| n_rebalances | 78 |
| cagr | 19.66 |
| vol_annual | 18.95 |
| sharpe | 0.984 |
| mdd | -14.81 |
| hit_rate | 21.8 |
| mean_ret_per_rebal | 1.51 |

## TWII Benchmark (aligned dates)

| Metric | TWII |
|---|---|
| n_years | 5.98 |
| cagr | 14.82 |
| vol_annual | 21.2 |
| sharpe | 0.652 |
| mdd | -34.7 |

## Alpha vs TWII

- **CAGR alpha**: +4.84 pp
- **Sharpe delta**: +0.332
- **MDD**: Value -14.81% vs TWII -34.7%

## Annual Breakdown

| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |
|---|---|---|---|---|
| 2020 | 39.33 | 33.59 | +5.74 | 28.6% |
| 2021 | 55.36 | 18.99 | +36.37 | 25.0% |
| 2022 | 7.64 | -23.97 | +31.61 | 23.1% |
| 2023 | 17.93 | 22.02 | -4.09 | 15.4% |
| 2024 | 8.97 | 19.22 | -10.25 | 23.1% |
| 2025 | -2.35 | 29.97 | -32.32 | 15.4% |


## Caveats

- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)
- `fwd_20d` 是 PIT-safe 但**不含股息再投入**
- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)
- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場
