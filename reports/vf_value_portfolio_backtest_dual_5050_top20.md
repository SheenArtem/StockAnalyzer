# Value Portfolio Backtest (2020-2025)

**Date**: 2020-01-03 -> 2025-12-26

**Spec**: Top-20 equal-weight, rebalance every 4 weeks, Stage 1 (PE<=12 / PB<=3.0 / Graham<=22.5 / TV>=30M), weights {'val': 0.3, 'quality': 0.25, 'revenue': 0.3, 'technical': 0.15, 'sm': 0.0}

**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)

## Value Top-20

| Metric | Value |
|---|---|
| n_years | 5.98 |
| n_rebalances | 78 |
| cagr | 17.45 |
| vol_annual | 20.45 |
| sharpe | 0.804 |
| mdd | -25.46 |
| hit_rate | 52.6 |
| mean_ret_per_rebal | 1.43 |

## TWII Benchmark (aligned dates)

| Metric | TWII |
|---|---|
| n_years | 5.98 |
| cagr | 14.82 |
| vol_annual | 21.2 |
| sharpe | 0.652 |
| mdd | -34.7 |

## Alpha vs TWII

- **CAGR alpha**: +2.63 pp
- **Sharpe delta**: +0.152
- **MDD**: Value -25.46% vs TWII -34.7%

## Annual Breakdown

| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |
|---|---|---|---|---|
| 2020 | 49.93 | 33.59 | +16.34 | 64.3% |
| 2021 | 43.03 | 18.99 | +24.04 | 58.3% |
| 2022 | -15.12 | -23.97 | +8.85 | 38.5% |
| 2023 | 38.71 | 22.02 | +16.69 | 61.5% |
| 2024 | -3.60 | 19.22 | -22.82 | 46.2% |
| 2025 | 7.50 | 29.97 | -22.47 | 46.2% |


## Caveats

- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)
- `fwd_20d` 是 PIT-safe 但**不含股息再投入**
- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)
- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場
