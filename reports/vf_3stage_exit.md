# 三段式獲利 vs Buy & Hold (60d hold) Backtest

**Date**: 2026-04-29
**Universe**: trade_journal_qm_tw.parquet (4,923 trades 2015-2025)
**Compared**: 3-stage exit (S1 +15% / S2 動能弱 / S3 MA60 跌破) vs B&H (60d hold)

## Summary

| Strategy | n | mean | median | std | Sharpe | win_rate | max_loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| 3stage | 4923 | +0.0161 | -0.0106 | +0.1272 | +0.254 | 0.340 | -0.1667 |
| buy_hold | 4923 | +0.0468 | +0.0078 | +0.2542 | +0.368 | 0.517 | -0.5971 |

## Stage Trigger Frequency

- S1 (+15% gain): 12.6% of trades
- S2 (momentum reversal / MA20 break): 43.1%
- S3 (MA60 break — full exit): 91.1%

## Yearly Breakdown

| Year | n | 3-stage | B&H | diff |
|---:|---:|---:|---:|---:|
| 2015.0 | 103.0 | -0.0169 | -0.0600 | +0.0431 |
| 2016.0 | 207.0 | +0.0004 | +0.0253 | -0.0249 |
| 2017.0 | 548.0 | +0.0110 | +0.0610 | -0.0501 |
| 2018.0 | 492.0 | -0.0030 | -0.0286 | +0.0255 |
| 2019.0 | 487.0 | +0.0120 | +0.0267 | -0.0147 |
| 2020.0 | 523.0 | +0.0252 | +0.1012 | -0.0760 |
| 2021.0 | 607.0 | +0.0247 | +0.0651 | -0.0404 |
| 2022.0 | 391.0 | -0.0115 | -0.0575 | +0.0460 |
| 2023.0 | 573.0 | +0.0302 | +0.1075 | -0.0772 |
| 2024.0 | 577.0 | +0.0238 | +0.0550 | -0.0313 |
| 2025.0 | 415.0 | +0.0386 | +0.0859 | -0.0473 |

## Verdict: WORSE

Diff mean = -0.0307
Diff Sharpe = -0.114
3-stage better in 3/11 years

落地門檻: diff_mean > 0.5% AND diff_sharpe > 0.05 AND >= 60% years better
