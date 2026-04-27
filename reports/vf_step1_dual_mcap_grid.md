# Step 1 — Dual 50/50 + Market Cap × Regime Grid

**Date range**: 2020-01-03 -> 2025-12-26 (309 weeks, 78 rebalances @ 4-week)

**Council verdict 2026-04-24**: 鎖 universe 先行，釐清 T1 tension (市值 filter 是否傷害 alpha) + Baseline A vs Dual 50/50 anchor 之爭。

**Top N**: Value=20, QM=20, Baseline A=5

**Mcap proxy**: `avg_tv_60d` weekly cross-sectional percentile rank (snapshot 內無 raw market cap；tv 與 mcap 高相關但非 1:1)。

**Stage 1**: PE 0~12 / PB <=3 / PE*PB <=22.5 / TV >=30M (live Value 設定)

## 結果表（排序：Sharpe 由高至低）

| strategy | mcap | regime | top_n | CAGR% | Sharpe | MDD% | Vol% | hit% | v_on% | n |
|---|---|---|---|---|---|---|---|---|---|---|
| Dual-tv_top_25-only_volatile | tv_top_25 | only_volatile | 20 | 27.41 | 1.242 | -25.17 | 21.26 | 67.1 | 28.9 | 76 |
| Dual-tv_top_75-only_volatile | tv_top_75 | only_volatile | 20 | 21.67 | 1.139 | -17.0 | 18.15 | 57.1 | 28.6 | 77 |
| Dual-tv_top_50-only_volatile | tv_top_50 | only_volatile | 20 | 21.27 | 1.106 | -16.64 | 18.34 | 55.8 | 28.6 | 77 |
| Dual-tv_top_75-none | tv_top_75 | none | 20 | 26.71 | 1.041 | -28.95 | 24.7 | 59.7 | 100.0 | 77 |
| Baseline-A_top5 | all | only_volatile | 5 | 19.66 | 0.984 | -14.81 | 18.95 | 21.8 | 26.9 | 78 |
| Dual-tv_top_50-none | tv_top_50 | none | 20 | 25.24 | 0.948 | -29.34 | 25.58 | 59.7 | 100.0 | 77 |
| Dual-tv_top_25-none | tv_top_25 | none | 20 | 36.94 | 0.908 | -45.87 | 39.57 | 61.8 | 100.0 | 76 |
| Dual-all-only_volatile | all | only_volatile | 20 | 17.45 | 0.804 | -25.46 | 20.45 | 52.6 | 26.9 | 78 |
| Dual-all-none | all | none | 20 | 15.73 | 0.532 | -43.03 | 27.71 | 56.4 | 100.0 | 78 |
| Baseline-A_top5_tv_top_75 | tv_top_75 | only_volatile | 5 | 8.93 | 0.509 | -18.39 | 15.59 | 18.2 | 28.6 | 77 |

## Caveats

- `market_cap_tier` via tv_60d percentile = **liquidity proxy**, not真 market cap
- 無交易成本扣除 (round-trip ~0.3% × 13 rebal/yr ≈ 4pp CAGR)
- 無股息再投入
- Sample 2020-2025 (QE + AI 雙牛市)，out-of-sample 2000/2008 regime 未測
- Baseline A in-market % 由 volatility regime 決定，非 100% exposure
