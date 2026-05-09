# System 2 Events Summary (TWII)

**Source**: `TAIEX_price.parquet`  
**Date range**: 1999-01-05 -> 2026-05-08  
**Window**: 60 trading days (rolling high + forward)  
**Trigger**: drawdown <= -5%

## Event count: 77

| Class | Range | N | % |
|---|---|---|---|
| A_small  | [-10%, -5%)   | 29  | 37.7%  |
| B_medium | [-20%, -10%)  | 28 | 36.4% |
| C_crash  | <= -20%        | 20  | 26.0%  |

## Spec target distribution

| Class | Spec %  | Actual % | Delta |
|---|---|---|---|
| A_small  | 60% | 37.7%  | -22.3 |
| B_medium | 25% | 36.4% | +11.4 |
| C_crash  | 15% | 26.0%  | +11.0 |

## Time-to-trough by class (median trading days)

- A_small: 5
- B_medium: 16
- C_crash: 45

## Final drawdown by class (median)

- A_small: -7.28%
- B_medium: -13.58%
- C_crash: -26.11%

## SOP-14 sample sufficiency gate

- N=77 (>=50), C_crash=20 (>=10): **PASS** -- proceed to Phase 2.2

## Top 10 deepest crashes

| event_id | trigger_date | trough_date | peak | trough | final_dd | t2t | class |
|---|---|---|---|---|---|---|---|
| 33 | 2008-08-20 | 2008-10-27 | 8745 | 4367 | -50.07% | 46 | C_crash |
| 6 | 2000-08-03 | 2000-10-19 | 9120 | 5081 | -44.28% | 58 | C_crash |
| 7 | 2000-10-23 | 2000-12-27 | 8258 | 4615 | -44.12% | 50 | C_crash |
| 34 | 2008-11-17 | 2008-11-20 | 7081 | 4090 | -42.24% | 3 | C_crash |
| 10 | 2001-07-16 | 2001-10-03 | 5598 | 3446 | -38.43% | 52 | C_crash |
| 14 | 2002-07-25 | 2002-10-11 | 5911 | 3850 | -34.86% | 54 | C_crash |
| 61 | 2020-01-30 | 2020-03-19 | 12180 | 8681 | -28.72% | 34 | C_crash |
| 32 | 2008-05-26 | 2008-07-16 | 9295 | 6711 | -27.81% | 37 | C_crash |
| 75 | 2025-03-10 | 2025-04-09 | 23730 | 17392 | -26.71% | 20 | C_crash |
| 9 | 2001-04-18 | 2001-07-13 | 6104 | 4486 | -26.52% | 60 | C_crash |
