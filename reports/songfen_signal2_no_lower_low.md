# Signal #2 「利空不再破底」 Event Study

**Date**: 2026-04-29
**Period**: 2015-01-29 ~ 2026-04-13
**Triggers**: 395,447 (across 1844 stocks)

## Conditions

- C1: rolling 20-day, TWII <= -0.5% 至少 3 天
- C2: stock Low(t) > rolling 60-day Low (容忍 0.5% 微破)
- C3: bounce_rate >= 0.5 (在 TWII 利空日內個股收紅比例)

## Result Summary

| Group | n | CAR_1d | CAR_5d | CAR_10d | CAR_20d | t-stat | Grade |
|---|---:|---:|---:|---:|---:|---:|:--:|
| all | 392013 | +0.0000 | -0.0002 | -0.0012 | -0.0046 | -7.93 | D |
| regime_bull | 298875 | +0.0000 | -0.0004 | -0.0019 | -0.0068 | -10.32 | D |
| regime_bear | 93138 | +0.0002 | +0.0007 | +0.0008 | +0.0022 | +2.68 | D |
| vol_high | 127712 | +0.0006 | +0.0022 | +0.0027 | +0.0058 | +9.31 | D |
| vol_low | 130350 | +0.0002 | -0.0005 | -0.0022 | -0.0068 | -9.31 | D |

## Verdict

Overall CAR_10d = -0.0012 (t=-7.93) → **D**

Grading rule:
- A: CAR_10d > 2% AND t > 2
- B: CAR_10d > 1% AND t > 1.5
- C: CAR_10d > 0.5% AND t > 1
- D: 否則

Compared to Signal #1 (D 歸檔)，#2 樣本稀少且 3 重 AND 條件理論上訊噪比更高。
若 final 仍 D，原因同 #1：大盤 proxy 不足以代表「真利空 events」。
