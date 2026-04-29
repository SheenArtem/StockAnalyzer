# 宋分擇時 #3 三風險同步 Event Study

**Date**: 2026-04-29
**Universe**: TWII 2007-05 ~ 2026-04 (HYG/VIX3M data limit)
**Signal**: HYG 20d ≥ 0 AND (VIX3M-VIX) 20d Δ ≥ 0 AND 10Y 20d Δ ≤ 0

## Result Table

| group | n | fwd_5d | fwd_10d | fwd_20d | t_20d |
|---|---:|---:|---:|---:|---:|
| all | 2741 | +0.0029 | +0.0057 | +0.0109 | +11.57 |
| signal=True | 545 | +0.0063 | +0.0092 | +0.0132 | +7.92 |
| signal=False | 2196 | +0.0020 | +0.0048 | +0.0103 | +9.39 |
| c1_only | 1461 | +0.0044 | +0.0081 | +0.0145 | +12.37 |
| c2_only | 1406 | +0.0039 | +0.0064 | +0.0119 | +9.88 |
| c3_only | 1319 | +0.0038 | +0.0062 | +0.0106 | +7.68 |
| regime_bull & signal | 396 | +0.0052 | +0.0073 | +0.0129 | +6.70 |
| regime_bear & signal | 149 | +0.0093 | +0.0142 | +0.0141 | +4.21 |

## Verdict

Edge over baseline = +0.0023 (t=+7.92) → **Grade C**

Grading rule:
- A: edge > 0.5% AND t > 2
- B: edge > 0.3% AND t > 1.5
- C: edge > 0.1%
- D: 否則
