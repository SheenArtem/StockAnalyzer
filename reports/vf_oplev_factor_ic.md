# Operating Leverage Factor IC

**Date**: 2026-04-29
**Universe**: TW common stocks 2015-2025
**Factors**: F1 yoy_diff (Rev YoY - OpEx YoY) / F2 yoy_diff_2q / F3 positive_2q

## Result Table

| factor | h | n_mo | IC | IR | hit | t | Q10-Q1 | mono | WF | Grade |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:--:|
| yoy_diff | 1m | 116 | +0.0024 | +0.054 | 0.517 | +0.58 | +0.0016 | +0.770 | n/a | D |
| yoy_diff | 3m | 116 | -0.0010 | -0.024 | 0.509 | -0.25 | -0.0005 | +0.394 | n/a | D |
| yoy_diff | 6m | 114 | -0.0040 | -0.087 | 0.500 | -0.93 | -0.0059 | +0.164 | n/a | D |
| yoy_diff | 12m | 108 | -0.0208 | -0.446 | 0.417 | -4.64 | -0.0221 | -0.164 | +0.750 | B |
| yoy_diff_2q | 1m | 113 | -0.0001 | -0.001 | 0.513 | -0.02 | -0.0001 | +0.588 | n/a | D |
| yoy_diff_2q | 3m | 113 | -0.0038 | -0.080 | 0.504 | -0.85 | -0.0006 | +0.273 | n/a | D |
| yoy_diff_2q | 6m | 111 | -0.0124 | -0.248 | 0.459 | -2.61 | -0.0104 | -0.188 | n/a | C |
| yoy_diff_2q | 12m | 105 | -0.0236 | -0.527 | 0.343 | -5.40 | -0.0140 | -0.382 | +1.000 | B |
| positive_2q | 1m | 113 | +0.0016 | +0.039 | 0.522 | +0.42 | +0.0063 | +0.648 | n/a | D |
| positive_2q | 3m | 113 | -0.0003 | -0.007 | 0.522 | -0.08 | +0.0193 | +0.539 | n/a | D |
| positive_2q | 6m | 111 | -0.0073 | -0.171 | 0.495 | -1.81 | +0.0386 | +0.176 | n/a | D |
| positive_2q | 12m | 105 | -0.0180 | -0.415 | 0.390 | -4.25 | +0.0863 | -0.018 | +1.000 | C |

## Verdict

Best factor by |IC|: **yoy_diff_2q** @ 12m, IC=-0.0236, IR=-0.527, Grade B

宋分原話: 「營收成長 > 費用成長 連 2 季 → 利潤噴發」

如果 best grade = D → thesis 不成立或 alpha 太弱 (跟 ROIC/CCC 一致)
如果 grade ≥ B → 考慮加進 value_screener 或做 portfolio backtest 確認
