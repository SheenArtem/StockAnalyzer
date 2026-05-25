# Vol Complex 4 訊號 IC Validation vs ^TWII

Date: 2026-05-25  Panel: 2007-01-03 ~ 2026-04-23 (4565 rows)
Outcome: ^TWII fwd 5/10/20d max drawdown (close-to-min), US T → TW T+1 對齊

## Verdict 摘要 (SOP-12 3-gate)

| Signal | Verdict | Best |IC| |
|---|---|---|
| `vix_vix3m_ratio` | MARGINAL (informational) | 0.077 |
| `vvix` | FAIL | 0.023 |
| `skew` | FAIL | 0.102 |
| `ovx` | FAIL | 0.071 |

## Per-feature univariate IC

| feature | horizon | n | IC | p-value | Q1 med MDD | Q10 med MDD | Spread (pp) |
|---|---|---:|---:|---:|---:|---:|---:|
| vix_vix3m_ratio | 5d | 4565 | -0.068 | 0.0000 | -0.67% | -1.10% | -0.43 |
| vix_vix3m_ratio | 10d | 4565 | -0.077 | 0.0000 | -1.00% | -1.70% | -0.70 |
| vix_vix3m_ratio | 20d | 4565 | -0.062 | 0.0000 | -1.89% | -2.62% | -0.73 |
| vvix | 5d | 4556 | -0.023 | 0.1242 | -0.50% | -0.65% | -0.15 |
| vvix | 10d | 4556 | -0.018 | 0.2319 | -1.12% | -1.32% | -0.19 |
| vvix | 20d | 4556 | -0.010 | 0.4925 | -2.20% | -1.93% | +0.28 |
| skew | 5d | 4507 | +0.088 | 0.0000 | -1.06% | -0.46% | +0.60 |
| skew | 10d | 4507 | +0.102 | 0.0000 | -1.91% | -0.95% | +0.96 |
| skew | 20d | 4507 | +0.088 | 0.0000 | -3.00% | -1.87% | +1.13 |
| ovx | 5d | 4486 | -0.071 | 0.0000 | -0.49% | -0.65% | -0.15 |
| ovx | 10d | 4486 | -0.068 | 0.0000 | -0.97% | -1.11% | -0.14 |
| ovx | 20d | 4486 | -0.038 | 0.0104 | -1.47% | -1.48% | -0.00 |

## Per-feature conditional lift (threshold-based)

### `vix_vix3m_ratio`

| Condition | n | % days | fwd5 med MDD | fwd20 med MDD | hit fwd20 <= -10% | lift_20d |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 4565 | 100.0% | -0.64% | -1.92% | 6.7% | 1.00x |
| vix_vix3m_ratio >= 0.95 | 1194 | 26.2% | -0.96% | -2.33% | 13.4% | 2.01x |
| vix_vix3m_ratio >= 1.00 | 513 | 11.2% | -1.08% | -2.45% | 16.2% | 2.42x |
| vix_vix3m_ratio >= 1.05 | 214 | 4.7% | -1.33% | -3.03% | 27.1% | 4.06x |

Gate failures / upgrade notes:
- Gate A FAIL @ 5d: |IC|=0.068 p=0.0000
- Gate A FAIL @ 10d: |IC|=0.077 p=0.0000
- Gate A FAIL @ 20d: |IC|=0.062 p=0.0000
- Gate C FAIL @ 5d: |spread|=0.43pp
- Gate C FAIL @ 10d: |spread|=0.70pp
- Gate C FAIL @ 20d: |spread|=0.73pp
- UPGRADE: high-threshold lift=4.06x → SOP-14 tier

### `vvix`

| Condition | n | % days | fwd5 med MDD | fwd20 med MDD | hit fwd20 <= -10% | lift_20d |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 4565 | 100.0% | -0.64% | -1.92% | 6.7% | 1.00x |
| vvix >= 100 | 1322 | 29.0% | -0.74% | -2.03% | 5.9% | 0.88x |
| vvix >= 110 | 705 | 15.4% | -0.75% | -1.96% | 6.5% | 0.98x |
| vvix >= 130 | 124 | 2.7% | -0.20% | -0.70% | 4.0% | 0.60x |

Gate failures / upgrade notes:
- Gate A FAIL @ 5d: |IC|=0.023 p=0.1242
- Gate A FAIL @ 10d: |IC|=0.018 p=0.2319
- Gate A FAIL @ 20d: |IC|=0.010 p=0.4925
- Gate B FAIL: spread signs inconsistent ['-0.15', '-0.19', '+0.28']
- Gate C FAIL @ 5d: |spread|=0.15pp
- Gate C FAIL @ 10d: |spread|=0.19pp
- Gate C FAIL @ 20d: |spread|=0.28pp

### `skew`

| Condition | n | % days | fwd5 med MDD | fwd20 med MDD | hit fwd20 <= -10% | lift_20d |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 4565 | 100.0% | -0.64% | -1.92% | 6.7% | 1.00x |
| skew >= 140 | 862 | 18.9% | -0.50% | -1.77% | 3.7% | 0.56x |
| skew >= 145 | 518 | 11.3% | -0.45% | -1.82% | 2.9% | 0.43x |
| skew >= 155 | 137 | 3.0% | -0.44% | -2.30% | 0.7% | 0.11x |

Gate failures / upgrade notes:
- Gate A FAIL @ 5d: |IC|=0.088 p=0.0000
- Gate A FAIL @ 20d: |IC|=0.088 p=0.0000
- Gate C FAIL @ 5d: |spread|=0.60pp
- Gate C FAIL @ 10d: |spread|=0.96pp
- Gate C FAIL @ 20d: |spread|=1.13pp

### `ovx`

| Condition | n | % days | fwd5 med MDD | fwd20 med MDD | hit fwd20 <= -10% | lift_20d |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 4565 | 100.0% | -0.64% | -1.92% | 6.7% | 1.00x |
| ovx >= 40 | 1547 | 33.9% | -0.87% | -2.21% | 10.1% | 1.52x |
| ovx >= 50 | 606 | 13.3% | -0.78% | -1.79% | 8.6% | 1.28x |
| ovx >= 80 | 120 | 2.6% | -1.22% | -3.04% | 14.2% | 2.12x |

Gate failures / upgrade notes:
- Gate A FAIL @ 5d: |IC|=0.071 p=0.0000
- Gate A FAIL @ 10d: |IC|=0.068 p=0.0000
- Gate A FAIL @ 20d: |IC|=0.038 p=0.0104
- Gate C FAIL @ 5d: |spread|=0.15pp
- Gate C FAIL @ 10d: |spread|=0.14pp
- Gate C FAIL @ 20d: |spread|=0.00pp

## Composite regime: lit_count vs fwd MDD

| lit_count | n | % days | fwd5 med | fwd20 med | hit fwd20 <= -10% | lift |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 4565 | 100.0% | -0.64% | -1.92% | 6.7% | 1.00x |
| 0 | 1663 | 36.4% | -0.54% | -1.71% | 3.2% | 0.48x |
| 1 | 1444 | 31.6% | -0.63% | -1.99% | 8.6% | 1.29x |
| 2 | 949 | 20.8% | -0.79% | -2.18% | 8.5% | 1.28x |
| 3 | 453 | 9.9% | -1.03% | -2.20% | 10.4% | 1.55x |
| 4 | 56 | 1.2% | -0.08% | -1.81% | 0.0% | 0.00x |

## Event study (5 known shocks)

| Shock | Date | First lit>=2 | Lead (TD) | Lit at alert | Lit at shock | fwd5 MDD | fwd20 MDD |
|---|---|---|---:|---:|---:|---:|---:|
| COVID 2020 | 2020-02-20 | 2019-12-30 | 28 | 2 | 0 | -4.41% | -25.71% |
| Russia/UA 2022 | 2022-02-24 | 2021-11-17 | 59 | 2 | 3 | -2.68% | -4.68% |
| SVB 2023 | 2023-03-09 | 2022-12-12 | 49 | 2 | 1 | -1.96% | -1.96% |
| Aug 2024 Yen carry | 2024-08-05 | 2024-07-18 | 10 | 2 | 4 | +1.80% | +1.80% |
| Trump tariff 2025 | 2025-03-03 | 2024-11-20 | 59 | 2 | 3 | -2.33% | -8.41% |

## Recommendation

**1/4 signals MARGINAL/PASS** — see per-feature lift tables above.
Consider promoting MARGINAL signals to system3_daily_check stage with SOP-14 informational push.
Composite lit_count regime: if lift >= 2x at lit>=2, framework's '2 lights = reduce 30%' threshold has some TW support.

**Caveat**: 對齊 US T → TW T+1 是 close-close 假設，盤中即時反應未捕捉；
Aug 2024 yen carry / SVB 等隔夜跳空無法看出真實 lead time。