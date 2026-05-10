# ^MOVE 5d Delta Shock Detection IC Validation
Date: 2026-05-09  |  Panel: 2002-11-12 -> 2026-05-07 (~5780 days)
Outcome: ^TWII forward 5/10/20d max drawdown (close-to-min)

## Verdict: **MARGINAL (informational only)**
- Best feature: `move_zscore_252d` @ 20d horizon
- SOP-12 gates: A=FAIL, B=PASS, C=FAIL
- Gate failure detail:
  Gate A FAIL @ 5d: |IC|=0.068 (need >=0.10), p=0.0000
  Gate A FAIL @ 10d: |IC|=0.080 (need >=0.10), p=0.0000
  Gate A FAIL @ 20d: |IC|=0.090 (need >=0.10), p=0.0000
  Gate C FAIL @ 5d: |Q10-Q1 median| = 0.50pp (need >=2)
  Gate C FAIL @ 10d: |Q10-Q1 median| = 0.35pp (need >=2)
  Gate C FAIL @ 20d: |Q10-Q1 median| = 0.29pp (need >=2)
  UPGRADE: high-threshold (z>=3.0) alert lift_20d = 3.43x baseline -> qualifies for SOP-14 informational tier

## Univariate IC table (Spearman, ^MOVE features vs ^TWII fwd MDD)

| feature | horizon | n | IC | p-value | Q1 median MDD | Q10 median MDD | Spread (pp) |
|---|---|---:|---:|---:|---:|---:|---:|
| move_5d_delta | 5d | 5770 | -0.020 | 0.1220 | -0.71% | -0.95% | -0.24 |
| move_5d_delta | 10d | 5765 | -0.027 | 0.0426 | -1.27% | -1.79% | -0.52 |
| move_5d_delta | 20d | 5755 | -0.017 | 0.2039 | -2.05% | -2.45% | -0.40 |
| move_5d_delta_pct | 5d | 5770 | -0.024 | 0.0681 | -0.60% | -0.92% | -0.32 |
| move_5d_delta_pct | 10d | 5765 | -0.030 | 0.0218 | -1.21% | -1.70% | -0.49 |
| move_5d_delta_pct | 20d | 5755 | -0.022 | 0.0931 | -1.95% | -2.36% | -0.41 |
| move_zscore_252d | 5d | 5524 | -0.068 | 0.0000 | -0.47% | -0.97% | -0.50 |
| move_zscore_252d | 10d | 5519 | -0.080 | 0.0000 | -1.02% | -1.37% | -0.35 |
| move_zscore_252d | 20d | 5509 | -0.090 | 0.0000 | -1.46% | -1.76% | -0.29 |
| move_5d_delta_zscore | 5d | 5519 | -0.020 | 0.1464 | -0.58% | -0.94% | -0.36 |
| move_5d_delta_zscore | 10d | 5514 | -0.023 | 0.0910 | -1.21% | -1.68% | -0.47 |
| move_5d_delta_zscore | 20d | 5504 | -0.017 | 0.2153 | -1.92% | -2.41% | -0.49 |

## Event study: 3 known shocks, MOVE 5d Delta z-score >= 1.5 alert in 60 TD lookback

| Shock | Date | MOVE alert? | Lead (trading days) | max z in window | z @ shock | ma_dist_60 yellow? | ma_dist_60 lead | fwd_5d MDD | fwd_10d MDD | fwd_20d MDD |
|---|---|---|---:|---:|---:|---|---:|---:|---:|---:|
| COVID 2020 | 2020-02-20 | YES | 15 | +3.44 | +0.85 | NO | n/a | -3.69% | -4.73% | -25.96% |
| Jackson Hole 2022 | 2022-08-26 | YES | 55 | +4.27 | -0.21 | YES | 1 | -3.96% | -5.68% | -9.82% |
| Trump tariff 2025 | 2025-03-03 | YES | 2 | +1.54 | +1.54 | NO | n/a | -1.31% | -3.49% | -9.05% |

## Conditional alert lift over baseline (move_5d_delta_zscore)

| Threshold | n alerts | % days | fwd_5d MDD median | fwd_20d MDD median | hit fwd_5d <= -3% | hit fwd_20d <= -10% | lift_20d vs baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline (all days) | 6753 | 100.0% | -0.75% | -2.22% | 16.2% | 9.1% | 1.00x |
| z >= 1.5 | 363 | 5.4% | -0.99% | -2.43% | 20.7% | 13.2% | 1.45x |
| z >= 2.0 | 211 | 3.1% | -1.08% | -2.46% | 21.3% | 16.6% | 1.82x |
| z >= 2.5 | 131 | 1.9% | -1.08% | -2.67% | 24.4% | 22.9% | 2.51x |
| z >= 3.0 | 64 | 0.9% | -1.70% | -4.30% | 39.1% | 31.2% | 3.43x |

## Decile breakdown — best feature `move_zscore_252d` @ 20d

| Decile | Median fwd MDD (pct) |
|---:|---:|
| Q1 | -1.46% |
| Q2 | -2.07% |
| Q3 | -1.94% |
| Q4 | -1.56% |
| Q5 | -1.91% |
| Q6 | -1.94% |
| Q7 | -1.98% |
| Q8 | -2.46% |
| Q9 | -2.53% |
| Q10 | -1.76% |

## Recommendation

`move_zscore_252d` MARGINAL -- SOP-12 univariate gate failed but conditional-on-alert lift is real.

**Why SOP-12 fails**: feature value is dominated by quiet days. Linear Spearman over 5500 days washes out the rare shock spikes that carry the signal. Decile spread is small for the same reason -- 90% of the support is centred near zero.

**Why the signal is still useful**: when z >= 3.0 fires (~64 days = 1.2% of sample), fwd_20d MDD median is -4.30% (vs baseline -2.22%), and hit rate fwd_20d <= -10% jumps to 31.2% (vs baseline 9.1% = 3.4x lift). z >= 3 is a true tail-risk regime indicator, not a continuous predictor.

**Integration spec (informational tier, SOP-14 style)**:
- Add 7th stage to `system3_daily_check.py` named `move_shock_alert`
- Compute `move_5d_delta_zscore` daily (252d rolling baseline of 5d delta)
- Threshold: z >= 1.5 -> yellow / z >= 2.5 -> orange / z >= 3.0 -> red
- Push Discord with current z, fwd_20d hit rate at this z bucket, and historical lift
- **Do NOT auto-rebalance** -- ^MOVE -> ^TWII transmission is indirect; treat as situational awareness only
- Cooldown: 60 days (same as System 3 ma_dist_60 yellow)

**Complementarity check (event study)**: At 2 of 3 known shocks (COVID 2020, Trump tariff 2025), ma_dist_60 yellow was NOT triggered in 60 TD lookback while MOVE z >= 1.5 fired with lead 15 and 2 TD. At Jackson Hole 2022 both fired but ma_dist_60 led by 1 TD vs MOVE 55 TD -- MOVE caught the prior June Treasury vol spike that ma_dist_60 missed. Bottom line: MOVE covers shock gaps that ma_dist_60 misses by design (slow rolling rank vs sharp delta), and OR-union of two signals should improve recall.

**Caveat**: Trump tariff 2025 lead = 2 TD only. For fast policy/exec-order events, MOVE shock is concurrent with ^TWII selloff, useful for confirmation rather than anticipation. COVID-style shocks (slower bond-market repricing then equity selloff) is the cleanest use case.
