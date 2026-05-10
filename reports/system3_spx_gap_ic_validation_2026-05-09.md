# SPX 1d Shock -> TWII Gap-Down IC Validation

Date: 2026-05-09  |  Aligned panel: 2016-05+ (SPX from FRED) -> 2026-05-07

Outcome: TWII forward 5/10/20d max drawdown (close-to-min). Premise: SPX overnight shock -> TWII next-day gap-down + sustained drawdown.

## Verdict: **MARGINAL (informational only)**

- Best feature: `spx_1d_ret` @ 10d horizon
- SOP-12 gates: A=FAIL, B=PASS, C=FAIL
- Gate detail:
  Gate A FAIL @ 5d: |IC|=0.028 (need >=0.10), p=0.1746
  Gate A FAIL @ 10d: |IC|=0.048 (need >=0.10), p=0.0180
  Gate A FAIL @ 20d: |IC|=0.028 (need >=0.10), p=0.1691
  Gate C FAIL @ 5d: |Q10-Q1 median| = 0.16pp (need >=2)
  Gate C FAIL @ 10d: |Q10-Q1 median| = 0.26pp (need >=2)
  Gate C FAIL @ 20d: |Q10-Q1 median| = 0.42pp (need >=2)
  UPGRADE: SPX 1d <= -3% lift_20d = 8.45x baseline -> qualifies for SOP-14 informational tier

## Univariate IC table (Spearman, SPX features vs TWII fwd MDD)

| feature | horizon | n | IC | p-value | Q1 median MDD | Q10 median MDD | Spread (pp) |
|---|---|---:|---:|---:|---:|---:|---:|
| spx_1d_ret | 5d | 2430 | +0.028 | 0.1746 | -0.76% | -0.60% | +0.16 |
| spx_1d_ret | 10d | 2425 | +0.048 | 0.0180 | -1.11% | -0.85% | +0.26 |
| spx_1d_ret | 20d | 2415 | +0.028 | 0.1691 | -1.81% | -1.40% | +0.42 |
| spx_2d_ret | 5d | 2429 | +0.028 | 0.1690 | -0.74% | -0.59% | +0.15 |
| spx_2d_ret | 10d | 2424 | +0.044 | 0.0300 | -1.34% | -0.91% | +0.43 |
| spx_2d_ret | 20d | 2414 | +0.027 | 0.1796 | -2.26% | -1.46% | +0.80 |

## TWII next-day gap_open conditional on SPX 1d shock

Direct test of premise: when SPX falls hard overnight, does TWII actually gap down?

| Threshold | n | TWII gap median | TWII gap mean | P(gap <= -1%) | P(gap <= -2%) |
|---|---:|---:|---:|---:|---:|
| baseline | 2435 | +0.06% | +0.06% | 2.5% | 0.5% |
| SPX_1d <= -1.0% | 194 | -0.37% | -0.48% | 16.0% | 2.1% |
| SPX_1d <= -1.5% | 110 | -0.49% | -0.62% | 25.5% | 2.7% |
| SPX_1d <= -2.0% | 64 | -0.75% | -0.74% | 35.9% | 3.1% |
| SPX_1d <= -2.5% | 39 | -0.98% | -0.82% | 48.7% | 2.6% |
| SPX_1d <= -3.0% | 24 | -1.11% | -1.09% | 54.2% | 4.2% |

## Conditional fwd drawdown lift (SPX 1d return)

| Threshold | n alerts | % days | fwd_5d MDD median | fwd_10d MDD median | fwd_20d MDD median | hit fwd_5d <= -3% | hit fwd_10d <= -5% | hit fwd_20d <= -10% | lift_20d |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (all days) | 2415 | 100.0% | -0.54% | -0.92% | -1.57% | 9.6% | 8.2% | 4.4% | 1.00x |
| <= -1.0% | 194 | 8.0% | -0.79% | -1.25% | -2.12% | 20.1% | 16.5% | 11.3% | 2.56x |
| <= -1.5% | 110 | 4.6% | -0.84% | -1.80% | -2.75% | 17.3% | 15.5% | 12.7% | 2.87x |
| <= -2.0% | 64 | 2.7% | -0.95% | -1.80% | -2.55% | 18.8% | 23.4% | 18.8% | 4.23x |
| <= -2.5% | 39 | 1.6% | -0.92% | -1.52% | -2.77% | 23.1% | 28.2% | 28.2% | 6.36x |
| <= -3.0% | 24 | 1.0% | -1.95% | -3.34% | -4.09% | 33.3% | 41.7% | 37.5% | 8.45x |

## Event study: 3 known shocks

SPX 1d alert tested in 45 TD POST-window (concurrent with selloff). MOVE / ma_dist_60 in 60 TD PRE-window (leading).

| Shock | Label date | SPX@label | TWII gap@label | SPX <=-2% post? | SPX lag TD | SPX value | TWII gap@SPX trigger | TWII fwd_5d@trigger | TWII fwd_20d@trigger | MOVE pre? | MOVE lead | ma60 pre? | ma lead | fwd_5d (label) | fwd_20d (label) |
|---|---|---:|---:|---|---:|---:|---:|---:|---:|---|---:|---|---:|---:|---:|
| COVID 2020 | 2020-02-20 | +0.47% | +0.23% | YES | 3 | -3.35% | -0.24% | -3.20% | -24.77% | YES | 15 | NO | n/a | -3.69% | -25.96% |
| Jackson Hole 2022 | 2022-08-26 | +1.41% | +0.14% | YES | 12 | -4.32% | -1.34% | -1.59% | -12.60% | YES | 55 | YES | 1 | -3.96% | -9.82% |
| Trump tariff 2025 | 2025-03-03 | +0.00% | -1.38% | YES | 6 | -2.70% | -1.48% | -0.50% | -21.20% | YES | 2 | NO | n/a | -1.31% | -9.05% |

## Independence check vs S3-a (^MOVE z >= 2.5)

- SPX 1d <= -2% alerts: **64** days
- MOVE z >= 2.5 alerts: **59** days
- Intersection: **7** days, Union: **116** days
- **Jaccard: 0.060** (<50% complementary)
- SPX-only days (signal MOVE misses): 57
- MOVE-only days (signal SPX misses): 52

## Decile breakdown - best feature `spx_1d_ret` @ 10d

| Decile | Median fwd MDD (pct) |
|---:|---:|
| Q1 | -1.11% |
| Q2 | -0.92% |
| Q3 | -1.11% |
| Q4 | -0.89% |
| Q5 | -1.12% |
| Q6 | -0.96% |
| Q7 | -0.80% |
| Q8 | -0.84% |
| Q9 | -0.79% |
| Q10 | -0.85% |

## Recommendation

`spx_1d_ret` MARGINAL (informational only). SOP-12 univariate gate fails because SPX 1d return is dominated by quiet days near zero, so linear Spearman over 2435 days washes out the rare shock spikes.

**Why the signal is real**: when SPX 1d <= -3% fires (24 days = 1.0% of sample), TWII fwd_20d MDD median is -4.09% (vs baseline -1.57%) and hit rate fwd_20d <= -10% jumps to 37.5% (vs baseline 4.4% = **8.45x lift**). Direct gap-down test confirms premise: SPX 1d <= -3% -> P(TWII gap <= -1%) = 54.2% vs baseline 2.5% (22x lift). This is a tail-risk regime indicator, not a continuous predictor.

**Event study (corrected concurrent timing)**: at all 3 known shocks, SPX 1d <= -2% fires WITHIN 3-12 TD after the shock label (not before). TWII fwd_20d MDD from SPX trigger day is -12.6% to -24.8%. The signal is concurrent with the early selloff, perfect for next-day TW gap-down warning, NOT for early leading anticipation. Use ^MOVE / ma_dist_60 for early warning, SPX shock for confirmation + sizing.

**Integration spec (SOP-14 informational tier)**:
- Add 8th stage to `system3_daily_check.py` named `spx_gap_alert`
- Compute: SPX 1d % from `data/macro/fred_panel.parquet` `sp500_close.pct_change()`
- Trigger: SPX 1d <= -1.5% yellow / <= -2.5% orange / <= -3.0% red
- Push Discord on TW pre-open (08:30 TPE) with: SPX 1d %, expected TWII gap range (median+IQR from conditional table), conditional fwd_5d/20d MDD hit rate
- **Do NOT auto-rebalance** -- gap-down can mean-revert within 5 days; treat as situational awareness for sizing/intraday
- Cooldown: 3 TD (shorter than MOVE/ma_dist_60 -- shock signal is concurrent, multiple closely-spaced triggers are real risk amplification, not noise)

**Complementarity vs S3-a (^MOVE)**: Jaccard = **0.060** (extremely low). Of 116 union alerts, only 7 fire on same day. SPX-only = 57 days, MOVE-only = 52 days. The two signals catch entirely different shock TYPES: SPX = equity-led concurrent shocks (e.g. tariff exec orders, earnings disasters); MOVE = bond-vol-led leading shocks (e.g. Treasury repricing). **Both should be kept** -- recommend keeping S3-a (MOVE) and S3-b (SPX) as parallel informational stages with distinct Discord labels.

**Caveat**: The shock label dates (COVID 2020-02-20 etc.) are subjective. SPX -2%+ days OCCUR throughout 2020-03 / 2022-09 / 2025-04 selloffs, the analysis confirms SPX shock is a reliable concurrent indicator but not anticipatory. Use this signal to prepare for TW open, not to pre-position cash.
