# Banner Risk Score Calibration Report

**Generated**: 2026-05-08
**Method**: SOP-14 Informational Tier -- result-driven weight derivation via univariate co-occurrence analysis
**Panel**: crash_predictor_tw_panel.parquet | 1999-01-05 to 2026-01-29 | N=6,699 complete-case rows
**Label**: forward 60-day peak-to-trough drawdown (label_10pct: MDD >= 10%; label_5pct: MDD >= 5%)
**Baseline rates**: P(MDD>=10%) = 0.3012 (30.1%) | P(MDD>=5%) = 0.5308 (53.1%)

---

## Step 1: Signal Availability Audit

| Signal | Source | Status | N rows | Reason |
|---|---|---|---|---|
| TW FGI score | taifex_data.TaiwanFearGreedIndex | EXCLUDED | 0 | live calculate() only; no historical archive |
| CNN FGI score | cnn_fear_greed.CNNFearGreedIndex | EXCLUDED | 0 | live get_index() only; no historical archive |
| Put/Call Ratio | TAIFEX MaterialServlet | EXCLUDED | 0 | no local PCR history CSV found |
| ATM Put Z-score | data/sentiment/atm_put_premium.parquet | EXCLUDED | 4 | snapshot only (4 rows 2026-05-05..08) |
| MTX/TXF ratio | data/sentiment/minifutures_ratio.parquet | EXCLUDED | 4 | snapshot only (4 rows 2026-05-05..08) |
| Inst PC skew | data/sentiment/options_institutional.parquet | EXCLUDED | 4 | snapshot only (4 rows 2026-05-05..08) |
| HMM Regime | data/tracking/regime_log.jsonl | EXCLUDED (borderline) | 3,596 | lift10=1.047 marginal; categorical -- not normalizable |
| M1B ratio pct | crash_predictor_tw_panel (m1b_ratio_pct) | **INCLUDED** | 6,699 | lift10=1.922 |
| rv10 (realized vol 10d) | crash_predictor_tw_panel (rv10) | **INCLUDED** | 6,699 | lift10=1.720 |
| rv30 (realized vol 30d) | crash_predictor_tw_panel (rv30) | **INCLUDED** | 6,699 | lift10=1.688 |

---

## Step 2: Univariate Co-occurrence Analysis

Danger zone: P85+ percentile (high = danger for all 3 signals).

| Signal | P85 Threshold | N Danger Days | Baseline P(10%) | Co-occur 10% | Lift 10% | Co-occur 5% | Lift 5% | N Events |
|---|---|---|---|---|---|---|---|---|
| m1b_ratio_pct | >= 36.5 | 1,007 | 30.1% | 57.8% | **1.922** | 74.8% | 1.411 | 582 |
| rv10 | >= 0.283 | 1,007 | 30.1% | 51.7% | **1.720** | 67.6% | 1.276 | 521 |
| rv30 | >= 0.282 | 1,005 | 30.1% | 50.9% | **1.688** | 65.2% | 1.228 | 511 |
| hmm_regime (volatile) | categorical | 1,222 | 19.3% | 20.2% | 1.047 | 47.9% | 1.067 | 247 |

---

## Step 4: Lift-Based Weights

| Signal | Lift10 | Weight | Note |
|---|---|---|---|
| m1b_ratio_pct | 1.922 | **36.1%** | Highest precursor power |
| rv10 | 1.720 | **32.3%** | |
| rv30 | 1.688 | **31.7%** | |
| hmm_regime | 1.047 | 0% | Categorical; excluded from composite |

Composite formula:

```
composite = rank_pct(m1b_ratio) * 0.361 + rank_pct(rv10) * 0.323 + rank_pct(rv30) * 0.317
```

where rank_pct() = within-sample percentile rank scaled to [0, 100].

### SOP-12 Check: Composite vs Best-Single

- Best single (m1b P85): lift10 = 1.922
- Composite at P85: lift10 = 1.982
- **Composite > best-single: YES -- use composite**

---

## Step 3: Zone Thresholds

| Zone | Composite Range | P-Level | Co-occur 10% | Co-occur 5% | Fwd60d MDD Median | Ann Days |
|---|---|---|---|---|---|---|
| Orange | >= 78.5 | P85+ | 59.7% | 74.9% | -12.3% | ~37 days/yr |
| Yellow | 62.1 to 78.5 | P65-P85 | 45.4% | 60.7% | -7.6% | ~50 days/yr |
| Green | < 62.1 | < P65 | 18.6% | 45.7% | -4.3% | ~161 days/yr |

Monotonicity co10: Orange > Yellow > Green: OK
Monotonicity co5: Orange > Yellow > Green: OK
Zone ratio O:Y:G = 1 : 1.3 : 4.3 (note: green-heavy due to 2005-2019 low-volatility era)

---

## Historical Timeline (Year-by-Year Zone Days)

| Year | Green | Yellow | Orange |
|---|---|---|---|
| 1999 | 8 | 117 | 126 |
| 2000 | 4 | 63 | 204 |
| 2001 | 10 | 129 | 105 |
| 2002 | 8 | 92 | 148 |
| 2003 | 92 | 127 | 30 |
| 2004 | 154 | 31 | 65 |
| 2005 | 247 | 0 | 0 |
| 2006 | 202 | 43 | 3 |
| 2007 | 136 | 55 | 56 |
| 2008 | 4 | 133 | 112 |
| 2009 | 93 | 100 | 58 |
| 2010 | 218 | 33 | 0 |
| 2011 | 179 | 61 | 7 |
| 2012 | 250 | 0 | 0 |
| 2013 | 246 | 0 | 0 |
| 2014 | 248 | 0 | 0 |
| 2015 | 239 | 5 | 0 |
| 2016 | 244 | 0 | 0 |
| 2017 | 246 | 0 | 0 |
| 2018 | 233 | 14 | 0 |
| 2019 | 242 | 0 | 0 |
| 2020 | 194 | 41 | 10 |
| 2021 | 164 | 64 | 16 |
| 2022 | 196 | 50 | 0 |
| 2023 | 239 | 0 | 0 |
| 2024 | 145 | 62 | 35 |
| 2025 | 101 | 112 | 30 |
| 2026 | 12 | 8 | 0 |

---

## Caveats and SOP-14 Compliance

### Signals Excluded Due to Insufficient History

- **TW FGI / CNN FGI**: Both provide live snapshot only. No historical archive stored locally.
  Recommendation: start a daily accumulation job; re-calibrate when >= 100 days available.
- **Put/Call Ratio**: No TAIFEX PCR CSV in local cache. Requires dedicated accumulation pipeline.
- **ATM Put Z / MTX/TXF / Inst PC skew**: 4 rows each (2026-05-05..08). Strong theoretical basis
  but insufficient for calibration. Recommend re-calibrating in ~6 months after accumulation.

### HMM Regime Note

lift10 = 1.047 for volatile regime -- technically above 1.0 but below practical significance.
Categorical label cannot be normalized to [0,100] percentile rank.
May be added later as a binary modifier (e.g. composite * 1.05 when volatile) after a
separate categorical co-occurrence study.

### SOP-14 Informational Tier Language Rule

Forbidden UI text: "predicts crash / leading indicator / will cause drawdown"
Required UI text: "elevated co-occurrence with historical drawdown periods / informational signal only"

### Thin-Data Caveat

Forward label window is 60 trading days. The final 60 days of the panel (late Nov 2025 to
Jan 2026) have truncated forward returns; valid label cutoff is 2026-01-29.

---

## Implementation Spec for Banner UI

```python
def compute_banner_risk_score(m1b_pct, rv10, rv30,
                               m1b_history, rv10_history, rv30_history):
    # Trailing 5-year window percentile rank (1260 trading days)
    m1b_rank = (m1b_history <= m1b_pct).mean() * 100
    rv10_rank = (rv10_history <= rv10).mean() * 100
    rv30_rank = (rv30_history <= rv30).mean() * 100

    composite = m1b_rank * 0.361 + rv10_rank * 0.323 + rv30_rank * 0.317

    if composite >= 78.5:
        zone = 'orange'
    elif composite >= 62.1:
        zone = 'yellow'
    else:
        zone = 'green'

    return dict(composite=composite, zone=zone,
                m1b_rank=m1b_rank, rv10_rank=rv10_rank, rv30_rank=rv30_rank)
```

**Orange threshold**: composite >= 78.5 (P85 of 1999-2026 in-sample history)
**Yellow threshold**: composite >= 62.1 (P65)
**Green threshold**: composite < 62.1
