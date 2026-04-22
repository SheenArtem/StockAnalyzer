# VF-Value-ex3 Mohanram G-Score IC Validation — TW (regime=TWII)

Generated: 2026-04-22 20:42

5-signal G-Lite: ROA / CFOA / Accruals / EarnStd / RevStd (vs per-quarter cross-section median within track)

Regime benchmark: **TWII** (MA200 slope + 20d realized vol, threshold 25%)

Grade: A (|IR|>=0.5) / B (|IR|>=0.3) / C (|IR|>=0.1) / D (noise)

## All Market

- Sample: 56,645 (ticker, quarter) obs after price join, 2216 tickers, 33 quarters
- Quarter range: 2016-12-31 ~ 2024-12-31

### G-Score distribution

| G | Count | Pct |
|---|---|---|
| 0 | 5,695 | 9.4% |
| 1 | 10,984 | 18.0% |
| 2 | 11,400 | 18.7% |
| 3 | 14,839 | 24.4% |
| 4 | 9,911 | 16.3% |
| 5 | 8,025 | 13.2% |

### IC Summary (Spearman, g_score vs forward return)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Q | Grade |
|---|---|---|---|---|---|---|---|
| ret_3m | +0.0713 | 0.0696 | +1.024 | +5.88 | 84.8% | 33 | A (strong) |
| ret_6m | +0.0726 | 0.0741 | +0.981 | +5.63 | 84.8% | 33 | A (strong) |
| ret_12m | +0.0706 | 0.0768 | +0.919 | +5.28 | 78.8% | 33 | A (strong) |

### Top (G>=4) vs Bot (G<=1) annualized returns

| Horizon | Top Ann | Bot Ann | Spread | N Q top | N Q bot |
|---|---|---|---|---|---|
| ret_3m | +18.26% | +17.04% | **+1.22%** | 33 | 33 |
| ret_6m | +18.55% | +18.81% | **-0.26%** | 33 | 33 |
| ret_12m | +17.70% | +17.33% | **+0.37%** | 33 | 33 |

### By Regime (IC IR 6m / Top-Bot spread ann)

| Regime | N obs | IC IR 6m | Top ann | Bot ann | Spread |
|---|---|---|---|---|---|
| bear | 11,881 | +1.688 | +26.92% | +18.46% | **+8.45%** |
| bull | 43,030 | +0.804 | +11.91% | +17.72% | **-5.81%** |
| volatile | 1,734 | +nan | +91.52% | -35.70% | **+127.23%** |

**Best horizon**: ret_3m IR=+1.024 (A (strong))
- Top(G>=4) vs Bot(G<=1) annualized spread: **+1.22%**

## Financials Subset

- Sample: 577 (ticker, quarter) obs after price join, 21 tickers, 33 quarters
- Quarter range: 2016-12-31 ~ 2024-12-31

### G-Score distribution

| G | Count | Pct |
|---|---|---|
| 0 | 51 | 7.6% |
| 1 | 175 | 26.2% |
| 2 | 130 | 19.4% |
| 3 | 223 | 33.3% |
| 4 | 59 | 8.8% |
| 5 | 31 | 4.6% |

### IC Summary (Spearman, g_score vs forward return)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Q | Grade |
|---|---|---|---|---|---|---|---|
| ret_3m | +0.0246 | 0.2699 | +0.091 | +0.52 | 54.5% | 33 | D (noise) |
| ret_6m | +0.0304 | 0.2967 | +0.102 | +0.59 | 54.5% | 33 | C (weak) |
| ret_12m | +0.0133 | 0.2971 | +0.045 | +0.26 | 57.6% | 33 | D (noise) |

### Top (G>=4) vs Bot (G<=1) annualized returns

| Horizon | Top Ann | Bot Ann | Spread | N Q top | N Q bot |
|---|---|---|---|---|---|
| ret_3m | +17.01% | +17.53% | **-0.52%** | 33 | 33 |
| ret_6m | +16.69% | +20.81% | **-4.12%** | 33 | 33 |
| ret_12m | +14.64% | +18.34% | **-3.69%** | 33 | 33 |

### By Regime (IC IR 6m / Top-Bot spread ann)

| Regime | N obs | IC IR 6m | Top ann | Bot ann | Spread |
|---|---|---|---|---|---|
| bear | 120 | -0.020 | +11.52% | +16.98% | **-5.45%** |
| bull | 440 | +0.213 | +18.21% | +19.52% | **-1.32%** |

**Best horizon**: ret_6m IR=+0.102 (C (weak))
- Top(G>=4) vs Bot(G<=1) annualized spread: **-4.12%**

---

## SPY vs TWII Regime — Side-by-Side (All Market)

Underlying panel is identical (56,645 obs, 2016Q4 ~ 2024Q4). Only the regime benchmark changes.

### Regime distribution

| Regime | SPY obs | TWII obs | delta |
|---|---|---|---|
| bear | 6,778 | 11,881 | +5,103 (TWII classifies more months as bear) |
| bull | 41,350 | 43,030 | +1,680 |
| volatile | 8,517 | 1,734 | -6,783 (TWII volatility threshold rarely hit) |

SPY bear months 2016+: 2016-04~05, 2018-11~12, 2019-06, 2022-07~09, 2022-12~2023-03 (12 months).
TWII bear months 2016+: 2016-01~06, 2018-11~2019-04, 2019-06~07, 2020-05, 2022-05~06, 2022-08~09, 2022-11~2023-04, 2025-05~06 (27 months).
TWII captures TW-specific drawdowns (2020 COVID local bottom, 2025 tariff shock) that SPY labels bull.

### ret_6m IC IR by regime

| Regime | N quarters | Mean IC | IR | t-stat | % positive |
|---|---|---|---|---|---|
| **SPY bear** | 4 | +0.123 | **+2.095** | +4.19 | 100.0% |
| **TWII bear** | 7 | +0.114 | **+1.688** | +4.47 | 85.7% |
| SPY bull | 25 | +0.090 | +1.111 | +5.56 | 92.0% |
| TWII bull | 26 | +0.079 | +0.804 | +4.10 | 84.6% |
| SPY volatile | 5 | -0.006 | -0.075 | -0.17 | 20.0% |
| TWII volatile | 1 | n/a | n/a | n/a | n/a |

Under TWII regime: **bear IR drops +2.095 -> +1.688 (-19%)**, but sample size **roughly doubles** (4q -> 7q,
6,778 obs -> 11,881 obs). Both t-stat > 4 so the bear-alpha effect is real with either benchmark.

### G=5 vs rest — ret_6m hit rate & mean (regime by TWII)

| Regime | n G=5 | n rest | G=5 hit rate | rest hit rate | G=5 mean ret_6m | rest mean ret_6m |
|---|---|---|---|---|---|---|
| bear | 1,757 | 10,123 | **74.84%** | 60.69% | **+13.62%** | +10.80% |
| bull | 5,876 | 37,096 | 56.28% | 48.50% | +6.81% | +6.66% |
| volatile | 262 | 1,445 | 97.33% | 89.55% | +37.51% | +44.79% |

G=5 vs rest (regime by SPY, same panel):

| Regime | n G=5 | n rest | G=5 hit rate | rest hit rate | G=5 mean ret_6m | rest mean ret_6m |
|---|---|---|---|---|---|---|
| bear | 1,023 | 5,754 | **83.28%** | 67.03% | **+18.56%** | +14.98% |
| bull | 5,609 | 35,656 | 56.09% | 47.56% | +6.83% | +5.92% |
| volatile | 1,263 | 7,254 | 69.60% | 63.62% | +13.02% | +17.09% |

TWII bear: G=5 hits 74.84% (vs rest 60.69%), **edge +14.15pp**.
SPY bear: G=5 hits 83.28% (vs rest 67.03%), **edge +16.25pp**.
TWII-bear hit-rate edge is only slightly smaller than SPY-bear (-2pp).

### Decile spread G=5 vs G<=1 (ret_6m, annualized)

| Regime | SPY top_ann | SPY bot_ann | SPY spread | TWII top_ann | TWII bot_ann | TWII spread |
|---|---|---|---|---|---|---|
| bear | +40.56% | +26.45% | **+14.11%** | +29.10% | +18.06% | **+11.03%** |
| bull | +14.13% | +12.43% | +1.70% | +14.08% | +14.65% | -0.58% |
| volatile | +27.74% | +40.11% | -12.37% | +89.09% | +126.39% | -37.30% (n=262, noise) |

---

## Conclusion (TWII regime)

### TW G=5 bear alpha survives TWII regime check

- **IR 6m**: SPY +2.095 -> TWII +1.688 (still grade A, same direction)
- **Sample thickness**: SPY 6,778 obs / 4 quarters -> TWII 11,881 obs / 7 quarters (**77% bigger, more robust**)
- **G=5 vs rest hit rate edge**: SPY +16.2pp -> TWII +14.2pp
- **G=5 vs bot decile spread (ret_6m ann)**: SPY +14.1% -> TWII +11.0%

The original worry — "SPY bear IR +2.10 might not hold under TWII regime" — is resolved.
Both regime definitions show the same alpha in TW bear markets; TWII just has a wider bear window
and therefore slightly diluted IR but larger sample.

### Bull / volatile regimes

- Bull: IR drops 1.111 -> 0.804 but still grade A. G=5 top-bot spread flips slightly negative in TWII bull
  (-0.58% vs SPY +1.70%), so **bull is not the entry condition**.
- Volatile: TWII volatile has only 262 G=5 obs and 1 quarter — statistically meaningless, ignore.

### Decision

**Promote TW G=5 quintile filter from D (archive) to B (conditional candidate) for VF-Value-ex2 TW test**,
conditioned on TWII regime = bear.

Gating rule for VF-Value-ex2 TW shadow run:
- When TWII in bear regime (MA200 slope < 0), use `g_score >= 5` as a quality filter for Value candidates.
- When TWII not bear (bull / ranged / volatile), do NOT apply G-filter (alpha vanishes or reverses).

Expected behavior: G=5 + bear regime should raise hit rate from 60.69% to 74.84% (+14pp), and
6-month mean return from +10.80% to +13.62% (+2.82pp) — matching what we saw in VF-Value-ex2 US
F-Score bear-regime analysis (but this time in the correct direction).

### Financials subset — still D

TW Financials (21 tickers, 577 obs) remains noise-grade under TWII regime too:
- All-horizon IR 0.04 ~ 0.10
- TWII bear (69 obs, 2 quarters): IR -0.02, spread n/a
- Sample too thin + G distribution too concentrated (G<=2 = 53% of obs).

Archive Financials-subset. Not useful as a sector-specific filter.
