# VF-Value-ex3 Mohanram G-Score IC Validation — TW

Generated: 2026-04-22 20:09

5-signal G-Lite: ROA / CFOA / Accruals / EarnStd / RevStd (vs per-quarter cross-section median within track)

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
| bear | 6,778 | +2.095 | +37.07% | +26.61% | **+10.46%** |
| bull | 41,350 | +1.111 | +11.65% | +7.83% | **+3.81%** |
| volatile | 8,517 | -0.075 | +29.24% | +43.70% | **-14.46%** |

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
| bear | 69 | +0.041 | +36.03% | +34.90% | **+1.13%** |
| bull | 423 | +0.349 | +18.96% | +18.43% | **+0.54%** |
| volatile | 85 | -0.943 | -7.65% | +21.32% | **-28.97%** |

**Best horizon**: ret_6m IR=+0.102 (C (weak))
- Top(G>=4) vs Bot(G<=1) annualized spread: **-4.12%**
