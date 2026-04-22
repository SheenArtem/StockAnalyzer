# VF-Value-ex3 Mohanram G-Score IC Validation — US

Generated: 2026-04-22 20:07

5-signal G-Lite: ROA / CFOA / Accruals / EarnStd / RevStd (vs per-quarter cross-section median within track)

Grade: A (|IR|>=0.5) / B (|IR|>=0.3) / C (|IR|>=0.1) / D (noise)

## All Market

- Sample: 74,499 (ticker, quarter) obs after price join, 1500 tickers, 57 quarters
- Quarter range: 2010-12-31 ~ 2024-12-31

### G-Score distribution

| G | Count | Pct |
|---|---|---|
| 0 | 1,520 | 2.0% |
| 1 | 12,298 | 16.5% |
| 2 | 19,429 | 26.0% |
| 3 | 21,769 | 29.2% |
| 4 | 13,567 | 18.2% |
| 5 | 6,012 | 8.1% |

### IC Summary (Spearman, g_score vs forward return)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Q | Grade |
|---|---|---|---|---|---|---|---|
| ret_3m | -0.0077 | 0.0872 | -0.088 | -0.66 | 35.1% | 57 | D (noise) |
| ret_6m | -0.0149 | 0.0894 | -0.166 | -1.25 | 35.1% | 57 | C (weak) |
| ret_12m | +0.0093 | 0.0773 | +0.121 | +0.91 | 66.7% | 57 | C (weak) |

### Top (G>=4) vs Bot (G<=1) annualized returns

| Horizon | Top Ann | Bot Ann | Spread | N Q top | N Q bot |
|---|---|---|---|---|---|
| ret_3m | +3.76% | +11.03% | **-7.27%** | 57 | 57 |
| ret_6m | +8.71% | +16.96% | **-8.24%** | 57 | 57 |
| ret_12m | +11.00% | +19.73% | **-8.73%** | 57 | 57 |

### By Regime (IC IR 6m / Top-Bot spread ann)

| Regime | N obs | IC IR 6m | Top ann | Bot ann | Spread |
|---|---|---|---|---|---|
| bear | 5,479 | +0.080 | +11.10% | -12.32% | **+23.43%** |
| bull | 39,565 | -0.249 | +9.90% | +88.10% | **-78.20%** |
| ranged | 23,856 | -0.365 | +0.34% | +1.90% | **-1.55%** |
| volatile | 5,599 | -0.217 | +23.14% | +35.34% | **-12.20%** |

**Best horizon**: ret_6m IR=-0.166 (C (weak))
- Top(G>=4) vs Bot(G<=1) annualized spread: **-8.24%**

## Financials Subset

- Sample: 13,055 (ticker, quarter) obs after price join, 251 tickers, 57 quarters
- Quarter range: 2010-12-31 ~ 2024-12-31

### G-Score distribution

| G | Count | Pct |
|---|---|---|
| 0 | 346 | 2.7% |
| 1 | 1,972 | 15.1% |
| 2 | 4,159 | 31.9% |
| 3 | 3,959 | 30.3% |
| 4 | 2,281 | 17.5% |
| 5 | 338 | 2.6% |

### IC Summary (Spearman, g_score vs forward return)

| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N Q | Grade |
|---|---|---|---|---|---|---|---|
| ret_3m | +0.0173 | 0.1573 | +0.110 | +0.83 | 50.9% | 57 | C (weak) |
| ret_6m | -0.0000 | 0.1659 | -0.000 | -0.00 | 49.1% | 57 | D (noise) |
| ret_12m | +0.0314 | 0.1532 | +0.205 | +1.55 | 63.2% | 57 | C (weak) |

### Top (G>=4) vs Bot (G<=1) annualized returns

| Horizon | Top Ann | Bot Ann | Spread | N Q top | N Q bot |
|---|---|---|---|---|---|
| ret_3m | +8.27% | +8.65% | **-0.38%** | 57 | 57 |
| ret_6m | +13.09% | +16.46% | **-3.37%** | 57 | 57 |
| ret_12m | +12.75% | +16.69% | **-3.93%** | 57 | 57 |

### By Regime (IC IR 6m / Top-Bot spread ann)

| Regime | N obs | IC IR 6m | Top ann | Bot ann | Spread |
|---|---|---|---|---|---|
| bear | 953 | +0.304 | +12.71% | +11.77% | **+0.93%** |
| bull | 6,768 | +0.066 | +14.39% | +16.25% | **-1.87%** |
| ranged | 4,365 | -0.372 | +8.83% | +9.48% | **-0.66%** |
| volatile | 969 | +0.241 | +27.48% | +10.92% | **+16.56%** |

**Best horizon**: ret_12m IR=+0.205 (C (weak))
- Top(G>=4) vs Bot(G<=1) annualized spread: **-3.93%**
