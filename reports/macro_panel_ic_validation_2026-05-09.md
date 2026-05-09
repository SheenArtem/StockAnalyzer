# Macro Panel IC Validation Report
**Generated**: 2026-05-09 16:55
**Panel**: 6130 rows, 42 features
**Date range**: 2006-01-03 ~ 2026-05-08
**Outcome**: future 60d MDD (negative = drawdown)

## Top 15 Features by |IC 60d|

| Rank | Feature | N | IC 60d | IC 40d | IC 20d | Hit top-10% | Lag (days) | Lag IC |
|------|---------|---|--------|--------|--------|-------------|------------|--------|
| 1 | `buffett_indicator_us` | 3087 | -0.371 | -0.329 | -0.281 | 22.3% | 10 | -0.381 |
| 2 | `buffett_indicator_tw` | 3196 | -0.342 | -0.325 | -0.282 | 29.4% | 60 | -0.366 |
| 3 | `tlt_spy_ratio` | 3196 | +0.317 | +0.289 | +0.250 | 20.2% | 3 | +0.319 |
| 4 | `hy_oas` | 890 | +0.295 | +0.168 | +0.173 | 37.4% | 59 | +0.444 |
| 5 | `us_buffett_strict_rank` | 3196 | +0.289 | +0.280 | +0.258 | 54.5% | 0 | +0.289 |
| 6 | `us_durable_yoy` | 3196 | -0.274 | -0.239 | -0.190 | 20.5% | 1 | -0.274 |
| 7 | `fed_bs_trillion` | 3196 | -0.230 | -0.227 | -0.192 | 50.5% | 60 | -0.242 |
| 8 | `st_louis_fsi` | 3196 | +0.229 | +0.169 | +0.102 | 28.4% | 12 | +0.257 |
| 9 | `buffett_rank_tw` | 3196 | -0.221 | -0.177 | -0.104 | 21.2% | 60 | -0.339 |
| 10 | `margin_to_index_ratio` | 3196 | +0.192 | +0.201 | +0.204 | 3.1% | 3 | +0.192 |
| 11 | `foreign_holding_chg_4w` | 3173 | +0.183 | +0.161 | +0.110 | 22.3% | 16 | +0.203 |
| 12 | `us_claims_ma4` | 3196 | +0.181 | +0.134 | +0.091 | 30.6% | 0 | +0.181 |
| 13 | `us_claims_yoy` | 3196 | +0.177 | +0.124 | +0.077 | 22.4% | 57 | +0.224 |
| 14 | `buffett_rank_us` | 2779 | -0.165 | -0.103 | -0.076 | 15.4% | 21 | -0.238 |
| 15 | `short_to_long_ratio` | 3196 | +0.158 | +0.206 | +0.140 | 40.6% | 58 | +0.181 |

## Composite Test (SOP-12)

- **Composite IC 60d**: -0.402
- **Composite IC 40d**: -0.104
- **Composite IC 20d**: -0.035
- **Best single feature**: `buffett_indicator_us` IC 60d = -0.371
- **SOP-12 verdict**: **✅ PASS**
  (Composite IC 較強，absolute |composite| 0.402 vs |best single| 0.371)

## Composite Weights & Directions (Top 10)

| Feature | Weight | Direction | Interpretation |
|---------|--------|-----------|----------------|
| `buffett_indicator_us` | 0.371 | +1 | 高值=danger |
| `buffett_indicator_tw` | 0.342 | +1 | 高值=danger |
| `tlt_spy_ratio` | 0.317 | -1 | 高值=safe (反向) |
| `hy_oas` | 0.295 | -1 | 高值=safe (反向) |
| `us_buffett_strict_rank` | 0.289 | -1 | 高值=safe (反向) |
| `us_durable_yoy` | 0.274 | +1 | 高值=danger |
| `fed_bs_trillion` | 0.230 | +1 | 高值=danger |
| `st_louis_fsi` | 0.229 | -1 | 高值=safe (反向) |
| `buffett_rank_tw` | 0.221 | +1 | 高值=danger |
| `margin_to_index_ratio` | 0.192 | -1 | 高值=safe (反向) |

## All Features (Full Table)

| Feature | N | IC 60d | IC 40d | IC 20d | Hit top-10% | Lag |
|---------|---|--------|--------|--------|-------------|-----|
| `buffett_indicator_us` | 3087 | -0.371 | -0.329 | -0.281 | 22.3% | 10d |
| `buffett_indicator_tw` | 3196 | -0.342 | -0.325 | -0.282 | 29.4% | 60d |
| `tlt_spy_ratio` | 3196 | +0.317 | +0.289 | +0.250 | 20.2% | 3d |
| `hy_oas` | 890 | +0.295 | +0.168 | +0.173 | 37.4% | 59d |
| `us_buffett_strict_rank` | 3196 | +0.289 | +0.280 | +0.258 | 54.5% | 0d |
| `us_durable_yoy` | 3196 | -0.274 | -0.239 | -0.190 | 20.5% | 1d |
| `fed_bs_trillion` | 3196 | -0.230 | -0.227 | -0.192 | 50.5% | 60d |
| `st_louis_fsi` | 3196 | +0.229 | +0.169 | +0.102 | 28.4% | 12d |
| `buffett_rank_tw` | 3196 | -0.221 | -0.177 | -0.104 | 21.2% | 60d |
| `margin_to_index_ratio` | 3196 | +0.192 | +0.201 | +0.204 | 3.1% | 3d |
| `foreign_holding_chg_4w` | 3173 | +0.183 | +0.161 | +0.110 | 22.3% | 16d |
| `us_claims_ma4` | 3196 | +0.181 | +0.134 | +0.091 | 30.6% | 0d |
| `us_claims_yoy` | 3196 | +0.177 | +0.124 | +0.077 | 22.4% | 57d |
| `buffett_rank_us` | 2779 | -0.165 | -0.103 | -0.076 | 15.4% | 21d |
| `short_to_long_ratio` | 3196 | +0.158 | +0.206 | +0.140 | 40.6% | 58d |
| `margin_ratio_z_252d` | 2877 | +0.158 | +0.145 | +0.160 | 18.8% | 13d |
| `us_buffett_strict` | 3196 | -0.156 | -0.142 | -0.127 | 18.5% | 60d |
| `hy_oas_rank` | 638 | +0.155 | -0.078 | -0.114 | 20.3% | 60d |
| `us_sent_yoy` | 3196 | +0.153 | +0.173 | +0.193 | 33.2% | 0d |
| `yield_curve_10y_2y` | 3196 | +0.151 | +0.125 | +0.110 | 17.9% | 0d |
| `hyg_to_lqd_ratio` | 3196 | -0.146 | -0.143 | -0.140 | 25.6% | 2d |
| `vix_close` | 3196 | -0.129 | -0.164 | -0.211 | 26.6% | 0d |
| `chicago_anfci` | 3196 | +0.124 | +0.046 | -0.015 | 32.1% | 17d |
| `us_unemp_chg_12m` | 3196 | +0.104 | +0.047 | -0.020 | 33.1% | 31d |
| `chicago_nfci` | 3196 | +0.097 | +0.014 | -0.069 | 20.8% | 24d |
| `dxy_close` | 3196 | -0.090 | -0.110 | -0.154 | 39.1% | 0d |
| `hyg_chg_4w` | 3196 | +0.087 | +0.128 | +0.109 | 32.5% | 34d |
| `hyg_volume_z_252d` | 3196 | -0.062 | -0.071 | -0.092 | 24.7% | 0d |
| `ad_ratio` | 3196 | +0.043 | +0.082 | +0.122 | 21.8% | 0d |
| `dxy_chg_4w` | 3196 | -0.038 | -0.023 | -0.003 | 23.4% | 52d |
| `new_highs_52w` | 3196 | -0.035 | -0.005 | +0.069 | 22.2% | 15d |
| `ad_diff` | 3196 | +0.032 | +0.055 | +0.079 | 24.7% | 0d |
| `us_unemp_chg_3m` | 3196 | -0.030 | -0.091 | -0.118 | 22.4% | 60d |
| `breadth_thrust_10d` | 3196 | +0.029 | +0.073 | +0.116 | 19.1% | 12d |
| `mcclellan_oscillator` | 3196 | +0.023 | +0.065 | +0.106 | 19.1% | 14d |
| `new_lows_52w` | 3196 | -0.018 | -0.071 | -0.093 | 29.7% | 59d |
| `sbl_change_4w_pct` | 3173 | -0.016 | -0.019 | -0.081 | 22.0% | 57d |
| `new_high_minus_low` | 3196 | -0.012 | +0.026 | +0.087 | 27.7% | 15d |
| `hyg_to_lqd_chg_4w` | 3196 | -0.011 | +0.005 | -0.033 | 17.8% | 37d |
| `fed_bs_chg_4w` | 3196 | +0.009 | -0.007 | +0.003 | 12.1% | 60d |
| `yield_curve_10y_3m` | 3196 | -0.007 | -0.004 | +0.014 | 34.2% | 43d |
| `tlt_spy_chg_4w` | 3196 | +0.005 | -0.029 | -0.029 | 20.9% | 60d |

## Methodology Notes

- **IC**: Spearman rank correlation of feature value at time `t` vs future MDD over `[t, t+H]`
- **Hit top-10%**: when feature is in top decile, what fraction of times MDD ≤ -10% within 60d
- **Best lag**: xcorr peak lag in 0-60 days (≥0 means feature precedes outcome)
- **Composite**: top-10 features weighted by |IC|, signed by direction (-1 if IC > 0 else 1)
- **Composite IC interpretation**: positive = high composite → high MDD risk

## Caveats (SOP 1-14)

- 此驗證為 **continuous outcome** (Spearman IC vs future MDD)，與 System 2 的 N=77 discrete events 互補
- xcorr peak lag 顯示「同期重合 vs 真領先」：lag>0 才是真 leading signal
- 若 SOP-12 FAIL → composite 不接 portfolio gating，僅 informational tier (SOP-14)
- 此驗證 **未做 walk-forward**（in-sample fit）；上線前需另跑 70/30 split
