# Line 3 — 流動性當 regime 狀態變數驗證

Generated: 2026-06-08 17:47

## 資料 / 清洗揭露

- Panel: `data_cache/backtest/ohlcv_tw.parquet` (clean)
- 剔 Close<=0: 0 列 / 剔 Volume<=0 凍結列: 112,473 列 → 保留 5,306,886 列
- Regime: **自 clean panel 重算** (top300 equal-weight, 同 market_regime_logger 規則, 聚合前剔 Volume<=0)。jsonl 2026-04-28+ 受凍結/尖刺價污染 (ret_20d 110-180%) 不採用。
- Regime 分布 (clean): {'volatile': 2213, 'neutral': 1258, 'ranging': 982, 'trending': 530}
- ⚠️ survivor-bias caveat: panel 為現存 universe，下市股缺漏 → 報酬/spread 可能虛高。
- ⚠️ 無 AdjClose → raw Close (除息 gap 壓低高動能股 fwd return)。
- ⚠️ turnover 需流通股數 (financials 2015-03+) → turnover 結論僅 2015+。

## Part A — turnover / Amihud × regime decile spread (clean panel)

**重驗目標**: 舊 `vf_turnover_summary.md` 宣稱 turnover_20d 在 volatile regime IC IR=**+0.71** (A 級)、D10-D1 spread 年化 **+128%** (跑在 survivor-biased trade_journal)。

### IC by regime (Spearman, 20d & 40d)

| Factor | Regime | Horizon | Mean IC | IC IR | t-stat | Win% | N days |
|---|---|---|---|---|---|---|---|
| turnover_20d | all | 20d | -0.0314 | -0.169 | -8.73 | 44.5% | 2678 |
| turnover_20d | all | 40d | -0.0378 | -0.219 | -11.27 | 44.1% | 2658 |
| turnover_20d | volatile | 20d | +0.0064 | +0.033 | +1.16 | 53.1% | 1233 |
| turnover_20d | volatile | 40d | +0.0030 | +0.016 | +0.57 | 52.4% | 1213 |
| turnover_20d | neutral | 20d | -0.0728 | -0.413 | -10.85 | 35.5% | 690 |
| turnover_20d | neutral | 40d | -0.0706 | -0.434 | -11.40 | 38.0% | 690 |
| turnover_20d | ranging | 20d | -0.0442 | -0.259 | -5.60 | 42.1% | 468 |
| turnover_20d | ranging | 40d | -0.0643 | -0.421 | -9.11 | 38.5% | 468 |
| turnover_20d | trending | 20d | -0.0732 | -0.471 | -7.98 | 33.1% | 287 |
| turnover_20d | trending | 40d | -0.0880 | -0.590 | -10.00 | 32.4% | 287 |
| amihud_20d | all | 20d | +0.0043 | +0.028 | +1.97 | 49.2% | 4968 |
| amihud_20d | all | 40d | +0.0149 | +0.094 | +6.62 | 51.3% | 4948 |
| amihud_20d | volatile | 20d | +0.0009 | +0.005 | +0.24 | 46.3% | 2193 |
| amihud_20d | volatile | 40d | +0.0106 | +0.062 | +2.90 | 51.2% | 2173 |
| amihud_20d | neutral | 20d | +0.0034 | +0.023 | +0.83 | 51.1% | 1258 |
| amihud_20d | neutral | 40d | +0.0087 | +0.058 | +2.07 | 47.9% | 1258 |
| amihud_20d | ranging | 20d | -0.0040 | -0.029 | -0.92 | 48.0% | 982 |
| amihud_20d | ranging | 40d | +0.0190 | +0.125 | +3.91 | 51.9% | 982 |
| amihud_20d | trending | 20d | +0.0380 | +0.283 | +6.52 | 59.4% | 530 |
| amihud_20d | trending | 40d | +0.0402 | +0.286 | +6.59 | 58.5% | 530 |

### Decile spread by regime (D10-D1, annualized)

| Factor | Regime | Horizon | D1 ret | D10 ret | LS spread | LS ann | mono rho | N days |
|---|---|---|---|---|---|---|---|---|
| turnover_20d | all | 20d | +0.0069 | +0.0166 | +0.0097 | +12.93% | +0.988 | 2678 |
| turnover_20d | all | 40d | +0.0155 | +0.0322 | +0.0167 | +10.97% | +0.952 | 2658 |
| turnover_20d | volatile | 20d | +0.0113 | +0.0339 | +0.0226 | +32.52% | +1.000 | 1233 |
| turnover_20d | volatile | 40d | +0.0228 | +0.0620 | +0.0392 | +27.38% | +1.000 | 1213 |
| turnover_20d | neutral | 20d | +0.0023 | -0.0031 | -0.0054 | -6.59% | -0.770 | 690 |
| turnover_20d | neutral | 40d | +0.0108 | +0.0074 | -0.0035 | -2.16% | -0.515 | 690 |
| turnover_20d | ranging | 20d | +0.0019 | +0.0052 | +0.0033 | +4.19% | +0.479 | 468 |
| turnover_20d | ranging | 40d | +0.0064 | +0.0043 | -0.0021 | -1.30% | -0.600 | 468 |
| turnover_20d | trending | 20d | +0.0070 | +0.0080 | +0.0010 | +1.31% | -0.188 | 287 |
| turnover_20d | trending | 40d | +0.0108 | +0.0112 | +0.0004 | +0.24% | -0.552 | 287 |
| amihud_20d | all | 20d | +0.0106 | +0.0207 | +0.0101 | +13.47% | +0.867 | 4968 |
| amihud_20d | all | 40d | +0.0211 | +0.0429 | +0.0218 | +14.55% | +0.879 | 4948 |
| amihud_20d | volatile | 20d | +0.0165 | +0.0247 | +0.0083 | +10.91% | +0.915 | 2193 |
| amihud_20d | volatile | 40d | +0.0286 | +0.0490 | +0.0204 | +13.56% | +0.891 | 2173 |
| amihud_20d | neutral | 20d | +0.0044 | +0.0151 | +0.0107 | +14.38% | +0.806 | 1258 |
| amihud_20d | neutral | 40d | +0.0151 | +0.0353 | +0.0202 | +13.43% | +0.794 | 1258 |
| amihud_20d | ranging | 20d | +0.0075 | +0.0149 | +0.0074 | +9.74% | +0.758 | 982 |
| amihud_20d | ranging | 40d | +0.0172 | +0.0390 | +0.0218 | +14.58% | +0.758 | 982 |
| amihud_20d | trending | 20d | +0.0070 | +0.0285 | +0.0215 | +30.77% | +0.830 | 530 |
| amihud_20d | trending | 40d | +0.0121 | +0.0434 | +0.0313 | +21.42% | +0.818 | 530 |

### Part A 裁決 — turnover D10-D1 long-short 可交易性 (RVOL/ATR killer test)

decile spread 是否在 **真實 portfolio (淨成本)** 存活，還是像 RVOL/ATR 一樣 rank 看似有訊號但籃子 LS 歸零/反轉 → non-tradeable。每日 rebalance long D10 / short D1, 吃 next-1d return；net_monthly = 月度換手近似成本, net_daily = 每日全換 (保守上界)。

| Regime filter | hold days | long ann | short ann | gross ann LS | gross Sharpe | net_monthly ann LS | net_monthly Sharpe | net_daily Sharpe | gross MDD |
|---|---|---|---|---|---|---|---|---|---|
| all_regime | 2697 | +24.03% | +7.94% | +14.91% | 0.490 | +7.90% | 0.268 | -3.950 | -54.59% |
| volatile_only | 1252 | +56.17% | +10.24% | +41.68% | 1.156 | +33.04% | 0.947 | -3.023 | -33.63% |
| volatile_2015 | 79 | +57.27% | -2.91% | +61.97% | 1.390 | +52.10% | 1.208 | -2.238 | -17.47% |
| volatile_2016 | 109 | +63.25% | +36.19% | +19.90% | 0.623 | +12.58% | 0.406 | -3.698 | -11.25% |
| volatile_2017 | 79 | +38.06% | +22.87% | +12.37% | 0.545 | +5.51% | 0.251 | -5.337 | -13.50% |
| volatile_2018 | 143 | -22.21% | -0.01% | -22.21% | -0.730 | -26.96% | -0.914 | -4.397 | -33.63% |
| volatile_2019 | 76 | +47.02% | +31.06% | +12.19% | 0.614 | +5.34% | 0.278 | -6.112 | -9.47% |
| volatile_2020 | 102 | +143.50% | -5.22% | +156.85% | 3.337 | +141.22% | 3.115 | -1.112 | -8.94% |
| volatile_2021 | 121 | +65.51% | +14.40% | +44.71% | 1.088 | +35.88% | 0.903 | -2.619 | -29.05% |
| volatile_2022 | 168 | -0.23% | -2.77% | +2.61% | 0.089 | -3.65% | -0.129 | -4.272 | -17.94% |
| volatile_2023 | 74 | +265.07% | +22.67% | +197.86% | 4.538 | +179.75% | 4.276 | -0.689 | -7.67% |
| volatile_2024 | 83 | +30.85% | +17.69% | +11.19% | 0.333 | +4.40% | 0.135 | -3.627 | -14.28% |
| volatile_2025 | 119 | +40.69% | +4.04% | +35.23% | 1.047 | +26.98% | 0.828 | -3.320 | -13.98% |
| volatile_2026 | 97 | +304.95% | +22.16% | +231.80% | 3.293 | +211.63% | 3.121 | -0.158 | -10.54% |

## Part B — aggregate-liquidity regime GATE (SOP-10~14)

**設計**: 既有訊號 = 截面 20d 動量 top-quintile (流動性過濾 avg_amount>=5000萬, daily equal-weight, round-trip 0.25% 換倉摩擦)。state variable = 市場整體 Amihud λ (top300 daily_illiq cross-sectional median, 20d 平滑, rolling 252d percentile rank)。

### SOP-10 portfolio gating sim (B&H + single + composite)

| Strategy | CAGR | Sharpe | MDD | cash% | N days |
|---|---|---|---|---|---|
| BH_momentum | +24.29% | 1.000 | -60.76% | 0.0% | 4982 |
| gate_illiquidity | +22.72% | 1.119 | -36.26% | 29.6% | 4982 |
| gate_liquidity(rev) | -0.19% | 0.072 | -60.99% | 68.0% | 4982 |
| gate_regime_volatile | +5.54% | 0.415 | -46.37% | 44.4% | 4982 |
| composite_illiq_OR_vol | +5.47% | 0.423 | -42.32% | 48.9% | 4982 |

- SOP-12 check: composite Sharpe 0.423 vs best-single 1.119 → FAIL
- B&H momentum Sharpe 1.000 (基準)

### SOP-13 xcorr lag (aggregate illiq vs forward 20d momentum drawdown)

- peak |corr| lag = **-1d** → **coincident_or_lagging**
- cash_pct of best gate: see gating table; >30% → low_exposure_artifact

| lag (d) | corr(illiq_t, dd_t+lag) |
|---|---|
| -40 | -0.0585 |
| -30 | -0.1415 |
| -20 | -0.2702 |
| -15 | -0.3411 |
| -10 | -0.3936 |
| -5 | -0.4310 |
| -3 | -0.4400 |
| -1 | -0.4428 |
| 0 | -0.4412 |
| 1 | -0.4392 |
| 3 | -0.4354 |
| 5 | -0.4315 |
| 10 | -0.4243 |
| 15 | -0.4174 |
| 20 | -0.4089 |
| 30 | -0.3998 |
| 40 | -0.4052 |

### SOP-14 episode / strict-fire count

- danger episodes (illiq pct>=0.80 onset): **50**
- danger days: 984
- strict-fire (onset → mom fwd_20d < -1%): **20**
- **SOP-14 gate: eligible** (<30 episodes OR <=5 strict-fire → informational_only)

### LOYO + COVID strip + WF annual (BH vs gated)

| Split | Strategy | CAGR | Sharpe | MDD | cash% |
|---|---|---|---|---|---|
| year_2006 | BH | +18.73% | 0.912 | -24.63% | 0.0% |
| year_2006 | gated | +14.64% | 0.815 | -24.63% | 18.1% |
| year_2007 | BH | +21.76% | 0.960 | -24.41% | 0.0% |
| year_2007 | gated | +39.30% | 1.622 | -19.69% | 18.5% |
| year_2008 | BH | -40.05% | -1.226 | -56.29% | 0.0% |
| year_2008 | gated | -9.55% | -1.086 | -14.86% | 87.1% |
| year_2009 | BH | +114.26% | 2.442 | -17.80% | 0.0% |
| year_2009 | gated | +114.26% | 2.442 | -17.80% | 0.0% |
| year_2010 | BH | +18.80% | 0.959 | -20.60% | 0.0% |
| year_2010 | gated | -6.43% | -0.118 | -27.60% | 23.6% |
| year_2011 | BH | -24.98% | -0.965 | -30.10% | 0.0% |
| year_2011 | gated | -3.29% | -0.228 | -11.48% | 74.5% |
| year_2012 | BH | +18.54% | 0.786 | -20.11% | 0.0% |
| year_2012 | gated | +0.23% | 0.127 | -22.65% | 12.6% |
| year_2013 | BH | +42.11% | 2.095 | -8.77% | 0.0% |
| year_2013 | gated | +42.11% | 2.095 | -8.77% | 0.0% |
| year_2014 | BH | +25.88% | 1.384 | -13.01% | 0.0% |
| year_2014 | gated | +24.29% | 1.606 | -8.83% | 34.4% |
| year_2015 | BH | +3.04% | 0.352 | -24.90% | 0.0% |
| year_2015 | gated | +3.58% | 0.464 | -10.54% | 42.6% |
| year_2016 | BH | +8.45% | 0.566 | -17.91% | 0.0% |
| year_2016 | gated | -8.49% | -0.430 | -18.12% | 29.9% |
| year_2017 | BH | +59.42% | 2.307 | -9.91% | 0.0% |
| year_2017 | gated | +59.42% | 2.307 | -9.91% | 0.0% |
| year_2018 | BH | -6.89% | -0.226 | -33.69% | 0.0% |
| year_2018 | gated | -4.77% | -0.323 | -11.48% | 66.4% |
| year_2019 | BH | +36.05% | 1.985 | -11.50% | 0.0% |
| year_2019 | gated | +26.42% | 1.585 | -11.50% | 13.2% |
| year_2020 | BH | +71.59% | 2.167 | -25.22% | 0.0% |
| year_2020 | gated | +44.33% | 1.609 | -18.65% | 17.1% |
| year_2021 | BH | +48.00% | 1.477 | -23.28% | 0.0% |
| year_2021 | gated | +27.72% | 1.050 | -23.28% | 28.3% |
| year_2022 | BH | -30.04% | -1.239 | -37.33% | 0.0% |
| year_2022 | gated | -18.09% | -1.243 | -18.40% | 64.6% |
| year_2023 | BH | +41.06% | 1.693 | -15.02% | 0.0% |
| year_2023 | gated | +41.06% | 1.693 | -15.02% | 0.0% |
| year_2024 | BH | +40.19% | 1.488 | -19.89% | 0.0% |
| year_2024 | gated | +13.44% | 0.720 | -26.31% | 31.8% |
| year_2025 | BH | +55.10% | 1.824 | -27.92% | 0.0% |
| year_2025 | gated | +51.25% | 2.209 | -13.91% | 23.5% |
| year_2026 | BH | +442.40% | 4.794 | -11.44% | 0.0% |
| year_2026 | gated | +442.40% | 4.794 | -11.44% | 0.0% |
| ex_2020_covid | BH | +21.04% | 0.938 | -60.76% | 0.0% |
| ex_2020_covid | gated | +20.50% | 1.089 | -36.26% | 30.2% |

## 最終裁決

**Part A — turnover×volatile**:
- 舊宣稱 IC IR +0.71 / spread +128% → clean panel 實際 **IC IR +0.033 (噪音)** / decile spread **+32.5% ann (20d), mono rho 1.00**。IC 與 decile 分歧 = alpha 在 tail 非 rank。
- LS portfolio net_monthly Sharpe (volatile-only) pooled **0.95**，但逐年 mean 1.12 / median 0.62，pos 10/12 yrs。
- **剔 2020/2023/2026 三爆發年後 Sharpe 崩到 mean 0.33**；3 outlier 年 net_monthly ann_ls 平均 178% vs 其他 9 年 12% (~14x 集中)。
- **volatile-DOWN 年 (2018/2022) 翻負** [2018, 2022] → 'volatile' regime 把 melt-up / melt-down 混為一談，turnover×volatile 實為**偽裝的 high-beta/動量 tilt**，牛市波動賺、熊市波動賠。
- net_daily Sharpe 全負 (每日全換成本殺死)；只有 monthly rebalance 才有 gross spread。
- **裁決: informational_only (非穩健)** — 非真 alpha，是 survivor + outlier-year 假象。舊 +0.71/+128% 推翻。

**Part B — aggregate-liquidity regime gate**:
- gate_illiquidity Sharpe 1.119 vs BH momentum 1.000 (+0.119)，但 CAGR *較低* (22.7% vs 24.3%)，cash_pct **29.6%** (逼近 30% artifact 線)。
- **SOP-12 FAIL**: composite Sharpe 0.423 << best-single 1.119。
- **SOP-13**: peak xcorr lag **-1d → coincident_or_lagging** (illiq 與 drawdown 同期甚至滯後，非領先)。
- LOYO: gate 僅 6/21 年 Sharpe 勝 BH、6/21 年 CAGR 勝；勝的年全是危機年靠 cash 減 MDD → 純 cash-drag，非 timing skill。
- **裁決: informational_only / reject as gate** — Sharpe 微升全來自 ~30% 現金的 MDD 縮減，非預測力。

**一句話**: 流動性 (Amihud λ / turnover) 當 regime 狀態變數 **gate 不出穩健 alpha**。turnover×volatile 的高 spread 是 2-3 個 melt-up 年 + survivor bias 的假象 (熊市波動反向)；aggregate illiquidity gate 是同期 (非領先) 風險指標，Sharpe 微升純 cash-drag。兩者皆 **informational_only**，禁 banner / rebalance / hard_rule / position_size。
