# 格1：量價彈性 (Kyle lambda / Amihud) 背離 — SOP-14 gauntlet 結果

_產出 2026-06-08 18:30 / FULL universe_

## 資料 caveat

- panel `data_cache/backtest/ohlcv_tw.parquet`：2054 tickers, 2006-01-02~2026-06-05
- **Volume<=0 剔除 112,473 列** (停牌凍結列；lambda Amihud 分母除零地雷) + Close<=0 剔除 0 列
- lambda_raw cross-section winsorize 1%/99% 後 rank/z
- **survivor-biased**：此 panel PIT 不完整 (~46% 缺價 backlog)、下市股被排除，回測結果偏樂觀；**不可宣稱 PIT-clean**


## 訊號 `sig_lambda_level` — lambda level (低衝擊=高分)

方向 (D10>D1?): **bot**

IC matrix (liq_50m):

| H | IC | IR | t | win% |
|---|---|---|---|---|
| 10 | +0.0279 | +0.186 | +13.13 | 58.6% |
| 20 | +0.0293 | +0.194 | +13.69 | 61.2% |
| 60 | +0.0372 | +0.240 | +16.85 | 65.9% |

Decile (h=20):

| tier | D1% | D10% | D10-D1% | mono_rho |
|---|---|---|---|---|
| all | +2.053 | +1.062 | -0.991 | -0.867 |
| liq_50m | +1.262 | +1.116 | -0.145 | -0.491 |
| liq_100m | +1.510 | +1.141 | -0.368 | -0.127 |
| ex_bottom20pct | +1.616 | +1.086 | -0.531 | -0.697 |

Net spread (h=20 liq_50m, long-short 2 腿):

| cost RT | gross% | net% | sign_ok |
|---|---|---|---|
| 0.25% | +0.145 | -0.355 | False |
| 0.35% | +0.145 | -0.555 | False |

- **Walk-forward** (liq_50m, 21 年)：signed-IC 正 3/21 年, LS-spread 正 10/21 年
- **LOYO**：full +0.145% / ex-2020 +0.054% / drop-year range [-0.033%, +0.242%]
- **Cross-regime** (spread 正 1/3 cell)：low_vol IC=+0.0394 sp=-0.17%, mid_vol IC=+0.0147 sp=+0.68%, high_vol IC=+0.0388 sp=-0.27%
- **Deflated Sharpe** h=10 (N_trials=72)：SR_ann +0.138, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False
- **Deflated Sharpe** h=20 (N_trials=72)：SR_ann +0.092, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False
- **Deflated Sharpe** h=60 (N_trials=72)：SR_ann +0.078, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False

## 訊號 `sig_lambda_diverge` — lambda 背離 (價漲+lambda 降=吸籌)

方向 (D10>D1?): **top**

IC matrix (liq_50m):

| H | IC | IR | t | win% |
|---|---|---|---|---|
| 10 | -0.0096 | -0.066 | -4.67 | 47.5% |
| 20 | -0.0179 | -0.130 | -9.19 | 43.0% |
| 60 | -0.0040 | -0.028 | -1.99 | 50.1% |

Decile (h=20):

| tier | D1% | D10% | D10-D1% | mono_rho |
|---|---|---|---|---|
| all | +1.119 | +1.947 | +0.828 | +0.842 |
| liq_50m | +0.992 | +1.620 | +0.628 | +0.867 |
| liq_100m | +0.984 | +1.552 | +0.568 | +0.673 |
| ex_bottom20pct | +0.950 | +1.820 | +0.870 | +0.867 |

Net spread (h=20 liq_50m, long-short 2 腿):

| cost RT | gross% | net% | sign_ok |
|---|---|---|---|
| 0.25% | +0.628 | +0.128 | False |
| 0.35% | +0.628 | -0.072 | False |

- **Walk-forward** (liq_50m, 21 年)：signed-IC 正 5/21 年, LS-spread 正 15/21 年
- **LOYO**：full +0.628% / ex-2020 +0.508% / drop-year range [+0.508%, +0.744%]
- **Cross-regime** (spread 正 3/3 cell)：low_vol IC=-0.0185 sp=+0.31%, mid_vol IC=-0.0115 sp=+0.71%, high_vol IC=-0.0258 sp=+0.84%
- **Deflated Sharpe** h=10 (N_trials=72)：SR_ann +0.759, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False
- **Deflated Sharpe** h=20 (N_trials=72)：SR_ann +0.385, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False
- **Deflated Sharpe** h=60 (N_trials=72)：SR_ann +0.407, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False

## 訊號 `sig_lambda_diverge_signed` — lambda 背離(帶符號 CLV)

方向 (D10>D1?): **top**

IC matrix (liq_50m):

| H | IC | IR | t | win% |
|---|---|---|---|---|
| 10 | -0.0020 | -0.013 | -0.91 | 50.8% |
| 20 | -0.0093 | -0.064 | -4.54 | 47.2% |
| 60 | +0.0034 | +0.023 | +1.62 | 53.2% |

Decile (h=20):

| tier | D1% | D10% | D10-D1% | mono_rho |
|---|---|---|---|---|
| all | +1.363 | +2.079 | +0.716 | +0.576 |
| liq_50m | +1.051 | +1.765 | +0.714 | +0.830 |
| liq_100m | +0.987 | +1.696 | +0.709 | +0.879 |
| ex_bottom20pct | +1.036 | +1.926 | +0.889 | +0.794 |

Net spread (h=20 liq_50m, long-short 2 腿):

| cost RT | gross% | net% | sign_ok |
|---|---|---|---|
| 0.25% | +0.714 | +0.214 | False |
| 0.35% | +0.714 | +0.014 | False |

- **Walk-forward** (liq_50m, 21 年)：signed-IC 正 8/21 年, LS-spread 正 16/21 年
- **LOYO**：full +0.714% / ex-2020 +0.619% / drop-year range [+0.608%, +0.897%]
- **Cross-regime** (spread 正 3/3 cell)：low_vol IC=-0.0083 sp=+0.44%, mid_vol IC=-0.0031 sp=+0.84%, high_vol IC=-0.0186 sp=+0.82%
- **Deflated Sharpe** h=10 (N_trials=72)：SR_ann +0.857, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False
- **Deflated Sharpe** h=20 (N_trials=72)：SR_ann +0.408, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False
- **Deflated Sharpe** h=60 (N_trials=72)：SR_ann +0.339, sr0_benchmark +2.413, PSR 0.000 -> DSR pass=False

---

## 自動 verdict heuristic (主 session 覆寫最終判定)

規則：IC|t|>2 且 net_spread>0 且 mono>=+0.5 且 WF 正年>=60% 且 DSR pass → production；部分滿足 → informational；spread 與 IC 反號 / mono<0.5 / net<0 → reject。

- `sig_lambda_level`：**D / reject (spread 與 IC 反號 = reverse-artifact)**  (IC_t@20=+13.69, net_spread@20 min=-0.555%)
- `sig_lambda_diverge`：**D / reject (spread 與 IC 反號 = reverse-artifact)**  (IC_t@20=-9.19, net_spread@20 min=-0.072%)
- `sig_lambda_diverge_signed`：**D / reject (spread 與 IC 反號 = reverse-artifact)**  (IC_t@20=-4.54, net_spread@20 min=+0.014%)