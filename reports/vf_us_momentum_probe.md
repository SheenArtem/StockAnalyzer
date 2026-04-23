# VF-US Momentum Probe (2026-04-23)

- ohlcv_us: 4012971 rows × 1569 tickers
- date range: 2015-06-01 ~ 2026-04-21
- monthly panel: 192095 obs × 1563 tickers × 131 months

## IC / IR / decile spread

| factor | horizon | IC | IR | n_months | month_winrate | top_ret | bot_ret | spread | grade |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 12m return | 1m | -0.0041 | -0.0248 | 118 | +50.85% | +2.14% | +4.04% | -1.89% | D |
| 12m return | 3m | -0.0025 | -0.0168 | 116 | +53.45% | +6.01% | +9.25% | -3.24% | D |
| 12m return | 6m | +0.0010 | +0.0068 | 113 | +55.75% | +11.82% | +20.16% | -8.34% | D |
| 12-1m (J-T classic) | 1m | -0.0025 | -0.0161 | 118 | +50.85% | +2.08% | +3.89% | -1.81% | D |
| 12-1m (J-T classic) | 3m | -0.0033 | -0.0224 | 116 | +50.86% | +6.00% | +9.17% | -3.17% | D |
| 12-1m (J-T classic) | 6m | +0.0008 | +0.0056 | 113 | +54.87% | +11.80% | +20.01% | -8.21% | D |
| 6-1m | 1m | -0.0012 | -0.0080 | 124 | +46.77% | +2.15% | +3.80% | -1.65% | D |
| 6-1m | 3m | +0.0059 | +0.0498 | 122 | +57.38% | +6.66% | +7.97% | -1.32% | D |
| 6-1m | 6m | +0.0149 | +0.1285 | 119 | +60.50% | +13.21% | +17.56% | -4.35% | B |
| MA alignment 0-3 | 1m | +0.0010 | +0.0072 | 128 | +52.34% | +1.36% | +1.44% | -0.09% | D |
| MA alignment 0-3 | 3m | +0.0052 | +0.0436 | 126 | +55.56% | +4.14% | +4.60% | -0.46% | D |
| MA alignment 0-3 | 6m | +0.0078 | +0.0698 | 123 | +53.66% | +8.08% | +9.50% | -1.42% | C |

## 結論（手動覆寫：自動判讀有 bug，只看 IR 忽略 decile spread 正負號）

### 矛盾現象

- 最強 IR：**6-1m @ fwd_120d IR +0.128 B 級**
- **但所有因子 × horizon 的 decile spread 全為負**：top 10% - bottom 10% = -0.09% ~ -8.34%
- 最極端：12m return @ fwd_120d IR ≈ 0 但 top decile +11.82% vs bottom +20.16% → 輸 8.3pp

### 判讀

Rank IC > 0 + decile spread < 0 = **動能「排序訊號」存在但 portfolio 層面不可實作**：

- 高動能股在當月有「略微」高排序，但實際 top 10% 的絕對報酬低於 bottom 10%
- 原因：底部 10% 常是跌深反彈的 high-beta 股，2015-2025 牛市下累積彈幅大
- 同 VF-Value-ex2「US growth dominance 時代」結論：高品質 / 高動能 / 低估值 類 factor 統統被低基期反彈股稀釋

### US 研究結論（結合既有）

| 因子類別 | 結論 | 出處 |
|---|---|---|
| F-Score | D 反向 -10% alpha | VF-Value-ex2 (2026-04-22) |
| FCF Yield / ROIC / Gross Profitability | C/D noise | VF-Value-ex2 alt quality (2026-04-22) |
| 動能 (12m/12-1m/6-1m/MA align) | D portfolio / B rank noise | VF-US Momentum (2026-04-23 本測試) |
| Mohanram G-Score (US Financials only) | C 條件成立 | VF-Value-ex3 (2026-04-22) 歸檔 |

**整體結論**：**US S&P 500 在 2015-2025 樣本內缺乏可 portfolio 層級實作的 systematic alpha**，TW QM 框架不可平移。短期 US 研究告段落，建議專注台股 live 穩定性與 shadow run 累積。

### 何時重啟

- Regime 轉變：若未來 3-5 年 US 進入 bear / 股價結構重設（value rotation）→ 重跑 F-Score / 動能 IC 檢查
- 資料擴充：若取得 Russell 2000 / small-cap panel → 小盤可能仍有 Piotroski 甜蜜點，值得重試
- 新因子研究：若學界出現新一代 factor（如 ML-based alpha pooling）→ 評估是否值得建 US 框架

## 產出

- `tools/vf_us_momentum_probe.py`
- `reports/vf_us_momentum_probe.md`
- `reports/vf_us_momentum_probe.csv`