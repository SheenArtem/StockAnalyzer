# V3 Layer 3 Catalyst Signal IC Validation

- 產生時間: 2026-04-24 19:29:54
- Universe: TW common stocks (4-digit), rolling top-600 by 60d turnover
- Pre-AI regime: 2016-01-01 to 2022-12-31
- AI era regime: 2023-01-01 to 2025-12-31
- Horizons: [20, 60] trading days
- Total triggers: 136,490  |  with fwd return: 136,490

## Verdict Summary

| Signal | Verdict | Description |
|---|---|---|
| C1_rev_yoy_turnaround | **C** | 月營收 YoY 從負轉正 (或 3m slope 由 -5pp 翻 +5pp) |
| C2_trust_etf_proxy_buy | **D** | 投信連 5 日淨買超 (Active ETF proxy), 5d 買超 > 2% 20d turnover |
| C3_foreign_sell_to_buy | **C** | 近 20 日外資累積賣超 + 最近 10 日外資轉淨買超 |

## Detailed Metrics

| signal | regime | horizon | n | mean_trigger | baseline | alpha | t-stat | p-value | ic_proxy | hit_rate |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| C1_rev_yoy_turnaround | ai_era | 20d | 3553 | 0.0256 | 0.0153 | 0.0103 | 4.33 | 0.0000 | 0.0726 | 0.546 |
| C1_rev_yoy_turnaround | ai_era | 60d | 3553 | 0.0564 | 0.0432 | 0.0132 | 2.91 | 0.0036 | 0.0489 | 0.518 |
| C1_rev_yoy_turnaround | all | 20d | 10874 | 0.0222 | 0.0126 | 0.0096 | 7.35 | 0.0000 | 0.0705 | 0.531 |
| C1_rev_yoy_turnaround | all | 60d | 10874 | 0.0538 | 0.0359 | 0.0179 | 6.56 | 0.0000 | 0.0629 | 0.525 |
| C1_rev_yoy_turnaround | pre_ai | 20d | 7321 | 0.0205 | 0.0115 | 0.0090 | 5.84 | 0.0000 | 0.0683 | 0.520 |
| C1_rev_yoy_turnaround | pre_ai | 60d | 7321 | 0.0525 | 0.0328 | 0.0197 | 5.79 | 0.0000 | 0.0677 | 0.528 |
| C2_trust_etf_proxy_buy | ai_era | 20d | 22138 | 0.0151 | 0.0153 | -0.0002 | -0.25 | 0.8058 | -0.0017 | 0.533 |
| C2_trust_etf_proxy_buy | ai_era | 60d | 22138 | 0.0516 | 0.0432 | 0.0084 | 6.05 | 0.0000 | 0.0406 | 0.581 |
| C2_trust_etf_proxy_buy | all | 20d | 43038 | 0.0117 | 0.0126 | -0.0009 | -1.79 | 0.0735 | -0.0086 | 0.519 |
| C2_trust_etf_proxy_buy | all | 60d | 43038 | 0.0380 | 0.0359 | 0.0021 | 2.13 | 0.0335 | 0.0102 | 0.536 |
| C2_trust_etf_proxy_buy | pre_ai | 20d | 20900 | 0.0081 | 0.0115 | -0.0034 | -4.54 | 0.0000 | -0.0314 | 0.502 |
| C2_trust_etf_proxy_buy | pre_ai | 60d | 20900 | 0.0236 | 0.0328 | -0.0092 | -6.64 | 0.0000 | -0.0459 | 0.499 |
| C3_foreign_sell_to_buy | ai_era | 20d | 6426 | 0.0208 | 0.0153 | 0.0055 | 3.17 | 0.0015 | 0.0395 | 0.501 |
| C3_foreign_sell_to_buy | ai_era | 60d | 6426 | 0.0481 | 0.0432 | 0.0049 | 1.48 | 0.1385 | 0.0185 | 0.499 |
| C3_foreign_sell_to_buy | all | 20d | 18167 | 0.0158 | 0.0126 | 0.0032 | 3.20 | 0.0014 | 0.0237 | 0.503 |
| C3_foreign_sell_to_buy | all | 60d | 18164 | 0.0369 | 0.0359 | 0.0011 | 0.58 | 0.5596 | 0.0043 | 0.494 |
| C3_foreign_sell_to_buy | pre_ai | 20d | 11741 | 0.0131 | 0.0115 | 0.0016 | 1.30 | 0.1924 | 0.0120 | 0.502 |
| C3_foreign_sell_to_buy | pre_ai | 60d | 11738 | 0.0309 | 0.0328 | -0.0019 | -0.91 | 0.3618 | -0.0084 | 0.493 |

## Verdict Criteria

- **A**: 兩段 IC proxy > 0.03 且 p < 0.01，hit rate > 55%
- **B**: 一段達 A 標準
- **C**: IC proxy 0.01-0.03 且 hit rate 50-55%（邊際）
- **D**: IC proxy < 0.01 或反向

## Per-Signal Interpretation

### C1 月營收 YoY 拐點 — Verdict **C** (最有 alpha 但 hit rate 未破 55%)
- pre-AI 20d: alpha **+0.90pp** (p<0.0001, ic=0.068, hit=52.0%), 60d: alpha **+1.97pp** (p<0.0001, ic=0.068, hit=52.8%)
- AI era 20d: alpha **+1.03pp** (p<0.0001, ic=0.073, hit=54.6%), 60d: alpha **+1.32pp** (p=0.0036, ic=0.049, hit=51.8%)
- 方向 **兩段一致且顯著**, mean alpha 最強但 hit rate 卡在 52-55% 之間
- **結論**: 這是三個 catalyst 裡表現最好的。期望報酬穩定正, 但 hit rate 低代表**賠的時候多、賺的時候大** (right-skew, 長尾 alpha)
- **適用**: 可作為 Mode D thesis entry timing 的 bias filter (yoy 翻正後權重上調), 但不能單獨當 binary trigger

### C2 投信連 5 日買超 (Active ETF proxy) — Verdict **D** (regime 不穩 + pre-AI 反向)
- pre-AI: 20d alpha **-0.34pp** (p<0.001), 60d alpha **-0.92pp** (p<0.001) → **顯著反向**
- AI era: 20d alpha near-zero (p=0.81), 60d alpha **+0.84pp** (p<0.001, hit=58.1%)
- **方向在兩 regime 間翻轉** — pre-AI 投信連買是反指標, AI era 才微正
- **可能解釋**: pre-AI 時期投信 flow 多為法人輪動 (高位出貨), AI era 投信主動型 ETF (009802/00940 等) 2023+ 崛起改變了 flow 結構
- **結論**: 不可作為 mechanical signal (regime 不穩即為 fail), 若要用必須先建 regime detection 機制

### C3 外資從賣轉買 — Verdict **C** (僅 AI era 20d 邊際有效)
- pre-AI 20d: alpha +0.16pp (**p=0.19 不顯著**), 60d 近零 / 輕微負
- AI era 20d: alpha **+0.55pp** (p=0.0015, ic=0.040, hit=50.1%), 60d: alpha +0.49pp (p=0.14)
- **短效 + 僅 AI era**, pre-AI 完全失效
- **結論**: AI era 才出現 alpha, 但 hit rate 只有 50.1%, 屬**小賭注可考慮**; 60d 快速衰退代表是**均值回歸** bounce 而非趨勢啟動

## Mode D 設計建議

| Signal | 角色 | 用法 |
|---|---|---|
| C1 月營收 YoY 拐點 | Weak tilt / bias | thesis buy 時若 yoy 剛翻正, 權重 ×1.2 |
| C2 投信連買 | **不用** | regime 反向, 非 rule-based alpha |
| C3 外資轉買 | Short-term confirm (AI era only) | 20d 窗口配合 thesis 進場可加分, 但不是獨立訊號 |

**三個 catalyst 裡沒有 A 級 signal**, 但 C1 足夠當 weak filter。實戰建議:

1. **C1 + thesis 組合**: 產業前景已定 + YoY 剛翻正 → 進場 confirmation
2. **C3 作為 AI era 時間點 hint**: 但 pre-AI 不可信, 放到 AI regime 專用
3. Mode D 的 mechanical entry 仍要仰賴 Layer 1 (regime) + Layer 2 (technical) 的組合, Layer 3 catalyst 只是 tilt, 不是核心 alpha 源
4. **不推薦單獨用 catalyst trigger 機械進場**: 三個最佳 hit rate 僅 58% (C2 AI era 60d), 而且 C2 自己就 regime 不穩

## Verdict Criteria (reference)

- **A**: 兩段 IC proxy > 0.03 且 p < 0.01，hit rate > 55%
- **B**: 一段達 A 標準
- **C**: IC proxy 正且穩定但 hit rate 未破 55% (邊際 alpha)
- **D**: IC proxy < 0.01, 反向, 或 regime 間方向翻轉

## Caveats

- **C2 proxy 限制**: 用投信淨買超 proxy Active ETF flow (真 Active ETF 2023+ 才普及且無歷史 API)。投信 ≠ Active ETF, 但 overlap 高。
- **IC proxy 定義**: alpha / std_trigger, 非標準截面 Spearman IC (trigger 是 binary event 所以 no per-day cross-section)。可視為 signal-to-noise ratio。
- **Look-ahead bias 處理**: 月營收公布日用 period month-end + 10 days 保守估計 (實際 T+10 公告), 再 align 到下一個交易日。
- **Universe**: top-600 by 60d turnover 作為 market-cap proxy (ohlcv 無流通股數直接欄位), 已排除冷門股避免流動性噪音。
- **Regime 切點**: pre-AI = 2016-2022, AI era = 2023-2025; 前者含 covid / 升息等 shocks, 後者含生成式 AI 題材 + 美股 AI 強勢帶動台股。
- **Trigger 集中問題**: C1 每月 10 日後集中觸發 (月營收公布), 同月 trigger 間存在橫截面相關 (同期間 market move); t-stat 可能高估。應視為 exploratory 而非嚴謹統計結論。
- **No overlap handling**: 同一檔股票若 60d 內多次 trigger, fwd return 視窗會重疊, 觀測並非 iid; 已對 C3 做 20d 去重, C1/C2 未做。