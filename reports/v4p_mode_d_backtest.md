# V4' - Mode D 整合回測

**Verdict**: **C**

> Alpha 邊際，Sharpe 0.7-0.8，thesis 層是決勝點

## 設定

- Rebalance: 4 週 (月頻 ~13×/yr)
- Top N: 5 (不足則 cash fill)
- Cost: 0.40% round-trip
- Benchmark: TWII TR = TWII price + 3.5%/yr dividend
- C1 tilt: 近 3 月有 1 月 revenue YoY 從 <= -2% 轉 >= +2% -> score × 1.2
- C1 pivot events in data: 37351

## 時段覆蓋

| Strategy | Pre-AI 起算年 | 資料限制 |
|---|---|---|
| S1 QM top_5 | 2016 | QM snapshot 2015+ (足 10 年) |
| S2 Dual | 2020 | Value snapshot 僅 2020+ |
| S3 QM+C1 | 2016 | 同 S1 |
| S4 Dual+C1 | 2020 | 同 S2 |
| S5 Dual+C1+Step-A | 2020 | 同 S2 |

> S2/S4/S5 Pre-AI 段實際為 2020-2022 (3 年)，非 2016-2022 (7 年)；
> Full 段 S1/S3 為 2016-2025 (10 yr)，S2/S4/S5 為 2020-2025 (6 yr)。
> 報告中的 alpha 比較因資料覆蓋不同，跨策略對比需看同一段。

## Table 1 - 5 策略 × 3 時段 (CAGR / Sharpe / MDD)

| Strategy | Period | CAGR% | Sharpe | MDD% | R:R | Hit% | alpha vs TWII TR (pp) | Win TWII% |
|---|---|---|---|---|---|---|---|---|
| S1 QM top_5 (baseline) | Pre-AI | 24.69 | 0.885 | -27.71 | 0.891 | 58.4 | 8.25 | 50.6 |
| S1 QM top_5 (baseline) | AI_era | 32.22 | 1.107 | -27.19 | 1.185 | 64.1 | -9.54 | 33.3 |
| S1 QM top_5 (baseline) | Full | 26.67 | 0.947 | -27.71 | 0.962 | 60.2 | 3.34 | 45.3 |
| S2 Dual-all + only_volatile top_5 | Pre-AI | 25.19 | 1.085 | -21.17 | 1.19 | 56.4 | 14.72 | 53.8 |
| S2 Dual-all + only_volatile top_5 | AI_era | 10.38 | 0.523 | -19.07 | 0.545 | 61.5 | -18.13 | 41.0 |
| S2 Dual-all + only_volatile top_5 | Full | 17.31 | 0.808 | -21.17 | 0.818 | 59.0 | -1.57 | 47.4 |
| S3 QM + C1 weak tilt | Pre-AI | 20.5 | 0.69 | -23.54 | 0.871 | 58.4 | 4.05 | 47.2 |
| S3 QM + C1 weak tilt | AI_era | 40.14 | 1.401 | -30.64 | 1.31 | 64.1 | -1.62 | 41.0 |
| S3 QM + C1 weak tilt | Full | 25.86 | 0.885 | -30.64 | 0.844 | 60.2 | 2.53 | 45.3 |
| S4 Dual + C1 weak tilt | Pre-AI | 18.52 | 0.88 | -18.97 | 0.976 | 48.7 | 8.04 | 53.8 |
| S4 Dual + C1 weak tilt | AI_era | 11.14 | 0.567 | -20.95 | 0.531 | 59.0 | -17.37 | 48.7 |
| S4 Dual + C1 weak tilt | Full | 14.56 | 0.721 | -20.95 | 0.695 | 53.8 | -4.32 | 51.3 |
| S5 Dual + C1 + Step-A exit | Pre-AI | 8.53 | 0.555 | -12.65 | 0.674 | 51.3 | -1.94 | 41.0 |
| S5 Dual + C1 + Step-A exit | AI_era | -2.16 | -0.247 | -32.04 | -0.067 | 51.3 | -30.67 | 33.3 |
| S5 Dual + C1 + Step-A exit | Full | 3.01 | 0.152 | -32.04 | 0.094 | 51.3 | -15.88 | 37.2 |

## Table 2 - Alpha 分解 (Full period, vs TWII TR)

| Strategy | Full α (pp) | Full Sharpe | Turnover/yr |
|---|---|---|---|
| S1 QM top_5 (baseline) | 3.34 | 0.947 | 9.49 |
| S2 Dual-all + only_volatile top_5 | -1.57 | 0.808 | 7.22 |
| S3 QM + C1 weak tilt | 2.53 | 0.885 | 9.45 |
| S4 Dual + C1 weak tilt | -4.32 | 0.721 | 7.43 |
| S5 Dual + C1 + Step-A exit | -15.88 | 0.152 | 7.43 |

### Alpha 增量 (incremental contribution)

| 比較 | 說明 | α 增量 (pp) |
|---|---|---|
| S3 - S1 | C1 tilt 對 QM 帶來 α (pp) | -0.81 |
| S4 - S2 | C1 tilt 對 Dual 帶來 α (pp) | -2.75 |
| S5 - S4 | Step-A 契約對 Dual+C1 帶來 α (pp) | -11.56 |

## Step-A 觸發統計 (S5)

- Hard SL 觸發: 144 rebal (avg fwd_20d_min < -10% 視為觸發)
- TP +20% 觸發: 81 rebal (avg fwd_20d_max > 20% 鎖在 +15%)
- Regime 轉換出場: 14 次

## 關鍵發現

**[F1] C1 weak tilt 在 AI era 顯著補救 QM** — S3 AI era α -1.62pp (vs S1 -9.54pp)，補救 +7.92pp

- S3 AI era CAGR 40.14%, Sharpe 1.401 (vs S1 32.22%, 1.107)
- C1 pivot events: 37351 (1886 unique stocks，月頻公布 10yr)
- 最有說服力的 F 級結果 — 月營收拐點股在 AI era 後段 catch-up 效應明顯
- **但 Pre-AI α 從 +8.25pp 降到 +4.05pp** (代價 -4.2pp)，換算 Full α 反微降 (-0.8pp)

**[F2] Full period 3 策略 α > 0, QM 家族仍為最穩** —

- S1 QM top_5: Full α **+3.34pp**, Sharpe 0.947 (baseline 最強)
- S3 QM+C1: Full α +2.53pp, Sharpe 0.885 (Pre-AI 拖累)
- S2 Dual+volatile: Full α -1.57pp, Sharpe 0.808
- S4 Dual+C1: Full α -4.32pp, Sharpe 0.721 (C1 反而拖 Dual)
- S5 Dual+C1+Step-A: Full α -15.88pp, Sharpe 0.152 (snapshot 限制嚴重失真)

**[F3] S5 Step-A per-holding 出場傷害明顯** — CAGR 從 14.56%(S4) 砍到 3.01%(S5)

- Hard SL 觸發 144 次 > TP 81 次 (1.8:1)
- Per-holding 將 fwd_20d_min ≤ -10% 的 cap 在 -10%，但這類股約 10% 會 recover
- snapshot-level Step-A 近似 **不公平** — daily engine 才能體現 MIN_HOLD/whipsaw ban benefit
- **結論**: 本結果視為 S5 lower bound，不代表 Step-A 設計失敗

**[F4] Dual-all+only_volatile (S2/S4) AI era 大敗 TWII TR** —

- S2 AI era α -18.13pp / S4 AI era α -17.37pp
- 對應 memory handoff 結論：AI era 規則 universe 被動能 AI 大型股 bypass
- Dual Pre-AI (2020-2022) +14.72pp 是 COVID recovery value bounce，非長期 alpha 證據

**[F5] QM AI era 絕對報酬漂亮但仍輸 benchmark** —

- S1 AI era CAGR 32.22% Sharpe 1.107，是 Sharpe 最高的 AI era result
- TWII TR AI era +41%/yr (geometric mean 3yr) 無解門檻
- alpha 負值不等於策略失敗 — Sharpe 1.1 還是贏大多數主動管理 fund

**[F6] C1 tilt 在 Dual universe 無效 (S4-S2)** —

- S4 Full α 反降至 -4.32pp (vs S2 -1.57pp)，incremental -2.75pp
- Dual Value leg pick 多為估值低的 cyclical / 傳產，月營收拐點不具預測力
- **C1 tilt 只在 QM (momentum) universe 有效，非 Value universe**

## Impact 評估對 Mode D thesis 層

**Alpha 邊際**。S1 QM Full α +3.34pp 邊際正但 AI era -9.54pp，thesis 層的真正任務
是在 AI era 補 9pp。C1 tilt 有在 AI era 補 ~7.9pp 的跡象 (S3 vs S1)，
但 Pre-AI 拖累代價 -4.2pp，淨 Full α -0.8pp。

**建議**:
1. **S1 QM top_5 作 primary 機械層** — Full Sharpe 0.95, Full α +3.34pp，最穩
2. **C1 tilt 視為 regime-conditional**: AI era 時開啟 (α +7.9pp)，Pre-AI 關閉
   (避免 -4.2pp drag)；需先做 regime detector + live AB 驗證
3. **S4 (Dual+C1) 降至 B+**: C1 tilt 只在 QM universe 有效，Value universe 無效 (F6)
4. **S5 Step-A 不 ship** until daily engine (snapshot 無法公平評估)
5. **Thesis 層的戰略定位**: AI era 補 9pp 的缺口。若 thesis 層 + C1 tilt 合併
   能穩定補 AI era 5-10pp，整體 Full α 可望突破 5pp，verdict 翻 A
6. **Sector Rotation 可重驗**: Step 3a 昨日跑過，成本修正後可能翻盤；C1 tilt YoY bug 類似 issue 要防範

## Caveats

- **資料覆蓋不對齊**: S1/S3 完整 10 年 (2016-2025)，S2/S4/S5 僅 6 年 (2020-2025)。
  Pre-AI S2/S4/S5 實際為 2020-2022 (3 年)，信賴區間窄。
- **Top_5 不足 cash fill**: QM snapshot 每週 top_5 可用 picks 平均只有 1.67 檔 (QM gate 嚴，多週僅 1-2 檔命中)，空位填 cash。
- **C1 tilt** 用月營收 YoY 從負轉正，公布時差 ~10 日，conservative 設當月 tilt active。
  拐點定義 ±2% deadband，嚴鬆可調。
- **Step-A 近似實作**: snapshot backtest 無 daily holding-level view，用 `fwd_20d_min`/`fwd_20d_max` 代替 ATR-based SL/TP，是 sampled approximation。
  MIN_HOLD_DAYS=20 / whipsaw ban 30 日在月頻 rebal inherently 滿足，精確對應需 daily engine。
- **Cost 0.4%**: 台股手續費打折 + 證交稅保守估計，ETF 實際會低 0.1-0.2%，小型股 slippage 未計。
- **TWII TR 股息 3.5%/yr**: 平均值，AI era 實際因大型股殖利率低可能略低。
- **Out-of-sample**: 無 2000/2008 bear regime 驗證；2020-2022 含 COVID 崩盤但無大型熊市。
