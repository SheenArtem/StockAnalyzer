# Signal #5 「好消息股價不推」 Event Study (出場訊號)

- 期間: 2015-01-05 ~ 2026-04-21
- TWII 利多門檻: 1.0% (close-to-close)
- 樣本過濾: 收盤價 >= 5.0 元、20 日均量 >= 200,000 股
- Forward horizons: [1, 5, 10, 20]
- 總 trigger 樣本數: 169,560

⚠ **方向跟 #1 進場訊號相反**: 出場訊號期望 forward CAR < 0 才有 alpha。

## Baseline 對照組 (利多日所有 liquid 個股, 沒篩 signal)

| Horizon | n | mean fwd | mean CAR | t-stat (CAR) |
|---|---:|---:|---:|---:|
| 1d | 384,578 | +0.29% | +0.16% | +38.34 |
| 5d | 381,093 | +0.66% | +0.24% | +24.72 |
| 10d | 379,912 | +1.06% | +0.28% | +19.90 |
| 20d | 376,303 | +1.86% | +0.29% | +13.99 |

## 各訊號彙整 (Full sample)

| Signal | n | fwd_5d | CAR_5d (t) | fwd_10d | CAR_10d (t) | fwd_20d | CAR_20d (t) | Grade |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| S1_no_response | 125,014 | +0.50% | +0.02% (+1.38) | +0.73% | -0.09% (-3.70) | +1.22% | -0.33% (-8.92) | D |
| S2_intraday_fade | 30,196 | +0.70% | +0.14% (+3.62) | +1.18% | +0.18% (+3.13) | +2.16% | +0.45% (+5.20) | D |
| S3_t1_breakdown | 12,498 | +1.27% | +0.41% (+7.90) | +1.28% | -0.08% (-1.07) | +2.06% | -0.31% (-2.99) | D |

## Regime Breakdown (CAR_10d mean / t-stat / n)

| Signal | Bull | Bear | Vol Low | Vol Mid | Vol High |
|---|---|---|---|---|---|
| S1_no_response | -0.32% (-11.60) n=99384 | +0.82% (+15.83) n=25519 | -0.65% (-16.17) n=44393 | -0.63% (-14.96) n=41631 | +1.12% (+24.51) n=38879 |
| S2_intraday_fade | -0.02% (-0.36) n=22539 | +0.78% (+7.91) n=7580 | -0.49% (-4.42) n=8314 | -0.52% (-5.27) n=10033 | +1.25% (+14.01) n=11772 |
| S3_t1_breakdown | -0.31% (-4.17) n=10262 | +1.05% (+5.18) n=2140 | -1.30% (-11.76) n=3865 | -0.39% (-3.81) n=4452 | +1.43% (+9.68) n=4085 |

## 最終判級 (含 regime 反向檢查)

- **S1_no_response**: D (bear regime CAR>0 (regime 反向); vs baseline CAR_10d edge = -0.37%)
- **S2_intraday_fade**: D (bear regime CAR>0 (regime 反向); vs baseline CAR_10d edge = -0.10%)
- **S3_t1_breakdown**: D (bear regime CAR>0 (regime 反向); vs baseline CAR_10d edge = -0.35%)

## 方法說明

- TWII 利多日: 當日 close-to-close 報酬 >= 門檻; 個股 forward 取 (Close_t+h / Close_t - 1)。
- CAR = stock_fwd_h - twii_fwd_h; 以同 horizon 區間 cumulative return 對齊。
- S3 entry_date = t+1 (避免 look-ahead bias, 學 Signal #1 教訓)。
- Bull/Bear 以 TWII Close vs 200d MA 切; Vol Low/Mid/High 以樣本 TWII 20d realized vol 33/66 quantile 切。
- Grade A 需 CAR_5d 與 CAR_10d 平均 <-2% 且 |t|>2、regime 不反向; B 是 <-1%、|t|>1.5; C 為負但 t 弱; D 為正或反向。
- 警告: 本研究只用大盤 proxy, 未引入 stock-level news/EPS beat sentiment; 若有 alpha, 後續可加上 EPS event window 細分。

## 結論

**全 D 歸檔，不上線到 position_monitor 出場規則或 trigger_score 軟警報。**

### 三 signal 一句話結論

- **S1_no_response (大盤大漲但個股翻黑)**: D — full sample CAR_10d 僅 -0.09% (t=-3.70)，
  量級遠不到 -1% B 級門檻；且 bear regime 完全反向 (+0.82% t=+15.83)，bull 也只有 -0.32%。
  baseline 已涵蓋 (edge -0.37%)，無 actionable alpha。
- **S2_intraday_fade (開高拉回, gap +1% 收盤 ≤ +0.3%)**: D — full sample CAR_10d **正向** +0.18%
  (t=+3.13)，跟出場訊號預期完全相反。bull 區內趨近 0、bear 全面反向 +0.78%。完全失靈。
- **S3_t1_breakdown (利多日後 t+1 跌破 20 日低)**: D — full sample CAR_10d 僅 -0.08%
  (t=-1.07, p≈0.28 不顯著)。雖然 vol_low regime 有 -1.30% (n=3865) 看起來像點東西，
  但 bear regime +1.05%、vol_high +1.43% 完全反向，整體 wash 掉。

### Regime 細粒度發現 (有趣但不 actionable)

**Bull + low/mid vol** 是唯一三 signal 都負的 sub-regime：
- S1: vol_low -0.65% / vol_mid -0.63% (大量樣本 n>40k, t > 14)
- S3: vol_low -1.30% (n=3865, t=-11.76) 量級最大但仍未到 B 級
- 一致性說明 bull 平靜期 follow-through 失敗確實預示弱勢，但 edge < 1% 不值得開特例

**Bear regime 全部反向**（最關鍵 kill）：
- S1: +0.82% (t=+15.83), S2: +0.78% (t=+7.91), S3: +1.05% (t=+5.18)
- 解讀: bear 期間出現 TWII +1% 反彈，個股「沒跟上」反而是超賣後的 mean reversion 標的，
  之後反而會補漲。完全顛覆「股價不推 = de-rate 前哨」的論點。

**vol_high regime 也反向**（次關鍵 kill）：
- S1 +1.12%, S2 +1.25%, S3 +1.43% — 高波動期間「沒跟上」也是 mean reversion 標的。

### 為何全 D（vs 宋分原話）

宋分的「正面 news + EPS beat 但股價不推」要求兩個條件同時成立：
1. 確實有 stock-level 利多 (news + EPS beat)
2. 股價不反映該利多

本研究只有 **大盤 proxy** (TWII +1%)，第 1 條件「stock-level 利多」缺失。
本質上量到的是「個股對市場 beta 不跟」而不是「對自身利多不反應」。
這兩件事差很遠：

- 真宋分情境: TSMC 法說超預期 → 股價平盤 → de-rate (這個有信息, 但本研究抓不到)
- 本研究情境: 大盤普漲 → 某弱勢股不跟漲 → 之後 (尤其 bear) 反而補漲 (mean reversion)

換言之，**「不跟大盤」與「不反映 stock-level 利多」是不同的 latent variable**。
要驗證宋分原意，必須引入：
- EPS surprise event window (FinMind 季報資料 + analyst consensus)
- News sentiment (Google News 正面標籤過濾)
- 然後在「個股有真利多」的子樣本內看 t+5/10d CAR

該方向工程量大 (≥ 1 週)，且本驗證已證 baseline proxy 路線無 alpha → **建議先擱置 #5，
等 stock-level news/EPS event panel 建好再重啟**，列入 `project_songfen_timing_signals.md`
TODO 並標 D-proxy。

### Action items

1. **不上線** — position_monitor 出場規則 / trigger_score 軟警報都不加 #5。
2. **歸檔到 `project_songfen_timing_signals.md`** — 標 D (proxy version)，
   重啟條件: EPS event panel + news sentiment 任一就位。
3. **副產出觀察**: bear + vol_high 「不跟漲 = mean reversion buy」反向訊號量級不小
   (CAR_10d +1%~+1.4%, t > 5)，雖非本研究目標，但後續可考慮做 contrarian 反向研究。
