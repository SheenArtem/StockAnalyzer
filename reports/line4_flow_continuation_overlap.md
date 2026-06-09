# Line 4 — 大型股 × 法人流量續勢 (informed-flow continuation) 驗證 + Whale 重疊檢查

> Verdict: **D (reject as standalone) + 對 Whale 大量重疊**。2026-06-08 full-run。
> 三格價量訊號 (line1 λ / line2 量條件化 / line3 流動性 regime) 全 D 後唯一存活線索:
> Hsieh-Hu (2010) 台股「大型股 × 外資/投信流量續勢」。本檔測它 (a) 自身有沒有可上線
> alpha、(b) 對現有 Whale composite 正交化後還剩不剩增量 IC。
> Panel: `data_cache/backtest/ohlcv_tw.parquet` (clean, 剔 Close<=0 + V<=0) ×
> `chip_history/institutional.parquet` (日法人) × `listed_shares.parquet` (市值)。
> 期間 **2015-01 ~ 2026-06** (法人資料起點限制), 上市 only **1071 檔**, large_cap = top quintile 市值。

## 假設 (pre-registered)
- Hsieh-Hu 2010 (TW): 動量續勢在**大型股**、由**外資+投信流量**載動 (小型股則 LMSW 反轉)。
- Kang 2025 (KR): 法人 conviction 用**市值** normalize (淨買股數/流通股), 非成交值。
- Signal: `flow_cont = z(prior_ret_Wd) × z(法人淨買累計_Wd / 市值)`, W=5/20, horizon 5/20/60。
  另測 flow_only (純 conviction 流量無動量)、flow_samesign (價漲+法人買同號 gate)、
  flow_turn_cont (turnover-normalized 對照, Whale smart_money 用此 normalize)。

## TL;DR Verdict 表

| 訊號 | universe | liq | 標準 verdict | 一句話 |
|---|---|---|---|---|
| flow_cont_20d | **large_cap** (假設所在) | liq_50m | **FAIL** | IC +0.003 (t=1.3 NS), 成本後淨 spread **−0.08%**, WF 僅 5-6/11 → Hsieh-Hu 大型股續勢**不複現** |
| flow_cont_20d | full | liq_50m | **MARGINAL→D** | IC +0.013 / 淨 +0.33% / WF 10/11 — 但被純動量完爆 (mom_only 淨 **+1.18%**), 且 edge 全來自動量腿 (flow_only IC 負) |
| flow_only (純流量) | both | both | **FAIL** | 純法人 conviction 流量 IC **負** — 法人淨買本身無正向預測力 |
| flow_turn_cont (turnover-norm) | both | both | **≈ flow_cont** | 與 mcap-norm ρ=0.85 近乎同訊號 → Kang conviction normalize **不加分** |

**重疊 verdict**: **SUBSUMED / 無增量 alpha** — 大型股池主訊號 raw IC 不顯著 (t=1.31), 對 Whale
正交化後殘差 IC **翻負** (−0.011, t=−4.04); 純流量與 Whale foreign_pct/total_pct 相關 0.48-0.53。見 §OVERLAP。

**一句話: 不值得開第四條線。** Hsieh-Hu 大型股流量續勢在 clean panel 不複現 (edge 反在小型股且純動量),
法人 conviction 訊號已被 Whale 籌碼欄涵蓋, 正交化後無正向殘差 → 答案確認已在 Whale 手上。三格 + 本線 = 台股
「自己撿一個價量/籌碼 alpha」四度落空, 收尾。

## 標準 verdict — 決定性證據

### 1. 假設方向反了:edge 在**小型/全市場**不在大型股 (殺手 1)
`reports/line4_flow_gauntlet.csv` (h=20, cost 0.25%, liq_50m):

| 訊號 | universe | IC | t | gross% | **net%** | mono |
|---|---|---|---|---|---|---|
| flow_cont_20d | full | +0.0133 | +9.1 | +0.833 | **+0.333** | +0.81 |
| flow_cont_20d | **large_cap** | +0.0029 | **+1.3** | +0.419 | **−0.081** | +0.71 |
| mom_only_20d | full | −0.010 | — | +1.675 | **+1.175** | +1.00 |

Hsieh-Hu 預測續勢在大型股;clean panel 實證**相反** — 大型股 IC ≈ 0 (t=1.3 不顯著)、成本後淨 spread 轉負。
edge 集中在 full universe (含中小型) = 典型 momentum/illiquidity 動能, 進可交易大型股池後蒸發。
**與 line2/line3「ranking 上有、可交易池扣成本後死」同型** (Avramov-Chordia-Goyal 2006)。

### 2. 「流量」沒加分,edge 純粹是動量 (殺手 2)
- `flow_only_20d` (純 conviction 流量, 無 prior_ret): IC **負** (full −0.007 / large_cap −0.006) →
  法人淨買/市值本身**無正向預測**, 甚至微負。
- `flow_cont_20d` (動量×流量) 淨 +0.33% **<** `mom_only_20d` 淨 +1.18% → 加上流量交互後反而**比純動量差**。
- 結論: flow_cont 的 edge 100% 來自 prior_ret (動量) 腿, 法人流量腿是稀釋而非增益。

### 3. conviction (mcap) vs turnover normalize — 無差異
獨立驗證 large_cap 池 Spearman(flow_mcap_20d, flow_turn_20d) = **0.85** (per-date 均值 0.85)。
gauntlet 兩者 IC 形態幾乎重合 (flow_cont_20d vs flow_turn_cont_20d)。
→ Kang 2025「用市值不用成交值」對台股大型股是 cosmetic, Whale 既有 turnover-norm smart_money 已涵蓋。

### 4. Robustness (gross LS spread, `reports/line4_flow_robust.csv`)
flow_cont_20d full: DSR=**1.00** (gross ann_SR 0.77), ex-2020 robust, LOYO all-positive →
**gross** 動量-流量 edge 統計上真實 (非多重檢定運氣), 但這是**未扣成本的 decile spread**;
可交易結論看 net spread + 大型股池 (上方已 FAIL)。large_cap DSR 0.97 但 ann_SR 僅 0.29 (邊際)。

## OVERLAP TEST (判生死) — 對 Whale composite + components 正交化

`reports/line4_flow_overlap.csv` (large_cap × liq_50m, fwd_20d, 430,784 obs;
正交化標的 = composite_score + smart_money_score + revenue_score + f_score + eps_yoy +
foreign_pct + total_pct; composite_score 覆蓋率 93.4%):

| 訊號 | raw IC | raw t | **resid IC** | **resid t** | IC retain | R² vs Whale | raw spread% | resid spread% |
|---|---|---|---|---|---|---|---|---|
| flow_cont_20d | +0.0029 | +1.31 | **−0.0110** | **−4.04** | −382% | 0.116 | +0.42 | +0.07 |
| flow_cont_5d | +0.0005 | +0.24 | −0.0064 | −2.44 | −1268% | 0.112 | +0.31 | +0.18 |
| flow_only_20d | −0.0056 | −2.10 | −0.0105 | −3.46 | +190% | 0.255 | +0.31 | +0.28 |
| flow_samesign_20d | −0.0008 | −0.28 | −0.0053 | −1.71 | +694% | 0.262 | +0.56 | +0.25 |

相關係數 (Spearman, pooled) vs Whale 各 col:

| 訊號 | composite_score | smart_money | revenue | f_score | eps_yoy | **foreign_pct** | **total_pct** |
|---|---|---|---|---|---|---|---|
| flow_cont_20d | −0.023 | +0.028 | +0.023 | +0.071 | +0.027 | +0.096 | +0.098 |
| flow_only_20d | +0.178 | +0.073 | +0.036 | +0.080 | +0.083 | **+0.483** | **+0.534** |
| flow_samesign_20d | +0.199 | +0.077 | +0.034 | +0.074 | +0.075 | **+0.465** | **+0.527** |

**重疊 verdict = SUBSUMED / NO INCREMENTAL ALPHA (確認, 不開第四條線)**:

1. **大型股池主訊號 (flow_cont_20d) raw IC 本就不顯著** (+0.0029, t=1.31)。正交化後殘差 IC
   **翻成 −0.0110 (t=−4.04)** — 不是「殘差≈0 被 subsume」, 是更強的「移掉 Whale 能解釋的部分後,
   剩下的甚至**負**預測」。任何正交化定義下都**沒有正向增量 alpha** → 不可能升候選。
2. **純法人流量 (flow_only) 與 Whale 的 foreign_pct / total_pct 相關 0.48 / 0.53** = 法人 conviction
   訊號**實質已在 Whale 的籌碼欄裡**; 且 flow_only raw IC 本身負 (−0.0056) → 連被 subsume 的「正
   alpha」都不存在。
3. composite_score 相關全 ≈ 0 (−0.02), R² vs Whale 僅 0.12-0.26 → Whale **production composite
   (純技術+基本面, 無流量)** 不直接解釋此訊號; 真正吃掉它的是 (a) 訊號自身就是 noise (t=1.3) +
   (b) 流量腿與 Whale 籌碼欄重疊。兩路都指向「不值得開」。

## 判準從嚴表 (四者全過才 PASS)

| 訊號 (large_cap, liq_50m) | ①流動性後單調存活 | ②WF ≥70% 同號 | ③成本後 spread 正 | ④rank-IC↔spread 同號 | 結論 |
|---|---|---|---|---|---|
| flow_cont_20d | ⚠️ mono +0.71 但 D1 偏高 | ✗ 5-6/11 (55%) | ✗ 淨 −0.08% | ✓ | **FAIL** |
| flow_only_20d | ✗ | ✗ | ✗ | ✗ (IC 負 spread 正) | **FAIL** |

## 資料品質註記 (SOP 1-14)
- **法人資料起點 2015-01** → 期間僅 ~11 年, 無 2008 GFC; 有 2015 中國貶值/2018Q4/2020 COVID/
  2022 熊/2024 yen-carry/2025 tariff。leave-one-crisis-out 樣本較 line2/3 (回到 2008) 少。
- **Survivor bias**: ohlcv_tw survivor-only (1071 上市有股數檔)。對**動量/續勢**訊號 survivor 偏**有利**
  (下市輸家被剔) → 若 survivor panel 上已 FAIL/marginal, PIT 完整 panel 只會更弱。方向: 對結論保守。
- **股數 snapshot 為當前值非 PIT** (`build_market_cap_panel` 同款近似): 市值**排序** (quintile) 對資本變動
  robust; conviction **量級** normalize 略有 drift, 不改 rank-based 結論。
- **僅上市 (TWSE)**: listed_shares 只覆蓋上市 → universe 偏大型 TWSE 名 (恰好是 Hsieh-Hu 效應所在, 可接受)。
- **decile 近似**: 為效能用 floor((rank-1)/n*10) 向量化分桶 (vs qcut 24x 快); 實測 n_days 完全相同,
  decile mean_ret 最大差 ~4bp (~4/N 邊界點落鄰桶)。IC 仍走 exact rank-Pearson (vs scipy 吻合 1e-16)。
- **法人缺值填 0**: large_cap 法人覆蓋 94% / full 87.8%; 缺值 (當日無揭露) 填 0 淨買 = 保守 (壓向中位數)。
- **PIT 對齊**: 法人 day-t 盤後公布 → 訊號用 ≤t 資料預測 t+1..t+h, 進場假設 t 收盤/t+1 開盤, 無 look-ahead
  (鏡像 line2/rvol_atr 慣例)。

## 輸出檔案
| 檔案 | 內容 |
|---|---|
| `line4_flow_gauntlet.csv` | IC/IR/gross/net spread/mono (universe × liq × horizon × cost) |
| `line4_flow_decile.csv` | 各訊號 decile 1-10 報酬 |
| `line4_flow_walkforward.csv` | 年度 IC + LS spread + sign-stability |
| `line4_flow_robust.csv` | deflated sharpe / ex-2020 / LOYO |
| `line4_flow_overlap.csv` | 對 Whale 正交化殘差 IC + 相關係數矩陣 |
