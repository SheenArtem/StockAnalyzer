# Sector Concentration Cap Grid Validation

**Source**: `tools/vf_sector_cap_grid.py`

**Anchor (PRE_POLICY no-cap)**: min_hold=20 / tp=tp_third / rebal=monthly_4w / defer=1mo / **sector_cap=None**

**Grid**: 2 sector taxonomies × 4 cap levels = 8 cells
  - taxonomies:
    - `industry_tse`: QM panel `industry` 欄位 (TSE 25 大類，粗粒度，符合 user 描述「集中在三大 sector」實況)
    - `theme_primary`: `data/sector_tags_manual.json` 24 themes 細粒度 (apple_supply_chain / cowos / ai_server_odm 等)
  - cap levels: no_cap / 0.40 / 0.30 / 0.20

**Design change (during validation)**: 原計畫只用 manual.json 24 themes + 兩種 multi-theme 處理 (primary_only vs equal_split)，但驗證發現細粒度 cap 從不 binding (單一 theme 最多 2 檔/週)。改加入 industry_tse mode (QM panel 自帶欄位) 才是 user 描述的 sector 集中真實層級。

**Universe note**:
  - manual.json: 140 ticker / 24 themes (AI era 主流題材)，QM picks sector="none" 比例 ~52%
  - industry_tse: 196 ticker / 25 大類，QM picks 涵蓋 ~95%+，Value picks unknown bucket 較大 (Value 大多 PE<12 傳產)
  - QM picks 平均 5-9 檔/週 (quality bar 嚴格)，top_n=20 是上限

## R1. Per-cell Performance (Dual side)

### FULL_2020_2025

| Run | CAGR % | Sharpe | MDD % | Hit % | top1 sec avg | top3 sec avg | QM skip |
|---|---|---|---|---|---|---|---|
| industry_tse__cap_20 | 12.99 | 1.027 | -12.08 | 44.3 | 0.482 | 0.895 | 22 |
| industry_tse__cap_30 | 13.04 | 1.035 | -11.52 | 44.3 | 0.487 | 0.897 | 16 |
| industry_tse__cap_40 | 12.82 | 1.012 | -11.52 | 44.0 | 0.491 | 0.897 | 13 |
| industry_tse__no_cap | 12.63 | 0.991 | -11.78 | 43.7 | 0.502 | 0.900 | 0 |
| theme_primary__cap_20 | 12.75 | 1.002 | -11.78 | 43.7 | 0.279 | 0.438 | 0 |
| theme_primary__cap_30 | 12.63 | 0.991 | -11.78 | 43.7 | 0.279 | 0.438 | 0 |
| theme_primary__cap_40 | 12.63 | 0.991 | -11.78 | 43.7 | 0.279 | 0.438 | 0 |
| theme_primary__no_cap | 12.63 | 0.991 | -11.78 | 43.7 | 0.279 | 0.438 | 0 |

### IS_2020_2022

| Run | CAGR % | Sharpe | MDD % | Hit % | top1 sec avg | top3 sec avg | QM skip |
|---|---|---|---|---|---|---|---|
| industry_tse__cap_20 | 15.24 | 1.244 | -9.68 | 42.6 | 0.525 | 0.925 | 19 |
| industry_tse__cap_30 | 15.21 | 1.231 | -9.68 | 43.2 | 0.534 | 0.929 | 14 |
| industry_tse__cap_40 | 14.81 | 1.184 | -10.60 | 43.2 | 0.541 | 0.930 | 11 |
| industry_tse__no_cap | 14.20 | 1.122 | -11.78 | 42.6 | 0.559 | 0.933 | 0 |
| theme_primary__cap_20 | 14.44 | 1.144 | -11.78 | 42.6 | 0.336 | 0.466 | 0 |
| theme_primary__cap_30 | 14.20 | 1.122 | -11.78 | 42.6 | 0.336 | 0.466 | 0 |
| theme_primary__cap_40 | 14.20 | 1.122 | -11.78 | 42.6 | 0.336 | 0.466 | 0 |
| theme_primary__no_cap | 14.20 | 1.122 | -11.78 | 42.6 | 0.336 | 0.466 | 0 |

### OOS_2023

| Run | CAGR % | Sharpe | MDD % | Hit % | top1 sec avg | top3 sec avg | QM skip |
|---|---|---|---|---|---|---|---|
| industry_tse__cap_20 | 3.47 | 0.229 | -13.38 | 33.3 | 0.595 | 0.937 | 6 |
| industry_tse__cap_30 | 3.47 | 0.229 | -13.38 | 33.3 | 0.595 | 0.937 | 6 |
| industry_tse__cap_40 | 3.47 | 0.229 | -13.38 | 33.3 | 0.595 | 0.937 | 6 |
| industry_tse__no_cap | 8.45 | 0.738 | -11.03 | 35.3 | 0.619 | 0.942 | 0 |
| theme_primary__cap_20 | 8.45 | 0.738 | -11.03 | 35.3 | 0.298 | 0.569 | 0 |
| theme_primary__cap_30 | 8.45 | 0.738 | -11.03 | 35.3 | 0.298 | 0.569 | 0 |
| theme_primary__cap_40 | 8.45 | 0.738 | -11.03 | 35.3 | 0.298 | 0.569 | 0 |
| theme_primary__no_cap | 8.45 | 0.738 | -11.03 | 35.3 | 0.298 | 0.569 | 0 |

### OOS_2024

| Run | CAGR % | Sharpe | MDD % | Hit % | top1 sec avg | top3 sec avg | QM skip |
|---|---|---|---|---|---|---|---|
| industry_tse__cap_20 | 10.63 | 1.494 | -4.12 | 57.7 | 0.437 | 0.873 | 4 |
| industry_tse__cap_30 | 10.74 | 1.532 | -4.12 | 59.6 | 0.451 | 0.878 | 2 |
| industry_tse__cap_40 | 9.39 | 1.300 | -4.12 | 59.6 | 0.458 | 0.880 | 1 |
| industry_tse__no_cap | 10.25 | 1.428 | -3.94 | 59.6 | 0.466 | 0.880 | 0 |
| theme_primary__cap_20 | 10.25 | 1.428 | -3.94 | 59.6 | 0.213 | 0.370 | 0 |
| theme_primary__cap_30 | 10.25 | 1.428 | -3.94 | 59.6 | 0.213 | 0.370 | 0 |
| theme_primary__cap_40 | 10.25 | 1.428 | -3.94 | 59.6 | 0.213 | 0.370 | 0 |
| theme_primary__no_cap | 10.25 | 1.428 | -3.94 | 59.6 | 0.213 | 0.370 | 0 |

### OOS_2025

| Run | CAGR % | Sharpe | MDD % | Hit % | top1 sec avg | top3 sec avg | QM skip |
|---|---|---|---|---|---|---|---|
| industry_tse__cap_20 | 5.41 | 0.407 | -10.18 | 39.2 | 0.595 | 0.962 | 0 |
| industry_tse__cap_30 | 5.96 | 0.463 | -9.75 | 39.2 | 0.595 | 0.962 | 0 |
| industry_tse__cap_40 | 7.26 | 0.587 | -8.98 | 39.2 | 0.595 | 0.962 | 0 |
| industry_tse__no_cap | 7.95 | 0.650 | -8.83 | 41.2 | 0.595 | 0.962 | 0 |
| theme_primary__cap_20 | 7.95 | 0.650 | -8.83 | 41.2 | 0.284 | 0.440 | 0 |
| theme_primary__cap_30 | 7.95 | 0.650 | -8.83 | 41.2 | 0.284 | 0.440 | 0 |
| theme_primary__cap_40 | 7.95 | 0.650 | -8.83 | 41.2 | 0.284 | 0.440 | 0 |
| theme_primary__no_cap | 7.95 | 0.650 | -8.83 | 41.2 | 0.284 | 0.440 | 0 |

### BEAR_2022

| Run | CAGR % | Sharpe | MDD % | Hit % | top1 sec avg | top3 sec avg | QM skip |
|---|---|---|---|---|---|---|---|
| industry_tse__cap_20 | 14.60 | 1.273 | -7.01 | 27.5 | 0.658 | 0.962 | 6 |
| industry_tse__cap_30 | 14.33 | 1.213 | -8.00 | 29.4 | 0.676 | 0.968 | 2 |
| industry_tse__cap_40 | 14.06 | 1.195 | -8.00 | 29.4 | 0.682 | 0.969 | 1 |
| industry_tse__no_cap | 14.05 | 1.198 | -7.89 | 29.4 | 0.687 | 0.971 | 0 |
| theme_primary__cap_20 | 14.78 | 1.270 | -7.50 | 29.4 | 0.540 | 0.676 | 0 |
| theme_primary__cap_30 | 14.05 | 1.198 | -7.89 | 29.4 | 0.540 | 0.676 | 0 |
| theme_primary__cap_40 | 14.05 | 1.198 | -7.89 | 29.4 | 0.540 | 0.676 | 0 |
| theme_primary__no_cap | 14.05 | 1.198 | -7.89 | 29.4 | 0.540 | 0.676 | 0 |

## R2. Delta vs no_cap anchor (Dual side, FULL_2020_2025)

| MT mode | Cap | ΔCAGR | ΔSharpe | ΔMDD | mean top1 sec | QM skip |
|---|---|---|---|---|---|---|
| industry_tse | no_cap | +0.00 | +0.000 | +0.00 | 0.502 | 0 |
| industry_tse | cap_40 | +0.19 | +0.021 | +0.26 | 0.491 | 13 |
| industry_tse | cap_30 | +0.41 | +0.044 | +0.26 | 0.487 | 16 |
| industry_tse | cap_20 | +0.36 | +0.036 | -0.30 | 0.482 | 22 |
| theme_primary | no_cap | +0.00 | +0.000 | +0.00 | 0.279 | 0 |
| theme_primary | cap_40 | +0.00 | +0.000 | +0.00 | 0.279 | 0 |
| theme_primary | cap_30 | +0.00 | +0.000 | +0.00 | 0.279 | 0 |
| theme_primary | cap_20 | +0.12 | +0.011 | +0.00 | 0.279 | 0 |

## R3. 2022 Bear Year Stress (Dual side, BEAR_2022)

Key signal: sector cap 是否在科技股集中崩盤年份救命？

| MT mode | Cap | CAGR % | Sharpe | MDD % | ΔMDD vs no_cap |
|---|---|---|---|---|---|
| industry_tse | no_cap | 14.05 | 1.198 | -7.89 | +0.00 |
| industry_tse | cap_40 | 14.06 | 1.195 | -8.00 | -0.11 |
| industry_tse | cap_30 | 14.33 | 1.213 | -8.00 | -0.11 |
| industry_tse | cap_20 | 14.60 | 1.273 | -7.01 | +0.88 |
| theme_primary | no_cap | 14.05 | 1.198 | -7.89 | +0.00 |
| theme_primary | cap_40 | 14.05 | 1.198 | -7.89 | +0.00 |
| theme_primary | cap_30 | 14.05 | 1.198 | -7.89 | +0.00 |
| theme_primary | cap_20 | 14.78 | 1.270 | -7.50 | +0.39 |

## R4. Leave-One-Year-Out Sign Stability (Dual ΔSharpe vs no_cap)

| MT mode | Cap | OOS_2023 ΔSharpe | OOS_2024 ΔSharpe | OOS_2025 ΔSharpe | OOS Win |
|---|---|---|---|---|---|
| industry_tse | cap_40 | -0.509 | -0.128 | -0.063 | 0/3 |
| industry_tse | cap_30 | -0.509 | +0.104 | -0.187 | 1/3 |
| industry_tse | cap_20 | -0.509 | +0.066 | -0.243 | 1/3 |
| theme_primary | cap_40 | +0.000 | +0.000 | +0.000 | 0/3 |
| theme_primary | cap_30 | +0.000 | +0.000 | +0.000 | 0/3 |
| theme_primary | cap_20 | +0.000 | +0.000 | +0.000 | 0/3 |

## R5. Verdict

Grade rubric (per project_validation_bias_warning.md):
- **A**: ΔMDD < -3pp 顯著降 tail risk 且 ΔCAGR > -1pp (機會成本可接受) → 上線
- **B**: ΔMDD < -1pp 但 ΔCAGR < -2pp → trade-off / shadow run
- **D 平原**: |ΔMDD| < 1pp → cap 沒用 / 不上線
- **D 反向**: ΔMDD > 0 → cap 反而更慘 / revert

| MT mode | Cap | ΔMDD FULL | ΔMDD 2022 bear | ΔCAGR FULL | OOS Sharpe Win | Grade |
|---|---|---|---|---|---|---|
| industry_tse | cap_40 | +0.26 | -0.11 | +0.19 | 0/3 | D 平原 |
| industry_tse | cap_30 | +0.26 | -0.11 | +0.41 | 1/3 | D 平原 |
| industry_tse | cap_20 | -0.30 | +0.88 | +0.36 | 1/3 | D 平原 |
| theme_primary | cap_40 | +0.00 | +0.00 | +0.00 | 0/3 | D 平原 |
| theme_primary | cap_30 | +0.00 | +0.00 | +0.00 | 0/3 | D 平原 |
| theme_primary | cap_20 | +0.00 | +0.39 | +0.12 | 0/3 | D 平原 |

## Final Recommendation

**Best cell**: industry_tse + cap_30 (FULL Sharpe Δ +0.044)

**判決**: 不建議上線。理由如下：

1. **theme_primary 模式 (manual.json 細粒度題材) 完全 zero binding** — QM picks 在 backtest 期間單一 theme 最多 2 檔/週，cap 無論設多嚴都不觸發。manual.json 的 24 themes 對 backtest 沒有實際限制力。
2. **industry_tse 模式 (TSE 25 大類) 雖有 binding 但效果參差**：
   - FULL CAGR 略升 (+0.36pp) / FULL Sharpe 略升 (+0.044)
   - **2022 bear MDD 改善小 (-0.88pp)**, 未達 A 級門檻 -3pp
   - **OOS_2023 大砍 (-5pp CAGR)**, 因為 cap 強制砍掉 GenAI 熱潮高表現 picks (3529 silicon_ip / 3680 半導體)
   - FULL MDD 改善 -0.26pp (cap_30) 至惡化 +0.30pp (cap_20)，**雜訊內**
3. **核心結論**：Dual contract picks 規模本來就小 (QM 平均 5-9 檔, Value 在 PRE_POLICY defer=1mo 下也常 cash)，sector 集中本來就被 quality bar 自然攤平。**不需要再加 sector cap，會誤砍真正的 trending sector winners**。

**保留 cap 的條件**: 若未來 portfolio size ≥ 15-20 檔常態 (例如 top_n 增大 + value 條件放寬)，industry_tse + cap_30 可作 risk overlay 重測。當前 contract 不需要。

## Sector Distribution Diagnostics

Backtest period 2020-2025 觀察：

- **manual.json 細粒度題材 (24 themes)**: QM picks 內單一 theme 平均 1.07 檔, max 2 檔 (從不達 cap 門檻)
- **TSE 25 大類 industry**: 2024-2025 AI 期最壞 4 檔/同產業 (半導體業/電子工業)，平均 max=1.68 檔
- **Value side 幾乎全是 sector="unknown"**: PE<12 過濾掉 manual.json 涵蓋的 AI 系，Value picks 主體是傳產 / 金融，industry-level 也分散到 紡織 / 食品 / 鋼鐵 / 航運

## Caveats

- **Sector taxonomy 設計變更**: 原計畫使用 manual.json 24 themes / multi-theme 處理 (equal_split vs primary_only)，驗證發現細粒度從不 binding，改用 QM panel 自帶 industry 欄位 (TSE 25 大類)
- **Sector definition look-ahead**: industry 欄位是 QM panel 抓取時的 classification snapshot；TSE 大類 (半導體業 / 電子工業) 命名穩定多年，look-ahead 影響輕微
- **Unknown bucket 不被 cap**: 不在 mapping 的 ticker (Value 大量 picks) 視為 unknown，不被 cap 限制 (保守做法)
- **Cap formula**: 解讀為「絕對 ticker 數上限 = ceil(cap_pct × n_actual)」，加 floor=2 (3rd ticker 才開始 block) 防小 portfolio 病態。否則 5 檔 portfolio cap_20 等於 1 檔限制
- **Existing positions 不強制 evict**: cap 只在 fill 階段排除新進，不主動踢已有部位 (避免過度換手)
- **無交易成本**: cap 觸發次數低 (13-22 across 6 年)，交易成本影響可忽略
