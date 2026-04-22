# VF-Turnover Validation Summary — Decision

Generated: 2026-04-22

## TL;DR (結論)

**Turnover rate (周轉率) 是真 alpha，但屬於「分組型 alpha」不是「線性 IC alpha」**。

| 方向 | QM 池 | Value 池 |
|---|---|---|
| **落地建議** | **不加分**（分組差異不足穩健） | **Top-Quintile 加分 / Top-Decile 篩選** |
| 使用方式 | 純 UI 資訊 / 排除極低流動性 | 納為 Top-N 選股條件或加分因子 |
| 與 RVOL 重疊 | 無 (rho=-0.02) | 無 (rho=-0.14) |

## 為什麼 IC 看似 D/C 級，但實際是真 alpha？

線性 Spearman IC 掩蓋了**分組效應**。Value 池 turnover_20d 的決浦 D1→D10 單調遞增且 spread 年化 +28.5%，
t-stat = 6.10 (per-week 178/309 週為正)，這是極強的訊號，但因為：

1. turnover 分佈極度右偏 (mean 3.8%, p95 13.7%, max > 100%)
2. cross-section Spearman 會被中間段 (D3~D7) 的 rank 噪音拉低
3. **因子真正的 alpha 集中在 tail**（D10 獨強 + D1 獨弱）

所以 **必須用 bucket / quantile 視角驗證，不能只看 IC**。

## 完整數據

### IC（線性）視角 — 表面看似 C/D 級

| Pool | Factor | Horizon | IC | IC IR | t-stat | Grade |
|---|---|---|---|---|---|---|
| QM | turnover_20d | fwd_10d | -0.034 | -0.079 | -1.64 | D |
| QM | turnover_5d | fwd_5d | -0.048 | -0.112 | -2.34 | C |
| Value | turnover_20d | fwd_60d | -0.038 | **-0.230** | -4.04 | C |
| Value | turnover_5d | fwd_40d | -0.035 | -0.198 | -3.49 | C |

### Quantile Spread 視角 — 明顯 alpha

| Pool | Bins | Low ann | High ann | High - Low ann | Monotonic? |
|---|---|---|---|---|---|
| QM | 5 | +12.2% | +31.7% | **+19.5%** | 4/4 Q1<=Q2<=Q4, Q3 dip |
| Value | 10 | +6.3% | +34.8% | **+28.5%** | 7/9 單調遞增 |

### Regime 拆分（Value pool 最乾淨）

**decile 分組：每週 D10-D1 fwd_40d mean 的 t-stat 都 >= 3**

| Regime | N weeks | D1 ann | Mid ann | D10 ann | D10-D1 ann | Monotonic | D10-D1 t-stat |
|---|---|---|---|---|---|---|---|
| **volatile** | 33 | +20.9% | +73.6% | +149.3% | **+128.4%** | **9/9 單調** | **4.81** |
| bear | 57 | +7.6% | +11.1% | +44.2% | +36.7% | 6/9 | 3.53 |
| bull | 219 | +3.8% | +11.1% | +20.0% | +16.2% | 5/9 | 3.43 |

**注意矛盾**：bull regime IC IR = -0.48 (負強), 但 decile D10 > D1。這是因為 bull 時 turnover 分佈內中段排名是噪音（rank correlation 被拉負），但極端 tail 仍有訊號。**所以不能用線性 IC 加分，要用 top-N 過濾**。

### RVOL 相關性（overlap check）

| Pool | Pair | Spearman rho | 結論 |
|---|---|---|---|
| QM | turnover_20d vs rvol_20_calc | -0.017 | 完全不重疊 |
| QM | turnover_5d vs rvol_20_calc | -0.032 | 完全不重疊 |
| Value | turnover_20d vs rvol_20 (native) | -0.139 | 不重疊 |
| Value | turnover_5d vs rvol_20 (native) | +0.088 | 不重疊 |
| Value | turnover_20d vs rvol_20_calc | +0.281 | 不重疊 |

**Turnover 與 RVOL 是完全不同的因子**。RVOL 是「自比較」(近 20d vs prior 60d)，turnover 是「絕對水位」(vs shares outstanding)。相關係數遠低於 0.7 門檻。

## 落地建議

### QM 動能池（保守建議：不加分）

- 池內本就是已篩過的 top-50 高分股，quantile n=5 時 Q3 反而低 (14 年化 9.3%)，**非單調**，t-stat -1.64 不顯著
- Regime 有分歧（bear IC +0.25, bull IC -0.08），訊號不穩
- **結論：純 UI 資訊顯示（讓使用者知道當前選出標的的流動性），不要加入 QM 動能分數**

### Value 價值池（建議落地）

- 全 regime D10 > D1 穩定，t-stat >= 3.4
- D10 年化 +34.8% 大幅贏 D1 +6.3%，差異達 28.5%
- **與 RVOL 不重疊，是獨立新因子**

**落地方案 A（保守加分）**：
- 將 turnover_20d 排 decile 後，前 30%（D8+D9+D10）+1 分、中間 40% (D4~D7) 0 分、後 30% (D1~D3) -1 分
- 整合進 value_score 的 technical_s（目前權重 0.15）

**落地方案 B（優先考慮）— 條件過濾**：
- Value 選股後階段加入「turnover_20d >= 池內中位數」作為**硬過濾條件**
- 好處：簡單 / 不增加分數複雜度 / 直接排除 D1~D5 低流動性長尾
- 注意：價值股常見低流動性，需觀察會砍掉多少候選

**特別提醒 volatile regime**：
- turnover_20d 在 volatile 時 IC IR = **+0.71 A 級**，D10-D1 spread 年化 128%
- 若 regime dashboard 偵測到 volatile，可將 turnover 權重加倍

### 不建議的用法

- **線性加分**：在 bull regime 時 IC IR=-0.48 但 decile 正, 線性權重會把方向搞反
- **QM 動能池**：池已經篩過，quantile 差異不單調

## 輸出檔案

- `reports/vf_turnover_ic_qm.md` - QM 池完整報告
- `reports/vf_turnover_ic_value.md` - Value 池完整報告
- `reports/vf_turnover_ic_qm_panel.parquet` - QM 加值後 panel
- `reports/vf_turnover_ic_value_panel.parquet` - Value 加值後 panel
- `tools/vf_turnover_ic.py` - 驗證腳本（--pool qm|value|both）
