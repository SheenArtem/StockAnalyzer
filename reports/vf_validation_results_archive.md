---
name: VF 驗證結果彙整（QM + Value 已落地決策）
description: VF-G1 (停損) + VF-V* Phase 1 (Value 因子) + VF-VC P3 (月營收窗口) 三組驗證結論與 live 落地紀錄
type: project
originSessionId: 3be68d43-f449-48bf-be34-67d149d97a4b
---
## QM 停損驗證 — VF-G1（2026-04-17，D 維持現值）

### ATR 乘數使用點盤點

`exit_manager.py` 5 個使用點用 ATR%：

| 使用點 | 常數 | 現值 | clip | 狀態 |
|---|---|---|---|---|
| SL 距 entry | ATR_STOP_MULTIPLIER | 3.0 | 5~14% | D 驗過維持 |
| Breakeven trigger | BREAKEVEN_ATR_MULTIPLIER | 3.0 | 5~15% | D 驗過維持 |
| MIN_SL_GAP | MIN_SL_GAP_ATR_MULT | **1.5** | ≥3% | **B**（VF-1 已驗）|
| MA20 break 容忍 | MA20_BREAK_ATR_MULT | **1.2** | 2~5% | D（需日線 path 模擬）|
| TP scale | ATR_PCT_MEDIAN | 2.5 | 0.7~1.6× | 相對基準 |

### Grid Search 關鍵發現
- **480 組合全部平原**（Sharpe 全距 0.012、mean 全距 0.14pp）
- **無停損 > 任何停損**：純持有 Sharpe 0.182 勝所有組合 +0.98pp
- 「volatile 要更寬 STOP」假說被推翻（最佳仍 3.0）
- Walk-forward best combo test_rank 中位數 261/480 → overfit 嚴重
- **結論**：維持現值，停損價值在尾端風控不在期望報酬增強

### 未驗項目（需日線 path-aware 模擬）
- `MA20_BREAK_ATR_MULT` 盤中 MA20 跌破警報
- Break-even 真實觸發時序
- 防甩轎參數（GRACE_PERIOD/CONSEC_BREACH/VOLUME_CONFIRM）

報告：`reports/vfg1_grid_search.md`

---

## Value Phase 1 — VF-VA/VB/VD/VE/VF 驗證（2026-04-19）

2026-04-19 VF-L1a universe 擴到 2400 檔後，跑 309 週 × 70,760 snapshot rows 完整驗證。

### VF-VA 估值門檻
| Factor | IR | Grade | decile spread |
|---|---|---|---|
| PE (lower) | 0.242 | B | +3.41% (63% 勝率) |
| PB (lower) | 0.153 | B | 無 spread |
| Graham ratio | 0.269 | B | +1.66% |

**Action 候選**：PE 20→12 (+0.77% alpha)，未落地待 walk-forward。
**2026-04-22 晚 VF-VA walk-forward ✅ 落地 (commit `67692cc`)**：
- qWF 15/22 (68.2%), all-period +0.28% 年化, by-year 多頭 +2~3pp / 空頭 -0.2~-1.1pp
- `value_screener.py` DEFAULT_CONFIG `max_pe: 20 → 12`
- **順帶修 BVPS bug** (commit `387e8c2`)：`value_historical_simulator.py` `OrdinaryShare` 是股本 TWD 不是股數，台股面額 10 → `shares = OrdinaryShare / 10`。修前 PB 高估 10 倍、Graham 失真、valuation_s 只剩 PE 起作用
- **VF-VF 擴充重驗 (7 組權重)** 確認 **30/25/30/15/0 仍最優**，V_val_40 marginal 拒絕，PB 修好後 valuation 權重不需提升（revenue 修復增益 >> PB）

### VF-VB 體質
- **F-Score IR 0.892 A 級**（decile spread +9.47%, winrate 79.6%）
- **Z-Score IR -0.271 B 反轉**（safe 加分反 alpha，已刪）

### VF-VD 技術 — 全部反轉
| Factor | IR | 方向 | live 現況 |
|---|---|---|---|
| RSI<30 加分 | -0.225 | 反轉 | 已刪 |
| RVOL<0.5 加分 | -0.208 | 反轉 | 已刪 |
| 近 52w 低加分 | - | 反轉 | 已刪 |
| RSI>70 超買 | +3.87% | 逆直覺 | 保留為 -5 扣分 |

### VF-VE SmartMoney — 無 alpha
- SM composite (外資+投信+自營): IR 0.029 D
- 投信+自營 50/50: IR -0.160 B 反轉
- **→ 砍 SmartMoney 15% 權重**（Round 2）

### VF-VF 權重 walk-forward
| Scheme | Weights (V/Q/R/T/SM) | Sharpe | 決定 |
|---|---|---|---|
| V1 current (原) | 30/25/15/15/15 | 0.813 | baseline |
| V11 (60/25/10/5/0) | 最佳 | 0.943 | **拒絕（10y walk-forward 只勝 57%, overfit）** |

### 已落地

**Round 1 (commit `77563e0`, 2026-04-19)**：
- `_score_quality`：Z-Score safe +8 加分刪，只保留 distress -20
- `_score_technical`：RSI<30 / <40 / RVOL<0.5 / <0.7 / Squeeze / 52w 低全刪，僅保留 RSI>80 -5

**Round 2 (commit `1bc8600`, 2026-04-19)**：砍 SM 15% 權重
```python
weight_valuation=0.35, weight_quality=0.30, weight_revenue=0.18,
weight_technical=0.17, weight_smart_money=0.00
```

---

## Value 營收窗口 — VF-VC P3 (2026-04-20)

### 三個關鍵 bug 修復

1. **`compute_historical_fscore.py` tail(15) 死碼** — `sub_sorted.iloc[-18:-15]` 永遠 out of range，「衰退收斂/加速衰退」分支永遠進不去
2. **方向錯誤**：Value pool 中「YoY 轉正」股票多半已反彈完，真正左側機會是「衰退收斂」
3. **Quarterly 更新太慢**：`quality_scores.parquet` 只存 44 季末日期，中間 3 個月訊號失效

### 解法：1m 單月 YoY + 月度 override

| 層 | 檔案 | 改動 |
|---|---|---|
| 演算法 | `compute_historical_fscore.py` | 3m rolling → 1m 單月 YoY，tail(15) → tail(16) |
| 月度資料 | `compute_revenue_scores_monthly.py` (新) | `revenue_scores_monthly.parquet` 212827 rows |
| 模擬器 | `value_historical_simulator.py` | 新 `revenue_monthly` 參數，PIT join T+10 lag |
| 資料補洞 | `vfvc_backfill_monthly_rev.py` (新) | 206/207 檔缺月營收 FinMind 補 |

### IR 驗證 — 淨改善 IR +1.54

| Factor | IC | IR | Grade |
|---|---|---|---|
| OLD (3m bug) | -0.100 | **-1.075** | A reverse |
| NEW (1m + 月度 + 補洞) | **+0.038** | **+0.465** | B+ |

5/6 年正向 IC（2022 空頭 near-zero），quarterly walk-forward 17/24 季正向。

### VF-VF 權重 walk-forward (shadow 新 snapshot)

| Scheme | Weights (V/Q/R/T/SM) | Sharpe | qWF vs V_live |
|---|---|---|---|
| V_live (舊) | 35/30/18/17/0 | 0.469 | baseline |
| **V_rev_heavy** | **30/25/30/15/0** | **0.489** | **15/24 (63%) ✅** |
| V3_val_heavy | 50/20/10/10/10 | 0.498 | 12/24 (50%) ⚠️ |
| V_no_rev | 43/36/0/21/0 | 0.442 | 8/24 (33%) ❌ |

### P3-b 落地 (commit 2026-04-20 18:35)

- `value_screener.py` DEFAULT_CONFIG 權重改 **30/25/30/15/0**
- `_score_revenue` live 邏輯無需改（RevenueTracker 已是 1m YoY）
- `run_scanner.bat` 恢復 QM + Value 雙 scan
- **首次 live 跑 2026-04-20 22:00**

### 未落地 / 遺留

- 6257 單檔月營收 backfill 失敗（極小影響）
- VF-VA/VF/VE 基於舊 snapshot，理論重跑但優先級低
- 2015-2019 未驗（等 VF-L1c smart_money 擴展完）

---

## VF-VC 營收窗口完整驗證 — 維持 live 不動（2026-04-23）

接續 P3 做完整閉環：P3 只驗 1m / 3m / 4Q-TTM × fwd_60d；完整驗證補齊 **6m / 12m / QoQ** 窗口 + **fwd_20/40/60/120** horizon 穩健性 + **V30 (收斂門檻 × scale × cap) 60 組 grid** + **scale=2 vs scale=4 walk-forward**。

### Phase 1+2 窗口 × horizon (new snapshot 309 週 × 857 檔 × 70,760 rows)

| 窗口 | fwd_20 | fwd_40 | fwd_60 | fwd_120 | 結論 |
|---|---|---|---|---|---|
| QoQ (MoM) | +0.137 B | +0.333 A | +0.234 B | +0.232 B | 有訊號但弱於 1m |
| **1m YoY (live)** | **+0.406 A** | **+0.451 A** | **+0.493 A** | **+0.437 A** | **跨 horizon 全 A，冠軍** |
| 3m rolling | +0.266 B | +0.280 B | +0.286 B | +0.248 B | 次之 |
| 6m rolling | +0.056 C | +0.115 B | +0.075 C | +0.054 C | 訊號被平滑 |
| 12m rolling | -0.002 D | -0.035 D | -0.057 C | -0.255 B(rev) | 120d 翻反轉 |
| 4Q TTM | -0.135 B(rev) | -0.271 B(rev) | -0.314 A(rev) | -0.265 B(rev) | 穩定反向 |

**Key finding**：IR 隨窗口加長單調下降（1m > 3m > 6m > 12m / 4Q TTM）。營收轉折訊號越新鮮越值錢，平均化會稀釋 alpha。

### Phase 3 V30 grid (5 conv × 4 scale × 3 cap = 60 組)

IR 全距 +0.387 ~ +0.502 (**Δ=+0.116，勉強算非平原但接近**)。前 10 名都 A 級 IR 0.49+，前 8 全 scale=4（cap 20 或 30 差 0.001-0.009 忽略不計）。

live combo (conv=0.5, scale=2, cap=20) 排 29/60，IR +0.486，**僅比 best 少 +0.016 IR**。

### Phase 4 穩定性 (live: 1m, 0.5/2/20 @ fwd_60d)

| 年 | IC | IR |
|---|---|---|
| 2020 | +0.0511 | +0.655 |
| 2021 | +0.0362 | +0.558 |
| 2022 (bear) | -0.0251 | -0.507 |
| 2023 | +0.0686 | +0.812 |
| 2024 | +0.0134 | +0.204 |
| 2025 | +0.1003 | +1.135 |

- Year winrate **83%** (5/6)，Quarterly WF **71%** (17/24)，WF IR +0.727
- 2022 空頭唯一負（同 VF-G 系列空頭偏差警告）

### Phase 5 scale=2 vs scale=4 walk-forward（關鍵決策）

24 季 OOS：
- scale=4 季勝率 **58%** (14/24) < 67% threshold
- mean Δ IC (alt - live) = **-0.0007**（scale=4 實際略輸 live）

→ **In-sample grid best 是 overfit，切 scale=4 無 OOS 加值。維持 live `0.5/2/20` 不動**。

### 最終決策

1. **窗口不動**：1m 單月 YoY 已最優，跨 horizon robust
2. **V30 參數不動**：conv=0.5, scale=2, cap=20 live 與 best 差 0.016 IR，Phase 5 WF 證實差距是雜訊
3. **6m/12m/4Q TTM 不採用**：訊號被平滑或反轉，無 alpha
4. **QoQ 環比可加分但不值得**：B 級但弱於 1m，雙用增加複雜度無收益

**Why**：P3 只對 1m vs 3m vs 4Q-TTM 做 horizon=60 單點比較，完整驗證把維度擴到窗口×horizon×參數 grid×WF，才能確認「1m 單月 YoY」不是特定 horizon 的偶然贏家，而是 **跨 horizon + 跨參數 robust winner**。同時排除「grid best 切換」的 overfit 誘惑。

**How to apply**：將來若要動 revenue 權重 / 分支邏輯，這份是 baseline；新提議須先在此框架跑一遍確認不是 in-sample overfit。

### 產出
- `tools/vf_vc_full_validation.py` — 完整驗證工具（Phase 1-5）
- `reports/vf_vc_full_validation.md` — 完整報告
- `reports/vf_vc_full_windows_horizons.csv` / `vf_vc_full_grid.csv`

---

## VF-G5 QM 進場閘門 + Scenario 區間係數 — 4 test D 維持（2026-04-23）

驗 4 個 magic number：Scenario A/B/C 進場區間係數 (0.98~1.02)、DEFAULT_BUY_THRESHOLD (≥3)、DEFAULT_SELL_THRESHOLD (≤-2)、momentum_screener QM entry gate (trigger≥3)。

資料：`trade_journal_qm_tw.parquet` 4923 picks × 205 檔 × 2015-07 ~ 2025-12 (445 週)。

### Test 1 — Score IC within picks

| score | fwd_20 IR | fwd_40 IR | fwd_60 IR | 結論 |
|---|---|---|---|---|
| **f_score** | **+0.106 B** | +0.090 C | +0.099 C | picks 內仍有排序力 |
| body_score | -0.022 D | **-0.100 B(rev)** | **-0.108 B(rev)** | **反轉！** |
| trend_score | +0.084 C | +0.001 D | +0.018 D | 短期 C，長期噪音 |
| qm_score | +0.075 C | +0.003 D | +0.008 D | 組合後被 body 拖累 |

**Key**：body_score 在 picks **內** 反轉（IR -0.10 B(rev)），但 VF-G4 在 **全 universe** 驗過 B 級 IC +0.073。解釋：Body 當 selection filter 有用（全 universe），但 post-filter 內部排序沒用（picks）。兩者不矛盾。

### Test 2 — rank_in_top50 階梯

| band | fwd_20 mean | fwd_40 mean | fwd_60 mean |
|---|---|---|---|
| 1-5 (n=629) | **+2.05%** | +1.94% | +3.14% |
| 6-10 (n=567) | +1.94% | **+4.75%** | **+6.63%** |
| 11-25 (n=1650) | +1.48% | +3.42% | +5.10% |
| 26-50 (n=2077) | +0.99% | +2.67% | +4.37% |

- **fwd_20d 單調遞減**：top 5 > 6-10 > 11-25 > 26-50（+0.99%→+2.05%）
- **fwd_40/60d 反直覺**：6-10 組反而最好，top 5 mean-revert（可能 over-extended）
- 實務持股期 20-40d 內 rank 有效；長期反而 rank 6-10 佳

### Test 3 — Scenario proxy 分類

Proxy：A (trend≥9) / B (=8) / C_mid (=7) / C_low (<7，picks ≥6)。

| scenario | fwd_20 mean | fwd_60 mean |
|---|---|---|
| A (n=1230) | +1.22% | **+2.72%** 最差 |
| B (n=1156) | +1.58% | +5.63% |
| C_mid (n=1842) | +1.47% | +5.05% |
| C_low (n=695) | +1.21% | **+5.88%** 最高 |

**A 劇本 fwd_60 反而最差** — 強力進攻 picks 60 天後 mean-revert 嚴重。fwd_20 差距不大（實務 hold 期影響小）。長期意涵：「強勢股再買」有 exhaustion 風險。

### Test 4 — Entry range fill 率 × fwd return

| range | fill rate | fwd20 fill | fwd20 no-fill | Δ fill vs no-fill |
|---|---|---|---|---|
| narrow (0.99/1.01) | 97% | +1.23% | +6.27% | -5.04% |
| **live (0.98/1.02)** | 99% | +1.33% | +8.41% | -7.08% |
| wide (0.95/1.05) | 100% | +1.39% | +7.96% | -6.57% |

- **fill rate 97%→100%，fwd_20 mean 僅差 0.16pp** → 寬窄 3 組是徹底平原
- no-fill 組 fwd_20 遠高於 fill 組（差 -7pp），但 no-fill 只 49 筆樣本統計不穩
- 方向與 VF-6（pure_right > mixed +22.8pp）一致：**等拉回系統性損失 alpha**

### 最終決策

| 項目 | 現值 | 信心 | 動作 |
|---|---|---|---|
| Scenario 進場區間係數 | 0.98~1.02 | **D 平原** | 不動（寬窄差 0.16pp） |
| DEFAULT_BUY_THRESHOLD | trigger≥3 | D 無法驗 | 不動（簡化 proxy） |
| picks 上限 (top 50) | 50 | **有單調但反轉** | 不動（fwd_20 單調但 fwd_60 反轉） |
| Scenario A/B/C 分類 | 現行 | **反直覺** | 記 follow-up（A fwd_60 -2.7pp） |

### 3 個 Follow-up（都需 walk-forward 才能動）

1. **body_score 30% 權重** — picks 內反轉 IR -0.10，但 VF-G4 full-universe 有 +0.073。需**做 within-picks walk-forward** 看 body IR 反轉是否跨期穩定；穩定才考慮降 body 權重或改 filter-only
2. **Scenario A fwd_60 反直覺** — 可能 regime-dependent（空頭 A 過度 extrapolate）。需跑 regime × scenario 2D 分析
3. **picks 上限由 50 → 20-30** — fwd_20 top 10 明顯優於 26-50，但 fwd_40/60 rank 6-10 最佳；需重跑 simulator 比較不同 top N

### 未驗項目（需重跑 simulator，非本次 scope）

- 真正的 BUY/SELL threshold 變動（journal 都是 post-filter，無法變 threshold 重跑）
- 實際 trigger_score 每週分佈（需建 QM-level snapshot parallel to value snapshot）

**Why**：VF-G5 4 個 magic number 中，entry range coefficient 是最清楚的平原（fill rate 97-100% × fwd 0.16pp 差），直接歸檔 D 維持。其餘 3 個 threshold 類 (buy/sell/top50) 需要 simulator 配合才能真正 vary，本輪沒做。

**How to apply**：未來若要動 QM 選股 threshold 或 scenario 分類 logic，先跑這份 baseline；body_score 反轉是個 flag，若再擴 shadow run 資料可閉環。

### 產出
- `tools/vf_g5_validation.py`
- `reports/vf_g5_validation.md`
- `reports/vf_g5_{score_ic,rank_steps,scenario_proxy,entry_range}.csv`

---

## VF-G5 Follow-ups #1 + #2（2026-04-23 同日完成）

針對 VF-G5 Test 1/2 的 body_score 反轉 + rank 階梯發現做 quarterly walk-forward。

### FU-1: body_score within-picks 反轉 → 雜訊歸檔

42 季 quarterly IC：

| horizon | neg_q_rate | IR_q | grade |
|---|---|---|---|
| fwd_20d | 56% (23/41) | -0.071 | C |
| fwd_40d | 56% (23/41) | -0.191 | B(rev) |
| fwd_60d | 51% (21/41) | -0.188 | B(rev) |

**全未達 67% threshold** → body 反轉 **跨期不穩定**。Test 1 的 IR -0.10 是單一點 sampling，擴大到季度就失穩。

**決策**：QM 權重 F50/Body30/Trend20 **不動**。VF-G4 full-universe body IR +0.073 仍是正確角色（body 做 filter 有用，做 ranking 在 picks 內不穩）。

### FU-2: top_n portfolio 比較 → **live top_20 已是 sweet spot**

跨 horizon 平均：

| top_n | avg Sharpe | avg annual_ret |
|---|---|---|
| **top_10** | 0.248 | **+24.73%** (ret 最大) |
| **top_20 (live)** | **0.286** (best) | +23.58% |
| top_30 | 0.273 | +20.89% |
| top_50 | 0.275 | +18.72% (worst) |

- **top_20 Sharpe 最佳 @ fwd_20d (0.245) 與 fwd_40d (0.293)**
- top_50 **全 horizon annual_ret 最差**（-5.9pp vs top_10）
- top_10 雖 return 最大，但 fwd_20 Sharpe 降 15%，集中度風險高

**關鍵驗證結論**：確認 `momentum_screener.py:37 'top_n': 20` 已是最佳設定（Sharpe-adjusted）。若要更激進 return，可考慮 top_10（但 Sharpe 略降）。

注意：trade_journal rank_in_top50 是 simulator 為了分析保留 top_50 pool，live 本來就只發 top_20 alert。驗證結果支持維持 live 設定不動。

### 最終決策（VF-G5 + 2 FU）

| 項目 | 信心 | Action |
|---|---|---|
| Scenario 進場區間係數 | **D 平原** | 不動 |
| body_score 權重 30% | **D 反轉不穩** | 不動，維持 F50/B30/T20 |
| picks 上限 | **B live 最佳** | 不動，確認 live `top_n=20` 在 Sharpe sweet spot |
| DEFAULT_BUY/SELL threshold | D 無法驗 | 不動（journal post-filter） |

**剩餘 1 個 follow-up 未做**：Scenario A fwd_60 反直覺是否 regime-dependent（低優先）。

### FU 產出
- `tools/vf_g5_followups.py`
- `reports/vf_g5_followups.md` + `vf_g5_fu1_body_wf.csv` + `vf_g5_fu2_topn_portfolio.csv`

---

## VF-US-Momentum Probe — **美股研究告段落**（2026-04-23）

因 VF-Value-ex2 已驗 US F-score / FCF / ROIC / GP 全 D 反向，**TW QM (F50/B30/T20) 框架直接搬 US 不可行**。轉為驗「US 純動能是否有 alpha」決定研究方向。

### 測試

4 個經典動能因子 × 3 horizon (fwd_20/60/120d)，131 個月 × 1563 SP500 + ex-SP500 tickers × 10.5yr。

| 因子 | fwd_20d | fwd_60d | fwd_120d | 備註 |
|---|---|---|---|---|
| 12m return | IR -0.02 / spread -1.89% | -0.02 / -3.24% | +0.01 / -8.34% | D 全反 |
| 12-1m (J-T 經典) | -0.02 / -1.81% | -0.02 / -3.17% | +0.01 / -8.21% | D 全反 |
| 6-1m | -0.01 / -1.65% | +0.05 / -1.32% | **+0.13 / -4.35%** | **IR B 但 spread 負** |
| MA alignment 0-3 | +0.01 / -0.09% | +0.04 / -0.46% | +0.07 / -1.42% | D/D/C |

### 關鍵矛盾

**6-1m fwd_120d IR +0.13 B 但 decile spread -4.35%**：rank IC 有訊號但 portfolio 層面不可實作。原因：US 2015-2025 牛市低基期股反彈巨大，bottom decile 實際報酬 (+17.56%) 遠勝 top decile (+13.21%)。

### US 研究整體結論

| 因子類別 | 結論 | 出處 |
|---|---|---|
| F-Score | D 反向 -10% alpha | VF-Value-ex2 |
| FCF Yield / ROIC / GP | C/D noise | VF-Value-ex2 alt quality |
| 動能 | D portfolio / B rank noise | 本測試 |
| Mohanram G-Score (US Financials) | C 條件成立，已歸檔 | VF-Value-ex3 |

→ **US S&P 500 2015-2025 樣本缺乏 portfolio 層級 systematic alpha**，美股研究短期告段落。

### 建議資源配置

1. **台股 live 穩定性**：shadow run VF-G4 regime filter 累積、position_monitor v2 F-Score 警報運作觀察
2. **Freeze 台股 code**，監控 1-3 個月，不再新增 validation
3. **Watch-list 重啟條件**：
   - Regime shift：US 進入 bear / value rotation → 重跑 F-Score / 動能 IC
   - 資料擴充：Russell 2000 small-cap panel → Piotroski 甜蜜點可能仍在
   - 新因子：學界 ML-based alpha → 評估值不值得建 US 框架

### 產出
- `tools/vf_us_momentum_probe.py`
- `reports/vf_us_momentum_probe.md` + `vf_us_momentum_probe.csv`

---

## V31 Value PEG 窗口驗證 — D 歸檔不動 live（2026-04-23）

Live 實作 `value_screener.py:824-839`: PEG = PE / avg(6 個月 monthly YoY)，加分 PEG<0.5 +12 / <1.0 +8 / >3.0 -5。

測 4 個窗口 (3m/6m/12m/24m) × 4 horizon IC + decile spread。

### 結果

| Window | fwd_20 IR | fwd_40 IR | fwd_60 IR | fwd_120 IR | 結論 |
|---|---|---|---|---|---|
| 3m | -0.09 C | +0.02 D | +0.05 C | +0.25 B | short 略負，long 反向 |
| **6m (live)** | -0.05 D | +0.03 D | +0.07 C | **+0.26 B** | short 平原，long 反向 |
| 12m | +0.09 C | +0.16 B | +0.15 B | +0.26 B | 所有 horizon 反向（最強） |
| 24m | -0.05 D | +0.02 D | +0.06 C | +0.24 B | short 平原，long 反向 |

### 核心矛盾

**PEG 在 value pool 中方向反向**：fwd_120d 全窗口 B 級正 IR (+0.24~+0.26)，意思「高 PEG 贏低 PEG 5-9pp 年化」，與 live 加分邏輯「低 PEG +12」相反。

原因：Value pool 2020-2025 偏 growth dominance 時期，高 PEG 成長股長期贏（同 VF-Value-ex2 F-Score US 失效結論）。

### 實務影響評估

- 短 horizon (20-40d) 實用期 IR 接近 0 (-0.09 ~ +0.16)：平原，邊際零
- PEG 加分在 `valuation_s` (0-100) 內，乘 `value_score` 30% 權重 → 最終 `±3.6` 分
- 影響 Value Top N 排序有限但方向錯會誤排成長股

### 決策

**D 歸檔，不動 live**：實用 horizon 平原 + 加分邊際小，優先級低於 VF-VD（已砍）/VF-VE（已砍 0 權重）。

**Follow-up flag**（未來 Value 重構時考慮）：
- A: 刪除 PEG 加分（同 VF-VD pattern）
- B: 反向 (PEG>1.5 +5 / <0.5 -5) growth tilt
- C: regime-aware

### 產出
- `tools/vf_v31_peg_validation.py`
- `reports/vf_v31_peg_validation.md` + `vf_v31_peg_validation.csv`

---

## VF-G6 QM 軟警報 / 部位參數驗證 — D 維持不動（2026-04-23）

涵蓋 6 項 magic numbers（含 base_pct 8% / trigger multiplier clip / GRACE 5d / CONSEC 2d / VOLUME 0.8 / entry gate ≥3）。

### 可驗項

**T1 QM-weighted vs Equal-weighted portfolio**:
- Live formula: `w_i = clip(qm_score/80, 0.2, 1.5)` normalized per week
- fwd_5/20/40/60d delta IR: +0.053/+0.097/+0.029/+0.047（全 C/D）
- fwd_20d p=0.04 略顯著但 IR C 級邊緣
- **結論**：QM-weighted 與等權統計上相同，部位 formula 無 alpha，但保留無害（UI 顯「根據分數配置」直覺）

**T2 base_pct exposure grid** (4%/6%/8%/10%/12%):
- 測試 journal 是 mixed mode gate 版本，每週僅 ~4.3 picks，total exposure 被系統性低估
- Live 估算（20 full picks × avg qm=61）：base_pct=8% → ~122% 略槓桿，屬合理量級
- **結論**：base_pct=8% 不動

### 未驗項（需 day-level 模擬，本輪跳過）

- GRACE_PERIOD_DAYS=5 / CONSEC_BREACH_DAYS=2 / VOLUME_CONFIRM_RATIO=0.8（防甩轎 3 參數）
- trigger multiplier clip(0.5, 1.5)（journal 無 trigger 欄位）
- QM entry gate threshold ≥3（同 VF-G5 BUY threshold，journal post-filter）

合理預期：exit/position 類參數 5 連 D 前例（VF-G1/G2/G3P1/G3P2/G5）下，G6 這些參數空間**平原機率極高**，需要時可另建 day-level 工具驗，ROI 低。

### 最終決策

**VF-G6 全 D 不動 live**：
- base_pct=8% 不動（sizing hint，無 alpha）
- QM-weighted formula 不動（C 級邊緣，有助 UI 直覺）
- 防甩轎 3 參數 + clip + entry gate 未驗，但 exit 類 5 連 D 推斷平原

### 產出
- `tools/vf_g6_validation.py`
- `reports/vf_g6_validation.md` + `vf_g6_t1_weighted_vs_equal.csv` + `vf_g6_t2_base_pct_exposure.csv`

---

## VF-G5 FU-3: Scenario × Regime 二維分析 — 2022 artifact + C_low bear novel（2026-04-23）

針對 VF-G5 Test 3 的 Scenario A fwd_60 +2.72% 最差 反直覺 flag，驗證是否 regime-dependent。

### 資料與方法

- Journal `trade_journal_qm_tw.parquet` 4923 picks × 205 檔 × 2015-2025
- Scenario proxy: A (trend_score≥9) / B (=8) / C_mid (=7) / C_low (<7)
- Regime 兩套: HMM (trending/ranging/volatile/neutral) + TWII SMA200 (bull/bear)

### 主結果 — Scenario × TWII

| Scenario | bear fwd_60 | bull fwd_60 | Δ (bull-bear) |
|---|---|---|---|
| A | **-2.72%** (n=131) | +3.54% (n=1080) | +6.26pp |
| B | +1.40% (n=129) | +6.48% (n=1007) | +5.08pp |
| C_low | **+10.34%** (n=130) | +4.96% (n=559) | **-5.38pp** （反轉！）|
| C_mid | +1.93% (n=244) | +5.76% (n=1568) | +3.83pp |

HMM regime 分辨力弱（各 Δ 只 -3.83 ~ +0.64pp），**TWII bull/bear 才是主要分辨因素**。

### Robustness check（移除 2022 單年）

**A bear -2.72% 是 2022 artifact**：
- 2022 (Fed 升息三重熊) n=57 平均 -7.17% 主導
- 移除 2022 後 A × bear = +0.71%（從 -2.72% 翻正）
- 2015 +8.18% / 2016 +6.44% / 2019 +11.31% → A bear **無 robust 虧損 pattern**

**C_low bear +10.34% 跨年 robust**：
- 2018 +21.43% (n=15) / 2019 +12.48% / 2020 +49.42%（COVID 反彈）/ 2023 +37.53%
- 移除 2022 後 = **+14.88%**（更強）
- 2022 表現反而最平 (+3.30%)，非單年驅動

### 最終判讀

1. **A bear 虧損 non-robust** → 取消「A × TWII bear filter」落地理由
2. **C_low bear +10.34% (excl 2022 +14.88%) robust pattern** → 新的 regime-aware flag
3. HMM regime 分辨力弱於 TWII SMA200（同 VF-G4 TWII regime filter A 級結論）

### C_low bear pattern 解釋

- C_low (trend_score < 7, picks ≥6) 是「弱動能但 F/body 過關」的 picks
- TWII bear 時這類「沒過熱」股反彈力道強（bottom-fishing 效果）
- 對應 value pool 「衰退收斂」本質（同 VF-VC P3 結論）

### 決策

**本輪不動 live**：
- A bear filter 否決（non-robust 2022 artifact）
- C_low bear 新 flag 歸檔，**需 walk-forward 才能落地**
- 實務可行性問號：live top_20 中 C_low picks 占比？

### 新 follow-up FU-4（歸檔，未做）

C_low × TWII bear 底部反彈 walk-forward 驗證：
1. Quarterly walk-forward 跨 2018/2019/2022 三個熊市 IC 穩定性
2. 落地設計：live scanner 在 TWII bear 時對 C_low picks 特別標記 or 獨立通道
3. 與 VF-G4 只做 volatile filter 的互補關係釐清

優先級：中（有 robust signal 但需驗證與設計複雜度）。

### 產出
- `tools/vf_g5_fu3_scenario_regime.py`
- `reports/vf_g5_fu3_scenario_regime.md` + 4 CSV (baseline/hmm/twii/year_pivot)

---

## VF-G5 FU-4: C_low × TWII bear walk-forward — **關閉，不落地**（2026-04-23）

延續 FU-3 flag，5 個主要 TWII bear cluster (>50 days) 檢驗 C_low bear pattern robustness。

### Bear Clusters 定義

| Cluster | Period | Picks n | C_low n |
|---|---|---|---|
| 2015H2_China | 2015-10 ~ 2016-03 | 40 | 9 |
| 2018H2_Fed_Trade | 2018-10 ~ 2019-03 | 65 | 16 |
| 2020Q1_COVID | 2020-03 ~ 2020-05 | 40 | 11 |
| 2022_FedHike | 2022-04 ~ 2023-01 | 244 | 52 |
| 2025H1_Tariff | 2025-03 ~ 2025-06 | 107 | 26 |

### C_low × bear per-cluster fwd_60d

| Cluster | C_low fwd_60 | Rank vs A/B/C_mid |
|---|---|---|
| 2015H2_China | **-6.12%** | **4 (最差)** |
| 2018H2_Fed_Trade | +7.29% | 2 |
| 2020Q1_COVID | **+49.42%** | 1（COVID V-shape outlier） |
| 2022_FedHike | +6.32% | 1 |
| 2025H1_Tariff | +3.10% | 1 |

### Outlier 敏感性（key finding）

| 情境 | mean fwd_60 | pos cluster |
|---|---|---|
| 全 5 cluster | +12.00% | 4/5 |
| 排除 2022 | +13.42% | 3/4 |
| **排除 COVID** | **+2.65%** | 3/4 |
| **排除 COVID + 2022** | **+1.42%** | 2/3 |

**COVID 是 dominant driver**：+49.42% 單一 cluster 將平均拉升 ~10pp。排除 COVID 後 C_low bear 只剩 +1-3%，與 noise 難區分。2015H2 C_low 甚至 rank 4 反向（A 反而贏 +7.74%）。

### Live 可行性

- Bear regime 週數 103
- 每週 C_low picks 平均 **1.1** (median 1, max 4)
- 0 picks 的週數 **38%**
- signal 太稀疏，無法獨立 portfolio

### 最終決策 — 關閉 FU-4

❌ **不落地**：
1. COVID V-shape 非 typical bear，不代表通用 pattern
2. 排除 COVID 後 +2.65% 與 noise 難區分
3. 2015H2 C_low rank 4 → cluster 間 inconsistency
4. signal 稀疏，實務難配置
5. VF-G4 regime filter (A 級 +78% Sharpe) 已覆蓋熊市防禦需求

**教訓**：
- Auto 判讀（mean + pos count）不足 → 必須 **leave-one-out outlier 檢查**
- 與 VF-G5 Test 2 (top_10 fwd_40 被 6-10 超車) 同 pattern：表面數字與實質 alpha 不一致
- 熊市 alpha 主要靠 **regime filter（策略層級）** 不靠 **scenario overlay（picks 級）**

### 與 VF-G4 的關係

VF-G4 only_volatile regime filter 是 **whole-strategy level** 的解決方案，C_low × bear scenario overlay 的邊際 alpha 在 VF-G4 套用後已吸收。熊市防禦單一答案：regime filter，不需要 scenario-level 差異化。

### 產出
- `tools/vf_g5_fu4_clow_bear_wf.py`
- `reports/vf_g5_fu4_clow_bear_wf.md` + 4 CSV (cluster_scenario / clow_pivot / cluster_pivot / weekly_clow_dist)

---

## 核心教訓

1. **砍掉比優化重要** — VF-G1 480 組合 / VF-G2 1125 組合 / VF-G3 regime 8組 全拿 D 級
2. **平原 > 尖峰** — Grid search 尖峰都是 overfit
3. **IC vs Portfolio 差異** — 截面 IC 略優 ≠ Top-N 選股更好
4. **Value pool 特性反直覺** — 營收轉正不是買點，衰退收斂才是
5. **Bug 與方向一起修** — VF-VC 淨 IR +1.54 就是 bug + 方向雙修

**Why:** QM + Value 主要因子驗證完結階段。三個 VF family 結論集中此處，取代三個獨立 memory。
**How to apply:** 新參數上線前對照「G1 停損維持 / V 權重 30/25/30/15/0 / F-Score A 級 / 技術加分已全刪」四大基線；違反要新驗證。

---

## 2026-04-21 晚 VF-G 系列 10.5yr 收尾重跑（全 D）

補齊 validation_bias_warning 提的「空頭年救命參數」10.5yr 驗證。

### VF-G2 TP ladder 10.5yr（D 維持）
- Best tp1=0.3/tp2=0.4/tp3=0.6/floor=0.9/ceil=2.0 Sharpe 0.169 vs baseline 0.150 vs pure-hold 0.167
- Grade D: pure-hold 仍勝 (mean delta -0.43pp)

**By-year 關鍵發現（SL vs TP 救命 attribution）**：

| Year | baseline_TP | best_TP | SL_only | pure_hold | Verdict |
|---|---|---|---|---|---|
| 2015 熊底 | -3.64% | -3.74% | -3.74% | **-5.05%** | SL 救 +1.3pp |
| 2018 貿易戰 | -1.02% | -1.04% | -1.03% | **-2.20%** | SL 救 +1.2pp |
| 2022 Fed 熊 | -1.98% | -2.11% | -2.13% | **-2.58%** | SL 救 +0.5pp |
| 2023 多頭 | 3.67% | 4.17% | 4.51% | **+6.36%** | pure-hold +2.7pp |
| 2024 多頭 | 3.47% | 4.58% | 4.63% | **+5.77%** | pure-hold +2.3pp |
| 2025 多頭 | 2.64% | 4.58% | **5.26%** | 3.77% | SL_only +1.5pp |

**核心結論**：SL_only ≈ baseline_TP ≈ best_TP（差 <0.2pp）→ 空頭救命主因是 **SL 不是 TP**。TP ladder 對空頭邊際貢獻 <0.2pp，多頭卻吃掉 2-2.7pp。

→ **可安全砍 TP**；SL 必須保留；空頭 alpha 靠 VF-G4 regime filter。
→ 報告：`reports/vfg2_tp_grid_search.md` + `vfg2_by_year.csv`

### VF-G1 WF threshold fix（D 維持）
- 原 bug `walk_forward()` threshold `train>=200/test>=100` 對 10.5yr 太嚴 → 全部 skip
- 改 `train>=80/test>=30` → 79 windows
- Best combo WF test_rank mean 263/480 (55%ile)，baseline 194/480 (40%ile) → **baseline 在 OOS 排名更好，best 過擬合**
- 改 `tools/vfg1_grid_search.py` 同時修 `tools/vfg2_tp_validation.py` (相同 bug)
- Grade D 維持現值；SL 價值在 tail protection，不在期望報酬

### VF-G3 P2 regime group weights 10.5yr（D 維持）
- 資料：用 `trade_journal_qm_tw.parquet` mixed 10.5yr（原本吃 pure_right 5yr）
- V1 current vs V2 flat: IC delta +0.0007（噪音內）, top20 delta 0%
- WF V3_trained IC 0.0367 > V1 0.0290 (+0.008) 但 grid best weights 與 live 反向（trending best w_trend=0.5 vs live 1.3）= 過擬合
- Decision: CUT → 維持全 1.0 砍值（跟現況一致）

### 重跑結論（所有 10.5yr VF-G 都砍）
1. **VF-G1 停損**：D 維持（pure-hold 最佳，SL 保留做 tail protection）
2. **VF-G2 停利**：D 維持 → **可安全砍 TP**（空頭救命來自 SL）
3. **VF-G3 P1 exit_mult**：D 全改 1.0 砍（已落地）
4. **VF-G3 P2 group_weights**：D 全改 1.0 砍（已落地）
5. **VF-G4 regime filter**：**A 級 entry-gate alpha**（F2 only_volatile Sharpe 0.208 vs baseline 0.117, +78%）

**剩餘唯一 bull-bear 救命機制**：VF-G4 regime filter（shadow run 建置完成，累積 1-3 月驗證後 enforce）。

**Bug fix 副作用**：`vfg1_grid_search.py` 與 `vfg2_tp_validation.py` 的 walk_forward threshold 200/100 → 80/30。未來跑這兩個工具 WF 部分會產出結果（之前是空 CSV）。

---

## 2026-04-22 晚 VF-L1b + VF-Value-ex1/ex2 落地

### VF-Value-ex1: DDM 折現率動態化 ✅
`value_screener.py` 加 `_get_discount_rate(is_us)`，替代原本 hardcode r=10%：
- **TW**: 1.8% (10Y 公債 2026Q2) + 6% ERP = **7.8%**（下降 2.2pp → fair price +37.9%）
- **US**: `^TNX` live (~4.3%) + 5.5% ERP = **~9.8%**（基本不變）
- 24h cache；yfinance 無 TW treasury ticker，TW 側 hardcode 先用（TODO: FinMind 訂閱後 live fetch）

### VF-L1b Phase 1 完成 ✅
- `tools/build_us_universe.py` → `universe_us.parquet` 503 檔 S&P 500
- `tools/fetch_us_ohlcv.py` → `ohlcv_us.parquet` 1,335,441 rows (503×10.5yr) / 35 秒
- `tools/fetch_us_financials.py` → `financials_us.parquet` 392,303 rows (503×5季) / 12.5 min
- `tools/compute_us_fscore.py` → `quality_scores_us.parquet` 503 當下 F-Score

**限制**：yfinance 季報只 5 季 → 只能算「當下 F-Score」，沒有歷史 panel。歷史 US IC 驗證仍需 SimFin/Sharadar 付費。

### VF-Value-ex2: F-Score 台美分拆 ✅（descriptive-only，但落地決策明確）

TW vs US F-Score 當下分布（同時間快照）：

| F 分數段 | US S&P 500 (n=503) | TW 全市場 (n=2400) |
|---|---|---|
| ≤3 (trap) | 9.9% | **33.8%** |
| 4 (neutral) | 8.0% | 19.8% |
| 5-6 (avg) | 46.3% | 37.3% |
| **≥7 (strong)** | **35.8%** | **9.2%** |
| ≥8 (elite) | 15.3% | 1.2% |

Mean: US 5.82 / TW 4.17（**差 1.65**）；Median: US 6 / TW 4

**結論**：原本台美共用 `F≥7 → +25` 在 US 失去篩選力（1/3 都達標）。改用分市場門檻：

**value_screener.py `_score_quality_us` 落地（commit TBD）**：
```python
if fscore >= 8: score += 25   # US elite 15.3%（對齊 TW 9.2% 稀缺度）
elif fscore >= 7: score += 10  # US strong 20.5%
elif fscore >= 5: score += 3   # US average 46.3%
elif fscore <= 3: score -= 20  # trap 9.9%
```
TW 保持 `F≥7 +25`（已驗 A 級 IR 0.892）。

**副產品觀察（未修）**：Trap 樣本含 BAC/AIG/AFL 金融保險股，原 Piotroski 2000 論文明確排除金融業（current/debt ratio 語意不同）。未來 VF-Value-ex3 可考慮金融業另算或排除 F-Score。TW 側也有相同限制。

**dry-run review**（`tools/review_fscore_threshold_change.py`）：
- 503 US 股平均 quality score 從 +11.59 → +5.28（-6.31）
- 103 檔 F=7 減 15pt / 233 檔 F=5-6 減 7pt / 167 檔不變
- 新 tier: Elite 77(15.3%) / Strong 103(20.5%) / OK 233(46.3%) / Neutral 40(8%) / Trap 50(9.9%)

---

## VF-Value-ex2 EDGAR 重驗 — **D 級反向**（2026-04-22 晚）

**背景**：上面落地的 `F≥8 +25 / F≥7 +10` 決策只是分佈層面（稀缺性對齊），沒跑 walk-forward IC。用 SEC EDGAR 歷史 panel 補驗後結論翻盤。

### 資料
- SEC EDGAR 全 panel：1512 檔 × 37 季 (2015-12 ~ 2024-12) = **52,062 (ticker, quarter) obs**
- Forward return: entry = quarter_end + 45d（10-Q filing lag），3m / 6m / 12m
- Tool: `tools/vfvex2_edgar_ic.py` + `tools/vfvex2_edgar_ic_pb.py`

### 全市場 cross-section IC

| Horizon | Mean IC | IC IR | t | F≥8 alpha | F≥7 alpha | Top-Bot spread | Grade |
|---|---|---|---|---|---|---|---|
| ret_3m | -0.006 | -0.080 | -0.49 | **-9.67%** | -8.53% | -7.24% | D noise |
| ret_6m | -0.008 | -0.101 | -0.61 | **-10.13%** | -8.38% | -10.61% | D noise |
| ret_12m | -0.017 | **-0.272** | -1.65 | **-10.11%** | -8.19% | **-13.08%** | D weak |

**F-Score 越高，後續報酬越低**。F≥8 alpha 全 horizon -10% 年化。

### 加 Piotroski 原版 P/B screen

| Scenario | N obs | IC IR 12m | F≥8 alpha 12m | Top-Bot spread 12m |
|---|---|---|---|---|
| Unfiltered | 52062 | -0.272 | -10.11% | -13.08% |
| P/B bottom 30% | 14737 | -0.296 | **-4.64%** | -10.83% |
| P/B bottom 20% | 9832 | -0.314 | **-3.15%** | -8.69% |

加 P/B screen 確實收窄反向幅度（-10% → -3%），但仍為負值，Piotroski 原版精神在 US 近 10 年也失效。

### Regime 分拆（6m horizon）

| Regime | N | F≥8 ann | F≤5 ann | Alpha |
|---|---|---|---|---|
| **Bear** | 5523 | +10.19% | +6.80% | **+3.39%** ✅ |
| **Bull** | 39662 | +12.39% | +38.10% | **-25.71%** ❌ |
| Ranged | 1236 | +50.72% | +62.85% | -12.14% |
| Volatile | 5641 | +19.16% | +20.54% | -1.38% |

**唯一正向**：Bear regime F≥8 有防禦價值（+3.39% alpha，符合 Piotroski 原意）。主體 75% 是 bull，整體被吃掉。

### 結構性解釋
在 value universe 中，F≤5 爛股 = 高 beta + 困境反彈潛力，2015-2024 牛市持續跑贏 F≥8 保守股。這是 growth dominance 時代對 value+quality 雙重 screen 的結構性打壓，不是 Piotroski 設計問題，是市場特性變了。

### TW A 級 vs US D 級差異解釋
- TW VF-VB IR 0.892 A 級 跑的是 **QM 動能右側 pick 後 trade_journal**（動能篩後 universe）
- US 這次跑 **全 1512 檔 cross-section**（無動能前置篩）
- 同 metric 在「動能篩後挑品質」vs「全市場純品質排序」效果可能相反
- TW 小盤 / 分析師覆蓋低 / 流動性差 = 正中 Piotroski 原論文 1976-1996 甜蜜點；US 大盤 / 覆蓋飽和 / 訊息效率高 = 甜蜜點不在

### 落地決策（2026-04-22 晚）
`value_screener._score_quality_us` 改：
```python
# F-Score 加分全砍，只保留 F<=3 -20 保底
if fscore <= 3: score -= 20  # value trap 警告
else: details.append("F-Score=X/9 (+0 中性 VF-Value-ex2 D)")
# Current Ratio >=2.0 +5 / <1.0 -8 保留（流動性防禦邏輯）
```

**TW 不動**，保持 `F≥7 +25`（VF-VB A 級）。

### Reports 產出
- `reports/vfvex2_edgar_ic_summary.md` — 全市場 IC summary
- `reports/vfvex2_edgar_ic_pb_filtered.md` — 加 P/B screen 對比
- `reports/vfvex2_edgar_panel.parquet` — raw panel 保留供後續
- `reports/vfvex2_edgar_ic_by_quarter.csv` / `_pb_by_quarter.csv`
- `reports/vfvex2_edgar_decile_spread.csv` / `_pb.csv`

### 下一步（進行中）
派 ic-validator agent 驗 **FCF yield / ROIC / Gross Profitability**（Novy-Marx 2013）三個現代品質因子在同 panel 的 IC，尋找 F-Score 的替代。結果待回報後再決定 US 是否要放回某種品質加分。


---

## 替代品質因子驗證 — 全部 D/C（2026-04-22 晚）

VF-Value-ex2 F-Score 翻 D 後，驗三個現代候選因子能否取代，同 52K panel：

| Factor | IC IR 12m | Top-Bot 12m | Top Q alpha 12m | Grade |
|---|---|---|---|---|
| Piotroski F-Score (ref) | -0.272 | -13.08% | -10.11% | D 反向 |
| FCF Yield | +0.091 | -8.51% | -4.97% | D noise |
| ROIC approx | -0.068 | -8.94% | -5.36% | D noise |
| Gross Profitability (Novy-Marx) | +0.204 | -3.47% | -3.86% | C weak |

**三因子彼此 rank 相關 0.12-0.33，是同一「品質構面」的不同表現**；這個構面在 US 2015-2024 growth dominance 時代**整體失效**（不是指標設計問題，是時代背景）。

### Gross Profitability 亮點（樣本不夠不上線）
Bear regime × P/B bottom 20% 小子集 IR +1.25，2022 單年 +10% spread。但 bear 有效橫斷面季只 4 個（2016-Q1 + 2022 兩 cluster），統計不足以建信號。

### 最終落地決策
- US `value_screener._score_quality_us` 維持：F≤3 -20 value trap + Current Ratio 流動性防禦，**不加任何品質 positive adder**
- **ROIC +8 / FCF Yield +8 加分同步砍** (commit `add628d`, 2026-04-22)：`_score_stock_us` 原本還有這兩個 adder，和 IC 驗證結論不一致，已改為 info-only
- TW 側 F≥7 +25 不動（VF-VB A 級仍有效）
- US alpha 結論：**動能/成長/技術**，不在 quality。未來新因子研究走 VF-G 系列，quality 先 pass

### 啟示
Piotroski 原論文 1976-1996 的小盤/覆蓋低/流動性差環境**已不存在於 S&P 500**；台股仍保有這些特性，所以 TW F-Score 仍 A 級。

### 工具 / 產出
- `tools/alt_quality_factors_ic.py`
- `reports/alt_quality_factors_ic_summary.md`
- `reports/alt_quality_ic_{fcf_yield,roic,gp_assets}_by_quarter.csv`
- `reports/alt_quality_decile_*_by_quarter.csv`
- `reports/alt_quality_factor_corr.csv`
- `reports/alt_quality_by_year.csv`
- `reports/alt_quality_factors_panel.parquet` / `alt_quality_extra_fields.parquet`

---

## VF-Value-ex3 Mohanram 5-lite G-Score（2026-04-22 晚，B conditional 歸檔）

使用者指定走 B1 Mohanram 路線 + 台美分開。實作後跑了 4 組 IC 驗證：

| Track | N obs | Best IR | Grade | 結論 |
|---|---|---|---|---|
| US 全市場 | 74,499 | 6m -0.17 | D 反向 | 拒用（與 F-Score US 翻反向同源，大盤 price-in） |
| **US Financials** | 13,055 | 12m +0.21 | C + G=5 顯著 | G=5 hit 84.3%, ret_12m mean +18.86%, 57 季 94.7% 正報酬 |
| **TW 全市場** | 56,645 | 3m +1.02 (SPY) / bear +1.688 (TWII) | B conditional | bear 單調，bull 非單調（IR 高但 spread -0.58% 賺不到） |
| TW Financials | 577 | 6m +0.10 | D | 21 ticker 樣本太少 |

### 5 signals 能算的（原 8 裡只有 5 個）
ROA / CFOA / Accruals (ROA-CFOA) / Earnings std / Sales growth std — R&D / CapEx / Ad spend 三項兩市場 EDGAR + TW 都無資料無法算。

### TWII regime 複驗（關鍵）
原 SPY regime 給 TW bear IR +2.095 但 validator 自點破「跨市場 regime 有誤差」。2026-04-22 補跑 TWII regime：
- bear IR +2.095 → +1.688（-19%），但 bear 樣本 6,778 → 11,881（**+77% 厚**），**t-stat +4.19 → +4.47 更顯著**
- 新 benchmark `data_cache/backtest/_twii_bench.parquet` (2015-01 ~ 2026-04)
- tool 加 `--regime-bench {spy,twii}` 參數，預設 US=spy / TW=twii

### 決策（2026-04-22 歸檔，暫不 live 落地）
- **US 全市場 D 反向**：拒用
- **US Financials G=5 binary**：C 級，但 US Value tab 目前 hidden，等 US QM VF 完整驗證後再回來考慮
- **TW 全市場 B conditional**：G=5 + TWII bear gate 有 alpha，但 bull 下反賠 -0.58%。條件複雜 + 邊際 alpha 小，**歸檔不進 shadow run**（排序低於主流程）
- **TW Financials D**：樣本 21 檔太少，放棄

### 關鍵教訓（memory 要點）
- **IR vs mean 不一致的陷阱**：TW 全市場 Spearman IC IR +1.02 看似 A 級，但 bucket mean 非單調（G=0 mean +9.68% 比 G=5 +9.34% 還高，被低基期 recovery 飆股扭曲）
- **跨市場 regime proxy 有誤差**：SPY regime 套 TW 抓錯 2016 上半/2019 初/2020 COVID/2025 關稅 bear → 必須用 TWII 複驗
- **「binary filter + regime gate」條件複雜度**：即使有 alpha，如果只在 bear regime 啟用，live 複雜度增加，邊際 alpha 需要夠大才值得維護

### 工具 / 產出
- `tools/vf_value_ex3_gscore_ic.py` (CLI: `--market {us,tw} --regime-bench {spy,twii} --out-suffix`)
- `tools/vf_g1_atr_bucket_feasibility.py` (Phase 2 可行性 3/3 PASS，另一條線)
- `reports/vf_value_ex3_gscore_summary.md` / `_ic_us.md` / `_ic_tw.md` / `_ic_tw_twii.md`
- `reports/vf_value_ex3_gscore_*_{all,fin}_{us,tw,tw_twii}.{csv,parquet}`
- `data_cache/backtest/_twii_bench.parquet` (台股 benchmark，未 commit)

**Why:** 使用者特別指定 B1 Mohanram + 台美分開，結論驗證該方向在當前資料基礎下不足以 live 落地。
**How to apply:** 下次有人提「金融業 F-Score 替代」或「Mohanram G-Score」時，指這份歸檔結論，不要重跑；US Financials G=5 binary 可作為 US Value tab 未來恢復的一個候選，但不是單獨理由。

---

## VF-G1 Phase 2 per-stock ATR 差異化出場（2026-04-22 晚，D 級拒絕）

使用者提 Phase 2 延伸「個股差異化 SL (Beta / ATR% / 融資比 / 主力集中度 / ADV)」。
我先做 ~30min 可行性快測 `tools/vf_g1_atr_bucket_feasibility.py` 3/3 PASS：
- ATR tercile fwd40_mean spread +2.9pp / DD_p5 spread +17pp / slope t=-25.91 極顯著
- **但這只證明橫截面 signal 存在，不代表 exit 可捕獲**

續做 minimal trade-level 模擬 `tools/vf_g1_atr_adaptive_walkforward.py`（4923 trades × 9 schemes, 42 季 OOS walk-forward）：

```
Scheme                    Sharpe   OOS wins/42   Verdict
FIXED_2.5                  0.166   21/42 50.0%   REJECT
FIXED_3.0 (live)           0.165   baseline     —
FIXED_3.5                  0.169   24/42 57.1%   MARGINAL
ADAPT_A (2.0/3.0/3.5)      0.163   14/42 33.3%   REJECT
ADAPT_B (2.5/3.0/3.5)      0.163   15/42 35.7%   REJECT
ADAPT_C (2.5/3.0/4.0)      0.162   15/42 35.7%   REJECT
ADAPT_D (3.0/3.0/4.0)      0.162   10/42 23.8%   REJECT
ADAPT_E (2.0/3.0/4.0)      0.162   15/42 35.7%   REJECT
NO_STOP theoretical        0.167   N/A          天花板
```

**所有 ADAPT 方案 OOS 33-36% 全 REJECT**，Sharpe 擠在 0.162-0.169（同 VF-G1 480 組合平原）。By-bucket 細看：Low ATR 0.169→0.171（差 0.002，毫無意義）、Mid 0.199→0.199（同）、High 0.157→0.153（反而差）。

### 為何 feasibility PASS 但 exit 驗 D？
ATR 差異化 SL 同時：
- 捕獲高 ATR 的 +2.9pp 多賺
- 吃下高 ATR 的 -17pp 更深 DD
- **淨效果 wash**

選股 alpha 已 price in ATR 效應，exit 調整再加維度捕獲不到。「QM alpha 在選股不在 exit」第四次驗證確認。

### 決策
- **Phase 2 per-stock 差異化 SL 砍**（不擴到 Beta / 融資比 / 主力集中度 / ADV 其他維度，橫截面 signal 類似結果可預期）
- 不嘗試 Phase 3（甩轎防護 + 分段停利）/ Phase 4（Regime overlay）的其他擴充
- **FIXED 3.5 uniform** 57% MARGINAL 小贏，不落地
- Exit 調整相關研究 **告一段落**，未來研究資源移到 selection-side (VF-VB / VF-G4 regime filter 類 A 級方向)

**Why:** 驗證 QM exit 參數空間全平原的第四輪確認（VF-G1/G2/G3P1/G3P2 後）。feasibility 快測 PASS 但 exit 捕獲不到 alpha。
**How to apply:** 未來有人提「per-stock 出場差異化」或「動態停損擴充」時，指這份歸檔。若要做 exit 類研究，先 feasibility → 再 trade-level 驗證，**別跳過驗證直接實作**。

### 產出
- `tools/vf_g1_atr_bucket_feasibility.py` (3/3 PASS feasibility)
- `tools/vf_g1_atr_adaptive_walkforward.py` (9 scheme walk-forward, D 級)

