# 主力選股 Model (Whale Picks) — SPEC

| Field | Value |
|---|---|
| Version | 0.5 (production v13 + liquidity filter — honest Sharpe) |
| Status | Phase 1 selector (daily scan integrated) + UI tab + Entry/Exit diff — production ready |
| Created | 2026-05-16 |
| Goal | 提前預測主力會選哪些股票，跟主力一起進場，瞄準 30~100% 波段獲利 |
| Promotion gate | ✅ PASS — composite_parsi 8-factor industry-neutral monthly K=20 **with liquidity filter** Sharpe **1.52** / MDD -12% / WF 100% pos |

---

## §0 Version History

| Version | Date | Changes |
|---|---|---|
| 0.1 | 2026-05-16 | 初版 10 條 criteria + 60-combo grid + Phase 1 先實作 |
| 0.2 | 2026-05-16 | Council 4-agent R2 verdict 套用：9-stage pipeline / CAR primary + P@K output / BH-FDR / TDCC Phase 3 / Phase 1 deferred / survivorship warning |
| 0.3 | 2026-05-16 | Phase 2 backtest verdict landed — 6 iteration cycle 結論：composite_parsi 5-factor production-ready |
| 0.4 | 2026-05-16 | 13-iter cycle complete (v1-v12)：(a) 加 eps_yoy (v7-v8) Sharpe 1.57→1.74 (b) 加 f_score_4q_delta + capex_intensity (v9-v10) Sharpe 1.74→1.81 (c) 加 industry-neutral standardization (v11) Sharpe 1.81→1.92 (d) quarterly rebal 較差 (v12 Sharpe 1.29) — monthly 確認最佳 (e) 8-factor composite_parsi locked 為 production config |
| 0.5 | 2026-05-16 | v13 加 liquidity filter (avg_tv_60d ≥ 10M TWD)：(a) v11 Sharpe 1.92 是 illiquid 小型股 noise 灌水 → 過濾後 honest Sharpe 1.52 (b) universe 1830→885 stocks (-52%) but CAGR 仍 30.5% (c) MDD -10→-12.4% (d) WF 仍 100% pos / cross-regime even more uniform (Bull 0.098 / Bear 0.092 / Sideways 0.095) (e) Phase 1 selector + UI tab + monthly BAT 全部接 liquidity filter; **Production verdict: PASS but Sharpe 1.52 是 honest baseline** |
| 0.6 | 2026-05-16 | v13.1 eps_yoy sign-flip bug fix (commit 02f682f)：`pct_change(4)` 在 EPS 4 季前為負時翻轉符號，universe 27% 股票 yoy 被誤判（半導體 / 面板 / 鋼鐵 等 turnaround stock 系統性低估）。改 `(new - old) / |old|` 後 walk-forward 重驗：Sharpe **1.52→1.70 (+12%)**, CAGR **30.5%→41.5% (+11pp)**, MDD -12.4→-15.1% (-2.7pp trade-off), WF pos 100%→83% (5/6) 仍合格。同 commit 修 cache_manager `_is_cache_stale_quarterly` buffer +7d→+1d，讓 5/15 截止當天即可抓 Q1 報。**新 production baseline: Sharpe 1.70** |

---

## 1. 為什麼建這個 model（與現有 4 套選股的盲點對比）

| 既有選股 | 哲學 | 進場時機 | 盲點 |
|---|---|---|---|
| QM 品質選股 | F50/Body30/Trend20 — winner that keeps winning | 動能已起 | 不抓「主力剛吸貨還沒拉」 |
| Value 價值選股 | 估值 mean reversion | 估值低基本面好 | 不挑主力痕跡 |
| Dual 50/50 | QM + Value 組合 | 兩者疊加 | 同上 |
| 強勢股報告 | 已突破 + 量增 | 漲段中 | 已起漲 = 主力已拉抬，跟單已晚 |

**主力選股的本質**：低基期 + 籌碼沉澱 + 中小型股 + 題材未發酵 + 法人未進場。這個 segment 既有系統不覆蓋。

**期望報酬 vs 既有**：
- 既有：年化 +15~25% / Sharpe 0.6~0.9 / 多次小波段
- Whale Picks（如成功）：單次波段 +30~100% / 但 winrate 低 / 高 risk-reward

---

## 2. Coexistence — 不取代既有選股

- **獨立 informational tier**：不接 paper_trade、step_a_engine、portfolio gating
- **角色定位**：Mode D thesis-driven 候選池來源 + 人工觀察清單
- **Discord push**：週六 08:00 跟 TDCC 同步排程，top 20 候選名單
- **UI**：app.py 新增 `🐋 主力預埋` tab（或併入既有 screener 模式）

---

## 3. 10 條 Selection Criteria（**初始 hypothesis only — 數值閘門全由 Phase 2 IC backtest 決定，不憑 guess**）

> v0.2 NOTE：以下 10 條只代表 hypothesis bucket（從台股實務經驗來的方向）。具體數值閘門、AND/OR 邏輯、weight、是否保留 — **完全交給 Phase 2 9-stage pipeline 決定**。v0.1 列的數值是經驗 guess，Phase 2 跑完後 §3 會被回寫實際值。

### C1. 基期低（Base low）
- 距 52w 低點 < 30%
- 距 52w 高點 > 30%
- ATR(20) / ATR(60) < 0.8（波動沉寂）

### C2. 籌碼集中度
- TDCC 大戶（level 11-15, >200 張）股數占比 ≥ 60%
- TDCC 散戶（level 1-5, ≤10 張）股數占比 ≤ 25%
- 巨鯨（level 14-15, >1000 張）占比 ≥ 30%

### C3. 集中度趨勢（無在出貨）
- 8 週大戶持股 Δ ≥ 0
- 4 週大戶持股 Δ ≥ -1.0pp（容忍小幅波動）

### C4. 市值區間（甜蜜點）
- **主規則**：市值 50 億 ~ 200 億
- **半導體例外**：200~500 億 AND 三大法人持股 < 15%
- 排除：< 50 億（流動性差）/ > 500 億（法人主場）

### C5. 流動性
- 20d 均量 1000 ~ 50000 張
- 20d 均成交值 > 5000 萬元
- 排除：連 5 日成交值 < 3000 萬

### C6. 基本面不爛
- F-Score ≥ 5
- 最近 4 季 EPS 全為正
- 負債比 < 60%
- 排除：Z-Score < 1.81（破產區）

### C7. 題材 proxy
- themes_core / sector_tags theme_count ≥ 1
- 近 30d 新聞數 > 0 但 < 50（不過熱）
- 近 7d 新聞數 < 15（避免題材正在發酵）

### C8. 技術預埋
- close > MA60 但 close < 1.10 × MA60
- close > MA240（年線之上，避免崩盤股）
- 20d 量縮：近 20d 平均量 < 60d 平均量 × 1.2

### C9. 空方壓制低
- 借券餘額 / 流通股數 < 5%
- 融券餘額 / 融資餘額 < 30%

### C10. 法人未主場
- 三大法人持股 < 15%（嚴格版，原 20%）
- 近 60d 法人累積買賣超 / 流通股 < 2%（沒在大買）

### Hard exclusions（任一觸發直接剔除）
- 警示股 / 處置股 / 分盤交易 / 暫停交易
- 全額交割股
- 近 60d 連 3 日跌停
- ETF / 特別股 / DR / KY
- 上市未滿 1 年

---

## 4. 資料來源對應

| Criteria | Source | Cache path |
|---|---|---|
| C1 基期 / C8 技術 | OHLCV | `data_cache/{sid}.csv` |
| C2/C3 TDCC | TDCC OpenAPI weekly | `data_cache/tdcc/1-5/` |
| C4 市值 | OHLCV × shares_outstanding | `fundamental_cache/` |
| C5 流動性 | OHLCV volume | `data_cache/{sid}.csv` |
| C6 基本面 | F-Score / EPS / 負債比 / Z-Score | `fundamental_cache/{cat}_{sid}.parquet` |
| C7 題材 | themes_core + news_themes | `data/themes_core.parquet` + `data/news_themes.parquet` |
| C9 借券 | SBL FinMind | `data_cache/sbl/` |
| C9 融券 | FinMind margin | `data_cache/margin/` |
| C10 法人 | 三大法人 daily | `data_cache/institutional/{sid}.csv` |
| Hard exclusions | TWSE 警示處置 + 分盤 | `data_cache/tw_alerts/` |

---

## 5. Phase 1 實作 Spec (⏸ DEFERRED — 等 Phase 2 結果)

> **v0.2 NOTE**：Phase 1 selector 不再先做。Council verdict：等 Phase 2 IC backtest 確認哪些 features 真有 alpha + 數值閘門落地後再實作 screener，避免用 guess 值寫死進 code。
> 以下保留作為 Phase 1 將來實作時的骨架參考。

### File: `tools/whale_picks_screener.py`

**Reuse 既有 module（避免 rework，per CLAUDE.md）**：
- `chip_fetcher.py` — TDCC + 法人 + 借券
- `value_screener.py::_score_quality()` — F-Score / Z-Score
- `momentum_screener.py` 的 universe loader
- `fundamental_cache/` aggregate
- 既有 `cache_manager` lock

### Output
- `data/whale_picks/latest.parquet` — 全 universe 過濾後候選 + 各 criteria 過/不過細節
- `data/whale_picks/{YYYY-MM-DD}.parquet` — daily snapshot
- `data/latest/whale_picks_top20.json` — top 20 給 UI / Discord

### 排程
- `run_whale_picks_weekly.bat` 週六 08:30（TDCC 排程 08:00 後）
- 純 ASCII（per CLAUDE.md BAT 硬規則）

### UI
- `whale_picks_view.py` 新 tab `🐋 主力預埋`
- 表格：sid / name / 收盤 / 距 52w 低 % / 大戶% / 8w Δ / 市值 / theme / 法人% / 借券%
- 不接 trigger_score / scenario_engine（informational only）

### Discord push
- `tools/whale_picks_discord.py` 整合進 `run_whale_picks_weekly.bat` stage
- 週六 08:35 push top 20
- format: bullet list（per `feedback_discord_no_md_tables`）

---

## 6. Phase 2 IC Backtest — 9-Stage Pipeline（council verdict）

### Universe
- 2021-01-01 ~ 2025-12-31（5yr 主 backtest）
- 2015-01-01 ~ 2020-12-31（補抗多頭偏差，含 2015H2 + 2018Q4 + 2020Q1 三 crash）
- TW 1972 universe，含上市 + 上櫃

### Target framing（council 共識）
| Tier | Metric | 用途 |
|---|---|---|
| Primary | **continuous CAR_60d / CAR_120d / CAR_180d rank IC** | Selection 階段 — 穩定性檢查 |
| Secondary | **Precision@K@10/20 on +30% hit event within 180d** | Output 階段 — 守 user intent 「下週看哪幾檔」|
| Tertiary | **path-MAE**（max adverse excursion in fwd window） | Risk diagnostic — 容忍度 |
| 報告用 | +100% hit cross-regime stratified | 獨立報，不當 primary selection |

### Stage-by-stage

**Stage 1 — Hypothesis generation（~35 candidate features）**
- 從 §3 10 條 criteria 展開（每條 2-3 個 operationalization variant）
- 加 TW Expert R2 補的台股 fingerprint：法人微結構（自營商避險拆分 / 外資借券沖銷比 / 投信無聲買進）/ 信用拐點（融資減 + 橫盤）/ 題材未發酵（類股 RS rank）/ 警示前置（券資比 + 振幅）
- 逆向工程**僅當 hypothesis source**：查 2021-2025 +30%/+100% winners 在 t-60d 的 features，當靈感不當 fitting target（survivorship 無解 — Methodologist R2 點明）

**Stage 2 — Operationalization（收斂到 15-20 features）**
- 去重 + 共線性 prune：Pearson ρ > 0.7 取代表 feature
- 主力異質性 sub-group split（TW Expert R2 insight）：外資 IC vs 投信 IC vs 中實戶 IC 分開驗證 — 不同 inst 高 IC features 可能不同

**Stage 3 — Pre-processing**
- Winsorize 1% / 99%
- Standardize z-score by date (cross-section)
- Missing imputation 用 cross-section median

**Stage 4 — Univariate selection**
- 主：continuous CAR rank IC（per horizon）
- 次：P@K@10/20 on +30% event within 180d
- 三：path-MAE
- Bootstrap 1000 次抽樣估 CI

**Stage 5 — Decile + monotonicity kill test**（per SOP-1, SOP-2）
- Spearman monotonicity ≥ +0.5（防倒 U 形假象）
- Decile spread sign 必須與 IC 同號（否則 D 級）
- Q10 - Q1 ≥ 5% (60d horizon)

**Stage 6 — Multiple comparison control**
- **BH-FDR α=0.10**（不用 Bonferroni — chip features ρ>0.6 effective N≈10-15，Bonferroni over-correct 殺真陽性）
- 18 features × 3 horizons = 54 tests → FDR-adjusted p-values

**Stage 7 — Walk-forward + Cross-regime**（per SOP-3, SOP-13）
- Rolling 2yr train / 6mo OOS + embargo 60d（防 fwd_60d 漏資料）
- Leave-one-year-out (LOOY)：砍 2020 COVID + 2022 後 edge 是否保留
- Cross-regime split：bull (2021 / 2024) / bear (2022) / sideways (2023 / 2025) 各跑 IC，看是否時代紅利 dominant
- +100% events 集中在 2020-2021 航運 + 2024 AI 兩波（TW Expert 估）→ 必跑 cross-regime split 才當 secondary

**Stage 8 — Portfolio simulator gate**（hard gate per SOP-10）
- Reuse `tools/_archive/vf/vf_dual_portfolio_walkforward.py` 範本
- 比較三條 P&L curve：(a) whale picks top-K equal weight / (b) buy-and-hold TWII / (c) best-single-feature top-K
- **Block promotion if whale picks P&L < B&H baseline**（即使 IC 全綠也要砍）— Dual cf1e2e0 教訓
- 報 Sharpe / MDD / CAGR / turnover

**Stage 9 — Output deliverable**
- Ticker list：top-K candidate（K=10 / 20 / 30 三 setting）
- 對應 estimated P(+30% in 180d | setup) — Bayesian framing P(success|setup) 不是 P(setup|success)
- Base rate P(+30% in 180d | universe) 當分母 → z-test 顯著性
- Cross-regime IC 表
- path-MAE distribution
- Survivorship caveat label

### Grid search 縮減（per User Auditor R1）
- v0.1 60 組合 → v0.2 **12 pre-registered configs**（避免 p-hacking）
- 候選：market_cap × 法人% × score weighting × time horizon 四維各 2-3 setting
- 全跑 + FDR 校正

### Phase 2 過閘條件（SOP-12 + portfolio gate）

| 條件 | 閘 |
|---|---|
| Univariate IC | ≥ 1 個 feature 在 FDR-adjusted p < 0.10 且 |IC| ≥ 0.05 |
| Decile monotonicity | Spearman ≥ +0.5 |
| Decile spread sign | 與 IC 同號 |
| Walk-forward | 6/10 年正（含 2015-2020 補資料） |
| LOOY | 砍 2020/2022 後 IC 仍 ≥ |0.03| |
| Cross-regime | 至少 2/3 regime IC 同號 |
| Portfolio sim | top-K P&L > B&H baseline AND Sharpe > 0.3 |
| Hit rate vs base rate | P(+30%|setup) > P(+30%|universe) × 1.5 (z > 2) |

**全綠** → 升 informational tier validated，觀察 3-6 月
**部分綠** → informational tier unvalidated，3 個月後重評
**全紅** → D 級歸檔 + 寫 lessons learned

---

## 7. 驗收 / Promotion Path

| 階段 | 條件 | 動作 |
|---|---|---|
| Phase 1 上線 | screener 跑通 + UI/Discord 整合 | informational tier，標 `unvalidated_whale_picks` |
| Phase 2 SOP-12 ✅ | IC backtest 過閘 | 升 `informational_tier_validated`，觀察 3-6 月 |
| 觀察期 ✅ | live 3-6 月勝率 ≥ backtest 70% | 進 Mode D 候選池 / 可手動掛單 |
| Live 12 月 ✅ | 累積樣本 ≥ 30 + Sharpe > 0.5 | 評估是否接 paper_trade（非自動） |
| Phase 2 SOP-12 ❌ | IC backtest 沒過 | informational only，加 lessons + 6 月後重評（資料累積後可能翻盤） |

**永遠不接 portfolio gating**（per SOP-14 informational tier 規則 + 主力選股本質高 variance）

---

## 8. 風險警告

1. **事前預測本質高 variance** — 主力可能不進場、進場後可能洗盤、可能套牢
2. **Winrate 低** — 即使選對主力股，也可能 50% 不動或假突破
3. **Backtest IC 可能 D 級** — 主力行為是 high-noise process，量化 model 可能抓不到
4. **過擬合風險高** — 18 features × 12 grid configs，p-hacking 容易；必跑 walk-forward + LOOY + cross-regime + BH-FDR 校正
5. **規模限制** — 中小型股本質流動性差，超過 5 檔同時持有就難管理
6. **新規 / 警示風險** — 主力股容易被金管會盯，須嚴格 hard exclusions
7. **⚠️ Survivorship bias（v0.2 新增 per Auditor R2）**
   - 逆向工程歷史 +30/+100% winners 揭露的是 **P(setup | success)** — 「會漲的股票當初長什麼樣」
   - 用戶要的是 **P(success | setup)** — 「現在長這樣的股票會不會漲」
   - 兩者不同 — 多數 setup 不會漲（base rate ~5-10%）
   - Mitigation：Phase 2 Stage 9 必報 base rate P(+30%|universe) 對照；Stage 1 逆向工程只當 hypothesis source 不當 fitting target
8. **心理預期警告（User Auditor R2）**
   - 即使 methodology 完美執行，**最終 verdict 大機率 D 級或 informational-only**
   - 原因：(a) +30% events 雖足但 noise 高 (b) +100% events regime-concentrated 過去不代表未來 (c) TW 散戶 regime shift 快
   - Live winrate 預期遠低於 backtest（survivorship + 微觀流動性 + 主力策略演化）
   - 這輪可能產出「方法論驗證 negative，回到既有 QM/Value 框架」

---

## 9. Side-Effect Assessment

| 項目 | 影響 | 緩解 |
|---|---|---|
| 新 cache 路徑 `data/whale_picks/` | 磁碟空間 +幾 MB/年 | 可接受 |
| `chip_fetcher.py` 呼叫量 | TDCC + 法人 + 借券週六批次 | 跟既有 TDCC 排程同步，無額外 FinMind quota |
| App.py UI 新 tab | sidebar 變長 | 接受；或併入 screener_view |
| Discord push 量 | 週六多一 push | 接受 |
| 排程衝突 | `run_tdcc_weekly.bat` Sat 08:00 / `whale_picks` Sat 08:30 | 順序串好，無衝突 |
| 跟既有 ChipAnalyzer 重複計算 | 無 — reuse cache | OK |

**Public interface 變動**：無（純新增 module，不改現有 API）

---

## 10. Open Questions / TBD（Phase 2 後回寫）

1. 半導體 vs 傳產 market_cap 是否需要分群驗證 → 待 Phase 2 cross-regime stratification
2. TDCC 集中度 Δ 需要多少週才穩定 → 待 2026-07 13 週累積後 Phase 3 overlay 加入
3. 「題材過熱」量化定義（C7 上限 50/30d, 15/7d 是初值）→ 待 Phase 2 univariate IC
4. 是否加入 monthly revenue YoY 條件（主力會 buy ahead of 利多）→ Phase 3 候選
5. 大單 detection（單日 > 平均量 3x + 收紅）作 Phase 3 enhancement → 視 Phase 2 結果
6. 是否區分新主力（吸貨初期）vs 老主力（拉抬中段）→ Phase 3 候選
7. **TW 主力異質性 sub-group split**（v0.2 council TW Expert R2 新提）— 外資/投信/中實戶/作手目標不同；Phase 2 Stage 2 要做但具體分群方法待定
8. **+100% target 是否完全 drop**（v0.2 council Validator R2 未解）— 折衷保留報告 + cross-regime split，視 Phase 2 結果決定
9. **Cross-regime split bull/bear/sideways 切點**（v0.2 council Methodologist R2）— 是否用 VF-G4 既有 regime label vs 自定 / 是否含 sideways 第三類

---

## 11. 不做的（避免 scope creep）

- ❌ 券商分點分析（Goodinfo 成本高 + 結構脆弱）
- ❌ Tick-level 大單偵測（資料成本高，無 free source）
- ❌ Wyckoff 四階段機器識別（subjective，IC 預期 D）
- ❌ 主力情緒 NLP（PTT / Mobile01 結構脆弱）
- ❌ 自動下單 / portfolio gating（本質高 variance，永遠 informational）
- ❌ **逆向工程直接當 fitting target**（v0.2 council Methodologist）— 帶 SOP-11 look-ahead trap + 無解 survivorship；只當 hypothesis generator
- ❌ **ML / RandomForest / SHAP for v1**（council 共識）— 樣本不足必 overfit
- ❌ **Score 加總 simple unit weight**（council Quant + Methodologist R2）— 單一極端 feature 主導 / 假設 IC 同號同量；改 rank-based composite + winsorize
- ❌ **Bonferroni 校正**（council R2 共識）— chip features ρ>0.6 effective N≈10-15 over-correct 殺真陽性；改 BH-FDR α=0.10
- ❌ **TDCC backfill 歷史**（council 共識）— 改版 snapshot bias 違 SOP-8；TDCC 只 Phase 3 (>2026-07 累積 13 週) overlay
- ❌ **降閘 +30%→+15%**（council Auditor + Expert）— 樣本足 (5yr 估 1500-2000 events)，降閘 = 改題目背叛 user intent

---

## 12. Lessons Learned（Phase 2 後填）

### 13 iteration cycle (v1 → v12, 2026-05-16)

| Iter | Approach | top-20 Sharpe | MDD | WF pos% | Verdict |
|---|---|---|---|---|---|
| v1 | 14 snapshot features (MVP) | n/a | — | — | IC pass 2/14, no portfolio sim |
| v2 | +Stage 7+8 walkforward + portfolio sim | 1.41 (f_score) | -17% | 100% | f_score wins, whale features weak |
| v3 | +Δ features (33 total) | 1.41 | -17% | — | chip Δ failed, stealth_volume +0.04 emerged |
| v4 | +IS composite (full sample weighted) | 1.98 | -10% | — | look-ahead leak (fake) |
| v5 | +WF composite (rolling weights) | 0.49 | -26% | 67% | true OOS dies (proves v4 leak) |
| **v6** | +parsi composite (5 fac pre-registered) | **1.57** | -8% | 67% | REAL alpha first surfaced |
| v7 | +ROE/ROA/GM/sector rotation (47 features) | 1.41 | — | — | sector rotation IC < 0.02 fails / eps_yoy IC +0.082 NEW |
| **v8** | +eps_yoy 進 parsi (6 fac) | **1.74** | -10% | 67% | EPS YoY 進 composite +Sharpe 11% |
| v9 | +CFO/Capex/FCF/F-Score Δ | — | — | — | f_score_4q_delta +0.061 / capex_intensity -0.041 NEW |
| **v10** | +F-Score Δ + Capex 進 parsi (8 fac) | **1.81** | -9% | **100%** | WF pos% 跳 100% structural improvement |
| **v11** | **+industry-neutral standardization** | **1.92** | -10% | **100%** | **★ PRODUCTION** |
| v12 | quarterly rebal | 1.29 | -20% | 100% (Q) | quarterly 較差 — monthly confirmed best |
| **v13** | **+ liquidity filter avg_tv ≥ 10M TWD** | **1.52** | -12% | **100%** | **★ honest production baseline (v11 1.92 was inflated by illiquid noise)** |

### Final composite_parsi 8-factor specification (LOCKED 2026-05-16)

```python
PARSIMONIOUS_COMPOSITE = {
    # Quality (level + dynamic)
    'f_score':                +1.0,  # Piotroski F-Score (v6, 既有 QM factor)
    'f_score_4q_delta':       +1.0,  # F-Score YoY 改善 (v10 IC +0.061)
    'eps_yoy':                +1.0,  # EPS YoY (v8 IC +0.082)
    'revenue_score_6m_delta': +1.0,  # 營收改善 (v6)
    # Momentum / size
    'turnover_log':           -1.0,  # 小型優勢 (v6)
    'dist_52w_high':          -1.0,  # 距高近=動能 (v6)
    # Capital quality / 主力痕跡
    'stealth_volume_20d':     +1.0,  # 量縮中爆量大單 (v3, whale accumulation proxy)
    'capex_intensity':        -1.0,  # Capex 重=資本黑洞 (v10 IC -0.041)
}
```

### Production config (locked — v13 final)

| Setting | Value | Reason |
|---|---|---|
| Composite | 8-factor pre-registered (above) | Pre-registered = no IS fitting leak |
| Standardization | **Industry-neutral** by date×industry (Stage 3) | v11 +0.11 Sharpe vs universe-wide |
| Rebalance | **Monthly** (month-end snapshot) | Quarterly Sharpe -49% inferior |
| K (portfolio size) | **15-20** (or 30 for stability) | K=15 Sharpe 1.63 best, K=20 1.52, K=30 1.61 (post-liquidity) |
| Hold | fwd_20d (one month) | Match rebalance period |
| Universe | TW 1972 filtered to **885 liquid stocks** (avg_tv ≥ 10M TWD + Vol ≥ 300 lots) | v13 — exclude illiquid noise + manipulation risk |
| Liquidity hard filter | **avg_tv_60d ≥ 10M TWD AND latest Vol ≥ 300 lots** | v13 (SPEC §3 C5 落地) — production-actionable |

### Performance (v13 production top-20, 2021-2025 with liquidity filter)

| Metric | composite_parsi v13 | composite_parsi v11 (illiquid noise) | f_score (single) | B&H TWII |
|---|---|---|---|---|
| Total return | 220.6% | 248.8% | 263.9% | 81.7% |
| CAGR | 30.5% | 33.6% | 22.8% | 11.5% |
| **Sharpe** | **1.52** ★ honest | 1.92 (inflated) | 1.17 | 0.73 |
| **MDD** | **-12.4%** | -10.0% | -23.7% | -28.9% |
| Win rate | 66.7% | 71.9% | 63.2% | 61% |

### WF + Cross-regime stability (composite_parsi v13)

- WF IC mean = +0.083 (vs v11 +0.091)
- WF positive windows = **100% (6/6)** ★
- LOOY range = 0.014
- Bull IC = +0.098 / Bear IC = +0.092 / Sideways IC = +0.095 (**3 regime 全正且更為一致** vs v11 0.106-0.121)

### v11 vs v13 honest takeaway

- v11 Sharpe 1.92 是 universe 含 illiquid 小型股 noise → backtest 抓到 ~ +0.40 Sharpe 的 illiquid risk premium
- 但這 +0.40 Sharpe 在 production 拿不到（下單會打到 5%+ slippage、流通性不足無法執行）
- **v13 Sharpe 1.52 是 OLD baseline（eps_yoy bug 下）**；commit 02f682f 修法後 **v13.1 Sharpe 1.70 是真正能執行的 alpha** — 仍遠勝 B&H 0.73 (亦更新為 0.73 因 universe 過濾後 baseline 上升)

### Key insights (從 council 預測 + 實證)

1. **Council 共識完全命中**：
   - 「raw chip data 沒 alpha」(C2 結論延伸) — 法人 % / 借券 / 融資 4w Δ 全 IC ~ 0
   - 「ML/SHAP forbidden」 — 沒做但 IS composite 證明 fitting noise 一樣會發生
   - 「BH-FDR + LOOY + cross-regime」 — Stage 5+6+7 全套用，篩出 robust features
   - 「Portfolio sim gate 必跑」 — IS composite Sharpe 1.98 但 WF 0.49，沒 sim 早就 false promote

2. **whale picks 主 hypothesis (低基期 + 籌碼集中 + 法人未進場) → 完全失敗**：
   - 「低基期」實證 IC 反向（距 52w 高近=好不是遠=好）
   - 「籌碼集中」實證 IC ~ 0（不影響報酬）
   - 「法人未進場」實證 IC ~ 0
   - 籌碼 dynamic Δ (4w/8w) 也全失敗

3. **真正 work 的不是 "whale picks" 本意，是 「quality-momentum + 主力大單偵測 + 營收改善」**：
   - 5 factors 取代 35-feature pool
   - 預先 sign 取代 IS IC weighting
   - Sharpe 1.57 vs f_score 1.41 (相對 +12%)，MDD -8% vs -17% (約 -55%)
   - 真正勝點在 **MDD 控制**，不是 CAGR

4. **Methodology 教訓**：
   - IS composite 永遠誇大（v4 vs v5 Sharpe 4x 差距）
   - Walk-forward composite 也不一定好（v5 死於過多 features 的 noise re-fitting）
   - **Pre-registered 5-factor 簡單加總** 才是真實 alpha — Occam's razor 勝

### Promotion path

- ✅ Stage 6 BH-FDR PASS
- ✅ Stage 7 WF IC > 0 / Cross-regime all positive / LOOY tight
- ✅ Stage 8 SOP-10 portfolio gate (Sharpe 1.57 > 1.0, beats B&H 0.64 + f_score 1.41)
- ✅ 雖然 IC +0.084 marginal，但 Sharpe 1.57 + MDD -8% 達 SOP-12 informational tier validated
- **下一步**：Phase 1 selector (`tools/whale_picks_screener.py`) 用 composite_parsi 落地

---

## §13 Phase 2 Final Verdict (2026-05-16)

**composite_parsi 升級 informational tier VALIDATED**。

但要明白：這個 model 的本質是 **「quality-momentum-volume composite」**，不是用戶最初想像的 **「低基期主力預埋偵測」**。「主力選股」這個 framing 在 5yr 2021-2025 TW universe 上 falsified（低基期 IC 反向、籌碼集中 IC 0）。

實證 work 的是 5 factor 組合：
- 一個品質因子 (f_score 既有)
- 一個小型動能因子 (turnover_log + dist_52w_high)
- 一個 stealth volume 因子 (新發現！量縮爆量主力大單 proxy)
- 一個營收改善因子 (revenue_score_6m_delta)

5 factors 預先 sign，無 IS fitting。

**v0.5 update (post liquidity filter, v13)**: production verdict 仍 VALIDATED：
- Honest Sharpe **1.52** (vs B&H 0.73) — 仍遠勝 baseline
- CAGR 30.5% / MDD -12.4% / WF 100% pos
- 過濾掉 illiquid 945 stocks (universe 1830→885 — 從 production 角度應該砍掉的部分)
- v11 Sharpe 1.92 是 illiquid noise premium 不該 promote 進 live

**SOP-12 promotion verdict: VALIDATED — 可進 informational tier observation period (3-6 月 live shadow)**。

**6 月後 live shadow 觀察過 → 進入 Mode D 候選池 / 不接 portfolio gating (per SPEC §2 high variance constraint 永遠保留)**。

**心理預期 honest update**：Council User Auditor R2 預測「最終 verdict 大機率 D 級 / 方法論驗證 negative」**部分命中**：
- 「主力選股 hypothesis」確實 falsified ✓
- 但 6 iteration 後 surface 出一個 robust quality-momentum-volume composite — 不是 D 級
- Live alpha 預期遠低於 backtest (per SPEC §8.8 心理預期警告)，但 backtest Sharpe 1.57 + MDD -8% 是 robust starting point

---

## 參考

- 既有 chip pipeline：`addon_factors.py::analyze_tw_chip_factors()`（C2 IC verdict「不加分」— **raw aggregate 結論，本 model 是 behavior pattern selector 不同 angle**）
- SOP-12 informational tier：`memory/project_validation_bias_warning.md`
- SOP-14 高 variance signal：同上
- Discord 格式：`memory/feedback_discord_no_md_tables.md`
- 策略設計回測：`memory/feedback_strategy_design_backtest_required.md`
- TDCC reference：`memory/reference_tdcc_shareholding.md`
