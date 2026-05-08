# Cross-Asset Crash Predictor (Route A) — Spec

**Date drafted**: 2026-05-08
**Status**: 規劃中（user 拍板「下次 session 動工」）
**Dependency**: B+E informational dashboard 已完工（commits 272446e + 4bed849 + d612b7c）

---

## 為什麼還要做 A 路線

B+E 整合完成後，banner 已能顯示綜合風險指標 (composite=72.2 → Orange)，但這是 **coincident composite** 不是 leading predictor — 即 banner 告訴使用者「**現在處於高風險區**」，不是「**未來會回檔**」。

要真正做出「預警」，必須突破純台股 N=29 events 的數學限制。Cross-asset stack 是唯一未試過的可行路徑（B+E 用台股本地訊號，A 借用美股 macro 因子的長歷史 + 學界已驗證的 lead time）。

### A 路線的核心 framing

不是「合併台美事件數」，是「**用對的因子（全球 macro）預測對的事件（台股 risk-off）**」。

理論基礎：
- 台股 ≥10% 回檔本質不是「純台股事件」 — 過去 5 大 crash（2000 dotcom / 2008 GFC / 2015 China dev / 2020 COVID / 2022 Fed）全是全球 risk-off 同步發生
- 台股是 SPX 衛星市場（correlation 70-80%）
- HY OAS spread / yield curve / MOVE 對 SPX 已有實證 lead time（HY OAS 中位數 7 個月）— 對台股應該也有 lead time

---

## Phase 1: FRED 資料撈取 + SPX panel 建構（5-7h）

### Phase 1.A: FRED API 接入（2-3h）

需要 FRED API key（free，註冊 5 min）。

| Series | FRED ID | 起點 | 用途 |
|---|---|---|---|
| HY OAS spread | `BAMLH0A0HYM2` | 1996-12 | 信用壓力，最強 single signal |
| 3M-10Y yield curve | `T10Y3M` | 1982-01 | 利率倒掛，12-18mo lead |
| 10Y Treasury yield | `DGS10` | 1962-01 | 利率水位 |
| Fed Funds rate | `DFF` | 1954-07 | 流動性 |
| SOFR rate | `SOFR` | 2018-04 | repo / funding stress |
| Trade-weighted USD | `DTWEXBGS` | 2006-01 | FX 壓力 |

**寫入**: `data/macro/fred_panel.parquet` daily index 對齊 NYSE 交易日

**Tool**: 擴 `tools/fred_fetcher.py`（既有，2026-04-29 用 yfinance proxy；改用真 FRED API）。

### Phase 1.B: 美股波動率指標（1h）

從 yfinance 撈：
- `^VIX` (1990-) — 短期 vol
- `^VIX3M` (2007-) — 3 個月 vol
- `^VVIX` (2007-) — vol of vol
- `^MOVE` (2003-) — rates vol（現有可能不齊）

寫入: `data/macro/vol_panel.parquet`

### Phase 1.C: SPX panel + cross-asset label（2-3h）

撈 SPX OHLCV 1990-2026（既有 `data_cache/backtest/ohlcv_us.parquet` 可能有部分）。

建構 dual-target panel:
- TWII forward 60d MDD ≥10% / ≥20% (已有 `crash_predictor_tw_panel.parquet`)
- SPX forward 60d MDD ≥10% / ≥20% (新)
- 兩 target 各對應 cross-asset features

**事件統計目標**:
- SPX 1990-2026 估 ~25 個 ≥10% events
- TWII 1999-2026 既有 29 個
- **不合併事件**，而是 dual model：用同一組 features 分別預測 SPX 跟 TWII，看訊號是否 transfer

---

## Phase 2: Univariate AUC + Cross-Asset Transfer Learning（4-6h）

### Phase 2.A: Univariate per-feature lift10 + lead time（2-3h）

對每個 macro feature 跑：
- AUC-ROC vs SPX label_10pct
- AUC-ROC vs TWII label_10pct
- **strict-preceding lead time**（SOP-11，fire 必須在 event_start 前 5d + forward 5d return < -1%）
- xcorr lag classification（SOP-13，lag<3d coincident / 3-15d mixed / >15d leading）

**SOP gate**: 任何 feature 必須對至少一個 target 的 lift10 > 1.2 + 至少 leading classification + 通過 portfolio gating sim 才算通過 univariate filter。

### Phase 2.B: Transfer learning sanity（1-2h）

關鍵 question: 用 SPX-trained model 預測 TWII 跟 用 TWII-trained model 預測 TWII 哪個 OOS AUC 較高？

如果 SPX-trained > TWII-trained → 證明 macro 訊號對台股有 transfer learning 價值
如果 TWII-trained > SPX-trained → 純台股 model 仍勝，A 路線假設破產

### Phase 2.C: Cluster correlation + composite weight（1h）

仿 B+E v3 calibration 流程：
- 各 feature lift10 normalize → weight
- Cluster: 信用 (HY OAS) / 利率 (T10Y3M, DGS10) / 波動 (VIX/VXV/MOVE) / FX (DTWEXBGS)
- 4-cluster z-score 平均成 4 個 composite features

---

## Phase 3: Portfolio Gating Sim + Final Verdict（4-6h）

仿 `tools/audit_crash_predictor_expected_value.py` 的 portfolio simulator：

對每個 strategy 跑 2002-2026 daily allocation sim（B&H + best-single + composite 對比）：
- B&H TWII baseline
- 最佳 single feature gating TWII
- 4-cluster composite gating TWII
- 4-cluster composite gating SPX

**SOP-12 gate**: 上線必須 composite Sharpe > best-single Sharpe + composite > B&H Sharpe（避免 cash drag artifact）

**SOP-14 gate**: 即使過 SOP-12，如果 N_events < 30 仍降 informational tier，不接 portfolio rebalance。但因為 SPX+TWII dual model + 26 年期間，N 應該 ~50+ 過 SOP-14。

### Phase 3.B: Banner UI 整合 v4（如果過 gate, 2-3h）

如果通過：
- banner 加第 3 個 row「全球 macro 訊號」(只在橘/紅燈 fire)
- 文案保持 SOP-14：「歷史此狀態下 60d 內 ≥10% drawdown 同期 X% / 平均 lead Y 天」
- 仍不接 portfolio rebalance

如果不過：
- 寫 verdict 報告封桃，跟 crash predictor 同等待遇歸 archive
- 教訓：「macro 訊號對台股 alpha 不存在 / 或僅 informational」

---

## 預估總工時

| Phase | 工時 |
|---|---|
| 1.A FRED API 接入 | 2-3h |
| 1.B 美股 vol panel | 1h |
| 1.C SPX panel + dual label | 2-3h |
| 2.A Univariate AUC + lead | 2-3h |
| 2.B Transfer learning sanity | 1-2h |
| 2.C Cluster composite | 1h |
| 3.A Portfolio gating sim | 3-4h |
| 3.B (條件式) banner v4 整合 | 2-3h |
| **總計** | **15-22h（不含 3.B）** |

---

## SOP 守則（從 R3 教訓繼承）

1. **SOP-10 Portfolio gating sim 強制 gate** — 任何 composite 上線必跑 daily-allocation sim，AUC 高不能上線
2. **SOP-11 Strict-preceding lead-time only** — 用 `event_start - first_fire_date ≥ 5d AND forward_5d_ret(fire) < -1%`
3. **SOP-12 Composite must beat best-single** — portfolio Sharpe 不只 lift
4. **SOP-13 xcorr lag classification + cash drag** — coincident vs leading 必標
5. **SOP-14 N>30 才能進 portfolio gating，N≤30 informational only**
6. **Council Portfolio Backtest Auditor mandatory role**（如果 verdict 前要 council）

---

## 重啟條件

下次 session 動工前確認：

1. ✅ B+E informational dashboard 已通過實際使用一段時間（≥1 週），確認當前 banner 沒被使用者誤解為「預警」
2. ✅ 沒有更高 ROI 的功能搶資源（沒新 user request / 沒大維護需求）
3. ✅ FRED API key 申請好（user 自行）
4. ✅ Working tree clean（沒在中途 refactor）

---

## 終局期望

走完 A 路線後，可能三種 verdict：

**Verdict A1 - 通過 (預期機率 30-40%)**: cross-asset macro composite 對台股 ≥10% 回檔 AUC 0.65+ / strict-preceding lead time ≥ 30d / 過 SOP-12 portfolio gating。Banner v4 加「全球 macro 訊號」row，informational/leading 並存。

**Verdict A2 - Informational only (預期機率 40-50%)**: AUC 過 SOP-14 但 portfolio gating 不過。視為「macro 訊號對台股有 statistical signal 但 portfolio cost 過高」，加進 banner informational tier 跟 B+E 並列，不接 rebalance。

**Verdict A3 - 封桃 (預期機率 15-25%)**: macro 訊號對台股 transfer learning 失效（TWII-trained 仍勝 SPX-trained），證明全球 macro 對台股 alpha 不存在。寫 verdict 報告歸 `reports/_history/` 永久封桃。

無論哪個 verdict，**SOP-10~14 都會被嚴格執行 + Council Portfolio Backtest Auditor mandatory** — 不會再踩第 3 次「proxy ≠ portfolio simulator」坑。

---

## 跨 session handoff 必讀

1. `~/.claude/.../memory/project_validation_bias_warning.md` — SOP 1-14 全文
2. `reports/_history/2026_05_crash_predictor_closed/crash_predictor_methodology_audit.md` — R3 council + smoking gun
3. `reports/banner_risk_score_calibration_v2.md` — B+E v3 calibration（A 的 baseline）
4. 本檔 — A 路線 spec
