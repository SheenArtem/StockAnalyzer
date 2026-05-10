# Crash Predictor — Closed 2026-05-08

**狀態**: 整條線封桃。N=29 events / 27 年 / 5 個不同 crisis regime 統計力不足以驗證 multi-factor crash predictor。

## 為什麼歸檔

Phase 2 univariate AUC 0.69-0.72 + lead time 55-60d 全綠通過閾值，但 Phase 3 進場前 B 機會成本檢驗（chip-analyst portfolio simulator）翻盤：

- **50/30/20 composite portfolio MDD -32.3% 比 B&H -31.6% 還差**
- **m1b alone（AUC 最高）是最差 gating** — CAGR -6pp / MDD 0 改善 / 540d cash 浪費
- **rv10 alone 唯一正 ROI** — 但 H/G 質疑可能是 cash drag artifact，不是真 leading

Council R3 抓到 smoking gun：Phase 2 lead time anchor `peak_date = forward 60d window 極大值` 是構造性偏誤，**真 strict-preceding leading event 只 5/29 = 17%**。

## 教訓 → SOP-10~14

擴 `~/.claude/projects/C--GIT-StockAnalyzer/memory/project_validation_bias_warning.md` 加：
- SOP-10 portfolio gating sim 強制 gate
- SOP-11 strict-preceding lead-time only
- SOP-12 composite must beat best-single (portfolio metric)
- SOP-13 xcorr lag classification + cash drag 鑑別
- SOP-14 N≤30 events 一律 informational tier
- Council 加 Portfolio Backtest Auditor mandatory role

## 檔案清單

| 檔案 | 說明 |
|---|---|
| `crash_predictor_tw_panel.parquet` | Daily panel 1999-2026 / 29 events / 5 viable factors |
| `crash_predictor_tw_panel_summary.md` | Panel 欄位 + 事件清單 |
| `crash_predictor_tw_phase2.md` | Phase 2 univariate AUC + cluster |
| `crash_predictor_tw_factor_metrics.csv` | 5 因子 × 兩 label × 兩軌 raw metrics |
| `crash_predictor_tw_factor_corr.csv` | Track B 5-factor correlation matrix |
| `crash_predictor_tw_dendrogram.png` | Cluster dendrogram |
| `crash_predictor_b_opportunity_cost_audit.md` | B 機會成本 audit（含 §3 ATR vs 固定 / §4 portfolio gating sim 翻盤）|
| `audit_atr_vs_fixed_stop.csv` | ATR K=2.0/2.5/3.0 vs 固定 8% trailing 11 年 3 ticker 結果 |
| `audit_crash_predictor_expected_value.csv` | 5 strategy × TWII gating sim 結果 |
| `crash_predictor_methodology_audit.md` | Council R3 final 報告 + SOP-10~14 全文 |

## Reproducer scripts（保留 `tools/` 可重用）

- `tools/build_crash_predictor_panel.py`（M1B 35d lag fix 已落地）
- `tools/crash_predictor_tw_phase2.py`
- `tools/audit_atr_vs_fixed_stop.py`
- `tools/audit_crash_predictor_expected_value.py`

## 重啟條件

要重啟此線需先滿足：
1. N_events ≥ 50（需累積到 2030+ 或補美股 SPX panel 並做 cross-asset stack）
2. SOP-10~14 reproducer 工程改進完成（lead_d_strict + portfolio sim block + xcorr 表）
3. Council R3 mandatory Portfolio Backtest Auditor role 已掛入 council 模板

否則維持封桃。
