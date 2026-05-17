# Regime-Aware Weight Phase A Audit Summary

**Generated**: 2026-05-17
**Verdict**: ✅ **整個方向已被 audit 完 + KILL，code 已 flatten。Phase B/C/D 不啟動。**

## TL;DR

「Regime-Aware 權重深化」3 個假設權重來源，**全部已經被 audit 完**，全部 verdict D，全部已 flatten 成 1.0 / regime-conditional 已落地。Phase A 重做意義為零。

| Surface | 位置 | Audit 報告 | Verdict | 當前狀態 |
|---|---|---|---|---|
| Selection: group weights (trend/momentum/volume × regime) | `analysis_engine.py:52-57` | `reports/vfg3_part2_regime_selection_mult.md` (2026-04-17) | **D** — IC delta +0.0007, top-20 ret delta +0.00%, WF 無穩定勝 | 全 1.0 flatten |
| Selection: addon cap multipliers (chip/sent/rev/etf × regime) | `analysis_engine.py:59-64` | 同 VF-G3 Part 2 | **D** | 全 1.0 flatten |
| Exit: stop-loss + take-profit multipliers (SL_mult/TP_mult × regime) | `exit_manager.py:69-73` | `reports/vfg3_part1_regime_exit_mult.md` (2026-04-21) | **D** — V1 vs V2 mean Δ -0.08%, Sharpe Δ -0.007 | 全 (1.0, 1.0) flatten |
| C1 tilt regime gate (AI era ON / Pre-AI OFF) | `tools/compute_c1_tilt.py` + `data/c1_tilt_flags.parquet` | V4' Pre-AI -4.2pp 反效果 audit (memory: project_mode_d) | **regime-conditional 已落地** | 當前 regime 判定 ON/OFF |

## 還活著但無 selection 影響

| 物件 | 位置 | 用途 | 影響面 |
|---|---|---|---|
| `pos_map = {trending:1.0, ranging:0.5, volatile:0.7, neutral:1.0}` | `analysis_engine.py:605` | 顯示「建議倉位」給用戶 + AI 報告 prompt 參考文字 | **純 UI / AI prompt 描述，不進 selection** |
| Per-stock ADX-based regime override (HMM trending + ADX<20 → 降 ranging 等) | `analysis_engine.py:577-588` | 改 regime label | 因 group/addon weight 全 1.0，對 selection 無影響；只改 UI 文字 + position_adj |

## 為什麼之前的計畫 / 記憶會錯

`memory/project_scoring_status.md` (31 天前 snapshot) 記的 `trending T=1.3/M=1.0/V=0.7, ranging T=0.7/M=1.0/V=1.3, volatile T=0.9/M=0.9/V=1.2` 是 **2026-04-12 那次 snapshot 的原始值**。

VF-G3 Part 2 audit (2026-04-17, commit 不詳但已落 code) 之後**全 flatten 成 1.0**，但 memory 沒被同步更新。今天 2026-05-17 grep code 才發現實際早已是 no-op。

## Verdict for Phase B/C/D

- **Phase B (grid search 新權重)**：不啟動。Grid search 已隱含在 VF-G3 P2 in-sample best per regime 結果，但 walk-forward 沒贏。
- **Phase C (regime expansion 4-5 state HMM + VIX/yield curve)**：**不啟動**。3-state HMM 連 selection effect 都 D-killed，加更多 state 無 base case 改善空間。
- **Phase D (sector regime)**：**不啟動**。Selection / exit 兩條 path 都 D-killed，sector 切細沒有理論優勢。
- **Phase E (SOP-14)**：N/A。

## 後續行動

1. ✅ 本檔釘住 audit summary
2. 更新 memory：
   - `project_scoring_status.md` mark stale + 補正當前實際值
   - `project_next_3_directions.md` 標 Regime 方向 KILL
3. 把 Phase 1 與 #2 (C3 Phase A+B) 提到當前首選

## 為什麼 flatten 不刪 dict

VF-G3 P2 報告明寫：「保留 dict 結構便於未來 regression / 空頭年復活比對」。樣本期 2021-2025 僅 2022 是空頭年，若未來累積足夠空頭年資料，可重跑 audit 看 V1 是否反轉勝 V2。**但這是 ≥3 年後的事**，不在當前路線圖。

## 相關 commit / 檔案

- `reports/vfg3_part1_regime_exit_mult.md`
- `reports/vfg3_part2_regime_selection_mult.md`
- `reports/vfg3_part2_versions.csv`、`vfg3_part2_walkforward.csv`、`vfg3_part2_grid.csv`
- `tools/vfg3_part1_validation.py`、`tools/vfg3_part2_*.py` (audit tooling 已存在，未來空頭年復活時 re-run 即可)
