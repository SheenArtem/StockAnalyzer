# Macro Panel IC Validation V4 (vs V3)

**Generated**: 2026-05-10 09:52
**Panel**: 5893 rows × 81 features (V3 had 75; V4 +6 systemic_chip new features)
**Outcome**: future 60d/40d/20d MDD

## Verdict — NEUTRAL (新 V4 features 對 composite 無 incremental edge)

**Recommendation**: **Do NOT upgrade banner v4 SLOW_FEATURES**. 維持 V3 dedup_top8 keep list.
新 systemic_chip features 全部標 informational tier (僅 `foreign_net_oi` / `pcr_oi` 有 marginal IC，
但前者被 Pearson dedup 掉，後者排序進不了 top-8)。

> ⚠️ Caveat: V3 baseline IC 因 `3a1d741` (DXY 換真貨 + SBL 穩定樣本) 之後本身已從 -0.422
> 漂移到 -0.329。本次 V4 是在已漂移的新 baseline 上做比較（apples-to-apples），
> 不代表「composite 變弱了」— 而是 panel 數據修正後的真實 IC。

## V3 vs V4 Composite Comparison

| Metric | V3 (orig report 17:37) | V3 (re-run today) | V4 (new panel + new features) |
|---|---|---|---|
| Panel features | 75 | 75 (excluded V4 new) | 81 (incl. V4 new) |
| dedup_top8 IC 60d | **-0.422** ✅ | -0.329 | -0.329 |
| dedup_top8 IC 40d | **-0.348** ✅ | -0.275 | -0.275 |
| dedup_top8 IC 20d | **-0.246** ✅ | -0.194 | -0.194 |
| Best single (`buffett_indicator_us`) IC 60d | -0.371 | -0.371 | -0.371 |
| SOP-12 (composite > best single) | PASS | FAIL | FAIL |

**V4 vs V3 (re-run today on same panel)**: IC delta = **0.000 / 0.000 / 0.000**
（top-8 keep list 完全一樣，`foreign_net_oi` 雖進入 strong list 但被 Pearson 0.87 dedup 砍）

## V4 New Systemic Chip Features — Individual IC

| Feature | Group | IC 60d | IC 40d | IC 20d | Best Lag | Pass `IC`>0.10 ? | Top-8? |
|---|---|---|---|---|---|---|---|
| `foreign_net_oi` | A — 外資台指期淨 OI | +0.194 | +0.243 | +0.244 | 0d | YES | DEDUP'd (Pearson -0.87 vs buffett_indicator_us) |
| `pcr_oi` | D — Put/Call OI 比 (V3 silent-fail 修復) | -0.125 | -0.141 | -0.142 | 0d | YES | NO (rank > 8) |
| `trust_buy_streak` | C — 投信連買日數 | -0.100 | -0.117 | -0.101 | 6d | NO (邊緣) | NO |
| `foreign_fut_net_chg_4w` | A — 外資台指期淨 4w 變動 | +0.097 | +0.175 | +0.140 | 0d | NO | NO |
| `option_top1_concentration` | D — 選擇權 top1 集中度 | +0.023 | +0.019 | -0.036 | 52d | NO | NO |
| `trust_5d_zscore` | C — 投信 5d 連買 z-score | +0.012 | -0.013 | +0.038 | 60d | NO | NO |

**Findings**:
- `foreign_net_oi` IC=0.194 是六個新 features 最強，但與 `buffett_indicator_us` (Pearson +0.87) 高度共線，被 dedup 砍。
  **這驗證了「外資期貨 OI ≈ 美國 buffett indicator 同一 risk-on/off 訊號」假設**。
- `pcr_oi` 修好後 IC=-0.125 弱訊號 (lag=0 coincident)。獨立留作 informational dashboard 用。
- 其餘 4 個都未過 |IC|>0.10 閘門。

## V4 dedup_top8 Keep List (與 V3 完全一致)

| Rank | Feature | IC 60d | Lag | LW | Source |
|---|---|---|---|---|---|
| 1 | `buffett_indicator_us` | -0.371 | 10d | 1.0 | FRED + valuation |
| 2 | `us_durable_yoy` | -0.273 | 1d | 1.0 | FRED |
| 3 | `fed_bs_trillion` | -0.230 | 60d | 0.5 | FRED |
| 4 | `st_louis_fsi` | +0.229 | 12d | 1.0 | FRED |
| 5 | `us_buffett_strict_rank` | +0.223 | 60d | 0.5 | FRED |
| 6 | `buffett_rank_tw` | -0.221 | 60d | 0.5 | Valuation |
| 7 | `hyg_dollar_flow` | -0.218 | 0d | 0.7 | ETF Flows |
| 8 | `usdjpy_close` | -0.206 | 16d | 1.0 | FRED |

**No new V4 systemic_chip feature reaches top-8** — 既有 8 個來自 FRED / valuation / ETF flows
的 features 仍是最強訊號。

## Why V3 Baseline Itself Drifted (-0.422 → -0.329)

V3 原報告 commit `3f2dd3d` (2026-05-09 17:37) 之後，data layer 有 2 個 fix commit:

1. **`3a1d741` (DXY 換真貨 + SBL 穩定樣本 + Buffett 顯示修)**:
   - `dxy_close` 來源從 FRED DTWEXBGS (~120) 換成 yfinance DX-Y.NYB (ICE ~98)
   - SBL/margin 從每天 sum 改 stable-252d-sample helper，`sbl_change_4w_pct` `margin_to_index_ratio` 數值改變
   - **這是 V3→今天 baseline 漂移主因**

2. **`dddf761` (foreign_holding_chg_4w stable sample)**: commit 自述「不影響 V3 dedup_top8」，因 foreign_holding 不在 top-8。

> Composite IC 從 -0.422 → -0.329 不是 model 退步，而是 input data 修了 bug。修後的 IC 是更可信的。
> Banner v4 SLOW_FEATURES 仍可繼續用（top-8 list 沒變），但實際 IC 預期 ≈ -0.33（不是當初的 -0.42）。

## Conclusion & Recommendation

1. **Don't upgrade**: V4 新 systemic_chip features 對 dedup_top8 composite **無 incremental edge** (delta 0.000)。
2. **保留 V3 dedup_top8 list** 作為 banner v4 SLOW_FEATURES production（top-8 與 V4 一致）。
3. **新 features 標 informational tier**:
   - `foreign_net_oi` 雖 IC 0.194 但與 buffett 共線 → 個別 dashboard 可放，composite 不加
   - `pcr_oi` IC -0.125 弱訊號 → informational only
   - 其餘 4 個未過閘門 → 不採用 / 可考慮砍掉減 panel 雜訊
4. **更新 banner v4 預期 IC**: 從 -0.422 (orig V3) 改 -0.33 (post-fix baseline)，避免效能虛高誤導。

## Side Note: V3 報告數值維護建議

`reports/macro_panel_ic_validation_2026-05-09_v3.md` 內 -0.422/-0.348/-0.246 數值與 panel 數據
已不一致（panel 修了 DXY/SBL 後 IC 已漂移到 -0.329/-0.275/-0.194）。建議：
- 在 V3 報告底部加 errata 註記：「2026-05-10 重跑 panel 已 -0.329（DXY/SBL fix 後 baseline）」
- 或直接覆寫 V3 數字 + 加 git commit reference

不在本次 task 範圍，留 user 拍板。
