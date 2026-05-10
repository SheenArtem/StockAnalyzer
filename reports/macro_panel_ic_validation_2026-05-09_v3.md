# Macro Panel IC Validation V3 (Composite Refactor)

**Generated**: 2026-05-09 17:37
**Panel**: 6130 rows × 75 features
**Outcome**: future 60d/40d/20d MDD

> ⚠️ **Errata 2026-05-10**: 本報告 dedup_top8 IC 數字 (-0.422 / -0.348 / -0.246) 已不可重現。
> commit `3a1d741` (DXY 來源換 ICE + SBL/margin stable-sample helper) 後 panel input 已修正，
> 重跑 V3 dedup_top8 IC 為 **-0.329 / -0.275 / -0.194**。Composite 仍 ≥ best single (`buffett_indicator_us` -0.371) 嗎？**否**，新 baseline 下 SOP-12 FAIL，建議 banner v4 預期 IC 改成 -0.33（不是原 -0.42）。
> 詳細對照與 V4 verdict 見 `macro_panel_ic_validation_2026-05-09_v4.md`。
> 本報告 body 保留為 commit `3f2dd3d` 當時 verdict 歷史記錄。

## Composite Comparison (4 variants vs best single)

| Variant | IC 60d | IC 40d | IC 20d | SOP-12 (60d) |
|---|---|---|---|---|
| `comp_v2_raw` | -0.293 | -0.055 | -0.105 | ❌ FAIL |
| `comp_v3_lag_weighted` | -0.165 | -0.013 | -0.144 | ❌ FAIL |
| `comp_v3_dedup_top5` | -0.398 | -0.330 | -0.237 | ✅ PASS |
| `comp_v3_dedup_top8` | -0.422 | -0.348 | -0.246 | ✅ PASS |
| **best single (`buffett_indicator_us`)** | -0.371 | -0.329 | -0.281 | (baseline) |

## Composite Configuration

- **comp_v2_raw**: top-10, weight=|IC|, no lag adjustment (V2 logic for comparison)
- **comp_v3_lag_weighted**: top-10, weight=|IC| × lag_factor (slow 0.5 / coincident 0.7 / lead 1.0)
- **comp_v3_dedup_top5**: dedup Pearson>0.75 + lag-weighted, top-5 only
- **comp_v3_dedup_top8**: dedup Pearson>0.75 + lag-weighted, top-8

## Top 10 by |IC 60d| (after filter |IC|>0.10)

| Rank | Feature | IC 60d | Lag | Lag-Weight | Cluster |
|---|---|---|---|---|---|
| 1 | `buffett_indicator_us` | -0.371 | 10d | 1.0 | KEEP |
| 2 | `eem_close` | -0.346 | 60d | 0.5 | DROP (dup) |
| 3 | `buffett_indicator_tw` | -0.342 | 60d | 0.5 | DROP (dup) |
| 4 | `tlt_spy_ratio` | +0.317 | 3d | 1.0 | DROP (dup) |
| 5 | `ewj_close` | -0.296 | 60d | 0.5 | DROP (dup) |
| 6 | `hy_oas` | +0.295 | 59d | 0.5 | DROP (dup) |
| 7 | `us_buffett_strict_rank` | +0.289 | 0d | 0.7 | KEEP |
| 8 | `us_durable_yoy` | -0.274 | 1d | 1.0 | KEEP |
| 9 | `eem_to_spy_ratio` | +0.232 | 2d | 1.0 | DROP (dup) |
| 10 | `fed_bs_trillion` | -0.230 | 60d | 0.5 | KEEP |
| 11 | `st_louis_fsi` | +0.229 | 12d | 1.0 | KEEP |
| 12 | `buffett_rank_tw` | -0.221 | 60d | 0.5 | KEEP |
| 13 | `hyg_dollar_flow` | -0.218 | 0d | 0.7 | KEEP |
| 14 | `usdjpy_close` | -0.206 | 16d | 1.0 | KEEP |
| 15 | `margin_to_index_ratio` | +0.192 | 3d | 1.0 | DROP (dup) |

## Dedup Drops (similar features removed)

| Feature dropped | Pearson | Kept (stronger) |
|---|---|---|
| `eem_close` | +0.86 | `buffett_indicator_us` |
| `buffett_indicator_tw` | +0.93 | `buffett_indicator_us` |
| `tlt_spy_ratio` | -0.84 | `buffett_indicator_us` |
| `ewj_close` | +0.95 | `buffett_indicator_us` |
| `hy_oas` | -0.86 | `buffett_indicator_us` |
| `eem_to_spy_ratio` | -0.80 | `buffett_indicator_us` |
| `margin_to_index_ratio` | -0.76 | `buffett_indicator_us` |
| `us_claims_yoy` | +0.86 | `us_claims_ma4` |
| `emb_close` | +0.79 | `buffett_indicator_us` |
| `buffett_rank_us` | +0.86 | `buffett_rank_tw` |
| `us_buffett_strict` | +0.93 | `buffett_indicator_us` |
| `hyg_to_lqd_ratio` | +0.81 | `usdjpy_close` |
| `vix_close` | +0.78 | `hy_oas_rank` |

## Verdict

**Best variant**: `comp_v3_dedup_top8` IC 60d = -0.422
**vs best single (buffett_indicator_us)**: -0.371

✅ **SOP-12 PASS** — composite 救回，可進 Banner v4 production
