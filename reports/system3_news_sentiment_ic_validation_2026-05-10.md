# S3-c News LLM Extreme Negative Sentiment — IC Validation

**Date**: 2026-05-10
**Analyst**: IC-validator (RD)
**Verdict**: **FAIL D — Insufficient sample**
**Action**: Archive, do NOT integrate. Re-run when archive ≥ 180 trading days.

---

## TL;DR

`articles_recent.parquet` is a **rolling recent-window** snapshot, not a historical archive. Actual coverage is **15 calendar days (2026-04-24 to 2026-05-09)** with only **9 days** of usable LLM-extracted volume (≥ 100 articles/day). No statistical power for IC validation against ^TWII fwd 5d/10d/20d drawdown — minimum required N for SOP-12 Spearman IC ≥ 30, ideally ≥ 100. **Skipped IC validate, decile spread, threshold lift, event study, and S3-a/b independence checks** per task escape clause.

---

## 1. Data Inventory (Step 1 — only step executed)

| Field                       | Value                                                        |
|-----------------------------|--------------------------------------------------------------|
| File                        | `data/news/articles_recent.parquet`                          |
| Total rows                  | 2,070                                                        |
| Schema                      | 24 columns (extract_version=1.0, B5 Phase 1 format)          |
| **Date span**               | **2026-04-24 → 2026-05-09 (15 calendar days)**               |
| Distinct dates              | 15                                                           |
| Days with ≥ 100 articles    | **9** (the only days usable for daily aggregate)             |
| Days with ≥ 25 articles     | 10                                                           |
| Pipeline ramp-up days       | 5 (2026-04-24 to 2026-04-29: 1, 2, 16, 19, 22 articles each) |
| `sentiment` field           | float -0.8 to +0.9, 100% non-null                            |
| `tone` field                | {bullish, neutral, bearish}, 100% non-null                   |
| Distribution skew           | mean +0.389, median +0.50 — heavily bullish-tilted           |

### Daily LLM-extracted article counts (usable days only)

```
2026-04-30  147
2026-05-01  252
2026-05-02  119
2026-05-03  171
2026-05-04  408
2026-05-05  363
2026-05-07  157
2026-05-08  180
2026-05-09  148
```

### LLM Re-extract Cost Check (Step 2 pre-flight)

**Skipped — not needed**. Existing `sentiment` (float) + `tone` (categorical) columns are 100% populated by the B5 builder per memory `project_news_theme_dual_layer` Phase 1. Re-run unnecessary. Reuse > redo confirmed.

---

## 2. Why This Fails the Gate

SOP-12 three-gate (IC + decile spread + threshold lift) requires:

| Requirement                        | Threshold                              | Actual           | Pass? |
|------------------------------------|----------------------------------------|------------------|:-----:|
| Spearman IC sample size N          | ≥ 30 (informational), ≥ 100 (PASS)     | **9**            |  No   |
| Decile spread (D1 vs D10)          | Needs ≥ 50 obs to form 10 buckets      | **9**            |  No   |
| Threshold lift conditional         | Needs ≥ 30 alert days post-threshold   | **0–2** at best  |  No   |
| Independence vs S3-a / S3-b        | Jaccard requires overlap window ≥ 30d  | **9**            |  No   |
| Event study (COVID 2020-02 etc.)   | Needs archive back to 2020+            | **archive starts 2026-04-24** | No |

Even the most permissive informational tier needs N ≥ 30 days; we have 9. Running IC math on 9 observations would produce noise indistinguishable from any random feature — false PASS risk is high, false FAIL risk is high, and either verdict would be invalidated by archive month 2.

The pipeline ramp-up days (1–22 articles) are also unusable: per-day LLM coverage that low means `daily_neg_ratio` swings violently from a single bearish article (1/1 = 100% neg vs 1/22 = 4.5%), which is sampling-noise, not sentiment signal.

---

## 3. Counter-checked: No Hidden Long History

Verified the other news parquets for back-archive:

| File                          | Rows   | Date span                                | Verdict                    |
|-------------------------------|--------|------------------------------------------|----------------------------|
| `news_flow_anomaly.parquet`   | 2      | detection-only, no history               | Not a sentiment archive    |
| `theme_momentum.parquet`      | 22     | detection-only, no history               | Not a sentiment archive    |
| `themes_core.parquet`         | 17     | aggregate (`first_seen` / `last_seen`)   | No daily series            |

Memory `project_news_data_extraction_roadmap` already flagged this: "等 archive 累積 6+ 月後跑". That hand-off note holds.

---

## 4. Verdict Rationale

- **FAIL D** (not MARGINAL, not "skip"): explicit verdict needed so this slot stays gated and informational integration into `system3_daily_check.py` does **not** silently happen.
- **Not weak-D accumulation**: weak-D is for features that pass 1–2 of the 3 gates. This passed 0/3 because gates can't be evaluated. Distinct status.
- **Re-run condition**: archive ≥ 180 trading days of daily-aggregable LLM volume (≥ 100 articles/day). At current ingestion rate (~150–400/day, 9/10 recent days qualify), expected re-run window: **~2026-11 to 2027-01** depending on pipeline uptime.

---

## 5. Follow-up Hooks (record only, no commits)

1. **Archive accumulation watchdog**: when `articles_recent.parquet` accumulator (or its daily snapshot output, if one exists) reaches `unique_dates_with_count_gte_100 ≥ 180`, re-trigger this POC. Suggested place: add to `tools/news_archive_health_check.py` if such tool exists, otherwise a calendar reminder.
2. **Pre-archive read-across**: while waiting, S3-a (^MOVE z-score, MARGINAL informational already shipped commit `d173a66`) and S3-b (SPX 1d急殺, integrating commit `60dd68d`) cover most of the 40% recall gap from the macro side. News-sentiment was the third leg specifically for sentiment-precedes-price scenarios — those scenarios still uncovered, but only re-attempt-able with real history.
3. **Schema lock note**: when re-running, confirm `extract_version` is still 1.0 or document migration. Daily aggregate definition to use:
   - `daily_neg_ratio = count(tone == 'bearish' AND sentiment <= -0.5) / count(all)` per date (only days with N≥100 articles)
   - `daily_sentiment_avg = mean(sentiment)` per date
   - Test both, pick higher IC.
4. **Independence test design (ready for re-run)**: after archive ready, alert-day Jaccard vs S3-a/S3-b is the second gate. If overlap ≥ 80% → no incremental info → drop. Documented now so future Claude doesn't re-derive.

---

## 6. Effort Spent

~15 minutes (data inspection only — fast-fail per task hand-off clause). LLM quota untouched. No code committed.

## 7. Files Touched

- Read-only: `data/news/articles_recent.parquet`, `data/news/news_flow_anomaly.parquet`, `data/news/theme_momentum.parquet`, `data/news/themes_core.parquet`
- Created: this report (`reports/system3_news_sentiment_ic_validation_2026-05-10.md`)
- No commits, no integration, no `system3_daily_check.py` change.
