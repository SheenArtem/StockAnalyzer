# VF — 籌碼雙法人同向訊號驗證

**Verdict: B 級** (informational tier, 不進 picks list)

## TL;DR

- IC 20d/60d/120d = +0.0086 / +0.0174 / +0.0196  全 t > 3 顯著但 magnitude 弱（< A 級門檻 +0.05）
- Binary spread 60d = +3.52% (t=+4.75), 120d = +6.90% (t=+5.51)
- n_sig 60d = 1,706 充足；hit rate 60d 52.1%（接近隨機）
- Top-N portfolio 60d Sharpe +0.75 vs 0050 Sharpe +2.00；IR -0.12 **不贏 0050**
- 2024 regime fail：sideways year hit 45%、mean +1.94%、median **負**
- 結論：訊號**有微弱正期望值 + bull-only**，**不該獨立成 picks list**；放 banner 教育用途 + 6 月觀察期

## Signal definition

```
外資 5d 淨買超 > 0  AND  投信 5d 淨買超 > 0
AND rvol_5 (vol_t / avg(vol[t-20:t-1])) >= 2.0
AND past_60d_ret <= +20%
AND turnover_60d >= 5e+08 TWD (>= 5 億)
AND TW common stock (exclude ETF / preferred / warrant)
```

Period: 2023-01-01 ~ 2026-05-15
Universe: 1962 stocks, 1,328,617 eligible (stock, date) rows
Total signal hits: 1,861

## Table 1: IC by horizon (Spearman, cross-sectional daily)

| Horizon | mean IC | IC_IR | t-stat | p-value | n days |
|---|---|---|---|---|---|
| 20d | +0.0086 | +0.149 | +4.10 | 0.0000 | 762 |
| 60d | +0.0174 | +0.350 | +9.42 | 0.0000 | 722 |
| 120d | +0.0196 | +0.412 | +10.61 | 0.0000 | 662 |

## Table 2: Binary signal mean forward return spread

| Horizon | n_sig | n_bg | μ_signal | μ_background | spread | t-stat | p | hit% sig | hit% bg |
|---|---|---|---|---|---|---|---|---|---|
| 20d | 1,828 | 1,290,300 | +0.0247 | +0.0132 | +0.0115 | +3.28 | 0.0010 | 49.0% | 48.5% |
| 60d | 1,706 | 1,217,659 | +0.0726 | +0.0374 | +0.0352 | +4.75 | 0.0000 | 52.1% | 49.1% |
| 120d | 1,572 | 1,111,817 | +0.1385 | +0.0695 | +0.0690 | +5.51 | 0.0000 | 53.6% | 49.2% |

## Table 3: Quintile spread (Q5 - Q1 by signal_strength)

| Horizon | Q1 | Q2 | Q3 | Q4 | Q5 | Q5-Q1 |
|---|---|---|---|---|---|---|
| 20d | +0.0104 | +0.0113 | +0.0125 | +0.0145 | +0.0176 | +0.0071 |
| 60d | +0.0322 | +0.0329 | +0.0363 | +0.0407 | +0.0451 | +0.0129 |
| 120d | +0.0600 | +0.0640 | +0.0692 | +0.0742 | +0.0806 | +0.0207 |

## Table 4: Top-N portfolio (signal-only basket, equal weight)

| Hold | CAGR signal | Sharpe signal | CAGR 0050 | Sharpe 0050 | IR | n_trades | avg trades / signal day |
|---|---|---|---|---|---|---|---|
| 20d | +32.82% | +0.761 | +44.19% | +2.114 | -0.219 | 1,828 | 2.83 |
| 60d | +36.55% | +0.746 | +43.33% | +2.000 | -0.119 | 1,706 | 2.81 |
| 120d | +33.78% | +0.593 | +42.73% | +1.696 | -0.150 | 1,572 | 2.81 |

## Table 5: Per-year regime check (signal-only)

| Year | n_sig | μ_20d | μ_60d | μ_120d | hit_60d | median_60d |
|---|---|---|---|---|---|---|
| 2023 | 489 | +2.21% | +8.14% | +16.41% | 56.9% | +2.62% |
| 2024 | 636 | +2.36% | +1.94% | -0.20% | 45.1% | -2.22% |
| 2025 | 571 | +3.31% | +12.20% | +31.04% | 55.9% | +3.91% |
| 2026 | 165 | +0.36% | +21.07% | +nan% | 2.4% | -4.26% |

⚠️ **2024 regime fail**: 60d mean +1.94% / hit 45.1% / median -2.22% — sideways market；
2023 / 2025 為強多頭，訊號才賺錢。**Regime-dependent，非純 alpha**。

## Caveat — 多頭偏差 (VF-G4)

樣本期 2023-01 ~ 2026-05 為純多頭年（TAIEX 牛市），benchmark 0050 報酬偏高。
若上線必須掛 informational tier 6+ 月觀察期，等空頭/盤整窗口出現後再升等。

## Verdict thresholds (user spec)

- **A**: ≥ 2/3 horizon IC > +0.05 AND 60d binary spread > +5% AND n_sig ≥ 300 AND |t| > 2
- **B**: IC > +0.03 in some horizon OR spread > +2% (informational tier)
- **D**: IC ≤ 0 OR spread ≤ 0
