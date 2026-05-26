# VF v2 — Dual-Inst + Volume Signal × Regime Gate

**Verdict: B 級**

## TL;DR

- Gate A 過濾後訊號 1361 / 1861 (73.1% 通過)
- 60d Gate A: IC +0.0194 | spread +2.83% | Sharpe(trade-level) +0.42 | IR vs 0050 -0.06
- Walk-forward OOS 60d Sharpe: 2023=+0.58(n=340) / 2024=+0.27(n=534) / 2025=+0.47(n=322) / 2026=+0.74(n=10)
- OOS 三段（2024/25/26）Sharpe 是否一致 > 0：True

## Gate A 定義（PIT-safe）

- TWA(^TWII) Close >= MA200，且 MA60 近 20d 斜率 > 0
- gate_a 用 t-1 收盤資料判斷 t 日訊號（pandas .shift(1)）
- Gate A 全期通過率：73.1%（gate=True 在 TWII 占 73%）

## Table 1: IC by horizon — no gate vs Gate A

| Horizon | no-gate IC | no-gate t | Gate A IC | Gate A t | Gate A n_days |
|---|---|---|---|---|---|
| 20d | +0.0086 | +4.10 | +0.0068 | +3.02 | 591 |
| 60d | +0.0174 | +9.42 | +0.0194 | +9.44 | 551 |
| 120d | +0.0196 | +10.61 | +0.0224 | +10.84 | 491 |

## Table 2: Binary spread — no gate vs Gate A

| Horizon | no-gate n_sig | no-gate spread | t | Gate A n_sig | Gate A spread | t |
|---|---|---|---|---|---|---|
| 20d | 1828 | +1.15% | +3.28 | 1328 | +1.15% | +2.64 |
| 60d | 1706 | +3.52% | +4.75 | 1206 | +2.83% | +2.98 |
| 120d | 1572 | +6.90% | +5.51 | 1072 | +5.20% | +3.34 |

## Table 3: Signal-only trade-level stats (per hold horizon)

| Hold | label | n_sig | mean ret | median | hit% | Sharpe(ann) | 0050 mean | IR |
|---|---|---|---|---|---|---|---|---|
| 20d | no-gate | 1828 | +2.47% | -0.11% | 49.0% | +0.59 | +2.95% | -0.22 |
| 20d | Gate A | 1328 | +2.78% | -0.20% | 48.2% | +0.62 | +3.05% | -0.11 |
| 60d | no-gate | 1706 | +7.26% | +1.21% | 52.1% | +0.49 | +8.95% | -0.12 |
| 60d | Gate A | 1206 | +6.67% | -0.52% | 49.1% | +0.42 | +8.43% | -0.06 |
| 120d | no-gate | 1572 | +13.85% | +2.38% | 53.6% | +0.40 | +18.46% | -0.15 |
| 120d | Gate A | 1072 | +12.01% | +0.81% | 52.0% | +0.34 | +16.24% | -0.08 |

## Table 4: Walk-forward OOS (year-by-year, 60d hold)

Gate 是固定規則無自由參數 → 每年數字即 OOS（不會被未來年資料污染）

| Year | no-gate n | no-gate mean | hit% | Sharpe | Gate A n | Gate A mean | hit% | Sharpe | Δ Sharpe |
|---|---|---|---|---|---|---|---|---|---|
| 2023 | 489 | +8.14% | 57% | +0.62 | 340 | +8.52% | 53% | +0.58 | -0.05 |
| 2024 | 636 | +1.94% | 45% | +0.18 | 534 | +3.04% | 47% | +0.27 | +0.09 |
| 2025 | 571 | +12.20% | 56% | +0.65 | 322 | +10.28% | 49% | +0.47 | -0.18 |
| 2026 | 10 | +21.07% | 40% | +0.74 | 10 | +21.07% | 40% | +0.74 | +0.00 |

## Table 5: Gate filter rate per year

| Year | n_total | n_passed | % passed |
|---|---|---|---|
| 2023 | 489 | 340 | 69.5% |
| 2024 | 636 | 534 | 84.0% |
| 2025 | 571 | 322 | 56.4% |
| 2026 | 165 | 165 | 100.0% |

## Diagnostic — gate 是真有用還是事後挑 2024

- 2024 被砍 **16%**（passed=84%）
- 各年砍除率：2023=30% | 2024=16% | 2025=44% | 2026=0%

## Walk-forward OOS 一致性判讀

- 三段年度 OOS Sharpe 全 > 0 → **gate 跨年度穩定**，非事後挑 2024

## Verdict 推導

- A 級條件：60d/120d IC > +0.05 (+0.0224), 60d spread > +5% (+2.83%), IR > 0 (-0.06), 三段 OOS Sharpe > 0 (True)
- 結論：**B 級**
