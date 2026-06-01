# 融資餘額佔市值比 IC Validation vs ^TWII

Date: 2026-06-01  Panel: 2016-01-04 ~ 2026-05-29 (2501 rows)
Outcome: ^TWII 自 D+1 起 fwd 20/60/120d 最大回檔 (close-to-min, 負值)
Feature@D 為當日盤後 TWSE 官方融資餘額 / 上市總市值，無 T+1 shift。

## Verdict 摘要 (SOP-12 3-gate)

| Feature | Verdict | Best |IC_mdd| |
|---|---|---|
| `margin_to_mktcap_pct` | FAIL | 0.121 |
| `margin_mktcap_z_252d` | FAIL | 0.121 |

## Univariate IC (Spearman)

| feature | horizon | n | IC vs MDD | p | IC vs ret | Q1 med MDD | Q10 med MDD | Spread (pp) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| margin_to_mktcap_pct | 20d | 2482 | -0.064 | 0.0015 | -0.224 | -1.25% | -1.42% | -0.17 |
| margin_to_mktcap_pct | 60d | 2442 | -0.114 | 0.0000 | -0.386 | -1.51% | -3.11% | -1.60 |
| margin_to_mktcap_pct | 120d | 2382 | -0.121 | 0.0000 | -0.481 | -1.42% | -5.74% | -4.32 |
| margin_mktcap_z_252d | 20d | 2363 | +0.005 | 0.8229 | +0.036 | -2.43% | -1.95% | +0.48 |
| margin_mktcap_z_252d | 60d | 2323 | -0.043 | 0.0387 | -0.018 | -4.52% | -3.68% | +0.84 |
| margin_mktcap_z_252d | 120d | 2263 | -0.121 | 0.0000 | -0.196 | -4.54% | -4.47% | +0.07 |

## Conditional lift (危險帶門檻; hit = fwd60d MDD <= -10%)

### `margin_to_mktcap_pct`

| Condition | n | % days | fwd20 med | fwd60 med | fwd120 med | hit fwd60<=-10% | lift |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline | 2501 | 100.0% | -1.60% | -2.87% | -3.59% | 14.7% | 1.00x |
| margin_to_mktcap_pct >= 0.43 | 1695 | 67.8% | -1.72% | -3.15% | -4.28% | 16.8% | 1.14x |
| margin_to_mktcap_pct >= 0.48 | 1000 | 40.0% | -1.86% | -3.20% | -3.98% | 15.4% | 1.05x |
| margin_to_mktcap_pct >= 0.53 | 815 | 32.6% | -1.67% | -2.87% | -3.32% | 10.2% | 0.69x |

Gate / upgrade notes:
- Gate A FAIL @ 20d: |IC|=0.064 p=0.0015
- Gate C FAIL @ 20d: |spread|=0.17pp
- Gate C FAIL @ 60d: |spread|=1.60pp

### `margin_mktcap_z_252d`

| Condition | n | % days | fwd20 med | fwd60 med | fwd120 med | hit fwd60<=-10% | lift |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline | 2501 | 100.0% | -1.60% | -2.87% | -3.59% | 14.7% | 1.00x |
| margin_mktcap_z_252d >= 1.5 | 274 | 11.0% | -1.89% | -3.61% | -4.31% | 9.5% | 0.65x |
| margin_mktcap_z_252d >= 2.0 | 149 | 6.0% | -2.69% | -4.11% | -5.20% | 13.4% | 0.91x |
| margin_mktcap_z_252d >= 2.5 | 59 | 2.4% | -3.28% | -3.52% | -3.52% | 20.3% | 1.39x |

Gate / upgrade notes:
- Gate A FAIL @ 20d: |IC|=0.005 p=0.8229
- Gate A FAIL @ 60d: |IC|=0.043 p=0.0387
- Gate C FAIL @ 20d: |spread|=0.48pp
- Gate C FAIL @ 60d: |spread|=0.84pp
- Gate C FAIL @ 120d: |spread|=0.07pp

## Event study: 重大 TWII 頂部前 feature 行為

測 build_systemic_chip_panel.py 之說「各大頂/崩盤前 0.43-0.53%, >=0.48 為當代頂部帶下緣」。

| Top | Date | pct@top | z@top | max pct (前120d) | max z (前120d) | 觸 0.48 | 觸 z2.5 |
|---|---|---:|---:|---:|---:|---|---|
| 2018 Q4 selloff | 2018-10-01 | 0.546% | -2.74 | 0.678% | +2.42 | YES | no |
| COVID 2020 | 2020-01-14 | 0.422% | -1.60 | 0.455% | -0.08 | no | no |
| 2022 bear | 2022-01-05 | 0.533% | +0.30 | 0.601% | +2.57 | YES | YES |
| 2024-08 yen carry | 2024-07-11 | 0.432% | +0.05 | 0.458% | +1.63 | no | no |
| 2025 tariff | 2025-03-18 | 0.451% | +1.07 | 0.460% | +2.00 | no | no |

## 結論與建議

**兩 feature 皆 FAIL SOP-12。** 融資佔市值比在 ^TWII 上無顯著 fwd-MDD 預測力。
維持 systemic_chip Group B informational tile，**不接 composite / rebalance gate** (SOP-14)。

**Caveats (SOP 1-14)**:
- 重疊窗口: fwd 120d MDD 相鄰日共用 119 天 -> 有效樣本遠小於名目 n, p-value 偏樂觀。
- 結構性下降: `margin_to_mktcap_pct` 絕對 level 2016 ~0.62% -> 2025 ~0.35%，絕對門檻 IC/lift 受 regime 主導 (2016-18 幾乎恆 >=0.48); z_252d 為去趨勢版，較可信。
- 絕對門檻校準漂移: 0.48 校準於 2024-26，但 2025/26 比值上限僅 0.460/0.385，危險帶近年幾乎不觸發 -> 該門檻已實質失效，需每 1-2 年 review。