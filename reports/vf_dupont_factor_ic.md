# VF — DuPont 三因子 IC validation

- Window: 2015-01-01 ~ 2025-12-31
- Universe: 普通股 (universe_tw_full.is_common_stock)
- Announce delay: 45 天
- Min cross-section: 100 stocks per month

## 因子定義

- **F1 (nm)**  Net Margin = IncomeAfterTaxes(TTM) / Revenue(TTM)
- **F2 (at)**  Asset Turnover = Revenue(TTM) / AvgTotalAssets
- **F3 (em)**  Equity Multiplier = TotalAssets(t) / Equity(t)
- **F4 (roe)** synthesized = F1 x F2 x F3 (DuPont identity sanity check)

AvgTotalAssets = (TotalAssets(t) + TotalAssets(t-4)) / 2

## R1-R3 一覽

| Factor | Horizon | n | IC | IR | hit | Sp Sharpe | Mono | WF sign-hit | LOO | Grade |
|---|---|---|---|---|---|---|---|---|---|---|
| nm | 1m | 119 | +0.0260 | +0.305 | 0.66 | -0.175 | -0.855 | 0.75 | Y | C* |
| nm | 3m | 119 | +0.0198 | +0.217 | 0.61 | -0.381 | -0.952 | 0.50 | Y | C* |
| nm | 6m | 117 | +0.0062 | +0.063 | 0.56 | -0.538 | -0.927 | 0.50 | N | C* |
| nm | 12m | 111 | -0.0107 | -0.097 | 0.48 | -0.550 | -0.915 | 0.50 | N | C |
| at | 1m | 116 | +0.0144 | +0.211 | 0.60 | +0.127 | +0.406 | 0.75 | Y | C |
| at | 3m | 116 | +0.0160 | +0.242 | 0.55 | +0.244 | +0.261 | 0.50 | Y | C |
| at | 6m | 114 | +0.0185 | +0.279 | 0.61 | +0.318 | +0.418 | 0.50 | Y | C |
| at | 12m | 108 | +0.0248 | +0.407 | 0.63 | +0.523 | +0.648 | 0.50 | Y | B |
| em | 1m | 128 | +0.0078 | +0.163 | 0.57 | +0.039 | +0.467 | 1.00 | Y | D |
| em | 3m | 128 | +0.0095 | +0.214 | 0.62 | +0.070 | +0.515 | 0.80 | Y | D |
| em | 6m | 126 | +0.0131 | +0.278 | 0.65 | +0.160 | +0.442 | 0.80 | Y | C |
| em | 12m | 120 | +0.0223 | +0.531 | 0.73 | +0.265 | +0.648 | 0.80 | Y | B |
| roe | 1m | 116 | +0.0291 | +0.280 | 0.65 | -0.105 | -0.842 | 0.75 | Y | C* |
| roe | 3m | 116 | +0.0235 | +0.221 | 0.59 | -0.250 | -0.927 | 0.75 | Y | C* |
| roe | 6m | 114 | +0.0113 | +0.101 | 0.57 | -0.379 | -0.927 | 0.00 | Y | C* |
| roe | 12m | 108 | -0.0024 | -0.021 | 0.48 | -0.400 | -0.976 | 0.25 | N | D |

## R4 Regime breakdown (TWII 200d MA, h=6m)

| Factor | Regime | n | IC | IR | hit |
|---|---|---|---|---|---|
| nm | bull | 93 | +0.0058 | +0.063 | 0.53 |
| nm | bear | 24 | +0.0079 | +0.065 | 0.71 |
| at | bull | 92 | +0.0119 | +0.179 | 0.57 |
| at | bear | 22 | +0.0461 | +0.785 | 0.77 |
| em | bull | 93 | +0.0158 | +0.379 | 0.65 |
| em | bear | 33 | +0.0055 | +0.091 | 0.67 |
| roe | bull | 92 | +0.0054 | +0.051 | 0.53 |
| roe | bear | 22 | +0.0363 | +0.266 | 0.73 |

## R5 與 F-Score 相關性 (Spearman)

| Factor | n | rho(factor, quality_score) | rho(factor, f_score) | 增量價值 |
|---|---|---|---|---|
| nm | 195341 | +0.501 | +0.276 | 低(共線) |
| at | 195528 | +0.137 | +0.148 | 高(獨立) |
| em | 213957 | -0.452 | -0.038 | 中 |
| roe | 188564 | +0.408 | +0.276 | 中 |

## Inter-factor 相關性 (DuPont 三分量間)

| pair | n | rho |
|---|---|---|
| nm_vs_at | 189282 | -0.028 |
| nm_vs_em | 194569 | -0.230 |
| at_vs_em | 194756 | +0.151 |
| nm_vs_roe | 188564 | +0.798 |
| at_vs_roe | 188564 | +0.406 |
| em_vs_roe | 188564 | +0.027 |

## Decile Q1-Q10 平均月報酬 (h=6m)

| Factor | Q1 | Q2 | Q3 | Q4 | Q5 | Q6 | Q7 | Q8 | Q9 | Q10 |
|---|---|---|---|---|---|---|---|---|---|---|
| nm | +10.25% | +10.02% | +9.86% | +10.26% | +8.97% | +8.12% | +7.70% | +7.00% | +6.29% | +5.49% |
| at | +7.65% | +7.85% | +7.69% | +8.57% | +9.51% | +7.91% | +7.43% | +9.11% | +7.73% | +10.23% |
| em | +7.77% | +7.80% | +8.29% | +8.89% | +7.31% | +7.48% | +8.02% | +8.69% | +8.70% | +8.88% |
| roe | +10.10% | +9.82% | +10.00% | +8.43% | +8.91% | +7.61% | +7.90% | +8.33% | +6.73% | +6.08% |

## R6 Quality filter: ROE x EM 3x3 grid (forward 6m return)

- N months used: 114

| ROE \ EM | low | mid | high |
|---|---|---|---|
| **low** | +9.91% (Sh +0.79) | +8.99% (Sh +0.65) | +10.20% (Sh +0.76) |
| **mid** | +7.54% (Sh +0.75) | +8.55% (Sh +0.83) | +8.97% (Sh +0.78) |
| **high** | +6.99% (Sh +0.62) | +6.50% (Sh +0.62) | +7.89% (Sh +0.70) |

### R6 關鍵比較

- **high-ROE x low-EM** (低槓桿真價值): +6.985% per 6m
- **high-ROE x high-EM** (高槓桿撐 ROE): +7.889% per 6m
- **Spread (low-EM minus high-EM)**: -0.904% per 6m -> NO quality filter alpha (反直覺)

## Grading 圖例

- **A**: |IC|>=0.03 且 |IR|>=0.3 + decile/IC 同向 + monotonicity 同向
- **B**: |IC| 0.02-0.03, |IR| 0.2-0.3 + 方向一致
- **C**: |IC| 0.01-0.02 觀察
- **C\***: IC 數字 OK 但 decile spread / monotonicity 反向 (假象)
- **D**: |IC|<0.01 或反向

---

## 最終 Verdict

### 三因子個別判級

| 因子 | 最佳 horizon | 判級 | 結論 |
|---|---|---|---|
| **F1 nm (淨利率)** | 12m IC -0.011 | **C\*** (1m/3m/6m) -> **D** (12m) | **不可上線**: 全 horizon decile spread 與 IC 反向，monotonicity -0.85 ~ -0.95 完全 inverse。Q1 +10.25% > Q10 +5.49%, 高淨利率 = 未來輸 4.77%/6m |
| **F2 at (資產周轉率)** | 12m IC +0.025 IR +0.41 | **B** (12m only), C (1-6m) | 可考慮 long horizon (12m) signal: IC 正、spread Sharpe +0.52、mono +0.65、LOO 穩。但 12m horizon 對選股切換週期太長 |
| **F3 em (權益乘數)** | 12m IC +0.022 IR +0.53 | **B** (12m), C (6m), D (1-3m) | 唯一三條件全 align (IC+spread+mono): 高槓桿 = 未來小幅跑贏。**完全顛覆教科書「高負債 = 高風險」直覺** |
| **F4 roe (合成 ROE)** | 1m IC +0.029 | **C\*** (1m/3m/6m) -> **D** (12m) | 與 nm 高度共線 (rho=+0.80), 同樣 IC vs decile 反向, 高 ROE 反向; 不可上線 |

### R6 三大發現 (顛覆 DuPont 教科書)

1. **「真價值 vs 偽價值」假說在台股不成立**:
   - high-ROE x low-EM (低槓桿真價值): +6.99% per 6m
   - high-ROE x high-EM (高槓桿撐 ROE): +7.89% per 6m
   - Spread = -0.90%, **低槓桿真價值反而輸高槓桿**
2. **整個 ROE 維度反轉**: low-ROE row (平均 +9.7%) 比 high-ROE row (平均 +7.1%) 跑贏 2.6%/6m, 即「ROE 越高越爛」
3. **Equity Multiplier 是反直覺正向因子**: 高負債 / 高槓桿股票在台股 cross-section 裡未來表現較好 (可能是金融股 / 高 leverage 股票享有 yield 補貼; 也可能是 mean-reversion: 低 EM 公司多為現金部位過剩無成長)

### R5 與既有 F-Score 對齊

| 因子 | rho(factor, quality_score) | 結論 |
|---|---|---|
| nm | +0.50 | **與既有 _score_quality 高度共線 (低增量)** |
| at | +0.14 | **獨立** |
| em | -0.45 | 與 quality 反向 (合理: F-Score 認為高負債 = 體質差) |
| roe | +0.41 | 中度共線 |

### 上線建議

**結論: 全部 D 級, 不調整 value_screener 既有權重 25/25/25/15/0/10**

**理由**:
1. 三個 raw factor 沒有任何一個達到 A 級 (IC>=0.03 且 IR>=0.3 且 decile/mono 同向)
2. 主要 alpha factor (nm, roe) 全部是 **C\* 假象** (IC 跟 decile 反向), 直接拿來加分等於踩雷
3. R6 quality filter「真價值 vs 偽價值」假說 spread = -0.90%, **負向 = 反而砍掉好股**, 完全不能拿來當 quality filter 用
4. F2 at / F3 em 雖在 12m 達 B 級, 但
   - at 的 IC 來源主要是 bear regime (IC=+0.046, IR=+0.79), bull regime IC 只有 +0.012, 不穩
   - em 是反直覺方向, 上線會違反 _score_quality 邏輯, 容易導致 portfolio 偏高槓桿股, 風險集中
5. nm 與既有 quality_score 共線 +0.50, 即使方向對也沒有獨立增量

### 副產物 / 後續可探索

1. **「低 ROE 反向訊號」是真的嗎?**
   - low-ROE 群組未來報酬最高 (+9.7%/6m), 比 high-ROE +2.6% 跑贏
   - 但要看 absolute return level: low-ROE 通常 entry 價格也較低, 可能是 deep value 效應而非 quality alpha
   - 跟既有 _score_value (PB rank) 對比看是否獨立, 若獨立可考慮 long-only 反向 ROE 信號

2. **F3 em 反直覺正向值得 fundamental 探究**:
   - 是金融股偏好? 還是 small-cap 高 leverage premium?
   - 可拆分 sector breakdown 看金融 / 科技 / 傳產 的 em IC 是否一致

3. **Quality filter 重新設計**:
   - 若要做 quality filter, 用 **F-Score >= 7** 比 DuPont 「高 ROE x 低 EM」更有效
   - 既有 _score_quality 已經 capture 主要訊號, DuPont 是 redundant 嘗試

### 風險提醒 (符合 SOP)

- IC vs decile sign 不一致 = ROIC C\* 假象重現, 已照 SOP 4 條 3 條檢驗到 (IC vs decile sign / 5+ OOS years / LOO)
- LOO consistency 在 nm h=6m/12m 跟 roe h=12m **不一致**, 進一步證實 nm/roe 訊號不穩
- WF sign-hit roe h=6m 只有 0.00 (4 個 OOS window 全部翻盤), 等同「IS 看到的 IC 在 OOS 完全消失甚至反向」, **典型 overfit 假象**

