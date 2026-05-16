# Niche B: DCF MOS Position Sizing 驗證

4 種 weight scheme 應用同一 universe (n_avg=132)，hold 252d:
- **Baseline (eq)**: 等權
- **MOS bucket**: Q4 area [+10%, +50%] 1.5x weight，其他 0.5x
- **MOS linear**: weight = clip(MOS, -50%, +100%) + 1.0 (避 Q10 outlier 主導)
- **MOS rank**: weight = base_mos rank percentile × 2.0

## Yearly returns

| FY | Baseline (eq) | MOS bucket | MOS linear | MOS rank |
|---|---:|---:|---:|---:|
| 2019-12-31 | +98.85% | +96.07% | +102.29% | +104.40% |
| 2020-12-31 | +9.79% | +16.52% | +8.33% | +6.94% |
| 2021-12-31 | -0.30% | +0.16% | -1.12% | -1.09% |
| 2022-12-31 | +59.77% | +69.32% | +60.36% | +58.96% |
| 2023-12-31 | -11.25% | -12.30% | -10.99% | -9.58% |

## Cross-year summary

| Scheme | Mean Ret | Std | Sharpe | Δ vs Base | t-stat | p |
|---|---:|---:|---:|---:|---:|---|
| Baseline (eq) | +31.37% | 46.49% | +0.67 | — | — | — |
| MOS bucket | +33.95% | 46.62% | +0.73 | +0.05 | +1.09 | ❌ noise |
| MOS linear | +31.77% | 48.09% | +0.66 | -0.01 | +0.48 | ❌ noise |
| MOS rank | +31.93% | 48.53% | +0.66 | -0.02 | +0.39 | ❌ noise |

⚠️ 5 個 yearly observation, t > 2.78 才能 95% 排除 noise (df=4, two-tail)

---
## Verdict

- 最佳 scheme: **MOS bucket**, Sharpe +0.73 (Δ +0.05), t=+1.09
- ❌ p>0.10 noise，跟 sweet spot 同結論：方向對但樣本不足，不上線

結論同 selection 驗證：DCF 訊號 directional 存在但 5-yr sample 統計力不足。