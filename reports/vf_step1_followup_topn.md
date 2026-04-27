# Step 1 Follow-up — Dual + tv_top_25 + only_volatile × top_n grid

**基準**: Main grid 已確認 mcap=tv_top_25 + regime=only_volatile 為 Sharpe 冠軍 (1.242)。

**問題**: 濃縮持股（top_n=3/5）能否再拉高 CAGR？MDD 會不會爆？

**Spec**: top_n 同時套用 Value 側與 QM 側；max holdings = top_n × 2 (volatile 週) / top_n (非 volatile 週)

## 結果（排序 by Sharpe）

| top_n | max holdings (V/非V) | CAGR% | Sharpe | MDD% | Vol% | hit% | v_on% | n |
|---|---|---|---|---|---|---|---|---|
| 20.0 | 40.0/20.0 | 27.41 | 1.242 | -25.17 | 21.26 | 67.1 | 28.9 | 76.0 |
| 10.0 | 20.0/10.0 | 25.8 | 1.039 | -38.58 | 23.87 | 57.9 | 28.9 | 76.0 |
| 5.0 | 10.0/5.0 | 22.16 | 0.893 | -15.38 | 23.71 | 40.8 | 28.9 | 76.0 |
| 3.0 | 6.0/3.0 | 8.33 | 0.327 | -23.02 | 22.41 | 28.9 | 28.9 | 76.0 |

## Caveats

- 無交易成本；top_n 越小 turnover 越集中，實務 slippage 影響更大
- top_n=3 可能遇到 Value pool 在某週 <3 檔 → 自動降檔不補
- MDD 在 top_n 小時通常放大（單檔爆雷 weight 高）
