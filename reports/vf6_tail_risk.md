# VF-6 Tail Risk 分析

生成時間: 2026-04-17
資料: 三份 trade_journal (pure_right 9263 / mixed 2556 / pure_left 4660)

## 1. Trade-level tail 分位 (fwd_20d)

| 模式 | min | P1 | P5 | P10 | Mean | N |
|------|----:|----:|----:|----:|-----:|--:|
| mixed | −44.78% | −26.22% | −16.90% | −12.13% | +1.80% | 2,556 |
| **pure_right** | **−55.95%** | −27.11% | −17.76% | −13.20% | **+3.20%** | 9,263 |
| pure_left | −44.78% | −27.90% | −17.96% | −13.20% | +1.49% | 4,660 |

**觀察**: pure_right 最大單筆虧損 −55.95%（mixed −44.78%），左尾加厚約 11 pp。這是因為 pure_right 包含更多「強趨勢但在走反轉」的股票（typical right-side trap）。

## 2. 期間內最大跌幅 (fwd_20d_min) 分位

| 模式 | P5 min | P10 min | 觸 −8% | 觸 −10% | 觸 −15% |
|------|-------:|--------:|------:|------:|------:|
| mixed | −22.96% | −17.49% | 36.6% | 28.1% | 14.2% |
| **pure_right** | −23.43% | −18.90% | **43.5%** | **33.6%** | **17.1%** |
| pure_left | −23.66% | −18.37% | 37.3% | 29.4% | 15.8% |

**觀察**: pure_right 持有期間觸及 −10% 停損的比例 33.6% vs mixed 28.1%（+5.5 pp）。停損紀律必要但可承受。

## 3. Portfolio 級 DD (可比 — non-overlap monthly rebalance)

每 4 週取一次 picks、持有 20d，avoid overlap:

| 模式 | Compound 5Y | CAGR | Max DD | Sharpe(年化) | Win% | N periods |
|------|-----------:|-----:|-------:|-------------:|-----:|----------:|
| **pure_right** | **+387.7%** | **+34.6%** | **−30.01%** | **1.29** | 62.5% | 64 |
| mixed | +81.1% | +11.8% | −31.21% | 0.58 | 54.7% | 64 |
| pure_left | +63.8% | +9.7% | −37.47% | 0.49 | 51.6% | 64 |

**結論**: 雖然 trade-level 左尾較重，但 **portfolio DD 在三版裡最小** (−30%)，CAGR 翻 3 倍。Diversification 效果 (36 檔 vs 10 檔) 減弱單筆左尾影響。

## 4. 下行風險調整指標

| 模式 | Sharpe (weekly, portfolio) | Sortino | Sortino/Sharpe |
|------|---------------------------:|--------:|---------------:|
| mixed | 1.21 | 2.12 | 1.75 |
| **pure_right** | **2.60** | **4.30** | 1.65 |
| pure_left | 1.39 | 2.14 | 1.54 |

**觀察**: Sortino/Sharpe 比約 1.6-1.8（三版接近），代表報酬分布的偏度沒有特別不對稱 — 上行與下行波動基本平衡，pure_right 沒因多進場而系統性拉低 Sortino。

## 5. 在 trending regime 的 tail 表現

fwd_20d by regime:

| Regime | mode | mean | 觸 −10% (同 regime 下比例) |
|--------|------|-----:|---------:|
| trending | mixed | **−1.30%** | 較高 |
| trending | pure_right | **−0.64%** | 較高 |
| trending | pure_left | −1.63% | 較高 |

trending regime 三版均負，但 pure_right 抗跌最佳。建議 trending 期可做 regime-aware 調整（例如切換為 threshold=10 濃縮進場）。

## 6. 硬停損 guardrail 建議

- `exit_manager.py` 現行 −8% / −10% 停損**必要保留**（pure_right 觸發比例 43.5% / 33.6%）
- 若停損正確執行，單筆最大虧損可壓到 ~−10%，避免 −30% / −55% 尾部事件
- 建議搭配:
  - 進場後 3 天內 gap down > 5% 硬停
  - 連續 2 根收盤低於 daily_ma10 退場
  - 持有期間觸及 ATR × 2 回撤退場

## 7. 最終 risk 判讀

| 面向 | 判定 |
|------|------|
| Trade 左尾加厚 11 pp | 可接受（因樣本 3.6×，單筆負擔分散）|
| Portfolio DD | **比 mixed 還小** (−30% vs −31%) |
| Sortino | 翻倍 (4.30 vs 2.12) |
| Tail event 頻率 | 觸 −10% 比例高 5.5 pp，需靠 exit_manager 截斷 |
| Regime 敏感 | trending regime 三版均負，pure_right 較抗跌 |

**Tail risk 整體可接受**，不構成阻擋 pure_right 採用的理由。
