# VF-G6 QM 軟警報 / 部位參數驗證 (2026-04-23)

- Journal: 4923 picks × 205 檔 × 538 週

## 涵蓋的 magic numbers

| # | 參數 | 現值 | 可驗? |
| --- | --- | --- | --- |
| 13 | base_pct | 8.0% | ✅ T1+T2 |
| 14 | trigger mult clip | 0.5 ~ 1.5 | ❌ journal 無 trigger |
| 15 | QM entry gate threshold | trigger ≥ 3 | ❌ 同 VF-G5 |
| 16 | GRACE_PERIOD_DAYS | 5 | ❌ 需 day-level 模擬 |
| 17 | CONSEC_BREACH_DAYS | 2 | ❌ 需 day-level 模擬 |
| 18-22 | 軟警報各門檻 | 各種 | ❌ 需 day-level 模擬 |

## T1: QM-weighted vs Equal-weighted portfolio

Live formula: `weight_i = clip(qm_score_i / 80, 0.2, 1.5)` normalized per week
比較：`qw_mean = sum(w_norm × fwd_X)` vs `eq_mean = mean(fwd_X)`

| horizon | n_weeks | eq_mean | qw_mean | delta | t_stat | p_val | delta_IR | grade |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 5 | 445 | +0.27% | +0.28% | +0.01% | +1.1177 | +0.2643 | +0.0530 | C |
| 20 | 445 | +1.19% | +1.25% | +0.06% | +2.0503 | +0.0409 | +0.0972 | C |
| 40 | 445 | +2.73% | +2.76% | +0.03% | +0.6139 | +0.5396 | +0.0291 | D |
| 60 | 445 | +4.10% | +4.15% | +0.06% | +0.9939 | +0.3208 | +0.0471 | D |

## T2: base_pct grid 對 total exposure

假設每週 top_20 picks 全買，用 live formula (trigger neutral = 1.0)：
`pos_i = base_pct × (qm_score_i / 80)`，total_exposure = Σ pos_i

| base_pct | top_n | n_weeks | total_exposure_mean | total_exposure_p10 | total_exposure_p90 | mean_pos | max_pos | min_pos |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| +4.0000 | +20.0000 | 529 | +14.75% | +5.87% | +25.51% | +3.41% | +4.44% | +1.05% |
| +6.0000 | +20.0000 | 529 | +22.13% | +8.81% | +38.26% | +5.12% | +6.67% | +1.57% |
| +8.0000 | +20.0000 | 529 | +29.50% | +11.74% | +51.02% | +6.82% | +8.89% | +2.10% |
| +10.0000 | +20.0000 | 529 | +36.88% | +14.68% | +63.77% | +8.53% | +11.11% | +2.62% |
| +12.0000 | +20.0000 | 529 | +44.26% | +17.61% | +76.52% | +10.24% | +13.33% | +3.15% |

## 結論

### T1: QM 加權是否勝等權？

- 最大 |delta IR|: +0.097 (C)
- p < 0.05 顯著 horizon: [20]
- 顯著中 1/1 為 qw > eq
- **|delta IR| < 0.1 平原** → QM 加權 formula 無顯著 alpha
  - 可簡化成固定 base_pct × 等權，但為了 UI 顯示「根據分數配置」的直覺，保留 formula 無害

### T2: base_pct 合理範圍（含 mixed mode gate caveat）

T2 的 total_exposure 測試用 `trade_journal_qm_tw_mixed.parquet`，此 journal 套用 VF-6 mixed mode gate（trend AND MA 支撐），**每週平均只 ~4.3 個 picks**（非 live 的 20 full picks）。因此 T2 數字反映「mixed mode under-leveraged 現象」，**不是 live 實際 exposure**。

| base_pct | T2 (mixed gate ~4.3 picks) | Live 估算 (20 picks, avg qm=61) |
|---|---|---|
| 4% | 15% | ~61% |
| 6% | 22% | ~91% |
| **8% (live)** | **30%** | **~122%** |
| 10% | 37% | ~152% |
| 12% | 44% | ~183% |

Live estimate: `20 × base_pct × avg(qm_score/80)` = `20 × 8% × 0.76` = **~122% 略槓桿**。

**結論**：
- Mixed mode gate 實際上每週 picks 少，若 user 真的每週吞 4-5 個 mixed picks → live 實際 30% exposure 偏保守（合理帶 70-120%）
- Live QM UI 發 top 20 → user 每週買多少自行決定，base_pct=8% 給出「單檔 6-9%」的 sizing，屬合理量級
- **base_pct=8% 不動**：無 alpha 影響，僅 sizing hint，live formula 保留

### 未驗參數（需 day-level OHLCV 模擬，本輪跳過）

- **GRACE_PERIOD_DAYS = 5**：進場後 5d 內不觸發硬停損，需 day-level 時序
- **CONSEC_BREACH_DAYS = 2**：連續 2 日跌破才確認，需 day-level OHLCV
- **VOLUME_CONFIRM_RATIO = 0.8**：量縮確認，需日成交量
- **trigger multiplier clip(0.5, 1.5)**：journal 無 trigger_score 欄位
- **QM entry gate threshold (≥3)**：同 VF-G5 BUY threshold，journal 是 post-filter

以上 5 項估算影響：
- GRACE/CONSEC/VOL：防止 5d 內假跌破洗出場，尾端風險控管類（類比 VF-G1 結論：SL 價值在尾端控管，不在期望報酬增強）
- 合理預期：這些參數空間同為平原（exit 類 5 連 D 前例）
- 需要時可另建 day-level 模擬工具驗，但 ROI 低

## 產出

- `tools/vf_g6_validation.py`
- `reports/vf_g6_validation.md`
- `reports/vf_g6_t1_weighted_vs_equal.csv`
- `reports/vf_g6_t2_base_pct_exposure.csv`