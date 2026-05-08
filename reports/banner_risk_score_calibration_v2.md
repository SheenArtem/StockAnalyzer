# Banner Risk Score Calibration v2 (with PCR + FGI backfilled)

**Generated**: 2026-05-08
**Method**: SOP-14 informational tier, result-driven weight derivation via univariate lift filter
**Panel**: crash_predictor_tw_panel.parquet | 1999-01-05 to 2026-01-29
**Composite valid range**: 2002-01-02 to 2026-01-29 (N=5906 rows, ~24.1 yrs)
**Label**: forward 60-day peak-to-trough drawdown (10% / 5% threshold)
**Baseline (composite valid range)**: P(MDD>=10%)=26.1%, P(MDD>=5%)=50.0%

---

## Step 1: 6 Signal Univariate Lift Analysis

| Signal | Direction | N rows | Danger Zone | Co-occur 10% | **Lift 10%** | Co-occur 5% | Lift 5% | N Events | Status |
|---|---|---|---|---|---|---|---|---|---|
| m1b_ratio | + (high=danger) | 6710 | >= P85 (36.4265) | 57.8% | **1.922** | 74.8% | 1.411 | 582 | **INCLUDED** |
| rv10 | + (high=danger) | 6709 | >= P85 (0.2832) | 51.7% | **1.720** | 67.6% | 1.276 | 521 | **INCLUDED** |
| rv30 | + (high=danger) | 6699 | >= P85 (0.2824) | 50.8% | **1.688** | 65.2% | 1.228 | 511 | **INCLUDED** |
| pcr_volume | + (high=danger) | 5933 | >= P85 (1.3257) | 43.5% | **1.670** | 64.9% | 1.301 | 387 | **INCLUDED** |
| pcr_oi | + (high=danger) | 5933 | >= P85 (1.2755) | 39.4% | **1.514** | 56.6% | 1.135 | 351 | **INCLUDED** |
| fgi_score | - (low=danger) | 6627 | <= P15 (34.8000) | 44.3% | **1.479** | 58.3% | 1.102 | 442 | **INCLUDED** |


---

## Step 2: Lift-Based Weights (INCLUDED only)

| Signal | Lift10 | Weight |
|---|---|---|
| m1b_ratio | 1.922 | **19.2%** |
| rv10 | 1.720 | **17.2%** |
| rv30 | 1.688 | **16.9%** |
| pcr_volume | 1.670 | **16.7%** |
| pcr_oi | 1.514 | **15.2%** |
| fgi_score | 1.479 | **14.8%** |


Total signals in composite: **6** (out of 6)

Composite formula:

```
composite = rank_pct(m1b_ratio) * 0.192 + rank_pct(rv10) * 0.172 + rank_pct(rv30) * 0.169 + rank_pct(pcr_volume) * 0.167 + rank_pct(pcr_oi) * 0.152 + rank_pct(fgi_score) * 0.148
```

For high-danger signals (m1b/rv/pcr): rank_pct = signal value's percentile rank (0-100)
For low-danger signals (fgi): rank_pct = (100 - signal value's percentile rank)

---

## Step 3: SOP-12 Check (composite must beat best-single)

| | Lift10 |
|---|---|
| Best-single (m1b_ratio P85) | 1.843 |
| Composite at P85 | 2.042 |

**Verdict**: USE COMPOSITE

---

## Step 4: 3-Tier Zone Thresholds

| Zone | Composite | P-Level | Co-occur 10% | Co-occur 5% | Fwd60d MDD Median | Days/Year |
|---|---|---|---|---|---|---|
| Orange | >= 70.7 | P85+ | 53.3% | 62.3% | -11.2% | ~37 |
| Yellow | 55.2-70.7 | P65-P85 | 33.3% | 55.7% | -5.6% | ~49 |
| Green | < 55.2 | < P65 | 17.6% | 45.4% | -4.2% | ~159 |

Monotonicity check (Orange > Yellow > Green): co10 PASS, co5 PASS

---

## Comparison with v1 (3 signals only)

| Metric | v1 (m1b/rv10/rv30) | v2 (this) | Delta |
|---|---|---|---|
| N signals included | 3 | 6 | +3 |
| Best-single lift10 | 1.922 | 1.843 | -0.079 |
| Composite lift10 | 1.982 | 2.042 | +0.060 |
| N composite-valid rows | 6,699 | 5906 | -793 |

---

## Caveats

### v2-specific
- **FGI backfill caveats**: market_breadth 是 ^TWII 5d/20d return proxy（無真實 advance/decline 歷史），margin_balance 2021- 才有，pcr_value 2010- 才有 — 加權正規化補
- **PCR backfill caveats**: 2010-2026 全覆蓋，2010 前 NaN 排除
- **Composite valid range 縮短**: PCR 起點 2010 限制 composite 整體期間，N 從 6,699 降到 5906
- **HMM regime 仍排除**: lift10 1.047 marginal + categorical 不可 percentile rank
- **5 個即時訊號中還缺 4 個**: CNN FGI / ATM Put / MTX/TXF / Inst PC skew 仍無歷史 panel（見 BL-5 backlog）

### SOP-14 informational tier
- Banner UI 文案禁: "predicts crash / leading / 領先 / 預警"
- Banner UI 文案 OK: "elevated co-occurrence / 同期重合率 / 歷史此狀態下"
- 不設紅燈，最高 Orange "需審視持倉但不自動調整"

### SOP-12 verdict adherence
- Composite passes SOP-12 (lift > best-single) -> deploy as composite

---

## Implementation Spec for Banner UI

```python
WEIGHTS = {
    'm1b_ratio': 0.1923,  # lift10=1.922
    'rv10': 0.1721,  # lift10=1.720
    'rv30': 0.1689,  # lift10=1.688
    'pcr_volume': 0.1671,  # lift10=1.670
    'pcr_oi': 0.1516,  # lift10=1.514
    'fgi_score': 0.1480,  # lift10=1.479
}

def compute_banner_risk_score(signal_values_today, history_dfs):
    """
    signal_values_today: dict of {signal_name: today's value}
    history_dfs: dict of {signal_name: pd.Series of historical values for percentile rank}
    """
    ranks = {}
    for name, value in signal_values_today.items():
        hist = history_dfs[name]
        # high-danger = direct rank; low-danger (fgi_score) = reversed
        rank = (hist <= value).mean() * 100
        if name == 'fgi_score':
            rank = 100 - rank
        ranks[name] = rank

    composite = sum(ranks[n] * w for n, w in WEIGHTS.items())

    if composite >= 70.7:
        zone = 'orange'
    elif composite >= 55.2:
        zone = 'yellow'
    else:
        zone = 'green'

    return {'composite': composite, 'zone': zone, 'breakdown': ranks}
```

**Thresholds**: Orange >= 70.7 | Yellow >= 55.2 | Green < 55.2

**UI 文案範本**:
- Orange: 「目前綜合風險指標 {composite:.0f}（橘燈）；歷史此區間 60d 內出現 ≥10% 回檔的同期重合率 53%」
- Yellow: 「目前綜合風險指標 {composite:.0f}（黃燈）；歷史此區間同期重合率 33%」
- Green: 「目前綜合風險指標 {composite:.0f}（綠燈）；歷史此區間同期重合率 18%」
