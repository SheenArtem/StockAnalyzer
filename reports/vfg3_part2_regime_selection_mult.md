# VF-G3 Part 2: REGIME_GROUP_WEIGHTS + ADDON_MULT validation

**Universe**: trade_journal_qm_tw.parquet -- 4,923 picks, 538 weeks

## TL;DR

- **Decision**: CUT: set GROUP_WEIGHTS all 1.0 (no evidence of regime alpha in selection)
- **Grade**: D
- V1 vs V2 IC_20d delta = +0.0007  (V1=+0.0294, V2=+0.0288)
- V1 vs V2 top-20 fwd_20d return delta = +0.00% (V1=2.63%, V2=2.63%)
- Walk-forward mean test IC_20d: V1=+0.0290  V2=+0.0282  V3_trained=+0.0367

## Methodology

- Trade journal `trade_journal_qm_tw_pure_right.parquet` (VF-6 winner)
- Components: `f_score` (quality anchor, base weight 50), `body_score` (momentum proxy, 30), `trend_score` (trend, 20)
- Regime weights applied multiplicatively to (trend, body) and renormalized so total = 100
- Metrics: weekly Spearman IC vs fwd_20d/fwd_40d; top-20 equal-weight portfolio returns

## V1 vs V2 (overall)

| version | ic_20d | ir_20d | n_weeks_20d | ic_40d | ir_40d | n_weeks_40d | top20_ret_20d | top20_sharpe_20d | top20_ret_40d | top20_sharpe_40d |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| V1_current | 0.0294 | 0.5589 | 445 | -0.0021 | -0.0415 | 445 | 0.0263 | 1.4565 | 0.0856 | 2.7689 |
| V2_flat | 0.0288 | 0.5458 | 445 | 0.0012 | 0.0231 | 445 | 0.0263 | 1.4565 | 0.0856 | 2.7689 |

## Per-regime breakdown

| regime | fwd | n_picks | ic_v1 | ic_v2 | ic_delta | ir_v1 | ir_v2 | top10_ret_v1 | top10_ret_v2 | top10_delta_ret |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| trending | fwd_20d | 740 | 0.0277 | 0.0425 | -0.0148 | 0.5744 | 0.8779 | -0.0032 | -0.0043 | 0.0012 |
| trending | fwd_40d | 740 | 0.0382 | 0.0547 | -0.0165 | 0.8195 | 1.1608 | 0.0226 | 0.0230 | -0.0004 |
| ranging | fwd_20d | 1272 | 0.0220 | 0.0186 | 0.0034 | 0.4808 | 0.4084 | 0.0150 | 0.0143 | 0.0006 |
| ranging | fwd_40d | 1272 | -0.0096 | -0.0036 | -0.0060 | -0.2131 | -0.0809 | 0.0288 | 0.0288 | 0.0000 |
| volatile | fwd_20d | 1440 | 0.0035 | -0.0034 | 0.0068 | 0.0579 | -0.0560 | 0.0315 | 0.0319 | -0.0004 |
| volatile | fwd_40d | 1440 | -0.0222 | -0.0241 | 0.0020 | -0.3844 | -0.4179 | 0.0596 | 0.0598 | -0.0001 |
| neutral | fwd_20d | 1471 | 0.0617 | 0.0617 | 0.0000 | 1.1856 | 1.1856 | 0.0130 | 0.0130 | 0.0000 |
| neutral | fwd_40d | 1471 | 0.0040 | 0.0040 | 0.0000 | 0.0839 | 0.0839 | 0.0216 | 0.0216 | 0.0000 |

## Walk-forward summary

| version | mean_test_ic | median_test_ic | pct_positive_ic | mean_top20_ret | n_windows |
| --- | --- | --- | --- | --- | --- |
| v1_current | 0.0290 | 0.0295 | 0.4962 | 0.0340 | 123 |
| v2_flat | 0.0282 | 0.0227 | 0.4962 | 0.0340 | 123 |
| v3_trained | 0.0367 | 0.0316 | 0.5115 | 0.0340 | 123 |

## Grid search best per regime (in-sample)

| regime | w_trend | w_body | ic_20d | ir_20d |
| --- | --- | --- | --- | --- |
| trending_BEST | 0.5000 | 1.0000 | 0.0455 | nan |
| ranging_BEST | 1.5000 | 0.5000 | 0.0414 | nan |
| volatile_BEST | 1.3000 | 0.5000 | 0.0245 | nan |
| neutral_BEST | 1.5000 | 0.5000 | 0.0799 | nan |

## Suggested diff (if CUT)

```python
# analysis_engine.py lines 31-43 -> replace with:
REGIME_GROUP_WEIGHTS = {r: {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0}
                        for r in ('trending', 'ranging', 'volatile', 'neutral')}
REGIME_ADDON_MULT    = {r: {'chip': 1.0, 'sentiment': 1.0, 'revenue': 1.0, 'etf': 1.0}
                        for r in ('trending', 'ranging', 'volatile', 'neutral')}
```