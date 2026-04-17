# VF-G3 Part 2: REGIME_GROUP_WEIGHTS + ADDON_MULT validation

**Universe**: trade_journal_qm_tw_pure_right.parquet -- 9,263 picks, 256 weeks

## TL;DR

- **Decision**: CUT: set GROUP_WEIGHTS all 1.0 (no evidence of regime alpha in selection)
- **Grade**: D
- V1 vs V2 IC_20d delta = +0.0003  (V1=+0.0363, V2=+0.0359)
- V1 vs V2 top-20 fwd_20d return delta = +0.04% (V1=3.67%, V2=3.63%)
- Walk-forward mean test IC_20d: V1=+0.0340  V2=+0.0333  V3_trained=+0.0459

## Methodology

- Trade journal `trade_journal_qm_tw_pure_right.parquet` (VF-6 winner)
- Components: `f_score` (quality anchor, base weight 50), `body_score` (momentum proxy, 30), `trend_score` (trend, 20)
- Regime weights applied multiplicatively to (trend, body) and renormalized so total = 100
- Metrics: weekly Spearman IC vs fwd_20d/fwd_40d; top-20 equal-weight portfolio returns

## V1 vs V2 (overall)

| version | ic_20d | ir_20d | n_weeks_20d | ic_40d | ir_40d | n_weeks_40d | top20_ret_20d | top20_sharpe_20d | top20_ret_40d | top20_sharpe_40d |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| V1_current | 0.0363 | 1.2611 | 255 | 0.0333 | 1.1207 | 255 | 0.0367 | 1.4861 | 0.0750 | 2.0184 |
| V2_flat | 0.0359 | 1.2544 | 255 | 0.0332 | 1.1201 | 255 | 0.0363 | 1.4830 | 0.0742 | 2.0096 |

## Per-regime breakdown

| regime | fwd | n_picks | ic_v1 | ic_v2 | ic_delta | ir_v1 | ir_v2 | top10_ret_v1 | top10_ret_v2 | top10_delta_ret |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| trending | fwd_20d | 1647 | -0.0147 | -0.0137 | -0.0011 | -0.5323 | -0.4987 | 0.0124 | 0.0082 | 0.0042 |
| trending | fwd_40d | 1647 | 0.0134 | 0.0143 | -0.0009 | 0.4742 | 0.5194 | 0.0680 | 0.0666 | 0.0014 |
| ranging | fwd_20d | 1947 | 0.0380 | 0.0357 | 0.0023 | 1.3132 | 1.2452 | 0.0345 | 0.0337 | 0.0008 |
| ranging | fwd_40d | 1947 | 0.0617 | 0.0603 | 0.0014 | 2.1905 | 2.1158 | 0.0603 | 0.0570 | 0.0032 |
| volatile | fwd_20d | 3392 | 0.0325 | 0.0325 | 0.0000 | 1.1074 | 1.1074 | 0.0500 | 0.0514 | -0.0014 |
| volatile | fwd_40d | 3392 | 0.0021 | 0.0020 | 0.0000 | 0.0641 | 0.0632 | 0.0839 | 0.0852 | -0.0012 |
| neutral | fwd_20d | 2277 | 0.0749 | 0.0749 | 0.0000 | 2.6968 | 2.6968 | 0.0627 | 0.0627 | 0.0000 |
| neutral | fwd_40d | 2277 | 0.0776 | 0.0776 | 0.0000 | 2.9077 | 2.9077 | 0.1276 | 0.1276 | 0.0000 |

## Walk-forward summary

| version | mean_test_ic | median_test_ic | pct_positive_ic | mean_top20_ret | n_windows |
| --- | --- | --- | --- | --- | --- |
| v1_current | 0.0340 | 0.0378 | 0.5574 | 0.0292 | 61 |
| v2_flat | 0.0333 | 0.0442 | 0.5738 | 0.0289 | 61 |
| v3_trained | 0.0459 | 0.0552 | 0.6557 | 0.0302 | 61 |

## Grid search best per regime (in-sample)

| regime | w_trend | w_body | ic_20d | ir_20d |
| --- | --- | --- | --- | --- |
| trending_BEST | 1.5000 | 0.5000 | 0.0079 | nan |
| ranging_BEST | 0.5000 | 0.5000 | 0.0592 | nan |
| volatile_BEST | 1.0000 | 0.5000 | 0.0470 | nan |
| neutral_BEST | 1.3000 | 0.5000 | 0.0997 | nan |

## Suggested diff (if CUT)

```python
# analysis_engine.py lines 31-43 -> replace with:
REGIME_GROUP_WEIGHTS = {r: {'trend': 1.0, 'momentum': 1.0, 'volume': 1.0}
                        for r in ('trending', 'ranging', 'volatile', 'neutral')}
REGIME_ADDON_MULT    = {r: {'chip': 1.0, 'sentiment': 1.0, 'revenue': 1.0, 'etf': 1.0}
                        for r in ('trending', 'ranging', 'volatile', 'neutral')}
```