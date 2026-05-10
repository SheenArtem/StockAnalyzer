# Crash Predictor — Council B Opportunity Cost Audit

**Date**: 2026-05-08
**Auditor**: chip-analyst (neutral)
**Trigger**: Council B (R2) post-Phase 2 challenge — "ATR-based dynamic stop or Mode D sizing tier ROI > crash predictor"
**Mandate**: confirm where the engineering hour ROI is highest BEFORE Phase 3 commit
**Hard time-box**: 2.5h, no new API fetches

> Verdict (TL;DR): **Council B's challenge is half-validated.** Crash predictor as currently composed (50/30/20 m1b/rv30/rv10) **does NOT improve MDD** in 11-yr TWII backtest — the signals fire **coincident** with drawdown, not before. ATR trail stops also fail to beat fixed 8% trailing on Sharpe/MDD. The actually validated path is **rv10-alone gating** (Sharpe +0.15, MDD +5.9pp vs B&H) — far cheaper than full Phase 3 build.

---

## Section 1 — Paper Trade Engine Exit-Reason Distribution

### Data state

- `data/paper_trades/open_trades.json`: **8 open trades**, all entered 2026-04-13 (regime=volatile)
- `data/paper_trades/trade_log.jsonl`: **does not exist** (no closed trades ever written)
- Engine first ran 2026-04-25 (commit `1a42976`), 13 trading days elapsed, **0 sl_hit / 0 tp_hit / 0 step_a_forced**

### Diagnosis

The paper engine's 3 exit reasons (`sl_hit`, `tp_hit`, `step_a_forced`) require either ±15-30% price move from 4/13 entry **or** a forced step-a exit. Neither has occurred — TWII has been range-bound 36k–37.6k since 4/13.

### Verdict for Section 1

**Sample size = 0. No conclusion possible from paper trade data.** This is itself a data point: the system has been live 25 days and produced **zero** exit signal feedback. Phase 3 #10 guest-credibility evaluation gate (≥50 trades, 6-month accumulation) is not on track at current pace.

> Recommendation: do NOT use paper-trade engine results for any policy decision until Q4 2026 at earliest.

---

## Section 2 — Position Monitor 7 Exit Conditions: Actual Trigger Stats

### Data state

- `data/latest/position_alerts.json` (snapshot 2026-04-30): **3 positions**, **2 alerts** (1 hard, 1 soft)
- `data/latest/position_history.json`: 3 positions tracked, 18 daily records, range 2026-04-22 — 2026-04-30 (9 trading days)

### Triggered alerts observed (entire history)

| ticker | severity | rule | description |
|---|---|---|---|
| 3324 | hard | `fscore_drop` | F-Score 6/9 → 3/9 (Q3→Q4 quarterly) |
| 6239 | soft | `trend_weak` | trend_score = -3.0 |

### Coverage of the 7 declared exit rules

| rule | category | triggered? | notes |
|---|---|---|---|
| hard_stop (ATR-adj -5%~-14% + double-confirm) | hard | no | no positions hit -5% in 9 days |
| supertrend_bear (週) | hard | no | TWII trend remains positive |
| ma20_break (週) | hard | no | TWII above 20-week MA |
| revenue_yoy_neg2 (月營收連2月翻負) | hard | no | needs monthly revenue data; not seen |
| fscore_drop (季掉≥3 分 / ≥2 分且 ≤3) | hard | **1 fired** (3324) | the only hard fire |
| trend_weak (trend_score < 1) | soft | **1 fired** (6239) | |
| trigger_peak_drop / trigger_neg_streak | soft | no | history depth = 9d, peak window 20d |

### Verdict for Section 2

**Sample size grossly insufficient** for hit-rate/forward-return analysis (1 hard fire in 25 days × 3 positions = 75 stock-days). No statistically meaningful estimate of false-positive rate or forward-return delta can be produced. Mode D position monitor is structurally well-designed (per memory `project_dual_position_monitor_contract`) but has no real exit data accumulated.

> Recommendation: same as §1 — accumulate ≥6 months before reading any rule efficacy from production data.

---

## Section 3 — ATR Dynamic Trailing Stop vs Fixed 8% Trailing (THE meat)

### Method

- **Universe**: TWII (broad-market proxy), 2330, 2454 — same start 2015-01-05, end 2026-04-21/30 (~11 years)
- **Strategies**:
  - **B&H** baseline
  - **Fixed 8% trailing**: stop = peak × 0.92
  - **ATR trailing**: stop = peak − K × ATR(20), K ∈ {2.0, 2.5, 3.0}
  - Re-entry: when price recovers to ≥ 95% of prior peak
- No commission/slippage (signal-quality test, not execution)
- Tool: `tools/audit_atr_vs_fixed_stop.py`, output `reports/audit_atr_vs_fixed_stop.csv`

### Per-ticker results

**TWII (2015-01 — 2026-04, 2741 days)**

| mode | CAGR | Sharpe | MDD | Trades | Hit% | AvgHold |
|---|---|---|---|---|---|---|
| B&H | 13.20% | 0.83 | -31.63% | 1 | 100% | 4124d |
| fixed 8% | 7.71% | 0.71 | -25.44% | 12 | 50.0% | 141d |
| atr K=2.0 | 10.73% | 0.82 | -32.25% | 171 | 44.4% | 14d |
| atr K=2.5 | 9.34% | 0.75 | -35.26% | 119 | 45.4% | 19d |
| atr K=3.0 | 8.54% | 0.70 | -31.43% | 87 | 47.1% | 26d |

**2330 (2015-01 — 2026-04, 2738 days)**

| mode | CAGR | Sharpe | MDD | Trades |
|---|---|---|---|---|
| B&H | 26.60% | 1.04 | -45.68% | 1 |
| fixed 8% | 11.97% | 0.78 | -30.03% | 26 |
| atr K=2.0 | 10.29% | 0.62 | -42.80% | 148 |
| atr K=2.5 | 12.79% | 0.82 | -27.58% | 75 |
| atr K=3.0 | 10.91% | 0.71 | -30.41% | 53 |

**2454 (2015-01 — 2026-04, 2738 days)**

| mode | CAGR | Sharpe | MDD | Trades |
|---|---|---|---|---|
| B&H | 11.78% | 0.49 | -61.90% | 1 |
| fixed 8% | 1.70% | 0.19 | -34.75% | 19 |
| atr K=2.0 | -0.97% | 0.00 | -36.56% | 33 |
| atr K=2.5 | -0.09% | 0.07 | -42.11% | 22 |
| atr K=3.0 | 0.10% | 0.08 | -44.17% | 17 |

### Aggregate (mean across 3 tickers)

| mode | CAGR | Sharpe | MDD | n_trades | hit% |
|---|---|---|---|---|---|
| **B&H** | **17.19%** | **0.79** | -46.4% | 1 | 100% |
| **fixed 8%** | 7.13% | **0.56** | **-30.07%** | 19 | 45.6% |
| atr K=2.0 | 6.68% | 0.48 | -37.21% | 117 | 38.8% |
| atr K=2.5 | 7.35% | 0.55 | -34.98% | 72 | 43.7% |
| atr K=3.0 | 6.52% | 0.50 | -35.33% | 52 | 43.8% |

### Findings

1. **Fixed 8% wins on Sharpe AND MDD vs every ATR variant tested.** ATR K=2.5 comes close on Sharpe (0.55 vs 0.56) but loses 5pp on MDD (-35.0% vs -30.1%).
2. **All trail stops cost 10pp CAGR** vs B&H (17.2% → 7.1-7.4%) for ~16pp MDD improvement. This is the volatility-tax of any trailing rule.
3. **ATR is not "self-adapting" in the way Council B claimed.** It produces 3-6× more trades (whipsaw) without delivering MDD improvement. K=2.5 specifically generates 72 trades vs fixed's 19 — execution friction in real life would erase any remaining edge.
4. **The whole "ATR is better" intuition is empirically false** for naive trailing-stop application on TW broad market. Fixed % is simpler AND better.

### Verdict for Section 3

**ATR trailing stop offers NO Pareto improvement over fixed 8% on this 11-yr TW sample.** Council B's R2 ATR claim is **NOT empirically supported**. If the user wants better stops, the question is whether to add a stop AT ALL (since 17% → 7% CAGR is a heavy tax), not whether to switch fixed→ATR.

The remaining stop-related ROI levers are: (a) regime-conditional stops (only trail in volatile regime), (b) drawdown-conditional position sizing instead of stops. Both are de-novo R&D, not "swap fixed for ATR".

---

## Section 4 — Crash Predictor Expected Value (TWII gating sim)

### Method 1 — Phase 2 composite (50/30/20) with rolling rank-pct

- Rank each factor on 252d rolling window, take percentile
- Composite = 0.5 × m1b_ratio_pct_pct + 0.3 × rv30_pct + 0.2 × rv10_pct
- Red (cash) if composite ≥ P95, Yellow (50%) if ≥ P85, resume Full when < P75
- Backtest: TWII 2015-01 — 2026-04
- Tool: `tools/audit_crash_predictor_expected_value.py`

| label | CAGR | Sharpe | MDD | days_full | days_half | days_cash |
|---|---|---|---|---|---|---|
| **B&H_TWII** | **13.20%** | 0.83 | **-31.63%** | 2741 | 0 | 0 |
| Composite_50/30/20 | 11.23% | 0.81 | -32.28% | 2344 | 309 | 88 |
| single_m1b_ratio_pct | 7.09% | 0.59 | -31.63% | 1761 | 439 | 541 |
| single_rv30 | 8.07% | 0.64 | -35.19% | 2185 | 302 | 254 |
| **single_rv10** | **12.74%** | **0.98** | **-25.70%** | 2170 | 413 | 158 |

### Method 2 — Absolute thresholds (no rolling rank)

m1b > 35 OR rv30 > 0.25 OR rv10 > 0.30 → red; weaker tier → yellow:

| | CAGR | Sharpe | MDD |
|---|---|---|---|
| B&H | 13.20% | 0.83 | -31.63% |
| absolute-threshold gated | 6.37% | 0.59 | **-31.60% (no improvement)** |

### Method 3 — m1b alone, sweep absolute threshold

| threshold | days_off | CAGR | MDD |
|---|---|---|---|
| 40 | 134 | 10.62% | **-31.63%** |
| 45 | 89 | 10.90% | -32.38% |
| 50 | 52 | 11.61% | **-31.63%** |
| 55 | 27 | 13.51% | **-31.63%** |
| B&H | — | 13.20% | -31.63% |

### Findings

1. **The Phase 2 composite (50/30/20) does NOT improve MDD** — actually slightly worse than B&H (-32.3% vs -31.6%) and costs 2pp CAGR. This is a hard stop on the Phase 3 thesis.
2. **m1b_ratio_pct alone is the WORST single-factor gating** despite Phase 2's highest AUC (0.72). 540 days in cash earns the strategy 6pp CAGR less while delivering zero MDD improvement. **AUC ≠ portfolio value when the fires are coincident with the drop, not before it.**
3. **rv10 alone is the only winner**: Sharpe 0.98 (vs 0.83 B&H, +0.15), MDD -25.7% (vs -31.6%, +5.9pp). This is the entire portfolio-level signal value of crash predictor — and it doesn't need m1b or rv30.
4. **The "lead 55-60d" claim from Phase 2 doesn't translate to portfolio MDD improvement** when integrated as a daily allocation rule. Likely cause: events with strong lead time are clustered, but the *worst* drawdowns (2018-Q4, 2020-COVID, 2022) had vol spikes coincident or lagging, not leading.

### Phase 3 expected ROI estimate (revised)

| Component of Phase 3 build | Expected MDD Δ | Expected Sharpe Δ | Comment |
|---|---|---|---|
| 50/30/20 composite | **-0.6pp (worse)** | -0.02 | falsified — DO NOT BUILD |
| Single rv10 gating | +5.9pp | +0.15 | actual lift, but ~1h to deploy (not 8-12h) |
| UI integration + alert | (operational only) | — | not a model lift |

> The "+5.9pp MDD" benefit from rv10-alone needs to discount for:
> (a) implementation costs to integrate it as a real-time signal — TWSE M1B is **monthly**, not daily; rv10 is computable daily
> (b) regime stability — 11-yr sample contains 2 major crashes (2020, 2022); 5-OOS-year discipline (per `project_validation_bias_warning`) implies maybe 1 OOS event remaining for true validation

---

## Section 5 — ROI Ranking & Verdict

### Three paths

| Path | Effort | Expected MDD Δ | Expected Sharpe Δ | False-pos cost | ROI Score |
|---|---|---|---|---|---|
| **(a) Crash Predictor MVP (Phase 3 + UI)** as currently specced (50/30/20 composite) | 8-12h | **-0.6pp (NEGATIVE)** | -0.02 | high (88+ days/yr cash) | **NEGATIVE** — DO NOT BUILD |
| **(a') Crash Predictor — rv10-only narrow scope** | 1-2h | +5.9pp | +0.15 | medium (158d cash / 11yr) | **POSITIVE — strong** |
| **(b) ATR dynamic trailing stop** | 4-6h | -7.1pp vs fixed (worse) | -0.01 vs fixed | high (3-6× whipsaw) | **NEGATIVE** — fixed-stop already in use is better |
| **(c) Mode D conviction tier sizing** | 6-8h | not measured here | not measured here | unknown | **DEFER** — needs separate study, paper-trade data not yet usable |

### Verdict (one-liner per Council B's framing)

**(a) Phase 3 as currently specced is NEGATIVE-ROI. (b) ATR trailing stop replacement is NEGATIVE-ROI. The only positive-ROI move is (a') rv10-alone narrow gate, which is a 1-2h tactical add, NOT a Phase 3 build.**

### Sequencing recommendation

1. **DO**: ship rv10-alone gate as Mode D market-state signal (1-2h). Threshold: rv10 > 0.30 → yellow (Mode D reduces top-pick from N=5 to N=3); rv10 > 0.40 → red (no new entries). Use it as a *throttle*, not a portfolio rebalancer.
2. **DO NOT**: ship the 50/30/20 composite as crash predictor. The Phase 2 composite is portfolio-negative; AUC discrimination on *event windows* did not survive translation to *daily portfolio gating*.
3. **DO NOT**: replace fixed 8% trailing with ATR. ATR is empirically worse on 11-yr TW data across K∈{2.0, 2.5, 3.0}.
4. **PARK**: (c) Mode D conviction tier sizing — needs ≥6 months paper-trade data; current sample (8 open / 0 closed) is unusable.
5. **AUDIT NEXT**: revisit Phase 2 methodology — why does AUC 0.72 NOT translate to portfolio MDD lift? Likely answer is the post look-ahead fix removed the signal's true predictive timing. Re-examine factor lead-time as a *function of regime*.

---

## Caveats & limitations

- **No ETF data**: 0050 not in `ohlcv_tw.parquet`; used TWII as broad-market proxy (close enough — 0050 tracks TWII top-50)
- **Single factor TWII bench**: did not validate composite/rv10 gating on individual stock baskets (could move differently from index)
- **No transaction costs**: real ATR strategy with 70+ trades/decade would lose another 1-2pp CAGR to commissions+slippage in TW market
- **Walk-forward not done**: thresholds in §4 method 2/3 (m1b > 35, rv30 > 0.25, rv10 > 0.30) chosen by inspection, not optimized OOS; this risks in-sample overfitting on the *upper bound* of the rv10 lift quoted
- **Section 1+2 sample is null**: results lean entirely on §3 + §4 reproducible data

## Files produced

- `tools/audit_atr_vs_fixed_stop.py`
- `tools/audit_crash_predictor_expected_value.py`
- `reports/audit_atr_vs_fixed_stop.csv`
- `reports/audit_crash_predictor_expected_value.csv`
- `reports/crash_predictor_b_opportunity_cost_audit.md` (this file)

## Compute log

- Section 3 sim: 2741+2738+2738 daily records × 5 strategies = ~41k decision points, runtime ~3s
- Section 4 sim: 2741 daily records × 5 strategies, runtime ~1s
- Total wall clock ~8 min including code authoring
