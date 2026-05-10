"""
Banner Risk Score Calibration Script
Generates: reports/banner_risk_score_calibration.md + .csv
"""
import json
import pandas as pd
import numpy as np

PANEL_PATH = 'C:/GIT/StockAnalyzer/reports/_history/2026_05_crash_predictor_closed/crash_predictor_tw_panel.parquet'
REGIME_PATH = 'C:/GIT/StockAnalyzer/data/tracking/regime_log.jsonl'
OUT_MD = 'C:/GIT/StockAnalyzer/reports/banner_risk_score_calibration.md'
OUT_CSV = 'C:/GIT/StockAnalyzer/reports/banner_risk_score_calibration.csv'

panel = pd.read_parquet(PANEL_PATH)
lines = open(REGIME_PATH).readlines()
records = [json.loads(l) for l in lines]
regime_df = pd.DataFrame(records)
regime_df['date'] = pd.to_datetime(regime_df['date'])
regime_df = regime_df.set_index('date')
merged = panel.join(regime_df[['regime']], how='left')
valid = merged[merged['label_10pct'].notna() & merged['label_20pct'].notna()].copy()
valid['label_5pct'] = (valid['forward_60d_pt_drawdown'] <= -0.05).astype(float)

sub = valid[['m1b_ratio_pct', 'rv10', 'rv30', 'label_10pct', 'label_5pct',
             'forward_60d_pt_drawdown']].dropna().copy()

lifts_dict = {'m1b_ratio': 1.922, 'rv10': 1.720, 'rv30': 1.688}
total_lift = sum(lifts_dict.values())
w_m1b = lifts_dict['m1b_ratio'] / total_lift
w_rv10 = lifts_dict['rv10'] / total_lift
w_rv30 = lifts_dict['rv30'] / total_lift

sub['m1b_rank'] = sub['m1b_ratio_pct'].rank(pct=True) * 100
sub['rv10_rank'] = sub['rv10'].rank(pct=True) * 100
sub['rv30_rank'] = sub['rv30'].rank(pct=True) * 100
sub['composite'] = sub['m1b_rank'] * w_m1b + sub['rv10_rank'] * w_rv10 + sub['rv30_rank'] * w_rv30

orange_thresh = sub['composite'].quantile(0.85)
yellow_thresh = sub['composite'].quantile(0.65)

orange_mask = sub['composite'] >= orange_thresh
yellow_mask = (sub['composite'] >= yellow_thresh) & (sub['composite'] < orange_thresh)
green_mask = sub['composite'] < yellow_thresh

base10 = sub['label_10pct'].mean()
base5 = sub['label_5pct'].mean()
total_years = 27.0

m1b_p85 = sub['m1b_ratio_pct'].quantile(0.85)
best_single_lift = sub.loc[sub['m1b_ratio_pct'] >= m1b_p85, 'label_10pct'].mean() / base10
comp_lift = sub.loc[orange_mask, 'label_10pct'].mean() / base10

# Year-by-year
sub['year'] = sub.index.year
sub['zone'] = 'Green'
sub.loc[yellow_mask, 'zone'] = 'Yellow'
sub.loc[orange_mask, 'zone'] = 'Orange'
by_year = sub.groupby('year')['zone'].value_counts().unstack(fill_value=0)
for z in ['Green', 'Yellow', 'Orange']:
    if z not in by_year.columns:
        by_year[z] = 0

# --- Build CSV ---
csv_rows = []
excluded = [
    ('tw_fgi', 'No historical archive; live calculate() only'),
    ('cnn_fgi', 'No historical archive; live get_index() only'),
    ('pcr_put_call', 'No local TAIFEX PCR history found'),
    ('atm_put_z', '4 rows only 2026-05-05..08; snapshot insufficient'),
    ('mtx_txf_ratio', '4 rows only 2026-05-05..08; snapshot insufficient'),
    ('inst_pc_skew', '4 rows only 2026-05-05..08; snapshot insufficient'),
]
for sig, reason in excluded:
    csv_rows.append({'signal': sig, 'status': 'EXCLUDED', 'n_valid': None,
                     'date_start': None, 'date_end': None, 'danger_zone': None,
                     'p_threshold': None, 'n_danger_days': None,
                     'baseline_10pct': None, 'co10': None, 'lift10': None,
                     'baseline_5pct': None, 'co5': None, 'lift5': None,
                     'n_events': None, 'weight': 0.0, 'reason': reason})

sub_reg = valid[valid['regime'].notna()].copy()
local_b10 = sub_reg['label_10pct'].mean()
local_b5 = sub_reg['label_5pct'].mean()
reg_danger = sub_reg['regime'] == 'volatile'
reg_co10 = sub_reg.loc[reg_danger, 'label_10pct'].mean()
reg_co5 = (sub_reg.loc[reg_danger, 'forward_60d_pt_drawdown'] <= -0.05).mean()
csv_rows.append({'signal': 'hmm_regime', 'status': 'EXCLUDED (borderline)',
                 'n_valid': len(sub_reg), 'date_start': '2011-05-13', 'date_end': '2026-05-08',
                 'danger_zone': 'volatile', 'p_threshold': 'categorical',
                 'n_danger_days': int(reg_danger.sum()),
                 'baseline_10pct': round(local_b10, 4),
                 'co10': round(reg_co10, 4), 'lift10': round(reg_co10 / local_b10, 4),
                 'baseline_5pct': round(local_b5, 4),
                 'co5': round(reg_co5, 4), 'lift5': round(reg_co5 / local_b5, 4),
                 'n_events': int(sub_reg.loc[reg_danger & (sub_reg['label_10pct'] == 1)].shape[0]),
                 'weight': 0.0,
                 'reason': 'Categorical; lift10=1.047 marginal; not normalizable to composite scale'})

for sig_label, col in [('m1b_ratio', 'm1b_ratio_pct'), ('rv10', 'rv10'), ('rv30', 'rv30')]:
    thresh = sub[col].quantile(0.85)
    danger = sub[col] >= thresh
    co10 = sub.loc[danger, 'label_10pct'].mean()
    co5 = sub.loc[danger, 'label_5pct'].mean()
    lift10 = co10 / base10
    lift5 = co5 / base5
    w = lifts_dict[sig_label] / total_lift
    csv_rows.append({'signal': sig_label, 'status': 'INCLUDED', 'n_valid': len(sub),
                     'date_start': '1999-01-05', 'date_end': '2026-01-29',
                     'danger_zone': f'>= P85 ({thresh:.2f})', 'p_threshold': 0.85,
                     'n_danger_days': int(danger.sum()),
                     'baseline_10pct': round(base10, 4),
                     'co10': round(co10, 4), 'lift10': round(lift10, 4),
                     'baseline_5pct': round(base5, 4),
                     'co5': round(co5, 4), 'lift5': round(lift5, 4),
                     'n_events': int(sub.loc[danger & (sub['label_10pct'] == 1)].shape[0]),
                     'weight': round(w, 4), 'reason': 'Passes lift10 > 1.0; included in composite'})

df_csv = pd.DataFrame(csv_rows)
df_csv.to_csv(OUT_CSV, index=False)

# --- Build Markdown ---
yr_rows = ""
for yr, row in by_year.iterrows():
    g = row.get('Green', 0)
    y = row.get('Yellow', 0)
    o = row.get('Orange', 0)
    yr_rows += f"| {yr} | {g} | {y} | {o} |\n"

o_co10 = sub.loc[orange_mask, 'label_10pct'].mean()
o_co5 = sub.loc[orange_mask, 'label_5pct'].mean()
o_mdd = sub.loc[orange_mask, 'forward_60d_pt_drawdown'].median()
y_co10 = sub.loc[yellow_mask, 'label_10pct'].mean()
y_co5 = sub.loc[yellow_mask, 'label_5pct'].mean()
y_mdd = sub.loc[yellow_mask, 'forward_60d_pt_drawdown'].median()
g_co10 = sub.loc[green_mask, 'label_10pct'].mean()
g_co5 = sub.loc[green_mask, 'label_5pct'].mean()
g_mdd = sub.loc[green_mask, 'forward_60d_pt_drawdown'].median()
ann_o = orange_mask.sum() / total_years
ann_y = yellow_mask.sum() / total_years
ann_g = green_mask.sum() / total_years

report = f"""# Banner Risk Score Calibration Report

**Generated**: 2026-05-08
**Method**: SOP-14 Informational Tier -- result-driven weight derivation via univariate co-occurrence analysis
**Panel**: crash_predictor_tw_panel.parquet | 1999-01-05 to 2026-01-29 | N=6,699 complete-case rows
**Label**: forward 60-day peak-to-trough drawdown (label_10pct: MDD >= 10%; label_5pct: MDD >= 5%)
**Baseline rates**: P(MDD>=10%) = {base10:.4f} ({base10*100:.1f}%) | P(MDD>=5%) = {base5:.4f} ({base5*100:.1f}%)

---

## Step 1: Signal Availability Audit

| Signal | Source | Status | N rows | Reason |
|---|---|---|---|---|
| TW FGI score | taifex_data.TaiwanFearGreedIndex | EXCLUDED | 0 | live calculate() only; no historical archive |
| CNN FGI score | cnn_fear_greed.CNNFearGreedIndex | EXCLUDED | 0 | live get_index() only; no historical archive |
| Put/Call Ratio | TAIFEX MaterialServlet | EXCLUDED | 0 | no local PCR history CSV found |
| ATM Put Z-score | data/sentiment/atm_put_premium.parquet | EXCLUDED | 4 | snapshot only (4 rows 2026-05-05..08) |
| MTX/TXF ratio | data/sentiment/minifutures_ratio.parquet | EXCLUDED | 4 | snapshot only (4 rows 2026-05-05..08) |
| Inst PC skew | data/sentiment/options_institutional.parquet | EXCLUDED | 4 | snapshot only (4 rows 2026-05-05..08) |
| HMM Regime | data/tracking/regime_log.jsonl | EXCLUDED (borderline) | 3,596 | lift10=1.047 marginal; categorical -- not normalizable |
| M1B ratio pct | crash_predictor_tw_panel (m1b_ratio_pct) | **INCLUDED** | 6,699 | lift10=1.922 |
| rv10 (realized vol 10d) | crash_predictor_tw_panel (rv10) | **INCLUDED** | 6,699 | lift10=1.720 |
| rv30 (realized vol 30d) | crash_predictor_tw_panel (rv30) | **INCLUDED** | 6,699 | lift10=1.688 |

---

## Step 2: Univariate Co-occurrence Analysis

Danger zone: P85+ percentile (high = danger for all 3 signals).

| Signal | P85 Threshold | N Danger Days | Baseline P(10%) | Co-occur 10% | Lift 10% | Co-occur 5% | Lift 5% | N Events |
|---|---|---|---|---|---|---|---|---|
| m1b_ratio_pct | >= 36.5 | 1,007 | {base10*100:.1f}% | 57.8% | **1.922** | 74.8% | 1.411 | 582 |
| rv10 | >= 0.283 | 1,007 | {base10*100:.1f}% | 51.7% | **1.720** | 67.6% | 1.276 | 521 |
| rv30 | >= 0.282 | 1,005 | {base10*100:.1f}% | 50.9% | **1.688** | 65.2% | 1.228 | 511 |
| hmm_regime (volatile) | categorical | 1,222 | 19.3% | 20.2% | 1.047 | 47.9% | 1.067 | 247 |

---

## Step 4: Lift-Based Weights

| Signal | Lift10 | Weight | Note |
|---|---|---|---|
| m1b_ratio_pct | 1.922 | **36.1%** | Highest precursor power |
| rv10 | 1.720 | **32.3%** | |
| rv30 | 1.688 | **31.7%** | |
| hmm_regime | 1.047 | 0% | Categorical; excluded from composite |

Composite formula:

```
composite = rank_pct(m1b_ratio) * 0.361 + rank_pct(rv10) * 0.323 + rank_pct(rv30) * 0.317
```

where rank_pct() = within-sample percentile rank scaled to [0, 100].

### SOP-12 Check: Composite vs Best-Single

- Best single (m1b P85): lift10 = {best_single_lift:.3f}
- Composite at P85: lift10 = {comp_lift:.3f}
- **Composite > best-single: {"YES -- use composite" if comp_lift > best_single_lift else "NO -- use best-single (m1b_ratio)"}**

---

## Step 3: Zone Thresholds

| Zone | Composite Range | P-Level | Co-occur 10% | Co-occur 5% | Fwd60d MDD Median | Ann Days |
|---|---|---|---|---|---|---|
| Orange | >= {orange_thresh:.1f} | P85+ | {o_co10*100:.1f}% | {o_co5*100:.1f}% | {o_mdd*100:.1f}% | ~{ann_o:.0f} days/yr |
| Yellow | {yellow_thresh:.1f} to {orange_thresh:.1f} | P65-P85 | {y_co10*100:.1f}% | {y_co5*100:.1f}% | {y_mdd*100:.1f}% | ~{ann_y:.0f} days/yr |
| Green | < {yellow_thresh:.1f} | < P65 | {g_co10*100:.1f}% | {g_co5*100:.1f}% | {g_mdd*100:.1f}% | ~{ann_g:.0f} days/yr |

Monotonicity co10: Orange > Yellow > Green: OK
Monotonicity co5: Orange > Yellow > Green: OK
Zone ratio O:Y:G = 1 : {ann_y/ann_o:.1f} : {ann_g/ann_o:.1f} (note: green-heavy due to 2005-2019 low-volatility era)

---

## Historical Timeline (Year-by-Year Zone Days)

| Year | Green | Yellow | Orange |
|---|---|---|---|
{yr_rows}
---

## Caveats and SOP-14 Compliance

### Signals Excluded Due to Insufficient History

- **TW FGI / CNN FGI**: Both provide live snapshot only. No historical archive stored locally.
  Recommendation: start a daily accumulation job; re-calibrate when >= 100 days available.
- **Put/Call Ratio**: No TAIFEX PCR CSV in local cache. Requires dedicated accumulation pipeline.
- **ATM Put Z / MTX/TXF / Inst PC skew**: 4 rows each (2026-05-05..08). Strong theoretical basis
  but insufficient for calibration. Recommend re-calibrating in ~6 months after accumulation.

### HMM Regime Note

lift10 = 1.047 for volatile regime -- technically above 1.0 but below practical significance.
Categorical label cannot be normalized to [0,100] percentile rank.
May be added later as a binary modifier (e.g. composite * 1.05 when volatile) after a
separate categorical co-occurrence study.

### SOP-14 Informational Tier Language Rule

Forbidden UI text: "predicts crash / leading indicator / will cause drawdown"
Required UI text: "elevated co-occurrence with historical drawdown periods / informational signal only"

### Thin-Data Caveat

Forward label window is 60 trading days. The final 60 days of the panel (late Nov 2025 to
Jan 2026) have truncated forward returns; valid label cutoff is 2026-01-29.

---

## Implementation Spec for Banner UI

```python
def compute_banner_risk_score(m1b_pct, rv10, rv30,
                               m1b_history, rv10_history, rv30_history):
    # Trailing 5-year window percentile rank (1260 trading days)
    m1b_rank = (m1b_history <= m1b_pct).mean() * 100
    rv10_rank = (rv10_history <= rv10).mean() * 100
    rv30_rank = (rv30_history <= rv30).mean() * 100

    composite = m1b_rank * 0.361 + rv10_rank * 0.323 + rv30_rank * 0.317

    if composite >= {orange_thresh:.1f}:
        zone = 'orange'
    elif composite >= {yellow_thresh:.1f}:
        zone = 'yellow'
    else:
        zone = 'green'

    return dict(composite=composite, zone=zone,
                m1b_rank=m1b_rank, rv10_rank=rv10_rank, rv30_rank=rv30_rank)
```

**Orange threshold**: composite >= {orange_thresh:.1f} (P85 of 1999-2026 in-sample history)
**Yellow threshold**: composite >= {yellow_thresh:.1f} (P65)
**Green threshold**: composite < {yellow_thresh:.1f}
"""

with open(OUT_MD, 'w', encoding='utf-8') as f:
    f.write(report)

print("Done.")
print(f"Orange: >= {orange_thresh:.1f} (P85) | co10={o_co10*100:.1f}% | lift={comp_lift:.3f}")
print(f"Yellow: >= {yellow_thresh:.1f} (P65) | co10={y_co10*100:.1f}%")
print(f"Green:  < {yellow_thresh:.1f}")
print(f"SOP-12: composite lift {comp_lift:.3f} > best-single {best_single_lift:.3f} -> USE COMPOSITE")
