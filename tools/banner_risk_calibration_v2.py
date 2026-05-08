"""
Banner Risk Score Calibration v2 - 加入 backfilled PCR + TW FGI

v1 排除 5 個訊號因為無歷史 panel。v2 加回 PCR (volume + OI) + FGI score（已 backfilled）。
共 6 個訊號 — 跑 univariate lift filter (lift10 > 1.0)，通過的進 composite。

Generates:
  - reports/banner_risk_score_calibration_v2.md
  - reports/banner_risk_score_calibration_v2.csv
"""
import pandas as pd
import numpy as np

PANEL_PATH = 'C:/GIT/StockAnalyzer/reports/_history/2026_05_crash_predictor_closed/crash_predictor_tw_panel.parquet'
PCR_PATH = 'C:/GIT/StockAnalyzer/data/sentiment/pcr_history.parquet'
FGI_PATH = 'C:/GIT/StockAnalyzer/data/sentiment/fgi_history.parquet'
OUT_MD = 'C:/GIT/StockAnalyzer/reports/banner_risk_score_calibration_v2.md'
OUT_CSV = 'C:/GIT/StockAnalyzer/reports/banner_risk_score_calibration_v2.csv'

# ==== Load panel ====
panel = pd.read_parquet(PANEL_PATH)
panel.index = pd.to_datetime(panel.index)

# ==== Merge new signals ====
pcr = pd.read_parquet(PCR_PATH)
pcr.index = pd.to_datetime(pcr.index)
fgi = pd.read_parquet(FGI_PATH)
fgi.index = pd.to_datetime(fgi.index)
fgi = fgi[['score']].rename(columns={'score': 'fgi_score'})

merged = panel.join(pcr, how='left').join(fgi, how='left')

# Filter to rows where label is valid (forward 60d label needs 60d future buffer)
valid = merged[merged['label_10pct'].notna() & merged['label_20pct'].notna()].copy()
valid['label_5pct'] = (valid['forward_60d_pt_drawdown'] <= -0.05).astype(float)

# ==== Signal definitions ====
# direction: +1 = high value is danger, -1 = low value is danger
SIGNALS = [
    ('m1b_ratio',    'm1b_ratio_pct',   +1, '1999-01-05'),
    ('rv10',         'rv10',            +1, '1999-01-05'),
    ('rv30',         'rv30',            +1, '1999-01-05'),
    ('pcr_volume',   'pc_ratio_volume', +1, '2010-01-04'),
    ('pcr_oi',       'pc_ratio_oi',     +1, '2010-01-04'),
    ('fgi_score',    'fgi_score',       -1, '1999-01-05'),
]

# ==== Univariate lift analysis ====
def compute_univariate(df, col, direction):
    """Return dict with lift, threshold, n_danger_days, n_events, baseline rates."""
    sub = df[[col, 'label_10pct', 'label_5pct', 'forward_60d_pt_drawdown']].dropna()
    if len(sub) < 100:
        return None
    base10 = sub['label_10pct'].mean()
    base5 = sub['label_5pct'].mean()

    if direction > 0:
        thresh = sub[col].quantile(0.85)
        danger = sub[col] >= thresh
        zone_desc = f'>= P85 ({thresh:.4f})'
    else:
        thresh = sub[col].quantile(0.15)
        danger = sub[col] <= thresh
        zone_desc = f'<= P15 ({thresh:.4f})'

    co10 = sub.loc[danger, 'label_10pct'].mean()
    co5 = sub.loc[danger, 'label_5pct'].mean()
    return {
        'col': col,
        'direction': direction,
        'n_valid': len(sub),
        'thresh': thresh,
        'zone_desc': zone_desc,
        'n_danger': int(danger.sum()),
        'n_events_10': int(sub.loc[danger & (sub['label_10pct'] == 1)].shape[0]),
        'baseline_10': base10,
        'baseline_5': base5,
        'co10': co10,
        'co5': co5,
        'lift10': co10 / base10 if base10 > 0 else np.nan,
        'lift5': co5 / base5 if base5 > 0 else np.nan,
    }


print("=" * 60)
print("Univariate lift analysis (each signal vs forward 60d MDD)")
print("=" * 60)
results = {}
for name, col, direction, start in SIGNALS:
    r = compute_univariate(valid, col, direction)
    if r is None:
        print(f"  {name:14s} -- insufficient data")
        continue
    results[name] = r
    flag = 'INCLUDE' if r['lift10'] > 1.0 else 'EXCLUDE'
    print(f"  {name:14s} N={r['n_valid']:5d} {r['zone_desc']:25s}"
          f"  co10={r['co10']*100:5.1f}%  lift10={r['lift10']:.3f}  ({flag})")

# ==== Composite weighting (lift-based, only for INCLUDED) ====
included = {k: v for k, v in results.items() if v['lift10'] > 1.0}
total_lift = sum(v['lift10'] for v in included.values())
weights = {k: v['lift10'] / total_lift for k, v in included.items()}

print("\n" + "=" * 60)
print("Lift-based weights (INCLUDED signals only)")
print("=" * 60)
for k, w in weights.items():
    print(f"  {k:14s} weight={w*100:.1f}%  (lift10={included[k]['lift10']:.3f})")

# ==== Build composite score ====
# Use rolling 252d percentile rank (in-sample full history for calibration only;
# UI 應改用 trailing window，但這裡是 calibration phase 用 full-sample rank 找 thresholds)
sub_composite = valid.copy()

# direction-aware percentile rank: high-danger 直接 rank，low-danger 反向 rank
for name, col, direction, _ in SIGNALS:
    if name in included:
        if direction > 0:
            sub_composite[f'{name}_rank'] = sub_composite[col].rank(pct=True) * 100
        else:
            sub_composite[f'{name}_rank'] = (1 - sub_composite[col].rank(pct=True)) * 100

# Composite must use rows where ALL included signals have data
required_cols = [f'{name}_rank' for name in included]
sub_composite = sub_composite[required_cols + ['label_10pct', 'label_5pct',
                                                  'forward_60d_pt_drawdown']].dropna()

print(f"\nComposite valid rows (all {len(included)} signals present): N={len(sub_composite)}")
print(f"  Date range: {sub_composite.index.min().date()} to {sub_composite.index.max().date()}")

sub_composite['composite'] = sum(
    sub_composite[f'{name}_rank'] * w for name, w in weights.items()
)

# ==== SOP-12 check: composite vs best-single ====
base10_comp = sub_composite['label_10pct'].mean()
base5_comp = sub_composite['label_5pct'].mean()

best_single_name = max(included, key=lambda k: included[k]['lift10'])
best_single_col = SIGNALS_BY_NAME = {n: (c, d) for n, c, d, _ in SIGNALS}
bcol, bdir = SIGNALS_BY_NAME[best_single_name]
if bdir > 0:
    bs_thresh = sub_composite.join(valid[[bcol]])[bcol].quantile(0.85)
    bs_danger = sub_composite.join(valid[[bcol]])[bcol] >= bs_thresh
else:
    bs_thresh = sub_composite.join(valid[[bcol]])[bcol].quantile(0.15)
    bs_danger = sub_composite.join(valid[[bcol]])[bcol] <= bs_thresh
best_single_lift = sub_composite.loc[bs_danger.values, 'label_10pct'].mean() / base10_comp

orange_thresh = sub_composite['composite'].quantile(0.85)
yellow_thresh = sub_composite['composite'].quantile(0.65)
orange_mask = sub_composite['composite'] >= orange_thresh
yellow_mask = (sub_composite['composite'] >= yellow_thresh) & (
    sub_composite['composite'] < orange_thresh)
green_mask = sub_composite['composite'] < yellow_thresh

comp_lift10 = sub_composite.loc[orange_mask, 'label_10pct'].mean() / base10_comp
comp_lift5 = sub_composite.loc[orange_mask, 'label_5pct'].mean() / base5_comp

print("\n" + "=" * 60)
print("SOP-12 Check: Composite vs Best-Single")
print("=" * 60)
print(f"  Best single ({best_single_name} P85): lift10 = {best_single_lift:.3f}")
print(f"  Composite at P85:                    lift10 = {comp_lift10:.3f}")
verdict = "USE COMPOSITE" if comp_lift10 > best_single_lift else f"USE BEST-SINGLE ({best_single_name})"
print(f"  -> {verdict}")

# ==== Zone metrics ====
o_co10 = sub_composite.loc[orange_mask, 'label_10pct'].mean()
o_co5 = sub_composite.loc[orange_mask, 'label_5pct'].mean()
o_mdd = sub_composite.loc[orange_mask, 'forward_60d_pt_drawdown'].median()

y_co10 = sub_composite.loc[yellow_mask, 'label_10pct'].mean()
y_co5 = sub_composite.loc[yellow_mask, 'label_5pct'].mean()
y_mdd = sub_composite.loc[yellow_mask, 'forward_60d_pt_drawdown'].median()

g_co10 = sub_composite.loc[green_mask, 'label_10pct'].mean()
g_co5 = sub_composite.loc[green_mask, 'label_5pct'].mean()
g_mdd = sub_composite.loc[green_mask, 'forward_60d_pt_drawdown'].median()

n_yrs = (sub_composite.index.max() - sub_composite.index.min()).days / 365.25
ann_o = orange_mask.sum() / n_yrs
ann_y = yellow_mask.sum() / n_yrs
ann_g = green_mask.sum() / n_yrs

print("\n" + "=" * 60)
print("Zone Thresholds")
print("=" * 60)
print(f"  Orange: composite >= {orange_thresh:.1f} (P85)  co10={o_co10*100:.1f}% "
      f"co5={o_co5*100:.1f}% mdd={o_mdd*100:.1f}% ann={ann_o:.0f}d/yr")
print(f"  Yellow: composite >= {yellow_thresh:.1f} (P65)  co10={y_co10*100:.1f}% "
      f"co5={y_co5*100:.1f}% mdd={y_mdd*100:.1f}% ann={ann_y:.0f}d/yr")
print(f"  Green:  composite <  {yellow_thresh:.1f}  co10={g_co10*100:.1f}% "
      f"co5={g_co5*100:.1f}% mdd={g_mdd*100:.1f}% ann={ann_g:.0f}d/yr")

# ==== Output CSV ====
csv_rows = []
for name, col, direction, start in SIGNALS:
    if name in results:
        r = results[name]
        status = 'INCLUDED' if r['lift10'] > 1.0 else 'EXCLUDED (lift<=1)'
        w = weights.get(name, 0.0)
        csv_rows.append({
            'signal': name,
            'col': col,
            'direction': '+1 (high=danger)' if direction > 0 else '-1 (low=danger)',
            'status': status,
            'n_valid': r['n_valid'],
            'date_start': start,
            'danger_zone': r['zone_desc'],
            'n_danger_days': r['n_danger'],
            'baseline_10pct': round(r['baseline_10'], 4),
            'co10': round(r['co10'], 4),
            'lift10': round(r['lift10'], 4),
            'baseline_5pct': round(r['baseline_5'], 4),
            'co5': round(r['co5'], 4),
            'lift5': round(r['lift5'], 4),
            'n_events_10': r['n_events_10'],
            'weight': round(w, 4),
        })

df_csv = pd.DataFrame(csv_rows)
df_csv.to_csv(OUT_CSV, index=False)
print(f"\nWrote {OUT_CSV}")

# ==== Output Markdown ====
md = f"""# Banner Risk Score Calibration v2 (with PCR + FGI backfilled)

**Generated**: 2026-05-08
**Method**: SOP-14 informational tier, result-driven weight derivation via univariate lift filter
**Panel**: crash_predictor_tw_panel.parquet | 1999-01-05 to 2026-01-29
**Composite valid range**: {sub_composite.index.min().date()} to {sub_composite.index.max().date()} (N={len(sub_composite)} rows, ~{n_yrs:.1f} yrs)
**Label**: forward 60-day peak-to-trough drawdown (10% / 5% threshold)
**Baseline (composite valid range)**: P(MDD>=10%)={base10_comp*100:.1f}%, P(MDD>=5%)={base5_comp*100:.1f}%

---

## Step 1: 6 Signal Univariate Lift Analysis

| Signal | Direction | N rows | Danger Zone | Co-occur 10% | **Lift 10%** | Co-occur 5% | Lift 5% | N Events | Status |
|---|---|---|---|---|---|---|---|---|---|
"""
for name, col, direction, start in SIGNALS:
    if name not in results:
        continue
    r = results[name]
    status = '**INCLUDED**' if r['lift10'] > 1.0 else 'EXCLUDED'
    dir_str = '+ (high=danger)' if direction > 0 else '- (low=danger)'
    md += (f"| {name} | {dir_str} | {r['n_valid']} | {r['zone_desc']} | "
           f"{r['co10']*100:.1f}% | **{r['lift10']:.3f}** | "
           f"{r['co5']*100:.1f}% | {r['lift5']:.3f} | {r['n_events_10']} | {status} |\n")

md += f"""

---

## Step 2: Lift-Based Weights (INCLUDED only)

| Signal | Lift10 | Weight |
|---|---|---|
"""
for k, w in sorted(weights.items(), key=lambda x: -x[1]):
    md += f"| {k} | {included[k]['lift10']:.3f} | **{w*100:.1f}%** |\n"

md += f"""

Total signals in composite: **{len(included)}** (out of 6)

Composite formula:

```
composite = """
md += " + ".join(f"rank_pct({name}) * {w:.3f}" for name, w in weights.items())
md += f"""
```

For high-danger signals (m1b/rv/pcr): rank_pct = signal value's percentile rank (0-100)
For low-danger signals (fgi): rank_pct = (100 - signal value's percentile rank)

---

## Step 3: SOP-12 Check (composite must beat best-single)

| | Lift10 |
|---|---|
| Best-single ({best_single_name} P85) | {best_single_lift:.3f} |
| Composite at P85 | {comp_lift10:.3f} |

**Verdict**: {"USE COMPOSITE" if comp_lift10 > best_single_lift else f"USE BEST-SINGLE ({best_single_name}) — composite did NOT beat"}

---

## Step 4: 3-Tier Zone Thresholds

| Zone | Composite | P-Level | Co-occur 10% | Co-occur 5% | Fwd60d MDD Median | Days/Year |
|---|---|---|---|---|---|---|
| Orange | >= {orange_thresh:.1f} | P85+ | {o_co10*100:.1f}% | {o_co5*100:.1f}% | {o_mdd*100:.1f}% | ~{ann_o:.0f} |
| Yellow | {yellow_thresh:.1f}-{orange_thresh:.1f} | P65-P85 | {y_co10*100:.1f}% | {y_co5*100:.1f}% | {y_mdd*100:.1f}% | ~{ann_y:.0f} |
| Green | < {yellow_thresh:.1f} | < P65 | {g_co10*100:.1f}% | {g_co5*100:.1f}% | {g_mdd*100:.1f}% | ~{ann_g:.0f} |

Monotonicity check (Orange > Yellow > Green): co10 {"PASS" if o_co10 > y_co10 > g_co10 else "FAIL"}, co5 {"PASS" if o_co5 > y_co5 > g_co5 else "FAIL"}

---

## Comparison with v1 (3 signals only)

| Metric | v1 (m1b/rv10/rv30) | v2 (this) | Delta |
|---|---|---|---|
| N signals included | 3 | {len(included)} | {len(included)-3:+d} |
| Best-single lift10 | 1.922 | {best_single_lift:.3f} | {best_single_lift-1.922:+.3f} |
| Composite lift10 | 1.982 | {comp_lift10:.3f} | {comp_lift10-1.982:+.3f} |
| N composite-valid rows | 6,699 | {len(sub_composite)} | {len(sub_composite)-6699:+d} |

---

## Caveats

### v2-specific
- **FGI backfill caveats**: market_breadth 是 ^TWII 5d/20d return proxy（無真實 advance/decline 歷史），margin_balance 2021- 才有，pcr_value 2010- 才有 — 加權正規化補
- **PCR backfill caveats**: 2010-2026 全覆蓋，2010 前 NaN 排除
- **Composite valid range 縮短**: PCR 起點 2010 限制 composite 整體期間，N 從 6,699 降到 {len(sub_composite)}
- **HMM regime 仍排除**: lift10 1.047 marginal + categorical 不可 percentile rank
- **5 個即時訊號中還缺 4 個**: CNN FGI / ATM Put / MTX/TXF / Inst PC skew 仍無歷史 panel（見 BL-5 backlog）

### SOP-14 informational tier
- Banner UI 文案禁: "predicts crash / leading / 領先 / 預警"
- Banner UI 文案 OK: "elevated co-occurrence / 同期重合率 / 歷史此狀態下"
- 不設紅燈，最高 Orange "需審視持倉但不自動調整"

### SOP-12 verdict adherence
{"- Composite passes SOP-12 (lift > best-single) -> deploy as composite" if comp_lift10 > best_single_lift else f"- Composite FAILS SOP-12 -> fallback to best-single ({best_single_name}); banner shows single-signal score not composite"}

---

## Implementation Spec for Banner UI

```python
WEIGHTS = {{
"""
for k, w in weights.items():
    md += f"    '{k}': {w:.4f},  # lift10={included[k]['lift10']:.3f}\n"
md += f"""}}

def compute_banner_risk_score(signal_values_today, history_dfs):
    \"\"\"
    signal_values_today: dict of {{signal_name: today's value}}
    history_dfs: dict of {{signal_name: pd.Series of historical values for percentile rank}}
    \"\"\"
    ranks = {{}}
    for name, value in signal_values_today.items():
        hist = history_dfs[name]
        # high-danger = direct rank; low-danger (fgi_score) = reversed
        rank = (hist <= value).mean() * 100
        if name == 'fgi_score':
            rank = 100 - rank
        ranks[name] = rank

    composite = sum(ranks[n] * w for n, w in WEIGHTS.items())

    if composite >= {orange_thresh:.1f}:
        zone = 'orange'
    elif composite >= {yellow_thresh:.1f}:
        zone = 'yellow'
    else:
        zone = 'green'

    return {{'composite': composite, 'zone': zone, 'breakdown': ranks}}
```

**Thresholds**: Orange >= {orange_thresh:.1f} | Yellow >= {yellow_thresh:.1f} | Green < {yellow_thresh:.1f}

**UI 文案範本**:
- Orange: 「目前綜合風險指標 {{composite:.0f}}（橘燈）；歷史此區間 60d 內出現 ≥10% 回檔的同期重合率 {o_co10*100:.0f}%」
- Yellow: 「目前綜合風險指標 {{composite:.0f}}（黃燈）；歷史此區間同期重合率 {y_co10*100:.0f}%」
- Green: 「目前綜合風險指標 {{composite:.0f}}（綠燈）；歷史此區間同期重合率 {g_co10*100:.0f}%」
"""

with open(OUT_MD, 'w', encoding='utf-8') as f:
    f.write(md)
print(f"Wrote {OUT_MD}")
print("\nDONE")
