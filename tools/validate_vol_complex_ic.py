"""
validate_vol_complex_ic.py -- Vol Complex 4 訊號 IC validation vs ^TWII

對齊 system3_move_check.py 的 SOP-12 + SOP-14 流程:

  Per-signal univariate Spearman IC vs fwd 5/10/20d MDD on ^TWII
  + conditional hit lift on light thresholds (yellow/orange/red)
  + composite lit_count regime breakdown

US close at T → TW reacts at T+1 (close-close)。對齊用 date shift + 1。

Output:
  reports/vol_complex_ic_validation.md
  reports/vol_complex_ic_validation.csv

Verdict 規則 (SOP-12 三 gate):
  A. |IC| >= 0.10 + p < 0.05 across 5/10/20d
  B. Decile spread sign consistent
  C. |Q10-Q1 median| >= 2pp
  ==
  3 gate pass → PASS
  2 → MARGINAL
  <2 → FAIL (但若高閾值 lift >= 2.5x 升 SOP-14 informational)

Usage:
  python tools/validate_vol_complex_ic.py
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

VC_PATH = REPO / "data" / "sentiment" / "vol_complex_history.parquet"
OUT_MD = REPO / "reports" / "vol_complex_ic_validation.md"
OUT_CSV = REPO / "reports" / "vol_complex_ic_validation.csv"

KNOWN_SHOCKS = {
    "COVID 2020": pd.Timestamp("2020-02-20"),
    "Russia/UA 2022": pd.Timestamp("2022-02-24"),
    "SVB 2023": pd.Timestamp("2023-03-09"),
    "Aug 2024 Yen carry": pd.Timestamp("2024-08-05"),
    "Trump tariff 2025": pd.Timestamp("2025-03-03"),
}

FEATURES = [
    ('vix_vix3m_ratio', '0.95', '1.00', '1.05'),
    ('vvix',            '100',  '110',  '130'),
    ('skew',            '140',  '145',  '155'),
    ('ovx',             '40',   '50',   '80'),
]


def load_twii() -> pd.Series:
    import yfinance as yf
    df = yf.Ticker('^TWII').history(start='2007-01-01', auto_adjust=False)
    df.index = pd.to_datetime(df.index.date)
    return df['Close'].sort_index().astype(float)


def compute_fwd_mdd(close: pd.Series, horizon: int) -> pd.Series:
    arr = close.values
    n = len(arr)
    out = np.full(n, np.nan)
    for i in range(n - horizon):
        seg = arr[i + 1: i + horizon + 1]
        if len(seg) == 0:
            continue
        out[i] = (seg.min() - arr[i]) / arr[i]
    return pd.Series(out, index=close.index)


def build_aligned_panel() -> pd.DataFrame:
    vc = pd.read_parquet(VC_PATH)
    vc['date'] = pd.to_datetime(vc['date'])
    vc = vc.set_index('date').sort_index()

    twii = load_twii()
    twii.index = pd.to_datetime(twii.index)

    # US close at T → TW reacts at T+1; shift TWII back 1 day so it aligns with US date
    # Equivalent: outcome at date D = TWII drawdown from D+1 (TW next session)
    fwd5 = compute_fwd_mdd(twii, 5).shift(-1) * 100
    fwd10 = compute_fwd_mdd(twii, 10).shift(-1) * 100
    fwd20 = compute_fwd_mdd(twii, 20).shift(-1) * 100

    out = vc.copy()
    out['fwd_5d_mdd'] = fwd5
    out['fwd_10d_mdd'] = fwd10
    out['fwd_20d_mdd'] = fwd20
    out = out.dropna(subset=['fwd_5d_mdd', 'fwd_10d_mdd', 'fwd_20d_mdd'])
    return out


def spearman_ic(feat: pd.Series, outcome: pd.Series):
    df = pd.concat([feat, outcome], axis=1).dropna()
    if len(df) < 30:
        return np.nan, np.nan, len(df)
    rho, p = stats.spearmanr(df.iloc[:, 0], df.iloc[:, 1])
    return rho, p, len(df)


def decile_spread(feat: pd.Series, outcome: pd.Series):
    df = pd.concat([feat, outcome], axis=1).dropna()
    df.columns = ['f', 'o']
    if len(df) < 100:
        return {}
    df['dec'] = pd.qcut(df['f'].rank(method='first'), 10, labels=False) + 1
    medians = df.groupby('dec')['o'].median()
    return {
        'q1_median': medians.iloc[0],
        'q10_median': medians.iloc[-1],
        'spread_med': medians.iloc[-1] - medians.iloc[0],
    }


def evaluate_feature(panel: pd.DataFrame, col: str) -> dict:
    f = panel[col]
    out = {'feature': col, 'horizons': {}}
    for h in [5, 10, 20]:
        outcome = panel[f'fwd_{h}d_mdd']
        rho, p, n = spearman_ic(f, outcome)
        spread = decile_spread(f, outcome)
        out['horizons'][h] = {'ic': rho, 'pvalue': p, 'n': n, **spread}
    return out


def conditional_lift(panel: pd.DataFrame, col: str, thresholds: list) -> list[dict]:
    base_n = len(panel)
    base_med20 = panel['fwd_20d_mdd'].median()
    base_hit20 = (panel['fwd_20d_mdd'] <= -10).mean() * 100
    rows = [{
        'cond': 'baseline',
        'n': base_n, 'pct': 100.0,
        'fwd5_med': panel['fwd_5d_mdd'].median(),
        'fwd20_med': base_med20,
        'hit20_neg10pct': base_hit20,
        'lift': 1.0,
    }]
    for t in thresholds:
        sub = panel[panel[col] >= float(t)]
        n = len(sub)
        if n < 5:
            continue
        med20 = sub['fwd_20d_mdd'].median()
        hit20 = (sub['fwd_20d_mdd'] <= -10).mean() * 100
        rows.append({
            'cond': f'{col} >= {t}',
            'n': n, 'pct': n / base_n * 100,
            'fwd5_med': sub['fwd_5d_mdd'].median(),
            'fwd20_med': med20,
            'hit20_neg10pct': hit20,
            'lift': hit20 / base_hit20 if base_hit20 > 0 else 0,
        })
    return rows


def composite_regime_stats(panel: pd.DataFrame) -> list[dict]:
    rows = []
    base_med20 = panel['fwd_20d_mdd'].median()
    base_hit20 = (panel['fwd_20d_mdd'] <= -10).mean() * 100
    for lit_target in [0, 1, 2, 3, 4]:
        sub = panel[panel['lit_count'] == lit_target]
        n = len(sub)
        if n < 5:
            continue
        med20 = sub['fwd_20d_mdd'].median()
        hit20 = (sub['fwd_20d_mdd'] <= -10).mean() * 100
        rows.append({
            'lit_count': lit_target,
            'n': n, 'pct': n / len(panel) * 100,
            'fwd5_med': sub['fwd_5d_mdd'].median(),
            'fwd20_med': med20,
            'hit20_neg10pct': hit20,
            'lift': hit20 / base_hit20 if base_hit20 > 0 else 0,
        })
    rows.insert(0, {
        'lit_count': 'baseline',
        'n': len(panel), 'pct': 100.0,
        'fwd5_med': panel['fwd_5d_mdd'].median(),
        'fwd20_med': base_med20,
        'hit20_neg10pct': base_hit20,
        'lift': 1.0,
    })
    return rows


def event_study(panel: pd.DataFrame) -> list[dict]:
    rows = []
    for label, sd in KNOWN_SHOCKS.items():
        if sd not in panel.index:
            try:
                sd = panel.index[panel.index.searchsorted(sd)]
            except IndexError:
                continue
        # Look back 60 trading days for any "lit >= 2" alert
        idx = panel.index.get_loc(sd)
        lookback_start = max(0, idx - 60)
        win = panel.iloc[lookback_start: idx + 1]
        first_alert = win[win['lit_count'] >= 2]
        if len(first_alert) > 0:
            alert_d = first_alert.index[0]
            lead = idx - panel.index.get_loc(alert_d)
            lit_at = int(first_alert.iloc[0]['lit_count'])
        else:
            alert_d, lead, lit_at = None, None, 0
        rows.append({
            'shock': label,
            'date': sd.strftime('%Y-%m-%d'),
            'first_alert_date': alert_d.strftime('%Y-%m-%d') if alert_d else 'no alert',
            'lead_trading_days': lead,
            'lit_at_alert': lit_at,
            'lit_at_shock': int(panel.loc[sd, 'lit_count']),
            'fwd_5d_mdd': panel.loc[sd, 'fwd_5d_mdd'],
            'fwd_20d_mdd': panel.loc[sd, 'fwd_20d_mdd'],
        })
    return rows


def sop12_verdict(eval_result: dict) -> tuple[str, list[str]]:
    notes = []
    gate_a = gate_b = gate_c = True
    for h in [5, 10, 20]:
        r = eval_result['horizons'][h]
        ic = r['ic']
        p = r['pvalue']
        if pd.isna(ic) or abs(ic) < 0.10 or p > 0.05:
            gate_a = False
            notes.append(f"  Gate A FAIL @ {h}d: |IC|={abs(ic):.3f} p={p:.4f}")
    spreads = [eval_result['horizons'][h].get('spread_med', 0) for h in [5, 10, 20]]
    if not (all(s < 0 for s in spreads) or all(s > 0 for s in spreads)):
        gate_b = False
        notes.append(f"  Gate B FAIL: spread signs inconsistent {[f'{s:+.2f}' for s in spreads]}")
    for h in [5, 10, 20]:
        spread = eval_result['horizons'][h].get('spread_med')
        if spread is None or pd.isna(spread) or abs(spread) < 2.0:
            gate_c = False
            notes.append(f"  Gate C FAIL @ {h}d: |spread|={abs(spread or 0):.2f}pp")

    n_pass = int(gate_a) + int(gate_b) + int(gate_c)
    if n_pass == 3:
        return 'PASS', notes
    if n_pass == 2:
        return 'MARGINAL', notes
    return 'FAIL', notes


def main():
    panel = build_aligned_panel()
    print(f"Aligned panel: {len(panel)} rows {panel.index.min().date()} ~ {panel.index.max().date()}")

    results = {}
    for col, _, _, _ in FEATURES:
        results[col] = evaluate_feature(panel, col)

    # Per-feature verdicts
    verdicts = {}
    for col, _, _, _ in FEATURES:
        v, notes = sop12_verdict(results[col])
        # Conditional lift might upgrade FAIL → MARGINAL
        thresholds = [t for _, *t in [next(f for f in FEATURES if f[0] == col)]][0]
        cond = conditional_lift(panel, col, list(thresholds))
        red_row = next((r for r in cond if r['cond'] == f"{col} >= {thresholds[2]}"), None)
        if v == 'FAIL' and red_row and red_row['lift'] >= 2.5:
            v = 'MARGINAL (informational)'
            notes.append(f"  UPGRADE: high-threshold lift={red_row['lift']:.2f}x → SOP-14 tier")
        verdicts[col] = (v, notes, cond)

    composite = composite_regime_stats(panel)
    events = event_study(panel)

    # ---------- print ----------
    print("\n=== IC Summary ===")
    for col, _, _, _ in FEATURES:
        for h in [5, 10, 20]:
            r = results[col]['horizons'][h]
            print(f"  {col:18s} fwd{h:>3}d  IC={r['ic']:+.3f} p={r['pvalue']:.4f} "
                  f"spread={r.get('spread_med', 0):+.2f}pp n={r['n']}")
        v, _, _ = verdicts[col]
        print(f"  → Verdict {col:18s}: {v}\n")

    print("=== Composite regime (lit_count) ===")
    for r in composite:
        print(f"  lit={r['lit_count']!s:8s} n={r['n']:5d} ({r['pct']:5.1f}%) "
              f"fwd5_med={r['fwd5_med']:+.2f}% fwd20_med={r['fwd20_med']:+.2f}% "
              f"hit20<=-10%={r['hit20_neg10pct']:5.1f}% lift={r['lift']:.2f}x")

    print("\n=== Event study ===")
    for e in events:
        print(f"  {e['shock']:25s} {e['date']}  first_alert={e['first_alert_date']:12s} "
              f"lead={e['lead_trading_days']!s:5s} lit_at_alert={e['lit_at_alert']} "
              f"lit_at_shock={e['lit_at_shock']} fwd20={e['fwd_20d_mdd']:+.2f}%")

    write_report(panel, results, verdicts, composite, events)
    write_csv(results, verdicts)


def write_report(panel, results, verdicts, composite, events):
    today = datetime.now().strftime('%Y-%m-%d')
    md = [
        f"# Vol Complex 4 訊號 IC Validation vs ^TWII",
        f"",
        f"Date: {today}  Panel: {panel.index.min().date()} ~ {panel.index.max().date()} ({len(panel)} rows)",
        f"Outcome: ^TWII fwd 5/10/20d max drawdown (close-to-min), US T → TW T+1 對齊",
        f"",
        f"## Verdict 摘要 (SOP-12 3-gate)",
        f"",
        f"| Signal | Verdict | Best |IC| |",
        f"|---|---|---|",
    ]
    for col, _, _, _ in FEATURES:
        v, _, _ = verdicts[col]
        best_ic = max((abs(results[col]['horizons'][h]['ic']) for h in [5, 10, 20]
                       if not pd.isna(results[col]['horizons'][h]['ic'])), default=0)
        md.append(f"| `{col}` | {v} | {best_ic:.3f} |")

    md.append("")
    md.append("## Per-feature univariate IC")
    md.append("")
    md.append("| feature | horizon | n | IC | p-value | Q1 med MDD | Q10 med MDD | Spread (pp) |")
    md.append("|---|---|---:|---:|---:|---:|---:|---:|")
    for col, _, _, _ in FEATURES:
        for h in [5, 10, 20]:
            r = results[col]['horizons'][h]
            md.append(f"| {col} | {h}d | {r['n']} | {r['ic']:+.3f} | {r['pvalue']:.4f} | "
                      f"{r.get('q1_median', float('nan')):+.2f}% | {r.get('q10_median', float('nan')):+.2f}% | "
                      f"{r.get('spread_med', float('nan')):+.2f} |")

    md.append("")
    md.append("## Per-feature conditional lift (threshold-based)")
    md.append("")
    for col, _, _, _ in FEATURES:
        _, notes, cond = verdicts[col]
        md.append(f"### `{col}`")
        md.append("")
        md.append("| Condition | n | % days | fwd5 med MDD | fwd20 med MDD | hit fwd20 <= -10% | lift_20d |")
        md.append("|---|---:|---:|---:|---:|---:|---:|")
        for r in cond:
            md.append(f"| {r['cond']} | {r['n']} | {r['pct']:.1f}% | {r['fwd5_med']:+.2f}% | "
                      f"{r['fwd20_med']:+.2f}% | {r['hit20_neg10pct']:.1f}% | {r['lift']:.2f}x |")
        if notes:
            md.append("")
            md.append("Gate failures / upgrade notes:")
            for n in notes:
                md.append(f"- {n.strip()}")
        md.append("")

    md.append("## Composite regime: lit_count vs fwd MDD")
    md.append("")
    md.append("| lit_count | n | % days | fwd5 med | fwd20 med | hit fwd20 <= -10% | lift |")
    md.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in composite:
        md.append(f"| {r['lit_count']} | {r['n']} | {r['pct']:.1f}% | {r['fwd5_med']:+.2f}% | "
                  f"{r['fwd20_med']:+.2f}% | {r['hit20_neg10pct']:.1f}% | {r['lift']:.2f}x |")

    md.append("")
    md.append("## Event study (5 known shocks)")
    md.append("")
    md.append("| Shock | Date | First lit>=2 | Lead (TD) | Lit at alert | Lit at shock | fwd5 MDD | fwd20 MDD |")
    md.append("|---|---|---|---:|---:|---:|---:|---:|")
    for e in events:
        md.append(f"| {e['shock']} | {e['date']} | {e['first_alert_date']} | "
                  f"{e['lead_trading_days'] if e['lead_trading_days'] is not None else 'n/a'} | "
                  f"{e['lit_at_alert']} | {e['lit_at_shock']} | "
                  f"{e['fwd_5d_mdd']:+.2f}% | {e['fwd_20d_mdd']:+.2f}% |")

    md.append("")
    md.append("## Recommendation")
    md.append("")
    n_marginal_or_pass = sum(1 for col, _, _, _ in FEATURES
                              if verdicts[col][0] in ('PASS', 'MARGINAL', 'MARGINAL (informational)'))
    if n_marginal_or_pass == 0:
        md.append("**All 4 signals FAIL SOP-12 on TW.** US-derived thresholds do not transfer cleanly.")
        md.append("Keep as informational tile in macro_dashboard but DO NOT integrate into composite risk_score.")
    else:
        md.append(f"**{n_marginal_or_pass}/4 signals MARGINAL/PASS** — see per-feature lift tables above.")
        md.append("Consider promoting MARGINAL signals to system3_daily_check stage with SOP-14 informational push.")
        md.append("Composite lit_count regime: if lift >= 2x at lit>=2, framework's '2 lights = reduce 30%' threshold has some TW support.")
    md.append("")
    md.append("**Caveat**: 對齊 US T → TW T+1 是 close-close 假設，盤中即時反應未捕捉；")
    md.append("Aug 2024 yen carry / SVB 等隔夜跳空無法看出真實 lead time。")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(md), encoding='utf-8')
    print(f"\n[OK] Report -> {OUT_MD}")


def write_csv(results, verdicts):
    rows = []
    for col, _, _, _ in FEATURES:
        for h in [5, 10, 20]:
            r = results[col]['horizons'][h]
            rows.append({
                'feature': col, 'horizon_d': h,
                'n': r['n'], 'ic': r['ic'], 'pvalue': r['pvalue'],
                'q1_median': r.get('q1_median'),
                'q10_median': r.get('q10_median'),
                'spread_med': r.get('spread_med'),
                'verdict': verdicts[col][0],
            })
    pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
    print(f"[OK] CSV -> {OUT_CSV}")


if __name__ == '__main__':
    main()
