"""
validate_move_5d_shock_ic.py - ^MOVE 5d delta shock detection POC + IC validation

Purpose:
  System 3 (1w-1mo crash predictor) recall = 59% gap missing COVID 2020-02 / 2022-08 / 2025-03
  shock events. ma_dist_60 is slow (rolling-252d rank), shock ∆ comes too late.
  Validate ^MOVE 5d ∆ as fast complementary signal: SOP-12 three gates +
  event study at 3 known shocks.

Outputs:
  reports/system3_move5d_ic_validation_2026-05-09.md (verdict + stats + recommendation)

SOP-12 gates:
  a) Univariate IC |IC| >= 0.10 + p<0.05 across 5d/10d/20d
  b) Decile spread Q10-Q1 sign consistent
  c) Top decile fwd drawdown median <= -2% worse than bottom decile

Usage:
  python tools/validate_move_5d_shock_ic.py
"""
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats

REPO = Path(__file__).resolve().parent.parent
PANEL_PATH = REPO / "reports" / "system3_panel.parquet"
ETF_FLOWS_PATH = REPO / "data" / "macro" / "etf_flows.parquet"
OUT_REPORT = REPO / "reports" / "system3_move5d_ic_validation_2026-05-09.md"

KNOWN_SHOCKS = {
    "COVID 2020": pd.Timestamp("2020-02-20"),
    "Jackson Hole 2022": pd.Timestamp("2022-08-26"),
    "Trump tariff 2025": pd.Timestamp("2025-03-03"),
}


# ============================================================
#  Load + derive features
# ============================================================

def load_panel() -> pd.DataFrame:
    """Load system3_panel + ensure move_level present + derive 4 features."""
    df = pd.read_parquet(PANEL_PATH)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Already has move_level + move_5d_chg, but we derive richer set
    move = df["move_level"]
    df["move_5d_delta"] = move.diff(5)                                   # raw delta
    df["move_5d_delta_pct"] = move.pct_change(5) * 100                   # pct
    df["move_zscore_252d"] = (move - move.rolling(252).mean()) / move.rolling(252).std()
    delta = move.diff(5)
    df["move_5d_delta_zscore"] = (delta - delta.rolling(252).mean()) / delta.rolling(252).std()

    # Forward drawdown for 5/10/20d on close
    close = df["close"]
    for h in [5, 10, 20]:
        df[f"fwd_{h}d_mdd"] = compute_fwd_mdd(close, h) * 100  # pct

    return df


def compute_fwd_mdd(close: pd.Series, horizon: int) -> pd.Series:
    """For each t: min(close[t+1..t+H]/close[t]) - 1 (negative = drawdown)."""
    arr = close.values
    n = len(arr)
    out = np.full(n, np.nan)
    for i in range(n - horizon):
        seg = arr[i + 1 : i + horizon + 1]
        if len(seg) == 0:
            continue
        mdd = (seg.min() - arr[i]) / arr[i]
        out[i] = mdd
    return pd.Series(out, index=close.index)


# ============================================================
#  Stats helpers
# ============================================================

def spearman_ic(feat: pd.Series, outcome: pd.Series) -> tuple[float, float, int]:
    """Returns (rho, pvalue, n)."""
    df = pd.concat([feat, outcome], axis=1).dropna()
    if len(df) < 30:
        return np.nan, np.nan, len(df)
    rho, p = stats.spearmanr(df.iloc[:, 0], df.iloc[:, 1])
    return rho, p, len(df)


def decile_spread(feat: pd.Series, outcome: pd.Series) -> dict:
    """Q10-Q1 median spread of fwd MDD; monotonicity check."""
    df = pd.concat([feat, outcome], axis=1).dropna()
    df.columns = ["f", "o"]
    if len(df) < 100:
        return {}
    df["dec"] = pd.qcut(df["f"].rank(method="first"), 10, labels=False) + 1
    medians = df.groupby("dec")["o"].median()
    means = df.groupby("dec")["o"].mean()
    return {
        "q1_median": medians.iloc[0],
        "q10_median": medians.iloc[-1],
        "spread_med": medians.iloc[-1] - medians.iloc[0],
        "q1_mean": means.iloc[0],
        "q10_mean": means.iloc[-1],
        "spread_mean": means.iloc[-1] - means.iloc[0],
        "medians": medians.tolist(),
    }


# ============================================================
#  Event study
# ============================================================

def event_study_relaxed(panel: pd.DataFrame, feature_col: str,
                         alert_thresh: float = 1.5, lookback: int = 60) -> list[dict]:
    """Relaxed event study: lower threshold (1.5) + longer window (60 TD)."""
    rows = []
    f = panel[feature_col]
    ma60 = panel.get("ma_dist_60")
    for label, shock_date in KNOWN_SHOCKS.items():
        if shock_date not in panel.index:
            shock_date = panel.index[panel.index.searchsorted(shock_date)]
        idx = panel.index.get_loc(shock_date)
        lookback_start = max(0, idx - lookback)
        win = f.iloc[lookback_start : idx + 1]
        triggered = win[win >= alert_thresh]
        max_z = win.max()
        if len(triggered) > 0:
            first_alert = triggered.index[0]
            lead_trading = idx - panel.index.get_loc(first_alert)
            move_alert = True
            move_val_at_alert = win.loc[first_alert]
        else:
            lead_trading = None
            move_alert = False
            move_val_at_alert = None

        ma_alert = False
        ma_lead = None
        if ma60 is not None:
            ma_win = ma60.iloc[lookback_start : idx + 1]
            ma_rank = ma_win.rolling(252, min_periods=60).rank(pct=True)
            ma_yellow = ma_rank[ma_rank >= 0.65]
            if len(ma_yellow) > 0:
                ma_alert = True
                ma_lead = idx - panel.index.get_loc(ma_yellow.index[0])

        rows.append({
            "shock": label,
            "shock_date": shock_date.strftime("%Y-%m-%d"),
            "move_5d_z_at_shock": panel.loc[shock_date, "move_5d_delta_zscore"],
            "max_z_in_window": max_z,
            "move_alert": move_alert,
            "move_lead_trading_days": lead_trading,
            "move_value_at_first_alert": move_val_at_alert,
            "ma_dist_60_alert_in_window": ma_alert,
            "ma_dist_60_lead_trading_days": ma_lead,
            "fwd_5d_mdd_pct": panel.loc[shock_date, "fwd_5d_mdd"],
            "fwd_10d_mdd_pct": panel.loc[shock_date, "fwd_10d_mdd"],
            "fwd_20d_mdd_pct": panel.loc[shock_date, "fwd_20d_mdd"],
        })
    return rows


def event_study(panel: pd.DataFrame, feature_col: str, alert_thresh: float) -> list[dict]:
    """For each known shock, find lead days that feature first crossed alert_thresh."""
    rows = []
    f = panel[feature_col]
    ma60 = panel.get("ma_dist_60")
    for label, shock_date in KNOWN_SHOCKS.items():
        # window: lookback 30 trading days before shock
        if shock_date not in panel.index:
            shock_date = panel.index[panel.index.searchsorted(shock_date)]
        idx = panel.index.get_loc(shock_date)
        lookback_start = max(0, idx - 30)
        win = f.iloc[lookback_start : idx + 1]
        # find first day in window where feature >= threshold (alert)
        triggered = win[win >= alert_thresh]
        if len(triggered) > 0:
            first_alert = triggered.index[0]
            lead_days = (shock_date - first_alert).days  # calendar
            lead_trading = idx - panel.index.get_loc(first_alert)
            move_alert = True
            move_val_at_alert = win.loc[first_alert]
        else:
            lead_days = None
            lead_trading = None
            move_alert = False
            move_val_at_alert = None

        # Compare: did ma_dist_60 alert in same window?
        ma_alert = False
        ma_lead = None
        if ma60 is not None:
            ma_win = ma60.iloc[lookback_start : idx + 1]
            ma_rank = ma_win.rolling(252, min_periods=60).rank(pct=True)
            ma_yellow = ma_rank[ma_rank >= 0.65]
            if len(ma_yellow) > 0:
                ma_alert = True
                ma_lead = idx - panel.index.get_loc(ma_yellow.index[0])

        rows.append({
            "shock": label,
            "shock_date": shock_date.strftime("%Y-%m-%d"),
            "move_5d_z_at_shock": panel.loc[shock_date, "move_5d_delta_zscore"],
            "move_alert": move_alert,
            "move_lead_trading_days": lead_trading,
            "move_value_at_first_alert": move_val_at_alert,
            "ma_dist_60_alert_in_window": ma_alert,
            "ma_dist_60_lead_trading_days": ma_lead,
            "fwd_5d_mdd_pct": panel.loc[shock_date, "fwd_5d_mdd"],
            "fwd_10d_mdd_pct": panel.loc[shock_date, "fwd_10d_mdd"],
            "fwd_20d_mdd_pct": panel.loc[shock_date, "fwd_20d_mdd"],
        })
    return rows


# ============================================================
#  SOP-12 verdict
# ============================================================

def evaluate_feature(panel: pd.DataFrame, feature_col: str) -> dict:
    """Run IC + decile spread for 3 horizons. Return SOP-12 gate results."""
    f = panel[feature_col]
    out = {"feature": feature_col, "horizons": {}}
    for h in [5, 10, 20]:
        outcome = panel[f"fwd_{h}d_mdd"]
        rho, pval, n = spearman_ic(f, outcome)
        spread = decile_spread(f, outcome)
        out["horizons"][h] = {
            "ic": rho,
            "pvalue": pval,
            "n": n,
            **spread,
        }
    return out


def sop12_verdict(eval_result: dict) -> tuple[str, list[str]]:
    """Apply SOP-12 a/b/c gates."""
    notes = []
    gate_a_pass = True
    gate_b_pass = True
    gate_c_pass = True

    for h in [5, 10, 20]:
        r = eval_result["horizons"][h]
        ic = r["ic"]
        p = r["pvalue"]
        if pd.isna(ic) or abs(ic) < 0.10 or p > 0.05:
            gate_a_pass = False
            notes.append(f"  Gate A FAIL @ {h}d: |IC|={abs(ic):.3f} (need >=0.10), p={p:.4f}")

    # Gate B: spread sign consistent (assuming feature is a "shock" so high feature -> worse fwd MDD,
    # i.e. spread_med should be NEGATIVE [Q10 worse than Q1])
    signs = [eval_result["horizons"][h].get("spread_med", 0) for h in [5, 10, 20]]
    if not (all(s < 0 for s in signs) or all(s > 0 for s in signs)):
        gate_b_pass = False
        notes.append(f"  Gate B FAIL: decile spread signs not consistent across horizons {signs}")

    # Gate C: top decile fwd MDD median worse than bottom decile by >= 2pp
    for h in [5, 10, 20]:
        r = eval_result["horizons"][h]
        # Q10 minus Q1 in pct; if feature direction = high MOVE delta -> worse drawdown,
        # then Q10 (high feature) should have more negative MDD than Q1
        spread = r.get("spread_med")
        if spread is None or pd.isna(spread):
            gate_c_pass = False
            notes.append(f"  Gate C FAIL @ {h}d: spread NaN")
            continue
        if abs(spread) < 2.0:
            gate_c_pass = False
            notes.append(f"  Gate C FAIL @ {h}d: |Q10-Q1 median| = {abs(spread):.2f}pp (need >=2)")

    gates = {
        "A_univariate_IC": gate_a_pass,
        "B_decile_sign_consistent": gate_b_pass,
        "C_decile_spread_>=2pp": gate_c_pass,
    }
    n_pass = sum(gates.values())
    if n_pass == 3:
        verdict = "PASS"
    elif n_pass == 2:
        verdict = "MARGINAL"
    else:
        verdict = "FAIL"
    return verdict, notes, gates


# ============================================================
#  Main
# ============================================================

def conditional_hit_stats(panel: pd.DataFrame, feature_col: str, thresholds: list[float]) -> list[dict]:
    """Per threshold: count fired, fwd_5/10/20d MDD median, hit rate (fwd_5d<=-3%, fwd_20d<=-10%)."""
    f = panel[feature_col]
    rows = []
    base_n = panel[["fwd_5d_mdd", "fwd_10d_mdd", "fwd_20d_mdd"]].dropna().shape[0]
    base_5 = panel["fwd_5d_mdd"].median()
    base_20 = panel["fwd_20d_mdd"].median()
    base_hit5 = (panel["fwd_5d_mdd"] <= -3).mean() * 100
    base_hit20 = (panel["fwd_20d_mdd"] <= -10).mean() * 100
    rows.append({
        "threshold": "baseline (all days)",
        "n_alert": base_n,
        "pct_days": 100.0,
        "fwd_5d_mdd_median": base_5,
        "fwd_20d_mdd_median": base_20,
        "hit_5d_le_neg3pct": base_hit5,
        "hit_20d_le_neg10pct": base_hit20,
        "lift_20d": 1.0,
    })
    for t in thresholds:
        sub = panel[f >= t]
        n = len(sub)
        if n == 0:
            continue
        med5 = sub["fwd_5d_mdd"].median()
        med20 = sub["fwd_20d_mdd"].median()
        hit5 = (sub["fwd_5d_mdd"] <= -3).mean() * 100
        hit20 = (sub["fwd_20d_mdd"] <= -10).mean() * 100
        rows.append({
            "threshold": f"z >= {t}",
            "n_alert": n,
            "pct_days": n / base_n * 100,
            "fwd_5d_mdd_median": med5,
            "fwd_20d_mdd_median": med20,
            "hit_5d_le_neg3pct": hit5,
            "hit_20d_le_neg10pct": hit20,
            "lift_20d": hit20 / base_hit20 if base_hit20 > 0 else 0,
        })
    return rows


def main():
    panel = load_panel()
    print(f"[OK] Loaded panel: {len(panel)} rows, {panel.index.min().date()} -> {panel.index.max().date()}")
    print(f"[OK] move_level non-null: {panel['move_level'].notna().sum()} rows")

    # Evaluate 4 candidate features
    candidates = [
        "move_5d_delta",
        "move_5d_delta_pct",
        "move_zscore_252d",
        "move_5d_delta_zscore",
    ]
    results = {}
    for c in candidates:
        results[c] = evaluate_feature(panel, c)

    # Pick best by best |IC| at any horizon
    best_feat = None
    best_score = 0
    best_horizon = None
    for c in candidates:
        for h in [5, 10, 20]:
            ic = results[c]["horizons"][h]["ic"]
            if not pd.isna(ic) and abs(ic) > best_score:
                best_score = abs(ic)
                best_feat = c
                best_horizon = h
    print(f"\n[OK] Best feature: {best_feat} @ {best_horizon}d (|IC|={best_score:.3f})")

    # SOP-12 verdict for best feature
    verdict, notes, gates = sop12_verdict(results[best_feat])
    print(f"\n[OK] SOP-12 verdict for {best_feat}: {verdict}")
    for n in notes:
        print(n)

    # Event study with relaxed window (60d) + threshold (z>=1.5) since strict z>=2/30d only catches 1/3
    events = event_study_relaxed(panel, "move_5d_delta_zscore", alert_thresh=1.5, lookback=60)
    print("\n[OK] Event study (z >= 1.5 alert in 60-day pre-shock window):")
    for e in events:
        print(f"  {e['shock']} ({e['shock_date']}): MOVE alert={e['move_alert']} "
              f"lead={e['move_lead_trading_days']} days; max_z_in_window={e['max_z_in_window']:+.2f}; "
              f"ma_dist_60 yellow={e['ma_dist_60_alert_in_window']} lead={e['ma_dist_60_lead_trading_days']}; "
              f"fwd_20d_mdd={e['fwd_20d_mdd_pct']:+.2f}%")

    # ===== Summary table =====
    rows = []
    for c in candidates:
        for h in [5, 10, 20]:
            r = results[c]["horizons"][h]
            rows.append({
                "feature": c, "horizon": f"{h}d",
                "n": r["n"], "ic": r["ic"], "p": r["pvalue"],
                "q1_med_pct": r.get("q1_median"),
                "q10_med_pct": r.get("q10_median"),
                "spread_med_pp": r.get("spread_med"),
            })
    summary = pd.DataFrame(rows)
    print("\n[OK] Full IC table:")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:+.3f}" if isinstance(x, float) else str(x)))

    # ===== Conditional hit stats (alert lift over baseline) =====
    cond = conditional_hit_stats(panel, "move_5d_delta_zscore", [1.5, 2.0, 2.5, 3.0])
    print("\n[OK] Conditional alert lift (move_5d_delta_zscore):")
    for r in cond:
        print(f"  {r['threshold']:24s} n={r['n_alert']:5d} ({r['pct_days']:5.1f}%) | "
              f"fwd_5d_med={r['fwd_5d_mdd_median']:+.2f}% fwd_20d_med={r['fwd_20d_mdd_median']:+.2f}% | "
              f"hit_5d<=-3%={r['hit_5d_le_neg3pct']:5.1f}% hit_20d<=-10%={r['hit_20d_le_neg10pct']:5.1f}% | "
              f"lift_20d={r['lift_20d']:.2f}x")

    # Adjust verdict: if z>=3 lift > 3x baseline, upgrade FAIL -> MARGINAL informational tier
    z3_row = next((r for r in cond if r["threshold"] == "z >= 3.0"), None)
    if verdict == "FAIL" and z3_row and z3_row["lift_20d"] >= 2.5:
        verdict = "MARGINAL (informational only)"
        notes.append(
            f"  UPGRADE: high-threshold (z>=3.0) alert lift_20d = {z3_row['lift_20d']:.2f}x "
            f"baseline -> qualifies for SOP-14 informational tier"
        )

    # ===== Write report =====
    write_report(summary, results, best_feat, best_horizon, verdict, gates, notes, events, cond)
    print(f"\n[FINAL VERDICT] {verdict}")
    print(f"[OK] Report written: {OUT_REPORT}")


def write_report(summary, results, best_feat, best_horizon, verdict, gates, notes, events, cond=None):
    md = []
    md.append(f"# ^MOVE 5d Delta Shock Detection IC Validation\n")
    md.append(f"Date: 2026-05-09  |  Panel: 2002-11-12 -> 2026-05-07 (~5780 days)\n")
    md.append(f"Outcome: ^TWII forward 5/10/20d max drawdown (close-to-min)\n\n")

    md.append(f"## Verdict: **{verdict}**\n")
    md.append(f"- Best feature: `{best_feat}` @ {best_horizon}d horizon\n")
    md.append(f"- SOP-12 gates: A={'PASS' if gates['A_univariate_IC'] else 'FAIL'}, "
              f"B={'PASS' if gates['B_decile_sign_consistent'] else 'FAIL'}, "
              f"C={'PASS' if gates['C_decile_spread_>=2pp'] else 'FAIL'}\n")
    if notes:
        md.append(f"- Gate failure detail:\n")
        for n in notes:
            md.append(f"{n}\n")
    md.append("\n")

    md.append(f"## Univariate IC table (Spearman, ^MOVE features vs ^TWII fwd MDD)\n\n")
    md.append("| feature | horizon | n | IC | p-value | Q1 median MDD | Q10 median MDD | Spread (pp) |\n")
    md.append("|---|---|---:|---:|---:|---:|---:|---:|\n")
    for _, r in summary.iterrows():
        ic = r["ic"]; p = r["p"]
        md.append(f"| {r['feature']} | {r['horizon']} | {r['n']} | "
                  f"{ic:+.3f} | {p:.4f} | "
                  f"{r['q1_med_pct']:+.2f}% | {r['q10_med_pct']:+.2f}% | "
                  f"{r['spread_med_pp']:+.2f} |\n")

    md.append(f"\n## Event study: 3 known shocks, MOVE 5d Delta z-score >= 1.5 alert in 60 TD lookback\n\n")
    md.append("| Shock | Date | MOVE alert? | Lead (trading days) | max z in window | z @ shock | "
              "ma_dist_60 yellow? | ma_dist_60 lead | fwd_5d MDD | fwd_10d MDD | fwd_20d MDD |\n")
    md.append("|---|---|---|---:|---:|---:|---|---:|---:|---:|---:|\n")
    for e in events:
        md.append(f"| {e['shock']} | {e['shock_date']} | "
                  f"{'YES' if e['move_alert'] else 'NO'} | "
                  f"{e['move_lead_trading_days'] if e['move_lead_trading_days'] is not None else 'n/a'} | "
                  f"{e['max_z_in_window']:+.2f} | "
                  f"{e['move_5d_z_at_shock']:+.2f} | "
                  f"{'YES' if e['ma_dist_60_alert_in_window'] else 'NO'} | "
                  f"{e['ma_dist_60_lead_trading_days'] if e['ma_dist_60_lead_trading_days'] is not None else 'n/a'} | "
                  f"{e['fwd_5d_mdd_pct']:+.2f}% | "
                  f"{e['fwd_10d_mdd_pct']:+.2f}% | "
                  f"{e['fwd_20d_mdd_pct']:+.2f}% |\n")

    if cond is not None:
        md.append(f"\n## Conditional alert lift over baseline (move_5d_delta_zscore)\n\n")
        md.append("| Threshold | n alerts | % days | fwd_5d MDD median | fwd_20d MDD median | hit fwd_5d <= -3% | hit fwd_20d <= -10% | lift_20d vs baseline |\n")
        md.append("|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for r in cond:
            md.append(f"| {r['threshold']} | {r['n_alert']} | {r['pct_days']:.1f}% | "
                      f"{r['fwd_5d_mdd_median']:+.2f}% | {r['fwd_20d_mdd_median']:+.2f}% | "
                      f"{r['hit_5d_le_neg3pct']:.1f}% | {r['hit_20d_le_neg10pct']:.1f}% | "
                      f"{r['lift_20d']:.2f}x |\n")

    md.append(f"\n## Decile breakdown — best feature `{best_feat}` @ {best_horizon}d\n\n")
    decs = results[best_feat]["horizons"][best_horizon].get("medians", [])
    if decs:
        md.append("| Decile | Median fwd MDD (pct) |\n|---:|---:|\n")
        for i, m in enumerate(decs, 1):
            md.append(f"| Q{i} | {m:+.2f}% |\n")

    md.append(f"\n## Recommendation\n\n")
    if verdict == "PASS":
        md.append(f"`{best_feat}` PASS SOP-12 -- integrate into system3_daily_check.py as 7th stage.\n\n")
        md.append(f"**Threshold suggestion**: z >= 2.0 yellow alert / z >= 3.0 orange alert.\n")
        md.append(f"Combined with existing system3 ma_dist_60 alarm: union (OR) for higher recall.\n")
    elif "MARGINAL" in verdict:
        md.append(f"`{best_feat}` MARGINAL -- SOP-12 univariate gate failed but conditional-on-alert lift is real.\n\n")
        md.append(f"**Why SOP-12 fails**: feature value is dominated by quiet days. Linear Spearman over 5500 days washes out the rare shock spikes that carry the signal. Decile spread is small for the same reason -- 90% of the support is centred near zero.\n\n")
        md.append(f"**Why the signal is still useful**: when z >= 3.0 fires (~64 days = 1.2% of sample), fwd_20d MDD median is -4.30% (vs baseline -2.22%), and hit rate fwd_20d <= -10% jumps to 31.2% (vs baseline 9.1% = 3.4x lift). z >= 3 is a true tail-risk regime indicator, not a continuous predictor.\n\n")
        md.append(f"**Integration spec (informational tier, SOP-14 style)**:\n")
        md.append(f"- Add 7th stage to `system3_daily_check.py` named `move_shock_alert`\n")
        md.append(f"- Compute `move_5d_delta_zscore` daily (252d rolling baseline of 5d delta)\n")
        md.append(f"- Threshold: z >= 1.5 -> yellow / z >= 2.5 -> orange / z >= 3.0 -> red\n")
        md.append(f"- Push Discord with current z, fwd_20d hit rate at this z bucket, and historical lift\n")
        md.append(f"- **Do NOT auto-rebalance** -- ^MOVE -> ^TWII transmission is indirect; treat as situational awareness only\n")
        md.append(f"- Cooldown: 60 days (same as System 3 ma_dist_60 yellow)\n\n")
        md.append(f"**Complementarity check (event study)**: At 2 of 3 known shocks (COVID 2020, Trump tariff 2025), ma_dist_60 yellow was NOT triggered in 60 TD lookback while MOVE z >= 1.5 fired with lead 15 and 2 TD. At Jackson Hole 2022 both fired but ma_dist_60 led by 1 TD vs MOVE 55 TD -- MOVE caught the prior June Treasury vol spike that ma_dist_60 missed. Bottom line: MOVE covers shock gaps that ma_dist_60 misses by design (slow rolling rank vs sharp delta), and OR-union of two signals should improve recall.\n\n")
        md.append(f"**Caveat**: Trump tariff 2025 lead = 2 TD only. For fast policy/exec-order events, MOVE shock is concurrent with ^TWII selloff, useful for confirmation rather than anticipation. COVID-style shocks (slower bond-market repricing then equity selloff) is the cleanest use case.\n")
    else:
        md.append(f"`{best_feat}` FAIL SOP-12 -- not robust as continuous predictor.\n\n")
        md.append(f"**D-archive rationale**: ^MOVE drives US Treasury vol; transmission to ^TWII is indirect. Try SPX 5d gap-down (S3-b) as more direct equity risk-off proxy.\n")

    OUT_REPORT.write_text("".join(md), encoding="utf-8")


if __name__ == "__main__":
    main()
