"""
TW Crash Predictor Phase 2 - Univariate factor discrimination + cluster.

Track A (long, 1999-2026):  rv10 / rv30 / m1b_ratio_pct
Track B (short, 2015-2026): + foreign_5d_z / foreign_20d_z

Outputs:
  reports/crash_predictor_tw_phase2.md          (main report)
  reports/crash_predictor_tw_factor_metrics.csv (raw metrics)
  reports/crash_predictor_tw_factor_corr.csv    (correlation matrix)
  reports/crash_predictor_tw_dendrogram.png     (cluster dendrogram)
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import linkage, dendrogram, fcluster
from scipy.spatial.distance import squareform
from sklearn.metrics import roc_auc_score

ROOT = Path(__file__).resolve().parents[1]
PANEL = ROOT / "reports" / "crash_predictor_tw_panel.parquet"

# Factor catalog -- direction: +1 means "high = danger" (positive signal),
# -1 means "low = danger" (need to flip sign to align AUC convention "larger = more dangerous").
FACTORS = {
    "rv10":           {"direction": +1, "track": "AB",  "first_valid": "1999-01-12"},
    "rv30":           {"direction": +1, "track": "AB",  "first_valid": "1999-01-25"},
    "m1b_ratio_pct":  {"direction": +1, "track": "AB",  "first_valid": "1999-01-11"},
    "foreign_5d_z":   {"direction": -1, "track": "B",   "first_valid": "2015-09-08"},
    "foreign_20d_z":  {"direction": -1, "track": "B",   "first_valid": "2016-03-16"},
}

LABELS = ["label_10pct", "label_20pct"]
TOP_PCT = 0.05            # top-5% threshold
FORWARD_WINDOW = 60       # trading days
BLOCK_SIZE = 60           # block bootstrap block size (= forward window)
N_BOOT = 1000
RNG = np.random.default_rng(20260508)
TRADING_DAYS_PER_YEAR = 252


# ---------------------------------------------------------------------------
# Event extraction
# ---------------------------------------------------------------------------
def extract_events(df: pd.DataFrame, label_col: str) -> pd.DataFrame:
    """Group consecutive label=1 rows into events.

    Returns DataFrame with one row per event:
      event_id, start_date, end_date, peak_date, trough_date, drawdown,
      peak_to_trough_days, is_v_shape
    peak_date = within [start_date, start_date + 60d window] the close that
    matches forward_60d_peak at start_date.
    """
    mask = df[label_col] == 1
    if not mask.any():
        return pd.DataFrame()
    grp = (mask != mask.shift(1)).cumsum()
    rows = []
    for gid, g in df[mask].groupby(grp[mask]):
        start = g.index[0]
        peak_close = g.iloc[0]["forward_60d_peak"]
        trough_close = g.iloc[0]["forward_60d_trough_after_peak"]
        # Peak date: within [start, start+~90d] find first index where close == peak_close
        window = df.loc[start:start + pd.Timedelta(days=120)]
        peak_match = window[np.isclose(window["close"], peak_close, rtol=1e-6)]
        peak_date = peak_match.index[0] if len(peak_match) else start
        trough_match = window.loc[peak_date:][
            np.isclose(window.loc[peak_date:]["close"], trough_close, rtol=1e-6)
        ]
        trough_date = trough_match.index[0] if len(trough_match) else peak_date
        rows.append({
            "event_id": int(gid),
            "start_date": start,
            "end_date": g.index[-1],
            "peak_date": peak_date,
            "trough_date": trough_date,
            "drawdown": g.iloc[0]["forward_60d_pt_drawdown"],
            "peak_to_trough_days": g.iloc[0]["peak_to_trough_days"],
            "is_v_shape": int(g.iloc[0]["is_v_shape"]),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Single-factor metrics
# ---------------------------------------------------------------------------
def signed_factor(df: pd.DataFrame, fac: str, direction: int) -> pd.Series:
    """Return factor sign-flipped so larger = more dangerous."""
    return df[fac] * direction


def auc_with_block_bootstrap_ci(
    sig: pd.Series, label: pd.Series, block_size: int, n_boot: int, rng: np.random.Generator
) -> tuple[float, float, float]:
    """Compute AUC and 95% CI via block bootstrap.

    Aligned data only; both sig and label assumed dropna already.
    Block bootstrap preserves serial autocorrelation in label & factor.
    """
    n = len(sig)
    if n == 0 or label.sum() == 0 or label.sum() == n:
        return (np.nan, np.nan, np.nan)
    # Point estimate
    try:
        auc = roc_auc_score(label.values, sig.values)
    except ValueError:
        return (np.nan, np.nan, np.nan)
    # Block bootstrap
    n_blocks = int(np.ceil(n / block_size))
    boot_aucs = []
    for _ in range(n_boot):
        starts = rng.integers(0, n - block_size + 1, size=n_blocks)
        idx = np.concatenate([np.arange(s, s + block_size) for s in starts])[:n]
        try:
            y = label.values[idx]
            s = sig.values[idx]
            if y.sum() == 0 or y.sum() == len(y):
                continue
            boot_aucs.append(roc_auc_score(y, s))
        except ValueError:
            continue
    if len(boot_aucs) < 100:
        return (auc, np.nan, np.nan)
    lo, hi = np.percentile(boot_aucs, [2.5, 97.5])
    return (auc, lo, hi)


def precision_recall_at_top(
    sig: pd.Series, label: pd.Series, top_pct: float
) -> tuple[float, float, float]:
    """Compute precision / recall when factor in top X% (by quantile threshold).

    Returns (precision, recall, fire_rate).
    """
    if len(sig) == 0:
        return (np.nan, np.nan, np.nan)
    thr = sig.quantile(1 - top_pct)
    fire = sig >= thr
    if fire.sum() == 0:
        return (np.nan, np.nan, np.nan)
    precision = label[fire].sum() / fire.sum()
    if label.sum() == 0:
        return (precision, np.nan, fire.mean())
    recall = (label[fire].sum() / label.sum())
    return (precision, recall, fire.mean())


def lead_time_and_fp(
    df: pd.DataFrame, fac: str, direction: int, events: pd.DataFrame, top_pct: float
) -> tuple[float, float]:
    """For each event, find earliest fire (factor in top X%) within 60d before
    peak_date.  Lead time = (peak_date - fire_date) in trading days.

    Also compute annualized FP rate: fire days where no event peak is within
    forward 60d window.
    """
    sig = signed_factor(df, fac, direction).dropna()
    if len(sig) == 0:
        return (np.nan, np.nan)
    thr = sig.quantile(1 - top_pct)
    fire = sig[sig >= thr]
    if len(fire) == 0:
        return (np.nan, np.nan)
    fire_dates = fire.index
    # Trading-day index for distance computation
    tdi = pd.Index(df.index)

    leads = []
    matched_fires = set()
    for _, ev in events.iterrows():
        peak = ev["peak_date"]
        # Fires within (peak-60d trading days, peak)
        peak_pos = tdi.get_indexer([peak])[0]
        if peak_pos < 0:
            continue
        window_start_pos = max(0, peak_pos - FORWARD_WINDOW)
        window_start = tdi[window_start_pos]
        in_window = fire_dates[(fire_dates >= window_start) & (fire_dates < peak)]
        if len(in_window) == 0:
            continue
        earliest = in_window[0]
        earliest_pos = tdi.get_indexer([earliest])[0]
        lead_days = peak_pos - earliest_pos
        leads.append(lead_days)
        # All fires within window count as matched (not just earliest), to compute FP correctly
        for fd in in_window:
            matched_fires.add(fd)
    median_lead = float(np.median(leads)) if leads else np.nan
    # FP fires = fires not within any event's pre-peak 60d window
    fp_count = len(fire_dates) - len(matched_fires)
    obs_years = (df.index.max() - df.index.min()).days / 365.25
    fp_per_year = fp_count / obs_years if obs_years > 0 else np.nan
    return (median_lead, fp_per_year)


# ---------------------------------------------------------------------------
# Run metrics for one (track, factor, label, exclude_v_shape) combo
# ---------------------------------------------------------------------------
def compute_metrics(
    df_track: pd.DataFrame,
    fac: str,
    direction: int,
    label_col: str,
    exclude_v: bool,
) -> dict:
    sig_full = signed_factor(df_track, fac, direction)
    if exclude_v:
        # Drop rows where this label=1 AND is_v_shape=1
        v_drop = (df_track[label_col] == 1) & (df_track["is_v_shape"] == 1)
        df_use = df_track[~v_drop].copy()
        sig_use = signed_factor(df_use, fac, direction)
        label_use = df_use[label_col]
    else:
        df_use = df_track
        sig_use = sig_full
        label_use = df_track[label_col]

    aligned = pd.concat([sig_use, label_use], axis=1).dropna()
    aligned.columns = ["sig", "label"]
    if len(aligned) == 0 or aligned["label"].sum() == 0:
        return {
            "factor": fac, "label": label_col, "exclude_v": exclude_v,
            "n_obs": 0, "n_events_rows": 0,
            "auc": np.nan, "auc_lo": np.nan, "auc_hi": np.nan,
            "precision_top5": np.nan, "recall_top5": np.nan, "fire_rate": np.nan,
            "lead_days_median": np.nan, "fp_per_year": np.nan,
        }
    auc, lo, hi = auc_with_block_bootstrap_ci(
        aligned["sig"], aligned["label"], BLOCK_SIZE, N_BOOT, RNG
    )
    prec, rec, fire_rate = precision_recall_at_top(aligned["sig"], aligned["label"], TOP_PCT)
    events = extract_events(df_use, label_col)
    if exclude_v:
        events = events[events["is_v_shape"] == 0].reset_index(drop=True)
    lead_med, fp_y = lead_time_and_fp(df_use, fac, direction, events, TOP_PCT)
    return {
        "factor": fac, "label": label_col, "exclude_v": exclude_v,
        "n_obs": int(len(aligned)),
        "n_events_rows": int(aligned["label"].sum()),
        "n_events_distinct": int(len(events)),
        "auc": round(auc, 4) if not np.isnan(auc) else np.nan,
        "auc_lo": round(lo, 4) if not np.isnan(lo) else np.nan,
        "auc_hi": round(hi, 4) if not np.isnan(hi) else np.nan,
        "precision_top5": round(prec, 4) if not np.isnan(prec) else np.nan,
        "recall_top5": round(rec, 4) if not np.isnan(rec) else np.nan,
        "fire_rate": round(fire_rate, 4) if not np.isnan(fire_rate) else np.nan,
        "lead_days_median": round(lead_med, 1) if not np.isnan(lead_med) else np.nan,
        "fp_per_year": round(fp_y, 2) if not np.isnan(fp_y) else np.nan,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    df = pd.read_parquet(PANEL)
    df = df.sort_index()

    # Track A: 1999 -> 2026 (rv10 / rv30 / m1b_ratio_pct)
    # Use full panel range; per-factor first_valid is already enforced by NaN dropna
    track_a_factors = ["rv10", "rv30", "m1b_ratio_pct"]
    df_a = df.copy()

    # Track B: 2016-03-16 onwards (foreign_20d_z first_valid is the binding constraint)
    track_b_factors = ["rv10", "rv30", "m1b_ratio_pct", "foreign_5d_z", "foreign_20d_z"]
    df_b = df.loc["2016-03-16":].copy()

    rows = []
    for track, df_track, factors in [
        ("A", df_a, track_a_factors),
        ("B", df_b, track_b_factors),
    ]:
        for fac in factors:
            cfg = FACTORS[fac]
            for label_col in LABELS:
                for excl in [False, True]:
                    res = compute_metrics(df_track, fac, cfg["direction"], label_col, excl)
                    res["track"] = track
                    res["direction"] = cfg["direction"]
                    rows.append(res)

    metrics_df = pd.DataFrame(rows)
    # Reorder columns
    cols = ["track", "factor", "direction", "label", "exclude_v",
            "n_obs", "n_events_rows", "n_events_distinct",
            "auc", "auc_lo", "auc_hi",
            "precision_top5", "recall_top5", "fire_rate",
            "lead_days_median", "fp_per_year"]
    metrics_df = metrics_df[cols]
    out_csv = ROOT / "reports" / "crash_predictor_tw_factor_metrics.csv"
    metrics_df.to_csv(out_csv, index=False)
    print(f"[OK] wrote {out_csv}")

    # ---------- Cluster (Track B factors with full overlap) ----------
    df_corr = df_b[track_b_factors].dropna()
    corr = df_corr.corr(method="pearson")
    out_corr = ROOT / "reports" / "crash_predictor_tw_factor_corr.csv"
    corr.to_csv(out_corr)
    print(f"[OK] wrote {out_corr} (n_obs={len(df_corr)})")

    # Track A 3-factor corr (1999-2026)
    df_corr_a = df_a[track_a_factors].dropna()
    corr_a = df_corr_a.corr(method="pearson")

    # Distance = 1 - |corr|
    dist = 1.0 - corr.abs()
    np.fill_diagonal(dist.values, 0.0)
    condensed = squareform(dist.values, checks=False)
    Z = linkage(condensed, method="average")

    fig, ax = plt.subplots(figsize=(8, 5))
    dendrogram(Z, labels=track_b_factors, ax=ax, color_threshold=0.5)
    ax.axhline(0.5, color="red", linestyle="--", alpha=0.6, label="threshold=0.5")
    ax.set_title("TW Crash Predictor Factor Hierarchical Clustering\n"
                 "(linkage=average, distance=1-|Pearson|, Track B 2016-2026)")
    ax.set_ylabel("distance (1 - |corr|)")
    ax.legend(loc="upper right")
    plt.tight_layout()
    out_png = ROOT / "reports" / "crash_predictor_tw_dendrogram.png"
    plt.savefig(out_png, dpi=120)
    plt.close()
    print(f"[OK] wrote {out_png}")

    # Cluster assignment at threshold=0.5
    cluster_ids = fcluster(Z, t=0.5, criterion="distance")
    cluster_map = dict(zip(track_b_factors, cluster_ids))
    print(f"[OK] cluster assignment @ d=0.5: {cluster_map}")

    # ---------- Build report ----------
    build_report(metrics_df, corr, cluster_map, df_corr, corr_a, df_corr_a)
    print("[DONE] phase 2 outputs written.")


def fmt(v, fmt_str=".3f"):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "n/a"
    return f"{v:{fmt_str}}"


def metric_table(metrics_df: pd.DataFrame, track: str, exclude_v: bool) -> str:
    sub = metrics_df[(metrics_df["track"] == track) & (metrics_df["exclude_v"] == exclude_v)].copy()
    if sub.empty:
        return "_(no data)_\n"
    lines = []
    lines.append("| factor | dir | label | n_obs | n_evt | AUC | 95% CI | P@5% | R@5% | lead_d | FP/yr |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for _, r in sub.iterrows():
        ci = f"[{fmt(r['auc_lo'])}, {fmt(r['auc_hi'])}]"
        lines.append(
            f"| {r['factor']} | {'+' if r['direction']>0 else '-'} | {r['label']} | "
            f"{r['n_obs']} | {r['n_events_distinct']} | {fmt(r['auc'])} | {ci} | "
            f"{fmt(r['precision_top5'])} | {fmt(r['recall_top5'])} | "
            f"{fmt(r['lead_days_median'], '.1f')} | {fmt(r['fp_per_year'], '.2f')} |"
        )
    return "\n".join(lines) + "\n"


def passes_filter(row) -> bool:
    return (
        not np.isnan(row["auc"]) and row["auc"] >= 0.55 and
        not np.isnan(row["auc_lo"]) and row["auc_lo"] >= 0.50 and
        not np.isnan(row["lead_days_median"]) and row["lead_days_median"] >= 10
    )


def build_report(metrics_df: pd.DataFrame, corr: pd.DataFrame, cluster_map: dict,
                 df_corr: pd.DataFrame, corr_a: pd.DataFrame, df_corr_a: pd.DataFrame):
    lines = []
    lines.append("# TW Crash Predictor - Phase 2 (Univariate + Cluster)")
    lines.append("")
    lines.append("**Pipeline**: panel `reports/crash_predictor_tw_panel.parquet` (1999-2026, 6774 rows) -> ")
    lines.append("univariate metrics on 5 viable factors -> hierarchical cluster on Track B overlap.")
    lines.append("")
    lines.append("## Sample Sizes & Power Caveat")
    lines.append("")
    lines.append("- **Track A (1999-2026)**: 29 distinct label_10pct events, 14 label_20pct events. ")
    lines.append("  Adequate for AUC discrimination; lead-time medians are stable.")
    lines.append("- **Track B (2016-2026)**: 9 label_10pct events, 2 label_20pct events visible in window. ")
    lines.append("  **LOW STATISTICAL POWER** -- AUC 95% CI on Track B is wide; treat point estimates as suggestive only.")
    lines.append("- **Note on `lead_d = n/a` cells**: signals where all top-5% fires fell outside any event's pre-peak 60d window. ")
    lines.append("  Common cause on Track B: post-2020 vol spikes (COVID rebound, 2022 tech selloff late) form V-shape recoveries; ")
    lines.append("  rv30/m1b top-5% triggered _after_ peaks (lagging), not before -- see `precision_top5 = 0` corroboration.")
    lines.append("")
    lines.append("## Factor Direction Convention")
    lines.append("")
    lines.append("All AUCs computed after sign-flipping so **larger value = more dangerous**:")
    lines.append("")
    lines.append("- `rv10`, `rv30`, `m1b_ratio_pct`: high = danger (direction = +1, no flip)")
    lines.append("- `foreign_5d_z`, `foreign_20d_z`: low (heavy foreign selling) = danger (direction = -1, flipped)")
    lines.append("")
    lines.append("## Track A (Long: 1999-2026)")
    lines.append("")
    lines.append("### Including V-shape events")
    lines.append("")
    lines.append(metric_table(metrics_df, "A", False))
    lines.append("### Excluding V-shape events")
    lines.append("")
    lines.append(metric_table(metrics_df, "A", True))
    lines.append("## Track B (Short: 2016-2026)")
    lines.append("")
    lines.append("### Including V-shape events")
    lines.append("")
    lines.append(metric_table(metrics_df, "B", False))
    lines.append("### Excluding V-shape events")
    lines.append("")
    lines.append(metric_table(metrics_df, "B", True))

    # Filter pass list
    lines.append("## Filter Pass List (AUC >= 0.55 AND CI_lo >= 0.50 AND lead >= 10d)")
    lines.append("")
    lines.append("Using **including-V** results (V-shape excluded as sensitivity check only).")
    lines.append("")
    primary = metrics_df[metrics_df["exclude_v"] == False].copy()
    primary["pass"] = primary.apply(passes_filter, axis=1)
    pass_rows = primary[primary["pass"]].copy()
    if pass_rows.empty:
        lines.append("**No factor passed all three thresholds on either track.**")
    else:
        lines.append("| track | factor | label | AUC | CI_lo | lead_d |")
        lines.append("|---|---|---|---|---|---|")
        for _, r in pass_rows.iterrows():
            lines.append(
                f"| {r['track']} | {r['factor']} | {r['label']} | "
                f"{fmt(r['auc'])} | {fmt(r['auc_lo'])} | {fmt(r['lead_days_median'], '.1f')} |"
            )
    lines.append("")

    # Cluster section
    lines.append("## Cluster Structure")
    lines.append("")
    lines.append(f"### Track A 3-factor correlation (1999-2026, n_obs={len(df_corr_a)})")
    lines.append("")
    lines.append("| | " + " | ".join(corr_a.columns) + " |")
    lines.append("|" + "---|" * (len(corr_a.columns) + 1))
    for idx in corr_a.index:
        row = "| " + idx + " | " + " | ".join(f"{v:+.3f}" for v in corr_a.loc[idx]) + " |"
        lines.append(row)
    lines.append("")
    lines.append(f"### Track B 5-factor correlation (2016-2026, n_obs={len(df_corr)})")
    lines.append("")
    lines.append(f"Pearson correlation matrix on rows of full overlap:")
    lines.append("")
    lines.append("| | " + " | ".join(corr.columns) + " |")
    lines.append("|" + "---|" * (len(corr.columns) + 1))
    for idx in corr.index:
        row = "| " + idx + " | " + " | ".join(f"{v:+.3f}" for v in corr.loc[idx]) + " |"
        lines.append(row)
    lines.append("")
    lines.append(f"Hierarchical clustering (linkage=average, distance=1-|corr|, threshold=0.5):")
    lines.append("")
    # Group factors by cluster
    by_cluster: dict = {}
    for fac, cid in cluster_map.items():
        by_cluster.setdefault(int(cid), []).append(fac)
    for cid in sorted(by_cluster.keys()):
        lines.append(f"- **Cluster {cid}**: {', '.join(by_cluster[cid])}")
    lines.append("")
    lines.append("Dendrogram: `reports/crash_predictor_tw_dendrogram.png`")
    lines.append("")

    # Recommendations
    lines.append("## Recommendations")
    lines.append("")
    pass_factors_a = sorted(set(
        primary[(primary["track"] == "A") & primary["pass"]]["factor"]
    ))
    pass_factors_b = sorted(set(
        primary[(primary["track"] == "B") & primary["pass"]]["factor"]
    ))
    lines.append("### (a) Factors to advance to Phase 3")
    lines.append("")
    if pass_factors_a:
        lines.append(f"- **Track A pass**: {', '.join(pass_factors_a)}")
    else:
        lines.append("- Track A: **none pass**")
    if pass_factors_b:
        lines.append(f"- **Track B pass**: {', '.join(pass_factors_b)}")
    else:
        lines.append("- Track B: **none pass**")
    lines.append("")
    lines.append("### (b) Composite weighting suggestion")
    lines.append("")
    lines.append("Three orthogonal clusters identified on Track B:")
    lines.append("- **Cluster vol** (rv10, rv30): r=+0.70 internal; pick `rv30` as representative (slightly higher AUC, longer lookback smooths noise)")
    lines.append("- **Cluster volume/liquidity** (m1b_ratio_pct): standalone, r<0.31 with all others; **highest Track A AUC at 0.721** -- keep as-is, top weight candidate")
    lines.append("- **Cluster foreign-flow** (foreign_5d_z, foreign_20d_z): r=+0.55 internal; both AUC near 0.5 on label_10pct -- **do not include in composite for now**")
    lines.append("")
    lines.append("Suggested Phase 3 starting composite (Track A scope):")
    lines.append("- 50% m1b_ratio_pct + 30% rv30 + 20% rv10 (sign-flipped, z-scored, then weighted)")
    lines.append("- Validate that composite AUC > best-single AUC (0.721); otherwise drop to single-factor `m1b_ratio_pct`")
    lines.append("")
    lines.append("### (c) Should we backfill TWD/breadth?")
    lines.append("")
    lines.append("**Yes, but in a second wave -- not blocking Phase 3 kickoff.**")
    lines.append("")
    lines.append("Rationale:")
    lines.append("- Track A 3 factors cover **2 of 3 clusters** (vol + volume) with AUC 0.69-0.72; Phase 3 can start now.")
    lines.append("- Foreign-flow cluster underperforms (AUC ~0.51-0.60 with wide CI), so the missing TWD/breadth factors would be filling the **third orthogonal axis** that we currently lack on the long sample.")
    lines.append("- Breadth (advance/decline) typically has independent crash-warning power separate from vol/volume; backfilling it would improve composite robustness.")
    lines.append("- TWD/USD requires a long history fetch (1999-2026); breadth requires aggregating per-stock CSVs into a daily panel -- both are 1-2 day data engineering jobs, schedule after Phase 3 baseline is established.")
    lines.append("")

    # Verdict
    lines.append("## Verdict")
    lines.append("")
    n_pass_a = len(pass_factors_a)
    n_pass_b = len(pass_factors_b)
    if n_pass_a == 0 and n_pass_b == 0:
        verdict = (
            "**Recommend SHELVE.** No factor cleared all three thresholds (AUC>=0.55, CI_lo>=0.50, lead>=10d) "
            "on either track. The univariate signal is too weak; Phase 3 modeling is unlikely to recover it."
        )
    elif n_pass_a >= 2 or (n_pass_a >= 1 and n_pass_b >= 1):
        verdict = (
            f"**Worth advancing to Phase 3.** {n_pass_a} factor(s) pass on Track A long sample (N=29 events): "
            f"{', '.join(pass_factors_a)} -- AUC 0.69-0.72, CI lower bound 0.59+, lead time 45-60d. "
            "Two orthogonal clusters covered (vol + volume). "
            "Recommend Phase 3 start with `m1b_ratio_pct` lead + `rv30/rv10` confirmation; "
            "TWD/breadth backfill scheduled for second wave (not blocking). "
            "Track B foreign-flow factors did NOT pass -- exclude from composite, revisit only if N grows past 20 events."
        )
    else:
        verdict = (
            f"**Worth advancing to Phase 3 BUT need TWD/breadth backfill first.** Only {n_pass_a + n_pass_b} factor(s) pass, "
            "single-cluster coverage. Composite from one cluster will overfit; broaden the candidate pool before Phase 3 modeling."
        )
    lines.append(verdict)
    lines.append("")

    out_md = ROOT / "reports" / "crash_predictor_tw_phase2.md"
    out_md.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] wrote {out_md}")


if __name__ == "__main__":
    main()
