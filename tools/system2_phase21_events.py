"""
System 2 Phase 2.1 - TWII -5% event detection + 3-class label.

Per spec (reports/cross_asset_crash_predictor_plan.md System 2 Phase 2.1):
  - From 60d rolling high compute drawdown
  - First day triggering <=-5% = event start
  - Within next 60 trading days find trough; final_drawdown = (trough - peak) / peak
  - Re-triggers within 60d window are treated as same event (extension rule)

Three-class label by final_drawdown:
  Class A (small reversal) : [-10%, -5%)
  Class B (medium)         : [-20%, -10%)
  Class C (crash)          : <= -20%

Outputs:
  reports/system2_events.parquet
  reports/system2_events_summary.md
  reports/system2_events.csv  (for quick inspection)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TAIEX = ROOT / "data_cache" / "TAIEX_price.parquet"
OUT_PARQUET = ROOT / "reports" / "system2_events.parquet"
OUT_CSV = ROOT / "reports" / "system2_events.csv"
OUT_MD = ROOT / "reports" / "system2_events_summary.md"

WINDOW_DAYS = 60          # trading days, both rolling high and forward window
TRIGGER = -0.05
RECOVERY_HORIZON = 30     # trading days after trough


def detect_events(close: np.ndarray, dates: np.ndarray) -> pd.DataFrame:
    rolling_max = pd.Series(close).rolling(WINDOW_DAYS, min_periods=1).max().to_numpy()
    drawdown = close / rolling_max - 1.0

    events: list[dict] = []
    n = len(close)
    i = 0
    while i < n:
        if drawdown[i] <= TRIGGER:
            trigger_close = float(close[i])
            peak_close = float(rolling_max[i])
            end_idx = min(i + WINDOW_DAYS, n - 1)
            window = close[i:end_idx + 1]
            trough_offset = int(np.argmin(window))
            trough_idx = i + trough_offset
            trough_close = float(close[trough_idx])
            final_dd = trough_close / peak_close - 1.0

            rec_end = min(trough_idx + RECOVERY_HORIZON, n - 1)
            if rec_end > trough_idx and trigger_close > trough_close:
                rec_max = float(np.max(close[trough_idx:rec_end + 1]))
                recovery_speed = (rec_max - trough_close) / (trigger_close - trough_close)
            else:
                recovery_speed = np.nan

            if final_dd > -0.10:
                cls = "A_small"
            elif final_dd > -0.20:
                cls = "B_medium"
            else:
                cls = "C_crash"

            events.append({
                "event_id": len(events) + 1,
                "trigger_date": pd.Timestamp(dates[i]),
                "trigger_close": trigger_close,
                "peak_close": peak_close,
                "trough_date": pd.Timestamp(dates[trough_idx]),
                "trough_close": trough_close,
                "final_drawdown": final_dd,
                "time_to_trough_days": int(trough_offset),
                "recovery_speed_30d": float(recovery_speed) if not np.isnan(recovery_speed) else None,
                "class": cls,
            })
            # extension rule: skip past 60d forward window
            i = end_idx + 1
        else:
            i += 1
    return pd.DataFrame(events)


def write_summary(events: pd.DataFrame, taiex_first: pd.Timestamp, taiex_last: pd.Timestamp) -> str:
    n = len(events)
    by_class = events["class"].value_counts().reindex(["A_small", "B_medium", "C_crash"]).fillna(0).astype(int)
    pct = (by_class / n * 100).round(1) if n else by_class

    lines = [
        "# System 2 Events Summary (TWII)",
        "",
        f"**Source**: `{TAIEX.name}`  ",
        f"**Date range**: {taiex_first.date()} -> {taiex_last.date()}  ",
        f"**Window**: {WINDOW_DAYS} trading days (rolling high + forward)  ",
        f"**Trigger**: drawdown <= {TRIGGER:.0%}",
        "",
        f"## Event count: {n}",
        "",
        "| Class | Range | N | % |",
        "|---|---|---|---|",
        f"| A_small  | [-10%, -5%)   | {by_class['A_small']}  | {pct['A_small']}%  |",
        f"| B_medium | [-20%, -10%)  | {by_class['B_medium']} | {pct['B_medium']}% |",
        f"| C_crash  | <= -20%        | {by_class['C_crash']}  | {pct['C_crash']}%  |",
        "",
        "## Spec target distribution",
        "",
        "| Class | Spec %  | Actual % | Delta |",
        "|---|---|---|---|",
        f"| A_small  | 60% | {pct.get('A_small', 0)}%  | {pct.get('A_small', 0) - 60:+.1f} |",
        f"| B_medium | 25% | {pct.get('B_medium', 0)}% | {pct.get('B_medium', 0) - 25:+.1f} |",
        f"| C_crash  | 15% | {pct.get('C_crash', 0)}%  | {pct.get('C_crash', 0) - 15:+.1f} |",
        "",
        "## Time-to-trough by class (median trading days)",
        "",
    ]
    if n:
        med = events.groupby("class")["time_to_trough_days"].median()
        for cls in ["A_small", "B_medium", "C_crash"]:
            v = int(med.get(cls, 0)) if cls in med.index else None
            lines.append(f"- {cls}: {v}")
        lines.append("")
        lines.append("## Final drawdown by class (median)")
        lines.append("")
        med_dd = events.groupby("class")["final_drawdown"].median()
        for cls in ["A_small", "B_medium", "C_crash"]:
            if cls in med_dd.index:
                lines.append(f"- {cls}: {med_dd[cls]:.2%}")
        lines.append("")

    # SOP-14 gate check
    n_c = int(by_class["C_crash"])
    lines.append("## SOP-14 sample sufficiency gate")
    lines.append("")
    if n >= 50 and n_c >= 10:
        lines.append(f"- N={n} (>=50), C_crash={n_c} (>=10): **PASS** -- proceed to Phase 2.2")
    else:
        lines.append(f"- N={n} (need>=50), C_crash={n_c} (need>=10): **FAIL** -- informational only or gather more data")
    lines.append("")

    lines.append("## Top 10 deepest crashes")
    lines.append("")
    if n:
        top = events.sort_values("final_drawdown").head(10)
        lines.append("| event_id | trigger_date | trough_date | peak | trough | final_dd | t2t | class |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for _, r in top.iterrows():
            lines.append(
                f"| {r['event_id']} | {r['trigger_date'].date()} | {r['trough_date'].date()} | "
                f"{r['peak_close']:.0f} | {r['trough_close']:.0f} | {r['final_drawdown']:.2%} | "
                f"{r['time_to_trough_days']} | {r['class']} |"
            )
    return "\n".join(lines) + "\n"


def main() -> None:
    df = pd.read_parquet(TAIEX)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    close = df["close"].astype(float).to_numpy()
    dates = df["date"].to_numpy()

    events = detect_events(close, dates)

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    events.to_parquet(OUT_PARQUET, index=False)
    events.to_csv(OUT_CSV, index=False)

    summary = write_summary(events, df["date"].iloc[0], df["date"].iloc[-1])
    OUT_MD.write_text(summary, encoding="utf-8")

    print(f"[OK] {len(events)} events -> {OUT_PARQUET.name}")
    print(f"[OK] summary -> {OUT_MD.name}")
    print()
    by_class = events["class"].value_counts().reindex(["A_small", "B_medium", "C_crash"]).fillna(0).astype(int)
    print("Class distribution:")
    for k, v in by_class.items():
        pct = v / len(events) * 100 if len(events) else 0
        print(f"  {k:10s}: {v:4d} ({pct:.1f}%)")


if __name__ == "__main__":
    main()
