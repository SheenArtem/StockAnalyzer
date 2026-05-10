"""
build_crash_predictor_panel.py
=================================
Build daily panel for TW market crash predictor backtest.

Output: reports/crash_predictor_tw_panel.parquet
        reports/crash_predictor_tw_panel_summary.md

Data sources (read-only, no new API calls except existing FinMind loader):
  - TAIEX close  : data_cache/TAIEX_price.parquet (FinMind, 1999-01-05+)
  - Institutional: data_cache/chip_history/institutional.parquet (2015-01-05+)
  - Margin/Short : data_cache/chip_history/margin.parquet (2021-04-16+)
  - M1B          : data_cache/cbc_m1b.csv via money_supply.get_m1b_series()
  - ATM Put prem : data/sentiment/atm_put_premium.parquet
  - Mini futures : data/sentiment/minifutures_ratio.parquet
  - TDCC 1-5     : data_cache/tdcc/1-5/*.parquet (weekly snapshots)
"""
from __future__ import annotations

import sys
import os
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanity(name: str, s: pd.Series) -> dict:
    """Return sanity stats for a series."""
    n_valid = s.notna().sum()
    n_total = len(s)
    null_pct = 100.0 * (n_total - n_valid) / n_total if n_total > 0 else 100.0
    first_valid = s.first_valid_index()
    last_valid = s.last_valid_index()
    return {
        "name": name,
        "n_valid": n_valid,
        "null_pct": round(null_pct, 1),
        "first_valid": first_valid,
        "last_valid": last_valid,
        "min": round(float(s.dropna().min()), 4) if n_valid > 0 else None,
        "max": round(float(s.dropna().max()), 4) if n_valid > 0 else None,
    }


# ---------------------------------------------------------------------------
# Section A: Label columns
# ---------------------------------------------------------------------------

def build_labels(close: pd.Series) -> pd.DataFrame:
    """
    Parameters
    ----------
    close : pd.Series, DatetimeIndex, sorted ascending

    Returns
    -------
    DataFrame with all label columns, same index as close.
    """
    n = len(close)
    close = close.astype(float)

    forward_peak = pd.Series(np.nan, index=close.index)
    forward_trough = pd.Series(np.nan, index=close.index)
    peak_day_offset = pd.Series(np.nan, index=close.index)
    trough_day_offset = pd.Series(np.nan, index=close.index)
    is_v_shape = pd.Series(0, index=close.index, dtype=int)

    HORIZON = 60

    vals = close.values
    dates = close.index

    for i in range(n - 1):
        end = min(i + HORIZON, n - 1)
        window = vals[i: end + 1]           # includes t+0 through t+60

        # Peak: highest close in [t, t+60]
        peak_rel = int(np.argmax(window))
        peak_val = float(window[peak_rel])
        forward_peak.iloc[i] = peak_val
        peak_day_offset.iloc[i] = peak_rel

        # Trough: lowest close in [peak_day, t+60]
        after_peak = window[peak_rel:]
        trough_rel_in_after = int(np.argmin(after_peak))
        trough_val = float(after_peak[trough_rel_in_after])
        forward_trough.iloc[i] = trough_val
        trough_day_offset.iloc[i] = peak_rel + trough_rel_in_after

        # V-shape: trough + 5 bar recovery >= midpoint of (peak - trough)
        # midpoint = trough + (peak - trough) * 0.5
        trough_abs_idx = i + peak_rel + trough_rel_in_after
        recovery_end = min(trough_abs_idx + 5, n - 1)
        midpoint = trough_val + (peak_val - trough_val) * 0.5
        if recovery_end > trough_abs_idx:
            recovery_window = vals[trough_abs_idx: recovery_end + 1]
            if np.max(recovery_window) >= midpoint:
                is_v_shape.iloc[i] = 1

    pt_drawdown = (forward_trough - forward_peak) / forward_peak
    label_10 = (pt_drawdown <= -0.10).astype(int)
    label_20 = (pt_drawdown <= -0.20).astype(int)
    pt_days = trough_day_offset - peak_day_offset

    df = pd.DataFrame({
        "close": close,
        "forward_60d_peak": forward_peak,
        "forward_60d_trough_after_peak": forward_trough,
        "forward_60d_pt_drawdown": pt_drawdown,
        "label_10pct": label_10,
        "label_20pct": label_20,
        "peak_to_trough_days": pt_days,
        "is_v_shape": is_v_shape,
    }, index=close.index)

    # Last HORIZON rows: labels are incomplete (look-ahead beyond data end)
    # Zero them out rather than leave potentially misleading partial values
    if n >= HORIZON:
        df.iloc[-HORIZON:, df.columns.get_loc("forward_60d_peak"):] = np.nan
        df.iloc[-HORIZON:, df.columns.get_loc("label_10pct")] = np.nan
        df.iloc[-HORIZON:, df.columns.get_loc("label_20pct")] = np.nan
        df.iloc[-HORIZON:, df.columns.get_loc("is_v_shape")] = np.nan

    return df


# ---------------------------------------------------------------------------
# Section B: Factor loaders
# ---------------------------------------------------------------------------

def load_institutional_foreign_z(trade_index: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series]:
    """
    Factor 1+2: Foreign net cumulative 5d / 20d z-score (252d rolling).
    Source: data_cache/chip_history/institutional.parquet
    Coverage: 2015-01-05+
    """
    path = ROOT / "data_cache" / "chip_history" / "institutional.parquet"
    if not path.exists():
        return pd.Series(np.nan, index=trade_index, name="foreign_5d_z"), \
               pd.Series(np.nan, index=trade_index, name="foreign_20d_z")

    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])

    # Aggregate all stocks, both markets -> market-total foreign net per day
    market_daily = (
        df.groupby("date")["foreign_net"]
        .sum()
        .rename("foreign_net_total")
    )
    market_daily = market_daily.reindex(trade_index)

    def rolling_z(s: pd.Series, window: int, roll: int = 252) -> pd.Series:
        cum = s.rolling(window, min_periods=window).sum()
        mu = cum.rolling(roll, min_periods=roll // 2).mean()
        sigma = cum.rolling(roll, min_periods=roll // 2).std()
        z = (cum - mu) / sigma.replace(0, np.nan)
        return z

    z5 = rolling_z(market_daily, window=5).rename("foreign_5d_z")
    z20 = rolling_z(market_daily, window=20).rename("foreign_20d_z")
    return z5, z20


def load_margin_z(trade_index: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series]:
    """
    Factor 3+4: Total market margin balance 7d change z-score + short/margin ratio.
    Source: data_cache/chip_history/margin.parquet
    Coverage: 2021-04-16+
    """
    path = ROOT / "data_cache" / "chip_history" / "margin.parquet"
    if not path.exists():
        return pd.Series(np.nan, index=trade_index, name="margin_7d_z"), \
               pd.Series(np.nan, index=trade_index, name="short_margin_ratio")

    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])

    market_daily = df.groupby("date").agg(
        margin_balance_total=("margin_balance", "sum"),
        short_balance_total=("short_balance", "sum"),
    )
    market_daily = market_daily.reindex(trade_index)

    # 7d change rate z-score (252d rolling)
    mb = market_daily["margin_balance_total"]
    mb_7d_chg = mb.pct_change(7)
    mu = mb_7d_chg.rolling(252, min_periods=126).mean()
    sigma = mb_7d_chg.rolling(252, min_periods=126).std()
    margin_7d_z = ((mb_7d_chg - mu) / sigma.replace(0, np.nan)).rename("margin_7d_z")

    # short / margin ratio
    short_margin = (
        market_daily["short_balance_total"] / market_daily["margin_balance_total"].replace(0, np.nan)
    ).rename("short_margin_ratio")

    return margin_7d_z, short_margin


def load_m1b_ratio(trade_index: pd.DatetimeIndex) -> pd.Series:
    """
    Factor 5: M1B / market trading value ratio.
    M1B: monthly (forward-filled to daily); Trading_money: from TAIEX_price.parquet (daily).
    Coverage: 1999-01-05+ (limited by TAIEX coverage), M1B from 1987.
    """
    # M1B monthly series
    try:
        from money_supply import get_m1b_series
        m1b_df = get_m1b_series(force_refresh=False)
    except Exception:
        return pd.Series(np.nan, index=trade_index, name="m1b_ratio_pct")

    # Parse period YYYYMM -> month-end -> add 35d publication lag -> forward fill to daily
    # Rationale: CBC publishes monthly M1B around the 25th of the following month;
    # using 35-day lag (highly conservative) eliminates look-ahead bias where panel
    # would otherwise use M1B values not yet released to the market.
    m1b_df["year"] = m1b_df["period"].astype(str).str[:4].astype(int)
    m1b_df["month"] = m1b_df["period"].astype(str).str[4:6].astype(int)
    m1b_df["effective_date"] = (
        pd.to_datetime(m1b_df[["year", "month"]].assign(day=1))
        + pd.offsets.MonthEnd(0)
        + pd.Timedelta(days=35)
    )
    m1b_daily = (
        m1b_df.set_index("effective_date")["m1b_mil_twd"]
        .reindex(trade_index, method="ffill")
    )

    # Trading_money from TAIEX parquet (TWD already)
    taiex_path = ROOT / "data_cache" / "TAIEX_price.parquet"
    if not taiex_path.exists():
        return pd.Series(np.nan, index=trade_index, name="m1b_ratio_pct")
    taiex = pd.read_parquet(taiex_path)
    taiex["date"] = pd.to_datetime(taiex["date"])
    tv = taiex.set_index("date")["Trading_money"].reindex(trade_index)

    # 20d trailing sum of trading value
    tv_20d = tv.rolling(20, min_periods=5).sum()

    # ratio = (20d TV) / (M1B in TWD) * 100
    m1b_twd = m1b_daily * 1e6
    ratio = (tv_20d / m1b_twd * 100).rename("m1b_ratio_pct")
    return ratio


def load_atm_put_z(trade_index: pd.DatetimeIndex) -> pd.Series:
    """
    Factor 6: ATM Put Premium 30d z-score.
    Source: data/sentiment/atm_put_premium.parquet
    Coverage: very sparse (only ~4 rows in cache, 2026-05 only)
    """
    path = ROOT / "data" / "sentiment" / "atm_put_premium.parquet"
    if not path.exists():
        return pd.Series(np.nan, index=trade_index, name="atm_put_z")

    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["data_date"])
    s = df.set_index("date")["atm_put_pct"].reindex(trade_index)

    if s.notna().sum() < 5:
        # Too sparse for rolling z; return raw with note
        return s.rename("atm_put_z")

    mu = s.rolling(30, min_periods=10).mean()
    sigma = s.rolling(30, min_periods=10).std()
    z = ((s - mu) / sigma.replace(0, np.nan)).rename("atm_put_z")
    return z


def load_minifutures_ratio(trade_index: pd.DatetimeIndex) -> pd.Series:
    """
    Factor 7: Mini futures OI / Regular futures OI ratio.
    Source: data/sentiment/minifutures_ratio.parquet
    Coverage: very sparse (only ~4 rows in cache, 2026-05 only)
    """
    path = ROOT / "data" / "sentiment" / "minifutures_ratio.parquet"
    if not path.exists():
        return pd.Series(np.nan, index=trade_index, name="mtx_txf_ratio")

    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["data_date"])
    s = df.set_index("date")["mtx_txf_ratio"].reindex(trade_index)
    return s.rename("mtx_txf_ratio")


def load_realized_vol(close: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Factor 9: Realized volatility from close (10d and 30d annualized).
    Computed directly from TAIEX close, no external data needed.
    Coverage: same as TAIEX (1999-01-05+)
    """
    log_ret = np.log(close / close.shift(1))
    rv10 = log_ret.rolling(10, min_periods=5).std() * np.sqrt(252)
    rv30 = log_ret.rolling(30, min_periods=15).std() * np.sqrt(252)
    return rv10.rename("rv10"), rv30.rename("rv30")


def load_breadth(trade_index: pd.DatetimeIndex) -> pd.Series:
    """
    Factor 10: Breadth (pct of stocks up on the day).
    Source: institutional.parquet change_pct proxy - NO, that is not available.
    The chip_history/institutional.parquet has foreign_net per stock but not
    price change. data_cache has per-stock price CSVs but reconstructing
    advance/decline for ALL listed stocks is computationally expensive and
    requires iterating thousands of CSVs without a pre-built broad market panel.
    => N/A: no pre-built advance/decline daily cache exists.
    """
    return pd.Series(np.nan, index=trade_index, name="breadth_up_pct")


def load_twd_usd(trade_index: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series]:
    """
    Factor 8: TWD/USD 1d change rate + 60d quantile rank.
    Source: No TWD/USD cache exists in data_cache/ or data/.
    => N/A
    """
    return (
        pd.Series(np.nan, index=trade_index, name="twdusd_1d_chg"),
        pd.Series(np.nan, index=trade_index, name="twdusd_60d_rank"),
    )


def load_tdcc_large_holder_delta(trade_index: pd.DatetimeIndex) -> pd.Series:
    """
    Bonus Factor 11: TDCC large holder (>1000 lots) shareholding pct 7d delta.
    Source: data_cache/tdcc/1-5/*.parquet (weekly snapshots).
    Coverage: only 3 snapshots (20260417, 20260424, 20260430).
    Strategy: aggregate pct for is_whale stocks, weekly forward-fill to daily.
    """
    tdcc_dir = ROOT / "data_cache" / "tdcc" / "1-5"
    if not tdcc_dir.exists():
        return pd.Series(np.nan, index=trade_index, name="tdcc_large_7d_delta")

    snaps = sorted(tdcc_dir.glob("*.parquet"))
    if not snaps:
        return pd.Series(np.nan, index=trade_index, name="tdcc_large_7d_delta")

    records = []
    for snap in snaps:
        date_str = snap.stem  # e.g. "20260417"
        try:
            snap_date = pd.Timestamp(date_str)
        except Exception:
            continue
        df = pd.read_parquet(snap)
        # is_whale = shareholding >= 1000 lots
        whale = df[df["is_whale"] == True]
        if "pct" in whale.columns and not whale.empty:
            total_pct = whale.groupby("data_date")["pct"].sum().mean()
        else:
            total_pct = np.nan
        records.append({"date": snap_date, "large_pct": total_pct})

    if not records:
        return pd.Series(np.nan, index=trade_index, name="tdcc_large_7d_delta")

    tdcc_weekly = pd.DataFrame(records).set_index("date")["large_pct"]
    tdcc_daily = tdcc_weekly.reindex(trade_index, method="ffill")
    delta7 = tdcc_daily.diff(7).rename("tdcc_large_7d_delta")
    return delta7


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=== Building TW Crash Predictor Panel ===")

    # --- Load TAIEX close ---
    taiex_path = ROOT / "data_cache" / "TAIEX_price.parquet"
    if not taiex_path.exists():
        print("[ERROR] TAIEX_price.parquet not found. Run FinMind TAIEX fetch first.")
        sys.exit(1)

    taiex = pd.read_parquet(taiex_path)
    taiex["date"] = pd.to_datetime(taiex["date"])
    taiex = taiex.set_index("date").sort_index()
    close = taiex["close"].rename("close").astype(float)
    close = close[close.notna() & (close > 0)]

    # Trade calendar = TAIEX trading days
    trade_index = close.index
    print(f"  TAIEX close: {trade_index[0].date()} - {trade_index[-1].date()} ({len(trade_index)} rows)")

    # --- Section A: Labels ---
    print("  Building labels...")
    df_labels = build_labels(close)

    # --- Section B: Factors ---
    print("  Loading factors...")

    z5, z20 = load_institutional_foreign_z(trade_index)
    margin_7d_z, short_margin = load_margin_z(trade_index)
    m1b_ratio = load_m1b_ratio(trade_index)
    atm_put_z = load_atm_put_z(trade_index)
    mtx_ratio = load_minifutures_ratio(trade_index)
    twdusd_1d, twdusd_rank = load_twd_usd(trade_index)
    rv10, rv30 = load_realized_vol(close)
    breadth = load_breadth(trade_index)
    tdcc_delta = load_tdcc_large_holder_delta(trade_index)

    # --- Assemble panel ---
    panel = df_labels.copy()
    panel["foreign_5d_z"] = z5
    panel["foreign_20d_z"] = z20
    panel["margin_7d_z"] = margin_7d_z
    panel["short_margin_ratio"] = short_margin
    panel["m1b_ratio_pct"] = m1b_ratio
    panel["atm_put_z"] = atm_put_z
    panel["mtx_txf_ratio"] = mtx_ratio
    panel["twdusd_1d_chg"] = twdusd_1d
    panel["twdusd_60d_rank"] = twdusd_rank
    panel["rv10"] = rv10
    panel["rv30"] = rv30
    panel["breadth_up_pct"] = breadth
    panel["tdcc_large_7d_delta"] = tdcc_delta

    panel.index.name = "date"

    # --- Output ---
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(exist_ok=True)

    out_parquet = reports_dir / "crash_predictor_tw_panel.parquet"
    panel.to_parquet(out_parquet)
    print(f"\n  Saved: {out_parquet}")
    print(f"  Panel shape: {panel.shape}")

    # --- Sanity checks ---
    print("\n=== Sanity Checks ===")
    sanity_rows = []
    for col in panel.columns:
        s = _sanity(col, panel[col])
        sanity_rows.append(s)
        print(f"  {col:40s} valid={s['n_valid']:5d}  null%={s['null_pct']:5.1f}%"
              f"  [{s['first_valid']} ~ {s['last_valid']}]"
              f"  min={s['min']}  max={s['max']}")

    # --- Event list for label_10pct ---
    # Find contiguous label_10pct periods and extract representative events
    print("\n=== Label_10pct Events ===")
    label10 = panel["label_10pct"].dropna()
    events_10 = extract_events(panel, "label_10pct", -0.10)
    events_20 = extract_events(panel, "label_20pct", -0.20)
    v_count = int(panel["is_v_shape"].dropna().sum())
    print(f"  label_10pct events: {len(events_10)}")
    print(f"  label_20pct events: {len(events_20)}")
    print(f"  is_v_shape events:  {v_count}")
    print("\n  Top 10% events (peak_date, trough_approx, drawdown, pt_days, v_shape):")
    for ev in events_10[:20]:
        print(f"    {ev['peak_date']}  drawdown={ev['drawdown']:+.1%}  "
              f"pt_days={ev['pt_days']}  v={ev['is_v']}")

    # --- Write summary MD ---
    md = build_summary_md(panel, sanity_rows, events_10, events_20, v_count, trade_index)
    out_md = reports_dir / "crash_predictor_tw_panel_summary.md"
    out_md.write_text(md, encoding="utf-8")
    print(f"\n  Saved summary: {out_md}")

    return panel, events_10, events_20


def extract_events(panel: pd.DataFrame, label_col: str, threshold: float) -> list[dict]:
    """
    Extract distinct crash events from label column.
    A new event starts when label=1 is first triggered after a gap of 0.
    Returns list of event dicts sorted by date.
    """
    lbl = panel[label_col].dropna()
    if lbl.empty:
        return []

    events = []
    in_event = False
    for dt, val in lbl.items():
        if val == 1 and not in_event:
            in_event = True
            # peak_date is the date 60d after which the drawdown was measured
            # (actually dt is the signal date; the peak happens within t+60d)
            row = panel.loc[dt]
            events.append({
                "signal_date": dt,
                "peak_date": dt,
                "drawdown": float(row.get("forward_60d_pt_drawdown", np.nan)),
                "pt_days": int(row.get("peak_to_trough_days", 0)) if not pd.isna(row.get("peak_to_trough_days", np.nan)) else None,
                "is_v": int(row.get("is_v_shape", 0)) if not pd.isna(row.get("is_v_shape", np.nan)) else None,
            })
        elif val == 0:
            in_event = False

    return events


def build_summary_md(
    panel: pd.DataFrame,
    sanity_rows: list[dict],
    events_10: list[dict],
    events_20: list[dict],
    v_count: int,
    trade_index: pd.DatetimeIndex,
) -> str:
    lines = [
        "# TW Crash Predictor Panel Summary",
        "",
        f"**Panel range**: {trade_index[0].date()} ~ {trade_index[-1].date()} "
        f"({len(trade_index)} trading days)",
        f"**label_10pct events**: {len(events_10)}  "
        f"| **label_20pct events**: {len(events_20)}  "
        f"| **is_v_shape events**: {v_count}",
        "",
        "## Column Validity",
        "",
        "| Column | First Valid | Last Valid | Null% | Min | Max |",
        "|--------|-------------|------------|-------|-----|-----|",
    ]
    for s in sanity_rows:
        lines.append(
            f"| {s['name']} | {s['first_valid']} | {s['last_valid']} "
            f"| {s['null_pct']}% | {s['min']} | {s['max']} |"
        )

    lines += [
        "",
        "## Known Data Gaps",
        "",
        "| Factor | Status | Reason |",
        "|--------|--------|--------|",
        "| twdusd_1d_chg | N/A | No TWD/USD cache in data_cache/ or data/ |",
        "| twdusd_60d_rank | N/A | No TWD/USD cache in data_cache/ or data/ |",
        "| breadth_up_pct | N/A | No pre-built advance/decline daily panel; per-stock CSVs exist but no aggregate |",
        "| atm_put_z | Sparse (4 rows) | data/sentiment/atm_put_premium.parquet only has 2026-05 data |",
        "| mtx_txf_ratio | Sparse (4 rows) | data/sentiment/minifutures_ratio.parquet only has 2026-05 data |",
        "| tdcc_large_7d_delta | Sparse (3 weekly snapshots) | TDCC only has 2026-04-17 to 2026-04-30; no weekly history |",
        "| foreign_5d_z / foreign_20d_z | From 2015-01-05 | institutional.parquet starts 2015 |",
        "| margin_7d_z / short_margin_ratio | From 2021-04-16 | chip_history/margin.parquet starts 2021 |",
        "| TAIEX close (all labels) | From 1999-01-05 | FinMind TAIEX dataset starts 1999; pre-1999 not in any cache |",
        "",
        "## Label_10pct Events (>=10% forward 60d peak-to-trough)",
        "",
        "| Signal Date | Drawdown | P2T Days | V-Shape |",
        "|-------------|----------|----------|---------|",
    ]
    for ev in events_10:
        dd = f"{ev['drawdown']:+.1%}" if ev["drawdown"] is not None and not (isinstance(ev["drawdown"], float) and np.isnan(ev["drawdown"])) else "N/A"
        pt = str(ev["pt_days"]) if ev["pt_days"] is not None else "N/A"
        vs = str(ev["is_v"]) if ev["is_v"] is not None else "N/A"
        lines.append(f"| {ev['signal_date'].date()} | {dd} | {pt} | {vs} |")

    lines += [
        "",
        "## Label_20pct Events (>=20% forward 60d peak-to-trough)",
        "",
        "| Signal Date | Drawdown | P2T Days | V-Shape |",
        "|-------------|----------|----------|---------|",
    ]
    for ev in events_20:
        dd = f"{ev['drawdown']:+.1%}" if ev["drawdown"] is not None and not (isinstance(ev["drawdown"], float) and np.isnan(ev["drawdown"])) else "N/A"
        pt = str(ev["pt_days"]) if ev["pt_days"] is not None else "N/A"
        vs = str(ev["is_v"]) if ev["is_v"] is not None else "N/A"
        lines.append(f"| {ev['signal_date'].date()} | {dd} | {pt} | {vs} |")

    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    panel, events_10, events_20 = main()
