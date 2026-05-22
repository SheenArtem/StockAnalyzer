"""Quantify whale_picks "sold too early" cost.

For each closed position with PnL >= 0 (winners that got cut), compute the stock's
post-exit return over 30d / 60d / 90d windows. Aggregate to see how much alpha
the monthly rebalance discipline left on the table.
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import timedelta

LEDGER = Path("data/whale_picks/trade_ledger.parquet")
CACHE_DIR = Path("data_cache")


def load_price(stock_id: str) -> pd.DataFrame | None:
    p = CACHE_DIR / f"{stock_id}_price.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, parse_dates=[0])
    df = df.rename(columns={df.columns[0]: "date"}).set_index("date")
    return df


def post_exit_return(stock_id: str, exit_date, exit_price: float, days: int) -> float | None:
    df = load_price(stock_id)
    if df is None or df.empty:
        return None
    target_date = pd.Timestamp(exit_date) + timedelta(days=days)
    # Find closest trading day on or before target_date
    after = df.loc[df.index >= pd.Timestamp(exit_date) + timedelta(days=1)]
    if after.empty:
        return None
    window = after.loc[after.index <= target_date]
    if window.empty:
        return None
    # Use the last close within the window (closest to "days after")
    fwd_price = window["Close"].iloc[-1]
    if pd.isna(fwd_price) or pd.isna(exit_price) or exit_price <= 0:
        return None
    return float(fwd_price / exit_price - 1.0)


def max_post_exit_return(stock_id: str, exit_date, exit_price: float, days: int) -> float | None:
    """Best (highest) close achieved within days after exit."""
    df = load_price(stock_id)
    if df is None or df.empty:
        return None
    after = df.loc[
        (df.index >= pd.Timestamp(exit_date) + timedelta(days=1))
        & (df.index <= pd.Timestamp(exit_date) + timedelta(days=days))
    ]
    if after.empty:
        return None
    peak = after["Close"].max()
    if pd.isna(peak) or pd.isna(exit_price) or exit_price <= 0:
        return None
    return float(peak / exit_price - 1.0)


def main():
    df = pd.read_parquet(LEDGER)
    closed = df[~df["still_holding"]].copy()
    closed["exit_date"] = pd.to_datetime(closed["exit_date"])

    print(f"Closed positions: {len(closed)}")
    print(f"Date range: {closed['exit_date'].min().date()} -> {closed['exit_date'].max().date()}")
    print()

    rows = []
    missing = 0
    for _, r in closed.iterrows():
        sid = str(r["stock_id"])
        ex_date = r["exit_date"]
        ex_price = float(r["exit_price"])
        if pd.isna(ex_price):
            missing += 1
            continue
        ret30 = post_exit_return(sid, ex_date, ex_price, 30)
        ret60 = post_exit_return(sid, ex_date, ex_price, 60)
        ret90 = post_exit_return(sid, ex_date, ex_price, 90)
        peak90 = max_post_exit_return(sid, ex_date, ex_price, 90)
        if ret30 is None and ret60 is None and ret90 is None:
            missing += 1
            continue
        rows.append({
            "stock_id": sid,
            "stock_name": r["stock_name"],
            "entry_date": r["entry_date"],
            "exit_date": ex_date.date(),
            "pnl_pct": r["pnl_pct"],
            "ret_30d_after": ret30,
            "ret_60d_after": ret60,
            "ret_90d_after": ret90,
            "peak_90d_after": peak90,
        })
    print(f"Analyzed: {len(rows)} / Missing price data: {missing}")
    print()

    out = pd.DataFrame(rows)

    # ============================================================
    # Overall post-exit return distribution
    # ============================================================
    print("=" * 70)
    print("Post-exit return distribution (ALL closed positions)")
    print("=" * 70)
    for col in ["ret_30d_after", "ret_60d_after", "ret_90d_after", "peak_90d_after"]:
        s = out[col].dropna()
        print(f"\n{col}  (N={len(s)})")
        print(f"  mean    {s.mean()*100:+.2f}%")
        print(f"  median  {s.median()*100:+.2f}%")
        print(f"  p75     {s.quantile(0.75)*100:+.2f}%")
        print(f"  p90     {s.quantile(0.90)*100:+.2f}%")
        print(f"  max     {s.max()*100:+.2f}%")
        print(f"  % positive  {(s > 0).mean()*100:.1f}%")
        print(f"  % >+10%     {(s > 0.10).mean()*100:.1f}%")
        print(f"  % >+20%     {(s > 0.20).mean()*100:.1f}%")

    # ============================================================
    # Winners-that-got-sold (PnL >= 0 at exit) — the "sold too early" cohort
    # ============================================================
    winners = out[out["pnl_pct"] >= 0].copy()
    print()
    print("=" * 70)
    print(f"WINNERS-AT-EXIT cohort (PnL>=0 at exit, N={len(winners)})")
    print("  → these are the 'sold too early' candidates")
    print("=" * 70)
    for col in ["ret_30d_after", "ret_60d_after", "ret_90d_after", "peak_90d_after"]:
        s = winners[col].dropna()
        print(f"\n{col}  (N={len(s)})")
        print(f"  median  {s.median()*100:+.2f}%")
        print(f"  p75     {s.quantile(0.75)*100:+.2f}%")
        print(f"  p90     {s.quantile(0.90)*100:+.2f}%")
        print(f"  max     {s.max()*100:+.2f}%")
        print(f"  % >+10%     {(s > 0.10).mean()*100:.1f}%")
        print(f"  % >+20%     {(s > 0.20).mean()*100:.1f}%")
        print(f"  % >+50%     {(s > 0.50).mean()*100:.1f}%")

    # ============================================================
    # Top 10 "sold too early" cases (by 90d peak after exit)
    # ============================================================
    print()
    print("=" * 70)
    print("TOP 10 'sold too early' cases (by peak_90d_after)")
    print("=" * 70)
    top = winners.dropna(subset=["peak_90d_after"]).nlargest(10, "peak_90d_after")
    cols = ["stock_id", "stock_name", "exit_date", "pnl_pct", "ret_30d_after", "ret_90d_after", "peak_90d_after"]
    fmt = top[cols].copy()
    for c in ["pnl_pct", "ret_30d_after", "ret_90d_after", "peak_90d_after"]:
        fmt[c] = fmt[c].apply(lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "—")
    print(fmt.to_string(index=False))

    # ============================================================
    # Aggregate cost: average alpha left on the table per position
    # ============================================================
    print()
    print("=" * 70)
    print("AGGREGATE 'sold too early' cost")
    print("=" * 70)
    s90 = winners["ret_90d_after"].dropna()
    pk90 = winners["peak_90d_after"].dropna()
    print(f"Mean 90d post-exit return (winners cohort):  {s90.mean()*100:+.2f}%")
    print(f"Mean 90d post-exit peak    (winners cohort): {pk90.mean()*100:+.2f}%")
    print()
    print("Reading: if we extended every 'winner' hold by 90d, average extra return.")
    print("(positive = sold too early on average / negative = sold at right time)")

    out_path = Path("reports/whale_picks_sold_too_early.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"\nFull table saved: {out_path}")


if __name__ == "__main__":
    main()
