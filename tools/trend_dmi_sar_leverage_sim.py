"""
trend_dmi_sar_leverage_sim.py

槓桿情境：load 既有 V4 變體 trades，trade-by-trade 套槓桿 + 融資利息 + ruin check，
重算每檔 equity curve / CAGR / MDD，aggregate 26-ticker universe。

槓桿 L 套用模型：
  equity_after = equity_before × (1 + L × net_ret_per_trade)
  interest_cost = equity_before × (L - 1) × (holding_days / 365) × margin_rate
  equity_after -= interest_cost
  if equity_after <= 0:  ruin → equity 歸 0, 停止後續

台股融資利率：default 6.5%/yr (券商實際 5.5~7%，取中值)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools.trend_dmi_sar_strategy import load_ohlcv, compute_indicators, run_backtest  # noqa: E402

UNIVERSE = [
    "2330", "2454", "2308", "2303", "3034", "3008", "2379",
    "2317", "2382", "2474", "2357",
    "2882", "2891", "2884", "2885", "2886",
    "1303", "1301", "2002", "1216",
    "2412", "2603", "2207", "2912", "3045",
    "0050",
]

START = "2014-01-01"
LEVERAGES = [1.0, 1.5, 2.0, 2.5, 3.0]
MARGIN_RATE = 0.065   # 6.5%/yr 台股融資利率

REPORTS_DIR = ROOT / "reports"


def simulate_levered_equity(trades: list, L: float, margin_rate: float = MARGIN_RATE) -> dict:
    """
    對單檔 trades 用 L 倍槓桿模擬 equity curve，回傳指標。
    """
    if not trades:
        return dict(L=L, n_trades=0, final_equity=1.0, cagr_pct=0, mdd_pct=0, ruined=False, ruin_date=None)

    equity = 1.0
    eq_curve = [1.0]
    ruined = False
    ruin_date = None

    for t in trades:
        if equity <= 0:
            break
        # gross trade return (decimal)
        r = t.net_return_pct / 100
        days = max(t.holding_days, 1)

        # equity after position return
        new_eq = equity * (1 + L * r)
        # interest cost (only on borrowed portion (L-1))
        interest = equity * (L - 1) * (days / 365) * margin_rate
        new_eq -= interest

        if new_eq <= 0:
            ruined = True
            ruin_date = t.exit_date
            new_eq = 0.0
            equity = 0.0
            eq_curve.append(0.0)
            break
        equity = new_eq
        eq_curve.append(equity)

    eq_curve = np.array(eq_curve)
    final = eq_curve[-1]

    # 期間
    span_days = (trades[-1].exit_date - trades[0].entry_date).days
    yrs = max(span_days / 365.25, 1 / 365.25)
    cagr = (final ** (1 / yrs) - 1) * 100 if final > 0 else -100.0

    peaks = np.maximum.accumulate(eq_curve)
    dd = (eq_curve - peaks) / peaks
    mdd = dd.min() * 100

    return dict(
        L=L,
        n_trades=len(trades),
        final_equity=round(float(final), 4),
        cagr_pct=round(float(cagr), 2),
        mdd_pct=round(float(mdd), 2),
        ruined=ruined,
        ruin_date=str(ruin_date) if ruin_date else None,
    )


def main():
    rows = []
    for t in UNIVERSE:
        try:
            df = load_ohlcv(t, start=START)
            df_ind = compute_indicators(df)
            trades = run_backtest(df_ind, t, switch_atr_mult=1.5, exit_on_adx_drop=True)  # V4
            for L in LEVERAGES:
                r = simulate_levered_equity(trades, L)
                r["ticker"] = t
                rows.append(r)
        except Exception as e:
            print(f"{t}: FAIL ({e})")

    df = pd.DataFrame(rows)
    df.to_csv(REPORTS_DIR / "trend_dmi_sar_leverage_raw.csv", index=False)

    print("=== Per-ticker (sample, 前 10 檔) ===")
    pivot_cagr = df.pivot(index="ticker", columns="L", values="cagr_pct")
    pivot_mdd = df.pivot(index="ticker", columns="L", values="mdd_pct")
    pivot_ruined = df.pivot(index="ticker", columns="L", values="ruined")

    print("\nCAGR (%) by leverage:")
    print(pivot_cagr.round(2).to_string())
    print("\nMDD (%) by leverage:")
    print(pivot_mdd.round(2).to_string())
    print("\nRuin (任一交易 equity → 0):")
    print(pivot_ruined.to_string())

    # Universe aggregate
    print("\n=== Universe aggregate (V4 strategy + leverage) ===")
    agg = df.groupby("L").agg(
        cagr_mean=("cagr_pct", "mean"),
        cagr_median=("cagr_pct", "median"),
        mdd_mean=("mdd_pct", "mean"),
        mdd_worst=("mdd_pct", "min"),
        ruin_count=("ruined", "sum"),
        n=("ticker", "count"),
    ).round(2)
    print(agg.to_string())

    agg.to_csv(REPORTS_DIR / "trend_dmi_sar_leverage_summary.csv")
    print(f"\n→ Raw: {REPORTS_DIR / 'trend_dmi_sar_leverage_raw.csv'}")
    print(f"→ Summary: {REPORTS_DIR / 'trend_dmi_sar_leverage_summary.csv'}")


if __name__ == "__main__":
    main()
