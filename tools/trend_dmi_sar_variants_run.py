"""
trend_dmi_sar_variants_run.py

跑 baseline + 4 個變體 × 25 檔台股 universe，輸出 5 個 summary csv +
變體×ticker matrix + universe-aggregated 對照。

變體
----
V0  baseline                            switch=1.0, adx_exit=off  (spec 字面)
V1  switch_1.5                          switch=1.5, adx_exit=off
V2  switch_2.0                          switch=2.0, adx_exit=off
V3  adx_exit                            switch=1.0, adx_exit=on
V4  switch_1.5 + adx_exit (combo)       switch=1.5, adx_exit=on

Universe (25 檔，跨產業)
------------------------
半導體：2330 2454 2308 2303 3034 3008 2379
電子代工 / PC：2317 2382 2474 2357
金融：2882 2891 2884 2885 2886
傳產：1303 1301 2002 1216
其他：2412 2603 2207 2912 3045
ETF：0050
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools.trend_dmi_sar_strategy import run_one  # noqa: E402

UNIVERSE = [
    "2330", "2454", "2308", "2303", "3034", "3008", "2379",
    "2317", "2382", "2474", "2357",
    "2882", "2891", "2884", "2885", "2886",
    "1303", "1301", "2002", "1216",
    "2412", "2603", "2207", "2912", "3045",
    "0050",
]

VARIANTS = [
    ("V0_baseline",       1.0, False),
    ("V1_switch_1.5",     1.5, False),
    ("V2_switch_2.0",     2.0, False),
    ("V3_adx_exit",       1.0, True),
    ("V4_switch_1.5+adx", 1.5, True),
]

START = "2014-01-01"
END = None
REPORTS_DIR = ROOT / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def main():
    all_rows = []
    for variant_name, switch_mult, adx_exit in VARIANTS:
        print(f"\n========== {variant_name} (switch={switch_mult}, adx_exit={adx_exit}) ==========")
        for t in UNIVERSE:
            try:
                s = run_one(
                    t, START, END,
                    save_trades=False,
                    switch_atr_mult=switch_mult,
                    exit_on_adx_drop=adx_exit,
                )
                row = {
                    "variant": variant_name,
                    "ticker": s.ticker,
                    "n_trades": s.n_trades,
                    "win_rate_pct": s.win_rate_pct,
                    "total_return_pct": s.total_return_pct,
                    "cagr_pct": s.cagr_pct,
                    "sharpe": s.sharpe,
                    "max_drawdown_pct": s.max_drawdown_pct,
                    "profit_factor": s.profit_factor if s.profit_factor != float("inf") else 999,
                    "avg_holding_days": s.avg_holding_days,
                    "bh_cagr_pct": s.bh_cagr_pct,
                    "bh_max_drawdown_pct": s.bh_max_drawdown_pct,
                }
                all_rows.append(row)
                print(f"  {t:6s} n={s.n_trades:3d} win={s.win_rate_pct:5.1f}% "
                      f"CAGR={s.cagr_pct:+6.2f}% Sharpe={s.sharpe:+.2f} "
                      f"MDD={s.max_drawdown_pct:+6.2f}%  | B&H CAGR={s.bh_cagr_pct:+6.2f}%")
            except Exception as e:
                print(f"  {t}: FAIL ({e})")

    df = pd.DataFrame(all_rows)
    raw_out = REPORTS_DIR / "trend_dmi_sar_variants_raw.csv"
    df.to_csv(raw_out, index=False)

    print("\n========== Universe Aggregated (mean across 26 tickers) ==========")
    agg = df.groupby("variant").agg(
        n_trades_mean=("n_trades", "mean"),
        win_rate_pct_mean=("win_rate_pct", "mean"),
        cagr_pct_mean=("cagr_pct", "mean"),
        cagr_pct_median=("cagr_pct", "median"),
        sharpe_mean=("sharpe", "mean"),
        mdd_pct_mean=("max_drawdown_pct", "mean"),
        win_vs_bh=("cagr_pct", lambda x: 0),  # placeholder fill below
    ).round(3)

    # 計算「跑贏 B&H 的 ticker 比例」
    df["beat_bh"] = (df["cagr_pct"] > df["bh_cagr_pct"]).astype(int)
    beat = df.groupby("variant")["beat_bh"].mean().round(3) * 100
    agg["beat_bh_pct"] = beat
    agg = agg.drop(columns=["win_vs_bh"])

    # B&H baseline (對所有 ticker mean，固定值)
    bh_mean = df.groupby("variant")["bh_cagr_pct"].mean().iloc[0]
    bh_mdd_mean = df.groupby("variant")["bh_max_drawdown_pct"].mean().iloc[0]
    print(f"\nB&H baseline (universe mean): CAGR {bh_mean:+.2f}%, MDD {bh_mdd_mean:+.2f}%\n")

    print(agg.to_string())

    summary_out = REPORTS_DIR / "trend_dmi_sar_variants_summary.csv"
    agg.to_csv(summary_out)
    print(f"\n→ Raw: {raw_out}")
    print(f"→ Summary: {summary_out}")


if __name__ == "__main__":
    main()
