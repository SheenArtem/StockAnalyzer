"""
trend_dmi_sar_quality_gate_run.py

把 trend_dmi_sar 策略接 whale_picks quality gate，看 alpha 是否出現。

⚠ Look-ahead caveat
-------------------
使用 2026-05-21 當天 whale_picks composite_score 排序：
  top_20    : 用「現在的 quality top」往回測 12 年 → 有 survivor + look-ahead 雙重偏差
  bottom_20 : 對照組，用「現在的 quality bottom (composite_score 最低)」 → 同樣偏差
  random_20 : 第三組對照 (seed=42 固定)

這是 **upper bound 估計**，不是 walk-forward。若 top_20 在這種偏好條件下仍跑輸 B&H，
quality gate 路線就直接 kill；若大勝，再規劃 walk-forward 嚴謹版。

跑 V0 (spec baseline) + V4 (switch=1.5 + adx_exit, 變體最佳) 兩條，
比較 quality gate 是否拉出策略 alpha。
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools.trend_dmi_sar_strategy import run_one  # noqa: E402

WHALE_PARQUET = ROOT / "data/whale_picks/2026-05-21.parquet"
PANEL_TW = ROOT / "data_cache/backtest/ohlcv_tw.parquet"

START = "2014-01-01"
END = None
REPORTS_DIR = ROOT / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def build_universes(n: int = 20) -> dict[str, list[str]]:
    """從 whale_picks 取 top-N / bottom-N / random-N (固定 seed)"""
    df = pd.read_parquet(WHALE_PARQUET)
    df = df.dropna(subset=["composite_score"])

    # 過濾掉 panel 沒有資料的 ticker (避免 driver crash)
    panel = pd.read_parquet(PANEL_TW)
    valid_tickers = set(panel["stock_id"].unique())
    df = df[df["stock_id"].isin(valid_tickers)].copy()

    # 至少要有 ~5 年資料 (1200 trading days) 才入選
    counts = panel["stock_id"].value_counts()
    df["history_days"] = df["stock_id"].map(counts)
    df = df[df["history_days"] >= 1200]

    sorted_by_score = df.sort_values("composite_score", ascending=False)
    top = sorted_by_score.head(n)["stock_id"].tolist()
    bottom = sorted_by_score.tail(n)["stock_id"].tolist()

    rng = np.random.default_rng(42)
    pool = sorted_by_score["stock_id"].tolist()
    random_sample = rng.choice(pool, size=n, replace=False).tolist()

    return {
        "top_20": top,
        "bottom_20": bottom,
        "random_20": random_sample,
    }


VARIANTS = [
    ("V0_baseline",       1.0, False),
    ("V4_switch_1.5+adx", 1.5, True),
]


def main():
    universes = build_universes(n=20)
    for name, tickers in universes.items():
        print(f"\n{name}: {tickers}")

    all_rows = []
    for uni_name, tickers in universes.items():
        for variant_name, switch_mult, adx_exit in VARIANTS:
            print(f"\n========== {uni_name} × {variant_name} ==========")
            for t in tickers:
                try:
                    s = run_one(
                        t, START, END,
                        save_trades=False,
                        switch_atr_mult=switch_mult,
                        exit_on_adx_drop=adx_exit,
                    )
                    row = {
                        "universe": uni_name,
                        "variant": variant_name,
                        "ticker": s.ticker,
                        "n_trades": s.n_trades,
                        "win_rate_pct": s.win_rate_pct,
                        "cagr_pct": s.cagr_pct,
                        "sharpe": s.sharpe,
                        "max_drawdown_pct": s.max_drawdown_pct,
                        "bh_cagr_pct": s.bh_cagr_pct,
                        "bh_max_drawdown_pct": s.bh_max_drawdown_pct,
                    }
                    all_rows.append(row)
                    print(f"  {t:6s} n={s.n_trades:3d} win={s.win_rate_pct:5.1f}% "
                          f"CAGR={s.cagr_pct:+7.2f}% Sharpe={s.sharpe:+.2f} "
                          f"MDD={s.max_drawdown_pct:+7.2f}%  | B&H CAGR={s.bh_cagr_pct:+7.2f}%")
                except Exception as e:
                    print(f"  {t}: FAIL ({e})")

    df = pd.DataFrame(all_rows)
    raw_out = REPORTS_DIR / "trend_dmi_sar_quality_gate_raw.csv"
    df.to_csv(raw_out, index=False)

    # 排除 CAGR <= -50 的 outlier (除權除息掉破停損 — 樣本太極端)
    df_clean = df[df["cagr_pct"] > -50].copy()
    df_clean["beat_bh"] = (df_clean["cagr_pct"] > df_clean["bh_cagr_pct"]).astype(int)

    print("\n========== Universe × Variant Aggregated (CAGR<-50% outliers excluded) ==========")
    agg = df_clean.groupby(["universe", "variant"]).agg(
        n_samples=("ticker", "count"),
        cagr_mean=("cagr_pct", "mean"),
        cagr_median=("cagr_pct", "median"),
        sharpe_mean=("sharpe", "mean"),
        mdd_mean=("max_drawdown_pct", "mean"),
        beat_bh_pct=("beat_bh", lambda x: x.mean() * 100),
        bh_cagr_mean=("bh_cagr_pct", "mean"),
        bh_mdd_mean=("bh_max_drawdown_pct", "mean"),
    ).round(2)
    print(agg.to_string())

    summary_out = REPORTS_DIR / "trend_dmi_sar_quality_gate_summary.csv"
    agg.to_csv(summary_out)
    print(f"\n→ Raw: {raw_out}")
    print(f"→ Summary: {summary_out}")

    # 額外輸出：top_20 vs bottom_20 vs random_20 直接對比 (用 V4)
    print("\n========== Quality Gate 訊號驗證 (V4 only) ==========")
    v4 = df_clean[df_clean["variant"] == "V4_switch_1.5+adx"].groupby("universe")["cagr_pct"].agg(["mean", "median", "std", "count"]).round(2)
    print(v4.to_string())
    print("→ 若 top > random > bottom，quality gate 是真訊號；否則是 post-hoc 偏差雜訊。")


if __name__ == "__main__":
    main()
