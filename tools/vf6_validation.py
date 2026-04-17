"""VF-6 QM 左右側混合矛盾 — 正式 IC / Walk-forward 驗證.

輸入: 三份 trade_journal (mixed / pure_right / pure_left) + 原 trade_journal_qm_tw.parquet
輸出:
- reports/vf6_left_right_validation.md
- reports/vf6_walkforward.csv
- reports/vf6_by_regime.csv
- reports/vf6_trend_sweep.csv
- reports/vf6_tail_risk.md

不改 momentum_screener.py。純驗證腳本。
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
BACKTEST = ROOT / "data_cache" / "backtest"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

MODES = {
    "mixed": BACKTEST / "trade_journal_qm_tw_mixed.parquet",
    "pure_right": BACKTEST / "trade_journal_qm_tw_pure_right.parquet",
    "pure_left": BACKTEST / "trade_journal_qm_tw_pure_left.parquet",
}


# --------------------------------------------------------------------------- #
# 讀檔 + 權重設定                                                               #
# --------------------------------------------------------------------------- #
def load_journals() -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for k, p in MODES.items():
        df = pd.read_parquet(p)
        df["week_end_date"] = pd.to_datetime(df["week_end_date"])
        out[k] = df.sort_values(["week_end_date", "stock_id"]).reset_index(drop=True)
    return out


def weekly_equal_weight(df: pd.DataFrame, horizon: str) -> pd.Series:
    """每週 equal-weight portfolio 的 fwd 報酬序列, index=week_end_date."""
    return (
        df.dropna(subset=[horizon])
        .groupby("week_end_date")[horizon]
        .mean()
        .sort_index()
    )


def compute_portfolio_stats(weekly_ret: pd.Series, period_per_year: float = 52.0) -> dict:
    """equal-weight portfolio 的統計指標. fwd 單位=小數(0.032=3.2%).

    注意: trade_journal 每週產生 picks, fwd_20d 本身 = 20 日報酬, 若每週再取一次
    等同 4 週重疊. 這裡的 sharpe 是 nominal weekly basis, 保守作三版**相對**比較用.
    複利 equity curve 不代表真實績效 (會過度放大), 僅用於 relative DD 比較.
    """
    if weekly_ret.empty:
        return {}
    mean = weekly_ret.mean()
    std = weekly_ret.std(ddof=1)
    downside = weekly_ret[weekly_ret < 0].std(ddof=1) if (weekly_ret < 0).any() else np.nan
    sharpe = mean / std * np.sqrt(period_per_year) if std and std > 0 else np.nan
    sortino = mean / downside * np.sqrt(period_per_year) if downside and downside > 0 else np.nan

    # Non-compound equity (簡單加總) — 比複利更能反映純 alpha 差異 + DD 可比性
    equity_simple = weekly_ret.cumsum()
    peak_simple = equity_simple.cummax()
    dd_simple = (equity_simple - peak_simple) * 100.0   # 累積損失 % (不是複利)
    max_dd_simple = dd_simple.min()

    # 20d rolling window max drawdown on weekly series
    rolling_4w = weekly_ret.rolling(4, min_periods=1).sum()
    worst_4w = rolling_4w.min() * 100.0

    return {
        "n_weeks": len(weekly_ret),
        "mean_pct": mean * 100.0,                    # 單期平均報酬 (20d 持有)
        "std_pct": std * 100.0,
        "sharpe": sharpe,                             # nominal weekly annualized
        "sortino": sortino,
        "win_rate_pct": (weekly_ret > 0).mean() * 100.0,
        # non-compound cumulative alpha (純加總, 真實 alpha proxy)
        "sum_alpha_pct": equity_simple.iloc[-1] * 100.0,
        # non-compound max "loss from peak" (%)
        "simple_max_dd_pct": max_dd_simple,
        # 連續 4 週最差報酬 (代表 ~1 個月 DD 惡化上限)
        "worst_4w_pct": worst_4w,
    }


def compute_trade_stats(df: pd.DataFrame, horizon: str) -> dict:
    """逐筆交易視角 (非 portfolio). fwd 單位=小數."""
    s = df[horizon].dropna()
    if s.empty:
        return {}
    return {
        "n_trades": len(s),
        "mean_pct": s.mean() * 100.0,
        "median_pct": s.median() * 100.0,
        "std_pct": s.std() * 100.0,
        "win_rate_pct": (s > 0).mean() * 100.0,
        "p05_pct": s.quantile(0.05) * 100.0,
        "p95_pct": s.quantile(0.95) * 100.0,
    }


# --------------------------------------------------------------------------- #
# Non-overlapping rebalance (每 4 週取一次, 對應 fwd_20d 持有期)                 #
# --------------------------------------------------------------------------- #
def non_overlap_monthly_stats(df: pd.DataFrame, horizon: str = "fwd_20d") -> dict:
    """每 4 週 rebalance 一次, 吃 fwd_20d 當該月報酬. 可比 DD / compound."""
    wk = weekly_equal_weight(df, horizon)
    # 取 index 為 date 的序列, 從第 0 週每 4 週取一次
    sample = wk.iloc[::4]
    if sample.empty:
        return {}
    equity = (1.0 + sample).cumprod()
    peak = equity.cummax()
    dd = (equity / peak - 1.0) * 100.0
    cagr = (equity.iloc[-1]) ** (12 / len(sample)) - 1.0 if len(sample) > 0 else np.nan
    mean = sample.mean()
    std = sample.std(ddof=1)
    sharpe = mean / std * np.sqrt(12) if std and std > 0 else np.nan
    return {
        "n_periods": len(sample),
        "mean_per_period_pct": mean * 100.0,
        "std_per_period_pct": std * 100.0,
        "sharpe_annualized": sharpe,
        "compound_return_pct": (equity.iloc[-1] - 1.0) * 100.0,
        "cagr_pct": cagr * 100.0,
        "max_drawdown_pct": dd.min(),
        "win_rate_pct": (sample > 0).mean() * 100.0,
    }


# --------------------------------------------------------------------------- #
# Test A: 三版基本績效 + Regime                                                 #
# --------------------------------------------------------------------------- #
def test_a_overall(journals: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    # fwd_20d 每週=4 週期, 年化 period_per_year 改 52/4=13; fwd_40d 同理.
    # 但因 trade_journal 每週產生一組 picks, 重疊會產生 autocorr.
    # 用每週 fwd_20d mean 當 portfolio 週報酬 (重疊 4 週=nominal), 年化用 52 保守比較.
    period_map = {"fwd_5d": 52.0, "fwd_10d": 52.0, "fwd_20d": 52.0, "fwd_40d": 52.0, "fwd_60d": 52.0}
    for mode, df in journals.items():
        for h, ppy in period_map.items():
            # portfolio 視角
            wk = weekly_equal_weight(df, h)
            ps = compute_portfolio_stats(wk, period_per_year=ppy)
            # trade 視角
            ts = compute_trade_stats(df, h)
            rows.append({
                "mode": mode,
                "horizon": h,
                "picks_per_week_mean": df.groupby("week_end_date").size().mean(),
                **{f"port_{k}": v for k, v in ps.items()},
                **{f"trade_{k}": v for k, v in ts.items()},
            })
    return pd.DataFrame(rows)


def test_a_by_regime(journals: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for mode, df in journals.items():
        for regime, sub in df.groupby("regime"):
            for h in ["fwd_20d", "fwd_40d"]:
                wk = weekly_equal_weight(sub, h)
                ps = compute_portfolio_stats(wk)
                ts = compute_trade_stats(sub, h)
                rows.append({
                    "mode": mode,
                    "regime": regime,
                    "horizon": h,
                    "n_trades": len(sub),
                    "n_weeks": ps.get("n_weeks", 0),
                    "picks_per_week": sub.groupby("week_end_date").size().mean(),
                    "port_mean_pct": ps.get("mean_pct"),
                    "port_sharpe": ps.get("sharpe"),
                    "port_simple_max_dd_pct": ps.get("simple_max_dd_pct"),
                    "port_worst_4w_pct": ps.get("worst_4w_pct"),
                    "trade_mean_pct": ts.get("mean_pct"),
                    "trade_win_rate_pct": ts.get("win_rate_pct"),
                })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Test B: Walk-Forward                                                         #
# --------------------------------------------------------------------------- #
def test_b_walkforward(
    journals: dict[str, pd.DataFrame],
    train_weeks: int = 12,
    test_weeks: int = 4,
    horizon: str = "fwd_20d",
) -> pd.DataFrame:
    """Rolling: 每窗計算三版的 test-period portfolio mean / sharpe, 並排名."""
    # 對齊三版的週日期 - 取 union (有空的版本該週視為 NaN)
    all_weeks = sorted(
        set().union(*[set(df["week_end_date"].unique()) for df in journals.values()])
    )
    all_weeks = pd.to_datetime(sorted(all_weeks))

    # 每版 weekly mean series
    wkly = {m: weekly_equal_weight(df, horizon).reindex(all_weeks) for m, df in journals.items()}

    rows = []
    stride = test_weeks  # 不重疊 test window
    for start in range(0, len(all_weeks) - train_weeks - test_weeks + 1, stride):
        train_start = all_weeks[start]
        train_end = all_weeks[start + train_weeks - 1]
        test_start = all_weeks[start + train_weeks]
        test_end = all_weeks[start + train_weeks + test_weeks - 1]

        window = {
            "train_start": train_start.date(),
            "train_end": train_end.date(),
            "test_start": test_start.date(),
            "test_end": test_end.date(),
        }
        for m, s in wkly.items():
            train = s.loc[train_start:train_end].dropna()
            test = s.loc[test_start:test_end].dropna()
            window[f"{m}_train_mean"] = train.mean() if not train.empty else np.nan
            window[f"{m}_train_sharpe"] = (
                train.mean() / train.std(ddof=1) * np.sqrt(52)
                if len(train) > 1 and train.std(ddof=1) > 0
                else np.nan
            )
            window[f"{m}_test_mean"] = test.mean() if not test.empty else np.nan
            window[f"{m}_test_sharpe"] = (
                test.mean() / test.std(ddof=1) * np.sqrt(52)
                if len(test) > 1 and test.std(ddof=1) > 0
                else np.nan
            )
        # rank on test_mean (higher better)
        test_means = {m: window[f"{m}_test_mean"] for m in journals}
        ranks = pd.Series(test_means).rank(ascending=False, method="min")
        for m in journals:
            window[f"{m}_test_rank"] = ranks[m]
        rows.append(window)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Test C: Trend Threshold Sweep (pure_right 上掃)                              #
# --------------------------------------------------------------------------- #
def test_c_trend_sweep(pure_right: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for thr in [6, 7, 8, 9, 10]:
        sub = pure_right[pure_right["trend_score"] >= thr]
        if sub.empty:
            continue
        picks_per_week = sub.groupby("week_end_date").size().mean()
        for h in ["fwd_20d", "fwd_40d"]:
            wk = weekly_equal_weight(sub, h)
            ps = compute_portfolio_stats(wk)
            ts = compute_trade_stats(sub, h)
            rows.append({
                "trend_threshold": thr,
                "horizon": h,
                "n_trades": len(sub),
                "picks_per_week": picks_per_week,
                "port_mean_pct": ps.get("mean_pct"),
                "port_sharpe": ps.get("sharpe"),
                "port_simple_max_dd_pct": ps.get("simple_max_dd_pct"),
                "port_sum_alpha_pct": ps.get("sum_alpha_pct"),
                "port_worst_4w_pct": ps.get("worst_4w_pct"),
                "trade_mean_pct": ts.get("mean_pct"),
                "trade_win_rate_pct": ts.get("win_rate_pct"),
                "trade_p05_pct": ts.get("p05_pct"),
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Test D: Tail Risk                                                            #
# --------------------------------------------------------------------------- #
def test_d_tail_risk(journals: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for mode, df in journals.items():
        for h, h_min in [("fwd_20d", "fwd_20d_min"), ("fwd_40d", "fwd_40d_min")]:
            s = df[h].dropna()
            smin = df[h_min].dropna()
            # 累積 portfolio DD
            wk = weekly_equal_weight(df, h)
            ps = compute_portfolio_stats(wk)
            rows.append({
                "mode": mode,
                "horizon": h,
                "n_trades": len(s),
                "trade_mean_pct": s.mean() * 100.0,
                "trade_p01_pct": s.quantile(0.01) * 100.0,
                "trade_p05_pct": s.quantile(0.05) * 100.0,
                "trade_p10_pct": s.quantile(0.10) * 100.0,
                "trade_min_pct": s.min() * 100.0,
                # 期間內最大跌幅 (fwd_xxd_min, 小數)
                "min_p05_pct": smin.quantile(0.05) * 100.0,
                "min_p10_pct": smin.quantile(0.10) * 100.0,
                # 觸及 -8% 停損比例 (小數 -0.08)
                "hit_sl_8_pct": (smin <= -0.08).mean() * 100.0 if len(smin) else np.nan,
                "hit_sl_10_pct": (smin <= -0.10).mean() * 100.0 if len(smin) else np.nan,
                "hit_sl_15_pct": (smin <= -0.15).mean() * 100.0 if len(smin) else np.nan,
                "port_simple_max_dd_pct": ps.get("simple_max_dd_pct"),
                "port_worst_4w_pct": ps.get("worst_4w_pct"),
                "port_sharpe": ps.get("sharpe"),
                "port_sortino": ps.get("sortino"),
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# main                                                                         #
# --------------------------------------------------------------------------- #
def non_overlap_all(journals: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for m, df in journals.items():
        for h in ["fwd_20d", "fwd_40d"]:
            stats = non_overlap_monthly_stats(df, h)
            rows.append({"mode": m, "horizon": h, **stats})
    return pd.DataFrame(rows)


def main() -> None:
    journals = load_journals()

    # Non-overlapping monthly rebalance (可比 DD)
    nonovl = non_overlap_all(journals)
    nonovl.to_csv(REPORTS / "vf6_nonoverlap_monthly.csv", index=False, float_format="%.4f")

    # Test A - overall
    a_all = test_a_overall(journals)
    a_all.to_csv(REPORTS / "vf6_overall.csv", index=False, float_format="%.4f")

    # Test A - by regime
    a_regime = test_a_by_regime(journals)
    a_regime.to_csv(REPORTS / "vf6_by_regime.csv", index=False, float_format="%.4f")

    # Test B - walk-forward (fwd_20d)
    b_wf = test_b_walkforward(journals, train_weeks=12, test_weeks=4, horizon="fwd_20d")
    b_wf.to_csv(REPORTS / "vf6_walkforward.csv", index=False, float_format="%.4f")

    # Test C - trend sweep
    c_sweep = test_c_trend_sweep(journals["pure_right"])
    c_sweep.to_csv(REPORTS / "vf6_trend_sweep.csv", index=False, float_format="%.4f")

    # Test D - tail risk
    d_tail = test_d_tail_risk(journals)
    d_tail.to_csv(REPORTS / "vf6_tail_risk.csv", index=False, float_format="%.4f")

    # 印出摘要
    print("=" * 60)
    print("Non-overlapping monthly (4-week rebalance, compoundable):")
    print(nonovl.to_string(index=False))
    print()
    print("Test A - Overall (fwd_20d):")
    print(a_all[a_all["horizon"] == "fwd_20d"].to_string(index=False))
    print()
    print("Test A - Overall (fwd_40d):")
    print(a_all[a_all["horizon"] == "fwd_40d"].to_string(index=False))
    print()
    print("Test B - Walk-forward summary:")
    wf_summary = b_wf[[c for c in b_wf.columns if "test_rank" in c or "test_mean" in c]].describe()
    print(wf_summary.to_string())
    # 排名分布
    print("\nTest B - test_rank value counts:")
    for m in journals:
        print(f"  {m}: {b_wf[f'{m}_test_rank'].value_counts().sort_index().to_dict()}")
    print()
    # pure_right vs mixed win rate
    if {"pure_right_test_mean", "mixed_test_mean"}.issubset(b_wf.columns):
        beats = (b_wf["pure_right_test_mean"] > b_wf["mixed_test_mean"]).sum()
        total = b_wf[["pure_right_test_mean", "mixed_test_mean"]].dropna().shape[0]
        print(f"pure_right > mixed in {beats}/{total} windows = {beats/total*100:.1f}%" if total else "N/A")
    print()
    print("Test C - Trend sweep (fwd_20d):")
    print(c_sweep[c_sweep["horizon"] == "fwd_20d"].to_string(index=False))
    print()
    print("Test D - Tail risk (fwd_20d):")
    print(d_tail[d_tail["horizon"] == "fwd_20d"].to_string(index=False))


if __name__ == "__main__":
    main()
