"""
VF -- 三段式獲利 only-bear regime backtest (宋分擇時 #6 二輪驗證).

Hypothesis:
  全 regime 啟用 3-stage 是 WORSE (vf_3stage_exit_backtest.py 驗過 -3.07%, Sharpe -0.114).
  原因 bull 過早出場 + MA60 break 觸發過頻 (91%).
  方向對的問題在 bear/volatile -- 那段防守該收緊。
  → 只在 bear 或 volatile regime 啟用 3-stage; bull/ranging 走純 B&H.

Definitions:
  bear regime = TWII close < TWII MA200 (entry day or trailing 5d)
  volatile = trade_journal_qm_tw.parquet baked-in regime label
  Strategy switch:
    bear OR volatile -> 3-stage exit (S1+15% / S2 動能弱 / S3 MA60 break)
    其他 -> 純 B&H 60d hold

Robustness:
  - Leave-one-year-out: hold out year Y, fit on rest, eval on Y, 跑 11 年 OOS
  - Yearly diff stability (3-stage > B&H 在 bear/volatile 才啟用 → 在那些 trade 上有 alpha?)
  - All-trades aggregate

Output:
  - reports/vf_3stage_bear_only.md
  - reports/vf_3stage_bear_only.csv (per-trade)
  - reports/vf_3stage_bear_only_yearly.csv

Verdict thresholds:
  A: diff_mean > +1.0% AND diff_sharpe > +0.10 AND >= 70% LOO years better
  B: diff_mean > +0.5% AND diff_sharpe > +0.05 AND >= 60% LOO years better
  C: tie within +/- 0.3%
  D: WORSE than baseline
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))
DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "vf_3stage_bear_only.csv"
OUT_MD = OUT_DIR / "vf_3stage_bear_only.md"
OUT_YR = OUT_DIR / "vf_3stage_bear_only_yearly.csv"
OUT_LOO = OUT_DIR / "vf_3stage_bear_only_loo.csv"

# Stage triggers (sync with vf_3stage_exit_backtest.py)
S1_GAIN_TRIGGER = 0.15
S1_SELL_PCT = 0.15
S2_5D_PEAK = 0.05
S2_5D_DROP = -0.02
S2_MA20_BREAK = True
S2_SELL_PCT = 0.25
S3_MA60_BREAK = True
S3_SELL_PCT = 1.00
HOLD_DAYS = 60
MA20_WINDOW = 20
MA60_WINDOW = 60
TWII_MA_WINDOW = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("3stage_bear")


def load_data():
    logger.info("Loading trade journal + OHLCV + TWII...")
    tj = pd.read_parquet(DATA_DIR / "trade_journal_qm_tw.parquet")
    tj["week_end_date"] = pd.to_datetime(tj["week_end_date"])
    tj["stock_id"] = tj["stock_id"].astype(str)

    px = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet",
                         columns=["stock_id", "date", "Close"])
    px["date"] = pd.to_datetime(px["date"])
    px["stock_id"] = px["stock_id"].astype(str)
    px = px.sort_values(["stock_id", "date"]).reset_index(drop=True)

    g = px.groupby("stock_id", sort=False)["Close"]
    px["MA20"] = g.transform(lambda s: s.rolling(MA20_WINDOW, min_periods=15).mean())
    px["MA60"] = g.transform(lambda s: s.rolling(MA60_WINDOW, min_periods=40).mean())

    # TWII bear regime: close < MA200
    twii = pd.read_parquet(DATA_DIR / "_twii_bench.parquet")
    # Flatten multi-level columns
    twii.columns = [c[0] if isinstance(c, tuple) else c for c in twii.columns]
    twii = twii.reset_index()
    twii["date"] = pd.to_datetime(twii["Date"] if "Date" in twii.columns else twii["date"])
    twii = twii[["date", "Close"]].sort_values("date").reset_index(drop=True)
    twii["MA200"] = twii["Close"].rolling(TWII_MA_WINDOW, min_periods=100).mean()
    twii["is_bear"] = twii["Close"] < twii["MA200"]
    logger.info(f"  trades: {len(tj):,}; OHLCV: {len(px):,} rows; TWII: {len(twii)} days; "
                f"bear days: {twii['is_bear'].sum()} ({twii['is_bear'].mean()*100:.1f}%)")
    return tj, px, twii


def simulate_3stage(entry_idx, entry_price, stock_px):
    """Same as vf_3stage_exit_backtest.simulate_one_trade."""
    n = len(stock_px)
    end_idx = min(entry_idx + HOLD_DAYS, n - 1)

    cash_position = 0.0
    remaining = 1.0
    stage_hits = {"S1": False, "S2": False, "S3": False}
    exit_day = None
    closes = stock_px["Close"].values
    ma20 = stock_px["MA20"].values
    ma60 = stock_px["MA60"].values

    s1_done = False
    five_day_window = []

    for i in range(entry_idx + 1, end_idx + 1):
        if remaining <= 1e-6:
            exit_day = i - entry_idx
            break

        cur_price = closes[i]
        if pd.isna(cur_price):
            continue
        ret_from_entry = cur_price / entry_price - 1
        five_day_window.append(ret_from_entry)
        if len(five_day_window) > 5:
            five_day_window.pop(0)

        # S3 first
        if not pd.isna(ma60[i]) and cur_price < ma60[i] and remaining > 0:
            cash_position += remaining * cur_price / entry_price
            remaining = 0
            stage_hits["S3"] = True
            exit_day = i - entry_idx
            break

        # S1
        if not s1_done and ret_from_entry >= S1_GAIN_TRIGGER:
            cash_position += S1_SELL_PCT * cur_price / entry_price
            remaining -= S1_SELL_PCT
            stage_hits["S1"] = True
            s1_done = True

        # S2
        peak_5d = max(five_day_window) if five_day_window else 0
        s2_trigger = (peak_5d >= S2_5D_PEAK and ret_from_entry <= S2_5D_DROP)
        if S2_MA20_BREAK and not pd.isna(ma20[i]) and cur_price < ma20[i] and (i - entry_idx) >= 5:
            s2_trigger = True

        if s2_trigger and not stage_hits["S2"]:
            sell_amt = min(S2_SELL_PCT, remaining)
            cash_position += sell_amt * cur_price / entry_price
            remaining -= sell_amt
            stage_hits["S2"] = True

    # End: liquidate remainder
    if remaining > 1e-6:
        end_price = closes[end_idx]
        if not pd.isna(end_price):
            cash_position += remaining * end_price / entry_price
        else:
            cash_position += remaining
        if exit_day is None:
            exit_day = end_idx - entry_idx

    return {
        "ret_3stage": cash_position - 1.0,
        "exit_day": exit_day or HOLD_DAYS,
        **{f"hit_{k}": v for k, v in stage_hits.items()},
    }


def simulate_buy_hold(entry_idx, entry_price, stock_px):
    n = len(stock_px)
    end_idx = min(entry_idx + HOLD_DAYS, n - 1)
    end_price = stock_px["Close"].values[end_idx]
    if pd.isna(end_price):
        return {"ret_bh": 0.0, "exit_day": end_idx - entry_idx}
    return {"ret_bh": end_price / entry_price - 1, "exit_day": end_idx - entry_idx}


def run_backtest(tj, px, twii):
    logger.info("Running backtest -- per-trade 3-stage + B&H + bear/volatile flag...")
    t0 = time.time()
    rows = []

    px_by_stock = {sid: g.reset_index(drop=True)
                   for sid, g in px.groupby("stock_id", sort=False)}

    twii_arr = twii["date"].values
    twii_bear = twii["is_bear"].values

    skipped = 0
    for i, trade in enumerate(tj.itertuples(index=False)):
        sid = trade.stock_id
        if sid not in px_by_stock:
            skipped += 1
            continue
        stock_px = px_by_stock[sid]
        entry_date = pd.Timestamp(trade.week_end_date) + pd.Timedelta(days=3)
        idx_arr = stock_px["date"].values
        pos = np.searchsorted(idx_arr, np.datetime64(entry_date))
        if pos >= len(stock_px):
            skipped += 1
            continue

        entry_price_actual = stock_px["Close"].values[pos]
        if pd.isna(entry_price_actual) or entry_price_actual <= 0:
            skipped += 1
            continue

        # TWII bear check at entry_date
        twii_pos = np.searchsorted(twii_arr, np.datetime64(entry_date))
        if twii_pos >= len(twii_arr):
            twii_pos = len(twii_arr) - 1
        is_bear = bool(twii_bear[twii_pos]) if not pd.isna(twii_bear[twii_pos]) else False

        # trade-level regime label
        regime = getattr(trade, "regime", "n/a")
        is_volatile = (regime == "volatile")
        # Bear-only switch: 3-stage 啟用 if bear OR volatile, else B&H
        use_3stage = is_bear or is_volatile

        res_3s = simulate_3stage(pos, entry_price_actual, stock_px)
        res_bh = simulate_buy_hold(pos, entry_price_actual, stock_px)

        # Combined strategy: bear/volatile -> 3stage; else -> B&H
        ret_combo = res_3s["ret_3stage"] if use_3stage else res_bh["ret_bh"]

        rows.append({
            "stock_id": sid,
            "entry_date": entry_date,
            "regime": regime,
            "is_bear": is_bear,
            "is_volatile": is_volatile,
            "use_3stage": use_3stage,
            "year": entry_date.year,
            "ret_3stage": res_3s["ret_3stage"],
            "ret_bh": res_bh["ret_bh"],
            "ret_combo": ret_combo,
            "hit_S1": res_3s["hit_S1"],
            "hit_S2": res_3s["hit_S2"],
            "hit_S3": res_3s["hit_S3"],
        })

        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1}/{len(tj)} trades... ({time.time()-t0:.1f}s)")

    df = pd.DataFrame(rows)
    logger.info(f"Done {len(df):,} trades, skipped {skipped} ({time.time()-t0:.1f}s)")
    logger.info(f"  Bear days entries: {df['is_bear'].sum()} ({df['is_bear'].mean()*100:.1f}%)")
    logger.info(f"  Volatile entries: {df['is_volatile'].sum()} ({df['is_volatile'].mean()*100:.1f}%)")
    logger.info(f"  use_3stage entries: {df['use_3stage'].sum()} ({df['use_3stage'].mean()*100:.1f}%)")
    return df


def metrics(rets):
    rets = pd.Series(rets).dropna()
    if len(rets) == 0:
        return {"n": 0, "mean": np.nan, "median": np.nan, "std": np.nan,
                "sharpe_ann": np.nan, "win_rate": np.nan, "max_loss": np.nan}
    std = rets.std(ddof=1)
    sharpe = (rets.mean() / std * np.sqrt(4)) if std > 0 else np.nan
    return {
        "n": len(rets),
        "mean": rets.mean(),
        "median": rets.median(),
        "std": std,
        "sharpe_ann": sharpe,
        "win_rate": (rets > 0).mean(),
        "max_loss": rets.min(),
    }


def yearly_breakdown(df):
    rows = []
    for yr, g in df.groupby("year"):
        m_combo = g["ret_combo"].mean()
        m_bh = g["ret_bh"].mean()
        m_3s = g["ret_3stage"].mean()
        # Sharpe per year (rough)
        s_combo = (g["ret_combo"].mean() / g["ret_combo"].std(ddof=1)
                   * np.sqrt(4)) if g["ret_combo"].std(ddof=1) > 0 else np.nan
        s_bh = (g["ret_bh"].mean() / g["ret_bh"].std(ddof=1)
                * np.sqrt(4)) if g["ret_bh"].std(ddof=1) > 0 else np.nan
        rows.append({
            "year": int(yr),
            "n": len(g),
            "n_bear": int(g["is_bear"].sum()),
            "n_volatile": int(g["is_volatile"].sum()),
            "n_use_3stage": int(g["use_3stage"].sum()),
            "ret_combo": m_combo,
            "ret_bh": m_bh,
            "ret_3stage_all": m_3s,
            "diff_combo_vs_bh": m_combo - m_bh,
            "sharpe_combo": s_combo,
            "sharpe_bh": s_bh,
            "win_combo": (g["ret_combo"] > 0).mean(),
            "win_bh": (g["ret_bh"] > 0).mean(),
        })
    return pd.DataFrame(rows)


def leave_one_year_out(df):
    """Each year held out as OOS; we just report year-by-year diff in/out sample.
    Simple version: per-year combo vs bh aggregate (no parameter fitting since
    the policy is fixed -- LOO here means 'out-of-sample stability test')."""
    rows = []
    for yr in sorted(df["year"].unique()):
        oos = df[df["year"] == yr]
        ins = df[df["year"] != yr]
        m_oos = metrics(oos["ret_combo"])
        m_oos_bh = metrics(oos["ret_bh"])
        m_ins = metrics(ins["ret_combo"])
        m_ins_bh = metrics(ins["ret_bh"])
        rows.append({
            "oos_year": int(yr),
            "n_oos": len(oos),
            "oos_combo_mean": m_oos["mean"],
            "oos_bh_mean": m_oos_bh["mean"],
            "oos_diff": m_oos["mean"] - m_oos_bh["mean"],
            "oos_combo_sharpe": m_oos["sharpe_ann"],
            "oos_bh_sharpe": m_oos_bh["sharpe_ann"],
            "oos_combo_win": m_oos["win_rate"],
            "oos_bh_win": m_oos_bh["win_rate"],
            "ins_combo_mean": m_ins["mean"],
            "ins_bh_mean": m_ins_bh["mean"],
            "ins_diff": m_ins["mean"] - m_ins_bh["mean"],
        })
    return pd.DataFrame(rows)


def conditional_breakdown(df):
    """Compare 3-stage vs B&H *within* each regime bucket -- shows whether the
    bear-only policy would even add alpha if you could perfectly timing it."""
    rows = []
    for label, mask in [
        ("all", pd.Series(True, index=df.index)),
        ("bear_or_volatile", df["use_3stage"]),
        ("bull_ranging", ~df["use_3stage"]),
        ("bear_only", df["is_bear"]),
        ("volatile_only", df["is_volatile"]),
        ("non_bear_non_volatile", ~df["is_bear"] & ~df["is_volatile"]),
    ]:
        g = df[mask]
        if len(g) == 0:
            continue
        m_3s = metrics(g["ret_3stage"])
        m_bh = metrics(g["ret_bh"])
        rows.append({
            "subset": label,
            "n": len(g),
            "ret_3stage_mean": m_3s["mean"],
            "ret_bh_mean": m_bh["mean"],
            "diff": m_3s["mean"] - m_bh["mean"],
            "sharpe_3stage": m_3s["sharpe_ann"],
            "sharpe_bh": m_bh["sharpe_ann"],
            "win_3stage": m_3s["win_rate"],
            "win_bh": m_bh["win_rate"],
        })
    return pd.DataFrame(rows)


def assess_verdict(combo_m, bh_m, yr_df, loo_df):
    diff_mean = combo_m["mean"] - bh_m["mean"]
    diff_sharpe = combo_m["sharpe_ann"] - bh_m["sharpe_ann"]
    yrs_better = (yr_df["diff_combo_vs_bh"] > 0).sum()
    yrs_total = len(yr_df)
    loo_better = (loo_df["oos_diff"] > 0).sum()
    loo_total = len(loo_df)

    if diff_mean > 0.010 and diff_sharpe > 0.10 and yrs_better >= yrs_total * 0.7 and loo_better >= loo_total * 0.7:
        verdict = "A"
    elif diff_mean > 0.005 and diff_sharpe > 0.05 and yrs_better >= yrs_total * 0.6 and loo_better >= loo_total * 0.6:
        verdict = "B"
    elif abs(diff_mean) < 0.003:
        verdict = "C"
    else:
        verdict = "D"
    return verdict, diff_mean, diff_sharpe, yrs_better, yrs_total, loo_better, loo_total


def main():
    tj, px, twii = load_data()
    df = run_backtest(tj, px, twii)
    if df.empty:
        logger.error("No backtest rows produced")
        return

    # All-trades aggregate
    summary = {
        "combo (bear-only 3stage)": metrics(df["ret_combo"]),
        "buy_hold (60d)": metrics(df["ret_bh"]),
        "all_3stage (always on)": metrics(df["ret_3stage"]),
    }

    yr_df = yearly_breakdown(df)
    loo_df = leave_one_year_out(df)
    cond_df = conditional_breakdown(df)

    verdict, diff_mean, diff_sharpe, yrs_better, yrs_total, loo_better, loo_total = \
        assess_verdict(summary["combo (bear-only 3stage)"], summary["buy_hold (60d)"], yr_df, loo_df)

    # Print
    print("\n=== 三段式 only-bear regime vs B&H Backtest ===\n")
    print(f"Total trades: {len(df):,} (use_3stage on: {df['use_3stage'].sum()}, "
          f"{df['use_3stage'].mean()*100:.1f}%)")
    print(f"  bear days entries: {df['is_bear'].sum()} ({df['is_bear'].mean()*100:.1f}%)")
    print(f"  volatile entries:  {df['is_volatile'].sum()} ({df['is_volatile'].mean()*100:.1f}%)")
    print()
    print(f"{'Strategy':28s} {'n':>5s} {'mean':>8s} {'med':>8s} {'std':>7s} "
          f"{'Sharpe':>8s} {'win':>6s} {'maxL':>9s}")
    print("-" * 92)
    for k, m in summary.items():
        print(f"{k:28s} {m['n']:>5d} {m['mean']:>+8.4f} {m['median']:>+8.4f} "
              f"{m['std']:>+7.4f} {m['sharpe_ann']:>+8.3f} {m['win_rate']:>6.3f} "
              f"{m['max_loss']:>+9.4f}")

    print(f"\nConditional breakdown (3-stage vs B&H per regime):")
    print(cond_df.to_string(index=False))

    print(f"\nYearly breakdown (combo vs B&H):")
    print(yr_df[["year", "n", "n_use_3stage", "ret_combo", "ret_bh", "diff_combo_vs_bh",
                 "sharpe_combo", "sharpe_bh", "win_combo", "win_bh"]].to_string(index=False))

    print(f"\nLeave-one-year-out OOS:")
    print(loo_df[["oos_year", "n_oos", "oos_combo_mean", "oos_bh_mean", "oos_diff",
                  "oos_combo_sharpe", "oos_bh_sharpe"]].to_string(index=False))

    # Save
    df.to_csv(OUT_CSV, index=False)
    yr_df.to_csv(OUT_YR, index=False)
    loo_df.to_csv(OUT_LOO, index=False)

    # MD
    md = f"""# 三段式 only-bear regime vs Buy & Hold (60d) Backtest

**Date**: 2026-04-29
**Universe**: trade_journal_qm_tw.parquet ({len(df):,} trades 2015-2025)
**Strategy switch**: bear (TWII<MA200) OR volatile (trade-level regime) -> 3-stage exit; else B&H 60d

## Trigger Coverage

- bear-day entries: {df['is_bear'].sum()} ({df['is_bear'].mean()*100:.1f}%)
- volatile-regime entries: {df['is_volatile'].sum()} ({df['is_volatile'].mean()*100:.1f}%)
- use_3stage union: {df['use_3stage'].sum()} ({df['use_3stage'].mean()*100:.1f}%)

## Aggregate Metrics

| Strategy | n | mean | median | std | Sharpe | win | max_loss |
|---|---:|---:|---:|---:|---:|---:|---:|
"""
    for k, m in summary.items():
        md += (f"| {k} | {m['n']} | {m['mean']:+.4f} | {m['median']:+.4f} | {m['std']:+.4f} | "
               f"{m['sharpe_ann']:+.3f} | {m['win_rate']:.3f} | {m['max_loss']:+.4f} |\n")

    md += f"""
## Conditional breakdown (3-stage vs B&H within each subset)

| subset | n | 3stage_mean | bh_mean | diff | sharpe_3s | sharpe_bh | win_3s | win_bh |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
"""
    for _, r in cond_df.iterrows():
        md += (f"| {r['subset']} | {r['n']} | {r['ret_3stage_mean']:+.4f} | "
               f"{r['ret_bh_mean']:+.4f} | {r['diff']:+.4f} | "
               f"{r['sharpe_3stage']:+.3f} | {r['sharpe_bh']:+.3f} | "
               f"{r['win_3stage']:.3f} | {r['win_bh']:.3f} |\n")

    md += f"""
## Yearly Breakdown (combo bear-only vs B&H)

| year | n | n_use_3s | combo | bh | diff | sharpe_combo | sharpe_bh | win_combo | win_bh |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
"""
    for _, r in yr_df.iterrows():
        md += (f"| {r['year']} | {r['n']} | {r['n_use_3stage']} | "
               f"{r['ret_combo']:+.4f} | {r['ret_bh']:+.4f} | {r['diff_combo_vs_bh']:+.4f} | "
               f"{r['sharpe_combo']:+.3f} | {r['sharpe_bh']:+.3f} | "
               f"{r['win_combo']:.3f} | {r['win_bh']:.3f} |\n")

    md += f"""
## Leave-One-Year-Out OOS

| oos_year | n_oos | combo_mean | bh_mean | diff | combo_sharpe | bh_sharpe | combo_win | bh_win |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
"""
    for _, r in loo_df.iterrows():
        md += (f"| {r['oos_year']} | {r['n_oos']} | {r['oos_combo_mean']:+.4f} | "
               f"{r['oos_bh_mean']:+.4f} | {r['oos_diff']:+.4f} | "
               f"{r['oos_combo_sharpe']:+.3f} | {r['oos_bh_sharpe']:+.3f} | "
               f"{r['oos_combo_win']:.3f} | {r['oos_bh_win']:.3f} |\n")

    md += f"""
## Verdict: {verdict}

- diff_mean (combo - B&H): {diff_mean:+.4f}
- diff_sharpe: {diff_sharpe:+.3f}
- combo better in {yrs_better}/{yrs_total} years
- LOO OOS: combo better in {loo_better}/{loo_total} years

### Thresholds
- A: diff_mean > +1.0% AND diff_sharpe > +0.10 AND >= 70% years better AND >= 70% LOO years
- B: diff_mean > +0.5% AND diff_sharpe > +0.05 AND >= 60% years better AND >= 60% LOO years
- C: tie within +/- 0.3%
- D: WORSE than baseline

### Note
Stage 3 (MA60 break) 在 vf_3stage_exit_backtest 全 regime 啟用時 trigger 91% 過頻,
本次只在 bear/volatile 啟用 -- 看 conditional breakdown subset 分組是否有 alpha.
"""
    OUT_MD.write_text(md, encoding="utf-8")
    print(f"\n=== Verdict: {verdict} (diff_mean={diff_mean:+.4f}, "
          f"diff_sharpe={diff_sharpe:+.3f}, "
          f"years_better={yrs_better}/{yrs_total}, "
          f"loo={loo_better}/{loo_total}) ===")
    print(f"Saved: {OUT_MD}")


if __name__ == "__main__":
    main()
