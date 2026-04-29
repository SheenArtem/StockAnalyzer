"""
VF — 三段式獲利了結 vs Buy & Hold backtest (宋分擇時 #6).

宋分原話：「漲一段賣 10-20% / 動能轉弱賣 20-30% / 結構破壞大幅減」

實作三段式 exit:
  Stage 1 (從買入點漲 15%): 賣 15% 部位 (區間 10-20% 中位)
  Stage 2 (動能轉弱):
      proxy: 5d 報酬從 +5% 以上回落到 -2% 以下
      or:    close 跌破 MA20 (TW 沒做 MACD/RSI 重算，用 MA20 cross 簡化代替)
      → 賣 25% 部位
  Stage 3 (結構破壞):
      close 跌破 MA60 → 賣剩餘全部
  Hard timeout: 持滿 60 個交易日 → 出清剩餘部位 (避免無限期持有)

Universe: trade_journal_qm_tw.parquet 4923 trades 2015-2025
- 每筆 trade 有 entry_price, week_end_date
- 用 OHLCV 重建每筆 trade 的 60d forward 價格
- 比較 3-stage vs Buy & Hold (一次性 60d 後出清)

Output metrics (across 4923 trades):
- Mean return / median / std / Sharpe (annualized)
- Win rate (>0 trades)
- Max single-trade loss (proxy for MDD)
- Stage trigger frequency
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "vf_3stage_exit.csv"
OUT_MD = OUT_DIR / "vf_3stage_exit.md"

# Stage triggers
S1_GAIN_TRIGGER = 0.15      # +15% gain
S1_SELL_PCT = 0.15          # 賣 15%
S2_5D_PEAK = 0.05           # 5d 高點 +5%
S2_5D_DROP = -0.02          # 5d 跌至 -2%
S2_MA20_BREAK = True        # close < MA20 觸發
S2_SELL_PCT = 0.25          # 賣 25%
S3_MA60_BREAK = True        # close < MA60 觸發
S3_SELL_PCT = 1.00          # 全清
HOLD_DAYS = 60              # 持有上限
MA20_WINDOW = 20
MA60_WINDOW = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("3stage")


def load_data():
    logger.info("Loading trade journal + OHLCV...")
    tj = pd.read_parquet(DATA_DIR / "trade_journal_qm_tw.parquet")
    tj["week_end_date"] = pd.to_datetime(tj["week_end_date"])
    tj["stock_id"] = tj["stock_id"].astype(str)

    px = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet",
                         columns=["stock_id", "date", "Close"])
    px["date"] = pd.to_datetime(px["date"])
    px["stock_id"] = px["stock_id"].astype(str)
    px = px.sort_values(["stock_id", "date"]).reset_index(drop=True)

    # Pre-compute MA20 / MA60 per stock
    g = px.groupby("stock_id", sort=False)["Close"]
    px["MA20"] = g.transform(lambda s: s.rolling(MA20_WINDOW, min_periods=15).mean())
    px["MA60"] = g.transform(lambda s: s.rolling(MA60_WINDOW, min_periods=40).mean())
    logger.info(f"  trades: {len(tj):,}; OHLCV: {len(px):,} rows")
    return tj, px


def simulate_one_trade(entry_idx, entry_price, stock_px):
    """
    Simulate single trade with 3-stage exit.

    Args:
        entry_idx: int, position in stock_px at entry day
        entry_price: float, entry price
        stock_px: DataFrame for one stock with columns date, Close, MA20, MA60

    Returns:
        dict with stage triggers, final return, holding period
    """
    n = len(stock_px)
    if entry_idx + HOLD_DAYS >= n:
        end_idx = n - 1
    else:
        end_idx = entry_idx + HOLD_DAYS

    cash_position = 0.0  # accumulated cash from partial sells
    remaining = 1.0      # share fraction still held
    stage_hits = {"S1": False, "S2": False, "S3": False}
    exit_day = None
    closes = stock_px["Close"].values
    ma20 = stock_px["MA20"].values
    ma60 = stock_px["MA60"].values

    s1_done = False
    # Track 5d window peak/drop for S2
    five_day_window = []  # (idx, return_from_entry)

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

        # --- Stage 3 first (priority: structural break exits everything) ---
        if not pd.isna(ma60[i]) and cur_price < ma60[i] and remaining > 0:
            cash_position += remaining * cur_price / entry_price
            remaining = 0
            stage_hits["S3"] = True
            exit_day = i - entry_idx
            break

        # --- Stage 1: +15% gain triggers single-time partial sell ---
        if not s1_done and ret_from_entry >= S1_GAIN_TRIGGER:
            cash_position += S1_SELL_PCT * cur_price / entry_price
            remaining -= S1_SELL_PCT
            stage_hits["S1"] = True
            s1_done = True

        # --- Stage 2: momentum reversal (5d peak->drop OR MA20 break) ---
        # Trigger 1: 5d window had +5% then current is -2%
        peak_5d = max(five_day_window) if five_day_window else 0
        s2_trigger = (peak_5d >= S2_5D_PEAK and ret_from_entry <= S2_5D_DROP)
        # Trigger 2: close drop below MA20 (only after some time so signal isn't noise)
        if S2_MA20_BREAK and not pd.isna(ma20[i]) and cur_price < ma20[i] and (i - entry_idx) >= 5:
            s2_trigger = True

        if s2_trigger and not stage_hits["S2"]:
            sell_amt = min(S2_SELL_PCT, remaining)
            cash_position += sell_amt * cur_price / entry_price
            remaining -= sell_amt
            stage_hits["S2"] = True

    # End of holding period: liquidate remainder at end_idx
    if remaining > 1e-6:
        end_price = closes[end_idx]
        if not pd.isna(end_price):
            cash_position += remaining * end_price / entry_price
        else:
            # Fall back to entry price (no signal)
            cash_position += remaining
        if exit_day is None:
            exit_day = end_idx - entry_idx

    return {
        "ret_3stage": cash_position - 1.0,
        "exit_day": exit_day or HOLD_DAYS,
        **{f"hit_{k}": v for k, v in stage_hits.items()},
    }


def simulate_buy_hold(entry_idx, entry_price, stock_px):
    """Buy at entry, hold 60 days, sell. (For comparison)"""
    n = len(stock_px)
    end_idx = min(entry_idx + HOLD_DAYS, n - 1)
    end_price = stock_px["Close"].values[end_idx]
    if pd.isna(end_price):
        return {"ret_bh": 0.0, "exit_day": end_idx - entry_idx}
    return {"ret_bh": end_price / entry_price - 1, "exit_day": end_idx - entry_idx}


def run_backtest(tj, px):
    """Iterate over trades, simulate both strategies."""
    logger.info("Running backtest on trades...")
    t0 = time.time()
    rows = []

    # Build per-stock index for fast lookup
    px_by_stock = {sid: g.reset_index(drop=True)
                   for sid, g in px.groupby("stock_id", sort=False)}

    skipped = 0
    for i, trade in enumerate(tj.itertuples(index=False)):
        sid = trade.stock_id
        if sid not in px_by_stock:
            skipped += 1
            continue
        stock_px = px_by_stock[sid]
        # Find entry index: first trading day on or after week_end_date
        # week_end_date is Friday; assume entry next trading day (Monday)
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

        res_3s = simulate_one_trade(pos, entry_price_actual, stock_px)
        res_bh = simulate_buy_hold(pos, entry_price_actual, stock_px)

        rows.append({
            "stock_id": sid,
            "entry_date": entry_date,
            "regime": getattr(trade, "regime", "n/a"),
            "year": entry_date.year,
            **res_3s,
            **res_bh,
        })

        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1}/{len(tj)} trades... ({time.time()-t0:.1f}s)")

    df = pd.DataFrame(rows)
    logger.info(f"Done {len(df):,} trades, skipped {skipped} ({time.time()-t0:.1f}s)")
    return df


def summarize(df):
    """Aggregate metrics."""
    def metrics(rets):
        rets = pd.Series(rets).dropna()
        if len(rets) == 0:
            return {"n": 0, "mean": np.nan, "median": np.nan, "std": np.nan,
                    "sharpe_ann": np.nan, "win_rate": np.nan, "max_loss": np.nan}
        # Sharpe annualized: assume 60d holding -> 4 trades/yr, scale by sqrt(4)
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

    return {
        "3stage": metrics(df["ret_3stage"]),
        "buy_hold": metrics(df["ret_bh"]),
    }


def main():
    tj, px = load_data()
    results = run_backtest(tj, px)
    if results.empty:
        logger.error("No backtest rows produced")
        return

    summary = summarize(results)

    # Stage trigger frequencies
    stage_freq = {
        "S1_hit_pct": results["hit_S1"].mean(),
        "S2_hit_pct": results["hit_S2"].mean(),
        "S3_hit_pct": results["hit_S3"].mean(),
    }

    # Yearly breakdown
    yr_summary = []
    for yr, g in results.groupby("year"):
        m_3s = g["ret_3stage"].mean()
        m_bh = g["ret_bh"].mean()
        yr_summary.append({"year": int(yr), "n": len(g),
                           "ret_3stage": m_3s, "ret_bh": m_bh,
                           "diff": m_3s - m_bh})
    yr_df = pd.DataFrame(yr_summary)

    print("\n=== 三段式 vs Buy & Hold (60d) Backtest ===\n")
    print(f"Total trades: {len(results):,} / {len(tj):,} (skipped {len(tj) - len(results)})")
    print()
    print(f"{'Strategy':12s} {'n':>6s} {'mean':>8s} {'median':>8s} {'std':>8s} "
          f"{'Sharpe':>8s} {'win':>6s} {'max_loss':>10s}")
    print("-" * 80)
    for k, m in summary.items():
        print(f"{k:12s} {m['n']:>6d} {m['mean']:>+8.4f} {m['median']:>+8.4f} "
              f"{m['std']:>+8.4f} {m['sharpe_ann']:>+8.3f} {m['win_rate']:>6.3f} "
              f"{m['max_loss']:>+10.4f}")

    print(f"\nStage trigger frequency:")
    for k, v in stage_freq.items():
        print(f"  {k}: {v*100:.1f}%")

    print(f"\nYearly breakdown:")
    print(f"{'Year':>5s} {'n':>5s} {'3stage':>9s} {'BH':>9s} {'diff':>9s}")
    for _, r in yr_df.iterrows():
        print(f"{r['year']:>5} {r['n']:>5} {r['ret_3stage']:>+9.4f} {r['ret_bh']:>+9.4f} "
              f"{r['diff']:>+9.4f}")

    # Save
    results.to_csv(OUT_CSV, index=False)
    yr_df.to_csv(OUT_DIR / "vf_3stage_exit_yearly.csv", index=False)
    logger.info(f"Saved: {OUT_CSV}")

    # Verdict
    diff_mean = summary["3stage"]["mean"] - summary["buy_hold"]["mean"]
    diff_sharpe = summary["3stage"]["sharpe_ann"] - summary["buy_hold"]["sharpe_ann"]
    yrs_3stage_better = (yr_df["diff"] > 0).sum()
    yrs_total = len(yr_df)
    verdict = ("LAND" if (diff_mean > 0.005 and diff_sharpe > 0.05 and yrs_3stage_better >= yrs_total * 0.6)
               else "TIE" if abs(diff_mean) < 0.003
               else "WORSE")

    md = f"""# 三段式獲利 vs Buy & Hold (60d hold) Backtest

**Date**: 2026-04-29
**Universe**: trade_journal_qm_tw.parquet ({len(results):,} trades 2015-2025)
**Compared**: 3-stage exit (S1 +15% / S2 動能弱 / S3 MA60 跌破) vs B&H (60d hold)

## Summary

| Strategy | n | mean | median | std | Sharpe | win_rate | max_loss |
|---|---:|---:|---:|---:|---:|---:|---:|
"""
    for k, m in summary.items():
        md += (f"| {k} | {m['n']} | {m['mean']:+.4f} | {m['median']:+.4f} | {m['std']:+.4f} | "
               f"{m['sharpe_ann']:+.3f} | {m['win_rate']:.3f} | {m['max_loss']:+.4f} |\n")

    md += f"""
## Stage Trigger Frequency

- S1 (+15% gain): {stage_freq['S1_hit_pct']*100:.1f}% of trades
- S2 (momentum reversal / MA20 break): {stage_freq['S2_hit_pct']*100:.1f}%
- S3 (MA60 break — full exit): {stage_freq['S3_hit_pct']*100:.1f}%

## Yearly Breakdown

| Year | n | 3-stage | B&H | diff |
|---:|---:|---:|---:|---:|
"""
    for _, r in yr_df.iterrows():
        md += f"| {r['year']} | {r['n']} | {r['ret_3stage']:+.4f} | {r['ret_bh']:+.4f} | {r['diff']:+.4f} |\n"

    md += f"""
## Verdict: {verdict}

Diff mean = {diff_mean:+.4f}
Diff Sharpe = {diff_sharpe:+.3f}
3-stage better in {yrs_3stage_better}/{yrs_total} years

落地門檻: diff_mean > 0.5% AND diff_sharpe > 0.05 AND >= 60% years better
"""
    OUT_MD.write_text(md, encoding="utf-8")
    print(f"\n=== Verdict: {verdict} (diff_mean={diff_mean:+.4f}, diff_sharpe={diff_sharpe:+.3f}, years_better={yrs_3stage_better}/{yrs_total}) ===")


if __name__ == "__main__":
    main()
