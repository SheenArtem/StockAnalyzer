"""
VF-G2 Ext: Trailing Stop vs Fixed 3-stage TP Validation (2026-04-17)

Scope:
  Test whether trailing stops can beat pure-hold 60d baseline (VF-G2 winner).
  Previous VF-G1/G2/G3 verdict: fixed SL+TP params (including current production
  3-stage ladder) all D-grade -- pure-hold 40d/60d beats them.

Hypothesis:
  Trailing stops may work better than fixed TP because they (a) have no ceiling
  (ride long-tail winners) and (b) adapt to volatility (% / ATR / EMA-based).

Exit Variants (6 groups):
  A1. pure_hold_40d   -- no exit, hold until day 40
  A2. pure_hold_60d   -- no exit, hold until day 60 (THE target to beat)
  A3. production      -- current VFG1 SL + VFG2 3-stage TP (scale 0.7-1.6, 15/25/40%)
  B1. pct_trailing    -- sweep trailing X in {5,8,10,12,15,20}%
  B2. atr_trailing    -- 22d highest - N*ATR, sweep N in {2.0, 2.5, 3.0, 3.5, 4.0}
  B3. ema_trailing    -- exit if close < daily_MA10 or MA20
  B4. atr_trailing_hard_stop -- ATR trailing + fixed hard stop (atr*3 clip 5-14%) floor
  (Bonus) atr_trailing_breakeven -- once +10% gain, lock SL at entry + trailing above

Simulation:
  - Per pick, take OHLCV for that stock from week_end_date onward (<=60 trading days).
  - Entry at day 0 close (= entry_price).
  - For each day, update trailing stop, check close-based SL trigger.
  - If triggered, exit at that day's close; compute realized ret.
  - If not triggered by day N, exit at day N close (N = 40 or 60).
  - For production: 3 tranches (1/3 each) with TP1/TP2/TP3 + SL; pessimistic (SL hits whole position).
    Close-based for SL; intraday high used to check TP touch (matches production reality).

Data:
  - Picks: trade_journal_qm_tw_pure_right.parquet (9,263 rows, VF-6 winner)
  - OHLCV: ohlcv_tw.parquet (1972 stocks x 15 yrs)
  - Optional Test E: trade_journal_qm_tw.parquet (2556 mixed rows) to confirm
    conclusion isn't VF-6 selection-dependent.

Tests:
  A. Full-sample comparison (all 9263 picks)
  B. Regime breakdown (volatile / neutral / ranging / trending)
  C. Walk-forward (12wk train + 4wk test, stride 4)
  D. Parameter sensitivity (heatmaps for pct and atr trailing sweeps)
  E. Unscreened mixed cross-check

Outputs:
  reports/vfg2ext_trailing_sweep.csv
  reports/vfg2ext_walkforward.csv
  reports/vfg2ext_by_regime.csv
  reports/vfg2ext_unscreened_crosscheck.csv
"""

import sys
from pathlib import Path
from dataclasses import dataclass

import numpy as np
import pandas as pd

ROOT = Path(r"c:\GIT\StockAnalyzer")
PICKS_PR = ROOT / "data_cache" / "backtest" / "trade_journal_qm_tw_pure_right.parquet"
PICKS_MIXED = ROOT / "data_cache" / "backtest" / "trade_journal_qm_tw.parquet"
OHLCV_PATH = ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"

OUT_SWEEP = ROOT / "reports" / "vfg2ext_trailing_sweep.csv"
OUT_WF = ROOT / "reports" / "vfg2ext_walkforward.csv"
OUT_REGIME = ROOT / "reports" / "vfg2ext_by_regime.csv"
OUT_CROSSCHECK = ROOT / "reports" / "vfg2ext_unscreened_crosscheck.csv"

# ------------------------------------------------------------
#  Exit strategy configuration
# ------------------------------------------------------------
HORIZON_DAYS = 60          # core horizon (beats 40d per VF-G2 memo)
TRAIL_WINDOW = 22          # days for chandelier 22d highest close

# Production (VFG1 SL + VFG2 3-stage TP) params
PROD_SL_MULT = 3.0
PROD_SL_FLOOR = 0.05
PROD_SL_CEIL = 0.14
PROD_TP_PCTS = (0.15, 0.25, 0.40)
PROD_TP_SCALE_FLOOR = 0.7
PROD_TP_SCALE_CEIL = 1.6
PROD_ATR_MEDIAN = 2.5
PROD_MIN_SL_GAP = 0.03
PROD_MIN_SL_GAP_ATR = 1.5

PCT_TRAIL_GRID = [0.05, 0.08, 0.10, 0.12, 0.15, 0.20]
ATR_TRAIL_GRID = [2.0, 2.5, 3.0, 3.5, 4.0]
EMA_TRAIL_GRID = [10, 20]


# ------------------------------------------------------------
#  Data loading
# ------------------------------------------------------------
def load_ohlcv_index():
    """Build {stock_id: DataFrame[date, Open, High, Low, Close, ATR14]} sorted by date."""
    print(f"[load] reading OHLCV from {OHLCV_PATH.name}")
    ohlcv = pd.read_parquet(OHLCV_PATH, columns=[
        "stock_id", "date", "Open", "High", "Low", "Close"
    ])
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv = ohlcv.sort_values(["stock_id", "date"]).reset_index(drop=True)

    # compute per-stock ATR14 (Wilder's true range, simple moving avg over 14d)
    # vectorized via groupby shift
    g = ohlcv.groupby("stock_id", sort=False)
    prev_close = g["Close"].shift(1)
    tr = np.maximum(
        ohlcv["High"] - ohlcv["Low"],
        np.maximum(
            (ohlcv["High"] - prev_close).abs(),
            (ohlcv["Low"] - prev_close).abs()
        )
    )
    # simple 14d MA (faster than Wilder; fine for trailing stop use)
    ohlcv["ATR14"] = tr.groupby(ohlcv["stock_id"]).transform(
        lambda s: s.rolling(14, min_periods=5).mean()
    )

    # index by stock_id for O(1) slicing
    print(f"[load] indexing {ohlcv['stock_id'].nunique()} stocks")
    idx = {}
    for sid, sub in ohlcv.groupby("stock_id", sort=False):
        idx[sid] = sub[["date", "Open", "High", "Low", "Close", "ATR14"]].reset_index(drop=True)
    return idx


def extract_paths(picks, ohlcv_idx, horizon=HORIZON_DAYS):
    """
    For each pick, return numpy arrays of the post-entry price path.
    Returns list of dicts with keys:
      High, Low, Close, ATR14 (np.ndarray length <= horizon+TRAIL_WINDOW)
      n_days (actual days found after entry)
      pre_high (np.ndarray of highs for TRAIL_WINDOW days BEFORE entry; for ATR trailing init)
      pre_close (ndarray of closes pre-entry)
    """
    paths = []
    missing = 0
    for _, row in picks.iterrows():
        sid = row["stock_id"]
        wk_end = pd.to_datetime(row["week_end_date"])
        sub = ohlcv_idx.get(sid)
        if sub is None:
            paths.append(None)
            missing += 1
            continue

        dates = sub["date"].values
        # Entry = first trading day strictly AFTER week_end_date OR = week_end_date close (friday)
        # pick uses entry_price = friday close, so we start from day AFTER wk_end_date
        after_idx = np.searchsorted(dates, np.datetime64(wk_end), side="right")
        if after_idx >= len(sub):
            paths.append(None)
            missing += 1
            continue

        # horizon days forward
        end_idx = min(after_idx + horizon, len(sub))
        fwd = sub.iloc[after_idx:end_idx]

        # pre-entry for trailing init
        pre_start = max(0, after_idx - TRAIL_WINDOW)
        pre = sub.iloc[pre_start:after_idx]

        paths.append({
            "High": fwd["High"].values,
            "Low": fwd["Low"].values,
            "Close": fwd["Close"].values,
            "ATR14": fwd["ATR14"].values,
            "n_days": len(fwd),
            "pre_high": pre["High"].values,
            "pre_close": pre["Close"].values,
        })
    return paths, missing


# ------------------------------------------------------------
#  Exit simulators -- per-pick, return (realized_ret, trigger_day, exited)
# ------------------------------------------------------------
def sim_pure_hold(path, entry, horizon):
    n = min(path["n_days"], horizon)
    if n < 1:
        return np.nan, -1, False
    exit_close = path["Close"][n-1]
    return (exit_close / entry) - 1.0, n-1, False


def sim_pct_trailing(path, entry, trail_pct, horizon, hard_stop_pct=None):
    """
    X% trailing on running-high close.
    hard_stop_pct (negative, e.g. -0.10): fixed floor below which we exit regardless.
    """
    n = min(path["n_days"], horizon)
    if n < 1:
        return np.nan, -1, False
    highest = entry
    for i in range(n):
        c = path["Close"][i]
        if c > highest:
            highest = c
        trail_line = highest * (1 - trail_pct)
        # hard stop floor
        if hard_stop_pct is not None:
            hs_line = entry * (1 + hard_stop_pct)
            trail_line = max(trail_line, hs_line)
        if c <= trail_line:
            return (c / entry) - 1.0, i, True
    exit_close = path["Close"][n-1]
    return (exit_close / entry) - 1.0, n-1, False


def sim_atr_trailing(path, entry, n_atr, horizon, atr_pct, hard_stop_pct=None):
    """
    Chandelier-style: exit line = (running 22d highest close) - N * ATR14.
    Uses pre-entry window to seed highest at day 0.
    """
    n = min(path["n_days"], horizon)
    if n < 1:
        return np.nan, -1, False
    # seed running window with pre-entry highs (close) + entry itself
    running = list(path["pre_close"][-TRAIL_WINDOW:]) if len(path["pre_close"]) else []
    running.append(entry)
    for i in range(n):
        c = path["Close"][i]
        running.append(c)
        if len(running) > TRAIL_WINDOW:
            running = running[-TRAIL_WINDOW:]
        highest = max(running)
        atr = path["ATR14"][i]
        if not np.isfinite(atr):
            # fallback: use pick's atr_pct (as fraction of entry)
            atr = atr_pct / 100.0 * entry
        trail_line = highest - n_atr * atr
        if hard_stop_pct is not None:
            hs_line = entry * (1 + hard_stop_pct)
            trail_line = max(trail_line, hs_line)
        if c <= trail_line:
            return (c / entry) - 1.0, i, True
    exit_close = path["Close"][n-1]
    return (exit_close / entry) - 1.0, n-1, False


def sim_ema_trailing(path, entry, ema_period, horizon, pre_ema_seed=None):
    """Exit when close < MA(ema_period) daily."""
    n = min(path["n_days"], horizon)
    if n < 1:
        return np.nan, -1, False
    # compute rolling MA over pre+post, take post slice
    all_close = np.concatenate([path["pre_close"], path["Close"][:n]])
    # simple rolling mean, right-aligned
    if len(all_close) < ema_period:
        # not enough data; treat as pure hold
        return (path["Close"][n-1] / entry) - 1.0, n-1, False
    ma = pd.Series(all_close).rolling(ema_period, min_periods=ema_period).mean().values
    post_ma = ma[len(path["pre_close"]):len(path["pre_close"]) + n]
    for i in range(n):
        c = path["Close"][i]
        if np.isfinite(post_ma[i]) and c < post_ma[i]:
            return (c / entry) - 1.0, i, True
    exit_close = path["Close"][n-1]
    return (exit_close / entry) - 1.0, n-1, False


def sim_production(path, entry, atr_pct, weekly_ma20, horizon):
    """
    Current production: 3 tranches (1/3 each), SL + TP1/TP2/TP3.
    Pessimistic: if SL hit first (by daily close), whole position stops out at that close.
    Else each tranche exits at its TP (intraday high >= TP price) or horizon close.
    """
    n = min(path["n_days"], horizon)
    if n < 1:
        return np.nan, -1, False

    # SL pct (negative)
    stop_pct = np.clip(atr_pct / 100.0 * PROD_SL_MULT, PROD_SL_FLOOR, PROD_SL_CEIL)
    hard_stop_price = entry * (1 - stop_pct)
    min_gap = max(PROD_MIN_SL_GAP, atr_pct * PROD_MIN_SL_GAP_ATR / 100.0)
    sl_price = hard_stop_price
    if np.isfinite(weekly_ma20) and 0 < weekly_ma20 < entry:
        ma20_gap = (entry - weekly_ma20) / entry
        if weekly_ma20 > hard_stop_price and ma20_gap >= min_gap:
            sl_price = weekly_ma20

    # TP scale
    tp_scale = np.clip(atr_pct / PROD_ATR_MEDIAN, PROD_TP_SCALE_FLOOR, PROD_TP_SCALE_CEIL)
    tp_prices = [entry * (1 + p * tp_scale) for p in PROD_TP_PCTS]

    # walk forward day by day; SL uses close; TP uses intraday high
    sl_hit_day = -1
    tp_hit_days = [-1, -1, -1]
    for i in range(n):
        c = path["Close"][i]
        h = path["High"][i]
        if sl_hit_day < 0 and c <= sl_price:
            sl_hit_day = i
            break   # pessimistic: whole position out
        for k in range(3):
            if tp_hit_days[k] < 0 and h >= tp_prices[k]:
                tp_hit_days[k] = i

    if sl_hit_day >= 0:
        exit_c = path["Close"][sl_hit_day]
        return (exit_c / entry) - 1.0, sl_hit_day, True

    # no SL -> blend 3 tranches
    final_close = path["Close"][n-1]
    returns = []
    for k in range(3):
        if tp_hit_days[k] >= 0:
            # exit at TP price (assume filled at price level)
            returns.append((tp_prices[k] / entry) - 1.0)
        else:
            returns.append((final_close / entry) - 1.0)
    return float(np.mean(returns)), n-1, any(t >= 0 for t in tp_hit_days)


# ------------------------------------------------------------
#  Metric aggregation
# ------------------------------------------------------------
def compute_metrics(realized, exit_days, triggered, label):
    r = np.asarray(realized, dtype=float)
    r = r[np.isfinite(r)]
    if len(r) == 0:
        return {"strategy": label, "n": 0}
    mean = float(np.mean(r))
    std = float(np.std(r))
    downside = r[r < 0]
    dstd = float(np.std(downside)) if len(downside) else 0.0
    sharpe = mean / std if std > 0 else np.nan
    sortino = mean / dstd if dstd > 0 else np.nan
    win = float((r > 0).sum()) / len(r)
    p5 = float(np.percentile(r, 5))
    p1 = float(np.percentile(r, 1))
    p95 = float(np.percentile(r, 95))
    trig_rate = float(np.sum(triggered)) / len(triggered) if len(triggered) else np.nan
    # avg holding days among triggered
    d_trig = np.array([d for d, t in zip(exit_days, triggered) if t])
    avg_hd_trig = float(np.mean(d_trig)) if len(d_trig) else np.nan
    return {
        "strategy": label,
        "n": len(r),
        "mean": mean,
        "std": std,
        "sharpe": sharpe,
        "sortino": sortino,
        "win_rate": win,
        "p5": p5, "p1": p1, "p95": p95,
        "trigger_rate": trig_rate,
        "avg_holding_days_when_triggered": avg_hd_trig,
    }


# ------------------------------------------------------------
#  Run all strategies on a set of (path, pick-row) pairs
# ------------------------------------------------------------
def run_all_strategies(picks, paths, horizon=HORIZON_DAYS):
    """Return dict {strategy_label: list of (ret, day, triggered)}."""
    strategies = {}

    labels = []
    # A1. pure_hold 40d
    labels.append(("pure_hold_40d", lambda p, r: sim_pure_hold(p, r["entry_price"], 40)))
    # A2. pure_hold 60d
    labels.append(("pure_hold_60d", lambda p, r: sim_pure_hold(p, r["entry_price"], 60)))
    # A3. production
    labels.append((
        "production_vfg1_vfg2",
        lambda p, r: sim_production(p, r["entry_price"], r["atr_pct"],
                                    r.get("weekly_ma20", np.nan), horizon)
    ))
    # B1. pct trailing sweep
    for x in PCT_TRAIL_GRID:
        label = f"pct_trail_{int(x*100)}"
        labels.append((label, lambda p, r, x=x: sim_pct_trailing(p, r["entry_price"], x, horizon)))
    # B2. ATR trailing sweep
    for n_atr in ATR_TRAIL_GRID:
        label = f"atr_trail_{n_atr:.1f}"
        labels.append((
            label,
            lambda p, r, n_atr=n_atr: sim_atr_trailing(
                p, r["entry_price"], n_atr, horizon, r["atr_pct"]
            )
        ))
    # B3. EMA trailing
    for m in EMA_TRAIL_GRID:
        label = f"ema{m}_trail"
        labels.append((
            label,
            lambda p, r, m=m: sim_ema_trailing(p, r["entry_price"], m, horizon)
        ))
    # B4. ATR trailing + hard stop (atr*3 clip)
    for n_atr in [2.5, 3.0, 3.5]:
        label = f"atr_trail_{n_atr:.1f}_hardstop"
        def make_atr_hs(n_atr=n_atr):
            def fn(p, r):
                stop_pct = -np.clip(r["atr_pct"] / 100.0 * PROD_SL_MULT, PROD_SL_FLOOR, PROD_SL_CEIL)
                return sim_atr_trailing(p, r["entry_price"], n_atr, horizon, r["atr_pct"],
                                        hard_stop_pct=stop_pct)
            return fn
        labels.append((label, make_atr_hs()))

    # Bonus: ATR trailing + breakeven (after +10% lift SL to entry)
    def sim_atr_be(path, entry, n_atr, horizon, atr_pct, be_trigger=0.10):
        n = min(path["n_days"], horizon)
        if n < 1:
            return np.nan, -1, False
        running = list(path["pre_close"][-TRAIL_WINDOW:]) if len(path["pre_close"]) else []
        running.append(entry)
        be_active = False
        for i in range(n):
            c = path["Close"][i]
            running.append(c)
            if len(running) > TRAIL_WINDOW:
                running = running[-TRAIL_WINDOW:]
            highest = max(running)
            atr = path["ATR14"][i]
            if not np.isfinite(atr):
                atr = atr_pct / 100.0 * entry
            trail_line = highest - n_atr * atr
            if (c / entry) - 1 >= be_trigger:
                be_active = True
            if be_active:
                trail_line = max(trail_line, entry)
            if c <= trail_line:
                return (c / entry) - 1.0, i, True
        exit_close = path["Close"][n-1]
        return (exit_close / entry) - 1.0, n-1, False

    labels.append((
        "atr_trail_3.0_breakeven",
        lambda p, r: sim_atr_be(p, r["entry_price"], 3.0, horizon, r["atr_pct"])
    ))

    # Execute
    pick_rows = picks.to_dict("records")
    for label, fn in labels:
        rets, days, trigs = [], [], []
        for pth, row in zip(paths, pick_rows):
            if pth is None:
                rets.append(np.nan); days.append(-1); trigs.append(False)
                continue
            r, d, t = fn(pth, row)
            rets.append(r); days.append(d); trigs.append(t)
        strategies[label] = (np.asarray(rets), np.asarray(days), np.asarray(trigs))
        m = compute_metrics(rets, days, trigs, label)
        print(f"  {label:36s} n={m['n']:5d} mean={m.get('mean',0)*100:6.2f}% "
              f"sharpe={m.get('sharpe',float('nan')):.3f} win={m.get('win_rate',0)*100:5.1f}% "
              f"trig={m.get('trigger_rate',0)*100:5.1f}%")
    return strategies


# ------------------------------------------------------------
#  Tests
# ------------------------------------------------------------
def test_A_B(picks, strategies):
    """Full sample + per regime."""
    rows = []
    for label, (rets, days, trigs) in strategies.items():
        m = compute_metrics(rets, days, trigs, label)
        m["regime"] = "ALL"
        rows.append(m)
        for regime in picks["regime"].dropna().unique():
            mask = (picks["regime"] == regime).values
            if mask.sum() < 50:
                continue
            m2 = compute_metrics(rets[mask], days[mask], trigs[mask], label)
            m2["regime"] = regime
            rows.append(m2)
    return pd.DataFrame(rows)


def test_C_walk_forward(picks, paths, train_w=12, test_w=4, stride=4):
    """Walk-forward: train to find best, test on held-out."""
    weeks = sorted(picks["week_end_date"].unique())
    n = len(weeks)
    print(f"[wf] weeks={n} train={train_w} test={test_w} stride={stride}")

    # strategies to evaluate
    cand_labels = (
        ["pure_hold_60d"]
        + [f"pct_trail_{int(x*100)}" for x in PCT_TRAIL_GRID]
        + [f"atr_trail_{n:.1f}" for n in ATR_TRAIL_GRID]
        + [f"ema{m}_trail" for m in EMA_TRAIL_GRID]
        + [f"atr_trail_{n:.1f}_hardstop" for n in [2.5, 3.0, 3.5]]
        + ["atr_trail_3.0_breakeven", "production_vfg1_vfg2"]
    )

    wf_rows = []
    pick_weeks = pd.to_datetime(picks["week_end_date"]).values
    weeks_np = np.array([np.datetime64(w) for w in weeks])
    for start in range(0, n - train_w - test_w + 1, stride):
        train_weeks = weeks_np[start:start + train_w]
        test_weeks = weeks_np[start + train_w:start + train_w + test_w]
        train_mask = np.isin(pick_weeks, train_weeks)
        test_mask = np.isin(pick_weeks, test_weeks)
        if train_mask.sum() < 200 or test_mask.sum() < 50:
            continue

        # Per strategy, compute train mean+sharpe, test mean+sharpe
        train_scores = {}
        test_scores = {}
        for label in cand_labels:
            # quick re-simulate: but we have full-sample strategies already
            pass

        # Actually we already have full-sample returns; slice by mask
        for label in cand_labels:
            if label not in strategies_global:
                continue
            rets, _, _ = strategies_global[label]
            r_tr = rets[train_mask]
            r_te = rets[test_mask]
            r_tr = r_tr[np.isfinite(r_tr)]
            r_te = r_te[np.isfinite(r_te)]
            if len(r_tr) == 0 or len(r_te) == 0:
                continue
            train_mean = float(np.mean(r_tr))
            train_std = float(np.std(r_tr))
            train_sharpe = train_mean / train_std if train_std > 0 else np.nan
            test_mean = float(np.mean(r_te))
            test_std = float(np.std(r_te))
            test_sharpe = test_mean / test_std if test_std > 0 else np.nan
            train_scores[label] = (train_mean, train_sharpe)
            test_scores[label] = (test_mean, test_sharpe)

        # Best in train (by Sharpe)
        if not train_scores:
            continue
        best_train = max(train_scores, key=lambda k: (train_scores[k][1] if np.isfinite(train_scores[k][1]) else -9))
        best_test_actual = test_scores[best_train]
        # Best in test (for reference)
        best_test_lbl = max(test_scores, key=lambda k: (test_scores[k][1] if np.isfinite(test_scores[k][1]) else -9))

        wf_rows.append({
            "train_start": str(train_weeks[0]),
            "test_start": str(test_weeks[0]),
            "best_train_strategy": best_train,
            "train_mean": train_scores[best_train][0],
            "train_sharpe": train_scores[best_train][1],
            "test_mean_at_best_train": best_test_actual[0],
            "test_sharpe_at_best_train": best_test_actual[1],
            "best_test_strategy": best_test_lbl,
            "best_test_mean": test_scores[best_test_lbl][0],
            "best_test_sharpe": test_scores[best_test_lbl][1],
            "ph60_test_mean": test_scores.get("pure_hold_60d", (np.nan, np.nan))[0],
            "ph60_test_sharpe": test_scores.get("pure_hold_60d", (np.nan, np.nan))[1],
        })
    return pd.DataFrame(wf_rows)


# ------------------------------------------------------------
#  Main
# ------------------------------------------------------------
def main():
    print("=" * 70)
    print("VF-G2 Ext: Trailing Stop Validation")
    print("=" * 70)

    ohlcv_idx = load_ohlcv_index()

    # --- Load picks ---
    print(f"\n[picks] pure_right: {PICKS_PR.name}")
    picks = pd.read_parquet(PICKS_PR)
    picks = picks.reset_index(drop=True)
    print(f"  {len(picks)} picks")

    # --- Extract paths ---
    print("\n[paths] extracting OHLCV paths per pick")
    paths, missing = extract_paths(picks, ohlcv_idx, horizon=max(HORIZON_DAYS, 60))
    print(f"  missing={missing} usable={len(paths)-missing}")

    # --- Run all strategies (full sample) ---
    print("\n[Test A] Full sample (pure_right picks)")
    global strategies_global
    strategies_global = run_all_strategies(picks, paths)

    # --- Test A + B ---
    df_sweep = test_A_B(picks, strategies_global)
    df_sweep.to_csv(OUT_SWEEP, index=False, float_format="%.6f")
    print(f"\n[out] sweep -> {OUT_SWEEP}")

    # Test B: regime-only pivot for readability
    reg = df_sweep[df_sweep["regime"] != "ALL"].copy()
    reg_pivot = reg.pivot_table(index="strategy", columns="regime", values="mean", aggfunc="first")
    reg_pivot.to_csv(OUT_REGIME, float_format="%.6f")
    print(f"[out] regime pivot -> {OUT_REGIME}")

    # --- Test C: Walk-forward ---
    print("\n[Test C] Walk-forward")
    df_wf = test_C_walk_forward(picks, paths)
    df_wf.to_csv(OUT_WF, index=False, float_format="%.6f")
    print(f"[out] walk-forward -> {OUT_WF}")

    # --- Test E: Unscreened cross-check ---
    print("\n[Test E] Unscreened cross-check (mixed journal)")
    try:
        picks2 = pd.read_parquet(PICKS_MIXED).reset_index(drop=True)
        print(f"  {len(picks2)} picks")
        paths2, _ = extract_paths(picks2, ohlcv_idx, horizon=max(HORIZON_DAYS, 60))
        strat2 = run_all_strategies(picks2, paths2)
        df_cross = test_A_B(picks2, strat2)
        df_cross["dataset"] = "mixed"
        df_cross.to_csv(OUT_CROSSCHECK, index=False, float_format="%.6f")
        print(f"[out] crosscheck -> {OUT_CROSSCHECK}")
    except Exception as e:
        print(f"[Test E] failed: {e}")

    # --- Summary print ---
    print("\n" + "=" * 70)
    print("SUMMARY (full sample, regime=ALL)")
    print("=" * 70)
    all_only = df_sweep[df_sweep["regime"] == "ALL"].copy()
    all_only = all_only.sort_values("sharpe", ascending=False)
    cols = ["strategy", "mean", "sharpe", "sortino", "win_rate", "trigger_rate", "p5"]
    print(all_only[cols].to_string(index=False, float_format=lambda v: f"{v:.4f}" if isinstance(v, float) else str(v)))

    # delta vs pure_hold_60d
    base = all_only[all_only["strategy"] == "pure_hold_60d"].iloc[0]
    print("\nDelta vs pure_hold_60d (mean pct, sortino, trigger_rate)")
    all_only["delta_mean_pp"] = (all_only["mean"] - base["mean"]) * 100
    all_only["delta_sortino"] = all_only["sortino"] - base["sortino"]
    print(all_only[["strategy", "delta_mean_pp", "delta_sortino", "trigger_rate"]].to_string(index=False, float_format=lambda v: f"{v:.3f}" if isinstance(v, float) else str(v)))


if __name__ == "__main__":
    main()
