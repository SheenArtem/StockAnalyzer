"""
V3 Layer 3 — Catalyst Signal IC Validation

三個 catalyst binary signal，對齊 Mode D thesis-driven 策略的 rule-based
entry timing 層。對每個 signal 計算 trigger 後 20d/60d 報酬 + t-stat
+ IC vs universe mean + hit rate，並拆 Pre-AI (2016-2022) / AI era (2023-2025)
兩段。

Signals
-------
C1  月營收 YoY 拐點 (revenue_yoy_turnaround)
    - rev_yoy(t) > 0 AND rev_yoy(t-1) <= 0 (從負轉正)
    - trigger T = publish_date = period_month_end + 10 days (conservative)
    - Fallback: 若 (t-1) NaN 則看 slope (3m avg yoy) 從 <= -5pp 翻到 >= +5pp

C2  主動型 ETF 連續買超 (active_etf_sync_buy)
    - 資料源限制: etf_signal.py 是 live API 無歷史, 改用 institutional
      投信買超當 proxy (Active ETF 多由投信發行, 投信 flow 重疊性高)
    - 連 5 日投信淨買 > 0 AND 5d sum / 股本 > X bps
    - ⚠️ 這是 proxy, 非真正主動型 ETF signal; 結論僅代表「投信連買」

C3  外資從賣轉買 (foreign_sell_to_buy)
    - 前 30 日外資累積淨賣超 (foreign_net_30d < 0)
    - 近 10 日外資累積淨買超 (foreign_net_10d > 0)
    - 且翻轉金額佔股本比 > threshold

Data
----
- data_cache/backtest/ohlcv_tw.parquet (price + volume)
- data_cache/backtest/financials_revenue.parquet (month revenue)
- data_cache/chip_history/institutional.parquet (foreign / trust / dealer net)

Output
------
- reports/v3_layer3_catalyst_ic.csv  (per catalyst × horizon × regime)
- reports/v3_layer3_catalyst_ic.md   (summary + verdict)
- reports/v3_layer3_catalyst_events.parquet (per trigger event detail, optional)

Usage
-----
    python tools/v3_layer3_catalyst_ic.py
    python tools/v3_layer3_catalyst_ic.py --sample 400   # dev
"""
import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

OHLCV = _ROOT / "data_cache" / "backtest" / "ohlcv_tw.parquet"
REV = _ROOT / "data_cache" / "backtest" / "financials_revenue.parquet"
CHIP = _ROOT / "data_cache" / "chip_history" / "institutional.parquet"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "v3_layer3_catalyst_ic.csv"
OUT_MD = OUT_DIR / "v3_layer3_catalyst_ic.md"
OUT_EVENTS = OUT_DIR / "v3_layer3_catalyst_events.parquet"

HORIZONS = [20, 60]
PRE_AI = ("2016-01-01", "2022-12-31")
AI_ERA = ("2023-01-01", "2025-12-31")
UNIV_TOP_N = 600                # market-cap / turnover proxy top N

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("catalyst")


# ------------------------------------------------------------
# Load & Universe
# ------------------------------------------------------------
def load_ohlcv(sample=None):
    log.info("Loading OHLCV ...")
    df = pd.read_parquet(
        OHLCV,
        columns=["stock_id", "date", "Close", "AdjClose", "Volume"],
    )
    df["date"] = pd.to_datetime(df["date"])
    # Filter to 4-digit common stocks (台股 common stock pattern)
    mask = df["stock_id"].str.match(r"^\d{4}$", na=False)
    df = df[mask].copy()
    if sample is not None:
        picks = df["stock_id"].drop_duplicates().sample(
            min(sample, df["stock_id"].nunique()), random_state=42
        )
        df = df[df["stock_id"].isin(picks)].copy()
    log.info(f"OHLCV loaded: {len(df):,} rows, {df['stock_id'].nunique()} tickers")
    return df.sort_values(["stock_id", "date"]).reset_index(drop=True)


def rolling_universe(ohlcv, top_n=UNIV_TOP_N):
    """
    Day-by-day top-N by 60d avg turnover (TWD) → dynamic universe membership.
    Returns set of (date, stock_id) tuples for fast lookup as long-form df.
    """
    log.info(f"Building rolling top-{top_n} universe ...")
    df = ohlcv[["stock_id", "date", "Close", "Volume"]].copy()
    df["tv"] = df["Close"] * df["Volume"]
    df["tv60"] = df.groupby("stock_id")["tv"].transform(
        lambda s: s.rolling(60, min_periods=30).mean()
    )
    # rank per date desc
    df["rk"] = df.groupby("date")["tv60"].rank(ascending=False, method="first")
    df["in_univ"] = df["rk"] <= top_n
    out = df.loc[df["in_univ"], ["date", "stock_id"]].drop_duplicates()
    log.info(f"Universe cells: {len(out):,}")
    return out


def compute_forward_returns(ohlcv):
    """For each stock/date, fwd ret over HORIZONS using AdjClose."""
    log.info("Computing forward returns ...")
    df = ohlcv[["stock_id", "date", "AdjClose"]].copy()
    df = df.sort_values(["stock_id", "date"])
    for h in HORIZONS:
        df[f"fwd_{h}"] = (
            df.groupby("stock_id")["AdjClose"].shift(-h) / df["AdjClose"] - 1.0
        )
    return df.drop(columns=["AdjClose"])


# ------------------------------------------------------------
# Catalyst C1: Revenue YoY Turnaround
# ------------------------------------------------------------
def catalyst_c1_triggers(rev_df):
    """
    Trigger when YoY flips from <= 0 to > 0, OR slope flips from <=-5pp to >=+5pp.
    Publish date = period_month_end + 10 days (conservative for look-ahead).

    Note: upstream revenue_year_growth is mostly NaN (only 2269/237977 populated),
    so we recompute YoY from `revenue` column (t vs t-12 month).
    """
    log.info("Building C1 (revenue YoY turnaround) triggers ...")
    df = rev_df[["stock_id", "date", "revenue"]].copy()
    df = df.sort_values(["stock_id", "date"])
    # Compute YoY from revenue (t / t-12 - 1) * 100, in pp
    df["rev_12"] = df.groupby("stock_id")["revenue"].shift(12)
    df["yoy"] = np.where(
        df["rev_12"] > 0,
        (df["revenue"] / df["rev_12"] - 1.0) * 100.0,
        np.nan,
    )
    df["yoy_prev"] = df.groupby("stock_id")["yoy"].shift(1)
    df["yoy_3m"] = df.groupby("stock_id")["yoy"].transform(
        lambda s: s.rolling(3, min_periods=2).mean()
    )
    df["yoy_3m_prev"] = df.groupby("stock_id")["yoy_3m"].shift(3)

    # Condition A: flip from <= 0 to > 0
    condA = (df["yoy"] > 0) & (df["yoy_prev"] <= 0) & df["yoy_prev"].notna()
    # Condition B: 3m-avg slope flips from <= -5pp to >= +5pp
    condB = (df["yoy_3m"] >= 5) & (df["yoy_3m_prev"] <= -5) & df["yoy_3m_prev"].notna()

    df["trig"] = condA | condB
    df["trig_kind"] = np.where(condA, "flip", np.where(condB, "slope", "none"))

    # Publish date = period_month_end + 10 days
    period = pd.to_datetime(df["date"])
    period_eom = period + pd.offsets.MonthEnd(0)
    df["pub_date"] = period_eom + pd.Timedelta(days=10)

    events = df.loc[df["trig"], ["stock_id", "pub_date", "trig_kind", "yoy", "yoy_prev"]].copy()
    events = events.rename(columns={"pub_date": "trigger_date"})
    events["signal"] = "C1_rev_yoy_turnaround"
    log.info(f"C1 triggers: {len(events):,}")
    return events


# ------------------------------------------------------------
# Catalyst C2: Trust (proxy for Active ETF) Sustained Buying
# ------------------------------------------------------------
def catalyst_c2_triggers(chip, ohlcv):
    """
    Proxy: trust_net buying 5 consecutive days AND 5d-sum > 0.3% of turnover.
    (真正 Active ETF 歷史不可得, 用投信當 proxy; 投信是主動型 ETF 主要發行者)
    """
    log.info("Building C2 (trust-net sustained buy, proxy for Active ETF) triggers ...")
    df = chip[["date", "stock_id", "trust_net"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])
    # consecutive 5-day trust_net > 0
    df["pos"] = (df["trust_net"] > 0).astype(int)
    df["streak"] = df.groupby("stock_id")["pos"].transform(
        lambda s: s.rolling(5, min_periods=5).sum()
    )
    df["sum5"] = df.groupby("stock_id")["trust_net"].transform(
        lambda s: s.rolling(5, min_periods=5).sum()
    )
    # Attach turnover to normalize
    turn = ohlcv[["stock_id", "date", "Close", "Volume"]].copy()
    turn["turnover"] = turn["Close"] * turn["Volume"]
    turn["tv20"] = turn.groupby("stock_id")["turnover"].transform(
        lambda s: s.rolling(20, min_periods=10).mean()
    )
    turn = turn[["stock_id", "date", "tv20"]]
    m = df.merge(turn, on=["stock_id", "date"], how="left")
    # net buy size relative to 20d avg turnover (shares * 1000 vs TWD turnover → rough ratio)
    # trust_net is in shares (張 * 1000); Value ~ trust_net * close (approximate)
    m = m.merge(
        ohlcv[["stock_id", "date", "Close"]],
        on=["stock_id", "date"],
        how="left",
    )
    m["nt_value_5d"] = m["sum5"] * m["Close"]
    m["nt_ratio"] = m["nt_value_5d"] / (m["tv20"] * 5.0)
    trig = (m["streak"] == 5) & (m["nt_ratio"] > 0.02)   # 5-day net buy > 2% of 5d turnover
    events = m.loc[trig, ["stock_id", "date", "sum5", "nt_ratio"]].copy()
    events = events.rename(columns={"date": "trigger_date"})
    events["signal"] = "C2_trust_etf_proxy_buy"
    log.info(f"C2 triggers: {len(events):,}")
    return events


# ------------------------------------------------------------
# Catalyst C3: Foreign Sell → Buy Reversal
# ------------------------------------------------------------
def catalyst_c3_triggers(chip):
    """
    Prior 30d cumulative foreign_net < 0 AND recent 10d cumulative foreign_net > 0,
    且兩者金額差距 > 某 threshold (|10d sum| > 0.3 * |30d sum|).
    """
    log.info("Building C3 (foreign sell→buy reversal) triggers ...")
    df = chip[["date", "stock_id", "foreign_net"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])
    df["sum30"] = df.groupby("stock_id")["foreign_net"].transform(
        lambda s: s.rolling(30, min_periods=20).sum()
    )
    df["sum10"] = df.groupby("stock_id")["foreign_net"].transform(
        lambda s: s.rolling(10, min_periods=8).sum()
    )
    # To avoid double-counting, use sum30 = prior 30d INCLUDING last 10d.
    # Better: prior 20d (from t-30 to t-10) + recent 10d
    df["sum_prior20"] = df["sum30"] - df["sum10"]   # = foreign_net from t-30 to t-10
    # Stricter: require 10d net buy size > 50% of |prior 20d net sell|
    trig = (df["sum_prior20"] < 0) & (df["sum10"] > 0) & (df["sum10"] > 0.5 * df["sum_prior20"].abs())
    # Dedup: keep only the first trigger within each 20-day window per stock
    # (suppress re-triggers for 20 trading days after one trigger)
    df["trig"] = trig
    # roll(20) sum of prior trig; if any trigger in prior 20d, skip
    df["trig_int"] = df["trig"].astype(int)
    df["prev_trig_20d"] = df.groupby("stock_id")["trig_int"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=1).sum()
    )
    df["new_trig"] = df["trig"] & (df["prev_trig_20d"].fillna(0) == 0)
    events = df.loc[df["new_trig"], ["stock_id", "date", "sum10", "sum_prior20"]].copy()
    events = events.rename(columns={"date": "trigger_date"})
    events["signal"] = "C3_foreign_sell_to_buy"
    log.info(f"C3 triggers: {len(events):,}")
    return events


# ------------------------------------------------------------
# Attach forward returns and evaluate
# ------------------------------------------------------------
def _attach_fwd(events, ohlcv_fwd, univ):
    """For each event, find the first trading day >= trigger_date and attach fwd."""
    events = events.copy()
    events["trigger_date"] = pd.to_datetime(events["trigger_date"])
    # map trigger_date → next trading day per stock_id
    td = ohlcv_fwd[["stock_id", "date"]].copy()
    td = td.sort_values(["stock_id", "date"])

    results = []
    # Process per stock
    for sid, grp in events.groupby("stock_id"):
        stock_td = td[td["stock_id"] == sid]["date"].values
        if len(stock_td) == 0:
            continue
        for _, row in grp.iterrows():
            # find next trading day on or after trigger_date
            idx = np.searchsorted(stock_td, np.datetime64(row["trigger_date"]))
            if idx >= len(stock_td):
                continue
            effective_date = pd.Timestamp(stock_td[idx])
            results.append(
                {
                    "stock_id": sid,
                    "signal": row["signal"],
                    "trigger_date": row["trigger_date"],
                    "effective_date": effective_date,
                }
            )
    if not results:
        return pd.DataFrame()
    edf = pd.DataFrame(results)
    # Merge forward returns
    edf = edf.merge(
        ohlcv_fwd,
        left_on=["stock_id", "effective_date"],
        right_on=["stock_id", "date"],
        how="left",
    ).drop(columns=["date"])

    # Universe filter
    univ_key = univ.assign(in_u=1)
    edf = edf.merge(
        univ_key,
        left_on=["effective_date", "stock_id"],
        right_on=["date", "stock_id"],
        how="left",
    )
    edf["in_univ"] = edf["in_u"].fillna(0).astype(int)
    edf = edf.drop(columns=["date", "in_u"])
    return edf


def _regime(d):
    if pd.Timestamp(PRE_AI[0]) <= d <= pd.Timestamp(PRE_AI[1]):
        return "pre_ai"
    if pd.Timestamp(AI_ERA[0]) <= d <= pd.Timestamp(AI_ERA[1]):
        return "ai_era"
    return "other"


def evaluate(events_with_fwd, baseline):
    """
    For each (signal × horizon × regime):
      - n (trigger events with fwd return)
      - mean trigger ret
      - baseline mean ret (same regime, same horizon, universe-wide)
      - alpha = mean_trigger - mean_baseline
      - t-stat (one-sample vs baseline)
      - IC proxy = alpha / std_trigger  (not standard IC, but analogous)
      - hit rate = P(trigger_ret > baseline_median)
    """
    # Only universe events
    ev = events_with_fwd[events_with_fwd["in_univ"] == 1].copy()
    ev["regime"] = ev["effective_date"].apply(_regime)
    ev = ev[ev["regime"] != "other"]

    rows = []
    for signal, grp_s in ev.groupby("signal"):
        for reg in ["pre_ai", "ai_era", "all"]:
            if reg == "all":
                g = grp_s.copy()
            else:
                g = grp_s[grp_s["regime"] == reg].copy()
            for h in HORIZONS:
                col = f"fwd_{h}"
                sub = g[g[col].notna()]
                if len(sub) < 30:
                    rows.append(
                        {
                            "signal": signal,
                            "regime": reg,
                            "horizon": h,
                            "n": len(sub),
                            "mean_trigger_ret": np.nan,
                            "baseline_mean": np.nan,
                            "alpha": np.nan,
                            "t_stat": np.nan,
                            "p_value": np.nan,
                            "ic_proxy": np.nan,
                            "hit_rate": np.nan,
                        }
                    )
                    continue
                mean_trig = sub[col].mean()
                std_trig = sub[col].std(ddof=1)
                # baseline: pick rows from baseline in same regime, same horizon
                bl = baseline[(baseline["regime"] == reg) & (baseline[col].notna())] if reg != "all" else baseline[baseline[col].notna()]
                if len(bl) == 0:
                    continue
                bl_mean = bl[col].mean()
                bl_median = bl[col].median()
                # one-sample t-test: is mean_trig != bl_mean?
                t_stat, p_val = stats.ttest_1samp(sub[col], bl_mean)
                alpha = mean_trig - bl_mean
                ic_proxy = alpha / std_trig if std_trig > 0 else np.nan
                hit = (sub[col] > bl_median).mean()
                rows.append(
                    {
                        "signal": signal,
                        "regime": reg,
                        "horizon": h,
                        "n": len(sub),
                        "mean_trigger_ret": mean_trig,
                        "baseline_mean": bl_mean,
                        "alpha": alpha,
                        "t_stat": t_stat,
                        "p_value": p_val,
                        "ic_proxy": ic_proxy,
                        "hit_rate": hit,
                    }
                )
    return pd.DataFrame(rows)


def build_baseline(ohlcv_fwd, univ):
    """Universe-wide baseline: for each date in universe, each fwd horizon."""
    log.info("Building baseline (universe-wide fwd returns) ...")
    # merge fwd with univ membership
    bl = ohlcv_fwd.merge(univ, on=["date", "stock_id"], how="inner")
    bl["regime"] = bl["date"].apply(_regime)
    bl = bl[bl["regime"] != "other"]
    log.info(f"Baseline rows: {len(bl):,}")
    return bl


def verdict(row_subset):
    """
    Per signal overall verdict (RFP definition):
      A: 兩段 IC > 0.03 且 p < 0.01，hit rate > 55%
      B: 一段達 A 標準
      C: IC 0.01-0.03 且 hit rate 50-55% (邊際)
      D: IC < 0.01 或反向
    Pick the best horizon per regime by ic_proxy.
    """
    def tier(ic, p, hit):
        if pd.isna(ic) or pd.isna(p) or pd.isna(hit):
            return "D"
        if ic > 0.03 and p < 0.01 and hit > 0.55:
            return "A"
        if 0.01 <= ic <= 0.03 and 0.50 <= hit <= 0.55:
            return "C"
        if ic < 0.01:
            return "D"
        # ic>0.03 but hit<=0.55 or p>=0.01 → partial, treat as B-level (mean positive but weak)
        if ic > 0.03:
            return "B"
        return "D"

    regs = {}
    best_ic = {}
    for reg in ["pre_ai", "ai_era"]:
        sub = row_subset[(row_subset["regime"] == reg)]
        if len(sub) == 0:
            regs[reg] = "D"
            best_ic[reg] = np.nan
            continue
        best = sub.loc[sub["ic_proxy"].idxmax()] if sub["ic_proxy"].notna().any() else None
        if best is None:
            regs[reg] = "D"
            best_ic[reg] = np.nan
            continue
        regs[reg] = tier(best["ic_proxy"], best["p_value"], best["hit_rate"])
        best_ic[reg] = best["ic_proxy"]
        # Also check worst IC in this regime — if any negative & significant, mark unstable
        worst = sub.loc[sub["ic_proxy"].idxmin()] if sub["ic_proxy"].notna().any() else None
        if worst is not None and worst["ic_proxy"] < -0.02 and worst["p_value"] < 0.05:
            # regime has significantly negative alpha at some horizon → degrade
            regs[reg] = "D"

    # Regime sign consistency: if pre_ai best IC < 0 OR ai_era best IC < 0, treat as unstable → downgrade
    if best_ic.get("pre_ai", 0) < 0 or best_ic.get("ai_era", 0) < 0:
        # Any regime with negative best ic → overall D (direction flip is fatal)
        return "D"

    a = regs["pre_ai"]
    b = regs["ai_era"]
    if a == "A" and b == "A":
        return "A"
    if a == "A" or b == "A":
        return "B"
    order = {"A": 3, "B": 2, "C": 1, "D": 0}
    if order[a] >= 2 and order[b] >= 2:
        return "C"
    if order[a] >= 1 and order[b] >= 1:
        return "C"
    return "D"


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None, help="dev sample N tickers")
    args = ap.parse_args()

    t0 = time.time()
    ohlcv = load_ohlcv(sample=args.sample)
    univ = rolling_universe(ohlcv, top_n=UNIV_TOP_N)
    fwd = compute_forward_returns(ohlcv)

    # Load revenue + chip
    log.info("Loading revenue ...")
    rev = pd.read_parquet(REV)
    rev["date"] = pd.to_datetime(rev["date"])
    log.info(f"Revenue rows: {len(rev):,}")

    log.info("Loading chip ...")
    chip = pd.read_parquet(CHIP)
    chip["date"] = pd.to_datetime(chip["date"])
    log.info(f"Chip rows: {len(chip):,}")

    # Build triggers
    c1 = catalyst_c1_triggers(rev)
    c2 = catalyst_c2_triggers(chip, ohlcv)
    c3 = catalyst_c3_triggers(chip)

    all_events = pd.concat([c1, c2, c3], ignore_index=True, sort=False)

    # Attach fwd + universe flag
    log.info("Attaching forward returns to events ...")
    ev_with_fwd = _attach_fwd(all_events, fwd, univ)
    log.info(f"Events with fwd: {len(ev_with_fwd):,}")

    # Build baseline
    baseline = build_baseline(fwd, univ)

    # Evaluate
    log.info("Evaluating ...")
    result = evaluate(ev_with_fwd, baseline)

    # Verdict per signal
    verdicts = {}
    for sig in result["signal"].unique():
        v = verdict(result[result["signal"] == sig])
        verdicts[sig] = v
    result["verdict"] = result["signal"].map(verdicts)

    # Save
    result = result.sort_values(["signal", "regime", "horizon"])
    result.to_csv(OUT_CSV, index=False, float_format="%.6f")
    log.info(f"Wrote {OUT_CSV}")

    # Save events detail
    ev_with_fwd.to_parquet(OUT_EVENTS, index=False)
    log.info(f"Wrote {OUT_EVENTS}")

    # MD report
    write_md(result, verdicts, len(all_events), len(ev_with_fwd))
    log.info(f"Wrote {OUT_MD}")
    log.info(f"Done in {time.time()-t0:.1f}s")


def write_md(result, verdicts, total_events, matched_events):
    lines = []
    lines.append("# V3 Layer 3 Catalyst Signal IC Validation")
    lines.append("")
    lines.append(f"- 產生時間: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Universe: TW common stocks (4-digit), rolling top-{UNIV_TOP_N} by 60d turnover")
    lines.append(f"- Pre-AI regime: {PRE_AI[0]} to {PRE_AI[1]}")
    lines.append(f"- AI era regime: {AI_ERA[0]} to {AI_ERA[1]}")
    lines.append(f"- Horizons: {HORIZONS} trading days")
    lines.append(f"- Total triggers: {total_events:,}  |  with fwd return: {matched_events:,}")
    lines.append("")
    lines.append("## Verdict Summary")
    lines.append("")
    lines.append("| Signal | Verdict | Description |")
    lines.append("|---|---|---|")
    desc = {
        "C1_rev_yoy_turnaround": "月營收 YoY 從負轉正 (或 3m slope 由 -5pp 翻 +5pp)",
        "C2_trust_etf_proxy_buy": "投信連 5 日淨買超 (Active ETF proxy), 5d 買超 > 2% 20d turnover",
        "C3_foreign_sell_to_buy": "近 20 日外資累積賣超 + 最近 10 日外資轉淨買超",
    }
    for sig in ["C1_rev_yoy_turnaround", "C2_trust_etf_proxy_buy", "C3_foreign_sell_to_buy"]:
        v = verdicts.get(sig, "D")
        d = desc.get(sig, "")
        lines.append(f"| {sig} | **{v}** | {d} |")
    lines.append("")
    lines.append("## Detailed Metrics")
    lines.append("")
    lines.append(
        "| signal | regime | horizon | n | mean_trigger | baseline | alpha | t-stat | p-value | ic_proxy | hit_rate |"
    )
    lines.append(
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    for _, r in result.iterrows():
        lines.append(
            f"| {r['signal']} | {r['regime']} | {r['horizon']}d | {r['n']} | "
            f"{r['mean_trigger_ret']:.4f} | {r['baseline_mean']:.4f} | {r['alpha']:.4f} | "
            f"{r['t_stat']:.2f} | {r['p_value']:.4f} | {r['ic_proxy']:.4f} | {r['hit_rate']:.3f} |"
        )
    lines.append("")
    lines.append("## Verdict Criteria")
    lines.append("")
    lines.append("- **A**: 兩段 IC proxy > 0.03 且 p < 0.01，hit rate > 55%")
    lines.append("- **B**: 一段達 A 標準")
    lines.append("- **C**: IC proxy 0.01-0.03 且 hit rate 50-55%（邊際）")
    lines.append("- **D**: IC proxy < 0.01 或反向")
    lines.append("")
    lines.append("## Caveats")
    lines.append("")
    lines.append(
        "- C2 用投信淨買超 proxy Active ETF flow (真正 Active ETF 2023+ 才普及且無歷史 API)"
    )
    lines.append(
        "- IC proxy = alpha / std_trigger，非標準截面 Spearman IC (trigger 訊號是 binary event)"
    )
    lines.append(
        "- 月營收公布日用 period month-end + 10 days 保守估計，避免 look-ahead bias"
    )
    lines.append(
        "- Universe = top-N by 60d turnover 作為 market-cap proxy (ohlcv 無流通股數直接欄位)"
    )
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
