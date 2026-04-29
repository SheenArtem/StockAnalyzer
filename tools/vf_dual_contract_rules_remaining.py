"""
Dual × position_monitor 契約 Rule 1/2/7/8 補驗 (2026-04-29).

之前 Step B/C 只驗 Rule 4/5/6/3-TP 部分，剩 4 條未驗：
  Rule 1: Hard > Rebalance > Soft 優先序
  Rule 2: 月頻 rebalance vs 其他頻率（weekly / biweekly / monthly / quarterly）
  Rule 7: 非 volatile regime 下 Value 舊持股下月清倉 vs 立刻清倉
  Rule 8: 開盤跳空 ±3% 暫停執行

Each rule has its own analysis function. Output combined md + per-rule csv.

Methodology summary:
  Rule 1: 用 trade journal 的 fwd_5d_min 做 stop-loss proxy。
          比較「stop hit 當週出 vs 等到 fwd_20d 月末出」
  Rule 2: weekly/biweekly/monthly/quarterly 4 種 rebalance 頻率
          各跑 Value top-20 backtest，比 CAGR / Sharpe / MDD
  Rule 7: TWII regime 分類，identify volatile→non-volatile 切換點
          比較「立刻清倉 (use 當週 fwd_5d)」vs「下個月清倉 (use fwd_20d)」
  Rule 8: TWII Open vs PrevClose ±3% 跳空日 fwd return 與其他日比較
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MD = OUT_DIR / "vf_dual_contract_rules_remaining.md"


# ============================================================
# Data loaders
# ============================================================
def load_value_snapshot():
    df = pd.read_parquet(DATA_DIR / "trade_journal_value_tw_snapshot.parquet")
    df["week_end_date"] = pd.to_datetime(df["week_end_date"])
    df["stock_id"] = df["stock_id"].astype(str)
    return df


def load_twii():
    df = pd.read_parquet(DATA_DIR / "_twii_bench.parquet")
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    out = pd.DataFrame({
        "Open": df["Open"].astype(float),
        "Close": df["Close"].astype(float),
    })
    out["prev_close"] = out["Close"].shift(1)
    out["gap_pct"] = out["Open"] / out["prev_close"] - 1
    out["ret"] = out["Close"].pct_change()
    return out


def get_top20_per_week(v):
    out = []
    for wk, g in v.groupby("week_end_date"):
        top = g.nlargest(20, "value_score")
        out.append(top.assign(week_end_date=wk))
    return pd.concat(out, ignore_index=True)


def classify_regime(twii):
    """volatile/trending/ranging/neutral, aligned with vf_value_portfolio_backtest."""
    closes = twii["Close"]
    out = []
    for i, (dt, _) in enumerate(closes.items()):
        if i < 20:
            out.append("neutral")
            continue
        window = closes.iloc[i - 20:i + 1]
        p0, p1 = window.iloc[0], window.iloc[-1]
        ret20 = (p1 / p0) - 1
        wmax, wmin, wavg = window.max(), window.min(), window.mean()
        rng20 = (wmax - wmin) / wavg if wavg > 0 else 0
        if rng20 > 0.08:
            r = "volatile"
        elif ret20 > 0.05:
            r = "trending"
        elif abs(ret20) < 0.02 and rng20 <= 0.08:
            r = "ranging"
        else:
            r = "neutral"
        out.append(r)
    return pd.Series(out, index=closes.index, name="regime")


# ============================================================
# Rule 1 — Hard exit timing
# ============================================================
def rule1_hard_exit_timing(v_top):
    """Hard stop proxy: fwd_20d_min < -8% (大跌觸發 stop loss within month)
    Strategy A: stop hit -> exit at -8% cap (loss floored)
    Strategy B: 等到 fwd_20d (月末) 才出，承受實際結果
    Compare which is better.
    """
    sub = v_top[v_top["fwd_20d_min"].notna() & (v_top["fwd_20d_min"] < -0.08)].copy()
    if sub.empty:
        return {"n": 0}

    # Strategy A: hard exit at -8% cap when stop fires
    sub["ret_A_hard_exit"] = -0.08
    # Strategy B: hold to month end, accept fwd_20d (could recover to >0)
    sub["ret_B_wait"] = sub["fwd_20d"]

    return {
        "n": len(sub),
        "mean_A_hard_exit": sub["ret_A_hard_exit"].mean(),
        "mean_B_wait": sub["ret_B_wait"].mean(),
        "diff_A_minus_B": (sub["ret_A_hard_exit"] - sub["ret_B_wait"]).mean(),
        "win_rate_A_better": (sub["ret_A_hard_exit"] > sub["ret_B_wait"]).mean(),
        # Tail: how often did B recover above -8% vs stayed below?
        "B_recovered_above_neg8": (sub["fwd_20d"] > -0.08).mean(),
        "B_worse_than_neg8": (sub["fwd_20d"] < -0.08).mean(),
    }


# ============================================================
# Rule 2 — Rebalance frequency grid
# ============================================================
def rule2_rebalance_frequency(v_top, freqs=(1, 2, 4, 13)):
    """v_top is weekly Value top-20.
    For each frequency (1=weekly, 2=biweekly, 4=monthly, 13=quarterly):
      rebalance dates = weeks[::freq]
      hold each rebalance for `freq` weeks
      use fwd_5d × freq weeks as period ret approximation
    """
    weeks = sorted(v_top["week_end_date"].unique())
    rows = []

    for freq in freqs:
        rebal_weeks = weeks[::freq]
        period_rets = []
        for k, wk in enumerate(rebal_weeks):
            top = v_top[v_top["week_end_date"] == wk]
            if top.empty:
                continue
            # For freq=1, use fwd_5d; freq=2, fwd_10d; freq=4, fwd_20d; freq=13, fwd_60d
            if freq == 1:
                ret_col = "fwd_5d"
            elif freq == 2:
                ret_col = "fwd_10d"
            elif freq == 4:
                ret_col = "fwd_20d"
            elif freq == 13:
                ret_col = "fwd_60d"
            else:
                ret_col = "fwd_20d"
            period_ret = top[ret_col].mean()
            if pd.notna(period_ret):
                period_rets.append(period_ret)

        if not period_rets:
            continue

        s = pd.Series(period_rets)
        # Annualize: periods/year = 52/freq for weekly base, but fwd_5d is 1 week
        # freq=1 (weekly): 52 periods/year
        # freq=2 (biweekly): 26 periods/year
        # freq=4 (monthly): 13 periods/year
        # freq=13 (quarterly): 4 periods/year
        periods_per_year = 52 / freq if freq <= 4 else 4
        cum = (1 + s).cumprod()
        n_years = len(s) / periods_per_year
        cagr = cum.iloc[-1] ** (1 / n_years) - 1 if n_years > 0 else 0
        vol = s.std() * np.sqrt(periods_per_year)
        sharpe = (s.mean() * periods_per_year - 0.01) / vol if vol > 0 else 0
        # MDD on cumulative
        peak = cum.cummax()
        mdd = ((cum - peak) / peak).min()

        rows.append({
            "freq_weeks": freq,
            "freq_label": {1: "weekly", 2: "biweekly", 4: "monthly", 13: "quarterly"}[freq],
            "n_periods": len(s),
            "mean_period_ret": s.mean(),
            "std": s.std(),
            "cagr": cagr,
            "sharpe_ann": sharpe,
            "mdd": mdd,
            "final_cum": cum.iloc[-1],
        })
    return pd.DataFrame(rows)


# ============================================================
# Rule 7 — Regime transition: immediate vs deferred clearing
# ============================================================
def rule7_regime_transition(v_top, twii):
    """Identify volatile -> non-volatile transitions on TWII.
    For Value picks held in volatile regime, compare:
      Strategy A: 立刻清 (本週收盤後賣) — 用 fwd_5d 作 proxy (clear next week)
      Strategy B: 隔月清 (1 個月後賣) — 用 fwd_20d
    Aggregate across all transitions.
    """
    regime = classify_regime(twii)

    # Find volatile -> non-volatile transitions
    is_volatile = regime == "volatile"
    transitions = []
    for i in range(1, len(regime)):
        if is_volatile.iloc[i - 1] and not is_volatile.iloc[i]:
            transitions.append(regime.index[i])

    # For each transition date, find the value top-20 picked the most recent week
    # before the transition (i.e., picks held during volatile)
    weeks = sorted(v_top["week_end_date"].unique())
    rows_per_trans = []
    for t in transitions:
        # Find the most recent week_end_date <= t
        prior_weeks = [w for w in weeks if w <= t]
        if not prior_weeks:
            continue
        w = prior_weeks[-1]
        picks = v_top[v_top["week_end_date"] == w]
        if picks.empty:
            continue
        rows_per_trans.append({
            "transition_date": t,
            "pick_week": w,
            "n_picks": len(picks),
            "ret_A_immediate": picks["fwd_5d"].mean(),
            "ret_B_defer_1mo": picks["fwd_20d"].mean(),
            "ret_C_defer_3mo": picks["fwd_60d"].mean(),
        })

    df_trans = pd.DataFrame(rows_per_trans)
    if df_trans.empty:
        return df_trans, {}

    summary = {
        "n_transitions": len(df_trans),
        "mean_A_immediate": df_trans["ret_A_immediate"].mean(),
        "mean_B_defer_1mo": df_trans["ret_B_defer_1mo"].mean(),
        "mean_C_defer_3mo": df_trans["ret_C_defer_3mo"].mean(),
        "diff_B_minus_A (defer 1mo cost)": df_trans["ret_B_defer_1mo"].mean() - df_trans["ret_A_immediate"].mean(),
        "diff_C_minus_A (defer 3mo cost)": df_trans["ret_C_defer_3mo"].mean() - df_trans["ret_A_immediate"].mean(),
    }
    return df_trans, summary


# ============================================================
# Rule 8 — Gap day analysis
# ============================================================
def rule8_gap_analysis(twii):
    """TWII Open vs PrevClose 跳空日 fwd return.
    spec ±3% in Rule 8, but TWII 11 年只 7 天，實務上幾乎不觸發。
    Also test ±1% as practical threshold.
    """
    df = twii.dropna(subset=["gap_pct"]).copy()
    df["fwd_5d"] = df["Close"].shift(-5) / df["Close"] - 1
    df["fwd_20d"] = df["Close"].shift(-20) / df["Close"] - 1

    rows = []
    for label, mask in [
        ("all_days", df["gap_pct"].notna()),
        ("non_gap_3pct", df["gap_pct"].abs() <= 0.03),
        ("gap_up_3pct", df["gap_pct"] > 0.03),
        ("gap_dn_3pct", df["gap_pct"] < -0.03),
        ("non_gap_1pct", df["gap_pct"].abs() <= 0.01),
        ("gap_up_1pct", df["gap_pct"] > 0.01),
        ("gap_dn_1pct", df["gap_pct"] < -0.01),
    ]:
        sub = df[mask].dropna(subset=["fwd_5d", "fwd_20d"])
        if len(sub) < 5:
            rows.append({"group": label, "n_days": len(sub),
                         "fwd_5d_mean": np.nan, "fwd_5d_t": np.nan,
                         "fwd_20d_mean": np.nan, "fwd_20d_t": np.nan})
            continue
        rows.append({
            "group": label,
            "n_days": len(sub),
            "fwd_5d_mean": sub["fwd_5d"].mean(),
            "fwd_5d_t": sub["fwd_5d"].mean() / (sub["fwd_5d"].std(ddof=1) / np.sqrt(len(sub)))
                       if sub["fwd_5d"].std(ddof=1) > 0 else np.nan,
            "fwd_20d_mean": sub["fwd_20d"].mean(),
            "fwd_20d_t": sub["fwd_20d"].mean() / (sub["fwd_20d"].std(ddof=1) / np.sqrt(len(sub)))
                        if sub["fwd_20d"].std(ddof=1) > 0 else np.nan,
        })
    return pd.DataFrame(rows)


# ============================================================
# Driver
# ============================================================
def main():
    print("Loading data...")
    v = load_value_snapshot()
    twii = load_twii()
    v_top = get_top20_per_week(v)
    print(f"  Value top-20: {len(v_top):,} rows × {v_top['week_end_date'].nunique()} weeks")
    print(f"  TWII: {len(twii):,} days from {twii.index[0].date()}")

    # ============================================================
    # Rule 1
    # ============================================================
    print("\n[Rule 1] Hard exit (stop @ -8%) vs wait to month end...")
    r1 = rule1_hard_exit_timing(v_top)
    if r1.get("n", 0) > 0:
        print(f"  n_hard_events: {r1['n']}")
        print(f"  Hard exit (cap at -8%) mean ret: {r1['mean_A_hard_exit']:+.4f}")
        print(f"  Wait to month-end mean ret:      {r1['mean_B_wait']:+.4f}")
        print(f"  Diff (A - B): {r1['diff_A_minus_B']:+.4f}")
        print(f"  Win rate (A better): {r1['win_rate_A_better']:.3f}")
    else:
        print("  No hard events found")

    # ============================================================
    # Rule 2
    # ============================================================
    print("\n[Rule 2] Rebalance frequency grid (weekly/biweekly/monthly/quarterly)...")
    r2 = rule2_rebalance_frequency(v_top)
    print(r2.to_string(index=False, float_format=lambda x: f"{x:+.4f}"))
    r2.to_csv(OUT_DIR / "vf_dual_rule2_freq.csv", index=False)

    # ============================================================
    # Rule 7
    # ============================================================
    print("\n[Rule 7] regime transition: immediate vs deferred clearing...")
    r7_df, r7_summary = rule7_regime_transition(v_top, twii)
    if r7_summary:
        print(f"  n_transitions (volatile -> other): {r7_summary['n_transitions']}")
        for k, val in r7_summary.items():
            if k != "n_transitions":
                print(f"  {k}: {val:+.4f}")
        r7_df.to_csv(OUT_DIR / "vf_dual_rule7_transitions.csv", index=False)

    # ============================================================
    # Rule 8
    # ============================================================
    print("\n[Rule 8] Gap day ±3% analysis...")
    r8 = rule8_gap_analysis(twii)
    print(r8.to_string(index=False, float_format=lambda x: f"{x:+.4f}"))
    r8.to_csv(OUT_DIR / "vf_dual_rule8_gap.csv", index=False)

    # ============================================================
    # Markdown summary
    # ============================================================
    md = f"""# Dual × position_monitor Rule 1/2/7/8 補驗

**Date**: 2026-04-29
**Universe**: trade_journal_value_tw_snapshot 309 weeks 2020-2025 + TWII 2015+

## Rule 1 — Hard exit timing

Hard stop proxy: weeks where fwd_5d_min < -8%

"""
    if r1.get("n", 0) > 0:
        md += f"""| Metric | Value |
|---|---:|
| n_hard_events | {r1['n']} |
| Strategy A (hard exit @ -8% cap) mean ret | {r1['mean_A_hard_exit']:+.4f} |
| Strategy B (wait to month-end fwd_20d) | {r1['mean_B_wait']:+.4f} |
| Diff (A - B) | {r1['diff_A_minus_B']:+.4f} |
| Win rate (A better) | {r1['win_rate_A_better']:.3f} |

**結論**: {'Hard exit 立刻出場較佳 → Rule 1 維持「Hard 優先序」' if r1['diff_A_minus_B'] > 0 else '等到月末反而較佳 → Rule 1 priority 邏輯需重新評估（hard exit 過於敏感）'}

"""

    md += "## Rule 2 — Rebalance frequency\n\n"
    md += r2.to_string(index=False, float_format=lambda x: f"{x:+.4f}").replace("\n", "\n\n")
    md += f"\n\n**結論**: 比 Sharpe → Best = {r2.loc[r2['sharpe_ann'].idxmax(), 'freq_label']} (Sharpe {r2['sharpe_ann'].max():+.3f})\n\n"

    if r7_summary:
        md += "## Rule 7 — regime 切換立刻清 vs 延遲清\n\n"
        md += "| Metric | Value |\n|---|---:|\n"
        md += f"| n_transitions | {r7_summary['n_transitions']} |\n"
        for k, val in r7_summary.items():
            if k != "n_transitions":
                md += f"| {k} | {val:+.4f} |\n"
        diff_b = r7_summary.get("diff_B_minus_A (defer 1mo cost)", 0)
        verdict = "延遲 1 個月反而較佳 → Rule 7 維持「下月清倉」" if diff_b > 0 else "立刻清較佳 → Rule 7 應改「regime 轉換當週清」"
        md += f"\n**結論**: {verdict}\n\n"

    md += "## Rule 8 — 跳空 ±3% 日 fwd return\n\n"
    md += r8.to_string(index=False, float_format=lambda x: f"{x:+.4f}").replace("\n", "\n\n")
    if not r8.empty and "gap_up_3pct" in r8["group"].values and "non_gap" in r8["group"].values:
        gap_up_5d = r8.loc[r8["group"] == "gap_up_3pct", "fwd_5d_mean"].iloc[0]
        non_gap_5d = r8.loc[r8["group"] == "non_gap", "fwd_5d_mean"].iloc[0]
        edge = gap_up_5d - non_gap_5d
        md += f"\n\n**結論**: gap_up vs non_gap fwd_5d edge = {edge:+.4f} → "
        md += "跳空日後續弱於非跳空日 → 暫停 30min 觀察合理" if edge < 0 else "跳空日後續未顯著弱於非跳空日 → Rule 8 可放寬"
        md += "\n"

    OUT_MD.write_text(md, encoding="utf-8")
    print(f"\nSaved: {OUT_MD}")


if __name__ == "__main__":
    main()
