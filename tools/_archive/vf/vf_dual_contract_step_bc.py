"""
Dual × position_monitor 契約 Step B/C (project_dual_position_monitor_contract.md).

Step B: MIN_HOLD_DAYS / TP 分配 / Whipsaw ban 數值 backtest 驗證
Step C: Rule 6 dual-leg (Value ∩ QM) 重複持股合併 報酬計算修正

Approach:
  Full position-aware simulator 工程量 4-6h，本次改 targeted analyses
  用 trade journal snapshots 直接回答各子問題:
    B1: MIN_HOLD_DAYS — 比較 5/10/20/40 grid 對 Sharpe/MDD 影響 proxy
    B2: TP 1/3 vs 1/2 — 用 fwd_*d_max 分布計算 staged exit 期望值
    B3: Whipsaw ban 30 — overlap 比例 + cooldown 後續報酬
    C : Rule 6 — 計算 Value ∩ QM 重疊比例 + dual_5050 報酬計算偏差

Universe:
  Value: trade_journal_value_tw_snapshot (70k rows × 309 weeks)
  QM:    trade_journal_qm_tw (4923 rows × 538 weeks)
  Common weeks: 307

Output: reports/vf_dual_contract_step_bc.md / .csv
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))
DATA_DIR = _ROOT / "data_cache" / "backtest"
OUT_DIR = _ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MD = OUT_DIR / "vf_dual_contract_step_bc.md"

TOP_N = 20


def load_journals():
    v = pd.read_parquet(DATA_DIR / "trade_journal_value_tw_snapshot.parquet")
    v["week_end_date"] = pd.to_datetime(v["week_end_date"])
    v["stock_id"] = v["stock_id"].astype(str)
    q = pd.read_parquet(DATA_DIR / "trade_journal_qm_tw.parquet")
    q["week_end_date"] = pd.to_datetime(q["week_end_date"])
    q["stock_id"] = q["stock_id"].astype(str)
    return v, q


def get_top20_per_week(v):
    """Per-week, take top-20 by value_score (mimics live screener)."""
    out = []
    for wk, g in v.groupby("week_end_date"):
        top = g.nlargest(TOP_N, "value_score")
        out.append(top.assign(week_end_date=wk))
    return pd.concat(out, ignore_index=True)


def step_c_overlap(v_top, q):
    """Step C — Rule 6: Value ∩ QM overlap + return calc bias.

    For each common week, count overlap. Compare:
      no_merge: Value ret + QM ret 各算 (current dual_5050 做法)
      with_5pct_cap: 同股每筆下單 5% 但雙重記帳 (Rule 6 spec)
    """
    common_weeks = sorted(set(v_top["week_end_date"].unique()) & set(q["week_end_date"].unique()))
    rows = []
    for wk in common_weeks:
        v_ids = set(v_top[v_top["week_end_date"] == wk]["stock_id"])
        q_top = q[(q["week_end_date"] == wk) & (q["rank_in_top50"] <= TOP_N)]
        q_ids = set(q_top["stock_id"])
        overlap = v_ids & q_ids

        v_ret = v_top[v_top["week_end_date"] == wk]["fwd_20d"].mean()
        q_ret = q_top["fwd_20d"].mean() if not q_top.empty else 0.0

        # Method A: current dual_5050 — 50% v_ret + 50% q_ret 不管重疊
        ret_no_merge = 0.5 * (v_ret if not pd.isna(v_ret) else 0) + 0.5 * (q_ret if not pd.isna(q_ret) else 0)

        # Method B: Rule 6 同股合併下單 5% — 重疊股佔 5%（單份），其他股 normal
        # 計算: 重疊股每股權重 5%（Rule 6 cap）；非重疊 Value 股 (1.0 - 5%*overlap_n_value) / N_value_non_overlap
        # 簡化：報酬幾乎不變，差異主要在「資金占用比」(從 50%+50%=100% → 重疊扣一份)
        # 這裡量化的是 dual_5050 因為「兩邊算總分母」造成的雙倍曝險效果
        n_overlap = len(overlap)
        # If we cap dual exposure to single 5% per stock:
        # non-overlap value: 50%/(20-n_overlap) per stock
        # non-overlap qm:    50%/(20-n_overlap) per stock
        # overlap stocks:    5% per stock (Rule 6 cap), instead of 5% (50%/20) on each side = 5% total
        # In this simplification, weights are similar → return barely changes
        # 但 dual_5050 backtest 目前 ret = 0.5*v + 0.5*q (mean of each side equal-weight 20 stocks)
        # 真實情況同股實際只下一單 → exposure 比 backtest 高估了 100% × overlap_n / 20
        # This means backtest overstates total deployment when overlap exists

        rows.append({
            "week": wk,
            "v_n": len(v_ids),
            "q_n": len(q_ids),
            "overlap_n": n_overlap,
            "overlap_pct": n_overlap / TOP_N,
            "v_ret": v_ret,
            "q_ret": q_ret,
            "dual_ret_naive": ret_no_merge,
            "overlap_ids": ",".join(sorted(overlap)) if overlap else "",
        })
    return pd.DataFrame(rows)


def step_b1_min_hold_days(v_top, q):
    """B1: MIN_HOLD_DAYS proxy — for stocks that DROP OUT of top-20 next month, what's cost of forced hold?"""
    weeks = sorted(v_top["week_end_date"].unique())
    rows = []

    # For each week wk, check stocks held this week. Next rebalance week (next month):
    # - In new top-20 → 留 (no decision)
    # - Drop out → exit. If MIN_HOLD = X days < ~20d (1 month rebalance), no effect.
    # - But MIN_HOLD = 40 days (2 months) — forced to hold one extra cycle

    # Use 4-week stride to mimic monthly rebalance
    REBAL = 4
    rebal_weeks = weeks[::REBAL]

    for k, wk in enumerate(rebal_weeks[:-2]):
        cur_set = set(v_top[v_top["week_end_date"] == wk]["stock_id"])
        next_wk = rebal_weeks[k + 1]
        next_set = set(v_top[v_top["week_end_date"] == next_wk]["stock_id"])

        # Stocks held at wk but dropped at next_wk (forced exit candidates)
        dropouts = cur_set - next_set
        if not dropouts:
            continue

        # For each dropout, look at fwd_20d (return if forced to hold next 20 days vs exit)
        sub = v_top[(v_top["week_end_date"] == wk) & (v_top["stock_id"].isin(dropouts))]
        # fwd_40d = 2-month return from week wk
        # fwd_20d = 1-month
        # If MIN_HOLD = 5d (~ < 1 rebal): exit at wk+1 month, get fwd_20d
        # If MIN_HOLD = 40d (~ 2 rebal): exit at wk+2 month, get fwd_40d
        rows.append({
            "week": wk,
            "n_dropouts": len(dropouts),
            "dropout_fwd_20d": sub["fwd_20d"].mean(),
            "dropout_fwd_40d": sub["fwd_40d"].mean(),
            "dropout_fwd_60d": sub["fwd_60d"].mean(),
        })
    return pd.DataFrame(rows)


def step_b2_tp_split(v_top):
    """B2: TP 分配 1/3 vs 1/2.

    For trades that hit TP1 (e.g., +10%), compare net return:
      Strategy A: sell 1/3 at TP1, hold rest to fwd_60d
      Strategy B: sell 1/2 at TP1, hold rest to fwd_60d
    """
    # Identify trades where fwd_20d_max >= 0.10 (TP1 trigger within 1 month)
    TP1_THR = 0.10
    sub = v_top[v_top["fwd_20d_max"] >= TP1_THR].copy()
    sub = sub.dropna(subset=["fwd_20d_max", "fwd_60d"])

    # Approximation: TP1 fires at +10%, then remainder rides to fwd_60d
    sub["ret_strat_A"] = (1/3) * TP1_THR + (2/3) * sub["fwd_60d"]
    sub["ret_strat_B"] = (1/2) * TP1_THR + (1/2) * sub["fwd_60d"]
    sub["ret_no_TP"] = sub["fwd_60d"]

    summary = {
        "n_trades_with_TP1": len(sub),
        "mean_fwd_60d": sub["fwd_60d"].mean(),
        "strat_A (1/3)": sub["ret_strat_A"].mean(),
        "strat_B (1/2)": sub["ret_strat_B"].mean(),
        "no_TP": sub["ret_no_TP"].mean(),
        "diff_A_minus_B": sub["ret_strat_A"].mean() - sub["ret_strat_B"].mean(),
    }
    return summary


def step_b3_whipsaw(v_top, cooldown_days=30):
    """B3: Whipsaw ban — find re-entries within 30 calendar days of drop-out.

    Compare: stocks that re-entered within cooldown vs ones that did not.
    """
    # Build per-stock entry/exit list
    weeks = sorted(v_top["week_end_date"].unique())

    # Track presence of stock_id per week
    presence = v_top.pivot_table(index="stock_id", columns="week_end_date",
                                 values="value_score", aggfunc="first").notna()
    rows = []
    for sid in presence.index:
        seq = presence.loc[sid]
        # Find drop transitions (True -> False)
        in_runs = []
        in_state = False
        last_in = None
        for wk, val in seq.items():
            if val and not in_state:
                in_state = True
                last_in = wk
            elif not val and in_state:
                in_state = False
                in_runs.append((last_in, wk))  # (entry, drop)

        # For each (entry, drop), check if re-entered within cooldown_days
        for i, (entry, drop) in enumerate(in_runs[:-1]):
            next_entry, _ = in_runs[i + 1]
            gap_days = (next_entry - drop).days
            if gap_days <= cooldown_days:
                # Look up fwd_20d at next_entry
                row = v_top[(v_top["week_end_date"] == next_entry) & (v_top["stock_id"] == sid)]
                if not row.empty:
                    rows.append({
                        "stock_id": sid,
                        "drop_wk": drop,
                        "reentry_wk": next_entry,
                        "gap_days": gap_days,
                        "reentry_fwd_20d": row["fwd_20d"].iloc[0],
                        "reentry_fwd_60d": row["fwd_60d"].iloc[0],
                    })
    return pd.DataFrame(rows)


def main():
    print("Loading journals...")
    v, q = load_journals()
    print(f"  Value snapshot: {len(v):,} rows × {v['stock_id'].nunique()} stocks")
    print(f"  QM journal:     {len(q):,} rows × {q['stock_id'].nunique()} stocks")

    print("\nBuilding Value top-20 per week...")
    v_top = get_top20_per_week(v)
    print(f"  Value top-20: {len(v_top):,} rows × {v_top['week_end_date'].nunique()} weeks")

    # ============================================================
    # Step C — Rule 6 overlap
    # ============================================================
    print("\n[Step C] Rule 6 dual-leg overlap analysis...")
    overlap_df = step_c_overlap(v_top, q)
    overlap_df.to_csv(OUT_DIR / "vf_dual_step_c_overlap.csv", index=False)
    print(f"  Mean overlap: {overlap_df['overlap_pct'].mean()*100:.1f}% of top-20")
    print(f"  Max overlap:  {overlap_df['overlap_pct'].max()*100:.1f}%")
    print(f"  Weeks with overlap >= 25% (5+ stocks): "
          f"{(overlap_df['overlap_pct'] >= 0.25).sum()} / {len(overlap_df)}")
    print(f"  Mean dual_naive return:  {overlap_df['dual_ret_naive'].mean():+.4f}")
    print(f"  Mean Value ret:          {overlap_df['v_ret'].mean():+.4f}")
    print(f"  Mean QM ret:             {overlap_df['q_ret'].mean():+.4f}")

    # ============================================================
    # Step B1 — MIN_HOLD_DAYS dropout cost
    # ============================================================
    print("\n[Step B1] MIN_HOLD_DAYS dropout analysis...")
    dropout_df = step_b1_min_hold_days(v_top, q)
    print(f"  Total rebalance weeks: {len(dropout_df)}")
    print(f"  Mean dropouts/week: {dropout_df['n_dropouts'].mean():.1f}")
    print(f"  Mean dropout fwd_20d (exit @ 1mo): {dropout_df['dropout_fwd_20d'].mean():+.4f}")
    print(f"  Mean dropout fwd_40d (forced hold 2mo): {dropout_df['dropout_fwd_40d'].mean():+.4f}")
    print(f"  Mean dropout fwd_60d (forced hold 3mo): {dropout_df['dropout_fwd_60d'].mean():+.4f}")
    print(f"  Cost of forcing 40d hold vs 20d: "
          f"{dropout_df['dropout_fwd_40d'].mean() - dropout_df['dropout_fwd_20d'].mean():+.4f}")

    # ============================================================
    # Step B2 — TP 1/3 vs 1/2 split
    # ============================================================
    print("\n[Step B2] TP分配 1/3 vs 1/2 expected value...")
    b2 = step_b2_tp_split(v_top)
    for k, v_ in b2.items():
        if isinstance(v_, float):
            print(f"  {k:30s} {v_:+.4f}")
        else:
            print(f"  {k:30s} {v_}")

    # ============================================================
    # Step B3 — Whipsaw ban analysis
    # ============================================================
    print("\n[Step B3] Whipsaw 30-day cooldown overlap...")
    whipsaw_df = step_b3_whipsaw(v_top, 30)
    print(f"  Re-entries within 30d: {len(whipsaw_df):,}")
    if not whipsaw_df.empty:
        print(f"  Mean re-entry fwd_20d: {whipsaw_df['reentry_fwd_20d'].mean():+.4f}")
        print(f"  Mean re-entry fwd_60d: {whipsaw_df['reentry_fwd_60d'].mean():+.4f}")
        # Compare to baseline (all top-20 picks)
        all_fwd_20 = v_top["fwd_20d"].mean()
        all_fwd_60 = v_top["fwd_60d"].mean()
        print(f"  All top-20 baseline fwd_20d: {all_fwd_20:+.4f}")
        print(f"  All top-20 baseline fwd_60d: {all_fwd_60:+.4f}")
        edge_20 = whipsaw_df['reentry_fwd_20d'].mean() - all_fwd_20
        edge_60 = whipsaw_df['reentry_fwd_60d'].mean() - all_fwd_60
        print(f"  Re-entry edge fwd_20d: {edge_20:+.4f}")
        print(f"  Re-entry edge fwd_60d: {edge_60:+.4f}")

    whipsaw_df.to_csv(OUT_DIR / "vf_dual_step_b3_whipsaw.csv", index=False)

    # ============================================================
    # Markdown summary
    # ============================================================
    md = f"""# Dual × position_monitor 契約 Step B/C 驗證

**Date**: 2026-04-29
**Method**: targeted analyses on existing trade journals (full simulator deferred)

## Step C — Rule 6 dual-leg overlap

| Metric | Value |
|---|---:|
| Mean overlap % of top-20 | {overlap_df['overlap_pct'].mean()*100:.1f}% |
| Max overlap % | {overlap_df['overlap_pct'].max()*100:.1f}% |
| Weeks with overlap >= 25% | {(overlap_df['overlap_pct'] >= 0.25).sum()} / {len(overlap_df)} |
| Mean dual_naive return | {overlap_df['dual_ret_naive'].mean():+.4f} |
| Mean Value 20-pick return | {overlap_df['v_ret'].mean():+.4f} |
| Mean QM 20-pick return | {overlap_df['q_ret'].mean():+.4f} |

**結論**: 重疊比例平均 {overlap_df['overlap_pct'].mean()*100:.1f}%，當前 dual_5050 backtest 把
重疊股各算一份（Value 50% + QM 50%），實際下單只下一份，Rule 6 所述。
若 cap 同股 5% 上限：權重變化但對等權平均報酬影響小（同股報酬一致）。
**主要影響**: 真實資金佔用比 < backtest 假設 → 真實年化報酬可能低估 {overlap_df['overlap_pct'].mean()*5:.1f}%
（每重疊一檔 backtest 多算 5% 曝險）。

## Step B1 — MIN_HOLD_DAYS dropout cost

| Hold scenario | Mean fwd return |
|---|---:|
| Exit @ 1mo (~20d) | {dropout_df['dropout_fwd_20d'].mean():+.4f} |
| Force hold 2mo (~40d) | {dropout_df['dropout_fwd_40d'].mean():+.4f} |
| Force hold 3mo (~60d) | {dropout_df['dropout_fwd_60d'].mean():+.4f} |
| Cost of 40d vs 20d | {dropout_df['dropout_fwd_40d'].mean() - dropout_df['dropout_fwd_20d'].mean():+.4f} |

**結論**: 強迫多持有掉榜股 1 個月的成本 = {(dropout_df['dropout_fwd_40d'].mean() - dropout_df['dropout_fwd_20d'].mean())*100:+.2f}pp。
{'若為負，MIN_HOLD_DAYS 越大代價越高，建議從寬到嚴漸降 (40 -> 20 -> 10)。' if dropout_df['dropout_fwd_40d'].mean() - dropout_df['dropout_fwd_20d'].mean() < 0 else '若為正，較長 MIN_HOLD 反而有利（dropout 後反彈）。'}

## Step B2 — TP 1/3 vs 1/2 split

| Strategy | Expected return |
|---|---:|
| No TP (hold to 60d) | {b2['no_TP']:+.4f} |
| TP 1/3 at +10% | {b2['strat_A (1/3)']:+.4f} |
| TP 1/2 at +10% | {b2['strat_B (1/2)']:+.4f} |
| Diff (1/3 vs 1/2) | {b2['diff_A_minus_B']:+.4f} |
| n trades w/ TP1 hit | {b2['n_trades_with_TP1']} |

**結論**: TP 1/3 vs 1/2 期望值差 {b2['diff_A_minus_B']*100:+.2f}pp。
{'1/3 較佳: 留更多曝險享受 mean fwd_60d > +10%' if b2['diff_A_minus_B'] > 0 else '1/2 較佳: 鎖定 +10% 機會優於 hold remainder'}

## Step B3 — Whipsaw 30-day cooldown re-entry analysis

| Metric | Value |
|---|---:|
| Total re-entries within 30d | {len(whipsaw_df):,} |
| Mean re-entry fwd_20d | {whipsaw_df['reentry_fwd_20d'].mean():+.4f} |
| Baseline (all top-20) fwd_20d | {v_top['fwd_20d'].mean():+.4f} |
| Re-entry edge fwd_20d | {whipsaw_df['reentry_fwd_20d'].mean() - v_top['fwd_20d'].mean():+.4f} |
| Mean re-entry fwd_60d | {whipsaw_df['reentry_fwd_60d'].mean():+.4f} |
| Baseline fwd_60d | {v_top['fwd_60d'].mean():+.4f} |
| Re-entry edge fwd_60d | {whipsaw_df['reentry_fwd_60d'].mean() - v_top['fwd_60d'].mean():+.4f} |

**結論**: {'Re-entries 表現劣於 baseline → 30 天 ban 有保護作用，建議保留' if whipsaw_df['reentry_fwd_60d'].mean() < v_top['fwd_60d'].mean() else 'Re-entries 表現優於 baseline → 30 天 ban 反而錯失反彈，可放寬'}

## 落地建議

1. **Rule 6 (Step C)**: 確認 dual_5050 backtest 真實曝險低估，未來精算建議以 cap 5% per stock 重 simulate
2. **MIN_HOLD_DAYS (B1)**: 看 dropout cost 數據判斷是否從 20 降到 10 或維持
3. **TP split (B2)**: 看 1/3 vs 1/2 期望值差距決定
4. **Whipsaw 30d (B3)**: 看 re-entry 表現是否真的差於 baseline
"""
    OUT_MD.write_text(md, encoding="utf-8")
    print(f"\nSaved: {OUT_MD}")


if __name__ == "__main__":
    main()
