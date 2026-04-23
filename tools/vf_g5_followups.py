"""
vf_g5_followups.py - VF-G5 Follow-up #1 & #2

VF-G5 (2026-04-23) Test 1 發現 body_score within-picks IR -0.10 B(rev) @ fwd_40/60，
Test 2 發現 rank 1-5 fwd_20 最好但 fwd_40/60 反被 6-10 超車。兩個 finding 都需要
walk-forward 確認是否跨期穩定（不穩定就是雜訊）。

FU-1: body_score within-picks walk-forward
  - 42 季 quarterly IC (2015-Q3 ~ 2025-Q4)
  - fwd_40 / 60 IC 分季統計；要求 ≥ 67% 季為負才算穩定反轉
  - 若確認 → QM 權重 (F50/Body30/Trend20) 該重新考慮

FU-2: top_n portfolio 比較（10 / 20 / 30 / 50）
  - 依 rank_in_top50 過濾 picks，計算 weekly mean fwd_X
  - 對比 4 個投組的 mean / Sharpe / maxDD / 季勝率
  - walk-forward：每季比勝率

Usage:
    python tools/vf_g5_followups.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

BT_DIR = ROOT / "data_cache" / "backtest"
JOURNAL_PATH = BT_DIR / "trade_journal_qm_tw.parquet"
REPORT_PATH = ROOT / "reports" / "vf_g5_followups.md"


def grade(ir: float) -> str:
    if pd.isna(ir):
        return 'N/A'
    if abs(ir) >= 0.3:
        return 'A' if ir > 0 else 'A(rev)'
    elif abs(ir) >= 0.1:
        return 'B' if ir > 0 else 'B(rev)'
    elif abs(ir) >= 0.05:
        return 'C'
    return 'D'


def _df_to_md(df: pd.DataFrame) -> str:
    if df.empty:
        return "(empty)"
    cols = list(df.columns)
    int_cols = {'n', 'weeks', 'horizon', 'rank_low', 'rank_high', 'n_obs', 'n_q',
                'top_n', 'pos_q', 'neg_q', 'n_weeks'}
    pct_cols = {'mean_ret', 'winrate', 'fill_rate', 'top', 'bot', 'spread',
                'mean_q_ic', 'neg_q_rate', 'pos_q_rate', 'maxdd', 'annual_ret'}

    def fmt(col, v):
        if isinstance(v, float):
            if pd.isna(v):
                return "NaN"
            if col in int_cols:
                return str(int(v))
            if col in pct_cols:
                return f"{v:+.2%}"
            return f"{v:+.4f}"
        return str(v)

    lines = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(fmt(c, row[c]) for c in cols) + " |")
    return "\n".join(lines)


# ================================================================
# FU-1: body_score within-picks quarterly walk-forward
# ================================================================
def fu1_body_walkforward(j: pd.DataFrame, horizons: list[int]) -> dict:
    """
    每季算 body_score vs fwd_X 的 spearman IC（per week in quarter），然後
    aggregate per quarter。回傳 q_df + summary stats。
    """
    j = j.copy()
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])
    j['quarter'] = j['week_end_date'].dt.to_period('Q')

    out = {}
    for h in horizons:
        fwd = f'fwd_{h}d'
        if fwd not in j.columns:
            continue
        q_rows = []
        for q, grp in j.groupby('quarter'):
            sub = grp[['body_score', fwd, 'week_end_date']].dropna()
            if sub.empty:
                continue
            weekly = []
            for _, wgrp in sub.groupby('week_end_date'):
                if len(wgrp) < 5:
                    continue
                rho, _ = stats.spearmanr(wgrp['body_score'], wgrp[fwd])
                if not pd.isna(rho):
                    weekly.append(rho)
            if not weekly:
                continue
            arr = np.array(weekly)
            q_rows.append({
                'quarter': str(q),
                'n_weeks': len(arr),
                'mean_q_ic': arr.mean(),
                'q_std': arr.std(ddof=1) if len(arr) > 1 else np.nan,
            })
        q_df = pd.DataFrame(q_rows)
        if q_df.empty:
            out[h] = None
            continue

        pos_q = (q_df['mean_q_ic'] > 0).sum()
        neg_q = (q_df['mean_q_ic'] < 0).sum()
        n_q = len(q_df)
        mean_ic = q_df['mean_q_ic'].mean()
        std_ic = q_df['mean_q_ic'].std(ddof=1)
        ir_q = mean_ic / std_ic if std_ic and std_ic > 0 else np.nan

        out[h] = {
            'q_df': q_df,
            'pos_q': pos_q,
            'neg_q': neg_q,
            'n_q': n_q,
            'pos_q_rate': pos_q / n_q,
            'neg_q_rate': neg_q / n_q,
            'mean_ic': mean_ic,
            'std_ic': std_ic,
            'ir_q': ir_q,
            'grade': grade(ir_q),
        }
    return out


# ================================================================
# FU-2: top_n portfolio walk-forward
# ================================================================
def fu2_topn_portfolio(j: pd.DataFrame, top_n_list: list[int],
                       horizons: list[int]) -> dict:
    """
    對每個 top_n ∈ top_n_list，過濾 rank_in_top50 <= N 的 picks，以週為單位
    計算 equal-weighted mean fwd_X。回傳：
      - summary: mean / std / sharpe / maxdd / positive_weeks_rate per top_n
      - q_df: quarterly comparison (哪個 top_n 季勝)
    """
    j = j.copy()
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])
    j['quarter'] = j['week_end_date'].dt.to_period('Q')

    out = {}
    for h in horizons:
        fwd = f'fwd_{h}d'
        if fwd not in j.columns:
            continue
        per_topn = {}
        for N in top_n_list:
            sub = j[j['rank_in_top50'] <= N][['week_end_date', 'quarter', fwd]].dropna()
            # weekly mean
            weekly = sub.groupby('week_end_date').agg(mean_ret=(fwd, 'mean'),
                                                      n_picks=(fwd, 'count')).reset_index()
            if weekly.empty:
                continue
            mean_w = weekly['mean_ret'].mean()
            std_w = weekly['mean_ret'].std(ddof=1)
            sharpe_w = mean_w / std_w if std_w and std_w > 0 else np.nan
            # Annualized: ~52 weeks / h-day horizon. fwd_h ≈ cumulative over h days;
            # ret is per-pick cumulative not weekly rebalanced — 用 simplified annualization
            # annual_ret = mean_w * (52 * fwd_scale); for fwd_20d w/ weekly sampling,
            # approximate number of non-overlapping periods per year ≈ 250/20 = 12.5
            # For comparability, use: annual_ret ≈ mean_w * (252 / h)
            annual_ret = mean_w * (252 / h)
            # maxDD on cumulative (assuming sequential picks)
            cum = (1 + weekly['mean_ret']).cumprod()
            peak = cum.cummax()
            dd = cum / peak - 1
            maxdd = dd.min()
            pos_w = (weekly['mean_ret'] > 0).sum()
            per_topn[N] = {
                'mean_w': mean_w,
                'std_w': std_w,
                'sharpe_w': sharpe_w,
                'maxdd': maxdd,
                'n_weeks': len(weekly),
                'pos_w': pos_w,
                'pos_w_rate': pos_w / len(weekly),
                'annual_ret_approx': annual_ret,
            }

        # quarterly comparison — 在每季計算各 top_n 的 mean fwd_X，看哪個勝
        q_rows = []
        quarters = sorted(j['quarter'].unique())
        for q in quarters:
            qsub = j[j['quarter'] == q]
            qrow = {'quarter': str(q)}
            for N in top_n_list:
                sel = qsub[qsub['rank_in_top50'] <= N]
                if sel.empty:
                    qrow[f'top{N}'] = np.nan
                else:
                    qrow[f'top{N}'] = sel[fwd].dropna().mean()
            q_rows.append(qrow)
        q_df = pd.DataFrame(q_rows)

        # Count wins: 哪個 top_n 在該季最高
        topn_cols = [f'top{N}' for N in top_n_list]
        if not q_df.empty and all(c in q_df.columns for c in topn_cols):
            sub_q = q_df[topn_cols].dropna()
            if not sub_q.empty:
                winners = sub_q.idxmax(axis=1)
                win_counts = winners.value_counts().to_dict()
                win_rates = {c: win_counts.get(c, 0) / len(sub_q) for c in topn_cols}
            else:
                win_rates = {c: 0 for c in topn_cols}
        else:
            win_rates = {}

        out[h] = {
            'per_topn': per_topn,
            'q_df': q_df,
            'q_win_rates': win_rates,
        }
    return out


# ================================================================
# Main
# ================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default=str(JOURNAL_PATH))
    ap.add_argument("--horizons", type=int, nargs='+', default=[20, 40, 60])
    args = ap.parse_args()

    print("=" * 80)
    print("VF-G5 Follow-ups #1 (body walk-forward) + #2 (top_n portfolio)")
    print("=" * 80)

    j = pd.read_parquet(args.journal)
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])
    print(f"Journal: {len(j)} picks × {len(j.columns)} cols")
    print(f"Date range: {j['week_end_date'].min().date()} ~ {j['week_end_date'].max().date()}")
    print(f"Stocks: {j['stock_id'].nunique()}, quarters: {j['week_end_date'].dt.to_period('Q').nunique()}")
    print()

    # === FU-1 ===
    print("=" * 80)
    print("FU-1: body_score within-picks quarterly walk-forward")
    print("=" * 80)
    fu1 = fu1_body_walkforward(j, args.horizons)
    fu1_summary = []
    for h in args.horizons:
        r = fu1.get(h)
        if r is None:
            continue
        fu1_summary.append({
            'horizon': h,
            'n_q': r['n_q'],
            'pos_q': r['pos_q'],
            'neg_q': r['neg_q'],
            'pos_q_rate': r['pos_q_rate'],
            'neg_q_rate': r['neg_q_rate'],
            'mean_ic': r['mean_ic'],
            'ir_q': r['ir_q'],
            'grade': r['grade'],
        })
    fu1_df = pd.DataFrame(fu1_summary)
    print(fu1_df.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))
    print()
    # Decision
    for h in args.horizons:
        r = fu1.get(h)
        if r is None:
            continue
        if r['neg_q_rate'] >= 0.67:
            print(f"  fwd_{h}d: {r['neg_q_rate']:.0%} 季負 IC >= 67% → **確認反轉穩定**，可動 body 權重")
        else:
            print(f"  fwd_{h}d: {r['neg_q_rate']:.0%} 季負 IC < 67% → 雜訊，body 反轉不穩定")
    print()

    # === FU-2 ===
    print("=" * 80)
    print("FU-2: top_n portfolio 比較 (10/20/30/50)")
    print("=" * 80)
    top_n_list = [10, 20, 30, 50]
    fu2 = fu2_topn_portfolio(j, top_n_list, args.horizons)

    fu2_summary_rows = []
    for h in args.horizons:
        r = fu2.get(h)
        if r is None:
            continue
        print(f"\nhorizon = fwd_{h}d")
        for N in top_n_list:
            pt = r['per_topn'].get(N)
            if pt is None:
                continue
            fu2_summary_rows.append({
                'horizon': h,
                'top_n': N,
                'n_weeks': pt['n_weeks'],
                'mean_ret': pt['mean_w'],
                'std_ret': pt['std_w'],
                'sharpe_w': pt['sharpe_w'],
                'annual_ret': pt['annual_ret_approx'],
                'maxdd': pt['maxdd'],
                'pos_w_rate': pt['pos_w_rate'],
                'q_win_rate': r['q_win_rates'].get(f'top{N}', 0),
            })

    fu2_df = pd.DataFrame(fu2_summary_rows)
    print(fu2_df.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))

    # === Save report ===
    print("\n" + "=" * 80)
    print("Saving report...")
    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = []
    lines.append("# VF-G5 Follow-ups (2026-04-23)\n")
    lines.append(f"- Journal: {len(j)} picks × {j['stock_id'].nunique()} 檔")
    lines.append(f"- Date range: {j['week_end_date'].min().date()} ~ {j['week_end_date'].max().date()}")
    lines.append(f"- Quarters: {j['week_end_date'].dt.to_period('Q').nunique()}\n")

    # --- FU-1 ---
    lines.append("## FU-1: body_score within-picks quarterly walk-forward\n")
    lines.append("每季計算 body_score vs fwd_X 的 Spearman IC (per week in quarter)，")
    lines.append("然後 aggregate per quarter。檢查「季負 IC 比例 ≥ 67%」→ 反轉穩定。\n")
    lines.append(_df_to_md(fu1_df))
    lines.append("")

    # per-quarter detail 只顯示 horizon=40 & 60
    for h in [40, 60]:
        r = fu1.get(h)
        if r is None:
            continue
        lines.append(f"### fwd_{h}d per-quarter IC\n")
        lines.append(_df_to_md(r['q_df'].head(50)))  # head 50 for brevity
        if len(r['q_df']) > 50:
            lines.append(f"\n(共 {len(r['q_df'])} 季，只顯示前 50)")
        lines.append("")

    # --- FU-2 ---
    lines.append("## FU-2: top_n portfolio 比較\n")
    lines.append("對 trade_journal 依 `rank_in_top50 <= N` 過濾 picks，計算每週 equal-weighted ")
    lines.append("mean fwd_X，得到投組的 mean / Sharpe / maxDD / 週勝率 / 季勝率。\n")
    lines.append("註：annual_ret 用 `mean_ret * 252/h` 近似（非 overlapping 也非實際 rebalance，僅供相對比較）\n")
    lines.append(_df_to_md(fu2_df))
    lines.append("")

    # --- 結論判讀 ---
    lines.append("## 結論判讀\n")

    # FU-1 conclusion
    lines.append("### FU-1: body_score 反轉是否穩定\n")
    reversal_confirmed = {}
    for h in args.horizons:
        r = fu1.get(h)
        if r is None:
            continue
        reversal_confirmed[h] = r['neg_q_rate'] >= 0.67
        msg = "**穩定反轉**" if reversal_confirmed[h] else "雜訊"
        lines.append(f"- fwd_{h}d: {r['neg_q']}/{r['n_q']} 季負 IC ({r['neg_q_rate']:.0%})，IR_q={r['ir_q']:+.3f} ({r['grade']}) → {msg}")

    any_confirmed = any(reversal_confirmed.values())
    long_confirmed = reversal_confirmed.get(40, False) or reversal_confirmed.get(60, False)
    if long_confirmed:
        lines.append("\n- 建議：fwd_40/60 任一確認穩定反轉 → QM 權重 F50/Body30/Trend20 應重新檢視")
        lines.append("  - 選項 A: body 降為 15% (拿 15% 給 F 或 Trend)")
        lines.append("  - 選項 B: body 改 filter-only (body >= 40 pass)，不再當 ranking 變數")
        lines.append("  - 下一步：跑 QM simulator with W_body=0 或 W_body=0.15 比 WF Sharpe")
    else:
        lines.append("\n- 結論：body 反轉 **不穩定**，歸檔為雜訊")
        lines.append("  - QM 權重 50/30/20 **不動**")
        lines.append("  - VF-G4 full-universe body IR +0.073 仍是 body 的正確角色")

    lines.append("")

    # FU-2 conclusion
    lines.append("### FU-2: top_n 上限是否調整\n")
    for h in args.horizons:
        r = fu2.get(h)
        if r is None:
            continue
        # Find best by annual_ret and by sharpe
        sub_rows = [row for row in fu2_summary_rows if row['horizon'] == h]
        if not sub_rows:
            continue
        best_ret = max(sub_rows, key=lambda x: x['annual_ret'])
        best_sharpe = max(sub_rows, key=lambda x: x['sharpe_w'] if not pd.isna(x['sharpe_w']) else -999)
        lines.append(f"- fwd_{h}d: best annual_ret = top_{best_ret['top_n']} ({best_ret['annual_ret']:+.2%})，"
                     f"best Sharpe = top_{best_sharpe['top_n']} ({best_sharpe['sharpe_w']:+.3f})")

    # Specific recommendation
    # Check if top 10 or top 20 consistently beats top 50 on annual_ret and sharpe
    all_rows = fu2_summary_rows
    if all_rows:
        # Across horizons, does top_10 or top_20 lead?
        sharpe_by_topn = {}
        ret_by_topn = {}
        for N in top_n_list:
            n_rows = [r for r in all_rows if r['top_n'] == N]
            if n_rows:
                sharpe_by_topn[N] = np.mean([r['sharpe_w'] for r in n_rows if not pd.isna(r['sharpe_w'])])
                ret_by_topn[N] = np.mean([r['annual_ret'] for r in n_rows])
        lines.append(f"\n- 跨 horizon 平均 Sharpe: " + ", ".join(f"top_{N}={s:+.3f}" for N, s in sharpe_by_topn.items()))
        lines.append(f"- 跨 horizon 平均 annual_ret: " + ", ".join(f"top_{N}={r:+.2%}" for N, r in ret_by_topn.items()))
        best_sharpe_n = max(sharpe_by_topn, key=sharpe_by_topn.get)
        best_ret_n = max(ret_by_topn, key=ret_by_topn.get)
        if best_sharpe_n < 50 and best_ret_n < 50:
            lines.append(f"\n- **建議：top_n 由 50 下修至 {max(best_sharpe_n, best_ret_n)}**"
                         f"（Sharpe 最佳 top_{best_sharpe_n}, annual_ret 最佳 top_{best_ret_n}）")
        elif best_sharpe_n == 50 or best_ret_n == 50:
            lines.append(f"\n- 結論：top_50 仍在 Sharpe / annual_ret 其一最佳，下修風險高 → **維持 top_50**")
        else:
            lines.append(f"\n- 結論：混合 → 無明確 dominant top_n，維持 top_50")

    lines.append("\n## 產出\n")
    lines.append("- `reports/vf_g5_followups.md`")
    fu1_df.to_csv(REPORT_PATH.parent / "vf_g5_fu1_body_wf.csv", index=False)
    fu2_df.to_csv(REPORT_PATH.parent / "vf_g5_fu2_topn_portfolio.csv", index=False)
    lines.append("- `reports/vf_g5_fu1_body_wf.csv`")
    lines.append("- `reports/vf_g5_fu2_topn_portfolio.csv`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
