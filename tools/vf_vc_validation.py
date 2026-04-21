"""
vf_vc_validation.py - VF-VC 營收窗口統一驗證

問題：Value 用月營收 3m 滾動 YoY（`revenue_score` 基線），QM 用季營收 4Q
       兩者窗口不一致，需驗哪個 IC 高。

比較 3 種定義：
  R_monthly_3m  ← 既有基線（snapshot.revenue_score；月營收 3m rolling YoY）
  R_monthly_1m  ← 單月最新 YoY（更快但更吵）
  R_quarterly_4q_ttm ← 季營收 4Q TTM YoY（跟 QM 對齊，較慢較穩）

每個定義算 0-100 分（同 compute_revenue_score 的分檔邏輯），再算 vs fwd_60d
的 rank IC + decile spread。IR 最高者為贏。

Usage:
    python tools/vf_vc_validation.py                   # 預設 horizon 60
    python tools/vf_vc_validation.py --horizon 40
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
SNAPSHOT_PATH = BT_DIR / "trade_journal_value_tw_snapshot.parquet"
MONTHLY_REV_PATH = BT_DIR / "financials_revenue.parquet"
INCOME_PATH = BT_DIR / "financials_income.parquet"


# ================================================================
# Scoring function — 同 compute_historical_fscore.compute_revenue_score
# ================================================================
def yoy_to_score(yoy_latest: float, yoy_prev: float | None) -> float:
    """統一打分：YoY 已轉正 → +10；衰退但收斂 → + min(20, diff*2)；加速衰退 → -。"""
    score = 50.0
    if pd.isna(yoy_latest):
        return score
    if yoy_latest > 0:
        score += 10
    elif yoy_prev is not None and not pd.isna(yoy_prev):
        if abs(yoy_latest - yoy_prev) >= 0.5:
            if yoy_latest > yoy_prev:
                score += min(20, (yoy_latest - yoy_prev) * 2)
            else:
                score -= min(20, abs(yoy_latest - yoy_prev) * 2)
    return max(0.0, min(100.0, score))


# ================================================================
# 1m / 3m monthly definitions
# ================================================================
def compute_monthly_scores(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Input: monthly revenue parquet (date=月初, stock_id, revenue)
    Output: (stock_id, date=月末, r_1m_yoy_score, r_3m_yoy_score)

    對每個 stock 計算：
      r_1m_yoy_score: score(this_month_rev vs year-ago same month)
      r_3m_yoy_score: score(last-3m sum vs year-ago same 3m sum)
    """
    df = monthly[['stock_id', 'date', 'revenue']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue'])
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)

    out_rows = []
    for sid, g in df.groupby('stock_id', sort=False):
        g = g.reset_index(drop=True)
        if len(g) < 15:
            continue
        for i in range(len(g)):
            d = g.loc[i, 'date']
            rev = g.loc[i, 'revenue']

            # 1m YoY: this month vs year-ago same month (idx i-12)
            if i >= 12:
                yr_ago = g.loc[i - 12, 'revenue']
                if yr_ago > 0:
                    yoy_1m_latest = (rev / yr_ago - 1) * 100
                else:
                    yoy_1m_latest = np.nan
                if i >= 15:
                    yr_ago_prev = g.loc[i - 15, 'revenue']
                    rev_prev = g.loc[i - 3, 'revenue']
                    if yr_ago_prev > 0:
                        yoy_1m_prev = (rev_prev / yr_ago_prev - 1) * 100
                    else:
                        yoy_1m_prev = np.nan
                else:
                    yoy_1m_prev = np.nan
                s_1m = yoy_to_score(yoy_1m_latest, yoy_1m_prev)
            else:
                s_1m = np.nan

            # 3m rolling: last 3m sum vs year-ago 3m sum
            if i >= 14:  # needs i-14, i-13, i-12 and i-2, i-1, i
                last3 = g.loc[i - 2:i, 'revenue'].sum()
                yr_ago_3 = g.loc[i - 14:i - 12, 'revenue'].sum()
                if yr_ago_3 > 0:
                    yoy_3m_latest = (last3 / yr_ago_3 - 1) * 100
                else:
                    yoy_3m_latest = np.nan
                if i >= 17:
                    prev3 = g.loc[i - 5:i - 3, 'revenue'].sum()
                    yr_ago_prev3 = g.loc[i - 17:i - 15, 'revenue'].sum()
                    if yr_ago_prev3 > 0:
                        yoy_3m_prev = (prev3 / yr_ago_prev3 - 1) * 100
                    else:
                        yoy_3m_prev = np.nan
                else:
                    yoy_3m_prev = np.nan
                s_3m = yoy_to_score(yoy_3m_latest, yoy_3m_prev)
            else:
                s_3m = np.nan

            out_rows.append({
                'stock_id': sid,
                'rev_date': d,
                'r_1m_score': s_1m,
                'r_3m_score': s_3m,
            })
    return pd.DataFrame(out_rows)


# ================================================================
# 4Q TTM quarterly definition
# ================================================================
def compute_quarterly_scores(income: pd.DataFrame) -> pd.DataFrame:
    """
    Input: financials_income.parquet (long format, type='Revenue' rows)
    Output: (stock_id, rev_date=季末, r_4q_ttm_score)

    r_4q_ttm_score: score(last 4Q sum vs prior 4Q sum)
    """
    df = income[income['type'] == 'Revenue'][['stock_id', 'date', 'value']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['revenue'])
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)

    out_rows = []
    for sid, g in df.groupby('stock_id', sort=False):
        g = g.reset_index(drop=True)
        if len(g) < 9:
            continue
        for i in range(len(g)):
            d = g.loc[i, 'date']
            if i >= 7:
                ttm_last = g.loc[i - 3:i, 'revenue'].sum()
                ttm_prev = g.loc[i - 7:i - 4, 'revenue'].sum()
                if ttm_prev > 0:
                    yoy_latest = (ttm_last / ttm_prev - 1) * 100
                else:
                    yoy_latest = np.nan
                # prev-quarter TTM for trend (shift 1 Q back)
                if i >= 8:
                    ttm_last_p = g.loc[i - 4:i - 1, 'revenue'].sum()
                    ttm_prev_p = g.loc[i - 8:i - 5, 'revenue'].sum()
                    if ttm_prev_p > 0:
                        yoy_prev = (ttm_last_p / ttm_prev_p - 1) * 100
                    else:
                        yoy_prev = np.nan
                else:
                    yoy_prev = np.nan
                s = yoy_to_score(yoy_latest, yoy_prev)
                out_rows.append({
                    'stock_id': sid,
                    'rev_date': d,
                    'r_4q_ttm_score': s,
                })
    return pd.DataFrame(out_rows)


# ================================================================
# PIT merge: for each (stock_id, week_end) get latest rev_date <= week_end
# ================================================================
def pit_merge(snap: pd.DataFrame, scored: pd.DataFrame, score_col: str,
              lag_days: int = 0) -> pd.Series:
    """Return a Series aligned with snap, containing PIT value.

    lag_days: 公告延遲（天）。rev_date 代表資料發生的月末/季末，實際可用於
    決策的日期 = rev_date + lag_days。月營收 ≈ T+10，季營收 Q1/Q2/Q3 ≈ T+45，
    Q4 ≈ T+90。
    """
    scored = scored.dropna(subset=[score_col]).sort_values(['stock_id', 'rev_date']).copy()
    # 把 rev_date 推後到可用日期
    scored['available_date'] = scored['rev_date'] + pd.Timedelta(days=lag_days)
    scored = scored.sort_values(['stock_id', 'available_date'])

    result = pd.Series(np.nan, index=snap.index, name=score_col)
    for sid, g_snap in snap.groupby('stock_id'):
        g_score = scored[scored['stock_id'] == sid]
        if g_score.empty:
            continue
        left = g_snap[['week_end_date']].sort_values('week_end_date').reset_index()
        right = g_score[['available_date', score_col]].sort_values('available_date')
        merged = pd.merge_asof(
            left, right,
            left_on='week_end_date', right_on='available_date',
            direction='backward',
        )
        result.loc[merged['index']] = merged[score_col].values
    return result


# ================================================================
# IC analysis
# ================================================================
def ic_analysis(df: pd.DataFrame, factor: str, horizon: int) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    if sub.empty:
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    weekly = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 10:
            continue
        rho, _ = stats.spearmanr(grp[factor], grp[target])
        if not np.isnan(rho):
            weekly.append(rho)
    if not weekly:
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    ic_arr = np.array(weekly)
    return {
        'IC': ic_arr.mean(),
        'std': ic_arr.std(ddof=1) if len(ic_arr) > 1 else np.nan,
        'IR': ic_arr.mean() / ic_arr.std(ddof=1) if len(ic_arr) > 1 and ic_arr.std(ddof=1) > 0 else np.nan,
        'weeks': len(ic_arr),
    }


def decile_spread(df: pd.DataFrame, factor: str, horizon: int) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    weekly_r = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 30:
            continue
        grp_s = grp.sort_values(factor).reset_index(drop=True)
        n = len(grp_s)
        cut = max(1, n // 10)
        bottom = grp_s.iloc[:cut][target].mean()
        top = grp_s.iloc[-cut:][target].mean()
        weekly_r.append({'top': top, 'bottom': bottom, 'spread': top - bottom})
    if not weekly_r:
        return {}
    r = pd.DataFrame(weekly_r)
    return {
        'top': r['top'].mean(),
        'bottom': r['bottom'].mean(),
        'spread': r['spread'].mean(),
        'winrate': (r['spread'] > 0).mean(),
    }


def grade(ir: float) -> str:
    if pd.isna(ir):
        return 'N/A'
    if abs(ir) >= 0.3:
        return 'A' if ir > 0 else 'A (rev)'
    elif abs(ir) >= 0.1:
        return 'B' if ir > 0 else 'B (rev)'
    elif abs(ir) >= 0.05:
        return 'C'
    return 'D'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", default=str(SNAPSHOT_PATH))
    ap.add_argument("--horizon", type=int, default=60)
    args = ap.parse_args()

    print("Loading snapshot + revenue sources...")
    snap = pd.read_parquet(args.snapshot)
    snap['week_end_date'] = pd.to_datetime(snap['week_end_date'])
    print(f"  snapshot: {len(snap)} rows, {snap['week_end_date'].nunique()} weeks, "
          f"{snap['stock_id'].nunique()} stocks")

    monthly = pd.read_parquet(MONTHLY_REV_PATH)
    income = pd.read_parquet(INCOME_PATH)
    print(f"  monthly rev: {len(monthly)} rows")
    print(f"  quarterly income: {len(income)} rows ({(income['type']=='Revenue').sum()} Revenue rows)\n")

    # --- Compute alternative scores per (stock, rev_date) ---
    print("Computing monthly 1m / 3m scores (per stock × month)...")
    mscores = compute_monthly_scores(monthly)
    print(f"  {len(mscores)} (stock, month) rows")

    print("Computing quarterly 4Q TTM scores (per stock × quarter)...")
    qscores = compute_quarterly_scores(income)
    print(f"  {len(qscores)} (stock, quarter) rows\n")

    # --- PIT merge each score into snapshot ---
    # Monthly revenue: T+10 (第 10 號公告). 用 40 天 buffer (月初 date + 40 ~= 下下月月中).
    # Quarterly Revenue (financial_statement): Q1/2/3 T+45, Q4 T+90. 用保守 95 天.
    MONTHLY_LAG = 40
    QUARTERLY_LAG = 95

    print("PIT merging into snapshot...")
    print(f"  monthly lag = {MONTHLY_LAG} days, quarterly lag = {QUARTERLY_LAG} days")
    snap = snap.sort_values(['stock_id', 'week_end_date']).reset_index(drop=True)
    snap['r_1m_score'] = pit_merge(snap, mscores, 'r_1m_score', lag_days=MONTHLY_LAG)
    snap['r_3m_score_recomputed'] = pit_merge(snap, mscores, 'r_3m_score', lag_days=MONTHLY_LAG)
    snap['r_4q_ttm_score'] = pit_merge(snap, qscores, 'r_4q_ttm_score', lag_days=QUARTERLY_LAG)
    print("  done.\n")

    # --- IC / IR / Decile ---
    factors = [
        ('revenue_score (baseline 3m in snapshot)', 'revenue_score'),
        ('r_1m_score', 'r_1m_score'),
        ('r_3m_score_recomputed', 'r_3m_score_recomputed'),
        ('r_4q_ttm_score', 'r_4q_ttm_score'),
    ]

    print("=" * 80)
    print(f"Layer 1: IC / IR (horizon = fwd_{args.horizon}d)")
    print("=" * 80)
    print(f"{'Factor':<40}{'IC':>10}{'IR':>8}{'Weeks':>8}{'Grade':>10}")
    print("-" * 80)
    ic_results = {}
    for label, col in factors:
        r = ic_analysis(snap, col, args.horizon)
        ic_results[col] = r
        print(f"{label:<40}{r['IC']:>10.4f}{r['IR']:>8.3f}{r['weeks']:>8}{grade(r['IR']):>10}")
    print()

    print("=" * 80)
    print(f"Layer 2: Decile spread (top 10% vs bottom 10%, horizon fwd_{args.horizon}d)")
    print("=" * 80)
    print(f"{'Factor':<40}{'TopRet':>10}{'BotRet':>10}{'Spread':>10}{'Winrate':>10}")
    print("-" * 80)
    for label, col in factors:
        r = decile_spread(snap, col, args.horizon)
        if r:
            print(f"{label:<40}{r['top']:>10.2%}{r['bottom']:>10.2%}"
                  f"{r['spread']:>10.2%}{r['winrate']:>10.1%}")
    print()

    # --- Per-year stability (light walk-forward) ---
    print("=" * 80)
    print(f"Layer 3: Per-year IC stability (fwd_{args.horizon}d)")
    print("=" * 80)
    snap['year'] = snap['week_end_date'].dt.year
    years = sorted(snap['year'].unique())
    print(f"{'Factor':<40}" + ''.join(f"{y:>9}" for y in years) + f"{'yrs+':>7}")
    print("-" * (40 + 9 * len(years) + 7))
    stability = {}
    for label, col in factors:
        row_str = f"{label:<40}"
        pos_years = 0
        for yr in years:
            yr_snap = snap[snap['year'] == yr]
            yr_res = ic_analysis(yr_snap, col, args.horizon)
            ic_val = yr_res['IC']
            row_str += f"{ic_val:>+9.4f}" if not pd.isna(ic_val) else f"{'N/A':>9}"
            if not pd.isna(ic_val) and ic_val > 0:
                pos_years += 1
        row_str += f"{pos_years}/{len(years):>3}"
        stability[col] = pos_years / len(years)
        print(row_str)
    print()

    # --- Quarterly walk-forward (24 slices) ---
    print("=" * 80)
    print(f"Layer 4: Quarterly walk-forward IC (24 slices, fwd_{args.horizon}d)")
    print("=" * 80)
    snap['quarter'] = snap['week_end_date'].dt.to_period('Q')
    quarters = sorted(snap['quarter'].unique())
    print(f"  slices: {len(quarters)}, range {quarters[0]} ~ {quarters[-1]}\n")
    print(f"{'Factor':<40}{'mean IC':>10}{'std':>10}{'IR':>10}{'pos Q':>10}{'winrate':>10}")
    print("-" * 90)
    wf_summary = {}
    for label, col in factors:
        quarter_ics = []
        for q in quarters:
            q_snap = snap[snap['quarter'] == q]
            if len(q_snap) < 100:
                continue
            q_res = ic_analysis(q_snap, col, args.horizon)
            if not pd.isna(q_res['IC']):
                quarter_ics.append(q_res['IC'])
        if not quarter_ics:
            continue
        arr = np.array(quarter_ics)
        ir = arr.mean() / arr.std(ddof=1) if len(arr) > 1 and arr.std(ddof=1) > 0 else np.nan
        pos = (arr > 0).sum()
        winrate = pos / len(arr)
        wf_summary[col] = {'IC': arr.mean(), 'IR': ir, 'winrate': winrate, 'n': len(arr)}
        print(f"{label:<40}{arr.mean():>+10.4f}{arr.std():>+10.4f}"
              f"{ir:>+10.3f}{pos}/{len(arr):>10}{winrate:>10.1%}")
    print()

    # --- Summary recommendation ---
    print("=" * 80)
    print("結論判讀")
    print("=" * 80)
    best = max(ic_results.items(), key=lambda kv: kv[1]['IR'] if not pd.isna(kv[1]['IR']) else -999)
    print(f"  最佳 IR: {best[0]} → IR = {best[1]['IR']:.3f} ({grade(best[1]['IR'])})")
    curr_ir = ic_results['revenue_score']['IR']
    print(f"  基線 IR: revenue_score = {curr_ir:.3f} ({grade(curr_ir)})")
    diff = best[1]['IR'] - curr_ir if not pd.isna(best[1]['IR']) and not pd.isna(curr_ir) else np.nan
    print(f"  IR 差距: {diff:+.3f}")
    print(f"  最佳 factor 年度穩定性: {stability[best[0]]:.0%} 年 IC 為正 ({int(stability[best[0]]*len(years))}/{len(years)})")
    if best[0] in wf_summary:
        wf = wf_summary[best[0]]
        print(f"  最佳 factor 季度 walk-forward: {wf['winrate']:.0%} 季 IC 為正 ({int(wf['winrate']*wf['n'])}/{wf['n']}), IR={wf['IR']:.3f}")
    if stability[best[0]] >= 0.67 and not pd.isna(diff) and diff > 0.05:
        print(f"  建議：可 live 改用 {best[0]}（IR 顯著 + 年度穩定性 >= 67%）")
    elif stability[best[0]] >= 0.5 and not pd.isna(diff) and diff > 0.05:
        print(f"  建議：{best[0]} IR 顯著但穩定性 < 67%，可考慮加權或先 shadow run")
    else:
        print(f"  建議：穩定性不足，建議砍 revenue 權重到 0 或再做 walk-forward 滑動驗證")


if __name__ == "__main__":
    main()
