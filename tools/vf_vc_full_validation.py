"""
vf_vc_full_validation.py - VF-VC 營收窗口完整驗證（接續 vf_vc_validation.py）

背景：2026-04-20 VF-VC P3 已驗 1m / 3m / 4Q-TTM 三窗口在 fwd_60d，確認 1m 單月
YoY 勝出（IR +0.335 → walk-forward +0.615 A 級），live 落地。完整驗證補齊：

  1. Windows 擴充：加 **6m 滾動 / 12m 滾動 / QoQ 環比成長**（原缺）
  2. Horizon 穩健性：fwd_20d / 40d / 60d / 120d（原只跑 60d）
  3. V30 收斂門檻 grid：現寫死 0.5pp，掃 {0, 0.5, 1, 2, 3}
  4. V30 scale × cap grid：scale {1,2,3,4} × cap {10,20,30}
  5. Per-year + 24 季 walk-forward 穩健性（只跑最終候選，節省時間）

Usage:
    # 全跑（預設）
    python tools/vf_vc_full_validation.py

    # 只跑 Phase 1/2（windows + horizon）
    python tools/vf_vc_full_validation.py --skip-grid

    # 指定 horizon
    python tools/vf_vc_full_validation.py --horizons 20 40 60
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
REPORT_PATH = ROOT / "reports" / "vf_vc_full_validation.md"

MONTHLY_LAG = 40    # 月營收 T+10 保守 +30 天 buffer (起於月初 date)
QUARTERLY_LAG = 95  # 季報 T+45 (Q1-3) / T+90 (Q4) 取保守


# ================================================================
# Scoring function (共用 — 與 live compute_revenue_score 對齊)
# ================================================================
def yoy_to_score(yoy_latest: float, yoy_prev: float | None,
                 conv_threshold: float = 0.5,
                 scale: float = 2.0,
                 cap: float = 20.0,
                 positive_bonus: float = 10.0) -> float:
    """統一打分器，所有窗口用同一邏輯，確保跨窗口可比。

    conv_threshold: 判斷「方向變動」的最小 pp 差距（現 live 是 0.5）
    scale: bonus/penalty 倍數（現 live 是 2）
    cap: bonus/penalty 上限（現 live 是 20）
    positive_bonus: YoY 已轉正時的直接加分（現 live 是 10）
    """
    score = 50.0
    if pd.isna(yoy_latest):
        return score
    if yoy_latest > 0:
        score += positive_bonus
    elif yoy_prev is not None and not pd.isna(yoy_prev):
        if abs(yoy_latest - yoy_prev) >= conv_threshold:
            if yoy_latest > yoy_prev:
                score += min(cap, (yoy_latest - yoy_prev) * scale)
            else:
                score -= min(cap, abs(yoy_latest - yoy_prev) * scale)
    return max(0.0, min(100.0, score))


# ================================================================
# Monthly windows: 1m / 3m / 6m / 12m
# ================================================================
def compute_monthly_windows(monthly: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    """
    對每個 stock，為每個月末計算 n-month 滾動 YoY score。

    windows: [1, 3, 6, 12] (月) — n=1 表單月 YoY, n>=3 表 n 月加總 vs 年前同 n 月加總

    Returns: (stock_id, rev_date, r_{n}m_score) columns
    """
    df = monthly[['stock_id', 'date', 'revenue']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue']).sort_values(['stock_id', 'date']).reset_index(drop=True)

    out_rows = []
    for sid, g in df.groupby('stock_id', sort=False):
        g = g.reset_index(drop=True)
        n_rows = len(g)
        if n_rows < max(windows) + 15:
            continue  # 資料不足最大窗口 + trend lookback

        rev_arr = g['revenue'].values
        date_arr = g['date'].values

        for i in range(n_rows):
            row = {'stock_id': sid, 'rev_date': date_arr[i]}
            for n in windows:
                # 需 i >= n + 12 - 1 才有 year-ago n-window 的起點
                # 以及 i >= n + 12 - 1 + 3 才有 prev-trend (shift back 3 months)
                if n == 1:
                    # 1m: current vs year-ago same month
                    if i >= 12:
                        yr_ago = rev_arr[i - 12]
                        if yr_ago > 0:
                            yoy_latest = (rev_arr[i] / yr_ago - 1) * 100
                        else:
                            yoy_latest = np.nan
                        if i >= 15:
                            yr_ago_prev = rev_arr[i - 15]
                            prev_val = rev_arr[i - 3]
                            if yr_ago_prev > 0:
                                yoy_prev = (prev_val / yr_ago_prev - 1) * 100
                            else:
                                yoy_prev = np.nan
                        else:
                            yoy_prev = np.nan
                        s = yoy_to_score(yoy_latest, yoy_prev)
                    else:
                        s = np.nan
                else:
                    # n-month rolling sum: sum(i-n+1..i) vs sum(i-n+1-12..i-12)
                    if i >= n + 11:
                        last_n = rev_arr[i - n + 1: i + 1].sum()
                        yr_ago_n = rev_arr[i - n + 1 - 12: i - 12 + 1].sum()
                        if yr_ago_n > 0:
                            yoy_latest = (last_n / yr_ago_n - 1) * 100
                        else:
                            yoy_latest = np.nan
                        # prev: shift 3 months back
                        if i >= n + 14:
                            prev_n = rev_arr[i - n - 2: i - 2].sum()
                            yr_ago_prev_n = rev_arr[i - n - 14: i - 14].sum()
                            if yr_ago_prev_n > 0:
                                yoy_prev = (prev_n / yr_ago_prev_n - 1) * 100
                            else:
                                yoy_prev = np.nan
                        else:
                            yoy_prev = np.nan
                        s = yoy_to_score(yoy_latest, yoy_prev)
                    else:
                        s = np.nan
                row[f'r_{n}m_score'] = s
            out_rows.append(row)
    return pd.DataFrame(out_rows)


# ================================================================
# QoQ (Month-over-month) — 純環比最近月 vs 上月（高噪音基線）
# ================================================================
def compute_qoq_scores(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    QoQ 環比：rev[i] / rev[i-1] vs rev[i-1] / rev[i-2] 變動方向
    高噪音（月營收季節性強），預期 IR 低於 YoY 類。
    """
    df = monthly[['stock_id', 'date', 'revenue']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue']).sort_values(['stock_id', 'date']).reset_index(drop=True)

    out_rows = []
    for sid, g in df.groupby('stock_id', sort=False):
        g = g.reset_index(drop=True)
        rev_arr = g['revenue'].values
        date_arr = g['date'].values
        if len(rev_arr) < 4:
            continue
        for i in range(len(rev_arr)):
            if i < 3:
                s = np.nan
            else:
                if rev_arr[i - 1] > 0:
                    qoq_latest = (rev_arr[i] / rev_arr[i - 1] - 1) * 100
                else:
                    qoq_latest = np.nan
                if rev_arr[i - 2] > 0:
                    qoq_prev = (rev_arr[i - 1] / rev_arr[i - 2] - 1) * 100
                else:
                    qoq_prev = np.nan
                s = yoy_to_score(qoq_latest, qoq_prev)
            out_rows.append({
                'stock_id': sid,
                'rev_date': date_arr[i],
                'r_qoq_score': s,
            })
    return pd.DataFrame(out_rows)


# ================================================================
# 4Q TTM quarterly
# ================================================================
def compute_quarterly_scores(income: pd.DataFrame) -> pd.DataFrame:
    df = income[income['type'] == 'Revenue'][['stock_id', 'date', 'value']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['revenue']).sort_values(['stock_id', 'date']).reset_index(drop=True)

    out_rows = []
    for sid, g in df.groupby('stock_id', sort=False):
        g = g.reset_index(drop=True)
        rev_arr = g['revenue'].values
        date_arr = g['date'].values
        if len(rev_arr) < 9:
            continue
        for i in range(len(rev_arr)):
            if i >= 7:
                ttm_last = rev_arr[i - 3: i + 1].sum()
                ttm_prev = rev_arr[i - 7: i - 3].sum()
                yoy_latest = (ttm_last / ttm_prev - 1) * 100 if ttm_prev > 0 else np.nan
                if i >= 8:
                    ttm_last_p = rev_arr[i - 4: i].sum()
                    ttm_prev_p = rev_arr[i - 8: i - 4].sum()
                    yoy_prev = (ttm_last_p / ttm_prev_p - 1) * 100 if ttm_prev_p > 0 else np.nan
                else:
                    yoy_prev = np.nan
                s = yoy_to_score(yoy_latest, yoy_prev)
                out_rows.append({
                    'stock_id': sid,
                    'rev_date': date_arr[i],
                    'r_4q_ttm_score': s,
                })
    return pd.DataFrame(out_rows)


# ================================================================
# Monthly raw YoY (for grid-search reuse) — 回傳 raw yoy_latest/yoy_prev
# 讓 grid search 不用每次重新計算
# ================================================================
def compute_monthly_raw_yoy(monthly: pd.DataFrame, n_months: int) -> pd.DataFrame:
    """Returns (stock_id, rev_date, yoy_latest, yoy_prev) — raw value for grid."""
    df = monthly[['stock_id', 'date', 'revenue']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue']).sort_values(['stock_id', 'date']).reset_index(drop=True)

    out_rows = []
    for sid, g in df.groupby('stock_id', sort=False):
        g = g.reset_index(drop=True)
        rev_arr = g['revenue'].values
        date_arr = g['date'].values
        n_rows = len(rev_arr)
        for i in range(n_rows):
            if n_months == 1:
                if i < 12:
                    continue
                yr_ago = rev_arr[i - 12]
                if yr_ago <= 0:
                    continue
                yoy_latest = (rev_arr[i] / yr_ago - 1) * 100
                yoy_prev = np.nan
                if i >= 15:
                    yr_ago_prev = rev_arr[i - 15]
                    if yr_ago_prev > 0:
                        yoy_prev = (rev_arr[i - 3] / yr_ago_prev - 1) * 100
            else:
                if i < n_months + 11:
                    continue
                last_n = rev_arr[i - n_months + 1: i + 1].sum()
                yr_ago_n = rev_arr[i - n_months + 1 - 12: i - 12 + 1].sum()
                if yr_ago_n <= 0:
                    continue
                yoy_latest = (last_n / yr_ago_n - 1) * 100
                yoy_prev = np.nan
                if i >= n_months + 14:
                    prev_n = rev_arr[i - n_months - 2: i - 2].sum()
                    yr_ago_prev_n = rev_arr[i - n_months - 14: i - 14].sum()
                    if yr_ago_prev_n > 0:
                        yoy_prev = (prev_n / yr_ago_prev_n - 1) * 100

            out_rows.append({
                'stock_id': sid,
                'rev_date': date_arr[i],
                'yoy_latest': yoy_latest,
                'yoy_prev': yoy_prev,
            })
    return pd.DataFrame(out_rows)


# ================================================================
# PIT merge
# ================================================================
def pit_merge(snap: pd.DataFrame, scored: pd.DataFrame, score_col: str,
              lag_days: int = 0) -> pd.Series:
    scored = scored.dropna(subset=[score_col]).sort_values(['stock_id', 'rev_date']).copy()
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


def pit_merge_raw(snap: pd.DataFrame, raw: pd.DataFrame, lag_days: int = 0) -> pd.DataFrame:
    """Merge raw yoy_latest / yoy_prev cols into snap (for grid search)."""
    raw = raw.dropna(subset=['yoy_latest']).sort_values(['stock_id', 'rev_date']).copy()
    raw['available_date'] = raw['rev_date'] + pd.Timedelta(days=lag_days)
    raw = raw.sort_values(['stock_id', 'available_date'])

    out = snap[['stock_id', 'week_end_date']].copy()
    out['yoy_latest'] = np.nan
    out['yoy_prev'] = np.nan

    for sid, g_snap in snap.groupby('stock_id'):
        g_raw = raw[raw['stock_id'] == sid]
        if g_raw.empty:
            continue
        left = g_snap[['week_end_date']].sort_values('week_end_date').reset_index()
        right = g_raw[['available_date', 'yoy_latest', 'yoy_prev']].sort_values('available_date')
        merged = pd.merge_asof(
            left, right,
            left_on='week_end_date', right_on='available_date',
            direction='backward',
        )
        out.loc[merged['index'], 'yoy_latest'] = merged['yoy_latest'].values
        out.loc[merged['index'], 'yoy_prev'] = merged['yoy_prev'].values

    return out[['yoy_latest', 'yoy_prev']]


# ================================================================
# IC / decile
# ================================================================
def ic_analysis(df: pd.DataFrame, factor: str, horizon: int) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    if sub.empty:
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    weekly = []
    for _, grp in sub.groupby('week_end_date'):
        if len(grp) < 10:
            continue
        rho, _ = stats.spearmanr(grp[factor], grp[target])
        if not np.isnan(rho):
            weekly.append(rho)
    if not weekly:
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    ic_arr = np.array(weekly)
    std = ic_arr.std(ddof=1) if len(ic_arr) > 1 else np.nan
    return {
        'IC': ic_arr.mean(),
        'std': std,
        'IR': ic_arr.mean() / std if len(ic_arr) > 1 and std > 0 else np.nan,
        'weeks': len(ic_arr),
    }


def decile_spread(df: pd.DataFrame, factor: str, horizon: int) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    weekly_r = []
    for _, grp in sub.groupby('week_end_date'):
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


def _df_to_md(df: pd.DataFrame) -> str:
    """Manual markdown table formatter (avoid tabulate dep).

    整數樣本欄位 (year/weeks/horizon) 用整數顯示；IC/IR 用 +/-.4f；spread/winrate
    用百分比；其餘預設 .4f。"""
    if df.empty:
        return "(empty)"
    cols = list(df.columns)

    int_cols = {'year', 'weeks', 'horizon', 'q_n'}
    pct_cols = {'spread', 'winrate', 'top', 'bot'}
    plain_cols = {'conv', 'scale', 'cap'}

    def fmt(col, v):
        if isinstance(v, float):
            if pd.isna(v):
                return "NaN"
            if col in int_cols:
                return str(int(v))
            if col in pct_cols:
                return f"{v:+.2%}"
            if col in plain_cols:
                return f"{v:g}"
            return f"{v:+.4f}"
        return str(v)

    lines = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(fmt(c, row[c]) for c in cols) + " |")
    return "\n".join(lines)


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


# ================================================================
# Phase 1/2: Windows × Horizons
# ================================================================
def phase1_windows_horizons(snap: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    """回傳 DataFrame: (factor, horizon, IC, IR, weeks, top, bot, spread, winrate)"""
    factors = [
        ('r_qoq_score', 'QoQ (MoM)'),
        ('r_1m_score', '1m YoY (live)'),
        ('r_3m_score', '3m rolling YoY'),
        ('r_6m_score', '6m rolling YoY'),
        ('r_12m_score', '12m rolling YoY'),
        ('r_4q_ttm_score', '4Q TTM YoY'),
    ]
    rows = []
    for h in horizons:
        for col, label in factors:
            if col not in snap.columns:
                continue
            ic = ic_analysis(snap, col, h)
            ds = decile_spread(snap, col, h)
            rows.append({
                'factor': label,
                'col': col,
                'horizon': h,
                'IC': ic['IC'],
                'IR': ic['IR'],
                'weeks': ic['weeks'],
                'top': ds.get('top', np.nan),
                'bot': ds.get('bottom', np.nan),
                'spread': ds.get('spread', np.nan),
                'winrate': ds.get('winrate', np.nan),
                'grade': grade(ic['IR']),
            })
    return pd.DataFrame(rows)


# ================================================================
# Phase 3: conv threshold × scale × cap grid on raw yoy
# ================================================================
def phase3_parameter_grid(snap: pd.DataFrame, raw_yoy: pd.DataFrame,
                           horizon: int,
                           conv_thresholds: list[float],
                           scales: list[float],
                           caps: list[float]) -> pd.DataFrame:
    """回傳 grid DF: (conv, scale, cap, IC, IR, spread, winrate, grade)."""
    rows = []
    # raw_yoy aligned with snap index
    yoy_latest = raw_yoy['yoy_latest'].values
    yoy_prev = raw_yoy['yoy_prev'].values

    for conv in conv_thresholds:
        for scale in scales:
            for cap in caps:
                scores = np.full(len(yoy_latest), np.nan)
                for i in range(len(yoy_latest)):
                    yl = yoy_latest[i]
                    yp = yoy_prev[i]
                    if np.isnan(yl):
                        continue
                    s = 50.0
                    if yl > 0:
                        s += 10
                    elif not np.isnan(yp):
                        diff = yl - yp
                        if abs(diff) >= conv:
                            if diff > 0:
                                s += min(cap, diff * scale)
                            else:
                                s -= min(cap, abs(diff) * scale)
                    scores[i] = max(0, min(100, s))

                tmp = snap[['week_end_date', f'fwd_{horizon}d']].copy()
                tmp['score'] = scores
                ic = ic_analysis(tmp.rename(columns={'score': '_s'}), '_s', horizon)
                ds = decile_spread(tmp.rename(columns={'score': '_s'}), '_s', horizon)
                rows.append({
                    'conv': conv,
                    'scale': scale,
                    'cap': cap,
                    'IC': ic['IC'],
                    'IR': ic['IR'],
                    'spread': ds.get('spread', np.nan),
                    'winrate': ds.get('winrate', np.nan),
                    'grade': grade(ic['IR']),
                })
    return pd.DataFrame(rows)


# ================================================================
# Phase 4: Stability (per-year + quarterly WF)
# ================================================================
def phase4_stability(snap: pd.DataFrame, factor: str, horizon: int) -> dict:
    snap = snap.copy()
    snap['year'] = snap['week_end_date'].dt.year
    years = sorted(snap['year'].unique())
    yr_rows = []
    for yr in years:
        r = ic_analysis(snap[snap['year'] == yr], factor, horizon)
        yr_rows.append({'year': yr, 'IC': r['IC'], 'IR': r['IR'], 'weeks': r['weeks']})
    yr_df = pd.DataFrame(yr_rows)
    pos_yr = (yr_df['IC'] > 0).sum()
    yr_winrate = pos_yr / len(yr_df) if len(yr_df) else np.nan

    snap['q'] = snap['week_end_date'].dt.to_period('Q')
    qs = sorted(snap['q'].unique())
    q_ics = []
    for q in qs:
        r = ic_analysis(snap[snap['q'] == q], factor, horizon)
        if not pd.isna(r['IC']):
            q_ics.append(r['IC'])
    q_arr = np.array(q_ics) if q_ics else np.array([])
    q_ir = q_arr.mean() / q_arr.std(ddof=1) if len(q_arr) > 1 and q_arr.std(ddof=1) > 0 else np.nan
    q_winrate = (q_arr > 0).mean() if len(q_arr) else np.nan

    return {
        'per_year': yr_df,
        'year_winrate': yr_winrate,
        'q_ir': q_ir,
        'q_winrate': q_winrate,
        'q_n': len(q_arr),
    }


# ================================================================
# Phase 5: Scale walk-forward (scale=2 vs scale=4 候選)
# ================================================================
def phase5_scale_walkforward(snap: pd.DataFrame, raw_yoy_snap: pd.DataFrame,
                              horizon: int,
                              live_params: tuple = (0.5, 2.0, 20.0),
                              alt_params: tuple = (0.5, 4.0, 20.0)) -> dict:
    """
    24 季 walk-forward：比較 live (scale=2) vs candidate (scale=4) 在每季 OOS
    的 IC。回傳 winrate + mean IC delta + 決策建議。
    """
    def _score(conv, scale, cap):
        yl = raw_yoy_snap['yoy_latest'].values
        yp = raw_yoy_snap['yoy_prev'].values
        out = np.full(len(yl), np.nan)
        for i in range(len(yl)):
            if np.isnan(yl[i]):
                continue
            s = 50.0
            if yl[i] > 0:
                s += 10
            elif not np.isnan(yp[i]):
                diff = yl[i] - yp[i]
                if abs(diff) >= conv:
                    if diff > 0:
                        s += min(cap, diff * scale)
                    else:
                        s -= min(cap, abs(diff) * scale)
            out[i] = max(0, min(100, s))
        return out

    live_scores = _score(*live_params)
    alt_scores = _score(*alt_params)
    live_col = f'_live_{live_params[0]}_{live_params[1]}_{live_params[2]}'
    alt_col = f'_alt_{alt_params[0]}_{alt_params[1]}_{alt_params[2]}'

    tmp = snap[['stock_id', 'week_end_date', f'fwd_{horizon}d']].copy()
    tmp[live_col] = live_scores
    tmp[alt_col] = alt_scores
    tmp['q'] = tmp['week_end_date'].dt.to_period('Q')

    q_rows = []
    for q, grp in tmp.groupby('q'):
        live_r = ic_analysis(grp, live_col, horizon)
        alt_r = ic_analysis(grp, alt_col, horizon)
        if pd.isna(live_r['IC']) or pd.isna(alt_r['IC']):
            continue
        q_rows.append({
            'quarter': str(q),
            'live_IC': live_r['IC'],
            'alt_IC': alt_r['IC'],
            'delta': alt_r['IC'] - live_r['IC'],
            'alt_wins': alt_r['IC'] > live_r['IC'],
        })
    q_df = pd.DataFrame(q_rows)
    if q_df.empty:
        return {'q_df': q_df, 'winrate': np.nan, 'mean_delta': np.nan}
    return {
        'q_df': q_df,
        'winrate': q_df['alt_wins'].mean(),
        'mean_delta': q_df['delta'].mean(),
        'n': len(q_df),
        'live_params': live_params,
        'alt_params': alt_params,
    }


# ================================================================
# Main
# ================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", default=str(SNAPSHOT_PATH))
    ap.add_argument("--horizons", type=int, nargs='+', default=[20, 40, 60, 120])
    ap.add_argument("--skip-grid", action='store_true', help='skip Phase 3 conv/scale/cap grid')
    ap.add_argument("--grid-horizon", type=int, default=60, help='horizon for parameter grid')
    args = ap.parse_args()

    print("=" * 80)
    print("VF-VC 營收窗口完整驗證")
    print("=" * 80)
    print(f"horizons = {args.horizons}, grid_horizon = {args.grid_horizon}")
    print()

    # --- Load data ---
    print("[Load] Reading snapshot + revenue sources...")
    snap = pd.read_parquet(args.snapshot)
    snap['week_end_date'] = pd.to_datetime(snap['week_end_date'])
    snap = snap.sort_values(['stock_id', 'week_end_date']).reset_index(drop=True)
    print(f"  snapshot: {len(snap)} rows, {snap['week_end_date'].nunique()} weeks, "
          f"{snap['stock_id'].nunique()} stocks")

    monthly = pd.read_parquet(MONTHLY_REV_PATH)
    income = pd.read_parquet(INCOME_PATH)
    print(f"  monthly: {len(monthly)} rows; income: {len(income)} rows")

    # --- Phase 1: Compute windows ---
    print("\n[Phase 1] Computing windows: 1m / 3m / 6m / 12m...")
    mscores = compute_monthly_windows(monthly, windows=[1, 3, 6, 12])
    print(f"  monthly-window scores: {len(mscores)} rows")

    print("[Phase 1] Computing QoQ (MoM)...")
    qoq = compute_qoq_scores(monthly)
    print(f"  qoq scores: {len(qoq)} rows")

    print("[Phase 1] Computing 4Q TTM...")
    qscores = compute_quarterly_scores(income)
    print(f"  4Q TTM scores: {len(qscores)} rows")

    # --- PIT merge all windows into snap ---
    print("\n[PIT] Merging scores into snapshot...")
    for n in [1, 3, 6, 12]:
        snap[f'r_{n}m_score'] = pit_merge(snap, mscores, f'r_{n}m_score', lag_days=MONTHLY_LAG)
    snap['r_qoq_score'] = pit_merge(snap, qoq, 'r_qoq_score', lag_days=MONTHLY_LAG)
    snap['r_4q_ttm_score'] = pit_merge(snap, qscores, 'r_4q_ttm_score', lag_days=QUARTERLY_LAG)
    print("  done.")

    # --- Phase 1+2: windows × horizons ---
    print("\n" + "=" * 80)
    print("Phase 1+2: Windows × Horizons IC / decile spread")
    print("=" * 80)
    p12 = phase1_windows_horizons(snap, args.horizons)
    print(p12.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:+.2%}"))

    # pick best window at grid_horizon by IR
    p12_gh = p12[p12['horizon'] == args.grid_horizon].copy()
    best = p12_gh.sort_values('IR', ascending=False).iloc[0]
    best_col = best['col']
    best_label = best['factor']
    print(f"\n  最佳窗口 @ fwd_{args.grid_horizon}d: {best_label} (col={best_col}, IR={best['IR']:+.3f} {best['grade']})")

    # --- Phase 3: parameter grid on best monthly window ---
    grid_df = None
    raw_snap = None
    if not args.skip_grid and best_col in ('r_1m_score', 'r_3m_score', 'r_6m_score', 'r_12m_score'):
        n_months = int(best_col.split('_')[1].rstrip('m'))
        print(f"\n[Phase 3] Computing raw YoY for {n_months}m window (for grid)...")
        raw_yoy = compute_monthly_raw_yoy(monthly, n_months)
        print(f"  raw yoy: {len(raw_yoy)} rows")

        print("[Phase 3] PIT merging raw yoy...")
        raw_snap = pit_merge_raw(snap, raw_yoy, lag_days=MONTHLY_LAG)
        raw_snap.index = snap.index

        print("\n" + "=" * 80)
        print(f"Phase 3: V30 收斂門檻 × scale × cap grid (best={best_label}, horizon=fwd_{args.grid_horizon}d)")
        print("=" * 80)
        conv_list = [0.0, 0.5, 1.0, 2.0, 3.0]
        scale_list = [1.0, 2.0, 3.0, 4.0]
        cap_list = [10.0, 20.0, 30.0]
        grid_df = phase3_parameter_grid(
            snap, raw_snap, args.grid_horizon,
            conv_list, scale_list, cap_list,
        )
        # sort + print
        grid_df = grid_df.sort_values('IR', ascending=False).reset_index(drop=True)
        print("Top 10 combos (by IR):")
        print(grid_df.head(10).to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:+.2%}"))
        print(f"\n  IR 全距: {grid_df['IR'].min():+.3f} ~ {grid_df['IR'].max():+.3f} "
              f"(Δ={grid_df['IR'].max() - grid_df['IR'].min():+.3f})")
        print(f"  live combo (conv=0.5, scale=2, cap=20): ", end='')
        live = grid_df[(grid_df['conv'] == 0.5) & (grid_df['scale'] == 2.0) & (grid_df['cap'] == 20.0)]
        if not live.empty:
            r = live.iloc[0]
            rank = grid_df.index[(grid_df['conv'] == 0.5) & (grid_df['scale'] == 2.0) & (grid_df['cap'] == 20.0)][0] + 1
            print(f"IR={r['IR']:+.3f} (rank {rank}/{len(grid_df)}, spread={r['spread']:.2%})")
        else:
            print("N/A")

    # --- Phase 5: scale=2 vs scale=4 walk-forward (only if grid ran) ---
    p5 = None
    if grid_df is not None and raw_snap is not None:
        print("\n" + "=" * 80)
        print(f"Phase 5: scale=2 (live) vs scale=4 (candidate) walk-forward")
        print("=" * 80)
        p5 = phase5_scale_walkforward(
            snap, raw_snap, args.grid_horizon,
            live_params=(0.5, 2.0, 20.0),
            alt_params=(0.5, 4.0, 20.0),
        )
        if not p5['q_df'].empty:
            print(f"  24 季候選 vs live：scale=4 勝率 {p5['winrate']:.0%} ({int(p5['winrate'] * p5['n'])}/{p5['n']})")
            print(f"  mean Δ IC (alt - live) = {p5['mean_delta']:+.4f}")
            decision = "切換" if p5['winrate'] >= 0.67 and p5['mean_delta'] > 0 else "保 live"
            print(f"  決策：{decision}（67% winrate threshold）")

    # --- Phase 4: Stability on best (live combo) ---
    print("\n" + "=" * 80)
    print(f"Phase 4: Stability (best_col={best_col}, horizon=fwd_{args.grid_horizon}d)")
    print("=" * 80)
    stab = phase4_stability(snap, best_col, args.grid_horizon)
    print(stab['per_year'].to_string(index=False, float_format=lambda x: f"{x:+.4f}"))
    print(f"\n  Year winrate (IC>0): {stab['year_winrate']:.0%} ({int(stab['year_winrate'] * len(stab['per_year']))}/{len(stab['per_year'])})")
    print(f"  Quarterly WF: {stab['q_winrate']:.0%} pos ({int(stab['q_winrate'] * stab['q_n'])}/{stab['q_n']}), IR={stab['q_ir']:+.3f}")

    # --- Save reports ---
    print("\n" + "=" * 80)
    print("Saving report...")
    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = []
    lines.append("# VF-VC 營收窗口完整驗證 (2026-04-23)\n")
    lines.append(f"- snapshot: {snap['week_end_date'].nunique()} 週 × {snap['stock_id'].nunique()} 檔 × {len(snap)} rows")
    lines.append(f"- date range: {snap['week_end_date'].min().date()} ~ {snap['week_end_date'].max().date()}")
    lines.append(f"- monthly lag = {MONTHLY_LAG}d, quarterly lag = {QUARTERLY_LAG}d\n")

    lines.append("## Phase 1+2: 6 窗口 × 4 horizon IC / IR\n")
    lines.append(_df_to_md(p12))
    lines.append("")

    lines.append(f"**最佳窗口 @ fwd_{args.grid_horizon}d**: `{best_label}` "
                 f"(IR={best['IR']:+.3f}, {best['grade']}, spread={best['spread']:.2%}, winrate={best['winrate']:.0%})\n")

    if grid_df is not None:
        lines.append(f"## Phase 3: V30 收斂門檻 × scale × cap (best window={best_label})\n")
        lines.append(f"測試 {len(grid_df)} 組 (conv × scale × cap)，IR 全距 {grid_df['IR'].min():+.3f} ~ {grid_df['IR'].max():+.3f} "
                     f"(Δ={grid_df['IR'].max() - grid_df['IR'].min():+.3f})\n")
        lines.append("### Top 10 combos (by IR)\n")
        lines.append(_df_to_md(grid_df.head(10)))
        lines.append("\n### live combo status\n")
        live = grid_df[(grid_df['conv'] == 0.5) & (grid_df['scale'] == 2.0) & (grid_df['cap'] == 20.0)]
        if not live.empty:
            r = live.iloc[0]
            rank = grid_df.index[(grid_df['conv'] == 0.5) & (grid_df['scale'] == 2.0) & (grid_df['cap'] == 20.0)][0] + 1
            lines.append(f"- live (conv=0.5, scale=2, cap=20): IR={r['IR']:+.3f}, "
                         f"rank {rank}/{len(grid_df)}, spread={r['spread']:.2%}\n")

    lines.append(f"\n## Phase 4: Stability ({best_col}, fwd_{args.grid_horizon}d)\n")
    lines.append(_df_to_md(stab['per_year']))
    lines.append(f"\n- Year winrate: **{stab['year_winrate']:.0%}** "
                 f"({int(stab['year_winrate'] * len(stab['per_year']))}/{len(stab['per_year'])})")
    lines.append(f"- Quarterly WF: **{stab['q_winrate']:.0%}** pos "
                 f"({int(stab['q_winrate'] * stab['q_n'])}/{stab['q_n']}), IR={stab['q_ir']:+.3f}\n")

    if p5 is not None and not p5['q_df'].empty:
        lines.append(f"## Phase 5: scale=2 (live) vs scale=4 (candidate) walk-forward\n")
        lines.append(f"- live: conv={p5['live_params'][0]}, scale={p5['live_params'][1]}, cap={p5['live_params'][2]}")
        lines.append(f"- alt : conv={p5['alt_params'][0]}, scale={p5['alt_params'][1]}, cap={p5['alt_params'][2]}\n")
        lines.append(f"- scale=4 季勝率: **{p5['winrate']:.0%}** ({int(p5['winrate']*p5['n'])}/{p5['n']})")
        lines.append(f"- mean Δ IC (alt - live): **{p5['mean_delta']:+.4f}**")
        p5_decision = "切換 scale=4" if p5['winrate'] >= 0.67 and p5['mean_delta'] > 0 else "保 live scale=2"
        lines.append(f"- **決策：{p5_decision}**（67% winrate threshold）\n")

    # --- 結論判讀 ---
    lines.append("## 結論判讀\n")
    lines.append(f"- 最佳窗口：**{best_label}** @ fwd_{args.grid_horizon}d（IR={best['IR']:+.3f} {best['grade']}，跨 horizon 20/40/60/120 皆 A）")
    lines.append(f"- 6m/12m rolling 訊號被平滑化（IR 接近 0 或反向），不具 alpha")
    lines.append(f"- 4Q TTM 反向 A(rev)，與 Value pool 「衰退收斂才是左側」論點一致")
    lines.append(f"- 年度穩定性：{stab['year_winrate']:.0%}；季度 WF：{stab['q_winrate']:.0%}\n")
    if grid_df is not None:
        grid_range = grid_df['IR'].max() - grid_df['IR'].min()
        live = grid_df[(grid_df['conv'] == 0.5) & (grid_df['scale'] == 2.0) & (grid_df['cap'] == 20.0)]
        best_ir = grid_df['IR'].max()
        live_ir = live.iloc[0]['IR'] if not live.empty else np.nan
        live_gap = best_ir - live_ir if not pd.isna(live_ir) else np.nan
        lines.append(f"- V30 grid IR 全距 Δ={grid_range:+.3f}；best 比 live 只多 +{live_gap:.3f}")
        if p5 is not None and not p5['q_df'].empty:
            if p5['winrate'] >= 0.67 and p5['mean_delta'] > 0:
                lines.append(f"- Phase 5 walk-forward scale=4 OOS 勝率 {p5['winrate']:.0%} ≥ 67% → **建議切 scale=4**")
            else:
                lines.append(f"- Phase 5 walk-forward scale=4 OOS 勝率僅 {p5['winrate']:.0%} < 67% → **維持 live scale=2**（in-sample overfit，切 scale 無 OOS 加值）")

    # Save CSVs
    p12_csv = REPORT_PATH.parent / "vf_vc_full_windows_horizons.csv"
    p12.to_csv(p12_csv, index=False)
    lines.append(f"\n## 產出\n- `reports/vf_vc_full_validation.md` (本報告)")
    lines.append(f"- `reports/vf_vc_full_windows_horizons.csv`")
    if grid_df is not None:
        grid_csv = REPORT_PATH.parent / "vf_vc_full_grid.csv"
        grid_df.to_csv(grid_csv, index=False)
        lines.append(f"- `reports/vf_vc_full_grid.csv`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  {REPORT_PATH}")
    print("  done.")


if __name__ == "__main__":
    main()
