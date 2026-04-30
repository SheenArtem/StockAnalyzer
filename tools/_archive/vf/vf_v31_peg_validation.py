"""
vf_v31_peg_validation.py - V31 Value PEG 窗口驗證

Live 實作 (value_screener.py:824-839)：
  - PEG = PE / avg(last 6 months of revenue YoY)
  - 加分: PEG<0.5 +12 / PEG<1.0 +8 / PEG>3.0 -5

驗證四個窗口 YoY 平均：3m / 6m (live) / 12m / 24m，比較 PEG 跨截面 IC +
decile spread vs fwd_X，看 6m 是否最優。

注意：V31 不是 V29 VF-VC 的同一回事：
  - VF-VC: revenue_score 本身（1m 單月 YoY 分數化）
  - V31: PEG 的 growth 分母（PE / avg growth window）
  兩者資料層用同一個 monthly YoY，但聚合方式不同。

Usage:
    python tools/vf_v31_peg_validation.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

BT_DIR = ROOT / "data_cache" / "backtest"
SNAPSHOT_PATH = BT_DIR / "trade_journal_value_tw_snapshot.parquet"
MONTHLY_REV_PATH = BT_DIR / "financials_revenue.parquet"
REPORT_PATH = ROOT / "reports" / "vf_v31_peg_validation.md"


def grade(ir):
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
    int_cols = {'n_obs', 'window_m', 'horizon', 'n_months', 'n_weeks', 'weeks'}
    pct_cols = {'mean_ret', 'winrate', 'top', 'bot', 'spread', 'top_ret', 'bot_ret',
                'median_ret'}

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

    lines = ["| " + " | ".join(cols) + " |",
             "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(fmt(c, row[c]) for c in cols) + " |")
    return "\n".join(lines)


def compute_monthly_yoy(monthly: pd.DataFrame) -> pd.DataFrame:
    """Returns (stock_id, rev_date, yoy_pct) per month."""
    df = monthly[['stock_id', 'date', 'revenue']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue']).sort_values(['stock_id', 'date']).reset_index(drop=True)

    out = []
    for sid, g in df.groupby('stock_id', sort=False):
        rev = g['revenue'].values
        dates = g['date'].values
        for i in range(12, len(rev)):
            if rev[i - 12] > 0:
                yoy = (rev[i] / rev[i - 12] - 1) * 100
                out.append({'stock_id': sid, 'rev_date': dates[i], 'yoy_pct': yoy})
    return pd.DataFrame(out)


def compute_avg_yoy_windows(yoy_df: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    """For each (stock_id, rev_date), compute rolling mean over last N months YoY."""
    yoy_df = yoy_df.sort_values(['stock_id', 'rev_date']).reset_index(drop=True)
    out_rows = []
    for sid, g in yoy_df.groupby('stock_id', sort=False):
        g = g.reset_index(drop=True)
        if len(g) < max(windows):
            continue
        for i in range(len(g)):
            row = {'stock_id': sid, 'rev_date': g.loc[i, 'rev_date']}
            for w in windows:
                if i + 1 >= w:
                    row[f'avg_yoy_{w}m'] = g.loc[i - w + 1: i, 'yoy_pct'].mean()
                else:
                    row[f'avg_yoy_{w}m'] = np.nan
            out_rows.append(row)
    return pd.DataFrame(out_rows)


def pit_merge(snap: pd.DataFrame, scored: pd.DataFrame, col: str,
              lag_days: int = 40) -> pd.Series:
    scored = scored.dropna(subset=[col]).sort_values(['stock_id', 'rev_date']).copy()
    scored['available_date'] = scored['rev_date'] + pd.Timedelta(days=lag_days)
    scored = scored.sort_values(['stock_id', 'available_date'])
    result = pd.Series(np.nan, index=snap.index, name=col)
    for sid, g_snap in snap.groupby('stock_id'):
        g_score = scored[scored['stock_id'] == sid]
        if g_score.empty:
            continue
        left = g_snap[['week_end_date']].sort_values('week_end_date').reset_index()
        right = g_score[['available_date', col]].sort_values('available_date')
        merged = pd.merge_asof(left, right,
                               left_on='week_end_date', right_on='available_date',
                               direction='backward')
        result.loc[merged['index']] = merged[col].values
    return result


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
        if not pd.isna(rho):
            weekly.append(rho)
    if not weekly:
        return {'IC': np.nan, 'IR': np.nan, 'weeks': 0}
    arr = np.array(weekly)
    std = arr.std(ddof=1) if len(arr) > 1 else np.nan
    return {
        'IC': arr.mean(),
        'IR': arr.mean() / std if len(arr) > 1 and std > 0 else np.nan,
        'weeks': len(arr),
    }


def decile_spread(df: pd.DataFrame, factor: str, horizon: int) -> dict:
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    weekly = []
    for _, grp in sub.groupby('week_end_date'):
        if len(grp) < 30:
            continue
        gs = grp.sort_values(factor).reset_index(drop=True)
        n = len(gs)
        cut = max(1, n // 10)
        weekly.append({
            'top': gs.iloc[-cut:][target].mean(),
            'bot': gs.iloc[:cut][target].mean(),
        })
    if not weekly:
        return {}
    r = pd.DataFrame(weekly)
    return {
        'top_ret': r['top'].mean(),
        'bot_ret': r['bot'].mean(),
        'spread': (r['top'] - r['bot']).mean(),
        'winrate': ((r['top'] - r['bot']) > 0).mean(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", default=str(SNAPSHOT_PATH))
    ap.add_argument("--horizons", type=int, nargs='+', default=[20, 40, 60, 120])
    args = ap.parse_args()

    print("=" * 80)
    print("V31: Value PEG 窗口驗證")
    print("=" * 80)

    print("Loading snapshot + monthly revenue...")
    snap = pd.read_parquet(args.snapshot)
    snap['week_end_date'] = pd.to_datetime(snap['week_end_date'])
    snap = snap.sort_values(['stock_id', 'week_end_date']).reset_index(drop=True)
    print(f"  snapshot: {len(snap)} rows")
    print(f"  PE available: {snap['pe'].notna().sum()} / {len(snap)}")

    monthly = pd.read_parquet(MONTHLY_REV_PATH)
    print(f"  monthly rev: {len(monthly)} rows")

    print("\nComputing monthly YoY...")
    yoy_df = compute_monthly_yoy(monthly)
    print(f"  yoy rows: {len(yoy_df)}")

    windows = [3, 6, 12, 24]
    print(f"\nComputing rolling YoY avg over {windows} months...")
    avg_df = compute_avg_yoy_windows(yoy_df, windows)
    print(f"  avg rows: {len(avg_df)}")

    print("\nPIT merge YoY averages into snapshot (lag=40d)...")
    for w in windows:
        col = f'avg_yoy_{w}m'
        snap[col] = pit_merge(snap, avg_df, col, lag_days=40)
        n_filled = snap[col].notna().sum()
        print(f"  {col}: {n_filled} filled")

    # Compute PEG for each window. Guard: pe>0, growth>1 (避免 divide by small/neg)
    # Also cap PE outliers at 150 (15577 max observed)
    snap['pe_capped'] = np.where((snap['pe'] > 0) & (snap['pe'] < 150), snap['pe'], np.nan)
    for w in windows:
        growth_col = f'avg_yoy_{w}m'
        peg_col = f'peg_{w}m'
        snap[peg_col] = np.where(
            (snap['pe_capped'] > 0) & (snap[growth_col] > 1),
            snap['pe_capped'] / snap[growth_col],
            np.nan,
        )
        # cap peg at 50 (extremes)
        snap.loc[snap[peg_col] > 50, peg_col] = np.nan

    print(f"\n  PEG obs per window (non-NaN):")
    for w in windows:
        print(f"    peg_{w}m: {snap[f'peg_{w}m'].notna().sum()}")

    # --- IC analysis ---
    rows = []
    print("\n" + "=" * 80)
    print("IC / decile spread across windows × horizons")
    print("=" * 80)
    # 低 PEG → 看好 → 預期 IC < 0 (PEG↓ return↑)
    for w in windows:
        peg_col = f'peg_{w}m'
        for h in args.horizons:
            ic = ic_analysis(snap, peg_col, h)
            ds = decile_spread(snap, peg_col, h)
            rows.append({
                'window_m': w,
                'horizon': h,
                'IC': ic['IC'],
                'IR': ic['IR'],
                'weeks': ic['weeks'],
                'top_ret': ds.get('top_ret', np.nan),
                'bot_ret': ds.get('bot_ret', np.nan),
                'spread': ds.get('spread', np.nan),  # high PEG - low PEG, 預期負
                'grade': grade(ic['IR']),
            })
    r = pd.DataFrame(rows)
    print(r.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))

    # --- Report ---
    print("\nSaving report...")
    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = []
    lines.append("# V31 Value PEG 窗口驗證 (2026-04-23)\n")
    lines.append(f"- snapshot: {snap['week_end_date'].nunique()} 週 × {snap['stock_id'].nunique()} 檔")
    lines.append(f"- PEG = capped(PE, 150) / avg(last N months of monthly revenue YoY)")
    lines.append(f"- 預期：**PEG 低 = 被低估 = fwd return 高 → IC 應為負**\n")

    lines.append("## IC / decile spread\n")
    lines.append(_df_to_md(r))
    lines.append("")

    # --- 結論 ---
    lines.append("## 結論\n")

    # Best by abs(IR), expect negative
    if not r.empty:
        r_abs = r.copy()
        r_abs['abs_IR'] = r_abs['IR'].abs()
        best = r_abs.sort_values('abs_IR', ascending=False).iloc[0]
        lines.append(f"- 最強 |IR|: window={int(best['window_m'])}m @ fwd_{int(best['horizon'])}d, IR={best['IR']:+.3f} ({best['grade']})")

        # Check live (6m) performance across horizons
        live_rows = r[r['window_m'] == 6]
        if not live_rows.empty:
            live_best = live_rows.loc[live_rows['IR'].abs().idxmax()]
            lines.append(f"- live (6m) 最強: @ fwd_{int(live_best['horizon'])}d, IR={live_best['IR']:+.3f} ({live_best['grade']})")
            # compare vs other windows
            for w in windows:
                if w == 6:
                    continue
                alt = r[r['window_m'] == w]
                if alt.empty:
                    continue
                alt_best = alt.loc[alt['IR'].abs().idxmax()]
                delta = abs(alt_best['IR']) - abs(live_best['IR']) if not pd.isna(alt_best['IR']) and not pd.isna(live_best['IR']) else np.nan
                lines.append(f"- {w}m 最強 |IR|: {alt_best['IR']:+.3f} vs live 6m {live_best['IR']:+.3f} → Δ|IR|={delta:+.3f}")

        # Overall verdict
        max_abs_ir = r['IR'].abs().max()
        if max_abs_ir >= 0.1:
            # Check direction
            best_row = r.loc[r['IR'].abs().idxmax()]
            if best_row['IR'] < 0:
                lines.append(f"\n- **PEG 方向正確且 B 級以上**（低 PEG → 高 fwd return），live 邏輯 (低 PEG 加分) 方向對")
            else:
                lines.append(f"\n- ⚠️ **PEG 方向反向**：高 PEG 反而 fwd return 高，live 加分邏輯可能錯")
        else:
            lines.append(f"\n- **全 |IR| < 0.1 平原**：PEG 在 value pool 中無顯著截面 alpha")
            lines.append("  - live 邏輯影響僅在 +12/+8/-5 加分邊際（value_score 1/100 scale），無需動")
            lines.append("  - 歸檔 V31 為 D 未驗不動")

    lines.append("\n## 產出\n")
    lines.append("- `reports/vf_v31_peg_validation.md`")
    r.to_csv(REPORT_PATH.parent / "vf_v31_peg_validation.csv", index=False)
    lines.append("- `reports/vf_v31_peg_validation.csv`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
