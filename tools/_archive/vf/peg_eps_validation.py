"""
peg_eps_validation.py - EPS 版 PEG 因子驗證 (仿 VF-V31, 分母改 EPS YoY)

VF-V31 (2026-04-23) 用 PE / 月營收 YoY -> D 歸檔 (實用 horizon 無 alpha, 長期反向)。
本驗證改用 EPS YoY 當分母, 測使用者關心的「最近兩季合計 YoY」等口徑,
看 EPS 版是否優於營收版, 並在「EPS 為正且正成長」的乾淨子集裡測 PEG 有無 alpha。

口徑:
  - eps_yoy_1q: 最近單季 vs 去年同季
  - eps_yoy_2q: 最近兩季合計 vs 去年同期兩季 (使用者口徑)
  - eps_yoy_4q: TTM 四季 vs 前一個 TTM

Guard (景氣股護欄): 基期 EPS <= 0 -> YoY = NaN (排除虧轉盈/趨近零的失真值)
PIT: 季末 + 95 天 (涵蓋 Q4 年報 3/31 公布) -> merge_asof backward, 杜絕 look-ahead
PEG: capped(PE,150) / YoY%, guard YoY>1, PEG cap 50  (全部沿用 VF-V31 以可比)

Usage: python tools/peg_eps_validation.py
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[3]  # tools/_archive/vf/ -> repo root
BT_DIR = ROOT / "data_cache" / "backtest"
SNAPSHOT_PATH = BT_DIR / "trade_journal_value_tw_snapshot.parquet"
INCOME_PATH = BT_DIR / "financials_income.parquet"
REPORT_PATH = ROOT / "reports" / "peg_eps_validation.md"

WINDOWS = ['1q', '2q', '4q']
HORIZONS = [20, 40, 60, 120]
PIT_LAG = 95


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


def _df_to_md(df):
    if df.empty:
        return "(empty)"
    cols = list(df.columns)
    pct_cols = {'top_ret', 'bot_ret', 'spread', 'winrate'}
    int_cols = {'horizon', 'weeks', 'n_obs'}

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


def compute_eps_yoy(eps: pd.DataFrame) -> pd.DataFrame:
    """單季 EPS panel -> 三口徑 YoY%。基期<=0 -> NaN。回傳 (stock_id, q_date, eps_yoy_1q/2q/4q)。"""
    eps = eps.copy()
    eps['date'] = pd.to_datetime(eps['date'])
    eps['value'] = pd.to_numeric(eps['value'], errors='coerce')
    eps = eps.dropna(subset=['value'])
    eps['pidx'] = eps['date'].dt.year * 4 + (eps['date'].dt.quarter - 1)

    out = []
    for sid, g in eps.groupby('stock_id', sort=False):
        s = g.drop_duplicates('pidx').set_index('pidx')['value'].sort_index()
        dmap = g.drop_duplicates('pidx').set_index('pidx')['date']

        def gv(p):
            return s[p] if p in s.index else np.nan

        def yoy(recent, base):
            if pd.isna(recent) or pd.isna(base) or base <= 0:   # 負基期 guard
                return np.nan
            return (recent / base - 1) * 100

        for p in s.index:
            e = gv(p)
            r1, b1 = e, gv(p - 4)
            r2, b2 = e + gv(p - 1), gv(p - 4) + gv(p - 5)
            r4 = sum(gv(p - k) for k in range(4))
            b4 = sum(gv(p - k) for k in range(4, 8))
            out.append({
                'stock_id': sid, 'q_date': dmap[p],
                'eps_yoy_1q': yoy(r1, b1),
                'eps_yoy_2q': yoy(r2, b2),
                'eps_yoy_4q': yoy(r4, b4),
            })
    return pd.DataFrame(out)


def pit_merge(snap, scored, col, lag_days=PIT_LAG):
    scored = scored.dropna(subset=[col]).copy()
    scored['available_date'] = scored['q_date'] + pd.Timedelta(days=lag_days)
    scored = scored.sort_values(['stock_id', 'available_date'])
    result = pd.Series(np.nan, index=snap.index, name=col)
    for sid, g_snap in snap.groupby('stock_id'):
        g_score = scored[scored['stock_id'] == sid]
        if g_score.empty:
            continue
        left = g_snap[['week_end_date']].sort_values('week_end_date').reset_index()
        right = g_score[['available_date', col]].sort_values('available_date')
        merged = pd.merge_asof(left, right, left_on='week_end_date',
                               right_on='available_date', direction='backward')
        result.loc[merged['index']] = merged[col].values
    return result


def ic_analysis(df, factor, horizon):
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
    return {'IC': arr.mean(),
            'IR': arr.mean() / std if len(arr) > 1 and std > 0 else np.nan,
            'weeks': len(arr)}


def decile_spread(df, factor, horizon):
    target = f'fwd_{horizon}d'
    sub = df[[factor, target, 'week_end_date']].dropna()
    weekly = []
    for _, grp in sub.groupby('week_end_date'):
        if len(grp) < 30:
            continue
        gs = grp.sort_values(factor).reset_index(drop=True)
        cut = max(1, len(gs) // 10)
        weekly.append({'top': gs.iloc[-cut:][target].mean(),
                       'bot': gs.iloc[:cut][target].mean()})
    if not weekly:
        return {}
    r = pd.DataFrame(weekly)
    return {'top_ret': r['top'].mean(), 'bot_ret': r['bot'].mean(),
            'spread': (r['top'] - r['bot']).mean(),
            'winrate': ((r['top'] - r['bot']) > 0).mean()}


def main():
    print("=" * 80)
    print("EPS 版 PEG 驗證 (分母 = EPS YoY, 仿 VF-V31)")
    print("=" * 80)

    snap = pd.read_parquet(SNAPSHOT_PATH)
    snap['week_end_date'] = pd.to_datetime(snap['week_end_date'])
    snap = snap.sort_values(['stock_id', 'week_end_date']).reset_index(drop=True)
    print(f"snapshot: {len(snap)} rows, {snap['stock_id'].nunique()} stocks, "
          f"PE valid={snap['pe'].notna().sum()}")

    inc = pd.read_parquet(INCOME_PATH)
    eps = inc[inc['type'] == 'EPS'][['date', 'stock_id', 'value']]
    eps = eps[eps['stock_id'].isin(snap['stock_id'].unique())]
    print(f"EPS rows (snapshot stocks)={len(eps)}, stocks={eps['stock_id'].nunique()}")

    print("\nComputing EPS YoY (1q/2q/4q, 基期<=0 -> NaN)...")
    yoy = compute_eps_yoy(eps)
    for w in WINDOWS:
        c = f'eps_yoy_{w}'
        tot = yoy[c].notna().sum()
        print(f"  {c}: {tot} non-NaN quarterly obs")

    print(f"\nPIT merge (lag={PIT_LAG}d)...")
    for w in WINDOWS:
        c = f'eps_yoy_{w}'
        snap[c] = pit_merge(snap, yoy, c)
        print(f"  {c}: {snap[c].notna().sum()} filled into snapshot")

    snap['pe_capped'] = np.where((snap['pe'] > 0) & (snap['pe'] < 150), snap['pe'], np.nan)
    for w in WINDOWS:
        gc, pc = f'eps_yoy_{w}', f'peg_{w}'
        snap[pc] = np.where((snap['pe_capped'] > 0) & (snap[gc] > 1),
                            snap['pe_capped'] / snap[gc], np.nan)
        snap.loc[snap[pc] > 50, pc] = np.nan

    print("\nPEG obs per 口徑:")
    for w in WINDOWS:
        print(f"  peg_{w}: {snap[f'peg_{w}'].notna().sum()}")

    rows = []
    for w in WINDOWS:
        pc = f'peg_{w}'
        for h in HORIZONS:
            ic = ic_analysis(snap, pc, h)
            ds = decile_spread(snap, pc, h)
            rows.append({'window': w, 'horizon': h, 'IC': ic['IC'], 'IR': ic['IR'],
                         'weeks': ic['weeks'], 'top_ret': ds.get('top_ret', np.nan),
                         'bot_ret': ds.get('bot_ret', np.nan),
                         'spread': ds.get('spread', np.nan), 'grade': grade(ic['IR'])})
    r = pd.DataFrame(rows)
    print("\n" + "=" * 80)
    print("IC / decile spread (低 PEG=便宜 預期 IC<0; spread=高PEG-低PEG 預期<0)")
    print("=" * 80)
    print(r.to_string(index=False))

    # verdict
    max_abs = r['IR'].abs().max()
    best = r.loc[r['IR'].abs().idxmax()]
    short = r[r['horizon'].isin([20, 40])]
    verdict = []
    verdict.append(f"最強 |IR|: window={best['window']} @ fwd_{int(best['horizon'])}d, "
                   f"IR={best['IR']:+.3f} ({best['grade']})")
    if max_abs < 0.1:
        verdict.append("全 |IR| < 0.1 -> 平原, EPS 版 PEG 同樣無顯著截面 alpha (對齊 VF-V31)")
    elif best['IR'] > 0:
        verdict.append("最強訊號為正 IR -> PEG 方向反向 (高 PEG 贏), 與 live 低-PEG-加分相反 (對齊 VF-V31)")
    else:
        verdict.append("最強訊號為負 IR -> 低 PEG 方向正確, 與營收版不同, 值得進一步 walk-forward")
    verdict.append(f"實用 horizon (20/40d) |IR| max = {short['IR'].abs().max():.3f}, "
                   f"spread 範圍 {short['spread'].min():+.2%} ~ {short['spread'].max():+.2%}")

    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = ["# EPS 版 PEG 因子驗證 (仿 VF-V31, 分母改 EPS YoY)\n"]
    lines.append(f"- snapshot: {snap['week_end_date'].nunique()} 週 x {snap['stock_id'].nunique()} 檔 (value pool)")
    lines.append(f"- 分母 = EPS YoY, 三口徑 1q/2q/4q; 基期 EPS<=0 -> NaN; PIT lag={PIT_LAG}d")
    lines.append("- PEG = capped(PE,150) / YoY%, guard YoY>1, PEG cap 50 (沿用 VF-V31)\n")
    lines.append("## IC / decile spread\n")
    lines.append(_df_to_md(r))
    lines.append("\n## 結論\n")
    for v in verdict:
        lines.append(f"- {v}")
    r.to_csv(REPORT_PATH.parent / "peg_eps_validation.csv", index=False)
    lines.append("\n## 產出\n- `reports/peg_eps_validation.md`\n- `reports/peg_eps_validation.csv`")
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nSaved {REPORT_PATH}")
    for v in verdict:
        print("VERDICT:", v)


if __name__ == "__main__":
    main()
