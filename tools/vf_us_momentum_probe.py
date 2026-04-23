"""
vf_us_momentum_probe.py - US 動能因子快速 IC probe

背景：VF-Value-ex2 已驗 US F-score / FCF / ROIC / GP 全 D 反向或噪音，所以 TW QM
(F50/B30/T20) 直接搬 US 不可行。轉向先確認「US 純動能 alpha 是否存在」。

若 US momentum 有 alpha → 考慮建 US momentum screener（不是 QM composite）
若 US momentum 也 D → 美股研究告段落，短期專注台股 live

從 ohlcv_us.parquet 算 4 個經典動能因子：
  1. mom_12m: 過去 252d 報酬
  2. mom_12_1: 過去 12m 報酬排除最近 1m (Jegadeesh-Titman 經典)
  3. mom_6_1:  過去 6m 報酬排除最近 1m
  4. ma_alignment: 20d > 60d > 120d 斜率對齊分數

每月底抽樣（不是每週），對每個 factor 算 cross-section IC vs fwd_20d/60d/120d，
計算 monthly walk-forward IC + IR，B 級以上就值得建 screener。

Usage:
    python tools/vf_us_momentum_probe.py
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
OHLCV_PATH = BT_DIR / "ohlcv_us.parquet"
REPORT_PATH = ROOT / "reports" / "vf_us_momentum_probe.md"


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
    int_cols = {'n_obs', 'n_months', 'horizon', 'pos_months', 'neg_months'}
    pct_cols = {'mean_ret', 'winrate', 'top', 'bot', 'spread', 'top_ret', 'bot_ret',
                'month_winrate'}

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


def build_monthly_panel(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """For each stock, resample daily to month-end sampling + compute momentum factors + fwd returns."""
    ohlcv = ohlcv[['ticker', 'date', 'AdjClose']].copy()
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])
    ohlcv = ohlcv.dropna(subset=['AdjClose']).sort_values(['ticker', 'date']).reset_index(drop=True)

    # For each ticker, pick month-end rows (the last date in each month)
    panels = []
    for tkr, g in ohlcv.groupby('ticker', sort=False):
        if len(g) < 260:  # need >= 1 yr
            continue
        g = g.set_index('date').sort_index()
        # month-end resample
        month_end = g['AdjClose'].resample('ME').last().dropna()
        if len(month_end) < 15:  # need >= ~1.2 yr of months
            continue

        df = pd.DataFrame({'close': month_end}).reset_index()
        df['ticker'] = tkr

        # Momentum factors (months)
        df['mom_12m'] = df['close'].pct_change(12)
        df['mom_12_1'] = df['close'].shift(1).pct_change(11)  # 11-month ret ending last month
        df['mom_6_1'] = df['close'].shift(1).pct_change(5)    # 5-month ret ending last month

        # MA alignment (daily MA ratio) — use month-end values of daily
        # Approximation: use monthly MA
        df['ma3'] = df['close'].rolling(3).mean()   # ~60d
        df['ma6'] = df['close'].rolling(6).mean()   # ~120d
        df['ma12'] = df['close'].rolling(12).mean()  # ~250d
        df['ma_align'] = ((df['close'] > df['ma3']).astype(int)
                          + (df['ma3'] > df['ma6']).astype(int)
                          + (df['ma6'] > df['ma12']).astype(int))  # 0-3

        # Forward returns (months)
        for h_m, h_d in [(1, 20), (3, 60), (6, 120)]:
            df[f'fwd_{h_d}d'] = df['close'].shift(-h_m) / df['close'] - 1

        panels.append(df)

    out = pd.concat(panels, ignore_index=True) if panels else pd.DataFrame()
    return out


def ic_by_month(panel: pd.DataFrame, factor: str, horizon_col: str) -> dict:
    """Monthly cross-section IC."""
    sub = panel[['date', factor, horizon_col]].dropna()
    if sub.empty:
        return {'IC': np.nan, 'IR': np.nan, 'n_months': 0}
    monthly = []
    for _, g in sub.groupby('date'):
        if len(g) < 20:
            continue
        rho, _ = stats.spearmanr(g[factor], g[horizon_col])
        if not pd.isna(rho):
            monthly.append(rho)
    if not monthly:
        return {'IC': np.nan, 'IR': np.nan, 'n_months': 0}
    arr = np.array(monthly)
    std = arr.std(ddof=1) if len(arr) > 1 else np.nan
    pos = (arr > 0).sum()
    return {
        'IC': arr.mean(),
        'IR': arr.mean() / std if len(arr) > 1 and std > 0 else np.nan,
        'n_months': len(arr),
        'pos_months': pos,
        'neg_months': len(arr) - pos,
        'month_winrate': pos / len(arr),
    }


def decile_spread(panel: pd.DataFrame, factor: str, horizon_col: str) -> dict:
    sub = panel[['date', factor, horizon_col]].dropna()
    if sub.empty:
        return {}
    monthly = []
    for _, g in sub.groupby('date'):
        if len(g) < 30:
            continue
        gs = g.sort_values(factor).reset_index(drop=True)
        n = len(gs)
        cut = max(1, n // 10)
        monthly.append({
            'top': gs.iloc[-cut:][horizon_col].mean(),
            'bot': gs.iloc[:cut][horizon_col].mean(),
        })
    if not monthly:
        return {}
    r = pd.DataFrame(monthly)
    return {
        'top_ret': r['top'].mean(),
        'bot_ret': r['bot'].mean(),
        'spread': (r['top'] - r['bot']).mean(),
        'winrate': ((r['top'] - r['bot']) > 0).mean(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ohlcv", default=str(OHLCV_PATH))
    ap.add_argument("--min-date", default="2015-06-01")
    args = ap.parse_args()

    print("=" * 80)
    print("VF-US-1: US pure momentum alpha probe")
    print("=" * 80)

    print("Loading ohlcv_us...")
    ohlcv = pd.read_parquet(args.ohlcv)
    ohlcv = ohlcv[pd.to_datetime(ohlcv['date']) >= pd.Timestamp(args.min_date)]
    print(f"  {len(ohlcv)} rows × {ohlcv['ticker'].nunique()} tickers")
    print(f"  Date range: {ohlcv['date'].min()} ~ {ohlcv['date'].max()}")

    print("\nBuilding monthly panel + momentum factors...")
    panel = build_monthly_panel(ohlcv)
    panel['date'] = pd.to_datetime(panel['date'])
    print(f"  panel: {len(panel)} rows × {panel['ticker'].nunique()} tickers × {panel['date'].nunique()} months")

    factors = [
        ('mom_12m', '12m return'),
        ('mom_12_1', '12-1m (J-T classic)'),
        ('mom_6_1', '6-1m'),
        ('ma_align', 'MA alignment 0-3'),
    ]
    horizons = [('fwd_20d', '1m'), ('fwd_60d', '3m'), ('fwd_120d', '6m')]

    rows = []
    print("\n" + "=" * 80)
    print("IC / IR / decile spread (factor × horizon)")
    print("=" * 80)
    for fcol, flabel in factors:
        for hcol, hlabel in horizons:
            ic = ic_by_month(panel, fcol, hcol)
            ds = decile_spread(panel, fcol, hcol)
            rows.append({
                'factor': flabel,
                'horizon': hlabel,
                'IC': ic['IC'],
                'IR': ic['IR'],
                'n_months': ic['n_months'],
                'month_winrate': ic.get('month_winrate', np.nan),
                'top_ret': ds.get('top_ret', np.nan),
                'bot_ret': ds.get('bot_ret', np.nan),
                'spread': ds.get('spread', np.nan),
                'grade': grade(ic['IR']),
            })

    r = pd.DataFrame(rows)
    print(r.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))

    # --- 寫 report ---
    print("\nSaving report...")
    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = []
    lines.append("# VF-US Momentum Probe (2026-04-23)\n")
    lines.append(f"- ohlcv_us: {len(ohlcv)} rows × {ohlcv['ticker'].nunique()} tickers")
    lines.append(f"- date range: {ohlcv['date'].min().date()} ~ {ohlcv['date'].max().date()}")
    lines.append(f"- monthly panel: {len(panel)} obs × {panel['ticker'].nunique()} tickers × {panel['date'].nunique()} months\n")

    lines.append("## IC / IR / decile spread\n")
    lines.append(_df_to_md(r))
    lines.append("")

    # --- 結論 ---
    lines.append("## 結論\n")
    # best factor by IR
    best_ir = r['IR'].max()
    best_row = r.loc[r['IR'].idxmax()] if not r.empty else None
    if best_row is not None:
        lines.append(f"- 最強因子: **{best_row['factor']}** @ {best_row['horizon']}: IR={best_ir:+.3f} ({best_row['grade']})")
    lines.append("")

    # Decision
    any_b_plus = (r['IR'] >= 0.1).any()
    any_a = (r['IR'] >= 0.3).any()
    if any_a:
        lines.append("- **A 級以上因子存在** → US momentum 有強 alpha，建議建 US momentum screener")
    elif any_b_plus:
        lines.append("- **B 級因子存在** → US momentum 有中等 alpha，可考慮簡化版 screener")
    else:
        lines.append("- **全 C 或 D 級** → US momentum 在本資料區間 (2015-2025) **無顯著 alpha**")
        lines.append("- 結合 VF-Value-ex2 F-score D 反向 → 美股研究短期告段落，建議專注台股 live")

    # Save CSV
    r.to_csv(REPORT_PATH.parent / "vf_us_momentum_probe.csv", index=False)
    lines.append("\n## 產出\n")
    lines.append("- `reports/vf_us_momentum_probe.md`")
    lines.append("- `reports/vf_us_momentum_probe.csv`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
