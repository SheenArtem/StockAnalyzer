"""
vf_g5_fu3_scenario_regime.py - VF-G5 FU-3: Scenario × regime 二維分析

VF-G5 Test 3 發現 Scenario A (trend_score >= 9) fwd_60d mean +2.72% 輸 B/C_mid/C_low
(+5.63% / +5.05% / +5.88%)。反直覺：強力進攻 picks 長期最差。

FU-3 目標：驗證這是否 regime-dependent。
  假設 1: 牛市中 A 已 overextended → mean-revert，熊市中 A 反而防禦好
  假設 2: 各 regime 都差 → scenario_engine logic 根本性問題（trend_score >=9 非好訊號）

測試：
  1. A/B/C × HMM regime (trending/ranging/volatile/neutral) × fwd_X
  2. A/B/C × TWII bull/bear (SMA200 filter) × fwd_X
  3. A/B/C × year × fwd_X （2015-2025 時變性）

Usage:
    python tools/vf_g5_fu3_scenario_regime.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

BT_DIR = ROOT / "data_cache" / "backtest"
JOURNAL_PATH = BT_DIR / "trade_journal_qm_tw.parquet"
TWII_PATH = BT_DIR / "_twii_bench.parquet"
REPORT_PATH = ROOT / "reports" / "vf_g5_fu3_scenario_regime.md"


def _df_to_md(df: pd.DataFrame, pct_cols=None, int_cols=None) -> str:
    if df.empty:
        return "(empty)"
    cols = list(df.columns)
    pct_cols = pct_cols or set()
    int_cols = int_cols or {'n', 'year'}

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


def classify_scenario(ts):
    if ts >= 9:
        return 'A'
    if ts >= 8:
        return 'B'
    if ts >= 7:
        return 'C_mid'
    return 'C_low'


def load_twii_bull_bear() -> pd.DataFrame:
    """Build TWII SMA200 bull/bear classification per date.

    bull = TWII close > SMA200, bear = TWII close < SMA200
    """
    b = pd.read_parquet(TWII_PATH)
    # Multi-index columns: ('Close', '^TWII')
    close = b[('Close', '^TWII')].copy()
    close = close.sort_index()
    sma200 = close.rolling(200).mean()
    bull_bear = pd.DataFrame({
        'twii_close': close,
        'twii_sma200': sma200,
    })
    bull_bear['twii_regime'] = np.where(
        bull_bear['twii_close'] > bull_bear['twii_sma200'], 'bull', 'bear'
    )
    bull_bear['twii_regime'] = np.where(
        bull_bear['twii_sma200'].isna(), 'unknown', bull_bear['twii_regime']
    )
    bull_bear = bull_bear.reset_index().rename(columns={'Date': 'date'})
    return bull_bear


def merge_twii_regime(j: pd.DataFrame, twii: pd.DataFrame) -> pd.DataFrame:
    """PIT merge TWII regime as of week_end_date - 1 trading day."""
    j = j.copy()
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])
    j = j.sort_values('week_end_date')
    twii = twii.sort_values('date')
    merged = pd.merge_asof(
        j, twii[['date', 'twii_regime']],
        left_on='week_end_date', right_on='date',
        direction='backward',
    )
    return merged


def scenario_regime_table(j: pd.DataFrame, regime_col: str,
                          horizons: list[int]) -> pd.DataFrame:
    """Return cross-tab (scenario, regime) × horizon mean fwd_X."""
    rows = []
    for (sc, rg), g in j.groupby(['scenario_proxy', regime_col]):
        row = {'scenario': sc, regime_col: rg, 'n': len(g)}
        for h in horizons:
            fwd = f'fwd_{h}d'
            if fwd in g.columns:
                rt = g[fwd].dropna()
                row[f'mean_{h}d'] = rt.mean() if len(rt) else np.nan
                row[f'win_{h}d'] = (rt > 0).mean() if len(rt) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def scenario_year_table(j: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    j = j.copy()
    j['year'] = pd.to_datetime(j['week_end_date']).dt.year
    rows = []
    for (sc, yr), g in j.groupby(['scenario_proxy', 'year']):
        row = {'scenario': sc, 'year': yr, 'n': len(g)}
        for h in horizons:
            fwd = f'fwd_{h}d'
            if fwd in g.columns:
                rt = g[fwd].dropna()
                row[f'mean_{h}d'] = rt.mean() if len(rt) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default=str(JOURNAL_PATH))
    ap.add_argument("--horizons", type=int, nargs='+', default=[20, 40, 60])
    args = ap.parse_args()

    print("=" * 80)
    print("VF-G5 FU-3: Scenario × regime 二維分析")
    print("=" * 80)

    j = pd.read_parquet(args.journal)
    j['week_end_date'] = pd.to_datetime(j['week_end_date'])
    j['scenario_proxy'] = j['trend_score'].apply(classify_scenario)
    print(f"Journal: {len(j)} picks × {j['week_end_date'].nunique()} weeks")

    # --- Load TWII bull/bear ---
    print("\nLoading TWII bench...")
    twii = load_twii_bull_bear()
    print(f"  TWII: {len(twii)} days, bull {(twii['twii_regime']=='bull').sum()} / bear {(twii['twii_regime']=='bear').sum()}")
    j = merge_twii_regime(j, twii)
    print(f"  After merge: bull picks {(j['twii_regime']=='bull').sum()}, bear {(j['twii_regime']=='bear').sum()}")

    # --- Base cross-tab (no regime) ---
    print("\n" + "=" * 80)
    print("Baseline: Scenario × fwd mean (no regime split)")
    print("=" * 80)
    base_rows = []
    for sc, g in j.groupby('scenario_proxy'):
        row = {'scenario': sc, 'n': len(g)}
        for h in args.horizons:
            fwd = f'fwd_{h}d'
            if fwd in g.columns:
                rt = g[fwd].dropna()
                row[f'mean_{h}d'] = rt.mean()
                row[f'win_{h}d'] = (rt > 0).mean()
        base_rows.append(row)
    base = pd.DataFrame(base_rows).sort_values('scenario').reset_index(drop=True)
    print(base.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))

    # --- HMM regime cross-tab ---
    print("\n" + "=" * 80)
    print("Scenario × HMM regime (trending/volatile/ranging/neutral)")
    print("=" * 80)
    hmm_tab = scenario_regime_table(j, 'regime', args.horizons)
    hmm_tab = hmm_tab.sort_values(['scenario', 'regime']).reset_index(drop=True)
    print(hmm_tab.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))

    # --- TWII bull/bear cross-tab ---
    print("\n" + "=" * 80)
    print("Scenario × TWII bull/bear (SMA200 filter)")
    print("=" * 80)
    twii_tab = scenario_regime_table(j, 'twii_regime', args.horizons)
    twii_tab = twii_tab.sort_values(['scenario', 'twii_regime']).reset_index(drop=True)
    print(twii_tab.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if abs(x) < 10 else f"{x:.2%}"))

    # --- Year × scenario ---
    print("\n" + "=" * 80)
    print("Scenario × Year fwd_60d")
    print("=" * 80)
    yr_tab = scenario_year_table(j, args.horizons)
    # Pivot year for readability, fwd_60d only
    pivot = yr_tab.pivot_table(index='year', columns='scenario', values='mean_60d')
    print(pivot.to_string(float_format=lambda x: f"{x:+.2%}"))
    # Also counts
    pivot_n = yr_tab.pivot_table(index='year', columns='scenario', values='n')
    print("\nPick counts:")
    print(pivot_n.fillna(0).astype(int).to_string())

    # --- Write report ---
    print("\nSaving report...")
    REPORT_PATH.parent.mkdir(exist_ok=True)
    lines = []
    lines.append("# VF-G5 FU-3 Scenario × Regime 二維分析 (2026-04-23)\n")
    lines.append(f"- Journal: {len(j)} picks × {j['week_end_date'].nunique()} weeks × {j['stock_id'].nunique()} tickers")
    lines.append(f"- Proxy: A (trend_score≥9) / B (=8) / C_mid (=7) / C_low (<7, picks ≥6)\n")

    lines.append("## Baseline: Scenario × fwd return (no regime)\n")
    pct_cols = set(f'mean_{h}d' for h in args.horizons) | set(f'win_{h}d' for h in args.horizons)
    lines.append(_df_to_md(base, pct_cols=pct_cols))
    lines.append("")

    lines.append("## Scenario × HMM regime\n")
    lines.append("HMM regime (from trade_journal): trending / volatile / ranging / neutral\n")
    lines.append(_df_to_md(hmm_tab, pct_cols=pct_cols))
    lines.append("")

    # A 在各 HMM regime 的表現
    a_hmm = hmm_tab[hmm_tab['scenario'] == 'A']
    lines.append("### A 在各 HMM regime fwd_60d mean\n")
    for _, r in a_hmm.iterrows():
        lines.append(f"- {r['regime']} (n={int(r['n'])}): fwd_60d = {r['mean_60d']:+.2%}, winrate = {r['win_60d']:+.0%}")
    lines.append("")

    lines.append("## Scenario × TWII bull/bear (SMA200)\n")
    lines.append("TWII bull = TWII close > 200-day SMA；bear = below\n")
    lines.append(_df_to_md(twii_tab, pct_cols=pct_cols))
    lines.append("")

    a_twii = twii_tab[twii_tab['scenario'] == 'A']
    lines.append("### A 在 bull vs bear\n")
    for _, r in a_twii.iterrows():
        lines.append(f"- {r['twii_regime']} (n={int(r['n'])}): fwd_60d = {r['mean_60d']:+.2%}, winrate = {r['win_60d']:+.0%}")
    lines.append("")

    # Compare A vs C avg across regimes
    lines.append("## A vs C avg 差距 (fwd_60d)\n")
    lines.append("| regime split | A fwd_60d | C avg fwd_60d | Δ (A - C) |")
    lines.append("| --- | --- | --- | --- |")
    # HMM
    for reg_col, reg_tab, reg_label in [('regime', hmm_tab, 'HMM'), ('twii_regime', twii_tab, 'TWII')]:
        for rg, rg_grp in reg_tab.groupby(reg_col):
            a_row = rg_grp[rg_grp['scenario'] == 'A']
            c_rows = rg_grp[rg_grp['scenario'].str.startswith('C')]
            if a_row.empty or c_rows.empty:
                continue
            a_ret = a_row.iloc[0]['mean_60d']
            c_n = c_rows['n'].sum()
            c_ret = (c_rows['mean_60d'] * c_rows['n']).sum() / c_n if c_n > 0 else np.nan
            delta = a_ret - c_ret
            lines.append(f"| {reg_label} / {rg} | {a_ret:+.2%} | {c_ret:+.2%} | {delta:+.2%} |")
    lines.append("")

    lines.append("## Scenario × Year fwd_60d\n")
    # Use the year pivot
    pivot_display = pivot.copy()
    pivot_display.columns = [f'{c}_fwd60' for c in pivot_display.columns]
    pivot_display = pivot_display.reset_index()
    pct_cols_pivot = set(pivot_display.columns) - {'year'}
    lines.append(_df_to_md(pivot_display, pct_cols=pct_cols_pivot))
    lines.append("")

    # --- 結論 ---
    lines.append("## 結論\n")

    # 1. TWII bull/bear 對 A 表現
    lines.append("### A 是否 regime-dependent?\n")
    bull_a = twii_tab[(twii_tab['scenario'] == 'A') & (twii_tab['twii_regime'] == 'bull')]
    bear_a = twii_tab[(twii_tab['scenario'] == 'A') & (twii_tab['twii_regime'] == 'bear')]
    if not bull_a.empty and not bear_a.empty:
        ba_ret = bull_a.iloc[0]['mean_60d']
        be_ret = bear_a.iloc[0]['mean_60d']
        lines.append(f"- TWII bull A fwd_60d: {ba_ret:+.2%} (n={int(bull_a.iloc[0]['n'])})")
        lines.append(f"- TWII bear A fwd_60d: {be_ret:+.2%} (n={int(bear_a.iloc[0]['n'])})")
        lines.append(f"- **差距: {ba_ret - be_ret:+.2%}**\n")

    # 2. C_low in bear vs bull (defensive check)
    bull_c = twii_tab[(twii_tab['scenario'] == 'C_low') & (twii_tab['twii_regime'] == 'bull')]
    bear_c = twii_tab[(twii_tab['scenario'] == 'C_low') & (twii_tab['twii_regime'] == 'bear')]
    if not bull_c.empty and not bear_c.empty:
        bc_ret = bull_c.iloc[0]['mean_60d']
        bec_ret = bear_c.iloc[0]['mean_60d']
        lines.append(f"- TWII bull C_low fwd_60d: {bc_ret:+.2%}")
        lines.append(f"- TWII bear C_low fwd_60d: {bec_ret:+.2%}")
        lines.append("")

    lines.append("### 判讀指南\n")
    lines.append("- 若 A bull vs bear 差距 > 5pp → strong regime-dependent，可考慮 regime-aware action_plan")
    lines.append("- 若 A 在所有 regime 都差 → scenario logic 根本問題（trend_score ≥ 9 非好訊號）")
    lines.append("- 若 A bear 反而好 → 強勢股熊市抗跌特性")
    lines.append("")

    # Save CSVs
    lines.append("## 產出\n")
    lines.append("- `tools/vf_g5_fu3_scenario_regime.py`")
    lines.append("- `reports/vf_g5_fu3_scenario_regime.md`")
    base.to_csv(REPORT_PATH.parent / "vf_g5_fu3_baseline.csv", index=False)
    hmm_tab.to_csv(REPORT_PATH.parent / "vf_g5_fu3_hmm_regime.csv", index=False)
    twii_tab.to_csv(REPORT_PATH.parent / "vf_g5_fu3_twii_regime.csv", index=False)
    pivot.to_csv(REPORT_PATH.parent / "vf_g5_fu3_year_pivot.csv")
    lines.append("- `reports/vf_g5_fu3_{baseline,hmm_regime,twii_regime,year_pivot}.csv`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
