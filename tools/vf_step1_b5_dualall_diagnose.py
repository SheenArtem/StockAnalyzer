"""
Step B.5 — Dual-all diagnose: QM leg / Value leg / Combined 分解 2024-2025

Council R2 共識：先跑 30min Dual-all 分 leg 年度分解，才能決定 Step D 能不能 ship。

分流 criteria:
  (a) QM 正常 + Value 崩  → factor decay（Value 需重驗），DELAY ship
  (b) QM 崩 + Value 崩    → regime shift（整個選股框架對 0050 失效），接受現實 ship Dual-all
  (c) QM 正常 + Value 正常 → tv_top_25 crowding 獨有，SAFE ship Dual-all

Pass threshold (Sharpe):
  - 2024-2025 兩年 avg Sharpe > 0.3 → 腿正常
  - < 0.3 → 腿崩
  - < 0   → 腿嚴重虧損

Benchmark: TWII 2024 年漲 ~29%、2025 年漲 ~23%（實際從 _twii_bench 算）

Output: reports/vf_step1_b5_dualall_diagnose.md + CSV
"""
import numpy as np
import pandas as pd
from pathlib import Path

from vf_step1_dual_mcap_grid import (
    QM_SNAPSHOT, apply_stage1, compute_live_score, add_tv_rank, load_twii,
    classify_regime_at, compute_metrics, ROOT,
    REBALANCE_EVERY,
)

LONG_SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_value_tw_long_snapshot.parquet'
OUT_CSV = ROOT / 'reports/vf_step1_b5_dualall_diagnose.csv'
OUT_MD = ROOT / 'reports/vf_step1_b5_dualall_diagnose.md'


def backtest_three_legs(stage1_ranked, qm_df, twii_close, top_n_value=20, top_n_qm=20):
    """同時算三條腿：Value only / QM only / Combined 50/50，only_volatile regime。

    返回 dict {leg_name: pr_df}
    """
    weeks = sorted(stage1_ranked['week_end_date'].unique())
    rebalance_weeks = weeks[::REBALANCE_EVERY]

    rows = []
    for wk in rebalance_weeks:
        regime = classify_regime_at(wk, twii_close)
        is_volatile = (regime == 'volatile')

        # Value leg
        val_ret = 0.0
        v_n = 0
        if is_volatile:
            vpool = stage1_ranked[stage1_ranked['week_end_date'] == wk]
            if not vpool.empty:
                vtop = vpool.nlargest(min(top_n_value, len(vpool)), 'v_score_live')
                val_ret = vtop['fwd_20d'].mean()
                v_n = len(vtop)

        # QM leg (always on)
        qm_ret = 0.0
        q_n = 0
        qpool = qm_df[qm_df['week_end_date'] == wk]
        if not qpool.empty:
            qtop = qpool[qpool['rank_in_top50'] <= top_n_qm]
            qm_ret = qtop['fwd_20d'].mean()
            q_n = len(qtop)

        rows.append({
            'date': wk,
            'regime': regime,
            'val_ret': val_ret if v_n > 0 else np.nan,
            'qm_ret': qm_ret if q_n > 0 else np.nan,
            'val_n': v_n,
            'qm_n': q_n,
            'combined_ret': 0.5 * val_ret + 0.5 * qm_ret,
        })

    pr = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    return pr


def annual_leg_decompose(pr, leg_col):
    """某腿的年度 Sharpe / CAGR / MDD / hit。"""
    sub = pr[['date', leg_col]].copy()
    sub['year'] = pd.to_datetime(sub['date']).dt.year
    sub = sub.rename(columns={leg_col: 'ret'})
    # value leg 非出場週可能 NaN；當作 0
    sub['ret'] = sub['ret'].fillna(0.0)

    rows = []
    for yr, g in sub.groupby('year'):
        if len(g) < 2:
            continue
        g = g.sort_values('date').reset_index(drop=True)
        g['cum'] = (1 + g['ret']).cumprod()
        ann_ret = g['cum'].iloc[-1] - 1
        vol = g['ret'].std() * np.sqrt(13) if len(g) > 1 else 0
        sharpe = (ann_ret - 0.01) / vol if vol > 0 else np.nan
        rolling = g['cum'].cummax()
        mdd = ((g['cum'] - rolling) / rolling).min()
        rows.append({
            'year': int(yr),
            'leg': leg_col,
            'n_rebal': len(g),
            'ann_ret_pct': round(ann_ret * 100, 2),
            'sharpe': round(sharpe, 3) if not np.isnan(sharpe) else None,
            'mdd_pct': round(mdd * 100, 2),
            'hit': round((g['ret'] > 0).mean() * 100, 1),
        })
    return pd.DataFrame(rows)


def twii_annual(twii_close):
    """TWII 年度報酬（自然年，全期在場）。"""
    rows = []
    for yr, grp in twii_close.groupby(twii_close.index.year):
        if len(grp) < 2:
            continue
        p0, p1 = grp.iloc[0], grp.iloc[-1]
        ret = p1 / p0 - 1
        rolling = grp.cummax()
        dd = ((grp - rolling) / rolling).min()
        rows.append({'year': int(yr), 'twii_ret_pct': round(ret * 100, 2),
                     'twii_mdd_pct': round(dd * 100, 2)})
    return pd.DataFrame(rows)


def classify_verdict(val_2024_sh, val_2025_sh, qm_2024_sh, qm_2025_sh):
    """三方共識 flow chart 分類。"""
    # Two-year average Sharpe per leg
    val_avg = np.mean([s for s in [val_2024_sh, val_2025_sh] if s is not None])
    qm_avg = np.mean([s for s in [qm_2024_sh, qm_2025_sh] if s is not None])

    val_ok = val_avg > 0.3
    qm_ok = qm_avg > 0.3

    if val_ok and qm_ok:
        return ('C - SAFE_SHIP',
                f'Value 2yr avg Sharpe {val_avg:.2f}, QM 2yr avg Sharpe {qm_avg:.2f} — '
                '兩腿正常 → tv_top_25 crowding 獨有，Dual-all 安全 ship')
    elif qm_ok and not val_ok:
        return ('A - FACTOR_DECAY',
                f'Value 2yr Sharpe {val_avg:.2f} < 0.3, QM {qm_avg:.2f} OK — '
                'Value factor decay，DELAY ship 重驗 Value')
    elif val_ok and not qm_ok:
        return ('D - QM_DECAY',
                f'QM 2yr Sharpe {qm_avg:.2f} < 0.3, Value {val_avg:.2f} OK — '
                '意外 QM decay，DELAY ship 重驗 QM')
    else:
        return ('B - REGIME_SHIFT',
                f'Value {val_avg:.2f} + QM {qm_avg:.2f} 雙崩 — '
                'regime shift，ship Dual-all 但接受現實（alpha 消失）')


def main():
    print('=== Step B.5 — Dual-all leg diagnose (2024-2025 focus) ===')

    # Load data
    df = pd.read_parquet(LONG_SNAPSHOT)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df['pb'] = df['pb'] / 10.0  # PB scale fix (verified in Step B)
    print(f'Value universe: {len(df)} rows, {df["stock_id"].nunique()} stocks, '
          f'{df["week_end_date"].min().date()} -> {df["week_end_date"].max().date()}')

    stage1 = apply_stage1(df)
    stage1['v_score_live'] = compute_live_score(stage1)
    stage1 = add_tv_rank(stage1)
    # Dual-all: 不過濾 mcap
    print(f'Stage 1 (no mcap filter): {len(stage1)} rows')

    qm_df = pd.read_parquet(QM_SNAPSHOT)
    qm_df['week_end_date'] = pd.to_datetime(qm_df['week_end_date'])
    qm_df = qm_df[qm_df['week_end_date'] >= df['week_end_date'].min()]
    print(f'QM universe: {len(qm_df)} rows')

    twii_close = load_twii()

    # Run three-leg backtest
    print('\n-- Running three-leg backtest (Value / QM / Combined) --')
    pr = backtest_three_legs(stage1, qm_df, twii_close)

    # Annual decomposition per leg
    legs = ['val_ret', 'qm_ret', 'combined_ret']
    annuals = []
    for leg in legs:
        ann = annual_leg_decompose(pr, leg)
        annuals.append(ann)
    all_annual = pd.concat(annuals, ignore_index=True)

    # TWII annual for comparison
    twii_ann = twii_annual(twii_close)
    print('\n-- TWII annual ref --')
    print(twii_ann.to_string(index=False))

    # Extract 2024/2025 per leg
    val_2024 = all_annual[(all_annual['leg'] == 'val_ret') & (all_annual['year'] == 2024)]
    val_2025 = all_annual[(all_annual['leg'] == 'val_ret') & (all_annual['year'] == 2025)]
    qm_2024 = all_annual[(all_annual['leg'] == 'qm_ret') & (all_annual['year'] == 2024)]
    qm_2025 = all_annual[(all_annual['leg'] == 'qm_ret') & (all_annual['year'] == 2025)]
    comb_2024 = all_annual[(all_annual['leg'] == 'combined_ret') & (all_annual['year'] == 2024)]
    comb_2025 = all_annual[(all_annual['leg'] == 'combined_ret') & (all_annual['year'] == 2025)]

    val_2024_sh = val_2024['sharpe'].iloc[0] if len(val_2024) else None
    val_2025_sh = val_2025['sharpe'].iloc[0] if len(val_2025) else None
    qm_2024_sh = qm_2024['sharpe'].iloc[0] if len(qm_2024) else None
    qm_2025_sh = qm_2025['sharpe'].iloc[0] if len(qm_2025) else None

    verdict, explain = classify_verdict(val_2024_sh, val_2025_sh, qm_2024_sh, qm_2025_sh)
    print(f'\n-- VERDICT: {verdict} --\n{explain}\n')

    # Print annual tables
    print('\n-- Annual breakdown per leg --')
    print(all_annual.pivot_table(index='year', columns='leg',
                                   values=['ann_ret_pct', 'sharpe'],
                                   aggfunc='first').to_string())

    # Save
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    all_annual.to_csv(OUT_CSV, index=False)
    print(f'CSV: {OUT_CSV.relative_to(ROOT)}')

    # MD report
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Step B.5 — Dual-all Leg Diagnose (2024-2025 focus)\n\n')
        f.write('**目的**：Council R2 共識 — 跑 Dual-all 分 Value/QM 雙腿年度分解，'
                '判斷 2024-2025 alpha 崩潰是 factor decay / regime shift / crowding。\n\n')
        f.write('**Spec**: Dual-all (no mcap filter) + only_volatile，'
                'top_20 each side，Value fwd_20d + QM fwd_20d，月頻 rebalance。\n\n')

        f.write(f'## VERDICT: **{verdict}**\n\n')
        f.write(f'> {explain}\n\n')

        f.write('## 1. 2024-2025 焦點比較\n\n')
        f.write('| leg | 2024 Ret% | 2024 Sharpe | 2024 MDD% | 2025 Ret% | 2025 Sharpe | 2025 MDD% |\n')
        f.write('|---|---|---|---|---|---|---|\n')
        for leg_name, leg_col in [('Value only', 'val_ret'), ('QM only', 'qm_ret'), ('Combined 50/50', 'combined_ret')]:
            r24 = all_annual[(all_annual['leg'] == leg_col) & (all_annual['year'] == 2024)]
            r25 = all_annual[(all_annual['leg'] == leg_col) & (all_annual['year'] == 2025)]
            if len(r24) and len(r25):
                f.write(f'| **{leg_name}** | '
                        f'{r24["ann_ret_pct"].iloc[0]} | {r24["sharpe"].iloc[0]} | {r24["mdd_pct"].iloc[0]} | '
                        f'{r25["ann_ret_pct"].iloc[0]} | {r25["sharpe"].iloc[0]} | {r25["mdd_pct"].iloc[0]} |\n')

        f.write('\n**TWII 對照**：\n')
        for _, row in twii_ann[twii_ann['year'].isin([2024, 2025])].iterrows():
            f.write(f'- {int(row["year"])}: TWII {row["twii_ret_pct"]:+.2f}% / MDD {row["twii_mdd_pct"]:.2f}%\n')

        f.write('\n## 2. 全年度分解 (2016-2025, 3 legs)\n\n')
        pivot = all_annual.pivot_table(index='year', columns='leg',
                                         values='ann_ret_pct', aggfunc='first')
        pivot.columns = [c.replace('_ret', '') for c in pivot.columns]
        f.write('### Annual Return %\n')
        f.write(pivot.to_string() + '\n\n')

        sharpe_pivot = all_annual.pivot_table(index='year', columns='leg',
                                                values='sharpe', aggfunc='first')
        sharpe_pivot.columns = [c.replace('_ret', '') for c in sharpe_pivot.columns]
        f.write('### Annual Sharpe\n')
        f.write(sharpe_pivot.to_string() + '\n\n')

        # TWII comparison
        f.write('### TWII 對照（全期）\n')
        f.write(twii_ann.to_string(index=False) + '\n\n')

        f.write('\n## 3. Step D 分流決定\n\n')
        if verdict.startswith('C'):
            f.write('✅ **Verdict C (SAFE_SHIP)**: tv_top_25 crowding 獨有，Dual-all 腿健康 → **進 Step D 更新 anchor 文件**，live code 改 0 行。\n')
        elif verdict.startswith('A'):
            f.write('⏸️ **Verdict A (VALUE FACTOR DECAY)**: Value 腿 2yr Sharpe < 0.3，QM 腿正常 → **DELAY ship**，回 brainstorm 重驗 Value factor（優先跑宋分毛利 Δ / ROIC IC）。\n')
        elif verdict.startswith('D'):
            f.write('⏸️ **Verdict D (QM DECAY)**: QM 腿 2yr Sharpe < 0.3，意外情境 → **DELAY ship**，回 brainstorm 重驗 QM 動能 factor。\n')
        else:
            f.write('⚠️ **Verdict B (REGIME SHIFT)**: 雙腿雙崩 → 整個選股框架對 0050 失效疑慮。**仍可 ship Dual-all** 作 baseline，但降低預期 CAGR 到 5-10% 範圍，flag 結構性風險需觀察。\n')

    print(f'MD:  {OUT_MD.relative_to(ROOT)}')
    print(f'\n== VERDICT: {verdict} ==')


if __name__ == '__main__':
    main()
