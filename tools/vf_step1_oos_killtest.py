"""
Step B — 2016-2020 OOS kill-test + annual decomposition

Council R2 verdict: 新 champion (Dual + tv_top_25 + only_volatile + top_20)
在 2020-2025 樣本有 QE + AI 多頭偏差，必須驗 2016-2019 OOS 活不活著。

Kill-test pass criteria:
  (a) 2016-2019 OOS 區段 Sharpe > 0.8
  (b) 2022 熊年 MDD 不破 -40%
  (c) tv_top_25 相對 tv_top_50 / tv_top_75 / all 排名不翻盤

資料：
  trade_journal_value_tw_long_snapshot.parquet (2016-01 → 2025-12, 878 stocks)
  trade_journal_qm_tw.parquet (2015-07 → 2025-12, 205 stocks)

Output:
  reports/vf_step1_oos_killtest.csv + .md
"""
import numpy as np
import pandas as pd
from pathlib import Path

from vf_step1_dual_mcap_grid import (
    QM_SNAPSHOT, TWII_BENCH,
    apply_stage1, compute_live_score, add_tv_rank, load_twii,
    backtest_dual, compute_metrics, ROOT, MCAP_CUTOFFS,
)

LONG_SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_value_tw_long_snapshot.parquet'
OUT_CSV = ROOT / 'reports/vf_step1_oos_killtest.csv'
OUT_MD = ROOT / 'reports/vf_step1_oos_killtest.md'


def annual_decompose(pr):
    """年度 CAGR / Sharpe / MDD 分解。"""
    if len(pr) == 0:
        return pd.DataFrame()
    pr = pr.copy()
    pr['year'] = pd.to_datetime(pr['date']).dt.year
    rows = []
    for yr, g in pr.groupby('year'):
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
            'n_rebal': len(g),
            'ann_ret_pct': round(ann_ret * 100, 2),
            'sharpe': round(sharpe, 3) if not np.isnan(sharpe) else None,
            'mdd_pct': round(mdd * 100, 2),
            'hit': round((g['ret'] > 0).mean() * 100, 1),
        })
    return pd.DataFrame(rows)


def run_subset_metrics(pr, start, end, label):
    """擷取 pr 在 [start, end] 之間計算 metrics。"""
    mask = (pr['date'] >= pd.Timestamp(start)) & (pr['date'] <= pd.Timestamp(end))
    sub = pr[mask].copy().reset_index(drop=True)
    if len(sub) == 0:
        return None
    # Recompute cumulative from 1.0
    sub['cum_ret'] = (1 + sub['ret']).cumprod()
    m = compute_metrics(sub)
    m['label'] = label
    return m


def main():
    print('=== Step B — OOS Kill-test (2016-2025) ===')

    # Load long snapshot
    df = pd.read_parquet(LONG_SNAPSHOT)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    print(f'Value universe (long): {len(df)} rows, {df["stock_id"].nunique()} stocks, '
          f'{df["week_end_date"].min().date()} -> {df["week_end_date"].max().date()}')

    # Fix long snapshot PB scale bug (PB systematically 10x of short snapshot, verified on 2330)
    df['pb'] = df['pb'] / 10.0
    print('Applied PB /10 scale fix (long snapshot has PB x10 bug vs live snapshot)')

    stage1 = apply_stage1(df)
    stage1['v_score_live'] = compute_live_score(stage1)
    stage1 = add_tv_rank(stage1)
    print(f'After Stage 1: {len(stage1)} rows')

    qm_df = pd.read_parquet(QM_SNAPSHOT)
    qm_df['week_end_date'] = pd.to_datetime(qm_df['week_end_date'])
    qm_df = qm_df[qm_df['week_end_date'] >= df['week_end_date'].min()]
    print(f'QM universe: {len(qm_df)} rows, {qm_df["week_end_date"].nunique()} weeks')

    twii_close = load_twii()

    # Run grid on full 2016-2025 (QM only covers 2015-07+ which includes 2016)
    print('\n-- Running mcap × regime grid on FULL 2016-2025 --')

    grid_results = []
    all_pr = {}
    for mcap_name in MCAP_CUTOFFS.keys():
        for regime_mode in ['only_volatile', 'none']:
            label = f'Dual-{mcap_name}-{regime_mode}'
            pr = backtest_dual(stage1, qm_df, twii_close,
                               mcap_cutoff_name=mcap_name,
                               regime_mode=regime_mode,
                               top_n_value=20,
                               top_n_qm=20)
            m = compute_metrics(pr)
            m['label'] = label
            m['mcap'] = mcap_name
            m['regime'] = regime_mode
            m['period'] = 'FULL_2016_2025'
            grid_results.append(m)
            all_pr[label] = pr
            print(f'  {label}: CAGR={m["cagr_pct"]}% Sharpe={m["sharpe"]} MDD={m["mdd_pct"]}% '
                  f'hit={m["hit_rate"]}%')

    # ---- OOS sub-period metrics ----
    print('\n-- OOS period breakdown --')
    periods = {
        'OOS_2016_2019':  ('2016-01-01', '2019-12-31'),
        'IS_2020_2025':   ('2020-01-01', '2025-12-31'),
        'BEAR_2022':      ('2022-01-01', '2022-12-31'),
        'BEAR_2018_Q4':   ('2018-10-01', '2019-03-31'),  # 2018 Q4 貿易戰
    }
    period_rows = []
    for label, pr in all_pr.items():
        for pname, (start, end) in periods.items():
            m = run_subset_metrics(pr, start, end, label)
            if m is None:
                continue
            m['period'] = pname
            m['mcap'] = label.split('-')[1]
            m['regime'] = label.split('-')[-1]
            period_rows.append(m)

    period_df = pd.DataFrame(period_rows)

    # ---- Annual decomp for champion only ----
    champion_label = 'Dual-tv_top_25-only_volatile'
    print(f'\n-- Annual decomposition for {champion_label} --')
    champion_pr = all_pr[champion_label]
    annual_df = annual_decompose(champion_pr)
    print(annual_df.to_string(index=False))

    # ---- Save ----
    full_df = pd.DataFrame(grid_results)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    full_df.to_csv(OUT_CSV, index=False)
    period_df.to_csv(OUT_CSV.parent / 'vf_step1_oos_period_breakdown.csv', index=False)
    annual_df.to_csv(OUT_CSV.parent / 'vf_step1_oos_champion_annual.csv', index=False)

    # ---- MD report ----
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Step B — OOS Kill-test (2016-2025)\n\n')
        f.write('**Council R2 criteria**: 新 champion `Dual + tv_top_25 + only_volatile + top_20` '
                '必須在 2016-2019 OOS Sharpe > 0.8、2022 MDD 不破 -40%、排名相對其他 mcap 不翻盤。\n\n')
        f.write('**資料延伸**: 從 `trade_journal_value_tw_long_snapshot.parquet` (2016-01 起)\n\n')
        f.write('## 1. FULL 2016-2025 grid（排序 by Sharpe）\n\n')
        full_sorted = full_df.sort_values('sharpe', ascending=False, na_position='last')
        f.write('| strategy | mcap | regime | CAGR% | Sharpe | MDD% | hit% | v_on% | n |\n')
        f.write('|---|---|---|---|---|---|---|---|---|\n')
        for _, r in full_sorted.iterrows():
            f.write(f'| {r["label"]} | {r["mcap"]} | {r["regime"]} | '
                    f'{r["cagr_pct"]} | {r["sharpe"]} | {r["mdd_pct"]} | '
                    f'{r["hit_rate"]} | {r["v_on_pct"]} | {r["n_rebal"]} |\n')

        f.write('\n## 2. Period breakdown (OOS vs IS vs Bear)\n\n')
        pivot = period_df.pivot_table(
            index=['mcap', 'regime'],
            columns='period',
            values='cagr_pct',
            aggfunc='first',
        )
        f.write('### CAGR% by period\n\n')
        f.write(pivot.to_string() + '\n\n')

        pivot_sharpe = period_df.pivot_table(
            index=['mcap', 'regime'],
            columns='period',
            values='sharpe',
            aggfunc='first',
        )
        f.write('### Sharpe by period\n\n')
        f.write(pivot_sharpe.to_string() + '\n\n')

        pivot_mdd = period_df.pivot_table(
            index=['mcap', 'regime'],
            columns='period',
            values='mdd_pct',
            aggfunc='first',
        )
        f.write('### MDD% by period\n\n')
        f.write(pivot_mdd.to_string() + '\n\n')

        f.write(f'\n## 3. Champion (`{champion_label}`) 年度分解\n\n')
        f.write(annual_df.to_string(index=False) + '\n\n')

        # Pass/fail criteria
        champion_full = full_df[full_df['label'] == champion_label].iloc[0]
        champion_oos = period_df[
            (period_df['label'] == champion_label) & (period_df['period'] == 'OOS_2016_2019')
        ]
        champion_2022 = period_df[
            (period_df['label'] == champion_label) & (period_df['period'] == 'BEAR_2022')
        ]

        f.write('## 4. Pass/Fail Verdict\n\n')
        oos_sharpe = champion_oos['sharpe'].iloc[0] if len(champion_oos) > 0 else None
        mdd_2022 = champion_2022['mdd_pct'].iloc[0] if len(champion_2022) > 0 else None

        f.write('| Criterion | Target | Actual | Pass? |\n')
        f.write('|---|---|---|---|\n')
        f.write(f'| 2016-2019 OOS Sharpe | > 0.8 | {oos_sharpe} | '
                f'{"✅" if oos_sharpe and oos_sharpe > 0.8 else "❌"} |\n')
        f.write(f'| 2022 MDD | > -40% | {mdd_2022}% | '
                f'{"✅" if mdd_2022 and mdd_2022 > -40 else "❌"} |\n')

        # Ranking check
        champion_rank = full_sorted.reset_index()[full_sorted.reset_index()['label'] == champion_label].index
        rank = champion_rank[0] + 1 if len(champion_rank) > 0 else None
        f.write(f'| 10-strategy Sharpe rank | Top 3 | #{rank} | '
                f'{"✅" if rank and rank <= 3 else "❌"} |\n')

    print(f'\nCSV saved: {OUT_CSV.relative_to(ROOT)}')
    print(f'MD saved: {OUT_MD.relative_to(ROOT)}')
    print(f'Period breakdown: reports/vf_step1_oos_period_breakdown.csv')
    print(f'Champion annual: reports/vf_step1_oos_champion_annual.csv')


if __name__ == '__main__':
    main()
