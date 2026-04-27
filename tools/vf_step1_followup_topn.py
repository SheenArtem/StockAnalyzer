"""
Step 1 Follow-up: Concentration grid on new champion

回答：新 champion (Dual + tv_top_25 + only_volatile) 濃縮持股能否更好？

Grid: tv_top_25 + only_volatile × top_n {3, 5, 10, 20} = 4 combos
      （top_n 同時套用 Value 側和 QM 側）

Output: reports/vf_step1_followup_topn.csv + .md
"""
import pandas as pd
from pathlib import Path

# Reuse from main grid script
from vf_step1_dual_mcap_grid import (
    SNAPSHOT, QM_SNAPSHOT, TWII_BENCH,
    apply_stage1, compute_live_score, add_tv_rank, load_twii,
    backtest_dual, compute_metrics, ROOT,
)

OUT_CSV = ROOT / 'reports/vf_step1_followup_topn.csv'
OUT_MD = ROOT / 'reports/vf_step1_followup_topn.md'

TOP_N_GRID = [3, 5, 10, 20]


def main():
    print('=== Step 1 Follow-up — tv_top_25 + only_volatile × top_n grid ===')

    df = pd.read_parquet(SNAPSHOT)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    stage1 = apply_stage1(df)
    stage1['v_score_live'] = compute_live_score(stage1)
    stage1 = add_tv_rank(stage1)

    qm_df = pd.read_parquet(QM_SNAPSHOT)
    qm_df['week_end_date'] = pd.to_datetime(qm_df['week_end_date'])
    qm_df = qm_df[qm_df['week_end_date'] >= df['week_end_date'].min()]

    twii_close = load_twii()

    results = []
    for n in TOP_N_GRID:
        print(f'\n-- top_n={n} (both sides) --')
        pr = backtest_dual(
            stage1, qm_df, twii_close,
            mcap_cutoff_name='tv_top_25',
            regime_mode='only_volatile',
            top_n_value=n,
            top_n_qm=n,
        )
        m = compute_metrics(pr)
        m['top_n'] = n
        m['total_max_holdings'] = n * 2
        results.append(m)
        cagr = m.get('cagr_pct')
        print(f'  CAGR={cagr}% Sharpe={m["sharpe"]} MDD={m["mdd_pct"]}% '
              f'hit={m["hit_rate"]}% v_on={m["v_on_pct"]}% '
              f'(max holdings: {n*2} volatile / {n} non-volatile)')

    res_df = pd.DataFrame(results)
    cols = ['top_n', 'total_max_holdings', 'cagr_pct', 'sharpe',
            'mdd_pct', 'vol_pct', 'hit_rate', 'v_on_pct', 'n_rebal']
    res_df = res_df[cols]
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    res_df.to_csv(OUT_CSV, index=False)

    # MD
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Step 1 Follow-up — Dual + tv_top_25 + only_volatile × top_n grid\n\n')
        f.write('**基準**: Main grid 已確認 mcap=tv_top_25 + regime=only_volatile 為 Sharpe 冠軍 (1.242)。\n\n')
        f.write('**問題**: 濃縮持股（top_n=3/5）能否再拉高 CAGR？MDD 會不會爆？\n\n')
        f.write('**Spec**: top_n 同時套用 Value 側與 QM 側；max holdings = top_n × 2 (volatile 週) / top_n (非 volatile 週)\n\n')
        f.write('## 結果（排序 by Sharpe）\n\n')
        sorted_df = res_df.sort_values('sharpe', ascending=False, na_position='last')
        f.write('| top_n | max holdings (V/非V) | CAGR% | Sharpe | MDD% | Vol% | hit% | v_on% | n |\n')
        f.write('|---|---|---|---|---|---|---|---|---|\n')
        for _, r in sorted_df.iterrows():
            f.write(f'| {r["top_n"]} | {r["total_max_holdings"]}/{r["top_n"]} | '
                    f'{r["cagr_pct"]} | {r["sharpe"]} | {r["mdd_pct"]} | '
                    f'{r["vol_pct"]} | {r["hit_rate"]} | {r["v_on_pct"]} | {r["n_rebal"]} |\n')
        f.write('\n## Caveats\n\n')
        f.write('- 無交易成本；top_n 越小 turnover 越集中，實務 slippage 影響更大\n')
        f.write('- top_n=3 可能遇到 Value pool 在某週 <3 檔 → 自動降檔不補\n')
        f.write('- MDD 在 top_n 小時通常放大（單檔爆雷 weight 高）\n')

    print(f'\nCSV: {OUT_CSV.relative_to(ROOT)}')
    print(f'MD:  {OUT_MD.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
