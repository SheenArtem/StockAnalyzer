"""
Step 1 — Dual 50/50 + Market Cap Filter × Regime Grid (council 2026-04-24)

目的：釐清 T1 tension（市值 filter 機械排除中小型動能股 vs 安全性）
      以及 Baseline A (Value+only_volatile top_5) 是否真的比 Dual 50/50 好。

Grid: 4 mcap cutoffs × 2 regime modes = 8 combinations
  mcap: tv_top_25 / tv_top_50 / tv_top_75 / all (proxy for 市值 rank 用 avg_tv_60d
        cross-sectional percentile per week; 因 snapshot 無 raw market cap)
  regime: only_volatile / none  (Value 側是否受 volatility gate)

外加 reference rows:
  - Baseline A: Value+only_volatile top_5 (council 既有 anchor)
  - Dual 50/50 no-filter, no regime (既有 memory 17.45% 基準)

Spec:
  - 數據: trade_journal_value_tw_snapshot.parquet (309 週 × 857 檔) + trade_journal_qm_tw.parquet
  - 配置: 月頻 rebalance (4 週)，top_20 each side for Dual / top_5 for Baseline A
  - Return: fwd_20d (PIT-safe)
  - Stage 1 filter: PE 0~12 / PB <=3 / PE*PB <=22.5 / TV >= 30M (live Value config)
  - tv_rank 計算: stage1 pool 內 avg_tv_60d 的 percentile rank per week

Output:
  reports/vf_step1_dual_mcap_grid.csv + .md
"""
import argparse
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_value_tw_snapshot.parquet'
QM_SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_qm_tw.parquet'
TWII_BENCH = ROOT / 'data_cache/backtest/_twii_bench.parquet'
OUT_CSV = ROOT / 'reports/vf_step1_dual_mcap_grid.csv'
OUT_MD = ROOT / 'reports/vf_step1_dual_mcap_grid.md'

# Live Value config
MAX_PE = 12
MAX_PB = 3.0
PE_X_PB_MAX = 22.5
MIN_TV = 3e7
WEIGHTS = {'val': 0.30, 'quality': 0.25, 'revenue': 0.30, 'technical': 0.15, 'sm': 0.00}
REBALANCE_EVERY = 4

MCAP_CUTOFFS = {
    'tv_top_25': 0.25,   # top 25% by tv (最嚴，~200 檔)
    'tv_top_50': 0.50,   # top 50% (~425 檔)
    'tv_top_75': 0.75,   # top 75% (~640 檔) ~接近 council 市值前 600 原意
    'all':       1.00,   # 無 filter (~857 檔)
}


def apply_stage1(df):
    mask = (df['pe'] > 0) & (df['pe'] <= MAX_PE)
    pb_pass = df['pb'].isna() | (df['pb'] <= MAX_PB)
    graham_pass = df['pb'].isna() | ((df['pe'] * df['pb']) <= PE_X_PB_MAX)
    tv_pass = df['avg_tv_60d'].fillna(0) >= MIN_TV
    return df[mask & pb_pass & graham_pass & tv_pass].copy()


def compute_live_score(df):
    return (
        WEIGHTS['val'] * df['valuation_s'] +
        WEIGHTS['quality'] * df['quality_s'] +
        WEIGHTS['revenue'] * df['revenue_s'] +
        WEIGHTS['technical'] * df['technical_s'] +
        WEIGHTS['sm'] * df['smart_money_s']
    )


def add_tv_rank(df):
    """Weekly cross-sectional tv_60d percentile rank."""
    df = df.copy()
    df['tv_pct'] = df.groupby('week_end_date')['avg_tv_60d'].rank(pct=True, ascending=False)
    # tv_pct = 0.0 (top) ... 1.0 (bottom)
    return df


def load_twii():
    df = pd.read_parquet(TWII_BENCH)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    return df['Close']


def classify_regime_at(date, twii_close):
    idx = twii_close.index.searchsorted(date, side='right') - 1
    if idx < 20:
        return 'neutral'
    window = twii_close.iloc[idx - 20:idx + 1]
    p0, p1 = float(window.iloc[0]), float(window.iloc[-1])
    ret20 = (p1 / p0) - 1
    wmax, wmin, wavg = float(window.max()), float(window.min()), float(window.mean())
    rng20 = (wmax - wmin) / wavg if wavg > 0 else 0
    if rng20 > 0.08:
        return 'volatile'
    if ret20 > 0.05:
        return 'trending'
    if abs(ret20) < 0.02 and rng20 <= 0.08:
        return 'ranging'
    return 'neutral'


def backtest_dual(stage1_ranked, qm_df, twii_close,
                  mcap_cutoff_name, regime_mode,
                  top_n_value=20, top_n_qm=20):
    """Dual 50/50: 50% Value (可選 regime gate) + 50% QM always.

    regime_mode='only_volatile': Value 側僅 volatile 週出場，其他週 Value=0
    regime_mode='none':          Value 側 always 出場（全時段 50/50 mix）
    """
    mcap_pct = MCAP_CUTOFFS[mcap_cutoff_name]
    stage1_ranked = stage1_ranked[stage1_ranked['tv_pct'] <= mcap_pct].copy()

    weeks = sorted(stage1_ranked['week_end_date'].unique())
    rebalance_weeks = weeks[::REBALANCE_EVERY]

    rows = []
    for wk in rebalance_weeks:
        regime = classify_regime_at(wk, twii_close)
        value_on = (regime_mode == 'none') or (regime == 'volatile')

        # Value side
        val_ret = 0.0
        v_n = 0
        if value_on:
            vpool = stage1_ranked[stage1_ranked['week_end_date'] == wk]
            if not vpool.empty:
                vtop = vpool.nlargest(min(top_n_value, len(vpool)), 'v_score_live')
                val_ret = vtop['fwd_20d'].mean()
                v_n = len(vtop)

        # QM side (always on in Dual 50/50)
        qm_ret = 0.0
        q_n = 0
        if qm_df is not None:
            qpool = qm_df[qm_df['week_end_date'] == wk]
            if not qpool.empty:
                qtop = qpool[qpool['rank_in_top50'] <= top_n_qm]
                qm_ret = qtop['fwd_20d'].mean()
                q_n = len(qtop)

        port_ret = 0.5 * val_ret + 0.5 * qm_ret
        rows.append({
            'date': wk, 'ret': port_ret, 'regime': regime,
            'v_on': value_on, 'v_n': v_n, 'q_n': q_n,
            'v_ret': val_ret, 'q_ret': qm_ret,
        })

    pr = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    pr['cum_ret'] = (1 + pr['ret']).cumprod()
    return pr


def backtest_baseline_a(stage1_ranked, twii_close, top_n=5, mcap_cutoff_name='all'):
    """Value+only_volatile top_N reference."""
    mcap_pct = MCAP_CUTOFFS[mcap_cutoff_name]
    stage1_ranked = stage1_ranked[stage1_ranked['tv_pct'] <= mcap_pct].copy()

    weeks = sorted(stage1_ranked['week_end_date'].unique())
    rebalance_weeks = weeks[::REBALANCE_EVERY]

    rows = []
    for wk in rebalance_weeks:
        regime = classify_regime_at(wk, twii_close)
        if regime != 'volatile':
            rows.append({'date': wk, 'ret': 0.0, 'regime': regime, 'in_market': False, 'n': 0})
            continue
        pool = stage1_ranked[stage1_ranked['week_end_date'] == wk]
        if pool.empty:
            rows.append({'date': wk, 'ret': 0.0, 'regime': regime, 'in_market': False, 'n': 0})
            continue
        top = pool.nlargest(min(top_n, len(pool)), 'v_score_live')
        rows.append({
            'date': wk, 'ret': top['fwd_20d'].mean(),
            'regime': regime, 'in_market': True, 'n': len(top),
        })
    pr = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    pr['cum_ret'] = (1 + pr['ret']).cumprod()
    return pr


def compute_metrics(pr, periods_per_year=13):
    if len(pr) == 0:
        return {}
    n_years = (pr['date'].iloc[-1] - pr['date'].iloc[0]).days / 365.25
    cum = pr['cum_ret'].iloc[-1]
    if cum <= 0 or np.isnan(cum):
        cagr = np.nan
    else:
        cagr = cum ** (1 / n_years) - 1
    vol = pr['ret'].std() * np.sqrt(periods_per_year)
    rf = 0.01
    sharpe = (cagr - rf) / vol if vol > 0 and not np.isnan(cagr) else np.nan
    rolling_max = pr['cum_ret'].cummax()
    dd = (pr['cum_ret'] - rolling_max) / rolling_max
    mdd = dd.min()
    hit_rate = (pr['ret'] > 0).mean()
    in_mkt = pr.get('in_market', pd.Series([True]*len(pr)))
    if 'v_on' in pr.columns:
        # Dual 模式 Value 側出場比例
        v_on_pct = pr['v_on'].mean() if 'v_on' in pr else 1.0
    else:
        v_on_pct = in_mkt.mean()
    return {
        'n_rebal': len(pr),
        'cagr_pct': round(cagr * 100, 2) if not np.isnan(cagr) else None,
        'vol_pct': round(vol * 100, 2),
        'sharpe': round(sharpe, 3) if not np.isnan(sharpe) else None,
        'mdd_pct': round(mdd * 100, 2),
        'hit_rate': round(hit_rate * 100, 1),
        'v_on_pct': round(v_on_pct * 100, 1),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--top-n-value', type=int, default=20)
    ap.add_argument('--top-n-qm', type=int, default=20)
    ap.add_argument('--top-n-baseline-a', type=int, default=5)
    args = ap.parse_args()

    print(f'=== Step 1 — Dual 50/50 + Market Cap Filter × Regime Grid ===')
    print(f'Top N: value={args.top_n_value}, qm={args.top_n_qm}, baseline_A={args.top_n_baseline_a}')

    # Load data
    df = pd.read_parquet(SNAPSHOT)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    stage1 = apply_stage1(df)
    stage1['v_score_live'] = compute_live_score(stage1)
    stage1 = add_tv_rank(stage1)
    print(f'Value universe: {len(stage1)} rows after Stage 1')

    qm_df = pd.read_parquet(QM_SNAPSHOT)
    qm_df['week_end_date'] = pd.to_datetime(qm_df['week_end_date'])
    qm_df = qm_df[qm_df['week_end_date'] >= df['week_end_date'].min()]
    print(f'QM universe: {len(qm_df)} picks, {qm_df["week_end_date"].nunique()} weeks')

    twii_close = load_twii()

    # Run grid
    results = []

    # Main grid: 4 mcap × 2 regime = 8 cells
    for mcap_name in MCAP_CUTOFFS.keys():
        for regime_mode in ['only_volatile', 'none']:
            print(f'\n-- Dual mcap={mcap_name} regime={regime_mode} --')
            pr = backtest_dual(stage1, qm_df, twii_close,
                               mcap_cutoff_name=mcap_name,
                               regime_mode=regime_mode,
                               top_n_value=args.top_n_value,
                               top_n_qm=args.top_n_qm)
            m = compute_metrics(pr)
            m['strategy'] = f'Dual-{mcap_name}-{regime_mode}'
            m['mcap'] = mcap_name
            m['regime'] = regime_mode
            m['top_n'] = args.top_n_value
            results.append(m)
            print(f'  CAGR={m["cagr_pct"]}% Sharpe={m["sharpe"]} MDD={m["mdd_pct"]}% hit={m["hit_rate"]}% v_on={m["v_on_pct"]}%')

    # Reference: Baseline A (Value+only_volatile top_5, no mcap filter)
    print(f'\n-- Reference: Baseline A (Value+only_volatile top_{args.top_n_baseline_a}, no mcap filter) --')
    pr_a = backtest_baseline_a(stage1, twii_close,
                                top_n=args.top_n_baseline_a,
                                mcap_cutoff_name='all')
    m_a = compute_metrics(pr_a)
    m_a['strategy'] = f'Baseline-A_top{args.top_n_baseline_a}'
    m_a['mcap'] = 'all'
    m_a['regime'] = 'only_volatile'
    m_a['top_n'] = args.top_n_baseline_a
    results.append(m_a)
    print(f'  CAGR={m_a["cagr_pct"]}% Sharpe={m_a["sharpe"]} MDD={m_a["mdd_pct"]}% hit={m_a["hit_rate"]}% in_mkt={m_a["v_on_pct"]}%')

    # Reference: Baseline A + tv_top_75 filter (看 mcap filter 對 top_5 影響)
    print(f'\n-- Reference: Baseline A + tv_top_75 filter --')
    pr_a75 = backtest_baseline_a(stage1, twii_close,
                                  top_n=args.top_n_baseline_a,
                                  mcap_cutoff_name='tv_top_75')
    m_a75 = compute_metrics(pr_a75)
    m_a75['strategy'] = f'Baseline-A_top{args.top_n_baseline_a}_tv_top_75'
    m_a75['mcap'] = 'tv_top_75'
    m_a75['regime'] = 'only_volatile'
    m_a75['top_n'] = args.top_n_baseline_a
    results.append(m_a75)
    print(f'  CAGR={m_a75["cagr_pct"]}% Sharpe={m_a75["sharpe"]} MDD={m_a75["mdd_pct"]}% hit={m_a75["hit_rate"]}% in_mkt={m_a75["v_on_pct"]}%')

    # Save
    res_df = pd.DataFrame(results)
    col_order = ['strategy', 'mcap', 'regime', 'top_n',
                 'cagr_pct', 'sharpe', 'mdd_pct', 'vol_pct', 'hit_rate', 'v_on_pct', 'n_rebal']
    res_df = res_df[col_order]
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    res_df.to_csv(OUT_CSV, index=False)
    print(f'\nCSV saved: {OUT_CSV.relative_to(ROOT)}')

    # MD report
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Step 1 — Dual 50/50 + Market Cap × Regime Grid\n\n')
        f.write('**Date range**: 2020-01-03 -> 2025-12-26 (309 weeks, 78 rebalances @ 4-week)\n\n')
        f.write('**Council verdict 2026-04-24**: 鎖 universe 先行，釐清 T1 tension (市值 filter 是否傷害 alpha)'
                ' + Baseline A vs Dual 50/50 anchor 之爭。\n\n')
        f.write(f'**Top N**: Value={args.top_n_value}, QM={args.top_n_qm}, Baseline A={args.top_n_baseline_a}\n\n')
        f.write('**Mcap proxy**: `avg_tv_60d` weekly cross-sectional percentile rank '
                '(snapshot 內無 raw market cap；tv 與 mcap 高相關但非 1:1)。\n\n')
        f.write('**Stage 1**: PE 0~12 / PB <=3 / PE*PB <=22.5 / TV >=30M (live Value 設定)\n\n')
        f.write('## 結果表（排序：Sharpe 由高至低）\n\n')
        sorted_df = res_df.sort_values('sharpe', ascending=False, na_position='last')
        f.write('| strategy | mcap | regime | top_n | CAGR% | Sharpe | MDD% | Vol% | hit% | v_on% | n |\n')
        f.write('|---|---|---|---|---|---|---|---|---|---|---|\n')
        for _, r in sorted_df.iterrows():
            f.write(f'| {r["strategy"]} | {r["mcap"]} | {r["regime"]} | {r["top_n"]} | '
                    f'{r["cagr_pct"]} | {r["sharpe"]} | {r["mdd_pct"]} | {r["vol_pct"]} | '
                    f'{r["hit_rate"]} | {r["v_on_pct"]} | {r["n_rebal"]} |\n')
        f.write('\n## Caveats\n\n')
        f.write('- `market_cap_tier` via tv_60d percentile = **liquidity proxy**, not真 market cap\n')
        f.write('- 無交易成本扣除 (round-trip ~0.3% × 13 rebal/yr ≈ 4pp CAGR)\n')
        f.write('- 無股息再投入\n')
        f.write('- Sample 2020-2025 (QE + AI 雙牛市)，out-of-sample 2000/2008 regime 未測\n')
        f.write('- Baseline A in-market % 由 volatility regime 決定，非 100% exposure\n')
    print(f'MD saved: {OUT_MD.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
