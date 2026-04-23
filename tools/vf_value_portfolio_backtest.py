"""
Value Screener 真實 portfolio 回測（2020-2025）

目的：把 IC-level validation 翻譯成 portfolio-level 實際報酬數字，回答
使用者「買 Value top 20 能期待什麼報酬？」的問題。

規格：
- Universe：trade_journal_value_tw_snapshot.parquet (309 週 × 857 檔)
- Stage 1 Filter：PE 0~12, PB <=3, PE*PB <=22.5, avg_tv_60d >= 30M (live config)
- Scoring：current live weights 30/25/30/15/0 (V/Q/R/T/SM)
- 配置：月頻 rebalance (每 4 週)，top 20 equal-weight
- Return：使用 snapshot 內建 fwd_20d（PIT-safe, 已含 Taiwan 手續費 transaction cost 未列）
- Benchmark：TWII (^TWII via data_cache/backtest/_twii_bench.parquet)
- Regime filter (optional)：only_volatile → 非 volatile 週持現金（ret=0）
  TWII 4-regime 分類 (對齊 market_regime_logger.py / VF-G4 A 級):
    volatile: range_20d > 8%
    trending: ret_20d > 5%
    ranging:  |ret_20d| < 2% AND range_20d <= 8%
    neutral:  otherwise

輸出：
- reports/vf_value_portfolio_backtest[_regime].md
- reports/vf_value_portfolio_backtest[_regime].csv
- reports/vf_value_portfolio_annual[_regime].csv

Usage:
    python tools/vf_value_portfolio_backtest.py                      # baseline (no filter)
    python tools/vf_value_portfolio_backtest.py --regime only_volatile
"""

import argparse
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_value_tw_snapshot.parquet'
QM_SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_qm_tw.parquet'
TWII_BENCH = ROOT / 'data_cache/backtest/_twii_bench.parquet'
OUT_MD = ROOT / 'reports/vf_value_portfolio_backtest.md'
OUT_CSV = ROOT / 'reports/vf_value_portfolio_backtest.csv'
OUT_ANNUAL = ROOT / 'reports/vf_value_portfolio_annual.csv'

# Live config (value_screener.DEFAULT_CONFIG as of 2026-04-23)
MAX_PE = 12
MAX_PB = 3.0
PE_X_PB_MAX = 22.5
MIN_TV = 3e7
WEIGHTS = {'val': 0.30, 'quality': 0.25, 'revenue': 0.30, 'technical': 0.15, 'sm': 0.00}
TOP_N = 20
REBALANCE_EVERY = 4   # weeks


def load_data():
    df = pd.read_parquet(SNAPSHOT)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    return df


def apply_stage1(df):
    """Stage 1 initial filter matching live value_screener."""
    mask = (df['pe'] > 0) & (df['pe'] <= MAX_PE)
    pb_pass = df['pb'].isna() | (df['pb'] <= MAX_PB)
    graham_pass = df['pb'].isna() | ((df['pe'] * df['pb']) <= PE_X_PB_MAX)
    tv_pass = df['avg_tv_60d'].fillna(0) >= MIN_TV
    return df[mask & pb_pass & graham_pass & tv_pass].copy()


def compute_live_score(df):
    """Recompute value_score using current live weights."""
    return (
        WEIGHTS['val'] * df['valuation_s'] +
        WEIGHTS['quality'] * df['quality_s'] +
        WEIGHTS['revenue'] * df['revenue_s'] +
        WEIGHTS['technical'] * df['technical_s'] +
        WEIGHTS['sm'] * df['smart_money_s']
    )


def load_twii_benchmark():
    df = pd.read_parquet(TWII_BENCH)
    # Flatten MultiIndex columns (yfinance multi-level)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    return df['Close']


def classify_regime_at(date, twii_close):
    """對齊 market_regime_logger 4-regime 規則，但用 TWII 做 proxy。

    volatile: range_20d > 8%       (VF-G4 only_volatile 目標)
    trending: ret_20d > 5%
    ranging:  |ret_20d| < 2% AND range_20d <= 8%
    neutral:  otherwise
    """
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


def backtest(df, regime_filter=None, twii_close=None, qm_df=None):
    """Monthly rebalance top-20, equal-weight, use fwd_20d.

    regime_filter='only_volatile':      非 volatile 週持現金
    regime_filter='qm_combo':           volatile → Value top-20, 否則 → QM top-20 (需 qm_df)
    regime_filter='qm_only':            忽略 Value，每週 QM top-20（baseline 對照）
    """
    stage1 = apply_stage1(df)
    stage1['v_score_live'] = compute_live_score(stage1)

    weeks = sorted(df['week_end_date'].unique())
    rebalance_weeks = weeks[::REBALANCE_EVERY]

    rows = []
    for wk in rebalance_weeks:
        regime = classify_regime_at(wk, twii_close) if twii_close is not None else 'neutral'

        # ---- regime filter 分流 ----
        use_value = False
        use_qm = False
        dual_mode = False

        if regime_filter is None:
            use_value = True
        elif regime_filter == 'only_volatile':
            use_value = (regime == 'volatile')
        elif regime_filter == 'qm_combo':
            if regime == 'volatile':
                use_value = True
            else:
                use_qm = True
        elif regime_filter == 'qm_only':
            use_qm = True
        elif regime_filter == 'dual_5050':
            # 50% Value+filter (volatile 時 value / 否則 cash) + 50% QM always
            dual_mode = True

        # Dual 5050: 混合 Value+filter 的 50% 與 QM 的 50% 平行持倉
        if dual_mode:
            val_ret = 0.0
            qm_ret = 0.0
            if regime == 'volatile':
                vpool = stage1[stage1['week_end_date'] == wk]
                if not vpool.empty:
                    vtop = vpool.nlargest(min(TOP_N, len(vpool)), 'v_score_live')
                    val_ret = vtop['fwd_20d'].mean()
            if qm_df is not None:
                qpool = qm_df[qm_df['week_end_date'] == wk]
                if not qpool.empty:
                    qtop = qpool[qpool['rank_in_top50'] <= TOP_N]
                    qm_ret = qtop['fwd_20d'].mean()
            # 50/50 混合
            port_ret = 0.5 * val_ret + 0.5 * qm_ret
            src = 'dual_both' if regime == 'volatile' else 'dual_qmonly'
            rows.append({
                'date': wk, 'ret': port_ret, 'n_stocks': 0,
                'regime': regime, 'source': src, 'in_market': True,
                'mean_pe': np.nan, 'mean_pb': np.nan, 'mean_fscore': np.nan,
            })
            continue

        if not (use_value or use_qm):
            rows.append({
                'date': wk, 'ret': 0.0, 'n_stocks': 0,
                'regime': regime, 'source': 'cash', 'in_market': False,
                'mean_pe': np.nan, 'mean_pb': np.nan, 'mean_fscore': np.nan,
            })
            continue

        if use_value:
            pool = stage1[stage1['week_end_date'] == wk]
            if len(pool) == 0:
                rows.append({
                    'date': wk, 'ret': 0.0, 'n_stocks': 0,
                    'regime': regime, 'source': 'value_empty', 'in_market': False,
                    'mean_pe': np.nan, 'mean_pb': np.nan, 'mean_fscore': np.nan,
                })
                continue
            top = pool.nlargest(min(TOP_N, len(pool)), 'v_score_live')
            port_ret = top['fwd_20d'].mean()
            rows.append({
                'date': wk, 'ret': port_ret, 'n_stocks': len(top),
                'regime': regime, 'source': 'value', 'in_market': True,
                'mean_pe': top['pe'].mean(), 'mean_pb': top['pb'].mean(),
                'mean_fscore': top['f_score'].mean(),
            })
        else:  # use_qm
            if qm_df is None:
                continue
            qm_pool = qm_df[qm_df['week_end_date'] == wk]
            if qm_pool.empty:
                # QM 該週無 pick（trigger 沒過任何股）→ 現金
                rows.append({
                    'date': wk, 'ret': 0.0, 'n_stocks': 0,
                    'regime': regime, 'source': 'qm_empty', 'in_market': False,
                    'mean_pe': np.nan, 'mean_pb': np.nan, 'mean_fscore': np.nan,
                })
                continue
            top_qm = qm_pool[qm_pool['rank_in_top50'] <= TOP_N]
            port_ret = top_qm['fwd_20d'].mean()
            rows.append({
                'date': wk, 'ret': port_ret, 'n_stocks': len(top_qm),
                'regime': regime, 'source': 'qm', 'in_market': True,
                'mean_pe': np.nan, 'mean_pb': np.nan,
                'mean_fscore': top_qm['f_score'].mean(),
            })

    pr = pd.DataFrame(rows).sort_values('date').reset_index(drop=True)
    pr['cum_ret'] = (1 + pr['ret']).cumprod()
    return pr


def compute_metrics(pr, periods_per_year=13):
    """Annualize using 13 rebalances per year."""
    if len(pr) == 0:
        return {}
    n_years = (pr['date'].iloc[-1] - pr['date'].iloc[0]).days / 365.25
    cagr = pr['cum_ret'].iloc[-1] ** (1 / n_years) - 1
    vol = pr['ret'].std() * np.sqrt(periods_per_year)
    # Risk-free ~ 1% TW (10Y 約 1.5-2%)
    rf = 0.01
    sharpe = (cagr - rf) / vol if vol > 0 else np.nan

    rolling_max = pr['cum_ret'].cummax()
    dd = (pr['cum_ret'] - rolling_max) / rolling_max
    mdd = dd.min()

    hit_rate = (pr['ret'] > 0).mean()
    return {
        'n_years': round(n_years, 2),
        'n_rebalances': len(pr),
        'cagr': round(cagr * 100, 2),
        'vol_annual': round(vol * 100, 2),
        'sharpe': round(sharpe, 3),
        'mdd': round(mdd * 100, 2),
        'hit_rate': round(hit_rate * 100, 1),
        'mean_ret_per_rebal': round(pr['ret'].mean() * 100, 2),
    }


def twii_metrics_aligned(pr, twii_close):
    """Compute TWII return over same rebalance dates for apples-to-apples."""
    # For each rebalance date, compute TWII return from date to date+20 trading days
    bench_rows = []
    dates = sorted(twii_close.index)
    dates_arr = np.array(dates)
    for _, row in pr.iterrows():
        d0 = row['date']
        # Nearest TWII date <= d0
        idx = np.searchsorted(dates_arr, d0, side='right') - 1
        if idx < 0 or idx + 20 >= len(dates_arr):
            continue
        p0 = twii_close.iloc[idx]
        p1 = twii_close.iloc[idx + 20]
        bench_rows.append({'date': d0, 'twii_ret': (p1 / p0) - 1})
    b = pd.DataFrame(bench_rows).sort_values('date').reset_index(drop=True)
    if b.empty:
        return pd.DataFrame(), {}
    b['twii_cum'] = (1 + b['twii_ret']).cumprod()
    n_years = (b['date'].iloc[-1] - b['date'].iloc[0]).days / 365.25
    cagr = b['twii_cum'].iloc[-1] ** (1 / n_years) - 1
    vol = b['twii_ret'].std() * np.sqrt(13)
    sharpe = (cagr - 0.01) / vol if vol > 0 else np.nan
    rolling = b['twii_cum'].cummax()
    mdd = ((b['twii_cum'] - rolling) / rolling).min()
    return b, {
        'n_years': round(n_years, 2),
        'cagr': round(cagr * 100, 2),
        'vol_annual': round(vol * 100, 2),
        'sharpe': round(sharpe, 3),
        'mdd': round(mdd * 100, 2),
    }


def annual_breakdown(pr, twii_df=None):
    pr['year'] = pr['date'].dt.year
    annual = pr.groupby('year').apply(
        lambda g: pd.Series({
            'value_ret': (1 + g['ret']).prod() - 1,
            'n_rebal': len(g),
            'hit_rate': (g['ret'] > 0).mean(),
        })
    )
    annual['value_ret_pct'] = (annual['value_ret'] * 100).round(2)
    if twii_df is not None and not twii_df.empty:
        twii_df = twii_df.copy()
        twii_df['year'] = twii_df['date'].dt.year
        twii_annual = twii_df.groupby('year')['twii_ret'].apply(lambda x: (1 + x).prod() - 1)
        annual['twii_ret_pct'] = (twii_annual * 100).round(2)
        annual['alpha_pct'] = (annual['value_ret_pct'] - annual['twii_ret_pct']).round(2)
    return annual


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--regime',
                    choices=['none', 'only_volatile', 'qm_combo', 'qm_only', 'dual_5050'],
                    default='none',
                    help='Regime filter mode (default: none = Value baseline). '
                         'dual_5050 = half Value+only_volatile + half QM always (parallel)')
    ap.add_argument('--top-n', type=int, default=None, help='Override TOP_N (default 20)')
    args = ap.parse_args()
    global TOP_N
    if args.top_n is not None:
        TOP_N = args.top_n
    suffix_topn = f'_top{TOP_N}' if args.top_n is not None else ''
    regime_filter = None if args.regime == 'none' else args.regime
    suffix = '' if regime_filter is None else f'_{regime_filter}'

    print(f'=== Value Portfolio Backtest (regime={args.regime}) ===')
    df = load_data()
    print(f'Universe: {len(df)} rows, {df["stock_id"].nunique()} stocks, '
          f'{df["week_end_date"].nunique()} weeks, '
          f'{df["week_end_date"].min().date()} -> {df["week_end_date"].max().date()}')

    qm_df = None
    if regime_filter in ('qm_combo', 'qm_only', 'dual_5050'):
        qm_df = pd.read_parquet(QM_SNAPSHOT)
        qm_df['week_end_date'] = pd.to_datetime(qm_df['week_end_date'])
        # 限定在 value snapshot 同時段
        qm_df = qm_df[qm_df['week_end_date'] >= df['week_end_date'].min()]
        print(f'QM universe: {len(qm_df)} picks, {qm_df["week_end_date"].nunique()} weeks '
              f'({qm_df["week_end_date"].min().date()} -> {qm_df["week_end_date"].max().date()})')

    twii_close = load_twii_benchmark()
    pr = backtest(df, regime_filter=regime_filter, twii_close=twii_close, qm_df=qm_df)
    metrics = compute_metrics(pr)
    label = f'Portfolio (regime={args.regime})'
    print(f'\n{label}:')
    for k, v in metrics.items():
        print(f'  {k}: {v}')
    if regime_filter:
        in_market = pr['in_market'].sum() if 'in_market' in pr else len(pr)
        print(f'  in_market: {in_market}/{len(pr)} rebalances ({in_market/len(pr)*100:.1f}%)')
        if 'source' in pr:
            src_counts = pr['source'].value_counts()
            print(f'  source breakdown: {src_counts.to_dict()}')

    # Benchmark
    twii_df, twii_m = twii_metrics_aligned(pr, twii_close)
    if twii_m:
        print(f'\nTWII (aligned, same rebalance dates):')
        for k, v in twii_m.items():
            print(f'  {k}: {v}')
        print(f'\nAlpha (Value - TWII):')
        print(f'  CAGR alpha: {metrics["cagr"] - twii_m["cagr"]:+.2f}pp')
        print(f'  Sharpe delta: {metrics["sharpe"] - twii_m["sharpe"]:+.3f}')
        print(f'  MDD: Value {metrics["mdd"]}% vs TWII {twii_m["mdd"]}%')

    annual = annual_breakdown(pr, twii_df)
    print(f'\nAnnual breakdown:')
    print(annual[['value_ret_pct', 'twii_ret_pct', 'alpha_pct', 'hit_rate']].to_string())

    # Save CSVs (append suffix based on regime + top-n)
    full_suffix = f'{suffix}{suffix_topn}'
    out_csv = OUT_CSV.parent / f'{OUT_CSV.stem}{full_suffix}.csv'
    out_annual = OUT_ANNUAL.parent / f'{OUT_ANNUAL.stem}{full_suffix}.csv'
    out_md = OUT_MD.parent / f'{OUT_MD.stem}{full_suffix}.md'
    pr.to_csv(out_csv, index=False)
    annual.to_csv(out_annual)
    print(f'\nCSV saved: {out_csv.relative_to(ROOT)}, {out_annual.relative_to(ROOT)}')

    # Markdown report
    out_md.parent.mkdir(parents=True, exist_ok=True)
    with open(out_md, 'w', encoding='utf-8') as f:
        f.write(f'# Value Portfolio Backtest (2020-2025)\n\n')
        f.write(f'**Date**: {pr["date"].iloc[0].date()} -> {pr["date"].iloc[-1].date()}\n\n')
        f.write(f'**Spec**: Top-{TOP_N} equal-weight, rebalance every {REBALANCE_EVERY} weeks, '
                f'Stage 1 (PE<={MAX_PE} / PB<={MAX_PB} / Graham<={PE_X_PB_MAX} / TV>={MIN_TV/1e6:.0f}M), '
                f'weights {WEIGHTS}\n\n')
        f.write(f'**Return**: fwd_20d from snapshot (PIT-safe, no transaction cost)\n\n')
        f.write(f'## Value Top-{TOP_N}\n\n')
        f.write('| Metric | Value |\n|---|---|\n')
        for k, v in metrics.items():
            f.write(f'| {k} | {v} |\n')
        if twii_m:
            f.write(f'\n## TWII Benchmark (aligned dates)\n\n')
            f.write('| Metric | TWII |\n|---|---|\n')
            for k, v in twii_m.items():
                f.write(f'| {k} | {v} |\n')
            f.write(f'\n## Alpha vs TWII\n\n')
            f.write(f'- **CAGR alpha**: {metrics["cagr"] - twii_m["cagr"]:+.2f} pp\n')
            f.write(f'- **Sharpe delta**: {metrics["sharpe"] - twii_m["sharpe"]:+.3f}\n')
            f.write(f'- **MDD**: Value {metrics["mdd"]}% vs TWII {twii_m["mdd"]}%\n')
        f.write(f'\n## Annual Breakdown\n\n')
        # Manual MD table to avoid tabulate dependency
        f.write('| year | value_ret_pct | twii_ret_pct | alpha_pct | hit_rate |\n')
        f.write('|---|---|---|---|---|\n')
        for yr, row in annual.iterrows():
            f.write(f'| {yr} | {row.get("value_ret_pct", 0):.2f} | {row.get("twii_ret_pct", 0):.2f} | '
                    f'{row.get("alpha_pct", 0):+.2f} | {row.get("hit_rate", 0)*100:.1f}% |\n')
        f.write('\n\n## Caveats\n\n')
        f.write('- 未計交易成本（台股約 0.3% round-trip），實際 CAGR 扣 ~4% (13 次 rebalance × 0.3%)\n')
        f.write('- `fwd_20d` 是 PIT-safe 但**不含股息再投入**\n')
        f.write('- Stage 1 filter 僅用 PE/PB/TV，不包含 Graham bypass (Value-#4 2026-04-23 上線，歷史 panel 未 backfill)\n')
        f.write('- 回測 universe 僅 857 檔（snapshot 建立時 universe），非全市場\n')
    print(f'Markdown saved: {out_md.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
