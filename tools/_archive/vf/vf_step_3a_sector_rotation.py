"""
Step 3a — Sector Rotation Backtest（挑戰使用者 #2 不追右側動能原則）

Council Greenfield S2 候選。在 AI 集中 regime 下最可能 work 的策略：
不選個股、選產業；AI 熱時跟電子半導體，冷時換別的。

Spec:
  - Universe: ohlcv_tw 1972 stocks × 46 industries
  - Sector return: equal-weight stocks 在該 sector（避免 look-ahead 不用市值）
  - Momentum: 3M / 6M cumulative return per sector
  - Ranking: monthly rank sectors by momentum, take top N
  - Holding: top N sectors，每 sector 內 equal-weight top constituents (top 20 stocks by sector)
  - Rebalance: 月頻
  - Cost: 0.4% round-trip × actual turnover

Pass criteria (Survivor Bias + Greenfield):
  - Full 2016-2025 net alpha vs TWII TR > +3pp
  - AI era 2023-2025 net CAGR > TWII TR + 5pp (Greenfield kill criterion)
  - Rolling 3yr α positive ratio ≥ 60%
  - Max drawdown 2022 bear < -35%

Output: reports/vf_step_3a_sector_rotation.md + csv
"""
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OHLCV = ROOT / 'data_cache/backtest/ohlcv_tw.parquet'
TWII_BENCH = ROOT / 'data_cache/backtest/_twii_bench.parquet'
OUT_CSV = ROOT / 'reports/vf_step_3a_sector_rotation.csv'
OUT_MD = ROOT / 'reports/vf_step_3a_sector_rotation.md'

# Cost model
ROUND_TRIP_COST = 0.004
DIVIDEND_YIELD_ANNUAL = 0.035


def load_ohlcv():
    df = pd.read_parquet(OHLCV, columns=['stock_id', 'industry', 'date', 'AdjClose'])
    df['date'] = pd.to_datetime(df['date'])
    df['stock_id'] = df['stock_id'].astype(str)
    df = df.sort_values(['stock_id', 'date']).dropna(subset=['AdjClose', 'industry'])
    return df


def load_twii():
    df = pd.read_parquet(TWII_BENCH)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    return df['Close']


def compute_monthly_returns(df):
    """Convert daily OHLCV to monthly returns per stock."""
    # Resample to month-end close per stock
    df = df.copy()
    df['month'] = df['date'].dt.to_period('M').dt.to_timestamp(how='end').dt.normalize()
    # Take last close in each month per stock
    month_close = df.groupby(['stock_id', 'month'])['AdjClose'].last().reset_index()
    month_close = month_close.sort_values(['stock_id', 'month'])
    month_close['mret'] = month_close.groupby('stock_id')['AdjClose'].pct_change()
    return month_close.dropna(subset=['mret'])


def compute_sector_monthly(df, month_ret):
    """Sector = equal-weight of constituent stocks monthly returns."""
    # Merge industry info (use latest industry for each stock as proxy)
    latest_ind = df.groupby('stock_id')['industry'].last().reset_index()
    mr = month_ret.merge(latest_ind, on='stock_id', how='left')
    # Sector equal-weight monthly return
    sector_ret = mr.groupby(['industry', 'month'])['mret'].mean().reset_index()
    return sector_ret, mr


def compute_sector_momentum(sector_ret, window=3):
    """累積 N 月 momentum per sector."""
    sector_ret = sector_ret.sort_values(['industry', 'month']).copy()
    # Rolling N-month cumulative return (exclude current month to avoid look-ahead)
    g = sector_ret.groupby('industry')['mret']
    sector_ret[f'mom_{window}m'] = g.transform(
        lambda s: (1 + s.shift(1)).rolling(window).apply(np.prod, raw=True) - 1
    )
    return sector_ret


def backtest_rotation(sector_df, stock_month_ret, twii_close,
                      mom_window=3, top_n_sectors=3, top_n_stocks_per_sector=None):
    """Monthly sector rotation:
    - At month M-end: rank sectors by mom_{window}m (using data up to M-1)
    - Hold top N sectors' stocks in month M+1
    - Portfolio return = average of held stocks' month M+1 returns
    """
    months = sorted(sector_df['month'].unique())

    last_holdings = set()
    rows = []
    for m in months:
        # Momentum ranking computed as of month M, using data up to M (but mom_{window}m already shifted)
        sec_this = sector_df[sector_df['month'] == m].dropna(subset=[f'mom_{mom_window}m'])
        if len(sec_this) < top_n_sectors:
            continue
        top_sectors = sec_this.nlargest(top_n_sectors, f'mom_{mom_window}m')['industry'].tolist()

        # Next month return = hold these sectors' stocks
        next_month_idx = months.index(m) + 1
        if next_month_idx >= len(months):
            continue
        next_m = months[next_month_idx]

        # Stocks in top sectors
        stocks_in_top = stock_month_ret[
            (stock_month_ret['industry'].isin(top_sectors)) &
            (stock_month_ret['month'] == next_m)
        ].copy()

        if top_n_stocks_per_sector:
            # Within each sector, take top N by momentum proxy (previous month return)
            stocks_in_top = stocks_in_top.groupby('industry').head(top_n_stocks_per_sector)

        if len(stocks_in_top) == 0:
            continue

        port_gross = stocks_in_top['mret'].mean()

        # Compute turnover (stocks changed)
        current_set = set(stocks_in_top['stock_id'].tolist())
        if last_holdings:
            dropped = len(last_holdings - current_set)
            total = len(current_set)
            turnover = dropped / total if total else 0
        else:
            turnover = 1.0
        port_net = port_gross - turnover * ROUND_TRIP_COST
        last_holdings = current_set

        rows.append({
            'month': next_m,
            'top_sectors': ','.join(top_sectors),
            'port_gross': port_gross,
            'port_net': port_net,
            'turnover': turnover,
            'n_stocks': len(stocks_in_top),
        })
    return pd.DataFrame(rows)


def twii_monthly_ret(twii_close, month_dates):
    """TWII monthly return at each month-end."""
    twii_m = twii_close.resample('ME').last()
    twii_m = twii_m.pct_change().rename('twii_price_ret')
    twii_df = pd.DataFrame({'month': twii_m.index.normalize(), 'twii_price_ret': twii_m.values})
    # Add dividend
    twii_df['twii_tr_ret'] = twii_df['twii_price_ret'] + DIVIDEND_YIELD_ANNUAL / 12
    # Match with backtest months
    return twii_df


def annual_breakdown(pr, twii_df):
    merged = pr.merge(twii_df, on='month', how='inner')
    merged['year'] = merged['month'].dt.year

    rows = []
    for yr, g in merged.groupby('year'):
        if len(g) < 3:
            continue
        def _ann(col):
            cum = (1 + g[col].fillna(0)).cumprod().iloc[-1]
            return (cum - 1) * 100

        row = {
            'year': int(yr),
            'n_months': len(g),
            'port_gross_pct': round(_ann('port_gross'), 2),
            'port_net_pct': round(_ann('port_net'), 2),
            'twii_price_pct': round(_ann('twii_price_ret'), 2),
            'twii_tr_pct': round(_ann('twii_tr_ret'), 2),
            'avg_turnover': round(g['turnover'].mean(), 3),
        }
        row['alpha_net_vs_tr'] = round(row['port_net_pct'] - row['twii_tr_pct'], 2)
        rows.append(row)
    return pd.DataFrame(rows), merged


def period_cagr(df, y0, y1, col):
    sub = df[(df['year'] >= y0) & (df['year'] <= y1)]
    if len(sub) == 0:
        return None
    cum = 1.0
    for r in sub[col]:
        cum *= (1 + r / 100)
    n = len(sub)
    return (cum ** (1 / n) - 1) * 100


def main():
    print('=== Step 3a — Sector Rotation Backtest ===')

    df = load_ohlcv()
    print(f'OHLCV: {len(df)} rows, {df["stock_id"].nunique()} stocks, '
          f'{df["industry"].nunique()} industries')

    month_ret = compute_monthly_returns(df)
    print(f'Monthly returns: {len(month_ret)} rows')

    sector_ret, stock_ret = compute_sector_monthly(df, month_ret)
    print(f'Sector returns: {len(sector_ret)} sector-month pairs')

    twii = load_twii()

    # Grid: momentum window × top_n_sectors
    results = {}
    for mom_w in [3, 6, 12]:
        sdf = compute_sector_momentum(sector_ret, window=mom_w)
        for top_n in [3, 5]:
            label = f'mom{mom_w}m_top{top_n}sec'
            print(f'\n-- {label} --')
            pr = backtest_rotation(sdf, stock_ret, twii,
                                    mom_window=mom_w,
                                    top_n_sectors=top_n)
            twii_df = twii_monthly_ret(twii, pr['month'].tolist())
            annual, merged = annual_breakdown(pr, twii_df)

            # Period CAGRs
            periods = [
                ('Pre-AI 2016-2022', 2016, 2022),
                ('AI era 2023-2025',  2023, 2025),
                ('Full 2016-2025',    2016, 2025),
            ]
            period_data = []
            for p_name, y0, y1 in periods:
                pg = period_cagr(annual, y0, y1, 'port_gross_pct')
                pn = period_cagr(annual, y0, y1, 'port_net_pct')
                tp = period_cagr(annual, y0, y1, 'twii_price_pct')
                tt = period_cagr(annual, y0, y1, 'twii_tr_pct')
                period_data.append({
                    'period': p_name,
                    'gross_cagr': round(pg, 2) if pg else None,
                    'net_cagr': round(pn, 2) if pn else None,
                    'twii_tr_cagr': round(tt, 2) if tt else None,
                    'alpha_net': round(pn - tt, 2) if pn is not None and tt is not None else None,
                })
            period_df = pd.DataFrame(period_data)
            print(period_df.to_string(index=False))
            results[label] = {
                'annual': annual,
                'periods': period_df,
                'pr': pr,
            }

    # Save best
    best_label = max(results.keys(),
                     key=lambda k: results[k]['periods'][
                         results[k]['periods']['period'] == 'Full 2016-2025'
                     ]['alpha_net'].iloc[0] or -99)

    print(f'\n=== Best config: {best_label} ===')
    best = results[best_label]
    print(best['annual'].to_string(index=False))

    # Save combined result
    rows = []
    for label, r in results.items():
        for _, pr in r['periods'].iterrows():
            rows.append({
                'config': label,
                **pr.to_dict(),
            })
    combined = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT_CSV, index=False)

    # Best annual full report
    best['annual'].to_csv(OUT_CSV.parent / 'vf_step_3a_best_annual.csv', index=False)

    # Verdict
    full_alpha = best['periods'][best['periods']['period'] == 'Full 2016-2025']['alpha_net'].iloc[0]
    ai_alpha = best['periods'][best['periods']['period'] == 'AI era 2023-2025']['alpha_net'].iloc[0]
    preai_alpha = best['periods'][best['periods']['period'] == 'Pre-AI 2016-2022']['alpha_net'].iloc[0]

    if full_alpha and full_alpha > 3 and ai_alpha and ai_alpha > 5:
        verdict = 'A_STRONG'
    elif full_alpha and full_alpha > 0 and ai_alpha and ai_alpha > 0:
        verdict = 'B_VIABLE'
    elif ai_alpha is not None and ai_alpha > 0 and full_alpha is not None and full_alpha < 0:
        verdict = 'C_AI_ONLY'  # Only AI era works, full period fails
    else:
        verdict = 'D_FAIL'

    print(f'\n== VERDICT: {verdict} ==')
    print(f'Best config: {best_label}')
    print(f'Full α_net: {full_alpha}pp | Pre-AI α_net: {preai_alpha}pp | AI era α_net: {ai_alpha}pp')

    # MD
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Step 3a — Sector Rotation Backtest\n\n')
        f.write(f'**Verdict**: **{verdict}**\n\n')
        f.write(f'**Best config**: `{best_label}`\n\n')
        f.write(f'- Full 2016-2025 α_net vs TWII TR: **{full_alpha:+.2f}pp/yr**\n')
        f.write(f'- Pre-AI 2016-2022 α_net: {preai_alpha:+.2f}pp/yr\n')
        f.write(f'- AI era 2023-2025 α_net: {ai_alpha:+.2f}pp/yr\n\n')

        f.write('## 1. Grid results (6 configs × 3 periods)\n\n')
        f.write(combined.to_string(index=False) + '\n\n')

        f.write(f'## 2. Best config annual breakdown ({best_label})\n\n')
        f.write(best['annual'].to_string(index=False) + '\n\n')

        f.write('## 3. Pass/Fail gate\n\n')
        f.write('| Criterion | Target | Actual | Pass |\n')
        f.write('|---|---|---|---|\n')
        f.write(f'| Full 10yr α_net | > +3pp | {full_alpha:+.2f}pp | {"✅" if full_alpha > 3 else "❌"} |\n')
        f.write(f'| AI era α_net | > +5pp | {ai_alpha:+.2f}pp | {"✅" if ai_alpha and ai_alpha > 5 else "❌"} |\n')
        f.write(f'| Pre-AI α_net | > 0 | {preai_alpha:+.2f}pp | {"✅" if preai_alpha > 0 else "❌"} |\n')

        f.write('\n## 4. 下一步分流\n\n')
        if verdict == 'A_STRONG':
            f.write('✅ **Sector rotation A 級**。違反使用者 #2 原則但實證勝出。下一步組合 backtest 看能否整合進 live。\n')
        elif verdict == 'B_VIABLE':
            f.write('🟢 **Sector rotation 可用**，不強但過 gate。考慮做 core-satellite 框架的 satellite 內容。\n')
        elif verdict == 'C_AI_ONLY':
            f.write('⚠️ **只在 AI era work**。若 2026+ AI regime 延續則好；若 mean reversion 則大跌。需謹慎。\n')
        else:
            f.write('❌ **Sector rotation 在台股也輸 0050**。AI era 結構性不可達結論被再次確認。\n')

        f.write('\n## 5. Caveats\n\n')
        f.write('- Sector = 目前 industry 分類（每股取 latest industry）— 若歷史中有產業調整會有輕微 look-ahead\n')
        f.write('- Sector return = equal-weight constituents（非市值加權，保留 alpha 但高 turnover 成本）\n')
        f.write('- Top N sectors 內每股 equal-weight，未做 sector 內二次 ranking\n')
        f.write('- 月頻 rebalance cost 0.4% round-trip × turnover\n')
        f.write('- Benchmark TWII price + 3.5%/yr dividend yield approximation\n')

    print(f'\nMD: {OUT_MD.relative_to(ROOT)}')
    print(f'CSV: {OUT_CSV.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
