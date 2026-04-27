"""
Step D0 — PEAD (Post-Earnings Announcement Drift) event study

驗證台股月營收 YoY surprise 公告後是否有 price drift。
Council 5-agent R2 共識 satellite 首選：PEAD + 券資比反轉。先驗 PEAD。

Spec:
  - Event: 月營收公告日（法定次月 10 日前）
  - Surprise: YoY growth 減去該股 rolling 12m median YoY，再 / rolling 12m std
             (standardize 掉個股常態 growth 水準)
  - CAR horizons: T+5 / T+20 / T+40 / T+60 trading days (vs TWII)
  - Quintile: 每次事件按 surprise 強度分 5 等分
  - Full / Pre-AI / AI era 三段分解

Pass criteria (Survivor Bias R2 gate):
  - Top quintile CAR20 > +2% 且 p-value < 0.05
  - Top-bottom spread > 3%
  - AI era (2023-2025) top quintile CAR20 > 0（不反向）
  - IC (rank correlation surprise → fwd_20d) > 0.03

Output: reports/vf_step_d0_pead_event_study.md + csv
"""
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
REVENUE = ROOT / 'data_cache/backtest/financials_revenue.parquet'
OHLCV = ROOT / 'data_cache/backtest/ohlcv_tw.parquet'
TWII_BENCH = ROOT / 'data_cache/backtest/_twii_bench.parquet'
OUT_CSV = ROOT / 'reports/vf_step_d0_pead_event_study.csv'
OUT_MD = ROOT / 'reports/vf_step_d0_pead_event_study.md'


def load_revenue():
    """Load and compute YoY from revenue directly (cached revenue_year_growth is 99% NaN)."""
    df = pd.read_parquet(REVENUE)
    df['date'] = pd.to_datetime(df['date'])
    df['stock_id'] = df['stock_id'].astype(str)
    # 法定月營收公告截止 = 次月 10 日
    df['announce_date'] = df['date'] + pd.DateOffset(days=10)
    df = df.sort_values(['stock_id', 'date']).reset_index(drop=True)
    # Self-computed YoY using 12-month shift per stock
    df['revenue_last_year_calc'] = df.groupby('stock_id')['revenue'].shift(12)
    df['yoy_pct'] = (df['revenue'] / df['revenue_last_year_calc'] - 1) * 100
    # Drop inf (revenue_last_year=0) and NaN
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['yoy_pct'])
    return df


def compute_surprise(rev_df):
    """Standardized surprise = (YoY - rolling 12m median YoY) / rolling 12m std YoY."""
    rev_df = rev_df.sort_values(['stock_id', 'date']).copy()
    g = rev_df.groupby('stock_id')['yoy_pct']
    # Shift(1) ensures baseline uses PRIOR months only (no look-ahead at announce time)
    rev_df['baseline_median'] = g.transform(
        lambda s: s.shift(1).rolling(12, min_periods=6).median()
    )
    rev_df['baseline_std'] = g.transform(
        lambda s: s.shift(1).rolling(12, min_periods=6).std()
    )
    rev_df['surprise_raw'] = rev_df['yoy_pct'] - rev_df['baseline_median']
    rev_df['surprise_std'] = rev_df['surprise_raw'] / rev_df['baseline_std'].replace(0, np.nan)
    return rev_df


def load_ohlcv_tw():
    df = pd.read_parquet(OHLCV, columns=['stock_id', 'date', 'AdjClose'])
    df['date'] = pd.to_datetime(df['date'])
    df['stock_id'] = df['stock_id'].astype(str)
    df = df.sort_values(['stock_id', 'date']).dropna(subset=['AdjClose'])
    return df


def load_twii():
    df = pd.read_parquet(TWII_BENCH)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    return df['Close']


def compute_car(ohlcv, twii_close, horizons=(5, 20, 40, 60)):
    """
    For each (stock, date) point in ohlcv, compute forward CAR at horizons.
    CAR = stock fwd return - TWII fwd return (over same window).
    """
    # TWII date-indexed series → daily returns
    twii_daily = twii_close.pct_change()

    # For each stock, compute forward return at each horizon
    out = ohlcv[['stock_id', 'date', 'AdjClose']].copy()
    for h in horizons:
        # Stock fwd return h days ahead
        out[f'stock_fwd_{h}d'] = out.groupby('stock_id')['AdjClose'].pct_change(h).shift(-h)

    # TWII fwd return at each date
    twii_df = pd.DataFrame({'date': twii_close.index, 'twii_close': twii_close.values})
    for h in horizons:
        twii_df[f'twii_fwd_{h}d'] = twii_df['twii_close'].pct_change(h).shift(-h)

    out = out.merge(twii_df[['date'] + [f'twii_fwd_{h}d' for h in horizons]],
                    on='date', how='left')
    for h in horizons:
        out[f'car_{h}d'] = out[f'stock_fwd_{h}d'] - out[f'twii_fwd_{h}d']

    return out[['stock_id', 'date'] + [f'car_{h}d' for h in horizons]]


def merge_events(rev_df, car_df):
    """Merge revenue events with ohlcv at announce_date (or nearest trading day)."""
    # For each rev event, find the closest trading date >= announce_date
    # Use merge_asof with direction='forward' on sorted data
    rev_df = rev_df.sort_values('announce_date').copy()
    car_df = car_df.sort_values('date').copy()
    merged = pd.merge_asof(
        rev_df, car_df,
        left_on='announce_date', right_on='date',
        by='stock_id', direction='forward',
        tolerance=pd.Timedelta(days=7),
    )
    # Drop events without matching price data
    merged = merged.dropna(subset=['car_20d'])
    return merged


def quintile_analysis(df, surprise_col='surprise_std', ret_col='car_20d'):
    """按 surprise 分 quintile，計算每組平均 CAR。"""
    df = df.dropna(subset=[surprise_col, ret_col]).copy()
    df['quintile'] = pd.qcut(df[surprise_col], 5, labels=['Q1_low', 'Q2', 'Q3', 'Q4', 'Q5_high'],
                              duplicates='drop')
    return df.groupby('quintile')[ret_col].agg(['mean', 'count', 'std'])


def period_quintile(df, periods):
    """多時段 × 多 horizon quintile 表。"""
    rows = []
    for name, mask in periods.items():
        sub = df[mask]
        for h in [5, 20, 40, 60]:
            col = f'car_{h}d'
            sur = sub.dropna(subset=['surprise_std', col]).copy()
            if len(sur) < 50:
                continue
            sur['quintile'] = pd.qcut(sur['surprise_std'], 5,
                                       labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'],
                                       duplicates='drop')
            q_means = sur.groupby('quintile')[col].mean() * 100
            # IC
            ic = sur['surprise_std'].corr(sur[col], method='spearman')
            row = {
                'period': name, 'horizon': f'{h}d', 'n_events': len(sur),
                **{f'{q}_pct': round(q_means.get(q, np.nan), 3) for q in ['Q1','Q2','Q3','Q4','Q5']},
                'spread_Q5_Q1': round(q_means.get('Q5', np.nan) - q_means.get('Q1', np.nan), 3),
                'ic_spearman': round(ic, 4) if ic == ic else None,
            }
            # t-test for Q5 > 0
            q5 = sur[sur['quintile'] == 'Q5'][col]
            if len(q5) > 10:
                t, p = stats.ttest_1samp(q5, 0)
                row['q5_tstat'] = round(t, 2)
                row['q5_pval'] = round(p, 4)
            rows.append(row)
    return pd.DataFrame(rows)


def main():
    print('=== Step D0 — PEAD Event Study (TW 月營收) ===')

    # Load
    rev = load_revenue()
    print(f'Revenue events: {len(rev)}, {rev["stock_id"].nunique()} stocks, '
          f'{rev["date"].min().date()} -> {rev["date"].max().date()}')

    rev = compute_surprise(rev)
    rev_clean = rev.dropna(subset=['surprise_std'])
    print(f'After surprise (std baseline rolling 12m): {len(rev_clean)} events')

    ohlcv = load_ohlcv_tw()
    print(f'OHLCV: {len(ohlcv)} rows, {ohlcv["stock_id"].nunique()} stocks')

    twii = load_twii()
    print(f'TWII: {len(twii)} days, {twii.index.min().date()} -> {twii.index.max().date()}')

    car = compute_car(ohlcv, twii)
    print(f'CAR computed: {len(car)} stock-date pairs')

    merged = merge_events(rev_clean, car)
    print(f'Events with price data: {len(merged)}')

    # Period masks
    merged['announce_year'] = merged['announce_date'].dt.year
    periods = {
        'Full 2016-2025':      (merged['announce_year'] >= 2016) & (merged['announce_year'] <= 2025),
        'Pre-AI 2016-2022':    (merged['announce_year'] >= 2016) & (merged['announce_year'] <= 2022),
        'AI era 2023-2025':    (merged['announce_year'] >= 2023) & (merged['announce_year'] <= 2025),
    }

    print('\n-- Quintile × Horizon × Period --')
    table = period_quintile(merged, periods)
    print(table.to_string(index=False))

    # Save
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(OUT_CSV, index=False)

    # Extract key metrics for verdict
    def get_row(period, horizon):
        r = table[(table['period'] == period) & (table['horizon'] == horizon)]
        return r.iloc[0] if len(r) else None

    full_20 = get_row('Full 2016-2025', '20d')
    preai_20 = get_row('Pre-AI 2016-2022', '20d')
    ai_20 = get_row('AI era 2023-2025', '20d')

    # Verdict
    def classify():
        if full_20 is None:
            return 'ERROR', 'insufficient data'
        q5 = full_20['Q5_pct']
        spread = full_20['spread_Q5_Q1']
        ic = full_20['ic_spearman']
        ai_q5 = ai_20['Q5_pct'] if ai_20 is not None else None
        pval = full_20.get('q5_pval', 1.0)

        # A: all criteria pass
        if q5 > 2 and spread > 3 and ic > 0.03 and ai_q5 and ai_q5 > 0 and pval < 0.05:
            return 'A_STRONG', f'Q5 {q5}%, spread {spread}pp, IC {ic}, AI era Q5 {ai_q5}%, p={pval}'
        # B: signal exists but weak
        if q5 > 1 and spread > 1.5 and ic > 0.02:
            return 'B_WEAK_SIGNAL', f'Q5 {q5}%, spread {spread}, IC {ic}'
        # C: flat or reverse
        return 'D_FAIL', f'Q5 {q5}%, spread {spread}, IC {ic} — signal not viable'

    verdict, detail = classify()
    print(f'\n== VERDICT: {verdict} ==\n{detail}')

    # MD report
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Step D0 — PEAD Event Study (TW 月營收)\n\n')
        f.write(f'**Verdict**: **{verdict}**\n\n')
        f.write(f'> {detail}\n\n')
        f.write('**Event**: 月營收公告日（營收月 +10 天近似）\n\n')
        f.write('**Surprise**: YoY growth − rolling 12m median, 標準化除 rolling 12m std\n\n')
        f.write('**CAR**: Stock fwd return − TWII fwd return，horizons {5,20,40,60}d\n\n')
        f.write('**Quintile**: 每事件按 surprise 強度 cross-section 分 5 等分\n\n')

        f.write('## 1. Quintile × Horizon × Period\n\n')
        f.write(table.to_string(index=False) + '\n\n')

        f.write('## 2. Pass/Fail Gate\n\n')
        if full_20 is not None:
            f.write(f'| Criterion | Target | Actual | Pass |\n')
            f.write(f'|---|---|---|---|\n')
            q5 = full_20['Q5_pct']
            spread = full_20['spread_Q5_Q1']
            ic = full_20['ic_spearman']
            pval = full_20.get('q5_pval', 1.0)
            ai_q5 = ai_20['Q5_pct'] if ai_20 is not None else 'N/A'
            f.write(f'| Top quintile 20d CAR | > +2% | {q5}% | {"✅" if q5 > 2 else "❌"} |\n')
            f.write(f'| Q5-Q1 spread | > +3pp | {spread}pp | {"✅" if spread > 3 else "❌"} |\n')
            f.write(f'| Spearman IC | > 0.03 | {ic} | {"✅" if ic and ic > 0.03 else "❌"} |\n')
            f.write(f'| Q5 t-test p-value | < 0.05 | {pval} | {"✅" if pval < 0.05 else "❌"} |\n')
            f.write(f'| AI era Q5 20d CAR | > 0 | {ai_q5}% | {"✅" if ai_q5 != "N/A" and ai_q5 > 0 else "❌"} |\n')

        f.write('\n## 3. 下一步分流\n\n')
        if verdict == 'A_STRONG':
            f.write('✅ **PEAD 可用為 satellite alpha 源**。下一步：跑券資比反轉 IC (D0-2) → 組合 Core-Satellite backtest (D1)。\n')
        elif verdict == 'B_WEAK_SIGNAL':
            f.write('⚠️ **訊號存在但弱**。可試 top-decile（Q5 再切細）或改用 earnings surprise magnitude 加權。考慮不獨立當 factor，只做 overlay。\n')
        else:
            f.write('❌ **PEAD 在台股 2016-2025 無效**。satellite 需換內容（券資比反轉為唯一備案 + 可能 sector rotation）。回 council 重議。\n')

        f.write('\n## 4. Caveats\n\n')
        f.write('- Announce date 用「營收月 + 10 天」近似，未用真實 MOPS 公告時間戳\n')
        f.write('- CAR 相對 TWII，未相對 sector/industry（未去除產業 beta）\n')
        f.write('- 無 size effect 控制（小型股 signal 可能較強但流動性差）\n')
        f.write('- Surprise baseline 用 12m rolling median + std，其他 baseline 未測試\n')
        f.write('- 未扣交易成本（event-driven trade，若每月全 Q5 進場 turnover 高）\n')

    print(f'\nMD: {OUT_MD.relative_to(ROOT)}')
    print(f'CSV: {OUT_CSV.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
