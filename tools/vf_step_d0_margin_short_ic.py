"""
Step D0-2 — 券資比反轉 IC 驗證

Council 5-agent R2 satellite 候選 #2：券資比（sbl_ratio = short / margin）。
Memory C2-b 已驗 IR -0.57，這裡快速 replicate + 延伸 quintile + AI era 分解。

資料：
  data_cache/chip_history/margin.parquet (2021-04 → 2026-04, ~5yr)
  OHLCV TW 算 fwd returns

方法：
  - 每週五收盤後計算 sbl_ratio = short_balance / margin_balance (per stock)
  - Cross-section quintile + fwd 5/20/40/60d CAR vs TWII
  - 預期方向：**高券資比 → 未來跌**（空方正確看空）
  - IC 應該 negative
  - Top (Q5 高券資比) - Bottom (Q1 低) spread 應該 negative

Pass criteria:
  - IC < -0.03 (negative = 高券資比預測低報酬)
  - Q5-Q1 spread < -2pp (Q5 比 Q1 表現差)
  - AI era (2023-2025) 仍負向（不反轉）

Output: reports/vf_step_d0_margin_short_ic.md + csv
"""
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
MARGIN = ROOT / 'data_cache/chip_history/margin.parquet'
OHLCV = ROOT / 'data_cache/backtest/ohlcv_tw.parquet'
TWII_BENCH = ROOT / 'data_cache/backtest/_twii_bench.parquet'
OUT_CSV = ROOT / 'reports/vf_step_d0_margin_short_ic.csv'
OUT_MD = ROOT / 'reports/vf_step_d0_margin_short_ic.md'


def load_margin_weekly():
    """Load margin data, compute sbl_ratio, keep weekly snapshots (Fridays)."""
    m = pd.read_parquet(MARGIN)
    m['date'] = pd.to_datetime(m['date'])
    m['stock_id'] = m['stock_id'].astype(str)
    m['sbl_ratio'] = m['short_balance'] / m['margin_balance'].replace(0, np.nan)
    # Must have margin_balance > some min volume (avoid noise from tiny margin stocks)
    m = m[m['margin_balance'] >= 100].copy()  # min 100 張 margin
    m = m.dropna(subset=['sbl_ratio'])
    # Weekly: take each Friday close
    m['dow'] = m['date'].dt.dayofweek
    # Use each day's data, but will merge with fwd returns directly
    return m


def load_ohlcv():
    df = pd.read_parquet(OHLCV, columns=['stock_id', 'date', 'AdjClose'])
    df['date'] = pd.to_datetime(df['date'])
    df['stock_id'] = df['stock_id'].astype(str)
    return df.sort_values(['stock_id', 'date']).dropna(subset=['AdjClose'])


def load_twii():
    df = pd.read_parquet(TWII_BENCH)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    return df['Close']


def compute_fwd_car(ohlcv, twii_close, horizons=(5, 20, 40, 60)):
    """For each (stock, date), compute fwd CAR = stock fwd ret - TWII fwd ret."""
    out = ohlcv.copy()
    for h in horizons:
        out[f'stock_fwd_{h}'] = out.groupby('stock_id')['AdjClose'].pct_change(h).shift(-h)
    twii_df = pd.DataFrame({'date': twii_close.index, 'twii': twii_close.values})
    for h in horizons:
        twii_df[f'twii_fwd_{h}'] = twii_df['twii'].pct_change(h).shift(-h)
    out = out.merge(twii_df[['date'] + [f'twii_fwd_{h}' for h in horizons]],
                    on='date', how='left')
    for h in horizons:
        out[f'car_{h}'] = out[f'stock_fwd_{h}'] - out[f'twii_fwd_{h}']
    return out[['stock_id', 'date'] + [f'car_{h}' for h in horizons]]


def merge_weekly_snapshots(m, car):
    """每週一次 cross-section event。取每週五當 snapshot。"""
    # Only keep Fridays in margin data
    m_fri = m[m['dow'] == 4].copy()  # Friday=4
    # Match with CAR on same date
    merged = m_fri.merge(car, on=['stock_id', 'date'], how='inner')
    merged = merged.dropna(subset=['sbl_ratio', 'car_20'])
    return merged


def weekly_quintile_analysis(df, periods):
    """每週 cross-section 分 quintile，再整段時期 aggregate。"""
    rows = []
    for name, mask in periods.items():
        sub = df[mask].copy()
        # Filter 掉 sbl_ratio == 0（無融券餘額的股票，非有意義空方 signal）
        sub = sub[sub['sbl_ratio'] > 0].copy()
        for h in [5, 20, 40, 60]:
            car_col = f'car_{h}'
            # Weekly quintile using percentile rank (robust to duplicates)
            def _rank_to_q(x):
                if len(x) < 5:
                    return pd.Series(np.nan, index=x.index)
                pct = x.rank(pct=True, method='average')
                labels = pd.Series('Q3', index=x.index)
                labels[pct <= 0.2] = 'Q1_low'
                labels[(pct > 0.2) & (pct <= 0.4)] = 'Q2'
                labels[(pct > 0.4) & (pct <= 0.6)] = 'Q3'
                labels[(pct > 0.6) & (pct <= 0.8)] = 'Q4'
                labels[pct > 0.8] = 'Q5_high'
                return labels
            sub['quintile'] = sub.groupby('date')['sbl_ratio'].transform(_rank_to_q)
            # Average per quintile across all weeks
            valid = sub.dropna(subset=['quintile', car_col])
            q_means = valid.groupby('quintile', observed=True)[car_col].mean() * 100
            # Spearman IC per week, then avg
            weekly_ic = valid.groupby('date').apply(
                lambda g: g['sbl_ratio'].corr(g[car_col], method='spearman') if len(g) >= 10 else np.nan,
                include_groups=False,
            )
            ic_mean = weekly_ic.mean()
            ic_tstat = (weekly_ic.mean() / weekly_ic.std() * np.sqrt(len(weekly_ic.dropna()))) if weekly_ic.std() > 0 else np.nan
            # Q5 (high) - Q1 (low) — 預期 NEGATIVE
            spread = q_means.get('Q5_high', np.nan) - q_means.get('Q1_low', np.nan)
            row = {
                'period': name, 'horizon': f'{h}d',
                'n_events': len(valid),
                'n_weeks': valid['date'].nunique(),
                **{f'{q}_pct': round(q_means.get(q, np.nan), 3)
                   for q in ['Q1_low', 'Q2', 'Q3', 'Q4', 'Q5_high']},
                'spread_Q5_Q1': round(spread, 3),
                'ic_spearman': round(ic_mean, 4) if ic_mean == ic_mean else None,
                'ic_tstat': round(ic_tstat, 2) if ic_tstat == ic_tstat else None,
            }
            rows.append(row)
    return pd.DataFrame(rows)


def main():
    print('=== Step D0-2 — 券資比 (sbl_ratio) IC ===')

    m = load_margin_weekly()
    print(f'Margin data: {len(m)} rows, {m["stock_id"].nunique()} stocks, '
          f'{m["date"].min().date()} -> {m["date"].max().date()}')

    ohlcv = load_ohlcv()
    twii = load_twii()
    car = compute_fwd_car(ohlcv, twii)
    print(f'CAR computed: {len(car)} pairs')

    merged = merge_weekly_snapshots(m, car)
    print(f'Weekly snapshots (Friday): {len(merged)} events, '
          f'{merged["date"].nunique()} weeks, '
          f'{merged["date"].min().date()} -> {merged["date"].max().date()}')

    merged['year'] = merged['date'].dt.year
    periods = {
        'Full 2021-2025':    (merged['year'] >= 2021) & (merged['year'] <= 2025),
        'Pre-AI 2021-2022':  (merged['year'] >= 2021) & (merged['year'] <= 2022),
        'AI era 2023-2025':  (merged['year'] >= 2023) & (merged['year'] <= 2025),
    }

    table = weekly_quintile_analysis(merged, periods)
    print('\n-- Quintile × Horizon × Period --')
    print(table.to_string(index=False))

    # Save
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(OUT_CSV, index=False)

    # Verdict
    full_20 = table[(table['period'] == 'Full 2021-2025') & (table['horizon'] == '20d')].iloc[0]
    ic = full_20['ic_spearman']
    spread = full_20['spread_Q5_Q1']
    ai_20 = table[(table['period'] == 'AI era 2023-2025') & (table['horizon'] == '20d')]
    ai_ic = ai_20['ic_spearman'].iloc[0] if len(ai_20) else None

    if ic < -0.03 and spread < -2 and ai_ic and ai_ic < 0:
        verdict = 'A_STRONG'
        detail = f'IC {ic} (negative), Q5-Q1 {spread}pp, AI era IC {ai_ic} — 反轉訊號穩健'
    elif ic < -0.02 and spread < -1:
        verdict = 'B_WEAK'
        detail = f'IC {ic}, spread {spread}pp — 弱負向反轉訊號'
    elif ic and ic > 0.02:
        verdict = 'D_WRONG_DIRECTION'
        detail = f'IC {ic} (POSITIVE) — 高券資比未來反而漲？方向反過來，策略必須翻轉'
    else:
        verdict = 'D_NOISE'
        detail = f'IC {ic}, spread {spread}pp — 訊號太弱'

    print(f'\n== VERDICT: {verdict} ==\n{detail}')

    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Step D0-2 — 券資比 (sbl_ratio) IC 驗證\n\n')
        f.write(f'**Verdict**: **{verdict}**\n\n')
        f.write(f'> {detail}\n\n')
        f.write('**Factor**: `short_balance / margin_balance` per stock, weekly (Friday)\n\n')
        f.write('**Expected direction**: 高券資比 → 未來跌（空方正確看空）→ IC 應為 NEGATIVE\n\n')
        f.write('**Sample**: 2021-04 → 2026-04（margin data coverage 限制，無 pre-2021）\n\n')

        f.write('## 1. Quintile × Horizon × Period\n\n')
        f.write(table.to_string(index=False) + '\n\n')

        f.write('## 2. 關鍵數字（20d horizon）\n\n')
        for _, r in table[table['horizon'] == '20d'].iterrows():
            f.write(f'- **{r["period"]}**: IC={r["ic_spearman"]} / Q5-Q1 spread={r["spread_Q5_Q1"]}pp / n={r["n_events"]}\n')

        f.write('\n## 3. 下一步分流\n\n')
        if verdict == 'A_STRONG':
            f.write('✅ **券資比反轉 A 級可用**。下一步：驗 PEAD × 券資比 monthly return corr（必須 < 0.7），再進組合 backtest。\n')
        elif verdict == 'B_WEAK':
            f.write('⚠️ 訊號存在但弱。考慮 decile 細分或組合進 multi-factor score。\n')
        elif verdict == 'D_WRONG_DIRECTION':
            f.write('❌ **方向反了**。C2-b memory 說的 IR -0.57 可能需更新或此次 implementation 差。檢查計算再跑。\n')
        else:
            f.write('❌ 訊號太雜訊。satellite 應僅靠 PEAD，或探索其他 factor（宋分 5 個）。\n')

        f.write('\n## 4. Caveats\n\n')
        f.write('- margin data 只有 2021-04 起（5 年），無 pre-AI 2016-2022 長 sample\n')
        f.write('- sbl_ratio 計算用絕對值比例，不 standardize（跨股比較可能 noisy）\n')
        f.write('- 週五 snapshot 近似 weekly rebalance，實際可 daily 更高頻\n')
        f.write('- Min margin_balance=100 張 filter 小型股，可能漏 alpha\n')

    print(f'\nMD: {OUT_MD.relative_to(ROOT)}')
    print(f'CSV: {OUT_CSV.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
