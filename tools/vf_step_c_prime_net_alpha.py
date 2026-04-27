"""
Step C' — Net Alpha Recompute (扣成本 + TWII TR 調整)

Council R2 共識：先確認 backtest 是否扣交易成本 → 驗證結果「沒扣」。
  - 策略側：gross fwd_20d，未扣 TW round-trip cost
  - Benchmark 側：^TWII price index，未含股息

本工具：
  (a) 逐 rebalance 追蹤 Value/QM 實際 holdings → 算真實 turnover
  (b) 每次 rebal 扣成本 = turnover × round_trip_cost
  (c) TWII 加股息（每月 3.5%/12 approx，配合月頻 rebal）
  (d) Gross / Net / TWII price / TWII TR 四欄並列
  (e) Annual breakdown + 2016-2022 pre-AI vs 2023-2025 AI era 分段

Cost model（可調）:
  ROUND_TRIP = 0.004 (0.4% — 手續費 0.1425×2 打 6 折 + 證交稅 0.3% ≈ 0.47%, 保守設 0.4%)
  DIVIDEND_YIELD_ANNUAL = 0.035 (台股平均殖利率估計)

Output: reports/vf_step_c_prime_net_alpha.md + .csv
"""
import argparse
import numpy as np
import pandas as pd
from pathlib import Path

from vf_step1_dual_mcap_grid import (
    QM_SNAPSHOT, apply_stage1, compute_live_score, add_tv_rank, load_twii,
    classify_regime_at, ROOT, REBALANCE_EVERY,
)

LONG_SNAPSHOT = ROOT / 'data_cache/backtest/trade_journal_value_tw_long_snapshot.parquet'
OUT_CSV = ROOT / 'reports/vf_step_c_prime_net_alpha.csv'
OUT_MD = ROOT / 'reports/vf_step_c_prime_net_alpha.md'

# Cost model
ROUND_TRIP_COST = 0.004        # 0.4% round-trip TW stock
DIVIDEND_YIELD_ANNUAL = 0.035  # 台股平均殖利率 3.5%/yr
DIVIDEND_PER_REBAL = DIVIDEND_YIELD_ANNUAL / 13  # spread across 13 rebals


def compute_turnover(old_set, new_set):
    """Fraction of portfolio replaced.

    turnover = |positions dropped| / |positions held|
    (single-direction; cost applies once per round-trip per changed position)
    """
    if len(new_set) == 0:
        return 0.0
    dropped = len(old_set - new_set)
    return dropped / len(new_set)


def backtest_with_costs(stage1_ranked, qm_df, twii_close, top_n=20, mcap_cutoff='all'):
    """Dual-all + only_volatile 3-leg with per-rebalance turnover tracking."""
    if mcap_cutoff != 'all':
        from vf_step1_dual_mcap_grid import MCAP_CUTOFFS
        stage1_ranked = stage1_ranked[stage1_ranked['tv_pct'] <= MCAP_CUTOFFS[mcap_cutoff]].copy()

    weeks = sorted(stage1_ranked['week_end_date'].unique())
    rebalance_weeks = weeks[::REBALANCE_EVERY]

    # Track holdings to compute turnover
    last_value_set = set()
    last_qm_set = set()

    rows = []
    for wk in rebalance_weeks:
        regime = classify_regime_at(wk, twii_close)
        is_volatile = (regime == 'volatile')

        # Value leg
        val_ret_gross = 0.0
        val_turnover = 0.0
        curr_value_set = set()
        if is_volatile:
            vpool = stage1_ranked[stage1_ranked['week_end_date'] == wk]
            if not vpool.empty:
                vtop = vpool.nlargest(min(top_n, len(vpool)), 'v_score_live')
                val_ret_gross = vtop['fwd_20d'].mean()
                curr_value_set = set(vtop['stock_id'].astype(str).tolist())
                # Turnover: what fraction of last holdings got dropped
                if last_value_set:
                    val_turnover = compute_turnover(last_value_set, curr_value_set)
                else:
                    val_turnover = 1.0  # initial entry
        else:
            # 非 volatile 週 Value 側全空手 = 全部清倉
            if last_value_set:
                val_turnover = 1.0  # full exit (sell all existing)
            # Note: turnover for 空手 applies to exiting but nothing to enter
        val_ret_net = val_ret_gross - val_turnover * ROUND_TRIP_COST * 0.5  # sell-only = half round-trip
        if is_volatile:
            val_ret_net = val_ret_gross - val_turnover * ROUND_TRIP_COST  # full round-trip for changed positions
        last_value_set = curr_value_set

        # QM leg
        qm_ret_gross = 0.0
        qm_turnover = 0.0
        curr_qm_set = set()
        qpool = qm_df[qm_df['week_end_date'] == wk]
        if not qpool.empty:
            qtop = qpool[qpool['rank_in_top50'] <= top_n]
            qm_ret_gross = qtop['fwd_20d'].mean()
            curr_qm_set = set(qtop['stock_id'].astype(str).tolist())
            if last_qm_set:
                qm_turnover = compute_turnover(last_qm_set, curr_qm_set)
            else:
                qm_turnover = 1.0
        qm_ret_net = qm_ret_gross - qm_turnover * ROUND_TRIP_COST
        last_qm_set = curr_qm_set

        combined_gross = 0.5 * val_ret_gross + 0.5 * qm_ret_gross
        combined_net = 0.5 * val_ret_net + 0.5 * qm_ret_net

        rows.append({
            'date': wk, 'regime': regime,
            'val_ret_gross': val_ret_gross,
            'val_ret_net': val_ret_net,
            'val_turnover': val_turnover,
            'qm_ret_gross': qm_ret_gross,
            'qm_ret_net': qm_ret_net,
            'qm_turnover': qm_turnover,
            'combined_gross': combined_gross,
            'combined_net': combined_net,
        })

    return pd.DataFrame(rows).sort_values('date').reset_index(drop=True)


def twii_per_rebal(rebal_dates, twii_close, include_dividend=True):
    """TWII 每 rebal 期報酬（20 交易日窗口），含股息選項。"""
    dates = sorted(twii_close.index)
    dates_arr = np.array(dates)
    rows = []
    for d in rebal_dates:
        idx = np.searchsorted(dates_arr, d, side='right') - 1
        if idx < 0 or idx + 20 >= len(dates_arr):
            rows.append({'date': d, 'twii_price_ret': np.nan, 'twii_tr_ret': np.nan})
            continue
        p0 = twii_close.iloc[idx]
        p1 = twii_close.iloc[idx + 20]
        price_ret = (p1 / p0) - 1
        tr_ret = price_ret + (DIVIDEND_PER_REBAL if include_dividend else 0)
        rows.append({'date': d, 'twii_price_ret': price_ret, 'twii_tr_ret': tr_ret})
    return pd.DataFrame(rows)


def annual_summary(pr, twii_df):
    """Merge rebal-level data and compute annual CAGR / alpha."""
    df = pr.merge(twii_df, on='date', how='inner')
    df['year'] = pd.to_datetime(df['date']).dt.year

    rows = []
    for yr, g in df.groupby('year'):
        if len(g) < 3:
            continue
        g = g.sort_values('date').reset_index(drop=True)

        def _ann(col):
            cum = (1 + g[col].fillna(0)).cumprod().iloc[-1]
            return (cum - 1) * 100

        row = {
            'year': int(yr),
            'n_rebal': len(g),
            'dual_gross_pct': round(_ann('combined_gross'), 2),
            'dual_net_pct': round(_ann('combined_net'), 2),
            'val_gross': round(_ann('val_ret_gross'), 2),
            'val_net': round(_ann('val_ret_net'), 2),
            'qm_gross': round(_ann('qm_ret_gross'), 2),
            'qm_net': round(_ann('qm_ret_net'), 2),
            'twii_price_pct': round(_ann('twii_price_ret'), 2),
            'twii_tr_pct': round(_ann('twii_tr_ret'), 2),
            'avg_val_turnover': round(g['val_turnover'].mean(), 3),
            'avg_qm_turnover': round(g['qm_turnover'].mean(), 3),
        }
        row['alpha_net_vs_tr'] = round(row['dual_net_pct'] - row['twii_tr_pct'], 2)
        row['alpha_gross_vs_price'] = round(row['dual_gross_pct'] - row['twii_price_pct'], 2)
        rows.append(row)

    return pd.DataFrame(rows)


def period_cagr(df, start_year, end_year, col):
    sub = df[(df['year'] >= start_year) & (df['year'] <= end_year)]
    if len(sub) == 0:
        return None
    cum = 1.0
    for r in sub[col]:
        cum *= (1 + r / 100)
    n = len(sub)
    return (cum ** (1 / n) - 1) * 100


def main():
    global ROUND_TRIP_COST, DIVIDEND_PER_REBAL
    ap = argparse.ArgumentParser()
    ap.add_argument('--cost', type=float, default=ROUND_TRIP_COST,
                    help=f'Round-trip cost ratio (default {ROUND_TRIP_COST})')
    ap.add_argument('--dividend', type=float, default=DIVIDEND_YIELD_ANNUAL,
                    help=f'Annual dividend yield for TWII TR (default {DIVIDEND_YIELD_ANNUAL})')
    args = ap.parse_args()
    ROUND_TRIP_COST = args.cost
    DIVIDEND_PER_REBAL = args.dividend / 13

    print(f'=== Step C\' — Net Alpha Recompute ===')
    print(f'Cost model: round_trip={ROUND_TRIP_COST*100:.2f}%')
    print(f'TWII TR: dividend={args.dividend*100:.2f}%/yr')
    print()

    # Load
    df = pd.read_parquet(LONG_SNAPSHOT)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df['pb'] = df['pb'] / 10.0  # PB scale fix
    stage1 = apply_stage1(df)
    stage1['v_score_live'] = compute_live_score(stage1)
    stage1 = add_tv_rank(stage1)

    qm_df = pd.read_parquet(QM_SNAPSHOT)
    qm_df['week_end_date'] = pd.to_datetime(qm_df['week_end_date'])
    qm_df = qm_df[qm_df['week_end_date'] >= df['week_end_date'].min()]

    twii_close = load_twii()

    # Backtest with turnover tracking
    pr = backtest_with_costs(stage1, qm_df, twii_close, top_n=20)
    twii_df = twii_per_rebal(pr['date'].tolist(), twii_close)
    annual = annual_summary(pr, twii_df)

    # Period CAGR
    periods = [
        ('Pre-AI 2016-2022', 2016, 2022),
        ('AI era 2023-2025',  2023, 2025),
        ('Full 2016-2025',    2016, 2025),
    ]
    period_rows = []
    for name, y0, y1 in periods:
        dg = period_cagr(annual, y0, y1, 'dual_gross_pct')
        dn = period_cagr(annual, y0, y1, 'dual_net_pct')
        tp = period_cagr(annual, y0, y1, 'twii_price_pct')
        tt = period_cagr(annual, y0, y1, 'twii_tr_pct')
        period_rows.append({
            'period': name,
            'dual_gross_cagr': round(dg, 2) if dg else None,
            'dual_net_cagr': round(dn, 2) if dn else None,
            'twii_price_cagr': round(tp, 2) if tp else None,
            'twii_tr_cagr': round(tt, 2) if tt else None,
            'alpha_gross_vs_price': round(dg - tp, 2) if dg and tp else None,
            'alpha_net_vs_tr': round(dn - tt, 2) if dn and tt else None,
        })
    period_df = pd.DataFrame(period_rows)

    print('\n=== Annual Summary ===')
    print(annual[['year', 'dual_gross_pct', 'dual_net_pct',
                   'twii_price_pct', 'twii_tr_pct',
                   'alpha_gross_vs_price', 'alpha_net_vs_tr',
                   'avg_val_turnover', 'avg_qm_turnover']].to_string(index=False))

    print('\n=== Period CAGR ===')
    print(period_df.to_string(index=False))

    # Save
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    annual.to_csv(OUT_CSV, index=False)
    period_df.to_csv(OUT_CSV.parent / 'vf_step_c_prime_period.csv', index=False)
    pr.to_csv(OUT_CSV.parent / 'vf_step_c_prime_rebal.csv', index=False)

    # Verdict classification
    full_alpha = period_df[period_df['period'] == 'Full 2016-2025']['alpha_net_vs_tr'].iloc[0]
    preai_alpha = period_df[period_df['period'] == 'Pre-AI 2016-2022']['alpha_net_vs_tr'].iloc[0]
    ai_alpha = period_df[period_df['period'] == 'AI era 2023-2025']['alpha_net_vs_tr'].iloc[0]

    if full_alpha > 0:
        verdict = 'A - SHIP_READY'
        detail = f'Full 10yr net alpha +{full_alpha}pp/yr — ship-ready'
    elif full_alpha >= -3 and preai_alpha > 3:
        verdict = 'B - REGIME_SWITCH_VIABLE'
        detail = f'Full 10yr {full_alpha}pp 但 pre-AI +{preai_alpha}pp → regime switch 避開 AI era 可救'
    elif preai_alpha > 0:
        verdict = 'B2 - REGIME_SWITCH_BORDERLINE'
        detail = f'Pre-AI +{preai_alpha}pp 有 alpha 但 AI era 太差（{ai_alpha}pp）；regime switch 需 execute 精準'
    else:
        verdict = 'C - FUNDAMENTAL_RETHINK'
        detail = f'Pre-AI 也沒 alpha（{preai_alpha}pp），策略框架失敗'

    # MD report
    with open(OUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Step C\' — Net Alpha Recompute\n\n')
        f.write(f'**Verdict**: **{verdict}**\n\n')
        f.write(f'> {detail}\n\n')
        f.write(f'**Cost model**: {ROUND_TRIP_COST*100:.2f}% round-trip × actual turnover\n\n')
        f.write(f'**TWII TR**: price + {args.dividend*100:.2f}%/yr dividend yield\n\n')

        f.write('## 1. Period CAGR 對照\n\n')
        f.write('| Period | Dual gross | Dual net | TWII price | TWII TR | α gross | **α net** |\n')
        f.write('|---|---|---|---|---|---|---|\n')
        for _, r in period_df.iterrows():
            f.write(f'| {r["period"]} | {r["dual_gross_cagr"]}% | {r["dual_net_cagr"]}% | '
                    f'{r["twii_price_cagr"]}% | {r["twii_tr_cagr"]}% | '
                    f'{r["alpha_gross_vs_price"]:+} | **{r["alpha_net_vs_tr"]:+}** |\n')

        f.write('\n## 2. 年度分解\n\n')
        f.write('| Year | Dual gross | Dual net | TWII price | TWII TR | α gross | α net | V turn | Q turn |\n')
        f.write('|---|---|---|---|---|---|---|---|---|\n')
        for _, r in annual.iterrows():
            f.write(f'| {int(r["year"])} | {r["dual_gross_pct"]}% | {r["dual_net_pct"]}% | '
                    f'{r["twii_price_pct"]}% | {r["twii_tr_pct"]}% | '
                    f'{r["alpha_gross_vs_price"]:+.2f} | {r["alpha_net_vs_tr"]:+.2f} | '
                    f'{r["avg_val_turnover"]} | {r["avg_qm_turnover"]} |\n')

        f.write('\n## 3. Leg breakdown\n\n')
        f.write('| Year | Val gross | Val net | QM gross | QM net |\n')
        f.write('|---|---|---|---|---|\n')
        for _, r in annual.iterrows():
            f.write(f'| {int(r["year"])} | {r["val_gross"]}% | {r["val_net"]}% | '
                    f'{r["qm_gross"]}% | {r["qm_net"]}% |\n')

        f.write('\n## 4. Verdict 分流建議\n\n')
        if verdict.startswith('A'):
            f.write('✅ **Ship-ready** — 即使扣成本 + 含息基準，Dual-all 仍 prove alpha。Step D 更新 anchor 文件，live code 已有 Value+QM scan。\n')
        elif verdict.startswith('B'):
            f.write('🔄 **Regime switch 可救** — pre-AI 有 alpha，AI era 崩。下一步 Step D 設計 regime detector（breadth 指標）+ threshold，若 switch cost < 1.5pp 就值得做。\n')
        elif verdict.startswith('B2'):
            f.write('⚠️ **Regime switch borderline** — pre-AI alpha 不夠強勁，AI era 需精準 switch 才救得回。考慮：是否 AI era 是永久 regime（則策略應重構）還是 3 年 outlier（則等 mean reversion）。\n')
        else:
            f.write('❌ **策略框架失敗** — pre-AI 也沒 alpha，非 regime issue 是 factor 結構性弱。建議放棄 Dual-all，走 PEAD / 60/40 固定配置 / 或直接買 0050。\n')

        f.write('\n## 5. Caveats\n\n')
        f.write(f'- Round-trip cost 0.4% 是 TW 股票中等估算（手續費打折 + 證交稅）；ETF 會更低（~0.15%）\n')
        f.write(f'- TWII TR 用 3.5% dividend yield 平均化，實際 2023-2025 台股殖利率偏低（AI 股票殖利率 < 1%）\n')
        f.write(f'- Turnover 計算只看 holdings set 變化，未考慮權重 drift 造成的 rebal 成本\n')
        f.write(f'- non-volatile 週 Value 側全空手 = 全出場 = cost 算 1.0 turnover 半 round-trip\n')
        f.write(f'- 小型股 slippage 未計（可能使實際成本比 0.4% 高 20-30bp）\n')

    print(f'\n== VERDICT: {verdict} ==')
    print(f'{detail}')
    print(f'\nCSV: {OUT_CSV.relative_to(ROOT)}')
    print(f'MD: {OUT_MD.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
