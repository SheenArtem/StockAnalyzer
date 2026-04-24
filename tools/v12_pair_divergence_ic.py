"""V12-pair: 台股同業 Pair Divergence leading signal IC 驗證.

目標：驗證能否用 leading signals 提前區分 pair 在 Convergence / Divergence / Neutral 哪個 regime。

Signal:
  S1 券資比差 = pair[A].short_ratio - pair[B].short_ratio
       （short_balance / margin_balance）
  S2 借券餘額佔流通比差 = pair[A].sbl_balance/shares_out - pair[B].sbl_balance/shares_out
       （近似：用 sbl_balance / 30d avg volume * 1000 當代理，因缺流通股本）
  S3 RS gap 20d 斜率變化（5 日 slope delta）
  S4 外資持股 delta = pair[A].foreign_net_10d_cum - pair[B].foreign_net_10d_cum

Ground truth at T+N (N=20,60):
  diff = pair[B].fwd_ret - pair[A].fwd_ret
  Convergence: diff > +3%
  Divergence: diff < -3%
  Neutral: |diff| <= 3%

Hit rate 定義（signal 顯示 B 弱）：
  signal 指向 B 弱時（券資比 B>A -> S1<0 / 借券 B>A -> S2<0 / RS gap slope 變 A>B -> S3<0 /
                    外資減 B 多於 A -> foreign_10d_A > foreign_10d_B -> S4>0）
  → regime 實際為 Divergence 的比例

分段：
  Pre-AI  2016-2022（但 margin/sbl 只有 2021-04-16 起）
  AI era  2023-2025

輸出：reports/v12_pair_divergence_ic.csv + .md
"""
from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(r'C:/GIT/StockAnalyzer')
CACHE = ROOT / 'data_cache'
CHIP = CACHE / 'chip_history'
REPORTS = ROOT / 'reports'
REPORTS.mkdir(exist_ok=True)

# 12 pair 定義
PAIRS = [
    # (theme, A, A_name, B, B_name)
    ('ai_server_odm',      '2382', '廣達',   '3231', '緯創'),
    ('ai_cooling',         '3017', '奇鋐',   '3324', '雙鴻'),
    ('abf_substrate',      '3037', '欣興',   '3189', '景碩'),
    ('abf_substrate',      '3037', '欣興',   '8046', '南電'),
    ('ccl',                '2383', '台光電', '6274', '台燿'),
    ('pcb_hard',           '2368', '金像電', '3044', '健鼎'),
    ('advanced_test',      '3711', '日月光', '2449', '京元電'),
    ('semi_equipment',     '6515', '穎崴',   '6223', '旺矽'),
    ('semi_equipment',     '6223', '旺矽',   '6510', '中華精測'),
    ('asic_design_service','3443', '創意',   '3661', '世芯'),
    ('silicon_wafer',      '6488', '環球晶', '5483', '中美晶'),
    ('optical_lens',       '3008', '大立光', '3406', '玉晶光'),
]

HORIZONS = [20, 60]
THRESHOLD = 0.03  # 3% regime threshold


def load_price(sid: str) -> pd.DataFrame:
    p = CACHE / f'{sid}_price.csv'
    df = pd.read_csv(p)
    df = df.rename(columns={df.columns[0]: 'date'})
    df['date'] = pd.to_datetime(df['date'])
    df = df[['date', 'Close', 'Volume']].dropna()
    df = df.sort_values('date').reset_index(drop=True)
    return df


def load_chip_for(sid: str) -> pd.DataFrame:
    """合併 institutional + margin + short_sale for 單檔 sid"""
    inst = pd.read_parquet(CHIP / 'institutional.parquet')
    inst = inst[inst['stock_id'] == sid][['date', 'foreign_net']].copy()
    inst['date'] = pd.to_datetime(inst['date'])

    mg = pd.read_parquet(CHIP / 'margin.parquet')
    mg = mg[mg['stock_id'] == sid][['date', 'margin_balance', 'short_balance']].copy()
    mg['date'] = pd.to_datetime(mg['date'])

    ss = pd.read_parquet(CHIP / 'short_sale.parquet')
    ss = ss[ss['stock_id'] == sid][['date', 'sbl_balance']].copy()
    ss['date'] = pd.to_datetime(ss['date'])

    df = inst.merge(mg, on='date', how='outer').merge(ss, on='date', how='outer')
    df = df.sort_values('date').reset_index(drop=True)
    return df


def build_panel(sid: str) -> pd.DataFrame:
    px = load_price(sid)
    chip = load_chip_for(sid)
    df = px.merge(chip, on='date', how='left')
    df = df.sort_values('date').reset_index(drop=True)

    # Short ratio = short_balance / margin_balance （高 = 空方強）
    df['short_ratio'] = np.where(
        df['margin_balance'] > 0,
        df['short_balance'] / df['margin_balance'],
        np.nan,
    )
    # 借券占流通比代理：sbl_balance 直接用（因為缺流通股本，轉為 z-score 在 pair diff 前）
    df['sbl_bal'] = df['sbl_balance']
    # 外資累計 10d
    df['foreign_10d'] = df['foreign_net'].rolling(10, min_periods=5).sum()
    # 20d ret
    df['ret_20d'] = df['Close'].pct_change(20)
    # RS gap slope 需 pair 層級算
    df['ret_1d'] = df['Close'].pct_change(1)

    df['sid'] = sid
    return df


def compute_forward_rets(df: pd.DataFrame, horizons=HORIZONS) -> pd.DataFrame:
    for h in horizons:
        df[f'fwd_{h}d'] = df['Close'].shift(-h) / df['Close'] - 1.0
    return df


def build_pair_signals(A: pd.DataFrame, B: pd.DataFrame) -> pd.DataFrame:
    """依 date 合併 A / B，算 4 個 signal + 未來 regime"""
    merged = A.merge(B, on='date', suffixes=('_a', '_b'))
    merged = merged.sort_values('date').reset_index(drop=True)

    # S1 券資比差 = A - B
    merged['S1_short_ratio_diff'] = merged['short_ratio_a'] - merged['short_ratio_b']
    # S2 借券餘額差（z-score 化後相減：各自 60d 平均）
    for side in ('a', 'b'):
        merged[f'sbl_z_{side}'] = (
            merged[f'sbl_bal_{side}'] - merged[f'sbl_bal_{side}'].rolling(60, min_periods=20).mean()
        ) / merged[f'sbl_bal_{side}'].rolling(60, min_periods=20).std()
    merged['S2_sbl_diff'] = merged['sbl_z_a'] - merged['sbl_z_b']
    # S3 RS gap 斜率（20d_ret 差的 5 日變化）
    merged['rs_gap_20d'] = merged['ret_20d_a'] - merged['ret_20d_b']
    merged['S3_rs_slope_5d'] = merged['rs_gap_20d'] - merged['rs_gap_20d'].shift(5)
    # S4 外資 10d 差 = A - B
    merged['S4_foreign_diff'] = merged['foreign_10d_a'] - merged['foreign_10d_b']

    # Forward regime per horizon
    for h in HORIZONS:
        diff = merged[f'fwd_{h}d_b'] - merged[f'fwd_{h}d_a']  # B - A
        merged[f'regime_{h}d'] = np.select(
            [diff > THRESHOLD, diff < -THRESHOLD],
            ['convergence', 'divergence'],
            default='neutral',
        )
        merged[f'regime_{h}d'] = np.where(diff.isna(), np.nan, merged[f'regime_{h}d'])
    return merged


def eval_signal(
    df: pd.DataFrame,
    signal_col: str,
    signal_direction: str,  # 'b_weak_if_negative' or 'b_weak_if_positive'
    horizon: int,
    date_mask: pd.Series,
) -> dict:
    """當 signal 顯示 B 弱 -> 未來實際 Divergence 機率（hit rate）

    signal_direction:
      'b_weak_if_negative': signal < 0 表 B 弱（ex: S1=A-B，B 券資比高 -> diff<0 -> B 弱）
      'b_weak_if_positive': signal > 0 表 B 弱（ex: S4=A-B foreign 10d，A 流入多於 B -> diff>0 -> B 弱）
    """
    reg_col = f'regime_{horizon}d'
    d = df[date_mask & df[signal_col].notna() & df[reg_col].notna()].copy()
    if len(d) < 30:
        return {'n': len(d), 'b_weak_signal_hit_div': np.nan, 'b_strong_signal_hit_conv': np.nan,
                'overall_acc': np.nan, 'div_rate_baseline': np.nan, 'conv_rate_baseline': np.nan}

    # 中位數拆正負兩側避免 0 附近的 noise
    if signal_direction == 'b_weak_if_negative':
        b_weak = d[signal_col] < 0
        b_strong = d[signal_col] > 0
    else:
        b_weak = d[signal_col] > 0
        b_strong = d[signal_col] < 0

    div_rate_baseline = (d[reg_col] == 'divergence').mean()
    conv_rate_baseline = (d[reg_col] == 'convergence').mean()

    # B 弱 signal 命中 divergence (A 強 B 弱, gap 擴大)
    if b_weak.sum() >= 10:
        hit_div = (d.loc[b_weak, reg_col] == 'divergence').mean()
    else:
        hit_div = np.nan
    # B 強 signal 命中 convergence (B 反超 A)
    if b_strong.sum() >= 10:
        hit_conv = (d.loc[b_strong, reg_col] == 'convergence').mean()
    else:
        hit_conv = np.nan

    # 整體 3-way accuracy（signal sign vs regime）
    # predicted: b_weak -> div, b_strong -> conv, |signal| 小 -> neutral（中間 33% 分位）
    q1, q2 = d[signal_col].quantile([1/3, 2/3])
    if signal_direction == 'b_weak_if_negative':
        pred = np.where(d[signal_col] <= q1, 'divergence',
                np.where(d[signal_col] >= q2, 'convergence', 'neutral'))
    else:
        pred = np.where(d[signal_col] >= q2, 'divergence',
                np.where(d[signal_col] <= q1, 'convergence', 'neutral'))
    overall_acc = (pred == d[reg_col]).mean()

    return {
        'n': int(len(d)),
        'n_b_weak': int(b_weak.sum()),
        'n_b_strong': int(b_strong.sum()),
        'b_weak_signal_hit_div': round(float(hit_div), 4) if not np.isnan(hit_div) else np.nan,
        'b_strong_signal_hit_conv': round(float(hit_conv), 4) if not np.isnan(hit_conv) else np.nan,
        'overall_acc': round(float(overall_acc), 4),
        'div_rate_baseline': round(float(div_rate_baseline), 4),
        'conv_rate_baseline': round(float(conv_rate_baseline), 4),
    }


def verdict(hit_pre, hit_ai) -> str:
    """A/B/C/D verdict based on two-segment hit_div"""
    vals = [v for v in [hit_pre, hit_ai] if not (v is None or (isinstance(v, float) and np.isnan(v)))]
    if len(vals) == 0:
        return 'n/a'
    if all(v > 0.60 for v in vals) and len(vals) == 2:
        return 'A'
    if any(v > 0.60 for v in vals):
        return 'B'
    if all(v < 0.25 for v in vals):  # 明顯反向（<1/4 機率 vs baseline ~1/3）
        return 'D'
    return 'C'


def main():
    print('>>> V12-pair: loading 12 pairs...')
    rows = []
    ai_start = pd.Timestamp('2023-01-01')
    pre_ai_start = pd.Timestamp('2016-01-01')  # margin/sbl 實際從 2021-04-16 起
    ai_end = pd.Timestamp('2025-12-31')

    signal_cfg = {
        'S1_short_ratio_diff': 'b_weak_if_negative',  # B 券資比高 (空方看空 B) -> diff<0 -> B 弱
        'S2_sbl_diff':         'b_weak_if_negative',  # B 借券餘額高 -> diff<0 -> B 弱 (被放空)
        'S3_rs_slope_5d':      'b_weak_if_positive',  # RS gap (A-B) 擴大中 -> slope>0 -> B 弱
        'S4_foreign_diff':     'b_weak_if_positive',  # 外資流入 A 多於 B -> diff>0 -> B 弱
    }

    for theme, a_sid, a_name, b_sid, b_name in PAIRS:
        print(f'  pair: {a_sid}({a_name}) vs {b_sid}({b_name}) [{theme}]')
        try:
            A = compute_forward_rets(build_panel(a_sid))
            B = compute_forward_rets(build_panel(b_sid))
        except Exception as e:
            print(f'   !! load fail: {e}')
            continue
        merged = build_pair_signals(A, B)

        for sig, direction in signal_cfg.items():
            for h in HORIZONS:
                for seg_name, mask in [
                    ('pre_ai_2021-2022', (merged['date'] >= pd.Timestamp('2021-04-16')) &
                                         (merged['date'] < ai_start)),
                    ('ai_era_2023-2025', (merged['date'] >= ai_start) &
                                         (merged['date'] <= ai_end)),
                ]:
                    res = eval_signal(merged, sig, direction, h, mask)
                    rows.append({
                        'theme': theme,
                        'pair': f'{a_sid}_{a_name}_vs_{b_sid}_{b_name}',
                        'signal': sig,
                        'horizon_d': h,
                        'segment': seg_name,
                        **res,
                    })

    df = pd.DataFrame(rows)
    out_csv = REPORTS / 'v12_pair_divergence_ic.csv'
    df.to_csv(out_csv, index=False, encoding='utf-8-sig')
    print(f'>>> 寫出 {out_csv}')

    # ===== 聚合 verdict per signal × horizon =====
    print('>>> aggregating verdicts...')
    agg_rows = []
    for sig in signal_cfg.keys():
        for h in HORIZONS:
            sub = df[(df['signal'] == sig) & (df['horizon_d'] == h)]
            if len(sub) == 0:
                continue
            pre = sub[sub['segment'] == 'pre_ai_2021-2022']
            ai = sub[sub['segment'] == 'ai_era_2023-2025']

            # pair-weighted average（以 n 加權）
            def wavg(g, col):
                g2 = g[g[col].notna()]
                if len(g2) == 0 or g2['n'].sum() == 0:
                    return np.nan
                return (g2[col] * g2['n']).sum() / g2['n'].sum()

            pre_hit_div = wavg(pre, 'b_weak_signal_hit_div')
            ai_hit_div = wavg(ai, 'b_weak_signal_hit_div')
            pre_hit_conv = wavg(pre, 'b_strong_signal_hit_conv')
            ai_hit_conv = wavg(ai, 'b_strong_signal_hit_conv')
            pre_div_base = wavg(pre, 'div_rate_baseline')
            ai_div_base = wavg(ai, 'div_rate_baseline')
            pre_acc = wavg(pre, 'overall_acc')
            ai_acc = wavg(ai, 'overall_acc')

            v = verdict(pre_hit_div, ai_hit_div)
            agg_rows.append({
                'signal': sig,
                'horizon_d': h,
                'n_pairs': sub['pair'].nunique(),
                'pre_ai_hit_div(B 弱→實際 div)': round(pre_hit_div, 4) if pd.notna(pre_hit_div) else np.nan,
                'ai_era_hit_div': round(ai_hit_div, 4) if pd.notna(ai_hit_div) else np.nan,
                'pre_ai_hit_conv(B 強→實際 conv)': round(pre_hit_conv, 4) if pd.notna(pre_hit_conv) else np.nan,
                'ai_era_hit_conv': round(ai_hit_conv, 4) if pd.notna(ai_hit_conv) else np.nan,
                'pre_ai_div_baseline': round(pre_div_base, 4) if pd.notna(pre_div_base) else np.nan,
                'ai_era_div_baseline': round(ai_div_base, 4) if pd.notna(ai_div_base) else np.nan,
                'pre_ai_overall_acc': round(pre_acc, 4) if pd.notna(pre_acc) else np.nan,
                'ai_era_overall_acc': round(ai_acc, 4) if pd.notna(ai_acc) else np.nan,
                'verdict': v,
            })
    agg = pd.DataFrame(agg_rows)
    agg_csv = REPORTS / 'v12_pair_divergence_ic_agg.csv'
    agg.to_csv(agg_csv, index=False, encoding='utf-8-sig')
    print(f'>>> 寫出 {agg_csv}')
    print('\n=== 聚合結果 ===')
    print(agg.to_string(index=False))

    # ===== AI era vs Pre-AI: Divergence 實際發生率 =====
    print('\n=== AI era vs Pre-AI: Divergence 發生率對比 ===')
    div_compare = df.groupby(['pair', 'horizon_d', 'segment']).agg(
        div_baseline=('div_rate_baseline', 'first'),
        n=('n', 'first'),
    ).reset_index()
    piv = div_compare.pivot_table(
        index=['pair', 'horizon_d'],
        columns='segment',
        values='div_baseline',
    ).reset_index()
    if 'pre_ai_2021-2022' in piv.columns and 'ai_era_2023-2025' in piv.columns:
        piv['div_delta_ai_minus_pre'] = piv['ai_era_2023-2025'] - piv['pre_ai_2021-2022']
    print(piv.to_string(index=False))
    piv.to_csv(REPORTS / 'v12_pair_div_rate_by_segment.csv', index=False, encoding='utf-8-sig')

    return df, agg, piv


if __name__ == '__main__':
    main()
