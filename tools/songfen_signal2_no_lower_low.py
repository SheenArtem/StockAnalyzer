"""
宋分擇時 Signal #2 — 「利空不再破底」 event study

訊號論點（宋分框架）：
  機構看風險是否開始下降——利空頻率不減但股價不再破前低 → 進場訊號

量化設計（嚴格版本）：
  Trigger 條件 (date t, stock s):
    C1: 近 20 個交易日內 TWII 至少 3 天 daily_ret <= -0.5% (利空頻率仍高)
    C2: stock 在 t 的 Low > rolling 60-day Low (從 t-60 到 t-1) - 0.5%（容忍微破）
    C3: 在那些 TWII <= -0.5% 的負面日中, stock 收紅 (stock_ret > 0) 比率 >= 50% (bounce rate)
  三條件全滿足 → trigger

CAR 計算 (與 signal1 一致):
  fwd_h = Close(t+h) / Close(t) - 1
  CAR_h = stock_fwd_h - twii_fwd_h

Regime split: bull/bear by TWII Close vs 200-day MA, vol regime by TWII 20d realized vol

Liquidity filter: avg_vol_20 >= 200K, price >= 5

預期結果（先驗）:
  類似 signal1 受 TWII proxy 限制 → 預期 D 級
  關鍵差異: signal2 樣本更稀少 (3 重 AND 條件), 訊噪比可能更高
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
OHLCV = ROOT / "data_cache/backtest/ohlcv_tw.parquet"
TWII_BENCH = ROOT / "data_cache/backtest/_twii_bench.parquet"
OUT_CSV = ROOT / "reports/songfen_signal2_no_lower_low.csv"
OUT_MD = ROOT / "reports/songfen_signal2_no_lower_low.md"

FWD_HORIZONS = [1, 5, 10, 20]
MIN_PRICE = 5.0
MIN_AVG_VOL = 200_000
TWII_BAD_THR = -0.005  # -0.5% 視為當日大盤利空
LOOKBACK_BAD_DAYS = 20
LOW_LOOKBACK = 60
MIN_BAD_DAYS = 3
MIN_BOUNCE_RATE = 0.5


def log(msg):
    print(f"[{pd.Timestamp.now():%H:%M:%S}] {msg}", flush=True)


def load_data():
    log("Loading TWII benchmark + TW OHLCV...")
    twii = pd.read_parquet(TWII_BENCH)
    twii.columns = [c[0] if isinstance(c, tuple) else c for c in twii.columns]
    twii.index = pd.to_datetime(twii.index)
    twii = twii.sort_index()
    twii_ret = twii['Close'].pct_change()
    twii_close = twii['Close']
    twii_ma200 = twii_close.rolling(200, min_periods=120).mean()
    twii_rv20 = twii_ret.rolling(20, min_periods=15).std()

    twii_panel = pd.DataFrame({
        'twii_close': twii_close,
        'twii_ret': twii_ret,
        'regime_bull': (twii_close >= twii_ma200).astype('Int8'),
        'twii_rv20': twii_rv20,
        'twii_bad': (twii_ret <= TWII_BAD_THR).astype(int),
    })

    ohlcv = pd.read_parquet(OHLCV, columns=['stock_id', 'date', 'Open', 'Close', 'High', 'Low', 'Volume'])
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])
    ohlcv['stock_id'] = ohlcv['stock_id'].astype(str)
    ohlcv = ohlcv.sort_values(['stock_id', 'date']).reset_index(drop=True)
    ohlcv = ohlcv.dropna(subset=['Close', 'Low'])
    log(f"  TWII: {len(twii_panel)} days; OHLCV: {ohlcv['stock_id'].nunique()} stocks × {len(ohlcv):,} rows")
    return twii_panel, ohlcv


def build_panel(ohlcv, twii_panel):
    log("Building per-stock panel + signals (vectorized)...")
    g = ohlcv.groupby('stock_id', sort=False)

    ohlcv['prev_close'] = g['Close'].shift(1)
    ohlcv['stock_ret'] = ohlcv['Close'] / ohlcv['prev_close'] - 1
    # 60-day rolling low (excluding current day) for "didn't break low" check
    ohlcv['low60_excl'] = g['Low'].transform(
        lambda s: s.shift(1).rolling(LOW_LOOKBACK, min_periods=40).min()
    )
    ohlcv['avg_vol20'] = g['Volume'].transform(
        lambda s: s.shift(1).rolling(20, min_periods=15).mean()
    )

    for h in FWD_HORIZONS:
        ohlcv[f'fwd_{h}d'] = g['Close'].shift(-h) / ohlcv['Close'] - 1

    panel = ohlcv.merge(twii_panel[['twii_ret', 'regime_bull', 'twii_rv20', 'twii_bad']],
                        left_on='date', right_index=True, how='left')

    panel['liquid'] = (panel['Close'] >= MIN_PRICE) & (panel['avg_vol20'] >= MIN_AVG_VOL)
    return panel


def compute_signal2(panel):
    """C1 + C2 + C3 vectorized."""
    log("Computing signal #2 (利空不再破底) per stock...")

    # C1: rolling 20-day count of TWII bad days (just date-based, same for all stocks)
    twii_bad_by_date = panel.groupby('date')['twii_bad'].first()
    twii_bad_count_20 = twii_bad_by_date.rolling(LOOKBACK_BAD_DAYS, min_periods=15).sum()
    c1_dates = twii_bad_count_20[twii_bad_count_20 >= MIN_BAD_DAYS].index
    log(f"  C1 (rolling 20d bad TWII >= {MIN_BAD_DAYS}): {len(c1_dates)} qualifying dates")

    # Restrict panel to liquid + C1 dates + C2 (didn't break 60d low)
    base = panel[panel['liquid'] & panel['date'].isin(c1_dates) & panel['low60_excl'].notna()].copy()
    # C2: Low(t) > low60_excl(t) * 0.995 (容忍 0.5% 微破)
    base['c2_pass'] = base['Low'] > base['low60_excl'] * 0.995
    base = base[base['c2_pass']].copy()
    log(f"  After C2 (didn't break 60d low): {len(base):,} stock-rows")

    # C3: bounce rate >= 50% in last 20 days where TWII was bad
    # = sum(stock_ret > 0 & twii_bad) / sum(twii_bad) over rolling 20d window
    # 為了 vectorize，per stock 計算: sum(positive_on_bad_day) over 20d / sum(bad_day) over 20d
    # 但 sum(bad_day) over 20d 對所有 stocks 在同一日相同（因為 twii_bad 是日期屬性）
    # sum(positive_on_bad_day) over 20d 是 per-stock

    # Pre-compute "positive on bad day" 標記
    panel['pos_on_bad'] = ((panel['stock_ret'] > 0) & (panel['twii_bad'] == 1)).astype(int)

    # Per-stock rolling 20d sum
    panel = panel.sort_values(['stock_id', 'date'])
    panel['pos_bad_20'] = panel.groupby('stock_id', sort=False)['pos_on_bad'].transform(
        lambda s: s.rolling(LOOKBACK_BAD_DAYS, min_periods=15).sum()
    )

    # Merge bounce_rate denominator (twii_bad_count_20 by date) back to panel
    bounce_denom = pd.Series(twii_bad_count_20, name='twii_bad_count_20')
    panel = panel.merge(bounce_denom, left_on='date', right_index=True, how='left')
    panel['bounce_rate'] = panel['pos_bad_20'] / panel['twii_bad_count_20'].replace(0, np.nan)

    # Now restrict base to those with bounce_rate >= 0.5
    base = base.merge(panel[['date', 'stock_id', 'bounce_rate']],
                      on=['date', 'stock_id'], how='left')
    base = base[base['bounce_rate'] >= MIN_BOUNCE_RATE].copy()
    log(f"  After C3 (bounce_rate >= {MIN_BOUNCE_RATE}): {len(base):,} triggers")
    return base


def compute_car(triggers, panel, twii_panel):
    """fwd_h(stock) - fwd_h(TWII) per event."""
    log("Computing CAR per event...")
    twii_close = twii_panel['twii_close']
    for h in FWD_HORIZONS:
        twii_fwd = twii_close.shift(-h) / twii_close - 1
        triggers[f'twii_fwd_{h}d'] = triggers['date'].map(twii_fwd)
        triggers[f'CAR_{h}d'] = triggers[f'fwd_{h}d'] - triggers[f'twii_fwd_{h}d']
    return triggers


def evaluate(triggers):
    """Aggregate CAR, t-stat, and regime breakdown."""
    rows = []
    overall = triggers.dropna(subset=[f'CAR_{h}d' for h in FWD_HORIZONS])
    rows.append({'group': 'all', 'n': len(overall),
                 **{f'CAR_{h}d_mean': overall[f'CAR_{h}d'].mean() for h in FWD_HORIZONS},
                 **{f'CAR_{h}d_t': stats.ttest_1samp(overall[f'CAR_{h}d'].dropna(), 0)[0]
                    if overall[f'CAR_{h}d'].dropna().shape[0] > 1 else np.nan for h in FWD_HORIZONS}})

    for label, mask in [
        ('regime_bull', triggers['regime_bull'] == 1),
        ('regime_bear', triggers['regime_bull'] == 0),
        ('vol_high',
         triggers['twii_rv20'] >= triggers['twii_rv20'].quantile(0.67)),
        ('vol_low',
         triggers['twii_rv20'] <= triggers['twii_rv20'].quantile(0.33)),
    ]:
        sub = triggers[mask].dropna(subset=[f'CAR_{h}d' for h in FWD_HORIZONS])
        if len(sub) < 30:
            continue
        rows.append({'group': label, 'n': len(sub),
                     **{f'CAR_{h}d_mean': sub[f'CAR_{h}d'].mean() for h in FWD_HORIZONS},
                     **{f'CAR_{h}d_t': stats.ttest_1samp(sub[f'CAR_{h}d'].dropna(), 0)[0]
                        if sub[f'CAR_{h}d'].dropna().shape[0] > 1 else np.nan for h in FWD_HORIZONS}})

    df = pd.DataFrame(rows)
    return df


def grade(car_10, t_10):
    """A: CAR>2% & t>2; B: CAR>1% & t>1.5; rest C/D."""
    if pd.isna(car_10) or pd.isna(t_10):
        return 'n/a'
    if car_10 > 0.02 and t_10 > 2:
        return 'A'
    if car_10 > 0.01 and t_10 > 1.5:
        return 'B'
    if car_10 > 0.005 and t_10 > 1:
        return 'C'
    return 'D'


def main():
    twii_panel, ohlcv = load_data()
    panel = build_panel(ohlcv, twii_panel)
    triggers = compute_signal2(panel)

    if len(triggers) < 100:
        log(f"FAIL: too few triggers ({len(triggers)}). Check thresholds.")
        sys.exit(1)

    triggers = compute_car(triggers, panel, twii_panel)
    summary = evaluate(triggers)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT_CSV, index=False)
    log(f"Saved: {OUT_CSV}")

    # Console summary
    print(f"\n=== Signal #2 'No Lower Low' Event Study ===")
    print(f"Triggers: {len(triggers):,}")
    print(f"Date range: {triggers['date'].min().date()} ~ {triggers['date'].max().date()}")
    print(f"Unique stocks: {triggers['stock_id'].nunique()}")
    print()
    print(f"{'Group':15s} {'n':>8s} {'CAR_1d':>9s} {'CAR_5d':>9s} {'CAR_10d':>9s} {'CAR_20d':>9s} {'t10':>7s} {'Grade':>6s}")
    print("-" * 90)
    for _, r in summary.iterrows():
        g = grade(r.get('CAR_10d_mean'), r.get('CAR_10d_t'))
        print(f"{r['group']:15s} {r['n']:>8.0f} "
              f"{r.get('CAR_1d_mean', np.nan):>+9.4f} "
              f"{r.get('CAR_5d_mean', np.nan):>+9.4f} "
              f"{r.get('CAR_10d_mean', np.nan):>+9.4f} "
              f"{r.get('CAR_20d_mean', np.nan):>+9.4f} "
              f"{r.get('CAR_10d_t', np.nan):>+7.2f} "
              f"{g:>6s}")

    overall_car_10 = summary.loc[summary['group'] == 'all', 'CAR_10d_mean'].iloc[0]
    overall_t_10 = summary.loc[summary['group'] == 'all', 'CAR_10d_t'].iloc[0]
    final = grade(overall_car_10, overall_t_10)

    md = f"""# Signal #2 「利空不再破底」 Event Study

**Date**: 2026-04-29
**Period**: {triggers['date'].min().date()} ~ {triggers['date'].max().date()}
**Triggers**: {len(triggers):,} (across {triggers['stock_id'].nunique()} stocks)

## Conditions

- C1: rolling 20-day, TWII <= -0.5% 至少 {MIN_BAD_DAYS} 天
- C2: stock Low(t) > rolling 60-day Low (容忍 0.5% 微破)
- C3: bounce_rate >= {MIN_BOUNCE_RATE} (在 TWII 利空日內個股收紅比例)

## Result Summary

| Group | n | CAR_1d | CAR_5d | CAR_10d | CAR_20d | t-stat | Grade |
|---|---:|---:|---:|---:|---:|---:|:--:|
"""
    for _, r in summary.iterrows():
        g = grade(r.get('CAR_10d_mean'), r.get('CAR_10d_t'))
        md += (f"| {r['group']} | {r['n']:.0f} | "
               f"{r.get('CAR_1d_mean', 0):+.4f} | "
               f"{r.get('CAR_5d_mean', 0):+.4f} | "
               f"{r.get('CAR_10d_mean', 0):+.4f} | "
               f"{r.get('CAR_20d_mean', 0):+.4f} | "
               f"{r.get('CAR_10d_t', 0):+.2f} | {g} |\n")

    md += f"""
## Verdict

Overall CAR_10d = {overall_car_10:+.4f} (t={overall_t_10:+.2f}) → **{final}**

Grading rule:
- A: CAR_10d > 2% AND t > 2
- B: CAR_10d > 1% AND t > 1.5
- C: CAR_10d > 0.5% AND t > 1
- D: 否則

Compared to Signal #1 (D 歸檔)，#2 樣本稀少且 3 重 AND 條件理論上訊噪比更高。
若 final 仍 D，原因同 #1：大盤 proxy 不足以代表「真利空 events」。
"""
    OUT_MD.write_text(md, encoding='utf-8')
    log(f"Saved: {OUT_MD}")
    print(f"\n=== Final Verdict: {final} ===")


if __name__ == '__main__':
    main()
