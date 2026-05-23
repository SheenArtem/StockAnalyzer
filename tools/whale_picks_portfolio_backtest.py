"""
Whale Picks Portfolio-level backtest vs TWII benchmark.

從 trade_ledger.parquet 重建 equal-weight K=10 monthly portfolio，計算每日 NAV，
產生跟 TWII (^TWII) 的對比 stats + year-by-year alpha breakdown。

Output:
  - data/whale_picks/portfolio_nav.parquet   (daily NAV + return)
  - data/whale_picks/portfolio_annual.parquet (yearly return vs TWII)
  - data/whale_picks/portfolio_stats.json     (CAGR/Sharpe/MDD/etc summary)

Usage:
  python tools/whale_picks_portfolio_backtest.py
  python tools/whale_picks_portfolio_backtest.py --start 2016-01-15 --end 2026-05-15
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

LEDGER_PATH = REPO / "data" / "whale_picks" / "trade_ledger.parquet"
OHLCV_PATH = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
TWII_PATH = REPO / "data_cache" / "backtest" / "_twii_bench.parquet"
OUT_DIR = REPO / "data" / "whale_picks"
NAV_PATH = OUT_DIR / "portfolio_nav.parquet"
ANNUAL_PATH = OUT_DIR / "portfolio_annual.parquet"
STATS_PATH = OUT_DIR / "portfolio_stats.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("portfolio_bt")


def load_data():
    ledger = pd.read_parquet(LEDGER_PATH)
    ledger['entry_date'] = pd.to_datetime(ledger['entry_date'])
    ledger['exit_date'] = pd.to_datetime(ledger['exit_date'])

    ohlcv = pd.read_parquet(OHLCV_PATH, columns=['stock_id', 'date', 'Close'])
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])

    twii = pd.read_parquet(TWII_PATH)
    twii.columns = [c[0] if isinstance(c, tuple) else c for c in twii.columns]
    twii = twii.reset_index().rename(columns={'index': 'date', 'Date': 'date'})
    twii['date'] = pd.to_datetime(twii['date'])

    return ledger, ohlcv, twii


def run_backtest(ledger, ohlcv, twii, start: str, end: str):
    start_d = pd.to_datetime(start)
    end_d = pd.to_datetime(end)

    ohlcv_dates = pd.DatetimeIndex(sorted(ohlcv['date'].unique()))
    trading_days = ohlcv_dates[(ohlcv_dates >= start_d) & (ohlcv_dates <= end_d)]
    log.info("Backtest period: %s ~ %s (%d trading days)",
             start_d.date(), end_d.date(), len(trading_days))

    ohlcv_wide = ohlcv.pivot(index='date', columns='stock_id', values='Close')
    ohlcv_ret = ohlcv_wide.pct_change(fill_method=None)

    port_returns = []
    for t in trading_days:
        held_mask = (ledger['entry_date'] <= t) & (
            (ledger['exit_date'].isna()) | (ledger['exit_date'] > t)
        )
        held_stocks = ledger.loc[held_mask, 'stock_id'].tolist()
        if len(held_stocks) == 0:
            port_returns.append((t, 0.0, 0))
            continue
        valid = []
        for sid in held_stocks:
            if sid in ohlcv_ret.columns and t in ohlcv_ret.index:
                r = ohlcv_ret.loc[t, sid]
                if pd.notna(r):
                    valid.append(r)
        port_returns.append((t, float(np.mean(valid)) if valid else 0.0, len(held_stocks)))

    port_df = pd.DataFrame(port_returns, columns=['date', 'ret', 'n_held'])
    port_df['nav'] = (1 + port_df['ret']).cumprod()

    twii_period = twii[(twii['date'] >= start_d) & (twii['date'] <= end_d)].copy()
    twii_period['ret'] = twii_period['Close'].pct_change(fill_method=None)
    twii_period['nav'] = (1 + twii_period['ret'].fillna(0)).cumprod()
    twii_period = twii_period[['date', 'ret', 'nav']].reset_index(drop=True)

    return port_df, twii_period


def compute_stats(df, name):
    ret = df['ret'].dropna()
    days = len(ret)
    years = days / 252
    cagr = float(df['nav'].iloc[-1] ** (1 / years) - 1) if years > 0 else 0.0
    vol = float(ret.std() * np.sqrt(252))
    sharpe = float((ret.mean() * 252) / vol) if vol > 0 else 0.0
    nav = df['nav']
    peak = nav.cummax()
    dd = (nav - peak) / peak
    mdd = float(dd.min())
    return {
        'name': name,
        'cagr': cagr,
        'sharpe': sharpe,
        'vol': vol,
        'mdd': mdd,
        'total_return': float(df['nav'].iloc[-1] - 1),
        'days': int(days),
        'years': round(years, 2),
    }


def compute_annual(port, twii):
    port = port.copy()
    twii = twii.copy()
    port['year'] = port['date'].dt.year
    twii['year'] = twii['date'].dt.year

    def yearly_ret(df):
        return df.groupby('year').agg(
            ret=('ret', lambda s: (1 + s.dropna()).prod() - 1)
        ).reset_index()

    p_yr = yearly_ret(port).rename(columns={'ret': 'whale_picks'})
    t_yr = yearly_ret(twii).rename(columns={'ret': 'twii'})
    merged = p_yr.merge(t_yr, on='year')
    merged['alpha'] = merged['whale_picks'] - merged['twii']
    return merged


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--start', default=None, help='ISO start date (default: ledger min entry_date)')
    p.add_argument('--end', default=None, help='ISO end date (default: today)')
    args = p.parse_args()

    ledger, ohlcv, twii = load_data()
    if args.start is None:
        start = pd.to_datetime(ledger['entry_date'].min()).strftime('%Y-%m-%d')
    else:
        start = args.start
    if args.end is None:
        end = datetime.now().strftime('%Y-%m-%d')
    else:
        end = args.end

    port_df, twii_period = run_backtest(ledger, ohlcv, twii, start, end)

    port_stats = compute_stats(port_df, 'Whale Picks')
    twii_stats = compute_stats(twii_period, 'TWII')

    annual = compute_annual(port_df, twii_period)
    n_wins = int((annual['alpha'] > 0).sum())
    n_years = len(annual)

    summary = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'start_date': start,
        'end_date': end,
        'whale_picks': port_stats,
        'twii': twii_stats,
        'delta': {
            'cagr_pp': round((port_stats['cagr'] - twii_stats['cagr']) * 100, 2),
            'sharpe': round(port_stats['sharpe'] - twii_stats['sharpe'], 3),
            'vol_pp': round((port_stats['vol'] - twii_stats['vol']) * 100, 2),
            'mdd_pp': round((port_stats['mdd'] - twii_stats['mdd']) * 100, 2),
        },
        'annual_wins': n_wins,
        'annual_years': n_years,
        'annual_hit_rate': round(n_wins / n_years, 3) if n_years > 0 else None,
        'caveats': [
            '未扣交易成本 (估 ~6 round-trips/年 × 0.3% = -1.8%/年 haircut)',
            '未扣 slippage (估 -0.5%/年)',
            '未含台股股息 (TWII/Whale Picks 兩邊都未計，殖利率 ~3-5%/年)',
            '11 年回測，含 2018 熊 / 2020 covid / 2022 Fed / 2024 AI 多個 regime',
            'PIT universe 1958 stocks 已 survivor-bias 修正',
        ],
    }

    # Save outputs
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    port_df.to_parquet(NAV_PATH, index=False)
    # Add twii nav alongside for UI convenience
    nav_combined = port_df[['date', 'ret', 'nav']].rename(
        columns={'ret': 'wp_ret', 'nav': 'wp_nav'}
    ).merge(
        twii_period.rename(columns={'ret': 'twii_ret', 'nav': 'twii_nav'}),
        on='date', how='left'
    )
    nav_combined.to_parquet(NAV_PATH, index=False)

    annual.to_parquet(ANNUAL_PATH, index=False)
    STATS_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    log.info("Saved: %s / %s / %s", NAV_PATH, ANNUAL_PATH, STATS_PATH)
    log.info("Whale Picks: CAGR %.2f%% / Sharpe %.3f / MDD %.2f%% / Total %+.1f%%",
             port_stats['cagr'] * 100, port_stats['sharpe'],
             port_stats['mdd'] * 100, port_stats['total_return'] * 100)
    log.info("TWII:        CAGR %.2f%% / Sharpe %.3f / MDD %.2f%% / Total %+.1f%%",
             twii_stats['cagr'] * 100, twii_stats['sharpe'],
             twii_stats['mdd'] * 100, twii_stats['total_return'] * 100)
    log.info("Beats TWII: %d / %d years (%.0f%%)", n_wins, n_years, 100 * n_wins / n_years)


if __name__ == '__main__':
    main()
