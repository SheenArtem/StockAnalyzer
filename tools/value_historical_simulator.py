"""
Value Historical Simulator
==========================
每週模擬 Value Screener 跑一次，記錄「哪些股票被選出」+ Value 5 面向分數 +
forward return，產出 trade_journal_value_tw.parquet 供 VF-VA / VF-VF 驗證。

用法:
    python tools/value_historical_simulator.py            # full run (2015-2026)
    python tools/value_historical_simulator.py --debug    # 只跑前 10 週
    python tools/value_historical_simulator.py --start 2020-01-01 --end 2025-12-31

Value 5 面向評分 (對齊 live value_screener.py):
  - Valuation 30%:  PE<12 +20 / PB<1.5 +15 / Graham Number
  - Quality   25%:  F-Score + Z-Score (復用 quality_scores.parquet)
  - Revenue   15%:  revenue_score (復用 quality_scores.parquet)
  - Technical 15%:  RSI < 40 / RVOL < 0.7 / 52週低距離 < 20%
  - SmartMoney 15%: [historical proxy 不易，先用 0 placeholder]

每檔初始 50 分，加減 after 5 面向。Top 50 按 value_score 排。

輸出: data_cache/backtest/trade_journal_value_tw.parquet
  欄位: week_end_date, stock_id, entry_price, PE, PB, graham,
        f_score, quality_score, revenue_score, technical_score, smart_money_score,
        value_score, market_cap_tier, regime, mode (= 'default'),
        fwd_5d/10d/20d/40d/60d + fwd_40d_max/min

此 trade_journal 接著供:
  - VF-VA 估值門檻 walk-forward: 看 PE/PB/Graham 門檻的 IC IR
  - VF-VF 5 面向權重驗證: 對比 30/25/15/15/15 vs 其他組合
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_cache" / "backtest"
OUT_PATH = DATA_DIR / "trade_journal_value_tw.parquet"

HORIZONS = [5, 10, 20, 40, 60, 120]
MAX_DRAWDOWN_HORIZONS = [20, 40, 60]
VALUE_TOP_N = 50
MIN_AVG_TV = 3e8           # 3 億（Value Stage 1 門檻較低，找中小型價值）
MIN_PRICE = 10.0

# === Value 5 面向權重（對齊 live 預設，V45 待驗）===
WEIGHTS = {
    'valuation': 0.30,
    'quality': 0.25,
    'revenue': 0.15,
    'technical': 0.15,
    'smart_money': 0.15,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("value_sim")


# ================================================================
# Data Loading
# ================================================================

def load_ohlcv() -> pd.DataFrame:
    logger.info("Loading OHLCV...")
    df = pd.read_parquet(DATA_DIR / "ohlcv_tw.parquet")
    df['date'] = pd.to_datetime(df['date'])
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    logger.info("  OHLCV: %d rows, %d stocks", len(df), df['stock_id'].nunique())
    return df


def load_quality_scores() -> pd.DataFrame:
    logger.info("Loading quality_scores (F-Score + quality + revenue)...")
    qs = pd.read_parquet(DATA_DIR / "quality_scores.parquet")
    qs['date'] = pd.to_datetime(qs['date'])
    logger.info("  quality_scores: %d rows, %d stocks, %s - %s",
                len(qs), qs['stock_id'].nunique(),
                qs['date'].min().date(), qs['date'].max().date())
    return qs


def load_income_eps() -> pd.DataFrame:
    """取得 EPS trailing 12m 等長期價值評估需要的資料。"""
    logger.info("Loading financials_income EPS/revenue...")
    inc = pd.read_parquet(DATA_DIR / "financials_income.parquet")
    inc['date'] = pd.to_datetime(inc['date'])
    # 只留 EPS
    eps = inc[inc['type'] == 'EPS'][['stock_id', 'date', 'value']].copy()
    eps = eps.rename(columns={'value': 'eps_q'})
    eps['eps_q'] = pd.to_numeric(eps['eps_q'], errors='coerce')
    eps = eps.dropna().sort_values(['stock_id', 'date'])
    # TTM EPS = 最近 4 季相加
    eps['eps_ttm'] = eps.groupby('stock_id')['eps_q'].transform(
        lambda x: x.rolling(4, min_periods=4).sum()
    )
    logger.info("  EPS rows: %d, stocks with TTM: %d",
                len(eps), eps.dropna(subset=['eps_ttm'])['stock_id'].nunique())
    return eps[['stock_id', 'date', 'eps_q', 'eps_ttm']]


def load_balance_bvps() -> pd.DataFrame:
    """Compute BVPS = Equity / shares outstanding per quarter per stock."""
    logger.info("Loading financials_balance (Equity + shares)...")
    bal = pd.read_parquet(DATA_DIR / "financials_balance.parquet")
    bal['date'] = pd.to_datetime(bal['date'])
    bal['value'] = pd.to_numeric(bal['value'], errors='coerce')

    # Equity
    eq = bal[bal['type'] == 'Equity'][['stock_id', 'date', 'value']].rename(
        columns={'value': 'equity'}
    )
    # Shares outstanding (MOPS = OrdinaryShare, FinMind legacy = CommonStockSharesOutstanding)
    shares = bal[bal['type'].isin(['OrdinaryShare', 'CommonStockSharesOutstanding'])][
        ['stock_id', 'date', 'value']].rename(columns={'value': 'shares'})
    # If both, keep first match
    shares = shares.groupby(['stock_id', 'date'], as_index=False)['shares'].first()

    bvps = eq.merge(shares, on=['stock_id', 'date'], how='inner')
    bvps = bvps[(bvps['equity'].notna()) & (bvps['shares'].notna()) & (bvps['shares'] > 0)]
    bvps['bvps'] = bvps['equity'] / bvps['shares']
    bvps = bvps[['stock_id', 'date', 'bvps']].sort_values(['stock_id', 'date'])
    logger.info("  BVPS rows: %d, stocks: %d", len(bvps), bvps['stock_id'].nunique())
    return bvps


# ================================================================
# Indicator Precompute (smaller subset than QM)
# ================================================================

def precompute_indicators(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """For each stock-date compute avg_tv_60d, RSI14, RVOL20, 52w low proximity."""
    logger.info("Precomputing technical indicators for %d stocks...",
                ohlcv['stock_id'].nunique())
    results = []
    stocks = ohlcv['stock_id'].unique()
    total = len(stocks)

    for i, sid in enumerate(stocks):
        if (i + 1) % 500 == 0:
            logger.info("  [%d/%d] precompute...", i + 1, total)

        sdf = ohlcv[ohlcv['stock_id'] == sid].sort_values('date').copy()
        if len(sdf) < 60:
            continue

        sdf = sdf.set_index('date')

        # 60d avg trading value
        sdf['tv'] = sdf['Close'] * sdf['Volume']
        sdf['avg_tv_60d'] = sdf['tv'].rolling(60, min_periods=20).mean()

        # RSI 14
        delta = sdf['Close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        sdf['rsi_14'] = 100 - (100 / (1 + rs))

        # RVOL 20 = today's volume / 20d avg
        vol20 = sdf['Volume'].rolling(20, min_periods=10).mean()
        sdf['rvol_20'] = sdf['Volume'] / vol20.replace(0, np.nan)

        # 52w low proximity (lower is closer to bottom -> higher score)
        lo52w = sdf['Low'].rolling(252, min_periods=120).min()
        sdf['low52w_prox'] = sdf['Close'] / lo52w

        sdf = sdf.reset_index()
        sdf['stock_id'] = sid
        cols = ['stock_id', 'date', 'Close', 'High', 'Low', 'Volume',
                'avg_tv_60d', 'rsi_14', 'rvol_20', 'low52w_prox']
        results.append(sdf[cols])

    df_ind = pd.concat(results, ignore_index=True)
    logger.info("  Indicators done: %d rows", len(df_ind))
    return df_ind


# ================================================================
# Forward Returns (reuse QM logic but inline to keep this file self-contained)
# ================================================================

def precompute_forward_returns(ohlcv: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing forward returns...")
    results = []
    stocks = ohlcv['stock_id'].unique()
    total = len(stocks)

    for i, sid in enumerate(stocks):
        if (i + 1) % 500 == 0:
            logger.info("  [%d/%d] forward returns...", i + 1, total)

        sdf = ohlcv[ohlcv['stock_id'] == sid].sort_values('date').copy().reset_index(drop=True)
        close = sdf['Close'].values
        high = sdf['High'].values
        low = sdf['Low'].values
        n = len(close)

        rows = {'stock_id': sdf['stock_id'], 'date': sdf['date']}
        for h in HORIZONS:
            fwd = np.full(n, np.nan)
            for j in range(n - h):
                if close[j] > 0 and not np.isnan(close[j]):
                    fwd[j] = close[j + h] / close[j] - 1
            rows[f'fwd_{h}d'] = fwd

        for h in MAX_DRAWDOWN_HORIZONS:
            fwd_max = np.full(n, np.nan)
            fwd_min = np.full(n, np.nan)
            for j in range(n - h):
                if close[j] > 0 and not np.isnan(close[j]):
                    fwd_max[j] = np.nanmax(high[j + 1: j + h + 1]) / close[j] - 1
                    fwd_min[j] = np.nanmin(low[j + 1: j + h + 1]) / close[j] - 1
            rows[f'fwd_{h}d_max'] = fwd_max
            rows[f'fwd_{h}d_min'] = fwd_min

        results.append(pd.DataFrame(rows))

    df_fwd = pd.concat(results, ignore_index=True)
    logger.info("  Forward returns done: %d rows", len(df_fwd))
    return df_fwd


# ================================================================
# 5-面向 Value Scoring
# ================================================================

def score_valuation(row: pd.Series) -> float:
    """Valuation score 0-100 from PE, PB.

    Mirror live value_screener._score_valuation basic logic (simplified).
    """
    score = 50.0
    pe = row.get('pe')
    pb = row.get('pb')
    graham = row.get('graham_number')

    # PE scoring
    if pd.notna(pe) and pe > 0:
        if pe < 8:
            score += 15
        elif pe < 12:
            score += 10
        elif pe < 20:
            score += 5
        elif pe < 30:
            pass
        else:
            score -= 10  # overvalued
    elif pd.notna(pe) and pe < 0:
        score -= 5  # loss-making

    # PB scoring
    if pd.notna(pb) and pb > 0:
        if pb < 1.0:
            score += 12
        elif pb < 1.5:
            score += 8
        elif pb < 3.0:
            score += 3
        elif pb > 5.0:
            score -= 8

    # Graham number: if Close < Graham -> undervalued
    # graham_number = sqrt(22.5 * eps_ttm * bvps)
    close = row.get('Close')
    if pd.notna(graham) and pd.notna(close) and graham > 0:
        if close < graham * 0.75:
            score += 8
        elif close < graham:
            score += 4
        elif close > graham * 1.5:
            score -= 5

    return max(0, min(100, score))


def score_technical(row: pd.Series) -> float:
    """Technical reversal signal 0-100 (RSI, RVOL, 52w low)."""
    score = 50.0
    rsi = row.get('rsi_14')
    rvol = row.get('rvol_20')
    low52w_prox = row.get('low52w_prox')

    # RSI oversold
    if pd.notna(rsi):
        if rsi < 30:
            score += 15
        elif rsi < 40:
            score += 8
        elif rsi > 70:
            score -= 10

    # RVOL shrinking (selling exhaustion)
    if pd.notna(rvol):
        if rvol < 0.5:
            score += 8
        elif rvol < 0.7:
            score += 4

    # Close to 52-week low
    if pd.notna(low52w_prox):
        if low52w_prox < 1.10:   # within 10% of low
            score += 12
        elif low52w_prox < 1.20:
            score += 6

    return max(0, min(100, score))


# ================================================================
# Main Simulation Loop
# ================================================================

def run_simulation(
    ohlcv: pd.DataFrame,
    indicators: pd.DataFrame,
    fwd_returns: pd.DataFrame,
    quality_scores: pd.DataFrame,
    eps_ttm: pd.DataFrame,
    bvps: pd.DataFrame,
    start_date: str,
    end_date: str,
    debug: bool = False,
    save_snapshot: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Main weekly simulation.

    Returns:
        (journal, snapshot)
        journal:  top 50 per week (for trade quality)
        snapshot: ALL Stage-1-passed stocks per week with raw scores + fwd returns
                  (for fair factor threshold / weight testing like VF-VA / VF-VF).
                  Empty DataFrame if save_snapshot=False.
    """
    # Build scan dates from 2330 trading days
    ref_dates = ohlcv[ohlcv['stock_id'] == '2330'].sort_values('date')['date']
    ref_series = pd.Series(ref_dates.values, index=pd.DatetimeIndex(ref_dates.values))
    weekly = ref_series.resample('W-FRI').last().dropna()
    weekly = weekly[(weekly >= start_date) & (weekly <= end_date)]
    scan_dates = weekly.values
    logger.info("Weekly scan dates: %d weeks (%s to %s)", len(scan_dates),
                pd.Timestamp(scan_dates[0]).date(), pd.Timestamp(scan_dates[-1]).date())

    if debug:
        scan_dates = scan_dates[:10]
        logger.info("DEBUG mode: only processing %d weeks", len(scan_dates))

    # Merge indicators with forward returns (big join)
    ind_fwd = indicators.merge(
        fwd_returns.drop(columns=['Close', 'Volume'], errors='ignore'),
        on=['stock_id', 'date'],
        how='left',
    )

    # Sort quality_scores / eps / bvps for PIT lookup
    qs_sorted = quality_scores.sort_values(['stock_id', 'date'])
    eps_sorted = eps_ttm.sort_values(['stock_id', 'date'])
    bvps_sorted = bvps.sort_values(['stock_id', 'date'])

    all_picks = []
    all_snapshots = []   # full Stage-1-passed snapshots (for VF-VA/VF)
    weeks_processed = 0
    weeks_no_picks = 0

    for week_end in scan_dates:
        d = pd.Timestamp(week_end)

        # Stage 1: liquidity / price filter
        snap = ind_fwd[ind_fwd['date'] == d].copy()
        snap = snap[snap['avg_tv_60d'] >= MIN_AVG_TV]
        snap = snap[snap['Close'] >= MIN_PRICE]
        snap = snap[snap['Close'].notna() & (snap['Close'] > 0)]

        if snap.empty:
            weeks_no_picks += 1
            continue

        # PIT lookup: quality_scores (last quarter <= d)
        qs_pit = (
            qs_sorted[qs_sorted['date'] <= d]
            .groupby('stock_id')
            .last()
            .reset_index()
        )
        snap = snap.merge(
            qs_pit[['stock_id', 'f_score', 'z_score',
                    'quality_score', 'revenue_score']],
            on='stock_id', how='left',
        )

        # PIT lookup: EPS TTM
        eps_pit = eps_sorted[eps_sorted['date'] <= d].groupby('stock_id').last().reset_index()
        snap = snap.merge(eps_pit[['stock_id', 'eps_ttm']], on='stock_id', how='left')

        # PIT lookup: BVPS
        bvps_pit = bvps_sorted[bvps_sorted['date'] <= d].groupby('stock_id').last().reset_index()
        snap = snap.merge(bvps_pit[['stock_id', 'bvps']], on='stock_id', how='left')

        # PE = Close / eps_ttm (only if eps_ttm > 0)
        snap['pe'] = np.where(
            snap['eps_ttm'].notna() & (snap['eps_ttm'] > 0),
            snap['Close'] / snap['eps_ttm'],
            np.nan,
        )
        # PB = Close / bvps
        snap['pb'] = np.where(
            snap['bvps'].notna() & (snap['bvps'] > 0),
            snap['Close'] / snap['bvps'],
            np.nan,
        )
        # Graham number = sqrt(22.5 * eps_ttm * bvps) — undervalued if Close < this
        snap['graham_number'] = np.where(
            (snap['eps_ttm'].notna()) & (snap['eps_ttm'] > 0)
            & (snap['bvps'].notna()) & (snap['bvps'] > 0),
            np.sqrt(22.5 * snap['eps_ttm'] * snap['bvps']),
            np.nan,
        )

        # ---- Compute 5 dimension scores ----
        snap['valuation_s'] = snap.apply(score_valuation, axis=1)
        snap['technical_s'] = snap.apply(score_technical, axis=1)

        # Quality: use f_score mapped to 0-100 + z_score
        # Simplified: use existing quality_score directly
        snap['quality_s'] = snap['quality_score'].clip(0, 100)

        # Revenue: use existing revenue_score
        snap['revenue_s'] = snap['revenue_score'].clip(0, 100)

        # Smart money: placeholder 50 (neutral) for historical sim
        snap['smart_money_s'] = 50.0

        # Composite value score (5-dim weighted)
        snap['value_score'] = (
            snap['valuation_s'] * WEIGHTS['valuation'] +
            snap['quality_s'] * WEIGHTS['quality'] +
            snap['revenue_s'] * WEIGHTS['revenue'] +
            snap['technical_s'] * WEIGHTS['technical'] +
            snap['smart_money_s'] * WEIGHTS['smart_money']
        )

        # Market cap tier proxy
        snap['tv_pct'] = snap['avg_tv_60d'].rank(pct=True)
        snap['market_cap_tier'] = 'small'
        snap.loc[snap['tv_pct'] >= 0.30, 'market_cap_tier'] = 'mid'
        snap.loc[snap['tv_pct'] >= 0.70, 'market_cap_tier'] = 'large'

        # Need at least valuation data to rank (drop no-data rows)
        snap = snap.dropna(subset=['value_score'])

        # Save full snapshot before top-N filter (for VF-VA/VF factor/weight tests)
        if save_snapshot and not snap.empty:
            snap_cols = [
                'stock_id', 'Close',
                'pe', 'pb', 'graham_number',
                'f_score', 'z_score', 'quality_score', 'revenue_score',
                'valuation_s', 'quality_s', 'revenue_s', 'technical_s', 'smart_money_s',
                'value_score',
                'rsi_14', 'rvol_20', 'low52w_prox',
                'market_cap_tier', 'avg_tv_60d',
                *[f'fwd_{h}d' for h in HORIZONS],
                *[f'fwd_{h}d_max' for h in MAX_DRAWDOWN_HORIZONS],
                *[f'fwd_{h}d_min' for h in MAX_DRAWDOWN_HORIZONS],
            ]
            snap_cols = [c for c in snap_cols if c in snap.columns]
            snapshot = snap[snap_cols].copy()
            snapshot['week_end_date'] = d
            all_snapshots.append(snapshot)

        # Top 50 by value_score
        top_n = snap.nlargest(VALUE_TOP_N, 'value_score').reset_index(drop=True)
        if top_n.empty:
            weeks_no_picks += 1
            continue
        top_n['rank_in_top50'] = top_n.index + 1

        picks = top_n[[
            'stock_id', 'Close',
            'pe', 'pb', 'graham_number',
            'f_score', 'z_score', 'quality_score', 'revenue_score',
            'valuation_s', 'quality_s', 'revenue_s', 'technical_s', 'smart_money_s',
            'value_score',
            'rsi_14', 'rvol_20', 'low52w_prox',
            'market_cap_tier', 'rank_in_top50',
            *[f'fwd_{h}d' for h in HORIZONS],
            *[f'fwd_{h}d_max' for h in MAX_DRAWDOWN_HORIZONS],
            *[f'fwd_{h}d_min' for h in MAX_DRAWDOWN_HORIZONS],
        ]].copy()
        picks.rename(columns={'Close': 'entry_price'}, inplace=True)
        picks['week_end_date'] = d
        picks['mode'] = 'default'  # placeholder for future VF-VF variants

        all_picks.append(picks)
        weeks_processed += 1

        if weeks_processed % 20 == 0:
            logger.info("  Processed %d weeks, %d picks so far",
                        weeks_processed, sum(len(p) for p in all_picks))

    if not all_picks:
        logger.error("No picks generated!")
        return pd.DataFrame(), pd.DataFrame()

    journal = pd.concat(all_picks, ignore_index=True)
    snapshot_df = pd.concat(all_snapshots, ignore_index=True) if all_snapshots else pd.DataFrame()
    logger.info("Simulation complete: %d weeks, %d picks, %d weeks no picks",
                weeks_processed, len(journal), weeks_no_picks)
    if save_snapshot:
        logger.info("  Snapshot: %d rows across %d weeks",
                    len(snapshot_df), snapshot_df['week_end_date'].nunique()
                    if not snapshot_df.empty else 0)
    return journal, snapshot_df


# ================================================================
# Main
# ================================================================

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="2020-01-01")
    ap.add_argument("--end", default="2025-12-31")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--out", default=str(OUT_PATH))
    ap.add_argument("--save-snapshot", action="store_true",
                    help="同時存 Stage-1-passed 全股 snapshot 供 VF-VA/VF 驗證")
    ap.add_argument("--snapshot-out", default=None,
                    help="snapshot 輸出檔（預設同 --out 但加 _snapshot 後綴）")
    ap.add_argument("--recompute", action="store_true",
                    help="強制重算 indicators + fwd_returns（略過 cache）")
    args = ap.parse_args()

    ohlcv = load_ohlcv()
    quality_scores = load_quality_scores()
    eps_ttm = load_income_eps()
    bvps = load_balance_bvps()

    # Cache precomputed indicators + forward returns (takes 10+ min on first run)
    IND_CACHE = DATA_DIR / "value_sim_indicators.parquet"
    FWD_CACHE = DATA_DIR / "value_sim_fwd_returns.parquet"
    if IND_CACHE.exists() and not args.recompute:
        logger.info("Loading cached indicators from %s", IND_CACHE)
        indicators = pd.read_parquet(IND_CACHE)
    else:
        indicators = precompute_indicators(ohlcv)
        indicators.to_parquet(IND_CACHE)
        logger.info("Saved indicators cache: %s", IND_CACHE)
    if FWD_CACHE.exists() and not args.recompute:
        logger.info("Loading cached fwd_returns from %s", FWD_CACHE)
        fwd_returns = pd.read_parquet(FWD_CACHE)
    else:
        fwd_returns = precompute_forward_returns(ohlcv)
        fwd_returns.to_parquet(FWD_CACHE)
        logger.info("Saved fwd_returns cache: %s", FWD_CACHE)

    journal, snapshot = run_simulation(
        ohlcv=ohlcv,
        indicators=indicators,
        fwd_returns=fwd_returns,
        quality_scores=quality_scores,
        eps_ttm=eps_ttm,
        bvps=bvps,
        start_date=args.start,
        end_date=args.end,
        debug=args.debug,
        save_snapshot=args.save_snapshot,
    )

    if journal.empty:
        logger.error("Empty journal, not saving.")
        sys.exit(1)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    journal.to_parquet(out_path)
    logger.info("Saved journal: %s (%d rows, %d weeks, %d unique stocks)",
                out_path, len(journal), journal['week_end_date'].nunique(),
                journal['stock_id'].nunique())

    if args.save_snapshot and not snapshot.empty:
        snap_path = Path(args.snapshot_out) if args.snapshot_out else (
            out_path.with_name(out_path.stem + "_snapshot" + out_path.suffix)
        )
        snapshot.to_parquet(snap_path)
        logger.info("Saved snapshot: %s (%d rows, %d weeks, %d unique stocks)",
                    snap_path, len(snapshot), snapshot['week_end_date'].nunique(),
                    snapshot['stock_id'].nunique())


if __name__ == "__main__":
    main()
