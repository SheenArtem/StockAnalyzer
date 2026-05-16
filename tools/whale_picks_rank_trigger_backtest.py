"""Whale Picks Rank-Trigger Backtest (v15 candidate).

對照 v13 monthly rebalance (OLD Sharpe 1.52 / 02f682f 修法後 1.70 / MDD -12.4%)，測試「日頻 rank-trigger 動態進出」：

  BUY: 任何一天 composite_parsi rank 進入 top-20
  SELL: rank 掉出 top-30 (10-rank buffer 避免邊緣震盪)
        OR Close / entry_close - 1 <= -15% (trailing stop loss)

策略立場：純價格報酬，無手續費/滑價（與 v13 baseline 一致以保 fair comparison）。

Output: reports/whale_picks_phase2_v15_rank_trigger/
  - portfolio_daily_returns.parquet
  - trade_log.parquet (每筆 position)
  - comparison_v13_vs_v15.csv (side-by-side metrics)
  - report.md
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("whale_picks_rank_trigger")

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from tools.whale_picks_phase2 import (  # noqa: E402
    load_indicators, load_smart_money, load_quality, load_revenue,
    load_financials_panel, load_universe_industry, build_feature_panel,
    winsorize_standardize,
)

# Production composite (v13 locked)
COMPOSITE_PARSI: Dict[str, float] = {
    'f_score':                +1.0,
    'f_score_4q_delta':       +1.0,
    'eps_yoy':                +1.0,
    'revenue_score_6m_delta': +1.0,
    'turnover_log':           -1.0,
    'dist_52w_high':          -1.0,
    'stealth_volume_20d':     +1.0,
    'capex_intensity':        -1.0,
}

# Rank-trigger thresholds
BUY_RANK = 20
SELL_RANK = 30   # 10-rank buffer to avoid edge whipsaw
STOP_LOSS = -0.15
MIN_AVG_TV = 1e7
FEE_PCT = 0.0    # Round-trip fee (TW: 0.1425% buy + 0.1425% sell + 0.3% tax = 0.585%)

CACHE = REPO / "data_cache" / "backtest"


# =============================================================================
# Stage A — Build daily panel with composite_parsi (NO monthly filter)
# =============================================================================

def build_daily_feat(start: str, end: str) -> pd.DataFrame:
    """Build daily feature panel with composite_parsi rank computed within each day."""
    log.info("Loading panels: %s ~ %s", start, end)
    indicators = load_indicators(start, end)
    fwd_returns = pd.DataFrame(columns=['stock_id', 'date', 'fwd_5d', 'fwd_10d',
                                         'fwd_20d', 'fwd_60d', 'fwd_120d',
                                         'fwd_60d_max', 'fwd_60d_min'])
    smart_money = load_smart_money(start, end)
    quality = load_quality(start, end)
    revenue = load_revenue(start, end)
    financials = load_financials_panel(start, end)
    universe_industry = load_universe_industry()

    log.info("Building feature panel (daily, 2-4 min)...")
    feat = build_feature_panel(indicators, smart_money, fwd_returns, quality,
                                revenue, financials, universe_industry)
    feat['date'] = pd.to_datetime(feat['date'])
    feat = feat[(feat['date'] >= start) & (feat['date'] <= end)].copy()
    log.info("Daily panel: %d rows", len(feat))

    # Liquidity filter (same as v13 production)
    if 'avg_tv_60d' in feat.columns:
        before = len(feat)
        feat = feat[feat['avg_tv_60d'] >= MIN_AVG_TV].copy()
        log.info("Liquidity filter: %d -> %d (-%.1f%%)",
                 before, len(feat), 100 * (before - len(feat)) / before)

    # Industry-neutral standardize across daily cross-sections (v13 logic but daily)
    log.info("Standardizing daily (this is the slow step, 2-5 min)...")
    feat = winsorize_standardize(feat, list(COMPOSITE_PARSI.keys()), industry_neutral=True)

    # Composite
    feat['composite_parsi'] = 0.0
    n_valid = pd.Series(0, index=feat.index)
    for f, w in COMPOSITE_PARSI.items():
        if f not in feat.columns:
            continue
        v = feat[f].fillna(0.0)
        feat['composite_parsi'] = feat['composite_parsi'] + w * v
        n_valid = n_valid + feat[f].notna().astype(int)
    feat.loc[n_valid < 5, 'composite_parsi'] = np.nan

    # Daily rank (1 = best)
    feat['rank'] = feat.groupby('date')['composite_parsi'].rank(ascending=False, method='first')

    log.info("Final feat: %d rows, %d sids, %d days, %d valid composite",
             len(feat), feat['stock_id'].nunique(), feat['date'].nunique(),
             feat['composite_parsi'].notna().sum())
    return feat[['stock_id', 'date', 'Close', 'composite_parsi', 'rank']].dropna(subset=['Close'])


# =============================================================================
# Stage B — State machine: per-stock entry/exit
# =============================================================================

def run_state_machine(feat: pd.DataFrame) -> pd.DataFrame:
    """For each stock, iterate daily and apply BUY/SELL state machine.

    State: 'out' or 'in'
      out -> in:  rank <= BUY_RANK
      in -> out:  rank > SELL_RANK OR drawdown <= STOP_LOSS
    """
    positions: List[Dict] = []
    feat = feat.sort_values(['stock_id', 'date']).reset_index(drop=True)

    for sid, grp in feat.groupby('stock_id', sort=False):
        state = 'out'
        entry_date = None
        entry_price = None
        entry_rank = None

        rows = grp[['date', 'Close', 'rank']].values
        for date, close, rank in rows:
            if state == 'out':
                if pd.notna(rank) and rank <= BUY_RANK and pd.notna(close):
                    state = 'in'
                    entry_date = date
                    entry_price = float(close)
                    entry_rank = float(rank)
            else:  # state == 'in'
                if pd.isna(close):
                    continue
                close_f = float(close)
                dd = close_f / entry_price - 1.0
                stop_hit = dd <= STOP_LOSS
                rank_out = pd.notna(rank) and float(rank) > SELL_RANK
                if stop_hit or rank_out:
                    positions.append({
                        'stock_id': sid,
                        'entry_date': pd.Timestamp(entry_date),
                        'entry_price': entry_price,
                        'entry_rank': entry_rank,
                        'exit_date': pd.Timestamp(date),
                        'exit_price': close_f,
                        'exit_reason': 'stop_loss' if stop_hit else 'rank_out',
                        'pnl_pct': dd,
                        'still_holding': False,
                    })
                    state = 'out'
                    entry_date = entry_price = entry_rank = None

        # Close out still-holding at data end
        if state == 'in':
            last = grp.iloc[-1]
            positions.append({
                'stock_id': sid,
                'entry_date': pd.Timestamp(entry_date),
                'entry_price': entry_price,
                'entry_rank': entry_rank,
                'exit_date': pd.Timestamp(last['date']),
                'exit_price': float(last['Close']),
                'exit_reason': 'data_end',
                'pnl_pct': float(last['Close']) / entry_price - 1.0,
                'still_holding': True,
            })

    pos_df = pd.DataFrame(positions)
    pos_df['holding_days'] = (pos_df['exit_date'] - pos_df['entry_date']).dt.days
    log.info("State machine: %d positions, %d unique stocks, %d still holding",
             len(pos_df), pos_df['stock_id'].nunique(), int(pos_df['still_holding'].sum()))
    return pos_df


# =============================================================================
# Stage C — Portfolio daily returns
# =============================================================================

def compute_portfolio_returns(feat: pd.DataFrame, positions: pd.DataFrame, fee_pct: float = 0.0) -> pd.Series:
    """Daily portfolio = equal-weight avg return of all currently-active positions.

    Active definition: position active on date D if entry_date < D <= exit_date.
    (Buy at entry_date close; first return contribution is D = entry_date+1.)

    Fee model: on each position's exit_date, deduct fee_pct / n_active_that_day from
    portfolio return (equal-weight, so fee impact = position_weight × fee_pct).
    """
    close_wide = feat.pivot_table(index='date', columns='stock_id', values='Close', aggfunc='last')
    close_wide = close_wide.sort_index()
    ret_wide = close_wide.pct_change(fill_method=None)

    all_dates = close_wide.index
    all_stocks = close_wide.columns
    active = pd.DataFrame(False, index=all_dates, columns=all_stocks)

    for _, p in positions.iterrows():
        sid = p['stock_id']
        if sid not in active.columns:
            continue
        mask = (active.index > p['entry_date']) & (active.index <= p['exit_date'])
        active.loc[mask, sid] = True

    masked = ret_wide.where(active)
    n_active = active.sum(axis=1)
    portfolio_ret = masked.mean(axis=1).fillna(0.0)

    # Apply fees on exit dates (round-trip fee per position)
    if fee_pct > 0:
        fee_drag = pd.Series(0.0, index=portfolio_ret.index)
        for _, p in positions.iterrows():
            exit_d = p['exit_date']
            if exit_d in fee_drag.index:
                n_a = max(1, int(n_active.loc[exit_d]))
                fee_drag.loc[exit_d] += fee_pct / n_a
        portfolio_ret = portfolio_ret - fee_drag
        log.info("Fee drag total over period: %.2f%% (sum of daily fee deductions)",
                 fee_drag.sum() * 100)

    log.info("Portfolio returns: %d days, max concurrent positions %d, avg concurrent %.1f, fee_pct=%.4f",
             (n_active > 0).sum(), int(n_active.max()), float(n_active[n_active > 0].mean()), fee_pct)
    return portfolio_ret


def portfolio_metrics(daily_ret: pd.Series, name: str = "v15") -> Dict:
    """Compute Sharpe / CAGR / MDD / win_rate (daily basis)."""
    active = daily_ret[daily_ret != 0]
    n = len(active)
    if n < 20:
        return {'strategy': name, 'error': 'too few data points'}

    equity = (1 + daily_ret).cumprod()
    n_days_total = len(daily_ret)
    years = n_days_total / 252.0
    total_ret = equity.iloc[-1] - 1.0
    cagr = (equity.iloc[-1]) ** (1.0 / years) - 1.0 if years > 0 else np.nan
    annual_vol = daily_ret.std() * np.sqrt(252)
    sharpe = (daily_ret.mean() * 252) / annual_vol if annual_vol > 0 else np.nan
    peak = equity.expanding().max()
    drawdown = equity / peak - 1.0
    mdd = drawdown.min()
    win_rate = (active > 0).mean()
    return {
        'strategy': name,
        'n_days': n_days_total,
        'years': round(years, 2),
        'total_return': round(total_ret, 4),
        'cagr': round(cagr, 4),
        'annual_vol': round(annual_vol, 4),
        'sharpe': round(sharpe, 3),
        'mdd': round(mdd, 4),
        'daily_win_rate': round(win_rate, 4),
    }


# =============================================================================
# Stage D — v13 reference (load existing report numbers for comparison)
# =============================================================================

V13_REF = {
    'strategy': 'v13 monthly K=20 (production)',
    'total_return': 2.5476,    # from stage8_portfolio.csv (top-20 composite_parsi)
    'cagr': 0.3055,
    'annual_vol': 0.1888,
    'sharpe': 1.519,
    'mdd': -0.1244,
    'win_rate_monthly': 0.6667,  # monthly basis (not daily)
    'n_periods_monthly': 57,
}


# =============================================================================
# Main
# =============================================================================

def main():
    global BUY_RANK, SELL_RANK, STOP_LOSS, FEE_PCT

    parser = argparse.ArgumentParser(description="Whale Picks rank-trigger backtest (v15 candidate)")
    parser.add_argument('--start', default='2021-01-01')
    parser.add_argument('--end', default='2025-12-31')
    parser.add_argument('--buy-rank', type=int, default=BUY_RANK)
    parser.add_argument('--sell-rank', type=int, default=SELL_RANK)
    parser.add_argument('--stop-loss', type=float, default=STOP_LOSS)
    parser.add_argument('--fee-pct', type=float, default=FEE_PCT,
                        help='Round-trip fee per position (TW 0.585%% = 0.1425+0.1425+0.3 tax)')
    parser.add_argument('--output-dir', default='reports/whale_picks_phase2_v15_rank_trigger')
    args = parser.parse_args()

    BUY_RANK = args.buy_rank
    SELL_RANK = args.sell_rank
    STOP_LOSS = args.stop_loss
    FEE_PCT = args.fee_pct

    out_dir = REPO / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info("Config: BUY_RANK<=%d, SELL_RANK>%d, STOP_LOSS=%.2f",
             BUY_RANK, SELL_RANK, STOP_LOSS)

    # Stage A
    feat = build_daily_feat(args.start, args.end)

    # Stage B
    positions = run_state_machine(feat)
    positions.to_parquet(out_dir / "trade_log.parquet", index=False)

    # Stage C
    daily_ret = compute_portfolio_returns(feat, positions, fee_pct=FEE_PCT)
    daily_ret.to_frame('ret').to_parquet(out_dir / "portfolio_daily_returns.parquet")

    # Metrics
    fee_tag = f" fee {FEE_PCT * 100:.3f}%" if FEE_PCT > 0 else ""
    v15 = portfolio_metrics(daily_ret, name=f"v15 rank-trigger (buy<={BUY_RANK} sell>{SELL_RANK} sl{STOP_LOSS:.0%}{fee_tag})")
    log.info("v15 metrics: %s", v15)

    # Trade stats
    closed = positions[~positions['still_holding']]
    trade_stats = {
        'n_positions': len(positions),
        'n_unique_stocks': positions['stock_id'].nunique(),
        'n_still_holding': int(positions['still_holding'].sum()),
        'avg_holding_days': round(float(positions['holding_days'].mean()), 1),
        'median_holding_days': round(float(positions['holding_days'].median()), 1),
        'win_rate_per_position': round(float((closed['pnl_pct'] > 0).mean()), 4) if len(closed) else None,
        'avg_pnl_pct_per_position': round(float(closed['pnl_pct'].mean()), 4) if len(closed) else None,
        'best_position_pnl': round(float(closed['pnl_pct'].max()), 4) if len(closed) else None,
        'worst_position_pnl': round(float(closed['pnl_pct'].min()), 4) if len(closed) else None,
        'exit_reason_counts': closed['exit_reason'].value_counts().to_dict() if len(closed) else {},
    }
    log.info("Trade stats: %s", trade_stats)

    # Comparison
    cmp_rows = [
        V13_REF,
        v15,
    ]
    cmp_df = pd.DataFrame(cmp_rows)
    cmp_df.to_csv(out_dir / "comparison_v13_vs_v15.csv", index=False)

    # Write report.md
    lines = [
        "# Whale Picks v15 — Rank-Trigger Backtest Report",
        f"\n**Period**: {args.start} ~ {args.end}",
        f"\n**Config**: BUY rank<={BUY_RANK} / SELL rank>{SELL_RANK} / Stop loss {STOP_LOSS:.0%}",
        "\n**Methodology**: 日頻 composite_parsi rank → 進入 top-20 BUY；掉出 top-30 或 -15% drawdown SELL；",
        "industry-neutral standardize / liquidity filter ≥ 10M TWD / 純價格報酬 (與 v13 一致 fair comparison)",
        "\n## Performance vs v13 monthly baseline",
        "",
        "| Metric | v13 monthly | v15 rank-trigger | Δ |",
        "|---|---|---|---|",
        f"| Total return | {V13_REF['total_return']:.2%} | {v15.get('total_return', float('nan')):.2%} | {(v15.get('total_return', 0) - V13_REF['total_return']) * 100:+.2f}pp |",
        f"| CAGR | {V13_REF['cagr']:.2%} | {v15.get('cagr', float('nan')):.2%} | {(v15.get('cagr', 0) - V13_REF['cagr']) * 100:+.2f}pp |",
        f"| Sharpe | {V13_REF['sharpe']:.3f} | {v15.get('sharpe', float('nan')):.3f} | {v15.get('sharpe', 0) - V13_REF['sharpe']:+.3f} |",
        f"| MDD | {V13_REF['mdd']:.2%} | {v15.get('mdd', float('nan')):.2%} | {(v15.get('mdd', 0) - V13_REF['mdd']) * 100:+.2f}pp |",
        f"| Annual vol | {V13_REF['annual_vol']:.2%} | {v15.get('annual_vol', float('nan')):.2%} | — |",
        "\n## Trade statistics (v15 only)",
        "",
        f"- **Total positions**: {trade_stats['n_positions']:,}",
        f"- **Unique stocks**: {trade_stats['n_unique_stocks']:,}",
        f"- **Still holding (data end)**: {trade_stats['n_still_holding']}",
        f"- **Avg holding days**: {trade_stats['avg_holding_days']} (median {trade_stats['median_holding_days']})",
        f"- **Win rate per position**: {trade_stats['win_rate_per_position'] * 100:.1f}%" if trade_stats['win_rate_per_position'] is not None else "- Win rate: n/a",
        f"- **Avg PnL per position**: {trade_stats['avg_pnl_pct_per_position'] * 100:+.2f}%" if trade_stats['avg_pnl_pct_per_position'] is not None else "",
        f"- **Best position**: {trade_stats['best_position_pnl'] * 100:+.1f}%" if trade_stats['best_position_pnl'] is not None else "",
        f"- **Worst position**: {trade_stats['worst_position_pnl'] * 100:+.1f}%" if trade_stats['worst_position_pnl'] is not None else "",
        f"- **Exit reasons**: {trade_stats['exit_reason_counts']}",
        "\n## Verdict",
        "",
        "_Auto-fill below based on metric deltas..._",
    ]
    # Verdict
    sharpe_delta = v15.get('sharpe', 0) - V13_REF['sharpe']
    mdd_delta = v15.get('mdd', 0) - V13_REF['mdd']
    if sharpe_delta >= 0 and mdd_delta >= -0.05:
        verdict = "✅ **PROMOTE**: v15 Sharpe 不輸 v13 且 MDD 不爆 (-5pp 內) → 值得切換 production"
    elif sharpe_delta >= 0.1:
        verdict = f"⚠️ **CONDITIONAL**: v15 Sharpe +{sharpe_delta:.2f} 有改善但 MDD {mdd_delta:+.2%} 風險加大，看 user 對 turnover 容忍度"
    else:
        verdict = f"❌ **REJECT**: v15 Sharpe {sharpe_delta:+.2f} 不夠 + MDD {mdd_delta:+.2%}，保 v13 monthly"
    lines.append(verdict)

    (out_dir / "report.md").write_text("\n".join(lines), encoding='utf-8')
    log.info("Report saved: %s", out_dir / "report.md")

    # Print summary to stdout (ASCII-safe to avoid cp950 console encode errors on Windows)
    import sys as _sys
    def _p(s: str) -> None:
        try:
            print(s)
        except UnicodeEncodeError:
            print(s.encode('ascii', 'replace').decode('ascii'))
    _p("\n" + "=" * 80)
    _p("SUMMARY")
    _p("=" * 80)
    _p(f"v13 monthly:       Sharpe {V13_REF['sharpe']:.3f} / CAGR {V13_REF['cagr']:.2%} / MDD {V13_REF['mdd']:.2%}")
    _p(f"v15 rank-trigger:  Sharpe {v15.get('sharpe', float('nan')):.3f} / CAGR {v15.get('cagr', float('nan')):.2%} / MDD {v15.get('mdd', float('nan')):.2%}")
    _p(f"Delta Sharpe: {sharpe_delta:+.3f}  /  Delta MDD: {mdd_delta * 100:+.2f}pp")
    _p(f"Trades: {trade_stats['n_positions']:,} positions, avg hold {trade_stats['avg_holding_days']}d, "
        f"win rate {trade_stats['win_rate_per_position'] * 100:.1f}%")
    _p(f"\nVerdict: {verdict}")
    _p(f"Full report: {out_dir / 'report.md'}")


if __name__ == "__main__":
    main()
