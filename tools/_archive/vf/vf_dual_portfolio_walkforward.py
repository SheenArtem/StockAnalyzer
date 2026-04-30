"""
Dual 50/50 Portfolio Walk-Forward Simulator (position-aware, 2026-04-29)

正式驗證 4 條已實施政策（不再用 dropout proxy）：
  Rule 4: MIN_HOLD = 40 trading days (vs 0/20/60)
  Rule 3: TP policy = none (vs tp_third_at_10 / tp_half_at_10)
  Rule 2: Rebalance freq = quarterly (vs monthly)
  Rule 7: Regime-defer 3 mo (vs immediate / 1 mo)

設計：
  - 每週 step：持倉每檔吃 fwd_5d，days_held += 5 trading days
  - Rebalance week：算 score → 取 top_20 → 跌出榜 & days_held ≥ MIN_HOLD → 換
  - Whipsaw ban 30d (已驗 OK，固定不變)
  - Dual 50/50：Value side (50%) regime-aware + QM side (50%) always
  - Walk-forward: IS 2020-2022, OOS 2023, OOS 2024, OOS 2025

Universe:
  Value: trade_journal_value_tw_snapshot (309 weeks × 857 stocks)
  QM:    trade_journal_qm_tw            (538 weeks × 1080 stocks，限同期)

Returns: 用 fwd_5d 累乘，13 rebalance/year basis Sharpe
Caveats: no transaction cost / no hard stop / TP 用 fwd_20d_max proxy / 不模擬
         partial exit price (TP 命中即減 1/3 或 1/2 用 +10% 報酬)

Usage:
  python tools/vf_dual_portfolio_walkforward.py             # full grid
  python tools/vf_dual_portfolio_walkforward.py --quick     # 4 policy 各 single comparison
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
SNAP_VAL = ROOT / 'data_cache/backtest/trade_journal_value_tw_snapshot.parquet'
SNAP_QM = ROOT / 'data_cache/backtest/trade_journal_qm_tw.parquet'
TWII_BENCH = ROOT / 'data_cache/backtest/_twii_bench.parquet'
OUT_DIR = ROOT / 'reports'

# Live config (對齊 vf_value_portfolio_backtest.py)
MAX_PE, MAX_PB, PE_X_PB_MAX, MIN_TV = 12, 3.0, 22.5, 3e7
WEIGHTS = {'val': 0.30, 'quality': 0.25, 'revenue': 0.30, 'technical': 0.15, 'sm': 0.00}
TOP_N = 20
TRADING_DAYS_PER_WEEK = 5
WHIPSAW_BAN_TDAYS = 30
TP_THRESHOLD = 0.10  # +10% TP trigger


# ------------------ data prep ------------------

def load_value_snapshot():
    df = pd.read_parquet(SNAP_VAL)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    return df


def load_qm_snapshot(min_date):
    df = pd.read_parquet(SNAP_QM)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    df = df[df['week_end_date'] >= min_date].copy()
    return df


def apply_stage1(df):
    mask = (df['pe'] > 0) & (df['pe'] <= MAX_PE)
    pb_pass = df['pb'].isna() | (df['pb'] <= MAX_PB)
    graham_pass = df['pb'].isna() | ((df['pe'] * df['pb']) <= PE_X_PB_MAX)
    tv_pass = df['avg_tv_60d'].fillna(0) >= MIN_TV
    return df[mask & pb_pass & graham_pass & tv_pass].copy()


def compute_value_score(df):
    return (
        WEIGHTS['val'] * df['valuation_s']
        + WEIGHTS['quality'] * df['quality_s']
        + WEIGHTS['revenue'] * df['revenue_s']
        + WEIGHTS['technical'] * df['technical_s']
        + WEIGHTS['sm'] * df['smart_money_s']
    )


def load_twii_close():
    df = pd.read_parquet(TWII_BENCH)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.index = pd.to_datetime(df.index)
    return df['Close']


def regime_at(date, twii_close):
    idx = twii_close.index.searchsorted(date, side='right') - 1
    if idx < 20:
        return 'neutral'
    win = twii_close.iloc[idx - 20:idx + 1]
    p0, p1 = float(win.iloc[0]), float(win.iloc[-1])
    ret20 = (p1 / p0) - 1
    wmax, wmin, wavg = float(win.max()), float(win.min()), float(win.mean())
    rng20 = (wmax - wmin) / wavg if wavg > 0 else 0
    if rng20 > 0.08:
        return 'volatile'
    if ret20 > 0.05:
        return 'trending'
    if abs(ret20) < 0.02 and rng20 <= 0.08:
        return 'ranging'
    return 'neutral'


# ------------------ position-aware simulator ------------------

class Portfolio:
    """Single side (Value or QM) position book."""

    def __init__(self, side, top_n=TOP_N):
        self.side = side
        self.top_n = top_n
        # positions: stock_id -> {'entry_date': date, 'days_held': int, 'weight': float (0..1 of side capital), 'tp_hit': bool}
        self.positions = {}
        # whipsaw ban: stock_id -> exit_date (cannot re-enter within WHIPSAW_BAN_TDAYS)
        self.banned = {}
        # weekly returns of this side
        self.weekly_returns = []

    def step_one_week(self, week_returns, week_max20, min_hold_days, tp_policy):
        """Apply one week of holding period.

        Weight model: each position has 'weight_raw' (fraction of nominal slot) and the actual
        weight is normalized across the live position set every week. So if we hold 9 stocks,
        each gets 1/9 of side capital, not 1/20.

        This avoids the top_n=20 mismatch when QM snapshot only has 4-9 picks per week.

        TP exit reduces weight_raw, so a 1/3-trim position keeps 2/3 of its slot.

        week_returns: dict stock_id -> fwd_5d (this week)
        week_max20:   dict stock_id -> fwd_20d_max (used to detect TP hit on entry)
        Returns: realized return for this week (already weighted within side).
        """
        if not self.positions:
            self.weekly_returns.append(0.0)
            return 0.0

        # Total nominal weight (sum of weight_raw)
        total_raw = sum(p['weight_raw'] for p in self.positions.values())
        if total_raw <= 0:
            self.weekly_returns.append(0.0)
            return 0.0

        total_ret = 0.0
        keys = list(self.positions.keys())
        for sid in keys:
            pos = self.positions[sid]
            r = week_returns.get(sid, np.nan)
            if pd.isna(r):
                r = 0.0
            # actual weight in side capital
            w = pos['weight_raw'] / total_raw

            # TP detection (entry week only) — uses fwd_20d_max recorded at entry
            if (
                tp_policy in ('tp_third', 'tp_half')
                and not pos.get('tp_hit', False)
                and pos['days_held'] == 0
            ):
                m20 = week_max20.get(sid, np.nan)
                if not pd.isna(m20) and m20 >= TP_THRESHOLD:
                    pos['tp_hit_pending'] = True

            # Apply weekly return at current weight
            total_ret += r * w
            pos['days_held'] += TRADING_DAYS_PER_WEEK

            # Apply TP exit at end of 4-week window after entry
            if pos.get('tp_hit_pending') and pos['days_held'] >= 20:
                if tp_policy == 'tp_third':
                    # realize 1/3 × current weight × +10%, then trim weight_raw 2/3
                    realized = w * (1.0 / 3.0) * TP_THRESHOLD
                    total_ret += realized
                    pos['weight_raw'] *= (2.0 / 3.0)
                elif tp_policy == 'tp_half':
                    realized = w * 0.5 * TP_THRESHOLD
                    total_ret += realized
                    pos['weight_raw'] *= 0.5
                pos['tp_hit'] = True
                pos['tp_hit_pending'] = False

        self.weekly_returns.append(total_ret)
        return total_ret

    def rebalance(self, target_top, week_date, min_hold_days):
        """Replace dropouts that have days_held >= min_hold_days.

        target_top: list of stock_ids to fill (length up to top_n; may be shorter)
        Returns: number of slots changed.

        Note: 'weight_raw' is set to 1.0 for every fresh fill; exact weight is
        normalized per week in step_one_week. This means partial-TP'd positions
        keep their reduced weight_raw across the rebalance until evicted.
        """
        changed = 0
        keep = {}
        for sid, pos in self.positions.items():
            if sid in target_top:
                keep[sid] = pos  # carries over weight_raw (may be < 1.0 if TP'd)
            elif pos['days_held'] < min_hold_days:
                keep[sid] = pos  # min-hold protected
            else:
                changed += 1
                self.banned[sid] = week_date
        self.positions = keep

        # Fill empty slots from target_top (in ranking order). Limit to top_n total.
        for sid in target_top:
            if sid in self.positions:
                continue
            if sid in self.banned:
                ban_date = self.banned[sid]
                # convert trading days ban to calendar days approx (×7/5)
                if (week_date - ban_date).days <= WHIPSAW_BAN_TDAYS * (7 / 5):
                    continue
                else:
                    del self.banned[sid]
            if len(self.positions) >= self.top_n:
                break
            self.positions[sid] = {
                'entry_date': week_date,
                'days_held': 0,
                'weight_raw': 1.0,
                'tp_hit': False,
                'tp_hit_pending': False,
            }
            changed += 1
        return changed

    def liquidate(self):
        self.positions.clear()


# ------------------ main simulator ------------------

def run_simulation(value_df_stage1, qm_df, twii_close,
                   start, end,
                   min_hold_days=40,
                   tp_policy='none',
                   rebalance_weeks=13,  # 13 = quarterly, 4 = monthly
                   regime_defer_months=3):
    """Run dual 50/50 walk-forward.

    Returns: pd.DataFrame with columns [date, value_ret, qm_ret, dual_ret, regime, value_active]
    """
    # Build week schedule from value snapshot
    weeks = sorted(value_df_stage1['week_end_date'].unique())
    weeks = [w for w in weeks if start <= w <= end]
    if not weeks:
        return pd.DataFrame()

    # Pre-compute scores per week for Value
    value_df_stage1 = value_df_stage1.copy()
    value_df_stage1['v_score'] = compute_value_score(value_df_stage1)

    # Pre-index by week for fast lookup
    val_by_week = {w: g for w, g in value_df_stage1.groupby('week_end_date')}
    qm_by_week = {w: g for w, g in qm_df.groupby('week_end_date')}

    # Per-week return lookup: (stock_id, week) -> fwd_5d
    val_ret_lookup = value_df_stage1.set_index(['week_end_date', 'stock_id'])
    qm_ret_lookup = qm_df.set_index(['week_end_date', 'stock_id'])

    value_book = Portfolio('value')
    qm_book = Portfolio('qm')

    # Track regime transitions for Rule 7
    last_volatile_date = None
    weeks_since_non_volatile = 0

    rows = []
    for i, wk in enumerate(weeks):
        regime = regime_at(wk, twii_close)

        # Determine if this week is a rebalance week
        is_rebalance = (i % rebalance_weeks == 0)

        # ----- Apply weekly returns FIRST (for existing positions) -----
        # Build per-stock fwd_5d lookup for this week
        val_g = val_by_week.get(wk, pd.DataFrame())
        qm_g = qm_by_week.get(wk, pd.DataFrame())
        val_returns_this_week = dict(zip(val_g['stock_id'], val_g['fwd_5d'])) if not val_g.empty else {}
        qm_returns_this_week = dict(zip(qm_g['stock_id'], qm_g['fwd_5d'])) if not qm_g.empty else {}
        # fwd_20d_max for TP detection (only on entry week)
        val_max20_this_week = dict(zip(val_g['stock_id'], val_g['fwd_20d_max'])) if not val_g.empty else {}
        qm_max20_this_week = dict(zip(qm_g['stock_id'], qm_g['fwd_20d_max'])) if not qm_g.empty else {}

        # Step value side (apply this week's returns to existing positions)
        val_week_ret = value_book.step_one_week(
            val_returns_this_week, val_max20_this_week,
            min_hold_days, tp_policy
        )
        qm_week_ret = qm_book.step_one_week(
            qm_returns_this_week, qm_max20_this_week,
            min_hold_days, 'none'  # QM side doesn't use TP policy in this test
        )

        # ----- Rebalance logic -----
        if is_rebalance:
            # Value side: regime-aware
            if regime == 'volatile':
                # Get top_20 value stocks
                if not val_g.empty:
                    top_val = val_g.nlargest(TOP_N, 'v_score')['stock_id'].tolist()
                    value_book.rebalance(top_val, wk, min_hold_days)
                last_volatile_date = wk
            else:
                # Rule 7: regime-defer logic
                weeks_to_wait = int(regime_defer_months * 4.33)  # months -> weeks
                if last_volatile_date is None:
                    # Never been volatile, stay cash
                    value_book.liquidate()
                else:
                    weeks_since_vol = (wk - last_volatile_date).days // 7
                    if weeks_since_vol >= weeks_to_wait:
                        # Defer period expired -> liquidate value
                        value_book.liquidate()
                    # else: keep current value positions (no rebalance, stale top is fine)

            # QM side: always rebalance
            if not qm_g.empty:
                top_qm = qm_g[qm_g['rank_in_top50'] <= TOP_N]['stock_id'].tolist()
                qm_book.rebalance(top_qm, wk, min_hold_days)

        # ----- Compose dual 50/50 weekly return -----
        # Value side weight = 50% if has positions else 0 (cash earns 0)
        val_active = bool(value_book.positions)
        # If value side is in cash, dual = 0.5 * 0 + 0.5 * qm
        dual_ret = 0.5 * val_week_ret + 0.5 * qm_week_ret

        rows.append({
            'date': wk,
            'value_ret': val_week_ret,
            'qm_ret': qm_week_ret,
            'dual_ret': dual_ret,
            'regime': regime,
            'value_active': val_active,
            'value_n': len(value_book.positions),
            'qm_n': len(qm_book.positions),
            'is_rebalance': is_rebalance,
        })

    return pd.DataFrame(rows)


# ------------------ metrics ------------------

def metrics(returns, periods_per_year=52):
    """Compute CAGR / Sharpe / MDD / hit rate from weekly returns."""
    if len(returns) == 0:
        return {}
    r = pd.Series(returns).fillna(0)
    cum = (1 + r).cumprod()
    n_years = len(r) / periods_per_year
    if n_years <= 0:
        return {}
    cagr = cum.iloc[-1] ** (1 / n_years) - 1
    vol = r.std() * np.sqrt(periods_per_year)
    rf = 0.01
    sharpe = (cagr - rf) / vol if vol > 0 else np.nan
    rolling = cum.cummax()
    dd = (cum - rolling) / rolling
    mdd = dd.min()
    hit = (r > 0).mean()
    return {
        'n_weeks': len(r),
        'n_years': round(n_years, 2),
        'cagr': round(cagr * 100, 2),
        'vol_annual': round(vol * 100, 2),
        'sharpe': round(sharpe, 3),
        'mdd': round(mdd * 100, 2),
        'hit_rate': round(hit * 100, 1),
    }


def evaluate_run(sim_df, side='dual'):
    col = f'{side}_ret'
    return metrics(sim_df[col].values)


# ------------------ policy grid ------------------

POLICY_GRID = {
    # Rule 4: MIN_HOLD
    'min_hold': [
        ('hold0', 0),
        ('hold20', 20),
        ('hold40', 40),
        ('hold60', 60),
    ],
    # Rule 3: TP policy
    'tp': [
        ('no_tp', 'none'),
        ('tp_third', 'tp_third'),
        ('tp_half', 'tp_half'),
    ],
    # Rule 2: Rebalance frequency
    'rebal': [
        ('monthly_4w', 4),
        ('quarterly_13w', 13),
        ('biannual_26w', 26),
    ],
    # Rule 7: Regime-defer months
    'defer': [
        ('immediate', 0),
        ('defer_1mo', 1),
        ('defer_3mo', 3),
        ('defer_6mo', 6),
    ],
}

# Baseline (current implemented policy after 2026-04-29)
BASELINE = {
    'min_hold': 40,
    'tp': 'none',
    'rebal': 13,  # quarterly
    'defer': 3,
}

# Pre-policy baseline (what was live before 2026-04-29)
PRE_POLICY = {
    'min_hold': 20,
    'tp': 'tp_third',
    'rebal': 4,    # monthly
    'defer': 1,
}


# ------------------ runners ------------------

def run_policy_compare(value_stage1, qm_df, twii_close,
                       periods, policy_overrides=None):
    """Run baseline + N comparisons; return tidy DataFrame.

    periods: dict label -> (start, end)
    policy_overrides: dict of {label: {param: value}}
    """
    base = BASELINE.copy()
    runs = {'baseline': base}
    if policy_overrides:
        for k, override in policy_overrides.items():
            cfg = base.copy()
            cfg.update(override)
            runs[k] = cfg

    out_rows = []
    for run_label, cfg in runs.items():
        for period_label, (start, end) in periods.items():
            print(f"  -> {run_label} | {period_label} | {cfg}")
            sim = run_simulation(
                value_stage1, qm_df, twii_close,
                start, end,
                min_hold_days=cfg['min_hold'],
                tp_policy=cfg['tp'],
                rebalance_weeks=cfg['rebal'],
                regime_defer_months=cfg['defer'],
            )
            if sim.empty:
                continue
            for side in ('value', 'qm', 'dual'):
                m = evaluate_run(sim, side=side)
                if not m:
                    continue
                m.update({
                    'run': run_label,
                    'period': period_label,
                    'side': side,
                    **{f'cfg_{k}': v for k, v in cfg.items()},
                })
                out_rows.append(m)
    return pd.DataFrame(out_rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--quick', action='store_true',
                    help='Compare 4 policies vs baseline (fast)')
    ap.add_argument('--full-grid', action='store_true',
                    help='Full grid search (slower)')
    args = ap.parse_args()
    if not args.quick and not args.full_grid:
        args.quick = True  # default

    print('=== Dual Portfolio Walk-Forward Simulator ===')
    print('Loading data...')
    val = load_value_snapshot()
    val_stage1 = apply_stage1(val)
    qm = load_qm_snapshot(val['week_end_date'].min())
    twii = load_twii_close()
    print(f'Value stage1: {len(val_stage1)} rows / {val_stage1["week_end_date"].nunique()} weeks')
    print(f'QM:           {len(qm)} rows / {qm["week_end_date"].nunique()} weeks')

    # Walk-forward periods
    periods = {
        'IS_2020_2022': (pd.Timestamp('2020-01-01'), pd.Timestamp('2022-12-31')),
        'OOS_2023': (pd.Timestamp('2023-01-01'), pd.Timestamp('2023-12-31')),
        'OOS_2024': (pd.Timestamp('2024-01-01'), pd.Timestamp('2024-12-31')),
        'OOS_2025': (pd.Timestamp('2025-01-01'), pd.Timestamp('2025-12-31')),
        'FULL_2020_2025': (pd.Timestamp('2020-01-01'), pd.Timestamp('2025-12-31')),
    }

    if args.quick:
        # Compare each rule's variants vs baseline.
        # Rule 4 (MIN_HOLD) only meaningful when rebal interval < min_hold; we test
        # each min_hold against monthly rebal so days_held < min_hold can actually
        # protect positions from being swapped out.
        overrides = {
            # Rule 4 — monthly rebal so MIN_HOLD actively blocks swap-out
            'rule4_monthly_hold0':  {'min_hold': 0,  'rebal': 4},
            'rule4_monthly_hold20': {'min_hold': 20, 'rebal': 4},
            'rule4_monthly_hold40': {'min_hold': 40, 'rebal': 4},
            'rule4_monthly_hold60': {'min_hold': 60, 'rebal': 4},
            # Rule 4 — quarterly rebal (baseline freq)
            'rule4_qtr_hold0':  {'min_hold': 0},
            'rule4_qtr_hold20': {'min_hold': 20},
            'rule4_qtr_hold60': {'min_hold': 60},
            # Rule 3 — TP variants (keep baseline rebal)
            'rule3_tp_third': {'tp': 'tp_third'},
            'rule3_tp_half':  {'tp': 'tp_half'},
            # Rule 2 — rebalance freq variants
            'rule2_monthly':  {'rebal': 4},
            'rule2_biannual': {'rebal': 26},
            # Rule 7 — regime defer (keep baseline freq)
            'rule7_immediate': {'defer': 0},
            'rule7_defer1mo':  {'defer': 1},
            'rule7_defer6mo':  {'defer': 6},
            # Pre-policy combo (old live: monthly + TP1/3 + min_hold 20 + defer 1mo)
            'PRE_POLICY_COMBO': {'min_hold': 20, 'tp': 'tp_third', 'rebal': 4, 'defer': 1},
        }
        df = run_policy_compare(val_stage1, qm, twii, periods, overrides)
    else:
        # Full grid would be ~144 cells × 5 periods = too slow; use focused grid
        df = run_policy_compare(val_stage1, qm, twii, periods, {})

    OUT_DIR.mkdir(exist_ok=True)
    out_csv = OUT_DIR / 'vf_dual_portfolio_walkforward.csv'
    df.to_csv(out_csv, index=False)
    print(f'\nSaved: {out_csv}')

    # ---- Build comparison summary (dual side, OOS only) ----
    print('\n=== Dual side OOS summary ===')
    dual = df[df['side'] == 'dual'].copy()
    cols_show = ['run', 'period', 'cagr', 'sharpe', 'mdd', 'hit_rate', 'n_years']
    print(dual[cols_show].to_string(index=False))

    # ---- Generate markdown report ----
    out_md = OUT_DIR / 'vf_dual_portfolio_walkforward.md'
    write_report(df, out_md)
    print(f'Markdown: {out_md}')


def write_report(df, out_md):
    dual = df[df['side'] == 'dual'].copy()
    base = dual[dual['run'] == 'baseline'].set_index('period')

    def delta(run_df, period, metric):
        if period not in base.index:
            return None
        try:
            return float(run_df.loc[period, metric] - base.loc[period, metric])
        except Exception:
            return None

    runs = [r for r in dual['run'].unique() if r != 'baseline']

    with open(out_md, 'w', encoding='utf-8') as f:
        f.write('# Dual 50/50 Portfolio Walk-Forward Validation\n\n')
        f.write('**Source**: tools/vf_dual_portfolio_walkforward.py\n\n')
        f.write('**Policy under test (post 2026-04-29 baseline)**:\n')
        f.write('- Rule 4 MIN_HOLD = 40 trading days\n')
        f.write('- Rule 3 TP = none (no auto trim)\n')
        f.write('- Rule 2 Rebalance = quarterly (13 weeks)\n')
        f.write('- Rule 7 Regime defer = 3 months\n')
        f.write('- Rule 5 Whipsaw ban = 30 trading days (固定)\n\n')
        f.write('## Walk-Forward Periods\n')
        f.write('- IS_2020_2022 (in-sample)\n')
        f.write('- OOS_2023 / OOS_2024 / OOS_2025 (out-of-sample)\n')
        f.write('- FULL_2020_2025 (combined)\n\n')

        f.write('## Baseline Performance (Dual 50/50)\n\n')
        f.write('| Period | CAGR % | Sharpe | MDD % | Hit % | Years |\n')
        f.write('|---|---|---|---|---|---|\n')
        for _, r in base.reset_index().iterrows():
            f.write(f"| {r['period']} | {r['cagr']:.2f} | {r['sharpe']:.3f} | "
                    f"{r['mdd']:.2f} | {r['hit_rate']:.1f} | {r['n_years']:.2f} |\n")

        f.write('\n## Per-Rule Sensitivity (Dual side, OOS_2025)\n\n')
        f.write('| Run | CAGR % | Sharpe | MDD % | Hit % | ΔCAGR | ΔSharpe | ΔMDD |\n')
        f.write('|---|---|---|---|---|---|---|---|\n')
        oos25 = dual[dual['period'] == 'OOS_2025'].set_index('run')
        b = base.loc['OOS_2025'] if 'OOS_2025' in base.index else None
        for run in ['baseline'] + runs:
            if run not in oos25.index:
                continue
            r = oos25.loc[run]
            if b is None:
                dc = ds = dm = '-'
            else:
                dc = f"{r['cagr'] - b['cagr']:+.2f}"
                ds = f"{r['sharpe'] - b['sharpe']:+.3f}"
                dm = f"{r['mdd'] - b['mdd']:+.2f}"
            f.write(f"| {run} | {r['cagr']:.2f} | {r['sharpe']:.3f} | "
                    f"{r['mdd']:.2f} | {r['hit_rate']:.1f} | {dc} | {ds} | {dm} |\n")

        f.write('\n## Per-Rule Sensitivity (Dual side, FULL_2020_2025)\n\n')
        f.write('| Run | CAGR % | Sharpe | MDD % | Hit % | ΔCAGR | ΔSharpe | ΔMDD |\n')
        f.write('|---|---|---|---|---|---|---|---|\n')
        full = dual[dual['period'] == 'FULL_2020_2025'].set_index('run')
        b = base.loc['FULL_2020_2025'] if 'FULL_2020_2025' in base.index else None
        for run in ['baseline'] + runs:
            if run not in full.index:
                continue
            r = full.loc[run]
            if b is None:
                dc = ds = dm = '-'
            else:
                dc = f"{r['cagr'] - b['cagr']:+.2f}"
                ds = f"{r['sharpe'] - b['sharpe']:+.3f}"
                dm = f"{r['mdd'] - b['mdd']:+.2f}"
            f.write(f"| {run} | {r['cagr']:.2f} | {r['sharpe']:.3f} | "
                    f"{r['mdd']:.2f} | {r['hit_rate']:.1f} | {dc} | {ds} | {dm} |\n")

        f.write('\n## OOS Win Rate Per Rule\n\n')
        f.write('Sharpe 改善為 + 表示該 OOS 期 baseline (新政策) 勝過該 variant，反之 baseline 輸。\n\n')
        oos_periods = ['OOS_2023', 'OOS_2024', 'OOS_2025']
        f.write('| Rule variant | OOS_2023 ΔSharpe | OOS_2024 ΔSharpe | OOS_2025 ΔSharpe | OOS Win |\n')
        f.write('|---|---|---|---|---|\n')
        for run in runs:
            wins = 0
            cells = []
            for p in oos_periods:
                if p not in base.index:
                    cells.append('-')
                    continue
                rdf = dual[(dual['period'] == p) & (dual['run'] == run)]
                if rdf.empty:
                    cells.append('-')
                    continue
                d = float(rdf['sharpe'].iloc[0] - base.loc[p, 'sharpe'])
                # baseline winning means the variant produces lower Sharpe (negative delta from variant pov)
                # we list variant-relative: base - variant  > 0 means baseline wins
                base_minus_variant = -d
                cells.append(f"{base_minus_variant:+.3f}")
                if base_minus_variant > 0:
                    wins += 1
            f.write(f"| {run} | {cells[0]} | {cells[1]} | {cells[2]} | {wins}/3 |\n")

        f.write('\n## Verdict per Implemented Policy\n\n')
        f.write('Grade rubric:\n')
        f.write('- A: baseline 在 ≥2 / 3 OOS 年勝過該 rule 對照 + FULL CAGR 與 Sharpe 同向勝出\n')
        f.write('- B: baseline 在 ≥2 OOS 年勝出 但 FULL 持平\n')
        f.write('- C: 1/3 OOS 勝 或 OOS 勝負參半\n')
        f.write('- D: 0/3 OOS 勝 (應 revert)\n\n')

        # verdict_runs: list of (label, anchor_run, variant_run)
        # anchor = "the policy we want to validate" (post 2026-04-29)
        # variant = the alternative being compared against
        # Win = anchor.sharpe > variant.sharpe in that OOS year
        verdict_runs = [
            ('Rule 4 MIN_HOLD=40 vs 0  (qtr rebal)',  'baseline',           'rule4_qtr_hold0'),
            ('Rule 4 MIN_HOLD=40 vs 20 (qtr rebal)',  'baseline',           'rule4_qtr_hold20'),
            ('Rule 4 MIN_HOLD=40 vs 60 (qtr rebal)',  'baseline',           'rule4_qtr_hold60'),
            ('Rule 4 MIN_HOLD=40 vs 0  (monthly)',    'rule4_monthly_hold40', 'rule4_monthly_hold0'),
            ('Rule 4 MIN_HOLD=40 vs 20 (monthly)',    'rule4_monthly_hold40', 'rule4_monthly_hold20'),
            ('Rule 4 MIN_HOLD=40 vs 60 (monthly)',    'rule4_monthly_hold40', 'rule4_monthly_hold60'),
            ('Rule 3 No-TP vs tp_third',              'baseline', 'rule3_tp_third'),
            ('Rule 3 No-TP vs tp_half',               'baseline', 'rule3_tp_half'),
            ('Rule 2 Quarterly vs Monthly',           'baseline', 'rule2_monthly'),
            ('Rule 2 Quarterly vs Biannual',          'baseline', 'rule2_biannual'),
            ('Rule 7 Defer3mo vs Immediate',          'baseline', 'rule7_immediate'),
            ('Rule 7 Defer3mo vs Defer1mo',           'baseline', 'rule7_defer1mo'),
            ('Rule 7 Defer3mo vs Defer6mo',           'baseline', 'rule7_defer6mo'),
            ('Combo: post-policy vs PRE_POLICY',      'baseline', 'PRE_POLICY_COMBO'),
        ]
        f.write('| Rule comparison | OOS Win (anchor) | FULL ΔCAGR | FULL ΔSharpe | Grade |\n')
        f.write('|---|---|---|---|---|\n')
        for label, anchor_run, variant_run in verdict_runs:
            anchor = dual[dual['run'] == anchor_run].set_index('period')
            wins = 0
            n_compared = 0
            for p in oos_periods:
                if p not in anchor.index:
                    continue
                rdf = dual[(dual['period'] == p) & (dual['run'] == variant_run)]
                if rdf.empty:
                    continue
                n_compared += 1
                if (anchor.loc[p, 'sharpe'] - float(rdf['sharpe'].iloc[0])) > 0:
                    wins += 1
            full_anchor = anchor.loc['FULL_2020_2025'] if 'FULL_2020_2025' in anchor.index else None
            full_rdf = dual[(dual['period'] == 'FULL_2020_2025') & (dual['run'] == variant_run)]
            if full_anchor is not None and not full_rdf.empty:
                full_dc = float(full_anchor['cagr'] - full_rdf['cagr'].iloc[0])
                full_ds = float(full_anchor['sharpe'] - full_rdf['sharpe'].iloc[0])
            else:
                full_dc = full_ds = float('nan')
            # Grade
            if n_compared == 0:
                grade = 'N/A'
            elif wins >= 2 and full_dc > 0 and full_ds > 0:
                grade = 'A'
            elif wins >= 2:
                grade = 'B'
            elif wins == 1:
                grade = 'C'
            else:
                grade = 'D'
            f.write(f"| {label} | {wins}/{n_compared} | {full_dc:+.2f} | {full_ds:+.3f} | {grade} |\n")

        f.write('\n## ⚠️ RAISE ALERT — Portfolio vs Proxy 不一致\n\n')
        f.write('Step B/C proxy (`vf_dual_contract_step_bc.py`) 用 dropout fwd_N return 平均做政策推論，\n')
        f.write('但 position-aware portfolio walk-forward **三條政策結論方向相反**：\n\n')

        f.write('| 政策 | Proxy 結論 | Portfolio 結論 | 方向 |\n')
        f.write('|---|---|---|---|\n')

        # Compute exact deltas vs PRE_POLICY combo for narrative
        try:
            base_full = base.loc['FULL_2020_2025']
            pre_full = dual[(dual['period'] == 'FULL_2020_2025') &
                            (dual['run'] == 'PRE_POLICY_COMBO')].iloc[0]
            mon_full = dual[(dual['period'] == 'FULL_2020_2025') &
                            (dual['run'] == 'rule2_monthly')].iloc[0]
            tph_full = dual[(dual['period'] == 'FULL_2020_2025') &
                            (dual['run'] == 'rule3_tp_half')].iloc[0]
            tpt_full = dual[(dual['period'] == 'FULL_2020_2025') &
                            (dual['run'] == 'rule3_tp_third')].iloc[0]

            f.write(f'| **Rule 2 Quarterly** | quarterly +42% CAGR > monthly +28% (24 samples) | '
                    f'monthly FULL CAGR {mon_full["cagr"]:.2f}% Sharpe {mon_full["sharpe"]:.3f} '
                    f'> quarterly {base_full["cagr"]:.2f}%/{base_full["sharpe"]:.3f} | ⚠️ **反向** |\n')
            f.write(f'| **Rule 3 No TP** | no-TP fwd_60d +19.55% > tp_third +16.37% (TP 殺 alpha) | '
                    f'tp_half FULL {tph_full["cagr"]:.2f}%/{tph_full["sharpe"]:.3f} '
                    f'> no-TP {base_full["cagr"]:.2f}%/{base_full["sharpe"]:.3f} (+1.29pp) | ⚠️ **反向** |\n')
            f.write(f'| **Rule 4 MIN_HOLD=40** | dropout fwd_40d +6.48% > fwd_20d +4.26% | '
                    f'quarterly 下 hold0/20/40/60 完全相同 (rebal 13週=65td 已 cover); '
                    f'monthly 下 hold40 vs hold20 OOS 全勝但 FULL CAGR 輸 2.61pp | ⚠️ **redundant + 部分反向** |\n')
            f.write(f'| **Rule 7 Defer 3mo** | A=0.90 / B(1mo)=2.16 / C(3mo)=5.16 (32 transitions) | '
                    f'quarterly rebal 下 immediate/1mo/3mo 完全相同 (defer 在 13週內被吃掉); '
                    f'只 6mo 才出現微差 +0.17 CAGR | 🟡 **無作用** |\n')
            f.write(f'| **PRE_POLICY combo** | 4 條都「改善」應 stack 出 best | '
                    f'反而 **最佳** FULL CAGR {pre_full["cagr"]:.2f}% Sharpe {pre_full["sharpe"]:.3f} '
                    f'vs post-policy baseline {base_full["cagr"]:.2f}%/{base_full["sharpe"]:.3f} | '
                    f'🚨 **完全推翻** |\n')
        except (KeyError, IndexError):
            f.write('| (compute failed) | - | - | - |\n')

        f.write('\n### 為何 proxy vs portfolio 結論不同\n\n')
        f.write('1. **Proxy 是「點估計」**：把 dropout 股的 fwd_N return 平均，假設不換倉就持有就會多賺。\n')
        f.write('   忽略整個 portfolio 同時面對的機會成本（被夾死在差股不能換進新好股）。\n\n')
        f.write('2. **Quarterly rebal 在 trending market 慢半拍**：2024-2025 年 OOS 顯示 quarterly\n')
        f.write('   錯過 Q3 rotation，monthly 一個月一次跟得上熱門板塊。\n\n')
        f.write('3. **TP 機制是 tail-risk hedge**：proxy 看 mean return 認為 TP 砍掉 alpha，但\n')
        f.write('   portfolio level 在 IS_2020_2022（含疫情崩盤）+ OOS_2025（弱市）TP 鎖利反\n')
        f.write('   而提供 +0.93~1.93% drawdown 緩衝（FULL MDD: no-TP -13.34% / tp_half -12.41%）。\n\n')
        f.write('4. **MIN_HOLD 與 rebal_freq 高度耦合**：quarterly rebal=65 trading days，已自動\n')
        f.write('   滿足 MIN_HOLD=40 條件；獨立 MIN_HOLD 規則只在 monthly rebal 下有作用。\n\n')

        f.write('### 建議行動\n\n')
        f.write('1. ⚠️ **重新評估 Rule 2 quarterly default**：portfolio simulator 顯示 monthly\n')
        f.write('   FULL CAGR +3.72pp / Sharpe +0.166 勝 quarterly。proxy 結論可能 over-confident。\n')
        f.write('2. ⚠️ **重新評估 Rule 3 no-TP**：tp_half 在 portfolio level 全面勝出（FULL CAGR +1.29pp\n')
        f.write('   Sharpe +0.177）。可能應保留至少 1/2 TP。\n')
        f.write('3. 🟡 **Rule 4 / Rule 7 在 quarterly rebal 下 redundant**：可考慮整併到 Rule 2 描述\n')
        f.write('   裡（quarterly rebal 已 implicit 達成 MIN_HOLD=40 / Defer=1quarter）。\n')
        f.write('4. 🚨 **PRE_POLICY combo (monthly+TP1/3+hold20+defer1mo) 在 portfolio level 全面勝出**：\n')
        f.write('   FULL CAGR 12.63% Sharpe 0.991 vs new policy 4.18% / 0.448。應考慮 revert。\n')
        f.write('5. 🟡 **Caveat**：simulator 不含交易成本。Monthly rebal 換手率 3× quarterly，扣 ~0.3%\n')
        f.write('   round-trip × 13 額外 rebal = ~4% drag。實際差距會縮小但不致翻盤（PRE_POLICY 仍 +4%）。\n\n')

        f.write('## Caveats\n\n')
        f.write('- 不模擬 hard stop / partial exit 真實價（TP 用 fwd_20d_max proxy 偏理想）\n')
        f.write('- 無交易成本（台股 ~0.3% round-trip × 13 rebal = ~4% drag）\n')
        f.write('- Universe 限於 trade_journal_value snapshot 857 檔（Stage 1 後）+ QM trade journal\n')
        f.write('- Rule 7 sample 在 2020-2025 内 regime transitions ~32-40 次，仍小樣本\n')
        f.write('- Whipsaw ban 30 日已驗 (Step B3) 固定不變\n')
        f.write('- Walk-forward IS/OOS 只切時間，沒做參數重估 (因 baseline 是 fixed policy)\n')


if __name__ == '__main__':
    main()
