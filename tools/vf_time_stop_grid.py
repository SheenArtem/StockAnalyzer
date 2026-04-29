"""
Time Stop Grid Validation (extends vf_dual_portfolio_walkforward.py)

驗證問題:
  Dual contract 既有 Rule 1-4 沒覆蓋「entry 後盤整 N 天無突破」這場景。
  資金被綁住但毫無進度。Time Stop 設計：

    若 entry 後 hold_days 內累計報酬未達 progress_pct -> 釋放部位

  獨立加在現有 hard exit / TP / monthly rebal 之前。

Grid:
  hold_days: 5 / 10 / 15 / 20 / 30
  progress_pct: +0% / +1% / +2% / +3%
  4 x 5 = 20 cells + PRE_POLICY (no time stop) baseline

Baseline = PRE_POLICY (b18758d revert 後 live 設定):
  MIN_HOLD = 20, TP staged 1/3, monthly rebal (4w), regime defer 1mo

實作要點:
  - position 加 'cum_ret' 欄位 (since entry 累積報酬)
  - 每週 step_one_week: cum_ret = (1+cum_ret)*(1+r) - 1
  - Time Stop check: days_held >= hold_days AND days_held >= MIN_HOLD AND cum_ret < progress_pct
    -> mark exit (current bar close), 進入 30d ban (avoid immediate re-buy)

Walk-forward:
  IS_2020_2022 (含疫情 + 通膨開啟)
  OOS_2023 / OOS_2024 / OOS_2025
  FULL_2020_2025

Output:
  reports/vf_time_stop_grid.csv (每 cell 完整 metrics)
  reports/vf_time_stop_grid.md (verdict + LOO 表)

Usage:
  python tools/vf_time_stop_grid.py
"""

import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Reuse all the heavy lifting from vf_dual_portfolio_walkforward.py
from tools.vf_dual_portfolio_walkforward import (  # noqa: E402
    SNAP_VAL, SNAP_QM, TWII_BENCH,
    MAX_PE, MAX_PB, PE_X_PB_MAX, MIN_TV,
    WEIGHTS, TOP_N, TRADING_DAYS_PER_WEEK, WHIPSAW_BAN_TDAYS, TP_THRESHOLD,
    load_value_snapshot, load_qm_snapshot, apply_stage1, compute_value_score,
    load_twii_close, regime_at,
    metrics, evaluate_run,
)

OUT_DIR = ROOT / 'reports'

# Time Stop grid
HOLD_DAYS_GRID = [5, 10, 15, 20, 30]
PROGRESS_PCT_GRID = [0.00, 0.01, 0.02, 0.03]

# Baseline = PRE_POLICY (revert 後 live 設定, 8 條 rule 全 PRE_POLICY)
# Use kwargs matching run_time_stop_simulation signature
PRE_POLICY = {
    'min_hold_days': 20,
    'tp_policy': 'tp_third',
    'rebalance_weeks': 4,    # monthly
    'regime_defer_months': 1,
}


# ------------------ Time Stop aware Portfolio ------------------

class TimeStopPortfolio:
    """Single-side position book with optional Time Stop.

    Extends the original Portfolio class with cum_ret tracking and
    time_stop_exit logic. When time_stop_hold_days is None, behaviour
    matches the baseline simulator exactly.
    """

    def __init__(self, side, top_n=TOP_N,
                 time_stop_hold_days=None,
                 time_stop_progress_pct=None):
        self.side = side
        self.top_n = top_n
        self.positions = {}   # stock_id -> {entry_date, days_held, weight_raw, tp_hit, cum_ret}
        self.banned = {}
        self.weekly_returns = []
        # Time Stop config
        self.ts_hold = time_stop_hold_days
        self.ts_pct = time_stop_progress_pct
        # Stats counters
        self.exit_counts = defaultdict(int)  # 'time_stop' / 'rebal_swap' / etc.
        self.holding_lengths = []  # closed-trade days_held samples

    def step_one_week(self, week_returns, week_max20, min_hold_days, tp_policy, week_date):
        """Apply one week of holding period.

        Returns: realized return for this week (already weighted within side).
        """
        if not self.positions:
            self.weekly_returns.append(0.0)
            return 0.0

        total_raw = sum(p['weight_raw'] for p in self.positions.values())
        if total_raw <= 0:
            self.weekly_returns.append(0.0)
            return 0.0

        total_ret = 0.0
        ts_evict = []  # positions to evict via Time Stop after returns applied

        for sid in list(self.positions.keys()):
            pos = self.positions[sid]
            r = week_returns.get(sid, np.nan)
            if pd.isna(r):
                r = 0.0
            w = pos['weight_raw'] / total_raw

            # TP detection (entry week only)
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

            # Update position stats: days held + cumulative return since entry
            pos['days_held'] += TRADING_DAYS_PER_WEEK
            pos['cum_ret'] = (1.0 + pos.get('cum_ret', 0.0)) * (1.0 + r) - 1.0

            # TP exit at end of 4-week window (uses week_date end-of-bar; same as baseline)
            if pos.get('tp_hit_pending') and pos['days_held'] >= 20:
                if tp_policy == 'tp_third':
                    realized = w * (1.0 / 3.0) * TP_THRESHOLD
                    total_ret += realized
                    pos['weight_raw'] *= (2.0 / 3.0)
                elif tp_policy == 'tp_half':
                    realized = w * 0.5 * TP_THRESHOLD
                    total_ret += realized
                    pos['weight_raw'] *= 0.5
                pos['tp_hit'] = True
                pos['tp_hit_pending'] = False

            # ----- Time Stop check (no look-ahead: uses cum_ret AFTER current week return) -----
            # Trigger only when:
            #   - time stop is enabled (ts_hold not None)
            #   - days_held >= ts_hold (window elapsed)
            #   - days_held >= min_hold_days (don't violate hard floor)
            #   - cum_ret < ts_pct (insufficient progress)
            if (
                self.ts_hold is not None
                and pos['days_held'] >= self.ts_hold
                and pos['days_held'] >= min_hold_days
                and pos['cum_ret'] < self.ts_pct
            ):
                ts_evict.append(sid)

        self.weekly_returns.append(total_ret)

        # Apply Time Stop evictions: ban from re-entry for WHIPSAW_BAN_TDAYS
        for sid in ts_evict:
            self.exit_counts['time_stop'] += 1
            self.holding_lengths.append(self.positions[sid]['days_held'])
            del self.positions[sid]
            self.banned[sid] = week_date

        return total_ret

    def rebalance(self, target_top, week_date, min_hold_days):
        """Replace dropouts (days_held >= min_hold_days)."""
        changed = 0
        keep = {}
        for sid, pos in self.positions.items():
            if sid in target_top:
                keep[sid] = pos
            elif pos['days_held'] < min_hold_days:
                keep[sid] = pos
            else:
                changed += 1
                self.exit_counts['rebal_swap'] += 1
                self.holding_lengths.append(pos['days_held'])
                self.banned[sid] = week_date
        self.positions = keep

        for sid in target_top:
            if sid in self.positions:
                continue
            if sid in self.banned:
                ban_date = self.banned[sid]
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
                'cum_ret': 0.0,
            }
            changed += 1
        return changed

    def liquidate(self, reason='liquidate'):
        for sid, pos in self.positions.items():
            self.exit_counts[reason] += 1
            self.holding_lengths.append(pos['days_held'])
        self.positions.clear()


# ------------------ Time Stop simulation runner ------------------

def run_time_stop_simulation(value_df_stage1, qm_df, twii_close,
                             start, end,
                             time_stop_hold_days=None,
                             time_stop_progress_pct=None,
                             min_hold_days=20,
                             tp_policy='tp_third',
                             rebalance_weeks=4,
                             regime_defer_months=1):
    """Run dual 50/50 walk-forward with optional Time Stop layer.

    When time_stop_hold_days=None and time_stop_progress_pct=None, behaves
    like the baseline simulator (PRE_POLICY).
    """
    weeks = sorted(value_df_stage1['week_end_date'].unique())
    weeks = [w for w in weeks if start <= w <= end]
    if not weeks:
        return pd.DataFrame(), {'value': {}, 'qm': {}}

    value_df_stage1 = value_df_stage1.copy()
    value_df_stage1['v_score'] = compute_value_score(value_df_stage1)

    val_by_week = {w: g for w, g in value_df_stage1.groupby('week_end_date')}
    qm_by_week = {w: g for w, g in qm_df.groupby('week_end_date')}

    value_book = TimeStopPortfolio('value',
                                   time_stop_hold_days=time_stop_hold_days,
                                   time_stop_progress_pct=time_stop_progress_pct)
    qm_book = TimeStopPortfolio('qm',
                                time_stop_hold_days=time_stop_hold_days,
                                time_stop_progress_pct=time_stop_progress_pct)

    last_volatile_date = None

    rows = []
    for i, wk in enumerate(weeks):
        regime = regime_at(wk, twii_close)
        is_rebalance = (i % rebalance_weeks == 0)

        val_g = val_by_week.get(wk, pd.DataFrame())
        qm_g = qm_by_week.get(wk, pd.DataFrame())
        val_returns = dict(zip(val_g['stock_id'], val_g['fwd_5d'])) if not val_g.empty else {}
        qm_returns = dict(zip(qm_g['stock_id'], qm_g['fwd_5d'])) if not qm_g.empty else {}
        val_max20 = dict(zip(val_g['stock_id'], val_g['fwd_20d_max'])) if not val_g.empty else {}
        qm_max20 = dict(zip(qm_g['stock_id'], qm_g['fwd_20d_max'])) if not qm_g.empty else {}

        # Apply weekly returns (including Time Stop evictions inside step_one_week)
        val_week_ret = value_book.step_one_week(
            val_returns, val_max20, min_hold_days, tp_policy, wk
        )
        qm_week_ret = qm_book.step_one_week(
            qm_returns, qm_max20, min_hold_days, 'none', wk
        )

        # Rebalance logic
        if is_rebalance:
            if regime == 'volatile':
                if not val_g.empty:
                    top_val = val_g.nlargest(TOP_N, 'v_score')['stock_id'].tolist()
                    value_book.rebalance(top_val, wk, min_hold_days)
                last_volatile_date = wk
            else:
                weeks_to_wait = int(regime_defer_months * 4.33)
                if last_volatile_date is None:
                    value_book.liquidate(reason='regime_liquidate')
                else:
                    weeks_since_vol = (wk - last_volatile_date).days // 7
                    if weeks_since_vol >= weeks_to_wait:
                        value_book.liquidate(reason='regime_liquidate')

            if not qm_g.empty:
                top_qm = qm_g[qm_g['rank_in_top50'] <= TOP_N]['stock_id'].tolist()
                qm_book.rebalance(top_qm, wk, min_hold_days)

        val_active = bool(value_book.positions)
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

    sim_df = pd.DataFrame(rows)
    stats = {
        'value_exits': dict(value_book.exit_counts),
        'qm_exits': dict(qm_book.exit_counts),
        'value_avg_hold': float(np.mean(value_book.holding_lengths)) if value_book.holding_lengths else 0,
        'qm_avg_hold': float(np.mean(qm_book.holding_lengths)) if qm_book.holding_lengths else 0,
        'value_n_closed': len(value_book.holding_lengths),
        'qm_n_closed': len(qm_book.holding_lengths),
    }
    return sim_df, stats


# ------------------ helpers ------------------

def cell_label(hold_days, progress_pct):
    return f'TS_h{hold_days}_p{int(progress_pct*100)}pct'


def yearly_metrics_from_sim(sim_df, side='dual'):
    """Compute per-year CAGR/Sharpe/MDD for LOO analysis."""
    df = sim_df[['date', f'{side}_ret']].copy()
    df['year'] = pd.to_datetime(df['date']).dt.year
    out = {}
    for y, g in df.groupby('year'):
        m = metrics(g[f'{side}_ret'].values)
        out[y] = m
    return out


def grid_run(value_stage1, qm_df, twii_close, start, end):
    """Run baseline + 20 Time Stop cells over a date range.

    Returns: list of dicts (each with cell label + metrics + stats)
    """
    cells = []

    # Baseline (no Time Stop) = PRE_POLICY
    print(f'  -> baseline (PRE_POLICY no TimeStop)')
    sim, stats = run_time_stop_simulation(
        value_stage1, qm_df, twii_close, start, end,
        time_stop_hold_days=None, time_stop_progress_pct=None,
        **PRE_POLICY,
    )
    if not sim.empty:
        for side in ('dual', 'value', 'qm'):
            m = evaluate_run(sim, side=side)
            if m:
                m.update({
                    'cell': 'baseline_no_ts',
                    'hold_days': None, 'progress_pct': None,
                    'side': side,
                    f'{side}_avg_hold': stats[f'{side}_avg_hold'] if side != 'dual' else None,
                    f'{side}_n_closed': stats[f'{side}_n_closed'] if side != 'dual' else None,
                })
                # Time stop counts
                if side == 'value':
                    m['time_stop_exits'] = stats['value_exits'].get('time_stop', 0)
                    m['rebal_swap_exits'] = stats['value_exits'].get('rebal_swap', 0)
                elif side == 'qm':
                    m['time_stop_exits'] = stats['qm_exits'].get('time_stop', 0)
                    m['rebal_swap_exits'] = stats['qm_exits'].get('rebal_swap', 0)
                cells.append(m)

    # Grid: 5 hold_days × 4 progress_pct
    for hd in HOLD_DAYS_GRID:
        for pp in PROGRESS_PCT_GRID:
            label = cell_label(hd, pp)
            print(f'  -> {label}')
            sim, stats = run_time_stop_simulation(
                value_stage1, qm_df, twii_close, start, end,
                time_stop_hold_days=hd, time_stop_progress_pct=pp,
                **PRE_POLICY,
            )
            if sim.empty:
                continue
            for side in ('dual', 'value', 'qm'):
                m = evaluate_run(sim, side=side)
                if not m:
                    continue
                m.update({
                    'cell': label,
                    'hold_days': hd,
                    'progress_pct': pp,
                    'side': side,
                })
                if side == 'value':
                    m['time_stop_exits'] = stats['value_exits'].get('time_stop', 0)
                    m['rebal_swap_exits'] = stats['value_exits'].get('rebal_swap', 0)
                    m['avg_hold_days'] = stats['value_avg_hold']
                    m['n_closed'] = stats['value_n_closed']
                elif side == 'qm':
                    m['time_stop_exits'] = stats['qm_exits'].get('time_stop', 0)
                    m['rebal_swap_exits'] = stats['qm_exits'].get('rebal_swap', 0)
                    m['avg_hold_days'] = stats['qm_avg_hold']
                    m['n_closed'] = stats['qm_n_closed']
                cells.append(m)
    return cells


def loo_run(value_stage1, qm_df, twii_close, hold_days, progress_pct, years):
    """Leave-one-out: drop each year, compute Sharpe of remaining FULL.

    Implementation: for each year y, run full sim, then drop that year's
    weekly returns and re-compute metrics.
    """
    start = pd.Timestamp(f'{min(years)}-01-01')
    end = pd.Timestamp(f'{max(years)}-12-31')

    sim, _ = run_time_stop_simulation(
        value_stage1, qm_df, twii_close, start, end,
        time_stop_hold_days=hold_days, time_stop_progress_pct=progress_pct,
        **PRE_POLICY,
    )
    if sim.empty:
        return {}

    sim['year'] = pd.to_datetime(sim['date']).dt.year
    out = {}
    for y in years:
        leftover = sim[sim['year'] != y]
        if leftover.empty:
            out[y] = {}
            continue
        m = metrics(leftover['dual_ret'].values)
        out[y] = m
    return out


# ------------------ main ------------------

def main():
    print('=== Time Stop Grid Validation ===')
    print('Loading data...')
    val = load_value_snapshot()
    val_stage1 = apply_stage1(val)
    qm = load_qm_snapshot(val['week_end_date'].min())
    twii = load_twii_close()
    print(f'Value stage1: {len(val_stage1)} rows / {val_stage1["week_end_date"].nunique()} weeks')
    print(f'QM:           {len(qm)} rows / {qm["week_end_date"].nunique()} weeks')

    periods = {
        'IS_2020_2022': (pd.Timestamp('2020-01-01'), pd.Timestamp('2022-12-31')),
        'OOS_2023': (pd.Timestamp('2023-01-01'), pd.Timestamp('2023-12-31')),
        'OOS_2024': (pd.Timestamp('2024-01-01'), pd.Timestamp('2024-12-31')),
        'OOS_2025': (pd.Timestamp('2025-01-01'), pd.Timestamp('2025-12-31')),
        'BEAR_2022': (pd.Timestamp('2022-01-01'), pd.Timestamp('2022-12-31')),
        'FULL_2020_2025': (pd.Timestamp('2020-01-01'), pd.Timestamp('2025-12-31')),
    }

    all_rows = []
    for period_label, (start, end) in periods.items():
        print(f'\n[Period] {period_label}: {start.date()} -> {end.date()}')
        cells = grid_run(val_stage1, qm, twii, start, end)
        for c in cells:
            c['period'] = period_label
            all_rows.append(c)

    df = pd.DataFrame(all_rows)
    OUT_DIR.mkdir(exist_ok=True)
    out_csv = OUT_DIR / 'vf_time_stop_grid.csv'
    df.to_csv(out_csv, index=False)
    print(f'\nSaved: {out_csv}')

    # ---- LOO on best cell ----
    print('\n=== Leave-one-out on best Sharpe cell (FULL_2020_2025, dual) ===')
    full_dual = df[(df['period'] == 'FULL_2020_2025') & (df['side'] == 'dual')].copy()
    full_dual_grid = full_dual[full_dual['cell'] != 'baseline_no_ts'].copy()
    if not full_dual_grid.empty:
        best = full_dual_grid.sort_values('sharpe', ascending=False).iloc[0]
        best_label = best['cell']
        best_hd = int(best['hold_days'])
        best_pp = float(best['progress_pct'])
        print(f'Best cell: {best_label} (hold_days={best_hd}, progress_pct={best_pp:.2f})')
        print(f'  Sharpe={best["sharpe"]:.3f} CAGR={best["cagr"]:.2f}% MDD={best["mdd"]:.2f}%')
        years = list(range(2020, 2026))
        loo = loo_run(val_stage1, qm, twii, best_hd, best_pp, years)
        # Also baseline LOO
        loo_base = {}
        sim_base, _ = run_time_stop_simulation(
            val_stage1, qm, twii,
            pd.Timestamp('2020-01-01'), pd.Timestamp('2025-12-31'),
            time_stop_hold_days=None, time_stop_progress_pct=None,
            **PRE_POLICY,
        )
        sim_base['year'] = pd.to_datetime(sim_base['date']).dt.year
        for y in years:
            leftover = sim_base[sim_base['year'] != y]
            loo_base[y] = metrics(leftover['dual_ret'].values) if not leftover.empty else {}
    else:
        best_label = None
        loo = {}
        loo_base = {}
        best_hd = None
        best_pp = None

    # ---- Markdown report ----
    out_md = OUT_DIR / 'vf_time_stop_grid.md'
    write_report(df, out_md, best_label=best_label, best_hd=best_hd, best_pp=best_pp,
                 loo=loo, loo_base=loo_base)
    print(f'Markdown: {out_md}')


def write_report(df, out_md, best_label=None, best_hd=None, best_pp=None,
                 loo=None, loo_base=None):
    dual = df[df['side'] == 'dual'].copy()
    base = dual[dual['cell'] == 'baseline_no_ts'].set_index('period')

    with open(out_md, 'w', encoding='utf-8') as f:
        f.write('# Time Stop Grid Validation\n\n')
        f.write('**Source**: tools/vf_time_stop_grid.py (extends vf_dual_portfolio_walkforward.py)\n\n')
        f.write('**Hypothesis**: 進場後盤整 N 天無突破無破停損 -> 釋放部位避免機會成本\n\n')
        f.write('**Baseline = PRE_POLICY** (b18758d revert 後 live 設定):\n')
        f.write('- MIN_HOLD = 20 trading days\n')
        f.write('- TP staged 1/3 at +10%\n')
        f.write('- Monthly rebalance (4w)\n')
        f.write('- Regime defer 1mo\n')
        f.write('- Whipsaw ban 30d (固定)\n\n')
        f.write('**Time Stop Grid**: hold_days {5,10,15,20,30} x progress_pct {0,1,2,3}% = 20 cells\n\n')
        f.write('Trigger condition: `days_held >= hold_days AND days_held >= MIN_HOLD AND cum_ret < progress_pct`\n')
        f.write('(MIN_HOLD floor 確保不違反 hard floor; cum_ret 計算到當週 close, 無 look-ahead)\n\n')

        # ---- Baseline performance ----
        f.write('## Baseline Performance (PRE_POLICY no Time Stop, Dual 50/50)\n\n')
        f.write('| Period | CAGR % | Sharpe | MDD % | Hit % | Years |\n')
        f.write('|---|---|---|---|---|---|\n')
        for _, r in base.reset_index().iterrows():
            f.write(f"| {r['period']} | {r['cagr']:.2f} | {r['sharpe']:.3f} | "
                    f"{r['mdd']:.2f} | {r['hit_rate']:.1f} | {r['n_years']:.2f} |\n")

        # ---- Full grid table (FULL_2020_2025) ----
        f.write('\n## Grid Search Results (Dual side, FULL_2020_2025)\n\n')
        f.write('Δ = (cell - baseline). Positive ΔCAGR/ΔSharpe = improvement.\n')
        f.write('Note: ΔMDD positive 也 = improvement (MDD 是負值, 趨近 0 = 改善).\n\n')

        full_dual = dual[dual['period'] == 'FULL_2020_2025'].set_index('cell')
        if 'baseline_no_ts' in full_dual.index:
            b = full_dual.loc['baseline_no_ts']
            f.write('| Cell | hold_d | prog% | CAGR % | Sharpe | MDD % | Hit % | ΔCAGR | ΔSharpe | ΔMDD |\n')
            f.write('|---|---|---|---|---|---|---|---|---|---|\n')
            f.write(f"| baseline | - | - | {b['cagr']:.2f} | {b['sharpe']:.3f} | "
                    f"{b['mdd']:.2f} | {b['hit_rate']:.1f} | - | - | - |\n")
            for hd in HOLD_DAYS_GRID:
                for pp in PROGRESS_PCT_GRID:
                    label = cell_label(hd, pp)
                    if label not in full_dual.index:
                        continue
                    r = full_dual.loc[label]
                    dc = r['cagr'] - b['cagr']
                    ds = r['sharpe'] - b['sharpe']
                    dm = r['mdd'] - b['mdd']
                    f.write(f"| {label} | {hd} | {int(pp*100)}% | {r['cagr']:.2f} | "
                            f"{r['sharpe']:.3f} | {r['mdd']:.2f} | {r['hit_rate']:.1f} | "
                            f"{dc:+.2f} | {ds:+.3f} | {dm:+.2f} |\n")

        # ---- OOS by-year table ----
        f.write('\n## OOS Sharpe by Year (Dual side)\n\n')
        f.write('| Cell | OOS_2023 | OOS_2024 | OOS_2025 | BEAR_2022 |\n')
        f.write('|---|---|---|---|---|\n')
        oos_2023 = dual[dual['period'] == 'OOS_2023'].set_index('cell')
        oos_2024 = dual[dual['period'] == 'OOS_2024'].set_index('cell')
        oos_2025 = dual[dual['period'] == 'OOS_2025'].set_index('cell')
        bear_2022 = dual[dual['period'] == 'BEAR_2022'].set_index('cell')

        labels_order = ['baseline_no_ts'] + [
            cell_label(hd, pp) for hd in HOLD_DAYS_GRID for pp in PROGRESS_PCT_GRID
        ]
        for label in labels_order:
            row = []
            for tbl in [oos_2023, oos_2024, oos_2025, bear_2022]:
                if label in tbl.index:
                    row.append(f"{tbl.loc[label, 'sharpe']:.3f}")
                else:
                    row.append('-')
            f.write(f"| {label} | {row[0]} | {row[1]} | {row[2]} | {row[3]} |\n")

        # ---- Bear 2022 MDD comparison ----
        f.write('\n## 2022 Bear MDD vs Baseline (Dual side)\n\n')
        f.write('Smaller |MDD| = better. ΔMDD positive = improvement.\n\n')
        f.write('| Cell | CAGR_2022 % | MDD_2022 % | ΔCAGR | ΔMDD |\n')
        f.write('|---|---|---|---|---|\n')
        if 'baseline_no_ts' in bear_2022.index:
            b22 = bear_2022.loc['baseline_no_ts']
            f.write(f"| baseline | {b22['cagr']:.2f} | {b22['mdd']:.2f} | - | - |\n")
            for label in labels_order:
                if label == 'baseline_no_ts' or label not in bear_2022.index:
                    continue
                r = bear_2022.loc[label]
                f.write(f"| {label} | {r['cagr']:.2f} | {r['mdd']:.2f} | "
                        f"{r['cagr'] - b22['cagr']:+.2f} | {r['mdd'] - b22['mdd']:+.2f} |\n")

        # ---- Time Stop trigger frequency (how aggressive each cell is) ----
        f.write('\n## Time Stop Exit Frequency (FULL_2020_2025, QM side)\n\n')
        f.write('Higher exits = more aggressive trim. Compare against rebal_swap_exits.\n\n')
        full_qm = df[(df['period'] == 'FULL_2020_2025') & (df['side'] == 'qm')].set_index('cell')
        f.write('| Cell | TS exits | Rebal swap exits | Avg hold days | n_closed |\n')
        f.write('|---|---|---|---|---|\n')
        for label in labels_order:
            if label not in full_qm.index:
                continue
            r = full_qm.loc[label]
            ts_x = int(r.get('time_stop_exits', 0)) if pd.notna(r.get('time_stop_exits', np.nan)) else 0
            rb_x = int(r.get('rebal_swap_exits', 0)) if pd.notna(r.get('rebal_swap_exits', np.nan)) else 0
            avg = r.get('avg_hold_days', np.nan)
            nc = r.get('n_closed', np.nan)
            avg_str = f"{avg:.1f}" if pd.notna(avg) else '-'
            nc_str = f"{int(nc)}" if pd.notna(nc) else '-'
            f.write(f"| {label} | {ts_x} | {rb_x} | {avg_str} | {nc_str} |\n")

        # ---- LOO for best cell ----
        if best_label and loo:
            f.write(f'\n## Leave-One-Out (Best cell: {best_label})\n\n')
            f.write(f'Best cell hold_days={best_hd}, progress_pct={best_pp:.2f}\n\n')
            f.write('Drop each year, compute Sharpe on remaining 5 years. ')
            f.write('If single year dominates -> Sharpe drops a lot when dropped.\n\n')
            f.write('| Drop year | Best cell Sharpe (5y) | Baseline Sharpe (5y) | Δ |\n')
            f.write('|---|---|---|---|\n')
            for y, m in sorted(loo.items()):
                if not m:
                    f.write(f"| {y} | - | - | - |\n")
                    continue
                bm = loo_base.get(y, {})
                bs = bm.get('sharpe', np.nan) if bm else np.nan
                cs = m.get('sharpe', np.nan)
                if pd.notna(bs) and pd.notna(cs):
                    f.write(f"| {y} | {cs:.3f} | {bs:.3f} | {cs - bs:+.3f} |\n")
                else:
                    f.write(f"| {y} | {cs} | {bs} | - |\n")

        # ---- Verdict ----
        f.write('\n## Verdict\n\n')
        f.write('### Grading Rubric\n')
        f.write('- **A**: ΔCAGR > +1pp AND ΔSharpe > +0.1 AND ΔMDD <= 0 (FULL)\n')
        f.write('- **B**: ΔCAGR > +0.5pp BUT ΔMDD worsens -> shadow run\n')
        f.write('- **D 平原**: 全部 cell |ΔCAGR| < 0.5pp -> noise, keep PRE_POLICY\n')
        f.write('- **D 反向**: Time Stop 越嚴績效越糟\n\n')

        # Determine verdict
        if 'baseline_no_ts' in full_dual.index:
            b = full_dual.loc['baseline_no_ts']
            grid_cells = full_dual[full_dual.index != 'baseline_no_ts'].copy()
            if not grid_cells.empty:
                grid_cells['dCAGR'] = grid_cells['cagr'] - b['cagr']
                grid_cells['dSharpe'] = grid_cells['sharpe'] - b['sharpe']
                grid_cells['dMDD'] = grid_cells['mdd'] - b['mdd']

                a_cells = grid_cells[(grid_cells['dCAGR'] > 1.0)
                                     & (grid_cells['dSharpe'] > 0.1)
                                     & (grid_cells['dMDD'] >= 0)]
                b_cells = grid_cells[(grid_cells['dCAGR'] > 0.5)
                                     & (grid_cells['dMDD'] < 0)]

                max_dcagr = grid_cells['dCAGR'].abs().max()
                avg_dcagr = grid_cells['dCAGR'].mean()
                avg_dsharpe = grid_cells['dSharpe'].mean()

                f.write(f'### Findings\n')
                f.write(f'- **Best cell ΔCAGR**: {grid_cells["dCAGR"].max():+.2f}pp ({grid_cells.loc[grid_cells["dCAGR"].idxmax()].name})\n')
                f.write(f'- **Best cell ΔSharpe**: {grid_cells["dSharpe"].max():+.3f} ({grid_cells.loc[grid_cells["dSharpe"].idxmax()].name})\n')
                f.write(f'- **Worst cell ΔCAGR**: {grid_cells["dCAGR"].min():+.2f}pp ({grid_cells.loc[grid_cells["dCAGR"].idxmin()].name})\n')
                f.write(f'- **Avg ΔCAGR across grid**: {avg_dcagr:+.2f}pp\n')
                f.write(f'- **Avg ΔSharpe across grid**: {avg_dsharpe:+.3f}\n')
                f.write(f'- **Max |ΔCAGR|**: {max_dcagr:.2f}pp\n')
                f.write(f'- **A-grade cells**: {len(a_cells)}\n')
                f.write(f'- **B-grade (trade-off) cells**: {len(b_cells)}\n\n')

                # Compute OOS sign tally for each grid cell
                oos_sign_tally = {}
                for cell_name in grid_cells.index:
                    wins = 0
                    n = 0
                    for p in ['OOS_2023', 'OOS_2024', 'OOS_2025']:
                        bdf = dual[(dual['period'] == p) & (dual['cell'] == 'baseline_no_ts')]
                        cdf = dual[(dual['period'] == p) & (dual['cell'] == cell_name)]
                        if not bdf.empty and not cdf.empty:
                            n += 1
                            if cdf['sharpe'].iloc[0] > bdf['sharpe'].iloc[0]:
                                wins += 1
                    oos_sign_tally[cell_name] = (wins, n)

                if len(a_cells) > 0:
                    f.write('### Grade: **A** -- Time Stop 落地\n\n')
                    f.write('A-grade cells:\n\n')
                    f.write('| Cell | ΔCAGR | ΔSharpe | ΔMDD | OOS sign-stab |\n')
                    f.write('|---|---|---|---|---|\n')
                    for idx, r in a_cells.iterrows():
                        ow, on = oos_sign_tally.get(idx, (0, 0))
                        f.write(f"| {idx} | {r['dCAGR']:+.2f} | {r['dSharpe']:+.3f} | "
                                f"{r['dMDD']:+.2f} | {ow}/{on} |\n")
                elif len(b_cells) > 0:
                    f.write('### Grade: **B** -- Trade-off, OOS sign 不穩\n\n')
                    f.write('B-grade cells (CAGR up but MDD worse OR Sharpe gap < +0.1):\n\n')
                    f.write('| Cell | ΔCAGR | ΔSharpe | ΔMDD | OOS sign-stab |\n')
                    f.write('|---|---|---|---|---|\n')
                    for idx, r in b_cells.iterrows():
                        ow, on = oos_sign_tally.get(idx, (0, 0))
                        f.write(f"| {idx} | {r['dCAGR']:+.2f} | {r['dSharpe']:+.3f} | "
                                f"{r['dMDD']:+.2f} | {ow}/{on} |\n")

                    # Check if any B-cell is OOS-stable (>=2/3)
                    stable_b = [k for k, (w, n) in oos_sign_tally.items()
                                if k in b_cells.index and n > 0 and w / n >= 2/3]
                    if not stable_b:
                        f.write('\n**注意**: 所有 B-grade cells **OOS sign-stab 都 < 2/3**,\n')
                        f.write('FULL CAGR 改善主要靠 IS_2020_2022 拉 -> 不應直接落地, 應視為 D 平原.\n')
                elif max_dcagr < 0.5:
                    f.write('### Grade: **D 平原** -- 全部 cell 在 noise 內, 維持 PRE_POLICY\n\n')
                elif avg_dcagr < 0:
                    f.write('### Grade: **D 反向** -- Time Stop 系統性傷 alpha\n\n')
                else:
                    f.write('### Grade: **C** -- 部分 cell 改善但不顯著\n\n')

                # Final recommendation
                f.write('\n### Recommendation\n\n')
                if len(a_cells) > 0:
                    f.write('Time Stop 應落地 (見 A-grade cells)\n')
                else:
                    # No A cells; check if best B cell is OOS-stable
                    best_b_stable = [k for k, (w, n) in oos_sign_tally.items()
                                     if k in b_cells.index and n > 0 and w / n >= 2/3]
                    if best_b_stable:
                        f.write(f'B-grade cells with OOS-stable sign exist: {best_b_stable}\n')
                        f.write('-> shadow run 6 mo, 看實際與 simulator 一致再決定\n')
                    else:
                        f.write('**不上線**: 沒有 A 級 cell, 所有 B 級 cell OOS sign 都不穩 (< 2/3),\n')
                        f.write('FULL ΔCAGR 改善是 IS-driven, 在 OOS 期間反向居多.\n')
                        f.write('-> 維持 PRE_POLICY, 不加 Time Stop\n')

        # ---- Caveats ----
        f.write('\n## Caveats\n\n')
        f.write('1. Time Stop 觸發判斷使用當週 fwd_5d 後的 cum_ret -> 等同 close-of-week 觸發, 無 look-ahead\n')
        f.write('2. Time Stop 觸發後 30d ban (跟 Whipsaw 同 mechanism) 避免立即重買\n')
        f.write('3. MIN_HOLD=20 為 hard floor, Time Stop 不會在 days_held<20 觸發\n')
        f.write('4. 不模擬交易成本 (台股 ~0.3% round-trip), Time Stop 多換手會有額外 drag\n')
        f.write('5. Baseline = PRE_POLICY (b18758d revert 後設定), 不是 post-policy baseline\n')
        f.write('6. Universe 限於 trade_journal_value snapshot (Stage 1 後 857 檔) + QM panel\n')
        f.write('7. cum_ret 計算用 fwd_5d 連乘, 不含 TP partial exit 後的真實組合報酬 (TP 機制與 baseline 一致)\n')


if __name__ == '__main__':
    main()
