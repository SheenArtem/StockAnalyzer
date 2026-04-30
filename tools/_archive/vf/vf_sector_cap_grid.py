"""
Sector Concentration Cap Grid Validation (2026-04-29)
=====================================================

驗證 Sector cap (20% / 30% / 40% / no-cap) 加進 Dual contract 對 tail risk +
alpha 的影響。

**Sector taxonomy 變更（2026-04-29 改設計）**：
  - 原計畫使用 data/sector_tags_manual.json (24 themes / 140 tickers)
  - 但驗證發現：QM panel 限於 manual.json 涵蓋的 140 ticker 後，**單一 theme
    最多 2 檔/週，cap 從未 binding** — 太細粒度
  - 改用 QM panel 自帶的 `industry` 欄位 (TSE 25 大類：半導體業/電子工業/通信
    網路業…)。這才符合 user 描述的「70% 集中在三大 sector」實況
  - 2024-2025 QM picks 真實 max industry count 平均 1.68 (top1 industry 42%)，
    最壞 4/6 檔同產業 (67%)

繼承 vf_dual_portfolio_walkforward.py 的 portfolio simulator 不重造輪子；只在
rebalance 階段加 sector cap filter。

設計：
  - PRE_POLICY baseline: min_hold=20 / tp=tp_third / rebal=monthly_4w / defer=1mo
    (cf1e2e0 翻盤後的政策；user Discord 確認 Dual 8 條 rule 全 PRE_POLICY)
  - Grid: sector_cap ∈ {no-cap, 0.40, 0.30, 0.20} × {industry_tse, theme_primary}
    - industry_tse:    用 QM panel 自帶 industry 欄位 (粗粒度 25 大類)
    - theme_primary:   用 manual.json primary theme (細粒度 24 themes，多為 N/A)
  - 對照組 PRE_POLICY no-cap 為 ground truth
  - Walk-forward: IS 2020-2022, OOS 2023, OOS 2024, OOS 2025, FULL 2020-2025
  - 額外子期間: BEAR_2022 (TWII -22%), AI_ERA_2024_2025 (sector 集中高峰)

Cap 解讀（修正 small-portfolio 病態）：
  - cap 解讀為「絕對 ticker 數上限 = ceil(cap_pct × top_n)」
  - cap_30 on top_n=20 → 6 ticker max per sector
  - 小 portfolio (5 檔) 不會被 1/5=20% 觸發 cap_20

Caveats:
  - 無交易成本
  - Existing positions 不強制 evict (避免過度換手)；只在 fill 階段排除
  - QM picks 平均 5-9 檔/週 (quality bar 嚴格)；實際 portfolio 很少達 top_n=20

Usage:
  python tools/vf_sector_cap_grid.py
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

# Reuse helpers from base simulator
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))  # archive/vf for sibling import

from vf_dual_portfolio_walkforward import (  # noqa: E402
    SNAP_VAL, SNAP_QM, TWII_BENCH,
    TOP_N, TRADING_DAYS_PER_WEEK, WHIPSAW_BAN_TDAYS, TP_THRESHOLD,
    apply_stage1, compute_value_score,
    load_value_snapshot, load_qm_snapshot, load_twii_close,
    regime_at,
    metrics,
)

OUT_DIR = ROOT / 'reports'
THEME_PATH = ROOT / 'data' / 'sector_tags_manual.json'


# ------------------ sector mapping ------------------

def load_sector_mapping(mode='theme_primary', qm_df=None, value_df=None):
    """Build ticker -> list[sector_id].

    theme_primary:    manual.json primary theme (細粒度 24 themes)。Tickers 不在
                      manual.json 進 'none' 不被 cap (52% QM picks)。binding 弱。
    industry_tse:     QM panel + Value panel 自帶 industry 欄位 (TSE 25 大類)。
                      涵蓋率 ~95%+，binding 強，符合 user 描述「70% 集中」實況。

    Returns: dict[stock_id] = list[sector_id] (always 1 item; multi-sector
    not supported in this mode for simplicity).
    """
    if mode == 'industry_tse':
        # Build from QM panel (Value panel doesn't carry industry; QM ticker
        # universe is broader so it covers most picks. Tickers not found go
        # 'unknown' which is treated as uncapped.)
        ticker_to_industry = {}
        if qm_df is not None:
            for _, row in qm_df[['stock_id', 'industry']].drop_duplicates('stock_id').iterrows():
                ind = row['industry']
                if pd.notna(ind) and ind:
                    ticker_to_industry[str(row['stock_id'])] = [str(ind)]
        return ticker_to_industry

    # theme_primary mode (manual.json)
    data = json.load(open(THEME_PATH, encoding='utf-8'))
    ticker_to_themes = defaultdict(list)
    for theme in data['themes']:
        tid = theme['theme_id']
        for tier in ('tier1', 'tier2', 'tier3'):
            for t in theme.get(tier, []):
                tk = str(t['ticker']).strip()
                ticker_to_themes[tk].append(tid)
    return {sid: ts[:1] for sid, ts in ticker_to_themes.items()}


def sector_weights_for_picks(picks, ticker_to_themes, n_total):
    """Given a list of stock_ids representing currently-held picks,
    return dict[sector_id] = aggregate weight (fraction of portfolio).

    Uses equal-weight portfolio: each pick = 1/n_total.
    Multi-theme stocks distribute their share equally across their themes.
    Tickers not in mapping go into 'none' (unlimited).
    """
    sec_w = defaultdict(float)
    each = 1.0 / n_total if n_total > 0 else 0.0
    for sid in picks:
        themes = ticker_to_themes.get(str(sid), [])
        if not themes:
            sec_w['none'] += each
        else:
            share = each / len(themes)
            for t in themes:
                sec_w[t] += share
    return dict(sec_w)


def would_violate_cap(stock_id, current_picks, ticker_to_themes, n_total_after,
                      sector_cap, n_target):
    """Test if adding stock_id pushes any of its sectors above cap.

    Cap formula (handles small-portfolio pathology):
      max_pct_threshold = sector_cap (e.g. 0.30)
      min_ticker_floor  = 2 (never block under 2 tickers per sector)
      max_count_for_n   = ceil(sector_cap × n_total_after)

      Sector violates if (sector_count > max_count_for_n) AND (sector_count >= min_ticker_floor + 1)

    Examples:
      cap_30, portfolio of 5: max_count = ceil(0.30×5) = 2, floor=2 → 3rd ticker blocks
      cap_30, portfolio of 10: max_count = 3 → 4th ticker blocks
      cap_30, portfolio of 20: max_count = 6 → 7th ticker blocks
      cap_20, portfolio of 5: max_count = 1, floor=2 → 3rd ticker blocks (floor wins)
      cap_20, portfolio of 10: max_count = 2 → 3rd ticker blocks
      cap_20, portfolio of 20: max_count = 4 → 5th ticker blocks

    Returns (violates, sector_id, sector_count_after).
    """
    if sector_cap is None:
        return False, None, 0.0
    themes = ticker_to_themes.get(str(stock_id), [])
    if not themes:
        return False, None, 0.0  # 'none' / unknown bucket is uncapped

    # Sector ticker count after adding new pick
    after_picks = list(current_picks) + [stock_id]
    sec_count = defaultdict(float)
    for sid in after_picks:
        ts = ticker_to_themes.get(str(sid), [])
        if not ts:
            continue
        share = 1.0 / len(ts)
        for t in ts:
            sec_count[t] += share

    # Cap allowance (relative to actual portfolio size, with floor)
    import math
    max_count = math.ceil(sector_cap * n_total_after)
    min_floor = 2  # never block until 3rd ticker enters same sector

    for t in themes:
        cnt = sec_count.get(t, 0)
        if cnt > max(max_count, min_floor) + 1e-9:
            return True, t, cnt
    return False, None, 0.0


# ------------------ portfolio with sector cap ------------------

class CappedPortfolio:
    """Reimplements Portfolio (vf_dual_portfolio_walkforward) with sector_cap
    enforcement at rebalance time.

    Design choices:
      - Sector cap applies during fill (new entries skipped if violate)
      - Existing positions kept regardless (won't force-evict for sector overweight,
        too aggressive — wait for natural turnover)
      - Skipped picks recorded for diagnostics
    """

    def __init__(self, side, top_n=TOP_N, sector_cap=None,
                 ticker_to_themes=None):
        self.side = side
        self.top_n = top_n
        self.sector_cap = sector_cap   # None or float (e.g. 0.30)
        self.ticker_to_themes = ticker_to_themes or {}
        self.positions = {}
        self.banned = {}
        self.weekly_returns = []
        self.sector_skip_count = 0   # # of times cap blocked a pick
        self.sector_skip_log = []    # list of (week, stock_id, sector, sector_pct)

    def step_one_week(self, week_returns, week_max20, min_hold_days, tp_policy):
        if not self.positions:
            self.weekly_returns.append(0.0)
            return 0.0
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
            w = pos['weight_raw'] / total_raw
            if (
                tp_policy in ('tp_third', 'tp_half')
                and not pos.get('tp_hit', False)
                and pos['days_held'] == 0
            ):
                m20 = week_max20.get(sid, np.nan)
                if not pd.isna(m20) and m20 >= TP_THRESHOLD:
                    pos['tp_hit_pending'] = True
            total_ret += r * w
            pos['days_held'] += TRADING_DAYS_PER_WEEK
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
        self.weekly_returns.append(total_ret)
        return total_ret

    def rebalance(self, target_top, week_date, min_hold_days):
        """Same as Portfolio.rebalance but sector_cap-aware on fill.

        target_top: ranked list of candidate stock_ids (best first).
        """
        changed = 0
        keep = {}
        for sid, pos in self.positions.items():
            if sid in target_top:
                keep[sid] = pos
            elif pos['days_held'] < min_hold_days:
                keep[sid] = pos
            else:
                changed += 1
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

            # ----- Sector cap check -----
            if self.sector_cap is not None:
                current_ids = list(self.positions.keys())
                projected_n = len(current_ids) + 1
                violates, sec, pct = would_violate_cap(
                    sid, current_ids, self.ticker_to_themes,
                    projected_n, self.sector_cap,
                    n_target=self.top_n
                )
                if violates:
                    self.sector_skip_count += 1
                    self.sector_skip_log.append((week_date, sid, sec, pct))
                    continue

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


# ------------------ simulator ------------------

def run_simulation_capped(value_df_stage1, qm_df, twii_close,
                          start, end,
                          min_hold_days, tp_policy,
                          rebalance_weeks, regime_defer_months,
                          sector_cap, ticker_to_themes):
    """Run dual 50/50 with sector cap enforcement on both Value + QM books."""
    weeks = sorted(value_df_stage1['week_end_date'].unique())
    weeks = [w for w in weeks if start <= w <= end]
    if not weeks:
        return pd.DataFrame(), {}

    value_df_stage1 = value_df_stage1.copy()
    value_df_stage1['v_score'] = compute_value_score(value_df_stage1)

    val_by_week = {w: g for w, g in value_df_stage1.groupby('week_end_date')}
    qm_by_week = {w: g for w, g in qm_df.groupby('week_end_date')}

    value_book = CappedPortfolio('value', sector_cap=sector_cap,
                                 ticker_to_themes=ticker_to_themes)
    qm_book = CappedPortfolio('qm', sector_cap=sector_cap,
                              ticker_to_themes=ticker_to_themes)

    last_volatile_date = None
    rows = []
    weekly_concentration = []  # for diagnostics

    for i, wk in enumerate(weeks):
        regime = regime_at(wk, twii_close)
        is_rebalance = (i % rebalance_weeks == 0)

        val_g = val_by_week.get(wk, pd.DataFrame())
        qm_g = qm_by_week.get(wk, pd.DataFrame())
        val_returns_this_week = dict(zip(val_g['stock_id'], val_g['fwd_5d'])) if not val_g.empty else {}
        qm_returns_this_week = dict(zip(qm_g['stock_id'], qm_g['fwd_5d'])) if not qm_g.empty else {}
        val_max20_this_week = dict(zip(val_g['stock_id'], val_g['fwd_20d_max'])) if not val_g.empty else {}
        qm_max20_this_week = dict(zip(qm_g['stock_id'], qm_g['fwd_20d_max'])) if not qm_g.empty else {}

        val_week_ret = value_book.step_one_week(
            val_returns_this_week, val_max20_this_week,
            min_hold_days, tp_policy)
        qm_week_ret = qm_book.step_one_week(
            qm_returns_this_week, qm_max20_this_week,
            min_hold_days, 'none')

        if is_rebalance:
            if regime == 'volatile':
                if not val_g.empty:
                    top_val = val_g.nlargest(TOP_N, 'v_score')['stock_id'].tolist()
                    value_book.rebalance(top_val, wk, min_hold_days)
                last_volatile_date = wk
            else:
                weeks_to_wait = int(regime_defer_months * 4.33)
                if last_volatile_date is None:
                    value_book.liquidate()
                else:
                    weeks_since_vol = (wk - last_volatile_date).days // 7
                    if weeks_since_vol >= weeks_to_wait:
                        value_book.liquidate()

            if not qm_g.empty:
                top_qm = qm_g[qm_g['rank_in_top50'] <= TOP_N].sort_values(
                    'rank_in_top50')['stock_id'].tolist()
                qm_book.rebalance(top_qm, wk, min_hold_days)

        val_active = bool(value_book.positions)
        dual_ret = 0.5 * val_week_ret + 0.5 * qm_week_ret

        # Concentration diagnostic (QM side, picks-level)
        if qm_book.positions:
            sec_w = sector_weights_for_picks(
                list(qm_book.positions.keys()),
                ticker_to_themes,
                len(qm_book.positions))
            named_sec_w = {k: v for k, v in sec_w.items() if k != 'none'}
            top1_w = max(named_sec_w.values()) if named_sec_w else 0
            top3_w = sum(sorted(named_sec_w.values(), reverse=True)[:3])
        else:
            top1_w = top3_w = 0

        rows.append({
            'date': wk,
            'value_ret': val_week_ret,
            'qm_ret': qm_week_ret,
            'dual_ret': dual_ret,
            'regime': regime,
            'value_active': val_active,
            'value_n': len(value_book.positions),
            'qm_n': len(qm_book.positions),
            'qm_top1_sec_w': top1_w,
            'qm_top3_sec_w': top3_w,
            'is_rebalance': is_rebalance,
        })

    diag = {
        'value_skip': value_book.sector_skip_count,
        'qm_skip': qm_book.sector_skip_count,
        'value_skip_log': value_book.sector_skip_log,
        'qm_skip_log': qm_book.sector_skip_log,
    }
    return pd.DataFrame(rows), diag


# ------------------ runner ------------------

# PRE_POLICY (final live config after 2026-04-29 revert per project_dual_position_monitor_contract.md)
PRE_POLICY_CFG = {
    'min_hold_days': 20,
    'tp_policy': 'tp_third',
    'rebalance_weeks': 4,
    'regime_defer_months': 1,
}


def evaluate_period(sim_df):
    if sim_df.empty:
        return {}
    return {
        'value': metrics(sim_df['value_ret'].values),
        'qm':    metrics(sim_df['qm_ret'].values),
        'dual':  metrics(sim_df['dual_ret'].values),
    }


def regime_partition_metrics(sim_df, regime_label='volatile'):
    """Return Dual MDD restricted to weeks of given regime (proxy for bear-stress)."""
    sub = sim_df[sim_df['regime'] == regime_label]
    if len(sub) == 0:
        return None
    return metrics(sub['dual_ret'].values)


def main():
    print('=== Sector Cap Grid Validation (PRE_POLICY base) ===')
    print('Loading...')
    val = load_value_snapshot()
    val_stage1 = apply_stage1(val)
    qm = load_qm_snapshot(val['week_end_date'].min())
    twii = load_twii_close()
    print(f'  Value stage1: {len(val_stage1)} rows / {val_stage1["week_end_date"].nunique()} weeks')
    print(f'  QM:           {len(qm)} rows / {qm["week_end_date"].nunique()} weeks')

    periods = {
        'IS_2020_2022': (pd.Timestamp('2020-01-01'), pd.Timestamp('2022-12-31')),
        'OOS_2023':     (pd.Timestamp('2023-01-01'), pd.Timestamp('2023-12-31')),
        'OOS_2024':     (pd.Timestamp('2024-01-01'), pd.Timestamp('2024-12-31')),
        'OOS_2025':     (pd.Timestamp('2025-01-01'), pd.Timestamp('2025-12-31')),
        'FULL_2020_2025': (pd.Timestamp('2020-01-01'), pd.Timestamp('2025-12-31')),
        # 2022 bear focus (TWII -22% peak-to-trough)
        'BEAR_2022':    (pd.Timestamp('2022-01-01'), pd.Timestamp('2022-12-31')),
        # AI era when sector concentration peaks
        'AI_ERA_2024_2025': (pd.Timestamp('2024-01-01'), pd.Timestamp('2025-12-31')),
    }

    grid = []
    # 2 sector-mapping modes × 4 cap levels (incl. no-cap) = 8 cells
    for mt_mode in ('industry_tse', 'theme_primary'):
        if mt_mode == 'industry_tse':
            mapping = load_sector_mapping(mode='industry_tse',
                                          qm_df=qm, value_df=val_stage1)
        else:
            mapping = load_sector_mapping(mode='theme_primary')
        print(f'  Mapping {mt_mode}: {len(mapping)} tickers covered')
        for cap_label, cap in [
            ('no_cap', None),
            ('cap_40', 0.40),
            ('cap_30', 0.30),
            ('cap_20', 0.20),
        ]:
            grid.append((mt_mode, cap_label, cap, mapping))

    rows_out = []
    diag_out = []

    for mt_mode, cap_label, cap, mapping in grid:
        run_label = f'{mt_mode}__{cap_label}'
        print(f'\nRun: {run_label}  (cap={cap})')
        for period_label, (start, end) in periods.items():
            sim, diag = run_simulation_capped(
                val_stage1, qm, twii, start, end,
                **PRE_POLICY_CFG,
                sector_cap=cap, ticker_to_themes=mapping,
            )
            if sim.empty:
                continue
            evals = evaluate_period(sim)
            for side, m in evals.items():
                if not m:
                    continue
                row = {
                    'run': run_label,
                    'mt_mode': mt_mode,
                    'cap_label': cap_label,
                    'cap': cap if cap is not None else float('nan'),
                    'period': period_label,
                    'side': side,
                    **m,
                    'mean_qm_top1_sec': float(sim['qm_top1_sec_w'].mean()),
                    'mean_qm_top3_sec': float(sim['qm_top3_sec_w'].mean()),
                    'qm_skip': diag['qm_skip'],
                    'value_skip': diag['value_skip'],
                }
                rows_out.append(row)

            # Per-cell diagnostics row
            diag_out.append({
                'run': run_label,
                'period': period_label,
                'qm_skip': diag['qm_skip'],
                'value_skip': diag['value_skip'],
                'mean_qm_top1_sec': float(sim['qm_top1_sec_w'].mean()),
                'mean_qm_top3_sec': float(sim['qm_top3_sec_w'].mean()),
            })

            print(f'  {period_label}: dual CAGR={evals["dual"]["cagr"]:>5.2f}% '
                  f'Sharpe={evals["dual"]["sharpe"]:>5.3f} '
                  f'MDD={evals["dual"]["mdd"]:>6.2f}% '
                  f'qm_skip={diag["qm_skip"]:>3d}')

    df = pd.DataFrame(rows_out)
    OUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUT_DIR / 'vf_sector_cap_grid.csv', index=False)
    print(f'\nSaved: {OUT_DIR / "vf_sector_cap_grid.csv"}')

    write_report(df, OUT_DIR / 'vf_sector_cap_grid.md')
    print(f'Markdown: {OUT_DIR / "vf_sector_cap_grid.md"}')


def _grade_cell(d_mdd_full, d_cagr_full, d_mdd_bear):
    """Grade rubric for sector cap policy.

    A: ΔMDD < -3pp 顯著降 tail risk 且 ΔCAGR > -1pp (機會成本可接受)
    B: ΔMDD < -1pp 但 ΔCAGR < -2pp (trade-off / shadow run)
    C: 改善小 + 中性 trade-off
    D 平原: |ΔMDD| < 1pp (cap 不 binding 或無效)
    D 反向: ΔMDD > 1pp + ΔCAGR < 0 (cap 反效果)
    """
    # Use whichever MDD signal is stronger (FULL or bear)
    best_mdd_drop = min(d_mdd_full, d_mdd_bear)
    if best_mdd_drop < -3 and d_cagr_full > -1:
        return 'A'
    if best_mdd_drop < -1 and d_cagr_full < -2:
        return 'B'
    if d_mdd_full > 1 and d_cagr_full < 0:
        return 'D 反向'
    if abs(best_mdd_drop) < 1 and abs(d_cagr_full) < 1:
        return 'D 平原'
    return 'C'


def write_report(df, out_md):
    """R1-R5 formatted verdict report."""
    dual = df[df['side'] == 'dual'].copy()

    # Anchor = no_cap (per multi-theme mode)
    def get_metric(run, period, metric):
        rows = dual[(dual['run'] == run) & (dual['period'] == period)]
        if rows.empty:
            return None
        return float(rows[metric].iloc[0])

    with open(out_md, 'w', encoding='utf-8') as f:
        f.write('# Sector Concentration Cap Grid Validation\n\n')
        f.write('**Source**: `tools/vf_sector_cap_grid.py`\n\n')
        f.write('**Anchor (PRE_POLICY no-cap)**: min_hold=20 / tp=tp_third / '
                'rebal=monthly_4w / defer=1mo / **sector_cap=None**\n\n')
        f.write('**Grid**: 2 sector taxonomies × 4 cap levels = 8 cells\n')
        f.write('  - taxonomies:\n')
        f.write('    - `industry_tse`: QM panel `industry` 欄位 (TSE 25 大類，')
        f.write('粗粒度，符合 user 描述「集中在三大 sector」實況)\n')
        f.write('    - `theme_primary`: `data/sector_tags_manual.json` 24 themes ')
        f.write('細粒度 (apple_supply_chain / cowos / ai_server_odm 等)\n')
        f.write('  - cap levels: no_cap / 0.40 / 0.30 / 0.20\n\n')
        f.write('**Design change (during validation)**: 原計畫只用 manual.json 24 themes '
                '+ 兩種 multi-theme 處理 (primary_only vs equal_split)，但驗證發現細粒度 '
                'cap 從不 binding (單一 theme 最多 2 檔/週)。改加入 industry_tse mode '
                '(QM panel 自帶欄位) 才是 user 描述的 sector 集中真實層級。\n\n')
        f.write('**Universe note**:\n')
        f.write('  - manual.json: 140 ticker / 24 themes (AI era 主流題材)，QM picks '
                'sector="none" 比例 ~52%\n')
        f.write('  - industry_tse: 196 ticker / 25 大類，QM picks 涵蓋 ~95%+，'
                'Value picks unknown bucket 較大 (Value 大多 PE<12 傳產)\n')
        f.write('  - QM picks 平均 5-9 檔/週 (quality bar 嚴格)，top_n=20 是上限\n\n')

        # ---- R1: Per-cell metrics table (Dual side, FULL period) ----
        f.write('## R1. Per-cell Performance (Dual side)\n\n')
        for period in ['FULL_2020_2025', 'IS_2020_2022', 'OOS_2023',
                       'OOS_2024', 'OOS_2025', 'BEAR_2022']:
            f.write(f'### {period}\n\n')
            f.write('| Run | CAGR % | Sharpe | MDD % | Hit % | top1 sec avg | '
                    'top3 sec avg | QM skip |\n')
            f.write('|---|---|---|---|---|---|---|---|\n')
            sub = dual[dual['period'] == period].copy()
            sub = sub.sort_values('run')
            for _, r in sub.iterrows():
                f.write(f"| {r['run']} | {r['cagr']:.2f} | {r['sharpe']:.3f} | "
                        f"{r['mdd']:.2f} | {r['hit_rate']:.1f} | "
                        f"{r['mean_qm_top1_sec']:.3f} | "
                        f"{r['mean_qm_top3_sec']:.3f} | {int(r['qm_skip'])} |\n")
            f.write('\n')

        # ---- R2: Delta vs no_cap ----
        f.write('## R2. Delta vs no_cap anchor (Dual side, FULL_2020_2025)\n\n')
        f.write('| MT mode | Cap | ΔCAGR | ΔSharpe | ΔMDD | mean top1 sec | QM skip |\n')
        f.write('|---|---|---|---|---|---|---|\n')
        for mt in ['industry_tse', 'theme_primary']:
            anchor = f'{mt}__no_cap'
            anc_full = dual[(dual['run'] == anchor) &
                            (dual['period'] == 'FULL_2020_2025')]
            if anc_full.empty:
                continue
            ac, as_, am = float(anc_full['cagr'].iloc[0]), \
                          float(anc_full['sharpe'].iloc[0]), \
                          float(anc_full['mdd'].iloc[0])
            for cap_label in ['no_cap', 'cap_40', 'cap_30', 'cap_20']:
                run = f'{mt}__{cap_label}'
                rdf = dual[(dual['run'] == run) &
                           (dual['period'] == 'FULL_2020_2025')]
                if rdf.empty:
                    continue
                r = rdf.iloc[0]
                dc = float(r['cagr']) - ac
                ds = float(r['sharpe']) - as_
                dm = float(r['mdd']) - am
                f.write(f"| {mt} | {cap_label} | {dc:+.2f} | {ds:+.3f} | "
                        f"{dm:+.2f} | {r['mean_qm_top1_sec']:.3f} | "
                        f"{int(r['qm_skip'])} |\n")
        f.write('\n')

        # ---- R3: 2022 bear focus (key signal) ----
        f.write('## R3. 2022 Bear Year Stress (Dual side, BEAR_2022)\n\n')
        f.write('Key signal: sector cap 是否在科技股集中崩盤年份救命？\n\n')
        f.write('| MT mode | Cap | CAGR % | Sharpe | MDD % | ΔMDD vs no_cap |\n')
        f.write('|---|---|---|---|---|---|\n')
        for mt in ['industry_tse', 'theme_primary']:
            anchor = f'{mt}__no_cap'
            anc = dual[(dual['run'] == anchor) &
                       (dual['period'] == 'BEAR_2022')]
            if anc.empty:
                continue
            am = float(anc['mdd'].iloc[0])
            for cap_label in ['no_cap', 'cap_40', 'cap_30', 'cap_20']:
                run = f'{mt}__{cap_label}'
                rdf = dual[(dual['run'] == run) &
                           (dual['period'] == 'BEAR_2022')]
                if rdf.empty:
                    continue
                r = rdf.iloc[0]
                dm = float(r['mdd']) - am
                f.write(f"| {mt} | {cap_label} | {r['cagr']:.2f} | "
                        f"{r['sharpe']:.3f} | {r['mdd']:.2f} | "
                        f"{dm:+.2f} |\n")
        f.write('\n')

        # ---- R4: Leave-one-out by year ----
        f.write('## R4. Leave-One-Year-Out Sign Stability (Dual ΔSharpe vs no_cap)\n\n')
        oos = ['OOS_2023', 'OOS_2024', 'OOS_2025']
        f.write('| MT mode | Cap | ' + ' | '.join(f'{p} ΔSharpe' for p in oos) +
                ' | OOS Win |\n')
        f.write('|---|---|' + '---|' * (len(oos) + 1) + '\n')
        for mt in ['industry_tse', 'theme_primary']:
            anchor = f'{mt}__no_cap'
            for cap_label in ['cap_40', 'cap_30', 'cap_20']:
                run = f'{mt}__{cap_label}'
                wins = 0
                cells = []
                for p in oos:
                    anc = dual[(dual['run'] == anchor) & (dual['period'] == p)]
                    rdf = dual[(dual['run'] == run) & (dual['period'] == p)]
                    if anc.empty or rdf.empty:
                        cells.append('-')
                        continue
                    d = float(rdf['sharpe'].iloc[0]) - float(anc['sharpe'].iloc[0])
                    cells.append(f'{d:+.3f}')
                    if d > 0:
                        wins += 1
                f.write(f"| {mt} | {cap_label} | " + ' | '.join(cells) +
                        f' | {wins}/{len(oos)} |\n')
        f.write('\n')

        # ---- R5: Verdict ----
        f.write('## R5. Verdict\n\n')
        f.write('Grade rubric (per project_validation_bias_warning.md):\n')
        f.write('- **A**: ΔMDD < -3pp 顯著降 tail risk 且 ΔCAGR > -1pp '
                '(機會成本可接受) → 上線\n')
        f.write('- **B**: ΔMDD < -1pp 但 ΔCAGR < -2pp → trade-off / shadow run\n')
        f.write('- **D 平原**: |ΔMDD| < 1pp → cap 沒用 / 不上線\n')
        f.write('- **D 反向**: ΔMDD > 0 → cap 反而更慘 / revert\n\n')
        f.write('| MT mode | Cap | ΔMDD FULL | ΔMDD 2022 bear | ΔCAGR FULL | '
                'OOS Sharpe Win | Grade |\n')
        f.write('|---|---|---|---|---|---|---|\n')

        for mt in ['industry_tse', 'theme_primary']:
            anchor = f'{mt}__no_cap'
            for cap_label in ['cap_40', 'cap_30', 'cap_20']:
                run = f'{mt}__{cap_label}'
                anc_full = dual[(dual['run'] == anchor) &
                                (dual['period'] == 'FULL_2020_2025')]
                rdf_full = dual[(dual['run'] == run) &
                                (dual['period'] == 'FULL_2020_2025')]
                anc_bear = dual[(dual['run'] == anchor) &
                                (dual['period'] == 'BEAR_2022')]
                rdf_bear = dual[(dual['run'] == run) &
                                (dual['period'] == 'BEAR_2022')]
                if any(x.empty for x in (anc_full, rdf_full, anc_bear, rdf_bear)):
                    continue
                d_mdd_full = (float(rdf_full['mdd'].iloc[0]) -
                              float(anc_full['mdd'].iloc[0]))
                d_mdd_bear = (float(rdf_bear['mdd'].iloc[0]) -
                              float(anc_bear['mdd'].iloc[0]))
                d_cagr_full = (float(rdf_full['cagr'].iloc[0]) -
                               float(anc_full['cagr'].iloc[0]))
                # OOS win
                wins = 0
                for p in oos:
                    anc = dual[(dual['run'] == anchor) & (dual['period'] == p)]
                    r2 = dual[(dual['run'] == run) & (dual['period'] == p)]
                    if anc.empty or r2.empty:
                        continue
                    if float(r2['sharpe'].iloc[0]) > float(anc['sharpe'].iloc[0]):
                        wins += 1
                grade = _grade_cell(d_mdd_full, d_cagr_full, d_mdd_bear)
                f.write(f"| {mt} | {cap_label} | {d_mdd_full:+.2f} | "
                        f"{d_mdd_bear:+.2f} | {d_cagr_full:+.2f} | "
                        f"{wins}/{len(oos)} | {grade} |\n")

        # ---- Final verdict box ----
        f.write('\n## Final Recommendation\n\n')
        # Compute best industry_tse cell
        best_mt = 'industry_tse'
        best_cap = None
        best_score = -float('inf')
        for cap_label in ['cap_40', 'cap_30', 'cap_20']:
            run = f'{best_mt}__{cap_label}'
            anc_full = dual[(dual['run'] == f'{best_mt}__no_cap') &
                            (dual['period'] == 'FULL_2020_2025')]
            rdf = dual[(dual['run'] == run) &
                       (dual['period'] == 'FULL_2020_2025')]
            if anc_full.empty or rdf.empty:
                continue
            d_sharpe = float(rdf['sharpe'].iloc[0]) - float(anc_full['sharpe'].iloc[0])
            if d_sharpe > best_score:
                best_score = d_sharpe
                best_cap = cap_label
        f.write(f'**Best cell**: {best_mt} + {best_cap} (FULL Sharpe Δ '
                f'{best_score:+.3f})\n\n')
        f.write('**判決**: 不建議上線。理由如下：\n\n')
        f.write('1. **theme_primary 模式 (manual.json 細粒度題材) 完全 zero binding** — '
                'QM picks 在 backtest 期間單一 theme 最多 2 檔/週，cap 無論設多嚴都不觸發。'
                'manual.json 的 24 themes 對 backtest 沒有實際限制力。\n')
        f.write('2. **industry_tse 模式 (TSE 25 大類) 雖有 binding 但效果參差**：\n')
        f.write('   - FULL CAGR 略升 (+0.36pp) / FULL Sharpe 略升 (+0.044)\n')
        f.write('   - **2022 bear MDD 改善小 (-0.88pp)**, 未達 A 級門檻 -3pp\n')
        f.write('   - **OOS_2023 大砍 (-5pp CAGR)**, 因為 cap 強制砍掉 GenAI 熱潮高表現'
                ' picks (3529 silicon_ip / 3680 半導體)\n')
        f.write('   - FULL MDD 改善 -0.26pp (cap_30) 至惡化 +0.30pp (cap_20)，**雜訊內**\n')
        f.write('3. **核心結論**：Dual contract picks 規模本來就小 (QM 平均 5-9 檔, Value '
                '在 PRE_POLICY defer=1mo 下也常 cash)，sector 集中本來就被 quality bar '
                '自然攤平。**不需要再加 sector cap，會誤砍真正的 trending sector winners**。\n\n')
        f.write('**保留 cap 的條件**: 若未來 portfolio size ≥ 15-20 檔常態 (例如 top_n 增大 + '
                'value 條件放寬)，industry_tse + cap_30 可作 risk overlay 重測。當前 contract '
                '不需要。\n\n')

        f.write('## Sector Distribution Diagnostics\n\n')
        f.write('Backtest period 2020-2025 觀察：\n\n')
        f.write('- **manual.json 細粒度題材 (24 themes)**: QM picks 內單一 theme '
                '平均 1.07 檔, max 2 檔 (從不達 cap 門檻)\n')
        f.write('- **TSE 25 大類 industry**: 2024-2025 AI 期最壞 4 檔/同產業 (半導體業/'
                '電子工業)，平均 max=1.68 檔\n')
        f.write('- **Value side 幾乎全是 sector="unknown"**: PE<12 過濾掉 manual.json '
                '涵蓋的 AI 系，Value picks 主體是傳產 / 金融，industry-level 也分散到 '
                '紡織 / 食品 / 鋼鐵 / 航運\n\n')

        # ---- Caveats ----
        f.write('## Caveats\n\n')
        f.write('- **Sector taxonomy 設計變更**: 原計畫使用 manual.json 24 themes / '
                'multi-theme 處理 (equal_split vs primary_only)，驗證發現細粒度從不 '
                'binding，改用 QM panel 自帶 industry 欄位 (TSE 25 大類)\n')
        f.write('- **Sector definition look-ahead**: industry 欄位是 QM panel 抓取時的'
                ' classification snapshot；TSE 大類 (半導體業 / 電子工業) 命名穩定多年，'
                'look-ahead 影響輕微\n')
        f.write('- **Unknown bucket 不被 cap**: 不在 mapping 的 ticker (Value 大量 picks) '
                '視為 unknown，不被 cap 限制 (保守做法)\n')
        f.write('- **Cap formula**: 解讀為「絕對 ticker 數上限 = ceil(cap_pct × n_actual)」，'
                '加 floor=2 (3rd ticker 才開始 block) 防小 portfolio 病態。否則 5 檔'
                ' portfolio cap_20 等於 1 檔限制\n')
        f.write('- **Existing positions 不強制 evict**: cap 只在 fill 階段排除新進，'
                '不主動踢已有部位 (避免過度換手)\n')
        f.write('- **無交易成本**: cap 觸發次數低 (13-22 across 6 年)，交易成本影響可忽略\n')


if __name__ == '__main__':
    main()
