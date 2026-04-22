"""
VF-Value-ex2 EDGAR IC 驗證 — Piotroski 原版精神 (P/B filtered)

目的：
  先篩 low P/B (bottom 30% 或 20% by cross-section)，
  在 value universe 內驗 F-Score 是否有 alpha。
  對比前一輪「全市場 cross-section」的 D 級反向結論。

流程：
  Step 1: 從 EDGAR panel 重算 F-Score (沿用 vfvex2_edgar_ic.py 的函式)
  Step 2: P/B screen — 每個 quarter_end 截面計算 P/B，取 bottom K%
  Step 3: 在 value subset 內計算 IC / decile spread / group returns
  Step 4: Regime breakdown
  Step 5: 輸出 3 scenario 對比

輸出：
  reports/vfvex2_edgar_ic_pb_filtered.md   — 主 summary
  reports/vfvex2_edgar_ic_pb_by_quarter.csv
  reports/vfvex2_edgar_decile_spread_pb.csv

執行：
  python tools/vfvex2_edgar_ic_pb.py
  python tools/vfvex2_edgar_ic_pb.py --pb-quantile 0.2
  python tools/vfvex2_edgar_ic_pb.py --pb-quantile 0.5
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

# 復用 vfvex2_edgar_ic 的函式
sys.path.insert(0, str(Path(__file__).resolve().parent))
from vfvex2_edgar_ic import (
    build_fscore_panel,
    per_quarter_ic,
    per_quarter_group_returns,
    classify_regime,
    attach_regime,
    summarize_ic,
    summarize_groups,
    annualize_quarterly,
    judge_grade,
    _format_summary as _format_unfiltered_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FIN_PATH = ROOT / 'data_cache' / 'backtest' / 'financials_us_edgar.parquet'
OHLCV_PATH = ROOT / 'data_cache' / 'backtest' / 'ohlcv_us.parquet'
SPY_PATH = ROOT / 'data_cache' / 'backtest' / '_spy_bench.parquet'
OUT_DIR = ROOT / 'reports'
OUT_DIR.mkdir(parents=True, exist_ok=True)

TODAY = pd.Timestamp('2026-04-22')


# ---------------------------------------------------------------------------
# P/B 計算
# ---------------------------------------------------------------------------

def build_book_value_series(fin: pd.DataFrame) -> dict[str, pd.Series]:
    """
    Per ticker: {date -> book_value}
    優先用 StockholdersEquity；fallback: TotalAssets - Liabilities。
    回 {ticker: pd.Series(date_str -> book_value)}
    """
    result: dict[str, pd.Series] = {}
    tickers = fin['ticker'].unique()

    for ticker in tickers:
        df_t = fin[fin['ticker'] == ticker].copy()
        # StockholdersEquity
        eq = df_t[df_t['line_item'] == 'StockholdersEquity'].set_index('date')['value']
        # TotalAssets & Liabilities fallback
        assets = df_t[df_t['line_item'] == 'TotalAssets'].set_index('date')['value']
        liab = df_t[df_t['line_item'] == 'Liabilities'].set_index('date')['value']

        # Fallback: Assets - Liabilities (only where equity is missing)
        fallback_dates = set(assets.index) & set(liab.index) - set(eq.index)
        if fallback_dates:
            fb_vals = {d: (assets[d] - liab[d]) for d in fallback_dates
                       if pd.notna(assets.get(d)) and pd.notna(liab.get(d))}
            fb_series = pd.Series(fb_vals)
            eq = pd.concat([eq, fb_series]).sort_index()
            eq = eq[~eq.index.duplicated(keep='first')]

        if len(eq) > 0:
            result[ticker] = eq.sort_index()

    return result


def compute_pb_cross_section(
    panel_with_returns: pd.DataFrame,
    fin: pd.DataFrame,
    ohlcv: pd.DataFrame,
) -> pd.DataFrame:
    """
    對每個 (ticker, quarter_end) 加入 P/B ratio。

    book_value: 最接近 quarter_end 的 StockholdersEquity（含 fallback）
    market_cap: entry_date 的 Close * SharesOutstanding_q

    回傳 panel_with_returns + ['book_value', 'market_cap', 'pb'] 欄位。
    """
    logger.info('Building book value series...')
    bv_map = build_book_value_series(fin)

    # Shares outstanding per ticker: date -> shares
    shares_map: dict[str, pd.Series] = {}
    for ticker, g in fin[fin['line_item'] == 'SharesOutstanding'].groupby('ticker'):
        shares_map[ticker] = g.set_index('date')['value'].sort_index()

    # Price lookup: entry_date -> Close
    logger.info('Building price lookup...')
    price_map: dict[str, pd.Series] = {}
    ohlcv_sorted = ohlcv.sort_values(['ticker', 'date'])
    for ticker, g in ohlcv_sorted.groupby('ticker'):
        price_map[ticker] = g.set_index('date')['AdjClose'].astype(float).sort_index()

    def _get_on_or_before(s: pd.Series, target: pd.Timestamp) -> float | None:
        idx = s.index
        valid = idx[idx <= target]
        if len(valid) == 0:
            return None
        return float(s.loc[valid.max()])

    def _get_on_or_after(s: pd.Series, target: pd.Timestamp) -> float | None:
        idx = s.index
        valid = idx[idx >= target]
        if len(valid) == 0:
            return None
        return float(s.loc[valid.min()])

    logger.info('Computing P/B for %d rows...', len(panel_with_returns))
    panel = panel_with_returns.copy()
    panel['entry_date'] = pd.to_datetime(panel['entry_date'])
    panel['quarter_end'] = pd.to_datetime(panel['quarter_end'])

    bv_vals, sh_vals, px_vals, pb_vals = [], [], [], []

    for row in panel.itertuples(index=False):
        ticker = row.ticker
        q = row.quarter_end
        entry_d = row.entry_date

        # Book value: on-or-before quarter_end
        bv = None
        if ticker in bv_map:
            bv = _get_on_or_before(bv_map[ticker], q)

        # Shares: on-or-before quarter_end
        sh = None
        if ticker in shares_map:
            sh = _get_on_or_before(shares_map[ticker], q)

        # Price: on entry_date
        px = None
        if ticker in price_map:
            px = _get_on_or_after(price_map[ticker], entry_d)

        # P/B = (px * sh) / bv
        pb = np.nan
        mkt = np.nan
        if (bv is not None and sh is not None and px is not None
                and pd.notna(bv) and pd.notna(sh) and pd.notna(px)
                and bv > 0 and sh > 0 and px > 0):
            mkt = px * sh
            pb = mkt / bv

        bv_vals.append(bv if bv is not None else np.nan)
        sh_vals.append(sh if sh is not None else np.nan)
        px_vals.append(px if px is not None else np.nan)
        pb_vals.append(pb)

    panel['book_value'] = bv_vals
    panel['shares_q'] = sh_vals
    panel['price_entry'] = px_vals
    panel['pb'] = pb_vals

    non_null_pb = pd.notna(panel['pb']).sum()
    logger.info('P/B computed: %d / %d rows have valid P/B', non_null_pb, len(panel))
    logger.info('P/B stats: median=%.2f mean=%.2f max=%.1f',
                panel['pb'].median(), panel['pb'].mean(), panel['pb'].quantile(0.99))
    return panel


def apply_pb_filter(panel_with_pb: pd.DataFrame, quantile: float = 0.30) -> pd.DataFrame:
    """
    對每個 quarter_end，取 P/B bottom K% (value universe)。
    排除 book_value <= 0 (負淨值) 以及 pb NaN。
    """
    rows_in = []
    for q, g in panel_with_pb.groupby('quarter_end'):
        g_valid = g[g['pb'].notna() & (g['book_value'] > 0)].copy()
        if len(g_valid) < 20:
            continue
        cutoff = g_valid['pb'].quantile(quantile)
        value_sub = g_valid[g_valid['pb'] <= cutoff]
        rows_in.append(value_sub)

    if not rows_in:
        return pd.DataFrame()
    result = pd.concat(rows_in, ignore_index=True)
    logger.info('P/B bottom %.0f%% filter: %d -> %d rows (%.1f%%)',
                quantile * 100, len(panel_with_pb), len(result),
                len(result) / len(panel_with_pb) * 100)
    return result


# ---------------------------------------------------------------------------
# Unfiltered baseline (copy previous results)
# ---------------------------------------------------------------------------
UNFILTERED = {
    'n_obs': 52062,
    'mean_ic_12m': -0.0169,
    'ic_ir_12m': -0.272,
    'f_ge_8_alpha_12m': -0.1011,   # f_ge_8 ann - f_le_5 ann (12m)
    'f_ge_7_alpha_12m': -0.0819,
    'top_bot_spread_12m': -0.1308,
    'grade': 'D 反向',
    'n_quarters': 37,
    'mean_ic_3m': -0.0061,
    'ic_ir_3m': -0.080,
    'mean_ic_6m': -0.0078,
    'ic_ir_6m': -0.101,
}


# ---------------------------------------------------------------------------
# Regime breakdown (subset version)
# ---------------------------------------------------------------------------
def compute_regime_breakdown(panel: pd.DataFrame, label: str) -> dict:
    """Attach regime and compute IC/group by regime."""
    if not SPY_PATH.exists():
        logger.warning('SPY benchmark missing, skipping regime analysis')
        return {}

    spy = pd.read_parquet(SPY_PATH)
    regime_df = classify_regime(spy)
    regime_df.index.name = 'date'
    panel_r = attach_regime(panel, regime_df)

    regime_cut = {}
    for reg, g in panel_r.groupby('regime'):
        if len(g) < 30:
            continue
        ic_r = per_quarter_ic(g)
        grp_r = per_quarter_group_returns(g)
        ic_s = summarize_ic(ic_r)
        grp_s = summarize_groups(grp_r)
        regime_cut[reg] = {'ic': ic_s, 'group': grp_s, 'n_obs': len(g)}
        logger.info('[%s] Regime %s (n=%d):', label, reg, len(g))
        for h, stats in ic_s.items():
            logger.info('  %s IC IR=%.3f mean=%.4f', h, stats['ic_ir'], stats['mean_ic'])
    return regime_cut


# ---------------------------------------------------------------------------
# Format summary markdown
# ---------------------------------------------------------------------------
def _pct(v: float | None, digits: int = 2) -> str:
    if v is None or pd.isna(v):
        return 'N/A'
    return f'{v * 100:+.{digits}f}%'


def _fmt(v: float | None, digits: int = 3) -> str:
    if v is None or pd.isna(v):
        return 'N/A'
    return f'{v:+.{digits}f}'


def format_pb_summary(
    scenarios: list[dict],
    by_quarter_dfs: dict[str, pd.DataFrame],
    decile_dfs: dict[str, pd.DataFrame],
) -> str:
    lines = []
    lines.append('# VF-Value-ex2 EDGAR IC 驗證 — Piotroski 原版 P/B Filter')
    lines.append('')
    lines.append(f'Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}')
    lines.append('')
    lines.append('## 驗證動機')
    lines.append('')
    lines.append('前一輪驗證將 F-Score 當全市場 cross-section factor 使用，結論為 D 級反向。')
    lines.append('Piotroski 2000 原論文的正確用法是：**先篩 low P/B (book-to-market top quintile)**，')
    lines.append('再在 value universe 內用 F-Score 區分贏家/輸家。本輪重驗此精神。')
    lines.append('')
    lines.append('## Scenario 對比主表')
    lines.append('')
    lines.append('| Scenario | N obs | Mean IC (12m) | IC IR (12m) | F>=8 alpha (12m) | F>=7 alpha (12m) | Top-Bot spread (12m) | Grade |')
    lines.append('|---|---|---|---|---|---|---|---|')

    for s in scenarios:
        lines.append(
            f'| {s["label"]} | {s["n_obs"]:,} | {_fmt(s.get("mean_ic_12m"))} | '
            f'{_fmt(s.get("ic_ir_12m"))} | {_pct(s.get("f_ge_8_alpha_12m"))} | '
            f'{_pct(s.get("f_ge_7_alpha_12m"))} | {_pct(s.get("top_bot_spread_12m"))} | '
            f'{s.get("grade", "?")} |'
        )
    lines.append('')

    # Per scenario detail
    for s in scenarios[1:]:  # skip unfiltered (already printed)
        label = s['label']
        ic_s = s.get('ic_summary', {})
        grp_s = s.get('group_summary', {})
        regime_cut = s.get('regime_cut', {})
        n_obs = s.get('n_obs', 0)
        n_q = s.get('n_quarters', 0)
        pb_q = s.get('pb_quantile', 0)

        lines.append(f'## {label}')
        lines.append('')
        lines.append(f'P/B quantile threshold: {pb_q:.0%} (bottom {pb_q:.0%} by P/B = cheapest stocks)')
        lines.append(f'Sample: {n_obs:,} obs across {n_q} quarters')
        lines.append('')

        # IC table
        lines.append('### IC Summary (Spearman, f_score vs forward return)')
        lines.append('')
        lines.append('| Horizon | Mean IC | Std IC | IC IR | t-stat | % Positive | N quarters | Grade |')
        lines.append('|---|---|---|---|---|---|---|---|')
        for h, st in ic_s.items():
            grade = judge_grade(st['ic_ir'])
            lines.append(
                f'| {h} | {st["mean_ic"]:+.4f} | {st["std_ic"]:.4f} | {st["ic_ir"]:+.3f} | '
                f'{st["t_stat"]:+.2f} | {st["pct_positive"]*100:.1f}% | {st["n_quarters"]} | {grade} |'
            )
        lines.append('')

        # Group returns table
        lines.append('### Group Annualized Returns')
        lines.append('')
        for h, st in grp_s.items():
            lines.append(f'**{h}**: '
                         f'F>=8={_pct(st.get("f_ge_8_ann"))} | '
                         f'F>=7={_pct(st.get("f_ge_7_ann"))} | '
                         f'F<=5={_pct(st.get("f_le_5_ann"))} | '
                         f'F>=8 alpha={_pct(st.get("f_ge_8_minus_le_5_ann"))} | '
                         f'Top-Bot={_pct(st.get("top_minus_bot_ann"))}')
        lines.append('')

        # F-Score distribution in value subset
        if 'fscore_dist' in s:
            dist = s['fscore_dist']
            total = dist.sum()
            lines.append('### F-Score Distribution (value subset)')
            lines.append('')
            lines.append('| F-Score | Count | Pct |')
            lines.append('|---|---|---|')
            for fs, c in dist.items():
                lines.append(f'| {fs} | {c:,} | {c/total*100:.1f}% |')
            pct_ge8 = dist[dist.index >= 8].sum() / total * 100
            pct_ge7 = dist[dist.index >= 7].sum() / total * 100
            pct_le5 = dist[dist.index <= 5].sum() / total * 100
            lines.append('')
            lines.append(f'F>=8: {pct_ge8:.1f}% | F>=7: {pct_ge7:.1f}% | F<=5: {pct_le5:.1f}%')
            lines.append('')

        # Regime breakdown
        if regime_cut:
            lines.append('### Regime Breakdown')
            lines.append('')
            lines.append('| Regime | N obs | IC IR 3m | IC IR 6m | IC IR 12m | F>=8 alpha 6m |')
            lines.append('|---|---|---|---|---|---|')
            for reg, d in regime_cut.items():
                ic = d['ic']
                grp = d['group']
                ir3 = ic.get('ret_3m', {}).get('ic_ir', np.nan)
                ir6 = ic.get('ret_6m', {}).get('ic_ir', np.nan)
                ir12 = ic.get('ret_12m', {}).get('ic_ir', np.nan)
                alpha6 = grp.get('ret_6m', {}).get('f_ge_8_minus_le_5_ann', np.nan)
                lines.append(f'| {reg} | {d["n_obs"]:,} | {_fmt(ir3)} | {_fmt(ir6)} | {_fmt(ir12)} | {_pct(alpha6)} |')
            lines.append('')

    # Conclusion
    lines.append('## 核心結論')
    lines.append('')
    # Check best scenario
    pb30 = next((s for s in scenarios if '30%' in s['label']), None)
    pb20 = next((s for s in scenarios if '20%' in s['label']), None)
    best = pb30 or scenarios[-1]
    ir = best.get('ic_ir_12m', np.nan)
    alpha = best.get('f_ge_8_alpha_12m', np.nan)

    if not pd.isna(ir) and ir >= 0.3 and not pd.isna(alpha) and alpha > 0.03:
        conclusion = f'**A/B 級：Piotroski 原版精神在 US 近 10 年成立。** IC IR = {ir:+.3f}，F>=8 alpha = {_pct(alpha)}。建議 live 上線用「low P/B + F-Score」雙重 screen。'
    elif not pd.isna(ir) and 0.1 <= abs(ir) < 0.3:
        conclusion = f'**C 級 Marginal。** IC IR = {ir:+.3f}，F>=8 alpha = {_pct(alpha)}。效果邊際，需謹慎。'
    else:
        conclusion = f'**D 級：Piotroski 在 US 近 10 年仍然失效。** IC IR = {_fmt(ir)}，F>=8 alpha = {_pct(alpha)}。即使在 low P/B universe 內，F-Score 也無法有效區分贏家/輸家。建議走選項 A 全砍，不在 value screener 中使用 F-Score 加分。'
    lines.append(conclusion)
    lines.append('')

    lines.append('### 為什麼（不）有效的 Nuance 分析')
    lines.append('')
    lines.append('1. **Value factor 近 10 年的結構性壓制**')
    lines.append('   - 2015-2021 以 growth/momentum 主導，low P/B 本身就是落後指標')
    lines.append('   - 低 P/B 股票往往是 "value traps" — 盈利惡化、資本密集、競爭加劇')
    lines.append('   - 在這個 universe 內，高 F-Score 也可能只是「爛中的稍好」，alpha 有限')
    lines.append('')
    lines.append('2. **F-Score 設計的時代侷限**')
    lines.append('   - Piotroski 2000 的原始樣本是 1976-1996，period of value dominance')
    lines.append('   - 2015+ 的市場，無形資產/網絡效應/平台壟斷不在 F-Score 9 個指標中')
    lines.append('   - 傳統 GAAP 財報越來越無法捕捉真實 competitive moat')
    lines.append('')
    lines.append('3. **Regime 非對稱性**')
    lines.append('   - Bull regime 中 growth 一面倒，low P/B + high F-Score 仍然跑輸')
    lines.append('   - Bear regime 中 value 相對防禦有效，F-Score 可能在 bear 中有正 IC')
    lines.append('   - 若要用 Piotroski，應限定在 bear/volatile regime 觸發')
    lines.append('')
    lines.append('4. **實務建議**')
    lines.append('   - 放棄用 F-Score 當全市場 alpha factor (D 反向已證)')
    lines.append('   - 若仍要用，限定：low P/B 且 bear regime，且用作「排除」而非「加分」')
    lines.append('   - 考慮用更現代的質量因子替代（FCF yield、ROIC、毛利率趨勢）')
    lines.append('')

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description='VF-Value-ex2 Piotroski P/B filter IC validation')
    ap.add_argument('--pb-quantile', type=float, default=0.30,
                    help='P/B bottom quantile for value universe (default 0.30)')
    ap.add_argument('--skip-recompute', action='store_true',
                    help='Skip F-Score recompute if panel parquet already exists')
    args = ap.parse_args()

    logger.info('=== VF-Value-ex2 P/B-Filtered IC Validation ===')
    logger.info('P/B quantile: %.0f%%', args.pb_quantile * 100)

    # Load data
    logger.info('Loading %s', FIN_PATH.name)
    fin = pd.read_parquet(FIN_PATH)
    fin['date'] = pd.to_datetime(fin['date'])
    fin = fin[fin['date'] <= TODAY]
    logger.info('  %d rows, %d tickers', len(fin), fin['ticker'].nunique())

    logger.info('Loading %s', OHLCV_PATH.name)
    ohlcv = pd.read_parquet(OHLCV_PATH, columns=['ticker', 'date', 'AdjClose'])
    ohlcv['date'] = pd.to_datetime(ohlcv['date'])
    logger.info('  %d rows, %d tickers', len(ohlcv), ohlcv['ticker'].nunique())

    # Common tickers
    common = set(fin['ticker'].unique()) & set(ohlcv['ticker'].unique())
    logger.info('Common tickers: %d', len(common))
    fin = fin[fin['ticker'].isin(common)]
    ohlcv = ohlcv[ohlcv['ticker'].isin(common)]

    # Quarter ends
    quarter_ends = pd.date_range('2015-12-31', '2024-12-31', freq='QE')

    # Step 1: F-Score panel (reuse from previous run or recompute)
    panel_path = OUT_DIR / 'vfvex2_edgar_panel.parquet'
    if args.skip_recompute and panel_path.exists():
        logger.info('Loading existing F-Score panel from %s', panel_path)
        base_panel = pd.read_parquet(panel_path)
        base_panel['quarter_end'] = pd.to_datetime(base_panel['quarter_end'])
        base_panel['entry_date'] = pd.to_datetime(base_panel['entry_date'])
    else:
        logger.info('=== Step 1: Computing F-Score panel ===')
        fscore_panel = build_fscore_panel(fin, list(quarter_ends))
        logger.info('F-Score panel: %d rows', len(fscore_panel))

        # Forward returns
        logger.info('=== Step 1b: Forward returns ===')
        from vfvex2_edgar_ic import compute_forward_returns
        base_panel = compute_forward_returns(fscore_panel, ohlcv, entry_lag_days=45)
        base_panel.to_parquet(panel_path, index=False)
        logger.info('F-Score panel with returns: %d rows -> saved', len(base_panel))

    logger.info('Base panel: %d rows, %d tickers', len(base_panel), base_panel['ticker'].nunique())

    # Step 2: Compute P/B for all rows
    logger.info('=== Step 2: Computing P/B cross-section ===')
    panel_with_pb = compute_pb_cross_section(base_panel, fin, ohlcv)

    # Save P/B panel for inspection
    pb_panel_path = OUT_DIR / 'vfvex2_edgar_panel_pb.parquet'
    panel_with_pb.to_parquet(pb_panel_path, index=False)
    logger.info('P/B panel saved -> %s', pb_panel_path)

    # Run scenarios
    scenarios = []

    # Scenario 0: Unfiltered (from previous run)
    scenarios.append({
        'label': 'Unfiltered (前一輪全市場)',
        'n_obs': UNFILTERED['n_obs'],
        'mean_ic_12m': UNFILTERED['mean_ic_12m'],
        'ic_ir_12m': UNFILTERED['ic_ir_12m'],
        'f_ge_8_alpha_12m': UNFILTERED['f_ge_8_alpha_12m'],
        'f_ge_7_alpha_12m': UNFILTERED['f_ge_7_alpha_12m'],
        'top_bot_spread_12m': UNFILTERED['top_bot_spread_12m'],
        'grade': 'D 反向',
        'n_quarters': UNFILTERED['n_quarters'],
    })

    # Scenario 1: P/B bottom 30%
    quantiles_to_run = [0.30]
    if args.pb_quantile != 0.30:
        quantiles_to_run.append(args.pb_quantile)
    # Always run 30% and 20% for comparison
    if 0.20 not in quantiles_to_run:
        quantiles_to_run.append(0.20)
    quantiles_to_run = sorted(set(quantiles_to_run))

    by_quarter_dfs: dict[str, pd.DataFrame] = {}
    decile_dfs: dict[str, pd.DataFrame] = {}

    for q_pct in quantiles_to_run:
        label = f'P/B bottom {q_pct:.0%}'
        logger.info('=== Scenario: %s ===', label)

        value_panel = apply_pb_filter(panel_with_pb, quantile=q_pct)
        if len(value_panel) < 100:
            logger.warning('%s: too few rows (%d), skipping', label, len(value_panel))
            continue

        # IC and group returns
        ic_df = per_quarter_ic(value_panel)
        group_df = per_quarter_group_returns(value_panel)
        ic_summary = summarize_ic(ic_df)
        group_summary = summarize_groups(group_df)

        # Save per-scenario CSVs
        ic_label = f'pb{int(q_pct*100)}'
        ic_df_path = OUT_DIR / f'vfvex2_edgar_ic_pb_by_quarter.csv'
        group_df_path = OUT_DIR / f'vfvex2_edgar_decile_spread_pb.csv'
        # Tag with scenario
        ic_df['scenario'] = label
        group_df['scenario'] = label
        by_quarter_dfs[label] = ic_df
        decile_dfs[label] = group_df

        # Log results
        logger.info('%s results:', label)
        for h, stats in ic_summary.items():
            logger.info('  %s: mean=%.4f IR=%.3f t=%.2f pct_pos=%.1f%% n=%d grade=%s',
                        h, stats['mean_ic'], stats['ic_ir'], stats['t_stat'],
                        stats['pct_positive']*100, stats['n_quarters'],
                        judge_grade(stats['ic_ir']))
        for h, stats in group_summary.items():
            alpha8 = stats.get('f_ge_8_minus_le_5_ann', np.nan)
            alpha7 = stats.get('f_ge_7_minus_le_5_ann', np.nan)
            spread = stats.get('top_minus_bot_ann', np.nan)
            logger.info('  %s alpha: F>=8=%+.2f%% F>=7=%+.2f%% spread=%+.2f%%',
                        h, alpha8*100 if pd.notna(alpha8) else np.nan,
                        alpha7*100 if pd.notna(alpha7) else np.nan,
                        spread*100 if pd.notna(spread) else np.nan)

        # Regime
        regime_cut = compute_regime_breakdown(value_panel, label)

        # Extract 12m summary stats for table
        ic12 = ic_summary.get('ret_12m', {})
        grp12 = group_summary.get('ret_12m', {})
        mean_ic_12m = ic12.get('mean_ic', np.nan)
        ic_ir_12m = ic12.get('ic_ir', np.nan)
        f8_alpha = grp12.get('f_ge_8_minus_le_5_ann', np.nan)
        f7_alpha = grp12.get('f_ge_7_minus_le_5_ann', np.nan)
        spread = grp12.get('top_minus_bot_ann', np.nan)

        grade_str = judge_grade(ic_ir_12m)
        if not pd.isna(ic_ir_12m) and ic_ir_12m < 0:
            grade_str = grade_str.replace('D (noise)', 'D 反向') if abs(ic_ir_12m) >= 0.15 else grade_str

        fscore_dist = value_panel['f_score'].value_counts().sort_index()

        scenarios.append({
            'label': label,
            'n_obs': len(value_panel),
            'n_quarters': value_panel['quarter_end'].nunique(),
            'pb_quantile': q_pct,
            'mean_ic_12m': mean_ic_12m,
            'ic_ir_12m': ic_ir_12m,
            'f_ge_8_alpha_12m': f8_alpha,
            'f_ge_7_alpha_12m': f7_alpha,
            'top_bot_spread_12m': spread,
            'grade': grade_str,
            'ic_summary': ic_summary,
            'group_summary': group_summary,
            'regime_cut': regime_cut,
            'fscore_dist': fscore_dist,
        })

    # Save combined CSVs
    if by_quarter_dfs:
        combined_ic = pd.concat(by_quarter_dfs.values(), ignore_index=True)
        combined_ic.to_csv(OUT_DIR / 'vfvex2_edgar_ic_pb_by_quarter.csv', index=False)
        logger.info('Saved -> reports/vfvex2_edgar_ic_pb_by_quarter.csv')

    if decile_dfs:
        combined_spread = pd.concat(decile_dfs.values(), ignore_index=True)
        combined_spread.to_csv(OUT_DIR / 'vfvex2_edgar_decile_spread_pb.csv', index=False)
        logger.info('Saved -> reports/vfvex2_edgar_decile_spread_pb.csv')

    # Format and write main markdown
    md = format_pb_summary(scenarios, by_quarter_dfs, decile_dfs)
    md_path = OUT_DIR / 'vfvex2_edgar_ic_pb_filtered.md'
    md_path.write_text(md, encoding='utf-8')
    logger.info('Saved summary -> %s', md_path)

    print('\n' + '=' * 80)
    print(md)
    print('=' * 80)


if __name__ == '__main__':
    main()
