"""
vf_chip_dual_inst_regime_gate.py
================================
È©óË??®„ÄåÂ?Ë≥??ï‰ø°?åÂ? + 5d ?èÊ????çË??ü‰?‰∏äÔ???TWA Ë∂®Âã¢ regime gate
?ØÂê¶?ΩÂ? A Á¥?/ ?ØÂê¶?üÊ??®Ô?OOS ?ØÂê¶‰∏Ä?¥Ô?ÔºåÈ??Ø‰?ÂæåÊ? 2024??
Gate A Ë¶èÂ?ÔºàPIT-safeÔºåt-1 ?•Ë??ôÂà§??t ?•Ë??üÔ?:
    - TWA(^TWII) Close_{t-1} >= MA200_{t-1}
    - MA60 slope: (MA60_{t-1} - MA60_{t-21}) / MA60_{t-21} > 0

È©óË? design:
    Step 1: in-sample (full 2023-01 ~ 2026-04) ??gate Âæ?IC/spread/Sharpe
    Step 2: walk-forward year-by-year out-of-sample
            -> ??gate ?ØÂõ∫ÂÆöË??áÊ? free paramÔºåÁ???split-by-year ?±Â??ÑÂπ¥?∏Â?
    Step 3: Â∞çÊ?Ë°?no-gate vs Gate A

Inputs:
    reports/vf_chip_dual_inst_results.csv     -- 1861 Á≠?signal hits + fwd returns
    data_cache/backtest/_twii_bench.parquet   -- TWII daily for gate
    data_cache/0050_price.csv                 -- 0050 daily for benchmark
    data_cache/backtest/ohlcv_tw.parquet      -- (?ÖÂ? background pool for IC, ?çÁî® panel)
    data_cache/chip_history/institutional.parquet -- (?çÁî® inst features for full panel)
    data_cache/backtest/universe_tw_full.parquet  -- (?çÁî® universe filter)

Output:
    reports/vf_chip_dual_inst_ic_v2_regime.md
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Re-use existing pipeline
from tools.vf_chip_dual_inst_signal import (
    load_universe,
    load_institutional,
    load_ohlcv,
    compute_signals,
    HORIZONS,
    START_DATE,
    END_DATE,
)

OUT_DIR = ROOT / "reports"
OUT_MD = OUT_DIR / "vf_chip_dual_inst_ic_v2_regime.md"
TWII_PATH = ROOT / "data_cache" / "backtest" / "_twii_bench.parquet"
BENCH_0050 = ROOT / "data_cache" / "0050_price.csv"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("vf-gate")


def load_twii() -> pd.DataFrame:
    """Load TWII daily and compute Gate A features.

    Returns: DataFrame with index=date and columns: close, ma200, ma60, ma60_slope_20d, gate_a_pit
    gate_a_pit @ date t uses ONLY t-1 (and earlier) data, so signal at t is PIT-safe.
    """
    df = pd.read_parquet(TWII_PATH)
    df.columns = ['_'.join([str(c) for c in col if c]).strip() for col in df.columns]
    df = df.rename(columns={'Close_^TWII': 'close'})[['close']].copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    # Pad up to 2026-05-15 with last close (handles edge case for last few signal dates)
    full_idx = pd.date_range(df.index.min(), pd.Timestamp("2026-05-31"), freq='D')
    # but easier: just keep the trading days we have

    # Compute MAs on the closing series itself (trading-day frequency, not calendar)
    df['ma200'] = df['close'].rolling(200, min_periods=200).mean()
    df['ma60'] = df['close'].rolling(60, min_periods=60).mean()
    df['ma60_lag20'] = df['ma60'].shift(20)
    df['ma60_slope_20d'] = df['ma60'] / df['ma60_lag20'] - 1.0

    # Raw same-day gate (close above MA200 + slope > 0)
    df['gate_a_same_day'] = (df['close'] >= df['ma200']) & (df['ma60_slope_20d'] > 0)

    # PIT: shift forward by 1 trading day so gate at date t uses {<=t-1} data
    df['gate_a_pit'] = df['gate_a_same_day'].shift(1).fillna(False)

    logger.info(f"TWII gate loaded: {len(df):,} rows, gate_a_pit True={df['gate_a_pit'].sum()} "
                f"({df['gate_a_pit'].mean():.1%})")
    return df


def load_0050_returns(hold_days: int) -> pd.DataFrame:
    """Load 0050 daily forward returns over hold_days."""
    df = pd.read_csv(BENCH_0050)
    df.rename(columns={df.columns[0]: 'date'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    df['bench_fwd'] = df['Close'].shift(-hold_days) / df['Close'] - 1.0
    return df[['date', 'bench_fwd']]


def apply_gate_to_signals(signals_csv: pd.DataFrame, twii: pd.DataFrame) -> pd.DataFrame:
    """Tag each signal row with gate_a_pit. NOT filter ??caller filters."""
    signals = signals_csv.copy()
    signals['date'] = pd.to_datetime(signals['date'])

    # Map TWII gate by date (asof to handle weekend/non-trading dates if any)
    twii_g = twii[['gate_a_pit']].reset_index().rename(columns={'Date': 'date'})
    twii_g = twii_g.sort_values('date').reset_index(drop=True)
    signals = signals.sort_values('date').reset_index(drop=True)
    signals = pd.merge_asof(signals, twii_g, on='date', direction='backward')
    signals['gate_a_pit'] = signals['gate_a_pit'].fillna(False)
    return signals


def signal_summary(signals: pd.DataFrame, label: str, hold_days: int = 60) -> dict:
    """Compute spread vs background-naive (using fwd_ret means among signals only ??not vs bg);
    here we use single-arm stats since CSV only contains signal rows.

    Outputs:
      n_sig, mean_ret, median_ret, std_ret, hit, t_vs_zero, sharpe_avg (annualised, treating
      each signal as 1 trade), 0050 mean_ret over same dates.
    """
    s = signals.copy()
    s = s[s[f'fwd_ret_{hold_days}d'].notna()]
    n = len(s)
    if n < 5:
        return {
            'label': label, 'n_sig': n, 'mean_ret': float('nan'),
            'median_ret': float('nan'), 'hit_rate': float('nan'),
            't_vs_zero': float('nan'), 'sharpe': float('nan'),
            'bench_mean': float('nan'), 'excess': float('nan'), 'ir': float('nan'),
        }
    rs = s[f'fwd_ret_{hold_days}d'].values
    mean_ret = float(np.mean(rs))
    median_ret = float(np.median(rs))
    std_ret = float(np.std(rs, ddof=1))
    hit_rate = float((rs > 0).mean())
    t_vs_zero = float(mean_ret / (std_ret / np.sqrt(n))) if std_ret > 0 else float('nan')
    # Trade-level Sharpe annualised
    sharpe = float(mean_ret / std_ret * np.sqrt(252.0 / hold_days)) if std_ret > 0 else float('nan')

    # 0050 fwd ret on same dates (basket-level: average per signal date, then average across dates)
    bench = load_0050_returns(hold_days)
    merged = s.merge(bench, on='date', how='left')
    merged = merged[merged['bench_fwd'].notna()]
    if len(merged) < 5:
        bench_mean = float('nan'); excess = float('nan'); ir = float('nan')
    else:
        # daily-basket level: avg by date
        daily = merged.groupby('date').agg(sig=('fwd_ret_'+str(hold_days)+'d', 'mean'),
                                            bench=('bench_fwd', 'first')).dropna()
        diff = daily['sig'] - daily['bench']
        bench_mean = float(daily['bench'].mean())
        excess = float(diff.mean())
        ir = float(diff.mean() / diff.std(ddof=1) * np.sqrt(252.0 / hold_days)) if diff.std(ddof=1) > 0 else float('nan')

    return {
        'label': label, 'n_sig': n, 'mean_ret': mean_ret,
        'median_ret': median_ret, 'hit_rate': hit_rate, 'std_ret': std_ret,
        't_vs_zero': t_vs_zero, 'sharpe': sharpe,
        'bench_mean': bench_mean, 'excess': excess, 'ir': ir,
    }


def compute_ic_with_gate(panel: pd.DataFrame, twii: pd.DataFrame, hold_days: int,
                          apply_gate: bool) -> dict:
    """Daily cross-sectional Spearman IC; when apply_gate=True, restrict to dates
    where gate_a_pit=True.
    """
    elig = panel[panel['eligible'] & panel[f'fwd_ret_{hold_days}d'].notna()].copy()
    if apply_gate:
        # Map gate to each date
        twii_g = twii[['gate_a_pit']].reset_index().rename(columns={'Date': 'date'})
        twii_g = twii_g.sort_values('date').reset_index(drop=True)
        elig = elig.sort_values('date').reset_index(drop=True)
        elig = pd.merge_asof(elig, twii_g, on='date', direction='backward')
        elig = elig[elig['gate_a_pit'] == True].copy()

    daily_ic = []
    for dt, grp in elig.groupby('date'):
        if len(grp) < 30:
            continue
        try:
            rho, _ = stats.spearmanr(grp['signal_strength'], grp[f'fwd_ret_{hold_days}d'])
            if pd.notna(rho):
                daily_ic.append(rho)
        except Exception:
            continue
    if not daily_ic:
        return {'mean_ic': float('nan'), 'ic_ir': float('nan'),
                't_stat': float('nan'), 'n_days': 0}
    arr = np.array(daily_ic)
    mean_ic = float(arr.mean())
    std_ic = float(arr.std(ddof=1))
    n = len(arr)
    ic_ir = mean_ic / std_ic if std_ic > 0 else float('nan')
    t = mean_ic / (std_ic / np.sqrt(n)) if std_ic > 0 else float('nan')
    return {'mean_ic': mean_ic, 'ic_ir': ic_ir, 't_stat': t, 'n_days': n}


def compute_binary_spread_with_gate(panel: pd.DataFrame, twii: pd.DataFrame,
                                     hold_days: int, apply_gate: bool) -> dict:
    """Binary spread: mean(signal & gate) vs mean(non-signal & gate)."""
    elig = panel[panel['eligible'] & panel[f'fwd_ret_{hold_days}d'].notna()].copy()
    if apply_gate:
        twii_g = twii[['gate_a_pit']].reset_index().rename(columns={'Date': 'date'})
        twii_g = twii_g.sort_values('date').reset_index(drop=True)
        elig = elig.sort_values('date').reset_index(drop=True)
        elig = pd.merge_asof(elig, twii_g, on='date', direction='backward')
        elig = elig[elig['gate_a_pit'] == True].copy()
    sig = elig[elig['signal']]
    bg = elig[~elig['signal']]
    if len(sig) < 5 or len(bg) < 100:
        return {'n_sig': len(sig), 'n_bg': len(bg), 'spread': float('nan'),
                't_spread': float('nan'), 'hit_sig': float('nan')}
    rs = sig[f'fwd_ret_{hold_days}d'].dropna()
    rb = bg[f'fwd_ret_{hold_days}d'].dropna()
    mean_s = float(rs.mean()); mean_b = float(rb.mean())
    spread = mean_s - mean_b
    t, p = stats.ttest_ind(rs, rb, equal_var=False)
    return {'n_sig': len(sig), 'n_bg': len(bg),
            'mean_sig': mean_s, 'mean_bg': mean_b,
            'spread': spread, 't_spread': float(t),
            'hit_sig': float((rs > 0).mean())}


def build_master_panel():
    """Re-build the same panel as v1 (so we can compute IC with/without gate)."""
    universe = load_universe()
    inst = load_institutional()
    ohlcv = load_ohlcv()
    panel = compute_signals(inst, ohlcv, universe)
    return panel


def per_year_breakdown(signals: pd.DataFrame, label: str, hold_days: int = 60) -> list[dict]:
    s = signals.copy()
    s['year'] = pd.to_datetime(s['date']).dt.year
    out = []
    for y in sorted(s['year'].unique()):
        sub = s[s['year'] == y]
        d = signal_summary(sub, f"{label}-{y}", hold_days=hold_days)
        d['year'] = int(y)
        out.append(d)
    return out


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Loading TWII gate...")
    twii = load_twii()

    logger.info("Loading existing signal CSV (1861 rows)...")
    sigs = pd.read_csv(ROOT / "reports" / "vf_chip_dual_inst_results.csv")
    sigs = apply_gate_to_signals(sigs, twii)
    sigs['date'] = pd.to_datetime(sigs['date'])

    n_total = len(sigs)
    n_with_gate = int(sigs['gate_a_pit'].sum())
    logger.info(f"Signals total: {n_total} / with Gate A passing: {n_with_gate} "
                f"({n_with_gate/n_total:.1%})")

    # Per-year gate filter ratio
    sigs['year'] = sigs['date'].dt.year
    gate_yr = sigs.groupby('year').agg(n_total=('stock_id','count'),
                                        n_passed=('gate_a_pit','sum'))
    gate_yr['pct_passed'] = gate_yr['n_passed'] / gate_yr['n_total']
    logger.info("Per-year gate pass:\n" + gate_yr.to_string())

    # Single-arm summaries (signal-only; cross-arm IC needs full panel)
    summary_nogate_60d = signal_summary(sigs, "no-gate", hold_days=60)
    summary_gate_60d = signal_summary(sigs[sigs['gate_a_pit']], "Gate A", hold_days=60)
    summary_nogate_20d = signal_summary(sigs, "no-gate", hold_days=20)
    summary_gate_20d = signal_summary(sigs[sigs['gate_a_pit']], "Gate A", hold_days=20)
    summary_nogate_120d = signal_summary(sigs, "no-gate", hold_days=120)
    summary_gate_120d = signal_summary(sigs[sigs['gate_a_pit']], "Gate A", hold_days=120)

    # Year-by-year walk-forward OOS report (Gate A only)
    yearly_nogate = per_year_breakdown(sigs, "no-gate", hold_days=60)
    yearly_gate = per_year_breakdown(sigs[sigs['gate_a_pit']], "Gate A", hold_days=60)

    # Rebuild full panel for IC + binary spread cross-sectional with bg comparison
    logger.info("Rebuilding full panel for IC computation (slow)...")
    panel = build_master_panel()

    ic_results = []
    for h in HORIZONS:
        logger.info(f"IC horizon {h}d (no gate)...")
        no_g = compute_ic_with_gate(panel, twii, h, apply_gate=False)
        bin_no = compute_binary_spread_with_gate(panel, twii, h, apply_gate=False)
        logger.info(f"IC horizon {h}d (with Gate A)...")
        with_g = compute_ic_with_gate(panel, twii, h, apply_gate=True)
        bin_with = compute_binary_spread_with_gate(panel, twii, h, apply_gate=True)
        ic_results.append({
            'horizon': h,
            'no_gate': {**no_g, **bin_no},
            'gate_a': {**with_g, **bin_with},
        })
        logger.info(f"  {h}d no-gate: IC {no_g['mean_ic']:+.4f} spread {bin_no['spread']:+.4f} | "
                    f"Gate A: IC {with_g['mean_ic']:+.4f} spread {bin_with['spread']:+.4f}")

    # Build report
    write_report(
        ic_results=ic_results,
        gate_yr=gate_yr,
        n_total=n_total, n_passed=n_with_gate,
        summary_nogate={20: summary_nogate_20d, 60: summary_nogate_60d, 120: summary_nogate_120d},
        summary_gate={20: summary_gate_20d, 60: summary_gate_60d, 120: summary_gate_120d},
        yearly_nogate=yearly_nogate,
        yearly_gate=yearly_gate,
    )


def write_report(ic_results, gate_yr, n_total, n_passed,
                  summary_nogate, summary_gate,
                  yearly_nogate, yearly_gate):
    lines = []
    lines.append("# VF v2 ??Dual-Inst + Volume Signal ? Regime Gate")
    lines.append("")

    # Determine verdict
    h60 = next(r for r in ic_results if r['horizon'] == 60)
    h120 = next(r for r in ic_results if r['horizon'] == 120)
    gate_ic60 = h60['gate_a'].get('mean_ic', 0) or 0
    gate_ic120 = h120['gate_a'].get('mean_ic', 0) or 0
    gate_spread60 = h60['gate_a'].get('spread', 0) or 0
    gate_ir = summary_gate[60]['ir']

    # walk-forward OOS check: are all years with Gate A signals positive on 60d Sharpe?
    oos_sharpes_gate = [y['sharpe'] for y in yearly_gate if not np.isnan(y['sharpe'])]
    oos_all_positive = all(s > 0 for s in oos_sharpes_gate) if oos_sharpes_gate else False
    oos_2024 = next((y for y in yearly_gate if y['year'] == 2024), None)
    oos_2024_sharpe = oos_2024['sharpe'] if oos_2024 else float('nan')

    # Grade
    A_cond = (
        (max(abs(gate_ic60), abs(gate_ic120)) > 0.05)
        and gate_spread60 > 0.05
        and (gate_ir if not np.isnan(gate_ir) else -99) > 0
        and oos_all_positive
    )
    B_cond_spread = gate_spread60 > 0.02
    if A_cond:
        verdict = "A"
    elif B_cond_spread and not oos_all_positive:
        verdict = "B"
    elif gate_spread60 < 0:
        verdict = "D"
    else:
        verdict = "B"

    lines.append(f"**Verdict: {verdict} Á¥?*")
    lines.append("")
    lines.append("## TL;DR")
    lines.append("")
    lines.append(f"- Gate A ?éÊøæÂæåË???{n_passed} / {n_total} ({n_passed/n_total:.1%} ?öÈ?)")
    lines.append(f"- 60d Gate A: IC {gate_ic60:+.4f} | spread {gate_spread60:+.2%} | "
                 f"Sharpe(trade-level) {summary_gate[60]['sharpe']:+.2f} | "
                 f"IR vs 0050 {gate_ir:+.2f}")
    lines.append(f"- Walk-forward OOS 60d Sharpe: " +
                 " / ".join([f"{y['year']}={y['sharpe']:+.2f}(n={y['n_sig']})" for y in yearly_gate if not np.isnan(y['sharpe'])]))
    lines.append(f"- OOS ‰∏âÊÆµÔº?024/25/26ÔºâSharpe ?ØÂê¶‰∏Ä??> 0Ôºö{oos_all_positive}")
    lines.append("")

    # Gate definition
    lines.append("## Gate A ÂÆöÁæ©ÔºàPIT-safeÔº?)
    lines.append("")
    lines.append("- TWA(^TWII) Close >= MA200Ôºå‰? MA60 Ëø?20d ?úÁ? > 0")
    lines.append("- gate_a ??t-1 ?∂Áõ§Ë≥áÊ??§Êñ∑ t ?•Ë??üÔ?pandas .shift(1)Ôº?)
    lines.append(f"- Gate A ?®Ê??öÈ??áÔ?{n_passed/n_total:.1%}Ôºàgate=True ??TWII ??{(n_passed/n_total):.0%}Ôº?)
    lines.append("")

    # IC table
    lines.append("## Table 1: IC by horizon ??no gate vs Gate A")
    lines.append("")
    lines.append("| Horizon | no-gate IC | no-gate t | Gate A IC | Gate A t | Gate A n_days |")
    lines.append("|---|---|---|---|---|---|")
    for r in ic_results:
        ng = r['no_gate']; g = r['gate_a']
        lines.append(f"| {r['horizon']}d | {ng['mean_ic']:+.4f} | {ng['t_stat']:+.2f} | "
                     f"{g['mean_ic']:+.4f} | {g['t_stat']:+.2f} | {g['n_days']} |")
    lines.append("")

    # Binary spread table
    lines.append("## Table 2: Binary spread ??no gate vs Gate A")
    lines.append("")
    lines.append("| Horizon | no-gate n_sig | no-gate spread | t | Gate A n_sig | Gate A spread | t |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in ic_results:
        ng = r['no_gate']; g = r['gate_a']
        lines.append(f"| {r['horizon']}d | {ng.get('n_sig','-')} | {ng.get('spread',0):+.2%} | "
                     f"{ng.get('t_spread',0):+.2f} | {g.get('n_sig','-')} | "
                     f"{g.get('spread',0):+.2%} | {g.get('t_spread',0):+.2f} |")
    lines.append("")

    # Single-arm trade-level
    lines.append("## Table 3: Signal-only trade-level stats (per hold horizon)")
    lines.append("")
    lines.append("| Hold | label | n_sig | mean ret | median | hit% | Sharpe(ann) | 0050 mean | IR |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for h in [20, 60, 120]:
        for lab, src in [('no-gate', summary_nogate), ('Gate A', summary_gate)]:
            s = src[h]
            lines.append(f"| {h}d | {lab} | {s['n_sig']} | {s['mean_ret']:+.2%} | "
                         f"{s['median_ret']:+.2%} | {s['hit_rate']:.1%} | "
                         f"{s['sharpe']:+.2f} | {s['bench_mean']:+.2%} | {s['ir']:+.2f} |")
    lines.append("")

    # Walk-forward year-by-year
    lines.append("## Table 4: Walk-forward OOS (year-by-year, 60d hold)")
    lines.append("")
    lines.append("Gate ?ØÂõ∫ÂÆöË??áÁÑ°?™Áî±?ÉÊï∏ ??ÊØèÂπ¥?∏Â???OOSÔºà‰??ÉË¢´?™‰?Âπ¥Ë??ôÊ±°?ìÔ?")
    lines.append("")
    lines.append("| Year | no-gate n | no-gate mean | hit% | Sharpe | Gate A n | Gate A mean | hit% | Sharpe | ? Sharpe |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    years = sorted({y['year'] for y in yearly_nogate} | {y['year'] for y in yearly_gate})
    for y in years:
        ng = next((d for d in yearly_nogate if d['year'] == y), None)
        g = next((d for d in yearly_gate if d['year'] == y), None)
        ng_str = (f"{ng['n_sig']} | {ng['mean_ret']:+.2%} | {ng['hit_rate']:.0%} | {ng['sharpe']:+.2f}"
                  if ng else "0 | - | - | -")
        g_str = (f"{g['n_sig']} | {g['mean_ret']:+.2%} | {g['hit_rate']:.0%} | {g['sharpe']:+.2f}"
                 if g and g['n_sig'] > 0 else "0 | - | - | -")
        delta = (g['sharpe'] - ng['sharpe']) if (ng and g and not np.isnan(ng['sharpe']) and not np.isnan(g['sharpe'])) else float('nan')
        delta_str = f"{delta:+.2f}" if not np.isnan(delta) else "-"
        lines.append(f"| {y} | {ng_str} | {g_str} | {delta_str} |")
    lines.append("")

    # Gate filter per year
    lines.append("## Table 5: Gate filter rate per year")
    lines.append("")
    lines.append("| Year | n_total | n_passed | % passed |")
    lines.append("|---|---|---|---|")
    for y, row in gate_yr.iterrows():
        lines.append(f"| {y} | {int(row['n_total'])} | {int(row['n_passed'])} | "
                     f"{row['pct_passed']:.1%} |")
    lines.append("")

    # Diagnostic
    lines.append("## Diagnostic ??gate ?ØÁ??âÁî®?ÑÊòØ‰∫ãÂ???2024")
    lines.append("")
    # If 2024 cut is very high (>80%) and 2024 Sharpe was negative ??cherry-pick suspicion
    y2024 = next((y for y, r in gate_yr.iterrows() if y == 2024), None)
    if y2024 is not None:
        cut_2024 = 1 - gate_yr.loc[2024, 'pct_passed']
        lines.append(f"- 2024 Ë¢´Á? **{cut_2024:.0%}**Ôºàpassed={gate_yr.loc[2024,'pct_passed']:.0%}Ôº?)
        if cut_2024 > 0.8:
            lines.append("  - ??>80% Â±¨Êñº retroactive cherry-pick Â´åÁ?ÔºåÈ?Ê™¢Êü• 2025/2026 ?ØÂê¶‰πüË??óË¢´?çËÄå‰?Ê≠?†±??)
    cuts = [(y, 1 - gate_yr.loc[y, 'pct_passed']) for y in gate_yr.index]
    lines.append(f"- ?ÑÂπ¥?çÈô§?áÔ?" + " | ".join([f"{y}={c:.0%}" for y, c in cuts]))

    # OOS consistency check
    lines.append("")
    lines.append("## Walk-forward OOS ‰∏Ä?¥ÊÄßÂà§ËÆÄ")
    lines.append("")
    if oos_all_positive:
        lines.append("- ‰∏âÊÆµÂπ¥Â∫¶ OOS Sharpe ??> 0 ??**gate Ë∑®Âπ¥Â∫¶Á©©ÂÆ?*ÔºåÈ?‰∫ãÂ???2024")
    else:
        bad = [y for y in yearly_gate if not np.isnan(y['sharpe']) and y['sharpe'] <= 0]
        lines.append(f"- ?≥Â???{len(bad)} ?ãÂπ¥Â∫?Sharpe ??0Ôº? +
                     " / ".join([f"{y['year']}={y['sharpe']:+.2f}" for y in bad]))
        lines.append("  - gate Ê≤íÊ?Ë∑®Âπ¥Â∫¶Á©©ÂÆ???**B ??D Á¥?*")
    lines.append("")

    # Verdict
    lines.append("## Verdict ?®Â?")
    lines.append("")
    lines.append(f"- A Á¥öÊ?‰ª∂Ô?60d/120d IC > +0.05 ({max(abs(gate_ic60), abs(gate_ic120)):+.4f}), "
                 f"60d spread > +5% ({gate_spread60:+.2%}), "
                 f"IR > 0 ({gate_ir:+.2f}), ‰∏âÊÆµ OOS Sharpe > 0 ({oos_all_positive})")
    lines.append(f"- ÁµêË?Ôº?*{verdict} Á¥?*")
    lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"Wrote {OUT_MD}")
    print(f"\n===== VERDICT: {verdict} =====\n")


if __name__ == "__main__":
    main()
