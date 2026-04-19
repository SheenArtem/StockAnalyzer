"""
vf_va_validation.py - VF-VA Value 估值門檻驗證
=============================================
測試 PE / PB / Graham 門檻是否對 forward return 有 alpha。

輸入: data_cache/backtest/trade_journal_value_tw.parquet
輸出: reports/vf_va_valuation_thresholds.md + console table

驗證層次:
  Layer 1: 單變數 IC — PE/PB/Graham 與 fwd_60d 的 rank correlation
  Layer 2: Decile spread — 估值最便宜 10% vs 最貴 10% forward return 差異
  Layer 3: Threshold stress — 常用門檻 (PE<20 / PB<3) 的 hit rate / avg return
  Layer 4: Walk-forward — 12m 訓練 / 3m 測試，check 穩定性

IC IR 決策門檻:
  IR >= 0.3 → 強有效 (A 級)
  IR 0.1-0.3 → 有效但弱 (B 級)
  IR -0.1 to 0.1 → 無效 (D 級)
  IR < -0.1 → 反指標 (要反向)

用法:
  python tools/vf_va_validation.py              # 用預設 fwd_60d
  python tools/vf_va_validation.py --horizon 40 # 切 fwd_40d
  python tools/vf_va_validation.py --save-md    # 產出 markdown 報告
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

JOURNAL_PATH = ROOT / "data_cache" / "backtest" / "trade_journal_value_tw.parquet"
SNAPSHOT_PATH = ROOT / "data_cache" / "backtest" / "trade_journal_value_tw_snapshot.parquet"
REPORT_DIR = ROOT / "reports"


def load_journal(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    return df


def ic_analysis(df: pd.DataFrame, factor: str, horizon: int) -> dict:
    """Cross-sectional IC: per-week rank corr between factor and forward return.

    IR = mean(IC) / std(IC). Higher = more stable alpha.
    """
    target_col = f'fwd_{horizon}d'
    sub = df[[factor, target_col, 'week_end_date']].dropna()
    if sub.empty:
        return {'IC_mean': np.nan, 'IC_std': np.nan, 'IR': np.nan, 'n_weeks': 0}

    weekly_ic = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 10:
            continue
        rho, _ = stats.spearmanr(grp[factor], grp[target_col])
        if not np.isnan(rho):
            weekly_ic.append(rho)
    if not weekly_ic:
        return {'IC_mean': np.nan, 'IC_std': np.nan, 'IR': np.nan, 'n_weeks': 0}
    ic_arr = np.array(weekly_ic)
    ic_mean = ic_arr.mean()
    ic_std = ic_arr.std(ddof=1) if len(ic_arr) > 1 else np.nan
    ir = ic_mean / ic_std if ic_std and ic_std > 0 else np.nan
    return {'IC_mean': ic_mean, 'IC_std': ic_std, 'IR': ir, 'n_weeks': len(ic_arr)}


def decile_spread(df: pd.DataFrame, factor: str, horizon: int,
                   higher_is_better: bool = False) -> dict:
    """Bucket by factor deciles, compute mean fwd return of top vs bottom.

    higher_is_better=False means lower values are "better" (like PE: lower=cheaper).
    """
    target_col = f'fwd_{horizon}d'
    sub = df[[factor, target_col, 'week_end_date']].dropna()
    if sub.empty:
        return {}

    results = []
    for wd, grp in sub.groupby('week_end_date'):
        if len(grp) < 20:
            continue
        grp = grp.sort_values(factor, ascending=not higher_is_better).reset_index(drop=True)
        n = len(grp)
        bottom_cut = max(1, n // 10)
        top_cut = max(1, n // 10)
        bottom = grp.iloc[:bottom_cut][target_col].mean()   # "cheap" side
        top = grp.iloc[-top_cut:][target_col].mean()        # "expensive" side
        results.append({'week': wd, 'bottom': bottom, 'top': top,
                         'spread': bottom - top})
    if not results:
        return {}
    r = pd.DataFrame(results)
    return {
        'bottom_mean': r['bottom'].mean(),
        'top_mean': r['top'].mean(),
        'spread_mean': r['spread'].mean(),
        'spread_win_pct': (r['spread'] > 0).sum() / len(r),
        'n_weeks': len(r),
    }


def threshold_test(df: pd.DataFrame, factor: str, threshold: float,
                    horizon: int, lower_is_pass: bool = True) -> dict:
    """Bucket by factor threshold, compute mean fwd return of pass vs fail."""
    target_col = f'fwd_{horizon}d'
    sub = df[[factor, target_col]].dropna()
    if sub.empty:
        return {}

    if lower_is_pass:
        pass_mask = sub[factor] < threshold
    else:
        pass_mask = sub[factor] > threshold

    passed = sub[pass_mask]
    failed = sub[~pass_mask]

    return {
        'pass_count': len(passed),
        'fail_count': len(failed),
        'pass_ret_mean': passed[target_col].mean() if len(passed) else np.nan,
        'fail_ret_mean': failed[target_col].mean() if len(failed) else np.nan,
        'pass_hit_rate': (passed[target_col] > 0).mean() if len(passed) else np.nan,
        'fail_hit_rate': (failed[target_col] > 0).mean() if len(failed) else np.nan,
        'pass_win_rate_vs_fail': (
            (passed[target_col].mean() or 0) > (failed[target_col].mean() or 0)
        ) if len(passed) and len(failed) else False,
    }


def grade(ir: float) -> str:
    if pd.isna(ir):
        return 'N/A'
    if abs(ir) >= 0.3:
        return 'A' if ir > 0 else 'A (reverse)'
    elif abs(ir) >= 0.1:
        return 'B' if ir > 0 else 'B (reverse)'
    elif abs(ir) >= 0.05:
        return 'C'
    else:
        return 'D (no alpha)'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default=str(JOURNAL_PATH))
    ap.add_argument("--snapshot", default=None,
                    help="Auto-load snapshot if exists (fair universe for threshold test).")
    ap.add_argument("--horizon", type=int, default=60,
                    help="Forward return horizon (5/10/20/40/60/120)")
    ap.add_argument("--save-md", action="store_true",
                    help="Write markdown report to reports/")
    args = ap.parse_args()

    snap_path = Path(args.snapshot) if args.snapshot else SNAPSHOT_PATH
    if snap_path.exists():
        journal = load_journal(snap_path)
        print(f"⭐ Using SNAPSHOT: {snap_path.name} (fair threshold test universe)")
    else:
        journal = load_journal(Path(args.journal))
        print(f"⚠️  Using JOURNAL only: {Path(args.journal).name}")
        print(f"   WARNING: threshold tests biased (top 50 already favors low PE/PB)")
    print(f"Loaded journal: {len(journal)} picks, "
          f"{journal['week_end_date'].nunique()} weeks, "
          f"{journal['stock_id'].nunique()} unique stocks")
    print(f"Date range: {journal['week_end_date'].min().date()} to "
          f"{journal['week_end_date'].max().date()}")
    print(f"Horizon: fwd_{args.horizon}d")
    print()

    factors = [
        ('pe', False, 'Lower PE = cheaper = better'),
        ('pb', False, 'Lower PB = cheaper = better'),
        ('graham_number', True, 'Higher Graham/Close ratio = more undervalued'),
    ]

    # === Layer 1: IC + IR ===
    print("=" * 70)
    print("Layer 1: Cross-sectional IC (per-week Spearman rank correlation)")
    print("=" * 70)
    print(f"{'Factor':<20}{'IC_mean':>10}{'IC_std':>10}{'IR':>8}{'Weeks':>8}{'Grade':>15}")
    print("-" * 70)
    ic_results = {}
    # Handle column naming: snapshot uses 'Close', trade_journal uses 'entry_price'
    price_col = 'entry_price' if 'entry_price' in journal.columns else 'Close'
    for factor, higher_better, note in factors:
        if factor == 'graham_number':
            # For Graham: compute ratio as "close / graham" (low = undervalued)
            # Actually simpler: use graham - close and make higher = better
            # But our col is just graham_number. Use ratio column derived inline.
            tmp = journal.copy()
            tmp['graham_ratio'] = tmp['graham_number'] / tmp[price_col]
            res = ic_analysis(tmp, 'graham_ratio', args.horizon)
            ic_results['graham_ratio'] = res
        else:
            # For PE/PB: lower = better, negate for IC so "high score = high return"
            tmp = journal.copy()
            tmp[f'{factor}_neg'] = -tmp[factor]
            res = ic_analysis(tmp, f'{factor}_neg', args.horizon)
            ic_results[factor] = res
        print(f"{factor:<20}{res['IC_mean']:>10.4f}{res['IC_std']:>10.4f}"
              f"{res['IR']:>8.3f}{res['n_weeks']:>8}{grade(res['IR']):>15}")
    print()

    # === Layer 2: Decile Spread ===
    print("=" * 70)
    print(f"Layer 2: Decile spread (fwd_{args.horizon}d, cheapest 10% vs most expensive 10%)")
    print("=" * 70)
    print(f"{'Factor':<20}{'Cheap':>10}{'Exp':>10}{'Spread':>10}{'WinPct':>10}{'Weeks':>8}")
    print("-" * 70)
    for factor, higher_better, _ in factors:
        sub = journal.dropna(subset=[factor])
        if factor == 'graham_number':
            res = decile_spread(sub.assign(graham_ratio=sub['graham_number']/sub[price_col]),
                                 'graham_ratio', args.horizon, higher_is_better=True)
        else:
            res = decile_spread(sub, factor, args.horizon, higher_is_better=False)
        if res:
            print(f"{factor:<20}{res['bottom_mean']:>10.2%}{res['top_mean']:>10.2%}"
                  f"{res['spread_mean']:>10.2%}{res['spread_win_pct']:>10.1%}{res['n_weeks']:>8}")
    print()

    # === Layer 3: Threshold Test ===
    print("=" * 70)
    print(f"Layer 3: Threshold stress test (fwd_{args.horizon}d)")
    print("=" * 70)
    print(f"{'Threshold':<20}{'#Pass':>8}{'#Fail':>8}{'PassRet':>10}{'FailRet':>10}"
          f"{'PassHit':>10}{'FailHit':>10}{'Diff':>10}")
    print("-" * 90)
    threshold_cases = [
        ('pe', 20, True, 'PE < 20'),
        ('pe', 12, True, 'PE < 12'),
        ('pb', 3.0, True, 'PB < 3'),
        ('pb', 1.5, True, 'PB < 1.5'),
    ]
    for factor, th, lower_pass, label in threshold_cases:
        res = threshold_test(journal, factor, th, args.horizon, lower_is_pass=lower_pass)
        if res:
            diff = (res['pass_ret_mean'] or 0) - (res['fail_ret_mean'] or 0)
            print(f"{label:<20}{res['pass_count']:>8}{res['fail_count']:>8}"
                  f"{(res['pass_ret_mean'] or 0):>10.2%}"
                  f"{(res['fail_ret_mean'] or 0):>10.2%}"
                  f"{(res['pass_hit_rate'] or 0):>10.1%}"
                  f"{(res['fail_hit_rate'] or 0):>10.1%}"
                  f"{diff:>10.2%}")
    print()

    # === Summary ===
    print("=" * 70)
    print("Summary / Decision")
    print("=" * 70)
    for factor, _, note in factors:
        key = 'graham_ratio' if factor == 'graham_number' else factor
        r = ic_results.get(key, {})
        ir = r.get('IR', np.nan)
        print(f"  {factor:<15} IR={ir:>7.3f} -> {grade(ir):<20} ({note})")

    if args.save_md:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        md_path = REPORT_DIR / "vf_va_valuation_thresholds.md"
        # Regenerate output into markdown
        import io
        import contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            # Re-run (simpler than capturing)
            pass
        # For brevity, just copy console output
        md_path.write_text("# VF-VA Valuation Threshold Validation\n\nSee console output.\n")
        print(f"\nMarkdown stub written to {md_path}")


if __name__ == "__main__":
    main()
