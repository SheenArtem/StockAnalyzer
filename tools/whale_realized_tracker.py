"""
Whale Picks — Realized (live) vs Backtest baseline 月度對照追蹤器
=================================================================

目的 (2026-06-09): 讓 Whale Picks **live 實單**績效能逐月對照 backtest baseline
(`data/whale_picks/trade_ledger_meta.json`: win 0.514 / avg +0.0311 / K=10 / M15 rebal)，
連續低於就 degradation flag。

⚠️ **誠實前提 (讀這段再用)**：
- Whale Picks **真正的 OOS 單位是 M15 「持有組合」**（每月 15 號 last-weekday rebalance，
  K=10 等權持有到下次 M15），不是每日 snapshot 的 would-be top-10。
  每日 dated snapshot (`data/whale_picks/{date}.parquet`) 雖然是 live point-in-time 存的，
  但它是「今天若 rebal 會選誰」的 ranking，**不是實際持倉**；持倉只在 M15 換。
  拿每日 top-10 當進場 = 製造策略根本沒有的換手率，會嚴重失真。
- 因此 live OOS = `_active_holdings.json` 每個 M15 cohort 的 entry_close（rebal 當天 live 存的）
  → forward 報酬到下次 M15（完整週期）或到最新收盤（in-flight）。
- **策略 2026-05-22 才切 M15，第一個（也是目前唯一）live M15 cohort = 2026-05-15**。
  在那之前 ledger 全是 backtest 重構，**無 live OOS 價值**。
  ⇒ 真 live OOS 史 **從 2026-05-15 起算，目前 N=1 cohort 且在飛行中**（下次 rebal 2026-06-15）。
  本工具設計成「從現在開始逐月累積」，不假裝歷史有 OOS。

本工具三件事：
  (1) **快照累積**：每跑一次，把當前 `_active_holdings.json`（real held cohort）+ entry 價
      寫進累積檔 `data/whale_picks/realized_tracking.parquet`，以 (rebalance_date, stock_id)
      為 key，idempotent（同 cohort 重跑只更新，不重複）。新 M15 cohort 出現自動納入。
  (2) **realized 報酬計算**：對每個 cohort，用 clean `ohlcv_tw`（剔 V=0 凍結列）算實際 fwd 報酬。
      下個 cohort 存在 → 前 cohort 視為「完整週期」（exit = 下個 cohort rebal 日）；
      否則 mark-to-latest（in-flight）。
  (3) **月度對照報告**：per-cohort + aggregate realized win/avg/Sharpe vs baseline
      51.4% / +3.11%；trailing-N cohort 顯著低於 baseline → degradation flag。

PIT 嚴格：cohort 只用 ≤ rebal 日資料（entry_close 是 rebal 當天 live 存的）；
fwd 報酬用 clean ohlcv_tw 剔 V=0；fail loud（缺 snapshot / 缺價 / NaN → WARN 不靜默）。

月度 SOP 接法（**不要自動排程** — 與 100% Whale SOP 對齊，手動 M15 換倉時順手跑）：
  每月 M15 換倉 (`whale_picks_alerts.py --update-holdings` 跑完更新 _active_holdings.json) 後，
  手動跑一次本工具 → 快照當月 cohort + 印對照報告。
  詳見檔末 "MONTHLY SOP" 區塊。

Usage:
    python tools/whale_realized_tracker.py                 # 快照當前 cohort + 印報告
    python tools/whale_realized_tracker.py --report-only   # 只算/印，不寫快照
    python tools/whale_realized_tracker.py --asof 2026-06-08  # 指定 MTM 截止日
    python tools/whale_realized_tracker.py --trailing 3    # degradation flag 看 trailing 3 cohort
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("whale_realized_tracker")

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WHALE_DIR = REPO / "data" / "whale_picks"
HOLDINGS_PATH = WHALE_DIR / "_active_holdings.json"
META_PATH = WHALE_DIR / "trade_ledger_meta.json"
TRACKING_PATH = WHALE_DIR / "realized_tracking.parquet"
OHLCV_PATH = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"

# Backtest baseline (read from meta, hard fallback if absent)
BASELINE_WIN = 0.514
BASELINE_AVG = 0.0311
DEFAULT_TRAILING = 3  # trailing N cohorts for degradation flag


# =============================================================================
# Load helpers (fail loud)
# =============================================================================

def load_baseline() -> Tuple[float, float]:
    """Read win/avg baseline from trade_ledger_meta.json; fall back to constants."""
    if not META_PATH.exists():
        log.warning("baseline meta not found at %s — using hardcoded win=%.3f avg=%.4f",
                    META_PATH, BASELINE_WIN, BASELINE_AVG)
        return BASELINE_WIN, BASELINE_AVG
    try:
        m = json.loads(META_PATH.read_text(encoding='utf-8'))
        win = float(m.get('win_rate', BASELINE_WIN))
        avg = float(m.get('avg_pnl_pct', BASELINE_AVG))
        log.info("Baseline from meta: win=%.4f avg=%+.4f (n_positions=%s)",
                 win, avg, m.get('n_positions'))
        return win, avg
    except Exception as e:
        log.warning("baseline meta parse failed (%s) — using hardcoded", e)
        return BASELINE_WIN, BASELINE_AVG


def load_current_cohort() -> Optional[Dict]:
    """Read the live held M15 cohort from _active_holdings.json. Fail loud if missing."""
    if not HOLDINGS_PATH.exists():
        log.error("FAIL: _active_holdings.json not found at %s — run "
                  "whale_picks_alerts.py first to materialize the held cohort", HOLDINGS_PATH)
        return None
    try:
        h = json.loads(HOLDINGS_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        log.error("FAIL: _active_holdings.json parse error: %s", e)
        return None
    if not h.get('tickers'):
        log.error("FAIL: _active_holdings.json has no tickers")
        return None
    if not h.get('rebalance_date'):
        log.error("FAIL: _active_holdings.json missing rebalance_date")
        return None
    return h


def load_clean_ohlcv() -> pd.DataFrame:
    """Load ohlcv_tw, drop V=0 frozen rows (PIT: 停牌參考價填充列不算成交)."""
    if not OHLCV_PATH.exists():
        log.error("FAIL: clean ohlcv_tw not found at %s", OHLCV_PATH)
        raise SystemExit(1)
    df = pd.read_parquet(OHLCV_PATH, columns=['stock_id', 'date', 'Close', 'Volume'])
    df['stock_id'] = df['stock_id'].astype(str)
    n_before = len(df)
    # PIT: 剔 V=0 凍結列 + NaN/<=0 收盤 (yfinance NaN-close corruption 防呆)
    df = df[(df['Volume'] > 0) & (df['Close'] > 0) & df['Close'].notna()].copy()
    log.info("Clean ohlcv_tw: %d rows (dropped %d frozen/invalid; latest=%s)",
             len(df), n_before - len(df), df['date'].max().date())
    return df


# =============================================================================
# Step 1: snapshot current cohort into accumulating tracking file
# =============================================================================

def snapshot_cohort(cohort: Dict) -> pd.DataFrame:
    """Upsert the current held cohort into realized_tracking.parquet.

    Key = (rebalance_date, stock_id). Idempotent: re-running the same cohort
    updates rows in place rather than duplicating. New M15 cohorts append.
    Includes both system tickers and any alert_adds (entry_type tagged).
    """
    reb = cohort['rebalance_date']
    rows = []
    for t in cohort.get('tickers', []):
        rows.append({
            'rebalance_date': reb,
            'stock_id': str(t['stock_id']),
            'stock_name': t.get('stock_name', ''),
            'industry': t.get('industry', ''),
            'entry_close': float(t['entry_close']),
            'entry_type': t.get('entry_type', 'system'),
            'snapshot_recorded_at': datetime.now().isoformat(timespec='seconds'),
        })
    for a in cohort.get('alert_adds', []) or []:
        rows.append({
            'rebalance_date': a.get('entry_date', reb),
            'stock_id': str(a['stock_id']),
            'stock_name': a.get('stock_name', ''),
            'industry': a.get('industry', ''),
            'entry_close': float(a['entry_close']),
            'entry_type': a.get('entry_type', 'alert'),
            'snapshot_recorded_at': datetime.now().isoformat(timespec='seconds'),
        })
    new = pd.DataFrame(rows)
    if new.empty:
        log.warning("cohort produced no rows to snapshot")
        return new

    if TRACKING_PATH.exists():
        old = pd.read_parquet(TRACKING_PATH)
        old['stock_id'] = old['stock_id'].astype(str)
        # Drop rows for the cohorts we're re-writing (upsert by rebalance_date+stock_id)
        key_new = set(zip(new['rebalance_date'], new['stock_id']))
        mask_keep = ~old.apply(lambda r: (r['rebalance_date'], str(r['stock_id'])) in key_new, axis=1)
        merged = pd.concat([old[mask_keep], new], ignore_index=True)
        n_added = len(merged) - mask_keep.sum()
        log.info("Tracking upsert: %d existing kept + %d cohort rows written (cohort %s)",
                 int(mask_keep.sum()), len(new), reb)
    else:
        merged = new
        log.info("Tracking file created with %d rows (cohort %s)", len(new), reb)

    merged = merged.sort_values(['rebalance_date', 'stock_id']).reset_index(drop=True)
    merged.to_parquet(TRACKING_PATH, index=False)
    log.info("Saved %s (%d total rows, %d cohorts)",
             TRACKING_PATH, len(merged), merged['rebalance_date'].nunique())
    return merged


# =============================================================================
# Step 2: compute realized forward returns per cohort (PIT, clean ohlcv)
# =============================================================================

def _next_rebal_dates(cohorts: List[str]) -> Dict[str, Optional[str]]:
    """Map each cohort rebalance_date -> the NEXT cohort's rebalance_date (or None if latest)."""
    s = sorted(cohorts)
    out = {}
    for i, c in enumerate(s):
        out[c] = s[i + 1] if i + 1 < len(s) else None
    return out


def compute_realized(tracking: pd.DataFrame, ohlcv: pd.DataFrame,
                     asof: Optional[date] = None) -> pd.DataFrame:
    """For each (cohort, stock) compute realized return.

    Closed cohort (a later cohort exists): exit = last clean close strictly BEFORE
      next cohort's rebal date (full M15 cycle).
    In-flight cohort (latest): mark-to-latest clean close <= asof.

    Fails loud per-stock: missing price -> WARN, ret=NaN (not silently 0).
    """
    asof_ts = pd.Timestamp(asof) if asof else ohlcv['date'].max()
    cohorts = tracking['rebalance_date'].unique().tolist()
    nxt = _next_rebal_dates(cohorts)

    # Pre-index ohlcv per stock for speed
    grp = {sid: g.sort_values('date') for sid, g in ohlcv.groupby('stock_id')}

    recs = []
    for _, r in tracking.iterrows():
        sid = str(r['stock_id'])
        reb = pd.Timestamp(r['rebalance_date'])
        entry = float(r['entry_close'])
        nxt_reb = nxt.get(r['rebalance_date'])
        is_inflight = nxt_reb is None

        if is_inflight:
            window_end = asof_ts
        else:
            # full cycle: exit at last close strictly before next rebal
            window_end = pd.Timestamp(nxt_reb) - pd.Timedelta(days=1)
            window_end = min(window_end, asof_ts)

        g = grp.get(sid)
        if g is None or len(g) == 0:
            log.warning("WARN no clean ohlcv for %s (cohort %s) — realized=NaN",
                        sid, r['rebalance_date'])
            exit_close, exit_date, ret = np.nan, pd.NaT, np.nan
        else:
            # last clean close in (reb, window_end]; if entry day itself missing
            # we still mark from whatever close exists in window
            w = g[(g['date'] >= reb) & (g['date'] <= window_end)]
            if len(w) == 0:
                log.warning("WARN %s (cohort %s): no clean trading day in (%s, %s] — realized=NaN",
                            sid, r['rebalance_date'], reb.date(), window_end.date())
                exit_close, exit_date, ret = np.nan, pd.NaT, np.nan
            else:
                last = w.iloc[-1]
                exit_close = float(last['Close'])
                exit_date = last['date']
                ret = exit_close / entry - 1.0 if entry > 0 else np.nan
                if entry <= 0:
                    log.warning("WARN %s (cohort %s): entry_close <=0 (%s) — realized=NaN",
                                sid, r['rebalance_date'], entry)

        recs.append({
            'rebalance_date': r['rebalance_date'],
            'stock_id': sid,
            'stock_name': r['stock_name'],
            'entry_type': r.get('entry_type', 'system'),
            'entry_close': entry,
            'exit_close': exit_close,
            'exit_date': exit_date,
            'realized_ret': ret,
            'status': 'in-flight' if is_inflight else 'closed',
            'hold_days': (exit_date - reb).days if pd.notna(exit_date) else np.nan,
        })
    return pd.DataFrame(recs)


# =============================================================================
# Step 3: monthly comparison report + degradation flag
# =============================================================================

def _sharpe_of_returns(rets: pd.Series) -> float:
    """Naive cross-sectional Sharpe of a cohort's per-stock returns (mean/std).

    NOT annualized — it's a dispersion-adjusted hit quality within the cohort.
    With N<2 or zero std, returns NaN.
    """
    r = rets.dropna()
    if len(r) < 2 or r.std(ddof=1) == 0:
        return np.nan
    return float(r.mean() / r.std(ddof=1))


def build_report(realized: pd.DataFrame, baseline_win: float, baseline_avg: float,
                 trailing: int = DEFAULT_TRAILING) -> Dict:
    """Per-cohort + aggregate realized stats vs baseline + degradation flag."""
    valid = realized.dropna(subset=['realized_ret']).copy()

    # Per-cohort
    per = []
    for reb, g in valid.groupby('rebalance_date'):
        rets = g['realized_ret']
        per.append({
            'rebalance_date': reb,
            'n': len(g),
            'status': g['status'].iloc[0],
            'win': float((rets > 0).mean()),
            'avg': float(rets.mean()),
            'median': float(rets.median()),
            'sharpe_xs': _sharpe_of_returns(rets),
            'win_vs_base_pp': float((rets > 0).mean() - baseline_win) * 100,
            'avg_vs_base_pp': float(rets.mean() - baseline_avg) * 100,
        })
    per_df = pd.DataFrame(per).sort_values('rebalance_date').reset_index(drop=True)

    # Aggregate (all live cohorts pooled, position-level)
    agg = {
        'n_cohorts': int(valid['rebalance_date'].nunique()),
        'n_positions': int(len(valid)),
        'win': float((valid['realized_ret'] > 0).mean()),
        'avg': float(valid['realized_ret'].mean()),
        'median': float(valid['realized_ret'].median()),
        'sharpe_xs': _sharpe_of_returns(valid['realized_ret']),
    }

    # Degradation flag: trailing N cohorts BOTH win and avg below baseline
    flag = False
    flag_reason = ''
    if len(per_df) >= trailing and trailing >= 1:
        tail = per_df.tail(trailing)
        win_below = (tail['win'] < baseline_win).all()
        avg_below = (tail['avg'] < baseline_avg).all()
        if win_below and avg_below:
            flag = True
            flag_reason = (f"trailing {trailing} cohorts ALL below baseline "
                           f"(win<{baseline_win:.3f} AND avg<{baseline_avg:+.4f})")
        elif win_below:
            flag_reason = f"trailing {trailing} cohorts win<baseline (avg ok)"
        elif avg_below:
            flag_reason = f"trailing {trailing} cohorts avg<baseline (win ok)"
        else:
            flag_reason = "no degradation (trailing cohorts at/above baseline)"
    else:
        flag_reason = (f"insufficient live cohorts ({len(per_df)} < trailing {trailing}) "
                       f"-- degradation flag NOT yet evaluable")

    return {
        'baseline': {'win': baseline_win, 'avg': baseline_avg},
        'per_cohort': per_df,
        'aggregate': agg,
        'degradation_flag': flag,
        'degradation_reason': flag_reason,
        'trailing': trailing,
    }


def print_report(report: Dict, realized: pd.DataFrame) -> None:
    base = report['baseline']
    print("\n" + "=" * 78)
    print("WHALE PICKS -- REALIZED (LIVE) vs BACKTEST BASELINE")
    print("=" * 78)
    print(f"Backtest baseline: win={base['win']:.3f}  avg={base['avg']:+.4f}  (K=10 / M15)")
    print()
    print("[!] LIVE OOS HISTORY STARTS 2026-05-15 (first M15 cohort after 2026-05-22 switch).")
    print("    Daily snapshot = would-be ranking, NOT held book; real OOS = M15 held cohort,")
    print("    accumulated month by month from now on.")
    print()

    per = report['per_cohort']
    if per.empty:
        print("(no live cohorts with valid realized returns yet)")
    else:
        print("PER-COHORT (M15 held portfolio):")
        print(f"  {'rebal':<12}{'N':>3}{'status':>11}{'win':>8}{'avg':>9}{'med':>9}"
              f"{'shrp':>7}{'win_vs_b':>10}{'avg_vs_b':>10}")
        for _, r in per.iterrows():
            shrp = f"{r['sharpe_xs']:.2f}" if pd.notna(r['sharpe_xs']) else "n/a"
            print(f"  {r['rebalance_date']:<12}{int(r['n']):>3}{r['status']:>11}"
                  f"{r['win']:>8.3f}{r['avg']:>+9.4f}{r['median']:>+9.4f}"
                  f"{shrp:>7}{r['win_vs_base_pp']:>+9.1f}p{r['avg_vs_base_pp']:>+9.1f}p")

    agg = report['aggregate']
    print()
    print("AGGREGATE (all live cohorts pooled, position-level):")
    shrp = f"{agg['sharpe_xs']:.3f}" if pd.notna(agg['sharpe_xs']) else "n/a"
    print(f"  cohorts={agg['n_cohorts']}  positions={agg['n_positions']}  "
          f"win={agg['win']:.3f}  avg={agg['avg']:+.4f}  median={agg['median']:+.4f}  "
          f"sharpe_xs={shrp}")
    print(f"  vs baseline: win {agg['win'] - base['win']:+.3f}  avg {agg['avg'] - base['avg']:+.4f}")

    print()
    print(f"DEGRADATION FLAG (trailing {report['trailing']} cohorts): "
          f"{'[RAISED]' if report['degradation_flag'] else '[clear]'}")
    print(f"  reason: {report['degradation_reason']}")

    # Per-position detail for in-flight cohort (most actionable)
    inflight = realized[realized['status'] == 'in-flight'].dropna(subset=['realized_ret'])
    if not inflight.empty:
        print()
        print(f"IN-FLIGHT cohort detail ({inflight['rebalance_date'].iloc[0]}, "
              f"mark-to-latest):")
        inf = inflight.sort_values('realized_ret', ascending=False)
        for _, r in inf.iterrows():
            print(f"  {r['stock_id']:>5} {r['stock_name']:<8} "
                  f"entry={r['entry_close']:>7.1f} exit={r['exit_close']:>7.1f} "
                  f"ret={r['realized_ret']:>+7.2%} ({int(r['hold_days'])}d) [{r['entry_type']}]")
    print("=" * 78 + "\n")


# =============================================================================
# Main
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="Whale Picks realized vs backtest tracker")
    p.add_argument('--report-only', action='store_true',
                   help="Only compute/print report; do NOT snapshot current cohort")
    p.add_argument('--asof', default=None,
                   help="MTM cutoff date YYYY-MM-DD (default: latest clean ohlcv date)")
    p.add_argument('--trailing', type=int, default=DEFAULT_TRAILING,
                   help=f"Trailing N cohorts for degradation flag (default {DEFAULT_TRAILING})")
    args = p.parse_args()

    asof = date.fromisoformat(args.asof) if args.asof else None
    baseline_win, baseline_avg = load_baseline()

    # Step 1: snapshot current cohort (unless report-only)
    if not args.report_only:
        cohort = load_current_cohort()
        if cohort is None:
            log.error("Cannot snapshot — _active_holdings.json missing/invalid. "
                      "Run report on existing tracking file only? Use --report-only.")
            return 1
        log.info("Current live cohort: rebalance_date=%s, %d system tickers, %d alert_adds",
                 cohort['rebalance_date'], len(cohort.get('tickers', [])),
                 len(cohort.get('alert_adds', []) or []))
        snapshot_cohort(cohort)

    # Need tracking file to report
    if not TRACKING_PATH.exists():
        log.error("FAIL: no tracking file at %s and --report-only set — "
                  "run once without --report-only to seed it", TRACKING_PATH)
        return 1
    tracking = pd.read_parquet(TRACKING_PATH)
    tracking['stock_id'] = tracking['stock_id'].astype(str)

    # Step 2: realized returns from clean ohlcv
    ohlcv = load_clean_ohlcv()
    realized = compute_realized(tracking, ohlcv, asof=asof)

    n_nan = realized['realized_ret'].isna().sum()
    if n_nan > 0:
        log.warning("%d / %d positions have NaN realized (missing clean price) — "
                    "excluded from stats, see WARN above", n_nan, len(realized))

    # Step 3: report
    report = build_report(realized, baseline_win, baseline_avg, trailing=args.trailing)
    print_report(report, realized)

    log.info("Tracker run OK. Tracking file: %s (%d cohorts). %s",
             TRACKING_PATH, tracking['rebalance_date'].nunique(),
             "DEGRADATION FLAG RAISED" if report['degradation_flag'] else "no degradation flag")
    return 0


# =============================================================================
# MONTHLY SOP (手動，不自動排程 — 與 100% Whale SOP 對齊)
# =============================================================================
# 每月 M15 換倉日 (last weekday <= 15th)，照既有 Whale 換倉 SOP 跑完：
#   1. python tools/whale_picks_screener.py            # 產新 top-10
#   2. python tools/whale_picks_alerts.py --update-holdings   # 刷新 _active_holdings.json
#      (scanner.bat M15 day 會自動跑步驟 1-2；手動換倉時自己跑)
#   3. python tools/whale_realized_tracker.py          # <<< 本工具：快照新 cohort + 印對照
#      → 把當月 cohort 寫進 realized_tracking.parquet
#      → 印出 per-cohort + aggregate realized vs baseline + degradation flag
#   4. 若 degradation flag 🔴 RAISED (trailing 3 cohort 全低於 baseline) → 人工檢視策略漂移，
#      不自動停用（informational tier per SPEC §13）。
#
# 任何非 M15 日也可跑 --report-only 看 in-flight cohort 即時 MTM（不寫快照）。
# =============================================================================

if __name__ == '__main__':
    raise SystemExit(main())
