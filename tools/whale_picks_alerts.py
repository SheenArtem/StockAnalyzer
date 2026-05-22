"""
Whale Picks Alerts — early-entry / trailing stop / mid-month BUY 三層 overlay.

Runs daily after whale_picks_screener.py (in run_scanner.bat).

Three alert types (2026-05-16 後從原 2 種擴充為 3 種):

(A) Early-entry signal — rapid rank rise:
    A ticker whose composite_score rank moved from >100 (7d ago)
    to ≤30 (today) → 主力剛發動候選 → Discord push.

(B) Trailing stop / early exit:
    Active holdings (last M15 rebal's top-10) that dropped ≥ 15% from
    rebalance-day close → 不等下次 rebal 才出 → Discord push.

(C) Mid-rebal BUY 候選 (2bfacf9 新加):
    Rank by composite_score 在 (10, 20] + 5d return ≥ 15% + 不在當前 holdings
    → M15 rebal 漏抓的 mid-cycle 爆發股 (e.g., 2344 case) → Discord push.
    不自動進場、僅手動評估提醒。

Outputs:
- data/whale_picks/_active_holdings.json — refreshed on M15 rebal day
- Discord push only if any alerts triggered

Usage:
    python tools/whale_picks_alerts.py            # run alerts
    python tools/whale_picks_alerts.py --update-holdings  # force refresh holdings
    python tools/whale_picks_alerts.py --dry-run  # log to stdout, no Discord
"""
from __future__ import annotations

import argparse
import calendar
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("whale_picks_alerts")

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
SNAPSHOT_DIR = REPO / "data" / "whale_picks"
HOLDINGS_PATH = SNAPSHOT_DIR / "_active_holdings.json"

# Thresholds (tunable)
RANK_NOW_TOP = 30          # 必須進入 top-N 才算 entry signal
RANK_THEN_OUT = 100        # 7d 前必須在 top-N 外才算 rapid rise
LOOKBACK_DAYS_ENTRY = 7    # rank 比較窗
DROP_THRESHOLD = -0.15     # -15% 觸發 exit warning
LOOKBACK_DAYS_EXIT = 14    # 從 holding 開始最多回看 N 天

# (C) Mid-rebal BUY candidate alert — 2026-05-16 加 (e2bdc05 + 5e10f6e 後)
# 解 M15 rebal 太慢、mid-cycle 新爆發股漏網問題（如 2344 4/24-5/14 +55%）
# 2026-05-16 K_DEFAULT 10 切後同步降低 thresholds: MID_RANK_LOW K_DEFAULT, HIGH 2*K
PRODUCTION_K = 10          # 對齊 screener.py K_DEFAULT (production picks list size)
MID_RANK_LOW = PRODUCTION_K     # 不在 top-K 內 (production 已選名單)
MID_RANK_HIGH = 2 * PRODUCTION_K  # 但已進入 top-2K (score 開始升)
MID_5D_RET_THRESHOLD = 0.15  # 5d return ≥ 15% 才算啟動

# Primary composite for ranking (2026-05-16: 切 composite_score, 舊 snap 用 composite_parsi)
def _pick_score_col(snap: pd.DataFrame) -> str:
    return 'composite_score' if 'composite_score' in snap.columns else 'composite_parsi'


def _last_business_day_of_month(d: date) -> date:
    """Return last business day (Mon-Fri) of the month containing d."""
    _, last = calendar.monthrange(d.year, d.month)
    last_dt = date(d.year, d.month, last)
    while last_dt.weekday() >= 5:
        last_dt = last_dt - timedelta(days=1)
    return last_dt


def _mid_month_rebal_day(d: date) -> date:
    """Return M15 rebal day = last weekday on or before the 15th of d's month.

    2026-05-22 切換 (取代 month-end). Backtest 顯示 M15 顯著勝月底.
    詳見 reports/whale_picks_rebal_timing/REPORT.md.
    """
    target = date(d.year, d.month, 15)
    while target.weekday() >= 5:
        target = target - timedelta(days=1)
    return target


def _list_snapshots() -> List[date]:
    """All available snapshot dates, sorted ascending."""
    if not SNAPSHOT_DIR.exists():
        return []
    dates = []
    for f in SNAPSHOT_DIR.glob('20*.parquet'):
        try:
            dates.append(date.fromisoformat(f.stem))
        except ValueError:
            continue
    return sorted(dates)


def _load_snapshot(d: date) -> Optional[pd.DataFrame]:
    fp = SNAPSHOT_DIR / f"{d.isoformat()}.parquet"
    if not fp.exists():
        return None
    return pd.read_parquet(fp)


def _find_snapshot_near(target: date, snapshots: List[date], tolerance_days: int = 5) -> Optional[date]:
    """Find snapshot closest to target within tolerance_days."""
    candidates = [s for s in snapshots if abs((s - target).days) <= tolerance_days]
    if not candidates:
        return None
    return min(candidates, key=lambda s: abs((s - target).days))


# ============================================================
# (A) Early-entry signal
# ============================================================

def detect_early_entries(today: date, snapshots: List[date]) -> List[Dict]:
    """Find stocks that rapidly rose into top-N from outside top-100."""
    snap_today = _load_snapshot(today)
    if snap_today is None:
        log.warning("No snapshot for today (%s)", today)
        return []

    target_then = today - timedelta(days=LOOKBACK_DAYS_ENTRY)
    snap_then_date = _find_snapshot_near(target_then, snapshots, tolerance_days=3)
    if snap_then_date is None:
        log.info("No snapshot near %s (target=%s); skip early-entry", target_then, target_then)
        return []
    snap_then = _load_snapshot(snap_then_date)
    if snap_then is None:
        return []

    # Compute rank by primary composite (descending = lower rank num = better)
    # 2026-05-16 切 composite_score primary，舊 snap fallback composite_parsi
    col_today = _pick_score_col(snap_today)
    col_then = _pick_score_col(snap_then)
    snap_today = snap_today.dropna(subset=[col_today]).copy()
    snap_today['rank_now'] = snap_today[col_today].rank(ascending=False, method='min')

    snap_then = snap_then.dropna(subset=[col_then]).copy()
    snap_then['rank_then'] = snap_then[col_then].rank(ascending=False, method='min')

    merged = snap_today.merge(
        snap_then[['stock_id', 'rank_then']], on='stock_id', how='inner'
    )

    rapid = merged[
        (merged['rank_now'] <= RANK_NOW_TOP)
        & (merged['rank_then'] > RANK_THEN_OUT)
    ].sort_values('rank_now')

    log.info("Early-entry comparison %s vs %s: %d rapid rises",
             snap_then_date, today, len(rapid))

    out = []
    for _, r in rapid.iterrows():
        out.append({
            'stock_id': r['stock_id'],
            'stock_name': r.get('stock_name', '?'),
            'industry': r.get('industry_category', ''),
            'rank_now': int(r['rank_now']),
            'rank_then': int(r['rank_then']),
            'rank_delta': int(r['rank_then'] - r['rank_now']),
            'composite_now': float(r[col_today]),
            'composite_col': col_today,
            'close_now': float(r.get('Close', np.nan)),
        })
    return out


# ============================================================
# (C) Mid-month BUY candidate — score 進 top-30 但不在 top-20 + 5d 漲 ≥ 15%
# ============================================================

def detect_mid_month_buys(today: date, snapshots: List[date],
                          holdings: Optional[Dict]) -> List[Dict]:
    """找 score 介於 (top-20, top-30] + 5d return ≥ 15% 的「即將進入但未進」候選。

    用途：M15 rebal 漏抓 mid-cycle 爆發股（e.g., 2344 4/24-5/14 +55%）。
    這 alert 不自動進場，只提醒 user 手動評估。

    Criteria:
      (1) Rank by composite_score (or composite_parsi fallback) 在 (RANK_NOW_TOP, MID_RANK_HIGH]
      (2) Close vs Close[-5] ≥ MID_5D_RET_THRESHOLD (+15%)
      (3) 不在當前 holdings (top-10 of last M15 rebal) — 避免與 alerts (A) 重複

    Returns:
        List of dicts with stock_id / rank / 5d_ret / close.
    """
    snap_today = _load_snapshot(today)
    if snap_today is None:
        return []

    # 5d snapshot for return calc
    target_5d_ago = today - timedelta(days=7)  # 7 calendar days ≈ 5 trading
    snap_5d_date = _find_snapshot_near(target_5d_ago, snapshots, tolerance_days=3)
    if snap_5d_date is None:
        log.info("No 5d-ago snapshot near %s; skip mid-month buy alert", target_5d_ago)
        return []
    snap_5d = _load_snapshot(snap_5d_date)
    if snap_5d is None:
        return []

    # Rank by primary composite
    col_today = _pick_score_col(snap_today)
    snap_today = snap_today.dropna(subset=[col_today, 'Close']).copy()
    snap_today['rank_now'] = snap_today[col_today].rank(ascending=False, method='min')

    # 5d return
    snap_5d_close = snap_5d.dropna(subset=['Close'])[['stock_id', 'Close']].rename(
        columns={'Close': 'close_5d_ago'})
    merged = snap_today.merge(snap_5d_close, on='stock_id', how='inner')
    merged['ret_5d'] = merged['Close'] / merged['close_5d_ago'] - 1.0

    holding_ids = set()
    if holdings and isinstance(holdings.get('tickers'), list):
        holding_ids = {t.get('stock_id') for t in holdings['tickers']}

    candidates = merged[
        (merged['rank_now'] > MID_RANK_LOW)
        & (merged['rank_now'] <= MID_RANK_HIGH)
        & (merged['ret_5d'] >= MID_5D_RET_THRESHOLD)
        & (~merged['stock_id'].isin(holding_ids))
    ].sort_values('ret_5d', ascending=False)

    log.info("Mid-month BUY candidates %s vs %s: %d (rank %d-%d + 5d_ret>=%.0f%%)",
             snap_5d_date, today, len(candidates),
             MID_RANK_LOW + 1, MID_RANK_HIGH, MID_5D_RET_THRESHOLD * 100)

    out = []
    for _, r in candidates.iterrows():
        out.append({
            'stock_id': r['stock_id'],
            'stock_name': r.get('stock_name', '?'),
            'industry': r.get('industry_category', ''),
            'rank_now': int(r['rank_now']),
            'composite_now': float(r[col_today]),
            'composite_col': col_today,
            'close_now': float(r['Close']),
            'close_5d_ago': float(r['close_5d_ago']),
            'ret_5d': float(r['ret_5d']),
        })
    return out


# ============================================================
# (B) Trailing stop / early exit
# ============================================================

def _maybe_update_holdings(today: date, snapshots: List[date], force: bool = False) -> Optional[Dict]:
    """Refresh _active_holdings.json on M15 rebal day (or when forced).

    Returns the holdings dict (or existing if no update needed).

    2026-05-22 加 K drift detection: 如果 existing holdings 數量 != PRODUCTION_K
    也強制 refresh (防止切版後 holdings 沒同步更新，例如 5/16 K=20->K=10 切版時
    holdings JSON 卡在舊 K=20 直到下個月底才會自動覆寫)。
    """
    existing = None
    if HOLDINGS_PATH.exists():
        try:
            existing = json.loads(HOLDINGS_PATH.read_text(encoding='utf-8'))
        except Exception:
            existing = None

    is_rebal_day = today == _mid_month_rebal_day(today)
    # K drift: existing holdings 數量 != production K → 強制 refresh
    n_existing = len(existing.get('tickers', [])) if existing else 0
    k_drift = existing is not None and n_existing != PRODUCTION_K
    needs_update = force or is_rebal_day or existing is None or k_drift

    if k_drift:
        log.info("K drift detected: existing=%d tickers vs PRODUCTION_K=%d → force refresh",
                 n_existing, PRODUCTION_K)

    if not needs_update:
        return existing

    snap = _load_snapshot(today)
    snap_date = today
    if snap is None:
        # Fallback to most recent available snapshot (scanner runs 00:00 daily,
        # so on the same day before scan you'd need yesterday's snapshot for refresh)
        # _list_snapshots() returns ascending → iterate reversed to pick newest first
        for d in reversed(snapshots):
            snap = _load_snapshot(d)
            if snap is not None:
                snap_date = d
                log.info("Using fallback snapshot %s (today %s not yet generated)", d, today)
                break
    if snap is None:
        log.warning("No snapshot available to update holdings (today=%s)", today)
        return existing

    score_col = _pick_score_col(snap)
    top_k = snap.dropna(subset=[score_col]).nlargest(PRODUCTION_K, score_col)

    # 2026-05-22: 計算 top-3 entry drivers (factor * weight 最大的 3 個, 給 UI 顯示用)
    from tools.whale_picks_trade_ledger import (
        FACTOR_LABEL_ZH,
        _get_composite_dict,
    )
    composite_weights = _get_composite_dict(score_col)
    factor_cols = list(composite_weights.keys())

    def _compute_drivers(row) -> str:
        contribs = []
        for c in factor_cols:
            v = row.get(c)
            if pd.isna(v):
                continue
            contribs.append((c, float(v) * composite_weights[c]))
        contribs.sort(key=lambda x: x[1], reverse=True)
        top = [c for c, val in contribs[:3] if val > 0]
        return " / ".join(FACTOR_LABEL_ZH.get(c, c) for c in top) if top else "n/a"

    holdings = {
        'rebalance_date': snap_date.isoformat(),
        'reason': 'm15_rebal' if is_rebal_day else ('forced' if force else ('k_drift' if k_drift else 'bootstrap')),
        'composite_name': score_col,
        'tickers': [
            {
                'stock_id': r['stock_id'],
                'stock_name': r.get('stock_name', '?'),
                'industry': r.get('industry_category', ''),
                'entry_close': float(r.get('Close', np.nan)),
                'entry_composite': float(r[score_col]),
                'entry_drivers': _compute_drivers(r),
            }
            for _, r in top_k.iterrows()
        ],
    }
    HOLDINGS_PATH.write_text(json.dumps(holdings, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    log.info("Updated holdings (%s): %d tickers", holdings['reason'], len(holdings['tickers']))
    return holdings


def detect_early_exits(today: date, snapshots: List[date], holdings: Optional[Dict]) -> List[Dict]:
    """For each active holding, check if current close vs entry_close dropped >= 15%."""
    if not holdings or not holdings.get('tickers'):
        log.info("No active holdings; skip exit check")
        return []
    rebalance_date = date.fromisoformat(holdings['rebalance_date'])
    if (today - rebalance_date).days > 35:
        log.info("Holdings stale (rebalance=%s, today=%s, gap>35d), skip until next M15 rebal",
                 rebalance_date, today)
        return []

    snap_today = _load_snapshot(today)
    if snap_today is None:
        return []
    snap_today_close = snap_today.set_index('stock_id')['Close'].to_dict()

    out = []
    for h in holdings['tickers']:
        sid = h['stock_id']
        entry = h.get('entry_close')
        if entry is None or np.isnan(entry):
            continue
        cur = snap_today_close.get(sid)
        if cur is None or pd.isna(cur):
            continue
        chg = cur / entry - 1.0
        if chg <= DROP_THRESHOLD:
            out.append({
                'stock_id': sid,
                'stock_name': h.get('stock_name', '?'),
                'industry': h.get('industry', ''),
                'entry_close': float(entry),
                'current_close': float(cur),
                'drawdown_pct': float(chg),
                'rebalance_date': rebalance_date.isoformat(),
            })
    log.info("Early-exit check: %d holdings, %d triggers (drop <= %.0f%%)",
             len(holdings['tickers']), len(out), DROP_THRESHOLD * 100)
    return out


# ============================================================
# Discord push
# ============================================================

def format_discord_alert(entries: List[Dict], exits: List[Dict],
                         mid_buys: List[Dict], today: date) -> str:
    """Format alert message for Discord."""
    lines = [f"🐋 **Whale Picks 警報 ({today.isoformat()})**"]

    if entries:
        lines.append(f"\n📈 **早期主力訊號 — Rapid rank rise (排名 {RANK_THEN_OUT}+ → top-{RANK_NOW_TOP}, {LOOKBACK_DAYS_ENTRY}d)**")
        for e in entries[:10]:
            close_s = f"close={e['close_now']:.1f}" if pd.notna(e['close_now']) else ""
            lines.append(f"• **{e['stock_id']}** {e['stock_name']}  rank: {e['rank_then']}→{e['rank_now']} (Δ-{e['rank_delta']})  {close_s}")
        if len(entries) > 10:
            lines.append(f"_...另 {len(entries)-10} 檔，詳見 UI_")

    if mid_buys:
        lines.append(f"\n🚀 **Mid-rebal BUY 候選 — rank {MID_RANK_LOW + 1}-{MID_RANK_HIGH} + 5d 漲 ≥ {MID_5D_RET_THRESHOLD*100:.0f}%**")
        lines.append(f"_M15 rebal 漏抓的 mid-cycle 爆發候選；不自動進場，僅提醒手動評估_")
        for m in mid_buys[:10]:
            lines.append(f"• **{m['stock_id']}** {m['stock_name']}  rank {m['rank_now']}  5d {m['ret_5d']*100:+.1f}%  close {m['close_5d_ago']:.1f}→{m['close_now']:.1f}")
        if len(mid_buys) > 10:
            lines.append(f"_...另 {len(mid_buys)-10} 檔_")

    if exits:
        lines.append(f"\n📉 **持股早期警報 — Drawdown ≥ {abs(DROP_THRESHOLD)*100:.0f}%**")
        for x in exits[:10]:
            lines.append(f"• **{x['stock_id']}** {x['stock_name']}  {x['entry_close']:.1f}→{x['current_close']:.1f} ({x['drawdown_pct']*100:+.1f}%)  rebal={x['rebalance_date']}")
        if len(exits) > 10:
            lines.append(f"_...另 {len(exits)-10} 檔_")

    if not entries and not exits and not mid_buys:
        return ""  # no alerts

    lines.append(f"\n_7-feature composite_score / informational tier / 不接 portfolio gating_")
    return "\n".join(lines)


def push_discord(text: str) -> bool:
    import requests
    url = os.environ.get('DISCORD_WEBHOOK_WHALE_PICKS') or os.environ.get('DISCORD_WEBHOOK')
    if not url:
        log.warning("No Discord webhook env set")
        return False
    chunks = [text[i:i+1900] for i in range(0, len(text), 1900)]
    for c in chunks:
        try:
            r = requests.post(url, json={'content': c}, timeout=20)
            r.raise_for_status()
        except Exception as e:
            log.error("Discord push failed: %s", e)
            return False
    log.info("Discord push OK")
    return True


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Whale Picks daily alerts (entry / exit)")
    parser.add_argument('--asof', default=date.today().isoformat())
    parser.add_argument('--update-holdings', action='store_true',
                        help='Force refresh _active_holdings.json from today snapshot')
    parser.add_argument('--dry-run', action='store_true', help='Print alerts to stdout, no Discord')
    args = parser.parse_args()

    today = date.fromisoformat(args.asof)
    snapshots = _list_snapshots()
    log.info("Whale Picks alerts — asof %s, %d snapshots available", today, len(snapshots))

    holdings = _maybe_update_holdings(today, snapshots, force=args.update_holdings)
    entries = detect_early_entries(today, snapshots)
    exits = detect_early_exits(today, snapshots, holdings)
    mid_buys = detect_mid_month_buys(today, snapshots, holdings)

    msg = format_discord_alert(entries, exits, mid_buys, today)
    if not msg:
        log.info("No alerts to push")
        return

    if args.dry_run:
        print(msg)
        return
    push_discord(msg)


if __name__ == "__main__":
    main()
