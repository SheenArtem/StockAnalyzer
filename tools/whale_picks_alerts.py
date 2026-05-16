"""
Whale Picks Alerts — early-entry signal + trailing stop / early exit warning.

Runs daily after whale_picks_screener.py (in run_scanner.bat).

Two alert types:

(A) Early-entry signal — rapid rank rise:
    A ticker whose composite_parsi rank moved from >100 (7d ago)
    to ≤30 (today) → 主力剛發動候選 → Discord push.

(B) Trailing stop / early exit:
    Active holdings (last month-end's top-20) that dropped ≥ 15% from
    rebalance-day close → 不等月底才出 → Discord push.

Outputs:
- data/whale_picks/_active_holdings.json — refreshed each month-end
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
SNAPSHOT_DIR = REPO / "data" / "whale_picks"
HOLDINGS_PATH = SNAPSHOT_DIR / "_active_holdings.json"

# Thresholds (tunable)
RANK_NOW_TOP = 30          # 必須進入 top-N 才算 entry signal
RANK_THEN_OUT = 100        # 7d 前必須在 top-N 外才算 rapid rise
LOOKBACK_DAYS_ENTRY = 7    # rank 比較窗
DROP_THRESHOLD = -0.15     # -15% 觸發 exit warning
LOOKBACK_DAYS_EXIT = 14    # 從 holding 開始最多回看 N 天


def _last_business_day_of_month(d: date) -> date:
    """Return last business day (Mon-Fri) of the month containing d."""
    _, last = calendar.monthrange(d.year, d.month)
    last_dt = date(d.year, d.month, last)
    while last_dt.weekday() >= 5:
        last_dt = last_dt - timedelta(days=1)
    return last_dt


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

    # Compute rank by composite_parsi (descending = lower rank num = better)
    snap_today = snap_today.dropna(subset=['composite_parsi']).copy()
    snap_today['rank_now'] = snap_today['composite_parsi'].rank(ascending=False, method='min')

    snap_then = snap_then.dropna(subset=['composite_parsi']).copy()
    snap_then['rank_then'] = snap_then['composite_parsi'].rank(ascending=False, method='min')

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
            'composite_parsi_now': float(r['composite_parsi']),
            'close_now': float(r.get('Close', np.nan)),
        })
    return out


# ============================================================
# (B) Trailing stop / early exit
# ============================================================

def _maybe_update_holdings(today: date, snapshots: List[date], force: bool = False) -> Optional[Dict]:
    """Refresh _active_holdings.json on month-end (or when forced).

    Returns the holdings dict (or existing if no update needed).
    """
    existing = None
    if HOLDINGS_PATH.exists():
        try:
            existing = json.loads(HOLDINGS_PATH.read_text(encoding='utf-8'))
        except Exception:
            existing = None

    is_month_end = today == _last_business_day_of_month(today)
    needs_update = force or is_month_end or existing is None

    if not needs_update:
        return existing

    snap = _load_snapshot(today)
    if snap is None:
        log.warning("No snapshot for %s to update holdings", today)
        return existing

    top_20 = snap.dropna(subset=['composite_parsi']).nlargest(20, 'composite_parsi')
    holdings = {
        'rebalance_date': today.isoformat(),
        'reason': 'month_end' if is_month_end else ('forced' if force else 'bootstrap'),
        'tickers': [
            {
                'stock_id': r['stock_id'],
                'stock_name': r.get('stock_name', '?'),
                'industry': r.get('industry_category', ''),
                'entry_close': float(r.get('Close', np.nan)),
                'entry_composite': float(r['composite_parsi']),
            }
            for _, r in top_20.iterrows()
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
        log.info("Holdings stale (rebalance=%s, today=%s, gap>35d), skip until next month-end",
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

def format_discord_alert(entries: List[Dict], exits: List[Dict], today: date) -> str:
    """Format alert message for Discord."""
    lines = [f"🐋 **Whale Picks 警報 ({today.isoformat()})**"]

    if entries:
        lines.append(f"\n📈 **早期主力訊號 — Rapid rank rise (排名 {RANK_THEN_OUT}+ → top-{RANK_NOW_TOP}, {LOOKBACK_DAYS_ENTRY}d)**")
        for e in entries[:10]:
            close_s = f"close={e['close_now']:.1f}" if pd.notna(e['close_now']) else ""
            lines.append(f"• **{e['stock_id']}** {e['stock_name']}  rank: {e['rank_then']}→{e['rank_now']} (Δ-{e['rank_delta']})  {close_s}")
        if len(entries) > 10:
            lines.append(f"_...另 {len(entries)-10} 檔，詳見 UI_")

    if exits:
        lines.append(f"\n📉 **持股早期警報 — Drawdown ≥ {abs(DROP_THRESHOLD)*100:.0f}%**")
        for x in exits[:10]:
            lines.append(f"• **{x['stock_id']}** {x['stock_name']}  {x['entry_close']:.1f}→{x['current_close']:.1f} ({x['drawdown_pct']*100:+.1f}%)  rebal={x['rebalance_date']}")
        if len(exits) > 10:
            lines.append(f"_...另 {len(exits)-10} 檔_")

    if not entries and not exits:
        return ""  # no alerts

    lines.append(f"\n_8-factor composite_parsi / informational tier / 不接 portfolio gating_")
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

    msg = format_discord_alert(entries, exits, today)
    if not msg:
        log.info("No alerts to push")
        return

    if args.dry_run:
        print(msg)
        return
    push_discord(msg)


if __name__ == "__main__":
    main()
