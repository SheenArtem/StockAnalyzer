"""
Whale Picks Ledger - Incremental Append (alert add + M15 rebal reconcile).

2026-05-26 加 — 配合 100% Whale Picks SOP 的「mid-month BUY alert 手動加碼」流程：
user 自帶外部資金下單後，跑 --alert-add 寫進 ledger；月底 M15 rebal scanner
自動跑 --rebal 對 alert add 做升級 / 強制結算。

Two modes:

(1) --alert-add <stock_id>: user 手動觸發 (買完才跑)
    - 讀最新 snapshot 拿 stock_name / industry / composite / entry_close
    - Append 1 row 到 trade_ledger.parquet, entry_type='alert', still_holding=True
    - 更新 _active_holdings.json: 加進 alert_adds 列表 (不動 tickers 系統 10)

(2) --rebal: scanner.bat M15 day 自動跑 (whale_picks_alerts.py 之後)
    - 讀新 tickers (post-M15 system top-10) + exit pricing snapshot
    - 對每筆 alert_add:
        - 進新 top-10 → flip entry_type 'alert' → 'upgraded', remove from alert_adds
        - 沒進 → 強制 exit (寫 exit_date / exit_price / pnl_pct), remove from alert_adds
    - system + upgraded 進出維護 (2026-06-15 加, 修接縫讓 ledger 末端 = 真實持倉):
        - 現有 still_holding 跌出新 top-10 → 平倉 (snapshot close)
        - 新進 top-10 (無 still_holding row) → append system entry (entry_drivers 取自 holdings)
        - 續抱 → 更新 holding_months
      => 每月 M15 自動延伸 ledger; 歷史段 (full rebuild) 之後無縫接真實 PIT cohort

Usage:
    # Manual after buy (user 跑):
    python tools/whale_picks_ledger_append.py --alert-add 2356
    python tools/whale_picks_ledger_append.py --alert-add 2356 --entry-price 38.5

    # Auto on M15 (scanner.bat 跑):
    python tools/whale_picks_ledger_append.py --rebal

    # Inspect alert adds:
    python tools/whale_picks_ledger_append.py --list
"""
from __future__ import annotations

import argparse
import calendar
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("whale_picks_ledger_append")

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

SNAPSHOT_DIR = REPO / "data" / "whale_picks"
HOLDINGS_PATH = SNAPSHOT_DIR / "_active_holdings.json"
LEDGER_PATH = SNAPSHOT_DIR / "trade_ledger.parquet"


def _list_snapshots() -> List[date]:
    if not SNAPSHOT_DIR.exists():
        return []
    out = []
    for f in SNAPSHOT_DIR.glob('20*.parquet'):
        try:
            out.append(date.fromisoformat(f.stem))
        except ValueError:
            continue
    return sorted(out)


def _load_snapshot(d: date) -> Optional[pd.DataFrame]:
    fp = SNAPSHOT_DIR / f"{d.isoformat()}.parquet"
    if not fp.exists():
        return None
    return pd.read_parquet(fp)


def _latest_snapshot() -> Optional[tuple]:
    snaps = _list_snapshots()
    if not snaps:
        return None
    for d in reversed(snaps):
        df = _load_snapshot(d)
        if df is not None and len(df) > 0:
            return d, df
    return None


def _pick_score_col(snap: pd.DataFrame) -> str:
    return 'composite_score' if 'composite_score' in snap.columns else 'composite_parsi'


def _fallback_close(sid: str) -> Optional[float]:
    """跌出 (流動性/universe) 過濾後 snapshot 的持倉抓不到價時, 從 data_cache 個股 CSV 取最近 Close。
    保證平倉不留殭屍部位。"""
    csv = REPO / "data_cache" / f"{sid}_price.csv"
    if not csv.exists():
        return None
    try:
        d = pd.read_csv(csv)
        if 'Close' in d.columns and len(d):
            v = float(d['Close'].dropna().iloc[-1])
            return v if v > 0 else None
    except Exception:
        pass
    return None


def _mid_month_rebal_day(d: date) -> date:
    """M15 rebal day = last weekday on or before 15th of d's month.
    Mirror of whale_picks_alerts._mid_month_rebal_day."""
    target = date(d.year, d.month, 15)
    while target.weekday() >= 5:
        target = target - timedelta(days=1)
    return target


def _load_holdings() -> Dict:
    if not HOLDINGS_PATH.exists():
        return {}
    try:
        return json.loads(HOLDINGS_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        log.error("holdings JSON parse failed: %s", e)
        return {}


def _save_holdings(h: Dict) -> None:
    HOLDINGS_PATH.write_text(
        json.dumps(h, ensure_ascii=False, indent=2, default=str), encoding='utf-8'
    )


def _load_ledger() -> pd.DataFrame:
    if not LEDGER_PATH.exists():
        log.error("Ledger not found at %s — run whale_picks_trade_ledger.py first", LEDGER_PATH)
        sys.exit(1)
    df = pd.read_parquet(LEDGER_PATH)
    if 'entry_type' not in df.columns:
        df['entry_type'] = 'system'
    return df


def _save_ledger(df: pd.DataFrame) -> None:
    df.to_parquet(LEDGER_PATH, index=False)
    log.info("Saved ledger: %s (%d rows)", LEDGER_PATH, len(df))


def _update_meta(df: pd.DataFrame) -> None:
    """append/rebal 後同步 trade_ledger_meta.json 統計, 避免 view 顯示 stale 日期/勝率。"""
    mp = SNAPSHOT_DIR / "trade_ledger_meta.json"
    meta = {}
    if mp.exists():
        try:
            meta = json.loads(mp.read_text(encoding='utf-8'))
        except Exception:
            meta = {}
    closed = df[~df['still_holding'].astype(bool)]
    meta.update({
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'last_rebal_append': date.today().isoformat(),
        'n_positions': int(len(df)),
        'n_stocks': int(df['stock_id'].nunique()),
        'n_still_holding': int(df['still_holding'].sum()),
        'win_rate': float((closed['pnl_pct'] > 0).mean()) if len(closed) else None,
        'avg_pnl_pct': float(closed['pnl_pct'].mean()) if len(closed) else None,
        'median_pnl_pct': float(closed['pnl_pct'].median()) if len(closed) else None,
    })
    mp.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    log.info("Updated meta: %s (n_positions=%d, still_holding=%d)",
             mp, len(df), int(df['still_holding'].sum()))


# =============================================================================
# Mode 1: --alert-add
# =============================================================================

def cmd_alert_add(stock_id: str, entry_price: Optional[float] = None,
                  entry_date: Optional[str] = None) -> None:
    """Append one alert-triggered position to ledger + holdings JSON."""
    stock_id = str(stock_id).strip()
    if not stock_id:
        log.error("stock_id required")
        sys.exit(1)

    latest = _latest_snapshot()
    if latest is None:
        log.error("No snapshot available — cannot lookup stock metadata")
        sys.exit(1)
    snap_date, snap = latest

    if entry_date:
        try:
            ent_dt = date.fromisoformat(entry_date)
        except ValueError:
            log.error("Invalid --entry-date: %s (need YYYY-MM-DD)", entry_date)
            sys.exit(1)
    else:
        ent_dt = snap_date

    row = snap[snap['stock_id'].astype(str) == stock_id]
    if row.empty:
        log.error("stock_id %s not found in snapshot %s", stock_id, snap_date)
        sys.exit(1)
    r = row.iloc[0]
    score_col = _pick_score_col(snap)

    stock_name = r.get('stock_name', '?')
    industry = r.get('industry_category', '')
    snap_close = float(r.get('Close', np.nan))
    composite_val = float(r.get(score_col)) if pd.notna(r.get(score_col)) else np.nan
    rank_at_entry = int(snap[score_col].rank(ascending=False, method='min').loc[row.index[0]]) \
        if pd.notna(composite_val) else None

    used_price = float(entry_price) if entry_price is not None else snap_close
    if not (used_price and used_price > 0):
        log.error("entry_price invalid: %s (snapshot Close=%s)", entry_price, snap_close)
        sys.exit(1)

    # Append to ledger
    df = _load_ledger()
    dup = df[
        (df['stock_id'].astype(str) == stock_id)
        & (df['entry_type'] == 'alert')
        & (df['still_holding'] == True)  # noqa: E712
    ]
    if len(dup) > 0:
        log.error("Active alert add already exists for %s (entry_date=%s) — close it first",
                  stock_id, pd.Timestamp(dup.iloc[0]['entry_date']).date())
        sys.exit(1)

    new_row = {
        'stock_id': stock_id,
        'stock_name': stock_name,
        'industry': industry,
        'entry_date': pd.Timestamp(ent_dt),
        'entry_price': used_price,
        'exit_date': pd.NaT,
        'exit_price': np.nan,
        'still_holding': True,
        'holding_months': 1,
        'pnl_pct': np.nan,
        'composite_at_entry': composite_val,
        'composite_at_exit': np.nan,
        'rank_at_entry': rank_at_entry,
        'entry_type': 'alert',
        'entry_reason_zh': f'Mid-month BUY alert 加碼 (rank {rank_at_entry}, composite {composite_val:+.3f})',
        'exit_reason_zh': '',
    }
    # Fill missing schema cols with NaN (factor f_*/r_* etc.)
    for col in df.columns:
        if col not in new_row:
            new_row[col] = np.nan

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    _save_ledger(df)
    log.info("Appended alert add: %s %s entry=%s @ %.2f rank=%s composite=%+.3f",
             stock_id, stock_name, ent_dt, used_price, rank_at_entry, composite_val)

    # Update holdings JSON
    h = _load_holdings()
    if not h:
        log.warning("No existing _active_holdings.json — creating minimal one")
        h = {
            'rebalance_date': snap_date.isoformat(),
            'reason': 'bootstrap',
            'composite_name': score_col,
            'tickers': [],
            'alert_adds': [],
        }
    if 'alert_adds' not in h or not isinstance(h.get('alert_adds'), list):
        h['alert_adds'] = []

    # Dedupe
    h['alert_adds'] = [a for a in h['alert_adds'] if str(a.get('stock_id')) != stock_id]
    h['alert_adds'].append({
        'stock_id': stock_id,
        'stock_name': stock_name,
        'industry': industry,
        'entry_close': used_price,
        'entry_date': ent_dt.isoformat(),
        'entry_composite': composite_val,
        'entry_type': 'alert',
    })
    _save_holdings(h)
    log.info("Updated _active_holdings.json: %d alert_adds active",
             len(h['alert_adds']))


# =============================================================================
# Mode 2: --rebal
# =============================================================================

def cmd_rebal(dry_run: bool = False, force: bool = False) -> None:
    """M15 rebal reconcile alert_adds and still_holding upgraded rows.

    Self-gates: only runs if today is M15 rebal day AND holdings.rebalance_date
    equals today (= _maybe_update_holdings actually refreshed). --force bypasses.
    """
    today = date.today()
    if not force:
        m15 = _mid_month_rebal_day(today)
        if today != m15:
            log.info("Today %s != M15 rebal day %s — skip reconcile (use --force to override)",
                     today, m15)
            return

    h = _load_holdings()
    if not h:
        log.info("No _active_holdings.json — nothing to reconcile")
        return

    if not force:
        reb_date = h.get('rebalance_date')
        if reb_date != today.isoformat():
            log.info("holdings.rebalance_date %s != today %s — _maybe_update_holdings 沒跑成功 ; "
                     "skip reconcile to avoid stale-tickers close (use --force to override)",
                     reb_date, today)
            return

    new_tickers = h.get('tickers') or []
    new_ids = {str(t.get('stock_id')) for t in new_tickers}
    log.info("Current system top-K: %d tickers", len(new_ids))

    alert_adds = h.get('alert_adds') or []
    log.info("Pending alert_adds: %d", len(alert_adds))

    df = _load_ledger()

    # Snapshot for exit pricing
    latest = _latest_snapshot()
    if latest is None:
        log.error("No snapshot for exit pricing — aborting rebal")
        return
    snap_date, snap = latest
    snap_close = snap.set_index(snap['stock_id'].astype(str))['Close'].to_dict()

    upgraded_count = 0
    closed_count = 0
    remaining_alert_adds = []

    # Process alert_adds
    for a in alert_adds:
        sid = str(a.get('stock_id'))
        if sid in new_ids:
            # Upgrade
            mask = (
                (df['stock_id'].astype(str) == sid)
                & (df['entry_type'] == 'alert')
                & (df['still_holding'] == True)  # noqa: E712
            )
            if mask.sum() == 0:
                log.warning("alert_add %s not found as 'alert' still_holding in ledger; skip", sid)
                remaining_alert_adds.append(a)
                continue
            if not dry_run:
                df.loc[mask, 'entry_type'] = 'upgraded'
            upgraded_count += 1
            log.info("Upgraded %s 'alert' -> 'upgraded' (進新 top-K)", sid)
        else:
            # Force close
            mask = (
                (df['stock_id'].astype(str) == sid)
                & (df['entry_type'] == 'alert')
                & (df['still_holding'] == True)  # noqa: E712
            )
            if mask.sum() == 0:
                log.warning("alert_add %s not found in ledger; skip", sid)
                continue
            exit_price = snap_close.get(sid)
            if exit_price is None or pd.isna(exit_price) or exit_price <= 0:
                log.warning("No exit price for %s at %s; using last entry_price as exit (pnl=0)",
                            sid, snap_date)
                exit_price = float(df.loc[mask, 'entry_price'].iloc[0])
            entry_price = float(df.loc[mask, 'entry_price'].iloc[0])
            pnl = exit_price / entry_price - 1.0 if entry_price > 0 else np.nan

            if not dry_run:
                df.loc[mask, 'exit_date'] = pd.Timestamp(snap_date)
                df.loc[mask, 'exit_price'] = exit_price
                df.loc[mask, 'still_holding'] = False
                df.loc[mask, 'pnl_pct'] = pnl
                df.loc[mask, 'exit_reason_zh'] = (
                    f'M15 rebal 強制結算 (沒進新 top-{len(new_ids)})'
                )
            closed_count += 1
            log.info("Closed alert_add %s @ %.2f (entry %.2f, pnl %+.2f%%)",
                     sid, exit_price, entry_price, pnl * 100 if pd.notna(pnl) else 0)

    # === 2026-06-15: system top-K 進出維護 (修接縫, 讓 ledger 末端 = 真實 _active_holdings) ===
    # 統一處理 system + upgraded 的 still_holding: 跌出新 top-K -> 平倉; 續抱 -> 更新 holding_months.
    # (alert 未升級者不在此處理 — 上方 alert_adds 迴圈已處理)
    # held_ids 在 alert 升級之後計算, 故含剛 flip 成 upgraded 的 rows.
    sys_held_mask = (df['entry_type'].isin(['system', 'upgraded'])) & (df['still_holding'] == True)  # noqa: E712
    held_ids = set(df.loc[sys_held_mask, 'stock_id'].astype(str))

    sys_closed = 0
    sys_kept = 0
    for idx in list(df[sys_held_mask].index):
        sid = str(df.at[idx, 'stock_id'])
        etype = df.at[idx, 'entry_type']
        if sid in new_ids:
            # 續抱: 更新 holding_months (=進場到本次 rebal 的月數 +1)
            if not dry_run:
                entry_dt = pd.Timestamp(df.at[idx, 'entry_date'])
                df.at[idx, 'holding_months'] = int(max(
                    1, (snap_date.year - entry_dt.year) * 12
                       + (snap_date.month - entry_dt.month) + 1))
            sys_kept += 1
            continue
        # 平倉: 跌出新 top-K
        exit_price = snap_close.get(sid)
        if exit_price is None or pd.isna(exit_price) or exit_price <= 0:
            exit_price = _fallback_close(sid)  # 跌出 universe 的股從個股 CSV 取價
        if exit_price is None or pd.isna(exit_price) or exit_price <= 0:
            exit_price = float(df.at[idx, 'entry_price'])  # 最後手段: entry 價平倉 (pnl=0)
            log.warning("No market exit price for %s %s at %s; closing at entry (pnl=0)", etype, sid, snap_date)
        entry_price = float(df.at[idx, 'entry_price'])
        pnl = exit_price / entry_price - 1.0 if entry_price > 0 else np.nan
        if not dry_run:
            df.at[idx, 'exit_date'] = pd.Timestamp(snap_date)
            df.at[idx, 'exit_price'] = exit_price
            df.at[idx, 'still_holding'] = False
            df.at[idx, 'pnl_pct'] = pnl
            df.at[idx, 'exit_reason_zh'] = f'M15 rebal 跌出 top-{len(new_ids)} ({etype})'
        sys_closed += 1
        log.info("Closed %s %s @ %.2f (entry %.2f, pnl %+.2f%%)",
                 etype, sid, exit_price, entry_price, pnl * 100 if pd.notna(pnl) else 0)

    # 新進: 在新 top-K 但無 still_holding row -> append system entry (entry_drivers 取自 holdings)
    sys_added = 0
    new_rows = []
    for rank_i, t in enumerate(new_tickers):
        sid = str(t.get('stock_id'))
        if sid in held_ids:
            continue  # 續抱, 已處理
        entry_close = t.get('entry_close')
        if entry_close is None or pd.isna(entry_close) or float(entry_close) <= 0:
            entry_close = snap_close.get(sid)
        if entry_close is None or pd.isna(entry_close) or float(entry_close) <= 0:
            log.warning("No entry price for new system %s; skip append", sid)
            continue
        new_row = {
            'stock_id': sid,
            'stock_name': t.get('stock_name', '?'),
            'industry': t.get('industry', ''),
            'entry_date': pd.Timestamp(snap_date),
            'entry_price': float(entry_close),
            'exit_date': pd.NaT,
            'exit_price': np.nan,
            'still_holding': True,
            'holding_months': 1,
            'pnl_pct': np.nan,
            'composite_at_entry': t.get('entry_composite', np.nan),
            'composite_at_exit': np.nan,
            'rank_at_entry': rank_i + 1,
            'entry_type': 'system',
            'entry_top_drivers': t.get('entry_drivers', ''),
            'exit_top_drivers': '(尚未出場)',
            'entry_reason_zh': '',
            'exit_reason_zh': '',
        }
        for col in df.columns:
            if col not in new_row:
                new_row[col] = np.nan
        new_rows.append(new_row)
        sys_added += 1
        log.info("Appended new system %s %s entry=%s @ %.2f (rank %d)",
                 sid, t.get('stock_name', '?'), snap_date, float(entry_close), rank_i + 1)

    if dry_run:
        log.info("[DRY] system: +%d new / %d kept / -%d closed | alert: %d upgraded / %d closed",
                 sys_added, sys_kept, sys_closed, upgraded_count, closed_count)
        return

    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    _save_ledger(df)
    _update_meta(df)

    # Drain alert_adds (processed all)
    h['alert_adds'] = remaining_alert_adds
    _save_holdings(h)
    log.info("Rebal done: system +%d/-%d (kept %d), alert upgraded %d/closed %d, %d alert_adds remain",
             sys_added, sys_closed, sys_kept, upgraded_count, closed_count, len(remaining_alert_adds))


# =============================================================================
# Mode 3: --list
# =============================================================================

def cmd_list() -> None:
    h = _load_holdings()
    alert_adds = (h.get('alert_adds') if h else None) or []
    print(f"Active alert_adds ({len(alert_adds)}):")
    for a in alert_adds:
        print(f"  {a.get('stock_id')} {a.get('stock_name')} entry={a.get('entry_date')} "
              f"@ {a.get('entry_close')}")

    df = _load_ledger()
    upg = df[(df['entry_type'] == 'upgraded') & (df['still_holding'] == True)]  # noqa: E712
    print(f"\nStill-holding upgraded ({len(upg)}):")
    for _, r in upg.iterrows():
        print(f"  {r['stock_id']} {r['stock_name']} entry={pd.Timestamp(r['entry_date']).date()} "
              f"@ {r['entry_price']:.2f}")


def main():
    p = argparse.ArgumentParser(description='Whale Picks ledger incremental append')
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--alert-add', metavar='STOCK_ID',
                   help='Append alert-triggered entry (after manual buy)')
    g.add_argument('--rebal', action='store_true',
                   help='M15 rebal reconcile (called by scanner.bat after whale_picks_alerts)')
    g.add_argument('--list', action='store_true',
                   help='List active alert_adds + still_holding upgraded rows')
    p.add_argument('--entry-price', type=float, default=None,
                   help='Override entry price (default: latest snapshot Close)')
    p.add_argument('--entry-date', default=None,
                   help='Override entry date YYYY-MM-DD (default: latest snapshot date)')
    p.add_argument('--dry-run', action='store_true', help='--rebal only: log changes without writing')
    p.add_argument('--force', action='store_true',
                   help='--rebal only: bypass M15 self-gate (for manual one-off reconcile)')
    args = p.parse_args()

    if args.alert_add:
        cmd_alert_add(args.alert_add, entry_price=args.entry_price, entry_date=args.entry_date)
    elif args.rebal:
        cmd_rebal(dry_run=args.dry_run, force=args.force)
    elif args.list:
        cmd_list()


if __name__ == '__main__':
    main()
