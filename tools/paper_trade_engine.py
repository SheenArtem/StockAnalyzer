"""
Paper Trade Engine (Mode D Phase 2 Wave 3 #6, 2026-04-25)

系統自動模擬交易引擎，累積 trade log 供日後人工檢視 hit rate / avg return /
Sharpe，是 Phase 3 #10 Guest credibility 啟動的 ground truth 來源。

Council R2 (2026-04-25) 共識:
- 不重做出場判斷,複用 step_a_engine.py 的 daily_alerts.json forced 訊號
- 自動建/平倉,無 API 真實下單 (使用者人工下單哲學不變)

Daily flow:
1. 讀 open_trades + 今日 ohlcv close → 對每筆 open trade 判斷:
   - close <= target_sl → CLOSE at close, exit_reason='sl_hit'
   - close >= target_tp → CLOSE at close, exit_reason='tp_hit'
   - ticker 在今日 daily_alerts forced 名單 → CLOSE at close, exit_reason='step_a_forced'
   - else → 留 open
2. 讀今日 qm_result top_5 → 未在 open 名單者建新虛擬倉
   - entry_price = today close
   - target_tp / target_sl from action_plan
   - tag: trigger_score / yt_mention / c1_tilt / regime / scenario / qm_rank
3. 寫 open_trades.json (overwrite) + trade_log.jsonl (append closed)

無 forward-fill / 無滑價 / close price 視為成交價. 目的是測 signal 品質,
非執行能力. 6 個月累積 ≥50 trades 後人工檢視績效決定 #10 guest credibility
是否啟動。

Usage:
  python tools/paper_trade_engine.py            # 跑一次
  python tools/paper_trade_engine.py --dry-run  # 不寫檔
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PAPER_DIR = REPO / "data" / "paper_trades"
OPEN_TRADES = PAPER_DIR / "open_trades.json"
TRADE_LOG = PAPER_DIR / "trade_log.jsonl"

QM_RESULT = REPO / "data" / "latest" / "qm_result.json"
DAILY_ALERTS = REPO / "data" / "latest" / "daily_alerts.json"
OHLCV_TW = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
YT_PANEL = REPO / "data" / "sector_tags_dynamic.parquet"
C1_FLAGS = REPO / "data" / "c1_tilt_flags.parquet"
REGIME_LOG = REPO / "data" / "tracking" / "regime_log.jsonl"

TOP_N_OPEN = 5         # 每日 QM top_N 自動建倉
MAX_CONCURRENT = 15    # 同時最多 open trade 數量


def load_json(p: Path, default=None):
    if not p.exists():
        return default
    try:
        with open(p, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] load_json {p}: {e}", file=sys.stderr)
        return default


def load_today_regime() -> str:
    if not REGIME_LOG.exists():
        return "unknown"
    try:
        last = ""
        for line in REGIME_LOG.read_text(encoding='utf-8').splitlines():
            if line.strip():
                last = line
        if not last:
            return "unknown"
        return str(json.loads(last).get("regime", "unknown"))
    except Exception:
        return "unknown"


def load_close_prices(tickers: set, today: date):
    """讀 ohlcv_tw 取得 tickers 今日(或最近)收盤價. Returns dict {ticker: (date, close)}."""
    if not OHLCV_TW.exists() or not tickers:
        return {}
    try:
        import pandas as pd
        df = pd.read_parquet(OHLCV_TW, columns=['stock_id', 'date', 'Close'])
        df = df[df['stock_id'].isin(tickers)]
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'] <= pd.Timestamp(today)]
        # take latest row per ticker
        latest = df.sort_values('date').groupby('stock_id').tail(1)
        out = {}
        for _, r in latest.iterrows():
            out[str(r['stock_id'])] = (r['date'].date().isoformat(), float(r['Close']))
        return out
    except Exception as e:
        print(f"[WARN] load_close_prices: {e}", file=sys.stderr)
        return {}


def get_yt_tag(ticker: str) -> dict:
    """近 7 日 YT mention sentiment / count / shows"""
    if not YT_PANEL.exists():
        return {"count": 0, "sentiment": 0.0, "shows": []}
    try:
        import pandas as pd
        from datetime import timedelta
        df = pd.read_parquet(YT_PANEL)
        cutoff = date.today() - timedelta(days=7)
        sub = df[(df['ticker'] == ticker) & (df['date'] >= cutoff)]
        if sub.empty:
            return {"count": 0, "sentiment": 0.0, "shows": []}
        return {
            "count": int(len(sub)),
            "sentiment": float(sub['sentiment'].mean()),
            "shows": list(sub['show_key'].unique()),
        }
    except Exception:
        return {"count": 0, "sentiment": 0.0, "shows": []}


def get_c1_tilt(ticker: str) -> bool:
    if not C1_FLAGS.exists():
        return False
    try:
        import pandas as pd
        df = pd.read_parquet(C1_FLAGS)
        sub = df[df['stock_id'].astype(str) == ticker]
        if sub.empty:
            return False
        return bool(sub['c1_tilt_on'].iloc[0])
    except Exception:
        return False


def close_trade(trade: dict, exit_date: str, exit_price: float, reason: str) -> dict:
    """Mutates trade dict to closed state, returns it."""
    entry = trade['entry_price']
    pnl_pct = (exit_price / entry - 1) * 100 if entry else 0.0
    trade['exit_date'] = exit_date
    trade['exit_price'] = exit_price
    trade['exit_reason'] = reason
    trade['pnl_pct'] = round(pnl_pct, 2)
    return trade


def evaluate_open_trades(open_trades: list, daily_alerts: dict,
                        today: date) -> tuple[list, list]:
    """Returns (still_open, just_closed)."""
    if not open_trades:
        return [], []

    forced_tickers = {a['ticker'] for a in daily_alerts.get('alerts', [])
                      if a.get('action_type') == 'forced'}

    tickers = {t['ticker'] for t in open_trades}
    closes = load_close_prices(tickers, today)

    still_open, just_closed = [], []
    for t in open_trades:
        sid = t['ticker']
        if sid not in closes:
            still_open.append(t)
            continue
        cdate, cprice = closes[sid]
        # SL hit
        if t.get('target_sl') and cprice <= t['target_sl']:
            just_closed.append(close_trade(t, cdate, cprice, 'sl_hit'))
            continue
        # TP hit
        if t.get('target_tp') and cprice >= t['target_tp']:
            just_closed.append(close_trade(t, cdate, cprice, 'tp_hit'))
            continue
        # Step-A forced exit
        if sid in forced_tickers:
            just_closed.append(close_trade(t, cdate, cprice, 'step_a_forced'))
            continue
        still_open.append(t)
    return still_open, just_closed


def open_new_trades(qm_picks: list, open_tickers: set, today: date,
                    regime: str) -> list:
    """從 QM top N 建新 paper trades (跳過已持有 + max concurrent cap)."""
    new_trades = []
    if len(open_tickers) >= MAX_CONCURRENT:
        return new_trades

    candidates = qm_picks[:TOP_N_OPEN]
    new_tickers = [str(p.get('stock_id', '')) for p in candidates
                   if str(p.get('stock_id', '')) not in open_tickers]

    closes = load_close_prices(set(new_tickers), today)

    for p in candidates:
        if len(open_tickers) + len(new_trades) >= MAX_CONCURRENT:
            break
        sid = str(p.get('stock_id', ''))
        if not sid or sid in open_tickers:
            continue
        if sid not in closes:
            continue
        cdate, cprice = closes[sid]
        ap = p.get('action_plan') or {}
        target_tp = ap.get('rec_tp_price')
        target_sl = ap.get('rec_sl_price')
        scenario = ap.get('scenario_code', '-')

        # Sanity: skip if SL/TP 不合理 (action_plan 偶有舊資料 / 反向設定)
        # 必須: target_sl < entry < target_tp,否則跳過避免立刻假觸發
        if target_sl is not None and float(target_sl) >= cprice:
            print(f"  [skip] {sid}: SL {target_sl} >= entry {cprice} (data invalid)",
                  file=sys.stderr)
            continue
        if target_tp is not None and float(target_tp) <= cprice:
            print(f"  [skip] {sid}: TP {target_tp} <= entry {cprice} (data invalid)",
                  file=sys.stderr)
            continue

        new_trades.append({
            "trade_id": f"PT-{cdate.replace('-', '')}-{sid}",
            "ticker": sid,
            "name": p.get('name', ''),
            "entry_date": cdate,
            "entry_price": cprice,
            "target_tp": float(target_tp) if target_tp else None,
            "target_sl": float(target_sl) if target_sl else None,
            "exit_date": None,
            "exit_price": None,
            "exit_reason": None,
            "pnl_pct": None,
            "trigger_score_entry": p.get('trigger_score'),
            "qm_composite_entry": p.get('composite_score'),
            "scenario_entry": scenario,
            "qm_rank_entry": qm_picks.index(p) + 1 if p in qm_picks else None,
            "yt_tag_entry": get_yt_tag(sid),
            "c1_tilt_entry": get_c1_tilt(sid),
            "regime_entry": regime,
        })
    return new_trades


def append_to_log(closed_trades: list, dry_run: bool):
    if not closed_trades or dry_run:
        return
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    with open(TRADE_LOG, 'a', encoding='utf-8') as f:
        for t in closed_trades:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--date", help="覆寫今日日期 (test)")
    args = ap.parse_args()

    today = date.today()
    if args.date:
        today = datetime.strptime(args.date, "%Y-%m-%d").date()

    regime = load_today_regime()
    open_data = load_json(OPEN_TRADES, {"open_trades": []}) or {"open_trades": []}
    open_trades = open_data.get("open_trades", [])

    daily_alerts = load_json(DAILY_ALERTS, {"alerts": []}) or {"alerts": []}
    qm_data = load_json(QM_RESULT, {"results": []}) or {"results": []}
    qm_picks = qm_data.get("results", [])

    print(f"=== Paper Trade Engine ({today}) ===", file=sys.stderr)
    print(f"  regime: {regime}", file=sys.stderr)
    print(f"  open trades (start): {len(open_trades)}", file=sys.stderr)
    print(f"  qm picks today: {len(qm_picks)}", file=sys.stderr)

    still_open, just_closed = evaluate_open_trades(open_trades, daily_alerts, today)
    print(f"  closed today: {len(just_closed)}", file=sys.stderr)
    for t in just_closed:
        print(f"    {t['ticker']} {t.get('name', '')[:6]} | "
              f"{t['exit_reason']} | pnl {t['pnl_pct']:+.2f}%",
              file=sys.stderr)

    open_tickers = {t['ticker'] for t in still_open}
    new_trades = open_new_trades(qm_picks, open_tickers, today, regime)
    print(f"  opened today: {len(new_trades)}", file=sys.stderr)
    for t in new_trades:
        print(f"    {t['ticker']} {t.get('name', '')[:6]} | "
              f"entry {t['entry_price']:.2f} TP {t.get('target_tp')} SL {t.get('target_sl')}",
              file=sys.stderr)

    final_open = still_open + new_trades
    print(f"  open trades (end): {len(final_open)}", file=sys.stderr)

    out = {
        "schema_version": 1,
        "as_of": str(today),
        "regime": regime,
        "open_trades": final_open,
    }

    if args.dry_run:
        print(json.dumps(out, ensure_ascii=False, indent=2)[:1500] + "...", file=sys.stderr)
        return 0

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    with open(OPEN_TRADES, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    append_to_log(just_closed, dry_run=False)
    print(f"Written: {OPEN_TRADES}", file=sys.stderr)
    if just_closed:
        print(f"Appended {len(just_closed)} closed trades to {TRADE_LOG}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
