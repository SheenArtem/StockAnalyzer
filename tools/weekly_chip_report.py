"""
BL-4: 三大法人週報 (週六 08:00 batch)

Spec (2026-04-27 user 確認):
- 最近 5 交易日全市場三大法人合計買賣超
- 4 個獨立 Top 10 排行榜 (標的可不重複):
    1. 連續買超天數 desc (tiebreak 當週金額 desc)
    2. 連續賣超天數 desc (tiebreak 當週金額 asc)
    3. 當週買超金額 desc
    4. 當週賣超金額 asc

輸出: reports/weekly_chip_report_YYYY-MM-DD.md

CLI:
    python tools/weekly_chip_report.py                  # 預設最近交易日
    python tools/weekly_chip_report.py --week-end 2026-04-25
    python tools/weekly_chip_report.py --push-discord   # 跑完推 Discord 摘要
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
INST_PARQUET = REPO / "data_cache" / "chip_history" / "institutional.parquet"
OHLCV_PARQUET = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
UNIVERSE_PARQUET = REPO / "data_cache" / "backtest" / "universe_tw_full.parquet"
OUT_DIR = REPO / "reports"


def consecutive_from_end(seq: list[int]) -> tuple[int, int]:
    """從尾倒推連續同向天數。回 (連續買超天數, 連續賣超天數)。
    最後一日決定方向；若最後一日 = 0,兩者皆 0。"""
    if not seq or seq[-1] == 0:
        return 0, 0
    direction = 1 if seq[-1] > 0 else -1
    count = 0
    for v in reversed(seq):
        if (direction > 0 and v > 0) or (direction < 0 and v < 0):
            count += 1
        else:
            break
    return (count, 0) if direction > 0 else (0, count)


def load_universe_names() -> dict[str, str]:
    """從 universe_tw_full.parquet 撈 stock_id -> 股名。"""
    if not UNIVERSE_PARQUET.exists():
        return {}
    try:
        u = pd.read_parquet(UNIVERSE_PARQUET)
    except Exception:
        return {}
    name_col = next((c for c in ('stock_name', 'name', '名稱') if c in u.columns), None)
    if not name_col:
        return {}
    return dict(zip(u['stock_id'].astype(str), u[name_col]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--week-end", type=str, default=None,
                    help="週末交易日 YYYY-MM-DD (default 取 institutional 最新日)")
    ap.add_argument("--out-dir", type=str, default=None, help="輸出目錄 (default reports/)")
    ap.add_argument("--push-discord", action="store_true", help="完成後送 Discord 摘要")
    args = ap.parse_args()

    if not INST_PARQUET.exists():
        raise FileNotFoundError(f"Need {INST_PARQUET}")
    if not OHLCV_PARQUET.exists():
        raise FileNotFoundError(f"Need {OHLCV_PARQUET}")

    out_dir = Path(args.out_dir) if args.out_dir else OUT_DIR

    print("Loading institutional...")
    inst = pd.read_parquet(INST_PARQUET)
    inst['date'] = pd.to_datetime(inst['date'])
    available = sorted(inst['date'].unique())

    target = pd.Timestamp(args.week_end) if args.week_end else available[-1]
    candidates = [d for d in available if d <= target]
    if not candidates:
        raise ValueError(f"institutional 沒有 <= {target.date()} 的資料")
    week_end = max(candidates)

    # 取 week_end 為止最近 5 個交易日
    window = sorted([d for d in available if d <= week_end])[-5:]
    print(f"  week_end={week_end.date()}, window={[d.date().isoformat() for d in window]}")

    sub = inst[inst['date'].isin(window)][['date', 'stock_id', 'total_net', 'foreign_net', 'trust_net', 'dealer_net']]
    print(f"  rows in window: {len(sub):,}")

    # 拿 week_end (或最近一個 ohlcv 日) 收盤,用來把股數轉成金額
    print("Loading ohlcv...")
    ohlcv = pd.read_parquet(OHLCV_PARQUET, columns=['stock_id', 'date', 'Close'])
    ohlcv['stock_id'] = ohlcv['stock_id'].astype(str)
    ohlcv_dates = sorted(ohlcv['date'].unique())
    close_target = max([d for d in ohlcv_dates if d <= week_end], default=None)
    if close_target is None:
        raise ValueError(f"ohlcv 沒有 <= {week_end.date()} 的資料")
    if close_target != week_end:
        print(f"  WARN: ohlcv 最新到 {close_target.date()},與 institutional week_end {week_end.date()} 不同步")
    closes = ohlcv[ohlcv['date'] == close_target][['stock_id', 'Close']].rename(columns={'Close': 'close_ref'})

    # 每檔股票算 consec_buy/sell + weekly_net_shares
    print("Aggregating per stock...")
    rows = []
    for stock_id, g in sub.groupby('stock_id'):
        g_dict = dict(zip(g['date'], g['total_net']))
        seq = [int(g_dict.get(d, 0)) for d in window]
        consec_buy, consec_sell = consecutive_from_end(seq)
        weekly_net_shares = sum(seq)
        rows.append({
            'stock_id': stock_id,
            'consec_buy': consec_buy,
            'consec_sell': consec_sell,
            'weekly_net_shares': weekly_net_shares,
        })
    summary = pd.DataFrame(rows)
    summary = summary.merge(closes, on='stock_id', how='left')
    # 仟元 = 股數 × 元 / 1000
    summary['weekly_net_amount_k'] = (summary['weekly_net_shares'] * summary['close_ref'] / 1000).round(0)
    print(f"  unique stocks: {len(summary):,}")

    # 4 個 top 10
    # (1) 連續買超 (consec_buy >= 1, sort consec desc, tie 金額 desc)
    cb = summary[summary['consec_buy'] >= 1].sort_values(
        ['consec_buy', 'weekly_net_amount_k'], ascending=[False, False]).head(10)
    # (2) 連續賣超
    cs = summary[summary['consec_sell'] >= 1].sort_values(
        ['consec_sell', 'weekly_net_amount_k'], ascending=[False, True]).head(10)
    # (3) 當週買超金額
    ba = summary[summary['weekly_net_amount_k'] > 0].sort_values('weekly_net_amount_k', ascending=False).head(10)
    # (4) 當週賣超金額
    sa = summary[summary['weekly_net_amount_k'] < 0].sort_values('weekly_net_amount_k', ascending=True).head(10)

    name_map = load_universe_names()

    def fmt_amount(v) -> str:
        if pd.isna(v):
            return "-"
        return f"{v:+,.0f}"

    def stock_label(sid: str) -> str:
        n = name_map.get(str(sid), '')
        return f"{sid} {n}".strip()

    L: list[str] = []
    L.append(f"# 三大法人週報 — {week_end.strftime('%Y-%m-%d')}")
    L.append("")
    L.append(f"- 統計窗口: **{window[0].date()} ~ {window[-1].date()}** (共 {len(window)} 個交易日)")
    L.append(f"- 產出時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    L.append(f"- Universe: 全市場 ({len(summary):,} 檔)")
    L.append(f"- 金額參考收盤日: {close_target.date()} (千元 = 當週淨買賣股數 × 收盤價 / 1000)")
    L.append("")

    L.append("## 1. 連續買超天數 Top 10")
    if cb.empty:
        L.append("(本週無連續買超標的)")
    else:
        L.append("| # | 股票 | 連續買超天數 | 當週金額 (千元) | 當週股數 |")
        L.append("|---|---|---|---|---|")
        for i, (_, r) in enumerate(cb.iterrows(), 1):
            L.append(f"| {i} | {stock_label(r['stock_id'])} | {int(r['consec_buy'])} | "
                     f"{fmt_amount(r['weekly_net_amount_k'])} | {int(r['weekly_net_shares']):+,d} |")
    L.append("")

    L.append("## 2. 連續賣超天數 Top 10")
    if cs.empty:
        L.append("(本週無連續賣超標的)")
    else:
        L.append("| # | 股票 | 連續賣超天數 | 當週金額 (千元) | 當週股數 |")
        L.append("|---|---|---|---|---|")
        for i, (_, r) in enumerate(cs.iterrows(), 1):
            L.append(f"| {i} | {stock_label(r['stock_id'])} | {int(r['consec_sell'])} | "
                     f"{fmt_amount(r['weekly_net_amount_k'])} | {int(r['weekly_net_shares']):+,d} |")
    L.append("")

    L.append("## 3. 當週買超金額 Top 10")
    if ba.empty:
        L.append("(本週無淨買超標的)")
    else:
        L.append("| # | 股票 | 當週金額 (千元) | 連續買超天數 |")
        L.append("|---|---|---|---|")
        for i, (_, r) in enumerate(ba.iterrows(), 1):
            L.append(f"| {i} | {stock_label(r['stock_id'])} | "
                     f"{fmt_amount(r['weekly_net_amount_k'])} | {int(r['consec_buy'])} |")
    L.append("")

    L.append("## 4. 當週賣超金額 Top 10")
    if sa.empty:
        L.append("(本週無淨賣超標的)")
    else:
        L.append("| # | 股票 | 當週金額 (千元) | 連續賣超天數 |")
        L.append("|---|---|---|---|")
        for i, (_, r) in enumerate(sa.iterrows(), 1):
            L.append(f"| {i} | {stock_label(r['stock_id'])} | "
                     f"{fmt_amount(r['weekly_net_amount_k'])} | {int(r['consec_sell'])} |")
    L.append("")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"weekly_chip_report_{week_end.strftime('%Y-%m-%d')}.md"
    out_path.write_text("\n".join(L), encoding='utf-8')
    print(f"Written: {out_path}")

    if args.push_discord:
        push_summary(week_end, window, cb, cs, ba, sa, name_map, out_path)


def push_summary(week_end, window, cb, cs, ba, sa, name_map, out_path):
    """送 Discord 簡報摘要 (各榜 Top 3 + 報告檔名)。"""
    sys.path.insert(0, str(REPO))
    try:
        from scanner_job import send_alert_notification
    except Exception as e:
        print(f"[push_discord] cannot import scanner_job: {e}")
        return

    def label(sid):
        n = name_map.get(str(sid), '')
        return f"{sid} {n}".strip()

    def top3(df, col_days, sign):
        if df.empty:
            return ["(無)"]
        out = []
        for _, r in df.head(3).iterrows():
            amt = r['weekly_net_amount_k']
            amt_str = f"{amt:+,.0f}k" if pd.notna(amt) else "-"
            day_str = f" ({int(r[col_days])}日)" if col_days else ""
            out.append(f"  {label(r['stock_id'])} {amt_str}{day_str}")
        return out

    issues = [
        f"窗口: {window[0].date()} ~ {window[-1].date()}",
        "",
        "【連續買超 Top 3】",
        *top3(cb, 'consec_buy', +1),
        "",
        "【連續賣超 Top 3】",
        *top3(cs, 'consec_sell', -1),
        "",
        "【當週買超金額 Top 3】",
        *top3(ba, '', +1),
        "",
        "【當週賣超金額 Top 3】",
        *top3(sa, '', -1),
        "",
        f"完整報告: {out_path.name}",
    ]
    try:
        ok = send_alert_notification(
            scan_type='weekly_chip',
            market='TW',
            issues=issues,
        )
        print(f"[push_discord] {'sent' if ok else 'NOT sent (no webhook)'}")
    except Exception as e:
        print(f"[push_discord] ERROR: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
