"""
BL-4: 三大法人週報 (週六 08:00 batch) — v2 各法人分開

Spec (2026-04-27 user 確認):
- 最近 5 交易日全市場
- 4 個維度: 三大法人合計 (total_net) / 外資 / 投信 / 自營商
- 每維度 4 個獨立 Top 10:
    1. 連續買超天數 desc (tiebreak 當週金額 desc)
    2. 連續賣超天數 desc (tiebreak 當週金額 asc)
    3. 當週買超金額 desc
    4. 當週賣超金額 asc
- 共 4 × 4 = 16 個 Top 10 表

輸出: reports/weekly_chip_report_YYYY-MM-DD.md

CLI:
    python tools/weekly_chip_report.py
    python tools/weekly_chip_report.py --week-end 2026-04-25
    python tools/weekly_chip_report.py --push-discord
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
LATEST_PARQUET = REPO / "data" / "weekly_chip_latest.parquet"  # UI 載入用 long-format snapshot

# 4 個維度 (順序決定報告 section 順序)
NET_DIMENSIONS = [
    ('total_net',   '三大法人合計', 'A'),
    ('foreign_net', '外資',         'B'),
    ('trust_net',   '投信',         'C'),
    ('dealer_net',  '自營商',       'D'),
]


def consecutive_from_end(seq: list[int]) -> tuple[int, int]:
    """從尾倒推連續同向天數。回 (連續買超天數, 連續賣超天數)。"""
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


def compute_summary(sub: pd.DataFrame, window: list, net_col: str,
                    closes: pd.DataFrame) -> pd.DataFrame:
    """對指定 net column 算每檔 consec_buy / consec_sell / weekly_net_shares + 金額。"""
    rows = []
    for stock_id, g in sub.groupby('stock_id'):
        g_dict = dict(zip(g['date'], g[net_col]))
        seq = [int(g_dict.get(d, 0)) for d in window]
        consec_buy, consec_sell = consecutive_from_end(seq)
        rows.append({
            'stock_id': stock_id,
            'consec_buy': consec_buy,
            'consec_sell': consec_sell,
            'weekly_net_shares': sum(seq),
        })
    df = pd.DataFrame(rows)
    df = df.merge(closes, on='stock_id', how='left')
    df['weekly_net_amount_k'] = (df['weekly_net_shares'] * df['close_ref'] / 1000).round(0)
    return df


def get_top10s(summary: pd.DataFrame) -> tuple:
    cb = summary[summary['consec_buy'] >= 1].sort_values(
        ['consec_buy', 'weekly_net_amount_k'], ascending=[False, False]).head(10)
    cs = summary[summary['consec_sell'] >= 1].sort_values(
        ['consec_sell', 'weekly_net_amount_k'], ascending=[False, True]).head(10)
    ba = summary[summary['weekly_net_amount_k'] > 0].sort_values(
        'weekly_net_amount_k', ascending=False).head(10)
    sa = summary[summary['weekly_net_amount_k'] < 0].sort_values(
        'weekly_net_amount_k', ascending=True).head(10)
    return cb, cs, ba, sa


def render_dimension_section(L: list, prefix: str, dim_name: str,
                             cb, cs, ba, sa, stock_label, fmt_amount):
    """寫一個維度的 4 個 top 10 markdown table。"""
    L.append(f"## {prefix}. {dim_name}")
    L.append("")

    L.append(f"### {prefix}.1 連續買超天數 Top 10")
    if cb.empty:
        L.append("(本週無連續買超標的)")
    else:
        L.append("| # | 股票 | 連續天數 | 當週金額 (千元) | 當週股數 |")
        L.append("|---|---|---|---|---|")
        for i, (_, r) in enumerate(cb.iterrows(), 1):
            L.append(f"| {i} | {stock_label(r['stock_id'])} | {int(r['consec_buy'])} | "
                     f"{fmt_amount(r['weekly_net_amount_k'])} | {int(r['weekly_net_shares']):+,d} |")
    L.append("")

    L.append(f"### {prefix}.2 連續賣超天數 Top 10")
    if cs.empty:
        L.append("(本週無連續賣超標的)")
    else:
        L.append("| # | 股票 | 連續天數 | 當週金額 (千元) | 當週股數 |")
        L.append("|---|---|---|---|---|")
        for i, (_, r) in enumerate(cs.iterrows(), 1):
            L.append(f"| {i} | {stock_label(r['stock_id'])} | {int(r['consec_sell'])} | "
                     f"{fmt_amount(r['weekly_net_amount_k'])} | {int(r['weekly_net_shares']):+,d} |")
    L.append("")

    L.append(f"### {prefix}.3 當週買超金額 Top 10")
    if ba.empty:
        L.append("(本週無淨買超標的)")
    else:
        L.append("| # | 股票 | 當週金額 (千元) | 連續買超天數 |")
        L.append("|---|---|---|---|")
        for i, (_, r) in enumerate(ba.iterrows(), 1):
            L.append(f"| {i} | {stock_label(r['stock_id'])} | "
                     f"{fmt_amount(r['weekly_net_amount_k'])} | {int(r['consec_buy'])} |")
    L.append("")

    L.append(f"### {prefix}.4 當週賣超金額 Top 10")
    if sa.empty:
        L.append("(本週無淨賣超標的)")
    else:
        L.append("| # | 股票 | 當週金額 (千元) | 連續賣超天數 |")
        L.append("|---|---|---|---|")
        for i, (_, r) in enumerate(sa.iterrows(), 1):
            L.append(f"| {i} | {stock_label(r['stock_id'])} | "
                     f"{fmt_amount(r['weekly_net_amount_k'])} | {int(r['consec_sell'])} |")
    L.append("")


def compute_weekly_rankings(week_end_str: str | None = None) -> tuple[dict, dict]:
    """純資料計算層 (UI 與 markdown 共用)。

    Returns:
      (metadata, dim_results) where:
        metadata: {week_end, window_start, window_end, window_days, close_ref_date}
        dim_results: {net_col: (cb, cs, ba, sa)}  # 4 DataFrames per dim
    """
    if not INST_PARQUET.exists():
        raise FileNotFoundError(f"Need {INST_PARQUET}")
    if not OHLCV_PARQUET.exists():
        raise FileNotFoundError(f"Need {OHLCV_PARQUET}")

    inst = pd.read_parquet(INST_PARQUET)
    inst['date'] = pd.to_datetime(inst['date'])
    available = sorted(inst['date'].unique())

    target = pd.Timestamp(week_end_str) if week_end_str else available[-1]
    candidates = [d for d in available if d <= target]
    if not candidates:
        raise ValueError(f"institutional 沒有 <= {target.date()} 的資料")
    week_end = max(candidates)
    window = sorted([d for d in available if d <= week_end])[-5:]

    sub = inst[inst['date'].isin(window)][['date', 'stock_id', 'total_net',
                                            'foreign_net', 'trust_net', 'dealer_net']]

    ohlcv = pd.read_parquet(OHLCV_PARQUET, columns=['stock_id', 'date', 'Close'])
    ohlcv['stock_id'] = ohlcv['stock_id'].astype(str)
    ohlcv_dates = sorted(ohlcv['date'].unique())
    close_target = max([d for d in ohlcv_dates if d <= week_end], default=None)
    if close_target is None:
        raise ValueError(f"ohlcv 沒有 <= {week_end.date()} 的資料")
    # 取每檔在 week_end (含) 之前的最新 close 為 close_ref
    # (ohlcv --resume 不會更新已存在 ticker 的新日期，故 close_target 那天可能只有少數股票
    #  有資料；改用 per-stock 最後可得 close 避免 weekly_net_amount_k 大量 NaN)
    ohlcv_pre = ohlcv[ohlcv['date'] <= week_end].sort_values('date')
    closes = ohlcv_pre.groupby('stock_id', as_index=False).tail(1)[
        ['stock_id', 'Close']].rename(columns={'Close': 'close_ref'})

    dim_results = {}
    for net_col, dim_name, _prefix in NET_DIMENSIONS:
        summary = compute_summary(sub, window, net_col, closes)
        dim_results[net_col] = get_top10s(summary)

    metadata = {
        'week_end': week_end,
        'window_start': window[0],
        'window_end': window[-1],
        'window_days': len(window),
        'close_ref_date': close_target,
    }
    return metadata, dim_results


def save_long_format_parquet(metadata: dict, dim_results: dict, name_map: dict,
                              out_path: Path = LATEST_PARQUET) -> None:
    """把 16 個 Top 10 攤成 long-format parquet 給 UI 載入。

    Schema: week_end | dim | rank_type | rank | stock_id | stock_name |
            consec_days | weekly_amount_k | weekly_shares
    """
    rank_type_keys = ['consec_buy', 'consec_sell', 'week_buy', 'week_sell']
    rows = []
    for net_col, dim_name, _prefix in NET_DIMENSIONS:
        cb, cs, ba, sa = dim_results[net_col]
        for rk_key, df in zip(rank_type_keys, [cb, cs, ba, sa]):
            consec_col = 'consec_buy' if rk_key in ('consec_buy', 'week_buy') else 'consec_sell'
            for rank_idx, (_, r) in enumerate(df.iterrows(), 1):
                rows.append({
                    'week_end': metadata['week_end'],
                    'dim': net_col.replace('_net', ''),  # total / foreign / trust / dealer
                    'dim_name_zh': dim_name,
                    'rank_type': rk_key,
                    'rank': rank_idx,
                    'stock_id': str(r['stock_id']),
                    'stock_name': name_map.get(str(r['stock_id']), ''),
                    'consec_days': int(r.get(consec_col, 0)),
                    'weekly_amount_k': float(r.get('weekly_net_amount_k', 0)) if pd.notna(r.get('weekly_net_amount_k')) else 0.0,
                    'weekly_shares': int(r.get('weekly_net_shares', 0)),
                })
    df_long = pd.DataFrame(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_long.to_parquet(out_path, index=False)
    print(f"Saved long-format parquet: {out_path} ({len(df_long)} rows)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--week-end", type=str, default=None,
                    help="週末交易日 YYYY-MM-DD (default 取 institutional 最新日)")
    ap.add_argument("--out-dir", type=str, default=None, help="輸出目錄 (default reports/)")
    ap.add_argument("--push-discord", action="store_true", help="完成後送 Discord 摘要")
    args = ap.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else OUT_DIR

    print("Computing weekly rankings...")
    metadata, dim_results = compute_weekly_rankings(args.week_end)
    week_end = metadata['week_end']
    window = pd.date_range(metadata['window_start'], metadata['window_end'])
    close_target = metadata['close_ref_date']
    print(f"  week_end={week_end.date()}, window_days={metadata['window_days']}")

    name_map = load_universe_names()

    def fmt_amount(v) -> str:
        return "-" if pd.isna(v) else f"{v:+,.0f}"

    def stock_label(sid: str) -> str:
        n = name_map.get(str(sid), '')
        return f"{sid} {n}".strip()

    # 寫 long-format parquet 給 UI 載入
    save_long_format_parquet(metadata, dim_results, name_map)

    # 從 metadata 還原 window list (markdown 用實際窗口而非連續 date_range)
    inst_dates = pd.read_parquet(INST_PARQUET, columns=['date'])
    inst_dates['date'] = pd.to_datetime(inst_dates['date'])
    avail = sorted(inst_dates['date'].unique())
    window = sorted([d for d in avail if d <= week_end])[-metadata['window_days']:]

    # 寫 markdown
    L: list[str] = []
    L.append(f"# 三大法人週報 — {week_end.strftime('%Y-%m-%d')}")
    L.append("")
    L.append(f"- 統計窗口: **{window[0].date()} ~ {window[-1].date()}** (共 {len(window)} 個交易日)")
    L.append(f"- 產出時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    L.append(f"- Universe: 全市場")
    L.append(f"- 金額參考收盤日: {close_target.date()} (千元 = 當週淨買賣股數 × 收盤價 / 1000)")
    L.append(f"- 維度: 4 個 (三大法人合計 / 外資 / 投信 / 自營商) × 4 個榜 = **16 個 Top 10**")
    L.append("")

    for net_col, dim_name, prefix in NET_DIMENSIONS:
        cb, cs, ba, sa = dim_results[net_col]
        render_dimension_section(L, prefix, dim_name, cb, cs, ba, sa, stock_label, fmt_amount)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"weekly_chip_report_{week_end.strftime('%Y-%m-%d')}.md"
    out_path.write_text("\n".join(L), encoding='utf-8')
    print(f"Written: {out_path}")

    if args.push_discord:
        push_summary(week_end, window, dim_results, name_map, out_path)


def push_summary(week_end, window, dim_results, name_map, out_path):
    """送 Discord 摘要: 各維度連續買賣超 Top 1 + 當週金額 Top 1。"""
    sys.path.insert(0, str(REPO))
    try:
        from scanner_job import send_alert_notification
    except Exception as e:
        print(f"[push_discord] cannot import scanner_job: {e}")
        return

    def label(sid):
        n = name_map.get(str(sid), '')
        return f"{sid} {n}".strip()

    def top1(df, col_days):
        if df.empty:
            return "(無)"
        r = df.iloc[0]
        amt = r['weekly_net_amount_k']
        amt_str = f"{amt:+,.0f}k" if pd.notna(amt) else "-"
        if col_days:
            return f"{label(r['stock_id'])} {int(r[col_days])}日 {amt_str}"
        return f"{label(r['stock_id'])} {amt_str}"

    issues = [f"窗口: {window[0].date()} ~ {window[-1].date()}", ""]
    for net_col, dim_name, _ in NET_DIMENSIONS:
        cb, cs, ba, sa = dim_results[net_col]
        issues.extend([
            f"=== {dim_name} ===",
            f"連續買: {top1(cb, 'consec_buy')}",
            f"連續賣: {top1(cs, 'consec_sell')}",
            f"買超王: {top1(ba, '')}",
            f"賣超王: {top1(sa, '')}",
            "",
        ])
    issues.append(f"完整報告: {out_path.name}")
    try:
        ok = send_alert_notification(scan_type='weekly_chip', market='TW', issues=issues)
        print(f"[push_discord] {'sent' if ok else 'NOT sent (no webhook)'}")
    except Exception as e:
        print(f"[push_discord] ERROR: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
