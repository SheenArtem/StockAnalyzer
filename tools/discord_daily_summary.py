"""
Discord Daily Summary (Mode D Phase 2 Wave 3 #5 capstone, 2026-04-25)

整合 Mode D 全 pipeline 產出 → 單一 Discord code block 每日 1 則。
讀:
  - data/latest/qm_result.json     (top 5 pick)
  - data/latest/daily_alerts.json  (Step-A forced/suggested 警報)
  - data/paper_trades/open_trades.json + trade_log.jsonl (paper 進度)
  - data/sector_tags_dynamic.parquet (YT mention)
  - data/c1_tilt_flags.parquet (C1 tilt)
  - data/latest/audits/ (使用者手動 /mode-d-audit 結果)

Council R2 + feedback_discord_no_md_tables (2026-04-23) 規範:
- 單一 code block 每日 1 則 (不用 markdown table - Discord 不渲染)
- 程式碼區塊用 padding 對齊欄位
- Robustness First: smoke test / fail loud / 失敗不阻擋 scanner

Usage:
  python tools/discord_daily_summary.py            # push to Discord
  python tools/discord_daily_summary.py --dry-run  # 印到 stderr 不 push
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
QM_RESULT = REPO / "data" / "latest" / "qm_result.json"
DAILY_ALERTS = REPO / "data" / "latest" / "daily_alerts.json"
OPEN_TRADES = REPO / "data" / "paper_trades" / "open_trades.json"
TRADE_LOG = REPO / "data" / "paper_trades" / "trade_log.jsonl"
YT_PANEL = REPO / "data" / "sector_tags_dynamic.parquet"
C1_FLAGS = REPO / "data" / "c1_tilt_flags.parquet"
AUDITS_DIR = REPO / "data" / "latest" / "audits"
ENV_FILE = REPO / "local" / ".env"


def load_json(p: Path, default=None):
    if not p.exists():
        return default
    try:
        with open(p, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def read_webhook() -> str | None:
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text(encoding='utf-8').splitlines():
        if line.startswith('DISCORD_WEBHOOK_URL='):
            return line.split('=', 1)[1].strip().strip('"').strip("'")
    return None


def get_yt_tag(ticker: str) -> str:
    """Returns YT badge string e.g. '🟢×3' or '—'"""
    if not YT_PANEL.exists():
        return "-"
    try:
        import pandas as pd
        df = pd.read_parquet(YT_PANEL)
        cutoff = date.today() - timedelta(days=7)
        sub = df[(df['ticker'] == ticker) & (df['date'] >= cutoff)]
        if sub.empty:
            return "-"
        cnt = len(sub)
        sent = sub['sentiment'].mean()
        icon = "🟢" if sent > 0.3 else ("🔴" if sent < -0.3 else "⚪")
        return f"{icon}x{cnt}"
    except Exception:
        return "-"


def get_c1_tilt_set() -> set:
    if not C1_FLAGS.exists():
        return set()
    try:
        import pandas as pd
        df = pd.read_parquet(C1_FLAGS)
        return set(df[df['c1_tilt_on']]['stock_id'].astype(str).tolist())
    except Exception:
        return set()


def load_audits_today() -> list[dict]:
    if not AUDITS_DIR.exists():
        return []
    today_str = date.today().strftime("%Y%m%d")
    audits = []
    for p in AUDITS_DIR.glob(f"*_{today_str}.json"):
        a = load_json(p)
        if a:
            audits.append(a)
    return audits


def count_trade_log() -> tuple[int, int]:
    """Returns (total_closed, today_closed)"""
    if not TRADE_LOG.exists():
        return 0, 0
    today_str = date.today().isoformat()
    total = 0
    today_cnt = 0
    try:
        for line in TRADE_LOG.read_text(encoding='utf-8').splitlines():
            if not line.strip():
                continue
            t = json.loads(line)
            total += 1
            if t.get('exit_date') == today_str:
                today_cnt += 1
    except Exception:
        pass
    return total, today_cnt


def build_summary() -> str:
    today = date.today()
    lines = []
    lines.append(f"=== Mode D Daily Summary {today} ===")

    # --- 1. Today's Top 5 Pick ---
    qm_data = load_json(QM_RESULT, {"results": []}) or {"results": []}
    picks = qm_data.get("results", [])[:5]
    daily = load_json(DAILY_ALERTS, {})
    regime = (daily.get("regime") if daily else "-") or "-"
    is_reb = "Yes" if (daily and daily.get("is_rebalance_day")) else "No"
    lines.append(f"Regime: {regime} | Rebalance day: {is_reb}")
    lines.append("")

    if picks:
        lines.append("Top 5 QM Pick:")
        c1_set = get_c1_tilt_set()
        for i, p in enumerate(picks, 1):
            sid = str(p.get("stock_id", ""))
            name = str(p.get("name", ""))[:6]
            qm = p.get("composite_score") or 0
            trig = p.get("trigger_score") or 0
            yt = get_yt_tag(sid)
            c1 = "C1" if sid in c1_set else "-"
            lines.append(
                f"  {i}. {sid:>5s} {name:<6s} | QM {qm:>5.1f} | Trig {trig:+.1f} | "
                f"YT {yt:<6s} | {c1}"
            )
        lines.append("")
    else:
        lines.append("Top 5 QM Pick: (no data)")
        lines.append("")

    # --- 2. Step-A Forced/Suggested Alerts ---
    if daily:
        alerts = daily.get("alerts", [])
        forced = [a for a in alerts if a.get("action_type") == "forced"]
        suggested = [a for a in alerts if a.get("action_type") == "suggested"]
        if forced:
            lines.append(f"FORCED EXITS (Step-A Hard) [{len(forced)}]:")
            for a in forced:
                tk = a.get("ticker", "")
                nm = str(a.get("name", ""))[:6]
                rsn = str(a.get("reason", ""))[:30]
                lines.append(f"  ! {tk} {nm} | {rsn} | 立即出場")
            lines.append("")
        if suggested:
            lines.append(f"SUGGESTED [{len(suggested)}]:")
            for a in suggested:
                tk = a.get("ticker", "")
                nm = str(a.get("name", ""))[:6]
                rsn = str(a.get("reason", ""))[:30]
                # Hard 軟化過的 (downgrade_reason 非空) 標記出來區分
                downgrade = a.get("downgrade_reason")
                if downgrade:
                    rule_short = str(a.get("rule", ""))[:24]
                    lines.append(f"  ? {tk} {nm} | {rsn} | <-{rule_short}")
                else:
                    lines.append(f"  ? {tk} {nm} | {rsn}")
            lines.append("")
    else:
        lines.append("Step-A: (no daily_alerts.json)")
        lines.append("")

    # --- 3. Paper Trade ---
    open_data = load_json(OPEN_TRADES, {"open_trades": []}) or {"open_trades": []}
    open_count = len(open_data.get("open_trades", []))
    total_log, today_closed = count_trade_log()

    today_opened = [
        t for t in open_data.get("open_trades", [])
        if t.get("entry_date") == today.isoformat()
    ]
    lines.append(f"Paper Trade: open={open_count} | closed today={today_closed} | "
                 f"total log={total_log}")
    if today_opened:
        op_str = " ".join(t.get("ticker", "") for t in today_opened)
        lines.append(f"  Opened today: {op_str}")
    lines.append("")

    # --- 3.5 News flow anomaly (Phase 1 #4) ---
    flow_path = REPO / 'data' / 'news' / 'news_flow_anomaly.parquet'
    if flow_path.exists():
        try:
            import pandas as _pd
            adf = _pd.read_parquet(flow_path)
            adf['detection_date'] = _pd.to_datetime(adf['detection_date']).dt.date
            today_anom = adf[adf['detection_date'] == today].sort_values(
                'ratio', ascending=False).head(8)
            if not today_anom.empty:
                lines.append(f"News Flow 爆量 [{len(today_anom)}]:")
                for _, r in today_anom.iterrows():
                    tk = str(r.get('ticker', ''))
                    nm = str(r.get('company_name', ''))[:6]
                    cnt = int(r.get('count_today', 0))
                    avg = float(r.get('count_7d_avg', 0))
                    ratio = float(r.get('ratio', 0))
                    ratio_str = "new" if ratio >= 999 else f"{ratio:.1f}x"
                    themes = str(r.get('top_themes', ''))[:30]
                    lines.append(f"  * {tk} {nm} | {cnt}篇 vs 7d avg {avg:.1f} ({ratio_str}) | {themes}")
                lines.append("")
        except Exception as exc:
            lines.append(f"News Flow 爆量: (load error: {exc})")
            lines.append("")

    # --- 4. Audits ---
    audits = load_audits_today()
    if audits:
        lines.append(f"AI Audits Today [{len(audits)}]:")
        for a in audits:
            tk = a.get("ticker", "")
            verdict = a.get("verdict", "?")
            short = str(a.get("verdict_short", ""))[:40]
            lines.append(f"  {tk}: {verdict} - {short}")
    else:
        lines.append("AI Audits Today: (none, 使用者未手動 /mode-d-audit)")

    return "\n".join(lines)


def push_to_discord(message: str, dry_run: bool) -> int:
    if dry_run:
        print("=== DRY RUN (would push to Discord) ===", file=sys.stderr)
        print(message, file=sys.stderr)
        return 0

    webhook = read_webhook()
    if not webhook:
        print("[discord_daily_summary] No DISCORD_WEBHOOK_URL configured.", file=sys.stderr)
        return 1

    # Wrap in code block
    payload = {"content": f"```\n{message}\n```"}
    try:
        import requests
        resp = requests.post(webhook, json=payload, timeout=10)
        if resp.status_code != 204:
            print(f"[discord_daily_summary] Discord status {resp.status_code}: {resp.text[:200]}",
                  file=sys.stderr)
            return 1
        print(f"[discord_daily_summary] Pushed {len(message)} chars to Discord.", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"[discord_daily_summary] Push failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="不 push,印到 stderr")
    args = ap.parse_args()

    summary = build_summary()
    return push_to_discord(summary, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
