"""
Step-A Daily Engine (Mode D Phase 2 Wave 2 #4, 2026-04-25)

讀 position_alerts.json + qm_result.json + value_result.json + regime_log
套用 Step-A 8 條 contract 規則 → 產出統一 daily_alerts.json
供 Wave 3 #5 Scanner / #6 Paper engine 消費。

Council R2 (2026-04-25) 共識實作要點:
- Hard alerts (Rule 1)            → action_type=forced  (立即出場,不等 rebalance)
- Rebalance drop top_20 (Rule 2)  → action_type=suggested (不強制,人工拍板)
- Soft alerts (Rule 3)            → action_type=info (純通知,累積到 rebalance)
- MIN_HOLD_DAYS=20 (Rule 4)       → 軟 badge 不擋出場 (Hard exit 永遠豁免)
- Whipsaw ban 30d (Rule 5)        → ban_list (Wave 3 paper engine 補完整)
- Rule 6/7                         → Wave 3 / Phase 3 補

不改 position_monitor.py 既有 alerts; 此 engine 是統合層。Schema_version 1 凍結。

Usage:
  python tools/step_a_engine.py            # 跑一次
  python tools/step_a_engine.py --dry-run  # 不寫檔只印
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
POSITIONS_FILE = REPO / "data" / "positions.json"
ALERTS_IN = REPO / "data" / "latest" / "position_alerts.json"
QM_RESULT = REPO / "data" / "latest" / "qm_result.json"
VALUE_RESULT = REPO / "data" / "latest" / "value_result.json"
REGIME_LOG = REPO / "data" / "tracking" / "regime_log.jsonl"
DAILY_ALERTS_OUT = REPO / "data" / "latest" / "daily_alerts.json"

SCHEMA_VERSION = 1
MIN_HOLD_DAYS = 20    # Rule 4
WHIPSAW_BAN_DAYS = 30 # Rule 5
TOP_N = 20            # Rule 2 top_20 threshold


def load_json(p: Path, default=None):
    if not p.exists():
        return default
    try:
        with open(p, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[WARN] load_json {p}: {e}", file=sys.stderr)
        return default


def load_today_regime() -> str:
    """讀 regime_log.jsonl 最後一筆,失敗回 'unknown'"""
    if not REGIME_LOG.exists():
        return "unknown"
    try:
        last_line = ""
        for line in REGIME_LOG.read_text(encoding='utf-8').splitlines():
            if line.strip():
                last_line = line
        if not last_line:
            return "unknown"
        d = json.loads(last_line)
        return str(d.get("regime", "unknown"))
    except Exception as e:
        print(f"[WARN] load_today_regime: {e}", file=sys.stderr)
        return "unknown"


def is_rebalance_day(today: date) -> bool:
    """月第 1 個交易日 = 簡化判定: day-of-month <= 5 且為 weekday"""
    if today.weekday() >= 5:  # Sat/Sun
        return False
    return today.day <= 5


def days_between(d1_str: str, d2: date) -> int:
    """buy_date YYYY-MM-DD 到 d2 的日曆日 (簡化,非交易日)"""
    try:
        d1 = datetime.strptime(d1_str, "%Y-%m-%d").date()
        return (d2 - d1).days
    except Exception:
        return 0


def categorize_existing_alerts(alerts: list[dict]) -> list[dict]:
    """把 position_alerts.json 的 alerts 分類成 daily_alerts schema."""
    out = []
    for a in alerts:
        sev = a.get("severity", "soft")
        triggers = a.get("triggers", []) or []
        # take first trigger for primary categorization
        primary = triggers[0] if triggers else {}
        category = primary.get("type", "unknown")
        desc = primary.get("desc", "")

        if sev == "hard":
            action_type = "forced"
            rule = "Rule 1 - Hard exit"
            severity = "critical"
            recommendation = "當日收盤前出場 (不等 rebalance)"
        else:  # soft
            action_type = "info"
            rule = "Rule 3 - Soft alert (累積到 rebalance)"
            severity = "warning"
            recommendation = "心裡有數,rebalance 日一併審視"

            # Special: TP 停利獨立處理 (不等 rebalance)
            if category == "take_profit":
                action_type = "suggested"
                rule = "Rule 3b - TP 三段式減碼"
                recommendation = "TP1 減 1/3 / TP2 再減 1/3 / TP3 全出"

        out.append({
            "ticker": str(a.get("stock_id", "")),
            "name": a.get("name", ""),
            "side": "QM",  # 目前 positions 都是 QM,Value side Wave 3 補
            "days_held": int(a.get("hold_days", 0)),
            "buy_price": a.get("buy_price"),
            "current_price": a.get("current_price"),
            "pnl_pct": a.get("pnl_pct"),
            "action_type": action_type,
            "category": category,
            "rule": rule,
            "severity": severity,
            "reason": desc,
            "recommendation": recommendation,
            "min_hold_warning": int(a.get("hold_days", 0)) < MIN_HOLD_DAYS,
            "whipsaw_banned": False,  # Wave 3 paper engine 補
        })
    return out


def add_rebalance_drops(positions: list[dict], qm_picks: list[dict],
                        existing_tickers: set, is_reb_day: bool,
                        today: date) -> list[dict]:
    """Rule 2: 當前持股若跌出當日 top_20 且 days_held >= MIN_HOLD → suggested 換股."""
    out = []
    if not is_reb_day:
        return out
    qm_top_n_tickers = {str(p.get("stock_id", ""))
                        for p in (qm_picks or [])[:TOP_N]}
    for pos in positions:
        sid = str(pos.get("stock_id", ""))
        if sid in existing_tickers:
            continue  # 已有 hard/soft alert,不重複
        if sid in qm_top_n_tickers:
            continue  # 仍在榜上,留
        days_held = days_between(pos.get("buy_date", ""), today)
        if days_held < MIN_HOLD_DAYS:
            continue  # Rule 4: min hold 保護,不換
        # 跌出 top_20 + 過 min hold → suggested 換股
        out.append({
            "ticker": sid,
            "name": pos.get("name", ""),
            "side": "QM",
            "days_held": days_held,
            "buy_price": pos.get("buy_price"),
            "current_price": None,
            "pnl_pct": None,
            "action_type": "suggested",
            "category": "rebalance_drop_top20",
            "rule": "Rule 2 - Rebalance drop top_20",
            "severity": "warning",
            "reason": f"跌出當日 QM top_{TOP_N},days_held={days_held} >= {MIN_HOLD_DAYS}",
            "recommendation": "建議換股 (人工拍板,council R2 改 suggested 不強制)",
            "min_hold_warning": False,
            "whipsaw_banned": False,
        })
    return out


def build_daily_alerts(today: date | None = None) -> dict:
    today = today or date.today()
    is_reb = is_rebalance_day(today)
    regime = load_today_regime()

    pos_data = load_json(POSITIONS_FILE, {"positions": []}) or {"positions": []}
    positions = pos_data.get("positions", [])

    alerts_data = load_json(ALERTS_IN, {"alerts": []}) or {"alerts": []}
    existing_alerts = alerts_data.get("alerts", [])

    qm_data = load_json(QM_RESULT, {"results": []}) or {"results": []}
    qm_picks = qm_data.get("results", [])

    # Step 1: existing 7 alerts → forced/info/suggested
    primary_alerts = categorize_existing_alerts(existing_alerts)
    primary_tickers = {a["ticker"] for a in primary_alerts}

    # Step 2: rebalance drop top_20 (Rule 2, suggested)
    rebalance_drops = add_rebalance_drops(
        positions, qm_picks, primary_tickers, is_reb, today
    )

    all_alerts = primary_alerts + rebalance_drops

    forced_count = sum(1 for a in all_alerts if a["action_type"] == "forced")
    suggested_count = sum(1 for a in all_alerts if a["action_type"] == "suggested")
    info_count = sum(1 for a in all_alerts if a["action_type"] == "info")

    return {
        "schema_version": SCHEMA_VERSION,
        "scan_date": str(today),
        "scan_time": datetime.now().strftime("%H:%M"),
        "regime": regime,
        "is_rebalance_day": is_reb,
        "position_count": len(positions),
        "alert_count": len(all_alerts),
        "forced_count": forced_count,
        "suggested_count": suggested_count,
        "info_count": info_count,
        "alerts": all_alerts,
        "ban_list": [],  # Rule 5 Wave 3 paper engine 補
        "notes": {
            "rule_5_whipsaw_ban": "Wave 3 #6 paper engine 落地後從 trade history 算 30d ban",
            "rule_6_dual_side": "Value side 持股實際出現後啟用 (目前無)",
            "rule_7_regime_switch": "Phase 3 補 (regime 切換 lag 1 月清倉)",
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="不寫檔,印到 stderr")
    ap.add_argument("--date", help="指定日期 YYYY-MM-DD (test 用)")
    args = ap.parse_args()

    today = date.today()
    if args.date:
        try:
            today = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"[ERROR] invalid --date: {args.date}", file=sys.stderr)
            return 2

    result = build_daily_alerts(today)

    print(f"=== Step-A Daily Engine ({today}) ===", file=sys.stderr)
    print(f"  regime: {result['regime']}", file=sys.stderr)
    print(f"  rebalance_day: {result['is_rebalance_day']}", file=sys.stderr)
    print(f"  positions: {result['position_count']}", file=sys.stderr)
    print(f"  alerts: forced={result['forced_count']} "
          f"suggested={result['suggested_count']} info={result['info_count']}",
          file=sys.stderr)

    if args.dry_run:
        print(json.dumps(result, ensure_ascii=False, indent=2), file=sys.stderr)
        return 0

    DAILY_ALERTS_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(DAILY_ALERTS_OUT, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Written: {DAILY_ALERTS_OUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
