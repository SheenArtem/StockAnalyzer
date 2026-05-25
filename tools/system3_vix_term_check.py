"""
System 3 VIX term structure backwardation alert.

vix_vix3m_ratio IC 驗證結果 (vs ^TWII fwd 20d MDD, panel 4565 days 2007-2026):
  baseline hit -10% = 6.7%
  ratio >= 0.95 → 13.4% hit, 2.01x lift
  ratio >= 1.00 → 16.2% hit, 2.42x lift  (backwardation)
  ratio >= 1.05 → 27.1% hit, 4.06x lift  (急性恐慌, n=214)

Why this exists:
  4 訊號 IC 驗證後唯一 MARGINAL → SOP-14 informational：
  vix_vix3m_ratio 是 vol complex 4 訊號裡唯一通過台股 conditional lift 門檻的
  單訊號 (4.06x at red 比 System 3 MOVE 3.43x 還強)。獨立警報路徑：
  archive_vol_complex 的 composite「2 燈 3 燈」IC 只 1.28x，弱位階；
  vix_term 4x lift，獨立佔一格 stage。

Behavior:
  - Read data/sentiment/vol_complex_history.parquet (archive_vol_complex 寫的)
  - 級別: yellow >= 0.95 / orange >= 1.00 / red >= 1.05
  - Discord push 只在「級別 upgrade」時觸發 (cooldown 60 calendar days)
  - State file: data/sentiment/system3_vix_term_last_alert.json

SOP-14 tier:
  - Discord 警報文字加重 (4.06x lift 是真訊號)
  - 但 **不接 auto-rebalance** — vix3m → TWII 經 risk-off flow 間接
  - 與 banner D + System 2 trigger + System 3 MOVE 三角驗證

Usage:
  python tools/system3_vix_term_check.py [--dry-run] [--as-of YYYY-MM-DD] [--force]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
VC_PATH = ROOT / "data" / "sentiment" / "vol_complex_history.parquet"
STATE_PATH = ROOT / "data" / "sentiment" / "system3_vix_term_last_alert.json"
ENV_FILE = ROOT / "local" / ".env"

THRESHOLDS = {
    "yellow": 0.95,
    "orange": 1.00,
    "red":    1.05,
}
LEVEL_RANK = {"green": 0, "yellow": 1, "orange": 2, "red": 3}
COOLDOWN_DAYS = 60

# Conditional lift stats from validate_vol_complex_ic.py (n=4565 panel)
COND_STATS = {
    "yellow": {"hit_20d_neg10": 13.4, "fwd_20d_med": -2.33, "lift": 2.01, "n_sample": 1194},
    "orange": {"hit_20d_neg10": 16.2, "fwd_20d_med": -2.45, "lift": 2.42, "n_sample": 513},
    "red":    {"hit_20d_neg10": 27.1, "fwd_20d_med": -3.03, "lift": 4.06, "n_sample": 214},
}
BASELINE_HIT_20D_NEG10 = 6.7


def read_webhook() -> str | None:
    """讀 DISCORD_WEBHOOK_MACRO (優先) 或 DISCORD_WEBHOOK_URL；fallback local/.env。"""
    env = os.environ.get('DISCORD_WEBHOOK_MACRO') or os.environ.get('DISCORD_WEBHOOK_URL')
    if env:
        return env
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        for key in ('DISCORD_WEBHOOK_MACRO=', 'DISCORD_WEBHOOK_URL='):
            if line.startswith(key):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def classify(ratio: float) -> str:
    if ratio is None or pd.isna(ratio):
        return "green"
    if ratio >= THRESHOLDS["red"]:
        return "red"
    if ratio >= THRESHOLDS["orange"]:
        return "orange"
    if ratio >= THRESHOLDS["yellow"]:
        return "yellow"
    return "green"


def compute_today_state(as_of: pd.Timestamp | None = None) -> dict:
    df = pd.read_parquet(VC_PATH)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    if as_of is not None:
        df = df[df['date'] <= as_of].reset_index(drop=True)
    if len(df) < 2:
        return {"ratio": None, "level": "unknown"}

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    return {
        "today": latest['date'],
        "ratio": float(latest['vix_vix3m_ratio']),
        "vix": float(latest['vix']),
        "vix3m": float(latest['vix3m']),
        "level": classify(latest['vix_vix3m_ratio']),
        "yesterday_ratio": float(prev['vix_vix3m_ratio']),
        "yesterday_level": classify(prev['vix_vix3m_ratio']),
    }


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def should_alert(today_state: dict, prev_state: dict) -> tuple[bool, str | None]:
    today_level = today_state["level"]
    if today_level == "green":
        return False, None
    today_rank = LEVEL_RANK[today_level]

    y_level = today_state.get("yesterday_level")
    if y_level is None:
        return False, None
    y_rank = LEVEL_RANK[y_level]
    if today_rank <= y_rank:
        return False, "no escalation"

    last_alert_level = prev_state.get("last_alert_level")
    last_alert_date_str = prev_state.get("last_alert_date")
    if last_alert_level and last_alert_date_str:
        try:
            last_dt = pd.Timestamp(last_alert_date_str)
            days_since = (today_state["today"] - last_dt).days
            if days_since < COOLDOWN_DAYS and LEVEL_RANK.get(last_alert_level, 0) >= today_rank:
                return False, f"cooldown ({days_since}d < {COOLDOWN_DAYS})"
        except Exception:
            pass

    return True, None


def format_alert(state: dict) -> str:
    level = state["level"]
    ratio = state["ratio"]
    vix = state["vix"]
    vix3m = state["vix3m"]

    level_emoji = {"yellow": "[!] YELLOW", "orange": "[!!] ORANGE BACKWARDATION",
                   "red": "[!!!] RED ACUTE PANIC"}[level]
    level_zh = {"yellow": "黃燈 期限結構平坦化",
                "orange": "橘燈 期限結構倒掛 (backwardation)",
                "red": "紅燈 急性恐慌期"}[level]

    cs = COND_STATS[level]
    suggestion = {
        "yellow": "建議：留意 hedge sizing",
        "orange": "建議：減倉 30% (用戶 framework)，補 OTM put",
        "red": "建議：減倉 60% + 買保護性 put，僅留高 conviction",
    }[level]

    return (
        f"[System 3 VIX term] VIX 期限結構 {level_emoji} - {state['today'].date()}\n"
        f"  ^VIX           : {vix:.2f}\n"
        f"  ^VIX3M         : {vix3m:.2f}\n"
        f"  VIX/VIX3M 比   : {ratio:.4f}  (level: {level_zh})\n"
        f"\n"
        f"Historical conditional stats (^TWII fwd 20d, n={cs['n_sample']}):\n"
        f"  fwd_20d MDD <=-10% : {cs['hit_20d_neg10']:.1f}% (baseline {BASELINE_HIT_20D_NEG10:.1f}%)\n"
        f"  fwd_20d MDD median : {cs['fwd_20d_med']:.2f}%\n"
        f"  conditional lift   : {cs['lift']:.2f}x baseline\n"
        f"\n"
        f"{suggestion}\n"
        f"\n"
        f"Why this matters: VIX 期限結構是 4 vol 訊號中 IC 驗證唯一 MARGINAL，\n"
        f"  red 級 4.06x lift 比 System 3 MOVE red 3.43x 還強，台股單訊號最強。\n"
        f"\n"
        f"NOTE: SOP-14 informational. VIX3M -> ^TWII 經 risk-off flow 間接。\n"
        f"      Do NOT auto-rebalance on this alone. 與 banner D + System 2 trigger\n"
        f"      + System 3 MOVE 三角驗證再決定。\n"
    )


def push_discord(message: str, dry_run: bool) -> int:
    if dry_run:
        print("=== DRY RUN ===", file=sys.stderr)
        print(message, file=sys.stderr)
        return 0
    webhook = read_webhook()
    if not webhook:
        print("[system3_vix_term] No DISCORD_WEBHOOK_MACRO/URL configured.", file=sys.stderr)
        return 1
    try:
        import requests
        resp = requests.post(webhook, json={"content": f"```\n{message}\n```"}, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[system3_vix_term] Discord status {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
            return 1
        print(f"[system3_vix_term] Pushed {len(message)} chars to Discord.", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"[system3_vix_term] Push failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--as-of", default=None)
    ap.add_argument("--force", action="store_true", help="ignore cooldown + escalation gate")
    args = ap.parse_args()

    if not VC_PATH.exists():
        print(f"[system3_vix_term] {VC_PATH} not found; run archive_vol_complex first.")
        return 1

    as_of = pd.Timestamp(args.as_of) if args.as_of else None
    state = compute_today_state(as_of=as_of)

    if state["ratio"] is None:
        print("[system3_vix_term] Insufficient history; silent.")
        return 0

    print(f"[system3_vix_term] today={state['today'].date()} "
          f"vix={state['vix']:.2f} vix3m={state['vix3m']:.2f} "
          f"ratio={state['ratio']:.4f} level={state['level']}")
    print(f"[system3_vix_term] yesterday ratio={state['yesterday_ratio']:.4f} "
          f"level={state['yesterday_level']}")

    if args.force and state['level'] != 'green':
        fire, reason = True, "force"
    else:
        prev_state = load_state()
        fire, reason = should_alert(state, prev_state)

    if not fire:
        print(f"[system3_vix_term] no alert ({reason or 'green'}); silent")
        return 0

    msg = format_alert(state)
    rc = push_discord(msg, args.dry_run)

    if not args.dry_run:
        save_state({
            "last_alert_level": state["level"],
            "last_alert_date": state["today"].strftime("%Y-%m-%d"),
            "ratio": state["ratio"],
            "vix": state["vix"],
            "vix3m": state["vix3m"],
        })
    return rc


if __name__ == "__main__":
    sys.exit(main())
