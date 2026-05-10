"""
System 3 daily check - 1w-1mo crash early-warning Discord push.

Per Phase 3.4 verdict 2026-05-09 (commit pending): single ma_dist_60 rolling
252d rank is best gating policy (Sharpe 0.898, MDD -19.5% vs B&H -31.6%).
Composite (vix_term + move_level + rv_20d + ma_dist_60) failed to beat single
feature on Sharpe -- ship single-feature gauge.

Behavior:
  - Read latest TAIEX, compute today's ma_dist_60 rolling-252d rank
  - 4 levels: green <0.65 / yellow 0.65-0.85 / orange 0.85-0.95 / red >=0.95
  - Push Discord on FIRST cross into yellow / orange / red within 60-day cooldown
  - State file: data/sentiment/system3_last_alert.json tracks last cross + cooldown

Lead-time honest disclosure (per Phase 3.4 audit, 70 events 1999-2026):
  - Yellow recall 59% (catches 41/70 crashes, median 30d lead, 68% are 22-30d ahead)
  - Red recall 36% (catches 25/70, similar lead distribution)
  - Misses 40% of crashes (sudden exogenous shocks like COVID 2020 / 2022 Fed / 2025-03)

SOP-14 informational tier:
  - Discord alert is "early warning to consider hedge / reduce" -- NOT auto-rebalance
  - Pair with banner D (now-state) + composite + HMM regime for triangulation

Usage:
  python tools/system3_daily_check.py [--dry-run] [--as-of YYYY-MM-DD] [--force]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from regime_extension import LEVEL_STATS, BASELINE_10PCT, BASELINE_5PCT, LEAD_RECALL  # noqa: E402

TAIEX_PATH = ROOT / "data_cache" / "TAIEX_price.parquet"
STATE_PATH = ROOT / "data" / "sentiment" / "system3_last_alert.json"
ENV_FILE = ROOT / "local" / ".env"

THRESHOLDS = {
    "yellow": 0.65,
    "orange": 0.85,
    "red": 0.95,
}
LEVEL_RANK = {"green": 0, "yellow": 1, "orange": 2, "red": 3}
COOLDOWN_DAYS = 60   # Trading days; same regime won't re-fire within this window


def read_webhook() -> str | None:
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("DISCORD_WEBHOOK_URL="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def load_taiex() -> pd.DataFrame:
    df = pd.read_parquet(TAIEX_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df.set_index("date")[["close"]].astype(float)


def classify(rank: float) -> str:
    if rank >= THRESHOLDS["red"]:
        return "red"
    if rank >= THRESHOLDS["orange"]:
        return "orange"
    if rank >= THRESHOLDS["yellow"]:
        return "yellow"
    return "green"


def compute_today_state(taiex: pd.DataFrame, as_of: pd.Timestamp | None = None) -> dict:
    close = taiex["close"]
    if as_of is not None:
        if as_of not in taiex.index:
            avail = taiex.index[taiex.index <= as_of]
            if len(avail) == 0:
                raise ValueError(f"No TAIEX bar at or before {as_of.date()}")
            as_of = avail[-1]
        sub = taiex.loc[:as_of]
    else:
        sub = taiex
    if len(sub) < 312:  # need 252 + 60 for stable rank
        return {"rank": None, "level": "unknown"}

    sub_close = sub["close"]
    ma60 = sub_close.rolling(60).mean()
    ma_dist = (sub_close - ma60) / ma60
    danger = -ma_dist
    rank_series = danger.rolling(252).rank(pct=True)
    today = sub.index[-1]
    today_rank = float(rank_series.iloc[-1])
    today_ma_dist = float(ma_dist.iloc[-1])
    yesterday_rank = float(rank_series.iloc[-2]) if len(rank_series) > 1 else None
    return {
        "today": today,
        "close": float(sub_close.iloc[-1]),
        "ma_dist_60": today_ma_dist,
        "rank": today_rank,
        "level": classify(today_rank),
        "yesterday_rank": yesterday_rank,
        "yesterday_level": classify(yesterday_rank) if yesterday_rank is not None else None,
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


def should_alert(today_state: dict, prev_state: dict, taiex: pd.DataFrame) -> tuple[bool, str | None]:
    """Decide if today's reading warrants a new Discord alert.

    Rules:
      - Alert when level *escalates* (today_rank > yesterday at threshold band)
      - Cooldown: don't re-alert same level within COOLDOWN_DAYS trading days
    """
    today_level = today_state["level"]
    if today_level == "green":
        return False, None
    today_rank = LEVEL_RANK[today_level]

    # Cross condition: yesterday was lower band
    y_level = today_state.get("yesterday_level")
    if y_level is None:
        return False, None
    y_rank = LEVEL_RANK[y_level]
    if today_rank <= y_rank:
        # Not an escalation (same or going down)
        return False, "no escalation"

    # Cooldown
    last_alert_level = prev_state.get("last_alert_level")
    last_alert_date_str = prev_state.get("last_alert_date")
    if last_alert_level and last_alert_date_str:
        try:
            last_dt = pd.Timestamp(last_alert_date_str)
            if last_dt in taiex.index:
                days_since = taiex.index.get_loc(today_state["today"]) - taiex.index.get_loc(last_dt)
                if days_since < COOLDOWN_DAYS and LEVEL_RANK.get(last_alert_level, 0) >= today_rank:
                    return False, f"cooldown ({days_since}d < {COOLDOWN_DAYS})"
        except Exception:
            pass

    return True, None


def format_alert(state: dict) -> str:
    level = state["level"]
    rank_pct = state["rank"] * 100
    md = state["ma_dist_60"]

    level_emoji = {"yellow": "[!] YELLOW", "orange": "[!!] ORANGE", "red": "[!!!] RED"}[level]
    level_zh = {"yellow": "黃燈 注意", "orange": "橘燈 偏高", "red": "紅燈 極端"}[level]

    s = LEVEL_STATS[level]
    co10 = s["co10"]
    co5 = s["co5"]
    mdd_med = s["mdd_median"]
    lead_recall = LEAD_RECALL[level]

    return (
        f"[System 3] 跌深延伸 (D) {level_emoji} - {state['today'].date()}\n"
        f"  TAIEX close   : {state['close']:.2f}\n"
        f"  ma_dist_60    : {md:+.2%}\n"
        f"  rolling-252d rank: {rank_pct:.0f}%  (level: {level_zh})\n"
        f"\n"
        f"Historical co-occurrence (TAIEX 1999-2026):\n"
        f"  60d MDD <=-10% : {co10:.0f}% (baseline {BASELINE_10PCT:.0f}%)\n"
        f"  60d MDD <=-5%  : {co5:.0f}% (baseline {BASELINE_5PCT:.0f}%)\n"
        f"  MDD median     : {mdd_med:.1f}%\n"
        f"\n"
        f"Lead time stats: this signal historically precedes\n"
        f"  ~60% of major crashes by 22-30 trading days (~1 month).\n"
        f"  Crash recall at this level: {lead_recall}\n"
        f"\n"
        f"NOTE: SOP-14 informational. Suggestion -- consider hedge sizing\n"
        f"      based on conviction; 40% of crashes (sudden shocks like\n"
        f"      COVID 2020 / 2022 Fed / 2025-03) WILL NOT trigger this.\n"
        f"      Pair with banner composite + HMM + System 2 alert.\n"
    )


def push_discord(message: str, dry_run: bool) -> int:
    if dry_run:
        print("=== DRY RUN ===", file=sys.stderr)
        print(message, file=sys.stderr)
        return 0
    webhook = read_webhook()
    if not webhook:
        print("[system3] No DISCORD_WEBHOOK_URL configured.", file=sys.stderr)
        return 1
    try:
        import requests
        resp = requests.post(webhook, json={"content": f"```\n{message}\n```"}, timeout=10)
        if resp.status_code != 204:
            print(f"[system3] Discord status {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
            return 1
        print(f"[system3] Pushed {len(message)} chars to Discord.", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"[system3] Push failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--as-of", default=None)
    ap.add_argument("--force", action="store_true", help="ignore cooldown")
    args = ap.parse_args()

    taiex = load_taiex()
    as_of = pd.Timestamp(args.as_of) if args.as_of else None
    state = compute_today_state(taiex, as_of=as_of)

    if state["rank"] is None:
        print("[system3] Insufficient history; silent.")
        return 0

    print(f"[system3] today={state['today'].date()} close={state['close']:.2f} "
          f"ma_dist_60={state['ma_dist_60']:+.2%} rank={state['rank']*100:.0f}% level={state['level']}")
    print(f"[system3] yesterday level={state['yesterday_level']} (rank={state['yesterday_rank']*100:.0f}%)")

    prev_state = {} if args.force else load_state()
    fire, reason = should_alert(state, prev_state, taiex)
    if not fire:
        print(f"[system3] no alert ({reason or 'green'}); silent")
        return 0

    msg = format_alert(state)
    rc = push_discord(msg, args.dry_run)

    if not args.dry_run:
        save_state({
            "last_alert_level": state["level"],
            "last_alert_date": state["today"].strftime("%Y-%m-%d"),
            "rank": state["rank"],
            "ma_dist_60": state["ma_dist_60"],
        })
    return rc


if __name__ == "__main__":
    sys.exit(main())
