"""
System 3 MOVE shock alert — 7th stage of run_taifex_signals_afterclose.bat.

Per S3-a verdict 2026-05-09 (reports/system3_move5d_ic_validation_2026-05-09.md):
^MOVE 5d delta z-score (252d rolling baseline) is MARGINAL on SOP-12 strict gates
but conditional lift @ z>=3 is 3.43x baseline (fwd 20d hit -10% probability) —
qualifies for SOP-14 informational tier.

Why this exists: System 3 (ma_dist_60 rolling rank) misses 40% of crashes —
COVID 2020-02 / Trump tariff 2025-03 are pure shocks ma_dist_60 cannot anticipate.
^MOVE catches Treasury vol regime shifts that often lead equity selloffs.

Behavior:
  - Read data_cache/fred/move.parquet (yfinance ^MOVE, 22 yr hist)
  - Compute 5d delta z-score against 252d rolling mean/std of 5d deltas
  - 4 levels: green <1.5 / yellow 1.5-2.5 / orange 2.5-3.0 / red >=3.0
  - Discord push on FIRST cross into yellow / orange / red within 60-day cooldown
  - State file: data/sentiment/system3_move_last_alert.json

SOP-14 tier:
  - Discord alert is "watch for hedge sizing" -- DO NOT auto-rebalance
  - ^MOVE -> ^TWII transmission is indirect (USD/TWD + risk-off equity flows)
  - Pair with banner D (ma_dist_60) + System 2 (drawdown trigger) for triangulation
  - At z>=3.0, fwd 20d hit -10% prob = 31% (3.43x baseline 9%) — actionable scarce signal

Usage:
  python tools/system3_move_check.py [--dry-run] [--as-of YYYY-MM-DD] [--force]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MOVE_PATH = ROOT / "data_cache" / "fred" / "move.parquet"
STATE_PATH = ROOT / "data" / "sentiment" / "system3_move_last_alert.json"
ENV_FILE = ROOT / "local" / ".env"

THRESHOLDS = {
    "yellow": 1.5,
    "orange": 2.5,
    "red": 3.0,
}
LEVEL_RANK = {"green": 0, "yellow": 1, "orange": 2, "red": 3}
COOLDOWN_DAYS = 60   # Calendar days; same regime won't re-fire within this window

# Conditional lift stats from validate_move_5d_shock_ic.py POC
COND_STATS = {
    "yellow": {"hit_20d_neg10": 13.2, "fwd_20d_med": -2.43, "lift": 1.45, "n_sample": 363},
    "orange": {"hit_20d_neg10": 22.9, "fwd_20d_med": -2.67, "lift": 2.51, "n_sample": 131},
    "red":    {"hit_20d_neg10": 31.2, "fwd_20d_med": -4.30, "lift": 3.43, "n_sample": 64},
}
BASELINE_HIT_20D_NEG10 = 9.1


def read_webhook() -> str | None:
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("DISCORD_WEBHOOK_URL="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def load_move() -> pd.Series:
    df = pd.read_parquet(MOVE_PATH)
    df.index = pd.to_datetime(df.index)
    s = df["move"].sort_index().astype(float)
    return s


def classify(z: float) -> str:
    if z >= THRESHOLDS["red"]:
        return "red"
    if z >= THRESHOLDS["orange"]:
        return "orange"
    if z >= THRESHOLDS["yellow"]:
        return "yellow"
    return "green"


def compute_today_state(move: pd.Series, as_of: pd.Timestamp | None = None) -> dict:
    if as_of is not None:
        avail = move.index[move.index <= as_of]
        if len(avail) == 0:
            raise ValueError(f"No ^MOVE bar at or before {as_of.date()}")
        as_of = avail[-1]
        sub = move.loc[:as_of]
    else:
        sub = move
    if len(sub) < 260:  # need 252 + buffer
        return {"z": None, "level": "unknown"}

    delta_5d = sub.diff(5)
    z_series = (delta_5d - delta_5d.rolling(252).mean()) / delta_5d.rolling(252).std()
    today = sub.index[-1]
    today_z = float(z_series.iloc[-1])
    yesterday_z = float(z_series.iloc[-2]) if len(z_series) > 1 else None
    return {
        "today": today,
        "move_close": float(sub.iloc[-1]),
        "move_5d_delta": float(delta_5d.iloc[-1]),
        "z": today_z,
        "level": classify(today_z),
        "yesterday_z": yesterday_z,
        "yesterday_level": classify(yesterday_z) if yesterday_z is not None else None,
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
    z = state["z"]
    move_close = state["move_close"]
    delta_5d = state["move_5d_delta"]

    level_emoji = {"yellow": "[!] YELLOW", "orange": "[!!] ORANGE", "red": "[!!!] RED"}[level]
    level_zh = {"yellow": "黃燈 國債波動上升",
                "orange": "橘燈 國債波動偏高",
                "red": "紅燈 國債波動極端"}[level]

    cs = COND_STATS[level]

    return (
        f"[System 3 MOVE] 國債波動 shock {level_emoji} - {state['today'].date()}\n"
        f"  ^MOVE close   : {move_close:.2f}\n"
        f"  5d Δ          : {delta_5d:+.2f}\n"
        f"  z-score (252d): {z:+.2f}  (level: {level_zh})\n"
        f"\n"
        f"Historical conditional stats (^TWII fwd 20d, n={cs['n_sample']}):\n"
        f"  fwd_20d MDD <=-10% : {cs['hit_20d_neg10']:.0f}% (baseline {BASELINE_HIT_20D_NEG10:.0f}%)\n"
        f"  fwd_20d MDD median : {cs['fwd_20d_med']:.2f}%\n"
        f"  conditional lift   : {cs['lift']:.2f}x baseline\n"
        f"\n"
        f"Why this matters: ma_dist_60 (System 3 main) is slow rolling rank.\n"
        f"  At known shocks COVID 2020 / Trump 2025-03, ma_dist_60 missed but\n"
        f"  ^MOVE 5d Δ caught (lead 15 / 2 trading days).\n"
        f"\n"
        f"NOTE: SOP-14 informational. ^MOVE -> ^TWII transmission is indirect.\n"
        f"      Suggestion: consider hedge sizing; do NOT auto-rebalance on this alone.\n"
        f"      Pair with banner D + System 2 trigger + System 3 ma_dist_60.\n"
    )


def push_discord(message: str, dry_run: bool) -> int:
    if dry_run:
        print("=== DRY RUN ===", file=sys.stderr)
        print(message, file=sys.stderr)
        return 0
    webhook = read_webhook()
    if not webhook:
        print("[system3_move] No DISCORD_WEBHOOK_URL configured.", file=sys.stderr)
        return 1
    try:
        import requests
        resp = requests.post(webhook, json={"content": f"```\n{message}\n```"}, timeout=10)
        if resp.status_code != 204:
            print(f"[system3_move] Discord status {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
            return 1
        print(f"[system3_move] Pushed {len(message)} chars to Discord.", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"[system3_move] Push failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--as-of", default=None)
    ap.add_argument("--force", action="store_true", help="ignore cooldown")
    args = ap.parse_args()

    move = load_move()
    as_of = pd.Timestamp(args.as_of) if args.as_of else None
    state = compute_today_state(move, as_of=as_of)

    if state["z"] is None:
        print("[system3_move] Insufficient history; silent.")
        return 0

    print(f"[system3_move] today={state['today'].date()} move={state['move_close']:.2f} "
          f"5d_delta={state['move_5d_delta']:+.2f} z={state['z']:+.2f} level={state['level']}")
    print(f"[system3_move] yesterday level={state['yesterday_level']} (z={state['yesterday_z']:+.2f})")

    prev_state = {} if args.force else load_state()
    fire, reason = should_alert(state, prev_state)
    if not fire:
        print(f"[system3_move] no alert ({reason or 'green'}); silent")
        return 0

    msg = format_alert(state)
    rc = push_discord(msg, args.dry_run)

    if not args.dry_run:
        save_state({
            "last_alert_level": state["level"],
            "last_alert_date": state["today"].strftime("%Y-%m-%d"),
            "z": state["z"],
            "move_close": state["move_close"],
        })
    return rc


if __name__ == "__main__":
    sys.exit(main())
