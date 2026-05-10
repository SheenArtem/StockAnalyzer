"""
System 3 SPX gap-down alert — 8th stage of run_taifex_signals_afterclose.bat.

Per S3-b verdict 2026-05-09 (reports/system3_spx_gap_ic_validation_2026-05-09.md):
SPX 1d return is MARGINAL on SOP-12 strict gates but conditional lift @ <=-3% is
8.45x baseline (fwd 20d hit -10% probability) and direct gap-down test shows
P(TWII gap <= -1%) = 54.2% vs baseline 2.5% (22x lift) — qualifies SOP-14.

Why this exists alongside S3-a (^MOVE): Jaccard=0.060 极低重叠.
  - SPX catches equity-side concurrent shocks (tariff exec / 財報炸 / risk-off)
  - ^MOVE catches bond-side leading shocks (Treasury vol 提前 2-15 TD)
  - ma_dist_60 catches slow rolling drawdown lead 22-30d

Behavior:
  - Read fred_panel.parquet → SP500_close (FRED SP500 series, 2010+)
  - Compute today's 1d return = (close - prev_close) / prev_close
  - 4 levels: green > -1.5% / yellow -1.5% to -2.5% / orange -2.5% to -3% / red <= -3%
  - Discord push on FIRST cross into yellow / orange / red within 3 TD cooldown
    (shorter than ^MOVE/ma_dist_60: shock concurrent 連續觸發是 real risk amplification)
  - State file: data/sentiment/system3_spx_last_alert.json

SOP-14 informational tier:
  - SPX 1d shock is CONFIRMATION grade, not anticipation
  - "TW 開盤前 brace gap-down" warning, not "提前一週減碼" trigger
  - Optimal push timing: 08:30 TPE (NY close +6.5h, before TW 09:00 open)

Usage:
  python tools/system3_spx_check.py [--dry-run] [--as-of YYYY-MM-DD] [--force]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FRED_PANEL_PATH = ROOT / "data" / "macro" / "fred_panel.parquet"
STATE_PATH = ROOT / "data" / "sentiment" / "system3_spx_last_alert.json"
ENV_FILE = ROOT / "local" / ".env"

# Thresholds: SPX 1d return (negative); lower = worse
THRESHOLDS = {
    "yellow": -0.015,   # 1d <= -1.5%
    "orange": -0.025,   # 1d <= -2.5%
    "red":    -0.030,   # 1d <= -3.0%
}
LEVEL_RANK = {"green": 0, "yellow": 1, "orange": 2, "red": 3}
COOLDOWN_DAYS = 3   # Calendar days; shorter than ^MOVE 60d (concurrent shocks chain)

# Conditional lift stats from validate_spx_gap_shock_ic.py POC
COND_STATS = {
    "yellow": {"hit_20d_neg10": 11.7, "fwd_20d_med": -2.16, "lift": 2.66, "n_sample": 137,
               "twii_gap_neg1_prob": 18.2},
    "orange": {"hit_20d_neg10": 28.2, "fwd_20d_med": -2.77, "lift": 6.36, "n_sample": 39,
               "twii_gap_neg1_prob": 41.0},
    "red":    {"hit_20d_neg10": 37.5, "fwd_20d_med": -4.09, "lift": 8.45, "n_sample": 24,
               "twii_gap_neg1_prob": 54.2},
}
BASELINE_HIT_20D_NEG10 = 4.4
BASELINE_TWII_GAP_NEG1 = 2.5


def read_webhook() -> str | None:
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("DISCORD_WEBHOOK_URL="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def load_spx() -> pd.Series:
    df = pd.read_parquet(FRED_PANEL_PATH)
    # fred_panel uses int range index + separate 'date' column
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
    else:
        df.index = pd.to_datetime(df.index)
    for col in ["SP500_close", "sp500_close", "SP500", "sp500"]:
        if col in df.columns:
            return df[col].sort_index().astype(float).dropna()
    raise KeyError(f"SP500 column not found in fred_panel; cols: {list(df.columns)[:20]}")


def classify(ret_1d: float) -> str:
    if ret_1d <= THRESHOLDS["red"]:
        return "red"
    if ret_1d <= THRESHOLDS["orange"]:
        return "orange"
    if ret_1d <= THRESHOLDS["yellow"]:
        return "yellow"
    return "green"


def compute_today_state(spx: pd.Series, as_of: pd.Timestamp | None = None) -> dict:
    if as_of is not None:
        avail = spx.index[spx.index <= as_of]
        if len(avail) == 0:
            raise ValueError(f"No SPX bar at or before {as_of.date()}")
        as_of = avail[-1]
        sub = spx.loc[:as_of]
    else:
        sub = spx
    if len(sub) < 2:
        return {"ret_1d": None, "level": "unknown"}

    today = sub.index[-1]
    today_close = float(sub.iloc[-1])
    prev_close = float(sub.iloc[-2])
    today_ret = (today_close - prev_close) / prev_close

    yesterday_ret = None
    yesterday_level = None
    if len(sub) >= 3:
        prev_prev_close = float(sub.iloc[-3])
        yesterday_ret = (prev_close - prev_prev_close) / prev_prev_close
        yesterday_level = classify(yesterday_ret)

    return {
        "today": today,
        "spx_close": today_close,
        "spx_prev_close": prev_close,
        "ret_1d": today_ret,
        "level": classify(today_ret),
        "yesterday_ret": yesterday_ret,
        "yesterday_level": yesterday_level,
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
        # First-day-with-history: still alert if level >= yellow (green→yellow on first day means escalation)
        return True, None
    y_rank = LEVEL_RANK[y_level]
    # Alert on escalation OR same-level repeat (concurrent shock chain is itself a signal)
    if today_rank < y_rank:
        return False, "level decreased"

    last_alert_level = prev_state.get("last_alert_level")
    last_alert_date_str = prev_state.get("last_alert_date")
    if last_alert_level and last_alert_date_str:
        try:
            last_dt = pd.Timestamp(last_alert_date_str)
            days_since = (today_state["today"] - last_dt).days
            # 3-day cooldown: only suppress same-level re-fire
            if days_since < COOLDOWN_DAYS and LEVEL_RANK.get(last_alert_level, 0) >= today_rank:
                return False, f"cooldown ({days_since}d < {COOLDOWN_DAYS})"
        except Exception:
            pass

    return True, None


def format_alert(state: dict) -> str:
    level = state["level"]
    ret_1d = state["ret_1d"]
    spx_close = state["spx_close"]

    level_emoji = {"yellow": "[!] YELLOW", "orange": "[!!] ORANGE", "red": "[!!!] RED"}[level]
    level_zh = {"yellow": "黃燈 SPX 1d 急殺",
                "orange": "橘燈 SPX 1d 重摔",
                "red": "紅燈 SPX 1d 崩跌"}[level]

    cs = COND_STATS[level]

    return (
        f"[System 3 SPX] 美股急殺 - TW 開盤前預警 {level_emoji} - {state['today'].date()}\n"
        f"  S&P 500 close : {spx_close:.2f}\n"
        f"  1d return     : {ret_1d:+.2%}  (level: {level_zh})\n"
        f"\n"
        f"Historical conditional stats (^TWII fwd 20d, n={cs['n_sample']}):\n"
        f"  fwd_20d MDD <=-10% : {cs['hit_20d_neg10']:.0f}% (baseline {BASELINE_HIT_20D_NEG10:.0f}%)\n"
        f"  fwd_20d MDD median : {cs['fwd_20d_med']:.2f}%\n"
        f"  conditional lift   : {cs['lift']:.2f}x baseline\n"
        f"\n"
        f"Direct TW gap-down test:\n"
        f"  P(TWII open gap <=-1% next day): {cs['twii_gap_neg1_prob']:.0f}% "
        f"(baseline {BASELINE_TWII_GAP_NEG1:.0f}%)\n"
        f"\n"
        f"NOTE: SOP-14 informational. SPX 1d shock is confirmation-grade,\n"
        f"      not anticipation. \"TW 開盤 brace gap-down\" warning;\n"
        f"      do NOT auto-rebalance. Pair with banner D + System 2 +\n"
        f"      System 3 ma_dist_60 + System 3 MOVE for triangulation.\n"
        f"      Cooldown 3d (shorter than ^MOVE 60d: concurrent shocks chain).\n"
    )


def push_discord(message: str, dry_run: bool) -> int:
    if dry_run:
        print("=== DRY RUN ===", file=sys.stderr)
        print(message, file=sys.stderr)
        return 0
    webhook = read_webhook()
    if not webhook:
        print("[system3_spx] No DISCORD_WEBHOOK_URL configured.", file=sys.stderr)
        return 1
    try:
        import requests
        resp = requests.post(webhook, json={"content": f"```\n{message}\n```"}, timeout=10)
        if resp.status_code != 204:
            print(f"[system3_spx] Discord status {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
            return 1
        print(f"[system3_spx] Pushed {len(message)} chars to Discord.", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"[system3_spx] Push failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--as-of", default=None)
    ap.add_argument("--force", action="store_true", help="ignore cooldown")
    args = ap.parse_args()

    spx = load_spx()
    as_of = pd.Timestamp(args.as_of) if args.as_of else None
    state = compute_today_state(spx, as_of=as_of)

    if state["ret_1d"] is None:
        print("[system3_spx] Insufficient history; silent.")
        return 0

    print(f"[system3_spx] today={state['today'].date()} spx={state['spx_close']:.2f} "
          f"1d={state['ret_1d']*100:+.2f}% level={state['level']}")
    if state["yesterday_level"]:
        print(f"[system3_spx] yesterday level={state['yesterday_level']} "
              f"(1d={state['yesterday_ret']*100:+.2f}%)")

    prev_state = {} if args.force else load_state()
    fire, reason = should_alert(state, prev_state)
    if not fire:
        print(f"[system3_spx] no alert ({reason or 'green'}); silent")
        return 0

    msg = format_alert(state)
    rc = push_discord(msg, args.dry_run)

    if not args.dry_run:
        save_state({
            "last_alert_level": state["level"],
            "last_alert_date": state["today"].strftime("%Y-%m-%d"),
            "ret_1d": state["ret_1d"],
            "spx_close": state["spx_close"],
        })
    return rc


if __name__ == "__main__":
    sys.exit(main())
