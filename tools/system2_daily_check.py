"""
System 2 daily check - informational tier (SOP-14 PARTIAL).

Per Phase 2.5 verdict 2026-05-09: model beats binary baseline B (Sharpe +0.132,
MDD +18.7pp) but does not beat best-single-feature D. SOP-14 informational tier
chosen -- Discord push P(A)/P(B)/P(C) when -5% triggers, NO portfolio rebalance.

Behavior:
  1. Load latest TAIEX, compute drawdown from 60d rolling high
  2. If today's drawdown <= -5% AND no active 60d hold window:
       train multinomial logistic on full event history (76 events)
       compute today's features (ma_dist_60 + rv_20d)
       predict P(A_small) / P(B_medium) / P(C_crash)
       push Discord alert
       write state file with hold-until date
  3. Otherwise: silent (log only)

Usage:
  python tools/system2_daily_check.py [--dry-run] [--force]

Output:
  data/sentiment/system2_last_trigger.json  (state)
  Discord push (if triggered)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[1]
TAIEX_PATH = ROOT / "data_cache" / "TAIEX_price.parquet"
EVENTS_PATH = ROOT / "reports" / "system2_events.parquet"
FEATURES_PATH = ROOT / "reports" / "system2_features.parquet"
STATE_PATH = ROOT / "data" / "sentiment" / "system2_last_trigger.json"
ENV_FILE = ROOT / "local" / ".env"

WINDOW_DAYS = 60
TRIGGER = -0.05
HOLD_DAYS = 60
SELECTED_FEATURES = ["ma_dist_60", "rv_20d"]


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
    df = df.rename(columns={"max": "high", "min": "low", "Trading_Volume": "volume"})
    return df.set_index("date")[["open", "high", "low", "close", "volume"]]


def compute_today_features(taiex: pd.DataFrame, as_of: pd.Timestamp | None = None) -> tuple[pd.Timestamp, dict]:
    """Compute features as-of a given date (default = latest bar)."""
    close = taiex["close"]
    log_ret = np.log(close).diff()
    if as_of is not None:
        if as_of not in taiex.index:
            avail = taiex.index[taiex.index <= as_of]
            if len(avail) == 0:
                raise ValueError(f"No TAIEX bar at or before {as_of.date()}")
            as_of = avail[-1]
        idx = taiex.index.get_loc(as_of)
        sub = taiex.iloc[: idx + 1]
        close_s = sub["close"]
        log_ret_s = np.log(close_s).diff()
        today = sub.index[-1]
        ma60 = close_s.rolling(60).mean().iloc[-1]
        ma_dist_60 = (close_s.iloc[-1] - ma60) / ma60
        rv_20d = log_ret_s.rolling(20).std().iloc[-1] * np.sqrt(252)
        rolling_high = close_s.rolling(WINDOW_DAYS, min_periods=1).max().iloc[-1]
        drawdown = close_s.iloc[-1] / rolling_high - 1.0
        c_val = close_s.iloc[-1]
    else:
        today = taiex.index[-1]
        ma60 = close.rolling(60).mean().iloc[-1]
        ma_dist_60 = (close.iloc[-1] - ma60) / ma60
        rv_20d = log_ret.rolling(20).std().iloc[-1] * np.sqrt(252)
        rolling_high = close.rolling(WINDOW_DAYS, min_periods=1).max().iloc[-1]
        drawdown = close.iloc[-1] / rolling_high - 1.0
        c_val = close.iloc[-1]
    return today, {
        "today": today,
        "close": float(c_val),
        "rolling_60d_high": float(rolling_high),
        "drawdown": float(drawdown),
        "ma_dist_60": float(ma_dist_60),
        "rv_20d": float(rv_20d),
    }


def train_full_model() -> tuple[LogisticRegression, StandardScaler, list[str]]:
    feat = pd.read_parquet(FEATURES_PATH)
    feat = feat.dropna(subset=SELECTED_FEATURES)
    X = feat[SELECTED_FEATURES].to_numpy(dtype=float)
    y = feat["class"].to_numpy()
    scaler = StandardScaler().fit(X)
    Xs = scaler.transform(X)
    model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000)
    model.fit(Xs, y)
    return model, scaler, list(model.classes_)


def is_within_active_hold(today: pd.Timestamp) -> tuple[bool, str | None]:
    if not STATE_PATH.exists():
        return False, None
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return False, None
    expires = state.get("expires_after")
    if not expires:
        return False, None
    if pd.Timestamp(expires) >= today:
        return True, expires
    return False, expires


def write_state(trigger_date: pd.Timestamp, expires: pd.Timestamp, payload: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "trigger_date": trigger_date.strftime("%Y-%m-%d"),
        "expires_after": expires.strftime("%Y-%m-%d"),
        **payload,
    }
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def push_discord(message: str, dry_run: bool) -> int:
    if dry_run:
        print("=== DRY RUN ===", file=sys.stderr)
        print(message, file=sys.stderr)
        return 0
    webhook = read_webhook()
    if not webhook:
        print("[system2_daily_check] No DISCORD_WEBHOOK_URL configured.", file=sys.stderr)
        return 1
    try:
        import requests
        resp = requests.post(webhook, json={"content": f"```\n{message}\n```"}, timeout=10)
        if resp.status_code != 204:
            print(f"[system2_daily_check] Discord status {resp.status_code}: {resp.text[:200]}",
                  file=sys.stderr)
            return 1
        print(f"[system2_daily_check] Pushed {len(message)} chars to Discord.", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"[system2_daily_check] Push failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


def format_alert(feat: dict, proba: dict) -> str:
    p_a = proba["A_small"] * 100
    p_b = proba["B_medium"] * 100
    p_c = proba["C_crash"] * 100
    bars = ""
    for label, p in [("A 小回 [-5,-10)%", p_a), ("B 中度 [-10,-20)%", p_b), ("C 大崩 <=-20%", p_c)]:
        n = int(round(p / 5))
        bar = "#" * n + "-" * (20 - n)
        bars += f"  {label:24s} [{bar}] {p:5.1f}%\n"

    expected_class = max(proba.items(), key=lambda kv: kv[1])[0]
    risk_emoji = "[!] HIGH RISK" if p_c >= 50 else ("[*] elevated" if p_c >= 30 else "[.] base risk")
    return (
        f"[System 2] -5% trigger: {feat['today'].date()}\n"
        f"  TAIEX close   : {feat['close']:.2f}  (60d high {feat['rolling_60d_high']:.2f})\n"
        f"  drawdown      : {feat['drawdown']:.2%}\n"
        f"  ma_dist_60    : {feat['ma_dist_60']:.2%}  rv_20d: {feat['rv_20d']:.2%}\n"
        f"\n"
        f"Forward 60-day final-drawdown probability:\n"
        f"{bars}"
        f"\n"
        f"Most likely : {expected_class}    {risk_emoji}\n"
        f"\n"
        f"NOTE: informational only (SOP-14). Phase 2.5 verdict PARTIAL.\n"
        f"      Model historical false-negatives: 2020-01 / 2022-08 / 2025-03\n"
        f"      (predicted A_small but were actually C_crash -- modern regimes\n"
        f"      under-represented in training). Treat low P(C) with skepticism\n"
        f"      if other risk signals (banner v3, FGI, PCR) flash red.\n"
        f"      Do NOT auto-rebalance. See reports/system2_phase25_summary.md."
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="ignore state cache; re-fire even within hold")
    ap.add_argument("--as-of", default=None, help="YYYY-MM-DD for testing/replay; default = latest bar")
    args = ap.parse_args()

    taiex = load_taiex()
    as_of = pd.Timestamp(args.as_of) if args.as_of else None
    today, feat = compute_today_features(taiex, as_of=as_of)

    print(f"[system2] today={today.date()} close={feat['close']:.2f} drawdown={feat['drawdown']:.2%}")

    if feat["drawdown"] > TRIGGER:
        print(f"[system2] no trigger (drawdown {feat['drawdown']:.2%} > {TRIGGER:.0%}); silent")
        return 0

    if not args.force:
        active, expires = is_within_active_hold(today)
        if active:
            print(f"[system2] within active hold (expires {expires}); silent (extension rule)")
            return 0

    # Train + predict
    model, scaler, classes = train_full_model()
    x = np.array([[feat["ma_dist_60"], feat["rv_20d"]]], dtype=float)
    xs = scaler.transform(x)
    proba_arr = model.predict_proba(xs)[0]
    proba = {c: float(p) for c, p in zip(classes, proba_arr)}
    for c in ["A_small", "B_medium", "C_crash"]:
        proba.setdefault(c, 0.0)

    msg = format_alert(feat, proba)
    rc = push_discord(msg, args.dry_run)

    if not args.dry_run:
        expires_idx = min(taiex.index.get_loc(today) + HOLD_DAYS, len(taiex) - 1)
        expires_date = taiex.index[expires_idx]
        write_state(today, expires_date, {
            "p_A_small": proba["A_small"],
            "p_B_medium": proba["B_medium"],
            "p_C_crash": proba["C_crash"],
            "drawdown": feat["drawdown"],
            "ma_dist_60": feat["ma_dist_60"],
            "rv_20d": feat["rv_20d"],
        })
    return rc


if __name__ == "__main__":
    sys.exit(main())
