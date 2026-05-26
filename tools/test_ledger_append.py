"""Smoke test for whale_picks_ledger_append.py: end-to-end alert add lifecycle.

Verifies 3 scenarios using a backup-restored copy of the real ledger + holdings:
  (1) --alert-add appends 1 row + adds to alert_adds; new entry_type='alert'
  (2) --rebal --force when stock_id NOT in tickers -> force exit
  (3) --rebal --force when stock_id IS in tickers -> entry_type 'alert' -> 'upgraded'

Run:
    python tools/ui_smoke_ledger_append.py
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
LEDGER_PATH = REPO / "data" / "whale_picks" / "trade_ledger.parquet"
HOLDINGS_PATH = REPO / "data" / "whale_picks" / "_active_holdings.json"
LEDGER_BAK = LEDGER_PATH.with_suffix('.parquet.smoke_bak')
HOLDINGS_BAK = HOLDINGS_PATH.with_suffix('.json.smoke_bak')

PY = sys.executable


def backup():
    shutil.copy(LEDGER_PATH, LEDGER_BAK)
    shutil.copy(HOLDINGS_PATH, HOLDINGS_BAK)
    print(f"[SETUP] Backed up ledger + holdings")


def restore():
    shutil.copy(LEDGER_BAK, LEDGER_PATH)
    shutil.copy(HOLDINGS_BAK, HOLDINGS_PATH)
    LEDGER_BAK.unlink()
    HOLDINGS_BAK.unlink()
    print(f"[CLEANUP] Restored ledger + holdings")


def run_cli(args: list, check: bool = True) -> str:
    cmd = [PY, "tools/whale_picks_ledger_append.py"] + args
    print(f"  $ {' '.join(args)}")
    r = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    out = (r.stdout or '') + (r.stderr or '')
    if check and r.returncode != 0:
        print(out)
        raise RuntimeError(f"CLI exit {r.returncode}")
    return out


def get_ledger() -> pd.DataFrame:
    return pd.read_parquet(LEDGER_PATH)


def get_holdings() -> dict:
    return json.loads(HOLDINGS_PATH.read_text(encoding='utf-8'))


def find_test_ticker(in_holdings: bool) -> str:
    """Pick a stock that IS or IS NOT in current holdings.tickers."""
    h = get_holdings()
    holding_ids = {str(t.get('stock_id')) for t in h.get('tickers', [])}
    snap_dir = REPO / "data" / "whale_picks"
    # Pick a recent snapshot to find available stock_ids
    snaps = sorted(snap_dir.glob('20*.parquet'))
    if not snaps:
        raise RuntimeError("No snapshots available")
    snap = pd.read_parquet(snaps[-1])
    snap_ids = set(snap['stock_id'].astype(str).unique())
    if in_holdings:
        candidates = sorted(holding_ids & snap_ids)
    else:
        candidates = sorted(snap_ids - holding_ids)
    if not candidates:
        raise RuntimeError(f"No candidates with in_holdings={in_holdings}")
    return candidates[0]


def test_scenario_1_alert_add():
    """Verify --alert-add appends a row + updates JSON."""
    print("\n=== Scenario 1: --alert-add (basic append) ===")
    test_sid = find_test_ticker(in_holdings=False)
    print(f"  Test sid: {test_sid} (not in current holdings.tickers)")

    ledger_before = get_ledger()
    holdings_before = get_holdings()

    run_cli(["--alert-add", test_sid])

    ledger_after = get_ledger()
    holdings_after = get_holdings()

    # Check 1 new ledger row
    delta = len(ledger_after) - len(ledger_before)
    assert delta == 1, f"Expected +1 ledger row, got {delta}"

    # Check entry_type='alert', still_holding=True
    new_row = ledger_after[
        (ledger_after['stock_id'].astype(str) == test_sid)
        & (ledger_after['entry_type'] == 'alert')
    ].iloc[-1]
    assert new_row['still_holding'] == True, f"still_holding should be True"
    assert new_row['entry_type'] == 'alert', f"entry_type should be 'alert'"

    # Check holdings JSON alert_adds
    n_alert_before = len(holdings_before.get('alert_adds') or [])
    n_alert_after = len(holdings_after.get('alert_adds') or [])
    assert n_alert_after == n_alert_before + 1, f"Expected alert_adds +1, got {n_alert_after - n_alert_before}"

    found = [a for a in holdings_after['alert_adds'] if str(a.get('stock_id')) == test_sid]
    assert len(found) == 1, f"Expected 1 alert_add for {test_sid}, got {len(found)}"
    assert found[0].get('entry_type') == 'alert', "holdings alert_add entry_type should be 'alert'"
    print(f"  [PASS] +1 ledger row (entry_type='alert', still_holding=True)")
    print(f"  [PASS] +1 alert_adds in JSON")
    return test_sid


def test_scenario_2_rebal_force_exit(test_sid: str):
    """Verify --rebal --force closes alert_add when stock_id NOT in tickers."""
    print("\n=== Scenario 2: --rebal force exit (sid not in new top-K) ===")

    h = get_holdings()
    holding_ids = {str(t.get('stock_id')) for t in h.get('tickers', [])}
    assert test_sid not in holding_ids, f"{test_sid} unexpectedly in tickers — test setup bug"
    # Set rebalance_date to today to pass self-gate
    h['rebalance_date'] = date.today().isoformat()
    HOLDINGS_PATH.write_text(json.dumps(h, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

    run_cli(["--rebal", "--force"])

    ledger_after = get_ledger()
    holdings_after = get_holdings()

    # Verify alert row closed
    closed = ledger_after[
        (ledger_after['stock_id'].astype(str) == test_sid)
        & (ledger_after['entry_type'] == 'alert')
    ]
    assert len(closed) >= 1, "Should still find the alert row"
    last = closed.iloc[-1]
    assert last['still_holding'] == False, f"still_holding should be False, got {last['still_holding']}"
    assert pd.notna(last['exit_date']), "exit_date should be set"
    assert pd.notna(last['exit_price']), "exit_price should be set"

    # Verify alert_adds drained for this sid
    remaining = [a for a in (holdings_after.get('alert_adds') or [])
                 if str(a.get('stock_id')) == test_sid]
    assert len(remaining) == 0, f"alert_adds should be drained for {test_sid}"

    print(f"  [PASS] alert row closed (exit_date={pd.Timestamp(last['exit_date']).date()}, "
          f"exit_price={last['exit_price']:.2f}, pnl={last['pnl_pct']*100:+.2f}%)")
    print(f"  [PASS] alert_adds drained for {test_sid}")


def test_scenario_3_rebal_upgrade():
    """Verify --rebal --force upgrades alert_add when stock_id IS in tickers."""
    print("\n=== Scenario 3: --rebal upgrade (sid IS in new top-K) ===")

    test_sid = find_test_ticker(in_holdings=True)
    print(f"  Test sid: {test_sid} (currently in holdings.tickers)")

    # Reset state: alert-add the sid first
    run_cli(["--alert-add", test_sid])
    # Set rebalance_date to today
    h = get_holdings()
    h['rebalance_date'] = date.today().isoformat()
    HOLDINGS_PATH.write_text(json.dumps(h, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

    run_cli(["--rebal", "--force"])

    ledger_after = get_ledger()
    holdings_after = get_holdings()

    # Verify row flipped to 'upgraded'
    upg = ledger_after[
        (ledger_after['stock_id'].astype(str) == test_sid)
        & (ledger_after['entry_type'] == 'upgraded')
        & (ledger_after['still_holding'] == True)  # noqa: E712
    ]
    assert len(upg) >= 1, f"Expected at least 1 'upgraded' still_holding row for {test_sid}"

    # Verify no longer in alert_adds
    remaining = [a for a in (holdings_after.get('alert_adds') or [])
                 if str(a.get('stock_id')) == test_sid]
    assert len(remaining) == 0, f"alert_adds should be drained for {test_sid}"

    print(f"  [PASS] alert row flipped to 'upgraded' (still holding)")
    print(f"  [PASS] alert_adds drained for {test_sid}")


def main():
    print("="*60)
    print("Whale Picks Ledger Append Smoke Test")
    print("="*60)
    backup()
    try:
        sid_1 = test_scenario_1_alert_add()
        test_scenario_2_rebal_force_exit(sid_1)
        test_scenario_3_rebal_upgrade()
        print("\n" + "="*60)
        print("[ALL PASS] 3/3 scenarios")
        print("="*60)
    except Exception as e:
        print(f"\n[FAIL] {type(e).__name__}: {e}")
        raise
    finally:
        restore()


if __name__ == '__main__':
    main()
