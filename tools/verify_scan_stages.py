"""Scanner post-run stage verifier.

Called at end of run_scanner.bat. Parses scanner.log and checks every expected
stage marker is present. Any missing stage = silent scheduler failure -> Discord
ping + non-zero exit so Task Scheduler surfaces the failure instead of showing
a bogus exit=0.

Catches the failure modes we've burned on:
  - 2026-04-20 exit=9009 (CJK REM breaks cmd.exe CP950 parsing)
  - 2026-04-23 BAT early stages all silently skipped (CJK REM again, different
    byte-offset triggered different cmd.exe bug)
  - Any future cmd.exe parsing glitch that makes BAT skip sections
  - Any scanner_job.py silent exit that leaves Pushed count < 2

Usage (from run_scanner.bat, AFTER "Scanner finished" echo, BEFORE exit /b):
    python tools\\verify_scan_stages.py >> scanner.log 2>&1

Exit code:
    0 = all expected stages present
    1 = one or more stages missing (Discord pinged if webhook configured)
"""

import os
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = ROOT / 'scanner.log'

# Expected BAT echo markers. Each echo fires ONLY if cmd.exe reaches that line,
# so a missing marker proves the section was skipped. Patterns are loose-anchored
# to survive leading date/time prefixes that cmd.exe garbles under CP950.
REQUIRED_STAGES = [
    ('Scanner started',        r'\] Scanner started'),
    ('YT sync done',           r'\] YT sync done'),
    ('MOPS probe done',        r'\] MOPS probe done'),
    ('RF-1 consistency done',  r'\] RF-1 consistency check done'),
    ('Market regime done',     r'\] Market regime logger done'),
    ('Step-A engine done',     r'\] Step-A engine done'),
    ('Paper trade engine done', r'\] Paper trade engine done'),
    ('Discord daily summary done', r'\] Discord daily summary done'),
    ('Substack sync done',     r'\] Substack sync done'),
    ('Scanner finished (exit=0)', r'\] Scanner finished \(exit=0\)'),
]

# QM push + Value push = at least 2 'Pushed: scan:' lines in scanner.log.
# (commit msg is hardcoded "momentum results" for both QM and Value, so we cannot
# distinguish by text, only by count.)
EXPECTED_PUSH_MIN = 2


def _read_webhook():
    env_path = ROOT / 'local' / '.env'
    if not env_path.exists():
        return None
    try:
        for line in env_path.read_text(encoding='utf-8').splitlines():
            if line.startswith('DISCORD_WEBHOOK_URL='):
                return line.split('=', 1)[1].strip().strip('"').strip("'")
    except Exception:
        return None
    return None


def _ping_discord(msg):
    webhook = _read_webhook()
    if not webhook:
        print('[verify_scan_stages] No DISCORD_WEBHOOK_URL configured, skipping ping.')
        return
    try:
        import requests
        resp = requests.post(webhook, json={'content': msg}, timeout=10)
        if resp.status_code != 204:
            print(f'[verify_scan_stages] Discord ping returned status {resp.status_code}')
    except Exception as e:
        print(f'[verify_scan_stages] Discord ping failed: {type(e).__name__}: {e}')


def main():
    if not LOG_PATH.exists():
        msg = f':rotating_light: **Scanner post-check FAIL** - scanner.log not found at {LOG_PATH}'
        print(msg)
        _ping_discord(msg)
        return 1

    log_text = LOG_PATH.read_text(encoding='utf-8', errors='replace')

    # Slice to current run only. If BAT log rotation silently fails (like the
    # 2026-04-23 incident), scanner.log keeps stale markers from previous runs
    # that would falsely satisfy the stage checks. Anchor on the LAST
    # "] Scanner started" line and validate only the tail.
    start_matches = list(re.finditer(r'\] Scanner started', log_text))
    if not start_matches:
        # Scanner started echo never fired -> log rotation + first echo both
        # skipped. This is a critical BAT failure (matches the 2026-04-23 mode).
        missing = [label for label, _ in REQUIRED_STAGES]
        push_count = 0
    else:
        last = start_matches[-1].start()
        line_start = log_text.rfind('\n', 0, last) + 1
        tail = log_text[line_start:]
        missing = [label for label, pat in REQUIRED_STAGES if not re.search(pat, tail)]
        push_count = len(re.findall(r'Pushed: scan:', tail))

    if not missing and push_count >= EXPECTED_PUSH_MIN:
        print(f'[verify_scan_stages] OK: all {len(REQUIRED_STAGES)} stages + {push_count} pushes')
        return 0

    problems = []
    if missing:
        problems.append(f'Missing {len(missing)}/{len(REQUIRED_STAGES)} BAT stages:')
        for label in missing:
            problems.append(f'  - {label}')
    if push_count < EXPECTED_PUSH_MIN:
        problems.append(f'Only {push_count} scan push(es) detected (expected >= {EXPECTED_PUSH_MIN})')

    ts = datetime.now().strftime('%Y-%m-%d %H:%M')
    msg = (
        f':rotating_light: **Scanner post-check FAIL** - {ts}\n'
        '```\n'
        + '\n'.join(problems)
        + '\n```\n'
        'Likely causes: BAT parsing error (CP950/CJK), scanner silent exit, '
        'or an entire stage skipped. Check scanner.log.'
    )
    print(msg)
    _ping_discord(msg)
    return 1


if __name__ == '__main__':
    sys.exit(main())
