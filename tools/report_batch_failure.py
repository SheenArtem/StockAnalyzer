"""
Batch scheduler failure reporter — pings Discord when a scheduled scanner stage fails.

Fixes the 2026-04-22 auto_ai_reports silent-failure bug: run_scanner.bat 吞了 exit code
把「non-critical」當正常結束，導致三個 bug 整夜沒人知道。

Usage (from run_scanner.bat):
    if errorlevel 1 (
        python tools\report_batch_failure.py --stage auto_ai_reports --exit-code %errorlevel%
    )

Exits 0 even on Discord delivery failure (don't block scanner on alert failure).
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--stage', required=True, help='stage name (e.g. auto_ai_reports, qm_tw)')
    ap.add_argument('--exit-code', type=int, required=True)
    ap.add_argument('--log-file', default='scanner.log', help='log file to tail for error context')
    ap.add_argument('--tail-lines', type=int, default=30, help='last N lines of log to include')
    args = ap.parse_args()

    if args.exit_code == 0:
        print(f'[report_batch_failure] stage={args.stage} exit=0 → no alert needed')
        return 0

    # Collect context from log tail
    log_tail = ''
    log_path = ROOT / args.log_file
    if log_path.exists():
        try:
            with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
            log_tail = ''.join(lines[-args.tail_lines:])
        except Exception as e:
            log_tail = f'(could not read log: {e})'

    issues = [
        f'Stage: {args.stage}',
        f'Exit code: {args.exit_code}',
        f'Log tail ({args.tail_lines} lines):',
        '```',
        log_tail[-1500:] if len(log_tail) > 1500 else log_tail,
        '```',
    ]

    try:
        from scanner_job import send_alert_notification
        ok = send_alert_notification(
            scan_type='batch_failure',
            market=args.stage,
            issues=issues,
        )
        if ok:
            print(f'[report_batch_failure] Discord alert sent for stage={args.stage} exit={args.exit_code}')
        else:
            print(f'[report_batch_failure] Discord alert NOT sent (no webhook configured)')
    except Exception as e:
        print(f'[report_batch_failure] ERROR sending alert: {type(e).__name__}: {e}')

    return 0  # never block scanner on alert failure


if __name__ == '__main__':
    sys.exit(main())
