"""Scanner post-run stage verifier.

Called at end of run_scanner.bat. Parses scanner.log and checks every expected
stage marker is present. Any missing stage = silent scheduler failure -> loud
log message + non-zero exit so Task Scheduler surfaces the failure instead of
showing a bogus exit=0. (Discord ping removed 2026-07-06.)

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
    1 = one or more stages missing
"""

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
    # MOPS probe stage removed 2026-05-05 (USE_MOPS default flipped to false).
    # Re-add ('MOPS probe done', r'\] MOPS probe done') when reactivating.
    ('RF-1 consistency done',  r'\] RF-1 consistency check done'),
    ('Market regime done',     r'\] Market regime logger done'),
    # Step-A engine + Paper trade engine markers disabled 2026-05-23 (goto skip_mode_d).
    # Re-add ('Step-A engine done', ...) + ('Paper trade engine done', ...) when reactivating.
    # Discord daily summary removed 2026-07-06 (tool deleted; was disabled since 2026-05-04).
    # Substack sync stage removed 2026-05-21 (v2026.05.21.3 整套 rm, marker no longer fires).
    ('Universe price refresh done', r'\] Universe price refresh done \(exit=0\)'),
    # 2026-08-02 補：這個 stage 失敗時 bat 只印 [WARN]（best-effort，不影響 scanner
    # exit），而這裡原本沒有對應 marker —— 於是「持續失敗」不會被任何後檢查發現，
    # macro_dashboard 的 Market Breadth 會靜靜地一直看舊資料（同一輪 diff 已替另外
    # 兩個 stage 加了 (exit=0) marker，唯獨漏掉 breadth）。
    ('TW breadth panel done', r'\] TW breadth panel done \(exit=0\)'),
    ('Refresh backtest panels done', r'\] Refresh backtest panels done \(exit=0\)'),
    # 2026-08-02 加：價格離群掃描。跟 breadth 同樣是 best-effort（找到問題只印 [WARN]，
    # 不擋 scanner），所以更需要 marker —— 沒有的話「掃描本身沒跑」與「掃描跑了沒發現」
    # 在 log 上長得一模一樣。
    # ⚠️ 這裡刻意**不要求 `(exit=0)`**：exit=1 代表「掃到毀損」，那是它正常工作而不是
    # stage 失敗，bat 也在兩條路徑都印同一行 done marker。
    ('Panel outlier scan done', r'\] Panel outlier scan done'),
    # 2026-08-04 加：美股 price cache 未完成 bar 掃描（防回歸）。同樣**不要求
    # `(exit=0)`** —— exit=1 是「掃到污染」，那是它正常工作。這個 marker 特別要緊：
    # 原始 bug（475 檔 / 5,951 列盤中快照被當收盤釘死）之所以躺了三個月沒人發現，
    # 就是因為沒有任何一道檢查會出聲。
    ('US cache bar scan done', r'\] US cache bar scan done'),
    ('Chip history resume done', r'\] Chip history resume done'),
    ('News flow anomaly done', r'\] News flow anomaly done'),
    ('Theme momentum done', r'\] Theme momentum done'),
    ('ATM PUT premium archive done', r'\] ATM PUT premium archive done'),
    ('Minifutures ratio archive done', r'\] Minifutures ratio archive done'),
    ('Options institutional archive done', r'\] Options institutional archive done'),
    ('Earnings calendar fetch done', r'\] Earnings calendar fetch done'),
    ('Scanner finished (exit=0)', r'\] Scanner finished \(exit=0\)'),
]

# QM + Value scan completion = at least 2 marker lines in scanner.log:
#   - 'Pushed: scan:' (had new commit to push), OR
#   - 'No changes to push' (gitignored daily output, nothing to push but scan did run)
# 2026-05-21 daily output gitignore policy 後 daily 全 "No changes to push",
# 改抓兩個 marker 任一都算 success.
#
# 2026-05-23: QM + Value scanner_job disabled (goto skip_qm_value_scan)，不再 push。
# EXPECTED_PUSH_MIN 改 0 (不檢查 push markers)；可日後重啟 scanner_job 再調回 2。
EXPECTED_PUSH_MIN = 0
PUSH_MARKER_RE = r'(Pushed: scan:|No changes to push)'


def main():
    if not LOG_PATH.exists():
        print(f'[verify_scan_stages] FAIL - scanner.log not found at {LOG_PATH}')
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
        push_count = len(re.findall(PUSH_MARKER_RE, tail))

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
        f'[verify_scan_stages] FAIL - {ts}\n'
        + '\n'.join(problems)
        + '\nLikely causes: BAT parsing error (CP950/CJK), scanner silent exit, '
        'or an entire stage skipped. Check scanner.log.'
    )
    print(msg)
    return 1


if __name__ == '__main__':
    sys.exit(main())
