@echo off
REM ============================================================
REM  StockAnalyzer Auto Scanner - Windows Task Scheduler
REM
REM  Schedule: Daily 22:00 (after chip data fully updated ~21:30)
REM
REM  Setup:
REM    1. Win+R -> taskschd.msc
REM    2. Create Basic Task -> "StockAnalyzer Scanner"
REM    3. Trigger: Daily, 22:00
REM    4. Action: Start a program
REM       Program: C:\GIT\StockAnalyzer\run_scanner.bat
REM       Start in: C:\GIT\StockAnalyzer
REM    5. Conditions: uncheck "Start only if on AC power"
REM    6. Settings: check "Run task as soon as possible after missed"
REM
REM  NOTE: This file must be ASCII-only (no CJK chars) to avoid cmd.exe
REM  cp950/UTF-8 encoding errors that previously corrupted exit codes
REM  (e.g. exit=9009 on 2026-04-20 was caused by mangled Chinese REM lines).
REM ============================================================

cd /d C:\GIT\StockAnalyzer

REM Force UTF-8 for Python I/O (prevents cp950 UnicodeDecodeError on emoji output)
set PYTHONIOENCODING=utf-8

REM 2026-04-20: MOPS WAF banned local IP; --no-mops CLI flag forces FinMind path.
REM Remove --no-mops once VPN/IP unblocks (env var approach failed, use CLI flag).

REM Rotate log: keep only previous + current
if exist scanner_prev.log del scanner_prev.log
if exist scanner.log ren scanner.log scanner_prev.log

REM Log start time
echo [%date% %time%] Scanner started >> scanner.log

REM ------------------------------------------------------------
REM MOPS WAF unblock probe (1 req/day). 3 consecutive successes -> Discord ping.
REM Runs before scanner to avoid extra Task Scheduler entry.
REM ------------------------------------------------------------
echo [%date% %time%] MOPS probe starting >> scanner.log
python tools\mops_probe.py >> scanner.log 2>&1
echo [%date% %time%] MOPS probe done >> scanner.log

REM ------------------------------------------------------------
REM RF-1 cache consistency check: detect fundamental_cache vs backtest drift.
REM On drift, --fix runs aggregate repair (non-blocking for scan).
REM Added 2026-04-21: guard against VF-VC type events.
REM ------------------------------------------------------------
echo [%date% %time%] RF-1 consistency check starting >> scanner.log
python tools\rf1_cache_consistency_check.py --fix >> scanner.log 2>&1
echo [%date% %time%] RF-1 consistency check done >> scanner.log

REM ------------------------------------------------------------
REM VF-G4 shadow run: log daily market regime for post-hoc volatile-only analysis.
REM Added 2026-04-21: no scanner logic change; feeds shadow_regime_analysis.py.
REM ------------------------------------------------------------
echo [%date% %time%] Market regime logger starting >> scanner.log
python tools\market_regime_logger.py >> scanner.log 2>&1
echo [%date% %time%] Market regime logger done >> scanner.log

REM Run QM + Value (TW only) -- VF-VC P3-b live 2026-04-20.
REM Value weights 30/25/30/15/0 (V_rev_heavy, WF 24 quarters 15 beats V_live 63%).
REM Chain: QM skips tracking; Value runs tracking last.
REM --regime-filter: VF-G4 DRY-RUN logs today's regime vs volatile filter
REM (audit only, does not drop picks).
python scanner_job.py --mode qm --market tw --no-tracking --no-mops --regime-filter volatile --push --notify >> scanner.log 2>&1
set PY_EXIT_QM=%ERRORLEVEL%

python scanner_job.py --mode value --market tw --no-mops --regime-filter volatile --push --notify >> scanner.log 2>&1
set PY_EXIT_VAL=%ERRORLEVEL%

REM US Value scan removed 2026-04-22 after VF-Value-ex2 EDGAR walk-forward D reverse
REM (F>=8 alpha -10%% annualized on 52K panel). US signals unvalidated -- restore
REM when US QM/Value VF verification completes with A/B-grade signals.

REM Take worst exit code
set PY_EXIT=%PY_EXIT_QM%
if not "%PY_EXIT_VAL%"=="0" set PY_EXIT=%PY_EXIT_VAL%

REM ------------------------------------------------------------
REM Auto-generate AI reports for QM office picks (top 3).
REM Added 2026-04-22 per user request: set-and-forget briefing ready by morning.
REM Claude CLI per ticker ~30-90s; 3 tickers = ~3-5min. Exit code NOT propagated
REM to PY_EXIT because report failures are non-critical to main scan success.
REM ------------------------------------------------------------
echo [%date% %time%] Auto AI reports starting >> scanner.log
python tools\auto_ai_reports.py --n 3 --format md >> scanner.log 2>&1
echo [%date% %time%] Auto AI reports done >> scanner.log

REM Log end time (include python exit code so Task Scheduler shows non-zero on failure)
echo [%date% %time%] Scanner finished (exit=%PY_EXIT%) >> scanner.log
echo. >> scanner.log
exit /b %PY_EXIT%
