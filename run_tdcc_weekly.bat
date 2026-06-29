REM ============================================================
REM  StockAnalyzer TDCC 1-5 Weekly Download - Windows Task Scheduler
REM
REM  Schedule: Weekly Saturday 08:00 (TDCC updates Friday night, Sat early morning)
REM  Purpose:  Accumulate TDCC shareholding distribution history
REM            (TDCC OpenAPI only gives latest week, so must self-archive)
REM
REM  Setup:
REM    1. Win+R -> taskschd.msc
REM    2. Create Basic Task -> "StockAnalyzer TDCC Weekly"
REM    3. Trigger: Weekly, Saturday, 08:00
REM    4. Action: Start a program
REM       Program: C:\GIT\StockAnalyzer\run_tdcc_weekly.bat
REM       Start in: C:\GIT\StockAnalyzer
REM    5. Conditions: uncheck "Start only if on AC power"
REM    6. Settings: check "Run task as soon as possible after missed"
REM ============================================================

cd /d C:\GIT\StockAnalyzer

set PYTHONIOENCODING=utf-8

if exist tdcc_weekly_prev.log del tdcc_weekly_prev.log
if exist tdcc_weekly.log ren tdcc_weekly.log tdcc_weekly_prev.log

call :log "TDCC weekly download started"

python tools/tdcc_shareholding.py >> tdcc_weekly.log 2>&1
set PY_EXIT=%ERRORLEVEL%

call :log "TDCC weekly download finished (exit=%PY_EXIT%)"

REM ============================================================
REM  BL-4: weekly_chip_report (4 top-10 boards: consec buy/sell days + weekly amount)
REM  Independent stage; runs even if TDCC failed (uses own data source)
REM ============================================================
call :log "Weekly chip report started"
REM Discord push DISABLED 2026-06-29 per user request: cancel weekly chip Discord summary.
REM Report MD still generated; to re-enable add "--push-discord" back below.
python tools/weekly_chip_report.py >> tdcc_weekly.log 2>&1
set CHIP_EXIT=%ERRORLEVEL%
call :log "Weekly chip report finished (exit=%CHIP_EXIT%)"

if not "%CHIP_EXIT%"=="0" (
    python tools/report_batch_failure.py --stage weekly_chip --exit-code %CHIP_EXIT% --log-file tdcc_weekly.log >> tdcc_weekly.log 2>&1
)

REM ============================================================
REM  Chip history margin / short_sale weekly resume.
REM  Added 2026-05-02: daily cron only runs institutional (5-10s); margin
REM  and short_sale go through TPEX FinMind per-stock fallback (913 stocks
REM  x 1.2s = ~18min per trading day) so weekly batch is the right slot.
REM  Best-effort: failures do not affect TDCC weekly exit.
REM ============================================================
call :log "Chip history margin/short_sale resume started"
python tools/chip_history_dl.py --dataset margin --resume >> tdcc_weekly.log 2>&1
python tools/chip_history_dl.py --dataset short_sale --resume >> tdcc_weekly.log 2>&1
call :log "Chip history margin/short_sale resume done"

REM Foreign holding ratio for the 23 TW0050_FIXED_UNIVERSE tickers that
REM systemic_chip Group A foreign_holding uses. These *_shareholding_chip.csv rode in
REM the QM/Value scan (stopped 56dcc6c 2026-05-23) so froze; ~23 FinMind calls weekly.
call :log "Foreign holding ratio (23-universe) refresh started"
python tools/refresh_foreign_holding.py >> tdcc_weekly.log 2>&1
call :log "Foreign holding ratio refresh done"

echo. >> tdcc_weekly.log
REM Use TDCC exit code as final to preserve original schedule failure semantics
exit /b %PY_EXIT%

REM ISO-8601 timestamped log line; %~1 = message (see CLAUDE.md ASCII-only rule)
:log
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] %~1 >> tdcc_weekly.log
goto :eof
