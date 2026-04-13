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
REM ============================================================

cd /d C:\GIT\StockAnalyzer

REM Log start time
echo [%date% %time%] Scanner started >> scanner.log

REM Run both momentum + value screener, all markets (with chip data for accuracy)
python scanner_job.py --mode both --market all --push --notify >> scanner.log 2>&1
set PY_EXIT=%ERRORLEVEL%

REM Log end time (include python exit code so Task Scheduler shows non-zero on failure)
echo [%date% %time%] Scanner finished (exit=%PY_EXIT%) >> scanner.log
echo. >> scanner.log
exit /b %PY_EXIT%
