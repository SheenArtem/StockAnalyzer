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

REM Force UTF-8 for Python I/O (prevents cp950 UnicodeDecodeError on emoji output)
set PYTHONIOENCODING=utf-8

REM Rotate log: keep only previous + current
if exist scanner_prev.log del scanner_prev.log
if exist scanner.log ren scanner.log scanner_prev.log

REM Log start time
echo [%date% %time%] Scanner started >> scanner.log

REM Run QM (quality momentum) TW only
REM Value screener paused pending Phase 1 enhancement (see project_value_enhancement.md)
python scanner_job.py --mode qm --market tw --push --notify >> scanner.log 2>&1
set PY_EXIT=%ERRORLEVEL%

REM Log end time (include python exit code so Task Scheduler shows non-zero on failure)
echo [%date% %time%] Scanner finished (exit=%PY_EXIT%) >> scanner.log
echo. >> scanner.log
exit /b %PY_EXIT%
