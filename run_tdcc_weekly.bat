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

echo [%date% %time%] TDCC weekly download started >> tdcc_weekly.log

python tools/tdcc_shareholding.py >> tdcc_weekly.log 2>&1
set PY_EXIT=%ERRORLEVEL%

echo [%date% %time%] TDCC weekly download finished (exit=%PY_EXIT%) >> tdcc_weekly.log
echo. >> tdcc_weekly.log
exit /b %PY_EXIT%
