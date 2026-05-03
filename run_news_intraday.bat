@echo off
REM ============================================================
REM  News Initiative Phase 1 #7 - Intraday alert (30-min slot)
REM
REM  Schedule (manual setup needed):
REM    Windows Task Scheduler -> Create Task
REM    Trigger: Daily, every 30 min, 09:00 to 13:30
REM    Action: Start a program
REM      Program: C:\GIT\StockAnalyzer\run_news_intraday.bat
REM      Start in: C:\GIT\StockAnalyzer
REM    Conditions: uncheck "Start only if on AC power"
REM    Settings: check "If task fails, restart every 5 min, max 3"
REM
REM  Cost: 9 slot/day * 1 LLM batch = ~10 calls/day (~$4-5/month)
REM  Best-effort: failures do not affect other schedules
REM ============================================================

cd /d C:\GIT\StockAnalyzer
set PYTHONIOENCODING=utf-8

if exist news_intraday_prev.log del news_intraday_prev.log
if exist news_intraday.log ren news_intraday.log news_intraday_prev.log

echo [%date% %time%] Intraday monitor started >> news_intraday.log
python tools\news_intraday_monitor.py >> news_intraday.log 2>&1
set PY_EXIT=%ERRORLEVEL%
echo [%date% %time%] Intraday monitor finished (exit=%PY_EXIT%) >> news_intraday.log

exit /b %PY_EXIT%
