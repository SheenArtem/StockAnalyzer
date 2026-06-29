@echo off
REM ============================================================
REM  MOPS WAF unblock probe - Windows Task Scheduler
REM
REM  Schedule: Daily 09:00 (MOPS traffic low in morning; 1 req/day never hits WAF)
REM
REM  Setup:
REM    1. Win+R -> taskschd.msc
REM    2. Create Basic Task -> "MOPS Probe"
REM    3. Trigger: Daily, 09:00
REM    4. Action: Start a program
REM       Program: C:\GIT\StockAnalyzer\run_mops_probe.bat
REM       Start in: C:\GIT\StockAnalyzer
REM
REM  Discord notify DISABLED 2026-06-29 per user request (--no-notify).
REM  Probe still runs and tracks consecutive successes in the state file;
REM  to re-enable Discord push, drop "--no-notify" from the python line below.
REM  State file: data_cache/mops_probe_state.json
REM ============================================================

cd /d C:\GIT\StockAnalyzer

set PYTHONIOENCODING=utf-8

REM Rotate log: keep only previous + current
if exist mops_probe_prev.log del mops_probe_prev.log
if exist mops_probe.log ren mops_probe.log mops_probe_prev.log

call :log "MOPS probe started"
python tools\mops_probe.py --no-notify >> mops_probe.log 2>&1
call :log "MOPS probe finished"
echo. >> mops_probe.log

exit /b 0

REM ISO-8601 timestamped log line; %~1 = message (see CLAUDE.md ASCII-only rule)
:log
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] %~1 >> mops_probe.log
goto :eof
