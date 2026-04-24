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
REM  Auto Discord notify after 3 consecutive unblock detections
REM  (requires DISCORD_WEBHOOK_URL in local/.env).
REM  State file: data_cache/mops_probe_state.json
REM ============================================================

cd /d C:\GIT\StockAnalyzer

set PYTHONIOENCODING=utf-8

REM Rotate log: keep only previous + current
if exist mops_probe_prev.log del mops_probe_prev.log
if exist mops_probe.log ren mops_probe.log mops_probe_prev.log

echo [%date% %time%] MOPS probe started >> mops_probe.log
python tools\mops_probe.py >> mops_probe.log 2>&1
echo [%date% %time%] MOPS probe finished >> mops_probe.log
echo. >> mops_probe.log

exit /b 0
