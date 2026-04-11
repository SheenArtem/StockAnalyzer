@echo off
REM ============================================================
REM  StockAnalyzer Auto Scanner - Windows Task Scheduler
REM
REM  Schedule: Daily 19:00 (after market close + ETF data ready)
REM
REM  Setup:
REM    1. Win+R -> taskschd.msc
REM    2. Create Basic Task -> "StockAnalyzer Scanner"
REM    3. Trigger: Daily, 19:00
REM    4. Action: Start a program
REM       Program: C:\GIT\StockAnalyzer\run_scanner.bat
REM       Start in: C:\GIT\StockAnalyzer
REM    5. Conditions: uncheck "Start only if on AC power"
REM    6. Settings: check "Run task as soon as possible after missed"
REM ============================================================

cd /d C:\GIT\StockAnalyzer

REM Log start time
echo [%date% %time%] Scanner started >> scanner.log

REM Run both momentum + value screener, all markets (no chip for speed)
python scanner_job.py --mode both --market all --no-chip --push --notify >> scanner.log 2>&1

REM Log end time
echo [%date% %time%] Scanner finished >> scanner.log
echo. >> scanner.log
