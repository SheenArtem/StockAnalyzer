@echo off
REM ============================================================
REM  TAIFEX Signals Afterclose Archiver - Windows Task Scheduler
REM
REM  Schedule: TUE-SAT 14:35 TW time (after TAIFEX 14:30 publishes
REM            three-institution options data + 14:30+ daily option/
REM            futures CSVs are settled).
REM
REM  Purpose: Daily archive of 3 hedging signals so the in-app
REM           market banner can read parquet (zero TAIFEX requests
REM           on every page render).
REM
REM    1. fetch_atm_put_premium      - ATM PUT % / put_skew / top-OI
REM    2. fetch_minifutures_ratio    - MTX / TXF near-month OI ratio
REM    3. fetch_options_institutional - 3-institution call/put OI net
REM
REM  Each archiver dedupes by data_date, so re-running same day is
REM  safe (no duplicate rows). Scanner 00:00 run also writes these
REM  -- this BAT just fronts that with a same-day update.
REM
REM  Setup:
REM    1. Win+R [stage]taskschd.msc
REM    2. Create Basic Task [stage]"StockAnalyzer TAIFEX Afterclose"
REM    3. Trigger: Weekly TUE-SAT, 14:35
REM    4. Action: Start a program
REM       Program: C:\GIT\StockAnalyzer\run_taifex_signals_afterclose.bat
REM       Start in: C:\GIT\StockAnalyzer
REM    5. Conditions: uncheck "Start only if on AC power"
REM    6. Settings: check "Run task as soon as possible after missed"
REM
REM  ASCII-only (no CJK chars) per project policy.
REM ============================================================

cd /d C:\GIT\StockAnalyzer

set PYTHONIOENCODING=utf-8

echo [%date% %time%] TAIFEX afterclose archive starting >> taifex_afterclose.log

echo [%date% %time%] [stage]ATM PUT premium >> taifex_afterclose.log
python tools\fetch_atm_put_premium.py >> taifex_afterclose.log 2>&1

echo [%date% %time%] [stage]Minifutures OI ratio >> taifex_afterclose.log
python tools\fetch_minifutures_ratio.py >> taifex_afterclose.log 2>&1

echo [%date% %time%] [stage]Options institutional >> taifex_afterclose.log
python tools\fetch_options_institutional.py >> taifex_afterclose.log 2>&1

echo [%date% %time%] TAIFEX afterclose archive done >> taifex_afterclose.log

REM Best-effort: failures do not fail the task. The 00:00 scanner run is
REM the authoritative source of truth (TUE-SAT) and will pick up gaps.
exit /b 0
