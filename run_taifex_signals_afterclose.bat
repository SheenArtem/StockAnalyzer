@echo off
REM ============================================================
REM  TAIFEX Signals Afterclose Archiver - Windows Task Scheduler
REM
REM  Schedule: TUE-SAT 14:35 TW time (after TAIFEX 14:30 publishes
REM            three-institution options data + 14:30+ daily option/
REM            futures CSVs are settled). Plus 15:30 / 16:30 backups.
REM
REM  Purpose: Daily archive of 3 hedging signals so the in-app
REM           market banner can read parquet (zero TAIFEX requests
REM           on every page render).
REM
REM    1. fetch_atm_put_premium       : ATM PUT pct / put_skew / top-OI
REM    2. fetch_minifutures_ratio     : MTX / TXF near-month OI ratio
REM    3. fetch_options_institutional : 3-institution call/put OI net
REM
REM  Each archiver dedupes by data_date, so re-running same day is
REM  safe (no duplicate rows). Scanner 00:00 run also writes these
REM  as next-day backup.
REM
REM  Setup (3-trigger fault-tolerance design):
REM    Add 3 triggers to the same task, all Weekly TUE-SAT:
REM      a) 14:35 (primary, just after TAIFEX 14:30 publish)
REM      b) 15:30 (backup #1, covers brief TAIFEX 503 / maintenance)
REM      c) 16:30 (backup #2, final fallback)
REM
REM    Manual setup via taskschd.msc:
REM      1. Win+R : taskschd.msc
REM      2. Create Basic Task : "StockAnalyzer TAIFEX Afterclose"
REM      3. Action: Start a program
REM           Program: C:\GIT\StockAnalyzer\run_taifex_signals_afterclose.bat
REM           Start in: C:\GIT\StockAnalyzer
REM      4. After creation, edit task -> Triggers tab -> add 2 more
REM         (15:30 and 16:30) Weekly TUE-SAT
REM      5. Conditions: uncheck "Start only if on AC power"
REM      6. Settings: check "Run task as soon as possible after missed"
REM
REM    Or one-shot PowerShell (run as admin):
REM      $bat = "C:\GIT\StockAnalyzer\run_taifex_signals_afterclose.bat"
REM      $action = New-ScheduledTaskAction -Execute $bat `
REM          -WorkingDirectory "C:\GIT\StockAnalyzer"
REM      $trigs = @(
REM        New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tue,Wed,Thu,Fri,Sat -At 14:35
REM        New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tue,Wed,Thu,Fri,Sat -At 15:30
REM        New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tue,Wed,Thu,Fri,Sat -At 16:30
REM      )
REM      Register-ScheduledTask -TaskName "StockAnalyzer TAIFEX Afterclose" `
REM          -Action $action -Trigger $trigs -RunLevel Highest
REM
REM  ASCII-only (no CJK chars) per project policy.
REM ============================================================

cd /d C:\GIT\StockAnalyzer

set PYTHONIOENCODING=utf-8

call :log "TAIFEX afterclose archive starting"

call :log "[stage]ATM PUT premium"
python tools\fetch_atm_put_premium.py >> taifex_afterclose.log 2>&1

call :log "[stage]Minifutures OI ratio"
python tools\fetch_minifutures_ratio.py >> taifex_afterclose.log 2>&1

call :log "[stage]Options institutional"
python tools\fetch_options_institutional.py >> taifex_afterclose.log 2>&1

call :log "[stage]Daily PCR + FGI append (BL-5 Part 2)"
python tools\append_today_pcr_fgi.py >> taifex_afterclose.log 2>&1

call :log "[stage]Banner Risk Score archive (depends on PCR/FGI above)"
python tools\archive_risk_score.py >> taifex_afterclose.log 2>&1

call :log "[stage]Banner TW FGI archive (5 sub-scores snapshot)"
python tools\archive_tw_fgi.py >> taifex_afterclose.log 2>&1

call :log "[stage]Banner M1B ratio archive (CBC + TWSE FMTQIK)"
python tools\archive_m1b_ratio.py >> taifex_afterclose.log 2>&1

call :log "[stage]CBC time deposits monthly fetch + notify (1.5-2mo lag)"
python tools\fetch_cbc_time_deposits.py --notify >> taifex_afterclose.log 2>&1

call :log "[stage]System 2 daily check (informational tier)"
python tools\system2_daily_check.py >> taifex_afterclose.log 2>&1

call :log "[stage]Vol Complex archive (VIX termstruct / VVIX / SKEW / OVX, informational)"
python tools\fred_fetcher.py --refresh >> taifex_afterclose.log 2>&1
python tools\archive_vol_complex.py --notify >> taifex_afterclose.log 2>&1

call :log "[stage]System 3 VIX term check (4.06x lift at backwardation, SOP-14)"
python tools\system3_vix_term_check.py >> taifex_afterclose.log 2>&1

call :log "[stage]System 3 daily check (1w-1mo early warning)"
python tools\system3_daily_check.py >> taifex_afterclose.log 2>&1

call :log "[stage]System 3 MOVE shock alert (S3-a, SOP-14 informational)"
python tools\system3_move_check.py >> taifex_afterclose.log 2>&1

call :log "[stage]System 3 SPX gap-down alert (S3-b, SOP-14 informational)"
python tools\system3_spx_check.py >> taifex_afterclose.log 2>&1

call :log "TAIFEX afterclose archive done"

REM Best-effort: failures do not fail the task. The 00:00 scanner run is
REM the authoritative source of truth (TUE-SAT) and will pick up gaps.
exit /b 0

REM ISO-8601 timestamped log line; %~1 = message (see CLAUDE.md ASCII-only rule)
:log
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] %~1 >> taifex_afterclose.log
goto :eof
