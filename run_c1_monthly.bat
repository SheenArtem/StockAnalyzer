@echo off
REM ============================================================
REM  What this task does ("C1 tilt", decoded in plain English)
REM ============================================================
REM  "C1 tilt" is a monthly-revenue TURNAROUND flag. For every Taiwan
REM  stock it checks whether year-over-year monthly revenue just flipped
REM  from shrinking to growing -- specifically: YoY was below -2% three
REM  months ago, and is now above +2% in the latest one or two months.
REM  That negative-to-positive inflection is the "C1" earnings-turnaround
REM  catalyst.
REM
REM  The flag is regime-gated: it only switches ON during an "AI era" bull
REM  market (the Taiwan top-300 stocks are up more than +20% over the
REM  trailing 12 months). In a flat or bear market the flag stays OFF,
REM  because back-tests showed the turnaround signal hurts returns outside
REM  the bull regime.
REM
REM  Output file: data/c1_tilt_flags.parquet -- one row per stock holding
REM  is_ai_era, c1_tilt_on, and the 1-month / 3-month revenue YoY numbers.
REM
REM  Who consumes it: the QM stock screener gives flagged stocks a small
REM  1.2x score boost, and the app lists them in the "C1 turnaround" tab.
REM  NOTE: since 100% Whale Picks became the production strategy
REM  (2026-05-23), the QM screener is informational-only, so this flag now
REM  mainly keeps that UI tab current rather than driving live trades.
REM
REM ============================================================
REM  Schedule: the 11th of each month, 01:00.
REM  Why the 11th: Taiwan-listed companies must publish last month's
REM  revenue by the 10th, so by 01:00 on the 11th all the new revenue is
REM  in FinMind and the recompute picks it up. Running it monthly stops
REM  c1_tilt_flags.parquet from going stale after fresh revenue is released.
REM
REM  ASCII-only hard rule - NO CJK allowed (see CLAUDE.md).
REM ============================================================

cd /d "%~dp0"

set PYTHONIOENCODING=utf-8
set LOG=c1_monthly.log

REM ============================================================
REM DISABLED 2026-05-30 per user request: C1 tilt is only consumed by the
REM informational QM screener (daily QM scan already disabled 2026-05-23)
REM and the Mode D UI tab (which already reads a frozen qm_result.json).
REM The production strategy (Whale Picks) does NOT use C1 at all, so
REM stopping this refresh affects nothing live. data/c1_tilt_flags.parquet
REM is LEFT IN PLACE (frozen at its last monthly snapshot, ~2026-05-11),
REM NOT deleted -- it is git-tracked and only feeds informational consumers
REM (the QM screener boost + Mode D UI tab, both already non-production).
REM Caveat: the frozen is_ai_era regime will not update, so if the market
REM ever leaves the AI-era bull regime the stale ON flag could keep applying
REM the C1 boost where back-tests show it hurts (-4.2pp). That only affects
REM the informational UI, never live trades. To wipe it cleanly later:
REM   git rm data/c1_tilt_flags.parquet
REM The Windows scheduled task "StockAnalyzer_C1_Monthly" was also REMOVED
REM (Unregister-ScheduledTask, 2026-05-30).
REM
REM Manual trigger still works:  python tools\compute_c1_tilt.py
REM To re-enable the scheduled run: remove the "goto skip_c1" line below AND
REM re-create the task (Register-ScheduledTask, or via taskschd.msc:
REM Monthly day 11 at 01:00 -> this BAT).
REM ============================================================
call :log "=== C1 tilt monthly refresh DISABLED (skipped) ==="
goto skip_c1

call :log "=== C1 tilt monthly refresh start ==="

python tools\compute_c1_tilt.py >> %LOG% 2>&1
set EC=%ERRORLEVEL%

call :log "=== C1 tilt refresh done (exit=%EC%) ==="

exit /b %EC%

:skip_c1
exit /b 0

REM ISO-8601 timestamped log line; %~1 = message (see CLAUDE.md ASCII-only rule)
:log
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] %~1 >> %LOG%
goto :eof
