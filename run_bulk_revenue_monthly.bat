@echo off
REM Bulk monthly revenue update via mopsfin CSV (Cache 3-layer Layer 2)
REM Schedule: 15th of each month, 00:30.
REM
REM Why the 15th and NOT the 11th: the statutory deadline is the 10th, but the
REM mopsfin bulk CSV does not switch to the new period until later on the 11th.
REM Measured twice: 2026-07-11 00:30 and 2026-08-11 00:30 both still returned the
REM previous period (the 2026-07-11 run wrote nothing yet still exited 0, before
REM the staleness gate existed); 2026-07-14 20:20 and 2026-08-11 21:59 both
REM returned the new one. The 15th leaves 4 days of margin.
REM
REM The CSV holds only the CURRENT period (no history), but keeps it until the
REM next month's deadline, so the 15th is still well inside the window. Missing
REM the window costs a per-stock FinMind refetch (~1960 requests, ~3.3h at
REM 600 req/hr) instead of 2 HTTP requests.
REM
REM (run_c1_monthly.bat was referenced here as the 01:00 follow-up; it has been
REM disabled since 2026-05-30 and its scheduled task was unregistered.)
REM
REM What this does:
REM   1. Fetch SII + OTC bulk monthly revenue (2 HTTP requests, ~1954 stocks)
REM   2. Merge into data_cache/fundamental_cache/month_revenue_*.parquet
REM      (only append missing periods, never overwrite existing)
REM   3. Aggregate into data_cache/backtest/financials_revenue.parquet
REM   4. Rebuild data_cache/backtest/revenue_scores_monthly.parquet
REM
REM Replaces: per-stock FinMind backfill loop (was ~39 min, now ~13 sec)
REM Saves: 1954 FinMind requests / month (75% of monthly quota burn)
REM
REM ASCII-only hard rule - NO CJK allowed (see AGENTS.md)

cd /d "%~dp0"

set PYTHONIOENCODING=utf-8
set LOG=bulk_revenue_monthly.log

for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] === Bulk revenue monthly start === >> %LOG%

set STEP=raw revenue aggregate
python tools\vfvc_backfill_monthly_rev.py --bulk-update >> %LOG% 2>&1
set EC=%ERRORLEVEL%
if not "%EC%"=="0" goto failed

set STEP=revenue score rebuild
python tools\compute_revenue_scores_monthly.py >> %LOG% 2>&1
set EC=%ERRORLEVEL%
if not "%EC%"=="0" goto failed

goto done

:failed
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] ERROR: %STEP% failed (exit=%EC%). >> %LOG%

:done
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] === Bulk revenue monthly done (exit=%EC%) === >> %LOG%

exit /b %EC%
