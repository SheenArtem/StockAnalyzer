@echo off
REM VF-L1a: verify coverage + retry missing + verify again
REM
REM Usage:
REM   Double-click this file, or run: tools\vfl1a_verify_and_retry.bat
REM
REM Flow:
REM   1. Run verify; produces _missing.txt
REM   2. If missing > 0, auto retry backfill
REM   3. Run verify again to confirm coverage
REM
REM VPN required? No (pure FinMind, does not touch MOPS)

setlocal enabledelayedexpansion
cd /d "%~dp0.."

echo ==============================================================
echo VF-L1a verify + retry workflow
echo Started: %DATE% %TIME%
echo ==============================================================

echo.
echo [Step 1/3] Running verify (pre-retry)...
python tools\verify_cache_coverage.py ^
  --universe data_cache\vfl1a_universe.txt ^
  --cache-dir data_cache\fundamental_cache ^
  --pattern "{category}_{sid}.parquet" ^
  --categories financial_statement,balance_sheet,cash_flows_statement,month_revenue,dividend ^
  --out-prefix data_cache\vfl1a
if errorlevel 1 (
    echo [ERROR] verify step 1 failed
    pause
    exit /b 1
)

REM Check missing count
for /f %%A in ('type "data_cache\vfl1a_missing.txt" ^| find /c /v ""') do set MISSING=%%A
echo.
echo [INFO] Missing stocks: !MISSING!

if !MISSING! EQU 0 (
    echo [INFO] No missing stocks, skipping retry.
    goto :done
)

echo.
echo [Step 2/3] Running retry backfill for !MISSING! stocks...
python tools\backfill_fundamentals.py ^
  --universe data_cache\vfl1a_missing.txt ^
  --source finmind ^
  --progress-every 25
if errorlevel 1 (
    echo [WARN] retry backfill exited non-zero (rate-limit normal)
)

echo.
echo [Step 3/3] Running verify (post-retry)...
python tools\verify_cache_coverage.py ^
  --universe data_cache\vfl1a_universe.txt ^
  --cache-dir data_cache\fundamental_cache ^
  --pattern "{category}_{sid}.parquet" ^
  --categories financial_statement,balance_sheet,cash_flows_statement,month_revenue,dividend ^
  --out-prefix data_cache\vfl1a_final

:done
echo.
echo ==============================================================
echo Finished: %DATE% %TIME%
echo Final coverage report:   data_cache\vfl1a_final_report.json
echo Still missing (if any):  data_cache\vfl1a_final_missing.txt
echo ==============================================================
pause
