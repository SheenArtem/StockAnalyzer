@echo off
REM VF-L1a: verify coverage + retry missing + verify again
REM
REM 用法:
REM   直接雙擊此檔 (或在 cmd 打 tools\vfl1a_verify_and_retry.bat)
REM
REM 流程:
REM   1. 跑 verify 產出 _missing.txt
REM   2. 如果 missing > 0，自動 retry backfill
REM   3. 再跑 verify 確認覆蓋率
REM
REM 需要 VPN 嗎？不需要（純 FinMind，不碰 MOPS）

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
    echo [WARN] retry backfill exited non-zero (rate-limit normal^)
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
