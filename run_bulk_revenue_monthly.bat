@echo off
REM Bulk monthly revenue update via mopsfin CSV (Cache 3-layer Layer 2)
REM Suggested schedule: 11th of each month, 00:30 (before run_c1_monthly.bat at 01:00)
REM
REM Why 11th: Taiwan listed companies must publish last-month revenue by 10th,
REM so by 11th 00:30 all new monthly revenue data is in mopsfin CSV.
REM
REM What this does:
REM   1. Fetch SII + OTC bulk monthly revenue (2 HTTP requests, ~1954 stocks)
REM   2. Merge into data_cache/fundamental_cache/month_revenue_*.parquet
REM      (only append missing periods, never overwrite existing)
REM   3. Aggregate into data_cache/backtest/financials_revenue.parquet
REM
REM Replaces: per-stock FinMind backfill loop (was ~39 min, now ~13 sec)
REM Saves: 1954 FinMind requests / month (75% of monthly quota burn)
REM
REM ASCII-only hard rule - NO CJK allowed (see CLAUDE.md)

cd /d "%~dp0"

set PYTHONIOENCODING=utf-8
set LOG=bulk_revenue_monthly.log

echo [%DATE% %TIME%] === Bulk revenue monthly start === >> %LOG%

python tools\vfvc_backfill_monthly_rev.py --bulk-update >> %LOG% 2>&1
set EC=%ERRORLEVEL%

echo [%DATE% %TIME%] === Bulk revenue monthly done (exit=%EC%) === >> %LOG%

exit /b %EC%
