@echo off
REM C1 tilt flags monthly refresh - scheduled 11th of each month 01:00
REM Why 11th: Taiwan listed companies must publish last-month revenue by 10th,
REM so by 11th 01:00 all new monthly revenue data is in FinMind.
REM
REM Recomputes data/c1_tilt_flags.parquet which scanner reads daily.
REM Without this, c1_tilt_flags.parquet goes stale after market releases new
REM monthly revenue, and QM composite_score C1 boost becomes outdated.
REM
REM ASCII-only hard rule - NO CJK allowed (see CLAUDE.md)

cd /d "%~dp0"

set PYTHONIOENCODING=utf-8
set LOG=c1_monthly.log

echo [%DATE% %TIME%] === C1 tilt monthly refresh start === >> %LOG%

python tools\compute_c1_tilt.py >> %LOG% 2>&1
set EC=%ERRORLEVEL%

echo [%DATE% %TIME%] === C1 tilt refresh done (exit=%EC%) === >> %LOG%

exit /b %EC%
