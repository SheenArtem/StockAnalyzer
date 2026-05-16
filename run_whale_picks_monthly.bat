@echo off
REM Whale Picks monthly selector
REM Schedule: Task Scheduler, monthly last business day or first business day
REM Production config per docs/whale_picks_spec.md v0.4

setlocal

cd /d "%~dp0"

REM Stage 1 - run selector + Discord push
echo [%date% %time%] Running whale_picks_screener
python tools\whale_picks_screener.py --push > whale_picks_screener.log 2>&1
if errorlevel 1 (
    echo [%date% %time%] whale_picks_screener FAILED
    exit /b 1
)
echo [%date% %time%] whale_picks_screener OK

REM Stage 2 - git commit snapshot
git add data\whale_picks\*.parquet data\latest\whale_picks_top20.json 2>nul
git commit -m "whale_picks: %date% monthly snapshot" 2>nul
git push 2>nul

echo [%date% %time%] Done
endlocal
exit /b 0
