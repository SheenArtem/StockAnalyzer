@echo off
REM ============================================================
REM  Universe Refresh Monthly - Windows Task Scheduler
REM
REM  Suggested schedule: 11th of each month, 02:00 (clusters with
REM  run_bulk_revenue_monthly.bat 00:30; leaves buffer before the
REM  M15 Whale Picks rebalance).
REM
REM  Purpose: keep the point-in-time (PIT) TW universe fresh so
REM  whale_picks_screener selects from a survivor-bias-correct
REM  universe (incl. delisted/suspended). Without this the universe
REM  silently ages: it was 46 days stale (2026-04-17 -> 2026-06-02)
REM  with NO scheduler before this bat was added.
REM
REM    1. tdcc_universe_download : TDCC OpenData 1-1 master file
REM                               -> universe_tw_full.parquet
REM                               (incl. delisted; ~50MB, retry x3)
REM    2. build_pit_universe     : merge full + live industry tag
REM                               -> universe_tw_pit.parquet (read by
REM                               whale_picks_phase2.load_universe_industry)
REM
REM  NOTE: universe_tw.parquet (live industry source) is NOT refreshed
REM  here -- industry_category is near-static and its producer
REM  (backtest_dl_ohlcv.py) is a heavy OHLCV job. New listings missing
REM  from it get "unclassified" until the next OHLCV rebuild, which is
REM  harmless (new listings are not Whale Picks candidates yet).
REM
REM  tdcc_universe_download.py skips re-download if already run today
REM  (no --force here), so a backup trigger is cheap.
REM
REM  ASCII-only per project rule.
REM
REM  Register (admin), monthly on 11th 02:00:
REM    schtasks /Create /TN "StockAnalyzer Universe Refresh Monthly" ^
REM      /TR "C:\GIT\StockAnalyzer\run_universe_refresh_monthly.bat" ^
REM      /SC MONTHLY /D 11 /ST 02:00 /RL HIGHEST /F
REM ============================================================

cd /d "%~dp0"

set PYTHONIOENCODING=utf-8
set LOG=universe_refresh_monthly.log

call :log "=== Universe refresh monthly start ==="

call :log "[stage]TDCC universe download (1-1 master, incl delisted)"
python tools\tdcc_universe_download.py >> %LOG% 2>&1

call :log "[stage]Build PIT universe (full + industry -> universe_tw_pit)"
python tools\build_pit_universe.py >> %LOG% 2>&1

call :log "=== Universe refresh monthly done ==="

REM Best-effort: failures do not fail the task. Next month retries.
exit /b 0

REM ISO-8601 timestamped log line; %~1 = message (see CLAUDE.md ASCII-only rule)
:log
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] %~1 >> %LOG%
goto :eof
