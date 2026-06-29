@echo off
REM ============================================================
REM  Macro Panels Dawn - Windows Task Scheduler
REM
REM  Schedule: TUE-SAT 06:00 TW time (after US market closes
REM            ~04:00-05:00 TW + after run_scanner.bat finishes
REM            chip CSV refresh ~04:00). Plus 07:00 backup.
REM
REM  Purpose: Daily refresh of US-side macro panels + final
REM           systemic_chip rebuild. Must run AFTER evening bat
REM           (TW data) AND after scanner.bat (chip CSV) so that
REM           build_systemic_chip_panel sees fresh inputs.
REM
REM  Stage order (2026-06-06 reordered): critical TW chain FIRST, slow
REM  FRED LAST. Rationale: 6/6 FRED timeouts ate 59min, task hit the
REM  scheduler ExecutionTimeLimit and stages after FRED were killed ->
REM  systemic_chip (macro_dashboard market-level tile) went stale.
REM  FRED/leadership being 1 day stale is tolerable; systemic_chip is not.
REM    1. fetch_etf_flows       : 10 yfinance ETF (HYG/JNK/LQD/TLT/SPY/...)
REM                               fast + feeds systemic_chip Group E
REM    2. fetch_cnn_fgi         : CNN US Fear-Greed history (GitHub mirror +
REM                               CNN endpoint top-up); offline IC panel only
REM    3. build_market_cap      : listed total mktcap (reuse ohlcv_tw x
REM                               t187ap03_L shares) + official MI_MARGN
REM                               margin value -> margin/mktcap pct + z
REM                               + margin maintenance ratio (MI_MARGN ALL
REM                               units x close / margin value; ETF prices
REM                               via yfinance gap panel)
REM    4. build_systemic_chip   : aggregate chip CSV + sentiment + ETF flows
REM                               + market_cap into 5-group (A/B/C/D/E)
REM                               macro_dashboard Section 1 panel
REM    5. fetch_fred_macro      : 18 FRED CSV + ICE DXY (daily/weekly/monthly)
REM                               SLOWEST + flakiest (timeout retries), so last
REM    6. build_leadership_panel: SOX+IXIC level/MA-dist + rel-strength vs
REM                               TWII + TSM ADR premium
REM                               (yfinance; reuses fred_panel usdtwd -> after 5)
REM
REM  NOTE: fred_fetcher.py --refresh stays in run_taifex_signals_
REM  afterclose.bat (writes data_cache/fred/ for vol_complex tile,
REM  independent of data/macro/fred_panel.parquet which this bat
REM  refreshes via fetch_fred_macro.py).
REM
REM  Dependency order matters:
REM    institutional_total (evening 17:30) -> systemic_chip Group C
REM    futures_institutional (evening 17:30) -> systemic_chip Group A (S2-A)
REM    atm_put_premium     (afterclose 14:35) -> systemic_chip Group D
REM    etf_flows           (this bat)          -> systemic_chip Group E
REM    chip CSV            (scanner ~04:00)    -> systemic_chip Group A/B
REM    ohlcv_tw            (scanner ~04:00)    -> build_market_cap denominator (total mktcap)
REM
REM  ASCII-only per project rule.
REM
REM  Setup (2-trigger fault tolerance, same task):
REM    a) 06:00 (primary, after US close + scanner done)
REM    b) 07:00 (backup, covers brief yfinance 429 / FRED 503)
REM
REM  PowerShell one-shot (admin):
REM    $bat = "C:\GIT\StockAnalyzer\run_macro_panels_dawn.bat"
REM    $action = New-ScheduledTaskAction -Execute $bat `
REM        -WorkingDirectory "C:\GIT\StockAnalyzer"
REM    $trigs = @(
REM      New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tue,Wed,Thu,Fri,Sat -At 06:00
REM      New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tue,Wed,Thu,Fri,Sat -At 07:00
REM    )
REM    Register-ScheduledTask -TaskName "StockAnalyzer Macro Panels Dawn" `
REM        -Action $action -Trigger $trigs -RunLevel Highest
REM ============================================================

cd /d C:\GIT\StockAnalyzer

set PYTHONIOENCODING=utf-8

call :log "Macro panels dawn starting"

call :log "[stage]ETF flows (HYG/JNK/LQD/TLT/SPY/MOVE/EEM/EMB/FXI/EWJ + HG/GC/CL commodities)"
python tools\fetch_etf_flows.py >> macro_panels.log 2>&1

call :log "[stage]CNN Fear-Greed history (US sentiment; GitHub mirror + CNN endpoint top-up; offline IC panel)"
python tools\fetch_cnn_fgi.py >> macro_panels.log 2>&1

call :log "[stage]Market cap panel (listed total mktcap + margin/mktcap pct)"
python tools\build_market_cap_panel.py >> macro_panels.log 2>&1

call :log "[stage]Systemic chip panel (5-group aggregate, depends on above)"
python tools\build_systemic_chip_panel.py >> macro_panels.log 2>&1

call :log "[stage]FRED macro panel (27 FRED CSV + ICE DXY; slowest, deliberately last)"
python tools\fetch_fred_macro.py >> macro_panels.log 2>&1

call :log "[stage]Leadership panel (SOX+IXIC level/MA-dist + RS vs TWII + TSM ADR premium)"
python tools\build_leadership_panel.py >> macro_panels.log 2>&1

call :log "Macro panels dawn done"

REM Best-effort: failures do not fail the task. Next-day evening + dawn
REM will retry.
exit /b 0

REM ISO-8601 timestamped log line; %~1 = message (see CLAUDE.md ASCII-only rule)
:log
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] %~1 >> macro_panels.log
goto :eof
