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
REM    1. fetch_fred_macro      : 18 FRED CSV + ICE DXY (daily/weekly/monthly)
REM    2. build_leadership_panel: SOX/TWII rel-strength + TSM ADR premium
REM                               (yfinance; reuses fred_panel usdtwd)
REM    3. fetch_etf_flows       : 10 yfinance ETF (HYG/JNK/LQD/TLT/SPY/...)
REM    3b. fetch_cnn_fgi        : CNN US Fear-Greed history (GitHub mirror +
REM                               CNN endpoint top-up); offline IC panel only
REM    4. build_market_cap      : listed total mktcap (reuse ohlcv_tw x
REM                               t187ap03_L shares) + official MI_MARGN
REM                               margin value -> margin/mktcap pct + z
REM    5. build_systemic_chip   : aggregate chip CSV + sentiment + ETF flows
REM                               + market_cap into 5-group (A/B/C/D/E)
REM                               macro_dashboard Section 1 panel
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

echo [%date% %time%] Macro panels dawn starting >> macro_panels.log

echo [%date% %time%] [stage]FRED macro panel (27 FRED CSV + ICE DXY) >> macro_panels.log
python tools\fetch_fred_macro.py >> macro_panels.log 2>&1

echo [%date% %time%] [stage]Leadership panel (SOX/TWII RS + TSM ADR premium) >> macro_panels.log
python tools\build_leadership_panel.py >> macro_panels.log 2>&1

echo [%date% %time%] [stage]ETF flows (HYG/JNK/LQD/TLT/SPY/MOVE/EEM/EMB/FXI/EWJ + HG/GC/CL commodities) >> macro_panels.log
python tools\fetch_etf_flows.py >> macro_panels.log 2>&1

echo [%date% %time%] [stage]CNN Fear-Greed history (US sentiment; GitHub mirror + CNN endpoint top-up; offline IC panel) >> macro_panels.log
python tools\fetch_cnn_fgi.py >> macro_panels.log 2>&1

echo [%date% %time%] [stage]Market cap panel (listed total mktcap + margin/mktcap pct) >> macro_panels.log
python tools\build_market_cap_panel.py >> macro_panels.log 2>&1

echo [%date% %time%] [stage]Systemic chip panel (5-group aggregate, depends on above) >> macro_panels.log
python tools\build_systemic_chip_panel.py >> macro_panels.log 2>&1

echo [%date% %time%] Macro panels dawn done >> macro_panels.log

REM Best-effort: failures do not fail the task. Next-day evening + dawn
REM will retry.
exit /b 0
