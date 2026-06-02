@echo off
REM ============================================================
REM  Macro Panels Evening - Windows Task Scheduler
REM
REM  Schedule: TUE-SAT 17:30 TW time (after TWSE/TPEX 17:00 publishes
REM            three-institution daily totals). Plus 18:30 backup.
REM
REM  Purpose: Daily refresh of TW-side macro panels backing
REM           macro_dashboard. Splits from afterclose so heavy I/O
REM           (TWSE ZIP, FinMind 90d window, NDC API) does not
REM           block the 14:35 TAIFEX archive path.
REM
REM    1. fetch_institutional_total --days 30  : TW three-major daily
REM    1b. fetch_futures_institutional         : TAIFEX TXF 3-major daily
REM                                              (feeds dawn systemic_chip Group A)
REM    2. fetch_aaii_sentiment                 : weekly Thursday XLS
REM    3. fetch_tw_lei_panel                   : monthly NDC LEI
REM    4. build_valuation_panel (incremental)  : monthly TWSE PE
REM
REM  ASCII-only per project rule. CJK in REM/echo is silent killer
REM  under CP950.
REM
REM  Setup (2-trigger fault tolerance, same task):
REM    a) 17:30 (primary, after TWSE 17:00 publish)
REM    b) 18:30 (backup, covers brief TWSE 503 / maintenance)
REM
REM  PowerShell one-shot (admin):
REM    $bat = "C:\GIT\StockAnalyzer\run_macro_panels_evening.bat"
REM    $action = New-ScheduledTaskAction -Execute $bat `
REM        -WorkingDirectory "C:\GIT\StockAnalyzer"
REM    $trigs = @(
REM      New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tue,Wed,Thu,Fri,Sat -At 17:30
REM      New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tue,Wed,Thu,Fri,Sat -At 18:30
REM    )
REM    Register-ScheduledTask -TaskName "StockAnalyzer Macro Panels Evening" `
REM        -Action $action -Trigger $trigs -RunLevel Highest
REM ============================================================

cd /d C:\GIT\StockAnalyzer

set PYTHONIOENCODING=utf-8

echo [%date% %time%] Macro panels evening starting >> macro_panels.log

echo [%date% %time%] [stage]Institutional total (TW 3 majors, FinMind --days 30) >> macro_panels.log
python tools\fetch_institutional_total.py --days 30 >> macro_panels.log 2>&1

echo [%date% %time%] [stage]Futures institutional (TAIFEX TXF 3 majors, feeds systemic_chip Group A) >> macro_panels.log
python tools\fetch_futures_institutional.py >> macro_panels.log 2>&1

echo [%date% %time%] [stage]AAII sentiment (weekly Thursday XLS) >> macro_panels.log
python tools\fetch_aaii_sentiment.py >> macro_panels.log 2>&1

echo [%date% %time%] [stage]TW LEI panel (NDC monthly) >> macro_panels.log
python tools\fetch_tw_lei_panel.py >> macro_panels.log 2>&1

echo [%date% %time%] [stage]Valuation panel (TWSE PE incremental + Buffett) >> macro_panels.log
python tools\build_valuation_panel.py >> macro_panels.log 2>&1

echo [%date% %time%] Macro panels evening done >> macro_panels.log

REM Best-effort: failures do not fail the task. Dawn run picks up gaps via
REM systemic_chip dependency check.
exit /b 0
