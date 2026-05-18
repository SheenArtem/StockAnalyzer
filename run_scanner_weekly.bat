@echo off
REM ============================================================
REM  StockAnalyzer Strong Stocks Weekly Report - Sunday 12:00
REM
REM  Schedule: every Sunday at 12:00 (TWSE/TPEX low traffic, T+1 chip settled)
REM
REM  Pipeline (3 stages):
REM    1. weekly_screener   -> data/latest/strong_stocks_weekly.json
REM       (universe scan + weekly 5-signal scoring + 5-day chip aggregate)
REM    2. ai_analysis --weekly (Opus + --allowedTools "*" + 14d news inject)
REM       -> in-place add ai_analysis section to weekly JSON
REM    3. render --weekly -> data/strong_stocks_reports/YYYY-Www.{html,pdf}
REM
REM  Setup (Windows Task Scheduler):
REM    1. Win+R -> taskschd.msc
REM    2. Create Basic Task -> "StockAnalyzer Weekly Scanner"
REM    3. Trigger: Weekly, every Sunday, 12:00
REM    4. Action: Start a program
REM       Program: C:\GIT\StockAnalyzer\run_scanner_weekly.bat
REM       Start in: C:\GIT\StockAnalyzer
REM    5. Conditions: uncheck "Start only if on AC power"
REM    6. Settings: check "Run task as soon as possible after missed"
REM
REM  NOTE: ASCII-only (no CJK) per CLAUDE.md BAT hard rule. CP950 cmd.exe
REM        parses UTF-8 BAT badly and may silently corrupt exit codes.
REM
REM  Informational tier: weekly scoring NOT yet IC-validated. Output is
REM  marked informational_tier=true and does not feed paper_trade or
REM  step_a_engine exit logic.
REM ============================================================

cd /d C:\GIT\StockAnalyzer

REM Force UTF-8 for Python I/O (prevents cp950 UnicodeDecodeError on emoji)
set PYTHONIOENCODING=utf-8

REM Rotate log: keep only previous + current
if exist scanner_weekly_prev.log del scanner_weekly_prev.log
if exist scanner_weekly.log ren scanner_weekly.log scanner_weekly_prev.log

echo [%date% %time%] Weekly scanner started >> scanner_weekly.log

REM ------------------------------------------------------------
REM Stage 1: weekly screener (universe scan + scoring + 5d chip)
REM
REM IMPORTANT: Stage 1 MUST succeed before Stage 2/3 run. If Stage 1
REM fails (e.g. TWSE MI_INDEX outage -> universe too small),
REM strong_stocks_weekly.json is NOT overwritten. Running Stage 2/3
REM would silently regenerate last week's stale report with a fresh
REM AI section, hiding the failure. Abort instead. (CLAUDE.md
REM Robustness: Fail loud, no swallowing.)
REM ------------------------------------------------------------
echo [%date% %time%] Stage 1 weekly_screener starting >> scanner_weekly.log
python tools\strong_stocks_weekly_screener.py >> scanner_weekly.log 2>&1
set EC1=%ERRORLEVEL%
echo [%date% %time%] Stage 1 done (exit=%EC1%) >> scanner_weekly.log

if not "%EC1%"=="0" (
    echo [%date% %time%] [FATAL] Stage 1 failed exit=%EC1% - aborting Stage 2/3 and skipping commit >> scanner_weekly.log
    echo [%date% %time%] [FATAL] Reason: weekly JSON was NOT regenerated; running Stage 2/3 would re-render stale report from last week >> scanner_weekly.log
    echo [%date% %time%] [FATAL] Inspect scanner_weekly.log above for root cause TWSE/TPEX/TradingView upstream failure >> scanner_weekly.log
    echo [%date% %time%] Weekly scanner ABORTED EC1=%EC1% >> scanner_weekly.log
    exit /b %EC1%
)

REM ------------------------------------------------------------
REM Stage 2: AI analysis (Opus + WebSearch + 14d news inject)
REM ------------------------------------------------------------
echo [%date% %time%] Stage 2 ai_analysis --weekly starting >> scanner_weekly.log
python tools\strong_stocks_ai_analysis.py --weekly >> scanner_weekly.log 2>&1
set EC2=%ERRORLEVEL%
echo [%date% %time%] Stage 2 done (exit=%EC2%) >> scanner_weekly.log

REM ------------------------------------------------------------
REM Stage 3: render -> HTML + PDF
REM ------------------------------------------------------------
echo [%date% %time%] Stage 3 render --weekly starting >> scanner_weekly.log
python tools\strong_stocks_render.py --weekly >> scanner_weekly.log 2>&1
set EC3=%ERRORLEVEL%
echo [%date% %time%] Stage 3 done (exit=%EC3%) >> scanner_weekly.log

REM ------------------------------------------------------------
REM Commit + push (best-effort, do not block on failure)
REM ------------------------------------------------------------
echo [%date% %time%] git commit + push starting >> scanner_weekly.log
git add data/latest/strong_stocks_weekly.json >> scanner_weekly.log 2>&1
git add data/strong_stocks_reports/*.html data/strong_stocks_reports/*.pdf >> scanner_weekly.log 2>&1
git commit -m "weekly scan: %date% %time% strong stocks weekly results" >> scanner_weekly.log 2>&1
git push >> scanner_weekly.log 2>&1

echo [%date% %time%] Weekly scanner finished (EC1=%EC1% EC2=%EC2% EC3=%EC3%) >> scanner_weekly.log

REM Exit with worst stage code (0 = all green)
set FINAL_EXIT=%EC1%
if not "%EC2%"=="0" set FINAL_EXIT=%EC2%
if not "%EC3%"=="0" set FINAL_EXIT=%EC3%
exit /b %FINAL_EXIT%
