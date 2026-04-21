REM ============================================================
REM  StockAnalyzer Auto Scanner - Windows Task Scheduler
REM
REM  Schedule: Daily 22:00 (after chip data fully updated ~21:30)
REM
REM  Setup:
REM    1. Win+R -> taskschd.msc
REM    2. Create Basic Task -> "StockAnalyzer Scanner"
REM    3. Trigger: Daily, 22:00
REM    4. Action: Start a program
REM       Program: C:\GIT\StockAnalyzer\run_scanner.bat
REM       Start in: C:\GIT\StockAnalyzer
REM    5. Conditions: uncheck "Start only if on AC power"
REM    6. Settings: check "Run task as soon as possible after missed"
REM ============================================================

cd /d C:\GIT\StockAnalyzer

REM Force UTF-8 for Python I/O (prevents cp950 UnicodeDecodeError on emoji output)
set PYTHONIOENCODING=utf-8

REM 2026-04-20: MOPS WAF ban local IP，用 --no-mops CLI flag 強制走 FinMind
REM VPN 解封後移除 --no-mops 即可（env var 方式行不通，改走 CLI 參數 set_use_mops）

REM Rotate log: keep only previous + current
if exist scanner_prev.log del scanner_prev.log
if exist scanner.log ren scanner.log scanner_prev.log

REM Log start time
echo [%date% %time%] Scanner started >> scanner.log

REM Run QM + Value (TW only) — VF-VC P3-b 2026-04-20 落地
REM Value 權重改 30/25/30/15/0 (V_rev_heavy, WF 24 季 15 贏 V_live 63%)
REM Chain invocations: QM 不做 tracking，Value 最後執行帶 tracking
python scanner_job.py --mode qm --market tw --no-tracking --no-mops --push --notify >> scanner.log 2>&1
set PY_EXIT_QM=%ERRORLEVEL%

python scanner_job.py --mode value --market tw --no-mops --push --notify >> scanner.log 2>&1
set PY_EXIT_VAL=%ERRORLEVEL%

REM Take worst exit code
set PY_EXIT=%PY_EXIT_QM%
if not "%PY_EXIT_VAL%"=="0" set PY_EXIT=%PY_EXIT_VAL%

REM Log end time (include python exit code so Task Scheduler shows non-zero on failure)
echo [%date% %time%] Scanner finished (exit=%PY_EXIT%) >> scanner.log
echo. >> scanner.log
exit /b %PY_EXIT%
