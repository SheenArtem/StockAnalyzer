@echo off
REM ============================================================
REM  StockAnalyzer App Autostart (Streamlit server)
REM
REM  Purpose: started at Windows logon by the scheduled task
REM           "StockAnalyzer App Autostart" so the Streamlit
REM           server comes up automatically after a reboot.
REM           Headless: does NOT auto-open a browser tab
REM           (per user choice "server only").
REM
REM  Launched indirectly through run_app_startup.vbs so no
REM  console window flashes on logon. The server then lives in
REM  a hidden window. Open http://localhost:8501 manually.
REM
REM  This BAT does NOT run "pip install" (a reboot must not
REM  depend on the network). When you change requirements.txt,
REM  run the normal run_app.bat once by hand to install.
REM
REM  ASCII-only (no CJK) per CLAUDE.md BAT hard rule. CP950
REM  cmd.exe parses UTF-8 BAT badly and can silently corrupt
REM  exit codes / cause silent scheduler failure.
REM ============================================================

cd /d C:\GIT\StockAnalyzer

REM Force UTF-8 for Python I/O (prevents cp950 UnicodeDecodeError on emoji)
set PYTHONIOENCODING=utf-8
set PYTHONUNBUFFERED=1

REM ------------------------------------------------------------
REM Guard: if port 8501 already has a LISTENER, the app is
REM already up (manual run_app.bat, or a previous logon already
REM started it). Do NOT launch a second instance - Streamlit
REM would fail to bind 8501, silently drift to 8502, and leave
REM two servers running. Skip and exit clean. (CLAUDE.md
REM Robustness: fail loud / no silent duplicate state.)
REM ------------------------------------------------------------
netstat -ano | findstr "LISTENING" | findstr ":8501" >nul
if %ERRORLEVEL%==0 (
    call :log "App already listening on 8501 - skip autostart"
    exit /b 0
)

REM Rotate log: keep only previous + current
if exist app_startup_prev.log del app_startup_prev.log
if exist app_startup.log ren app_startup.log app_startup_prev.log

call :log "App autostart: launching streamlit (headless, port 8501)"

REM --server.headless true : do NOT auto-open a browser tab.
REM --server.port 8501     : pin the port explicitly.
streamlit run app.py --server.headless true --server.port 8501 >> app_startup.log 2>&1

REM Only reached if the server process exits (crash / manual stop).
call :log "streamlit process exited (code=%ERRORLEVEL%)"

exit /b %ERRORLEVEL%

REM ISO-8601 timestamped log line; %~1 = message (see CLAUDE.md ASCII-only rule)
:log
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] %~1 >> app_startup.log
goto :eof
