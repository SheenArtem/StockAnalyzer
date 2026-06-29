@echo off
REM E1 YT sync - integrated into run_scanner.bat front (TUE-SAT 00:00); standalone BAT kept as manual backup
REM Stage 1: fetch YT transcripts (money100 + money_deploy)
REM Stage 2: LLM extract to JSON
REM Stage 3: build sector_tags_dynamic.parquet panel
REM
REM ASCII-only hard rule - NO CJK allowed (see CLAUDE.md)

cd /d "%~dp0"

set PYTHONIOENCODING=utf-8
set LOG=yt_sync.log

call :log "=== YT sync start ==="

call :log "Stage 1: fetch transcripts"
python tools\fetch_yt_transcripts.py --end 3 >> %LOG% 2>&1
set EC1=%ERRORLEVEL%
call :log "Stage 1 done (exit=%EC1%)"

call :log "Stage 2: LLM extract"
python tools\extract_yt_sector_tags.py --all >> %LOG% 2>&1
set EC2=%ERRORLEVEL%
call :log "Stage 2 done (exit=%EC2%)"

call :log "Stage 3: build panel"
python tools\build_yt_sector_panel.py >> %LOG% 2>&1
set EC3=%ERRORLEVEL%
call :log "Stage 3 done (exit=%EC3%)"

call :log "=== YT sync done (EC1=%EC1% EC2=%EC2% EC3=%EC3%) ==="

REM Exit with aggregate failure code: non-zero if any stage failed
if not "%EC1%"=="0" exit /b %EC1%
if not "%EC2%"=="0" exit /b %EC2%
if not "%EC3%"=="0" exit /b %EC3%
exit /b 0

REM ISO-8601 timestamped log line; %~1 = message (see CLAUDE.md ASCII-only rule)
:log
for /f "delims=" %%i in ('python -c "import datetime;print(datetime.datetime.now().isoformat())"') do set TS=%%i
echo [%TS%] %~1 >> %LOG%
goto :eof
