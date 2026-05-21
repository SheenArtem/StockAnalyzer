@echo off
REM Brokerage YT sync - 3 stage pipeline for Taiwan brokerage analyst YT channels
REM Stage 1: fetch_yt_brokerage.py (yt-dlp VTT subtitles, archive dedup)
REM Stage 2: extract_yt_brokerage.py --all (Claude Sonnet -> JSON)
REM Stage 3: build_yt_brokerage_panel.py (JSON -> mention/video parquets)
REM
REM Integrated into run_scanner.bat as best-effort stage; standalone BAT kept as manual backup.
REM ASCII-only hard rule (see CLAUDE.md) - NO CJK allowed in this file

cd /d "%~dp0"

set PYTHONIOENCODING=utf-8
set LOG=yt_brokerage_sync.log

echo [%DATE% %TIME%] === Brokerage YT sync start === >> %LOG%

echo [%DATE% %TIME%] Stage 1: fetch transcripts >> %LOG%
python tools\fetch_yt_brokerage.py --end 3 >> %LOG% 2>&1
set EC1=%ERRORLEVEL%
echo [%DATE% %TIME%] Stage 1 done (exit=%EC1%) >> %LOG%

echo [%DATE% %TIME%] Stage 2: LLM extract >> %LOG%
python tools\extract_yt_brokerage.py --all >> %LOG% 2>&1
set EC2=%ERRORLEVEL%
echo [%DATE% %TIME%] Stage 2 done (exit=%EC2%) >> %LOG%

echo [%DATE% %TIME%] Stage 3: build panel >> %LOG%
python tools\build_yt_brokerage_panel.py >> %LOG% 2>&1
set EC3=%ERRORLEVEL%
echo [%DATE% %TIME%] Stage 3 done (exit=%EC3%) >> %LOG%

echo [%DATE% %TIME%] === Brokerage YT sync done (EC1=%EC1% EC2=%EC2% EC3=%EC3%) === >> %LOG%

REM Exit with aggregate failure code: non-zero if any stage failed
if not "%EC1%"=="0" exit /b %EC1%
if not "%EC2%"=="0" exit /b %EC2%
if not "%EC3%"=="0" exit /b %EC3%
exit /b 0
