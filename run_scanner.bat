@echo off
REM ============================================================
REM  StockAnalyzer Auto Scanner - Windows Task Scheduler
REM
REM  Schedule: TUE-SAT 00:00 (midnight after each market day, so late-upload
REM            YT shows from previous evening are captured; chip data is fully
REM            settled by ~21:30 same day). Changed from MON-FRI 22:00 on
REM            2026-04-25 when YT sync was folded into scanner.
REM
REM  Setup:
REM    1. Win+R -> taskschd.msc
REM    2. Create Basic Task -> "StockAnalyzer Scanner"
REM    3. Trigger: Weekly TUE-SAT, 00:00
REM    4. Action: Start a program
REM       Program: C:\GIT\StockAnalyzer\run_scanner.bat
REM       Start in: C:\GIT\StockAnalyzer
REM    5. Conditions: uncheck "Start only if on AC power"
REM    6. Settings: check "Run task as soon as possible after missed"
REM
REM  NOTE: This file must be ASCII-only (no CJK chars) to avoid cmd.exe
REM  cp950/UTF-8 encoding errors that previously corrupted exit codes
REM  (e.g. exit=9009 on 2026-04-20 was caused by mangled Chinese REM lines).
REM ============================================================

cd /d C:\GIT\StockAnalyzer

REM Force UTF-8 for Python I/O (prevents cp950 UnicodeDecodeError on emoji output)
set PYTHONIOENCODING=utf-8

REM 2026-04-20: MOPS WAF banned local IP; --no-mops CLI flag forces FinMind path.
REM Remove --no-mops once VPN/IP unblocks (env var approach failed, use CLI flag).

REM Rotate log: keep only previous + current
if exist scanner_prev.log del scanner_prev.log
if exist scanner.log ren scanner.log scanner_prev.log

REM Log start time
echo [%date% %time%] Scanner started >> scanner.log

REM ------------------------------------------------------------
REM E1 YT sync: fetch transcripts + LLM extract + build panel.
REM Runs FIRST so downstream scanner stages (QM push) can consume fresh
REM sector_tags_dynamic.parquet. Best-effort: failures do not affect PY_EXIT.
REM Late-upload shows (after 00:00 cutoff) will be captured on next day's run.
REM Added 2026-04-25, replaces standalone run_yt_sync.bat scheduled task.
REM ------------------------------------------------------------
echo [%date% %time%] YT sync Stage 1 fetch starting >> scanner.log
python tools\fetch_yt_transcripts.py --end 3 >> scanner.log 2>&1
set YT_EC1=%ERRORLEVEL%
echo [%date% %time%] YT sync Stage 1 done (exit=%YT_EC1%) >> scanner.log

echo [%date% %time%] YT sync Stage 2 extract starting >> scanner.log
python tools\extract_yt_sector_tags.py --all >> scanner.log 2>&1
set YT_EC2=%ERRORLEVEL%
echo [%date% %time%] YT sync Stage 2 done (exit=%YT_EC2%) >> scanner.log

echo [%date% %time%] YT sync Stage 3 panel starting >> scanner.log
python tools\build_yt_sector_panel.py >> scanner.log 2>&1
set YT_EC3=%ERRORLEVEL%
echo [%date% %time%] YT sync done (EC1=%YT_EC1% EC2=%YT_EC2% EC3=%YT_EC3%) >> scanner.log

REM ------------------------------------------------------------
REM RAG #4 Path A POC removed 2026-05-01: verdict MARGINAL +0.074 (N=1 TSMC),
REM both yt-dlp + youtube-transcript-api hit YT timedtext IP throttle (~10
REM calls/session) so N=5 scale-up is structurally fragile. POC closed; tools
REM in tools\rag_fetch_yt_earnings.py + rag_embed_yt_earnings.py +
REM rag_compare_yt_vs_pdf.py kept for reference. Existing TSMC YT chunks in
REM chromadb (142 from Q4 2025) preserved.
REM ------------------------------------------------------------

REM ------------------------------------------------------------
REM News theme discovery (2026-05-01 Phase 0 Commit 1: dual-write):
REM UDN money RSS direct + cnyes JSON API -> Claude Sonnet batch extract.
REM Dual-write storage (Phase 0 6/6 done 2026-05-01):
REM   - data_cache/news_archive/YYYY-MM/articles.parquet (new SoT, permanent)
REM   - data/news_themes.parquet (legacy, 30d TTL, kept permanently as backup +
REM     reader fallback for graceful degradation; Robustness > cleanliness)
REM POC accuracy ~95% strict (Day 1-3 audit, commit de836ba).
REM Best-effort: failures do not affect scanner exit.
REM ------------------------------------------------------------
echo [%date% %time%] News theme extract starting >> scanner.log
python tools\news_theme_extract.py >> scanner.log 2>&1
set NEWS_EC=%ERRORLEVEL%
echo [%date% %time%] News theme extract done (exit=%NEWS_EC%) >> scanner.log

REM ------------------------------------------------------------
REM MOPS WAF unblock probe (1 req/day). 3 consecutive successes -> Discord ping.
REM Runs before scanner to avoid extra Task Scheduler entry.
REM ------------------------------------------------------------
echo [%date% %time%] MOPS probe starting >> scanner.log
python tools\mops_probe.py >> scanner.log 2>&1
echo [%date% %time%] MOPS probe done >> scanner.log

REM ------------------------------------------------------------
REM RF-1 cache consistency check: detect fundamental_cache vs backtest drift.
REM On drift, --fix runs aggregate repair (non-blocking for scan).
REM Added 2026-04-21: guard against VF-VC type events.
REM ------------------------------------------------------------
echo [%date% %time%] RF-1 consistency check starting >> scanner.log
python tools\rf1_cache_consistency_check.py --fix >> scanner.log 2>&1
echo [%date% %time%] RF-1 consistency check done >> scanner.log

REM ------------------------------------------------------------
REM VF-G4 shadow run: log daily market regime for post-hoc volatile-only analysis.
REM Added 2026-04-21: no scanner logic change; feeds shadow_regime_analysis.py.
REM ------------------------------------------------------------
echo [%date% %time%] Market regime logger starting >> scanner.log
python tools\market_regime_logger.py >> scanner.log 2>&1
echo [%date% %time%] Market regime logger done >> scanner.log

REM Run QM + Value (TW only) -- VF-VC P3-b live 2026-04-20.
REM Value weights 30/25/30/15/0 (V_rev_heavy, WF 24 quarters 15 beats V_live 63%).
REM Chain: QM skips tracking; Value runs tracking last.
REM --regime-filter: VF-G4 DRY-RUN logs today's regime vs volatile filter
REM (audit only, does not drop picks).
python scanner_job.py --mode qm --market tw --no-tracking --no-mops --regime-filter volatile --push --notify >> scanner.log 2>&1
set PY_EXIT_QM=%ERRORLEVEL%

python scanner_job.py --mode value --market tw --no-mops --regime-filter volatile --push --notify >> scanner.log 2>&1
set PY_EXIT_VAL=%ERRORLEVEL%

REM US Value scan removed 2026-04-22 after VF-Value-ex2 EDGAR walk-forward D reverse
REM (F>=8 alpha -10%% annualized on 52K panel). US signals unvalidated -- restore
REM when US QM/Value VF verification completes with A/B-grade signals.

REM Take worst exit code
set PY_EXIT=%PY_EXIT_QM%
if not "%PY_EXIT_VAL%"=="0" set PY_EXIT=%PY_EXIT_VAL%

REM ------------------------------------------------------------
REM Mode D Phase 2 Wave 2/3 pipeline (2026-04-25):
REM   1. Step-A engine -> daily_alerts.json (forced/suggested/info)
REM   2. Paper trade engine -> open_trades.json + trade_log.jsonl
REM   3. Discord daily summary -> single code block 1 push/day
REM Best-effort: failures do not affect scanner exit code.
REM ------------------------------------------------------------
echo [%date% %time%] Step-A engine starting >> scanner.log
python tools\step_a_engine.py >> scanner.log 2>&1
echo [%date% %time%] Step-A engine done >> scanner.log

echo [%date% %time%] Paper trade engine starting >> scanner.log
python tools\paper_trade_engine.py >> scanner.log 2>&1
echo [%date% %time%] Paper trade engine done >> scanner.log

echo [%date% %time%] Discord daily summary starting >> scanner.log
python tools\discord_daily_summary.py >> scanner.log 2>&1
echo [%date% %time%] Discord daily summary done >> scanner.log

REM ------------------------------------------------------------
REM Substack sync: download new songfen articles + detect pending INDEX updates.
REM Added 2026-04-23. Best-effort: failures do not affect scanner exit code.
REM ------------------------------------------------------------
echo [%date% %time%] Substack sync starting >> scanner.log
python tools\sync_substack.py >> scanner.log 2>&1
echo [%date% %time%] Substack sync done >> scanner.log

REM ------------------------------------------------------------
REM Auto-generate AI reports for QM office picks (top 3).
REM Added 2026-04-22 per user request: set-and-forget briefing ready by morning.
REM DISABLED 2026-04-29 per user request: cancel daily auto AI reports.
REM   To re-enable: remove the "goto skip_ai_reports" line directly below.
REM   All original logic preserved for easy revival.
REM ------------------------------------------------------------
goto skip_ai_reports
echo [%date% %time%] Auto AI reports smoke check starting >> scanner.log
python tools\auto_ai_reports.py --smoke >> scanner.log 2>&1
set AI_SMOKE_EXIT=%ERRORLEVEL%
if not "%AI_SMOKE_EXIT%"=="0" (
    echo [%date% %time%] [FAIL] auto_ai_reports smoke check FAILED exit=%AI_SMOKE_EXIT% >> scanner.log
    python tools\report_batch_failure.py --stage auto_ai_reports_smoke --exit-code %AI_SMOKE_EXIT% >> scanner.log 2>&1
    goto skip_ai_reports
)

echo [%date% %time%] Auto AI reports starting >> scanner.log
python tools\auto_ai_reports.py --n 3 --format md >> scanner.log 2>&1
set AI_RUN_EXIT=%ERRORLEVEL%
if not "%AI_RUN_EXIT%"=="0" (
    echo [%date% %time%] [FAIL] auto_ai_reports FAILED exit=%AI_RUN_EXIT% >> scanner.log
    python tools\report_batch_failure.py --stage auto_ai_reports --exit-code %AI_RUN_EXIT% >> scanner.log 2>&1
) else (
    echo [%date% %time%] Auto AI reports done (exit=0) >> scanner.log
)

:skip_ai_reports
REM Log end time (include python exit code so Task Scheduler shows non-zero on failure)
echo [%date% %time%] Scanner finished (exit=%PY_EXIT%) >> scanner.log

REM ------------------------------------------------------------
REM Layer 4: post-check verifier.
REM Parses scanner.log to confirm every expected BAT echo marker fired
REM and that git push happened at least twice (QM + Value). Missing any
REM marker = silent scheduler failure -> Discord ping.
REM Non-zero exit here propagates to %PY_EXIT% so Task Scheduler sees it.
REM Added 2026-04-24 after CJK BAT incident.
REM ------------------------------------------------------------
python tools\verify_scan_stages.py >> scanner.log 2>&1
set POST_EXIT=%ERRORLEVEL%
if not "%POST_EXIT%"=="0" (
    echo [%date% %time%] [FAIL] verify_scan_stages detected missing stages exit=%POST_EXIT% >> scanner.log
    REM Only promote post-check failure to PY_EXIT if scanner itself reported success
    REM (do not mask an already-failing scanner exit code).
    if "%PY_EXIT%"=="0" set PY_EXIT=%POST_EXIT%
)

echo. >> scanner.log
exit /b %PY_EXIT%
