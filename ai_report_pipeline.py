"""
AI 報告 pipeline — 單一入口，UI worker 與 CLI 共用。

Refactor rationale (2026-04-23, Robustness First Phase 2 H3):
  之前 `app.py:_ai_report_worker` 與 `tools/auto_ai_reports.py:_run_one` 是兩份
  幾乎一樣的 pipeline。UI 每天被操爆 → 錯誤立刻看到；CLI 只有 TUE-SAT 00:00 排程跑 →
  錯誤會躲過。兩邊 signature drift 在昨天炸光（import 路徑錯 / 建構子誤用 /
  tuple 未 unpack）。

  本模組把 pipeline 抽成單一 `generate_one_report()`，兩個 caller 都改成 thin
  wrapper → 不可能再 drift。

Usage:
    # UI worker（Streamlit thread）:
    def worker_progress(msg):
        with job_lock:
            job['progress'].append(msg)
    result = generate_one_report(ticker, fmt='md', progress_cb=worker_progress)
    if result['ok']:
        job['result'] = {'rid': result['rid'], 'content': result['content'], 'format': fmt}
        job['status'] = 'done'
    else:
        job['result'] = result['error']
        job['status'] = 'error'

    # CLI scheduler:
    result = generate_one_report(ticker, fmt='md', progress_cb=logger.info)
    if result['ok']:
        logger.info('[%s] OK rid=%s (%.0fs)', ticker, result['rid'], result['elapsed_s'])
"""

import logging
import time
import traceback
from typing import Callable, Optional

logger = logging.getLogger(__name__)


def _noop(_msg: str) -> None:
    pass


def _load_report_inputs(ticker: str, progress_cb: Callable[[str], None]):
    """Run steps 1-4 of the report pipeline: load price/chip/fundamental + run technical analysis.

    Returns:
        tuple (report, chip_data, us_chip_data, fund_data, df_day) — same shape as
        assemble_prompt() / assemble_dashboard_prompt() expect.

    Raises:
        RuntimeError if price data unavailable (downstream cannot proceed).
    """
    # --- 1. Load price data ---
    progress_cb("📥 載入價量資料...")
    from technical_analysis import plot_dual_timeframe
    figures, errors, df_week, df_day, stock_meta = plot_dual_timeframe(ticker, force_update=False)
    if df_day is None or df_day.empty:
        raise RuntimeError('no price data available')

    # --- 2. Load chip data (TW or US) ---
    progress_cb("📥 載入籌碼資料...")
    chip_data, us_chip_data = None, None
    if ticker.isdigit() or ticker.endswith('.TW'):
        try:
            from chip_analysis import ChipAnalyzer, ChipFetchError
            chip_data = ChipAnalyzer().fetch_chip(ticker, scan_mode=False)
        except ChipFetchError as e:
            logger.warning("[%s] TW chip fetch failed: %s", ticker, e)
        except Exception as e:
            logger.warning("[%s] TW chip load unexpected error: %s: %s", ticker, type(e).__name__, e)
    else:
        try:
            from us_stock_chip import USStockChipAnalyzer
            us_chip_data, _err = USStockChipAnalyzer().get_chip_data(ticker)
        except Exception as e:
            logger.warning("[%s] US chip load failed: %s: %s", ticker, type(e).__name__, e)

    # --- 3. Load fundamental data ---
    progress_cb("📥 載入基本面資料...")
    try:
        from fundamental_analysis import get_fundamentals
        fund_data = get_fundamentals(ticker)
    except Exception as e:
        logger.warning("[%s] fundamental load failed: %s: %s", ticker, type(e).__name__, e)
        fund_data = None

    # --- 4. Run technical analyzer (trigger score + regime) ---
    progress_cb("📊 計算技術分析與觸發分數...")
    from analysis_engine import TechnicalAnalyzer
    analyzer = TechnicalAnalyzer(
        ticker, df_week, df_day,
        chip_data=chip_data, us_chip_data=us_chip_data,
    )
    report = analyzer.run_analysis()
    return report, chip_data, us_chip_data, fund_data, df_day


def assemble_prompt_only(
    ticker: str,
    fmt: str = 'html',
    progress_cb: Optional[Callable[[str], None]] = None,
    user_focus: Optional[str] = None,
) -> dict:
    """Run pipeline steps 1-4 + assemble prompt, but skip Claude CLI call.

    用途：2026-06-15 起 `claude -p` quota 與 claude.ai 訂閱分流計費後，user 可
    手動把 prompt 貼到 claude.ai 網頁跑（不消耗 Agent SDK Credit pool）。

    Returns:
        dict with keys:
          - ok: bool
          - prompt: str | None     — assembled prompt on success
          - format: 'html' | 'md'
          - elapsed_s: float
          - error: str | None
    """
    if progress_cb is None:
        progress_cb = _noop

    t_start = time.time()
    out = {'ok': False, 'prompt': None, 'format': fmt, 'elapsed_s': 0.0, 'error': None}
    try:
        inputs = _load_report_inputs(ticker, progress_cb)
        progress_cb("📝 組裝 prompt...")
        if fmt == 'html':
            from ai_report import assemble_dashboard_prompt
            prompt = assemble_dashboard_prompt(ticker, *inputs, user_focus=user_focus)
        else:
            from ai_report import assemble_prompt
            prompt = assemble_prompt(ticker, *inputs)
        out['ok'] = True
        out['prompt'] = prompt
    except RuntimeError as e:
        out['error'] = str(e)
    except Exception as e:
        logger.error("[%s] assemble_prompt_only unexpected error: %s", ticker, e, exc_info=True)
        out['error'] = f"{type(e).__name__}: {e}"
    finally:
        out['elapsed_s'] = time.time() - t_start
    return out


def generate_one_report(
    ticker: str,
    fmt: str = 'md',
    progress_cb: Optional[Callable[[str], None]] = None,
    with_research: bool = True,
    user_focus: Optional[str] = None,
) -> dict:
    """Run full AI report pipeline for one ticker.

    Args:
        ticker: stock id ('2330' / '2330.TW' / 'AAPL')
        fmt: 'md' or 'html'
        progress_cb: optional callable(msg: str) for progress updates.
            Use `logger.info` for CLI, `lambda m: with lock: job['progress'].append(m)` for UI.
        with_research: True 時 (僅 html 路徑) 先跑多代理 fan-out web 研究階段
            (report_web_research)，把已查證底稿注入主報告 prompt;fail-soft。

    Returns:
        dict with keys:
          - ok: bool
          - rid: str | None       — report id on success
          - content: str | None   — report content on success (for UI to display)
          - format: 'md' | 'html'
          - elapsed_s: float
          - error: str | None     — user-facing error on failure
          - traceback: str | None — full traceback on unexpected exception
    """
    if progress_cb is None:
        progress_cb = _noop

    t_start = time.time()
    result = {
        'ok': False,
        'rid': None,
        'content': None,
        'format': fmt,
        'elapsed_s': 0.0,
        'error': None,
        'traceback': None,
    }

    try:
        # --- Steps 1-4: load inputs ---
        try:
            report, chip_data, us_chip_data, fund_data, df_day = _load_report_inputs(ticker, progress_cb)
        except RuntimeError as e:
            result['error'] = str(e)
            result['elapsed_s'] = time.time() - t_start
            return result

        # --- 4.5 (html only) 多代理 fan-out web 研究階段 (fail-soft) ---
        web_research = None
        if fmt == 'html' and with_research:
            try:
                from report_web_research import run_web_research
                _is_us = bool(ticker) and not ticker.replace('.TW', '').isdigit()
                _stock_name = ''
                if fund_data:
                    for _k in ['stock_name', 'Name', 'shortName']:
                        _v = fund_data.get(_k, '')
                        if _v and str(_v) not in ('', 'N/A', 'None'):
                            _stock_name = str(_v)
                            break
                _research = run_web_research(ticker, stock_name=_stock_name,
                                             is_us=_is_us, progress_cb=progress_cb,
                                             user_focus=user_focus)
                if _research.get('ok'):
                    web_research = _research['brief']
            except Exception as _re:  # noqa: BLE001 - 研究階段失敗不可阻斷報告
                logger.warning("[%s] web research stage failed (fail-soft): %s", ticker, _re)

        # --- 5. Call Claude CLI + save ---
        if fmt == 'html':
            progress_cb("🤖 Claude AI 生成儀表板 JSON 中（10 min timeout，預期 1-5 分鐘）...")
            from ai_report import generate_report_html, save_report_html
            ok, content_or_err, json_data = generate_report_html(
                ticker, report, chip_data, us_chip_data, fund_data, df_day,
                timeout=600,  # LLM 規範 (2026-05-01): Claude 10 min
                web_research=web_research,
                user_focus=user_focus,
            )
            if not ok:
                result['error'] = content_or_err
                result['elapsed_s'] = time.time() - t_start
                return result
            progress_cb("💾 組裝 HTML + 儲存到報告庫...")
            rid = save_report_html(
                ticker, content_or_err,
                trigger_score=report.get('trigger_score'),
                trend_score=report.get('trend_score'),
                json_data=json_data,
            )
            result['ok'] = True
            result['rid'] = rid
            result['content'] = content_or_err
        else:
            progress_cb("🤖 Claude AI 生成 Markdown 報告中（10 min timeout，預期 1-5 分鐘）...")
            from ai_report import generate_report, save_report, post_validate_numbers, send_drift_discord
            ok, content = generate_report(
                ticker, report, chip_data, us_chip_data, fund_data, df_day,
                timeout=600,  # LLM 規範 (2026-05-01): Claude 10 min
            )
            if not ok:
                result['error'] = content
                result['elapsed_s'] = time.time() - t_start
                return result

            # Phase 3 safety net: 偵測 Section 8 三欄是否漂移到 candidate 外
            drift_check = post_validate_numbers(content, report.get('action_plan'))
            if drift_check['drift']:
                progress_cb(f"⚠️ 偵測到 Section 8 漂移: {drift_check['unexpected_numbers']} (預期 {drift_check['expected_numbers']})")
                logger.warning("[%s] DRIFT_DETECTED: %s", ticker, drift_check)
                send_drift_discord(ticker, drift_check)  # 不阻擋，仍繼續存檔
                # 在報告頂部加 badge 警告人工 audit
                badge = (
                    f"> ⚠️ **[DRIFT_DETECTED]** Section 8 三欄出現未預期數字 "
                    f"`{drift_check['unexpected_numbers']}`（預期 ground truth: "
                    f"`{drift_check['expected_numbers']}`）。Phase 1 hard rule 在此案例未完全服從，"
                    f"請人工 audit Section 8 三欄是否真的對應 deterministic action_plan。\n\n"
                )
                content = badge + content

            progress_cb("💾 儲存到報告庫...")
            rid = save_report(
                ticker, content,
                trigger_score=report.get('trigger_score'),
                trend_score=report.get('trend_score'),
            )
            result['ok'] = True
            result['rid'] = rid
            result['content'] = content

        result['elapsed_s'] = time.time() - t_start
        return result

    except Exception as e:
        logger.error("[%s] unexpected pipeline exception: %s", ticker, e, exc_info=True)
        result['error'] = f"{type(e).__name__}: {e}"
        result['traceback'] = traceback.format_exc()
        result['elapsed_s'] = time.time() - t_start
        return result
