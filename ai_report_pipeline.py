"""
AI 報告 pipeline — 單一入口，UI worker 與 CLI 共用。

Refactor rationale (2026-04-23, Robustness First Phase 2 H3):
  之前 `app.py:_ai_report_worker` 與 `tools/auto_ai_reports.py:_run_one` 是兩份
  幾乎一樣的 pipeline。UI 每天被操爆 → 錯誤立刻看到；CLI 只有 22:00 排程跑 →
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


def generate_one_report(
    ticker: str,
    fmt: str = 'md',
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """Run full AI report pipeline for one ticker.

    Args:
        ticker: stock id ('2330' / '2330.TW' / 'AAPL')
        fmt: 'md' or 'html'
        progress_cb: optional callable(msg: str) for progress updates.
            Use `logger.info` for CLI, `lambda m: with lock: job['progress'].append(m)` for UI.

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
        # --- 1. Load price data ---
        progress_cb("📥 載入價量資料...")
        from technical_analysis import plot_dual_timeframe
        figures, errors, df_week, df_day, stock_meta = plot_dual_timeframe(ticker, force_update=False)
        if df_day is None or df_day.empty:
            result['error'] = 'no price data available'
            result['elapsed_s'] = time.time() - t_start
            return result

        # --- 2. Load chip data (TW or US) ---
        progress_cb("📥 載入籌碼資料...")
        chip_data, us_chip_data = None, None
        if ticker.isdigit() or ticker.endswith('.TW'):
            # 用新的 fetch_chip (H5, 2026-04-23): 回純 dict，不會踩到 tuple-unpack footgun
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

        # --- 5. Call Claude CLI + save ---
        if fmt == 'html':
            progress_cb("🤖 Claude AI 生成儀表板 JSON 中（不設逾時，1-5 分鐘）...")
            from ai_report import generate_report_html, save_report_html
            ok, content_or_err, json_data = generate_report_html(
                ticker, report, chip_data, us_chip_data, fund_data, df_day,
                timeout=None,
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
            progress_cb("🤖 Claude AI 生成 Markdown 報告中（不設逾時，1-5 分鐘）...")
            from ai_report import generate_report, save_report
            ok, content = generate_report(
                ticker, report, chip_data, us_chip_data, fund_data, df_day,
                timeout=None,
            )
            if not ok:
                result['error'] = content
                result['elapsed_s'] = time.time() - t_start
                return result
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
