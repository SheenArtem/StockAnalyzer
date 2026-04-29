"""UI helper 函式集中地（Phase A 從 app.py 抽出）

收錄：
- 籌碼快取 wrapper（@st.cache_data）
- 歷史紀錄 dropdown callback
- 分析主流程封裝（run_analysis）
- AI 報告背景 worker + lock
- ticker validation
- picks 表 column 短字串 helper（週榜 / 題材 / 共振）

設計原則：保留原 app.py 的 lazy import 風格（避免 streamlit 啟動慢）。
"""

import logging
import re
import threading

import streamlit as st

from technical_analysis import (
    calculate_all_indicators,
    load_and_resample,
    plot_dual_timeframe,
    plot_interactive_chart,
)

logger = logging.getLogger(__name__)


# ====================================================================
# 籌碼快取（H5 2026-04-23 改用 ChipAnalyzer.fetch_chip 乾淨 API）
# ====================================================================
@st.cache_data(ttl=3600)
def get_chip_data_cached(ticker, force):
    """取得籌碼快取。

    Returns:
        dict | None: chip data dict on success, None on fetch failure.
        Caller 直接 `if chip_data is not None: ...`，不需 unpack。
    """
    from chip_analysis import ChipAnalyzer, ChipFetchError
    try:
        return ChipAnalyzer().fetch_chip(ticker, force_update=force)
    except ChipFetchError as e:
        logger.warning("Chip fetch failed for %s: %s", ticker, e)
        return None


# ====================================================================
# 歷史紀錄 dropdown callback（sidebar 用）
# ====================================================================
def on_history_change():
    """side bar 歷史紀錄 selectbox 的 on_change callback。"""
    selected = st.session_state.get('history_selected', '')
    if selected:
        selected = selected.strip()
        # Basic character check: only allow alphanumeric, dot, hyphen
        if not re.match(r'^[A-Za-z0-9.\-]{1,20}$', selected):
            logger.error(f"Invalid ticker from history dropdown: {selected!r}")
            return  # Do not activate analysis for invalid ticker
    st.session_state['ticker_input'] = selected
    st.session_state['analysis_active'] = True
    st.session_state['force_run'] = False


# ====================================================================
# 個股分析主流程封裝
# ====================================================================
def run_analysis(source_data, force_update=False):
    """個股分析主流程：股票代號或 CSV 都能餵。

    Returns:
        (figures, errors, df_week, df_day, stock_meta) 5-tuple
    """
    # 1. 股票代號情況
    if isinstance(source_data, str):
        return plot_dual_timeframe(source_data, force_update=force_update)

    # 2. CSV 資料情況 (DataFrame 無法直接 hash，需注意 cache 機制，這裡簡化處理)
    # Streamlit 對 DataFrame 有支援 hashing，所以通常可以直接傳
    ticker_name, df_day, df_week, stock_meta = load_and_resample(source_data)  # CSV no force update

    figures = {}
    errors = {}

    # 手動計算
    if not df_week.empty:
        try:
            df_week = calculate_all_indicators(df_week)
            fig_week = plot_interactive_chart(ticker_name, df_week, "Trend (Long)", "Weekly")
            figures['Weekly'] = fig_week
        except Exception as e:
            errors['Weekly'] = str(e)

    if not df_day.empty:
        try:
            df_day = calculate_all_indicators(df_day)
            fig_day = plot_interactive_chart(ticker_name, df_day, "Action (Short)", "Daily")
            figures['Daily'] = fig_day
        except Exception as e:
            errors['Daily'] = str(e)

    return figures, errors, df_week, df_day, stock_meta


# ====================================================================
# AI 報告背景執行緒 worker
# H3 (2026-04-23): 重構後與 tools/auto_ai_reports 共用 ai_report_pipeline
# H4 (2026-04-23): _ai_report_job_lock 保護 job dict 多步 state transition
# ====================================================================
_ai_report_job_lock = threading.Lock()


def _ai_report_worker(job, ticker, report_format='md', include_songfen=True):
    """在背景 thread 跑完整 AI 報告流程。

    job 是 session_state 裡的 dict 參照，thread 透過 _ai_report_job_lock 安全 mutate。
    禁止呼叫任何 st.* UI 函式（會觸發 ScriptRunContext 警告）。

    Args:
        report_format: 'md' = 傳統 Markdown 報告；'html' = 互動儀表板
        include_songfen: bool，md 格式時在最末尾附加「宋分視角補充分析」區塊。html 忽略。
    """
    from ai_report_pipeline import generate_one_report

    def _progress(msg):
        with _ai_report_job_lock:
            job['progress'].append(msg)

    try:
        result = generate_one_report(ticker, fmt=report_format,
                                     progress_cb=_progress,
                                     include_songfen=include_songfen)
        with _ai_report_job_lock:
            if result['ok']:
                job['result'] = {
                    'rid': result['rid'],
                    'content': result['content'],
                    'format': result['format'],
                }
                job['status'] = 'done'
            else:
                err = result.get('error') or 'unknown error'
                if result.get('traceback'):
                    err = f"{err}\n\n{result['traceback']}"
                job['result'] = err
                job['status'] = 'error'
    except Exception as _e:
        # Defensive: 通常 generate_one_report 自己會 catch，這層是 last-resort
        import traceback
        logger.error(f"[AI worker] uncaught exception: {_e}", exc_info=True)
        with _ai_report_job_lock:
            job['result'] = f"{type(_e).__name__}: {_e}\n\n{traceback.format_exc()}"
            job['status'] = 'error'


# ====================================================================
# Ticker validation
# ====================================================================
def validate_ticker(ticker):
    """驗證股票代號格式 (只允許英數字、點號、連字號)"""
    if not ticker:
        return False, "請輸入股票代號"
    # 只允許英數字、點號、連字號，長度 1-20
    pattern = r'^[A-Za-z0-9.\-]{1,20}$'
    if not re.match(pattern, ticker):
        return False, "股票代號格式不正確 (只允許英數字、點號)"
    return True, ""


# ====================================================================
# Picks 表 column 短字串 helper
# ====================================================================
def _wc_tags_short(stock_id):
    """取個股本週上榜 tags 並 join 成短字串給表格 column (BL-4 Phase C)。Empty -> ''."""
    try:
        from weekly_chip_loader import get_stock_tags as _wc_get
        tags = _wc_get(stock_id)
        return '; '.join(tags) if tags else ''
    except Exception:
        return ''


def _theme_tags_short(stock_id):
    """回傳 ticker 所屬題材中文名 short string；最多顯示 2 個 + 餘數 (VF-GM Phase 3)。

    從 sector_tags_manual.json 140 ticker / 29 multi-label 反向索引帶入。Empty -> ''.
    """
    try:
        from peer_comparison import get_ticker_themes as _gtt
        themes = _gtt(stock_id)
        if not themes:
            return ''
        zh_names = [t.get('zh', t.get('id', '')) for t in themes]
        head = ' / '.join(zh_names[:2])
        if len(zh_names) > 2:
            head += f' +{len(zh_names) - 2}'
        return head
    except Exception:
        return ''


def _convergence_label(stock_id, conv_map):
    """產生共振標記文字（QM/Value/Swing/MeanRev 多選股交集）"""
    c = conv_map.get(stock_id)
    if not c:
        return ''
    modes = c['modes']
    tier = c['tier']
    has_val = 'value' in modes
    has_mom = bool(set(modes) & {'momentum', 'swing', 'qm'})
    if has_val and has_mom:
        return f'T{tier} 動能+價值'
    return f'T{tier} {"+".join(modes)}'
