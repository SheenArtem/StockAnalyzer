import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import logging
import threading
import time
from technical_analysis import plot_dual_timeframe, load_and_resample, calculate_all_indicators, plot_interactive_chart
from fundamental_analysis import get_fundamentals, get_revenue_history, get_per_history, get_financial_statements
from ui_helpers import (
    get_chip_data_cached,
    run_analysis,
    _ai_report_worker,
    _ai_report_job_lock,
    validate_ticker,
    _wc_tags_short,
    _theme_tags_short,
    _convergence_label,
)

logger = logging.getLogger(__name__)


# 設定頁面配置
st.set_page_config(
    page_title="StockPulse 智能選股分析系統",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS 美化
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    .stButton>button {
        width: 100%;
        background-color: #ff4b4b;
        color: white;
    }
    .main-header {
        font-size: 2.5rem;
        color: #fafafa;
        text-align: center;
        margin-bottom: 1rem;
    }
    /* Increase Sidebar Width - Only when expanded */
    section[data-testid="stSidebar"][aria-expanded="true"] {
        min-width: 250px !important;
        width: 250px !important;
    }
</style>
""", unsafe_allow_html=True)

# 標題
st.markdown('<div class="main-header">📈 StockPulse 智能選股分析系統</div>', unsafe_allow_html=True)

# 初始化 session state 用於追蹤是否顯示過免責聲明（expander 預設展開狀態）
if 'disclaimer_shown' not in st.session_state:
    st.session_state['disclaimer_shown'] = False
# 初始化分析快取 session state，避免 KeyError
for _key in ('df_week_cache', 'df_day_cache', 'force_update_cache', 'fund_cache'):
    if _key not in st.session_state:
        st.session_state[_key] = None

# 側邊欄
with st.sidebar:
    st.caption("Version: v2026.05.30.8")

    # 初始化 ticker_input session state（其他模式切回個股時要有預設值）
    if 'ticker_input' not in st.session_state:
        st.session_state['ticker_input'] = '2330'

    # Mode toggle: 個股分析 / 市場掃描 / AI 報告 / 主力選股 / 總經大盤風向
    # 2026-05-22: whale_picks 重啟上 UI; brokerage_yt 從 UI 移除 (節省 LLM quota)
    # 強勢股報告仍停用 (Opus 6/15 後改 SDK Credit pool)
    # 2026-05-23: 'screener' (自動選股 QM/Value/Mode D) 從 UI 移除 — daily scheduler 已停
    # (commit 56dcc6c)，UI 顯示會 stale 且 100% Whale Picks 拍板後不再需要。
    # 復原方式：把對應 mode 名加回 _mode_options + idx_map (render handler/label 保留為死代碼)
    _mode_options = ['individual', 'market_scan', 'ai_reports', 'whale_picks', 'macro']
    _mode_labels = {'individual': '📈 個股分析', 'screener': '🔍 自動選股',
                    'market_scan': '📡 市場掃描', 'ai_reports': '📝 AI 報告',
                    'strong_stocks': '🌟 強勢股報告', 'whale_picks': '🐋 主力選股',
                    'macro': '🧭 總經大盤風向',
                    'brokerage_yt': '📺 投顧追蹤'}
    _current_mode = st.session_state.get('app_mode', 'analysis')
    _mode_idx_map = {'market_scan': 1, 'ai_reports': 2,
                     'whale_picks': 3, 'macro': 4}
    _mode_idx = _mode_idx_map.get(_current_mode, 0)
    app_mode = st.radio(
        "功能模式",
        options=_mode_options,
        format_func=lambda x: _mode_labels[x],
        index=_mode_idx,
        key='mode_radio',
        horizontal=True,
        label_visibility="collapsed",
    )
    # screener (自動選股) 2026-05-23 從 UI 移除；session_state 設回 analysis
    if app_mode == 'market_scan':
        st.session_state['app_mode'] = 'market_scan'
    elif app_mode == 'ai_reports':
        st.session_state['app_mode'] = 'ai_reports'
    elif app_mode == 'strong_stocks':
        st.session_state['app_mode'] = 'strong_stocks'
    elif app_mode == 'whale_picks':
        st.session_state['app_mode'] = 'whale_picks'
    elif app_mode == 'macro':
        st.session_state['app_mode'] = 'macro'
    elif app_mode == 'brokerage_yt':
        st.session_state['app_mode'] = 'brokerage_yt'
    else:
        st.session_state['app_mode'] = 'analysis'

    st.markdown("---")
    
    # === 免責聲明 + 數據來源 + 風險提示（2026-05-06 從主畫面移到 sidebar）===
    with st.expander("⚠️ 投資風險提示 (請詳閱)",
                     expanded=not st.session_state['disclaimer_shown']):
        st.markdown("""
**本系統為技術分析輔助工具，所有分析結果僅供參考，不構成任何投資建議。**

#### ⚠️ 投資風險
- 股市投資有風險，過去績效不代表未來表現
- AI 評分模型基於歷史數據訓練，無法預測突發事件
- 籌碼數據存在延遲 (T+1 或更久)，可能不反映即時狀況
- 技術指標在盤整行情中可能產生大量假訊號
- 建議結合基本面分析與自身判斷，審慎決策

#### 📊 數據來源
- **台股股價**: Yahoo Finance / FinMind（每日收盤後）
- **美股股價**: Yahoo Finance（即時，延遲 15 分鐘）
- **台股籌碼**: FinMind 三大法人 / 融資券（每日 21:30 後）
- **美股籌碼**: Yahoo Finance 機構持股 / 空頭（每季 / 每月）
- **基本面**: Yahoo Finance / FinMind（每季 / 每月）
- **SEC 申報**: SEC EDGAR 13F / Form 4（即時）
- **美股情緒**: CNN Fear & Greed Index（每日）
- **美股快照**: Finviz 技術面 / 估值（盤中）

#### 📝 使用條款
- 本系統僅供個人學習研究使用，禁止商業用途
- 用戶應自行承擔投資決策的全部風險
- 系統開發者不對任何投資損失負責
""")
        st.session_state['disclaimer_shown'] = True

    st.markdown("---")

    # === Cache 健康度監控 (Cache 三層 P5, 2026-04-29) ===
    with st.expander("📊 Cache 健康度", expanded=False):
        try:
            from pathlib import Path as _CP
            import json as _CJ
            from datetime import date as _CD

            _repo = _CP(__file__).resolve().parent
            _frozen_dir = _repo / 'data_cache' / 'fundamental_frozen'
            _live_dir = _repo / 'data_cache' / 'fundamental_cache'

            # Layer 0/1 stocks count
            _frozen_files = list(_frozen_dir.glob('*.parquet')) if _frozen_dir.exists() else []
            _live_files = list(_live_dir.glob('*.parquet')) if _live_dir.exists() else []
            _frozen_stocks = len({f.stem.split('_', 1)[1] for f in _frozen_files if '_' in f.stem})
            _live_stocks = len({f.stem.split('_', 1)[1] for f in _live_files if '_' in f.stem})

            st.markdown("**Layer 0/1 fundamental cache**")
            st.caption(f"frozen: {len(_frozen_files)} parquets / {_frozen_stocks} stocks")
            st.caption(f"live: {len(_live_files)} parquets / {_live_stocks} stocks")

            # MOPS daily usage
            _mops_usage_file = _repo / 'data_cache' / 'mops_daily_usage.json'
            if _mops_usage_file.exists():
                try:
                    _mu = _CJ.loads(_mops_usage_file.read_text(encoding='utf-8'))
                    _today = _CD.today().isoformat()
                    if _mu.get('date') == _today:
                        st.markdown("**MOPS 今日用量**")
                        import os as _COS
                        _cap = int(_COS.getenv('MOPS_DAILY_CAP', '500'))
                        _cnt = _mu.get('count', 0)
                        _pct = (_cnt / _cap * 100) if _cap else 0
                        if _pct < 50:
                            st.success(f"{_cnt} / {_cap} req ({_pct:.0f}%)")
                        elif _pct < 80:
                            st.warning(f"{_cnt} / {_cap} req ({_pct:.0f}%)")
                        else:
                            st.error(f"{_cnt} / {_cap} req ({_pct:.0f}%)")
                    else:
                        st.caption(f"MOPS 今日尚未呼叫 (last: {_mu.get('date', '?')})")
                except Exception:
                    pass

            # FinMind hour usage
            try:
                from cache_manager import get_finmind_stats as _gfs
                _fs = _gfs()
                if _fs:
                    st.markdown("**FinMind 當前小時用量**")
                    _fcnt = _fs.get('request_count', 0)
                    _frem = _fs.get('remaining', 0)
                    _frate = _fs.get('rate_per_hour', 0)
                    _ftok = _fs.get('has_token', False)
                    _flim = _fcnt + _frem
                    _fpct = (_fcnt / _flim * 100) if _flim else 0
                    _ttag = "🔑 token" if _ftok else "⚪ anon"
                    if _fpct < 50:
                        st.success(f"{_fcnt}/{_flim} req ({_fpct:.0f}%) {_ttag}")
                    elif _fpct < 80:
                        st.warning(f"{_fcnt}/{_flim} req ({_fpct:.0f}%) {_ttag}")
                    else:
                        st.error(f"{_fcnt}/{_flim} req ({_fpct:.0f}%) {_ttag}")
                    st.caption(f"當前 rate: {_frate} req/hr")
            except Exception:
                pass

            # data_cache total size
            try:
                _cache_root = _repo / 'data_cache'
                if _cache_root.exists():
                    _total_bytes = sum(p.stat().st_size for p in _cache_root.rglob('*') if p.is_file())
                    _gb = _total_bytes / (1024 ** 3)
                    st.caption(f"**data_cache 總大小**: {_gb:.2f} GB")
            except Exception:
                pass

            st.caption("📌 frozen layer 唯讀（promote_to_frozen.py 推升），live 為日常 backfill 寫入點")
        except Exception as _e:
            st.caption(f"cache 健康度載入失敗: {_e}")

    st.markdown("---")

# ====================================================================
#  大盤儀表板 Banner placeholder（所有模式共用）
#  於頁面頂端保留位置，等主內容渲染完畢後才填入，避免 cache miss 卡住整頁
# ====================================================================
_banner_slot = st.empty()

# 自動選股 2026-05-23 從 UI 移除 (commit 56dcc6c 100% Whale Picks 拍板，daily
# scheduler 已停 QM/Value/Mode D，UI 顯示會 stale)
# 復原：把以下三行 elif 取消註解 + app_mode 'screener' 加回 _mode_options + idx_map
# if st.session_state.get('app_mode') == 'screener':
#     from screener_view import render_screener
#     render_screener()

if st.session_state.get('app_mode') == 'ai_reports':
    from ai_reports_view import render_ai_reports
    render_ai_reports()

elif st.session_state.get('app_mode') == 'market_scan':
    from market_scan_view import render_market_scan
    render_market_scan()

# 強勢股報告 2026-05-21 暫時停用（Opus stage 已停 → render 出來只會是過期報告）
# 復原：把以下三行 elif 取消註解 + app_mode 'strong_stocks' 加回 _mode_options
# elif st.session_state.get('app_mode') == 'strong_stocks':
#     from strong_stocks_view import render_strong_stocks
#     render_strong_stocks()

# 主力選股 2026-05-22 重啟（scanner whale_picks stage 同步啟用）
elif st.session_state.get('app_mode') == 'whale_picks':
    from whale_picks_view import render_whale_picks
    render_whale_picks()

elif st.session_state.get('app_mode') == 'macro':
    from macro_dashboard import render_macro_dashboard
    render_macro_dashboard()

# 投顧追蹤 2026-05-22 從 UI 移除（節省 codex/Sonnet LLM quota）
# 復原：把以下三行 elif 取消註解 + app_mode 'brokerage_yt' 加回 _mode_options
# elif st.session_state.get('app_mode') == 'brokerage_yt':
#     from brokerage_view import render_brokerage_yt
#     render_brokerage_yt()

else:
    # 個股分析（預設 tab）：輸入表單 + 分析結果都在 individual_view 內
    from individual_view import render_individual
    render_individual()

# ====================================================================
#  填入大盤儀表板 Banner（延後渲染，讓主內容先顯示）
#  放檔案最尾端：主內容全部 render 完才執行，fetch 卡住也不阻塞頁面
#
#  分流規則 (2026-05-09)：
#    - macro tab: 不渲染（總經大盤風向 內部已 call 完整 banner）
#    - individual analysis: 完整 banner（個股分析需要大盤背景）
#    - 其他 (screener / scan / ai_reports / strong_stocks / whale_picks): 不渲染，避免卡 fetch
# ====================================================================
_mode_now = st.session_state.get('app_mode')
_should_render_banner = (
    _mode_now == 'analysis' or
    (_mode_now is None and st.session_state.get('analysis_active', False))
)
if _should_render_banner:
    try:
        from market_banner import render_market_banner
        with _banner_slot.container():
            render_market_banner()
    except Exception as _banner_err:
        logger.debug("Market banner failed: %s", _banner_err)

