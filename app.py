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
    on_history_change,
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

# Sidebar
st.sidebar.title("🔧 設定 (Settings)")

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

# ==========================================
# [NEW] 免責聲明與風險提示
# ==========================================
# 初始化 session state 用於追蹤是否顯示過免責聲明
if 'disclaimer_shown' not in st.session_state:
    st.session_state['disclaimer_shown'] = False
# 初始化分析快取 session state，避免 KeyError
for _key in ('df_week_cache', 'df_day_cache', 'force_update_cache', 'fund_cache'):
    if _key not in st.session_state:
        st.session_state[_key] = None

# 使用 expander 顯示免責聲明 (可收合)
with st.expander("⚠️ 投資風險提示 (請詳閱)", expanded=not st.session_state['disclaimer_shown']):
    st.markdown("""
    ### 📜 免責聲明 (Disclaimer)
    
    **本系統為技術分析輔助工具，所有分析結果僅供參考，不構成任何投資建議。**
    
    #### ⚠️ 投資風險提示
    - 🔹 股市投資有風險，過去績效不代表未來表現
    - 🔹 AI 評分模型基於歷史數據訓練，無法預測突發事件
    - 🔹 籌碼數據存在延遲 (T+1 或更久)，可能不反映即時狀況
    - 🔹 技術指標在盤整行情中可能產生大量假訊號
    - 🔹 建議結合基本面分析與自身判斷，審慎決策
    
    #### 📊 數據來源說明
    | 數據類型 | 來源 | 更新頻率 |
    |---------|------|---------|
    | 台股股價 | Yahoo Finance / FinMind | 每日收盤後 |
    | 美股股價 | Yahoo Finance | 即時 (延遲 15 分鐘) |
    | 台股籌碼 | FinMind (三大法人/融資券) | 每日 21:30 後 |
    | 美股籌碼 | Yahoo Finance (機構持股/空頭) | 每季 / 每月 |
    | 基本面數據 | Yahoo Finance / FinMind | 每季 / 每月 |
    | SEC 申報 | SEC EDGAR (13F/Form 4) | 即時 |
    | 美股情緒 | CNN Fear & Greed Index | 每日 |
    | 美股快照 | Finviz (技術面/估值) | 盤中 |
    
    #### 📝 使用條款
    - 本系統僅供個人學習研究使用，禁止商業用途
    - 用戶應自行承擔投資決策的全部風險
    - 系統開發者不對任何投資損失負責
    
    ---
    *點擊「收合」按鈕可隱藏此聲明*
    """)
    st.session_state['disclaimer_shown'] = True

# 側邊欄
with st.sidebar:
    st.header("⚙️ 設定面板")
    st.caption("Version: v2026.05.01.3")
    
    # input_method = "股票代號 (Ticker)" # Default, hidden
    
    target_ticker = "2330" # 預設值
    uploaded_file = None
    
    # [NEW] Search History (Dropdown)
    from cache_manager import CacheManager
    cm = CacheManager()
    cached_list = cm.list_cached_tickers()

    # History Dropdown
    if cached_list:
        st.selectbox(
            "🕒 歷史紀錄 (最近20筆)", 
            options=cached_list, 
            index=None, 
            placeholder="選擇歷史紀錄...",
            key='history_selected',
            on_change=on_history_change
        )

    # Always show Ticker input
    # Initialize session state if not present
    if 'ticker_input' not in st.session_state:
        st.session_state['ticker_input'] = '2330'
        
    target_ticker = st.text_input("輸入股票代號 (台股請加 .TW)", 
                                  key='ticker_input', # Bind to session state
                                  help="例如: 2330, TSM, AAPL")
    # CSV 上傳功能已移除，僅支援股票代號輸入

    # Only Run Button remains
    if st.button("🚀 開始分析", type="primary"):
        st.session_state['analysis_active'] = True
        st.session_state['force_run'] = False
        st.session_state['app_mode'] = 'analysis'

    st.markdown("---")

    # Mode toggle: 個股分析 / 自動選股 / 市場掃描 / AI 報告
    _mode_options = ['individual', 'screener', 'market_scan', 'ai_reports']
    _mode_labels = {'individual': '📈 個股分析', 'screener': '🔍 自動選股',
                    'market_scan': '📡 市場掃描', 'ai_reports': '📝 AI 報告'}
    _current_mode = st.session_state.get('app_mode', 'analysis')
    _mode_idx_map = {'screener': 1, 'market_scan': 2, 'ai_reports': 3}
    _mode_idx = _mode_idx_map.get(_current_mode, 0)
    app_mode = st.radio(
        "功能模式",
        options=_mode_options,
        format_func=lambda x: _mode_labels[x],
        index=_mode_idx,
        key='mode_radio',
        horizontal=True,
    )
    if app_mode == 'screener':
        st.session_state['app_mode'] = 'screener'
    elif app_mode == 'market_scan':
        st.session_state['app_mode'] = 'market_scan'
    elif app_mode == 'ai_reports':
        st.session_state['app_mode'] = 'ai_reports'
    else:
        st.session_state['app_mode'] = 'analysis'

    st.markdown("---")
    
    # === 數據來源與風險提示 (側邊欄底部) ===
    st.markdown("### 📊 數據來源")
    st.caption("""
    **台股**: FinMind / Yahoo Finance
    **美股**: Yahoo Finance / SEC EDGAR / Finviz
    **情緒**: CNN F&G
    **籌碼更新**: 每日 21:30 後
    """)
    
    st.markdown("### ⚠️ 風險提示")
    st.caption("""
    本系統分析結果僅供參考
    股市有風險，投資需謹慎
    歷史績效不代表未來表現
    """)

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

if st.session_state.get('app_mode') == 'screener':
    from screener_view import render_screener
    render_screener()

elif st.session_state.get('app_mode') == 'ai_reports':
    from ai_reports_view import render_ai_reports
    render_ai_reports()

elif st.session_state.get('app_mode') == 'market_scan':
    from market_scan_view import render_market_scan
    render_market_scan()

elif st.session_state.get('analysis_active', False):
    from individual_view import render_individual
    render_individual(target_ticker)

else:
    # 初始歡迎畫面
    st.info("👈 請在左測試欄輸入代號並點擊「開始分析」")

# ====================================================================
#  填入大盤儀表板 Banner（延後渲染，讓主內容先顯示）
#  放檔案最尾端：主內容全部 render 完才執行，fetch 卡住也不阻塞頁面
#  注意：若上方分支觸發 st.stop()，此段不會執行 → banner 在錯誤頁不顯示（可接受）
# ====================================================================
try:
    from market_banner import render_market_banner
    with _banner_slot.container():
        render_market_banner()
except Exception as _banner_err:
    logger.debug("Market banner failed: %s", _banner_err)

