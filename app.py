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
    st.caption("Version: v2026.04.29.7")
    
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
    # ====================================================================
    #  AI 研究報告庫
    # ====================================================================
    from ai_report import (
        generate_report as _gen_report,
        save_report as _save_report,
        load_report_index as _load_index,
        load_report_content as _load_content,
        delete_report as _delete_report,
    )

    _report_tab_gen, _report_tab_lib = st.tabs(["✏️ 生成報告", "📚 報告庫"])

    # --- Tab 1: Generate ---
    with _report_tab_gen:
        # ==========================================================
        # [NEW] 背景生成：job 放 session_state，thread 跑完寫回結果
        # 切換 tab / app_mode 都不中斷；每次 rerun 檢查 job 狀態
        # ==========================================================
        _job = st.session_state.get('ai_report_job')

        # --- 處理已完成的 job ---
        if _job and _job.get('status') == 'done':
            _res = _job.get('result') or {}
            st.success(f"✅ **{_job['ticker']}** 報告生成完成！已儲存到報告庫（ID: `{_res.get('rid', 'N/A')}`）")
            st.info("請切到「📚 報告庫」tab 查看報告內容。")
            if st.button("清除通知並繼續", key='ai_clear_done'):
                del st.session_state['ai_report_job']
                st.rerun()

        elif _job and _job.get('status') == 'error':
            st.error(f"❌ **{_job['ticker']}** 生成失敗")
            with st.expander("錯誤訊息", expanded=True):
                st.code(_job.get('result') or "(無訊息)", language=None)
            if st.button("清除通知並重試", key='ai_clear_err'):
                del st.session_state['ai_report_job']
                st.rerun()

        # --- 顯示執行中的 banner + 自動刷新 ---
        if _job and _job.get('status') == 'running':
            _elapsed = int(time.time() - _job['start_time'])
            _mm, _ss = _elapsed // 60, _elapsed % 60
            st.warning(f"⏳ 正在生成 **{_job['ticker']}** 的研究報告... (已過 {_mm} 分 {_ss} 秒)")
            st.info("💡 可以安心切換到其他頁面，生成會在背景繼續進行。回到此頁會看到進度。")
            with st.expander("進度", expanded=True):
                # H4: 用 list() 快照避免 worker thread append 時 iterate 出錯
                with _ai_report_job_lock:
                    _progress_snapshot = list(_job.get('progress', []))
                for _msg in _progress_snapshot:
                    st.write(f"• {_msg}")
            # Auto-refresh every 2s to update elapsed + progress
            time.sleep(2)
            st.rerun()

        # --- 輸入區（執行中時禁用） ---
        st.markdown("輸入股票代號，Claude AI 將根據系統所有數據生成深度研究報告。")
        _is_running = _job is not None and _job.get('status') == 'running'

        _col_t, _col_f = st.columns([3, 2])
        with _col_t:
            _ai_ticker = st.text_input(
                "股票代號", placeholder="例: 2330, AAPL",
                key='ai_report_ticker',
                disabled=_is_running,
            )
        with _col_f:
            _format_labels = {
                'html': '📊 互動儀表板 (HTML)',
                'md': '📝 傳統報告 (Markdown)',
            }
            _ai_format = st.radio(
                "產出格式",
                options=['html', 'md'],
                format_func=lambda x: _format_labels[x],
                key='ai_report_format',
                disabled=_is_running,
                horizontal=False,
            )

        # 宋分視角 — 只對 Markdown 格式生效；HTML 儀表板 schema 固定不支援
        _songfen_disabled = _is_running or (_ai_format == 'html')
        _songfen_help = (
            "新增第 10 區塊「宋分視角補充分析」：套用機構分析師 re-rate 訊號 / "
            "5-layer 損益表 / 擇時紀律 / 反面論點。框架來源見 prompts/songfen_framework.md。"
            if _ai_format == 'md'
            else "HTML 互動儀表板 schema 固定，不支援宋分區塊。請改選 Markdown 格式。"
        )
        _include_songfen = st.checkbox(
            "✅ 加入宋分視角區塊（Markdown 格式限定）",
            value=True,
            key='ai_report_include_songfen',
            disabled=_songfen_disabled,
            help=_songfen_help,
        )

        if st.button("生成研究報告", type="primary", key='ai_gen_btn', disabled=_is_running):
            if not _ai_ticker or not _ai_ticker.strip():
                st.error("請輸入股票代號")
            else:
                _ai_ticker = _ai_ticker.strip().upper()
                is_valid, err_msg = validate_ticker(_ai_ticker)
                if not is_valid:
                    st.error(f"代號格式不正確: {err_msg}")
                else:
                    # md 格式才套用 songfen；html 忽略
                    _effective_songfen = bool(_include_songfen) and (_ai_format == 'md')
                    _new_job = {
                        'ticker': _ai_ticker,
                        'status': 'running',
                        'start_time': time.time(),
                        'progress': [],
                        'result': None,
                        'format': _ai_format,
                        'include_songfen': _effective_songfen,
                    }
                    st.session_state['ai_report_job'] = _new_job
                    _t = threading.Thread(
                        target=_ai_report_worker,
                        args=(_new_job, _ai_ticker, _ai_format, _effective_songfen),
                        daemon=True,
                    )
                    _t.start()
                    st.rerun()

    # --- Tab 2: Library ---
    with _report_tab_lib:
        _index = _load_index()

        if not _index:
            st.info("報告庫是空的。請先在「✏️ 生成報告」tab 生成報告。")
        else:
            # Filter
            _all_tickers = sorted(set(r['ticker'] for r in _index))
            _filter_ticker = st.selectbox(
                "篩選股票", ['全部'] + _all_tickers, key='report_filter_ticker')

            _filtered = _index if _filter_ticker == '全部' else [
                r for r in _index if r['ticker'] == _filter_ticker]
            _filtered = sorted(_filtered, key=lambda x: x.get('date', '') + x.get('time', ''), reverse=True)

            st.caption(f"共 {len(_filtered)} 篇報告")

            # Report list
            _list_rows = []
            for _r in _filtered:
                _fmt = _r.get('format', 'md')
                _fmt_label = '📊 儀表板' if _fmt == 'html' else '📝 Markdown'
                _list_rows.append({
                    '日期': f"{_r.get('date', '')} {_r.get('time', '')[:5]}",
                    '股票': _r['ticker'],
                    '格式': _fmt_label,
                    '觸發分數': _r.get('trigger_score') or '',
                    '趨勢分數': _r.get('trend_score') or '',
                    'ID': _r['report_id'],
                })
            if _list_rows:
                st.dataframe(pd.DataFrame(_list_rows), width='stretch', hide_index=True)

            # Report viewer
            def _opt_label(r):
                _f = r.get('format', 'md')
                _badge = '📊' if _f == 'html' else '📝'
                return f"{_badge} {r.get('date', '')} {r['ticker']}"

            _report_options = [_opt_label(r) for r in _filtered]
            _report_ids = [r['report_id'] for r in _filtered]
            _report_formats = [r.get('format', 'md') for r in _filtered]

            if _report_options:
                _sel_idx = st.selectbox(
                    "選擇報告", range(len(_report_options)),
                    format_func=lambda i: _report_options[i],
                    key='report_viewer_sel',
                )
                _sel_id = _report_ids[_sel_idx]
                _sel_fmt = _report_formats[_sel_idx]
                _sel_content = _load_content(_sel_id)

                if _sel_content:
                    st.markdown("---")

                    if _sel_fmt == 'html':
                        # 工具列：在瀏覽器開啟 + 下載
                        from ai_report import get_report_filepath as _get_fp
                        _fp = _get_fp(_sel_id)

                        _c1, _c2, _c3 = st.columns([2, 2, 6])
                        with _c1:
                            if _fp and st.button("🌐 在瀏覽器開啟", key='html_open_btn', type='primary'):
                                import webbrowser
                                webbrowser.open(f"file:///{_fp.replace(chr(92), '/')}")
                        with _c2:
                            st.download_button(
                                "💾 下載 HTML",
                                data=_sel_content,
                                file_name=f"{_sel_id}.html",
                                mime='text/html',
                                key='html_download_btn',
                            )

                        st.caption("💡 如顯示不全請點「在瀏覽器開啟」看完整版（無高度限制）")
                        # 內嵌 iframe 預覽 — 固定高 2600px、關閉內部 scrolling 消除雙層 scrollbar。
                        # Streamlit 的 components.v1.html 不會實際響應 postMessage(streamlit:setFrameHeight)，
                        # 所以改走「給夠高的固定 height + scrolling=False」，多數報告 2600 可完整顯示；
                        # 超長報告（極端多 risk item 等）走「在瀏覽器開啟」按鈕。
                        import streamlit.components.v1 as _components
                        _components.html(_sel_content, height=2600, scrolling=False)
                    else:
                        # 報告含 <span style="color:..."> 顏色標記，需允許 HTML 才能正確渲染
                        st.markdown(_sel_content, unsafe_allow_html=True)

                    st.markdown("---")
                    st.caption("此報告由 Claude AI 基於系統數據自動生成，僅供參考，不構成投資建議。")

                    if st.button("🗑️ 刪除此報告", key='report_delete_btn'):
                        _delete_report(_sel_id)
                        st.success("報告已刪除")
                        st.rerun()

elif st.session_state.get('app_mode') == 'market_scan':
    # ====================================================================
    #  📡 市場掃描 mode (2026-04-27 新增)
    #  目前 1 個 tab: 法人週榜 (BL-4 三大法人週報，4 維度 × 4 排行 = 16 個 Top 10)
    #  未來可擴展 ETF 換手榜 / 月營收熱度 / 處置股清單 等 market-wide 報告
    # ====================================================================
    st.title("📡 市場掃描")

    _ms_tab_chip, = st.tabs(["📊 法人週榜"])

    with _ms_tab_chip:
        from weekly_chip_loader import (
            load_latest as _wc_load,
            get_metadata as _wc_meta,
            get_rankings as _wc_rank,
            DIM_LABELS_ZH as _WC_DIM_LABELS,
        )

        _wc_df = _wc_load()
        _wc_md = _wc_meta()
        if _wc_df is None or _wc_md is None:
            st.warning("⚠️ 週榜資料尚未產出。請先跑 `python tools/weekly_chip_report.py` 或等週六 08:00 自動 batch。")
        else:
            _week_end_str = _wc_md['week_end'].strftime('%Y-%m-%d')
            st.caption(
                f"統計窗口收尾於 **{_week_end_str}** · 共 {_wc_md['unique_stocks']} 檔上榜 · "
                f"全市場掃描 · 4 維度 × 4 排行 = 16 個 Top 10 · "
                f"每週六 08:00 自動更新"
            )

            # 維度 selectbox
            _dim_choice = st.selectbox(
                "維度",
                options=['total', 'foreign', 'trust', 'dealer'],
                format_func=lambda d: _WC_DIM_LABELS[d],
                key='ms_dim_choice',
            )

            # 4 個 ranking 欄位顯示
            _col_a, _col_b = st.columns(2)
            _rank_specs = [
                ('consec_buy', '🔥 連續買超天數 Top 10', _col_a),
                ('consec_sell', '🧊 連續賣超天數 Top 10', _col_b),
                ('week_buy', '💰 當週買超金額 Top 10', _col_a),
                ('week_sell', '💸 當週賣超金額 Top 10', _col_b),
            ]
            for _rt_key, _rt_label, _col in _rank_specs:
                with _col:
                    st.markdown(f"**{_rt_label}**")
                    _rdf = _wc_rank(_dim_choice, _rt_key, top_n=10)
                    if _rdf.empty:
                        st.caption("(本週無此類標的)")
                        continue
                    # 美化 columns 給 UI 顯示
                    _disp = _rdf[['rank', 'stock_id', 'stock_name', 'consec_days', 'weekly_amount_k']].copy()
                    _disp.columns = ['#', 'ID', '名稱', '連續日', '金額(千)']
                    # 金額單位千→億 顯示
                    _disp['金額(億)'] = (_disp['金額(千)'] / 1e5).round(1)
                    _disp = _disp.drop(columns=['金額(千)'])
                    st.dataframe(_disp, hide_index=True, use_container_width=True,
                                  column_config={
                                      '#': st.column_config.NumberColumn(width='small'),
                                      'ID': st.column_config.TextColumn(width='small'),
                                      '名稱': st.column_config.TextColumn(width='small'),
                                      '連續日': st.column_config.NumberColumn(width='small'),
                                      '金額(億)': st.column_config.NumberColumn(format="%+.1f"),
                                  })

            st.markdown("---")
            # 跳轉個股分析功能
            with st.expander("🔍 跳轉個股分析（從週榜挑股深入研究）", expanded=False):
                _all_ids = _wc_df[_wc_df['dim'] == _dim_choice]['stock_id'].unique().tolist()
                if _all_ids:
                    _picked = st.selectbox(
                        "選股",
                        options=[''] + _all_ids,
                        format_func=lambda s: '— 請選擇 —' if not s else f"{s} {_wc_df[_wc_df['stock_id']==s]['stock_name'].iloc[0]}",
                        key='ms_jump_pick',
                    )
                    if _picked and st.button("🚀 跳到個股分析", key='ms_jump_btn'):
                        st.session_state['ticker_input'] = _picked
                        st.session_state['analysis_active'] = True
                        st.session_state['app_mode'] = 'analysis'
                        st.rerun()

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

