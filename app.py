import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import logging
from technical_analysis import plot_dual_timeframe, load_and_resample, calculate_all_indicators, plot_interactive_chart
from fundamental_analysis import get_fundamentals, get_revenue_history, get_per_history, get_financial_statements

logger = logging.getLogger(__name__)

@st.cache_data(ttl=3600)
def get_chip_data_cached(ticker, force):
    from chip_analysis import ChipAnalyzer
    analyzer = ChipAnalyzer()
    return analyzer.get_chip_data(ticker, force_update=force)


# 設定頁面配置
st.set_page_config(
    page_title="股票右側分析系統",
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
st.markdown('<div class="main-header">📈 股票右側分析系統</div>', unsafe_allow_html=True)

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
    | 搜尋熱度 | Google Trends | 每日 |
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
    st.caption("Version: v2026.04.11.06")
    
    # input_method = "股票代號 (Ticker)" # Default, hidden
    
    target_ticker = "2330" # 預設值
    uploaded_file = None
    
    # [NEW] Search History (Dropdown)
    from cache_manager import CacheManager
    cm = CacheManager()
    cached_list = cm.list_cached_tickers()
    
    # Callback for history selection
    def on_history_change():
        import re
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

    # Mode toggle: 個股分析 vs 自動選股
    app_mode = st.radio(
        "功能模式",
        options=['individual', 'screener'],
        format_func=lambda x: '📈 個股分析' if x == 'individual' else '🔍 自動選股',
        index=0 if st.session_state.get('app_mode', 'analysis') != 'screener' else 1,
        key='mode_radio',
        horizontal=True,
    )
    if app_mode == 'screener':
        st.session_state['app_mode'] = 'screener'
    else:
        st.session_state['app_mode'] = 'analysis'

    st.markdown("---")
    
    # === 數據來源與風險提示 (側邊欄底部) ===
    st.markdown("### 📊 數據來源")
    st.caption("""
    **台股**: FinMind / Yahoo Finance
    **美股**: Yahoo Finance / SEC EDGAR / Finviz
    **情緒**: CNN F&G / PTT / Google Trends
    **籌碼更新**: 每日 21:30 後
    """)
    
    st.markdown("### ⚠️ 風險提示")
    st.caption("""
    本系統分析結果僅供參考  
    股市有風險，投資需謹慎  
    歷史績效不代表未來表現
    """)
    
    st.markdown("---")

# 封裝分析函數 (暫時移除 Cache 以確保代碼更新生效)
# @st.cache_data(ttl=3600)
def run_analysis(source_data, force_update=False):
    # 這裡的邏輯與原本 main 當中的一樣，但搬進來做 cache
    
    # 1. 股票代號情況
    if isinstance(source_data, str):
        return plot_dual_timeframe(source_data, force_update=force_update)
        
    # 2. CSV 資料情況 (DataFrame 無法直接 hash，需注意 cache 機制，這裡簡化處理)
    # Streamlit 對 DataFrame 有支援 hashing，所以通常可以直接傳
    ticker_name, df_day, df_week, stock_meta = load_and_resample(source_data) # CSV no force update
    
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

# 主程式邏輯


def validate_ticker(ticker):
    """驗證股票代號格式 (只允許英數字、點號、連字號)"""
    import re
    if not ticker:
        return False, "請輸入股票代號"
    # 只允許英數字、點號、連字號，長度 1-20
    pattern = r'^[A-Za-z0-9.\-]{1,20}$'
    if not re.match(pattern, ticker):
        return False, "股票代號格式不正確 (只允許英數字、點號)"
    return True, ""

if st.session_state.get('app_mode') == 'screener':
    # ====================================================================
    #  自動選股模式 — 右側動能 + 左側價值
    # ====================================================================
    import json as _json
    from pathlib import Path as _Path

    screener_tab1, screener_tab_us, screener_tab2 = st.tabs(["📈 右側動能 (台股)", "🇺🇸 右側動能 (美股)", "💎 左側價值"])

    # ====================================================================
    # Tab 1: 右側動能選股
    # ====================================================================
    with screener_tab1:

        latest_file = _Path('data/latest/momentum_result.json')
    scan_result = None
    if latest_file.exists():
        try:
            with open(latest_file, 'r', encoding='utf-8') as _f:
                scan_result = _json.load(_f)
        except Exception:
            scan_result = None

    # --- Scan controls ---
    col_scan1, col_scan2, col_scan3 = st.columns([2, 2, 3])
    with col_scan1:
        if st.button("⚡ 快速預覽 (Stage 1)", help="只跑初篩，不算觸發分數，約10秒"):
            st.session_state['screener_run'] = 'stage1'
    with col_scan2:
        no_chip = st.checkbox("跳過籌碼 (加速)", value=True, key='screener_no_chip')

    # --- Stage 1 quick preview ---
    if st.session_state.get('screener_run') == 'stage1':
        with st.spinner("初篩中..."):
            from momentum_screener import MomentumScreener
            _screener = MomentumScreener()
            _df = _screener.run_stage1_only()
        st.session_state['screener_run'] = None
        if not _df.empty:
            st.success(f"初篩通過: {len(_df)} 檔")
            # Format for display
            _df['TV (億)'] = (_df['trading_value'] / 1e8).round(1)
            _df['漲跌%'] = _df['change_pct'].round(2)
            _df['佔比%'] = (_df['tv_pct'] * 100).round(3) if 'tv_pct' in _df.columns else 0
            _show_cols = ['stock_id', 'stock_name', 'market', 'close', '漲跌%', 'TV (億)', '佔比%']
            _show_cols = [c for c in _show_cols if c in _df.columns]
            st.dataframe(
                _df[_show_cols].rename(columns={
                    'stock_id': '代號', 'stock_name': '名稱', 'market': '市場', 'close': '收盤'
                }),
                use_container_width=True,
                height=500,
            )
        else:
            st.warning("初篩無結果（可能休市）")

    # --- Show latest full scan results ---
    elif scan_result and scan_result.get('results'):
        results = scan_result['results']
        st.caption(
            f"掃描日期: {scan_result.get('scan_date', '?')} {scan_result.get('scan_time', '')} | "
            f"全市場 {scan_result.get('total_scanned', 0)} 檔 → "
            f"初篩 {scan_result.get('passed_initial', 0)} 檔 → "
            f"評分 {scan_result.get('scored_count', 0)} 檔 | "
            f"耗時 {scan_result.get('elapsed_seconds', 0):.0f}s"
        )

        # Build DataFrame for display
        _rows = []
        for r in results:
            _rows.append({
                '排名': len(_rows) + 1,
                '代號': r['stock_id'],
                '名稱': r.get('name', ''),
                '市場': r.get('market', ''),
                '收盤': r.get('price', 0),
                '漲跌%': r.get('change_pct', 0),
                '觸發分數': r.get('trigger_score', 0),
                '趨勢分數': r.get('trend_score', 0),
                '百分位': r.get('score_percentile', ''),
                'Regime': r.get('regime', ''),
                'ETF買超': r.get('etf_buy_count', 0),
                '關鍵訊號': ', '.join(r.get('signals', [])[:3]),
            })
        _df_results = pd.DataFrame(_rows)

        # Color-code trigger score
        st.dataframe(
            _df_results,
            use_container_width=True,
            height=600,
            column_config={
                '觸發分數': st.column_config.NumberColumn(format="%.1f"),
                '趨勢分數': st.column_config.NumberColumn(format="%.1f"),
                '漲跌%': st.column_config.NumberColumn(format="%.1f%%"),
                '收盤': st.column_config.NumberColumn(format="%.1f"),
            },
        )

        # Click to analyze: user can copy stock ID from table and paste to sidebar
        st.info("點擊表格中的股票代號，複製後切回「個股分析」模式即可深入分析")

        # Show signal distribution
        if len(results) > 5:
            with st.expander("訊號統計"):
                from collections import Counter
                _sig_counter = Counter()
                for r in results:
                    for s in r.get('signals', []):
                        if not s.startswith('regime_'):
                            _sig_counter[s] += 1
                if _sig_counter:
                    _sig_df = pd.DataFrame(
                        _sig_counter.most_common(15),
                        columns=['訊號', '出現次數']
                    )
                    st.bar_chart(_sig_df.set_index('訊號'))

        # Detailed trigger breakdown (expandable per stock)
        with st.expander("個股詳細評分"):
            _selected = st.selectbox(
                "選擇股票",
                options=[f"{r['stock_id']} {r.get('name', '')}" for r in results],
                key='screener_detail_select',
            )
            if _selected:
                _sid = _selected.split()[0]
                _match = next((r for r in results if r['stock_id'] == _sid), None)
                if _match:
                    st.markdown(f"**{_sid} {_match.get('name', '')}** — "
                                f"觸發分數: {_match['trigger_score']:+.1f} / "
                                f"趨勢分數: {_match['trend_score']:+.1f}")
                    for d in _match.get('trigger_details', []):
                        st.markdown(f"- {d}")

    else:
        st.info("尚無掃描結果。\n\n"
                "**使用方式:**\n"
                "1. 點擊「快速預覽」查看今日初篩結果\n"
                "2. 在命令列執行 `python scanner_job.py` 進行完整掃描\n"
                "3. 完整掃描含觸發分數，約需 15-30 分鐘")

        st.caption("💡 完整掃描: `python scanner_job.py --mode momentum --no-chip`")

    # ====================================================================
    # Tab US: 美股動能選股
    # ====================================================================
    with screener_tab_us:

        us_file = _Path('data/latest/momentum_us_result.json')
        us_result = None
        if us_file.exists():
            try:
                with open(us_file, 'r', encoding='utf-8') as _f:
                    us_result = _json.load(_f)
            except Exception:
                us_result = None

        col_us1, col_us2 = st.columns([2, 3])
        with col_us1:
            if st.button("⚡ 快速預覽 (S&P 500)", key='us_stage1_btn',
                          help="S&P 500 初篩，約15秒"):
                st.session_state['us_screener_run'] = 'stage1'

        if st.session_state.get('us_screener_run') == 'stage1':
            with st.spinner("Downloading S&P 500 data..."):
                from momentum_screener import MomentumScreener as _MS
                _us_screener = _MS()
                _us_df = _us_screener.run_stage1_only(market='us')
            st.session_state['us_screener_run'] = None
            if not _us_df.empty:
                st.success(f"S&P 500 passed: {len(_us_df)} stocks")
                _us_df['TV ($B)'] = (_us_df['trading_value'] / 1e9).round(1)
                _us_df['Chg%'] = _us_df['change_pct'].round(2)
                _show = ['stock_id', 'market', 'close', 'Chg%', 'volume', 'TV ($B)']
                _show = [c for c in _show if c in _us_df.columns]
                st.dataframe(
                    _us_df[_show].rename(columns={
                        'stock_id': 'Ticker', 'market': 'Market', 'close': 'Close'
                    }),
                    use_container_width=True,
                    height=500,
                )
            else:
                st.warning("No data (market may be closed)")

        elif us_result and us_result.get('results'):
            us_results = us_result['results']
            st.caption(
                f"Scan: {us_result.get('scan_date', '?')} {us_result.get('scan_time', '')} | "
                f"Universe: {us_result.get('total_scanned', 0)} → "
                f"Passed: {us_result.get('passed_initial', 0)} → "
                f"Scored: {us_result.get('scored_count', 0)} | "
                f"Time: {us_result.get('elapsed_seconds', 0):.0f}s"
            )

            _us_rows = []
            for r in us_results:
                _us_rows.append({
                    '#': len(_us_rows) + 1,
                    'Ticker': r['stock_id'],
                    'Price': r.get('price', 0),
                    'Chg%': r.get('change_pct', 0),
                    'Score': r.get('trigger_score', 0),
                    'Trend': r.get('trend_score', 0),
                    'Regime': r.get('regime', ''),
                    'Signals': ', '.join(r.get('signals', [])[:3]),
                })
            st.dataframe(
                pd.DataFrame(_us_rows),
                use_container_width=True,
                height=600,
                column_config={
                    'Score': st.column_config.NumberColumn(format="%.1f"),
                    'Trend': st.column_config.NumberColumn(format="%.1f"),
                    'Chg%': st.column_config.NumberColumn(format="%.1f%%"),
                    'Price': st.column_config.NumberColumn(format="$%.2f"),
                },
            )

            with st.expander("Detailed Scores"):
                _us_selected = st.selectbox(
                    "Select stock",
                    options=[r['stock_id'] for r in us_results],
                    key='us_detail_select',
                )
                if _us_selected:
                    _us_match = next((r for r in us_results if r['stock_id'] == _us_selected), None)
                    if _us_match:
                        st.markdown(f"**{_us_selected}** — Score: {_us_match['trigger_score']:+.1f} / Trend: {_us_match['trend_score']:+.1f}")
                        for d in _us_match.get('trigger_details', []):
                            st.markdown(f"- {d}")

        else:
            st.info("No US scan results yet.\n\n"
                    "**Usage:**\n"
                    "1. Click 'Quick Preview' for S&P 500 initial filter\n"
                    "2. Run `python scanner_job.py --mode momentum --market us` for full scan\n"
                    "3. Full scan takes ~30-60 min for S&P 500")

        st.caption("💡 Full scan: `python scanner_job.py --mode momentum --market us`")

    # ====================================================================
    # Tab 2: 左側價值選股
    # ====================================================================
    with screener_tab2:

        value_file = _Path('data/latest/value_result.json')
        value_result = None
        if value_file.exists():
            try:
                with open(value_file, 'r', encoding='utf-8') as _f:
                    value_result = _json.load(_f)
            except Exception:
                value_result = None

        # Scan controls
        col_v1, col_v2 = st.columns([2, 3])
        with col_v1:
            if st.button("⚡ 快速預覽 (PE/PB 篩選)", key='value_stage1_btn',
                          help="用 PE/PB/殖利率快速篩選，約10秒"):
                st.session_state['value_screener_run'] = 'stage1'

        # Stage 1 preview
        if st.session_state.get('value_screener_run') == 'stage1':
            with st.spinner("估值初篩中..."):
                from value_screener import ValueScreener
                _v_screener = ValueScreener()
                _v_df = _v_screener.run_stage1_only()
            st.session_state['value_screener_run'] = None
            if not _v_df.empty:
                st.success(f"初篩通過: {len(_v_df)} 檔")
                _v_df['TV (億)'] = (_v_df['trading_value'] / 1e8).round(1)
                _v_df['漲跌%'] = _v_df['change_pct'].round(2)
                _show = ['stock_id', 'stock_name', 'market', 'close', 'PE', 'PB',
                          'dividend_yield', '漲跌%', 'TV (億)']
                _show = [c for c in _show if c in _v_df.columns]
                st.dataframe(
                    _v_df[_show].rename(columns={
                        'stock_id': '代號', 'stock_name': '名稱', 'market': '市場',
                        'close': '收盤', 'dividend_yield': '殖利率%'
                    }),
                    use_container_width=True,
                    height=500,
                )
            else:
                st.warning("初篩無結果")

        # Show latest full value scan results
        elif value_result and value_result.get('results'):
            v_results = value_result['results']
            st.caption(
                f"掃描日期: {value_result.get('scan_date', '?')} {value_result.get('scan_time', '')} | "
                f"全市場 {value_result.get('total_scanned', 0)} 檔 → "
                f"初篩 {value_result.get('passed_initial', 0)} 檔 → "
                f"評分 {value_result.get('scored_count', 0)} 檔 | "
                f"耗時 {value_result.get('elapsed_seconds', 0):.0f}s"
            )

            _v_rows = []
            for r in v_results:
                s = r.get('scores', {})
                _v_rows.append({
                    '排名': len(_v_rows) + 1,
                    '代號': r['stock_id'],
                    '名稱': r.get('name', ''),
                    '收盤': r.get('price', 0),
                    'PE': r.get('PE', 0),
                    'PB': r.get('PB', 0),
                    '殖利率%': r.get('dividend_yield', 0),
                    '綜合分數': r.get('value_score', 0),
                    '估值': s.get('valuation', 0),
                    '體質': s.get('quality', 0),
                    '營收': s.get('revenue', 0),
                    '技術轉折': s.get('technical', 0),
                    '聰明錢': s.get('smart_money', 0),
                })
            _v_df_results = pd.DataFrame(_v_rows)

            st.dataframe(
                _v_df_results,
                use_container_width=True,
                height=600,
                column_config={
                    '綜合分數': st.column_config.NumberColumn(format="%.1f"),
                    'PE': st.column_config.NumberColumn(format="%.1f"),
                    'PB': st.column_config.NumberColumn(format="%.2f"),
                    '殖利率%': st.column_config.NumberColumn(format="%.1f%%"),
                    '收盤': st.column_config.NumberColumn(format="%.1f"),
                },
            )

            # Detailed scoring
            with st.expander("個股詳細評分"):
                _v_selected = st.selectbox(
                    "選擇股票",
                    options=[f"{r['stock_id']} {r.get('name', '')}" for r in v_results],
                    key='value_detail_select',
                )
                if _v_selected:
                    _v_sid = _v_selected.split()[0]
                    _v_match = next((r for r in v_results if r['stock_id'] == _v_sid), None)
                    if _v_match:
                        _vs = _v_match.get('scores', {})
                        st.markdown(
                            f"**{_v_sid} {_v_match.get('name', '')}** — "
                            f"綜合: {_v_match['value_score']:.1f} | "
                            f"估值: {_vs.get('valuation', 0):.0f} | "
                            f"體質: {_vs.get('quality', 0):.0f} | "
                            f"營收: {_vs.get('revenue', 0):.0f} | "
                            f"技術: {_vs.get('technical', 0):.0f} | "
                            f"聰明錢: {_vs.get('smart_money', 0):.0f}"
                        )
                        for d in _v_match.get('details', []):
                            st.markdown(f"- {d}")

        else:
            st.info("尚無掃描結果。\n\n"
                    "**使用方式:**\n"
                    "1. 點擊「快速預覽」查看 PE/PB 篩選結果\n"
                    "2. 在命令列執行 `python scanner_job.py --mode value` 進行完整掃描\n"
                    "3. 完整掃描含 5 維評分，約需 20-40 分鐘")

        st.caption("💡 完整掃描: `python scanner_job.py --mode value --no-chip`")

    st.markdown("---")
    st.caption("💡 完整掃描 (右側+左側): `python scanner_job.py --no-chip`")

elif st.session_state.get('analysis_active', False):
    # 決定資料來源
    source = None
    display_ticker = ""
    # Use session state for force if available, else False
    is_force = st.session_state.get('force_run', False)
    
    if target_ticker:
        # 驗證輸入
        is_valid, err_msg = validate_ticker(target_ticker)
        if not is_valid:
            st.error(f"❌ {err_msg}")
            st.session_state['analysis_active'] = False
            st.stop()
        # 簡單判斷台股 - 讓 technical_analysis 自動處理後綴 (.TW/.TWO/FinMind)
        source = target_ticker.upper().strip()
        display_ticker = source
    else:
        st.error("❌ 請輸入有效的股票代號")
        st.session_state['analysis_active'] = False # Reset
        st.stop()

    # 執行分析
    
    # 執行分析
    status_text = st.empty()
    action_text = "強制下載" if is_force else "分析"
    # Show spinner only if strict run or different ticker? 
    # Actually just show it, it's fast if cached.
    # But if backtest button is clicked, we assume analysis is already done.
    # Whatever, let it re-run run_analysis (it hits cache).
    
    status_text.info(f"⏳ 正在{action_text} {display_ticker} ...")
    
    try:
        # 呼叫有快取的函數
        figures, errors, df_week, df_day, stock_meta = run_analysis(source, force_update=is_force)

        # Display analysis warnings from errors dict
        for key, err_msg in errors.items():
            if err_msg:
                st.warning(f"⚠️ {key} 計算警告: {err_msg}")

        # [NEW] Pre-load Chip Data for Analysis (籌碼預載)
        chip_data = None
        if source and isinstance(source, str) and ("TW" in source or source.isdigit()):
             try:
                 status_text.info(f"⏳ 正在分析 {display_ticker} (技術+籌碼)...")
                 chip_data, chip_err = get_chip_data_cached(source, is_force)
             except Exception as e:
                 logger.error(f"Chip Load Error: {e}", exc_info=True)
                 st.warning(f"⚠️ 籌碼預載失敗: {e}")

        # 暫存給 Analyzer 用 (使用 session_state)
        st.session_state['df_week_cache'] = df_week
        st.session_state['df_day_cache'] = df_day
        st.session_state['force_update_cache'] = is_force

        status_text.success("✅ 分析完成！")
        
        # ==========================================
        # 顯示股票基本資訊 (Header)
        # ==========================================

        # ==========================================
        # 顯示基本面資訊 (Fundamentals) - Moved to Header Area
        # ==========================================
        fund_data = None
        if source and isinstance(source, str):
             with st.spinner("📋 載入基本面資料..."):
                 fund_data = get_fundamentals(display_ticker)
             st.session_state['fund_cache'] = fund_data

        if stock_meta and 'name' in stock_meta:
             st.markdown(f"## 🏢 {display_ticker} {stock_meta.get('name', '')}")
             
             if not df_day.empty and len(df_day) >= 2:
                 last_price = df_day['Close'].iloc[-1]
                 prev_price = df_day['Close'].iloc[-2]
                 chg = last_price - prev_price
                 pct = (chg / prev_price) * 100 if prev_price != 0 else 0
                 
                 # Combine Price and Fundamentals
                 # Row 1: Price | P/E | EPS | Yield | P/B | ROE
                 
                 st.markdown("##### 概況與基本面")
                 
                 # Dynamic Columns: Price(1) + Fund(5) = 6 columns
                 c_price, c_pe, c_eps, c_yield, c_pb, c_roe = st.columns(6)
                 
                 # 1. Price
                 c_price.metric("收盤價", f"{last_price:.2f}", f"{chg:.2f} ({pct:.2f}%)", delta_color="inverse")
                 
                 # 2. Fundamentals
                 if fund_data:
                     c_pe.metric("本益比", fund_data['PE Ratio'])
                     c_eps.metric("EPS", fund_data['EPS (TTM)'])
                     c_yield.metric("殖利率", fund_data['Dividend Yield'])
                     c_pb.metric("淨值比", fund_data['PB Ratio'])
                     c_roe.metric("ROE", fund_data.get('ROE', 'N/A'))
                 else:
                     # Fill with N/A if no fund data
                     c_pe.metric("本益比", "N/A")
                     c_eps.metric("EPS", "N/A")
                     c_yield.metric("殖利率", "N/A")
                     c_pb.metric("淨值比", "N/A")
                     c_roe.metric("ROE", "N/A")

                 # Row 2: Sector | Currency | Market Cap (Optional)
                 # 資料新鮮度指示
                 data_date = df_day.index[-1]
                 import datetime as _dt
                 days_ago = (_dt.datetime.now() - data_date).days
                 freshness = f"📅 {data_date.strftime('%Y-%m-%d')}"
                 if days_ago == 0:
                     freshness += " (今日)"
                 elif days_ago == 1:
                     freshness += " (昨日)"
                 elif days_ago > 1:
                     freshness += f" ({days_ago} 天前)"
                 st.caption(f"產業: {stock_meta.get('sector', 'N/A')} | 幣別: {stock_meta.get('currency', 'TWD')} | 資料: {freshness}")
        
        # 顯示如果有錯誤
                 

        # ==========================================
        # AI 分析報告 (Analysis Report)
        # ==========================================
        from analysis_engine import TechnicalAnalyzer
        from strategy_manager import StrategyManager

        
        # 只有當兩者都有數據時才進行完整分析
        if 'Weekly' in figures and 'Daily' in figures:
            # Load Strategy from cache
            sm = StrategyManager()
            strategy_params = sm.load_strategy(display_ticker) # Returns dict or None
            
            # 注意: 這裡需要傳入原始 DataFrame，而不是 Figure
            # run_analysis 回傳的是 dict
            
            # [NEW] 美股籌碼數據預載
            us_chip_data = None
            if source and isinstance(source, str) and not source.isdigit() and not source.endswith('.TW'):
                with st.spinner("📊 載入美股籌碼..."):
                    try:
                        from us_stock_chip import USStockChipAnalyzer
                        us_analyzer = USStockChipAnalyzer()
                        us_chip_data, us_err = us_analyzer.get_chip_data(source)
                        if us_err:
                            logger.warning(f"US Chip Warning: {us_err}")
                            st.warning(f"⚠️ 美股籌碼資料警告: {us_err}")
                    except Exception as e:
                        logger.error(f"US Chip Load Error: {e}", exc_info=True)
                        st.warning(f"⚠️ 美股籌碼預載失敗: {e}")

            with st.spinner("🤖 AI 分析中..."):
                analyzer = TechnicalAnalyzer(
                    display_ticker,
                    st.session_state['df_week_cache'],
                    st.session_state['df_day_cache'],
                    strategy_params,
                    chip_data=chip_data,
                    us_chip_data=us_chip_data
                )
                report = analyzer.run_analysis()
            
            st.markdown("---")
            st.subheader("📝 AI 智能分析報告 (Beta)")
            
            # 1. 劇本卡片 (Scenario Card)
            sc = report['scenario']
            if sc['color'] == 'red':
                st.error(f"### {sc['title']}\n{sc['desc']}")
            elif sc['color'] == 'orange':
                st.warning(f"### {sc['title']}\n{sc['desc']}")
            elif sc['color'] == 'green':
                st.success(f"### {sc['title']}\n{sc['desc']}")
            else:
                st.info(f"### {sc['title']}\n{sc['desc']}")
            

                
            # Score Summary (觸發分數 + 趨勢分數 + 百分位)
            sm1, sm2, sm3 = st.columns(3)
            sm1.metric("觸發分數 (Trigger)", f"{report['trigger_score']:.1f}")
            sm2.metric("趨勢分數 (Trend)", f"{report['trend_score']:.0f}")
            pct = report.get('score_percentile', 50)
            pct_label = f"前 {100-pct:.0f}%" if pct >= 50 else f"後 {pct:.0f}%"
            sm3.metric("全市場排名", pct_label, f"百分位 {pct:.0f}%")

            # Regime Detection 提示
            regime = report.get('regime', {})
            if regime and regime.get('regime') != 'unknown':
                regime_icon = {'trending': '📈', 'ranging': '📦', 'squeeze': '⏳', 'neutral': '⚖️'}.get(regime['regime'], '❓')
                regime_label = {'trending': '趨勢市', 'ranging': '盤整市', 'squeeze': '波動壓縮', 'neutral': '中性'}.get(regime['regime'], '未知')
                pos_adj = regime.get('position_adj', 1.0)
                regime_text = f"{regime_icon} **市場狀態: {regime_label}**"
                if pos_adj < 1.0:
                    regime_text += f"　｜　建議倉位: **{pos_adj:.0%}** (減碼)"
                for detail in regime.get('details', []):
                    regime_text += f"\n- {detail}"
                if regime['regime'] == 'ranging':
                    st.warning(regime_text)
                elif regime['regime'] == 'squeeze':
                    st.info(regime_text)
                elif regime['regime'] == 'trending':
                    st.success(regime_text)
                else:
                    st.caption(regime_text)

            # [NEW] 🔔 盤中監控看板 (Monitoring & Outlook)
            if 'checklist' in report and report['checklist']:
                cl = report['checklist']
                with st.expander("🔔 盤中監控看板 (Monitoring & Outlook)", expanded=True):
                    
                    # Layout: 3 Columns
                    mc1, mc2, mc3 = st.columns(3)
                    
                    with mc1:
                        st.markdown("#### 🛑 停損/調節 (Risk)")
                        if cl['risk']:
                            for item in cl['risk']:
                                st.warning(item, icon="⚠️")
                        else:
                            st.caption("(暫無緊急風險訊號)")

                    with mc2:
                        st.markdown("#### 🚀 追價/加碼 (Active)")
                        if cl['active']:
                            for item in cl['active']:
                                st.success(item, icon="🔥")
                        else:
                            st.caption("(暫無追價訊號)")
                            
                    with mc3:
                        st.markdown("#### 🔭 未來觀察 (Future)")
                        if cl['future']:
                            for item in cl['future']:
                                st.info(item, icon="👀")
                        else:
                            st.caption("(持續觀察)")

        # 2. 核心操作建議 (Key Actionables) - Moved to Top
            if report.get('action_plan'):
                ap = report['action_plan']
                is_actionable = ap.get('is_actionable', True) # Default True for backward compatibility
                
                # 第一排：策略 (Always Show)
                st.info(f"**操作策略**：\n\n{ap['strategy']}")
                
                if is_actionable:
                    c2, c3, c4, c5 = st.columns(4)

                    # 2. 進場 + 型態信心
                    confidence = ap.get('entry_confidence', 'standard')
                    conf_badge = ""
                    if confidence == "high":
                        conf_badge = "\n\n**信心: 高**"
                    elif confidence == "wait":
                        conf_badge = "\n\n**信心: 等待確認**"

                    if ap.get('rec_entry_low', 0) > 0:
                         c2.warning(f"**建議進場**：\n\n📉 **{ap['rec_entry_low']:.2f}~{ap['rec_entry_high']:.2f}**{conf_badge}")
                    else:
                         c2.warning(f"**建議進場**：\n\n(暫無建議)")

                    # 3. 停利
                    c3.success(f"**推薦停利**：\n\n🎯 **{ap['rec_tp_price']:.2f}**")
                    
                    # 4. 停損
                    c4.error(f"**推薦停損**：\n\n🛑 **{ap['rec_sl_price']:.2f}**")
                    
                    # 5. 風報比 (RR Ratio)
                    rr = ap.get('rr_ratio', 0)
                    rr_text = f"1 : {rr:.1f}"
                    if rr >= 2.0:
                        c5.success(f"**風報比**：\n\n⚖️ **{rr_text}**") # Excellent
                    elif rr >= 1.0:
                        c5.warning(f"**風報比**：\n\n⚖️ **{rr_text}**") # Okay
                    elif rr > 0:
                        c5.error(f"**風報比**：\n\n⚖️ **{rr_text}**") # Bad
                    else:
                         c5.info(f"**風報比**：\n\nN/A")
                    # 6. 部位管理建議 (Position Sizing)
                    ps = ap.get('position_sizing', {})
                    if ps:
                        with st.expander("📐 部位管理建議 (2% 風險法則)", expanded=False):
                            is_us = ap.get('is_us_stock', False)
                            ps_data = []
                            for cap, info in ps.items():
                                if info['lots'] > 0:
                                    if is_us:
                                        ps_data.append({
                                            "資金規模": f"${cap:,.0f}",
                                            "建議股數": f"{info['shares']} 股",
                                            "所需資金": f"${info['cost']:,.0f}",
                                            "停損虧損": f"${info['risk_amount']:,.0f}",
                                            "風險比例": f"{info['risk_pct']:.1f}%"
                                        })
                                    else:
                                        ps_data.append({
                                            "資金規模": f"{cap/10000:.0f} 萬",
                                            "建議張數": f"{info['lots']} 張",
                                            "所需資金": f"{info['cost']:,.0f}",
                                            "停損虧損": f"{info['risk_amount']:,.0f}",
                                            "風險比例": f"{info['risk_pct']:.1f}%"
                                        })
                            if ps_data:
                                st.table(pd.DataFrame(ps_data))
                                st.caption("💡 2% 法則：單筆交易最大虧損不超過總資金的 2%")
                            else:
                                st.caption("⚠️ 停損距離過大或股價過高，建議降低部位或等待更佳進場點")

                else:
                    # Not actionable: Show simple message or nothing else?
                    # User request: "If not suggested entry, don't give"
                    pass

            st.markdown("---")

            # 3. 詳細因子分析 (Detailed Breakdown)
            fund_alerts = report.get('fundamental_alerts', [])
            if fund_alerts:
                c1, c2, c3 = st.columns(3)
            else:
                c1, c2 = st.columns(2)
                c3 = None
            with c1:
                st.markdown("#### 📅 週線趨勢因子")
                for item in report['trend_details']:
                    st.write(item)
            with c2:
                st.markdown("#### ⚡ 日線訊號因子")
                for item in report['trigger_details']:
                    st.write(item)
            if c3 and fund_alerts:
                with c3:
                    st.markdown("#### 📋 基本面快照")
                    for item in fund_alerts:
                        st.write(item)
            
            # 3.5 ML Signal (if available)
            try:
                from ml_signal import MLSignalClassifier
                ml = MLSignalClassifier()
                if ml.load_model(display_ticker):
                    ml_score = ml.get_ml_score(df_day)
                    ensemble = ml.ensemble_score(report['trigger_score'], ml_score)
                    with st.expander("🤖 AI/ML 混合信號", expanded=False):
                        mc1, mc2, mc3 = st.columns(3)
                        mc1.metric("規則分數", f"{report['trigger_score']:.1f}")
                        mc2.metric("ML 分數", f"{ml_score:.1f}")
                        mc3.metric("混合分數", f"{ensemble:.1f}")
                        fi = ml.get_feature_importance()
                        if fi:
                            st.markdown("**Top 特徵重要性:**")
                            top5 = dict(list(fi.items())[:5])
                            st.bar_chart(pd.Series(top5))
            except ImportError:
                pass
            except Exception as e:
                logger.debug(f"ML Signal error: {e}")

            # 4. 完整價位規劃表 (Detailed Price Levels)
            with st.expander("📊 查看完整支撐壓力與停損清單", expanded=False):
                if report.get('action_plan'):
                    ap = report['action_plan']
                    
                    # [RESTORED] 停利目標清單
                    if ap.get('tp_list'):
                        st.markdown("#### 🔭 停利目標預估清單")
                        tp_data = []
                        for t in ap['tp_list']:
                            mark = "⭐️" if t.get('is_rec') else ""
                            tp_data.append({
                                "推薦": mark,
                                "測幅方法": t['method'],
                                "目標價格": f"{t['price']:.2f}",
                                "說明": t['desc']
                            })
                        st.table(pd.DataFrame(tp_data))

                    if ap.get('sl_list'):
                        st.markdown("#### 🛡️ 支撐防守清單")
                        sl_data = []
                        for sl in ap['sl_list']:
                            sl_data.append([sl['desc'], f"{sl['price']:.2f}", f"{sl['loss']}%"])
                        st.table(pd.DataFrame(sl_data, columns=['支撐位置', '價格', '風險幅度']))





        # 顯示圖表
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["週K", "日K", "籌碼面", "🏢 基本面", "🔮 情緒/期權", "📊 除息/營收"])
        
        with tab1:
            if 'Weekly' in figures:
                st.plotly_chart(figures['Weekly'], use_container_width=True)
                

                
                # 新增: Weekly EFI
                if not df_week.empty and 'EFI_EMA13' in df_week.columns:
                    st.markdown("### ⚡ 週線能量 (Weekly EFI)")
                    st.caption("週線 EFI 能夠過濾短期雜訊，更準確判斷主力長線資金動向。")
                    # Create Static Plotly Figure for EFI
                    import plotly.express as px
                    fig_efi_w = px.line(df_week.iloc[-100:], y=['EFI_EMA13'])
                    fig_efi_w.update_layout(xaxis_title=None, yaxis_title=None, showlegend=True, margin=dict(l=0, r=0, t=10, b=0))
                    # Disable Zoom via config
                    st.plotly_chart(fig_efi_w, use_container_width=True, config={'staticPlot': True})
                    
            else:
                st.warning("⚠️ 無法產生週線圖表 (請查看上方錯誤訊息)")
        
        with tab2:
            if 'Daily' in figures:
                st.plotly_chart(figures['Daily'], use_container_width=True)
                


                # 新增: EFI 能量圖 (獨立顯示)
                if not df_day.empty and 'EFI_EMA13' in df_day.columns:
                    st.markdown("### ⚡ 埃爾德強力指標 (EFI - Elder's Force Index)")
                    st.caption("原理：結合「價格變動」與「成交量」。EFI > 0 代表多方有力，EFI < 0 代表空方有力。")
                    
                    # Create Static Plotly Figure for EFI
                    import plotly.express as px
                    fig_efi_d = px.line(df_day.iloc[-60:], y=['EFI_EMA13', 'EFI_EMA2'])
                    fig_efi_d.update_layout(xaxis_title=None, yaxis_title=None, showlegend=True, margin=dict(l=0, r=0, t=10, b=0))
                    st.plotly_chart(fig_efi_d, use_container_width=True, config={'staticPlot': True})
                    
                    # 簡易解讀
                    last_efi = df_day['EFI_EMA13'].iloc[-1]
                    if last_efi > 0:
                        st.success(f"🔥 主力力道：多方控盤 (EFI_13={last_efi:,.0f})")
                    else:
                        st.error(f"❄️ 主力力道：空方控盤 (EFI_13={last_efi:,.0f})")
            else:
                st.warning("⚠️ 無法產生日線圖表 (請查看上方錯誤訊息)")

        with tab3:
            # 籌碼資料更新時間提醒
            st.info("⏰ **籌碼資料更新時間**：每日晚上 21:30 之後更新（T+0 日資料）")
            
            # ==========================================
            # [NEW] 籌碼成交分佈 (Volume Profile)
            # ==========================================
            from technical_analysis import calculate_volume_profile
            import plotly.graph_objects as go
            
            # 使用 Expander 包裹，但預設展開，讓它成為 Tab 的第一部分
            with st.expander("📊 籌碼成交分佈 (Volume Profile)", expanded=True):
                try:
                    # Calculate Profile
                    vp_df, poc_price = calculate_volume_profile(df_day)
                    
                    if not vp_df.empty:
                        # Plot
                        fig_vp = go.Figure()
                        
                        # 1. Volume Bars (Horizontal)
                        # Color bars: Grey for normal, Yellow for POC area
                        colors = ['rgba(100, 100, 100, 0.5)'] * len(vp_df)
                        # Find index closest to POC
                        if not vp_df['Price'].empty:
                            poc_idx = (vp_df['Price'] - poc_price).abs().idxmin()
                            if 0 <= poc_idx < len(colors):
                                colors[poc_idx] = 'rgba(255, 215, 0, 0.8)' # Gold
                        
                        fig_vp.add_trace(go.Bar(
                            y=vp_df['Price'],
                            x=vp_df['Volume'],
                            orientation='h',
                            name='成交量',
                            marker_color=colors,
                            opacity=0.6,
                            hovertemplate="價格: %{y:.2f}<br>成交量: %{x:,.0f}<extra></extra>"
                        ))
                        
                        # 2. Current Price Line
                        curr_price = df_day['Close'].iloc[-1]
                        fig_vp.add_hline(
                            y=curr_price, 
                            line_dash="dash", 
                            line_color="cyan", 
                            annotation_text=f"現價 {curr_price}", 
                            annotation_position="top right"
                        )
                        
                        # 3. POC Line
                        fig_vp.add_hline(
                            y=poc_price, 
                            line_width=2, 
                            line_color="orange", 
                            annotation_text=f"大量支撐 (POC) {poc_price:.2f}", 
                            annotation_position="bottom right"
                        )

                        fig_vp.update_layout(
                            title="近半年籌碼成交分佈圖 (Volume Profile)",
                            xaxis_title="成交量 (Volume)",
                            yaxis_title="價格 (Price)",
                            template="plotly_dark",
                            height=400,
                            showlegend=False,
                            margin=dict(l=20, r=20, t=40, b=20),
                            hovermode="y unified"
                        )
                        st.plotly_chart(fig_vp, use_container_width=True)
                        
                        # Interpretation Text
                        if curr_price > poc_price:
                            st.caption(f"✅ **多頭優勢**：股價位於大量成本區 ({poc_price:.2f}) 之上，下檔有撐。")
                        else:
                            st.caption(f"⚠️ **空頭壓力**：股價位於大量套牢區 ({poc_price:.2f}) 之下，上檔有壓。")
                            
                    else:
                        st.info("資料不足，無法計算籌碼分佈。")
                except Exception as e:
                    st.error(f"籌碼圖繪製失敗: {e}")

            st.markdown("---")
            # 寬鬆判斷：只要是字串且 (含TW 或 純數字) 都嘗試顯示籌碼
            if source and isinstance(source, str) and ("TW" in source or source.isdigit()):
                 # 嘗試抓取籌碼數據
                 try:
                     loading_msg = st.empty()
                     loading_msg.info(f"⏳ 正在抓取 {display_ticker} 近一年籌碼數據 (FinMind)...")

                     # Use force state from session_state
                     is_force = st.session_state.get('force_update_cache', False)
                     chip_data, err = get_chip_data_cached(source, is_force)
                     loading_msg.empty() # Clear message
                     
                     if chip_data:
                         st.success(f"✅ {display_ticker} 籌碼數據讀取成功")
                         
                         # [NEW] Margin Utilization Metric (融資使用率)
                         df_m = chip_data.get('margin', pd.DataFrame())
                         if not df_m.empty and '融資限額' in df_m.columns:
                             # Ensure numeric stats
                             try:
                                 latest_m = df_m.iloc[-1]
                                 bal = latest_m.get('融資餘額', 0)
                                 lim = latest_m.get('融資限額', 0)
                                 
                                 if lim > 0:
                                     util_rate = (bal / lim) * 100
                                     
                                     st.markdown("#### 💳 信用交易概況")
                                     c_m1, c_m2, c_m3 = st.columns(3)
                                     c_m1.metric("融資餘額", f"{bal:,.0f} 張")
                                     c_m2.metric("融資限額", f"{lim:,.0f} 張")
                                     
                                     state_color = "normal"
                                     state_label = "水位健康"
                                     if util_rate > 60:
                                         state_label = "⚠️ 融資過熱"
                                         state_color = "inverse"
                                     elif util_rate > 40:
                                         state_label = "偏高"
                                         state_color = "inverse"
                                         
                                     c_m3.metric("融資使用率", f"{util_rate:.2f}%", delta=state_label, delta_color=state_color)
                             except Exception as e:
                                 st.caption(f"融資數據計算異常: {e}")
                         elif not df_m.empty:
                             st.warning("⚠️ 檢測到舊的快取數據，缺少「融資限額」欄位。請勾選側邊欄的 **強制更新數據 (Force Update)** 以取得最新資料。")

                         # [NEW] Day Trading Rate (當沖率)
                         df_dt = chip_data.get('day_trading')
                         if df_dt is not None and not df_dt.empty and not df_day.empty:
                             try:
                                 # Align data
                                 common_idx = df_day.index.intersection(df_dt.index)
                                 if not common_idx.empty:
                                     latest_date = common_idx[-1]
                                     # Values might be Series if index duplicate? Ensured unique in chip_analysis.
                                     dt_vol = df_dt.loc[latest_date, 'DayTradingVolume']
                                     total_vol = df_day.loc[latest_date, 'Volume']
                                     
                                     # Handle potential Series if scalar expected
                                     if isinstance(dt_vol, pd.Series): dt_vol = dt_vol.iloc[0]
                                     if isinstance(total_vol, pd.Series): total_vol = total_vol.iloc[0]

                                     if total_vol > 0:
                                         # 注意：FinMind和yfinance的Volume都是「股」為單位
                                         # 台股：1000股 = 1張，需要轉換
                                         dt_vol_lots = dt_vol / 1000  # 轉換為張
                                         total_vol_lots = total_vol / 1000  # 轉換為張
                                         dt_rate = (dt_vol / total_vol) * 100
                                         
                                         st.markdown("#### ⚡ 當沖週轉概況")
                                         st.caption(f"資料日期: {latest_date.strftime('%Y-%m-%d')}")
                                         c_dt1, c_dt2, c_dt3 = st.columns(3)
                                         c_dt1.metric("當沖成交量", f"{dt_vol_lots:,.0f} 張")
                                         c_dt2.metric("當日總量", f"{total_vol_lots:,.0f} 張")
                                         
                                         state_color = "normal"
                                         state_label = "籌碼穩定"
                                         if dt_rate > 50:
                                             state_label = "⚠️ 過熱 (賭場)"
                                             state_color = "inverse"
                                         elif dt_rate > 35:
                                             state_label = "偏高"
                                             state_color = "inverse"
                                         
                                         c_dt3.metric("當沖率", f"{dt_rate:.2f}%", delta=state_label, delta_color=state_color)
                             except Exception as e:
                                 st.caption(f"當沖數據計算異常: {e}")

                         # [NEW] Foreign Holding Ratio (外資持股比率)
                         df_sh = chip_data.get('shareholding')
                         if df_sh is not None and not df_sh.empty:
                             st.markdown("#### 🌍 外資持股比率 (Foreign Holding Trends)")
                             
                             # Filter common date range
                             if not df_day.empty and 'ForeignHoldingRatio' in df_sh.columns:
                                 # Align dates
                                 common_idx = df_day.index.intersection(df_sh.index)
                                 # Take last 180 days max
                                 common_idx = common_idx[-180:]
                                 
                                 if not common_idx.empty:
                                     aligned_sh = df_sh.loc[common_idx]
                                     aligned_price = df_day.loc[common_idx]
                                     
                                     fig_sh = go.Figure()
                                     
                                     # 1. Foreign Ratio (Line, Left Y)
                                     fig_sh.add_trace(go.Scatter(
                                         x=aligned_sh.index, 
                                         y=aligned_sh['ForeignHoldingRatio'],
                                         mode='lines',
                                         name='外資持股比率(%)',
                                         line=dict(color='#FFA500', width=2), # Orange
                                         yaxis='y1'
                                     ))
                                     
                                     # 2. Price (Line, Right Y)
                                     fig_sh.add_trace(go.Scatter(
                                         x=aligned_price.index,
                                         y=aligned_price['Close'],
                                         mode='lines',
                                         name='股價',
                                         line=dict(color='gray', width=1, dash='dot'),
                                         yaxis='y2'
                                     ))
                                     
                                     fig_sh.update_layout(
                                         xaxis_title="日期",
                                         yaxis=dict(
                                             title="持股比率 (%)",
                                             side="left",
                                             showgrid=True,
                                             tickformat=".1f"
                                         ),
                                         yaxis2=dict(
                                             title="股價",
                                             side="right",
                                             overlaying="y",
                                             showgrid=False
                                         ),
                                         legend=dict(orientation="h", y=1.2, x=0.5, xanchor='center'),
                                         height=300,
                                         margin=dict(l=20, r=20, t=30, b=20),
                                         hovermode='x unified'
                                     )
                                     st.plotly_chart(fig_sh, use_container_width=True)
                             else:
                                 st.caption("⚠️ 尚無足夠的外資持股比率數據")

                         
                         # 1. 整合圖表：三大法人 + 融資融券 (Plotly Dual Subplot)
                         st.markdown("### 📊 籌碼綜合分析 (Institutional & Margin)")
                         
                         df_inst = chip_data.get('institutional', pd.DataFrame())
                         df_margin = chip_data.get('margin', pd.DataFrame())
                         
                         # Data Slicing (Last 120 days for clear view)
                         days_show = 120
                         df_inst_plot = df_inst.iloc[-days_show:] if not df_inst.empty else pd.DataFrame()
                         df_margin_plot = df_margin.iloc[-days_show:] if not df_margin.empty else pd.DataFrame()
                         
                         if not df_inst_plot.empty:
                             # Import Plotly
                             import plotly.graph_objects as go
                             from plotly.subplots import make_subplots
                             
                             # Create Subplots: Row 1 = Investors (Bar), Row 2 = Margin (Line)
                             fig_chip = make_subplots(
                                 rows=2, cols=1,
                                 shared_xaxes=True,
                                 vertical_spacing=0.05,
                                 subplot_titles=("三大法人買賣超 (張)", "融資融券餘額 (張)"),
                                 row_heights=[0.6, 0.4]
                             )
                             
                             # Utils for color
                             def get_color(val): return 'red' if val > 0 else 'green'
                             
                             # --- Row 1: Institutional Investors ---
                             # Data in FinMind is 'Shares' (股). Convert to 'Zhang' (張) = Shares / 1000
                             
                             # Foreign
                             if '外資' in df_inst_plot.columns:
                                 # Convert to Zhang
                                 val_zhang = df_inst_plot['外資'] / 1000
                                 fig_chip.add_trace(go.Bar(
                                     x=df_inst_plot.index, y=val_zhang,
                                     name='外資', marker_color='orange',
                                     hovertemplate="外資: %{y:,.0f} 張<extra></extra>"
                                 ), row=1, col=1)
                             # Trust
                             if '投信' in df_inst_plot.columns:
                                 val_zhang = df_inst_plot['投信'] / 1000
                                 fig_chip.add_trace(go.Bar(
                                     x=df_inst_plot.index, y=val_zhang,
                                     name='投信', marker_color='red',
                                     hovertemplate="投信: %{y:,.0f} 張<extra></extra>"
                                 ), row=1, col=1)
                             # Dealer
                             if '自營商' in df_inst_plot.columns:
                                 val_zhang = df_inst_plot['自營商'] / 1000
                                 fig_chip.add_trace(go.Bar(
                                     x=df_inst_plot.index, y=val_zhang,
                                     name='自營商', marker_color='lightgreen',  # 淺綠色，更容易識別
                                     hovertemplate="自營商: %{y:,.0f} 張<extra></extra>"
                                 ), row=1, col=1)
                                 
                             # --- Row 2: Margin Trading ---
                             # Ensure Margin data aligns with Inst data dates if possible
                             # Or just plot what we have. Aligning index intersection is safer.
                             common_idx = df_inst_plot.index.intersection(df_margin.index)
                             if not common_idx.empty:
                                 df_margin_aligned = df_margin.loc[common_idx]
                                 
                                 # Margin is usually also in Shares? FinMind units: usually Shares for Balance
                                 # Convert to Zhang as well for consistency
                                 margin_zhang = df_margin_aligned['融資餘額'] / 1000
                                 short_zhang = df_margin_aligned['融券餘額'] / 1000

                                 fig_chip.add_trace(go.Scatter(
                                     x=df_margin_aligned.index, y=margin_zhang,
                                     name='融資餘額', mode='lines', line=dict(color='red', width=2),
                                     hovertemplate="融資: %{y:,.0f} 張<extra></extra>"
                                 ), row=2, col=1)
                                 
                                 fig_chip.add_trace(go.Scatter(
                                     x=df_margin_aligned.index, y=short_zhang,
                                     name='融券餘額', mode='lines', line=dict(color='green', width=2),
                                     hovertemplate="融券: %{y:,.0f} 張<extra></extra>"
                                 ), row=2, col=1)

                             # Layout
                             fig_chip.update_layout(
                                 height=600,
                                 hovermode='x unified', # Key requirement: Unified Hover
                                 barmode='group',
                                 bargap=0.3,  # 增加柱狀圖之間的間隙（0-1之間，0.3表示30%間隙）
                                 bargroupgap=0.1,  # 增加同組柱狀圖之間的間隙
                                 margin=dict(l=30, r=30, t=50, b=50), # Increased Margins for Titles/Legend
                                 # Move Legend to Bottom to avoid overlap with Modebar/Title Hover
                                 legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5)
                             )
                             # Spikes and Grid
                             fig_chip.update_xaxes(
                                 showspikes=True, 
                                 spikemode='across', 
                                 spikesnap='cursor',
                                 showgrid=True,  # 顯示垂直網格線
                                 gridcolor='rgba(128, 128, 128, 0.2)',  # 淺灰色網格線
                                 dtick=86400000*7,  # 每週顯示一次刻度（毫秒）
                                 tickformat='%m/%d',  # 日期格式：月/日
                             )
                             # Y軸網格線
                             fig_chip.update_yaxes(
                                 showgrid=True,  # 顯示水平網格線
                                 gridcolor='rgba(128, 128, 128, 0.15)',  # 更淺的灰色
                                 zeroline=True,  # 顯示零線
                                 zerolinecolor='rgba(0, 0, 0, 0.3)',  # 零線顏色
                                 zerolinewidth=1.5
                             )
                             
                             st.plotly_chart(fig_chip, use_container_width=True)
                             
                         else:
                             st.warning("⚠️ 查無法人數據")

                         st.markdown("---")
                         st.info("💡 **集保股權分散 (Shareholding Distribution)**：因 API 限制為付費數據，暫無法顯示詳細大戶/散戶比例。建議搭配「三大法人」與「EFI 指標」判斷主力動向。")
                         
                     else:
                         st.error(f"❌ 籌碼讀取失敗: {err}")
                 except Exception as e:
                     st.error(f"❌ 發生錯誤: {e}")
            
            # === 美股籌碼分析 ===
            elif source and isinstance(source, str) and not source.isdigit() and not source.endswith('.TW'):
                try:
                    st.markdown("### 🇺🇸 美股籌碼分析 (US Stock Chip Analysis)")
                    
                    loading_msg = st.empty()
                    loading_msg.info(f"⏳ 正在取得 {display_ticker} 美股籌碼數據...")
                    
                    from us_stock_chip import USStockChipAnalyzer
                    us_analyzer = USStockChipAnalyzer()
                    us_chip, us_err = us_analyzer.get_chip_data(source)
                    
                    loading_msg.empty()
                    
                    if us_chip:
                        st.success(f"✅ {display_ticker} 美股籌碼數據讀取成功")
                        
                        # 1. 機構持股概況
                        inst = us_chip.get('institutional', {})
                        major = us_chip.get('major_holders', {})
                        
                        st.markdown("#### 🏛️ 機構持股概況")
                        col_inst1, col_inst2, col_inst3, col_inst4 = st.columns(4)
                        
                        col_inst1.metric("機構持股比例", f"{inst.get('percent_held', 0):.1f}%")
                        col_inst2.metric("機構家數", f"{inst.get('holders_count', 0):,}")
                        col_inst3.metric("內部人持股", f"{major.get('insiders_percent', 0):.1f}%")
                        col_inst4.metric("流通股比例", f"{major.get('float_percent', 0):.1f}%")
                        
                        # 機構持股變化
                        inst_change = inst.get('change_vs_prior', 0)
                        if inst_change != 0:
                            if inst_change > 0:
                                st.success(f"📈 機構近期增持 {inst_change:+.1f}%")
                            else:
                                st.warning(f"📉 機構近期減持 {inst_change:+.1f}%")
                        
                        # 前十大機構持股
                        top_holders = inst.get('top_holders', pd.DataFrame())
                        if not top_holders.empty:
                            with st.expander("📊 查看前十大機構持股"):
                                st.dataframe(top_holders, use_container_width=True)
                        
                        st.markdown("---")
                        
                        # 2. 空頭持倉分析
                        short = us_chip.get('short_interest', {})
                        
                        st.markdown("#### 🐻 空頭持倉 (Short Interest)")
                        col_s1, col_s2, col_s3, col_s4 = st.columns(4)
                        
                        short_pct = short.get('short_percent_of_float', 0)
                        short_ratio = short.get('short_ratio', 0)
                        short_change = short.get('short_change_pct', 0)
                        
                        col_s1.metric("空頭佔流通股", f"{short_pct:.1f}%")
                        col_s2.metric("回補天數", f"{short_ratio:.1f}天")
                        col_s3.metric("空頭股數", f"{short.get('shares_short', 0)/1_000_000:.2f}M")
                        
                        delta_color = "inverse" if short_change > 0 else "normal"
                        col_s4.metric("較上月變化", f"{short_change:+.1f}%", delta_color=delta_color)
                        
                        # 空頭風險提示
                        if short_pct > 20:
                            st.warning(f"🔥 **高軋空風險**：空頭比例 {short_pct:.1f}% 極高，若股價上漲可能引發軋空行情")
                        elif short_pct > 10:
                            st.info(f"⚠️ 空頭比例偏高 ({short_pct:.1f}%)，留意軋空機會")
                        
                        st.markdown("---")
                        
                        # 3. 內部人交易
                        insider = us_chip.get('insider_trades', {})
                        
                        st.markdown("#### 👔 內部人交易 (Insider Trading)")
                        col_i1, col_i2, col_i3 = st.columns(3)
                        
                        buy_count = insider.get('buy_count', 0)
                        sell_count = insider.get('sell_count', 0)
                        sentiment = insider.get('sentiment', 'neutral')
                        
                        col_i1.metric("買入次數", buy_count)
                        col_i2.metric("賣出次數", sell_count)
                        
                        sentiment_map = {'bullish': '🟢 偏多', 'bearish': '🔴 偏空', 'neutral': '⚪ 中性'}
                        col_i3.metric("內部人情緒", sentiment_map.get(sentiment, '⚪ 中性'))
                        
                        # 內部人交易明細
                        recent_trades = insider.get('recent_trades', pd.DataFrame())
                        if not recent_trades.empty:
                            with st.expander("📋 查看內部人交易明細"):
                                st.dataframe(recent_trades.head(10), use_container_width=True)
                        
                        st.markdown("---")
                        
                        # 4. 分析師評等
                        recs = us_chip.get('recommendations', {})
                        
                        st.markdown("#### 📊 分析師評等 (Analyst Recommendations)")
                        col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                        
                        rec_key = recs.get('recommendation', 'N/A')
                        target_price = recs.get('target_price', 0)
                        current_price = recs.get('current_price', 0)
                        upside = recs.get('upside', 0)
                        
                        rec_map = {
                            'strong_buy': '🟢 強力買進',
                            'buy': '🟢 買進',
                            'hold': '🟡 持有',
                            'sell': '🔴 賣出',
                            'strong_sell': '🔴 強力賣出'
                        }
                        
                        col_r1.metric("評等", rec_map.get(rec_key, rec_key))
                        col_r2.metric("目標價", f"${target_price:.2f}" if target_price else "N/A")
                        col_r3.metric("現價", f"${current_price:.2f}" if current_price else "N/A")
                        
                        delta_color = "normal" if upside > 0 else "inverse"
                        col_r4.metric("上漲空間", f"{upside:+.1f}%", delta_color=delta_color)
                        
                        # 目標價區間
                        target_high = recs.get('target_high', 0)
                        target_low = recs.get('target_low', 0)
                        if target_high and target_low:
                            st.caption(f"目標價區間: ${target_low:.2f} ~ ${target_high:.2f}")

                    else:
                        st.warning(f"⚠️ 無法取得美股籌碼數據: {us_err}")

                except Exception as e:
                    st.error(f"❌ 美股籌碼分析錯誤: {e}")

                # === SEC EDGAR 申報資料 ===
                try:
                    from sec_edgar import SECEdgarAnalyzer
                    st.markdown("---")
                    st.markdown("### 📋 SEC EDGAR 申報資料")

                    edgar = SECEdgarAnalyzer()
                    edgar_data, edgar_err = edgar.get_edgar_data(source)

                    if edgar_data:
                        # 內部人交易活躍度
                        insider_sec = edgar_data.get('insider', {})
                        form4_count = insider_sec.get('form4_count_90d', 0)
                        activity = insider_sec.get('activity_level', '無資料')

                        ec1, ec2, ec3 = st.columns(3)
                        ec1.metric("近 90 天 Form 4 申報", f"{form4_count} 筆")
                        ec2.metric("內部人交易活躍度", activity)

                        # 13F 機構申報
                        inst_13f = edgar_data.get('institutional', {})
                        latest_13f = inst_13f.get('latest_date', 'N/A')
                        ec3.metric("最新 13F 申報", latest_13f or 'N/A')

                        # 近期重要申報清單
                        filings = edgar_data.get('filings', [])
                        if filings:
                            with st.expander(f"📄 近期重要申報 ({len(filings)} 筆)", expanded=False):
                                filing_data = []
                                for f in filings[:15]:
                                    filing_data.append({
                                        '表單': f['form'],
                                        '類型': f['description'],
                                        '日期': f['date'],
                                    })
                                st.table(pd.DataFrame(filing_data))
                    elif edgar_err:
                        st.caption(f"SEC EDGAR: {edgar_err}")
                except ImportError:
                    pass
                except Exception as e:
                    st.caption(f"SEC EDGAR 資料取得失敗: {e}")

                # === Finviz 數據 ===
                try:
                    from finviz_data import FinvizAnalyzer
                    st.markdown("---")
                    st.markdown("### 📊 Finviz 技術快照")

                    fv = FinvizAnalyzer()
                    fv_data, fv_err = fv.get_stock_data(source)

                    if fv_data:
                        # 分析師目標價
                        analyst = fv_data.get('analyst', {})
                        target_p = analyst.get('target_price')
                        current_p = analyst.get('current_price')
                        upside = analyst.get('upside_pct')
                        recom = analyst.get('recommendation', 'N/A')

                        fc1, fc2, fc3, fc4 = st.columns(4)
                        fc1.metric("Finviz 目標價", f"${target_p:.2f}" if target_p else "N/A")
                        fc2.metric("分析師建議", recom)
                        if upside is not None:
                            fc3.metric("上漲空間", f"{upside:+.1f}%")
                        else:
                            fc3.metric("上漲空間", "N/A")

                        # 技術指標
                        tech = fv_data.get('technical', {})
                        fc4.metric("RSI(14)", tech.get('rsi14', 'N/A'))

                        # 估值與 SMA 距離
                        val = fv_data.get('valuation', {})
                        with st.expander("📈 Finviz 詳細指標", expanded=False):
                            vc1, vc2 = st.columns(2)
                            with vc1:
                                st.markdown("**估值指標**")
                                val_items = [
                                    ("P/E (TTM)", val.get('pe', 'N/A')),
                                    ("Forward P/E", val.get('forward_pe', 'N/A')),
                                    ("PEG", val.get('peg', 'N/A')),
                                    ("P/S", val.get('ps', 'N/A')),
                                    ("P/B", val.get('pb', 'N/A')),
                                    ("EPS (TTM)", val.get('eps_ttm', 'N/A')),
                                    ("EPS 未來成長", val.get('eps_growth_next_5y', 'N/A')),
                                    ("殖利率", val.get('dividend_yield', 'N/A')),
                                ]
                                st.table(pd.DataFrame(val_items, columns=['指標', '數值']))
                            with vc2:
                                st.markdown("**技術指標**")
                                tech_items = [
                                    ("SMA20 距離", tech.get('sma20', 'N/A')),
                                    ("SMA50 距離", tech.get('sma50', 'N/A')),
                                    ("SMA200 距離", tech.get('sma200', 'N/A')),
                                    ("Beta", tech.get('beta', 'N/A')),
                                    ("52 週高點距離", tech.get('high_52w', 'N/A')),
                                    ("52 週低點距離", tech.get('low_52w', 'N/A')),
                                    ("放空比例", tech.get('short_float', 'N/A')),
                                    ("相對成交量", tech.get('rel_volume', 'N/A')),
                                ]
                                st.table(pd.DataFrame(tech_items, columns=['指標', '數值']))
                    elif fv_err:
                        st.caption(f"Finviz: {fv_err}")
                except ImportError:
                    pass
                except Exception as e:
                    st.caption(f"Finviz 資料取得失敗: {e}")
            
            else:
                 st.info("💡 籌碼分析支援台股代號 (如 2330) 與美股代號 (如 AAPL, NVDA)。CSV 模式不支援。")

        with tab4:
             st.markdown("### 🏢 基本面數據 (Fundamentals)")
             
             # 1. Company Profile
             fd = st.session_state.get('fund_cache', None)
             if fd:
                 c1, c2 = st.columns([1, 3])
                 with c1:
                      st.markdown(f"#### {stock_meta.get('name', display_ticker)}")
                      st.write(f"**產業**: {fd.get('Sector', 'N/A')}")
                      st.write(f"**市值**: {fd.get('Market Cap', 'N/A')}")
                      st.metric("本益比 (P/E)", fd.get('PE Ratio', 'N/A'))
                      st.metric("殖利率 (Yield)", fd.get('Dividend Yield', 'N/A'))
                 with c2:
                      st.info(fd.get('Business Summary', '暫無簡介'))
                      st.json(fd, expanded=False)
             else:
                 st.warning("⚠️ 無基本面數據 (可能為 CSV 模式或查無資料)")

             st.markdown("---")
             
             # 2. Charts
             # Extract pure stock ID
             stock_id_pure = display_ticker.split('.')[0] if '.' in display_ticker else display_ticker
             
             if stock_id_pure.isdigit():
                 # A. Monthly Revenue
                 rev_df = get_revenue_history(stock_id_pure)
                 if not rev_df.empty:
                     st.markdown("#### 📊 月營收趨勢 (Monthly Revenue)")
                     
                     # Check columns
                     if 'revenue' in rev_df.columns:
                         # revenue unit in FinMind is usually raw value
                         rev_df['revenue_e'] = rev_df['revenue'] / 100_000_000 
                         
                         fig_rev = go.Figure()
                         fig_rev.add_trace(go.Bar(
                             x=rev_df['date'], y=rev_df['revenue_e'],
                             name='營收(億)', marker_color='#3366CC', yaxis='y1'
                         ))
                         # YoY might be null for first year
                         if 'revenue_year_growth' in rev_df.columns:
                             fig_rev.add_trace(go.Scatter(
                                 x=rev_df['date'], y=rev_df['revenue_year_growth'],
                                 name='年增率(%)', marker_color='#DC3912', yaxis='y2', mode='lines+markers'
                             ))
                         
                         fig_rev.update_layout(
                             height=350,
                             yaxis=dict(title='營收 (億)', side='left'),
                             yaxis2=dict(title='年增率 (%)', side='right', overlaying='y', showgrid=False),
                             hovermode='x unified',
                             legend=dict(orientation="h", y=1.1)
                         )
                         st.plotly_chart(fig_rev, use_container_width=True)
                 
                 # B. PE/PB History
                 per_df = get_per_history(stock_id_pure)
                 if not per_df.empty:
                     st.markdown("#### 📉 本益比與股價淨值比趨勢 (PE & PB Trend)")
                     
                     fig_pe = go.Figure()
                     if 'PER' in per_df.columns:
                         fig_pe.add_trace(go.Scatter(
                             x=per_df['date'], y=per_df['PER'],
                             name='本益比 (PE)', line=dict(color='purple'),
                         ))
                     if 'PBR' in per_df.columns:
                         fig_pe.add_trace(go.Scatter(
                             x=per_df['date'], y=per_df['PBR'],
                             name='股價淨值比 (PB)', line=dict(color='green'),
                             yaxis='y2'
                         ))
                     
                     fig_pe.update_layout(
                         height=300,
                         yaxis=dict(title='PE Times', side='left'),
                         yaxis2=dict(title='PB Times', side='right', overlaying='y', showgrid=False),
                         hovermode='x unified',
                         legend=dict(orientation="h", y=1.1)
                     )
                     st.plotly_chart(fig_pe, use_container_width=True)

                 # C. Profitability (EPS & Margins)
                 fin_df = get_financial_statements(stock_id_pure)
                 if not fin_df.empty:
                     st.markdown("#### 💰 獲利能力分析 (Profitability)")
                     
                     # 1. EPS Chart
                     if 'EPS' in fin_df.columns:
                         fig_eps = go.Figure()
                         fig_eps.add_trace(go.Bar(
                             x=fin_df.index, y=fin_df['EPS'],
                             name='EPS (元)', marker_color='#1E88E5'
                         ))
                         fig_eps.update_layout(
                             title="每股盈餘 (EPS)",
                             height=300,
                             yaxis_title="EPS (元)",
                             hovermode='x unified',
                             margin=dict(l=20, r=20, t=40, b=20)
                         )
                         st.plotly_chart(fig_eps, use_container_width=True)
                         
                     # 2. Three Rates Chart
                     fig_margin = go.Figure()
                     has_margin = False
                     if 'GrossMargin' in fin_df.columns:
                         fig_margin.add_trace(go.Scatter(
                            x=fin_df.index, y=fin_df['GrossMargin'],
                            name='毛利率 (%)', mode='lines+markers', line=dict(color='#FFC107', width=2)
                         ))
                         has_margin = True
                     if 'OperatingMargin' in fin_df.columns:
                         fig_margin.add_trace(go.Scatter(
                            x=fin_df.index, y=fin_df['OperatingMargin'],
                            name='營益率 (%)', mode='lines+markers', line=dict(color='#FF5722', width=2)
                         ))
                         has_margin = True
                     if 'NetProfitMargin' in fin_df.columns:
                         fig_margin.add_trace(go.Scatter(
                            x=fin_df.index, y=fin_df['NetProfitMargin'],
                            name='淨利率 (%)', mode='lines+markers', line=dict(color='#4CAF50', width=2)
                         ))
                         has_margin = True
                         
                     if has_margin:
                         fig_margin.update_layout(
                             title="三率走勢圖 (Margins)",
                             height=350,
                             yaxis_title="百分比 (%)",
                             hovermode='x unified',
                             legend=dict(orientation="h", y=1.2),
                             margin=dict(l=20, r=20, t=40, b=20)
                         )
                         st.plotly_chart(fig_margin, use_container_width=True)
             else:
                st.info("💡 歷史基本面圖表僅支援台股代號")

        # ==========================================
        # Tab 5: 情緒/期權分析
        # ==========================================
        with tab5:
            st.markdown("#### 🔮 市場情緒與期權分析")

            # Fear & Greed Index
            try:
                from taifex_data import TaiwanFearGreedIndex
                fgi = TaiwanFearGreedIndex()
                with st.spinner("計算恐懼貪婪指數..."):
                    fg_result = fgi.calculate()
                    fg_score = fg_result.get('score', 50)
                    fg_label = fg_result.get('label', 'N/A')

                    fg1, fg2 = st.columns([1, 2])
                    with fg1:
                        # Gauge-like display
                        color = '#FF4444' if fg_score < 25 else '#FF8800' if fg_score < 40 else '#FFD700' if fg_score < 60 else '#88CC00' if fg_score < 75 else '#00CC44'
                        st.metric("恐懼貪婪指數", f"{fg_score:.0f}", delta=fg_label)
                        st.progress(int(fg_score))
                        st.caption("0=極度恐懼 → 100=極度貪婪")
                    with fg2:
                        components = fg_result.get('components', {})
                        if components:
                            label_map = {
                                'market_momentum': '市場動能',
                                'market_breadth': '漲跌家數',
                                'put_call_ratio': 'Put/Call比',
                                'volatility': '波動率',
                                'margin_balance': '融資餘額'
                            }
                            comp_data = []
                            for name, val in components.items():
                                # val 是 dict，score 可能是 None（取得失敗）
                                if isinstance(val, dict):
                                    score = val.get('score')
                                    if score is not None:
                                        status = "恐懼" if score < 40 else "貪婪" if score > 60 else "中性"
                                        comp_data.append({"指標": label_map.get(name, name), "分數": f"{score:.0f}", "狀態": status})
                                    else:
                                        comp_data.append({"指標": label_map.get(name, name), "分數": "N/A", "狀態": "無資料"})
                                else:
                                    comp_data.append({"指標": label_map.get(name, name), "分數": f"{val:.0f}" if isinstance(val, (int, float)) else str(val), "狀態": ""})
                            if comp_data:
                                st.table(pd.DataFrame(comp_data))
            except ImportError:
                st.info("taifex_data 模組尚未安裝")
            except Exception as e:
                st.warning(f"恐懼貪婪指數暫時無法取得: {e}")

            # === CNN Fear & Greed Index (美股) ===
            try:
                from cnn_fear_greed import CNNFearGreedIndex
                st.markdown("---")
                st.markdown("#### 🇺🇸 CNN Fear & Greed Index (美股)")

                cnn_fg = CNNFearGreedIndex()
                with st.spinner("取得 CNN Fear & Greed Index..."):
                    cnn_result = cnn_fg.get_index()
                    cnn_score = cnn_result.get('score')

                    if cnn_score is not None:
                        cnn_c1, cnn_c2 = st.columns([1, 2])
                        with cnn_c1:
                            cnn_color = CNNFearGreedIndex.get_color(cnn_score)
                            st.metric("CNN 恐懼貪婪", f"{cnn_score:.0f}", delta=cnn_result.get('label', ''))
                            st.progress(int(min(cnn_score, 100)))

                            # 歷史比較
                            prev = cnn_result.get('previous_close')
                            week = cnn_result.get('one_week_ago')
                            month = cnn_result.get('one_month_ago')
                            year = cnn_result.get('one_year_ago')

                            hist_data = []
                            if prev is not None:
                                hist_data.append({"時間": "前日收盤", "分數": f"{prev:.0f}"})
                            if week is not None:
                                hist_data.append({"時間": "一週前", "分數": f"{week:.0f}"})
                            if month is not None:
                                hist_data.append({"時間": "一月前", "分數": f"{month:.0f}"})
                            if year is not None:
                                hist_data.append({"時間": "一年前", "分數": f"{year:.0f}"})
                            if hist_data:
                                st.table(pd.DataFrame(hist_data))

                        with cnn_c2:
                            # 子指標
                            cnn_components = cnn_result.get('components', {})
                            if cnn_components:
                                cnn_comp_data = []
                                for name, val in cnn_components.items():
                                    c_score = val.get('score')
                                    c_rating = val.get('rating', 'N/A')
                                    if c_score is not None:
                                        status = "恐懼" if c_score < 40 else "貪婪" if c_score > 60 else "中性"
                                        cnn_comp_data.append({"指標": name, "分數": f"{c_score:.0f}", "狀態": status})
                                    else:
                                        cnn_comp_data.append({"指標": name, "分數": "N/A", "狀態": c_rating})
                                if cnn_comp_data:
                                    st.table(pd.DataFrame(cnn_comp_data))
                    else:
                        st.caption(f"CNN F&G: {cnn_result.get('error', '無法取得')}")
            except ImportError:
                pass
            except Exception as e:
                st.caption(f"CNN Fear & Greed 暫時無法取得: {e}")

            st.markdown("---")

            # TAIFEX Data
            try:
                from taifex_data import TAIFEXData
                taifex = TAIFEXData()
                tc1, tc2 = st.columns(2)
                with tc1:
                    st.markdown("**期貨正逆價差**")
                    try:
                        basis = taifex.get_futures_basis()
                        if basis.get('basis') is not None:
                            b_val = basis['basis']
                            st.metric("基差", f"{b_val:.0f} 點", delta="正價差 (偏多)" if b_val > 0 else "逆價差 (偏空)")
                    except Exception:
                        st.caption("期貨數據暫時無法取得")
                with tc2:
                    st.markdown("**Put/Call Ratio**")
                    try:
                        pcr = taifex.get_put_call_ratio()
                        if pcr.get('pc_ratio') is not None:
                            pc = pcr['pc_ratio']
                            st.metric("P/C Ratio", f"{pc:.2f}", delta="恐懼" if pc > 1.0 else "貪婪" if pc < 0.7 else "中性")
                    except Exception:
                        st.caption("選擇權數據暫時無法取得")
            except ImportError:
                pass
            except Exception:
                pass

            st.markdown("---")

            # PTT Sentiment (結果存 session_state 避免 rerun 跳 tab)
            try:
                from ptt_sentiment import PTTSentimentAnalyzer
                stock_id_clean = display_ticker.split('.')[0] if '.' in display_ticker else display_ticker

                if stock_id_clean.isdigit():
                    ptt_stock_name = stock_meta.get('name', '') if stock_meta else ''
                    search_hint = f"{stock_id_clean}"
                    if ptt_stock_name and ptt_stock_name != stock_id_clean:
                        search_hint += f" / {ptt_stock_name}"

                    ptt_cache_key = f"ptt_result_{stock_id_clean}"
                    if st.button(f"🔍 分析 PTT 情緒 ({search_hint})", key="ptt_btn"):
                        with st.spinner(f"爬取 PTT Stock 板 {search_hint} 相關討論..."):
                            ptt = PTTSentimentAnalyzer()
                            sentiment = ptt.get_stock_sentiment(stock_id_clean, pages=5, stock_name=ptt_stock_name if ptt_stock_name else None)
                            st.session_state[ptt_cache_key] = sentiment

                    # 顯示快取結果 (button rerun 後仍可見)
                    sentiment = st.session_state.get(ptt_cache_key)
                    if sentiment is not None:
                        if sentiment['total_posts'] > 0:
                            ps1, ps2, ps3 = st.columns(3)
                            ps1.metric("相關文章數", sentiment['total_posts'])
                            ps2.metric("推噓比", f"{sentiment['push_ratio']:.0%}")
                            ps3.metric("情緒分數", f"{sentiment['sentiment_score']:.0f}", delta=sentiment['sentiment_label'])
                            if sentiment.get('contrarian_warning'):
                                st.warning("⚠️ 極度樂觀！擦鞋童效應警告 — 過度看多可能是頂部信號")
                            if sentiment.get('recent_posts'):
                                with st.expander("相關文章"):
                                    for post in sentiment['recent_posts'][:10]:
                                        st.write(f"[{post['date']}] {post['title']} (推{post['push']}/噓{post['boo']})")
                        else:
                            st.info(f"PTT 近期無 {stock_id_clean} 相關討論")
                else:
                    st.info("PTT 情緒分析僅支援台股")
            except ImportError:
                st.info("ptt_sentiment 模組尚未安裝")
            except Exception as e:
                st.warning(f"PTT 情緒分析失敗: {e}")

            # === Google Trends 搜尋熱度 ===
            try:
                from google_trends import GoogleTrendsAnalyzer
                st.markdown("---")
                st.markdown("#### 🔍 Google Trends 搜尋熱度")

                stock_id_gt = display_ticker.split('.')[0] if '.' in display_ticker else display_ticker
                gt_stock_name = stock_meta.get('name', '') if stock_meta else ''

                gt_cache_key = f"gtrends_result_{stock_id_gt}"
                if st.button("📊 分析搜尋趨勢", key="gtrends_btn"):
                    with st.spinner("取得 Google Trends 數據 (約 5 秒)..."):
                        gt = GoogleTrendsAnalyzer()
                        gt_result = gt.get_search_trend(stock_id_gt, stock_name=gt_stock_name if gt_stock_name else None)
                        st.session_state[gt_cache_key] = gt_result

                # 顯示快取結果 (button rerun 後仍可見)
                gt_result = st.session_state.get(gt_cache_key)
                if gt_result is not None:
                    if gt_result.get('error'):
                        st.warning(f"Google Trends: {gt_result['error']}")
                    else:
                        gt1, gt2, gt3 = st.columns(3)
                        gt1.metric("目前搜尋熱度", gt_result['current_interest'])
                        gt2.metric("近 7 日平均", f"{gt_result['recent_avg']:.0f}")
                        gt3.metric("變化率", f"{gt_result['change_pct']:+.0f}%", delta=gt_result['trend_label'])

                        # 趨勢圖表
                        trend_df = gt_result.get('trend_df')
                        if trend_df is not None and not trend_df.empty:
                            import plotly.express as px
                            fig_gt = px.line(trend_df, y=trend_df.columns[:2], title="搜尋趨勢 (近 90 天)")
                            fig_gt.update_layout(
                                height=300,
                                xaxis_title=None, yaxis_title="搜尋熱度 (0-100)",
                                hovermode='x unified',
                                margin=dict(l=20, r=20, t=40, b=20)
                            )
                            st.plotly_chart(fig_gt, use_container_width=True)

                        # 相關搜尋
                        related = gt_result.get('related_queries', [])
                        if related:
                            st.caption(f"相關搜尋: {', '.join(related)}")

                        # 提醒
                        if gt_result['change_pct'] > 50:
                            st.warning("⚠️ 搜尋量暴增 — 散戶關注度激增，留意過熱風險")
                        elif gt_result['change_pct'] < -30:
                            st.info("💡 搜尋量低迷 — 市場關注度低，可能處於冷門期")
            except ImportError:
                st.caption("💡 安裝 pytrends 可啟用搜尋熱度分析: pip install pytrends")
            except Exception as e:
                st.caption(f"Google Trends 暫時無法取得: {e}")

        # ==========================================
        # Tab 6: 除息/營收分析
        # ==========================================
        with tab6:
            st.markdown("#### 📊 除權息行事曆 & 月營收追蹤")
            stock_id_clean = display_ticker.split('.')[0] if '.' in display_ticker else display_ticker

            if not stock_id_clean.isdigit():
                st.info("除息/營收分析僅支援台股")
            else:
                try:
                    from dividend_revenue import DividendAnalyzer, RevenueTracker

                    # Dividend Section
                    st.markdown("##### 💰 除權息分析")
                    try:
                        da = DividendAnalyzer()
                        with st.spinner("載入股利資料..."):
                            div_hist = da.get_dividend_history(stock_id_clean)
                            if not div_hist.empty:
                                st.dataframe(div_hist, use_container_width=True)

                                # Fill-gap stats
                                fg_stats = da.get_fill_gap_stats(stock_id_clean)
                                if fg_stats:
                                    dc1, dc2, dc3 = st.columns(3)
                                    dc1.metric("平均填息天數", f"{fg_stats.get('avg_fill_days', 0):.0f} 天")
                                    dc2.metric("填息率", f"{fg_stats.get('fill_rate', 0):.0f}%")
                                    dc3.metric("建議", fg_stats.get('recommendation', 'N/A'))
                            else:
                                st.info("查無股利資料")

                        # Upcoming ex-date
                        upcoming = da.get_upcoming_ex_dates(stock_id_clean)
                        if upcoming and upcoming.get('has_upcoming'):
                            st.success(f"📅 即將除息：{upcoming['ex_date']}，股利 {upcoming['dividend_amount']:.2f} 元，殖利率 {upcoming['yield_pct']:.1f}%，距今 {upcoming['days_until']} 天")
                    except Exception as e:
                        st.warning(f"股利資料暫時無法取得: {e}")

                    st.markdown("---")

                    # Revenue Section
                    st.markdown("##### 📈 月營收追蹤")
                    try:
                        rt = RevenueTracker()
                        with st.spinner("載入營收資料..."):
                            rev_df = rt.get_monthly_revenue(stock_id_clean)
                            if not rev_df.empty:
                                # Revenue chart
                                import plotly.graph_objects as go
                                fig_rev = go.Figure()
                                fig_rev.add_trace(go.Bar(
                                    x=rev_df['year_month'], y=rev_df['revenue'],
                                    name='月營收', marker_color='#4CAF50'
                                ))
                                if 'yoy_pct' in rev_df.columns:
                                    fig_rev.add_trace(go.Scatter(
                                        x=rev_df['year_month'], y=rev_df['yoy_pct'],
                                        name='YoY%', yaxis='y2', mode='lines+markers',
                                        line=dict(color='#FF9800', width=2)
                                    ))
                                fig_rev.update_layout(
                                    title="月營收趨勢", height=350,
                                    yaxis=dict(title='營收 (千元)'),
                                    yaxis2=dict(title='YoY %', overlaying='y', side='right'),
                                    hovermode='x unified',
                                    margin=dict(l=20, r=60, t=40, b=20)
                                )
                                st.plotly_chart(fig_rev, use_container_width=True)
                            else:
                                st.info("查無營收資料")

                        # Revenue alert
                        alert = rt.get_revenue_alert(stock_id_clean)
                        if alert and alert.get('alert_text'):
                            st.info(f"📢 {alert['alert_text']}")

                        # Revenue surprise
                        surprise = rt.detect_revenue_surprise(stock_id_clean)
                        if surprise and surprise.get('is_surprise'):
                            if surprise['direction'] == 'positive':
                                st.success(f"🎉 營收正驚喜！{surprise['text']}")
                            else:
                                st.error(f"⚠️ 營收負驚喜！{surprise['text']}")
                    except Exception as e:
                        st.warning(f"營收資料暫時無法取得: {e}")

                except ImportError:
                    st.info("dividend_revenue 模組尚未安裝")

        # ==========================================
        # 6. 策略回測系統 (Strategy Backtester)
        # ==========================================
        st.markdown("---")
        st.subheader("📈 策略歷史回測與優化 (Backtest & Optimization)")
        st.info("驗證 AI 評分模型在過去 3 年的即時績效。")

        bc1, bc2 = st.columns(2)
        
        run_default = bc1.button("🚀 執行 AI 策略 (預設參數)", use_container_width=True)
        run_opt = bc2.button("✨ 自動最佳化 (Auto Optimize)", use_container_width=True)

        if run_default or run_opt:
            # [Visual Feedback] Progress Bar
            prog_bar = st.progress(0, text="正在初始化回測引擎...")
            
            with st.spinner("正在模擬歷史交易與運算分數... (需時約 10 秒)"):
                try:
                    from backtest_engine import BacktestEngine
                    from technical_analysis import load_and_resample, calculate_all_indicators
                    from strategy_manager import StrategyManager 
                    
                    # 1. Reload Data
                    prog_bar.progress(20, text="正在載入歷史數據...")
                    # Use display_ticker which holds the actual ticker string (e.g. "2330.TW")
                    # If CSV mode, display_ticker is "Uploaded File", which might crash load_and_resample if not handled.
                    # But Backtest is primarily for Tickers. For CSV, we might need to use 'source' if it was preserved?
                    # But load_and_resample expects a ticker string usually to fetch fresh data for backtest?
                    # Actually, if we are in CSV mode, 'source' is a DataFrame. load_and_resample accepts DataFrame too in my wrapper?
                    # Let's check app.py definition of load_and_resample wrapper (none, it imports).
                    # app.py run_analysis wrapper handles checks.
                    
                    # Safe approach: Pass 'source' (which is ticker str OR DataFrame)
                    # But load_and_resample signature: (ticker_or_df, force_update=True)
                    
                    # For Backtesting, we want strict consistency.
                    target_source = source 
                    if isinstance(source, str):
                         target_source = source
                    elif input_method == "上傳 CSV 檔":
                         # Re-read CSV? or use cached?
                         # For now, let's use display_ticker if string, else handle error.
                         pass

                    _, df_bt, _, _ = load_and_resample(display_ticker, force_update=False)
                    
                    if not df_bt.empty:
                        prog_bar.progress(40, text="正在計算技術指標...")
                        df_bt = calculate_all_indicators(df_bt)
                        
                        # 2. Initialize Engine
                        engine = BacktestEngine(df_bt, initial_capital=100000)
                        sm = StrategyManager() 
                        
                        results = {}
                        params = ""
                        
                        if run_opt:
                            prog_bar.progress(60, text="正在執行 AI 參數最佳化 (Grid Search)...")
                            st.toast("正在進行網格搜索最佳參數...", icon="🔍")
                            best_p, results = engine.optimize()
                            
                            # Auto-Save
                            sm.save_strategy(display_ticker, best_p['buy'], best_p['sell'])
                            st.toast(f"已儲存 {display_ticker} 專屬策略參數！", icon="💾")
                            
                            params = f"最佳參數: 買進分數 > {best_p['buy']}, 賣出分數 < {best_p['sell']} (已自動儲存)"
                            st.success(f"✨ 找到並儲存最佳策略組合！ {params}")
                        else:
                            prog_bar.progress(60, text="正在執行歷史回測...")
                            # Default AI Logic
                            results = engine.run(buy_threshold=3, sell_threshold=-2)
                            params = "目前參數: 買進分數 > 3, 賣出分數 < -2 (預設)"
                        
                        prog_bar.progress(100, text="回測完成！")
                        prog_bar.empty() # Clear bar
                        
                        st.success("✅ 回測模擬完成！以下是過去 3 年的績效報告", icon="🏁")
                        
                        # 3. Display Results
                        st.markdown(f"### 📊 回測結果 ({params})")
                        
                        # === 基本績效指標 ===
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("總報酬率", f"{results['total_return']:.2f}%")
                        m2.metric("交易勝率", f"{results['win_rate']:.1f}%")
                        m3.metric("最大回檔", f"{results['max_drawdown']:.2f}%")
                        m4.metric("目前持倉", "持有中" if results['holding'] else "空手")

                        # === Alpha & 進階指標 ===
                        rm = results.get('risk_metrics', {})
                        if rm:
                            m5, m6, m7, m8 = st.columns(4)
                            alpha = results.get('alpha', 0)
                            m5.metric("Alpha (超額報酬)", f"{alpha:+.2f}%")
                            m6.metric("Sharpe Ratio", f"{rm.get('sharpe_ratio', 0):.2f}")
                            m7.metric("Profit Factor", f"{rm.get('profit_factor', 0):.2f}")
                            m8.metric("平均持有天數", f"{rm.get('avg_holding_days', 0):.0f} 天")

                        # === 績效曲線 (含大盤基準) ===
                        fig_bt = engine.plot_results(results)
                        st.plotly_chart(fig_bt, use_container_width=True)

                        # === 進階分析 (展開式) ===
                        with st.expander("📊 進階風險指標 & 交易統計", expanded=False):
                            if rm:
                                rc1, rc2 = st.columns(2)
                                with rc1:
                                    st.markdown("**風險指標**")
                                    risk_data = {
                                        "Sharpe Ratio": f"{rm.get('sharpe_ratio', 0):.3f}",
                                        "Sortino Ratio": f"{rm.get('sortino_ratio', 0):.3f}",
                                        "Calmar Ratio": f"{rm.get('calmar_ratio', 0):.3f}",
                                        "Profit Factor": f"{rm.get('profit_factor', 0):.2f}",
                                        "最大回撤持續天數": f"{rm.get('max_dd_duration_days', 0)} 天",
                                    }
                                    st.table(pd.DataFrame(risk_data.items(), columns=["指標", "數值"]))
                                with rc2:
                                    st.markdown("**交易統計**")
                                    trade_data = {
                                        "平均獲利": f"{results.get('avg_win', 0):.2f}%",
                                        "平均虧損": f"{results.get('avg_loss', 0):.2f}%",
                                        "最大單筆獲利": f"{results.get('largest_win', 0):.2f}%",
                                        "最大單筆虧損": f"{results.get('largest_loss', 0):.2f}%",
                                        "最長連勝": f"{rm.get('max_consecutive_wins', 0)} 次",
                                        "最長連敗": f"{rm.get('max_consecutive_losses', 0)} 次",
                                    }
                                    st.table(pd.DataFrame(trade_data.items(), columns=["指標", "數值"]))

                            # 月報酬熱力圖
                            monthly_ret = results.get('monthly_returns')
                            if monthly_ret is not None and not monthly_ret.empty:
                                st.markdown("**月度報酬一覽**")
                                # 轉換為年/月 pivot
                                mr_df = monthly_ret.reset_index()
                                mr_df.columns = ['month', 'return']
                                mr_df['year'] = mr_df['month'].str[:4]
                                mr_df['mon'] = mr_df['month'].str[5:7]
                                pivot = mr_df.pivot(index='year', columns='mon', values='return')
                                st.dataframe(pivot.style.format("{:.1f}%").background_gradient(cmap='RdYlGn', axis=None))

                        # === 交易紀錄 ===
                        with st.expander("查看詳細交易紀錄 (Trade Log)"):
                            if not results['trades'].empty:
                                st.dataframe(results['trades'])
                            else:
                                st.info("期間無交易產生。")

                        # === Walk-Forward 前推最佳化 ===
                        st.markdown("---")
                        st.markdown("#### 🔬 進階回測工具")
                        wf_col, mc_col = st.columns(2)

                        with wf_col:
                            if st.button("🔄 Walk-Forward 前推驗證", use_container_width=True):
                                with st.spinner("執行 Walk-Forward 最佳化... (分段驗證防過擬合)"):
                                    try:
                                        wf_result = engine.walk_forward_optimize()
                                        st.success(f"✅ Walk-Forward 完成！OOS 總報酬: {wf_result['total_return']:.2f}%")
                                        fig_wf = engine.plot_walk_forward(wf_result)
                                        st.plotly_chart(fig_wf, use_container_width=True)
                                        with st.expander("各段詳細結果"):
                                            for i, seg in enumerate(wf_result['windows']):
                                                st.write(f"**Window {i+1}**: 買={seg['best_params']['buy']}, 賣={seg['best_params']['sell']} → OOS報酬={seg['oos_return']:.2f}%")
                                    except Exception as e:
                                        st.error(f"Walk-Forward 失敗: {e}")

                        with mc_col:
                            if st.button("🎲 Monte Carlo 模擬", use_container_width=True):
                                with st.spinner("執行 Monte Carlo 模擬 (1000 次隨機排列)..."):
                                    try:
                                        mc_result = engine.monte_carlo(results)
                                        st.success(f"✅ Monte Carlo 完成！95% 信心區間: {mc_result['p5_return']:.1f}% ~ {mc_result['p95_return']:.1f}%")
                                        fig_mc = engine.plot_monte_carlo(mc_result)
                                        st.plotly_chart(fig_mc, use_container_width=True)
                                        mc_data = {
                                            "平均報酬": f"{mc_result['mean_return']:.2f}%",
                                            "中位數報酬": f"{mc_result['median_return']:.2f}%",
                                            "5% 最差情境": f"{mc_result['p5_return']:.2f}%",
                                            "95% 最佳情境": f"{mc_result['p95_return']:.2f}%",
                                            "平均最大回撤": f"{mc_result['mean_dd']:.2f}%",
                                            "95% 最差回撤": f"{mc_result['p5_dd']:.2f}%",
                                        }
                                        st.table(pd.DataFrame(mc_data.items(), columns=["指標", "數值"]))
                                    except Exception as e:
                                        st.error(f"Monte Carlo 失敗: {e}")

                        # === Pyramiding 分批進場回測 ===
                        if st.button("📐 Pyramiding 分批進場回測", use_container_width=True):
                            with st.spinner("執行金字塔分批回測 (1/3 + 1/3 + 1/3)..."):
                                try:
                                    pyr_result = engine.run_pyramid(
                                        buy_threshold=3, sell_threshold=-2,
                                        slippage=0.001, max_positions=3
                                    )
                                    pc1, pc2, pc3, pc4 = st.columns(4)
                                    pc1.metric("總報酬率", f"{pyr_result['total_return']:.2f}%")
                                    pc2.metric("勝率", f"{pyr_result['win_rate']:.1f}%")
                                    pc3.metric("最大回撤", f"{pyr_result['max_drawdown']:.2f}%")
                                    pc4.metric("平均批次", f"{pyr_result['avg_batches']:.1f}")
                                    fig_pyr = engine.plot_pyramid_results(pyr_result)
                                    st.plotly_chart(fig_pyr, use_container_width=True)
                                    if not pyr_result['trades'].empty:
                                        with st.expander("Pyramiding 交易紀錄"):
                                            st.dataframe(pyr_result['trades'])
                                except Exception as e:
                                    st.error(f"Pyramiding 失敗: {e}")

                    else:
                        st.error("無法載入數據進行回測")
                        
                except Exception as e:
                    st.error(f"回測執行失敗: {str(e)}")

            st.markdown("---")

    except Exception as e:
        status_text.error(f"❌ 發生未預期錯誤: {e}")
        st.exception(e)

else:
    # 初始歡迎畫面
    st.info("👈 請在左測試欄輸入代號並點擊「開始分析」")

