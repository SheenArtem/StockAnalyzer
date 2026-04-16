import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import logging
import threading
import time
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
    st.caption("Version: v2026.04.16.10")
    
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

    # Mode toggle: 個股分析 / 自動選股 / AI 報告庫
    _mode_options = ['individual', 'screener', 'ai_reports']
    _mode_labels = {'individual': '📈 個股分析', 'screener': '🔍 自動選股', 'ai_reports': '📝 AI 報告'}
    _current_mode = st.session_state.get('app_mode', 'analysis')
    _mode_idx = 1 if _current_mode == 'screener' else (2 if _current_mode == 'ai_reports' else 0)
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


# ====================================================================
#  AI 報告背景執行緒 Worker
#  讓報告生成不會被 Streamlit rerun 中斷
# ====================================================================
def _ai_report_worker(job, ticker, report_format='md'):
    """
    在背景 thread 跑完整 AI 報告流程。
    job 是一個 dict (session_state 裡的參照)，thread 直接 mutate 欄位。
    禁止呼叫任何 st.* UI 函式（會觸發 ScriptRunContext 警告）。

    Args:
        report_format: 'md' = 傳統 Markdown 報告；'html' = 互動儀表板
    """
    try:
        job['progress'].append("📥 載入價量資料...")
        figures, errors, df_week, df_day, stock_meta = run_analysis(ticker, force_update=False)

        job['progress'].append("📥 載入籌碼資料...")
        chip_data = None
        us_chip_data = None
        if ticker.isdigit() or ticker.endswith('.TW'):
            try:
                chip_data, _ = get_chip_data_cached(ticker, False)
            except Exception as _e:
                logger.warning(f"[AI worker] chip load failed: {_e}")
        else:
            try:
                from us_stock_chip import USStockChipAnalyzer
                _usc = USStockChipAnalyzer()
                us_chip_data, _ = _usc.get_chip_data(ticker)
            except Exception as _e:
                logger.warning(f"[AI worker] US chip load failed: {_e}")

        job['progress'].append("📥 載入基本面資料...")
        try:
            fund_data = get_fundamentals(ticker)
        except Exception as _e:
            logger.warning(f"[AI worker] fundamental load failed: {_e}")
            fund_data = None

        job['progress'].append("📊 計算技術分析與觸發分數...")
        from analysis_engine import TechnicalAnalyzer as _TA
        _analyzer = _TA(ticker, df_week, df_day, chip_data=chip_data, us_chip_data=us_chip_data)
        _report = _analyzer.run_analysis()

        if report_format == 'html':
            job['progress'].append("🤖 Claude AI 生成儀表板 JSON 中（不設逾時，可能需要 1-5 分鐘）...")
            from ai_report import generate_report_html as _gen_html, save_report_html as _save_html
            _ok, _content_or_err, _json = _gen_html(
                ticker, _report, chip_data, us_chip_data, fund_data, df_day,
                timeout=None,
            )
            if _ok:
                job['progress'].append("💾 組裝 HTML + 儲存到報告庫...")
                _rid = _save_html(
                    ticker, _content_or_err,
                    trigger_score=_report.get('trigger_score'),
                    trend_score=_report.get('trend_score'),
                    json_data=_json,
                )
                job['result'] = {'rid': _rid, 'content': _content_or_err, 'format': 'html'}
                job['status'] = 'done'
            else:
                job['result'] = _content_or_err
                job['status'] = 'error'
        else:
            job['progress'].append("🤖 Claude AI 生成 Markdown 報告中（不設逾時，可能需要 1-5 分鐘）...")
            from ai_report import generate_report as _gen_report, save_report as _save_report
            _ok, _content = _gen_report(
                ticker, _report, chip_data, us_chip_data, fund_data, df_day,
                timeout=None,
            )
            if _ok:
                job['progress'].append("💾 儲存到報告庫...")
                _rid = _save_report(
                    ticker, _content,
                    trigger_score=_report.get('trigger_score'),
                    trend_score=_report.get('trend_score'),
                )
                job['result'] = {'rid': _rid, 'content': _content, 'format': 'md'}
                job['status'] = 'done'
            else:
                job['result'] = _content
                job['status'] = 'error'
    except Exception as _e:
        import traceback
        logger.error(f"[AI worker] exception: {_e}", exc_info=True)
        job['result'] = f"{type(_e).__name__}: {_e}\n\n{traceback.format_exc()}"
        job['status'] = 'error'


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

# ====================================================================
#  大盤儀表板 Banner（所有模式共用）
# ====================================================================
try:
    from market_banner import render_market_banner
    render_market_banner()
except Exception as _banner_err:
    logger.debug("Market banner failed: %s", _banner_err)

if st.session_state.get('app_mode') == 'screener':
    # ====================================================================
    #  自動選股模式 — 右側動能 + 左側價值
    # ====================================================================
    import json as _json
    from pathlib import Path as _Path

    screener_tab_qm, screener_tab_meanrev, screener_tab_track = st.tabs(
        ["🛡️ 品質選股", "🔄 均值回歸", "📊 績效追蹤"]
    )
    # Hidden tabs (code preserved, just not displayed)
    # Value tabs paused pending Phase 1 enhancement (see project_value_enhancement.md)
    screener_tab1 = screener_tab_us = screener_tab_swing = screener_tab_conv = None
    screener_tab2 = screener_tab_us_val = None

    # ====================================================================
    # Pre-load convergence data for badges on all tabs
    # ====================================================================
    _conv_map_tw = {}   # stock_id -> {'tier': int, 'modes': [...]}
    _conv_map_us = {}
    for _conv_suffix, _conv_target in [('', _conv_map_tw), ('_us', _conv_map_us)]:
        _conv_file = _Path(f'data/latest/convergence{_conv_suffix}_result.json')
        if _conv_file.exists():
            try:
                with open(_conv_file, 'r', encoding='utf-8') as _f:
                    _conv_data = _json.load(_f)
                for _cr in _conv_data.get('results', []):
                    _conv_target[_cr['stock_id']] = {
                        'tier': _cr.get('convergence_tier', 0),
                        'modes': _cr.get('modes', []),
                    }
            except Exception:
                pass

    def _convergence_label(stock_id, conv_map):
        """產生共振標記文字"""
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

    # ====================================================================
    # Tab 1: 右側動能選股 (hidden)
    # ====================================================================
    if False:  # hidden tab

        with st.expander("📋 篩選條件說明"):
            st.markdown("""
**Stage 1 初篩（市值 + 流動性聯集）**

| 條件 | 門檻 | 說明 |
|------|------|------|
| 市值前 300 大 | TradingView | 大型+中型股池 |
| **OR** 20 日均成交值 | > 5 億 | 高流動性中小型股也入選 |
| 當日漲跌幅 | > -1% | 允許微跌，排除大跌股 |

**Stage 2 評分（觸發分數 + 趨勢分數）**

由 `analysis_engine.py` 計算，綜合技術面、籌碼面、型態辨識等指標。

**訊號代碼對照表**

| 訊號 | 中文 | 說明 |
|------|------|------|
| `supertrend_bull` | Supertrend 多方 | 價格在趨勢線上方，趨勢向上 |
| `supertrend_bear` | Supertrend 空方 | 價格在趨勢線下方，趨勢向下 |
| `macd_golden` | MACD 黃金交叉 | MACD 線突破訊號線 / 柱狀體翻正 |
| `macd_dead` | MACD 死亡交叉 | MACD 線跌破訊號線 / 柱狀體翻負 |
| `rsi_bull_div` | RSI 底背離 | 價格創新低但 RSI 沒有，反彈訊號 |
| `rsi_bear_div` | RSI 頂背離 | 價格創新高但 RSI 沒有，轉弱訊號 |
| `rvol_high` | 爆量確認 | 成交量放大，突破有量能支撐 |
| `rvol_low` | 量能萎縮 | 成交量極低，賣壓枯竭訊號 |
| `inst_buy` | 法人買超 | 三大法人積極或持續買超 |
| `inst_sell` | 法人賣超 | 三大法人大量賣超 |
| `etf_sync_buy` | ETF 同步買超 | 多檔主動型 ETF 同時買入 |
| `etf_buy` | ETF 買超 | 主動型 ETF 買超 |
| `etf_sync_sell` | ETF 同步賣超 | 多檔主動型 ETF 同時賣出 |
| `squeeze_fire` | 壓縮釋放 | 布林帶壓縮後突破，波動率擴張 |

**低波放量指標（RVOL-ATR）**

```
RVOL     = 今日成交量 / 20日均量        （相對成交量）
ATR_pct  = ATR(14) / 收盤價 x 100      （波動率佔比）
低波放量 = RVOL 的 Z-Score - ATR_pct 的 Z-Score
         （Z-Score = 252 日滾動標準化）
```

越高 = 成交量異常放大 + 波動率異常收斂 = 有人安靜吃貨。
IC 驗證 60 日 Sharpe 9.50、勝率 76%，單一指標最強。
""")

        latest_file = _Path('data/latest/momentum_result.json')
        scan_result = None
        if latest_file.exists():
            try:
                with open(latest_file, 'r', encoding='utf-8') as _f:
                    scan_result = _json.load(_f)
            except Exception:
                scan_result = None

        if scan_result and scan_result.get('results'):
            results = scan_result['results']
            st.caption(
                f"掃描日期: {scan_result.get('scan_date', '?')} {scan_result.get('scan_time', '')} | "
                f"全市場 {scan_result.get('total_scanned', 0)} 檔 → "
                f"初篩 {scan_result.get('passed_initial', 0)} 檔 → "
                f"評分 {scan_result.get('scored_count', 0)} 檔 | "
                f"耗時 {scan_result.get('elapsed_seconds', 0):.0f}s"
            )

            # Market regime (same for all picks in one scan)
            _regime_raw = results[0].get('regime', 'unknown') if results else 'unknown'
            _regime_map = {
                'trending': ('趨勢盤', '大盤有明確方向', '加重趨勢信號 (T×1.3)，降低量能權重'),
                'ranging':  ('盤整盤', '大盤橫盤震盪',   '加重量能信號 (V×1.3)，降低趨勢權重'),
                'volatile': ('高波動', '大盤劇烈震盪',   '三組略降，量能稍加重，籌碼面權重降低'),
                'neutral':  ('中性',   '無法判斷',       '三組等權重 (1:1:1)'),
            }
            _rn, _rd, _re = _regime_map.get(_regime_raw, ('未知', '', ''))
            st.info(f"🏛️ 目前市場狀態: **{_rn}** — {_rd}｜評分影響: {_re}")

            # Build DataFrame for display
            _scenario_map = {'A': 'A 強攻', 'B': 'B 拉回', 'C': 'C 搶短', 'D': 'D 空手', 'N': 'N 觀望'}
            _rows = []
            for r in results:
                _rl = r.get('rvol_lowatr')
                _sc = r.get('scenario', {}).get('code', '')
                _rows.append({
                    '代號': r['stock_id'],
                    '名稱': r.get('name', ''),
                    '收盤': r.get('price', 0),
                    '漲跌%': r.get('change_pct', 0),
                    '均量(億)': round(r.get('avg_trading_value_5d', 0) / 1e8, 2),
                    '觸發分數': r.get('trigger_score', 0),
                    '趨勢分數': r.get('trend_score', 0),
                    '劇本': _scenario_map.get(_sc, _sc),
                    'ETF買超': r.get('etf_buy_count', 0),
                    '低波放量': round(_rl, 2) if _rl is not None else None,
                    'Top20': '★' if r.get('rvol_lowatr_top20') else '',
                    '共振': _convergence_label(r['stock_id'], _conv_map_tw),
                    '關鍵訊號': ', '.join(r.get('signals', [])[:3]),
                })
            _df_results = pd.DataFrame(_rows)

            # rvol_lowatr Top20 filter
            _show_top20 = st.checkbox("只顯示 低波放量 Top 20 (低波放量精選)", key='tw_mom_top20')
            if _show_top20:
                _df_results = _df_results[_df_results['Top20'] == '★']

            # Sorting
            _sort_options_m = {
                '觸發分數 (高→低)': ('觸發分數', False),
                '趨勢分數 (高→低)': ('趨勢分數', False),
                '低波放量 (高→低)': ('低波放量', False),
                '均量(億) (高→低)': ('均量(億)', False),
                '漲跌% (高→低)': ('漲跌%', False),
            }
            _sort_choice = st.selectbox(
                "排序方式", list(_sort_options_m.keys()),
                key='momentum_tw_sort',
            )
            _sort_col, _sort_asc = _sort_options_m[_sort_choice]
            _df_results = _df_results.sort_values(_sort_col, ascending=_sort_asc).reset_index(drop=True)
            _df_results.index = range(1, len(_df_results) + 1)

            st.dataframe(
                _df_results,
                width='stretch',
                height=600,
                column_config={
                    '觸發分數': st.column_config.NumberColumn(format="%.1f"),
                    '趨勢分數': st.column_config.NumberColumn(format="%.1f"),
                    '漲跌%': st.column_config.NumberColumn(format="%.1f%%"),
                    '收盤': st.column_config.NumberColumn(format="%.1f"),
                    '均量(億)': st.column_config.NumberColumn(format="%.2f"),
                },
            )

            st.caption("觸發分數 -10~+10 (日線進場信號) / 趨勢分數 -5~+5 (週線趨勢) / 低波放量 越高=低波放量")

            # Detailed action plan (expandable per stock)
            with st.expander("個股操作建議"):
                _selected = st.selectbox(
                    "選擇股票",
                    options=[f"{r['stock_id']} {r.get('name', '')}" for r in results],
                    key='screener_detail_select',
                )
                if _selected:
                    _sid = _selected.split()[0]
                    _match = next((r for r in results if r['stock_id'] == _sid), None)
                    if _match:
                        _sc = _match.get('scenario', {})
                        _ap = _match.get('action_plan', {})
                        _cl = _match.get('checklist', {})

                        # Header: score + scenario
                        st.markdown(f"### {_sid} {_match.get('name', '')}")
                        st.markdown(f"**{_sc.get('title', '')}** — {_sc.get('desc', '')}")
                        st.markdown(f"觸發分數: **{_match['trigger_score']:+.1f}** / "
                                    f"趨勢分數: **{_match['trend_score']:+.1f}**")

                        # Strategy + entry/exit
                        if _ap.get('strategy'):
                            st.markdown(f"\n{_ap['strategy']}")

                        _col_l, _col_r = st.columns(2)
                        with _col_l:
                            st.markdown("**進場區間**")
                            _el = _ap.get('rec_entry_low')
                            _eh = _ap.get('rec_entry_high')
                            if _el and _eh:
                                st.markdown(f"- {_ap.get('rec_entry_desc', '')}: **{_el:.1f} ~ {_eh:.1f}**")
                            else:
                                st.markdown("- (無建議)")

                            if _ap.get('tp_list'):
                                st.markdown("**停利參考**")
                                for _tp in _ap['tp_list']:
                                    _rec = ' **(建議)**' if _tp.get('is_rec') else ''
                                    st.markdown(f"- {_tp['method']}: {_tp['price']:.1f} — {_tp.get('desc', '')}{_rec}")

                        with _col_r:
                            if _ap.get('sl_list'):
                                st.markdown("**停損參考**")
                                for _sl in _ap['sl_list']:
                                    _loss = _sl.get('loss')
                                    _loss_str = f" ({_loss:+.1f}%)" if _loss is not None else ''
                                    st.markdown(f"- {_sl['method']}: {_sl['price']:.1f}{_loss_str}")

                            _rr = _ap.get('rr_ratio')
                            if _rr:
                                st.markdown(f"\n**風險報酬比**: {_rr:.1f}:1")

                        # Checklist
                        if _cl:
                            st.markdown("---")
                            if _cl.get('risk'):
                                st.markdown("**風險警示**")
                                for _item in _cl['risk']:
                                    st.markdown(f"- {_item}")
                            if _cl.get('active'):
                                st.markdown("**加碼訊號**")
                                for _item in _cl['active']:
                                    st.markdown(f"- {_item}")
                            if _cl.get('future'):
                                st.markdown("**後續觀察**")
                                for _item in _cl['future']:
                                    st.markdown(f"- {_item}")

                        # Trigger details (collapsed)
                        with st.expander("評分明細", expanded=False):
                            for d in _match.get('trigger_details', []):
                                st.markdown(f"- {d}")

        else:
            st.info("尚無掃描結果。\n\n"
                    "在命令列執行 `python scanner_job.py --mode momentum` 進行完整掃描\n"
                    "（含觸發分數，約需 15-30 分鐘）")

    # ====================================================================
    # Tab US: 美股動能選股 (hidden)
    # ====================================================================
    if False:  # hidden tab

        with st.expander("📋 Screening Criteria"):
            st.markdown("""
**Stage 1 Initial Filter**

| Criteria | Threshold | Description |
|----------|-----------|-------------|
| Universe | S&P 500 | 美股以 S&P 500 成分股為掃描範圍 |
| Min Volume | > 500,000 | 日均成交量，過濾低流動性 |
| Min Price | > $5.00 | 排除低價股 (penny stocks) |
| Daily Change | > -1% | 允許微跌，排除大跌股 |

**Stage 2 Scoring**

同台股動能評分，由 `analysis_engine.py` 計算觸發分數 + 趨勢分數。

**Signal Reference**

| Signal | 中文 | Description |
|--------|------|-------------|
| `supertrend_bull` | Supertrend 多方 | Price above Supertrend line |
| `macd_golden` | MACD 黃金交叉 | MACD line crosses above signal |
| `rsi_bull_div` | RSI 底背離 | Bullish divergence |
| `rsi_bear_div` | RSI 頂背離 | Bearish divergence |
| `rvol_high` | 爆量確認 | Volume surge confirms breakout |
| `rvol_low` | 量能萎縮 | Volume dry-up, selling exhaustion |
| `squeeze_fire` | 壓縮釋放 | Bollinger squeeze breakout |
""")

        us_file = _Path('data/latest/momentum_us_result.json')
        us_result = None
        if us_file.exists():
            try:
                with open(us_file, 'r', encoding='utf-8') as _f:
                    us_result = _json.load(_f)
            except Exception:
                us_result = None

        if us_result and us_result.get('results'):
            us_results = us_result['results']
            st.caption(
                f"Scan: {us_result.get('scan_date', '?')} {us_result.get('scan_time', '')} | "
                f"Universe: {us_result.get('total_scanned', 0)} → "
                f"Passed: {us_result.get('passed_initial', 0)} → "
                f"Scored: {us_result.get('scored_count', 0)} | "
                f"Time: {us_result.get('elapsed_seconds', 0):.0f}s"
            )

            _us_regime_raw = us_results[0].get('regime', 'unknown') if us_results else 'unknown'
            _us_regime_map = {
                'trending': ('Trending', 'Clear market direction', 'Trend weight +30%, Volume weight -30%'),
                'ranging':  ('Ranging',  'Sideways market',        'Volume weight +30%, Trend weight -30%'),
                'volatile': ('Volatile', 'High volatility',        'All groups slightly reduced, chip weight lowered'),
                'neutral':  ('Neutral',  'Undetermined',           'Equal weights (1:1:1)'),
            }
            _urn, _urd, _ure = _us_regime_map.get(_us_regime_raw, ('Unknown', '', ''))
            st.info(f"🏛️ Market Regime: **{_urn}** — {_urd} | Scoring impact: {_ure}")

            _us_rows = []
            for r in us_results:
                _rl = r.get('rvol_lowatr')
                _us_rows.append({
                    'Ticker': r['stock_id'],
                    'Price': r.get('price', 0),
                    'Chg%': r.get('change_pct', 0),
                    'TV(M)': round(r.get('avg_trading_value_5d', 0) / 1e6, 1),
                    'Score': r.get('trigger_score', 0),
                    'Trend': r.get('trend_score', 0),
                    '低波放量': round(_rl, 2) if _rl is not None else None,
                    'Top20': '★' if r.get('rvol_lowatr_top20') else '',
                    'Conv': _convergence_label(r['stock_id'], _conv_map_us),
                    'Signals': ', '.join(r.get('signals', [])[:3]),
                })
            _us_df = pd.DataFrame(_us_rows)

            _show_top20_us = st.checkbox("Show 低波放量 Top 20 only", key='us_mom_top20')
            if _show_top20_us:
                _us_df = _us_df[_us_df['Top20'] == '★']

            _sort_opts_us_m = {
                'Score (High→Low)': ('Score', False),
                'Trend (High→Low)': ('Trend', False),
                '低波放量 (High→Low)': ('低波放量', False),
                'TV(M) (High→Low)': ('TV(M)', False),
                'Chg% (High→Low)': ('Chg%', False),
            }
            _us_sort = st.selectbox("Sort by", list(_sort_opts_us_m.keys()), key='momentum_us_sort')
            _us_sc, _us_sa = _sort_opts_us_m[_us_sort]
            _us_df = _us_df.sort_values(_us_sc, ascending=_us_sa).reset_index(drop=True)
            _us_df.index = range(1, len(_us_df) + 1)

            st.dataframe(
                _us_df,
                width='stretch',
                height=600,
                column_config={
                    'Score': st.column_config.NumberColumn(format="%.1f"),
                    'Trend': st.column_config.NumberColumn(format="%.1f"),
                    'Chg%': st.column_config.NumberColumn(format="%.1f%%"),
                    'Price': st.column_config.NumberColumn(format="$%.2f"),
                    'TV(M)': st.column_config.NumberColumn(format="%.1f"),
                },
            )
            st.caption("Score -10~+10 (daily trigger) / Trend -5~+5 (weekly trend) / 低波放量 higher=low vol+high vol")

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
    # Tab Swing: 波段選股 (hidden)
    # ====================================================================
    if False:  # hidden tab
        st.markdown("### 🔄 波段選股 (台股)")
        st.markdown("""
**持倉期 2 週 ~ 3 個月**，結合動能評分與週線趨勢，以 低波放量 排序。

**選股邏輯**：觸發分數 Top 候選 → 趨勢分數 >= 1（週線上升趨勢）→ 低波放量 排序（放量+低波動優先）

| 依據 | 回測績效（60 日 horizon） |
|------|------------------------|
| 低波放量 Top-20 | Sharpe **9.50**, 勝率 **76%**, 平均報酬 +3.2% |
| Scanner Top-20 | Sharpe 6.50, 勝率 66%, 平均報酬 +5.5% |

**低波放量計算方式**

```
RVOL     = 今日成交量 / 20日均量        （相對成交量）
ATR_pct  = ATR(14) / 收盤價 x 100      （波動率佔比）
低波放量 = RVOL 的 Z-Score - ATR_pct 的 Z-Score
         （Z-Score = 252 日滾動標準化）
```

越高 = 成交量異常放大 + 波動率異常收斂 = 有人安靜吃貨。
""")

        swing_file = _Path('data/latest/swing_result.json')
        swing_result = None
        if swing_file.exists():
            try:
                with open(swing_file, 'r', encoding='utf-8') as _f:
                    swing_result = _json.load(_f)
            except Exception:
                swing_result = None

        if swing_result and swing_result.get('results'):
            sw_results = swing_result['results']
            st.caption(
                f"掃描日期: {swing_result.get('scan_date', '?')} {swing_result.get('scan_time', '')} | "
                f"全市場 {swing_result.get('total_scanned', 0)} 檔 → "
                f"評分 {swing_result.get('scored_count', 0)} 檔 | "
                f"耗時 {swing_result.get('elapsed_seconds', 0):.0f}s"
            )

            _scenario_map_sw = {'A': 'A 強攻', 'B': 'B 拉回', 'C': 'C 搶短', 'D': 'D 空手', 'N': 'N 觀望'}
            _sw_rows = []
            for r in sw_results:
                _rl = r.get('rvol_lowatr')
                _sc = r.get('scenario', {}).get('code', '')
                _sw_rows.append({
                    '代號': r['stock_id'],
                    '名稱': r.get('name', ''),
                    '收盤': r.get('price', 0),
                    '漲跌%': r.get('change_pct', 0),
                    '均量(億)': round(r.get('avg_trading_value_5d', 0) / 1e8, 2),
                    '趨勢分數': r.get('trend_score', 0),
                    '觸發分數': r.get('trigger_score', 0),
                    '劇本': _scenario_map_sw.get(_sc, _sc),
                    '低波放量': round(_rl, 2) if _rl is not None else None,
                    'ETF買超': r.get('etf_buy_count', 0),
                    '共振': _convergence_label(r['stock_id'], _conv_map_tw),
                    '關鍵訊號': ', '.join(r.get('signals', [])[:3]),
                })
            _df_swing = pd.DataFrame(_sw_rows)

            _sort_opts_sw = {
                '低波放量 (高→低)': ('低波放量', False),
                '趨勢分數 (高→低)': ('趨勢分數', False),
                '觸發分數 (高→低)': ('觸發分數', False),
                '均量(億) (高→低)': ('均量(億)', False),
            }
            _sw_sort = st.selectbox("排序方式", list(_sort_opts_sw.keys()), key='swing_tw_sort')
            _sw_col, _sw_asc = _sort_opts_sw[_sw_sort]
            _df_swing = _df_swing.sort_values(_sw_col, ascending=_sw_asc).reset_index(drop=True)
            _df_swing.index = range(1, len(_df_swing) + 1)

            st.dataframe(
                _df_swing,
                width='stretch',
                height=600,
                column_config={
                    '觸發分數': st.column_config.NumberColumn(format="%.1f"),
                    '趨勢分數': st.column_config.NumberColumn(format="%.1f"),
                    '漲跌%': st.column_config.NumberColumn(format="%.1f%%"),
                    '收盤': st.column_config.NumberColumn(format="%.1f"),
                    '均量(億)': st.column_config.NumberColumn(format="%.2f"),
                },
            )

            st.caption("趨勢分數 >= 1（週線上升趨勢）/ 低波放量 越高=低波放量 / 建議持倉 2w-3m")

            # 操作建議
            with st.expander("個股操作建議"):
                _sw_selected = st.selectbox(
                    "選擇股票",
                    options=[f"{r['stock_id']} {r.get('name', '')}" for r in sw_results],
                    key='swing_detail_select',
                )
                if _sw_selected:
                    _sw_sid = _sw_selected.split()[0]
                    _sw_match = next((r for r in sw_results if r['stock_id'] == _sw_sid), None)
                    if _sw_match:
                        _sc = _sw_match.get('scenario', {})
                        _ap = _sw_match.get('action_plan', {})

                        st.markdown(f"### {_sw_sid} {_sw_match.get('name', '')}")
                        st.markdown(f"**{_sc.get('title', '')}** -- {_sc.get('desc', '')}")
                        st.markdown(f"趨勢分數: **{_sw_match['trend_score']:+.1f}** / "
                                    f"觸發分數: **{_sw_match['trigger_score']:+.1f}** / "
                                    f"低波放量: **{_sw_match.get('rvol_lowatr', 'N/A')}**")

                        if _ap.get('strategy'):
                            st.markdown(f"\n{_ap['strategy']}")

                        st.markdown("**波段操作要點**")
                        st.markdown("- 停損參考: 週線 Supertrend 或 MA60 跌破")
                        st.markdown("- 停利方式: 趨勢跟蹤，週線翻空才出場")
                        st.markdown("- 加碼條件: 拉回週線 MA20 不破 + 量縮後放量")

                        _el = _ap.get('rec_entry_low')
                        _eh = _ap.get('rec_entry_high')
                        if _el and _eh:
                            st.markdown(f"- 進場區間: **{_el:.1f} ~ {_eh:.1f}** ({_ap.get('rec_entry_desc', '')})")

                        if _ap.get('sl_list'):
                            st.markdown("**停損參考價位**")
                            for _sl in _ap['sl_list']:
                                st.markdown(f"- {_sl['method']}: {_sl['price']:.1f}")

                        with st.expander("評分明細", expanded=False):
                            for d in _sw_match.get('trigger_details', []):
                                st.markdown(f"- {d}")

        else:
            st.info("尚無波段掃描結果。\n\n"
                    "在命令列執行 `python scanner_job.py --mode swing` 進行波段掃描\n"
                    "（使用與動能相同的分析引擎，但以趨勢分數+低波放量排序）")

        st.caption("💡 Full scan: `python scanner_job.py --mode swing`")

    # ====================================================================
    # Tab QM: 品質選股選股
    # ====================================================================
    with screener_tab_qm:
        st.markdown("### 🛡️ 品質選股")

        # ================================================================
        # 持股監控 + 每日警報（B 任務）
        # ================================================================
        _POS_FILE = _Path('data/positions.json')
        _ALERT_FILE = _Path('data/latest/position_alerts.json')

        _alert_data = None
        if _ALERT_FILE.exists():
            try:
                with open(_ALERT_FILE, 'r', encoding='utf-8') as _f:
                    _alert_data = _json.load(_f)
            except Exception:
                pass

        _n_hard = (_alert_data or {}).get('hard_count', 0)
        _n_soft = (_alert_data or {}).get('soft_count', 0)
        _n_pos_a = (_alert_data or {}).get('position_count', 0)

        if _n_hard > 0:
            _pm_title = f"🚨 持股警報 — 硬警報 {_n_hard} 筆（立即處理）"
            _pm_expanded = True
        elif _n_soft > 0:
            _pm_title = f"⚠️ 持股警報 — 軟警報 {_n_soft} 筆（考慮減碼）"
            _pm_expanded = True
        elif _n_pos_a > 0:
            _pm_title = f"✅ 持股監控 — {_n_pos_a} 檔持股全部正常"
            _pm_expanded = False
        else:
            _pm_title = "📦 我的持股 + 每日警報"
            _pm_expanded = False

        with st.expander(_pm_title, expanded=_pm_expanded):
            from position_monitor import (
                load_positions as _pm_load,
                save_positions as _pm_save,
            )

            # A. 警報區
            if _alert_data and _alert_data.get('alerts'):
                st.markdown("#### 🚨 今日警報")
                for _a in _alert_data['alerts']:
                    _sev = _a['severity']
                    _ic = '🔴' if _sev == 'hard' else '🟡'
                    _ts = _a.get('trigger_score')
                    _ts_txt = f" · trigger {_ts:+.1f}" if _ts is not None else ""
                    st.markdown(
                        f"**{_ic} {_a['stock_id']} {_a.get('name','')}** · "
                        f"PnL {_a['pnl_pct']:+.1f}% · 持有 {_a.get('hold_days',0)} 天 · "
                        f"現價 {_a.get('current_price',0):.2f} / 進場 {_a.get('buy_price',0):.2f}"
                        f"{_ts_txt}"
                    )
                    for _t in _a.get('triggers', []):
                        _sub = '❌' if _t.get('severity') == 'hard' else '⚠️'
                        st.markdown(f"  - {_sub} {_t.get('desc','')}：{_t.get('value','')}")
                st.caption(
                    f"警報產生時間：{_alert_data.get('scan_date','?')} "
                    f"{_alert_data.get('scan_time','')}"
                )
                st.markdown("---")
            elif _alert_data and _alert_data.get('position_count', 0) > 0:
                st.caption(
                    f"✅ 最後檢查：{_alert_data.get('scan_date','?')} "
                    f"{_alert_data.get('scan_time','')} — 所有持股正常"
                )
                st.markdown("---")

            # B. 持股清單 + 最近 trigger_score（軟警報資料）
            _pos_list = _pm_load()
            st.markdown(f"#### 📋 持股清單（{len(_pos_list)} 檔）")
            if _pos_list:
                # 載入 trigger_score 歷史（軟警報累積資料）
                from position_monitor import (
                    load_history as _pm_load_hist,
                    _history_key as _pm_hkey,
                )
                _pm_hist = _pm_load_hist()

                _pos_rows = []
                for p in _pos_list:
                    _hk = _pm_hkey(p.get('stock_id', ''), p.get('buy_date', ''))
                    _series = _pm_hist.get(_hk, [])
                    _last_ts = _series[-1]['trigger_score'] if _series else None
                    _peak_ts = max((e['trigger_score'] for e in _series), default=None)
                    _pos_rows.append({
                        '代號': p.get('stock_id', ''),
                        '名稱': p.get('name', ''),
                        '進場日': p.get('buy_date', ''),
                        '進場價': p.get('buy_price', 0),
                        '股數': p.get('shares', 0),
                        '近峰值': _peak_ts,
                        '最新': _last_ts,
                        '歷史天數': len(_series),
                        '備註': p.get('notes', ''),
                    })
                st.dataframe(
                    pd.DataFrame(_pos_rows),
                    width='stretch',
                    hide_index=True,
                    column_config={
                        '進場價': st.column_config.NumberColumn(format="%.2f"),
                        '股數': st.column_config.NumberColumn(format="%d"),
                        '近峰值': st.column_config.NumberColumn(format="%+.1f", help="trigger_score 近 20 日峰值"),
                        '最新': st.column_config.NumberColumn(format="%+.1f", help="trigger_score 最近一次值"),
                        '歷史天數': st.column_config.NumberColumn(format="%d", help="已累積幾天 trigger_score"),
                    },
                )
                st.caption("軟警報觸發條件：近峰值 ≥ +5 且最新 ≤ -2（動能急轉）/ 連續 5 日 < 0（持續弱化）")
            else:
                st.caption("尚未新增持股。填下方表單新增第一筆。")

            # C. 新增持股
            st.markdown("#### ➕ 新增持股")
            with st.form('pm_add_form', clear_on_submit=True):
                _pm_c1, _pm_c2, _pm_c3 = st.columns(3)
                _pm_sid = _pm_c1.text_input("代號", key='pm_sid')
                _pm_nm = _pm_c2.text_input("名稱（選填）", key='pm_name')
                _pm_dt = _pm_c3.date_input("進場日期", value=None, key='pm_date')
                _pm_c4, _pm_c5, _pm_c6 = st.columns(3)
                _pm_pr = _pm_c4.number_input(
                    "進場價", min_value=0.0, step=0.01, format="%.2f", key='pm_price')
                _pm_sh = _pm_c5.number_input(
                    "股數", min_value=0, step=100, key='pm_shares')
                _pm_nt = _pm_c6.text_input("備註（選填）", key='pm_notes')
                _pm_ok = st.form_submit_button("新增持股")
                if _pm_ok:
                    if not _pm_sid.strip():
                        st.error("必填：股票代號")
                    elif _pm_pr <= 0:
                        st.error("必填：進場價 > 0")
                    else:
                        _pos_list.append({
                            'stock_id': _pm_sid.strip(),
                            'name': _pm_nm.strip(),
                            'buy_date': _pm_dt.isoformat() if _pm_dt else '',
                            'buy_price': float(_pm_pr),
                            'shares': int(_pm_sh),
                            'notes': _pm_nt.strip(),
                        })
                        _pm_save(_pos_list)
                        st.success(f"已新增 {_pm_sid}")
                        st.rerun()

            # D. 刪除持股
            if _pos_list:
                st.markdown("#### 🗑️ 刪除持股")
                _pm_del_opts = [
                    f"{p['stock_id']} {p.get('name','')} @ {p.get('buy_date','-')}"
                    for p in _pos_list
                ]
                _pm_del_sel = st.selectbox(
                    "選擇要刪除", options=_pm_del_opts, key='pm_del_sel')
                if st.button("確認刪除", key='pm_del_btn'):
                    _pm_tgt_sid = _pm_del_sel.split()[0]
                    _pm_tgt_dt = _pm_del_sel.split('@')[-1].strip()
                    _pos_list = [
                        p for p in _pos_list
                        if not (p.get('stock_id') == _pm_tgt_sid
                                and p.get('buy_date', '-') == _pm_tgt_dt)
                    ]
                    _pm_save(_pos_list)
                    st.success(f"已刪除 {_pm_tgt_sid}")
                    st.rerun()

            # E. 手動執行監控
            st.markdown("---")
            _pm_run_col, _pm_cap_col = st.columns([1, 4])
            if _pm_run_col.button("🔄 立即檢查", key='pm_run_btn',
                                  disabled=not _pos_list):
                with st.spinner("檢查中..."):
                    from position_monitor import run_monitor as _pm_run
                    _pm_result = _pm_run(positions=_pos_list)
                    st.success(
                        f"完成：{_pm_result['position_count']} 檔 / "
                        f"硬警報 {_pm_result['hard_count']} · "
                        f"軟警報 {_pm_result['soft_count']}"
                    )
                    st.rerun()
            _pm_cap_col.caption(
                "出場條件：動態停損(ATR%調整, -5%~-14%) / 週 Supertrend 翻空 / 週 MA20 動態跌破 / "
                "月營收 YoY 連 2 月負 / trend_score < 1。"
                "每日 22:00 scanner 自動跑，此按鈕可立即檢查。"
            )

        with st.expander("📋 篩選條件說明", expanded=False):
            st.markdown("""
結合**技術面動能**、**基本面品質**與**波段趨勢**，三層篩選找出體質好、趨勢向上、有人吃貨的股票。

---

#### Stage 1：初篩（市值 + 流動性）

從全市場約 1,900 檔中，用兩個條件的**聯集**快速篩出候選池：

| 條件 | 門檻 | 說明 |
|------|------|------|
| 市值前 300 大 | TradingView 即時市值 | 涵蓋大型+中型股，確保機構有在看 |
| **OR** 20 日均成交值 | > 5 億 | 高流動性的中小型股也入選，不會錯殺熱門股 |
| 當日漲跌幅 | > -1% | 允許微跌，排除當天大跌的股票 |

兩個條件取聯集：市值大的一定選，成交活躍的也選。通過約 300-400 檔。

---

#### Stage 1.5：品質門檻（基本面快篩）

用 TradingView 免費批次資料，刷掉明顯地雷：

| 條件 | 門檻 | 目的 |
|------|------|------|
| ROE | > 0% | 公司有在賺錢 |
| 淨利率 | > 0% | 本業不虧損 |
| 負債比 | < 200% | 不是高槓桿爆雷股 |
| 營收 YoY | > -20% | 營收沒有崩盤 |

門檻故意**寬鬆**，目的只是排除明顯有問題的。資料缺失的股票**不懲罰**（放行）。
通常刷掉約 80 檔，剩 250-320 檔進入 Stage 2。

---

#### Stage 2：逐檔分析

每檔股票逐一載入 1 年歷史 K 線，計算以下分數（**QM 最終排序不用觸發分數**，僅供參考顯示）：

**趨勢分數（-5 ~ +5）** — 週線趨勢方向（QM 綜合評分佔 20%）

週 K 的 MA、Supertrend、DMI 綜合判斷，正值 = 週線多頭，負值 = 空頭。

**觸發分數（-10 ~ +10）** — 日線多空信號（QM 不採用，IC=+0.010 接近無效）

| 組別 | 指標 | 說明 |
|------|------|------|
| 趨勢組 | MA 均線回歸、Supertrend、DMI | 價格相對趨勢的位置 |
| 動能組 | MACD 交叉/背離、KD 交叉、RSI 背離 | 動能轉折信號 |
| 量能組 | RVOL（相對成交量 Z-Score） | 量能確認 |
| 籌碼組 | 法人動向、融資、券資比、借券 | 籌碼面評分（見下方） |

**籌碼面評分（±2.0）** — C2-b IC 驗證修正版（2026-04-16）

方向依據 5 年截面 IC 驗證：「籌碼乾淨 = 好」（法人不追、散戶不擠）

| 因子 | 加/減分 | IC 驗證 |
|------|---------|---------|
| **外資** 5 日買賣超 | 買超 +0.3 / 賣超 -0.3 | IR +0.06（微弱正，保守給分） |
| **投信** 5 日買賣超 | **買超 -0.5** / 賣超 +0.3 | IR **-0.32**（過熱逆向指標） |
| **融資使用率** | >60% -0.4 / <20% +0.2 | IR -0.24（散戶追漲） |
| **融資增量** 5 日 | 增 >5% -0.3 / 減 >5% +0.2 | 同上 |
| **券資比** | >30% **-0.6** / >15% -0.3 / <3% +0.2 | IR **-0.57**（最強因子） |
| **借券** 5 日增減 | 大增 -0.6 / 增 -0.3 / 大減 +0.4 | IR -0.33 |

**低波放量** — 僅供參考（QM 品質池內 IC=-0.037 負向，不列入評分）

```
低波放量 = RVOL 的 Z-Score - ATR_pct 的 Z-Score
```
- 越高 = 成交量異常放大 + 波動率異常收斂
- 注意：全市場 Sharpe 9.50，但品質池（移除爛股後）IC 反轉為負

Stage 2 完成後，過濾**趨勢分數 >= 1**，通常剩 50-100 檔。

趨勢分數由週 K 線的 6 個因子加總（-5 ~ +5）：均線架構(±2)、DMI 趨勢(±1)、OBV 能量潮(±1)、EFI 資金流(±1)、K 線形態(±2)、量價配合(±1)。
>= 1 表示至少有一個多方因子成立（例如站上週 MA20），週線偏多。

---

#### Stage 3：品質評分

對所有趨勢 >= 1 的股票，逐檔計算精細品質分（FinMind 財報 + 月營收）。

**品質分（0-100）= 體質分 x 60% + 營收分 x 40%**

**體質分（基準 50，加減分制）**

| 項目 | 來源 | 加/減分規則 |
|------|------|-----------|
| **F-Score** (0-9) | FinMind 財報三表 | >= 7: +25（強）/ <= 3: -20（價值陷阱） |
| **Z-Score** | FinMind | 安全區: +8 / 危險區: -20（破產風險） |
| ROIC | FinMind | > 15%: +8 / < 0: -5 |
| FCF Yield | FinMind | > 8%: +8 / < -5%: -5 |
| ROE | FinMind / TradingView | > 15%: +5 / < 0: -10 |
| 連續獲利 | FinMind EPS | 連續 4 季: +5 / 僅 1 季: -10 |
| 毛利率 | TradingView | > 40%: +5 / < 10%: -5 |
| 營益率 | TradingView | > 20%: +5 / < 0: -8 |
| 負債/權益 | TradingView | > 200%: -5 |
| 流動比率 | FinMind | > 2.0: +5 / < 1.0: -8 |

**營收分（基準 50，加減分制）**

| 項目 | 來源 | 加/減分規則 |
|------|------|-----------|
| 營收 YoY 已轉正 | 月營收 | +10 |
| 營收衰退收斂 | 月營收趨勢 | 最高 +20（收斂幅度越大越多） |
| 營收加速衰退 | 月營收趨勢 | 最高 -20 |
| 營收正驚喜 | 月營收 | +12 |
| 營收負驚喜 | 月營收 | -8 |

---

#### Stage 4：綜合評分 → Top 20

三個維度加權計算**綜合評分**（組內百分位加權），取 Top 20 輸出：

| 維度 | 權重 | 60d IC | 60d 勝率 | 來源 |
|------|------|--------|---------|------|
| **F-Score** (0-9) | **50%** | **+0.113** | **81%** | Piotroski 9 項財報指標 |
| **體質分** (0-100) | **30%** | +0.073 | 76% | ROE/Z-Score/ROIC/三率/流動比率 |
| **趨勢分數** (-5~+5) | **20%** | +0.043 | 52% | 週線 MA/DMI/OBV/EFI/形態 |

每個維度先算組內百分位排名（0-100），再加權得到綜合分。**最終排序和選取都以綜合評分為準。**

**權重來源：2026-04-15 IC 驗證 + NN 測試**
- 驗證期間：2022-01 ~ 2026-04（Test: 2024-07 之後）
- 回測 Sharpe：**60d 1.67**（勝率 76%, 報酬 +13.99%）
- 對比原始權重（rvol30/trig25/qual25/trend20）Sharpe 1.28，改善 **+30%**

**已移除的維度：**
- 低波放量（rvol_lowatr）60d IC=-0.037 負向，全市場有效但在品質池內反指標
- 觸發分數（trigger_score）60d IC=+0.010 幾乎無效

---

#### 訊號代碼對照

| 訊號 | 說明 |
|------|------|
| `supertrend_bull/bear` | Supertrend 多方/空方 |
| `macd_golden/dead` | MACD 黃金/死亡交叉 |
| `rsi_bull_div/bear_div` | RSI 底/頂背離 |
| `rvol_high/low` | 爆量確認/量能萎縮 |
| `inst_buy/sell` | 法人買超/賣超 |
| `etf_sync_buy/sell` | ETF 同步買超/賣超 |
| `squeeze_fire` | 布林帶壓縮釋放 |
""")

        with st.expander("📖 操作 SOP（選出來之後怎麼做）", expanded=False):
            st.markdown("""
### 一、QM 定位：**右側交易 + 基本面保險**

**不是左側抄底**。進場門檻 `trend_score >= 1` 代表週線+日線都已在趨勢中
（MA 多頭排列、Supertrend 多方、ADX 上升），在確認「股票已經開漲」後才進。

| 左側 | 右側 | **QM (右側 + 品質)** |
|------|------|---------------------|
| 逆勢抄底，等反轉 | 順勢追擊，等確認 | 趨勢確認後挑 F-Score 高、體質好的 |
| 勝率低、單次報酬大 | 勝率中、穩定 | 勝率 **76%** 高、平均 +14%/60d |
| Left tail 風險大 | Left tail 中等 | Left tail 低（品質過濾掉地雷） |

價值選股（左側抄底）請看 `💎 價值 (台股)` tab，QM 不要拿來抄底用。

---

### 二、驗證數據錨點（決定操作參數）

| Horizon | 平均報酬 | Sharpe | 勝率 |
|---------|---------|--------|------|
| **20d** | +4.4% | **1.99** | 79% |
| **40d** | +9.2% | 1.81 | 78% |
| **60d** | **+14.0%** | 1.67 | 76% |

- Sharpe 高點在 20d，絕對報酬高點在 60d
- 最佳 R:R 出現在 **40d**（兼顧兩者）
- **建議基準持倉 = 40-60 天**

---

### 三、操作 SOP

#### 進場

| 項目 | 建議 | 理由 |
|------|------|------|
| 批次 | **分 2 批（50%+50%）** | 右側怕追在短線高點 |
| 第二批加碼條件 | 日 RSI 回 45-55 或觸日 MA10 | 短線過熱釋放後 |
| 放棄進場 | 當日已漲 >5% 或跳空缺口 >3% | R:R 惡化 |
| 時段避開 | 財報前 5 個交易日 | QM 靠基本面，不在資訊盲區加倉 |
| 單檔上限 | 總資金 **8-10%** | 勝率 76% 可進取，但分散仍重要 |

#### 停損（雙保險）

```
硬停損 = max(動態停損, 週線 MA20)
動態停損 = 進場價 × (1 - clip(ATR% × 3, 5%, 14%))
```

- **動態硬停損**：依 ATR% 自動調整（低波動 -5% / 中等 -8% / 高波動 -14%）
- **週線 MA20 跌破**：趨勢結構破壞即出（容忍度依 ATR% 調整 -2%~-5%）
- **基本面急煞**：月營收 YoY **連續 2 個月轉負 → 立刻全出**
  （QM alpha 來源是基本面，基本面破 = 論據失效）

#### 停利 / 減碼

| 階段 | 動作 | 理由 |
|------|------|------|
| +8% 或持倉 20 日 | 移動停損升至成本價 | 鎖定不虧 |
| **TP1 (依 ATR% 縮放)** | **減碼 1/3** | 低波動 +10% / 中等 +15% / 高波動 +24% |
| **TP2** | **改用週 MA10 移動停利** | 已拿走超額，剩餘跟趨勢 |
| **TP3 或 60 日滿** | 清倉或換股輪動 | 驗證期外報酬衰減 |

#### 出場訊號（任一觸發即出）

1. 週線 Supertrend 翻空
2. 週 MA20 跌破 3% 以上（非插針）
3. 月營收 YoY 連 2 個月轉負
4. F-Score 季更新後掉 2 分以上

---

### 四、風報比試算

```
期望值 = 勝率 × 平均賺 − 敗率 × 平均賠
       = 0.76 × 14% − 0.24 × 8%
       = +8.7% 每筆 (60d)
```

年化：60 天轉一次，理論一年 5-6 循環 → **年化期望 ~40-50%**（未計成本）。
扣交易成本 0.5%/次 × 5 次 = 2.5% 損耗 → **淨年化約 35-45%**。

組合建議 **5-8 檔**分散單一事件風險，Sharpe 能從單檔 1.67 提升到組合 2.0+。

---

### 五、三個最容易犯的錯

1. **短抱** — 看到 +5% 就賣。QM 的 alpha 集中在 20-60 天，短抱等於丟掉 2/3 報酬
2. **當抄底用** — 跌下來加碼 QM 名單。QM 要求 `trend_score >= 1`，跌破趨勢後這檔已經不再是 QM
3. **忽略營收** — 只看技術停損。F-Score/營收是最強因子（IC +0.113），營收崩是比週 MA20 跌破**更早**的警報

---

> 選出個股後，下方「個股操作建議」區塊的 `strategy / 停損 / 三段停利 / 出場訊號`
> 已依本 SOP 自動計算並顯示（驗證錨點：Round 4 F50/Body30/Trend20 權重）。
""")

        qm_file = _Path('data/latest/qm_result.json')
        qm_result = None
        if qm_file.exists():
            try:
                with open(qm_file, 'r', encoding='utf-8') as _f:
                    qm_result = _json.load(_f)
            except Exception:
                qm_result = None

        if qm_result and qm_result.get('results'):
            qm_results = qm_result['results']
            st.caption(
                f"掃描日期: {qm_result.get('scan_date', '?')} {qm_result.get('scan_time', '')} | "
                f"全市場 {qm_result.get('total_scanned', 0)} 檔 → "
                f"品質篩 {qm_result.get('passed_initial', 0)} 檔 → "
                f"評分 {qm_result.get('scored_count', 0)} 檔 | "
                f"耗時 {qm_result.get('elapsed_seconds', 0):.0f}s"
            )

            # 🎯 今日擇時 Top 5（依 trigger_score 由高到低）
            #    trigger_score 整合日線 MACD/KD/RSI/RVOL/籌碼/情緒/營收/ETF，
            #    用於「今天該下手哪檔」的進場時機判斷（不影響選股排名）
            def _timing_badge(ts):
                if ts is None:
                    return '⚪'
                if ts >= 3:
                    return '🟢'
                if ts >= 0:
                    return '🟡'
                return '🔴'

            _qm_by_trigger = sorted(
                qm_results,
                key=lambda r: r.get('trigger_score', 0) or 0,
                reverse=True,
            )[:5]
            if _qm_by_trigger:
                st.markdown("#### 🎯 今日擇時 Top 5")
                _top5_cols = st.columns(5)
                for _i, _r in enumerate(_qm_by_trigger):
                    _ts = _r.get('trigger_score', 0) or 0
                    _cs = _r.get('composite_score')
                    _trend = _r.get('trend_score', 0) or 0
                    _badge = _timing_badge(_ts)
                    _cs_txt = f"{_cs:.0f}" if _cs is not None else "-"
                    with _top5_cols[_i]:
                        st.metric(
                            label=f"{_badge} {_r['stock_id']} {_r.get('name', '')[:6]}",
                            value=f"{_ts:+.1f}",
                            delta=f"綜合 {_cs_txt} / 趨勢 {_trend:+.1f}",
                            delta_color="off",
                        )
                st.caption("🟢 ≥3 今日可進場 / 🟡 0-3 觀察 / 🔴 <0 等訊號轉強（trigger_score 為日線擇時指標）")

            _qm_rows = []
            for r in qm_results:
                _fs = r.get('qm_f_score')
                _bs = r.get('qm_body_score')
                _cs = r.get('composite_score')
                _ts = r.get('trigger_score', 0) or 0
                _ap = r.get('action_plan', {}) or {}
                _sl = _ap.get('rec_sl_price')
                _rr = _ap.get('rr_ratio')
                _qm_rows.append({
                    '代號': r['stock_id'],
                    '名稱': r.get('name', ''),
                    '綜合': _cs if _cs is not None else None,
                    'F-Score': _fs if _fs is not None else None,
                    '體質分': round(_bs, 0) if _bs is not None else None,
                    '趨勢分數': r.get('trend_score', 0),
                    '擇時': _timing_badge(_ts),
                    '觸發分數': _ts,
                    '收盤': r.get('price', 0),
                    '推薦停損': _sl if _sl else None,
                    'R:R': _rr if _rr else None,
                    '漲跌%': r.get('change_pct', 0),
                })
            _df_qm = pd.DataFrame(_qm_rows)

            _sort_opts_qm = {
                '綜合 (高→低)': ('綜合', False),
                '觸發分數 (高→低)': ('觸發分數', False),
                'F-Score (高→低)': ('F-Score', False),
                '體質分 (高→低)': ('體質分', False),
                '趨勢分數 (高→低)': ('趨勢分數', False),
                'R:R (高→低)': ('R:R', False),
            }
            _qm_sort = st.selectbox("排序方式", list(_sort_opts_qm.keys()), key='qm_tw_sort')
            _qm_col, _qm_asc = _sort_opts_qm[_qm_sort]
            _df_qm = _df_qm.sort_values(_qm_col, ascending=_qm_asc).reset_index(drop=True)
            _df_qm.index = range(1, len(_df_qm) + 1)

            st.dataframe(
                _df_qm,
                width='stretch',
                height=600,
                column_config={
                    '綜合': st.column_config.NumberColumn(format="%.1f"),
                    'F-Score': st.column_config.NumberColumn(format="%d"),
                    '體質分': st.column_config.NumberColumn(format="%.0f"),
                    '趨勢分數': st.column_config.NumberColumn(format="%.1f"),
                    '觸發分數': st.column_config.NumberColumn(format="%+.1f"),
                    '漲跌%': st.column_config.NumberColumn(format="%.1f%%"),
                    '收盤': st.column_config.NumberColumn(format="%.1f"),
                    '推薦停損': st.column_config.NumberColumn(format="%.1f"),
                    'R:R': st.column_config.NumberColumn(format="%.2f"),
                },
            )

            st.caption("綜合 = F-Score 50% + 體質分 30% + 趨勢分數 20%（選股排名用） · "
                       "觸發分數為日線擇時指標（決定今天該下手哪檔，不影響選股）")

            # 操作建議
            with st.expander("個股操作建議"):
                _qm_selected = st.selectbox(
                    "選擇股票",
                    options=[f"{r['stock_id']} {r.get('name', '')}" for r in qm_results],
                    key='qm_detail_select',
                )
                if _qm_selected:
                    _qm_sid = _qm_selected.split()[0]
                    _qm_match = next((r for r in qm_results if r['stock_id'] == _qm_sid), None)
                    if _qm_match:
                        _ap = _qm_match.get('action_plan', {})

                        st.markdown(f"### {_qm_sid} {_qm_match.get('name', '')}")
                        _cs = _qm_match.get('composite_score')
                        _fs = _qm_match.get('qm_f_score')
                        _bs = _qm_match.get('qm_body_score')
                        _qs = _qm_match.get('qm_quality_score')
                        st.markdown(f"**綜合: {_cs}** / "
                                    f"F-Score: {_fs if _fs is not None else 'N/A'} / "
                                    f"體質: {round(_bs, 0) if _bs is not None else 'N/A'} / "
                                    f"趨勢: {_qm_match['trend_score']:+.1f} / "
                                    f"品質總分: {_qs if _qs is not None else 'N/A'}")

                        if _ap.get('strategy'):
                            st.markdown(f"\n{_ap['strategy']}")

                        _el = _ap.get('rec_entry_low')
                        _eh = _ap.get('rec_entry_high')
                        if _el and _eh:
                            st.markdown(f"- 進場區間: **{_el:.1f} ~ {_eh:.1f}** ({_ap.get('rec_entry_desc', '')})")

                        # QM 風險報酬摘要
                        _qm_sl = _ap.get('rec_sl_price')
                        _qm_tp = _ap.get('rec_tp_price')
                        _qm_rr = _ap.get('rr_ratio')
                        if _qm_sl and _qm_tp:
                            _c1, _c2, _c3 = st.columns(3)
                            _c1.metric("推薦停損", f"{_qm_sl:.2f}", _ap.get('rec_sl_method', ''))
                            _c2.metric("首要停利 (+15%)", f"{_qm_tp:.2f}", "TP1 減碼 1/3")
                            if _qm_rr:
                                _c3.metric("風報比 R:R", f"{_qm_rr:.2f}", "TP1 vs 停損")

                        # QM 分批進場（A#2：依 trigger_score 色燈顯示）
                        _qm_batches = _ap.get('qm_entry_batches')
                        _qm_gate = _ap.get('qm_entry_gate') or {}
                        _qm_gate_level = _qm_gate.get('level', 'unknown')
                        if _qm_batches:
                            if _qm_gate_level == 'green':
                                st.success(f"📥 **分批進場**: {_qm_batches}")
                            elif _qm_gate_level == 'yellow':
                                st.warning(f"📥 **分批進場**: {_qm_batches}")
                            elif _qm_gate_level == 'red':
                                st.error(f"📥 **分批進場**: {_qm_batches}")
                            else:
                                st.info(f"📥 **分批進場**: {_qm_batches}")

                        # QM 動態倉位建議（A#3：composite × trigger）
                        _qm_size = _ap.get('qm_position_size')
                        if _qm_size:
                            _qm_pct = _qm_size.get('recommended_pct', 0)
                            _qm_base = _qm_size.get('base_pct', 0)
                            _qm_mult = _qm_size.get('multiplier', 1.0)
                            _sc1, _sc2, _sc3 = st.columns(3)
                            _sc1.metric("建議倉位", f"{_qm_pct:.1f}%",
                                        f"×{_qm_mult:.2f} 擇時調整")
                            _sc2.metric("基礎倉位", f"{_qm_base:.1f}%",
                                        "依綜合評分 / 80")
                            _sc3.metric("擇時係數", f"×{_qm_mult:.2f}",
                                        "clip(trigger/5, 0.5, 1.5)")
                            st.caption(f"💰 {_qm_size.get('rationale', '')}")

                        # QM 三段停利
                        if _ap.get('tp_list'):
                            st.markdown("**停利階梯**")
                            for _tp in _ap['tp_list']:
                                _mark = " ← 推薦" if _tp.get('is_rec') else ""
                                st.markdown(f"- {_tp['method']}: {_tp['price']:.1f} ({_tp.get('desc', '')}){_mark}")

                        if _ap.get('sl_list'):
                            st.markdown("**停損參考價位**")
                            for _sl in _ap['sl_list']:
                                _p = _sl.get('price', 0)
                                _loss = _sl.get('loss')
                                _loss_txt = f" ({_loss:+.1f}%)" if _loss is not None else ""
                                st.markdown(f"- {_sl['method']}: {_p:.1f}{_loss_txt}")

                        # QM 出場訊號
                        _qm_exits = _ap.get('qm_exit_signals', [])
                        if _qm_exits:
                            st.markdown("**出場訊號 (任一觸發即全出)**")
                            for _e in _qm_exits:
                                st.markdown(f"- 🚨 {_e}")

                        _q_details = _qm_match.get('qm_quality_details', [])
                        if _q_details:
                            with st.expander("品質評分明細", expanded=False):
                                for d in _q_details:
                                    st.markdown(f"- {d}")

                        with st.expander("技術評分明細", expanded=False):
                            for d in _qm_match.get('trigger_details', []):
                                st.markdown(f"- {d}")
        else:
            st.info("尚無品質選股掃描結果。\n\n"
                    "在命令列執行 `python scanner_job.py --mode qm` 進行品質選股掃描\n"
                    "（動能選股 + 品質門檻，過濾虧損/高負債/營收崩的股票）")

        st.caption("💡 Full scan: `python scanner_job.py --mode qm`")

    # ====================================================================
    # Tab Convergence: 多策略共振 (hidden)
    # ====================================================================
    if False:  # hidden tab
        st.markdown("### 🔀 多策略共振")

        with st.expander("📋 共振偵測說明"):
            st.markdown("""
**同時出現在多個掃描模式的股票 = 多策略共振**

所有模式（動能/波段/品質選股/價值）各自獨立掃描後，系統自動交叉比對，找出重疊的股票。

**共振等級**

| Tier | 條件 | 意義 |
|------|------|------|
| **T1** | 動能類 + 價值 | 技術面強勢 + 基本面便宜 = 最高信號 |
| **T2** | 純動能交叉 | 多個技術模式認同，但缺基本面驗證 |

**為什麼共振重要？**
- 單一模式可能有偏差（動能追高、價值陷阱）
- 多策略同時選中 = 不同角度的共識
- 共振本身是稀缺事件（通常 0~5 支），每一支都值得關注
""")

        # TW convergence
        _conv_tw_file = _Path('data/latest/convergence_result.json')
        _conv_tw = None
        if _conv_tw_file.exists():
            try:
                with open(_conv_tw_file, 'r', encoding='utf-8') as _f:
                    _conv_tw = _json.load(_f)
            except Exception:
                _conv_tw = None

        # US convergence
        _conv_us_file = _Path('data/latest/convergence_us_result.json')
        _conv_us = None
        if _conv_us_file.exists():
            try:
                with open(_conv_us_file, 'r', encoding='utf-8') as _f:
                    _conv_us = _json.load(_f)
            except Exception:
                _conv_us = None

        _has_any = ((_conv_tw and _conv_tw.get('results'))
                    or (_conv_us and _conv_us.get('results')))

        if _has_any:
            for _conv_label, _conv_data in [('台股', _conv_tw), ('美股', _conv_us)]:
                if not _conv_data or not _conv_data.get('results'):
                    continue
                _cr = _conv_data['results']
                st.markdown(f"#### {_conv_label} ({len(_cr)} 支共振)")
                st.caption(f"偵測日期: {_conv_data.get('scan_date', '?')} {_conv_data.get('scan_time', '')}")

                _conv_rows = []
                for r in _cr:
                    _modes_str = ' + '.join(r.get('modes', []))
                    _conv_rows.append({
                        '代號': r['stock_id'],
                        '名稱': r.get('name', ''),
                        '收盤': r.get('price', 0),
                        '漲跌%': r.get('change_pct', 0),
                        'Tier': f"T{r.get('convergence_tier', '?')}",
                        '模式': _modes_str,
                        '模式數': r.get('mode_count', 0),
                        '觸發分數': r.get('trigger_score'),
                        '趨勢分數': r.get('trend_score'),
                        '價值分數': r.get('value_score'),
                        'PE': r.get('PE'),
                        '殖利率%': r.get('dividend_yield'),
                        '訊號': ', '.join(r.get('signals', [])[:3]),
                    })
                _df_conv = pd.DataFrame(_conv_rows)
                _df_conv.index = range(1, len(_df_conv) + 1)

                st.dataframe(
                    _df_conv,
                    width='stretch',
                    column_config={
                        '觸發分數': st.column_config.NumberColumn(format="%.1f"),
                        '趨勢分數': st.column_config.NumberColumn(format="%.1f"),
                        '價值分數': st.column_config.NumberColumn(format="%.1f"),
                        '漲跌%': st.column_config.NumberColumn(format="%.1f%%"),
                        '收盤': st.column_config.NumberColumn(format="%.1f"),
                        'PE': st.column_config.NumberColumn(format="%.1f"),
                        '殖利率%': st.column_config.NumberColumn(format="%.1f%%"),
                    },
                )

                # 個股細節
                for r in _cr:
                    ranks = r.get('mode_ranks', {})
                    ranks_str = ', '.join(f"{m} #{rk}" for m, rk in ranks.items())
                    vs = r.get('value_scores', {})
                    vs_str = ' / '.join(f"{k}={v:.0f}" for k, v in vs.items()) if vs else ''
                    with st.expander(f"{r['stock_id']} {r.get('name', '')} — T{r.get('convergence_tier', '?')} [{' + '.join(r.get('modes', []))}]"):
                        st.markdown(f"**模式排名**: {ranks_str}")
                        if r.get('trigger_score') is not None:
                            st.markdown(f"**觸發分數**: {r['trigger_score']:+.1f} / 趨勢: {r.get('trend_score', 0):+.1f}")
                        if r.get('value_score') is not None:
                            st.markdown(f"**價值分數**: {r['value_score']:.1f} ({vs_str})")
                        if r.get('signals'):
                            st.markdown(f"**訊號**: {', '.join(r['signals'])}")
        else:
            st.info("尚無共振結果。\n\n"
                    "共振偵測在所有 scanner 模式跑完後自動執行。\n"
                    "執行 `python scanner_job.py --mode all` 後會自動產出共振結果。\n\n"
                    "共振本身是稀缺事件，結果為 0 是正常的。")

        st.caption("💡 共振偵測自動執行於 `--mode all` / `--mode both` 掃描後")

    # ====================================================================
    # Tab 2: 左側價值選股 (paused pending Phase 1 enhancement)
    # ====================================================================
    if False:  # Value tab hidden — restore by changing to: with screener_tab2:
     with st.container():

        with st.expander("📋 篩選條件說明"):
            st.markdown("""
**Stage 1 初篩**

| 條件 | 門檻 | 說明 |
|------|------|------|
| PE (本益比) | 0.1 ~ 30 | 排除虧損股 (PE<0) 和高估值股 |
| PB (股價淨值比) | ≤ 5.0 | 排除資產泡沫股 |
| 成交值 | > 500 萬 | 過濾極低流動性 |

**Stage 2 綜合評分（0-100 分）**

| 面向 | 權重 | 評分項目 | 加分/扣分規則 |
|------|------|----------|---------------|
| **估值** | 30% | PE/PB 高低、歷史分位、殖利率、PEG、DDM 折價 | PE<8 +25, PB<1 +15, 殖利率>6% +10, PEG<0.5 +12 |
| **體質** | 25% | Piotroski F-Score、Altman Z-Score、ROIC、FCF Yield | F≥7/9 +25, Z-Score 安全 +8, ROIC>15% +8 |
| **營收** | 15% | 月營收 YoY 趨勢、營收驚喜 | YoY轉正 +10, 衰退收斂 +改善幅度×2, 驚喜 +12 |
| **技術轉折** | 15% | RSI 超賣、量能萎縮、BB 壓縮、距 52 週低點 | RSI<30 +20, RVOL<0.5 +15, 近低點10% +15 |
| **聰明錢** | 15% | ETF 同步買超、法人累積 | ETF≥3檔買超 +20, 法人5日淨買 +10 |

**體質指標說明**

| 指標 | 說明 |
|------|------|
| **Piotroski F-Score** | 9 項財務健康指標（獲利/槓桿/效率），7 分以上為強健 |
| **Altman Z-Score** | 破產風險指標，>2.99 安全，<1.81 有風險 |
| **ROIC** | 投入資本報酬率，衡量公司用資本賺錢的效率 |
| **FCF Yield** | 自由現金流殖利率，衡量實際產生的現金回報 |
| **PEG** | PE / 盈餘成長率，<1 表示成長相對估值便宜 |
| **DDM** | 股利折現模型，估算合理股價與目前折溢價 |
""")

        value_file = _Path('data/latest/value_result.json')
        value_result = None
        if value_file.exists():
            try:
                with open(value_file, 'r', encoding='utf-8') as _f:
                    value_result = _json.load(_f)
            except Exception:
                value_result = None

        if value_result and value_result.get('results'):
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
                    '代號': r['stock_id'],
                    '名稱': r.get('name', ''),
                    '綜合分數': r.get('value_score', 0),
                    '收盤': r.get('price', 0),
                    'PE': r.get('PE', 0),
                    'PB': r.get('PB', 0),
                    '殖利率%': r.get('dividend_yield', 0),
                    '均量(億)': round(r.get('avg_trading_value_5d', 0) / 1e8, 2),
                    '估值': s.get('valuation', 0),
                    '體質': s.get('quality', 0),
                    '營收': s.get('revenue', 0),
                    '技術轉折': s.get('technical', 0),
                    '聰明錢': s.get('smart_money', 0),
                })
            _v_df_results = pd.DataFrame(_v_rows)

            _sort_opts_v = {
                '綜合分數 (高→低)': ('綜合分數', False),
                '均量(億) (高→低)': ('均量(億)', False),
                '殖利率% (高→低)': ('殖利率%', False),
                'PE (低→高)': ('PE', True),
            }
            _v_sort = st.selectbox("排序方式", list(_sort_opts_v.keys()), key='value_tw_sort')
            _v_sc, _v_sa = _sort_opts_v[_v_sort]
            _v_df_results = _v_df_results.sort_values(_v_sc, ascending=_v_sa).reset_index(drop=True)
            _v_df_results.index = range(1, len(_v_df_results) + 1)

            st.dataframe(
                _v_df_results,
                width='stretch',
                height=600,
                column_config={
                    '綜合分數': st.column_config.NumberColumn(format="%.1f"),
                    'PE': st.column_config.NumberColumn(format="%.1f"),
                    'PB': st.column_config.NumberColumn(format="%.2f"),
                    '殖利率%': st.column_config.NumberColumn(format="%.1f%%"),
                    '收盤': st.column_config.NumberColumn(format="%.1f"),
                    '均量(億)': st.column_config.NumberColumn(format="%.2f"),
                },
            )
            st.caption("綜合分數 0~100 (估值+體質+營收+技術轉折+聰明錢加總)")

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
                    "在命令列執行 `python scanner_job.py --mode value` 進行完整掃描\n"
                    "（含 5 維評分，約需 20-40 分鐘）")

    # ====================================================================
    # Tab US Value: 美股價值選股 (paused pending Phase 1 enhancement)
    # ====================================================================
    if False:  # Value tab hidden — restore by changing to: with screener_tab_us_val:
     with st.container():

        with st.expander("📋 Screening Criteria"):
            st.markdown("""
**Stage 1 Initial Filter**

| Criteria | Threshold | Description |
|----------|-----------|-------------|
| Universe | S&P 500 | 掃描範圍 |
| Min Price | > $5.00 | 排除低價股 |
| Min Volume | > 500,000 | 過濾低流動性 |

**Stage 2 Scoring (0-100)**

| Dimension | Weight | Metrics | Scoring Examples |
|-----------|--------|---------|------------------|
| **Valuation** | 30% | PE/PB, Forward PE, Finviz PEG, DDM, Analyst Target | PEG<0.5 +12, Target>30% +10 |
| **Quality** | 25% | F-Score, Z-Score, ROIC, FCF Yield | F≥7/9 +25, ROIC>15% +8 |
| **Revenue** | 15% | Sales Q/Q, EPS Q/Q, Revenue YoY trend | Sales Q/Q>20% +15, EPS Q/Q>25% +10 |
| **Technical** | 15% | RSI oversold, Volume dry-up, BB squeeze, 52W low | RSI<30 +20, Near 52W low +15 |
| **Smart Money** | 15% | Institutional %, Short interest, Insider activity | Inst>80% +10, Insider bullish +12, Short>10% -10 |

**Key Metrics**

| Metric | Description |
|--------|-------------|
| **F-Score** | Piotroski 9-point financial health (≥7 = strong) |
| **Z-Score** | Altman bankruptcy risk (>2.99 safe, <1.81 distress) |
| **ROIC** | Return on invested capital |
| **FCF Yield** | Free cash flow yield |
| **PEG** | PE / Earnings growth, <1 = undervalued |
| **Forward PE** | PE based on estimated future earnings |
| **Short %** | Short interest as % of float, >10% = risky |
""")

        us_val_file = _Path('data/latest/value_us_result.json')
        us_val_result = None
        if us_val_file.exists():
            try:
                with open(us_val_file, 'r', encoding='utf-8') as _f:
                    us_val_result = _json.load(_f)
            except Exception:
                us_val_result = None

        if us_val_result and us_val_result.get('results'):
            uv_results = us_val_result['results']
            st.caption(
                f"Scan: {us_val_result.get('scan_date', '?')} {us_val_result.get('scan_time', '')} | "
                f"Scored: {us_val_result.get('scored_count', 0)} | "
                f"Time: {us_val_result.get('elapsed_seconds', 0):.0f}s"
            )
            _uv_rows = []
            for r in uv_results:
                s = r.get('scores', {})
                _uv_rows.append({
                    'Ticker': r['stock_id'],
                    'Score': r.get('value_score', 0),
                    'Price': r.get('price', 0),
                    'PE': r.get('PE', 0),
                    'PB': r.get('PB', 0),
                    'DY%': r.get('dividend_yield', 0),
                    'TV(M)': round(r.get('avg_trading_value_5d', 0) / 1e6, 1),
                    'Val': s.get('valuation', 0),
                    'Qual': s.get('quality', 0),
                    'Tech': s.get('technical', 0),
                    'Smart$': s.get('smart_money', 0),
                })
            _uv_df = pd.DataFrame(_uv_rows)

            _sort_opts_uv = {
                'Score (High→Low)': ('Score', False),
                'TV(M) (High→Low)': ('TV(M)', False),
                'DY% (High→Low)': ('DY%', False),
                'PE (Low→High)': ('PE', True),
            }
            _uv_sort = st.selectbox("Sort by", list(_sort_opts_uv.keys()), key='value_us_sort')
            _uv_sc, _uv_sa = _sort_opts_uv[_uv_sort]
            _uv_df = _uv_df.sort_values(_uv_sc, ascending=_uv_sa).reset_index(drop=True)
            _uv_df.index = range(1, len(_uv_df) + 1)

            st.dataframe(
                _uv_df,
                width='stretch', height=600,
                column_config={
                    'Score': st.column_config.NumberColumn(format="%.1f"),
                    'Price': st.column_config.NumberColumn(format="$%.2f"),
                    'PE': st.column_config.NumberColumn(format="%.1f"),
                    'TV(M)': st.column_config.NumberColumn(format="%.1f"),
                },
            )
            st.caption("Score 0~100 (valuation + quality + revenue + technical + smart money)")
            with st.expander("Detailed Scores"):
                _uv_sel = st.selectbox("Select", [r['stock_id'] for r in uv_results], key='us_val_detail')
                if _uv_sel:
                    _uv_m = next((r for r in uv_results if r['stock_id'] == _uv_sel), None)
                    if _uv_m:
                        for d in _uv_m.get('details', []):
                            st.markdown(f"- {d}")
        else:
            st.info("No US value scan results yet.\n\n"
                    "Run: `python scanner_job.py --mode value --market us`")

        st.caption("💡 Full scan: `python scanner_job.py --mode value --market us`")

    # ====================================================================
    # Tab: 短線均值回歸 (P3)
    # ====================================================================
    with screener_tab_meanrev:

        st.markdown("""
**短線均值回歸掃描** — 找出超賣/超買股票，供 **1-3 天**短線操作參考。

獨立於 Scanner 動能策略，用 5 個高度相關的均值回歸指標（MA20偏離/VWAP偏離/BB%B/RSI偏離/EFI）合成單一 MeanRev Composite。
MeanRev 越負 = 越超賣（買入候選），越正 = 越超買（避開）。

| 驗證項目 | 數據 |
|----------|------|
| IC (1d) | +0.060 (75.5% 勝率) |
| IC (5d) | +0.055 (73.3% 勝率) |
| Walk-forward 1d | IS +1.67 → OOS +0.89 (**-47% 衰退**，短線最不穩定) |
| Walk-forward 5-20d | OOS > IS (穩健) |

> **注意**: 此策略 1d horizon 在 out-of-sample 有顯著衰退。建議持倉 **2-5 天**而非隔日沖，
> 並搭配 RSI < 30 + BIAS < -5% 雙重確認再進場。10 天後信號衰退，不適合長抱。
""")
        with st.expander("MeanRev Composite 用在哪些地方"):
            st.markdown("""
MeanRev Composite 是 5 個高度相關指標（corr 0.78-0.93）的 252 日 z-score 均值：

| 用途 | 模組 | 說明 |
|------|------|------|
| **動能 Scanner T1 信號** | `analysis_engine.py` | 取代原本的 binary MA20 → tanh(MeanRev) 連續值 [-1,+1]，讓趨勢組中位數更平滑 |
| **本 Tab 超賣/超買掃描** | `tools/meanrev_scanner.py` | 排序 MeanRev 最負（超賣）/ 最正（超買）的股票 |
| **個股分析技術圖表** | `technical_analysis.py` | 計算並存入 DataFrame，供 AI 報告參考 |

不直接影響：價值選股、籌碼面評分、週線趨勢分數。
""")

        _mr_top_n = st.slider("顯示前 N 檔", 5, 50, 20, key='mr_top_n')
        _mr_source = st.radio(
            "掃描範圍",
            ["最近 Scanner Picks (快速)", "所有快取股票 (完整)"],
            key='mr_source', horizontal=True
        )

        if st.button("開始掃描", key='mr_scan_btn'):
            with st.spinner("掃描中..."):
                from tools.meanrev_scanner import get_stock_ids, scan
                import types
                _mr_args = types.SimpleNamespace(
                    stocks=None,
                    all=(_mr_source != "最近 Scanner Picks (快速)"),
                )
                _mr_ids = get_stock_ids(_mr_args)
                if not _mr_ids:
                    st.warning("無可掃描股票。請先執行 Scanner 或使用「所有快取股票」。")
                else:
                    _mr_results = scan(_mr_ids, _mr_top_n)
                    _mr_tw = [r for r in _mr_results if r['market'] == 'tw']
                    _mr_us = [r for r in _mr_results if r['market'] == 'us']
                    st.success(f"掃描完成: {len(_mr_results)} 檔 (台股 {len(_mr_tw)} / 美股 {len(_mr_us)})")

                    def _mr_table(data, top_n, is_tw=True):
                        """Build DataFrame for display."""
                        if not data:
                            st.info("無資料")
                            return
                        df = pd.DataFrame(data)
                        df.index = range(1, len(df) + 1)
                        if is_tw:
                            df = df[['stock_id', 'name', 'close', 'meanrev', 'rsi', 'bias']]
                            df.columns = ['代號', '名稱', '收盤', 'MeanRev', 'RSI', 'BIAS%']
                        else:
                            df = df[['stock_id', 'close', 'meanrev', 'rsi', 'bias']]
                            df.columns = ['Ticker', 'Price', 'MeanRev', 'RSI', 'BIAS%']
                        st.dataframe(df, use_container_width=True, column_config={
                            'MeanRev': st.column_config.NumberColumn(format="%+.3f"),
                            'RSI': st.column_config.NumberColumn(format="%.0f"),
                            'BIAS%': st.column_config.NumberColumn(format="%+.1f"),
                        })

                    # === 台股 ===
                    if _mr_tw:
                        st.markdown("### 🇹🇼 台股")
                        _c1, _c2 = st.columns(2)
                        with _c1:
                            st.markdown(f"**📉 超賣 Top {_mr_top_n}**")
                            _mr_table(_mr_tw[:_mr_top_n], _mr_top_n, is_tw=True)
                        with _c2:
                            st.markdown(f"**📈 超買 Top {_mr_top_n}**")
                            _mr_table(list(reversed(_mr_tw[-_mr_top_n:])), _mr_top_n, is_tw=True)

                    # === 美股 ===
                    if _mr_us:
                        st.markdown("### 🇺🇸 美股")
                        _c3, _c4 = st.columns(2)
                        with _c3:
                            st.markdown(f"**📉 Oversold Top {_mr_top_n}**")
                            _mr_table(_mr_us[:_mr_top_n], _mr_top_n, is_tw=False)
                        with _c4:
                            st.markdown(f"**📈 Overbought Top {_mr_top_n}**")
                            _mr_table(list(reversed(_mr_us[-_mr_top_n:])), _mr_top_n, is_tw=False)

        st.caption("💡 CLI: `python tools/meanrev_scanner.py --top 20`")

    # ====================================================================
    # Tab: 績效追蹤
    # ====================================================================
    with screener_tab_track:

        st.markdown("""
**品質選股績效追蹤** — 追蹤品質選股 (QM) 選出的股票在 5 / 10 / 20 / 40 / 60 個交易日後的表現。
每次掃描後自動更新，資料越多越有參考價值。
""")

        try:
            from scan_tracker import ScanTracker
            _tracker = ScanTracker()
            _track_data = _tracker.load_latest()
            _summary = _track_data.get('summary', {})
            _updated = _track_data.get('updated_at', '')

            if _summary:
                if _updated:
                    st.caption(f"最後更新: {_updated[:19]}")

                _type_labels = {
                    'qm': '品質選股', 'momentum': '動能', 'value': '價值',
                    'swing': '波段', 'convergence': '共振',
                }
                for _tk, _ts in _summary.items():
                    # Only show QM tracks
                    if _ts.get('scan_type') != 'qm':
                        continue
                    _type_label = _type_labels.get(_ts['scan_type'], _ts['scan_type'])
                    _mkt_label = '台股' if _ts['market'] == 'tw' else '美股'
                    st.markdown(f"#### {_type_label} ({_mkt_label})")
                    st.caption(f"掃描次數: {_ts['total_scans']} | 總選股: {_ts['total_picks']}")

                    _perf_rows = []
                    for _d in [5, 10, 20, 40, 60]:
                        _tracked = _ts.get(f'tracked_{_d}d', 0)
                        if _tracked > 0:
                            _perf_rows.append({
                                '追蹤天數': f'{_d}d',
                                '追蹤檔數': _tracked,
                                '勝率': f"{_ts.get(f'win_rate_{_d}d', 0):.1f}%",
                                '平均報酬': f"{_ts.get(f'avg_return_{_d}d', 0):+.2f}%",
                                '中位數': f"{_ts.get(f'median_return_{_d}d', 0):+.2f}%",
                                '最佳': f"{_ts.get(f'best_{_d}d', 0):+.2f}%",
                                '最差': f"{_ts.get(f'worst_{_d}d', 0):+.2f}%",
                            })
                        else:
                            _perf_rows.append({
                                '追蹤天數': f'{_d}d',
                                '追蹤檔數': 0,
                                '勝率': '—',
                                '平均報酬': '—',
                                '中位數': '—',
                                '最佳': '—',
                                '最差': '—',
                            })

                    if _perf_rows:
                        st.dataframe(pd.DataFrame(_perf_rows), width='stretch', hide_index=True)

                    # Benchmark IR (BM-b)
                    _bm_data = _ts.get('benchmarks', {})
                    if _bm_data:
                        from scan_tracker import _bm_display_name
                        _ir_rows = []
                        for _bm, _horizons in _bm_data.items():
                            _bm_label = _bm_display_name(_bm)
                            for _d in [5, 10, 20, 40, 60]:
                                _h = _horizons.get(f'{_d}d')
                                if _h:
                                    _ir_rows.append({
                                        'Benchmark': _bm_label,
                                        'Horizon': f'{_d}d',
                                        'N': _h['n'],
                                        'Excess': f"{_h['avg_excess']:+.2f}%",
                                        'TE': f"{_h['tracking_error']:.2f}%",
                                        'IR': f"{_h['ir']:+.3f}",
                                        'Win vs BM': f"{_h['win_rate_vs_bm']:.1f}%",
                                    })
                        if _ir_rows:
                            st.markdown("**vs Benchmark (Information Ratio)**")
                            st.dataframe(pd.DataFrame(_ir_rows), width='stretch', hide_index=True)

                # Detailed picks table
                with st.expander("個股追蹤明細"):
                    _track_mkt = st.selectbox("市場", ['tw', 'us'], key='track_mkt_sel',
                                              format_func=lambda x: '台股' if x == 'tw' else '美股')
                    _picks_df = _tracker.get_picks_dataframe('qm', _track_mkt)
                    if not _picks_df.empty:
                        _show_cols = ['scan_date', 'stock_id', 'name', 'price_at_scan']
                        if 'trigger_score' in _picks_df.columns:
                            _show_cols.append('trigger_score')
                        for _d in [5, 10, 20, 40, 60]:
                            col = f'return_{_d}d'
                            if col in _picks_df.columns:
                                _show_cols.append(col)
                        _show_cols = [c for c in _show_cols if c in _picks_df.columns]
                        st.dataframe(_picks_df[_show_cols], width='stretch', height=400)
                    else:
                        st.info("尚無品質選股追蹤資料")

            else:
                st.info("尚無績效追蹤資料。\n\n"
                        "Scanner 每次執行後會自動追蹤歷史選股表現。\n"
                        "需要累積至少 5 個交易日的掃描歷史才會出現數據。\n\n"
                        "手動更新: `python scan_tracker.py`")

        except Exception as _track_err:
            st.warning(f"追蹤模組載入失敗: {_track_err}")

    st.markdown("---")
    st.caption("💡 品質選股掃描: `python scanner_job.py --mode qm` | 價值掃描: `python scanner_job.py --mode value`")

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
                for _msg in _job.get('progress', []):
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

        if st.button("生成研究報告", type="primary", key='ai_gen_btn', disabled=_is_running):
            if not _ai_ticker or not _ai_ticker.strip():
                st.error("請輸入股票代號")
            else:
                _ai_ticker = _ai_ticker.strip().upper()
                is_valid, err_msg = validate_ticker(_ai_ticker)
                if not is_valid:
                    st.error(f"代號格式不正確: {err_msg}")
                else:
                    # 啟動背景 job
                    _new_job = {
                        'ticker': _ai_ticker,
                        'status': 'running',
                        'start_time': time.time(),
                        'progress': [],
                        'result': None,
                        'format': _ai_format,
                    }
                    st.session_state['ai_report_job'] = _new_job
                    _t = threading.Thread(
                        target=_ai_report_worker,
                        args=(_new_job, _ai_ticker, _ai_format),
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

                        st.caption("💡 iframe 預覽可能受沙盒限制；如排版不完整請點「在瀏覽器開啟」查看完整儀表板")
                        # 內嵌 iframe 預覽（React 需要 JS 執行，components.html 會放在 iframe 內可用）
                        import streamlit.components.v1 as _components
                        _components.html(_sel_content, height=1400, scrolling=True)
                    else:
                        # 報告含 <span style="color:..."> 顏色標記，需允許 HTML 才能正確渲染
                        st.markdown(_sel_content, unsafe_allow_html=True)

                    st.markdown("---")
                    st.caption("此報告由 Claude AI 基於系統數據自動生成，僅供參考，不構成投資建議。")

                    if st.button("🗑️ 刪除此報告", key='report_delete_btn'):
                        _delete_report(_sel_id)
                        st.success("報告已刪除")
                        st.rerun()

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
    status_text = st.empty()

    # ==========================================
    # [NEW] 快取檢查：切換 app_mode 返回時直接復用
    # 同 ticker + 非強制更新 → 跳過所有 load，避免 UI 閃爍
    # ==========================================
    _ind_cache = st.session_state.get('_individual_cache')
    _ind_cache_hit = (
        _ind_cache is not None
        and _ind_cache.get('ticker') == source
        and not is_force
    )

    try:
        if _ind_cache_hit:
            # Silent reuse
            figures = _ind_cache['figures']
            errors = _ind_cache['errors']
            df_week = _ind_cache['df_week']
            df_day = _ind_cache['df_day']
            stock_meta = _ind_cache['stock_meta']
            chip_data = _ind_cache.get('chip_data')
            fund_data = _ind_cache.get('fund_data')
            # Sync 到原有 session_state keys（其他區塊會讀）
            st.session_state['df_week_cache'] = df_week
            st.session_state['df_day_cache'] = df_day
            st.session_state['force_update_cache'] = is_force
            st.session_state['fund_cache'] = fund_data
            status_text.caption(f"✅ 已復用 {display_ticker} 的分析結果（切換頁面快速返回）")
        else:
            action_text = "強制下載" if is_force else "分析"
            status_text.info(f"⏳ 正在{action_text} {display_ticker} ...")

            # 1. 價量 + 指標 + 圖表
            figures, errors, df_week, df_day, stock_meta = run_analysis(source, force_update=is_force)

            # Display analysis warnings from errors dict
            for key, err_msg in errors.items():
                if err_msg:
                    st.warning(f"⚠️ {key} 計算警告: {err_msg}")

            # 2. 台股籌碼
            chip_data = None
            if source and isinstance(source, str) and ("TW" in source or source.isdigit()):
                try:
                    status_text.info(f"⏳ 正在分析 {display_ticker} (技術+籌碼)...")
                    chip_data, chip_err = get_chip_data_cached(source, is_force)
                except Exception as e:
                    logger.error(f"Chip Load Error: {e}", exc_info=True)
                    st.warning(f"⚠️ 籌碼預載失敗: {e}")

            # 3. 基本面
            fund_data = None
            if source and isinstance(source, str):
                with st.spinner("📋 載入基本面資料..."):
                    try:
                        fund_data = get_fundamentals(display_ticker)
                    except Exception as e:
                        logger.error(f"Fundamental Load Error: {e}", exc_info=True)

            # Sync 到原有 session_state keys
            st.session_state['df_week_cache'] = df_week
            st.session_state['df_day_cache'] = df_day
            st.session_state['force_update_cache'] = is_force
            st.session_state['fund_cache'] = fund_data

            # 4. 存快取供下次 rerun 直接復用
            st.session_state['_individual_cache'] = {
                'ticker': source,
                'figures': figures,
                'errors': errors,
                'df_week': df_week,
                'df_day': df_day,
                'stock_meta': stock_meta,
                'chip_data': chip_data,
                'fund_data': fund_data,
            }

            status_text.success("✅ 分析完成！")

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

            # Cache report in session_state to avoid re-running on every rerun
            # (prevents widget tree shifts that reset tab selection)
            _report_cache_key = f"_report_{display_ticker}"
            if _report_cache_key not in st.session_state or is_force:
                with st.spinner("🤖 AI 分析中..."):
                    analyzer = TechnicalAnalyzer(
                        display_ticker,
                        st.session_state['df_week_cache'],
                        st.session_state['df_day_cache'],
                        strategy_params,
                        chip_data=chip_data,
                        us_chip_data=us_chip_data
                    )
                    st.session_state[_report_cache_key] = analyzer.run_analysis()
            report = st.session_state[_report_cache_key]
            
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
        tab1, tab2, tab3, tab4, tab6 = st.tabs(
            ["週K", "日K", "籌碼面", "🏢 基本面", "📊 除息/營收"])
        
        with tab1:
            if 'Weekly' in figures:
                st.plotly_chart(figures['Weekly'], width='stretch')
            else:
                st.warning("⚠️ 無法產生週線圖表 (請查看上方錯誤訊息)")

        with tab2:
            if 'Daily' in figures:
                st.plotly_chart(figures['Daily'], width='stretch')
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
                        st.plotly_chart(fig_vp, width='stretch')
                        
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

                         # [NEW] SBL (借券賣出) — 法人放空管道
                         df_sbl = chip_data.get('sbl', pd.DataFrame())
                         if not df_sbl.empty and '借券賣出餘額' in df_sbl.columns:
                             try:
                                 latest_sbl = df_sbl.iloc[-1]
                                 bal_sbl = latest_sbl.get('借券賣出餘額', 0) / 1000  # 股 -> 張
                                 sold_today = latest_sbl.get('借券賣出', 0) / 1000

                                 # 5 日累計
                                 recent5 = df_sbl.iloc[-5:] if len(df_sbl) >= 5 else df_sbl
                                 net5d = (recent5['借券賣出'].sum() - recent5['借券還券'].sum()) / 1000

                                 # 趨勢判斷：餘額 vs 30 日平均
                                 if len(df_sbl) >= 30:
                                     ma30_bal = df_sbl['借券賣出餘額'].iloc[-30:].mean() / 1000
                                     trend_pct = (bal_sbl / ma30_bal - 1) * 100 if ma30_bal > 0 else 0
                                 else:
                                     trend_pct = 0

                                 st.markdown("#### 🏦 借券賣出 (法人放空)")
                                 c_s1, c_s2, c_s3 = st.columns(3)
                                 c_s1.metric("借券餘額", f"{bal_sbl:,.0f} 張")
                                 c_s2.metric("當日新借", f"{sold_today:,.0f} 張")

                                 if net5d > 0:
                                     net_label = f"⚠️ 法人加空 (+{net5d:,.0f})"
                                     net_color = "inverse"
                                 elif net5d < 0:
                                     net_label = f"✅ 法人回補 ({net5d:,.0f})"
                                     net_color = "normal"
                                 else:
                                     net_label = "持平"
                                     net_color = "off"
                                 c_s3.metric("5日淨增", f"{net5d:+,.0f} 張", delta=net_label, delta_color=net_color)

                                 if abs(trend_pct) > 1:
                                     trend_emoji = "📈" if trend_pct > 0 else "📉"
                                     st.caption(f"{trend_emoji} 借券餘額相對近 30 日均值 {trend_pct:+.1f}%")
                             except Exception as e:
                                 st.caption(f"借券數據計算異常: {e}")

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
                                     st.plotly_chart(fig_sh, width='stretch')
                             else:
                                 st.caption("⚠️ 尚無足夠的外資持股比率數據")

                         
                         # 1. 整合圖表：三大法人 + 融資融券 (Plotly Dual Subplot)
                         st.markdown("### 📊 籌碼綜合分析 (Institutional & Margin)")
                         
                         df_inst = chip_data.get('institutional', pd.DataFrame())
                         df_margin = chip_data.get('margin', pd.DataFrame())
                         df_sbl_chart = chip_data.get('sbl', pd.DataFrame())

                         # Data Slicing (Last 120 days for clear view)
                         days_show = 120
                         df_inst_plot = df_inst.iloc[-days_show:] if not df_inst.empty else pd.DataFrame()
                         df_margin_plot = df_margin.iloc[-days_show:] if not df_margin.empty else pd.DataFrame()
                         df_sbl_plot = df_sbl_chart.iloc[-days_show:] if not df_sbl_chart.empty else pd.DataFrame()

                         if not df_inst_plot.empty:
                             # Import Plotly
                             import plotly.graph_objects as go
                             from plotly.subplots import make_subplots

                             # Create Subplots: Row 1 = Investors, Row 2 = Margin, Row 3 = SBL
                             has_sbl = not df_sbl_plot.empty and '借券賣出餘額' in df_sbl_plot.columns
                             if has_sbl:
                                 fig_chip = make_subplots(
                                     rows=3, cols=1,
                                     shared_xaxes=True,
                                     vertical_spacing=0.04,
                                     subplot_titles=("三大法人買賣超 (張)", "融資融券餘額 (張)", "借券賣出餘額 (張)"),
                                     row_heights=[0.5, 0.25, 0.25]
                                 )
                             else:
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

                             # --- Row 3: SBL (借券賣出) ---
                             if has_sbl:
                                 sbl_bal_zhang = df_sbl_plot['借券賣出餘額'] / 1000
                                 fig_chip.add_trace(go.Scatter(
                                     x=df_sbl_plot.index, y=sbl_bal_zhang,
                                     name='借券餘額', mode='lines',
                                     line=dict(color='purple', width=2),
                                     fill='tozeroy', fillcolor='rgba(128,0,128,0.1)',
                                     hovertemplate="借券餘額: %{y:,.0f} 張<extra></extra>"
                                 ), row=3, col=1)

                                 # Daily new shorts (bar)
                                 if '借券賣出' in df_sbl_plot.columns:
                                     daily_short = df_sbl_plot['借券賣出'] / 1000
                                     fig_chip.add_trace(go.Bar(
                                         x=df_sbl_plot.index, y=daily_short,
                                         name='當日新借', marker_color='rgba(255,140,0,0.6)',
                                         yaxis='y4',
                                         hovertemplate="當日新借: %{y:,.0f} 張<extra></extra>"
                                     ), row=3, col=1)

                             # Layout
                             fig_chip.update_layout(
                                 height=750 if has_sbl else 600,
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
                             
                             st.plotly_chart(fig_chip, width='stretch')
                             
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
                                st.dataframe(top_holders, width='stretch')
                        
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
                                st.dataframe(recent_trades.head(10), width='stretch')
                        
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
                         st.plotly_chart(fig_rev, width='stretch')
                 
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
                     st.plotly_chart(fig_pe, width='stretch')

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
                         st.plotly_chart(fig_eps, width='stretch')
                         
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
                         st.plotly_chart(fig_margin, width='stretch')
             else:
                st.info("💡 歷史基本面圖表僅支援台股代號")

        # ==========================================
        # Tab 5: 除息/營收分析（原 Tab 6，情緒/期權已移至大盤儀表板）
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
                                st.dataframe(div_hist, width='stretch')

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
                                st.plotly_chart(fig_rev, width='stretch')
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

    except Exception as e:
        status_text.error(f"❌ 發生未預期錯誤: {e}")
        st.exception(e)

else:
    # 初始歡迎畫面
    st.info("👈 請在左測試欄輸入代號並點擊「開始分析」")

