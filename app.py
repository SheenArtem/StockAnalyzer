import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import mplfinance as mpf
from report_fetcher import get_latest_report
from technical_analysis import plot_dual_timeframe, load_and_resample, calculate_all_indicators, plot_interactive_chart
from fundamental_analysis import get_fundamentals, get_revenue_history, get_per_history, get_financial_statements


# 設定頁面配置
st.set_page_config(
    page_title="股票右側分析系統",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar
st.sidebar.title("🔧 設定 (Settings)")
# User provided Key
# DEFAULT_KEY removed.
# Input removed.

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
</style>
""", unsafe_allow_html=True)

# 標題
st.markdown('<div class="main-header">📈 股票右側分析系統</div>', unsafe_allow_html=True)

# 側邊欄
with st.sidebar:
    st.header("⚙️ 設定面板")
    st.caption("Version: v2026.01.21.01")
    
    input_method = st.radio("選擇輸入方式", ["股票代號 (Ticker)", "上傳 CSV 檔"])
    
    target_ticker = "2330" # 預設值
    uploaded_file = None
    
    # [NEW] Search History
    from cache_manager import CacheManager
    cm = CacheManager()
    cached_list = cm.list_cached_tickers()
    
    # 使用 Expander 管理歷史紀錄 (取代 Selectbox)
    with st.expander("🕒 歷史紀錄管理", expanded=False):
        if not cached_list:
            st.info("尚無歷史紀錄")
        else:
            for past_ticker in cached_list:
                c1, c2, c3 = st.columns([3, 2, 2])
                with c1:
                    st.write(f"**{past_ticker}**")
                
                with c2:
                    if st.button("載入", key=f"load_{past_ticker}"):
                        st.session_state['ticker_input'] = past_ticker
                        st.session_state['trigger_analysis'] = True # Trigger auto-run
                        st.rerun() # Rerun to update the input box immediately
                
                with c3:
                    if st.button("刪除", key=f"del_{past_ticker}"):
                        cm.delete_ticker_cache(past_ticker)
                        st.toast(f"🗑️ 已刪除 {past_ticker}", icon="🗑️")
                        st.rerun()

    if input_method == "股票代號 (Ticker)":
        # Initialize session state if not present
        if 'ticker_input' not in st.session_state:
            st.session_state['ticker_input'] = '2330'
            
        target_ticker = st.text_input("輸入股票代號 (台股請加 .TW)", 
                                      key='ticker_input', # Bind to session state
                                      help="例如: 2330, TSM, AAPL")
    else:
        uploaded_file = st.file_uploader("上傳股票 CSV", type=['csv'])

    col_run, col_force = st.columns([1, 1])
    with col_run:
        run_btn = st.button("🚀 開始分析", type="primary")
    with col_force:
        force_btn = st.button("🔄 強制重抓", help="忽略快取，重新下載最新資料")

    # Clear cache button (Moved to Expander or kept here? Kept here for global clear)
    if st.button("🧹 清除所有快取"):
        try:
             import shutil
             import os
             if os.path.exists("data_cache"):
                 shutil.rmtree("data_cache")
             st.toast("✅ 快取已清除！", icon="🧹")
        except Exception as e:
             st.error(f"清除失敗: {e}")

    st.markdown("---")

# 封裝分析函數 (暫時移除 Cache 以確保代碼更新生效)
# @st.cache_data(ttl=3600) 
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
# Check for auto-trigger from history load
auto_run = st.session_state.get('trigger_analysis', False)
if auto_run:
    st.session_state['trigger_analysis'] = False # Reset immediately
    st.session_state['analysis_active'] = True

if run_btn or force_btn:
    st.session_state['analysis_active'] = True

# Persist 'force' state only if clicked, otherwise default to False (use cache)
if force_btn:
    st.session_state['force_run'] = True
elif run_btn or auto_run:
    st.session_state['force_run'] = False 
# If just creating backtest (rerun), preserve existing 'force_run' or default False? 
# actually, just let it be.

if st.session_state.get('analysis_active', False):
    # 決定資料來源
    source = None
    display_ticker = ""
    # Use session state for force if available, else False
    is_force = st.session_state.get('force_run', False)
    
    if input_method == "股票代號 (Ticker)":
        if target_ticker:
            # 簡單判斷台股 - 讓 technical_analysis 自動處理後綴 (.TW/.TWO/FinMind)
            source = target_ticker.upper()
            display_ticker = source
        else:
            st.error("❌ 請輸入有效的股票代號")
            st.session_state['analysis_active'] = False # Reset
            st.stop()
    else:
        if uploaded_file is not None:
            # 讀取 CSV
            try:
                source = pd.read_csv(uploaded_file)
                display_ticker = "Uploaded File"
            except Exception as e:
                st.error(f"❌ 讀取 CSV 失敗: {e}")
                st.session_state['analysis_active'] = False # Reset
                st.stop()
        else:
            st.warning("⚠️ 請先上傳 CSV 檔案")
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
        
        # [NEW] Pre-load Chip Data for Analysis (籌碼預載)
        chip_data = None
        if source and isinstance(source, str) and ("TW" in source or source.isdigit()):
             try:
                 from chip_analysis import ChipAnalyzer
                 
                 @st.cache_data(ttl=3600)
                 def get_chip_data_cached(ticker, force):
                     analyzer = ChipAnalyzer()
                     return analyzer.get_chip_data(ticker, force_update=force)
                 
                 status_text.info(f"⏳ 正在分析 {display_ticker} (技術+籌碼)...")
                 chip_data, chip_err = get_chip_data_cached(source, is_force)
             except Exception as e:
                 print(f"Chip Load Error: {e}")

        # 暫存給 Analyzer 用 (Hack: 把變數掛在函式上，或者直接傳變數)
        run_analysis.df_week_cache = df_week
        run_analysis.df_day_cache = df_day
        # Save force state for chip loader
        run_analysis.force_update = is_force

        status_text.success("✅ 分析完成！")
        
        # ==========================================
        # 顯示股票基本資訊 (Header)
        # ==========================================

        # ==========================================
        # 顯示基本面資訊 (Fundamentals) - Moved to Header Area
        # ==========================================
        fund_data = None
        if source and isinstance(source, str):
             # 靜默載入，不顯示 Spinner 以免閃爍
             fund_data = get_fundamentals(display_ticker)
             run_analysis.fund_cache = fund_data # Cache for Tab

        if stock_meta and 'name' in stock_meta:
             st.markdown(f"## 🏢 {display_ticker} {stock_meta.get('name', '')}")
             
             if not df_day.empty:
                 last_price = df_day['Close'].iloc[-1]
                 prev_price = df_day['Close'].iloc[-2]
                 chg = last_price - prev_price
                 pct = (chg / prev_price) * 100
                 
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
                 st.caption(f"產業: {stock_meta.get('sector', 'N/A')} | 幣別: {stock_meta.get('currency', 'TWD')} | 更新時間: {df_day.index[-1].strftime('%Y-%m-%d')}")
        
        # 顯示如果有錯誤
                 

        # 新增 AI 分析報告 (Analysis Report)
        # ==========================================
        # 新增 AI 分析報告 (Analysis Report)
        # ==========================================
        import analysis_engine
        import importlib
        importlib.reload(analysis_engine)
        from analysis_engine import TechnicalAnalyzer
        from strategy_manager import StrategyManager

        
        # 只有當兩者都有數據時才進行完整分析
        if 'Weekly' in figures and 'Daily' in figures:
            # Load Strategy from cache
            sm = StrategyManager()
            strategy_params = sm.load_strategy(display_ticker) # Returns dict or None
            
            # 注意: 這裡需要傳入原始 DataFrame，而不是 Figure
            # run_analysis 回傳的是 dict
            analyzer = TechnicalAnalyzer(display_ticker, run_analysis.df_week_cache, run_analysis.df_day_cache, strategy_params, chip_data=chip_data)
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
                    
                    # 2. 進場
                    if ap.get('rec_entry_low', 0) > 0:
                         c2.warning(f"**建議進場**：\n\n📉 **{ap['rec_entry_low']:.2f}~{ap['rec_entry_high']:.2f}**")
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
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("#### 📅 週線趨勢因子")
                for item in report['trend_details']:
                    st.write(item)
            with c2:
                st.markdown("#### ⚡ 日線訊號因子")
                for item in report['trigger_details']:
                    st.write(item)
            
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
        col1, col2 = st.columns(2)
        
        # 顯示圖表
        col1, col2 = st.columns(2)
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📝 AI 分析報告 (週線趨勢)", "📈 技術指標 (日線操作)", "💰 籌碼分佈", "🏢 基本面", "📊 研究報告"])
        
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
                     from chip_analysis import ChipAnalyzer
                     
                     @st.cache_data(ttl=3600)
                     def get_chip_data_cached(ticker, force):
                         analyzer = ChipAnalyzer()
                         return analyzer.get_chip_data(ticker, force_update=force)

                     # Use force state from run_analysis
                     is_force = getattr(run_analysis, 'force_update', False)
                     chip_data, err = get_chip_data_cached(source, is_force)
                     loading_msg.empty() # Clear message
                     
                     if chip_data:
                         st.success(f"✅ {display_ticker} 籌碼數據讀取成功")
                         
                         # [NEW] Margin Utilization Metric (融資使用率)
                         df_m = chip_data['margin']
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
                                         dt_rate = (dt_vol / total_vol) * 100
                                         
                                         st.markdown("#### ⚡ 當沖週轉概況")
                                         st.caption(f"資料日期: {latest_date.strftime('%Y-%m-%d')}")
                                         c_dt1, c_dt2, c_dt3 = st.columns(3)
                                         c_dt1.metric("當沖成交量", f"{dt_vol:,.0f} 張")
                                         c_dt2.metric("當日總量", f"{total_vol:,.0f} 張")
                                         
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
                         
                         df_inst = chip_data['institutional']
                         df_margin = chip_data['margin']
                         
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
                                     name='自營商', marker_color='blue',
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
                                 margin=dict(l=30, r=30, t=50, b=50), # Increased Margins for Titles/Legend
                                 # Move Legend to Bottom to avoid overlap with Modebar/Title Hover
                                 legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5)
                             )
                             # Spikes
                             fig_chip.update_xaxes(showspikes=True, spikemode='across', spikesnap='cursor')
                             
                             st.plotly_chart(fig_chip, use_container_width=True)
                             
                         else:
                             st.warning("⚠️ 查無法人數據")

                         st.markdown("---")
                         st.info("💡 **集保股權分散 (Shareholding Distribution)**：因 API 限制為付費數據，暫無法顯示詳細大戶/散戶比例。建議搭配「三大法人」與「EFI 指標」判斷主力動向。")
                         
                     else:
                         st.error(f"❌ 籌碼讀取失敗: {err}")
                 except Exception as e:
                     st.error(f"❌ 發生錯誤: {e}")
            else:
                 st.info("💡 籌碼分析目前僅支援台股代號 (如 2330.TW)，CSV 模式不支援。")

        with tab4:
             st.markdown("### 🏢 基本面數據 (Fundamentals)")
             
             # 1. Company Profile
             fd = getattr(run_analysis, 'fund_cache', None)
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

        with tab5:
            st.subheader(f"📊 {display_ticker} 研究報告 (Github)")
            
            # Fetch report
            with st.spinner("正在搜尋最新研究報告..."):
                report_content, report_date, report_url = get_latest_report(display_ticker)
            
            if report_content:
                st.success(f"✅ 找到報告！日期: {report_date}")
                st.markdown(f"[🔗 在 GitHub 查看原文]({report_url})")
                st.markdown("---")
                st.markdown(report_content)
            else:
                st.info(f"ℹ️ 目前尚無 {display_ticker} 的相關研究報告。")
                st.caption(f"報告來源: https://github.com/SheenArtem/stock-research-reports")

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
                        
                        m1, m2, m3, m4 = st.columns(4)
                        val_color = "normal"
                        if results['total_return'] > 0: val_color = "off" # Streamlit metric doesn't allow color param directly easily
                        
                        m1.metric("總報酬率 (Total Return)", f"{results['total_return']:.2f}%", delta=None)
                        m2.metric("交易勝率 (Win Rate)", f"{results['win_rate']:.1f}%")
                        m3.metric("最大回檔 (Max DD)", f"{results['max_drawdown']:.2f}%")
                        m4.metric("目前持倉", "持有中" if results['holding'] else "空手")
                        
                        # Plot
                        fig_bt = engine.plot_results(results)
                        st.plotly_chart(fig_bt, use_container_width=True)
                        
                        # Trade Log
                        with st.expander("查看詳細交易紀錄 (Trade Log)"):
                            if not results['trades'].empty:
                                st.dataframe(results['trades'])
                            else:
                                st.info("期間無交易產生。")
                    else:
                        st.error("無法載入數據進行回測")
                        
                except Exception as e:
                    st.error(f"回測執行失敗: {str(e)}")

                    # 停損矩陣
                    st.markdown(f"#### 🛑 停損防守價位")
                    
                    def get_mark(name):
                        return "⭐️" if name == ap['rec_sl_method'] else ""
                        
                    sl_data = {
                        "推薦": [
                            get_mark("A. ATR 波動停損 (科學)"),
                            get_mark("B. 均線停損 (趨勢)"),
                            get_mark("C. 關鍵 K 線停損 (積極)"),
                            get_mark("D. 波段低點停損 (形態)"),
                        ],
                        "策略類型": ["A. ATR 波動停損 (科學)", "B. 均線停損 (趨勢)", "C. 關鍵 K 線停損 (積極)", "D. 波段低點停損 (形態)"],
                        "防守價位": [
                            f"{ap['sl_atr']:.2f}",
                            f"{ap['sl_ma']:.2f}",
                            f"{ap['sl_key_candle']:.2f}",
                            f"{ap['sl_low']:.2f}"
                        ]
                    }
                    st.table(pd.DataFrame(sl_data))

            st.markdown("---")

    except Exception as e:
        status_text.error(f"❌ 發生未預期錯誤: {e}")
        st.exception(e)

else:
    # 初始歡迎畫面
    st.info("👈 請在左測試欄輸入代號並點擊「開始分析」")

