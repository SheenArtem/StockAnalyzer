import streamlit as st
import pandas as pd
import mplfinance as mpf
from technical_analysis import plot_dual_timeframe, load_and_resample, calculate_all_indicators, plot_single_chart

# 設定頁面配置
st.set_page_config(
    page_title="Stock Technical Analyzer",
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
</style>
""", unsafe_allow_html=True)

# 標題
st.markdown('<div class="main-header">📈 右側交易技術分析系統</div>', unsafe_allow_html=True)

# 側邊欄
with st.sidebar:
    st.header("⚙️ 設定面板")
    st.caption("Version: v2025.12.25.47")
    
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
                        st.rerun() # Rerun to update the input box immediately
                
                with c3:
                    if st.button("刪除", key=f"del_{past_ticker}"):
                        cm.delete_ticker_cache(past_ticker)
                        st.toast(f"🗑️ 已刪除 {past_ticker}", icon="🗑️")
                        st.rerun()

    if input_method == "股票代號 (Ticker)":
        # 如果 session_state 有值 (剛按了載入)，就用它
        default_val = st.session_state.get('ticker_input', '2330')
        
        target_ticker = st.text_input("輸入股票代號 (台股請加 .TW)", 
                                      value=default_val, 
                                      key='ticker_input', # Bind to session state
                                      help="例如: 2330, TSM, AAPL")
    else:
        uploaded_file = st.file_uploader("上傳股票 CSV", type=['csv'])

    col_run, col_clear = st.columns([2, 1])
    with col_run:
        run_btn = st.button("🚀 開始分析", type="primary")
    with col_clear:
        if st.button("🧹 清除快取"):
            try:
                import shutil
                import os
                if os.path.exists("data_cache"):
                    shutil.rmtree("data_cache")
                    # os.makedirs("data_cache") # lazy create
                st.toast("✅ 快取已清除！下一次分析將重新下載資料。", icon="🧹")
            except Exception as e:
                st.error(f"清除失敗: {e}")

    st.markdown("---")

# 封裝分析函數 (暫時移除 Cache 以確保代碼更新生效)
# @st.cache_data(ttl=3600) 
def run_analysis(source_data):
    # 這裡的邏輯與原本 main 當中的一樣，但搬進來做 cache
    
    # 1. 股票代號情況
    if isinstance(source_data, str):
        return plot_dual_timeframe(source_data)
        
    # 2. CSV 資料情況 (DataFrame 無法直接 hash，需注意 cache 機制，這裡簡化處理)
    # Streamlit 對 DataFrame 有支援 hashing，所以通常可以直接傳
    ticker_name, df_day, df_week, stock_meta = load_and_resample(source_data)
    
    figures = {}
    errors = {}
    
    # 手動計算
    if not df_week.empty:
        try:
            df_week = calculate_all_indicators(df_week)
            fig_week = plot_single_chart(ticker_name, df_week, "Trend (Long)", "Weekly")
            figures['Weekly'] = fig_week
        except Exception as e:
            errors['Weekly'] = str(e)
            
    if not df_day.empty:
        try:
            df_day = calculate_all_indicators(df_day)
            fig_day = plot_single_chart(ticker_name, df_day, "Action (Short)", "Daily")
            figures['Daily'] = fig_day
        except Exception as e:
            errors['Daily'] = str(e)
            
    return figures, errors, df_week, df_day, stock_meta

# 主程式邏輯
if run_btn:
    # 決定資料來源
    source = None
    display_ticker = ""
    
    if input_method == "股票代號 (Ticker)":
        if target_ticker:
            # 簡單判斷台股 - 讓 technical_analysis 自動處理後綴 (.TW/.TWO/FinMind)
            source = target_ticker.upper()
            display_ticker = source
        else:
            st.error("❌ 請輸入有效的股票代號")
            st.stop()
    else:
        if uploaded_file is not None:
            # 讀取 CSV
            try:
                source = pd.read_csv(uploaded_file)
                display_ticker = "Uploaded File"
            except Exception as e:
                st.error(f"❌ 讀取 CSV 失敗: {e}")
                st.stop()
        else:
            st.warning("⚠️ 請先上傳 CSV 檔案")
            st.stop()

    # 執行分析
    status_text = st.empty()
    status_text.info(f"⏳ 正在分析 {display_ticker} ...")
    
    try:
        # 呼叫有快取的函數
        figures, errors, df_week, df_day, stock_meta = run_analysis(source)
        
        # 暫存給 Analyzer 用 (Hack: 把變數掛在函式上，或者直接傳變數)
        run_analysis.df_week_cache = df_week
        run_analysis.df_day_cache = df_day

        status_text.success("✅ 分析完成！")
        
        # ==========================================
        # 顯示股票基本資訊 (Header)
        # ==========================================
        if stock_meta and 'name' in stock_meta:
             st.markdown(f"## 🏢 {display_ticker} {stock_meta.get('name', '')}")
             if not df_day.empty:
                 last_price = df_day['Close'].iloc[-1]
                 prev_price = df_day['Close'].iloc[-2]
                 chg = last_price - prev_price
                 pct = (chg / prev_price) * 100
                 color = "red" if chg > 0 else "green" # 台股紅漲綠跌
                 
                 cols = st.columns(4)
                 cols[0].metric("最新收盤價", f"{last_price:.2f}", f"{chg:.2f} ({pct:.2f}%)", delta_color="inverse")
                 cols[1].metric("產業類別", stock_meta.get('sector', 'N/A'))
                 cols[2].metric("幣別", stock_meta.get('currency', 'TWD'))
                 
        # 顯示如果有錯誤
        if errors:
            with st.expander("⚠️ 部分圖表產生失敗原因", expanded=True):
                for k, v in errors.items():
                    st.error(f"{k}: {v}")

        # ==========================================
        # 新增 AI 分析報告 (Analysis Report)
        # ==========================================
        from analysis_engine import TechnicalAnalyzer
        
        # 只有當兩者都有數據時才進行完整分析
        if 'Weekly' in figures and 'Daily' in figures:
            # 注意: 這裡需要傳入原始 DataFrame，而不是 Figure
            # run_analysis 回傳的是 dict
            analyzer = TechnicalAnalyzer(display_ticker, run_analysis.df_week_cache, run_analysis.df_day_cache)
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
                
            # 2. 核心操作建議 (Key Actionables) - Moved to Top
            if report.get('action_plan'):
                ap = report['action_plan']
                
                # 第一排：策略、進場、停利、停損 (4欄)
                c1, c2, c3, c4 = st.columns(4)
                
                # 1. 策略
                c1.info(f"**操作策略**：\n\n{ap['strategy']}")
                
                # 2. 進場
                if ap.get('rec_entry_low', 0) > 0:
                     c2.warning(f"**建議進場**：\n\n📉 **{ap['rec_entry_low']:.2f}~{ap['rec_entry_high']:.2f}**")
                else:
                     c2.warning(f"**建議進場**：\n\n(暫無建議)")

                # 3. 停利
                c3.success(f"**推薦停利**：\n\n🎯 **{ap['rec_tp_price']:.2f}**")
                
                # 4. 停損
                c4.error(f"**推薦停損**：\n\n🛑 **{ap['rec_sl_price']:.2f}** ({ap['rec_sl_method'].split(' ')[0]})")
                
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
                    # 停利目標清單
                    if ap.get('tp_list'):
                        st.markdown("#### 🔭 停利目標預估清單")
                        tp_data = []
                        for t in ap['tp_list']:
                            mark = "⭐️" if t['is_rec'] else ""
                            tp_data.append({
                                "推薦": mark,
                                "測幅方法": t['method'],
                                "目標價格": f"{t['price']:.2f}",
                                "說明": t['desc']
                            })
                        st.table(pd.DataFrame(tp_data))

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

        # 顯示圖表
        col1, col2 = st.columns(2)
        
        # 顯示圖表
        col1, col2 = st.columns(2)
        
        tab1, tab2, tab3 = st.tabs(["📅 週線趨勢 (Trend)", "🌞 日線操作 (Action)", "💰 籌碼分佈 (Chips)"])
        
        with tab1:
            if 'Weekly' in figures:
                st.pyplot(figures['Weekly'])
                
                # 圖例說明 (新增)
                st.info("""
                **圖表符號說明：**
                - 🔺 紅色三角形 + 數字 9：**神奇九轉 (買進)** - 股價連續 9 天低於前 4 天收盤，短線超賣，隨時可能反彈。
                - 🔻 綠色倒三角 + 數字 9：**神奇九轉 (賣出)** - 股價連續 9 天高於前 4 天收盤，短線超漲，隨時可能回檔。
                - 🔢 數字 6~8：代表趨勢正在累積中，即將出現轉折訊號。
                """)
                
                # 新增: Weekly EFI
                if not df_week.empty and 'EFI_EMA13' in df_week.columns:
                    st.markdown("### ⚡ 週線能量 (Weekly EFI)")
                    st.caption("週線 EFI 能夠過濾短期雜訊，更準確判斷主力長線資金動向。")
                    st.line_chart(df_week[['EFI_EMA13']].iloc[-100:])
                    
            else:
                st.warning("⚠️ 無法產生週線圖表 (請查看上方錯誤訊息)")
        
        with tab2:
            if 'Daily' in figures:
                st.pyplot(figures['Daily'])
                
                # 圖例說明
                st.info("""
                **圖表符號說明：**
                - 🔺 紅色三角形 + 數字 9：**神奇九轉 (買進)** - 股價連續 9 天低於前 4 天收盤，短線超賣，隨時可能反彈。
                - 🔻 綠色倒三角 + 數字 9：**神奇九轉 (賣出)** - 股價連續 9 天高於前 4 天收盤，短線超漲，隨時可能回檔。
                - 🔢 數字 6~8：代表趨勢正在累積中，即將出現轉折訊號。
                """)

                # 新增: EFI 能量圖 (獨立顯示)
                if not df_day.empty and 'EFI_EMA13' in df_day.columns:
                    st.markdown("### ⚡ 埃爾德強力指標 (EFI - Elder's Force Index)")
                    st.caption("原理：結合「價格變動」與「成交量」。EFI > 0 代表多方有力，EFI < 0 代表空方有力。")
                    
                    st.line_chart(df_day[['EFI_EMA13', 'EFI_EMA2']].iloc[-60:])
                    
                    # 簡易解讀
                    last_efi = df_day['EFI_EMA13'].iloc[-1]
                    if last_efi > 0:
                        st.success(f"🔥 主力力道：多方控盤 (EFI_13={last_efi:,.0f})")
                    else:
                        st.error(f"❄️ 主力力道：空方控盤 (EFI_13={last_efi:,.0f})")
            else:
                st.warning("⚠️ 無法產生日線圖表 (請查看上方錯誤訊息)")

        with tab3:
            # 寬鬆判斷：只要是字串且 (含TW 或 純數字) 都嘗試顯示籌碼
            if source and isinstance(source, str) and ("TW" in source or source.isdigit()):
                 # 嘗試抓取籌碼數據
                 try:
                     st.info(f"⏳ 正在抓取 {display_ticker} 近一年籌碼數據 (FinMind)...")
                     from chip_analysis import ChipAnalyzer
                     
                     @st.cache_data(ttl=3600)
                     def get_chip_data_cached(ticker):
                         analyzer = ChipAnalyzer()
                         return analyzer.get_chip_data(ticker)

                     chip_data, err = get_chip_data_cached(source)
                     
                     if chip_data:
                         st.success(f"✅ {display_ticker} 籌碼數據讀取成功")
                         
                         # 1. 三大法人買賣超 (Bar Chart)
                         st.markdown("### 🏛️ 三大法人買賣超 (Institutional Investors)")
                         df_inst = chip_data['institutional']
                         if not df_inst.empty:
                             # 只顯示最近 60 天以保持圖表清晰
                             df_inst_recent = df_inst.iloc[-60:]
                             cols_to_plot = [c for c in df_inst_recent.columns if c != '三大法人合計' and c != 'stock_id']
                             st.bar_chart(df_inst_recent[cols_to_plot])
                             st.caption("三大法人近期動向 (Foreign/Trust/Dealer)")
                         else:
                             st.warning("⚠️ 查無法人數據")

                         st.markdown("---")

                         # 2. 融資融券 (Line Chart)
                         st.markdown("### 🎢 融資融券餘額 (Margin Trading)")
                         df_margin = chip_data['margin']
                         if not df_margin.empty:
                             df_margin_recent = df_margin.iloc[-120:]
                             st.line_chart(df_margin_recent)
                             st.caption("融資(Margin Buy) vs 融券(Short Sell) 餘額走勢")
                         else:
                             st.warning("⚠️ 查無融資券數據")

                         st.markdown("---")
                         st.info("💡 **集保股權分散 (Shareholding Distribution)**：因 API 限制為付費數據，暫無法顯示詳細大戶/散戶比例。建議搭配「三大法人」與「EFI 指標」判斷主力動向。")
                         
                     else:
                         st.error(f"❌ 籌碼讀取失敗: {err}")
                 except Exception as e:
                     st.error(f"❌ 發生錯誤: {e}")
            else:
                 st.info("💡 籌碼分析目前僅支援台股代號 (如 2330.TW)，CSV 模式不支援。")

    except Exception as e:
        status_text.error(f"❌ 發生未預期錯誤: {e}")
        st.exception(e)

else:
    # 初始歡迎畫面
    st.info("👈 請在左測試欄輸入代號並點擊「開始分析」")

