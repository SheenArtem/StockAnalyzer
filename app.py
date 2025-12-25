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
    st.caption("Version: v2025.12.25.25")
    
    input_method = st.radio("選擇輸入方式", ["股票代號 (Ticker)", "上傳 CSV 檔"])
    
    target_ticker = "2330" # 預設值
    uploaded_file = None
    
    if input_method == "股票代號 (Ticker)":
        target_ticker = st.text_input("輸入股票代號 (台股請加 .TW)", value="2330", help="例如: 2330, TSM, AAPL")
    else:
        uploaded_file = st.file_uploader("上傳股票 CSV", type=['csv'])

    run_btn = st.button("🚀 開始分析", type="primary")

    st.markdown("---")

# 封裝分析函數以加入快取 (Cache)
@st.cache_data(ttl=3600)  # 快取 1 小時
def run_analysis(source_data):
    # 這裡的邏輯與原本 main 當中的一樣，但搬進來做 cache
    
    # 1. 股票代號情況
    if isinstance(source_data, str):
        return plot_dual_timeframe(source_data)
        
    # 2. CSV 資料情況 (DataFrame 無法直接 hash，需注意 cache 機制，這裡簡化處理)
    # Streamlit 對 DataFrame 有支援 hashing，所以通常可以直接傳
    ticker_name, df_day, df_week = load_and_resample(source_data)
    
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
            
    return figures, errors, df_week, df_day

# 主程式邏輯
if run_btn:
    # 決定資料來源
    source = None
    display_ticker = ""
    
    if input_method == "股票代號 (Ticker)":
        if target_ticker:
            # 簡單判斷台股
            if target_ticker.isdigit():
                source = f"{target_ticker}.TW"
            else:
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
        figures, errors, df_week, df_day = run_analysis(source)
        
        # 暫存給 Analyzer 用 (Hack: 把變數掛在函式上，或者直接傳變數)
        run_analysis.df_week_cache = df_week
        run_analysis.df_day_cache = df_day

        status_text.success("✅ 分析完成！")
        
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
                st.error(f"### {sc['title']}\n{sc['desc']}") # Streamlit red is error
            elif sc['color'] == 'orange':
                st.warning(f"### {sc['title']}\n{sc['desc']}")
            elif sc['color'] == 'green':
                st.success(f"### {sc['title']}\n{sc['desc']}")
            else:
                st.info(f"### {sc['title']}\n{sc['desc']}")
                
            st.markdown("---")
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("#### 📅 週線趨勢因子")
                for item in report['trend_details']:
                    st.write(item)
            with c2:
                st.markdown("#### ⚡ 日線訊號因子")
                for item in report['trigger_details']:
                    st.write(item)
            
            # 3. 操作劇本與風控 (Action Plan)
            st.markdown("---")
            st.subheader("🛡️ 操作劇本與風控建議 (Action Plan)")
            if report.get('action_plan'):
                ap = report['action_plan']
                
                # 進場與停利
                col_strat, col_tp = st.columns(2)
                col_strat.info(f"**進場策略**：\n\n{ap['strategy']}")
                col_tp.success(f"**推薦停利 (第一目標)**：\n\n🎯 **{ap['rec_tp_price']:.2f}**")
                
                # 停利目標清單
                if ap.get('tp_list'):
                    st.markdown("#### 🔭 停利目標預估清單 (依價格排序)")
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
                st.markdown("#### 🛑 停損防守價位 (建議 4 選 1)")
                sl_data = {
                    "策略類型": ["A. ATR 波動停損 (科學)", "B. 均線停損 (趨勢)", "C. 關鍵 K 線停損 (積極)", "D. 波段低點停損 (形態)"],
                    "防守價位": [
                        f"{ap['sl_atr']:.2f} (Close - 2*ATR)",
                        f"{ap['sl_ma']:.2f} (MA20)",
                        f"{ap['sl_key_candle']:.2f} (爆量低點)",
                        f"{ap['sl_low']:.2f} (近期低點)"
                    ],
                    "說明": [
                        "依據市場波動率動態調整，適合一般交易者。",
                        "依據月線支撐，適合波段順勢操作。",
                        "跌破主力攻擊發起點即停損，適合短線積極者。",
                        "跌破箱型或波段最低點，最後防線。"
                    ]
                }
                st.table(pd.DataFrame(sl_data))
            else:
                st.warning("⚠️ 數據不足，無法生成風控建議")

            st.markdown("---")

        # 顯示圖表
        col1, col2 = st.columns(2)
        
        # 顯示圖表
        col1, col2 = st.columns(2)
        
        tab1, tab2, tab3 = st.tabs(["📅 週線趨勢 (Trend)", "🌞 日線操作 (Action)", "💰 籌碼分佈 (Chips)"])
        
        with tab1:
            if 'Weekly' in figures:
                st.pyplot(figures['Weekly'])
            else:
                st.warning("⚠️ 無法產生週線圖表 (請查看上方錯誤訊息)")
        
        with tab2:
            if 'Daily' in figures:
                st.pyplot(figures['Daily'])
                
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
            if source and isinstance(source, str) and "TW" in source:
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

