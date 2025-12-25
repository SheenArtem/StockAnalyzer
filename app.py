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
st.markdown('<div class="main-header">📈 全方位股票技術分析系統</div>', unsafe_allow_html=True)

# 側邊欄
with st.sidebar:
    st.header("⚙️ 設定面板")
    
    input_method = st.radio("選擇輸入方式", ["股票代號 (Ticker)", "上傳 CSV 檔"])
    
    target_ticker = "2330" # 預設值
    uploaded_file = None
    
    if input_method == "股票代號 (Ticker)":
        target_ticker = st.text_input("輸入股票代號 (台股請加 .TW)", value="2330", help="例如: 2330, TSM, AAPL")
    else:
        uploaded_file = st.file_uploader("上傳股票 CSV", type=['csv'])

    run_btn = st.button("🚀 開始分析", type="primary")

    st.markdown("---")
    st.markdown("### 📊 支援指標")
    st.info("""
    - **MA**: 5, 10, 20, 60
    - **Bollinger Bands**
    - **Ichimoku (一目均衡表)**
    - **ATR Stop Loss**
    - **MACD / RSI / KD / OBV / DMI**
    """)

# 主程式邏輯
if run_btn:
    # 決定資料來源
    source = None
    if input_method == "股票代號 (Ticker)":
        if target_ticker:
            # 簡單判斷台股
            if target_ticker.isdigit():
                source = f"{target_ticker}.TW"
            else:
                source = target_ticker.upper()
        else:
            st.error("❌ 請輸入有效的股票代號")
            st.stop()
    else:
        if uploaded_file is not None:
            # 讀取 CSV
            try:
                source = pd.read_csv(uploaded_file)
            except Exception as e:
                st.error(f"❌ 讀取 CSV 失敗: {e}")
                st.stop()
        else:
            st.warning("⚠️ 請先上傳 CSV 檔案")
            st.stop()

    # 執行分析
    status_text = st.empty()
    status_text.info(f"⏳ 正在分析 {target_ticker if isinstance(source, str) else 'Uploaded File'} ...")
    
    try:
        # 重接 output 以捕捉 print (Optional, Streamlit 通常直接顯示圖表)
        # 這裡我們直接呼叫修改後的函數取得 Figure
        
        # 1. 直接呼叫 plot_dual_timeframe (已修改為回傳 dict)
        if isinstance(source, str):
            figures = plot_dual_timeframe(source)
            ticker_display = source.replace('.TW', '')
        else:
            # 針對 CSV 的邏輯需手動處理，因為 plot_dual_timeframe 主要設計給 Ticker
            # 我們稍微改寫一下邏輯重用 load_and_resample
            ticker_name, df_day, df_week = load_and_resample(source)
            ticker_display = ticker_name
            figures = {}
            
            # 手動計算與繪圖 (複製 plot_dual_timeframe 的邏輯)
            if not df_week.empty:
                df_week = calculate_all_indicators(df_week)
                fig_week = plot_single_chart(ticker_name, df_week, "Trend (Long)", "Weekly")
                figures['Weekly'] = fig_week
            
            if not df_day.empty:
                df_day = calculate_all_indicators(df_day)
                fig_day = plot_single_chart(ticker_name, df_day, "Action (Short)", "Daily")
                figures['Daily'] = fig_day

        status_text.success("✅ 分析完成！")

        # 顯示圖表
        col1, col2 = st.columns(2)
        
        # 為了更好的手機體驗，改為上下排列或根據 User 需求，這裡先用 Tabs
        tab1, tab2 = st.tabs(["📅 週線趨勢 (Trend)", "🌞 日線操作 (Action)"])
        
        with tab1:
            if 'Weekly' in figures:
                st.pyplot(figures['Weekly'])
            else:
                st.warning("⚠️ 無法產生週線圖表")
        
        with tab2:
            if 'Daily' in figures:
                st.pyplot(figures['Daily'])
            else:
                st.warning("⚠️ 無法產生日線圖表")

    except Exception as e:
        status_text.error(f"❌ 發生錯誤: {e}")
        st.exception(e)

else:
    # 初始歡迎畫面
    st.info("👈 請在左測試欄輸入代號並點擊「開始分析」")
