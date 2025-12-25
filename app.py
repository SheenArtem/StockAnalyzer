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
            
    return figures, errors

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
        figures, errors = run_analysis(source)

        status_text.success("✅ 分析完成！")
        
        # 顯示如果有錯誤
        if errors:
            with st.expander("⚠️ 部分圖表產生失敗原因", expanded=True):
                for k, v in errors.items():
                    st.error(f"{k}: {v}")

        # 顯示圖表
        col1, col2 = st.columns(2)
        
        tab1, tab2 = st.tabs(["📅 週線趨勢 (Trend)", "🌞 日線操作 (Action)"])
        
        with tab1:
            if 'Weekly' in figures:
                st.pyplot(figures['Weekly'])
            else:
                st.warning("⚠️ 無法產生週線圖表 (請查看上方錯誤訊息)")
        
        with tab2:
            if 'Daily' in figures:
                st.pyplot(figures['Daily'])
            else:
                st.warning("⚠️ 無法產生日線圖表 (請查看上方錯誤訊息)")

    except Exception as e:
        status_text.error(f"❌ 發生未預期錯誤: {e}")
        st.exception(e)

else:
    # 初始歡迎畫面
    st.info("👈 請在左測試欄輸入代號並點擊「開始分析」")
