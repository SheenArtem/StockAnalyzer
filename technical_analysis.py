# filename: technical_analysis.py

import yfinance as yf
import mplfinance as mpf
import pandas as pd
import numpy as np

def calculate_all_indicators(df):
    """
    核心運算引擎：計算所有技術指標
    包含：MA, BB, ATR, Ichimoku, RSI, KD, MACD, OBV, DMI
    """
    print("DEBUG: VERSION v2025.12.25.08 - CHECKING CODE UPDATE")
    # 1. 基礎數據清洗
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    # 2. 均線系統 (Moving Averages)
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA10'] = df['Close'].rolling(window=10).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()

    # 3. 布林通道 (Bollinger Bands)
    df['std20'] = df['Close'].rolling(window=20).std()
    df['BB_Up'] = df['MA20'] + (2 * df['std20'])
    df['BB_Lo'] = df['MA20'] - (2 * df['std20'])

    # 4. ATR 與 停損線 (Chandelier Exit)
    prev_close = df['Close'].shift(1)
    df['H-L'] = df['High'] - df['Low']
    df['H-PC'] = abs(df['High'] - prev_close)
    df['L-PC'] = abs(df['Low'] - prev_close)
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    df['ATR'] = df['TR'].rolling(window=14).mean()
    df['ATR_Stop'] = df['Close'] - (2 * df['ATR'])

    # 5. 一目均衡表 (Ichimoku) - 簡化版
    # 轉換線 (Tenkan) & 基準線 (Kijun)
    df['Tenkan'] = (df['High'].rolling(window=9).max() + df['Low'].rolling(window=9).min()) / 2
    df['Kijun'] = (df['High'].rolling(window=26).max() + df['Low'].rolling(window=26).min()) / 2

    # 6. RSI (Relative Strength Index)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # 7. KD (Stochastic)
    low_min = df['Low'].rolling(window=9).min()
    high_max = df['High'].rolling(window=9).max()
    df['RSV'] = (df['Close'] - low_min) / (high_max - low_min) * 100
    df['K'] = df['RSV'].ewm(com=2).mean()
    df['D'] = df['K'].ewm(com=2).mean()

    # 8. MACD
    exp12 = df['Close'].ewm(span=12, adjust=False).mean()
    exp26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = exp12 - exp26
    df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['Hist'] = df['MACD'] - df['Signal']

    # 9. OBV (On-Balance Volume)
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()

    # 10. DMI & ADX
    up = df['High'].diff()
    down = -df['Low'].diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr_smooth = df['TR'].rolling(window=14).mean()
    df['+DI'] = 100 * (pd.Series(plus_dm).rolling(window=14).mean() / tr_smooth)
    df['-DI'] = 100 * (pd.Series(minus_dm).rolling(window=14).mean() / tr_smooth)
    df['DX'] = 100 * abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI'])
    df['ADX'] = df['DX'].rolling(window=14).mean()

    return df
# ==========================================
# 新增模組：數據載入與重採樣 (Data Loader & Resampler)
# ==========================================

def load_and_resample(source):
    """
    智慧數據載入器：
    1. 若輸入是字串 (Ticker) -> 用 yfinance 下載
    2. 若輸入是 DataFrame (CSV) -> 直接使用並自動產生週線
    """
    df_day = pd.DataFrame()
    df_week = pd.DataFrame()
    ticker_name = "Unknown"

    # 情境 A: 傳入的是股票代號 (字串)
    if isinstance(source, str):
        ticker_name = source
        if source.isdigit(): ticker_name = f"{source}.TW"
        
        print(f"📥 正在下載 {ticker_name} 網路數據...")
        # 下載日線
        df_day = yf.download(ticker_name, period='1y', interval='1d', progress=False)
        # 下載週線
        df_week = yf.download(ticker_name, period='3y', interval='1wk', progress=False)

    # 情境 B: 傳入的是 CSV 資料 (DataFrame)
    elif isinstance(source, pd.DataFrame):
        print(f"📂 正在處理上傳的 CSV 數據...")
        ticker_name = "Uploaded_Data"
        df_day = source.copy()
        
        # 確保 Index 是 Datetime
        if not isinstance(df_day.index, pd.DatetimeIndex):
            # 嘗試尋找日期欄位
            for col in df_day.columns:
                if 'date' in col.lower() or '時間' in col:
                    df_day[col] = pd.to_datetime(df_day[col])
                    df_day.set_index(col, inplace=True)
                    break
        
        # 確保欄位名稱標準化 (Open, High, Low, Close, Volume)
        # 這裡做簡單映射，視您的 CSV 格式而定
        df_day.columns = [c.capitalize() for c in df_day.columns] 

        # 自動生成週線 (Resample) - 這是關鍵！
        # 將日線 CSV 轉換為週線，規則：週五收盤、週一開盤、最高、最低、總量
        logic = {
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        }
        # 過濾只保留存在的欄位
        agg_logic = {k: v for k, v in logic.items() if k in df_day.columns}
        
        if not df_day.empty:
            df_week = df_day.resample('W-FRI').agg(agg_logic)

    # 處理 MultiIndex (共用清洗邏輯)
    for df in [df_day, df_week]:
        if not df.empty and isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

    return ticker_name, df_day, df_week

# ==========================================
# 修改後的主程式：支援 CSV 與 Ticker
# ==========================================

def plot_dual_timeframe(source):
    """
    主程式：接受 '代號' 或 'DataFrame' 進行雙週期分析
    """
    # 1. 呼叫智慧載入器
    ticker, df_day, df_week = load_and_resample(source)

    print(f"🚀 啟動雙週期全方位分析引擎: {ticker}")

    # 2. 繪製週線
    if not df_week.empty:
        try:
            df_week = calculate_all_indicators(df_week)
            plot_single_chart(ticker, df_week, "Trend (Long)", "Weekly")
        except Exception as e:
            print(f"❌ 週線計算錯誤: {e}")
    else:
        print("❌ 無法取得週線數據 (可能是 CSV 資料不足)")

    # 3. 繪製日線
    if not df_day.empty:
        try:
            df_day = calculate_all_indicators(df_day)
            plot_single_chart(ticker, df_day, "Action (Short)", "Daily")
        except Exception as e:
            print(f"❌ 日線計算錯誤: {e}")
    else:
        print("❌ 無法取得日線數據")
        
def plot_single_chart(ticker, df, title_suffix, timeframe_label):
    """繪製單張圖表 (包含 5 個面板)"""
    
    # 裁切數據: 週線看 100 根 (約2年), 日線看 120 根 (約半年)
    bars = 100 if timeframe_label == 'Weekly' else 120
    plot_df = df.tail(bars).copy()

    # 設定面板 (Subplots)
    # 設定面板 (Subplots)
    apds = []

    # Helper: 安全添加 plot 的小函數
    def add_plot_safe(series, **kwargs):
        # 檢查是否全為 NaN
        # 注意: 如果 series 是 DataFrame (例如 MA5, MA10 畫在一起), isna().all() 會回傳 Series 導致錯誤
        # 解法: 轉成 numpy array 再檢查是否全部為 True
        if not series.isna().values.all():
            apds.append(mpf.make_addplot(series, **kwargs))

    # Panel 0: 主圖
    add_plot_safe(plot_df[['MA5', 'MA10', 'MA20']], width=1.0)
    add_plot_safe(plot_df['MA60'], color='black', width=1.5)
    add_plot_safe(plot_df['BB_Up'], color='gray', linestyle='--', alpha=0.5)
    add_plot_safe(plot_df['BB_Lo'], color='gray', linestyle='--', alpha=0.5)
    add_plot_safe(plot_df['Tenkan'], color='cyan', linestyle=':', width=0.8)
    add_plot_safe(plot_df['Kijun'], color='brown', linestyle=':', width=0.8)
    add_plot_safe(plot_df['ATR_Stop'], color='purple', type='scatter', markersize=6, marker='_')

    # Panel 1: OBV
    add_plot_safe(plot_df['OBV'], panel=1, color='blue', width=1.2, ylabel='OBV')

    # Panel 2: MACD
    add_plot_safe(plot_df['Hist'], type='bar', panel=2, color='dimgray', alpha=0.5, ylabel='MACD')
    add_plot_safe(plot_df['MACD'], panel=2, color='fuchsia')
    add_plot_safe(plot_df['Signal'], panel=2, color='c')

    # Panel 3: KD & RSI
    add_plot_safe(plot_df['K'], panel=3, color='orange', ylabel='KD & RSI')
    add_plot_safe(plot_df['D'], panel=3, color='blue')
    add_plot_safe(plot_df['RSI'], panel=3, color='green', linestyle='--', width=1)

    # Panel 4: DMI
    add_plot_safe(plot_df['ADX'], panel=4, color='black', width=1.5, ylabel='DMI')
    add_plot_safe(plot_df['+DI'], panel=4, color='red', width=0.8)
    add_plot_safe(plot_df['-DI'], panel=4, color='green', width=0.8)

    print(f"📊 正在繪製 {timeframe_label} 全方位分析圖...")
    
    # 檢查成交量是否有效 (全部為 0 或 NaN 則不畫成交量)
    # 檢查成交量是否有效 (全部為 0 或 NaN 則不畫成交量)
    use_volume = True
    if 'Volume' not in plot_df.columns:
        use_volume = False
    else:
        # 先把 NaN 填 0，避免 sum() 出錯，並檢查是否有任何非零值
        vol_clean = plot_df['Volume'].fillna(0)
        if (vol_clean == 0).all():
            print("⚠️ 偵測到無效成交量 (全為0)，將隱藏 Volume 面板")
            use_volume = False

    # 最後防線: 檢查 plot_df 是否太少
    if len(plot_df) < 2:
        raise ValueError("數據行數不足，無法繪圖 (Less than 2 rows)")

    # 如果要回傳 figure 給 Streamlit，需要 returnfig=True
    # 注意: mpf.plot 的 returnfig=True 會回傳 (fig, axes)
    fig, axes = mpf.plot(plot_df, type='candle', addplot=apds, 
             volume=use_volume, 
             returnfig=True)
             
    return fig

def plot_dual_timeframe(ticker_symbol):
    """
    主程式：執行 [週線] + [日線] 雙重分析
    """
    ticker_symbol = str(ticker_symbol).strip()
    if ticker_symbol.isdigit():
        ticker = f"{ticker_symbol}.TW"
    else:
        ticker = ticker_symbol.upper()

    print(f"🚀 啟動雙週期全方位分析引擎: {ticker}")

    # 儲存圖表物件與錯誤訊息
    figures = {}
    errors = {}

    # 1. 週線 (Weekly) - 抓 3 年
    try:
        df_week = yf.download(ticker, period='3y', interval='1wk', progress=False)
        if not df_week.empty:
            df_week = calculate_all_indicators(df_week)
            fig_week = plot_single_chart(ticker, df_week, "Trend (Long)", "Weekly")
            figures['Weekly'] = fig_week
        else:
            errors['Weekly'] = "無法下載週線數據 (Data Empty)"
            print("❌ 無法下載週線數據")
    except Exception as e:
        errors['Weekly'] = f"週線錯誤: {str(e)}"
        print(f"❌ 週線下載錯誤: {e}")

    # 2. 日線 (Daily) - 抓 1 年
    try:
        df_day = yf.download(ticker, period='1y', interval='1d', progress=False)
        if not df_day.empty:
            df_day = calculate_all_indicators(df_day)
            fig_day = plot_single_chart(ticker, df_day, "Action (Short)", "Daily")
            figures['Daily'] = fig_day
        else:
            errors['Daily'] = "無法下載日線數據 (Data Empty)"
            print("❌ 無法下載日線數據")
    except Exception as e:
        errors['Daily'] = f"日線錯誤: {str(e)}"
        print(f"❌ 日線下載錯誤: {e}")
        
    return figures, errors

if __name__ == "__main__":
    # 測試用
    plot_dual_timeframe('2330')


# ==========================================
# 新增模組：ZIP 批次處理器 (Batch Processor)
# ==========================================
import zipfile
import os

def analyze_zip_batch(zip_path):
    """
    功能：解壓縮 ZIP 檔，並列出裡面有哪些股票 CSV
    注意：Gemini 雖然可以解壓縮，但一次畫太多圖會當機。
    策略：先列出清單，讓使用者選擇要分析哪一檔。
    """
    print(f"📦 收到壓縮檔，正在解壓縮...")
    
    extracted_files = []
    extract_path = "/mnt/data/extracted_stocks" # Gemini 沙盒常用路徑
    
    try:
        # 建立解壓目錄
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)
            
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            extracted_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            
        print(f"✅ 解壓縮成功！共發現 {len(extracted_files)} 檔股票數據。")
        print("請告訴我您想優先分析哪一檔？(輸入代號即可)")
        
        # 回傳檔案對應字典 {'2330': 'path/to/2330.TW.csv'}
        file_map = {}
        for f in extracted_files:
            # 假設檔名是 2330.TW.csv，提取 2330
            stock_id = f.split('.')[0] 
            full_path = os.path.join(extract_path, f)
            file_map[stock_id] = full_path
            
        return file_map

    except Exception as e:
        print(f"❌ 解壓縮失敗: {e}")
        return {}
