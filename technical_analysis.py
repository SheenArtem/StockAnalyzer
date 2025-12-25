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
    print("DEBUG: VERSION v2025.12.25.19 - ADDING MA120/MA240 & ADVANCED TARGETS")
    # 1. 基礎數據清洗
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    # 2. 均線系統 (Moving Averages)
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA10'] = df['Close'].rolling(window=10).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    df['MA120'] = df['Close'].rolling(window=120).mean()
    df['MA240'] = df['Close'].rolling(window=240).mean()

    # 3. 布林通道 (Bollinger Bands)
    df['std20'] = df['Close'].rolling(window=20).std()
    df['BB_Up'] = df['MA20'] + (2 * df['std20'])
    df['BB_Lo'] = df['MA20'] - (2 * df['std20'])
    
    # 3.5 乖離率 (BIAS)
    df['BIAS'] = (df['Close'] - df['MA20']) / df['MA20'] * 100

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

    # 6. 埃爾德強力指標 (Elder's Force Index)
    # EFI = (Close - PrevClose) * Volume
    change = df['Close'].diff()
    df['EFI'] = change * df['Volume']
    df['EFI_EMA13'] = df['EFI'].ewm(span=13, adjust=False).mean() # 長期趨勢 (歸零軸判斷)
    df['EFI_EMA2'] = df['EFI'].ewm(span=2, adjust=False).mean()   # 短期力道 (抓轉折)

    return df
# ==========================================
# 新增模組：數據載入與重採樣 (Data Loader & Resampler)
# ==========================================

from FinMind.data import DataLoader
import datetime

# ==========================================
# 新增模組：數據載入與重採樣 (Data Loader & Resampler)
# ==========================================

def fetch_from_finmind(stock_id):
    """
    從 FinMind 抓取股價資料 (Fallback)
    """
    try:
        print(f"🔄 嘗試從 FinMind 抓取 {stock_id} ...")
        dl = DataLoader()
        # 抓取近 3 年 (涵蓋週線需求)
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365*3 + 30)).strftime('%Y-%m-%d')
        
        df = dl.taiwan_stock_daily(stock_id=stock_id, start_date=start_date)
        
        if df.empty:
            return pd.DataFrame()
            
        # 標準化欄位
        # FinMind: date, stock_id, Trading_Volume, Trading_money, open, max, min, close, spread, Trading_turnover
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.rename(columns={
            'open': 'Open',
            'max': 'High',
            'min': 'Low',
            'close': 'Close',
            'Trading_Volume': 'Volume'
        })
        
        # 轉換型別
        cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        df = df[cols].astype(float)
        
        return df
    except Exception as e:
        print(f"❌ FinMind Download Error: {e}")
        return pd.DataFrame()

def load_and_resample(source):
    """
    智慧數據載入器：
    1. 若輸入是字串 (Ticker) -> 智慧抓取 (.TW -> .TWO -> FinMind)
    2. 若輸入是 DataFrame (CSV) -> 直接使用並自動產生週線
    """
    df_day = pd.DataFrame()
    df_week = pd.DataFrame()
    ticker_name = "Unknown"

    # 情境 A: 傳入的是股票代號 (字串)
    if isinstance(source, str):
        raw_input = source.strip()
        
        # 1. 如果是純數字，啟動智慧判斷序列
        if raw_input.isdigit():
            # 嘗試 1: .TW (上市)
            try_ticker = f"{raw_input}.TW"
            print(f"📥 嘗試下載 {try_ticker} (yfinance)...")
            df_day = yf.download(try_ticker, period='3y', interval='1d', progress=False)
            
            if df_day.empty:
                # 嘗試 2: .TWO (上櫃)
                try_ticker = f"{raw_input}.TWO"
                print(f"📥 嘗試下載 {try_ticker} (yfinance)...")
                df_day = yf.download(try_ticker, period='3y', interval='1d', progress=False)
                
            if df_day.empty:
                # 嘗試 3: FinMind (Fallback)
                print(f"⚠️ yfinance 無數據，切換至 FinMind API...")
                df_day = fetch_from_finmind(raw_input)
                ticker_name = raw_input # FinMind 只用數字
            else:
                ticker_name = try_ticker
                
        else:
            # 2. 非純數字 (如 TSM, AAPL)，直接透過 yfinance
            ticker_name = raw_input
            print(f"📥 正在下載 {ticker_name} (yfinance)...")
            df_day = yf.download(ticker_name, period='3y', interval='1d', progress=False)

    # 情境 B: 傳入的是 CSV 資料 (DataFrame)
    elif isinstance(source, pd.DataFrame):
        print(f"📂 正在處理上傳的 CSV 數據...")
        ticker_name = "Uploaded_Data"
        df_day = source.copy()
        
        # 確保 Index 是 Datetime
        if not isinstance(df_day.index, pd.DatetimeIndex):
            for col in df_day.columns:
                if 'date' in col.lower() or '時間' in col:
                    df_day[col] = pd.to_datetime(df_day[col])
                    df_day.set_index(col, inplace=True)
                    break
        
        # 確保欄位名稱標準化
        df_day.columns = [c.capitalize() for c in df_day.columns] 

    # -----------------------------------------------
    # 統一處理週線生成 (Resample)
    # -----------------------------------------------
    # yfinance 雖然可以抓 1wk，但為了與 FinMind/CSV 邏輯一致且確保能 fallback，
    # 這裡統一用日線 resample 出週線 (如果原本下載的是3年日線)
    
    if not df_day.empty:
        # 清洗 MultiIndex
        if isinstance(df_day.columns, pd.MultiIndex):
            df_day.columns = df_day.columns.get_level_values(0)

        # 自動生成週線
        logic = {
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        }
        agg_logic = {k: v for k, v in logic.items() if k in df_day.columns}
        
        df_week = df_day.resample('W-FRI').agg(agg_logic)
        
        # 切分長度: 日線只留近 1 年，週線留 3 年 (已在下載時抓了3年)
        # 注意: 為了顯示流暢，這裡只裁切 df_day 顯示用，df_week 保持完整
        # 但回傳時通常 df_day for chart 是近期的
        # 我們這裏不做破壞性裁切，只在繪圖時 tail()
        pass

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
    
    # 檢查是否有數據
    if df_day.empty:
        print("❌ 錯誤: 無法取得任何股價數據 (所有來源皆失敗)")
        return {}, {'Error': '無法取得數據，請確認代號或網路狀態'}, pd.DataFrame(), pd.DataFrame()

    print(f"🚀 啟動雙週期全方位分析引擎: {ticker}")
    
    figures = {}
    errors = {}

    # 2. 繪製週線
    if not df_week.empty:
        try:
            df_week = calculate_all_indicators(df_week)
            fig_week = plot_single_chart(ticker, df_week, "Trend (Long)", "Weekly")
            figures['Weekly'] = fig_week
        except Exception as e:
            errors['Weekly'] = f"週線計算錯誤: {e}"
            print(f"❌ 週線計算錯誤: {e}")
    else:
        errors['Weekly'] = "無週線數據"

    # 3. 繪製日線 (取最近 1 年繪圖 optimize)
    if not df_day.empty:
        try:
            # 為了運算指標精確，先算全部，再切最近1年繪圖? No, plot_single_chart handles tail.
            # But calculating indicators on 3 years of daily data is fine.
            df_day = calculate_all_indicators(df_day)
            fig_day = plot_single_chart(ticker, df_day, "Action (Short)", "Daily")
            figures['Daily'] = fig_day
        except Exception as e:
            errors['Daily'] = f"日線計算錯誤: {e}"
            print(f"❌ 日線計算錯誤: {e}")
    else:
        errors['Daily'] = "無日線數據"
        
    return figures, errors, df_week, df_day

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
