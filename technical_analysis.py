# filename: technical_analysis.py

import yfinance as yf
import mplfinance as mpf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pattern_recognition import identify_patterns

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

    # 11. 神奇九轉 (Magic Nine / TD Sequential Setup)
    # Logic: 
    #  - Buy Setup: Close < Close[4] for 9 consecutive days
    #  - Sell Setup: Close > Close[4] for 9 consecutive days
    df['TD_Buy_Setup'] = 0
    df['TD_Sell_Setup'] = 0
    
    # Vectorized approach or Loop? Loop is safer for consecutive logic.
    # Since df is typically small (< 3 years), loop is fast enough.
    buy_count = 0
    sell_count = 0
    
    # Pre-calculate Close shift 4
    close_shift_4 = df['Close'].shift(4)
    
    # 轉成 numpy 加速
    closes = df['Close'].values
    shifts = close_shift_4.values
    buy_setups = np.zeros(len(df), dtype=int)
    sell_setups = np.zeros(len(df), dtype=int)
    
    for i in range(4, len(df)):
        # Buy Setup
        if closes[i] < shifts[i]:
            buy_count += 1
        else:
            buy_count = 0
        
        # Sell Setup
        if closes[i] > shifts[i]:
            sell_count += 1
        else:
            sell_count = 0
            
        # 只要是 1~9 都記錄，方便作圖
        if buy_count > 0:
            # 只保留 1-9，超過 9 就歸零 (避免圖上出現 10, 11...)
            buy_setups[i] = buy_count if buy_count <= 9 else 0 
        if sell_count > 0:
            sell_setups[i] = sell_count if sell_count <= 9 else 0
            
    df['TD_Buy_Setup'] = buy_setups
    df['TD_Sell_Setup'] = sell_setups

    return df

# ==========================================
# 新增模組：數據載入與重採樣 (Data Loader & Resampler)
# ==========================================

from FinMind.data import DataLoader
import datetime

# ==========================================
# 新增模組：數據載入與重採樣 (Data Loader & Resampler)
# ==========================================

from FinMind.data import DataLoader
import datetime
import functools

# Global Cache for Stock Info
_TW_STOCK_INFO_CACHE = None

def get_stock_info_smart(ticker):
    """
    取得股票資訊 (名稱、產業類別)
    回傳: dict {'name': '台積電', 'sector': '半導體', ...}
    """
    global _TW_STOCK_INFO_CACHE
    meta = {'name': ticker, 'sector': '', 'currency': 'TWD'}
    
    # 清洗 Ticker 取得純數字代號
    stock_id = ticker.split('.')[0] if '.' in ticker else ticker
    
    # 1. 如果是台股 (數字)，嘗試從 FinMind 取得中文名稱
    if stock_id.isdigit():
        try:
            if _TW_STOCK_INFO_CACHE is None:
                print("📥 下載台股清單 (Cache)...")
                dl = DataLoader()
                _TW_STOCK_INFO_CACHE = dl.taiwan_stock_info()
            
            # 搜尋
            row = _TW_STOCK_INFO_CACHE[_TW_STOCK_INFO_CACHE['stock_id'] == stock_id]
            if not row.empty:
                meta['name'] = row.iloc[0]['stock_name']
                meta['sector'] = row.iloc[0]['industry_category']
        except Exception as e:
            print(f"❌ 無法取得台股資訊: {e}")

    # 2. 如果是美股 (英文)，嘗試用 yfinance (但比較慢，暫時略過或簡單處理)
    else:
        meta['currency'] = 'USD'
    
    return meta

def fetch_from_finmind(stock_id):
    """
    從 FinMind 抓取股價資料 (Fallback)
    """
    try:
        print(f"🔄 嘗試從 FinMind 抓取 {stock_id} ...")
        dl = DataLoader()
        # 抓取近 10 年 (涵蓋週線需求)
        start_date = '2016-01-01'
        
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

def load_and_resample(source, force_update=False):
    """
    智慧數據載入器：
    1. 若輸入是字串 (Ticker) -> 智慧抓取 (.TW -> .TWO -> FinMind)
    2. 若輸入是 DataFrame (CSV) -> 直接使用並自動產生週線
    """
    df_day = pd.DataFrame()
    df_week = pd.DataFrame()
    ticker_name = "Unknown"
    stock_meta = {'name': 'Unknown', 'sector': '', 'currency': ''}

    # 情境 A: 傳入的是股票代號 (字串)
    if isinstance(source, str):
        raw_input = source.strip()
        
        # [CACHE] Initialize Cache Manager
        from cache_manager import CacheManager
        cm = CacheManager()
        
        # 1. 嘗試讀取快取 (Price Data)
        cached_df, status, last_date = cm.load_cache(raw_input, 'price', force_reload=force_update)
        
        if status == "hit" and not cached_df.empty:
            print(f"⚡ [Cache Hit] 讀取 {raw_input} 本地快取")
            df_day = cached_df
            ticker_name = raw_input
            stock_meta = get_stock_info_smart(ticker_name)
            
        elif status == "partial" and not cached_df.empty:
            print(f"🔄 [Incremental] 發現舊資料 (至 {last_date.date()})，正在更新缺少部分...")
            ticker_name = raw_input
            
            # Start from next day
            start_date_new = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Download only new data
            new_df = pd.DataFrame()
            try:
                if raw_input.isdigit():
                     try_ticker = f"{raw_input}.TW"
                     new_df = yf.download(try_ticker, start=start_date_new, interval='1d', progress=False, auto_adjust=False)
                     if new_df.empty:
                         try_ticker = f"{raw_input}.TWO"
                         new_df = yf.download(try_ticker, start=start_date_new, interval='1d', progress=False, auto_adjust=False)
                     if new_df.empty:
                          # Fallback FinMind
                          dl = DataLoader()
                          new_df = dl.taiwan_stock_daily(stock_id=raw_input, start_date=start_date_new)
                          # Normalize FinMind if needed (reuse fetch implementation logic or just trust yf for now)
                          # To be safe, let's just use the full fetch logic if yf fails, or assume yf works.
                          # Limitation: FinMind fetcher here returns different columns, need standardization.
                          # Simplification: If yfinance works, use it.
                else:
                     new_df = yf.download(raw_input, start=start_date_new, interval='1d', progress=False, auto_adjust=False)
            except Exception as e:
                print(f"⚠️ 增量更新失敗 ({e})，將嘗試完整重抓...")
                status = "miss" # Toggle to miss to trigger full re-download
            
            if not new_df.empty:
                 # Standardize Index Name
                 # yf download might have timezone, cache usually doesn't.
                 # Ensure consistency?
                 if isinstance(new_df.columns, pd.MultiIndex):
                     new_df.columns = new_df.columns.get_level_values(0)
                 
                 # Concat
                 # Ensure no duplicates
                 df_day = pd.concat([cached_df, new_df])
                 df_day = df_day[~df_day.index.duplicated(keep='last')]
                 df_day.sort_index(inplace=True)
                 
                 print(f"✅ 增量更新完成，新增 {len(new_df)} 筆資料")
                 ticker_name = raw_input if raw_input.isdigit() else raw_input # Simple fix
                 if raw_input.isdigit():
                      stock_meta = get_stock_info_smart(raw_input) 
                 else:
                      stock_meta['name'] = ticker_name

                 # Save merged Cache
                 cm.save_cache(raw_input, df_day, 'price')
            else:
                 # No new data found (maybe holiday), trust cache
                 print(f"✅ 無新資料 (可能是假日)，使用快取數據")
                 df_day = cached_df
                 ticker_name = raw_input
                 stock_meta = get_stock_info_smart(ticker_name)
                 
        if df_day.empty: # Either status="miss" or partial failed catastrophically
            # Cache Miss - Start Download
            # 1. 如果是純數字，啟動智慧判斷序列
            if raw_input.isdigit():
                # 嘗試 1: .TW (上市)
                try_ticker = f"{raw_input}.TW"
                print(f"📥 嘗試下載 {try_ticker} (yfinance)...")
                df_day = yf.download(try_ticker, period='10y', interval='1d', progress=False, auto_adjust=False)
                
                if df_day.empty:
                    # 嘗試 2: .TWO (上櫃)
                    try_ticker = f"{raw_input}.TWO"
                    print(f"📥 嘗試下載 {try_ticker} (yfinance)...")
                    df_day = yf.download(try_ticker, period='10y', interval='1d', progress=False, auto_adjust=False)
                    
                if df_day.empty:
                    # 嘗試 3: FinMind (Fallback)
                    print(f"⚠️ yfinance 無數據，切換至 FinMind API...")
                    df_day = fetch_from_finmind(raw_input)
                    ticker_name = raw_input # FinMind 只用數字
                else:
                    ticker_name = try_ticker
                    
                # 取得台股中文資訊
                stock_meta = get_stock_info_smart(ticker_name)
    
            else:
                # 2. 非純數字 (如 TSM, AAPL)，直接透過 yfinance
                ticker_name = raw_input
                print(f"📥 正在下載 {ticker_name} (yfinance)...")
                df_day = yf.download(ticker_name, period='10y', interval='1d', progress=False, auto_adjust=False)
                stock_meta['name'] = ticker_name
            
            # [CACHE] Save to Cache
            if not df_day.empty:
                 cm.save_cache(raw_input, df_day, 'price')

    # 情境 B: 傳入的是 CSV 資料 (DataFrame)
    elif isinstance(source, pd.DataFrame):
        print(f"📂 正在處理上傳的 CSV 數據...")
        ticker_name = "Uploaded_Data"
        df_day = source.copy()
        stock_meta['name'] = "CSV Data"
        
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
    
    if not df_day.empty:
        # 清洗 MultiIndex
        if isinstance(df_day.columns, pd.MultiIndex):
            df_day.columns = df_day.columns.get_level_values(0)

        # [Defensive] Ensure Index is DatetimeIndex (Crucial for Cache Loaded Data)
        if not isinstance(df_day.index, pd.DatetimeIndex):
            print("⚠️ Index is NOT DatetimeIndex. Attempting conversion...")
            try:
                # Force Coerce
                df_day.index = pd.to_datetime(df_day.index, errors='coerce')
                print(f"✅ Conversion Result: {type(df_day.index)}")
                
                # Drop NaT (invalid dates)
                if df_day.index.isna().any():
                     print("⚠️ Found NaT in index after conversion. Dropping...")
                     df_day = df_day[df_day.index.notna()]
                     
            except Exception as e:
                print(f"❌ Index conversion failed: {e}")
                raise ValueError(f"Index conversion failed: {e}")

        # [Defensive] Ensure Columns are Numeric
        cols_to_numeric = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in cols_to_numeric:
            if col in df_day.columns:
                try:
                    df_day[col] = pd.to_numeric(df_day[col], errors='coerce')
                except Exception as e:
                    print(f"⚠️ Column {col} conversion failed: {e}")
        
        # Clean up any rows that became NaN after coercion
        if df_day[cols_to_numeric].isna().any().any():
            print("⚠️ Found NaN values after numeric conversion. Dropping...")
            df_day = df_day.dropna(subset=cols_to_numeric)

        # 自動生成週線
        logic = {
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        }
        agg_logic = {k: v for k, v in logic.items() if k in df_day.columns}
        
        df_week = df_day.resample('W-FRI').agg(agg_logic)

    return ticker_name, df_day, df_week, stock_meta

# ==========================================
# 修改後的主程式：支援 CSV 與 Ticker
# ==========================================

def plot_dual_timeframe(source, force_update=False):
    """
    主程式：接受 '代號' 或 'DataFrame' 進行雙週期分析
    """
    # 1. 呼叫智慧載入器
    ticker, df_day, df_week, stock_meta = load_and_resample(source, force_update=force_update)
    
    # 檢查是否有數據
    if df_day.empty:
        print("❌ 錯誤: 無法取得任何股價數據 (所有來源皆失敗)")
        return {}, {'Error': '無法取得數據，請確認代號或網路狀態'}, pd.DataFrame(), pd.DataFrame(), {}

    print(f"🚀 啟動雙週期全方位分析引擎: {ticker}")
    
    figures = {}
    errors = {}

    # 2. 繪製週線
    if not df_week.empty:
        try:
            df_week = calculate_all_indicators(df_week)
            fig_week = plot_interactive_chart(ticker, df_week, "Trend (Long)", "Weekly")
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
            fig_day = plot_interactive_chart(ticker, df_day, "Action (Short)", "Daily")
            figures['Daily'] = fig_day
        except Exception as e:
            errors['Daily'] = f"日線計算錯誤: {e}"
            print(f"❌ 日線計算錯誤: {e}")
    else:
        errors['Daily'] = "無日線數據"
        
    return figures, errors, df_week, df_day, stock_meta

def plot_single_chart(ticker, df, title_suffix, timeframe_label):
    """繪製單張圖表 (包含 5 個面板)"""
    
    # 裁切數據: 週線看 100 根 (約2年), 日線看 120 根 (約半年)
    bars = 100 if timeframe_label == 'Weekly' else 120
    plot_df = df.tail(bars).copy()

    # 設定面板 (Subplots)
    apds = []

    # Helper: 安全添加 plot 的小函數
    def add_plot_safe(name, series, **kwargs):
        try:
            # 檢查是否全為 NaN
            vals = series.values
            if pd.isna(vals).all():
                # print(f"DEBUG: Skipping {name} (All NaN)")
                return

            apds.append(mpf.make_addplot(series, **kwargs))
        except Exception as e:
            print(f"❌ Error adding plot {name}: {e}")
            
    # Panel 0: 主圖
    add_plot_safe("MA_Lines", plot_df[['MA5', 'MA10', 'MA20']], width=1.0)
    add_plot_safe("MA60", plot_df['MA60'], color='black', width=1.5)
    add_plot_safe("BB_Up", plot_df['BB_Up'], color='gray', linestyle='--', alpha=0.5)
    add_plot_safe("BB_Lo", plot_df['BB_Lo'], color='gray', linestyle='--', alpha=0.5)
    # add_plot_safe("Tenkan", plot_df['Tenkan'], color='cyan', linestyle=':', width=0.8) # Removed per user request
    # add_plot_safe("Kijun", plot_df['Kijun'], color='brown', linestyle=':', width=0.8) # Removed per user request
    add_plot_safe("ATR_Stop", plot_df['ATR_Stop'], color='purple', type='scatter', markersize=6, marker='_')

    # Magic Nine (TD Sequential) Markers
    # 只標示 "9" 轉折點
    # Buy Setup 9: 出現在低檔，提示買進 (marker='^', color='red')
    # Sell Setup 9: 出現在高檔，提示賣出 (marker='v', color='green')
    
    # 製作只包含 9 的 Series，其餘 NaN
    td_buy_9 = plot_df['TD_Buy_Setup'].apply(lambda x: x if x == 9 or x == 13 else np.nan) # 9 or 13
    td_sell_9 = plot_df['TD_Sell_Setup'].apply(lambda x: x if x == 9 or x == 13 else np.nan)
    
    # 為了位置好看，Buy 9 畫在 Low 下方，Sell 9 畫在 High 上方
    td_buy_vals = plot_df['Low'] * 0.99
    td_buy_vals = td_buy_vals.where(td_buy_9.notna(), np.nan)
    
    td_sell_vals = plot_df['High'] * 1.01
    td_sell_vals = td_sell_vals.where(td_sell_9.notna(), np.nan) # Fix: Apply filter!

    # Restore missing plot calls
    add_plot_safe("TD_Buy_9", td_buy_vals, type='scatter', markersize=15, marker='^', color='red')
    add_plot_safe("TD_Sell_9", td_sell_vals, type='scatter', markersize=15, marker='v', color='green')
    
    # 檢查成交量是否有效
    use_volume = True
    if 'Volume' not in plot_df.columns:
        use_volume = False
    else:
        vol_clean = plot_df['Volume'].fillna(0)
        if (vol_clean == 0).all():
            print("⚠️ 偵測到無效成交量 (全為0)，將隱藏 Volume 面板")
            use_volume = False

    if len(plot_df) < 2:
        raise ValueError("數據行數不足，無法繪圖 (Less than 2 rows)")

    fig, axes = mpf.plot(plot_df, type='candle', addplot=apds, 
             volume=use_volume, 
             returnfig=True)
    
    # --------------------------------------------------------
    # 手動標註 Magic Nine 數字 (6, 7, 8, 9)
    # mplfinance 的 x 軸在 candle 模式下是 0, 1, 2... 的整數序列
    # --------------------------------------------------------
    ax_main = axes[0]
    
    # 預先取得欄位以免一直 access
    td_buys = plot_df['TD_Buy_Setup'].values
    td_sells = plot_df['TD_Sell_Setup'].values
    lows = plot_df['Low'].values
    highs = plot_df['High'].values
    
    for i in range(len(plot_df)):
        b_val = td_buys[i]
        if b_val >= 6:
            # 畫在 Low 下方一點點
            label = str(int(b_val))
            # 微調位置: Low * 0.98 (如果是 9 號有三角形，避開一下)
            pos_y = lows[i] * 0.995 if b_val != 9 else lows[i] * 0.98 
            ax_main.text(i, pos_y, label, 
                         color='red', fontsize=6, 
                         ha='center', va='top', fontweight='bold')
                         
        # Sell Setup >= 6
        s_val = td_sells[i]
        if s_val >= 6:
            # 畫在 High 上方一點點
            label = str(int(s_val))
            pos_y = highs[i] * 1.005 if s_val != 9 else highs[i] * 1.02
            ax_main.text(i, pos_y, label, 
                         color='green', fontsize=6, 
                         ha='center', va='bottom', fontweight='bold')

    return fig

def plot_interactive_chart(ticker, df, title_suffix, timeframe_label):
    """
    使用 Plotly 繪製互動式 K 線圖 (含成交量、指標)
    """
    # 裁切數據
    bars = 100 if timeframe_label == 'Weekly' else 120
    plot_df = df.tail(bars).copy()
    
    # [FIX] 1. Remove empty rows (essential for Weekly resampled data) to prevents gaps
    plot_df = plot_df.dropna(subset=['Close'])
    
    # [FIX] 2. Format Date Index to String (YYYY-MM-DD) removes HH:MM:SS
    if isinstance(plot_df.index, pd.DatetimeIndex):
        plot_df.index = plot_df.index.strftime('%Y-%m-%d')

    # Check volume
    use_volume = True
    if 'Volume' not in plot_df.columns:
        use_volume = False
    else:
        vol_check = plot_df['Volume'].fillna(0)
        if (vol_check == 0).all():
            use_volume = False

    # Create Subplots
    rows = 2 if use_volume else 1
    row_heights = [0.7, 0.3] if use_volume else [1.0]
    vertical_spacing = 0.03

    fig = make_subplots(
        rows=rows, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=vertical_spacing,
        row_heights=row_heights,
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]] if use_volume else [[{"secondary_y": False}]]
    )

    # 1. Candlestick (K線)
    # Colors: Up=Red, Down=Green (Taiwan Style)
    # Plotly default: increasing.line.color, decreasing.line.color
    
    # Create Custom Hover Text (to force Chinese labels in Unified Mode)
    # Unified mode usually ignores hovertemplate header/labels, so we pass the whole block as 'text'
    custom_hover_text = [
        f"日期: {idx}<br>開盤: {o:.2f}<br>最高: {h:.2f}<br>最低: {l:.2f}<br>收盤: {c:.2f}"
        for idx, o, h, l, c in zip(plot_df.index, plot_df['Open'], plot_df['High'], plot_df['Low'], plot_df['Close'])
    ]

    fig.add_trace(go.Candlestick(
        x=plot_df.index,
        open=plot_df['Open'],
        high=plot_df['High'],
        low=plot_df['Low'],
        close=plot_df['Close'],
        name='K線',
        increasing_line_color='red', 
        decreasing_line_color='green',
        increasing_fillcolor='red', # Optional: fill body
        decreasing_fillcolor='green',
        hoverinfo='skip' # [FIX] Hide default English labels completely
    ), row=1, col=1)

    # [FIX] Add invisible Scatter trace to provide the custom localized hover info
    # This works better with 'unified' hovermode than trying to override Candlestick
    fig.add_trace(go.Scatter(
        x=plot_df.index,
        y=plot_df['Close'], # Follow Close price for position
        mode='markers',
        marker=dict(opacity=0), # Invisible
        name='K線', # Header in unified box
        text=custom_hover_text,
        hovertemplate="%{text}<extra></extra>", # Only show custom text
        showlegend=False
    ), row=1, col=1)

    # 2. Indicators (MA, BB, etc.) - Only add if they exist
    
    # MA Lines
    colors = {'MA5': 'blue', 'MA10': 'orange', 'MA20': 'purple', 'MA60': 'black', 'MA120': 'gray', 'MA240': 'brown'}
    for ma_name, color in colors.items():
        if ma_name in plot_df.columns:
            fig.add_trace(go.Scatter(
                x=plot_df.index, y=plot_df[ma_name],
                mode='lines', name=ma_name,
                line=dict(color=color, width=1),
                hovertemplate=f'{ma_name}: %{{y:.2f}}<extra></extra>',
                connectgaps=True # [FIX] Bridge gaps
            ), row=1, col=1)

    # Bollinger Bands
    if 'BB_Up' in plot_df.columns and 'BB_Lo' in plot_df.columns:
        fig.add_trace(go.Scatter(
            x=plot_df.index, y=plot_df['BB_Up'],
            mode='lines', name='BB_Up',
            line=dict(color='red', width=1.5), # Solid, Red, Thicker
            hovertemplate='BB_Up: %{y:.2f}<extra></extra>',
            showlegend=True,
            connectgaps=True # [FIX] Bridge gaps
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=plot_df.index, y=plot_df['BB_Lo'],
            mode='lines', name='BB_Lo',
            line=dict(color='green', width=1.5), # Solid, Green, Thicker
            hovertemplate='BB_Lo: %{y:.2f}<extra></extra>',
            showlegend=True,
            connectgaps=True # [FIX] Bridge gaps
        ), row=1, col=1)

    # Ichimoku Removed per user request
    # if 'Tenkan' in plot_df.columns: ...

    # ATR Stop
    if 'ATR_Stop' in plot_df.columns:
        fig.add_trace(go.Scatter(
            x=plot_df.index, y=plot_df['ATR_Stop'],
            mode='markers', name='ATR_Stop',
            marker=dict(symbol='line-ew', color='purple', size=10, line=dict(width=2)),
            hovertemplate='ATR_Stop: %{y:.2f}<extra></extra>',
            visible='legendonly' # Hidden by default per user request
        ), row=1, col=1)
    
    # 3. Magic Nine Markers
    if 'TD_Buy_Setup' in plot_df.columns:
        # Buy 9 (Triangle Up, Red)
        buy_mask = (plot_df['TD_Buy_Setup'] == 9) | (plot_df['TD_Buy_Setup'] == 13)
        buy_pts = plot_df[buy_mask]
        if not buy_pts.empty:
            fig.add_trace(go.Scatter(
                x=buy_pts.index, y=buy_pts['Low'] * 0.99,
                mode='markers+text', name='TD Buy 9',
                marker=dict(symbol='triangle-up', color='red', size=10),
                text=buy_pts['TD_Buy_Setup'].astype(str),
                textposition="bottom center"
            ), row=1, col=1)

    if 'TD_Sell_Setup' in plot_df.columns:
        # Sell 9 (Triangle Down, Green)
        sell_mask = (plot_df['TD_Sell_Setup'] == 9) | (plot_df['TD_Sell_Setup'] == 13)
        sell_pts = plot_df[sell_mask]
        if not sell_pts.empty:
            fig.add_trace(go.Scatter(
                x=sell_pts.index, y=sell_pts['High'] * 1.01,
                mode='markers+text', name='TD Sell 9',
                marker=dict(symbol='triangle-down', color='green', size=10),
                text=sell_pts['TD_Sell_Setup'].astype(str),
                textposition="top center"
            ), row=1, col=1)

    # 4. Volume Chart
    if use_volume:
        # Color based on Close change (Standard Taiwan: Red if Close > Open, Green otherwise. 
        # Actually usually Close > PrevClose, but candlestick convention is often Close > Open)
        # Check standard: usually matches candle color.
        colors_vol = ['red' if row['Close'] >= row['Open'] else 'green' for i, row in plot_df.iterrows()]
        
        fig.add_trace(go.Bar(
            x=plot_df.index, 
            y=plot_df['Volume'],
            name='成交量',
            marker_color=colors_vol
        ), row=2, col=1)

    # ----------------------------------------------------
    # 7. Candlestick Patterns (Markers)
    # ----------------------------------------------------
    try:
        pat_df = identify_patterns(plot_df)
        # Filter where patterns are found
        bullish_pats = pat_df[pat_df['Pattern_Type'] == 'Bullish']
        bearish_pats = pat_df[pat_df['Pattern_Type'] == 'Bearish']
        neutral_pats = pat_df[pat_df['Pattern_Type'] == 'Neutral']
        
        # Bullish Markers (Purple Triangle Up)
        if not bullish_pats.empty:
            # Align with original df index to get Low prices
            bull_marker_y = plot_df.loc[bullish_pats.index, 'Low'] * 0.99
            
            fig.add_trace(go.Scatter(
                x=bullish_pats.index,
                y=bull_marker_y,
                mode='markers',
                marker=dict(symbol='triangle-up', size=8, color='purple'),
                name='Bullish Pattern',
                text=bullish_pats['Pattern'],
                hovertemplate='%{x}<br>Pattern: %{text}<extra></extra>'
            ), row=1, col=1)

        # Bearish Markers (Orange Triangle Down)
        if not bearish_pats.empty:
             bear_marker_y = plot_df.loc[bearish_pats.index, 'High'] * 1.01
             
             fig.add_trace(go.Scatter(
                x=bearish_pats.index,
                y=bear_marker_y,
                mode='markers',
                marker=dict(symbol='triangle-down', size=8, color='orange'),
                name='Bearish Pattern',
                text=bearish_pats['Pattern'],
                hovertemplate='%{x}<br>Pattern: %{text}<extra></extra>'
            ), row=1, col=1)

        # Neutral Markers (Yellow Diamond)
        if not neutral_pats.empty:
             neutral_marker_y = plot_df.loc[neutral_pats.index, 'High'] * 1.01 # Slightly above high like bearish? or on top?
             # Let's put equal to High * 1.005
             neutral_marker_y = plot_df.loc[neutral_pats.index, 'High'] * 1.005
             
             fig.add_trace(go.Scatter(
                x=neutral_pats.index,
                y=neutral_marker_y,
                mode='markers',
                marker=dict(symbol='diamond', size=6, color='yellow'),
                name='Neutral Pattern',
                text=neutral_pats['Pattern'],
                hovertemplate='%{x}<br>Pattern: %{text}<extra></extra>'
            ), row=1, col=1)
            
    except Exception as e:
        print(f"Pattern Warning: {e}")

    # Layout Configuration
    fig.update_layout(
        title=dict(text=f"{ticker} - {title_suffix} ({timeframe_label})", x=0.5), # Centered title
        xaxis_rangeslider_visible=False,
        hovermode='x unified', # Reverted to unified for single-box UI
        height=600 if use_volume else 450,
        margin=dict(l=50, r=50, t=50, b=50),
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02)
    )
    
    # Enable Spikelines for better "Ruler" feel in standard hover mode
    fig.update_xaxes(showspikes=True, spikemode='across', spikesnap='cursor', showline=True, spikedash='dash')

    # Y-axis formatting
    fig.update_yaxes(title_text="Price", row=1, col=1)
    if use_volume:
        fig.update_yaxes(title_text="Volume", row=2, col=1)
    
    # Remove weekends gaps (Optional - Plotly default treats x as continuous time if it detects dates)
    # Depending on preference, one might want to treat it as category to remove gaps.
    # But that messes up ticks sometimes. 
    # Providing a rangebreaks arg is the standard Plotly way for stocks.
    
    # 2025-12-27 User Request: Fix gaps. Use Category Axis.
    # We must ensure x-axis is discrete categories (Strings) to strictly remove gaps.
    # Plotly 'category' axis treats every point as equal width.
    # Update: Format index to strings for cleaner hover
    # plot_df.index = plot_df.index.strftime('%Y-%m-%d') # Don't mutate original DF index globally if reused?
    # Actually plot_df is a copy (.tail().copy()), so it's safe.
    
    fig.update_xaxes(
        type='category', 
        tickmode='auto',
        nticks=20 # Limit ticks to avoid clutter
    )

    return fig

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
