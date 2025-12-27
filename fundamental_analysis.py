import yfinance as yf
import pandas as pd
import datetime
from FinMind.data import DataLoader

def get_fundamentals(ticker):
    """
    Fetch fundamental data for a given ticker.
    Returns a dictionary of metrics.
    """
    try:
        # Ticker Correction for Taiwan Stocks
        # yfinance needs "2330.TW", but user might input "2330"
        search_ticker = ticker
        if ticker.isdigit():
            search_ticker = f"{ticker}.TW"
            
        stock = yf.Ticker(search_ticker)
        info = stock.info
        
        # 判斷是否為台股 (FinMind 支援)
        is_tw_stock = False
        stock_id = ""
        
        if ticker.isdigit(): # e.g. 2330
            is_tw_stock = True
            stock_id = ticker
        elif ".TW" in ticker.upper(): # e.g. 2330.TW
            stock_id = ticker.split('.')[0]
            if stock_id.isdigit():
                 is_tw_stock = True
        
        # Helper to safely get value or 'N/A'
        def get_val(key, fmt="{:.2f}"):
            val = info.get(key)
            if val is None:
                return "N/A"
            if isinstance(val, (int, float)):
                return fmt.format(val)
            return val

        data = {
            'Market Cap': get_val('marketCap', "{:,.0f}"),
            'PE Ratio': get_val('trailingPE'),
            'Forward PE': get_val('forwardPE'),
            'EPS (TTM)': get_val('trailingEps'),
            'PEG Ratio': get_val('pegRatio'),
            'PB Ratio': get_val('priceToBook'),
            'Dividend Yield': "N/A",
            'Sector': info.get('sector', 'N/A'),
            'Industry': info.get('industry', 'N/A'),
            'Website': info.get('website', 'N/A'),
            'Business Summary': info.get('longBusinessSummary', 'No summary available.')
        }

        # [PATCH] 如果是台股，嘗試用 FinMind 覆蓋數據
        if is_tw_stock:
             tw_data = get_taiwan_stock_fundamentals(stock_id)
             if tw_data:
                 print(f"✅ 使用 FinMind 數據覆蓋台股基本面: {stock_id}")
                 if tw_data.get('PE Ratio') != 'N/A': data['PE Ratio'] = tw_data['PE Ratio']
                 if tw_data.get('PB Ratio') != 'N/A': data['PB Ratio'] = tw_data['PB Ratio']
                 # 嘗試計算 EPS (Close / PE)
                 try:
                     close_price = info.get('currentPrice') or info.get('previousClose')
                     pe_val = float(tw_data['PE Ratio'])
                     if close_price and pe_val > 0:
                         eps_est = close_price / pe_val
                         data['EPS (TTM)'] = f"{eps_est:.2f} (Est.)"
                 except:
                     pass
                     
                 # [PATCH] 嘗試補全 Profile (Sector/Industry)
                 if data['Sector'] == 'N/A':
                      profile = get_taiwan_stock_profile(stock_id)
                      if profile:
                          data['Sector'] = profile.get('sector', 'N/A')
                          data['Industry'] = profile.get('industry', 'N/A')

        # Handle Dividend Yield (yfinance returns decimal like 0.015 for 1.5%)

        # Handle Dividend Yield (yfinance returns percentage e.g. 1.36 for 1.36%)
        # Note: Previous assumption of decimal (0.0136) was wrong based on debug.
        dy = info.get('dividendYield')
        if dy is not None:
             data['Dividend Yield'] = f"{dy:.2f}%"

        # Handle ROE (Return on Equity)
        roe = info.get('returnOnEquity')
        if roe:
             data['ROE'] = f"{roe*100:.2f}%"
        else:
             data['ROE'] = "N/A"
             
        # Handle Profit Margin
        pm = info.get('profitMargins')
        if pm:
            data['Profit Margin'] = f"{pm*100:.2f}%"
        else:
            data['Profit Margin'] = "N/A"

        return data
    except Exception as e:
        print(f"Error fetching fundamentals for {ticker}: {e}")
        return None

def get_taiwan_stock_fundamentals(stock_id):
    """
    從 FinMind 取得台股基本面數據 (PER, PBR, Yield)
    """
    try:
        dl = DataLoader()
        # 取最近 10 天數據確保有最新值
        start_date = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
        
        df = dl.taiwan_stock_per_pbr(stock_id=stock_id, start_date=start_date)
        
        if df.empty:
            return {}
            
        # 取最新一筆
        latest = df.iloc[-1]
        
        return {
            'PE Ratio': f"{latest['PER']:.2f}" if latest['PER'] > 0 else "N/A",
            'PB Ratio': f"{latest['PBR']:.2f}" if latest['PBR'] > 0 else "N/A",
            'Dividend Yield': f"{latest['dividend_yield']:.2f}%" if latest['dividend_yield'] > 0 else "N/A"
        }
    except Exception as e:
        print(f"FinMind Error: {e}")
        return {}

def get_taiwan_stock_profile(stock_id):
    """
    從 FinMind 取得產業類別 (Fallback)
    """
    try:
        dl = DataLoader()
        df = dl.taiwan_stock_info()
        row = df[df['stock_id'] == stock_id]
        if not row.empty:
            return {
                'sector': row.iloc[0]['industry_category'],
                'industry': row.iloc[0]['industry_category'] # FinMind usually groups them
            }
    except:
        pass
    return None
