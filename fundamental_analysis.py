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
            'Business Summary': info.get('longBusinessSummary', 'No summary available.'),
            # Placeholder for new fields
            'Revenue YoY': 'N/A',
            'Monthly Revenue': 'N/A',
            'Cash Dividend': 'N/A', 
            'Stock Dividend': 'N/A',
            'Payout Ratio': 'N/A'
        }

        # [PATCH] 如果是台股，嘗試用 FinMind 覆蓋數據
        if is_tw_stock:
             tw_data = get_taiwan_stock_fundamentals(stock_id)
             if tw_data:
                 print(f"✅ 使用 FinMind 數據覆蓋台股基本面: {stock_id}")
                 if tw_data.get('PE Ratio') != 'N/A': data['PE Ratio'] = tw_data['PE Ratio']
                 if tw_data.get('PE Ratio') != 'N/A': data['PE Ratio'] = tw_data['PE Ratio']
                 if tw_data.get('PB Ratio') != 'N/A': data['PB Ratio'] = tw_data['PB Ratio']
                 if tw_data.get('Dividend Yield') != 'N/A': 
                     data['Dividend Yield'] = f"{tw_data['Dividend Yield']:.2f}%" # Format here
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
                          
                 # [PATCH] 新增：月營收成長率
                 rev_data = get_taiwan_stock_revenue(stock_id)
                 if rev_data:
                     data['Monthly Revenue'] = f"{rev_data['revenue']:,.0f} (M)"
                     data['Revenue YoY'] = f"{rev_data['yoy']:.2f}%"
                     
                 # [PATCH] 新增：股利政策詳情
                 div_data = get_taiwan_stock_dividend_policy(stock_id)
                 if div_data:
                     data['Cash Dividend'] = f"{div_data['cash']:.2f}"
                     data['Stock Dividend'] = f"{div_data['stock']:.2f}"
                     # Payout Ratio? Need EPS.
                     try:
                         eps_val = float(data['EPS (TTM)'].split()[0])
                         total_div = div_data['cash'] + div_data['stock']
                         if eps_val > 0:
                             payout = (total_div / eps_val) * 100
                             data['Payout Ratio'] = f"{payout:.1f}%"
                     except:
                         pass

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
        # 取最近 365 天數據確保有收盤與殖利率資料 (有些冷門股可能交易少)
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        
        df = dl.taiwan_stock_per_pbr(stock_id=stock_id, start_date=start_date)
        
        if df.empty:
            return {}
            
        # 取最新一筆
        latest = df.iloc[-1]
        
        return {
            'PE Ratio': f"{latest['PER']:.2f}" if latest['PER'] > 0 else "N/A",
            'PB Ratio': f"{latest['PBR']:.2f}" if latest['PBR'] > 0 else "N/A",
            'Dividend Yield': latest['dividend_yield'] # Return raw float for formatting
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

def get_taiwan_stock_revenue(stock_id):
    """
    從 FinMind 取得最近一月營收與年增率 (taiwan_stock_month_revenue)
    """
    try:
        dl = DataLoader()
        # 抓取近 90 天 (確保有上個月資料)
        start_date = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
        df = dl.taiwan_stock_month_revenue(stock_id=stock_id, start_date=start_date)
        
        if not df.empty:
            latest = df.iloc[-1]
            # rev in millions? No, unit is usually raw int
            rev_val = latest['revenue'] / 1_000_000 # Convert to Millions
            return {
                'revenue': rev_val,
                'yoy': latest['revenue_year_growth']
            }
    except Exception as e:
        print(f"Revenue Error: {e}")
    return None

def get_taiwan_stock_dividend_policy(stock_id):
    """
    從 FinMind 取得最近一年股利政策 (taiwan_stock_dividend)
    """
    try:
        dl = DataLoader()
        # 股利通常一年一次，抓 2 年確保有資料
        start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime('%Y-%m-%d')
        df = dl.taiwan_stock_dividend(stock_id=stock_id, start_date=start_date)
        
        if not df.empty:
            # Sort by date
            df['date'] = pd.to_datetime(df['date'])
            df.sort_values('date', inplace=True)
            latest = df.iloc[-1]
            
            return {
                'cash': latest.get('CashEarningsDistribution', 0),
                'stock': latest.get('StockEarningsDistribution', 0)
            }
    except Exception as e:
        print(f"Dividend Error: {e}")
    return None
