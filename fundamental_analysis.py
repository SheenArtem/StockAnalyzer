import yfinance as yf
import pandas as pd
import datetime
import logging
from cache_manager import get_finmind_loader

logger = logging.getLogger(__name__)


def _get_data_loader():
    return get_finmind_loader()


# TradingView data cache
_tv_cache = {}
_tv_cache_ttl = 3600  # 1 hour

def _get_tradingview_fundamentals(ticker):
    """
    Fetch fundamental data from TradingView Screener API.
    Works for both TW and US stocks. Free, no token needed.

    Returns:
        dict or None: {gross_margin, operating_margin, net_margin, ROE, ROA, ...}
    """
    import time
    cache_key = f"tv_{ticker}"
    if cache_key in _tv_cache:
        data, ts = _tv_cache[cache_key]
        if time.time() - ts < _tv_cache_ttl:
            return data

    try:
        from tradingview_screener import Query, Column

        # Determine market
        clean = ticker.replace('.TW', '').replace('.TWO', '')
        is_us = not clean.isdigit()

        market = 'america' if is_us else 'taiwan'
        search_name = ticker if is_us else clean

        result = (Query()
            .select('name', 'description', 'close',
                    'gross_margin', 'operating_margin', 'net_margin',
                    'return_on_equity', 'return_on_assets',
                    'total_revenue_yoy_growth_fq',
                    'debt_to_equity', 'market_cap_basic')
            .set_markets(market)
            .where(Column('name') == search_name)
            .limit(1)
            .get_scanner_data()
        )

        if result[0] == 0:
            _tv_cache[cache_key] = (None, time.time())
            return None

        row = result[1].iloc[0]
        data = {
            'description': row.get('description', ''),
            'gross_margin': row.get('gross_margin'),
            'operating_margin': row.get('operating_margin'),
            'net_margin': row.get('net_margin'),
            'ROE': row.get('return_on_equity'),
            'ROA': row.get('return_on_assets'),
            'revenue_yoy': row.get('total_revenue_yoy_growth_fq'),
            'debt_to_equity': row.get('debt_to_equity'),
            'market_cap': row.get('market_cap_basic'),
        }
        # Remove NaN values
        data = {k: v for k, v in data.items() if v is not None and (not isinstance(v, float) or not pd.isna(v))}

        _tv_cache[cache_key] = (data, time.time())
        logger.info("TradingView data fetched for %s: %d fields", ticker, len(data))
        return data

    except ImportError:
        logger.debug("tradingview-screener not installed")
        return None
    except Exception as e:
        logger.warning("TradingView data fetch failed for %s: %s", ticker, e)
        _tv_cache[cache_key] = (None, time.time())
        return None


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
                 except Exception as e:
                     logger.debug(f"Failed to estimate EPS from PE for {stock_id}: {e}")
                     
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
                     except Exception as e:
                         logger.debug(f"Failed to calculate payout ratio for {stock_id}: {e}")

        # Handle Dividend Yield (yfinance returns decimal, e.g. 0.0136 for 1.36%)
        # 與 ROE/profitMargins 一致，統一 *100 轉為百分比
        dy = info.get('dividendYield')
        if dy is not None:
             data['Dividend Yield'] = f"{dy*100:.2f}%"

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

        # [TradingView] 補充三率/ROE/ROA — 統一資料源，台股美股都用
        # (yfinance 台股常回 N/A，TradingView 更完整)
        try:
            tv_data = _get_tradingview_fundamentals(ticker)
            if tv_data:
                # 只補缺的，不覆蓋已有的
                if data.get('ROE') == 'N/A' and tv_data.get('ROE'):
                    data['ROE'] = f"{tv_data['ROE']:.2f}%"
                if data.get('Profit Margin') == 'N/A' and tv_data.get('net_margin'):
                    data['Profit Margin'] = f"{tv_data['net_margin']:.2f}%"
                # 新增欄位（之前完全沒有）
                if tv_data.get('gross_margin'):
                    data['Gross Margin'] = f"{tv_data['gross_margin']:.2f}%"
                if tv_data.get('operating_margin'):
                    data['Operating Margin'] = f"{tv_data['operating_margin']:.2f}%"
                if tv_data.get('net_margin'):
                    data['Net Margin'] = f"{tv_data['net_margin']:.2f}%"
                if tv_data.get('ROA'):
                    data['ROA'] = f"{tv_data['ROA']:.2f}%"
                if tv_data.get('debt_to_equity'):
                    data['Debt/Equity'] = f"{tv_data['debt_to_equity']:.2f}"
                if tv_data.get('revenue_yoy') and data.get('Revenue YoY') == 'N/A':
                    data['Revenue YoY'] = f"{tv_data['revenue_yoy']:.2f}%"
                # Store raw stock_name for news search
                if tv_data.get('description'):
                    data['stock_name'] = tv_data['description']
        except Exception as e:
            logger.debug(f"TradingView data overlay failed: {e}")

        return data
    except Exception as e:
        print(f"Error fetching fundamentals for {ticker}: {e}")
        return None

def get_taiwan_stock_fundamentals(stock_id):
    """
    從 FinMind 取得台股基本面數據 (PER, PBR, Yield)
    """
    try:
        dl = _get_data_loader()
        # 取最近 365 天數據確保有收盤與殖利率資料 (有些冷門股可能交易少)
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        
        # Use generic get_data since specific method is unstable
        df = dl.get_data(
            dataset="TaiwanStockPER",
            data_id=stock_id,
            start_date=start_date
        )
        
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
        dl = _get_data_loader()
        df = dl.taiwan_stock_info()
        row = df[df['stock_id'] == stock_id]
        if not row.empty:
            return {
                'sector': row.iloc[0]['industry_category'],
                'industry': row.iloc[0]['industry_category'] # FinMind usually groups them
            }
    except Exception as e:
        logger.warning(f"Failed to fetch stock profile for {stock_id}: {e}")
    return None

def get_taiwan_stock_revenue(stock_id):
    """
    從 FinMind 取得最近一月營收與年增率 (taiwan_stock_month_revenue)
    """
    try:
        dl = _get_data_loader()
        # 抓取近 90 天 (確保有上個月資料)
        start_date = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
        # P2 磁碟快取：月營收月更，TTL 20 天（省 ~97% FinMind 配額）
        from cache_manager import get_finmind_cached
        df = get_finmind_cached(dl, 'month_revenue', stock_id,
                                'taiwan_stock_month_revenue',
                                ttl_days=20, start_date_filter=start_date)

        if df is not None and not df.empty:
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
        dl = _get_data_loader()
        # 股利通常一年一次，抓 2 年確保有資料
        start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime('%Y-%m-%d')
        # P3 磁碟快取：股利年/半年更，TTL 30 天（省 ~98% FinMind 配額）
        from cache_manager import get_finmind_cached
        df = get_finmind_cached(dl, 'dividend', stock_id,
                                'taiwan_stock_dividend',
                                ttl_days=30, start_date_filter=start_date)

        if df is not None and not df.empty:
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

def get_revenue_history(stock_id, months=36):
    """ Fetch historical revenue data for plotting (Last 3 years default) """
    try:
        dl = _get_data_loader()
        # 36 months + buffer
        start_date = (datetime.datetime.now() - datetime.timedelta(days=months*30 + 30)).strftime('%Y-%m-%d')
        # P2 復用同一份 month_revenue 快取（caller 提 36m window，cache 存 10 年從 2015）
        from cache_manager import get_finmind_cached
        df = get_finmind_cached(dl, 'month_revenue', stock_id,
                                'taiwan_stock_month_revenue',
                                ttl_days=20, start_date_filter=start_date)
        if df is not None and not df.empty:
             df['date'] = pd.to_datetime(df['date'])
             df.sort_values('date', inplace=True)
        return df
    except Exception as e:
        logger.warning(f"Failed to fetch revenue history for {stock_id}: {e}")
        return pd.DataFrame()

def get_per_history(stock_id, days=500):
    """ Fetch historical PER/PBR data for plotting """
    try:
        dl = _get_data_loader()
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
        df = dl.get_data(dataset="TaiwanStockPER", data_id=stock_id, start_date=start_date)
        if not df.empty:
             df['date'] = pd.to_datetime(df['date'])
             df.sort_values('date', inplace=True)
        return df
    except Exception as e:
        logger.warning(f"Failed to fetch PER history for {stock_id}: {e}")
        return pd.DataFrame()

def get_financial_statements(stock_id, quarters=12):
    """
    Fetch quarterly financial statements and calculate Three Rates (Margins) & EPS.
    """
    try:
        dl = _get_data_loader()
        # Estimate days: 12 quarters * 100 days
        start_date = (datetime.datetime.now() - datetime.timedelta(days=quarters*100)).strftime('%Y-%m-%d')
        
        df = dl.get_data(
            dataset="TaiwanStockFinancialStatements",
            data_id=stock_id,
            start_date=start_date
        )
        
        if df.empty:
            return pd.DataFrame()
            
        # Pivot: index=date, columns=type, values=value
        # aggfunc='mean' handles potential duplicates by averaging (though usually unique)
        df_pivot = df.pivot_table(index='date', columns='type', values='value', aggfunc='mean')
        df_pivot.index = pd.to_datetime(df_pivot.index)
        df_pivot.sort_index(inplace=True)
        
        # Calculate Margins (%)
        # Check if Revenue exists to avoid divide by zero
        if 'Revenue' in df_pivot.columns:
            # Gross Margin
            if 'GrossProfit' in df_pivot.columns:
                df_pivot['GrossMargin'] = (df_pivot['GrossProfit'] / df_pivot['Revenue']) * 100
            
            # Operating Margin
            if 'OperatingIncome' in df_pivot.columns:
                df_pivot['OperatingMargin'] = (df_pivot['OperatingIncome'] / df_pivot['Revenue']) * 100
                
            # Net Profit Margin
            if 'IncomeAfterTaxes' in df_pivot.columns:
                df_pivot['NetProfitMargin'] = (df_pivot['IncomeAfterTaxes'] / df_pivot['Revenue']) * 100
                
        return df_pivot
        
    except Exception as e:
        print(f"Financials Error: {e}")
        return pd.DataFrame()
