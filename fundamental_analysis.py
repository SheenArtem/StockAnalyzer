import yfinance as yf
import pandas as pd

def get_fundamentals(ticker):
    """
    Fetch fundamental data for a given ticker.
    Returns a dictionary of metrics.
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
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

        # Handle Dividend Yield (yfinance returns decimal like 0.015 for 1.5%)
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

        return data
    except Exception as e:
        print(f"Error fetching fundamentals for {ticker}: {e}")
        return None
