
import yfinance as yf
from FinMind.data import DataLoader
import pandas as pd
import datetime

def debug_yfinance():
    print("--- Debugging yfinance (2330.TW) ---")
    try:
        t = yf.Ticker("2330.TW")
        info = t.info
        print("Existing Keys in info:", list(info.keys()))
        print(f"Trailing PE: {info.get('trailingPE')}")
        print(f"Dividend Yield: {info.get('dividendYield')}")
    except Exception as e:
        print(f"yfinance error: {e}")

def debug_finmind():
    print("\n--- Debugging FinMind (2330) ---")
    try:
        dl = DataLoader()
        # FinMind usually requires a date range. Let's get the latest data.
        start_date = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
        
        print(f"Fetching TaiwanStockPER from {start_date}...")
        df = dl.taiwan_stock_per_pbr(stock_id='2330', start_date=start_date)
        
        if not df.empty:
            print("Successfully fetched PER/PBR data:")
            print(df.tail(1).T)
        else:
            print("FinMind returned empty DataFrame.")

    except Exception as e:
        print(f"FinMind error: {e}")

if __name__ == "__main__":
    debug_yfinance()
    debug_finmind()
