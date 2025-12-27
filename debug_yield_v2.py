
import yfinance as yf
from FinMind.data import DataLoader
import datetime

def debug():
    dl = DataLoader()
    stocks = ['2330', '2603']
    
    print("DEBUGGING YIELD VALUES")
    print("======================")
    
    for s in stocks:
        print(f"\nStock: {s}")
        
        # 1. FinMind
        try:
            # FinMind API
            df = dl.taiwan_stock_per_pbr(stock_id=s, start_date='2025-12-01')
            if not df.empty:
                val = df.iloc[-1]['dividend_yield']
                print(f"  FinMind Yield Raw: {val}  (Type: {type(val)})")
            else:
                print("  FinMind: No Data")
        except Exception as e:
            print(f"  FinMind Error: {e}")

        # 2. yfinance
        try:
            t = yf.Ticker(f"{s}.TW")
            dy = t.info.get('dividendYield')
            print(f"  yfinance Yield Raw: {dy} (Type: {type(dy)})")
        except Exception as e:
            print(f"  yfinance Error: {e}")

if __name__ == "__main__":
    debug()
