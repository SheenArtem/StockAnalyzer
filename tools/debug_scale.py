
import yfinance as yf

def debug():
    print("DEBUGGING YFINANCE SCALE (Yield / ROE)")
    
    # Check US Stock
    print("\nStock: AAPL")
    t = yf.Ticker("AAPL")
    dy = t.info.get('dividendYield')
    roe = t.info.get('returnOnEquity')
    pm = t.info.get('profitMargins')
    print(f"  Yield Raw: {dy}")
    print(f"  ROE Raw: {roe}")
    print(f"  Profit Margin Raw: {pm}")
    
    # Check TW Stock
    print("\nStock: 2330.TW")
    t = yf.Ticker("2330.TW")
    dy = t.info.get('dividendYield')
    roe = t.info.get('returnOnEquity')
    pm = t.info.get('profitMargins')
    print(f"  Yield Raw: {dy}")
    print(f"  ROE Raw: {roe}")
    print(f"  Profit Margin Raw: {pm}")

if __name__ == "__main__":
    debug()
