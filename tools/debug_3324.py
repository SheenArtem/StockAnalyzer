
import yfinance as yf
from technical_analysis import load_and_resample, calculate_all_indicators
from analysis_engine import TechnicalAnalyzer

def test_3324():
    ticker = "3324.TWO" # Try TWO first
    
    print(f"Downloading {ticker}...")
    # Use the same loader logic
    # Mocking arguments for load_and_resample or just using yf directly for speed if load_and_resample is complex with cache
    # Let's try to reuse load_and_resample from technical_analysis to be exact
    
    ticker_name, df_day, df_week, meta = load_and_resample(ticker)
    
    if df_day.empty:
        print("Empty DF")
        return

    # Calculate indicators
    df_day = calculate_all_indicators(df_day)
    df_week = calculate_all_indicators(df_week) # Should be robust
    
    analyzer = TechnicalAnalyzer(ticker, df_week, df_day)
    res = analyzer.run_analysis()
    
    ap = res['action_plan']
    print(f"Scenario: {res['scenario']['code']} - {res['scenario']['desc']}")
    print(f"Trend Score: {res['trend_score']}")
    print(f"Trigger Score: {res['trigger_score']}")
    
    # Check Data
    last = df_day.iloc[-1]
    print(f"Data Date: {df_day.index[-1]}")
    print(f"Close: {last['Close']}")
    print(f"MA20: {last['MA20']}")
    print(f"MA60: {last['MA60']}")
    print(f"ATR: {last['ATR']}")
    
    print(f"Current Close: {ap['current_price']}")
    
    print(f"Stop Loss: {ap['rec_sl_price']} ({ap['rec_sl_method']})")
    print(f"Entry: {ap['rec_entry_low']} ~ {ap['rec_entry_high']} ({ap['rec_entry_desc']})")
    print("-" * 20)
    print("Full Action Plan:")
    print(ap)

if __name__ == "__main__":
    test_3324()
