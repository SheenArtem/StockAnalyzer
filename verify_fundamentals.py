import fundamental_analysis
import sys

def test_fundamentals():
    print("Testing Fundamental Analysis...")
    
    # Test 1: TW Stock
    ticker = "2330.TW"
    print(f"\nFetching {ticker}...")
    data = fundamental_analysis.get_fundamentals(ticker)
    if data and data['PE Ratio'] != 'N/A':
        print("✅ TW Stock Success")
        print(data)
    else:
        print("❌ TW Stock Failed or No Data")

    # Test 2: US Stock
    ticker = "AAPL"
    print(f"\nFetching {ticker}...")
    data = fundamental_analysis.get_fundamentals(ticker)
    if data and data['Market Cap'] != 'N/A':
        print("✅ US Stock Success")
        print(data)
    else:
        print("❌ US Stock Failed")

if __name__ == "__main__":
    test_fundamentals()
