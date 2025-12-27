import fundamental_analysis
import sys

def test_fundamentals():
    print("Testing Fundamental Analysis...")
    
    # Test 1: TW Stock (Numeric Input) - This previously failed for profile
    ticker = "2330" 
    print(f"\nFetching {ticker} (Numeric)...")
    data = fundamental_analysis.get_fundamentals(ticker)
    
    if data:
        print(f"PE Ratio: {data.get('PE Ratio')}")
        print(f"Sector: {data.get('Sector')}")
        
        if data.get('PE Ratio') != 'N/A' and data.get('Sector') != 'N/A':
            print("✅ TW Stock (2330) Success - Profile & Metrics Found")
        else:
            print("❌ TW Stock (2330) Partial Failure - Check Sector/PE")
            print(data)
    else:
        print("❌ TW Stock Failed Completely")

if __name__ == "__main__":
    test_fundamentals()
