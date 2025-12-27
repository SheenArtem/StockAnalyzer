
import yfinance as yf
import pandas as pd
from pattern_recognition import identify_patterns

def debug_3324():
    print("Fetching 3324.TW...")
    df = yf.download("3324.TWO", period="1y")
    
    if df.empty:
        print("No data for 3324.TWO")
        return

    print(f"Data shape: {df.shape}")
    
    # Flatten columns if MultiIndex (e.g. ('Close', '3324.TWO') -> 'Close')
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    # Run pattern recognition
    pat_df = identify_patterns(df)
    
    # Filter only detected patterns
    detected = pat_df[pat_df['Pattern'].notna()]
    
    if detected.empty:
        print("No patterns detected.")
    else:
        print("\nLast 20 Detected Patterns:")
        print(detected.tail(20))
        
        # Count stats
        print("\nPattern Counts:")
        print(detected['Pattern'].value_counts())

if __name__ == "__main__":
    debug_3324()
