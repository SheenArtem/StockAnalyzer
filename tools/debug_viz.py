
import pandas as pd
import numpy as np
import mplfinance as mpf
from technical_analysis import calculate_all_indicators, plot_single_chart

def debug_technical():
    print("🚀 Starting Debugging...")
    
    # 1. Create Dummy Data
    dates = pd.date_range(start='2024-01-01', periods=200)
    df = pd.DataFrame({
        'Open': np.random.rand(200) * 100,
        'High': np.random.rand(200) * 105,
        'Low': np.random.rand(200) * 95,
        'Close': np.random.rand(200) * 100,
        'Volume': np.random.randint(1000, 5000, 200)
    }, index=dates)
    
    # Force some MAs to avoid NaN
    df['Close'] = df['Close'].rolling(5).mean().fillna(100)
    
    # 2. Run Indicators
    print("Checking calculate_all_indicators...")
    df = calculate_all_indicators(df)
    
    print("Columns generated:", df.columns.tolist())
    
    # Check Magic Nine
    print("\n[Magic Nine Check]")
    print("TD_Sell_Setup counts:", df['TD_Sell_Setup'].value_counts())
    
    # 3. Simulate Plot Logic (Excerpt from plot_single_chart)
    plot_df = df.tail(100).copy()
    
    td_sell_9 = plot_df['TD_Sell_Setup'].apply(lambda x: x if x == 9 or x == 13 else np.nan)
    td_sell_vals = plot_df['High'] * 1.01
    td_sell_vals_filtered = td_sell_vals.where(td_sell_9.notna(), np.nan)
    
    print("\n[Arrow Logic Check]")
    print("td_sell_9 Not-NaN count:", td_sell_9.notna().sum())
    print("td_sell_vals_filtered Not-NaN count:", td_sell_vals_filtered.notna().sum())
    
    if td_sell_vals_filtered.notna().sum() != td_sell_9.notna().sum():
        print("❌ CRITICAL: Filter mismatch! where() might be failing.")
    else:
        print("✅ Filter seems correct.")
        
    # 4. Check Plotting (apds)
    # We can't verify apds inside plot_single_chart easily without modifying it.
    # But we can try running it and see if it crashes.
    print("\n[Plot Execution Check]")
    try:
        plot_single_chart("DEBUG_TICKER", df, "Test", "Daily")
        print("✅ plot_single_chart ran successfully (Figures created in memory).")
    except Exception as e:
        print(f"❌ Execution failed: {e}")

if __name__ == "__main__":
    debug_technical()
