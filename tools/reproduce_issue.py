import pandas as pd
import numpy as np
from technical_analysis import plot_single_chart, calculate_all_indicators

def create_mock_data(rows=100, invalid_volume=False):
    dates = pd.date_range('2024-01-01', periods=rows)
    data = np.random.randn(rows, 5)
    df = pd.DataFrame(data, index=dates, columns=['Open', 'High', 'Low', 'Close', 'Volume'])
    df['Open'] = 100 + df['Open'].cumsum()
    df['Close'] = df['Open'] + np.random.randn(rows)
    df['High'] = df[['Open', 'Close']].max(axis=1) + 1
    df['Low'] = df[['Open', 'Close']].min(axis=1) - 1
    df['Volume'] = np.abs(df['Volume']) * 1000
    
    if invalid_volume:
        df['Volume'] = 0
        
    return df

print("🧪 Starting Local Self-Verification...")

# Test Case 1: Standard Valid Data
print("\n[Test 1] Standard Data (100 rows)")
try:
    df = create_mock_data(100)
    df = calculate_all_indicators(df)
    fig = plot_single_chart("TEST_TICKER", df, "Test", "Daily")
    print("✅ Success: Figure generated")
except Exception as e:
    print(f"❌ Failed: {e}")

# Test Case 2: Insufficient Data for MA60 (e.g., 30 rows)
# expected: MA60 is all NaN. The code should skip adding MA60 plot and succeed.
print("\n[Test 2] Short Data (30 rows, MA60=NaN)")
try:
    df = create_mock_data(30) # Less than 60
    df = calculate_all_indicators(df) 
    # Note: calculate_all_indicators might fill MA60 with NaN
    fig = plot_single_chart("TEST_SHORT", df, "Test", "Daily")
    print("✅ Success: Figure generated despite NaN indicators")
except Exception as e:
    print(f"❌ Failed: {e}")

# Test Case 3: Zero Volume
print("\n[Test 3] Zero Volume")
try:
    df = create_mock_data(100, invalid_volume=True)
    df = calculate_all_indicators(df)
    fig = plot_single_chart("TEST_NO_VOL", df, "Test", "Daily")
    print("✅ Success: Figure generated (Volume should be hidden)")
except Exception as e:
    print(f"❌ Failed: {e}")

print("\n🏁 Verification Complete")
