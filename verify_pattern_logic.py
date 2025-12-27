
import pandas as pd
import numpy as np
from pattern_recognition import identify_patterns

def create_synthetic_df(data_dict):
    """
    Helper to create a DataFrame from a list of dicts.
    """
    df = pd.DataFrame(data_dict)
    if 'Volume' not in df.columns:
        df['Volume'] = 1000
    if 'MA20' not in df.columns:
        # Default MA20 to a flat line if not provided
        df['MA20'] = 100
    return df

def test_pattern(name, candles, expected_pattern, expected_type, ma20_val=100):
    print(f"Testing {name}...", end=" ")
    
    # Inject MA20 into candles if not present
    for c in candles:
        if 'MA20' not in c:
            c['MA20'] = ma20_val
            
    df = create_synthetic_df(candles)
    res = identify_patterns(df)
    last_pat = res.iloc[-1]['Pattern']
    last_type = res.iloc[-1]['Pattern_Type']
    
    if last_pat == expected_pattern:
        print(f"✅ PASS")
    else:
        print(f"❌ FAIL (Expected: {expected_pattern}, Got: {last_pat})")
        # print(res.tail(1)) # Debug

def run_tests():
    print("=== Running Candlestick Pattern Logic Verification ===")
    
    # Base params
    # Context usually 20 candles for rolling body mean.
    context = [{'Open': 100, 'Close': 102, 'High': 103, 'Low': 99, 'Volume': 1000, 'MA20': 100}] * 20
    
    # 1. Hammer (Downtrend)
    # Price < MA20 (e.g. Price=90, MA20=100)
    # Hammer shape: Body small, Long Lower Shadow.
    hammer_candle = {'Open': 90, 'Close': 91, 'High': 91.5, 'Low': 85, 'Volume': 1000, 'MA20': 100}
    test_pattern("Hammer (Downtrend)", context + [hammer_candle], "槌子線 (Hammer)", "Bullish")

    # 2. Hanging Man (Uptrend)
    # Price > MA20 (e.g. Price=110, MA20=100)
    # Same shape as Hammer.
    hanging_candle = {'Open': 110, 'Close': 111, 'High': 111.5, 'Low': 105, 'Volume': 1000, 'MA20': 100}
    test_pattern("Hanging Man (Uptrend)", context + [hanging_candle], "吊人線 (Hanging Man)", "Bearish")

    # 3. Morning Star
    # Down Trend.
    # 1. Long Black (Prev-Prev)
    # 2. Star (Prev) - Gap Down
    # 3. Long Red (Curr) - Penetrate
    p1 = {'Open': 95, 'Close': 85, 'High': 95, 'Low': 85, 'Volume': 1000, 'MA20': 100} # Long Black
    p2 = {'Open': 82, 'Close': 83, 'High': 83, 'Low': 81, 'Volume': 1000, 'MA20': 100} # Star (Gap Down from 85)
    p3 = {'Open': 84, 'Close': 92, 'High': 92, 'Low': 84, 'Volume': 1000, 'MA20': 100} # Long Red (Close 92 > Midpoint 90)
    
    test_pattern("Morning Star", context + [p1, p2, p3], "晨星 (Morning Star)", "Bullish")

    # 4. Evening Star
    # Up Trend
    # 1. Long Red
    # 2. Star - Gap Up
    # 3. Long Black
    u1 = {'Open': 105, 'Close': 115, 'High': 115, 'Low': 105, 'Volume': 1000, 'MA20': 100} # Long Red
    u2 = {'Open': 118, 'Close': 117, 'High': 119, 'Low': 117, 'Volume': 1000, 'MA20': 100} # Star (Gap Up from 115)
    u3 = {'Open': 116, 'Close': 108, 'High': 116, 'Low': 108, 'Volume': 1000, 'MA20': 100} # Long Black (Close 108 < Midpoint 110)
    
    test_pattern("Evening Star", context + [u1, u2, u3], "夜星 (Evening Star)", "Bearish")

if __name__ == "__main__":
    run_tests()
