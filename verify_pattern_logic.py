
import pandas as pd
import numpy as np
from pattern_recognition import identify_patterns

def create_synthetic_df(data_dict):
    """
    Helper to create a DataFrame from a list of dicts.
    """
    df = pd.DataFrame(data_dict)
    # Add fake Volume if not present (default 1000)
    if 'Volume' not in df.columns:
        df['Volume'] = 1000
    return df

def test_pattern(name, candles, expected_pattern, expected_type):
    print(f"Testing {name}...", end=" ")
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
    O, H, L, C, V = 'Open', 'High', 'Low', 'Close', 'Volume'
    
    # 1. Doji
    # Open=100, Close=100.1, High=105, Low=95. Body=0.1. Len=10. Ratio=0.01 < 0.1
    # Need avg_body context. Let's make previous candles normal (Body ~2).
    # Previous: 100->102.
    context = [{'Open': 100, 'Close': 102, 'High': 103, 'Low': 99, 'Volume': 1000}] * 20
    doji_candle = {'Open': 100, 'Close': 100.1, 'High': 105, 'Low': 95, 'Volume': 1000}
    test_pattern("Doji", context + [doji_candle], "Doji", "Neutral")
    
    # 2. Marubozu (Bull)
    # Open=100, Close=110, High=110, Low=100. No shadow.
    maru_candle = {'Open': 100, 'Close': 110, 'High': 110.1, 'Low': 99.9, 'Volume': 2000} # Tiny shadow allowed
    test_pattern("Marubozu (Bull)", context + [maru_candle], "Marubozu (Bull)", "Bullish")
    
    # 3. Harami (Bull) inside Bear
    # Prev: 100 -> 90 (Big Red). Curr: 92 -> 98 (Small Green, inside 90-100 range)
    flat_ctx = [{'Open': 100, 'Close': 102, 'High': 103, 'Low': 99, 'Volume': 1000}] * 20
    big_red = {'Open': 100, 'Close': 90, 'High': 100, 'Low': 90, 'Volume': 1000}
    small_green = {'Open': 92, 'Close': 98, 'High': 98, 'Low': 92, 'Volume': 1000}
    test_pattern("Harami (Bull)", flat_ctx + [big_red, small_green], "Harami (Bull)", "Bullish")

    # 4. Piercing Line
    # Prev: 100 -> 90. Midpoint = 95.
    # Curr: Open=88 (Gap down < 90/90Low). Close=96 (>95).
    piercing = {'Open': 88, 'Close': 96, 'High': 96, 'Low': 88, 'Volume': 1000}
    test_pattern("Piercing Line", flat_ctx + [big_red, piercing], "Piercing Line", "Bullish")
    
    # 5. Dark Cloud Cover
    # Prev: 90 -> 100 (Big Green). Midpoint = 95.
    # Curr: Open=102 (Gap Up > 100High). Close=94 (<95).
    big_green = {'Open': 90, 'Close': 100, 'High': 100, 'Low': 90, 'Volume': 1000}
    dark_cloud = {'Open': 102, 'Close': 94, 'High': 102, 'Low': 94, 'Volume': 1000}
    test_pattern("Dark Cloud Cover", flat_ctx + [big_green, dark_cloud], "Dark Cloud", "Bearish")
    
    # 6. Three Black Crows
    # 3 Red candles.
    crow1 = {'Open': 100, 'Close': 90, 'High': 100, 'Low': 90, 'Volume': 1000}
    crow2 = {'Open': 90, 'Close': 80, 'High': 90, 'Low': 80, 'Volume': 1000}
    crow3 = {'Open': 80, 'Close': 70, 'High': 80, 'Low': 70, 'Volume': 1000}
    test_pattern("Three Black Crows", flat_ctx + [crow1, crow2, crow3], "3 Black Crows", "Bearish")

if __name__ == "__main__":
    run_tests()
