
import pandas as pd
import os
import time
from cache_manager import CacheManager

def test_cache_manager():
    print("🧪 Testing CacheManager...")
    cm = CacheManager()
    
    # 1. Clear Test
    ticker = "TEST_9999"
    key = "TEST_9999_price"
    base_path = cm._get_path(ticker, 'price')
    if os.path.exists(base_path):
        os.remove(base_path)
        
    # 2. Test Miss
    df, hit = cm.load_cache(ticker, 'price')
    print(f"Load Non-existent: Hit={hit}, Empty={df.empty}")
    assert hit == False
    
    # 3. Test Save & Load
    dummy_df = pd.DataFrame({'Close': [1, 2, 3]}, index=pd.date_range('2024-01-01', periods=3))
    cm.save_cache(ticker, dummy_df, 'price')
    
    if os.path.exists(base_path):
        print("✅ File created successfully.")
    else:
        print("❌ File creation failed.")
        
    # 4. Test Hit
    df2, hit2 = cm.load_cache(ticker, 'price')
    print(f"Load Existing: Hit={hit2}, Rows={len(df2)}")
    assert hit2 == True
    assert len(df2) == 3
    
    print("✅ CacheManager Logic Passed!")

if __name__ == "__main__":
    test_cache_manager()
