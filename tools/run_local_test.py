
import sys
import os
import shutil
import pandas as pd

# Ensure we can import modules from current directory
sys.path.append(os.getcwd())

from technical_analysis import plot_dual_timeframe
from cache_manager import CacheManager

def run_test(ticker="2330", clear_cache=False):
    print(f"\n🚀 [TEST] Starting Local Verification for {ticker}...")
    
    # 1. Option to Clear Cache
    if clear_cache:
        print("🧹 Clearing Cache for fresh test...")
        cm = CacheManager()
        base = cm._get_path(ticker, 'price')
        if os.path.exists(base):
            os.remove(base)
            print("   - Cache deleted.")
            
    # 2. Run Main Logic
    print("⏳ Running plot_dual_timeframe...")
    try:
        figures, errors, df_week, df_day, stock_meta = plot_dual_timeframe(ticker)
        
        # 3. Validation
        print("\n📊 [VALIDATION] Checking Results...")
        
        # Check DataFrames
        if df_day.empty:
            print("❌ FAILURE: df_day is empty.")
        else:
            print(f"✅ df_day loaded. Shape: {df_day.shape}")
            # Verify Index 
            if isinstance(df_day.index, pd.DatetimeIndex):
                print("✅ df_day.index is DatetimeIndex.")
            else:
                print(f"❌ FAILURE: df_day.index is {type(df_day.index)} (Expected DatetimeIndex).")

        if df_week.empty:
            print("❌ FAILURE: df_week is empty.")
        else:
            print(f"✅ df_week loaded. Shape: {df_week.shape}")

        # Check Figures
        if 'Daily' in figures:
            print("✅ Daily Chart Figure created.")
        else:
            print(f"❌ FAILURE: Daily Chart missing. Error: {errors.get('Daily')}")

        if 'Weekly' in figures:
            print("✅ Weekly Chart Figure created.")
        else:
            print(f"❌ FAILURE: Weekly Chart missing. Error: {errors.get('Weekly')}")

        # Check Meta
        print(f"✅ Stock Meta: {stock_meta}")

        if not errors and not df_day.empty and 'Daily' in figures:
            print("\n🎉 TEST PASSED! System is stable.")
            return True
        else:
            print("\n⚠️ TEST FAILED with errors.")
            return False

    except Exception as e:
        import traceback
        print(f"\n❌ EXCEPTION DETECTED:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Test 1: Fresh Download
    print("========================================")
    print("TEST CASE 1: Fresh Download (No Cache)")
    run_test("2330", clear_cache=True)
    
    # Test 2: Cache Hit
    print("\n========================================")
    print("TEST CASE 2: Cache Hit (Load from CSV)")
    run_test("2330", clear_cache=False)
