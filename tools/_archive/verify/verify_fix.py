
import sys
import os
import pandas as pd
# Add current directory to path
sys.path.append(os.getcwd())

from technical_analysis import plot_dual_timeframe

def test_plot():
    print("🚀 Starting verification test...")
    try:
        # Test with a known ticker
        figures, errors, df_week, df_day, meta = plot_dual_timeframe('2330')
        
        if 'Daily' in figures:
            print("✅ Daily Chart generated successfully.")
        else:
            print("❌ Daily Chart missing.")
            print(f"Errors: {errors}")
            
        if 'Weekly' in figures:
            print("✅ Weekly Chart generated successfully.")
        else:
            print("❌ Weekly Chart missing.")
            
        if not errors:
            print("✅ No errors reported.")
        else:
            print(f"⚠️ Errors reported: {errors}")
            
    except Exception as e:
        print(f"❌ CRITICAL EXCEPTION: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_plot()
