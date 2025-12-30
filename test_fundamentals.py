
from FinMind.data import DataLoader
import pandas as pd

dl = DataLoader()

stock_id = '2330'
print(f"Testing Fundamentals for {stock_id}...")

# 1. P/E, P/B, Dividend Yield (TaiwanStockPER)
print("\n[1] P/E, P/B, Yield (TaiwanStockPER)")
try:
    df_per = dl.taiwan_stock_per(stock_id=stock_id, start_date='2024-12-01')
    if not df_per.empty:
        print("✅ Found!")
        print(df_per.tail(2))
    else:
        print("❌ Empty")
except Exception as e:
    print(f"❌ Error: {e}")

# 2. Monthly Revenue (TaiwanStockMonthRevenue)
print("\n[2] Monthly Revenue (TaiwanStockMonthRevenue)")
try:
    df_rev = dl.taiwan_stock_month_revenue(stock_id=stock_id, start_date='2024-01-01')
    if not df_rev.empty:
        print("✅ Found!")
        print(df_rev.tail(2))
    else:
        print("❌ Empty")
except Exception as e:
    print(f"❌ Error: {e}")

# 3. Dividend (TaiwanStockDividend)
print("\n[3] Dividend Policy (TaiwanStockDividend)")
try:
    df_div = dl.taiwan_stock_dividend(stock_id=stock_id, start_date='2020-01-01')
    if not df_div.empty:
        print("✅ Found!")
        print(df_div.tail(2))
    else:
        print("❌ Empty")
except Exception as e:
    print(f"❌ Error: {e}")
