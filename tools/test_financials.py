
from FinMind.data import DataLoader
import pandas as pd

dl = DataLoader()
stock_id = '2330'

print(f"Testing Financial Statements for {stock_id}...")

# 1. Financial Statements (Generic)
try:
    # TaiwanStockFinancialStatements usually contains Balance Sheet, Income, Cashflow together?
    # Or specific id.
    # FinMind datasets: TaiwanStockFinancialStatements
    
    print("\n[1] TaiwanStockFinancialStatements")
    df = dl.get_data(
        dataset="TaiwanStockFinancialStatements",
        data_id=stock_id,
        start_date="2024-01-01"
    )
    if not df.empty:
        print("✅ Found!")
        print("Columns:", df.columns.tolist())
        print("Unique Types:", df['type'].unique() if 'type' in df.columns else "No 'type' column")
        print(df.tail())
    else:
        print("❌ Empty")

except Exception as e:
    print(f"❌ Error: {e}")
