
from FinMind.data import DataLoader
import pandas as pd

dl = DataLoader()
print("Fetching Shareholding Data levels...")
try:
    df = dl.taiwan_stock_shareholding(
        stock_id='2330',
        start_date='2024-12-01'
    )
    if not df.empty:
        print("Columns:", df.columns.tolist())
        # Print first row to see structure
        print("First Row:", df.iloc[0].to_dict())
    else:
        print("Empty.")
except Exception as e:
    print(f"Error: {e}")
