
from FinMind.data import DataLoader
import pandas as pd

dl = DataLoader()
print("Fetching Day Trading Data for 2330...")
try:
    df = dl.taiwan_stock_day_trading(
        stock_id='2330',
        start_date='2023-12-01'
    )
    if not df.empty:
        print("Success! Data found.")
        print(df.tail())
        print("Columns:", df.columns)
    else:
        print("Empty DataFrame returned.")
except Exception as e:
    print(f"Error: {e}")
