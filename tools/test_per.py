
from FinMind.data import DataLoader
import pandas as pd

dl = DataLoader()

print("Testing Supply of TaiwanStockPER via get_data...")
try:
    df_per = dl.get_data(
        dataset="TaiwanStockPER",
        data_id="2330",
        start_date="2024-12-01"
    )
    if not df_per.empty:
        print("✅ PER Data Found!")
        print(df_per.tail(2))
        print("Columns:", df_per.columns)
    else:
        print("❌ PER Data Empty")

except Exception as e:
    print(f"❌ Error: {e}")
