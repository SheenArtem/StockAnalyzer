
from FinMind.data import DataLoader
import pandas as pd

dl = DataLoader()

print("Testing Supply of TaiwanStockHoldingSharesPer (Shareholding Spread)...")
try:
    # Attempting to fetch via generic get_data if method unknown, or try likely method name
    # Official name is TaiwanStockHoldingSharesPer
    
    # Method 1: Generic
    # Note: FinMind API requires token for some datasets. Let's see if this one is free.
    df = dl.get_data(
        dataset="TaiwanStockHoldingSharesPer",
        data_id="2330",
        start_date="2024-11-01"
    )
    
    if not df.empty:
        print("✅ Spread Data Found!")
        print(df.tail())
        print("Columns:", df.columns)
        if 'HoldingSharesLevel' in df.columns:
            print("Levels:", df['HoldingSharesLevel'].unique())
    else:
        print("❌ Spread Data Empty (Might need token or wrong params)")

except Exception as e:
    print(f"❌ Error: {e}")
