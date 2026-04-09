
from FinMind.data import DataLoader
import pandas as pd

dl = DataLoader()

print("1. Testing Shareholding (股權分散)...")
try:
    # 股權分散通常是週資料，比較稀疏
    df_hold = dl.taiwan_stock_shareholding(
        stock_id='2330',
        start_date='2023-11-01'
    )
    if not df_hold.empty:
        print("✅ Shareholding Data Found:")
        print(df_hold.tail(2))
        print(df_hold.columns)
    else:
        print("❌ Shareholding Data Empty")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n2. Testing Government Bank Buy/Sell (八大官股)...")
# Note: FinMind API name might vary. Checking likely candidates.
# Often mapped to 'TaiwanStockGovernmentBankBuySell' ? 
# Actually FinMind documentation says 'TaiwanStockGovernmentBankBuySell'.
try:
    df_gov = dl.taiwan_stock_government_bank_buy_sell(
        stock_id='2330',
        start_date='2024-12-01'
    )
    if not df_gov.empty:
        print("✅ Government Bank Data Found:")
        print(df_gov.tail(2))
    else:
        print("❌ Government Bank Data Empty")
except Exception as e:
    print(f"❌ Error: {e}")
