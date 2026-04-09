from FinMind.data import DataLoader
dl = DataLoader()

# Check 1: Broker Branch Data (Real "Major")
# CAUTION: This dataset is huge and often paid/restricted.
print("Checking Broker Transaction Data...")
try:
    df_broker = dl.taiwan_stock_securities_trader_transaction(
        stock_id='2330',
        start_date='2024-01-01',
        end_date='2024-01-05'
    )
    if not df_broker.empty:
        print("✅ Broker Data Found!")
        print(df_broker.head())
    else:
        print("❌ Broker Data Empty (Might be restricted)")
except Exception as e:
    print(f"❌ Broker Data Error: {e}")

# Check 2: Large Shareholder Data (Alternative "Major")
print("\nChecking Holding Shares Per Data...")
try:
    df_holding = dl.taiwan_stock_holding_shares_per(
        stock_id='2330',
        start_date='2023-11-01'
    )
    if not df_holding.empty:
        print("✅ Holding Shares Data Found!")
        print(df_holding.head())
        print(df_holding.tail())
    else:
        print("❌ Holding Shares Data Empty")
except Exception as e:
    print(f"❌ Holding Shares Error: {e}")
