from FinMind.data import DataLoader
dl = DataLoader()
df = dl.taiwan_stock_holding_shares_per(stock_id='2330', start_date='2024-01-01')
if not df.empty:
    print(df['HoldingSharesLevel'].unique())
else:
    print("Empty dataframe")
