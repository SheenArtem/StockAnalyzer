from FinMind.data import DataLoader
dl = DataLoader()
df = dl.taiwan_stock_institutional_investors(stock_id='2330', start_date='2024-01-01')
print(df.columns)
print(df.head(2))
