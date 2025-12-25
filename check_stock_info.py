from FinMind.data import DataLoader
dl = DataLoader()
df = dl.taiwan_stock_info()
print(df.head())
print(df[df['stock_id'] == '2330'])
