import mplfinance as mpf
import pandas as pd
import numpy as np

# Mock data
dates = pd.date_range('2024-01-01', periods=100)
data = np.random.randn(100, 5)
df = pd.DataFrame(data, index=dates, columns=['Open', 'High', 'Low', 'Close', 'Volume'])
df['Open'] = 100 + df['Open'].cumsum()
df['Close'] = df['Open'] + np.random.randn(100)
df['High'] = df[['Open', 'Close']].max(axis=1) + 1
df['Low'] = df[['Open', 'Close']].min(axis=1) - 1
df['Volume'] = np.abs(df['Volume']) * 1000

# Add indicators
df['MA5'] = df['Close'].rolling(5).mean()
df['OBV'] = df['Volume'].cumsum()

# Setup style
mc = mpf.make_marketcolors(up='r', down='g', inherit=True)
s = mpf.make_mpf_style(marketcolors=mc, style='yahoo', grid_style=':')

# Test make_addplot WITHOUT ax=None
try:
    apds = [
        mpf.make_addplot(df['MA5'], width=1.0),
        mpf.make_addplot(df['OBV'], panel=1, color='blue', width=1.2, ylabel='OBV')
    ]
    
    print("✅ make_addplot success")
    
    # Test plot with returnfig=True
    fig, axes = mpf.plot(df, type='candle', style=s, addplot=apds, 
             volume=True, 
             panel_ratios=(4, 1),
             title="Test Chart",
             figsize=(12, 14),
             tight_layout=True,
             returnfig=True)
             
    print("✅ mpf.plot success")

except Exception as e:
    print(f"❌ Error caught: {e}")
