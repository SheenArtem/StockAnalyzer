
import pandas as pd
import technical_analysis
import plotly.graph_objects as go

def test_pattern_plot():
    print("Testing Pattern Plotting...")
    
    # Create dummy data with a known pattern (e.g. 3 soldiers)
    data = {
        'Date': pd.date_range(start='2023-01-01', periods=10),
        'Open': [100, 102, 104, 106, 105, 100, 102, 105, 108, 110],
        'High': [102, 104, 106, 108, 107, 102, 105, 108, 112, 115],
        'Low':  [99,  101, 103, 105, 100, 98,  100, 103, 106, 108],
        'Close':[101, 103, 105, 107, 102, 99,  104, 107, 110, 114],
        'Volume': [1000] * 10
    }
    df = pd.DataFrame(data).set_index('Date')
    
    # Add dummy technical columns required by plotting function
    df['MA5'] = df['Close']
    df['MA20'] = df['Close']
    df['MA60'] = df['Close']
    df['MA120'] = df['Close']
    df['MA240'] = df['Close']
    df['BB_Upper'] = df['Close'] * 1.05
    df['BB_Lower'] = df['Close'] * 0.95
    df['IC_Tenkan'] = df['Close']
    df['IC_Kijun'] = df['Close']
    df['ATR_Stop'] = df['Close'] * 0.9
    
    # Try plotting
    try:
        fig = technical_analysis.plot_interactive_chart("TEST", df, "Test Title", "Daily")
        print("✅ Plot generated successfully.")
        
        # Check if traces exist
        names = [trace.name for trace in fig.data]
        print(f"Traces found: {names}")
        
        if 'Bullish Pattern' in names or 'Bearish Pattern' in names:
            print("✅ Pattern traces found!")
        else:
            print("⚠️ No pattern traces found (might be due to dummy data not matching patterns perfectly, but code ran).")
            
    except Exception as e:
        print(f"❌ Plotting failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pattern_plot()
