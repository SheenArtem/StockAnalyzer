
import pandas as pd
import yfinance as yf
from pattern_recognition import identify_patterns
from technical_analysis import load_and_resample

def test_pattern_logic():
    print("🔍 Testing Pattern Recognition...")
    
    # 1. Load Data (Try 2330)
    ticker = "2330.TW"
    print(f"📥 Loading data for {ticker}...")
    # Use load_and_resample to get the exact same df used in app
    # Note: load_and_resample returns (ticker_name, df_day, df_week, meta)
    name, df, df_w, meta = load_and_resample(ticker)
    
    if df.empty:
        print("❌ Limit: No data found.")
        return

    # Check if 'Pattern' column exists (it should be added by calculate_all_indicators -> which calls identify_patterns??)
    # Wait, load_and_resample DOES NOT call calculate_all_indicators!
    # In app.py: run_analysis calls load_and_resample, THEN calls calculate_all_indicators.
    # IN technical_analysis.py, identify_patterns is called inside calculate_all_indicators?
    # Let me check technical_analysis.py again.
    
    # Checking technical_analysis.py from previous View..
    # calculate_all_indicators(df) was modified to call identify_patterns.
    
    # So we need to call calculate_all_indicators here.
    from technical_analysis import calculate_all_indicators
    
    print("🔄 Running calculate_all_indicators...")
    df = calculate_all_indicators(df)
    
    print("\n📊 Checking 'Pattern' column for last 10 days:")
    if 'Pattern' not in df.columns:
        print("❌ 'Pattern' column MISSING from DataFrame!")
        return
        
    tail = df[['Close', 'Open', 'High', 'Low', 'Pattern']].tail(10)
    print(tail)
    
    last_pat = df['Pattern'].iloc[-1]
    print(f"\n📝 Last Candle Pattern: {last_pat}")
    
    if pd.isna(last_pat) or last_pat == 'None':
        print("ℹ️ Result: No pattern detected on the last day. This explains why UI shows nothing.")
    else:
        print(f"✅ Result: Pattern detected '{last_pat}'. UI SHOULD show this.")

    # [NEW] Test Analysis Engine Logic
    print("\n🕵️ Testing Technical Analyzer Integration...")
    from analysis_engine import TechnicalAnalyzer
    
    # Mock week df
    analyzer = TechnicalAnalyzer(ticker, df_w, df)
    
    # Run Pattern Detection
    score, msgs = analyzer._detect_kline_patterns(df)
    
    print(f"📊 Score: {score}")
    print("📢 Messages:")
    for m in msgs:
        print(f"  - {m}")
        
    # Check if our pattern is in msgs
    found = any("吊人線" in m for m in msgs)
    if found:
        print("✅ SUCCESS: Analysis Engine produced the message!")
    else:
        print("❌ FAILURE: Analysis Engine did NOT produce the message.")
        # Debug why
        last_row = df.iloc[-1]
        pat = last_row.get('Pattern')
        print(f"   Debug: Row Pattern='{pat}', Type={type(pat)}")
        if pat and isinstance(pat, str):
             print(f"   Debug: '{pat}' is string. Split='{pat.split('(')[0]}'")


if __name__ == "__main__":
    test_pattern_logic()
