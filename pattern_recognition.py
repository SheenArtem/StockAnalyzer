import pandas as pd
import numpy as np

def identify_patterns(df):
    """
    Identify candlestick patterns in the DataFrame.
    Returns a DataFrame with boolean columns for each pattern.
    """
    # Work on a copy
    data = df.copy()
    
    # Basic candle features
    data['Body'] = data['Close'] - data['Open']
    data['Body_Abs'] = data['Body'].abs()
    data['Upper_Shadow'] = data['High'] - data[['Close', 'Open']].max(axis=1)
    data['Lower_Shadow'] = data[['Close', 'Open']].min(axis=1) - data['Low']
    data['Candle_Len'] = data['High'] - data['Low']
    
    # Volume MA for confirmation
    if 'Volume' in data.columns:
        data['Vol_MA5'] = data['Volume'].rolling(5).mean()
        data['Vol_Surge'] = data['Volume'] > 1.5 * data['Vol_MA5']
    else:
        data['Vol_Surge'] = False

    # Initialize Pattern Columns
    data['Pattern'] = None # Stores the name of the strongest pattern found
    data['Pattern_Type'] = None # 'Bullish' or 'Bearish'

    # Iterating is slow, but easy for multi-candle patterns. 
    # For efficiency we can use vectorized operations where possible.
    
    # 1. Hammer (Bullish Reversal)
    # Lower shadow > 2 * Body, Small Upper Shadow, Downtrend context (simplified)
    # We won't strictly check downtrend here to keep it "light", just the shape.
    is_hammer = (
        (data['Lower_Shadow'] >= 2 * data['Body_Abs']) & 
        (data['Upper_Shadow'] <= 0.5 * data['Body_Abs']) &
        (data['Body_Abs'] > 0) # Avoid doji division issues
    )
    
    # 2. Shooting Star (Bearish Reversal)
    # Upper shadow > 2 * Body, Small Lower Shadow
    is_shooting_star = (
        (data['Upper_Shadow'] >= 2 * data['Body_Abs']) & 
        (data['Lower_Shadow'] <= 0.5 * data['Body_Abs']) &
        (data['Body_Abs'] > 0)
    )
    
    # 3. Engulfing
    # Bullish: Prev Red, Curr Green, Curr Open < Prev Close, Curr Close > Prev Open
    # Bearish: Prev Green, Curr Red, Curr Open > Prev Close, Curr Close < Prev Open
    # Using shift for previous candle
    prev_close = data['Close'].shift(1)
    prev_open = data['Open'].shift(1)
    prev_body = data['Body'].shift(1)
    
    is_bullish_engulfing = (
        (prev_body < 0) & (data['Body'] > 0) &
        (data['Open'] <= prev_close) & (data['Close'] >= prev_open) &
        (data['Body_Abs'] > prev_body.abs())
    )
    
    is_bearish_engulfing = (
        (prev_body > 0) & (data['Body'] < 0) &
        (data['Open'] >= prev_close) & (data['Close'] <= prev_open) &
        (data['Body_Abs'] > prev_body.abs())
    )

    # 4. Red Three Soldiers (Bullish) / Three Black Crows (Bearish)
    # 3 consecutive positive candles
    is_3_soldiers = (
        (data['Body'] > 0) & (data['Body'].shift(1) > 0) & (data['Body'].shift(2) > 0) &
        (data['Close'] > data['Close'].shift(1)) & (data['Close'].shift(1) > data['Close'].shift(2))
    )

    # Assign priority (Later overrides earlier if multiple match, or prioritize usually rarer stronger ones)
    
    # Map booleans to text
    mask_hammer = is_hammer
    data.loc[mask_hammer, 'Pattern'] = 'Hammer'
    data.loc[mask_hammer, 'Pattern_Type'] = 'Bullish'
    
    mask_shoot = is_shooting_star
    data.loc[mask_shoot, 'Pattern'] = 'Shooting Star'
    data.loc[mask_shoot, 'Pattern_Type'] = 'Bearish'
    
    mask_bull_eng = is_bullish_engulfing
    data.loc[mask_bull_eng, 'Pattern'] = 'Engulfing (Bull)'
    data.loc[mask_bull_eng, 'Pattern_Type'] = 'Bullish'
    
    mask_bear_eng = is_bearish_engulfing
    data.loc[mask_bear_eng, 'Pattern'] = 'Engulfing (Bear)'
    data.loc[mask_bear_eng, 'Pattern_Type'] = 'Bearish'
    
    mask_3sol = is_3_soldiers
    data.loc[mask_3sol, 'Pattern'] = '3 Red Soldiers'
    data.loc[mask_3sol, 'Pattern_Type'] = 'Bullish'

    return data[['Pattern', 'Pattern_Type']]
