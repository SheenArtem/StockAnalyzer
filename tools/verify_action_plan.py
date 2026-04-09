
import pandas as pd
import numpy as np
from analysis_engine import TechnicalAnalyzer

# Mock Data
dates = pd.date_range(end=pd.Timestamp.now(), periods=100)
df = pd.DataFrame({
    'Open': 100, 'High': 105, 'Low': 95, 'Close': 100, 'Volume': 1000
}, index=dates)

# Feature columns
df['MA5'] = 100
df['MA10'] = 100
df['MA20'] = 100
df['MA60'] = 100
df['MA120'] = 100
df['MA240'] = 100
df['ATR'] = 2.0
df['BB_Up'] = 110
df['BB_Lo'] = 90
df['Tenkan'] = 100
df['Kijun'] = 100

analyzer = TechnicalAnalyzer('TEST', df, df)

print("\n--- Test Scenario A (Active Buy) ---")
# Setup: Strong Trend
scenario_a = {'code': 'A', 'desc': 'Active', 'title': 'Bull', 'color': 'red'}
# Set Close > MA20 for visual
df['Close'].iloc[-1] = 102
plan_a = analyzer._generate_action_plan(df, scenario_a)
print("Actionable:", plan_a['is_actionable'])
print("Rec Entry:", plan_a['rec_entry_low'], "-", plan_a['rec_entry_high'])
print("Rec TP:", plan_a['rec_tp_price'])
print("Rec SL:", plan_a['rec_sl_price'])


print("\n--- Test Scenario B (Wait/Pullback) ---")
# Setup: Pullback to support. Support MA60=100. Current=105.
# If Code B, it should suggest entry near MA60 (100).
scenario_b = {'code': 'B', 'desc': 'Wait', 'title': 'Pullback', 'color': 'orange'}
df['Close'].iloc[-1] = 108
df['MA60'].iloc[-1] = 100
df['MA20'].iloc[-1] = 110 # MA20 > MA60, but price < MA20? Or just wait for MA60.
plan_b = analyzer._generate_action_plan(df, scenario_b)
print("Actionable:", plan_b['is_actionable'])
print("Rec Entry:", plan_b['rec_entry_low'], "-", plan_b['rec_entry_high'])
# SL should be based on Entry (100), not Current (108).
# ATR = 2. SL = 100 - 4 = 96.
print("Rec SL:", plan_b['rec_sl_price'])


print("\n--- Test Scenario D (Bearish) ---")
scenario_d = {'code': 'D', 'desc': 'Bear', 'title': 'Bear', 'color': 'green'}
plan_d = analyzer._generate_action_plan(df, scenario_d)
print("Actionable:", plan_d['is_actionable'])
if not plan_d['is_actionable']:
    print("Correctly not actionable.")
