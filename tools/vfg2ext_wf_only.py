"""Quick rerun: walk-forward only, reusing the strategies_global simulation."""
import sys
sys.path.insert(0, r"c:\GIT\StockAnalyzer\tools")

from vfg2ext_trailing_validation import (
    load_ohlcv_index, extract_paths, run_all_strategies,
    test_C_walk_forward, PICKS_PR, OUT_WF, HORIZON_DAYS
)
import pandas as pd

# Patch module-global
import vfg2ext_trailing_validation as mod

ohlcv_idx = load_ohlcv_index()
picks = pd.read_parquet(PICKS_PR).reset_index(drop=True)
print(f"[paths] extract {len(picks)} picks")
paths, missing = extract_paths(picks, ohlcv_idx, horizon=max(HORIZON_DAYS, 60))
print(f"  missing={missing}")

print("[run] all strategies for WF base returns")
mod.strategies_global = run_all_strategies(picks, paths)

print("\n[Test C] Walk-forward")
df_wf = test_C_walk_forward(picks, paths)
df_wf.to_csv(OUT_WF, index=False, float_format="%.6f")
print(f"[out] walk-forward -> {OUT_WF}")
print(f"windows: {len(df_wf)}")
if len(df_wf):
    print(df_wf.head(10).to_string())
