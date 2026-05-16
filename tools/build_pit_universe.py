"""Build PIT universe — 合併 TDCC 全量 (含下市) + live universe industry_category

2026-05-16 新增以修 whale_picks_phase2 survivor bias (Audit D Blocker #4)。

來源：
  - data_cache/backtest/universe_tw_full.parquet  (TDCC OpenData 1-1, 含下市)
  - data_cache/backtest/universe_tw.parquet        (live survivor universe, 有 industry_category)

產出：
  - data_cache/backtest/universe_tw_pit.parquet    (3610 共通股 = 2127 survivor + 1483 下市)

用法：
  python tools/build_pit_universe.py

下市股 industry_category 因 live universe 不含 → fill '未分類(已下市/暫停)'。
backtest OHLCV 對下市股自然在 delist 後缺資料 → feature 自然 drop out (PIT-equivalent)。
"""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "data_cache" / "backtest"


def build():
    full = pd.read_parquet(CACHE / "universe_tw_full.parquet")
    full = full[full['is_common_stock'] == True].copy()

    live = pd.read_parquet(CACHE / "universe_tw.parquet")[['stock_id', 'industry_category']]
    full = full.merge(live, on='stock_id', how='left')
    full['industry_category'] = full['industry_category'].fillna('未分類(已下市/暫停)')

    out = full[['stock_id', 'industry_category', 'name', 'status', 'market']].drop_duplicates(subset='stock_id')
    out_path = CACHE / "universe_tw_pit.parquet"
    out.to_parquet(out_path)

    print(f"PIT universe: {len(out)} stocks (active={int((out['status']=='正常').sum())}, "
          f"delisted/suspended={int((out['status']!='正常').sum())})")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    build()
