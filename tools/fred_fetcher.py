"""
fred_fetcher.py — 三風險同步 (宋分擇時 #3) 資料層

宋分原話：「市場只要不再變更壞就會反彈，不需要等利多」。三條件：
  1. HY spread 20 日 Δ ≤ 0 (信用利差不擴大)
  2. VIX3M - VIX 20 日 Δ ≥ 0 (期限結構 contango 變深，恐慌減退)
  3. 10Y 殖利率 20 日 Δ 不再創新高

實作限制 (2026-04-29):
  - FRED CSV 從本機 timeout，改 yfinance 抓 proxy:
      ^TNX  → 10Y yield
      ^VIX  → VIX
      ^VIX3M → VIX3M
      HYG   → HY corp bond ETF, 反向作為 HY spread proxy
              (HYG drop = HY spread widen)
  - 所有資料 daily, 緩存到 data_cache/fred/

Usage:
  python tools/fred_fetcher.py --refresh
  → 寫 data_cache/fred/{tnx,vix,vix3m,hyg}.parquet
"""
from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
CACHE_DIR = _ROOT / "data_cache" / "fred"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = {
    "tnx": "^TNX",
    "vix": "^VIX",
    "vix3m": "^VIX3M",
    "hyg": "HYG",
}


def fetch_one(label: str, symbol: str, period: str = "max") -> pd.DataFrame:
    import yfinance as yf
    t = yf.Ticker(symbol)
    df = t.history(period=period, auto_adjust=False)
    if df.empty:
        return df
    df = df[["Close"]].copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.rename(columns={"Close": label})
    df.index.name = "date"
    return df


def refresh_all(period: str = "max") -> dict:
    out = {}
    for label, symbol in SYMBOLS.items():
        print(f"Fetching {label} ({symbol})...")
        df = fetch_one(label, symbol, period)
        if not df.empty:
            df.to_parquet(CACHE_DIR / f"{label}.parquet")
            out[label] = df
            print(f"  OK: {len(df)} rows, {df.index[0].date()} ~ {df.index[-1].date()}")
        else:
            print(f"  WARN: {label} empty")
    return out


def load_all() -> pd.DataFrame:
    """Load + merge all 4 series into one DataFrame indexed by date."""
    dfs = []
    for label in SYMBOLS:
        p = CACHE_DIR / f"{label}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"Missing {p}; run --refresh first")
        df = pd.read_parquet(p)
        dfs.append(df)
    merged = pd.concat(dfs, axis=1).sort_index()
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="Refetch from yfinance")
    ap.add_argument("--period", default="max")
    args = ap.parse_args()

    if args.refresh:
        refresh_all(args.period)
    else:
        merged = load_all()
        print(f"Loaded merged FRED panel: {merged.shape}")
        print(f"Date range: {merged.index[0].date()} ~ {merged.index[-1].date()}")
        print(merged.tail(3))


if __name__ == "__main__":
    main()
