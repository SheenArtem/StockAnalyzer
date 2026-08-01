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

資料源 (2026-08-01 起雙源):
  - CBOE 系指數 (VIX/VIX3M/VVIX/SKEW/OVX): CBOE 官方日線 CSV 為權威值，
    yfinance 補官方檔缺的早期歷史 (如 VIX3M 2006~2009)。
    緣由: Yahoo ^VIX3M/^MOVE 自 2026-07-20 斷供，只回一筆 NaN 尾列，
    害 vol_complex 面板凍在 7/17、NaN 尾列還騙過日期式 staleness check。
  - 非 CBOE (TNX/HYG/MOVE): 僅 yfinance。MOVE 是 ICE 指數無官方免費源，
    斷供時靠 dropna 後的日期讓下游 staleness check 正確 fail loud。

Usage:
  python tools/fred_fetcher.py --refresh
  → 寫 data_cache/fred/{tnx,vix,vix3m,hyg,vvix,skew,ovx,move}.parquet
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
    # 2026-05-25 vol complex 追加 (VIX 期限結構共振 / 跨資產波動率)
    "vvix": "^VVIX",     # VIX 的 VIX (VIX 選擇權隱波)，反映尾端對沖需求
    "skew": "^SKEW",     # CBOE SKEW，OTM put 相對價格 = 左尾溢價
    "ovx": "^OVX",       # CBOE Crude Oil ETF VIX，地緣事件領先指標
    # 2026-07-06 補漏: system3_move_check 讀 move.parquet 但從未在 --refresh 清單內
    # (5/9 手動抓一次後凍結, S3-a 吃了兩個月舊資料)
    "move": "^MOVE",     # ICE BofA MOVE，美債隱波 (S3-a shock alert 輸入)
}

# CBOE 官方日線 CSV — 這些指數的真來源 (Yahoo 只是轉載, 2026-07-20 起 ^VIX3M 斷供)
CBOE_INDICES = {"vix": "VIX", "vix3m": "VIX3M", "vvix": "VVIX", "skew": "SKEW", "ovx": "OVX"}
CBOE_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/{name}_History.csv"
STALE_WARN_DAYS = 7  # 與 system3_*_check 的 STALE_LIMIT_DAYS 一致


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
    # 砍 NaN 列: yfinance 斷供時回「有日期無收盤」尾列, 落地會騙過日期式 staleness check
    return df.dropna(subset=[label])


def fetch_cboe(label: str, name: str) -> pd.DataFrame:
    """CBOE 官方歷史 CSV → 單欄 DataFrame (schema 與 fetch_one 一致)。"""
    import io

    import requests
    r = requests.get(CBOE_URL.format(name=name), timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.set_index("DATE")
    # 兩種 schema: OHLC 型 (VIX/VIX3M: DATE,OPEN,HIGH,LOW,CLOSE) 取 CLOSE;
    # 兩欄型 (VVIX/SKEW/OVX: DATE,<指數名>) 取最後一欄
    col = "CLOSE" if "CLOSE" in df.columns else df.columns[-1]
    df = df[[col]].rename(columns={col: label})
    df.index.name = "date"
    return df.astype(float).dropna().sort_index()


def refresh_all(period: str = "max") -> dict:
    out = {}
    for label, symbol in SYMBOLS.items():
        print(f"Fetching {label} ({symbol})...")
        df = fetch_one(label, symbol, period)
        if label in CBOE_INDICES:
            try:
                cboe = fetch_cboe(label, CBOE_INDICES[label])
                # 官方值優先, yfinance 只補官方檔沒有的早期歷史 (VIX3M pre-2009)
                df = cboe.combine_first(df) if not df.empty else cboe
                print(f"  CBOE official merged: last bar {cboe.index[-1].date()}")
            except Exception as e:
                print(f"  WARN: CBOE fetch failed for {label} ({e}); yfinance only")
        if not df.empty:
            df.to_parquet(CACHE_DIR / f"{label}.parquet")
            out[label] = df
            print(f"  OK: {len(df)} rows, {df.index[0].date()} ~ {df.index[-1].date()}")
            age = int((pd.Timestamp.now().normalize() - df.index[-1]).days)
            if age > STALE_WARN_DAYS:
                print(f"  WARN: {label} last bar {df.index[-1].date()} is {age} days old -- all sources stale")
        else:
            print(f"  WARN: {label} empty; keeping previous cache")
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
