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
  - ^MOVE (ICE 指數, 無官方免費源): yfinance 深度歷史 (2002~) + Barchart EOD
    補近期。2026-08-01 加 Barchart — 重疊 789 日有 785 日誤差 <0.005，
    且其 7/30=77.09 / 7/31=83.02 與 CNBC 獨立報價吻合。
  - TNX/HYG: 僅 yfinance。

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

# Barchart EOD 補源 — 專給無官方免費源的 ICE 指數 (^MOVE)。
# 深度約 800 交易日 (~3 年)，只補 yfinance 缺的日期，不覆蓋既有歷史。
# ⚠️ 禮貌用量: 每次 refresh 只打 1 次報價頁 + 1 次 API，勿加頻/勿批次掃。
BARCHART_SYMBOLS = {"move": "$MOVE"}
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

# ⚠️ 別用 Yahoo 小時線補 ^MOVE 日線: 2026-08-01 實測 interval=1h 雖然在日線斷供期
# 仍有值, 但整條序列**落後一個交易日** (Barchart 7/20=72.66 出現在 Yahoo 7/21),
# 直接 resample 會讓 5d delta / z-score 無聲錯位。Barchart 與 CNBC 報價則互相吻合。


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


def fetch_barchart(label: str, symbol: str, limit: int = 800) -> pd.DataFrame:
    """Barchart 內部 EOD API → 單欄 DataFrame。需先訪報價頁取得 XSRF-TOKEN cookie。"""
    import urllib.parse

    import requests
    s = requests.Session()
    s.headers.update({"User-Agent": _UA})
    page = f"https://www.barchart.com/stocks/quotes/{symbol}/overview"
    s.get(page, timeout=30)
    token = urllib.parse.unquote(s.cookies.get("XSRF-TOKEN", ""))
    if not token:
        raise RuntimeError("Barchart 未給 XSRF-TOKEN cookie (反爬策略可能已變)")
    s.headers.update({"X-XSRF-TOKEN": token, "Referer": page})
    url = ("https://www.barchart.com/proxies/core-api/v1/historical/get"
           f"?symbol={urllib.parse.quote(symbol)}"
           "&fields=tradeTime.format(Y-m-d),lastPrice"
           f"&type=eod&orderBy=tradeTime&orderDir=asc&limit={limit}")
    rows = s.get(url, timeout=40).json().get("data") or []
    if not rows:
        raise RuntimeError("Barchart 回傳空資料")
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["tradeTime"])
    df[label] = df["lastPrice"].astype(float)
    return df.set_index("date")[[label]].sort_index()


def refresh_all(period: str = "max") -> dict:
    out = {}
    for label, symbol in SYMBOLS.items():
        print(f"Fetching {label} ({symbol})...")
        df = fetch_one(label, symbol, period)
        if label in BARCHART_SYMBOLS:
            # yfinance 深度歷史優先, Barchart 只補它缺的近期日期 (絕不覆蓋既有值)
            try:
                bc = fetch_barchart(label, BARCHART_SYMBOLS[label])
                before = df.index[-1].date() if not df.empty else None
                df = df.combine_first(bc) if not df.empty else bc
                filled = 0 if before is None else int((bc.index.date > before).sum())
                print(f"  Barchart EOD merged: {len(bc)} rows to {bc.index[-1].date()}"
                      f" (+{filled} rows past yfinance's {before})")
            except Exception as e:
                print(f"  WARN: Barchart fetch failed for {label} ({e}); yfinance only")
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
