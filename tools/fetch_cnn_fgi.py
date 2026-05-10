"""
fetch_cnn_fgi.py — CNN Fear & Greed Index daily 2011+

兩源 merge：
1. GitHub mirror `whit3rabbit/fear-greed-data`（2011-01-03+，週自動更新 via GitHub Actions）
2. CNN 隱藏 endpoint `production.dataviz.cnn.io/index/fearandgreed/graphdata/YYYY-MM-DD`
   每 call 回約 7 天，作 freshness top-up（若 GitHub stale）

Schema:
  date
  cnn_fgi_score (0-100)
  cnn_fgi_rating ('extreme fear' | 'fear' | 'neutral' | 'greed' | 'extreme greed')

輸出: data/sentiment/cnn_fgi_history.parquet
執行: python tools/fetch_cnn_fgi.py
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "sentiment" / "cnn_fgi_history.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

GITHUB_CSV = "https://raw.githubusercontent.com/whit3rabbit/fear-greed-data/main/fear-greed.csv"
CNN_ENDPOINT = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/{date}"
CNN_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Origin': 'https://edition.cnn.com',
    'Referer': 'https://edition.cnn.com/',
}


def fetch_github() -> pd.DataFrame:
    logger.info("Downloading GitHub CSV mirror...")
    r = requests.get(GITHUB_CSV, timeout=30, verify=False)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={'Date': 'date', 'Fear Greed': 'cnn_fgi_score', 'Rating': 'cnn_fgi_rating'})
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    logger.info("GitHub: %d rows, %s ~ %s",
                len(df), df['date'].min().strftime('%Y-%m-%d'),
                df['date'].max().strftime('%Y-%m-%d'))
    return df


def fetch_cnn_recent(start_date: datetime) -> pd.DataFrame:
    """從 CNN endpoint 抓 start_date 起的最近資料（每 call 約回 7 天）。"""
    rows = []
    cursor = start_date
    today = datetime.now()
    safety = 0
    while cursor <= today and safety < 60:
        url = CNN_ENDPOINT.format(date=cursor.strftime('%Y-%m-%d'))
        try:
            r = requests.get(url, headers=CNN_HEADERS, timeout=20, verify=False)
            r.raise_for_status()
            d = r.json()
            for h in d.get('fear_and_greed_historical', {}).get('data', []):
                rows.append({
                    'date': pd.to_datetime(h['x'], unit='ms').normalize(),
                    'cnn_fgi_score': float(h['y']),
                    'cnn_fgi_rating': h.get('rating', ''),
                })
        except Exception as e:
            logger.warning("CNN %s failed: %s", cursor.strftime('%Y-%m-%d'), e)
            break
        cursor += timedelta(days=7)
        safety += 1
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates('date').sort_values('date').reset_index(drop=True)
    logger.info("CNN endpoint: %d rows, %s ~ %s",
                len(df), df['date'].min().strftime('%Y-%m-%d'),
                df['date'].max().strftime('%Y-%m-%d'))
    return df


def main():
    df_gh = fetch_github()

    # CNN endpoint 補 freshness（GitHub last date + 1 起）
    cnn_start = df_gh['date'].max() + pd.Timedelta(days=1)
    if cnn_start.to_pydatetime() <= datetime.now():
        df_cnn = fetch_cnn_recent(cnn_start.to_pydatetime())
    else:
        df_cnn = pd.DataFrame()
        logger.info("GitHub already up-to-date, skip CNN top-up")

    # Merge: GitHub 作 base, CNN 補後段
    if not df_cnn.empty:
        merged = pd.concat([df_gh, df_cnn], ignore_index=True)
        merged = merged.drop_duplicates('date', keep='last').sort_values('date').reset_index(drop=True)
    else:
        merged = df_gh

    merged.to_parquet(OUT, index=False)
    logger.info("Saved -> %s (%d rows)", OUT, len(merged))
    last = merged.iloc[-1]
    logger.info("Latest: %s score=%.1f rating=%s",
                last['date'].strftime('%Y-%m-%d'),
                last['cnn_fgi_score'],
                last['cnn_fgi_rating'])


if __name__ == '__main__':
    main()
