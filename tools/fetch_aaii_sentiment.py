"""
fetch_aaii_sentiment.py — AAII Investor Sentiment Survey weekly bull/bear/neutral 1987+

Source: https://www.aaii.com/files/surveys/sentiment.xls
- 直連 XLS 不需 auth
- ⚠️ 必加 Referer: https://www.aaii.com/sentimentsurvey/sent_results 過 Incapsula CDN bot detection
- 前嘗試「robot block」是因缺 Referer，加上後通

Schema:
  date: weekly 觀察日 (Thursday)
  aaii_bullish: 看多 %
  aaii_neutral: 中性 %
  aaii_bearish: 看空 %
  aaii_bull_bear_spread: 看多 - 看空 (high = euphoria, low = capitulation)
  aaii_bullish_8w_ma: 8 週 MA

輸出: data/macro/aaii_sentiment.parquet (1987-06+ weekly)
執行: python tools/fetch_aaii_sentiment.py
"""
from __future__ import annotations

import logging
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import urllib3
import xlrd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "macro" / "aaii_sentiment.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

URL = "https://www.aaii.com/files/surveys/sentiment.xls"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/vnd.ms-excel,application/octet-stream,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.aaii.com/sentimentsurvey/sent_results',
}


def fetch() -> pd.DataFrame:
    logger.info("Downloading AAII XLS...")
    r = requests.get(URL, headers=HEADERS, timeout=30, verify=False)
    r.raise_for_status()
    if r.content[:8].startswith(b'<html') or len(r.content) < 100_000:
        raise RuntimeError(f"AAII returned non-XLS (size={len(r.content)}, head={r.content[:100]!r}); Incapsula block?")

    wb = xlrd.open_workbook(file_contents=r.content)
    sh = wb.sheet_by_name('SENTIMENT')

    # data starts ~R5 (col 0 = Excel serial date), aggregate stats start ~R2030
    # 規則：col 0 是 numeric AND col 1 (Bullish) 是 numeric → 算 data row
    rows = []
    for r_idx in range(5, sh.nrows):
        date_val = sh.cell_value(r_idx, 0)
        bull = sh.cell_value(r_idx, 1)
        neu = sh.cell_value(r_idx, 2)
        bear = sh.cell_value(r_idx, 3)
        ma8 = sh.cell_value(r_idx, 5)
        spread = sh.cell_value(r_idx, 6)
        if not isinstance(date_val, (int, float)):
            break  # 遇到 'Observations over life' / 'Avg' / 'Max YY' 等 row 停
        if not isinstance(bull, (int, float)) or bull == '':
            continue  # 早期 R5/R6 有 date 沒數據，skip
        # Excel serial → date
        date = pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(date_val))
        rows.append({
            'date': date,
            'aaii_bullish': float(bull),
            'aaii_neutral': float(neu) if isinstance(neu, (int, float)) else None,
            'aaii_bearish': float(bear) if isinstance(bear, (int, float)) else None,
            'aaii_bullish_8w_ma': float(ma8) if isinstance(ma8, (int, float)) else None,
            'aaii_bull_bear_spread': float(spread) if isinstance(spread, (int, float)) else None,
        })
    df = pd.DataFrame(rows).sort_values('date').drop_duplicates('date').reset_index(drop=True)
    logger.info("AAII parsed: %d weeks, %s ~ %s",
                len(df), df['date'].min().strftime('%Y-%m-%d'),
                df['date'].max().strftime('%Y-%m-%d'))
    return df


def main():
    df = fetch()
    df.to_parquet(OUT, index=False)
    logger.info("Saved -> %s (%d rows)", OUT, len(df))
    last = df.iloc[-1]
    logger.info("Latest: %s bullish=%.1f%% neutral=%.1f%% bearish=%.1f%% spread=%+.1f%%",
                last['date'].strftime('%Y-%m-%d'),
                last['aaii_bullish'] * 100,
                (last['aaii_neutral'] or 0) * 100,
                (last['aaii_bearish'] or 0) * 100,
                (last['aaii_bull_bear_spread'] or 0) * 100)


if __name__ == '__main__':
    main()
