"""台股代號判別 — data_cache/*_price.csv 的 TW / US 分流單一來源。

`data_cache/` 以通用的 `{ticker}_price.csv` 命名同時存放台股與美股（使用者在 App
分析任何美股都會刷新一份），所以任何「掃 data_cache 算全市場」的工具都必須先過濾，
否則會把約 500 檔美股一起算進台股統計。

2026-08-02 前 refresh_backtest_panels 有過濾、build_tw_breadth 沒有，兩邊漂移導致
台股廣度面板混入美股（含「台股休市但美股開盤」的整列純美股資料）。抽成共用模組
避免再犯。

註：`refresh_universe_prices.py` 另用 `.isdigit()` 決定要更新哪些 CSV（不含
2891A 這類特別股），與本檔的 regex 口徑不同。那是「要去抓哪些股」的決策，與
「已有的 CSV 哪些算台股」不同，暫不強行統一。
"""
from __future__ import annotations

import re
from pathlib import Path

# 4~6 位數字，可帶一個大寫字母後綴（2891A 等特別股）
TW_TICKER_RE = re.compile(r'^\d{4,6}[A-Z]?$')

_PRICE_SUFFIX = '_price'


def is_tw_ticker(ticker: str) -> bool:
    """純代號（非檔名）是否為台股。"""
    return bool(TW_TICKER_RE.match(str(ticker).strip()))


def ticker_from_price_csv(path) -> str:
    """data_cache/2330_price.csv -> '2330'。"""
    return Path(path).stem.replace(_PRICE_SUFFIX, '')


def tw_price_csvs(cache_dir) -> list[Path]:
    """回傳 cache_dir 下所有台股 *_price.csv（已排序，美股已濾除）。"""
    return sorted(f for f in Path(cache_dir).glob('*_price.csv')
                  if is_tw_ticker(ticker_from_price_csv(f)))
