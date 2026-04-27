"""
MOPS 全市場月營收 bulk fetcher (Cache 三層架構 Layer 2)

從 TWSE 公開資料平台一次抓全市場 (上市 + 上櫃 ~1954 stocks) 月營收，
僅 2 個 HTTP request，省掉 N 個個股 MOPS API 呼叫。

Source URL:
- 上市 SII: https://mopsfin.twse.com.tw/opendata/t187ap05_L.csv
- 上櫃 OTC: https://mopsfin.twse.com.tw/opendata/t187ap05_O.csv
- 兩 CSV 都僅含「最新一個月」資料 (公告月後 10-15 日更新)
- 不適合做歷史 backfill；適合每月 batch 增量更新

設計原則 (project_cache_architecture_redesign 鐵則 #3):
- 精確 gap 偵測：caller 該檢查每檔 (stock_id, year, month) 是否已在 live cache，
  缺才寫；本 module 僅負責 fetch + 轉換 schema，不做合併
- Schema 與 FinMind taiwan_stock_month_revenue 對齊，方便整合既有 fundamental_cache
"""
from __future__ import annotations

import io
import logging
from typing import Optional

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

URL_SII = 'https://mopsfin.twse.com.tw/opendata/t187ap05_L.csv'
URL_OTC = 'https://mopsfin.twse.com.tw/opendata/t187ap05_O.csv'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/csv, */*',
}


def _roc_yyyymm_to_year_month(yyyymm: int | str) -> tuple[int, int]:
    """民國年月 (e.g., 11503) → (西元年, 月) e.g., (2026, 3)."""
    s = str(yyyymm).strip()
    if len(s) < 5:
        raise ValueError(f"unexpected ROC yyyymm: {yyyymm}")
    roc_year = int(s[:-2])
    month = int(s[-2:])
    return roc_year + 1911, month


def _parse_market_csv(content: bytes, market_label: str) -> pd.DataFrame:
    """解析單一 market CSV → 統一 schema (對齊 FinMind month_revenue)。"""
    df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
    if df.empty:
        return df

    # 必要欄位檢查
    required = ['資料年月', '公司代號', '營業收入-當月營收',
                '營業收入-上月營收', '營業收入-去年當月營收',
                '營業收入-上月比較增減(%)', '營業收入-去年同月增減(%)']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{market_label} CSV 缺欄位: {missing}")

    # 篩可解析的 row
    df = df[df['資料年月'].notna() & df['公司代號'].notna()].copy()

    # 民國年月 → 西元
    parsed = df['資料年月'].apply(lambda v: _roc_yyyymm_to_year_month(v))
    df['revenue_year'] = parsed.apply(lambda t: t[0])
    df['revenue_month'] = parsed.apply(lambda t: t[1])

    # date = revenue_month + 1 月 1 日 (公告日近似 = 次月初，與 FinMind convention 一致)
    def _next_month_first(y: int, m: int) -> pd.Timestamp:
        if m == 12:
            return pd.Timestamp(year=y + 1, month=1, day=1)
        return pd.Timestamp(year=y, month=m + 1, day=1)
    df['date'] = df.apply(lambda r: _next_month_first(r['revenue_year'], r['revenue_month']), axis=1)

    df['stock_id'] = df['公司代號'].astype(str)
    df['country'] = 'Taiwan'
    # 千元 → 元
    df['revenue'] = (df['營業收入-當月營收'] * 1000).astype('int64')
    df['revenue_last_month'] = (df['營業收入-上月營收'] * 1000).astype('int64')
    df['revenue_last_year'] = (df['營業收入-去年當月營收'] * 1000).astype('int64')
    df['revenue_month_growth'] = pd.to_numeric(df['營業收入-上月比較增減(%)'], errors='coerce')
    df['revenue_year_growth'] = pd.to_numeric(df['營業收入-去年同月增減(%)'], errors='coerce')

    out_cols = ['date', 'stock_id', 'country', 'revenue', 'revenue_month',
                'revenue_year', 'revenue_last_year', 'revenue_year_growth',
                'revenue_last_month', 'revenue_month_growth']
    out = df[out_cols].copy()
    out['_source_market'] = market_label  # 內部標記，caller 可選擇丟棄
    return out


def fetch_bulk_monthly_revenue(timeout: int = 30,
                                include_otc: bool = True) -> pd.DataFrame:
    """抓全市場最新月營收 (上市 + 上櫃 union)。

    Returns:
        DataFrame schema: date / stock_id / country / revenue / revenue_month /
                          revenue_year / ... / _source_market
        若 fetch 失敗回 empty DataFrame。
    """
    parts = []
    for label, url in [('SII', URL_SII)] + ([('OTC', URL_OTC)] if include_otc else []):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, verify=False)
            r.raise_for_status()
            parsed = _parse_market_csv(r.content, label)
            logger.info("MOPS bulk fetch %s: %d rows", label, len(parsed))
            parts.append(parsed)
        except Exception as e:
            logger.warning("MOPS bulk fetch %s failed: %s", label, e)
            continue

    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    return out


def merge_into_existing_cache(bulk_df: pd.DataFrame, cache_dir: str = 'data_cache/fundamental_cache',
                               dry_run: bool = False) -> dict:
    """把 bulk fetch 結果 merge 進 per-stock fundamental_cache parquet (僅補缺，不覆寫已有期間)。

    Returns:
        dict {written: N, skipped: N, new_files: N, ...}
    """
    from pathlib import Path
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    stats = {'written': 0, 'skipped_already_exists': 0, 'new_files': 0,
              'append_to_existing': 0, 'errors': 0}
    if bulk_df.empty:
        return stats

    # 丟掉內部標記欄位
    bulk_clean = bulk_df.drop(columns=['_source_market'], errors='ignore').copy()

    for sid, g in bulk_clean.groupby('stock_id'):
        sid = str(sid)
        target = cache_path / f'month_revenue_{sid}.parquet'
        try:
            if target.exists():
                existing = pd.read_parquet(target)
                existing['date'] = pd.to_datetime(existing['date'])
                # 用 (revenue_year, revenue_month) 比對「期數」
                existing_periods = set(zip(existing['revenue_year'], existing['revenue_month']))
                new_rows = g[~g.apply(
                    lambda r: (r['revenue_year'], r['revenue_month']) in existing_periods, axis=1
                )]
                if new_rows.empty:
                    stats['skipped_already_exists'] += 1
                    continue
                if dry_run:
                    stats['append_to_existing'] += 1
                    continue
                merged = pd.concat([existing, new_rows], ignore_index=True)
                merged = merged.sort_values('date').reset_index(drop=True)
                merged.to_parquet(target)
                stats['append_to_existing'] += 1
                stats['written'] += 1
            else:
                if dry_run:
                    stats['new_files'] += 1
                    continue
                g_sorted = g.sort_values('date').reset_index(drop=True)
                g_sorted.to_parquet(target)
                stats['new_files'] += 1
                stats['written'] += 1
        except Exception as e:
            logger.warning("merge %s failed: %s", sid, e)
            stats['errors'] += 1

    return stats


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    ap = argparse.ArgumentParser()
    ap.add_argument('--otc', action='store_true', help='也抓上櫃 (default 兩個都抓)')
    ap.add_argument('--no-otc', action='store_true', help='只抓上市')
    ap.add_argument('--merge', action='store_true', help='合併進 fundamental_cache')
    ap.add_argument('--dry-run', action='store_true', help='只顯示會寫多少個 file 但不真的寫')
    args = ap.parse_args()

    include_otc = not args.no_otc
    df = fetch_bulk_monthly_revenue(include_otc=include_otc)
    print(f"Fetched: {len(df)} rows from {df['_source_market'].nunique() if not df.empty else 0} markets")
    if df.empty:
        raise SystemExit(1)
    print(df['_source_market'].value_counts().to_string())
    print(f"Date range: {df['date'].min()} ~ {df['date'].max()}")
    print(f"Unique stocks: {df['stock_id'].nunique()}")

    if args.merge:
        stats = merge_into_existing_cache(df, dry_run=args.dry_run)
        print(f"Merge stats (dry_run={args.dry_run}): {stats}")
