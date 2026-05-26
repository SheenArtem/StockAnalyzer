"""
Phase 1: 補抓 PIT universe 中現役但 OHLCV cache 沒抓過的 stocks。

緣由 (2026-05-23): 用戶 push 真 PIT 回測 → audit 出 OHLCV cache 1078 檔 vs PIT
universe 3610 檔的 gap。用戶決定跳過下市股 (1182)，只補「現役但缺檔」的 991 檔
(active normal 1964 - 已有 OHLCV 973)。

策略:
  - yfinance 抓 (不耗 FinMind quota，留給其他 panel)
  - 全期 2018-01-01 → today (含上市起所有資料)
  - 不預過濾 liquidity (Phase 2 才用歷史 liquidity 過濾)
  - reuse `backtest_dl_ohlcv.py` 的 _download_batch / _parse_batch_result / _flush_to_parquet

Output:
  - 擴增 data_cache/backtest/ohlcv_tw.parquet (append + dedup)
  - 寫 data_cache/backtest/_phase1_missing_active.json (清單 + 統計)

Usage:
    python tools/whale_picks_backfill_ohlcv_missing.py            # 全跑
    python tools/whale_picks_backfill_ohlcv_missing.py --test     # 測試 10 檔
    python tools/whale_picks_backfill_ohlcv_missing.py --resume   # 跳過已抓的
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from tools.backtest_dl_ohlcv import (
    _make_yf_session, _download_batch, _parse_batch_result,
    _flush_to_parquet, _get_completed_tickers,
    OUTPUT_PATH, OUTPUT_DIR, BATCH_SIZE, SLEEP_BETWEEN_BATCHES,
    FLUSH_EVERY_N_BATCHES,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("phase1_backfill")

START_DATE = '2013-01-01'  # 給 2015-Q1 features 留 2 年 lookback (52w high / MA200)
MANIFEST_PATH = OUTPUT_DIR / "_phase1_missing_active.json"


def build_missing_active_list(full_universe: bool = False) -> pd.DataFrame:
    """從 PIT - OHLCV 算出真實 active 缺 OHLCV 的 stocks，附 industry。

    2026-05-23 改：加 full_universe flag — True 時回 PIT active 全 1964 檔
    (用於 corrupt cache recovery，一次重抓全 universe + 含 missing)。
    """
    pit = pd.read_parquet(OUTPUT_DIR / "universe_tw_pit.parquet")
    ohlcv_set = set()
    if OUTPUT_PATH.exists():
        ohlcv = pd.read_parquet(OUTPUT_PATH, columns=['stock_id'])
        ohlcv_set = set(ohlcv['stock_id'].unique())

    normal_status = pit['status'].value_counts().index[0]
    pit_normal = pit[pit['status'] == normal_status].copy()
    bad = pit_normal['industry_category'].str.contains(
        '下市|暫停|處置|興櫃', na=False, regex=True)
    pit_active = pit_normal[~bad].copy()

    logger.info("PIT normal status:           %d", len(pit_normal))
    logger.info("PIT active (filtered):       %d", len(pit_active))
    logger.info("  已有 OHLCV:                %d", len(set(pit_active['stock_id']) & ohlcv_set))
    logger.info("  缺 OHLCV:                  %d", len(set(pit_active['stock_id']) - ohlcv_set))

    if full_universe:
        target = pit_active
        logger.info("Mode: FULL universe (1964 檔，含已有)")
    else:
        target = pit_active[~pit_active['stock_id'].isin(ohlcv_set)].copy()
        logger.info("Mode: missing-only (要抓 %d 檔)", len(target))
    missing = target

    # 補 yf_ticker - market 從 universe_tw 主檔對照
    u_main = pd.read_parquet(OUTPUT_DIR / "universe_tw.parquet")
    market_map = u_main.set_index('stock_id')[['type', 'yf_ticker']].to_dict('index')

    # universe_tw 沒涵蓋的 stocks 用 PIT 的 market column
    def make_yf_ticker(row):
        if row['stock_id'] in market_map:
            return market_map[row['stock_id']]['yf_ticker']
        # PIT market column 是 'twse'/'tpex' lowercase
        mk = row.get('market', 'twse')
        suffix = '.TWO' if mk == 'tpex' else '.TW'
        return f"{row['stock_id']}{suffix}"

    def make_market_type(row):
        if row['stock_id'] in market_map:
            return market_map[row['stock_id']]['type']
        return row.get('market', 'twse')

    missing['yf_ticker'] = missing.apply(make_yf_ticker, axis=1)
    missing['type'] = missing.apply(make_market_type, axis=1)
    # rename 對齊 backtest_dl_ohlcv expectation
    if 'name' in missing.columns:
        missing = missing.rename(columns={'name': 'stock_name'})

    return missing[['stock_id', 'stock_name', 'industry_category', 'type', 'yf_ticker']].reset_index(drop=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--test', action='store_true', help='Test 10 stocks only')
    p.add_argument('--resume', action='store_true', help='Skip already-downloaded yf_tickers')
    p.add_argument('--full-universe', action='store_true',
                   help='Fetch ALL PIT active 1964 stocks (recovery mode); default: missing-only')
    p.add_argument('--batch-size', type=int, default=BATCH_SIZE)
    p.add_argument('--start', default=START_DATE)
    args = p.parse_args()

    end_date = datetime.now().strftime('%Y-%m-%d')
    logger.info("Phase 1 backfill: %s → %s (full_universe=%s)", args.start, end_date, args.full_universe)

    missing = build_missing_active_list(full_universe=args.full_universe)
    if args.test:
        missing = missing.head(10)
        logger.info("TEST mode: trimming to first 10")

    # Resume: skip already-downloaded yf_tickers
    completed = _get_completed_tickers() if args.resume else set()
    if completed:
        before = len(missing)
        missing = missing[~missing['yf_ticker'].isin(completed)].copy()
        logger.info("Resume: skip %d already-downloaded → remaining %d", before - len(missing), len(missing))

    if len(missing) == 0:
        logger.info("No tickers to fetch. Exiting.")
        return

    # Universe row map for parse_batch_result
    universe_row_map = {
        r['yf_ticker']: dict(r) for _, r in missing.iterrows()
    }

    sess = _make_yf_session()
    if sess is None:
        logger.warning("curl_cffi unavailable — 429 risk higher, will retry as needed")

    tickers = missing['yf_ticker'].tolist()
    n_batches = (len(tickers) + args.batch_size - 1) // args.batch_size

    results_buffer = []
    success_tickers = set()
    failed_tickers = []
    t0 = time.time()

    for bi in range(n_batches):
        chunk = tickers[bi * args.batch_size:(bi + 1) * args.batch_size]
        logger.info("Batch %d/%d (%d tickers)...", bi + 1, n_batches, len(chunk))
        raw = _download_batch(chunk, args.start, end_date, session=sess)
        parsed = _parse_batch_result(raw, chunk, universe_row_map)

        if not parsed.empty:
            results_buffer.append(parsed)
            tkrs_with_data = set(parsed['yf_ticker'].unique())
            success_tickers.update(tkrs_with_data)
            no_data = set(chunk) - tkrs_with_data
            if no_data:
                failed_tickers.extend(no_data)
                logger.info("  batch %d: %d/%d got data (%d no-data)",
                           bi + 1, len(tkrs_with_data), len(chunk), len(no_data))
        else:
            failed_tickers.extend(chunk)
            logger.warning("  batch %d: ALL %d tickers no data", bi + 1, len(chunk))

        if (bi + 1) % FLUSH_EVERY_N_BATCHES == 0:
            _flush_to_parquet(results_buffer)
            results_buffer = []

        time.sleep(SLEEP_BETWEEN_BATCHES)

    # Final flush
    if results_buffer:
        _flush_to_parquet(results_buffer)

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info("Phase 1 done in %.1f min", elapsed / 60)
    logger.info("  Attempted:    %d", len(tickers))
    logger.info("  Got data:     %d (%.1f%%)", len(success_tickers), 100 * len(success_tickers) / max(1, len(tickers)))
    logger.info("  No data:      %d", len(failed_tickers))

    manifest = {
        'phase': 'phase1_ohlcv_backfill',
        'run_at': datetime.now().isoformat(timespec='seconds'),
        'start_date': args.start,
        'end_date': end_date,
        'attempted_count': len(tickers),
        'success_count': len(success_tickers),
        'failed_count': len(failed_tickers),
        'failed_tickers': sorted(failed_tickers),
        'elapsed_min': round(elapsed / 60, 2),
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info("Manifest: %s", MANIFEST_PATH)


if __name__ == '__main__':
    main()
