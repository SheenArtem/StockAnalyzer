"""
Phase 1: Bulk download TW OHLCV for IC backtest.

抓「全量」台股現役清單 + 行業分類，下載 15 年日 K / 股利 / 分割，存 parquet。
用途：後續 Phase 2a 做技術指標 IC 驗證。

用法:
    python tools/backtest_dl_ohlcv.py                # 全量 15y
    python tools/backtest_dl_ohlcv.py --years 5      # 只抓 5 年
    python tools/backtest_dl_ohlcv.py --test         # 測試: 抓 10 檔
    python tools/backtest_dl_ohlcv.py --resume       # 斷點續抓
    python tools/backtest_dl_ohlcv.py --validate     # 只跑連續性驗證

已知限制:
    - v1 僅含現役股票（FinMind taiwan_stock_info），存在倖存者偏誤
    - industry 是當下 snapshot，不處理歷史分類變動
    - v1 不抓 shares_outstanding（若需計算市值用 Close proxy）
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# ============================================================
# Config
# ============================================================
OUTPUT_DIR = _ROOT / "data_cache" / "backtest"
OUTPUT_PATH = OUTPUT_DIR / "ohlcv_tw.parquet"
UNIVERSE_PATH = OUTPUT_DIR / "universe_tw.parquet"
ISSUES_PATH = OUTPUT_DIR / "continuity_issues.csv"

BATCH_SIZE = 30                  # yfinance 批次大小（太大易 429）
SLEEP_BETWEEN_BATCHES = 2        # 秒
MAX_RETRIES = 3
RETRY_BACKOFF = [60, 180, 300]   # 1 / 3 / 5 分鐘
FLUSH_EVERY_N_BATCHES = 10       # 每 N 批 flush 一次 parquet

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger("backtest_dl")


# ============================================================
# yfinance session (curl_cffi for Cloudflare bypass)
# ============================================================
def _make_yf_session():
    """用 curl_cffi 模擬 Chrome 繞過 Cloudflare。拿不到 curl_cffi 就 None fallback。"""
    try:
        from curl_cffi.requests import Session
        return Session(impersonate="chrome120")
    except ImportError:
        logger.warning("curl_cffi not available, using default requests (higher 429 risk)")
        return None
    except Exception as e:
        logger.warning(f"curl_cffi session init failed: {e}")
        return None


# ============================================================
# Universe fetch
# ============================================================
def get_universe():
    """從 FinMind 拿所有現役台股 + 行業分類。"""
    logger.info("Fetching TW stock universe from FinMind...")
    from cache_manager import get_finmind_loader
    dl = get_finmind_loader()
    info = dl.taiwan_stock_info()

    # 只要 twse / tpex（不要權證、ETF 等）
    info = info[info['type'].isin(['twse', 'tpex'])].copy()

    # stock_id 必須是 4 碼純數字
    info = info[info['stock_id'].astype(str).str.match(r'^\d{4}$')].copy()

    # 排 ETF (00* 開頭)
    info = info[~info['stock_id'].str.startswith('00')].copy()

    # yfinance ticker 格式
    info['yf_ticker'] = info.apply(
        lambda r: f"{r['stock_id']}.TW" if r['type'] == 'twse' else f"{r['stock_id']}.TWO",
        axis=1,
    )

    # 只保留需要的欄位
    cols = ['stock_id', 'stock_name', 'industry_category', 'type', 'yf_ticker']
    info = info[cols].drop_duplicates(subset=['stock_id']).reset_index(drop=True)

    n_twse = (info['type'] == 'twse').sum()
    n_tpex = (info['type'] == 'tpex').sum()
    logger.info(f"Universe: {len(info)} stocks ({n_twse} TWSE, {n_tpex} TPEX)")

    # 存一份 universe 方便後續用
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    info.to_parquet(UNIVERSE_PATH, engine='pyarrow', compression='snappy')
    logger.info(f"Universe saved to {UNIVERSE_PATH}")

    return info


# ============================================================
# Batch download
# ============================================================
def _download_batch(tickers, start, end, session=None):
    """下載一批 tickers；含 429 retry + backoff。失敗回空 DataFrame。"""
    import yfinance as yf
    for attempt in range(MAX_RETRIES):
        try:
            kwargs = dict(
                tickers=tickers,
                start=start,
                end=end,
                group_by='ticker',
                auto_adjust=False,      # 保留 raw Close + Adj Close
                actions=True,           # 含 Dividends + Stock Splits
                progress=False,
                threads=True,
                timeout=30,
            )
            if session is not None:
                kwargs['session'] = session
            df = yf.download(**kwargs)
            return df
        except Exception as e:
            msg = str(e).lower()
            is_rate_limit = '429' in msg or 'too many' in msg or 'rate' in msg
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                reason = "rate limit" if is_rate_limit else "error"
                logger.warning(f"Batch download {reason} (attempt {attempt+1}), sleep {wait}s: {e}")
                time.sleep(wait)
                continue
            logger.error(f"Batch download failed after {MAX_RETRIES} retries: {e}")
    return pd.DataFrame()


def _parse_batch_result(raw_df, tickers, universe_row_map):
    """把 yf.download (multi-index columns) 拆成 long format。"""
    if raw_df.empty:
        return pd.DataFrame()

    rows = []
    level0 = set(raw_df.columns.get_level_values(0))

    for tkr in tickers:
        if tkr not in level0:
            continue
        try:
            sub = raw_df[tkr].copy()
        except Exception:
            continue
        sub = sub.dropna(how='all')
        if sub.empty:
            continue
        sub = sub.reset_index()
        sub['yf_ticker'] = tkr

        meta = universe_row_map.get(tkr, {})
        sub['stock_id'] = meta.get('stock_id', '')
        sub['stock_name'] = meta.get('stock_name', '')
        sub['industry'] = meta.get('industry_category', '')
        sub['market_type'] = meta.get('type', '')
        rows.append(sub)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=True)
    out = out.rename(columns={
        'Date': 'date',
        'Adj Close': 'AdjClose',
        'Stock Splits': 'Splits',
    })

    # 確保關鍵欄位存在
    for col in ['Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume', 'Dividends', 'Splits']:
        if col not in out.columns:
            out[col] = pd.NA

    # 強制數值化（yfinance 2024+ 偶爾回傳 "0.94 TWD" 這類字串，pyarrow 會爆）
    for col in ['Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume', 'Dividends', 'Splits']:
        if out[col].dtype == object:
            # 抽取數字前綴（"0.94 TWD" → 0.94）
            out[col] = (
                out[col].astype(str)
                .str.extract(r'(-?\d+\.?\d*)', expand=False)
            )
        out[col] = pd.to_numeric(out[col], errors='coerce')

    # 正規化
    out['date'] = pd.to_datetime(out['date']).dt.tz_localize(None)
    return out[[
        'yf_ticker', 'stock_id', 'stock_name', 'industry', 'market_type',
        'date', 'Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume',
        'Dividends', 'Splits',
    ]]


# ============================================================
# Parquet I/O
# ============================================================
def _flush_to_parquet(results_list):
    """合併 batch results 寫進 parquet（upsert：同 ticker+date 以新的為準）。"""
    if not results_list:
        return
    new_df = pd.concat(results_list, ignore_index=True)
    if OUTPUT_PATH.exists():
        existing = pd.read_parquet(OUTPUT_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['yf_ticker', 'date'], keep='last')
    else:
        combined = new_df
    combined = combined.sort_values(['yf_ticker', 'date']).reset_index(drop=True)
    combined.to_parquet(OUTPUT_PATH, engine='pyarrow', compression='snappy')
    size_mb = OUTPUT_PATH.stat().st_size / 1e6
    logger.info(f"Flushed {len(new_df):,} new rows → parquet total {len(combined):,} rows ({size_mb:.1f} MB)")


def _get_completed_tickers():
    """讀 parquet 裡已下載的 tickers（resume 用）。"""
    if not OUTPUT_PATH.exists():
        return set()
    try:
        df = pd.read_parquet(OUTPUT_PATH, columns=['yf_ticker'])
        return set(df['yf_ticker'].unique())
    except Exception as e:
        logger.warning(f"Failed to read existing parquet: {e}")
        return set()


# ============================================================
# Continuity validation
# ============================================================
def validate_continuity(df, threshold=0.105, skip_first_n_days=30):
    """
    檢查相鄰日收盤價跳動超過 threshold 的異常。回傳問題清單。

    threshold 預設 10.5%（避開台股漲跌停 10% 浮點誤差）。
    skip_first_n_days: 跳過新上市初期（無漲跌幅限制）。
    """
    issues = []
    for ticker, sub in df.groupby('yf_ticker'):
        sub = sub.sort_values('date').dropna(subset=['Close'])
        if len(sub) < 2:
            continue
        # 跳過新上市初期
        sub = sub.iloc[skip_first_n_days:].copy() if len(sub) > skip_first_n_days else sub
        if len(sub) < 2:
            continue
        close = sub['Close'].astype(float)
        splits = sub.get('Splits', pd.Series(0, index=sub.index)).fillna(0)
        divs = sub.get('Dividends', pd.Series(0, index=sub.index)).fillna(0)

        pct_change = close.pct_change().abs()
        # 排除有 split (Split != 0 且非 1) 或 div 的日子
        has_action = (splits != 0) & (splits != 1) | (divs != 0)
        suspicious = (pct_change > threshold) & (~has_action)

        if suspicious.any():
            dates = sub.loc[suspicious, 'date'].dt.strftime('%Y-%m-%d').tolist()
            issues.append({
                'ticker': ticker,
                'stock_id': sub.iloc[0].get('stock_id', ''),
                'stock_name': sub.iloc[0].get('stock_name', ''),
                'n_anomalies': int(suspicious.sum()),
                'max_jump': float(pct_change[suspicious].max()),
                'sample_dates': ', '.join(dates[:5]),
            })
    return pd.DataFrame(issues)


# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Bulk download TW OHLCV for IC backtest")
    parser.add_argument('--years', type=int, default=15, help='Years of history (default 15)')
    parser.add_argument('--test', action='store_true', help='Test mode: 10 sample stocks')
    parser.add_argument('--resume', action='store_true', help='Skip already-downloaded tickers')
    parser.add_argument('--validate', action='store_true', help='Only validate existing parquet')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ----- Validate only -----
    if args.validate:
        if not OUTPUT_PATH.exists():
            logger.error(f"No parquet found at {OUTPUT_PATH}")
            return 1
        logger.info(f"Loading {OUTPUT_PATH}...")
        df = pd.read_parquet(OUTPUT_PATH)
        logger.info(f"Rows: {len(df):,}  Tickers: {df['yf_ticker'].nunique()}")
        issues = validate_continuity(df)
        if issues.empty:
            logger.info("OK No continuity issues (no unexplained >10% jumps)")
        else:
            logger.warning(f"WARN Found {len(issues)} tickers with continuity issues")
            issues.sort_values('max_jump', ascending=False).to_csv(ISSUES_PATH, index=False)
            logger.warning(f"Detail written to {ISSUES_PATH}")
            print(issues.sort_values('max_jump', ascending=False).head(20).to_string())
        return 0

    # ----- Universe -----
    universe = get_universe()
    if args.test:
        test_ids = ['2330', '2454', '2317', '2412', '3034', '3661', '5269', '6415', '2881', '2382']
        universe = universe[universe['stock_id'].isin(test_ids)].copy()
        logger.info(f"TEST MODE: {len(universe)} stocks")

    # ----- Date range -----
    end_date = datetime.now()
    start_date = end_date - timedelta(days=int(args.years * 365.25))
    logger.info(f"Date range: {start_date.date()} ~ {end_date.date()} ({args.years} years)")

    # ----- Resume -----
    completed = _get_completed_tickers() if args.resume else set()
    if completed:
        logger.info(f"Resume: skipping {len(completed)} tickers already in parquet")

    tickers_to_fetch = [t for t in universe['yf_ticker'] if t not in completed]
    if not tickers_to_fetch:
        logger.info("All universe tickers already downloaded.")
        return 0

    logger.info(f"Will fetch {len(tickers_to_fetch)} tickers in batches of {args.batch_size}")

    # ----- Download loop -----
    session = _make_yf_session()
    universe_row_map = universe.set_index('yf_ticker').to_dict(orient='index')

    buffer = []
    failed = []
    t0 = time.time()
    total_batches = (len(tickers_to_fetch) + args.batch_size - 1) // args.batch_size

    for i in range(0, len(tickers_to_fetch), args.batch_size):
        batch = tickers_to_fetch[i:i + args.batch_size]
        batch_idx = i // args.batch_size + 1
        logger.info(f"Batch {batch_idx}/{total_batches}: {batch[0]} ~ {batch[-1]} ({len(batch)} tickers)")

        raw = _download_batch(batch, start_date, end_date, session=session)
        if raw.empty:
            failed.extend(batch)
            continue

        parsed = _parse_batch_result(raw, batch, universe_row_map)
        if parsed.empty:
            failed.extend(batch)
            continue

        got = set(parsed['yf_ticker'].unique())
        failed.extend([t for t in batch if t not in got])
        buffer.append(parsed)

        # Incremental flush
        if batch_idx % FLUSH_EVERY_N_BATCHES == 0:
            _flush_to_parquet(buffer)
            buffer = []

        time.sleep(SLEEP_BETWEEN_BATCHES)

    # Final flush
    if buffer:
        _flush_to_parquet(buffer)

    elapsed = time.time() - t0
    logger.info(f"Download complete in {elapsed/60:.1f} min")
    if failed:
        logger.warning(f"Failed tickers: {len(failed)} ({failed[:10]}{'...' if len(failed)>10 else ''})")

    # ----- Final validation -----
    if OUTPUT_PATH.exists():
        df = pd.read_parquet(OUTPUT_PATH)
        size_mb = OUTPUT_PATH.stat().st_size / 1e6
        logger.info(f"Final: {len(df):,} rows, {df['yf_ticker'].nunique()} tickers, {size_mb:.1f} MB")
        logger.info("Running continuity validation...")
        issues = validate_continuity(df)
        if issues.empty:
            logger.info("OK No continuity issues")
        else:
            logger.warning(f"WARN {len(issues)} tickers have >10% jumps (after excluding split/div days)")
            issues.sort_values('max_jump', ascending=False).to_csv(ISSUES_PATH, index=False)
            logger.warning(f"Detail: {ISSUES_PATH}")
    return 0


if __name__ == '__main__':
    sys.exit(main() or 0)
