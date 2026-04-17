
import os
import re
import logging
import threading
import pandas as pd
import datetime
import time

logger = logging.getLogger(__name__)

CACHE_DIR = "data_cache"

# Module-level lock for cache write operations
_cache_lock = threading.Lock()


# ================================================================
# FinMind low-frequency disk cache (P1-P3 optimization, 2026-04-17)
# ================================================================
# 財報季更、月營收月更、股利年更 — 不需要每日抓
# 磁碟快取顯著節省 FinMind 配額（估計 -75% 每日 SCAN 用量）

def get_finmind_cached(dl, cache_key, stock_id, method_name, ttl_days,
                       start_date_filter=None, fixed_wide_start='2015-01-01'):
    """Disk-cached FinMind fetch for low-frequency datasets.

    永遠用 `fixed_wide_start` 抓完整歷史快取，caller 可選傳 `start_date_filter`
    在回傳前過濾。TTL 依資料更新頻率設定：
      - 財報（季更）: 60 days
      - 月營收（月更）: 20 days
      - 股利（年/半年）: 30 days

    Args:
        dl: FinMindTracker (or raw DataLoader) instance
        cache_key: 快取分類鍵，例：'financial_statement' / 'month_revenue'
        stock_id: 股票代號
        method_name: dl 上的方法名，例：'taiwan_stock_financial_statement'
        ttl_days: 快取有效天數
        start_date_filter: 呼叫端想要的起始日期（optional，讀取時過濾）
        fixed_wide_start: 快取底層一律用此起始日抓（預設 2015-01-01，涵蓋 10 年）

    Returns:
        DataFrame（empty 時回 pd.DataFrame()）
    """
    from pathlib import Path
    path = Path(CACHE_DIR) / 'finmind_cache' / f"{cache_key}_{stock_id}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)

    df = None
    # 1. 嘗試讀磁碟快取
    if path.exists():
        age_days = (time.time() - path.stat().st_mtime) / 86400
        if age_days < ttl_days:
            try:
                df = pd.read_parquet(path)
                logger.debug("finmind_cache HIT %s/%s (age %.1fd)", cache_key, stock_id, age_days)
            except Exception as e:
                logger.warning("finmind_cache read failed %s: %s", path, e)
                df = None

    # 2. 快取 miss 或過期 → 抓 FinMind
    if df is None:
        try:
            method = getattr(dl, method_name)
            df = method(stock_id=stock_id, start_date=fixed_wide_start)
            if df is not None and not df.empty:
                with _cache_lock:
                    try:
                        df.to_parquet(path)
                        logger.debug("finmind_cache WRITE %s/%s (%d rows)",
                                     cache_key, stock_id, len(df))
                    except Exception as e:
                        logger.warning("finmind_cache write failed %s: %s", path, e)
        except Exception as e:
            logger.warning("FinMind fetch failed %s/%s: %s", cache_key, stock_id, e)
            return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    # 3. Apply start_date filter
    if start_date_filter and 'date' in df.columns:
        try:
            filter_date = pd.to_datetime(start_date_filter)
            df_dates = pd.to_datetime(df['date'])
            df = df[df_dates >= filter_date].copy()
        except Exception as e:
            logger.debug("start_date_filter skipped: %s", e)

    return df

# ================================================================
# FinMind DataLoader Factory (shared, token-aware, rate-tracked)
# ================================================================
_finmind_tracker = None
_finmind_lock = threading.Lock()

_FINMIND_RATE_LIMIT = 600       # requests per hour (free tier with token)
_FINMIND_RATE_WARN = 540        # warn threshold
_FINMIND_RATE_PAUSE = 580       # auto-pause threshold


class FinMindTracker:
    """Wraps DataLoader with request counting and rate limit tracking."""

    def __init__(self, dl, has_token):
        self._dl = dl
        self.has_token = has_token
        self.request_count = 0
        self._hour_start = time.time()
        self._lock = threading.Lock()

    def __getattr__(self, name):
        """Proxy all DataLoader method calls through the tracker."""
        attr = getattr(self._dl, name)
        if not callable(attr):
            return attr

        def tracked_call(*args, **kwargs):
            self._check_rate_limit()
            with self._lock:
                self.request_count += 1
                count = self.request_count
            if count % 50 == 0:
                elapsed = time.time() - self._hour_start
                rate = count / (elapsed / 3600) if elapsed > 0 else 0
                logger.info("FinMind API: %d requests (%.0f/hr rate), %.0fs elapsed",
                            count, rate, elapsed)
            if count == _FINMIND_RATE_WARN:
                logger.warning("FinMind API: approaching rate limit (%d/%d)",
                               count, _FINMIND_RATE_LIMIT)
            try:
                return attr(*args, **kwargs)
            except KeyError as e:
                if str(e) == "'data'":
                    # FinMind server-side quota: response has no 'data' key
                    logger.warning("FinMind quota hit (KeyError 'data'), "
                                   "waiting 65s then retry once...")
                    time.sleep(65)
                    try:
                        return attr(*args, **kwargs)
                    except KeyError:
                        raise  # second failure: give up
                raise

        return tracked_call

    @staticmethod
    def _seconds_until_next_wall_hour():
        """Calculate seconds until the next wall-clock hour boundary + 5s buffer."""
        import datetime as _dt
        now = _dt.datetime.now()
        next_hour = now.replace(minute=0, second=0, microsecond=0) + _dt.timedelta(hours=1)
        return (next_hour - now).total_seconds() + 5

    def _check_rate_limit(self):
        """Auto-pause if approaching rate limit, reset counter each hour."""
        elapsed = time.time() - self._hour_start

        # Reset counter every hour
        if elapsed >= 3600:
            with self._lock:
                old_count = self.request_count
                self.request_count = 0
                self._hour_start = time.time()
            if old_count > 0:
                logger.info("FinMind API: hour reset (was %d requests)", old_count)
            return

        if self.request_count >= _FINMIND_RATE_PAUSE:
            # Wait until next wall-clock hour (more likely to align with server reset)
            wait_seconds = self._seconds_until_next_wall_hour()
            logger.warning(
                "FinMind API: rate limit reached (%d/%d), pausing %.0fs until next hour",
                self.request_count, _FINMIND_RATE_LIMIT, wait_seconds,
            )
            print(f"[FinMind] Rate limit ({self.request_count}/{_FINMIND_RATE_LIMIT}), "
                  f"waiting {wait_seconds:.0f}s until next hour...")
            time.sleep(wait_seconds)
            with self._lock:
                self.request_count = 0
                self._hour_start = time.time()

    def get_stats(self):
        """Return current API usage stats."""
        elapsed = time.time() - self._hour_start
        return {
            'request_count': self.request_count,
            'elapsed_seconds': round(elapsed, 1),
            'rate_per_hour': round(self.request_count / (elapsed / 3600), 1) if elapsed > 0 else 0,
            'remaining': _FINMIND_RATE_LIMIT - self.request_count,
            'has_token': self.has_token,
        }


def get_finmind_loader():
    """
    Get a shared FinMind DataLoader with API token and rate tracking.

    Token is read from local/.env (FINMIND_API_TOKEN=...).
    Tracks API call count, logs every 50 calls, auto-pauses at 580/600.
    """
    global _finmind_tracker
    if _finmind_tracker is not None:
        return _finmind_tracker

    with _finmind_lock:
        if _finmind_tracker is not None:
            return _finmind_tracker

        from FinMind.data import DataLoader
        dl = DataLoader()

        # Try to load token from local/.env
        env_path = os.path.join(os.path.dirname(__file__), 'local', '.env')
        token = None
        if os.path.exists(env_path):
            try:
                with open(env_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('FINMIND_API_TOKEN=') and '=' in line:
                            token = line.split('=', 1)[1].strip()
                            break
            except Exception as e:
                logger.warning("Failed to read FinMind token: %s", e)

        has_token = False
        if token:
            try:
                dl.login_by_token(api_token=token)
                has_token = True
                logger.info("FinMind: logged in with API token")
            except Exception as e:
                logger.warning("FinMind token login failed: %s", e)
        else:
            logger.warning("FinMind: no API token, anonymous mode (lower rate limit)")

        _finmind_tracker = FinMindTracker(dl, has_token)
        return _finmind_tracker


def get_finmind_stats():
    """Get current FinMind API usage stats (or None if not initialized)."""
    if _finmind_tracker is not None:
        return _finmind_tracker.get_stats()
    return None

# 盤中快取過期時間 (秒) - 交易時段內快取僅維持 5 分鐘
INTRADAY_CACHE_TTL = 60 * 5  # 5 分鐘

class CacheManager:
    """
    Local Data Cache Manager
    
    Strategy:
    - Save DataFrames as CSV in `data_cache/`
    - Timestamp Check:
        - If file modified date == Today and Current Time > 13:30 (Market Close), it's considered "Final" for today.
        - If file modified date < Today, it's stale (needs update).
        - During trading hours (09:00-13:30 weekdays), cache expires after INTRADAY_CACHE_TTL (5 min).
    """
    
    def __init__(self):
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)

    @staticmethod
    def _is_tw_trading_hours():
        """
        判斷目前是否在台股盤中交易時段 (週一至週五 09:00 ~ 13:30)
        """
        now = datetime.datetime.now()
        # 週六(5) / 週日(6) 不是交易日
        if now.weekday() >= 5:
            return False
        market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
        market_close = now.replace(hour=13, minute=30, second=0, microsecond=0)
        return market_open <= now <= market_close

    def _get_path(self, ticker, data_type):
        """
        data_type: 'price' or 'chip'
        """
        safe_ticker = ticker.replace('.TW', '').replace('.TWO', '')
        # Defense-in-depth: strip any path separators
        safe_ticker = os.path.basename(safe_ticker)
        safe_ticker = re.sub(r'[^A-Za-z0-9_\-]', '', safe_ticker)
        return os.path.join(CACHE_DIR, f"{safe_ticker}_{data_type}.csv")

    def load_cache(self, ticker, data_type, force_reload=False):
        """
        Attempt to load data from cache.
        Returns: (DataFrame, status, last_date)
        status: "hit", "miss", "partial"
        last_date: datetime or None (only for partial)
        """
        if force_reload:
            return pd.DataFrame(), "miss", None
            
        file_path = self._get_path(ticker, data_type)
        
        if not os.path.exists(file_path):
            return pd.DataFrame(), "miss", None
            
        # Check timestamp
        mtime = os.path.getmtime(file_path)
        file_time = datetime.datetime.fromtimestamp(mtime)
        now = datetime.datetime.now()
        file_age_seconds = time.time() - mtime
        
        try:
            logger.debug(f"Loading {data_type} cache for {ticker}...")
            if data_type == 'price':
                df = pd.read_csv(file_path, index_col=0)
                # [FIX] yfinance 新版多層 header: 移除 "Ticker"/"Price"/"Date" 等非數據行
                bad_idx = df.index.astype(str).isin(['Ticker', 'Price', 'Date', ''])
                if bad_idx.any():
                    df = df[~bad_idx]
                # Ensure index is datetime
                df.index = pd.to_datetime(df.index, errors='coerce')
                df = df[df.index.notna()]
                # Ensure numeric columns
                for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df = pd.read_csv(file_path)
                # Chip data usually has 'date' column
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)

            # Check if stale
            last_date = df.index[-1] if (isinstance(df.index, pd.DatetimeIndex) and not df.empty) else None

            # 1. 盤中時段: 不論檔案日期，只要超過 TTL 就觸發增量更新
            if self._is_tw_trading_hours() and data_type == 'price':
                if file_age_seconds > INTRADAY_CACHE_TTL:
                    logger.info(f"盤中模式: 快取已超過 {INTRADAY_CACHE_TTL//60} 分鐘，觸發增量更新...")
                    return df, "partial", last_date
                else:
                    remaining = int(INTRADAY_CACHE_TTL - file_age_seconds)
                    logger.debug(f"盤中模式: 快取仍有效 (剩餘 {remaining} 秒)")
                    return df, "hit", None

            # 2. 非盤中: 若資料最後日期 < 今天，觸發增量更新
            if last_date and last_date.date() < now.date():
                return df, "partial", last_date

            return df, "hit", None
            
        except Exception as e:
            logger.error(f"Corrupt cache: {e}", exc_info=True)
            return pd.DataFrame(), "miss", None

    def save_cache(self, ticker, df, data_type):
        """
        Save DataFrame to cache (atomic write with temp file)
        """
        if df.empty:
            return

        file_path = self._get_path(ticker, data_type)
        tmp_path = file_path + ".tmp"
        with _cache_lock:
            try:
                df.to_csv(tmp_path)
                os.replace(tmp_path, file_path)
                logger.info(f"Saved {data_type} cache for {ticker}")
            except Exception as e:
                logger.error(f"Failed to save cache: {e}", exc_info=True)
                # Clean up temp file on failure
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
            
    def clear_cache(self):
        """Delete all files in cache dir"""
        import shutil
        with _cache_lock:
            try:
                shutil.rmtree(CACHE_DIR)
            except Exception as e:
                logger.error(f"Failed to clear cache directory: {e}")
            os.makedirs(CACHE_DIR, exist_ok=True)

    def list_cached_tickers(self):
        """
        List all unique tickers currently in cache (based on price files).
        Returns: list of str (e.g., ['2330', 'TSM'])
        """
        if not os.path.exists(CACHE_DIR):
            return []
            
        ticker_files = []
        for f in os.listdir(CACHE_DIR):
            if f.endswith('_price.csv'):
                # filename format: {ticker}_price.csv
                ticker = f.replace('_price.csv', '')
                file_path = os.path.join(CACHE_DIR, f)
                mtime = os.path.getmtime(file_path)
                ticker_files.append((ticker, mtime))
        
        # Sort by mtime desc
        ticker_files.sort(key=lambda x: x[1], reverse=True)
        
        # Return top 20 tickers
        return [t[0] for t in ticker_files[:20]]

    def delete_ticker_cache(self, ticker):
        """
        Delete cache files for a specific ticker.
        """
        deleted = False
        # Try delete price
        price_path = self._get_path(ticker, 'price')
        if os.path.exists(price_path):
            os.remove(price_path)
            deleted = True

        # Try delete chip (inst, margin, day_trading, shareholding)
        for chip_type in ['inst', 'margin', 'day_trading', 'shareholding']:
            chip_path = self._get_path(f"{ticker}_{chip_type}", 'chip')
            if os.path.exists(chip_path):
                os.remove(chip_path)
                deleted = True

        return deleted
