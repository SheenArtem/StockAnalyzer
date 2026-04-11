
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
# FinMind DataLoader Factory (shared, token-aware)
# ================================================================
_finmind_dl = None
_finmind_lock = threading.Lock()


def get_finmind_loader():
    """
    Get a shared FinMind DataLoader instance with API token.

    Token is read from local/.env (FINMIND_API_TOKEN=...).
    Falls back to anonymous mode if token not found.
    """
    global _finmind_dl
    if _finmind_dl is not None:
        return _finmind_dl

    with _finmind_lock:
        if _finmind_dl is not None:
            return _finmind_dl

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

        if token:
            try:
                dl.login_by_token(api_token=token)
                logger.info("FinMind: logged in with API token")
            except Exception as e:
                logger.warning("FinMind token login failed: %s", e)
        else:
            logger.warning("FinMind: no API token found, using anonymous mode (lower rate limit)")

        _finmind_dl = dl
        return _finmind_dl

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
