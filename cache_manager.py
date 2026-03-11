
import os
import pandas as pd
import datetime
import time

CACHE_DIR = "data_cache"

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
            print(f"📂 Loading {data_type} cache for {ticker}...")
            if data_type == 'price':
                df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                # Ensure index is datetime
                if not isinstance(df.index, pd.DatetimeIndex):
                     df.index = pd.to_datetime(df.index)
            else:
                df = pd.read_csv(file_path)
                # Chip data usually has 'date' column
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)

            # Check if stale
            # 1. If file is from yesterday or older -> Stale (Need new daily candle)
            if file_time.date() < now.date() and not df.empty:
                # Return partial hit with last date
                last_date = df.index[-1] if isinstance(df.index, pd.DatetimeIndex) else None
                # If last_date is today or later, it's actually fresh
                if last_date and last_date.date() >= now.date():
                     return df, "hit", None
                
                return df, "partial", last_date

            # 2. File is from today
            # 2a. 盤中時段: 若快取超過 INTRADAY_CACHE_TTL (5分鐘)，視為過期需重抓
            if self._is_tw_trading_hours() and data_type == 'price':
                if file_age_seconds > INTRADAY_CACHE_TTL:
                    print(f"🔄 盤中模式: 快取已超過 {INTRADAY_CACHE_TTL//60} 分鐘，觸發增量更新...")
                    last_date = df.index[-1] if isinstance(df.index, pd.DatetimeIndex) and not df.empty else None
                    return df, "partial", last_date
                else:
                    remaining = int(INTRADAY_CACHE_TTL - file_age_seconds)
                    print(f"⚡ 盤中模式: 快取仍有效 (剩餘 {remaining} 秒)")

            return df, "hit", None
            
        except Exception as e:
            print(f"❌ Corrupt cache: {e}")
            return pd.DataFrame(), "miss", None

    def save_cache(self, ticker, df, data_type):
        """
        Save DataFrame to cache
        """
        if df.empty:
            return
            
        file_path = self._get_path(ticker, data_type)
        try:
            df.to_csv(file_path)
            print(f"💾 Saved {data_type} cache for {ticker}")
        except Exception as e:
            print(f"❌ Failed to save cache: {e}")
            
    def clear_cache(self):
        """Delete all files in cache dir"""
        import shutil
        shutil.rmtree(CACHE_DIR)
        os.makedirs(CACHE_DIR)

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
            
        # Try delete chip (inst & margin)
        # Chip keys were like {ticker}_inst, {ticker}_margin
        inst_path = self._get_path(f"{ticker}_inst", 'chip')
        if os.path.exists(inst_path):
            os.remove(inst_path)
            deleted = True

        margin_path = self._get_path(f"{ticker}_margin", 'chip')
        if os.path.exists(margin_path):
            os.remove(margin_path)
            deleted = True
            
        return deleted
