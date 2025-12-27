
import os
import pandas as pd
import datetime

CACHE_DIR = "data_cache"

class CacheManager:
    """
    Local Data Cache Manager
    
    Strategy:
    - Save DataFrames as CSV in `data_cache/`
    - Timestamp Check:
        - If file modified date == Today and Current Time > 13:30 (Market Close), it's considered "Final" for today.
        - If file modified date < Today, it's stale (needs update).
        - If current time < 13:30, we might want to fetch real-time, BUT to save calls, 
          we can set a 'cache_validity_period' (e.g. 1 hour).
    """
    
    def __init__(self):
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)
            
    def _get_path(self, ticker, data_type):
        """
        data_type: 'price' or 'chip'
        """
        safe_ticker = ticker.replace('.TW', '').replace('.TWO', '')
        return os.path.join(CACHE_DIR, f"{safe_ticker}_{data_type}.csv")

    def load_cache(self, ticker, data_type, force_reload=False):
        """
        Attempt to load data from cache.
        Returns: (DataFrame, bool_is_hit)
        """
        if force_reload:
            return pd.DataFrame(), False
            
        file_path = self._get_path(ticker, data_type)
        
        if not os.path.exists(file_path):
            return pd.DataFrame(), False
            
        # Check timestamp
        mtime = os.path.getmtime(file_path)
        file_time = datetime.datetime.fromtimestamp(mtime)
        now = datetime.datetime.now()
        
        # Logic:
        # 1. If file is from yesterday or older -> Stale (Need new daily candle)
        if file_time.date() < now.date():
            # Special case: If today is weekend, Friday's data is still valid?
            # Simpler: Just expire at midnight.
            return pd.DataFrame(), False
            
        # 2. If file is from today
        # If today is trading day and time is < 13:30, data might be incomplete.
        # But for 'Right-Side Trading', we usually analyze after close.
        # Let's assume: If we have a file from today, we trust it for "Performance".
        # User can click "Force Reload" if they want real-time.
        
        try:
            print(f"📂 Loading {data_type} cache for {ticker}...")
            if data_type == 'price':
                df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            else:
                df = pd.read_csv(file_path) # Chip data might not have date index
                
            return df, True
        except Exception as e:
            print(f"❌ Corrupt cache: {e}")
            return pd.DataFrame(), False

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
            
        tickers = set()
        for f in os.listdir(CACHE_DIR):
            if f.endswith('_price.csv'):
                # filename format: {ticker}_price.csv
                ticker = f.replace('_price.csv', '')
                tickers.add(ticker)
                
        return sorted(list(tickers))

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
