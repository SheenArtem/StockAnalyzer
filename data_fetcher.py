"""
數據獲取工具模組 (Data Fetcher Utilities)

提供統一的數據獲取、快取、錯誤處理接口
重構以減少代碼重複

功能:
1. 統一的數據快取策略
2. 增量更新支持
3. 錯誤處理與重試機制
4. 數據驗證
"""

import os
import pandas as pd
import numpy as np
import datetime
import logging
from abc import ABC, abstractmethod
from functools import wraps
import time

logger = logging.getLogger(__name__)

# 快取目錄
CACHE_DIR = "data_cache"


class CacheConfig:
    """
    快取配置類
    """
    # 快取有效期 (秒)
    PRICE_DATA_TTL = 3600 * 4  # 4小時
    CHIP_DATA_TTL = 3600 * 8   # 8小時 (籌碼通常隔夜更新)
    FUNDAMENTAL_TTL = 3600 * 24  # 24小時
    US_CHIP_TTL = 3600 * 1  # 1小時
    
    # 台股收盤時間
    TW_MARKET_CLOSE_HOUR = 13
    TW_MARKET_CLOSE_MINUTE = 30
    
    # 美股收盤時間 (美東)
    US_MARKET_CLOSE_HOUR = 16
    
    @classmethod
    def is_tw_market_closed(cls):
        """判斷台股是否已收盤"""
        now = datetime.datetime.now()
        close_time = now.replace(hour=cls.TW_MARKET_CLOSE_HOUR, minute=cls.TW_MARKET_CLOSE_MINUTE)
        return now > close_time


def retry_on_failure(max_retries=3, delay=1, backoff=2):
    """
    重試裝飾器
    
    Args:
        max_retries: 最大重試次數
        delay: 初始延遲秒數
        backoff: 延遲倍數
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {current_delay}s...")
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
            
            raise last_exception
        return wrapper
    return decorator


class DataValidator:
    """
    數據驗證工具類
    """
    
    @staticmethod
    def validate_price_df(df):
        """
        驗證價格 DataFrame 的有效性
        """
        if df is None or df.empty:
            return False, "DataFrame is empty"
        
        required_cols = ['Open', 'High', 'Low', 'Close']
        missing = [c for c in required_cols if c not in df.columns]
        
        if missing:
            return False, f"Missing columns: {missing}"
        
        # 檢查是否有 NaN
        nan_pct = df[required_cols].isna().sum().sum() / (len(df) * len(required_cols)) * 100
        if nan_pct > 10:
            return False, f"Too many NaN values: {nan_pct:.1f}%"
        
        return True, "Valid"
    
    @staticmethod
    def validate_chip_df(df, required_cols=None):
        """
        驗證籌碼 DataFrame 的有效性
        """
        if df is None or df.empty:
            return False, "DataFrame is empty"
        
        if required_cols:
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                return False, f"Missing columns: {missing}"
        
        return True, "Valid"


class BaseDataFetcher(ABC):
    """
    數據獲取基類
    
    提供統一的:
    - 快取讀取/寫入
    - 增量更新
    - 錯誤處理
    """
    
    def __init__(self, cache_ttl=3600):
        self.cache_ttl = cache_ttl
        self._ensure_cache_dir()
    
    def _ensure_cache_dir(self):
        """確保快取目錄存在"""
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)
    
    def _get_cache_path(self, key, data_type):
        """
        獲取快取文件路徑
        
        Args:
            key: 快取鍵 (通常是 ticker)
            data_type: 數據類型 ('price', 'chip', 'fundamental', 'us_chip')
        """
        safe_key = str(key).replace('.', '_').replace('/', '_')
        return os.path.join(CACHE_DIR, f"{safe_key}_{data_type}.csv")
    
    def _is_cache_valid(self, file_path, ttl=None):
        """
        檢查快取是否有效
        
        Args:
            file_path: 快取文件路徑
            ttl: 有效期 (秒)，None 則使用默認值
        """
        if not os.path.exists(file_path):
            return False, "miss"
        
        ttl = ttl or self.cache_ttl
        mtime = os.path.getmtime(file_path)
        file_age = time.time() - mtime
        
        if file_age < ttl:
            return True, "hit"
        
        return False, "stale"
    
    def load_from_cache(self, key, data_type, force_reload=False):
        """
        從快取加載數據
        
        Returns:
            tuple: (DataFrame, status, metadata)
            status: 'hit', 'miss', 'partial', 'stale'
        """
        if force_reload:
            return pd.DataFrame(), "miss", None
        
        file_path = self._get_cache_path(key, data_type)
        is_valid, status = self._is_cache_valid(file_path)
        
        if status == "miss":
            return pd.DataFrame(), "miss", None
        
        try:
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            
            # 確保索引是 DatetimeIndex
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index, errors='coerce')
            
            if is_valid:
                logger.debug(f"Cache hit for {key}_{data_type}")
                return df, "hit", None
            else:
                # 快取過期但數據存在，返回 partial 以支持增量更新
                last_date = df.index[-1] if not df.empty else None
                logger.debug(f"Cache stale for {key}_{data_type}, last date: {last_date}")
                return df, "partial", {"last_date": last_date}
                
        except Exception as e:
            logger.warning(f"Cache load error for {key}_{data_type}: {e}")
            return pd.DataFrame(), "miss", None
    
    def save_to_cache(self, key, df, data_type):
        """
        保存數據到快取
        """
        if df is None or df.empty:
            logger.warning(f"Attempted to save empty DataFrame for {key}_{data_type}")
            return False
        
        file_path = self._get_cache_path(key, data_type)
        
        try:
            df.to_csv(file_path)
            logger.debug(f"Saved cache for {key}_{data_type}")
            return True
        except Exception as e:
            logger.error(f"Cache save error for {key}_{data_type}: {e}")
            return False
    
    def merge_dataframes(self, old_df, new_df):
        """
        合併新舊數據 (用於增量更新)
        """
        if old_df.empty:
            return new_df
        if new_df.empty:
            return old_df
        
        # 合併並去重
        merged = pd.concat([old_df, new_df])
        merged = merged[~merged.index.duplicated(keep='last')]
        merged.sort_index(inplace=True)
        
        return merged
    
    @abstractmethod
    def fetch(self, key, **kwargs):
        """
        獲取數據 (子類需實現)
        
        Args:
            key: 數據鍵 (如股票代號)
        
        Returns:
            tuple: (data, error_message)
        """
        pass


class PriceDataFetcher(BaseDataFetcher):
    """
    股價數據獲取器
    """
    
    def __init__(self):
        super().__init__(cache_ttl=CacheConfig.PRICE_DATA_TTL)
    
    @retry_on_failure(max_retries=2, delay=1)
    def fetch(self, ticker, force_update=False, start_date=None):
        """
        獲取股價數據
        
        Args:
            ticker: 股票代號
            force_update: 是否強制更新
            start_date: 起始日期 (用於增量更新)
        """
        import yfinance as yf
        from FinMind.data import DataLoader
        
        # 嘗試讀取快取
        cached_df, status, meta = self.load_from_cache(ticker, 'price', force_update)
        
        if status == "hit":
            return cached_df, None
        
        # 確定起始日期
        if status == "partial" and meta and meta.get("last_date"):
            start_date = meta["last_date"] + datetime.timedelta(days=1)
        else:
            start_date = None
        
        df = pd.DataFrame()
        error = None
        
        # 判斷是否為台股
        is_tw = ticker.isdigit() or ticker.endswith('.TW') or ticker.endswith('.TWO')
        
        if is_tw:
            # 台股邏輯
            stock_id = ticker.split('.')[0] if '.' in ticker else ticker
            
            try:
                # 嘗試 yfinance
                yf_ticker = f"{stock_id}.TW"
                if start_date:
                    df = yf.download(yf_ticker, start=start_date.strftime('%Y-%m-%d'), progress=False)
                else:
                    df = yf.download(yf_ticker, period='10y', progress=False)
                
                if df.empty:
                    yf_ticker = f"{stock_id}.TWO"
                    if start_date:
                        df = yf.download(yf_ticker, start=start_date.strftime('%Y-%m-%d'), progress=False)
                    else:
                        df = yf.download(yf_ticker, period='10y', progress=False)
                
                if df.empty:
                    # Fallback to FinMind
                    dl = DataLoader()
                    fm_start = start_date.strftime('%Y-%m-%d') if start_date else '2016-01-01'
                    df = dl.taiwan_stock_daily(stock_id=stock_id, start_date=fm_start)
                    
                    if not df.empty:
                        df['date'] = pd.to_datetime(df['date'])
                        df = df.set_index('date')
                        df = df.rename(columns={
                            'open': 'Open', 'max': 'High', 'min': 'Low',
                            'close': 'Close', 'Trading_Volume': 'Volume'
                        })
                        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
                        
            except Exception as e:
                error = str(e)
                logger.error(f"TW Stock fetch error for {ticker}: {e}")
        
        else:
            # 美股邏輯
            try:
                if start_date:
                    df = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'), progress=False)
                else:
                    df = yf.download(ticker, period='10y', progress=False)
            except Exception as e:
                error = str(e)
                logger.error(f"US Stock fetch error for {ticker}: {e}")
        
        # 清理 MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        # 合併增量數據
        if status == "partial" and not cached_df.empty:
            df = self.merge_dataframes(cached_df, df)
        
        # 驗證並保存
        is_valid, msg = DataValidator.validate_price_df(df)
        if is_valid:
            self.save_to_cache(ticker, df, 'price')
        else:
            logger.warning(f"Invalid price data for {ticker}: {msg}")
        
        return df, error


class ChipDataFetcher(BaseDataFetcher):
    """
    籌碼數據獲取器 (台股)
    """
    
    def __init__(self):
        super().__init__(cache_ttl=CacheConfig.CHIP_DATA_TTL)
    
    def fetch(self, ticker, data_type='institutional', force_update=False):
        """
        獲取籌碼數據
        
        Args:
            ticker: 股票代號
            data_type: 'institutional', 'margin', 'day_trading', 'shareholding'
            force_update: 是否強制更新
        """
        from FinMind.data import DataLoader
        
        # 確保是台股
        stock_id = ticker.split('.')[0] if '.' in ticker else ticker
        if not stock_id.isdigit():
            return None, "Only Taiwan stocks supported"
        
        cache_key = f"{stock_id}_{data_type}"
        cached_df, status, meta = self.load_from_cache(cache_key, 'chip', force_update)
        
        if status == "hit":
            return cached_df, None
        
        try:
            dl = DataLoader()
            start_date = '2016-01-01'
            
            if status == "partial" and meta and meta.get("last_date"):
                start_date = (meta["last_date"] + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            
            df = pd.DataFrame()
            
            if data_type == 'institutional':
                raw = dl.taiwan_stock_institutional_investors(stock_id=stock_id, start_date=start_date)
                if not raw.empty:
                    raw['name'] = raw['name'].replace({
                        'Foreign_Investor': '外資', 'Investment_Trust': '投信',
                        'Dealer_Self': '自營商', 'Dealer_Hedging': '自營商'
                    })
                    if 'buy_sell' not in raw.columns and 'buy' in raw.columns:
                        raw['buy_sell'] = raw['buy'] - raw['sell']
                    df = raw.groupby(['date', 'name'])['buy_sell'].sum().unstack(fill_value=0)
                    df.index = pd.to_datetime(df.index)
                    
            elif data_type == 'margin':
                raw = dl.taiwan_stock_margin_purchase_short_sale(stock_id=stock_id, start_date=start_date)
                if not raw.empty:
                    raw['date'] = pd.to_datetime(raw['date'])
                    raw.set_index('date', inplace=True)
                    cols = ['MarginPurchaseTodayBalance', 'ShortSaleTodayBalance', 'MarginPurchaseLimit']
                    df = raw[[c for c in cols if c in raw.columns]].copy()
                    df = df.rename(columns={
                        'MarginPurchaseTodayBalance': '融資餘額',
                        'ShortSaleTodayBalance': '融券餘額',
                        'MarginPurchaseLimit': '融資限額'
                    })
                    
            elif data_type == 'day_trading':
                raw = dl.taiwan_stock_day_trading(stock_id=stock_id, start_date=start_date)
                if not raw.empty:
                    raw['date'] = pd.to_datetime(raw['date'])
                    raw.set_index('date', inplace=True)
                    df = raw[['Volume']].rename(columns={'Volume': 'DayTradingVolume'})
                    
            elif data_type == 'shareholding':
                raw = dl.taiwan_stock_shareholding(stock_id=stock_id, start_date=start_date)
                if not raw.empty:
                    raw['date'] = pd.to_datetime(raw['date'])
                    raw.set_index('date', inplace=True)
                    if 'ForeignInvestmentSharesRatio' in raw.columns:
                        df = raw[['ForeignInvestmentSharesRatio']].rename(
                            columns={'ForeignInvestmentSharesRatio': 'ForeignHoldingRatio'}
                        )
            
            # 合併增量數據
            if status == "partial" and not cached_df.empty:
                df = self.merge_dataframes(cached_df, df)
            
            if not df.empty:
                self.save_to_cache(cache_key, df, 'chip')
            
            return df, None
            
        except Exception as e:
            logger.error(f"Chip data fetch error for {ticker} ({data_type}): {e}")
            return cached_df if not cached_df.empty else None, str(e)


# 單例實例 (方便直接使用)
_price_fetcher = None
_chip_fetcher = None


def get_price_fetcher():
    """獲取價格數據獲取器單例"""
    global _price_fetcher
    if _price_fetcher is None:
        _price_fetcher = PriceDataFetcher()
    return _price_fetcher


def get_chip_fetcher():
    """獲取籌碼數據獲取器單例"""
    global _chip_fetcher
    if _chip_fetcher is None:
        _chip_fetcher = ChipDataFetcher()
    return _chip_fetcher


# =========================================
# 便捷函數 (兼容舊接口)
# =========================================

def fetch_price_data(ticker, force_update=False):
    """
    便捷函數: 獲取股價數據
    """
    fetcher = get_price_fetcher()
    return fetcher.fetch(ticker, force_update=force_update)


def fetch_chip_data(ticker, force_update=False):
    """
    便捷函數: 獲取完整籌碼數據
    """
    fetcher = get_chip_fetcher()
    
    result = {}
    errors = []
    
    for dtype in ['institutional', 'margin', 'day_trading', 'shareholding']:
        df, err = fetcher.fetch(ticker, data_type=dtype, force_update=force_update)
        result[dtype] = df if df is not None else pd.DataFrame()
        if err:
            errors.append(f"{dtype}: {err}")
    
    return result, "; ".join(errors) if errors else None


if __name__ == "__main__":
    # 測試
    logging.basicConfig(level=logging.DEBUG)
    
    print("\n=== Testing Price Fetcher ===")
    df, err = fetch_price_data("2330")
    print(f"2330: {len(df)} rows, error: {err}")
    
    df, err = fetch_price_data("AAPL")
    print(f"AAPL: {len(df)} rows, error: {err}")
    
    print("\n=== Testing Chip Fetcher ===")
    chip, err = fetch_chip_data("2330")
    for k, v in chip.items():
        print(f"  {k}: {len(v)} rows")
