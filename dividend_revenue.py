"""
Dividend ex-date analysis and monthly revenue tracking for Taiwan stocks.

Provides two main classes:
- DividendAnalyzer: ex-dividend calendar, fill-gap analysis, dividend history
- RevenueTracker: monthly revenue data, YoY/MoM trends, surprise detection
"""

import logging
import time
import datetime
import threading

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level in-memory cache with TTL
# ---------------------------------------------------------------------------
_mem_cache = {}
_mem_cache_lock = threading.Lock()

# TTL constants (seconds)
_DIVIDEND_TTL = 3600   # 1 hour
_REVENUE_TTL = 1800    # 30 minutes


def _cache_get(key: str):
    """Return cached value if still within TTL, else None."""
    with _mem_cache_lock:
        entry = _mem_cache.get(key)
        if entry is None:
            return None
        value, ts, ttl = entry
        if time.time() - ts > ttl:
            del _mem_cache[key]
            return None
        return value


def _cache_set(key: str, value, ttl: int):
    """Store value in memory cache with given TTL."""
    with _mem_cache_lock:
        _mem_cache[key] = (value, time.time(), ttl)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
_GOODINFO_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'https://goodinfo.tw/',
    'Accept-Language': 'zh-TW,zh;q=0.9',
}


def _request_goodinfo(url: str, timeout: int = 15):
    """
    Request a Goodinfo page.  Try requests first; fall back to curl_cffi if
    Goodinfo blocks the plain requests library (403 / connection reset).
    """
    # 第一順位: requests
    try:
        resp = requests.get(url, headers=_GOODINFO_HEADERS, timeout=timeout)
        if resp.status_code == 200:
            # Goodinfo 頁面用 big5 編碼
            resp.encoding = 'big5'
            return resp.text
        logger.warning("Goodinfo returned status %d for %s", resp.status_code, url)
    except Exception as exc:
        logger.warning("requests failed for Goodinfo: %s", exc)

    # 第二順位: curl_cffi (已在專案依賴)
    try:
        from curl_cffi import requests as curl_requests  # type: ignore
        resp = curl_requests.get(
            url,
            headers=_GOODINFO_HEADERS,
            timeout=timeout,
            impersonate="chrome",
        )
        if resp.status_code == 200:
            resp.encoding = 'big5'
            return resp.text
        logger.warning("curl_cffi returned status %d for %s", resp.status_code, url)
    except ImportError:
        logger.warning("curl_cffi not available, skipping fallback")
    except Exception as exc:
        logger.warning("curl_cffi failed for Goodinfo: %s", exc)

    return None


def _get_finmind_loader():
    """Get shared FinMind DataLoader with API token."""
    from cache_manager import get_finmind_loader
    return get_finmind_loader()


# ===================================================================
# DividendAnalyzer
# ===================================================================

class DividendAnalyzer:
    """Ex-dividend calendar, fill-gap analysis, and dividend history."""

    # -----------------------------------------------------------------
    # get_dividend_history
    # -----------------------------------------------------------------
    def get_dividend_history(self, stock_id: str, years: int = 5) -> pd.DataFrame:
        """Fetch historical dividend data for a Taiwan stock.

        Tries Goodinfo.tw first, then falls back to FinMind TaiwanStockDividend.

        Returns
        -------
        pd.DataFrame
            Columns: year, cash_dividend, stock_dividend, total_dividend,
                     ex_date, fill_days, yield_pct
        """
        stock_id = self._normalize_id(stock_id)
        cache_key = f"div_hist_{stock_id}_{years}"
        cached = _cache_get(cache_key)
        if cached is not None:
            logger.info("Dividend history cache hit for %s", stock_id)
            return cached

        df = self._fetch_dividend_goodinfo(stock_id, years)
        if df is None or df.empty:
            logger.info("Goodinfo unavailable for %s, trying FinMind", stock_id)
            df = self._fetch_dividend_finmind(stock_id, years)

        if df is None or df.empty:
            logger.warning("No dividend data found for %s", stock_id)
            df = self._empty_dividend_df()

        _cache_set(cache_key, df, _DIVIDEND_TTL)
        return df

    # -----------------------------------------------------------------
    # get_upcoming_ex_dates
    # -----------------------------------------------------------------
    def get_upcoming_ex_dates(self, stock_id: str) -> dict:
        """Check if there are upcoming ex-dividend dates.

        Returns
        -------
        dict
            has_upcoming, ex_date, dividend_amount, yield_pct, days_until
        """
        stock_id = self._normalize_id(stock_id)
        default = {
            'has_upcoming': False,
            'ex_date': None,
            'dividend_amount': 0.0,
            'yield_pct': 0.0,
            'days_until': 0,
        }

        try:
            dl = _get_finmind_loader()
            today = datetime.date.today()
            start = today.strftime('%Y-%m-%d')
            # 查詢未來 120 天的除息日
            end = (today + datetime.timedelta(days=120)).strftime('%Y-%m-%d')

            raw = dl.taiwan_stock_dividend(
                stock_id=stock_id,
                start_date=start,
                end_date=end,
            )
            if raw is None or raw.empty:
                return default

            # FinMind 欄位: date, stock_id, CashEarningsDistribution, ...
            if 'date' not in raw.columns:
                return default

            raw['date'] = pd.to_datetime(raw['date'])
            raw = raw.sort_values('date')
            next_row = raw.iloc[0]

            ex_date = next_row['date']
            cash = float(next_row.get('CashEarningsDistribution', 0) or 0) + \
                   float(next_row.get('CashStaticDistribution', 0) or 0)
            stock_div = float(next_row.get('StockEarningsDistribution', 0) or 0) + \
                        float(next_row.get('StockStaticDistribution', 0) or 0)
            total = cash + stock_div

            # 估算殖利率 (需要現價)
            yield_pct = self._estimate_yield(stock_id, cash)
            days_until = (ex_date.date() - today).days if hasattr(ex_date, 'date') else 0

            return {
                'has_upcoming': True,
                'ex_date': ex_date.strftime('%Y-%m-%d'),
                'dividend_amount': round(total, 2),
                'yield_pct': round(yield_pct, 2),
                'days_until': max(days_until, 0),
            }
        except Exception as exc:
            logger.error("Error fetching upcoming ex-dates for %s: %s", stock_id, exc)
            return default

    # -----------------------------------------------------------------
    # get_fill_gap_stats
    # -----------------------------------------------------------------
    def get_fill_gap_stats(self, stock_id: str) -> dict:
        """Analyze historical fill-gap (填息) patterns.

        Returns
        -------
        dict
            avg_fill_days, fill_rate, fastest_fill, slowest_fill, recommendation
        """
        stock_id = self._normalize_id(stock_id)
        default = {
            'avg_fill_days': 0.0,
            'fill_rate': 0.0,
            'fastest_fill': 0,
            'slowest_fill': 0,
            'recommendation': '資料不足，無法評估',
        }

        df = self.get_dividend_history(stock_id, years=10)
        if df.empty:
            return default

        # Exclude future ex-dates (they can't have fill_days yet)
        today_str = datetime.date.today().strftime('%Y-%m-%d')
        past_df = df[df['ex_date'].notna() & (df['ex_date'] < today_str)]
        if past_df.empty:
            return default

        # 有填息天數的 = 成功填息的年度
        filled = past_df.dropna(subset=['fill_days'])
        if filled.empty:
            # All past ex-dates failed to fill within 60 days
            return {
                'avg_fill_days': 0.0,
                'fill_rate': 0.0,
                'fastest_fill': 0,
                'slowest_fill': 0,
                'recommendation': '過去均未在60日內完成填息',
            }

        fill_days_list = filled['fill_days'].astype(float).tolist()

        # 60 個交易日內完成填息的比率
        # 分母 = 所有過去曾除息的年度 (含未填息的)
        filled_within_60 = [d for d in fill_days_list if d <= 60]
        total_past_events = len(past_df)
        fill_rate = (len(filled_within_60) / total_past_events * 100) if total_past_events > 0 else 0.0
        avg_fill = float(np.mean(fill_days_list)) if fill_days_list else 0.0

        # 建議
        if fill_rate > 80 and avg_fill < 30:
            recommendation = '填息能力強，適合除息前佈局'
        elif fill_rate > 60:
            recommendation = '填息能力中等，可搭配其他指標判斷'
        else:
            recommendation = '填息能力偏弱，建議除息後觀察'

        return {
            'avg_fill_days': round(avg_fill, 1),
            'fill_rate': round(fill_rate, 1),
            'fastest_fill': int(min(fill_days_list)) if fill_days_list else 0,
            'slowest_fill': int(max(fill_days_list)) if fill_days_list else 0,
            'recommendation': recommendation,
        }

    # =================================================================
    # Private helpers
    # =================================================================
    @staticmethod
    def _normalize_id(stock_id: str) -> str:
        """Strip .TW suffix and whitespace."""
        stock_id = str(stock_id).strip()
        if stock_id.upper().endswith('.TW'):
            stock_id = stock_id[:-3]
        if stock_id.upper().endswith('.TWO'):
            stock_id = stock_id[:-4]
        return stock_id

    @staticmethod
    def _empty_dividend_df() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            'year', 'cash_dividend', 'stock_dividend', 'total_dividend',
            'ex_date', 'fill_days', 'yield_pct',
        ])

    # -----------------------------------------------------------------
    # Goodinfo scraper
    # -----------------------------------------------------------------
    def _fetch_dividend_goodinfo(self, stock_id: str, years: int) -> pd.DataFrame | None:
        """Scrape dividend policy page from Goodinfo.tw."""
        url = f"https://goodinfo.tw/tw/StockDividendPolicy.asp?STOCK_ID={stock_id}"
        html = _request_goodinfo(url)
        if html is None:
            return None

        try:
            soup = BeautifulSoup(html, 'lxml')
        except Exception:
            # lxml 解析失敗改用 html.parser
            try:
                soup = BeautifulSoup(html, 'html.parser')
            except Exception as exc:
                logger.error("HTML parse error for Goodinfo dividend: %s", exc)
                return None

        # Goodinfo 股利政策表通常在 id='tblDetail' 或包含「股利」的表格
        table = None
        for t in soup.find_all('table'):
            header_text = t.get_text()
            if '現金股利' in header_text and '股票股利' in header_text:
                table = t
                break

        if table is None:
            logger.warning("Could not locate dividend table on Goodinfo for %s", stock_id)
            return None

        rows = table.find_all('tr')
        records = []
        current_year = datetime.date.today().year
        cutoff_year = current_year - years

        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 5:
                continue

            cell_texts = [c.get_text(strip=True) for c in cells]

            # 第一欄通常是年度 (e.g., "113", "112" 或 "2024", "2023")
            year_text = cell_texts[0]
            year_val = self._parse_year(year_text)
            if year_val is None or year_val < cutoff_year:
                continue

            # 嘗試從表格列提取: 現金股利, 股票股利, 合計, 除息日, 填息天數, 殖利率
            # 各 Goodinfo 版面欄位位置可能不同，用安全取值
            cash_div = self._safe_float(cell_texts, 1, 0.0)
            stock_div = self._safe_float(cell_texts, 2, 0.0)
            total_div = self._safe_float(cell_texts, 3, cash_div + stock_div)

            # 除息日 / 填息天數 / 殖利率可能在不同欄位
            ex_date_str = self._find_date_cell(cell_texts[4:])
            fill_days_val = self._find_fill_days(cell_texts)
            yield_val = self._find_yield(cell_texts)

            records.append({
                'year': year_val,
                'cash_dividend': cash_div,
                'stock_dividend': stock_div,
                'total_dividend': total_div,
                'ex_date': ex_date_str,
                'fill_days': fill_days_val,
                'yield_pct': yield_val,
            })

        if not records:
            return None

        df = pd.DataFrame(records)
        df = df.sort_values('year', ascending=False).reset_index(drop=True)
        return df

    # -----------------------------------------------------------------
    # FinMind fallback
    # -----------------------------------------------------------------
    def _fetch_dividend_finmind(self, stock_id: str, years: int) -> pd.DataFrame | None:
        """Fetch dividend history from FinMind TaiwanStockDividend.

        FinMind does NOT provide fill_days, so we compute it via yfinance
        price data: count trading days from ex_date until close >= pre-ex close.
        """
        try:
            dl = _get_finmind_loader()
            current_year = datetime.date.today().year
            start_date = f"{current_year - years}-01-01"
            raw = dl.taiwan_stock_dividend(
                stock_id=stock_id,
                start_date=start_date,
            )
            if raw is None or raw.empty:
                return None

            # Shared price cache across all ex-dates for this stock
            # to minimise yfinance downloads
            price_cache = {}  # type: dict

            # First pass: collect all ex-dates and download a single wide
            # price window covering all of them
            ex_dates_info = []
            for _, r in raw.iterrows():
                cash = float(r.get('CashEarningsDistribution', 0) or 0) + \
                       float(r.get('CashStaticDistribution', 0) or 0)
                stock_d = float(r.get('StockEarningsDistribution', 0) or 0) + \
                          float(r.get('StockStaticDistribution', 0) or 0)
                total = cash + stock_d
                ex_date_val = str(r.get('date', ''))
                year_val = None
                if ex_date_val:
                    try:
                        year_val = pd.to_datetime(ex_date_val).year
                    except Exception:
                        pass
                ex_dates_info.append((year_val, cash, stock_d, total, ex_date_val))

            # Pre-download price data covering all ex-dates in one call
            self._prefetch_prices_for_fill(stock_id, ex_dates_info, price_cache)

            records = []
            for year_val, cash, stock_d, total, ex_date_val in ex_dates_info:
                fill_days_val, yield_pct = self._compute_fill_days(
                    stock_id, ex_date_val, total, price_cache,
                )

                records.append({
                    'year': year_val,
                    'cash_dividend': round(cash, 2),
                    'stock_dividend': round(stock_d, 2),
                    'total_dividend': round(total, 2),
                    'ex_date': ex_date_val,
                    'fill_days': fill_days_val,
                    'yield_pct': yield_pct,
                })

            if not records:
                return None

            df = pd.DataFrame(records)
            df = df.sort_values('year', ascending=False).reset_index(drop=True)
            return df

        except Exception as exc:
            logger.error("FinMind dividend fetch error for %s: %s", stock_id, exc)
            return None

    def _prefetch_prices_for_fill(self, stock_id: str,
                                   ex_dates_info: list,
                                   price_cache: dict) -> None:
        """Download a single wide price window covering all ex-dates.

        This avoids multiple yfinance downloads when computing fill_days
        for many ex-dates of the same stock.
        """
        import yfinance as yf

        today = datetime.date.today()
        min_date = None
        max_date = None

        for _, _, _, _, ex_date_val in ex_dates_info:
            if not ex_date_val:
                continue
            try:
                d = pd.to_datetime(ex_date_val).date()
            except Exception:
                continue
            if d >= today:
                continue  # Skip future dates
            start = d - datetime.timedelta(days=10)
            end = min(today, d + datetime.timedelta(days=120))
            if min_date is None or start < min_date:
                min_date = start
            if max_date is None or end > max_date:
                max_date = end

        if min_date is None or max_date is None:
            return  # All dates are future or invalid

        ticker_str = f"{stock_id}.TW"
        try:
            df_price = yf.download(
                ticker_str,
                start=min_date.strftime('%Y-%m-%d'),
                end=max_date.strftime('%Y-%m-%d'),
                progress=False, auto_adjust=True, timeout=30,
            )
            if df_price is not None and not df_price.empty:
                if isinstance(df_price.columns, pd.MultiIndex):
                    df_price.columns = df_price.columns.get_level_values(0)
                price_cache[stock_id] = df_price
                logger.info("Pre-fetched %d price rows for %s (%s to %s)",
                            len(df_price), stock_id,
                            min_date.strftime('%Y-%m-%d'),
                            max_date.strftime('%Y-%m-%d'))
        except Exception as exc:
            logger.warning("Price pre-fetch failed for %s: %s", stock_id, exc)

    # -----------------------------------------------------------------
    # Parsing utilities
    # -----------------------------------------------------------------
    @staticmethod
    def _parse_year(text: str):
        """Parse ROC year (e.g., '113') or AD year (e.g., '2024') to AD int."""
        text = text.strip().replace(',', '')
        if not text:
            return None
        try:
            val = int(text)
        except ValueError:
            return None
        if val < 200:
            # 民國年轉西元
            return val + 1911
        if 1990 <= val <= 2100:
            return val
        return None

    @staticmethod
    def _safe_float(cells: list, idx: int, default: float = 0.0) -> float:
        """Safely extract float from cell list."""
        if idx >= len(cells):
            return default
        text = cells[idx].strip().replace(',', '').replace('%', '')
        if not text or text == '-':
            return default
        try:
            return round(float(text), 2)
        except ValueError:
            return default

    @staticmethod
    def _find_date_cell(cells: list) -> str | None:
        """Try to find a date-like string (YYYY-MM-DD or YYYY/MM/DD) in cells."""
        import re
        date_pattern = re.compile(r'(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})')
        for c in cells:
            m = date_pattern.search(c)
            if m:
                return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return None

    @staticmethod
    def _find_fill_days(cells: list) -> float | None:
        """Try to locate fill-gap days in cell list (look for small integer near end)."""
        import re
        # 填息天數通常是一個小正整數, 出現在較後面的欄位
        for c in reversed(cells):
            text = c.strip().replace(',', '')
            if text == '-' or text == '' or text == '尚未填息':
                continue
            # 如果這個欄位包含「天」字
            m = re.search(r'(\d+)\s*天?', text)
            if m:
                val = int(m.group(1))
                if 0 < val < 1000:
                    return float(val)
        return None

    @staticmethod
    def _find_yield(cells: list) -> float:
        """Try to locate yield percentage in cell list."""
        import re
        for c in reversed(cells):
            text = c.strip().replace(',', '')
            m = re.search(r'(\d+\.?\d*)\s*%', text)
            if m:
                return round(float(m.group(1)), 2)
        return 0.0

    def _estimate_yield(self, stock_id: str, cash_dividend: float) -> float:
        """Estimate dividend yield by fetching current price from yfinance."""
        if cash_dividend <= 0:
            return 0.0
        try:
            import yfinance as yf
            ticker_str = f"{stock_id}.TW"
            info = yf.Ticker(ticker_str).info
            price = info.get('regularMarketPrice') or info.get('previousClose', 0)
            if price and price > 0:
                return round(cash_dividend / price * 100, 2)
        except Exception as exc:
            logger.warning("Could not estimate yield for %s: %s", stock_id, exc)
        return 0.0

    def _compute_fill_days(self, stock_id: str, ex_date_str: str,
                           total_dividend: float,
                           price_cache: dict | None = None) -> tuple:
        """Compute fill-gap days and yield_pct for a single ex-dividend event.

        Fill gap (填息) = number of trading days after ex_date until the
        closing price reaches (or exceeds) the closing price on the day
        BEFORE the ex-date.

        Parameters
        ----------
        stock_id : str
            Taiwan stock ID (digits only, no .TW suffix).
        ex_date_str : str
            Ex-dividend date in 'YYYY-MM-DD' format.
        total_dividend : float
            Total dividend amount per share.
        price_cache : dict, optional
            Mutable dict used to cache downloaded price DataFrames across
            multiple calls for the same stock.  Key = stock_id, value = df.

        Returns
        -------
        tuple(fill_days: int | None, yield_pct: float)
            fill_days is None if the stock never recovered within 60 trading
            days, or if ex_date is in the future, or on data error.
        """
        import yfinance as yf

        if not ex_date_str or total_dividend <= 0:
            return (None, 0.0)

        try:
            ex_date = pd.to_datetime(ex_date_str).date()
        except Exception:
            return (None, 0.0)

        today = datetime.date.today()
        if ex_date >= today:
            # Future ex-date -- cannot compute
            return (None, 0.0)

        # We need prices from ~5 trading days before ex_date to ~90 calendar
        # days after.  Download a wide window and reuse via cache.
        ticker_str = f"{stock_id}.TW"
        if price_cache is None:
            price_cache = {}

        if stock_id not in price_cache:
            # Download enough history to cover all ex-dates in recent years
            dl_start = (ex_date - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
            # End at today or ex_date + 120 days (whichever is earlier)
            dl_end_date = min(today, ex_date + datetime.timedelta(days=120))
            dl_end = dl_end_date.strftime('%Y-%m-%d')
            try:
                df_price = yf.download(
                    ticker_str, start=dl_start, end=dl_end,
                    progress=False, auto_adjust=True, timeout=30,
                )
                if df_price is not None and not df_price.empty:
                    # Flatten MultiIndex columns if present (yfinance >= 0.2.x)
                    if isinstance(df_price.columns, pd.MultiIndex):
                        df_price.columns = df_price.columns.get_level_values(0)
                    price_cache[stock_id] = df_price
                else:
                    return (None, 0.0)
            except Exception as exc:
                logger.warning("yfinance download failed for %s: %s", ticker_str, exc)
                return (None, 0.0)
        else:
            # Check if cached data covers this ex-date window; extend if not
            cached_df = price_cache[stock_id]
            need_start = ex_date - datetime.timedelta(days=10)
            need_end = min(today, ex_date + datetime.timedelta(days=120))
            cached_start = cached_df.index.min().date() if len(cached_df) > 0 else today
            cached_end = cached_df.index.max().date() if len(cached_df) > 0 else today

            if need_start < cached_start or need_end > cached_end:
                # Re-download with wider window
                new_start = min(need_start, cached_start) - datetime.timedelta(days=5)
                new_end = max(need_end, cached_end) + datetime.timedelta(days=5)
                new_end = min(new_end, today)
                try:
                    df_price = yf.download(
                        ticker_str,
                        start=new_start.strftime('%Y-%m-%d'),
                        end=new_end.strftime('%Y-%m-%d'),
                        progress=False, auto_adjust=True, timeout=30,
                    )
                    if df_price is not None and not df_price.empty:
                        if isinstance(df_price.columns, pd.MultiIndex):
                            df_price.columns = df_price.columns.get_level_values(0)
                        price_cache[stock_id] = df_price
                    else:
                        return (None, 0.0)
                except Exception as exc:
                    logger.warning("yfinance re-download failed for %s: %s",
                                   ticker_str, exc)
                    return (None, 0.0)

        df_price = price_cache[stock_id]

        # Ensure index is DatetimeIndex
        if not isinstance(df_price.index, pd.DatetimeIndex):
            df_price.index = pd.to_datetime(df_price.index)

        # Find the closing price on the last trading day BEFORE ex_date
        ex_dt = pd.Timestamp(ex_date)
        before_ex = df_price[df_price.index < ex_dt]
        if before_ex.empty:
            logger.debug("No price data before ex-date %s for %s", ex_date_str, stock_id)
            return (None, 0.0)

        pre_ex_close = float(before_ex['Close'].iloc[-1])
        if pre_ex_close <= 0:
            return (None, 0.0)

        # yield_pct = total_dividend / pre_ex_close * 100
        yield_pct = round(total_dividend / pre_ex_close * 100, 2)

        # Count trading days from ex_date onward until close >= pre_ex_close
        after_ex = df_price[df_price.index >= ex_dt]
        if after_ex.empty:
            return (None, yield_pct)

        fill_days = None
        for i, (dt, row) in enumerate(after_ex.iterrows()):
            close_price = float(row['Close'])
            if close_price >= pre_ex_close:
                fill_days = i + 1  # 1-based: ex_date itself is day 1
                break
            if i >= 59:
                # 60 trading days cap
                break

        return (fill_days, yield_pct)


# ===================================================================
# RevenueTracker
# ===================================================================

class RevenueTracker:
    """Monthly revenue data, YoY/MoM trends, and revenue-surprise detection."""

    # -----------------------------------------------------------------
    # get_monthly_revenue
    # -----------------------------------------------------------------
    def get_monthly_revenue(self, stock_id: str, months: int = 24) -> pd.DataFrame:
        """Fetch monthly revenue data for a Taiwan stock.

        Primary source: FinMind TaiwanStockMonthRevenue
        Fallback: MOPS website scraping

        Returns
        -------
        pd.DataFrame
            Columns: year_month, revenue, yoy_pct, mom_pct,
                     cumulative_revenue, cumulative_yoy_pct
        """
        stock_id = self._normalize_id(stock_id)
        cache_key = f"rev_monthly_{stock_id}_{months}"
        cached = _cache_get(cache_key)
        if cached is not None:
            logger.info("Monthly revenue cache hit for %s", stock_id)
            return cached

        df = self._fetch_revenue_finmind(stock_id, months)
        if df is None or df.empty:
            logger.info("FinMind revenue unavailable for %s, trying MOPS", stock_id)
            df = self._fetch_revenue_mops(stock_id, months)

        if df is None or df.empty:
            logger.warning("No revenue data found for %s", stock_id)
            df = self._empty_revenue_df()

        _cache_set(cache_key, df, _REVENUE_TTL)
        return df

    # -----------------------------------------------------------------
    # get_revenue_alert
    # -----------------------------------------------------------------
    def get_revenue_alert(self, stock_id: str) -> dict:
        """Check revenue announcement timing and recent trend.

        Returns
        -------
        dict
            next_announcement_date, days_until, last_revenue, last_yoy_pct,
            trend, consecutive_growth_months, alert_text
        """
        stock_id = self._normalize_id(stock_id)
        default = {
            'next_announcement_date': '',
            'days_until': 0,
            'last_revenue': 0.0,
            'last_yoy_pct': 0.0,
            'trend': '資料不足',
            'consecutive_growth_months': 0,
            'alert_text': '無營收資料',
        }

        df = self.get_monthly_revenue(stock_id, months=24)
        if df.empty:
            return default

        # 計算下次營收公布日 (每月10日前公布上月營收)
        today = datetime.date.today()
        # 如果今天 <= 10號, 本月10日就是下次公布日
        # 如果今天 > 10號, 下月10日是下次公布日
        if today.day <= 10:
            next_ann = today.replace(day=10)
        else:
            # 下個月10號
            if today.month == 12:
                next_ann = today.replace(year=today.year + 1, month=1, day=10)
            else:
                next_ann = today.replace(month=today.month + 1, day=10)

        days_until = (next_ann - today).days

        # 最近一筆營收
        latest = df.iloc[0]
        last_revenue = float(latest.get('revenue', 0))
        last_yoy_pct = float(latest.get('yoy_pct', 0))

        # 趨勢判斷: 連續成長 / 連續衰退 / 波動
        trend, consec = self._evaluate_trend(df)

        # 組合提示文字
        yoy_sign = '+' if last_yoy_pct >= 0 else ''
        alert_text = (
            f"距離營收公布還有{days_until}天，"
            f"上月YoY{yoy_sign}{last_yoy_pct:.1f}%"
        )

        return {
            'next_announcement_date': next_ann.strftime('%Y-%m-%d'),
            'days_until': days_until,
            'last_revenue': round(last_revenue, 0),
            'last_yoy_pct': round(last_yoy_pct, 2),
            'trend': trend,
            'consecutive_growth_months': consec,
            'alert_text': alert_text,
        }

    # -----------------------------------------------------------------
    # detect_revenue_surprise
    # -----------------------------------------------------------------
    def detect_revenue_surprise(self, stock_id: str) -> dict:
        """Compare latest revenue to 3-month and 12-month averages.

        Returns
        -------
        dict
            is_surprise, direction, magnitude, text
        """
        stock_id = self._normalize_id(stock_id)
        default = {
            'is_surprise': False,
            'direction': 'neutral',
            'magnitude': 0.0,
            'text': '無異常',
        }

        df = self.get_monthly_revenue(stock_id, months=24)
        if df.empty or len(df) < 13:
            return default

        latest_rev = float(df.iloc[0]['revenue'])
        if latest_rev <= 0:
            return default

        # 12 個月平均 (不含最新月)
        avg_12m = float(df.iloc[1:13]['revenue'].mean())
        # 3 個月平均 (不含最新月)
        avg_3m = float(df.iloc[1:4]['revenue'].mean())

        if avg_12m <= 0:
            return default

        ratio_12m = latest_rev / avg_12m
        ratio_3m = latest_rev / avg_3m if avg_3m > 0 else 1.0

        is_surprise = False
        direction = 'neutral'
        magnitude = round((ratio_12m - 1.0) * 100, 1)

        if ratio_12m > 1.20:
            is_surprise = True
            direction = 'positive'
            text = f"營收年均正驚喜: 較12月均值高{magnitude:.1f}%"
        elif ratio_12m < 0.80:
            is_surprise = True
            direction = 'negative'
            text = f"營收年均負驚喜: 較12月均值低{abs(magnitude):.1f}%"
        else:
            text = f"營收在正常範圍內 (較12月均值{'+' if magnitude >= 0 else ''}{magnitude:.1f}%)"

        return {
            'is_surprise': is_surprise,
            'direction': direction,
            'magnitude': magnitude,
            'text': text,
        }

    # =================================================================
    # Private helpers
    # =================================================================
    @staticmethod
    def _normalize_id(stock_id: str) -> str:
        """Strip .TW suffix and whitespace."""
        stock_id = str(stock_id).strip()
        if stock_id.upper().endswith('.TW'):
            stock_id = stock_id[:-3]
        if stock_id.upper().endswith('.TWO'):
            stock_id = stock_id[:-4]
        return stock_id

    @staticmethod
    def _empty_revenue_df() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            'year_month', 'revenue', 'yoy_pct', 'mom_pct',
            'cumulative_revenue', 'cumulative_yoy_pct',
        ])

    # -----------------------------------------------------------------
    # FinMind revenue
    # -----------------------------------------------------------------
    def _fetch_revenue_finmind(self, stock_id: str, months: int) -> pd.DataFrame | None:
        """Fetch monthly revenue from FinMind."""
        try:
            dl = _get_finmind_loader()
            today = datetime.date.today()
            start_date = (today - datetime.timedelta(days=months * 35)).strftime('%Y-%m-%d')

            raw = dl.taiwan_stock_month_revenue(
                stock_id=stock_id,
                start_date=start_date,
            )
            if raw is None or raw.empty:
                return None

            # FinMind 欄位: date, stock_id, revenue, ...
            if 'revenue' not in raw.columns:
                logger.warning("FinMind revenue response missing 'revenue' column for %s", stock_id)
                return None

            raw = raw.sort_values('date', ascending=False).reset_index(drop=True)

            # 限制到指定月數
            raw = raw.head(months)

            records = []
            for i, row in raw.iterrows():
                date_str = str(row.get('date', ''))
                revenue = float(row.get('revenue', 0) or 0)

                # year_month: YYYY-MM
                year_month = date_str[:7] if len(date_str) >= 7 else date_str

                records.append({
                    'year_month': year_month,
                    'revenue': revenue,
                })

            if not records:
                return None

            df = pd.DataFrame(records)
            df = self._compute_revenue_metrics(df)
            return df

        except Exception as exc:
            logger.error("FinMind revenue fetch error for %s: %s", stock_id, exc)
            return None

    # -----------------------------------------------------------------
    # MOPS fallback scraper
    # -----------------------------------------------------------------
    def _fetch_revenue_mops(self, stock_id: str, months: int) -> pd.DataFrame | None:
        """Fallback: scrape revenue from MOPS (公開資訊觀測站)."""
        try:
            records = []
            today = datetime.date.today()

            # MOPS 查詢以「年/月」為單位, 逐月查太慢, 改用彙總查詢
            # 使用「彙總報表」API: 查詢最近兩年即可涵蓋 24 個月
            for year_offset in range(0, (months // 12) + 2):
                target_year = today.year - year_offset
                roc_year = target_year - 1911

                for month in range(12, 0, -1):
                    if len(records) >= months:
                        break

                    url = (
                        "https://mops.twse.com.tw/nas/t21/sii/t21sc03_"
                        f"{roc_year}_{month}_0.html"
                    )
                    try:
                        resp = requests.get(url, timeout=10)
                        if resp.status_code != 200:
                            continue
                        resp.encoding = 'big5'
                        soup = BeautifulSoup(resp.text, 'html.parser')

                        # 找到該股票的列
                        for tr in soup.find_all('tr'):
                            tds = tr.find_all('td')
                            if len(tds) < 5:
                                continue
                            if tds[0].get_text(strip=True) == stock_id:
                                rev_text = tds[2].get_text(strip=True).replace(',', '')
                                try:
                                    revenue = float(rev_text) * 1000  # 千元 -> 元
                                except ValueError:
                                    continue
                                year_month = f"{target_year}-{month:02d}"
                                records.append({
                                    'year_month': year_month,
                                    'revenue': revenue,
                                })
                                break
                    except Exception:
                        continue

            if not records:
                return None

            df = pd.DataFrame(records)
            df = df.sort_values('year_month', ascending=False).reset_index(drop=True)
            df = df.head(months)
            df = self._compute_revenue_metrics(df)
            return df

        except Exception as exc:
            logger.error("MOPS revenue fetch error for %s: %s", stock_id, exc)
            return None

    # -----------------------------------------------------------------
    # Revenue metric computation
    # -----------------------------------------------------------------
    @staticmethod
    def _compute_revenue_metrics(df: pd.DataFrame) -> pd.DataFrame:
        """Compute YoY%, MoM%, cumulative revenue, cumulative YoY% columns.

        Input DataFrame must have 'year_month' and 'revenue' columns,
        sorted descending by year_month.
        """
        if df.empty or 'revenue' not in df.columns:
            return df

        df = df.copy()
        df = df.sort_values('year_month', ascending=False).reset_index(drop=True)

        # 建立用於對齊的 lookup (year_month -> revenue)
        rev_map = dict(zip(df['year_month'], df['revenue']))

        yoy_list = []
        mom_list = []

        for _, row in df.iterrows():
            ym = row['year_month']
            rev = row['revenue']

            # 去年同月
            prev_year_ym = _shift_year_month(ym, -12)
            prev_month_ym = _shift_year_month(ym, -1)

            prev_year_rev = rev_map.get(prev_year_ym)
            prev_month_rev = rev_map.get(prev_month_ym)

            if prev_year_rev and prev_year_rev > 0:
                yoy_list.append(round((rev / prev_year_rev - 1) * 100, 2))
            else:
                yoy_list.append(0.0)

            if prev_month_rev and prev_month_rev > 0:
                mom_list.append(round((rev / prev_month_rev - 1) * 100, 2))
            else:
                mom_list.append(0.0)

        df['yoy_pct'] = yoy_list
        df['mom_pct'] = mom_list

        # 累計營收 (同年度 1 月到當月)
        cumulative = []
        cumulative_yoy = []
        # 按年度分組計算累計
        year_cum = {}  # year -> running total
        # 需要按時間順序計算
        df_asc = df.sort_values('year_month').reset_index(drop=True)
        for _, row in df_asc.iterrows():
            ym = row['year_month']
            rev = row['revenue']
            year = ym[:4]
            if year not in year_cum:
                year_cum[year] = 0.0
            year_cum[year] += rev

        # 同年去年累計
        for _, row in df.iterrows():
            ym = row['year_month']
            year = ym[:4]
            month = ym[5:7]

            # 計算本年度截至該月的累計
            cum = 0.0
            for m in range(1, int(month) + 1):
                key = f"{year}-{m:02d}"
                cum += rev_map.get(key, 0.0)
            cumulative.append(round(cum, 0))

            # 去年同期累計
            prev_year = str(int(year) - 1)
            cum_prev = 0.0
            for m in range(1, int(month) + 1):
                key = f"{prev_year}-{m:02d}"
                cum_prev += rev_map.get(key, 0.0)

            if cum_prev > 0:
                cumulative_yoy.append(round((cum / cum_prev - 1) * 100, 2))
            else:
                cumulative_yoy.append(0.0)

        df['cumulative_revenue'] = cumulative
        df['cumulative_yoy_pct'] = cumulative_yoy

        return df

    # -----------------------------------------------------------------
    # Trend evaluation
    # -----------------------------------------------------------------
    @staticmethod
    def _evaluate_trend(df: pd.DataFrame) -> tuple:
        """Evaluate consecutive growth/decline trend.

        Returns (trend_str, consecutive_count).
        """
        if df.empty or 'yoy_pct' not in df.columns:
            return ('資料不足', 0)

        yoy_values = df['yoy_pct'].tolist()
        if not yoy_values:
            return ('資料不足', 0)

        # 從最新月份往回數連續成長或衰退月數
        first = yoy_values[0]
        if first > 0:
            direction = 'growth'
        elif first < 0:
            direction = 'decline'
        else:
            return ('波動', 0)

        count = 0
        for v in yoy_values:
            if direction == 'growth' and v > 0:
                count += 1
            elif direction == 'decline' and v < 0:
                count += 1
            else:
                break

        if direction == 'growth':
            trend = '連續成長' if count >= 2 else '波動'
        else:
            trend = '連續衰退' if count >= 2 else '波動'

        return (trend, count)


# ===================================================================
# Module-level utility
# ===================================================================

def _shift_year_month(ym: str, offset_months: int) -> str:
    """Shift a YYYY-MM string by offset_months. E.g., ('2024-03', -12) -> '2023-03'."""
    try:
        year = int(ym[:4])
        month = int(ym[5:7])
        total = year * 12 + (month - 1) + offset_months
        new_year = total // 12
        new_month = total % 12 + 1
        return f"{new_year}-{new_month:02d}"
    except (ValueError, IndexError):
        return ''


# ===================================================================
# __main__ test block
# ===================================================================

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    print("=" * 60)
    print("DividendAnalyzer / RevenueTracker - Manual Test")
    print("=" * 60)

    test_stocks = ['2330', '2412']

    da = DividendAnalyzer()
    rt = RevenueTracker()

    for sid in test_stocks:
        print(f"\n{'='*60}")
        print(f"  Stock: {sid}")
        print(f"{'='*60}")

        # --- Dividend History ---
        print(f"\n--- Dividend History (last 5 years) ---")
        div_df = da.get_dividend_history(sid, years=5)
        if not div_df.empty:
            print(div_df.to_string(index=False))
        else:
            print("  (no data)")

        # --- Upcoming Ex-Dates ---
        print(f"\n--- Upcoming Ex-Dates ---")
        upcoming = da.get_upcoming_ex_dates(sid)
        for k, v in upcoming.items():
            print(f"  {k}: {v}")

        # --- Fill-Gap Stats ---
        print(f"\n--- Fill-Gap Stats ---")
        stats = da.get_fill_gap_stats(sid)
        for k, v in stats.items():
            print(f"  {k}: {v}")

        # --- Monthly Revenue (last 12 months) ---
        print(f"\n--- Monthly Revenue (last 12 months) ---")
        rev_df = rt.get_monthly_revenue(sid, months=12)
        if not rev_df.empty:
            print(rev_df.to_string(index=False))
        else:
            print("  (no data)")

        # --- Revenue Alert ---
        print(f"\n--- Revenue Alert ---")
        alert = rt.get_revenue_alert(sid)
        for k, v in alert.items():
            print(f"  {k}: {v}")

        # --- Revenue Surprise ---
        print(f"\n--- Revenue Surprise ---")
        surprise = rt.detect_revenue_surprise(sid)
        for k, v in surprise.items():
            print(f"  {k}: {v}")

    print(f"\n{'='*60}")
    print("Test complete.")
