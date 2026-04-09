"""
TWSE/TPEX Open Data API Module (台灣證交所/櫃買中心 開放資料 API)

免費、官方、無需 API Key 的台股數據來源。
提供三大法人買賣超、融資融券、本益比/殖利率、每月營收等資料。

數據來源:
- TWSE (台灣證券交易所): https://www.twse.com.tw
- TPEX (櫃買中心): https://www.tpex.org.tw
- MOPS (公開資訊觀測站): https://mops.twse.com.tw
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# TWSE API 有頻率限制，每次請求間隔至少 3 秒較安全
_TWSE_REQUEST_INTERVAL = 3.0
_MAX_RETRIES = 3
_RETRY_DELAY = 1.0
_CACHE_TTL_SECONDS = 3600  # 1 小時快取


class TWSEOpenData:
    """
    台灣證交所 / 櫃買中心 開放資料 API 整合模組

    Features:
    - 三大法人買賣超 (institutional trading)
    - 融資融券 (margin trading)
    - 本益比/殖利率/淨值比 (P/E, dividend yield, P/B)
    - 每月營收 (monthly revenue from MOPS)
    - 上櫃三大法人 (TPEX institutional trading)
    """

    def __init__(self):
        # 簡易記憶體快取: key -> (data, timestamp)
        self._cache = {}
        # 上次請求時間，用於控制頻率
        self._last_request_time = 0.0
        # HTTP Session 重用連線
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        })

    # ------------------------------------------------------------------ #
    #  Internal Helpers
    # ------------------------------------------------------------------ #

    def _throttle(self):
        """控制 API 請求頻率，避免被 TWSE 封鎖"""
        elapsed = time.time() - self._last_request_time
        if elapsed < _TWSE_REQUEST_INTERVAL:
            wait = _TWSE_REQUEST_INTERVAL - elapsed
            logger.debug("Throttle: waiting %.1f sec before next request", wait)
            time.sleep(wait)
        self._last_request_time = time.time()

    def _get_cache(self, key):
        """讀取快取，若已過期則回傳 None"""
        if key in self._cache:
            data, ts = self._cache[key]
            if time.time() - ts < _CACHE_TTL_SECONDS:
                logger.debug("Cache hit: %s", key)
                return data
            else:
                # 快取過期，刪除
                del self._cache[key]
        return None

    def _set_cache(self, key, data):
        """寫入快取"""
        self._cache[key] = (data, time.time())

    def _fetch_json(self, url, params=None):
        """
        Fetch JSON from URL with retry logic.
        先用 requests，失敗則改用 curl_cffi 作為 fallback。

        Returns:
            dict or None
        """
        for attempt in range(1, _MAX_RETRIES + 1):
            self._throttle()
            try:
                resp = self._session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(
                    "requests failed (attempt %d/%d) for %s: %s",
                    attempt, _MAX_RETRIES, url, e,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAY)

        # Fallback: 使用 curl_cffi
        logger.info("Falling back to curl_cffi for %s", url)
        try:
            from curl_cffi import requests as cffi_requests
            self._throttle()
            resp = cffi_requests.get(
                url, params=params, timeout=15, impersonate="chrome"
            )
            return resp.json()
        except Exception as e:
            logger.error("curl_cffi also failed for %s: %s", url, e)
            return None

    def _fetch_html(self, url, params=None, encoding='utf-8'):
        """
        Fetch HTML content with retry logic.

        Returns:
            str (HTML text) or None
        """
        for attempt in range(1, _MAX_RETRIES + 1):
            self._throttle()
            try:
                resp = self._session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                resp.encoding = encoding
                return resp.text
            except Exception as e:
                logger.warning(
                    "HTML fetch failed (attempt %d/%d) for %s: %s",
                    attempt, _MAX_RETRIES, url, e,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAY)

        # Fallback: curl_cffi
        logger.info("Falling back to curl_cffi for HTML: %s", url)
        try:
            from curl_cffi import requests as cffi_requests
            self._throttle()
            resp = cffi_requests.get(
                url, params=params, timeout=15, impersonate="chrome"
            )
            resp.encoding = encoding
            return resp.text
        except Exception as e:
            logger.error("curl_cffi HTML fetch also failed for %s: %s", url, e)
            return None

    @staticmethod
    def _get_recent_trading_dates(days=15):
        """
        Generate a list of recent potential trading dates (skip weekends).
        Returns list of datetime objects, newest first.
        """
        dates = []
        current = datetime.now()
        checked = 0
        while len(dates) < days and checked < days * 2:
            # 跳過週末
            if current.weekday() < 5:
                dates.append(current)
            current -= timedelta(days=1)
            checked += 1
        return dates

    @staticmethod
    def _to_twse_date(dt):
        """Convert datetime to TWSE format: YYYYMMDD"""
        return dt.strftime('%Y%m%d')

    @staticmethod
    def _to_tpex_date(dt):
        """Convert datetime to TPEX ROC format: YYY/MM/DD (e.g., 115/04/09)"""
        roc_year = dt.year - 1911
        return f"{roc_year}/{dt.strftime('%m/%d')}"

    @staticmethod
    def _safe_int(val):
        """安全轉換為整數，處理逗號和無效值"""
        if val is None:
            return 0
        if isinstance(val, (int, float)):
            return int(val)
        try:
            # 移除逗號和空白
            cleaned = str(val).replace(',', '').replace(' ', '').strip()
            if cleaned == '' or cleaned == '--' or cleaned == 'N/A':
                return 0
            return int(cleaned)
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _safe_float(val):
        """安全轉換為浮點數，處理逗號和無效值"""
        if val is None:
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        try:
            cleaned = str(val).replace(',', '').replace(' ', '').strip()
            if cleaned == '' or cleaned == '--' or cleaned == 'N/A':
                return 0.0
            return float(cleaned)
        except (ValueError, TypeError):
            return 0.0

    # ------------------------------------------------------------------ #
    #  1. 三大法人買賣超 (TWSE)
    # ------------------------------------------------------------------ #

    def get_institutional_trading(self, stock_id, days=10):
        """
        Fetch institutional investor buy/sell data from TWSE.

        Args:
            stock_id: Taiwan stock ID (e.g., '2330')
            days: Number of trading days to fetch

        Returns:
            DataFrame indexed by date with columns:
            ['外資', '投信', '自營商', '合計']
            Values are net buy/sell amounts (positive=buy, negative=sell).
        """
        cache_key = f"twse_inst_{stock_id}_{days}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        stock_id = str(stock_id).strip()
        logger.info("Fetching TWSE institutional trading for %s (last %d days)", stock_id, days)

        results = []
        dates = self._get_recent_trading_dates(days=days + 10)  # 多嘗試幾天以跳過假日

        for dt in dates:
            if len(results) >= days:
                break

            date_str = self._to_twse_date(dt)
            url = "https://www.twse.com.tw/rwd/zh/fund/T86"
            params = {
                'date': date_str,
                'selectType': 'ALL',
                'response': 'json',
            }

            data = self._fetch_json(url, params=params)
            if data is None:
                continue

            # 檢查回應狀態
            stat = data.get('stat', '')
            if stat != 'OK':
                logger.debug("No data for date %s (stat=%s)", date_str, stat)
                continue

            # 解析 fields 和 data
            fields = data.get('fields', [])
            rows = data.get('data', [])

            if not fields or not rows:
                logger.debug("Empty data for date %s", date_str)
                continue

            # 尋找目標股票
            found = False
            for row in rows:
                if len(row) < 2:
                    continue
                # 第一欄通常是證券代號
                row_id = str(row[0]).strip()
                if row_id == stock_id:
                    # T86 欄位順序 (可能因 API 版本調整):
                    # 證券代號, 證券名稱, 外陸資買進股數(不含外資自營商),
                    # 外陸資賣出股數(不含外資自營商), 外陸資買賣超股數(不含外資自營商),
                    # 外資自營商買進股數, 外資自營商賣出股數, 外資自營商買賣超股數,
                    # 外資及陸資買賣超股數, 投信買進股數, 投信賣出股數, 投信買賣超股數,
                    # 自營商買賣超股數, ...
                    try:
                        # 使用欄位名稱來定位，比硬編碼索引更穩健
                        field_map = {f.strip(): i for i, f in enumerate(fields)}

                        # 外資買賣超
                        foreign_net = 0
                        for key in ['外陸資買賣超股數(不含外資自營商)', '外資及陸資買賣超股數',
                                     '外資買賣超股數']:
                            if key in field_map:
                                foreign_net = self._safe_int(row[field_map[key]])
                                break
                        # 如果有外資自營商，加上
                        for key in ['外資自營商買賣超股數']:
                            if key in field_map:
                                foreign_net += self._safe_int(row[field_map[key]])
                                break

                        # 投信買賣超
                        trust_net = 0
                        for key in ['投信買賣超股數']:
                            if key in field_map:
                                trust_net = self._safe_int(row[field_map[key]])
                                break

                        # 自營商買賣超 (合計)
                        dealer_net = 0
                        for key in ['自營商買賣超股數']:
                            if key in field_map:
                                dealer_net = self._safe_int(row[field_map[key]])
                                break
                        # 若無合計欄位，嘗試 自營商(自行) + 自營商(避險)
                        if dealer_net == 0:
                            d1, d2 = 0, 0
                            for key in ['自營商(自行)買賣超股數']:
                                if key in field_map:
                                    d1 = self._safe_int(row[field_map[key]])
                                    break
                            for key in ['自營商(避險)買賣超股數']:
                                if key in field_map:
                                    d2 = self._safe_int(row[field_map[key]])
                                    break
                            if d1 != 0 or d2 != 0:
                                dealer_net = d1 + d2

                        # 三大法人合計
                        total_net = 0
                        for key in ['三大法人買賣超股數']:
                            if key in field_map:
                                total_net = self._safe_int(row[field_map[key]])
                                break
                        if total_net == 0:
                            total_net = foreign_net + trust_net + dealer_net

                        results.append({
                            'date': dt.strftime('%Y-%m-%d'),
                            '外資': foreign_net,
                            '投信': trust_net,
                            '自營商': dealer_net,
                            '合計': total_net,
                        })
                        found = True
                    except (IndexError, KeyError) as e:
                        logger.warning("Error parsing institutional data for %s on %s: %s",
                                       stock_id, date_str, e)
                    break  # 已找到目標股票

            if not found:
                logger.debug("Stock %s not found in T86 data for %s", stock_id, date_str)

        if not results:
            logger.warning("No institutional trading data found for %s", stock_id)
            df = pd.DataFrame(columns=['外資', '投信', '自營商', '合計'])
            df.index.name = 'date'
            self._set_cache(cache_key, df)
            return df

        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()

        self._set_cache(cache_key, df)
        logger.info("Fetched %d days of institutional data for %s", len(df), stock_id)
        return df

    # ------------------------------------------------------------------ #
    #  2. 融資融券 (Margin Trading)
    # ------------------------------------------------------------------ #

    def get_margin_trading(self, stock_id, days=10):
        """
        Fetch margin trading (融資融券) data from TWSE.

        Args:
            stock_id: Taiwan stock ID (e.g., '2330')
            days: Number of trading days to fetch

        Returns:
            DataFrame indexed by date with columns:
            ['融資買進', '融資賣出', '融資餘額', '融券買進', '融券賣出', '融券餘額']
        """
        cache_key = f"twse_margin_{stock_id}_{days}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        stock_id = str(stock_id).strip()
        logger.info("Fetching TWSE margin trading for %s (last %d days)", stock_id, days)

        results = []
        dates = self._get_recent_trading_dates(days=days + 10)

        for dt in dates:
            if len(results) >= days:
                break

            date_str = self._to_twse_date(dt)
            url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
            params = {
                'date': date_str,
                'selectType': 'ALL',
                'response': 'json',
            }

            data = self._fetch_json(url, params=params)
            if data is None:
                continue

            stat = data.get('stat', '')
            if stat != 'OK':
                logger.debug("No margin data for date %s (stat=%s)", date_str, stat)
                continue

            # MI_MARGN 回應可能有多個 table，融資融券在 tables[1] (creditList)
            # 結構: data.tables -> list of {title, fields, data}
            tables = data.get('tables', [])
            if not tables:
                logger.debug("No tables in margin response for %s", date_str)
                continue

            # 尋找「融資融券」表格 (通常是第二個表格)
            target_table = None
            for table in tables:
                title = table.get('title', '')
                # 包含「融資融券」的表格
                if '融資' in title or '信用交易' in title:
                    target_table = table
                    break
            # 若沒找到，嘗試用索引 (通常 index 1 是個股)
            if target_table is None and len(tables) > 1:
                target_table = tables[1]

            if target_table is None:
                logger.debug("No margin table found for %s", date_str)
                continue

            fields = target_table.get('fields', [])
            rows = target_table.get('data', [])

            if not fields or not rows:
                continue

            # 建立欄位對應
            field_map = {f.strip(): i for i, f in enumerate(fields)}

            for row in rows:
                if len(row) < 2:
                    continue
                # 第一欄: 股票代號
                row_id = str(row[0]).strip()
                if row_id == stock_id:
                    try:
                        # 融資欄位
                        margin_buy = 0
                        margin_sell = 0
                        margin_balance = 0
                        # 融券欄位
                        short_buy = 0
                        short_sell = 0
                        short_balance = 0

                        for key in ['融資買進', '資買進']:
                            if key in field_map:
                                margin_buy = self._safe_int(row[field_map[key]])
                                break
                        for key in ['融資賣出', '資賣出']:
                            if key in field_map:
                                margin_sell = self._safe_int(row[field_map[key]])
                                break
                        for key in ['融資餘額', '資餘額', '融資今日餘額']:
                            if key in field_map:
                                margin_balance = self._safe_int(row[field_map[key]])
                                break
                        for key in ['融券買進', '券買進']:
                            if key in field_map:
                                short_buy = self._safe_int(row[field_map[key]])
                                break
                        for key in ['融券賣出', '券賣出']:
                            if key in field_map:
                                short_sell = self._safe_int(row[field_map[key]])
                                break
                        for key in ['融券餘額', '券餘額', '融券今日餘額']:
                            if key in field_map:
                                short_balance = self._safe_int(row[field_map[key]])
                                break

                        results.append({
                            'date': dt.strftime('%Y-%m-%d'),
                            '融資買進': margin_buy,
                            '融資賣出': margin_sell,
                            '融資餘額': margin_balance,
                            '融券買進': short_buy,
                            '融券賣出': short_sell,
                            '融券餘額': short_balance,
                        })
                    except (IndexError, KeyError) as e:
                        logger.warning("Error parsing margin data for %s on %s: %s",
                                       stock_id, date_str, e)
                    break

        if not results:
            logger.warning("No margin trading data found for %s", stock_id)
            df = pd.DataFrame(columns=['融資買進', '融資賣出', '融資餘額',
                                        '融券買進', '融券賣出', '融券餘額'])
            df.index.name = 'date'
            self._set_cache(cache_key, df)
            return df

        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()

        self._set_cache(cache_key, df)
        logger.info("Fetched %d days of margin data for %s", len(df), stock_id)
        return df

    # ------------------------------------------------------------------ #
    #  3. 本益比 / 殖利率 / 淨值比 (全市場)
    # ------------------------------------------------------------------ #

    def get_pe_dividend_all(self):
        """
        Fetch P/E ratio, P/B ratio, and dividend yield for all TWSE-listed stocks.

        Returns:
            DataFrame with columns:
            ['stock_id', 'stock_name', 'PE', 'dividend_yield', 'PB']
        """
        cache_key = "twse_pe_dividend_all"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        logger.info("Fetching TWSE P/E, P/B, dividend yield (all stocks)")

        url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_ALL"
        params = {'response': 'json'}

        data = self._fetch_json(url, params=params)
        if data is None:
            logger.error("Failed to fetch PE/dividend data")
            return pd.DataFrame(columns=['stock_id', 'stock_name', 'PE', 'dividend_yield', 'PB'])

        stat = data.get('stat', '')
        if stat != 'OK':
            logger.warning("PE/dividend API returned stat=%s", stat)
            return pd.DataFrame(columns=['stock_id', 'stock_name', 'PE', 'dividend_yield', 'PB'])

        fields = data.get('fields', [])
        rows = data.get('data', [])

        if not fields or not rows:
            logger.warning("Empty PE/dividend data")
            return pd.DataFrame(columns=['stock_id', 'stock_name', 'PE', 'dividend_yield', 'PB'])

        # 欄位對應 (BWIBBU_ALL 欄位: 證券代號, 證券名稱, 殖利率(%), 股利年度,
        #           本益比, 股價淨值比, 財報年/季)
        field_map = {f.strip(): i for i, f in enumerate(fields)}

        results = []
        for row in rows:
            try:
                stock_id_val = ''
                stock_name_val = ''
                pe_val = 0.0
                dy_val = 0.0
                pb_val = 0.0

                for key in ['證券代號']:
                    if key in field_map:
                        stock_id_val = str(row[field_map[key]]).strip()
                        break
                for key in ['證券名稱']:
                    if key in field_map:
                        stock_name_val = str(row[field_map[key]]).strip()
                        break
                for key in ['本益比']:
                    if key in field_map:
                        pe_val = self._safe_float(row[field_map[key]])
                        break
                for key in ['殖利率(%)', '殖利率']:
                    if key in field_map:
                        dy_val = self._safe_float(row[field_map[key]])
                        break
                for key in ['股價淨值比']:
                    if key in field_map:
                        pb_val = self._safe_float(row[field_map[key]])
                        break

                if stock_id_val:
                    results.append({
                        'stock_id': stock_id_val,
                        'stock_name': stock_name_val,
                        'PE': pe_val,
                        'dividend_yield': dy_val,
                        'PB': pb_val,
                    })
            except (IndexError, KeyError) as e:
                logger.debug("Error parsing PE row: %s", e)
                continue

        df = pd.DataFrame(results)
        self._set_cache(cache_key, df)
        logger.info("Fetched PE/dividend data for %d stocks", len(df))
        return df

    # ------------------------------------------------------------------ #
    #  4. 每月營收 (MOPS 公開資訊觀測站)
    # ------------------------------------------------------------------ #

    def get_monthly_revenue(self, stock_id, months=12):
        """
        Fetch monthly revenue data from MOPS (公開資訊觀測站).
        Supports both TWSE (上市, sii) and TPEX (上櫃, otc) stocks.

        Args:
            stock_id: Taiwan stock ID (e.g., '2330')
            months: Number of months to fetch

        Returns:
            DataFrame with columns:
            ['year_month', 'revenue', 'yoy_pct', 'mom_pct']
            Revenue unit: thousands of TWD (千元).
        """
        cache_key = f"mops_revenue_{stock_id}_{months}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        stock_id = str(stock_id).strip()
        logger.info("Fetching monthly revenue for %s (last %d months)", stock_id, months)

        results = []
        now = datetime.now()

        # 營收資料通常延遲 1 個月 (例如: 4月初可查 3月營收)
        # 從當月往前推
        for i in range(months + 2):  # 多查 2 個月以確保足夠資料
            if len(results) >= months:
                break

            # 計算目標年月
            target_date = now - timedelta(days=30 * i)
            year = target_date.year
            month = target_date.month

            # 嘗試上市 (sii) 和上櫃 (otc) 兩種來源
            for market_type in ['sii', 'otc']:
                roc_year = year - 1911
                url = (
                    f"https://mops.twse.com.tw/nas/t21/{market_type}/"
                    f"t21sc03_{roc_year}_{month}_0.html"
                )

                html = self._fetch_html(url, encoding='big5')
                if html is None:
                    continue

                # 解析 HTML 表格尋找目標股票
                row_data = self._parse_mops_revenue_html(html, stock_id, year, month)
                if row_data is not None:
                    results.append(row_data)
                    break  # 找到了就不用查另一個市場

        if not results:
            logger.warning("No monthly revenue data found for %s", stock_id)
            df = pd.DataFrame(columns=['year_month', 'revenue', 'yoy_pct', 'mom_pct'])
            self._set_cache(cache_key, df)
            return df

        df = pd.DataFrame(results)
        # 依年月排序
        df = df.sort_values('year_month').reset_index(drop=True)
        # 只取最近 months 筆
        df = df.tail(months).reset_index(drop=True)

        self._set_cache(cache_key, df)
        logger.info("Fetched %d months of revenue data for %s", len(df), stock_id)
        return df

    def _parse_mops_revenue_html(self, html, stock_id, year, month):
        """
        Parse MOPS revenue HTML table to extract data for a specific stock.

        Returns:
            dict with keys: year_month, revenue, yoy_pct, mom_pct
            or None if stock not found.
        """
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')

            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < 10:
                        continue

                    # MOPS 營收表格欄位:
                    # 公司代號, 公司名稱, 當月營收, 上月營收, 去年同月營收,
                    # 上月比較增減(%), 去年同月增減(%), 當月累計營收, 去年累計營收,
                    # 前期比較增減(%)
                    cell_text = [c.get_text(strip=True) for c in cells]
                    row_stock_id = cell_text[0].strip()

                    if row_stock_id == stock_id:
                        revenue = self._safe_float(cell_text[2])  # 當月營收
                        mom_pct = self._safe_float(cell_text[5])  # 上月比較增減(%)
                        yoy_pct = self._safe_float(cell_text[6])  # 去年同月增減(%)

                        return {
                            'year_month': f"{year}-{month:02d}",
                            'revenue': revenue,
                            'yoy_pct': yoy_pct,
                            'mom_pct': mom_pct,
                        }

        except ImportError:
            logger.error("beautifulsoup4 is required for parsing MOPS revenue data")
        except Exception as e:
            logger.warning("Error parsing MOPS revenue HTML for %s (%d-%02d): %s",
                           stock_id, year, month, e)

        return None

    # ------------------------------------------------------------------ #
    #  5. 上櫃三大法人 (TPEX)
    # ------------------------------------------------------------------ #

    def get_tpex_institutional(self, stock_id, days=10):
        """
        Fetch institutional investor trading data for OTC (上櫃) stocks from TPEX.

        Args:
            stock_id: Taiwan OTC stock ID (e.g., '6547')
            days: Number of trading days to fetch

        Returns:
            DataFrame indexed by date with columns:
            ['外資', '投信', '自營商', '合計']
        """
        cache_key = f"tpex_inst_{stock_id}_{days}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        stock_id = str(stock_id).strip()
        logger.info("Fetching TPEX institutional trading for %s (last %d days)", stock_id, days)

        results = []
        dates = self._get_recent_trading_dates(days=days + 10)

        for dt in dates:
            if len(results) >= days:
                break

            roc_date = self._to_tpex_date(dt)
            url = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
            params = {
                'l': 'zh-tw',
                'o': 'json',
                'se': 'EW',
                't': 'D',
                'd': roc_date,
                's': '0,asc,0',
            }

            data = self._fetch_json(url, params=params)
            if data is None:
                continue

            # TPEX 回應格式: reportDate, iTotalRecords, aaData
            total = data.get('iTotalRecords', 0)
            if total == 0:
                logger.debug("No TPEX data for date %s", roc_date)
                continue

            rows = data.get('aaData', [])
            if not rows:
                continue

            # aaData 每一行: [代號, 名稱, 外資及陸資-買進, 外資及陸資-賣出, 外資及陸資-買賣超,
            #                 投信-買進, 投信-賣出, 投信-買賣超,
            #                 自營商(自行)-買進, 自營商(自行)-賣出, 自營商(自行)-買賣超,
            #                 自營商(避險)-買進, 自營商(避險)-賣出, 自營商(避險)-買賣超,
            #                 三大法人買賣超]
            for row in rows:
                if len(row) < 15:
                    continue
                row_id = str(row[0]).strip()
                if row_id == stock_id:
                    try:
                        foreign_net = self._safe_int(row[4])    # 外資買賣超
                        trust_net = self._safe_int(row[7])      # 投信買賣超
                        dealer_self = self._safe_int(row[10])   # 自營商(自行)
                        dealer_hedge = self._safe_int(row[13])  # 自營商(避險)
                        dealer_net = dealer_self + dealer_hedge
                        total_net = self._safe_int(row[14]) if len(row) > 14 else (
                            foreign_net + trust_net + dealer_net
                        )

                        results.append({
                            'date': dt.strftime('%Y-%m-%d'),
                            '外資': foreign_net,
                            '投信': trust_net,
                            '自營商': dealer_net,
                            '合計': total_net,
                        })
                    except (IndexError, KeyError) as e:
                        logger.warning("Error parsing TPEX data for %s on %s: %s",
                                       stock_id, roc_date, e)
                    break

        if not results:
            logger.warning("No TPEX institutional data found for %s", stock_id)
            df = pd.DataFrame(columns=['外資', '投信', '自營商', '合計'])
            df.index.name = 'date'
            self._set_cache(cache_key, df)
            return df

        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()

        self._set_cache(cache_key, df)
        logger.info("Fetched %d days of TPEX institutional data for %s", len(df), stock_id)
        return df

    # ------------------------------------------------------------------ #
    #  Utility: 清除快取
    # ------------------------------------------------------------------ #

    def clear_cache(self):
        """Clear all in-memory cache."""
        count = len(self._cache)
        self._cache.clear()
        logger.info("Cleared %d cached entries", count)


# ====================================================================== #
#  __main__ test block
# ====================================================================== #

if __name__ == '__main__':
    # 設定 logging 格式 (避免 emoji，Windows cp950 相容)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    )

    api = TWSEOpenData()
    test_stock = '2330'

    print("=" * 60)
    print(f"  TWSE Open Data API Test - Stock: {test_stock}")
    print("=" * 60)

    # Test 1: 三大法人買賣超
    print("\n--- [1] Institutional Trading (TWSE) ---")
    try:
        df_inst = api.get_institutional_trading(test_stock, days=5)
        if df_inst.empty:
            print("  (No data returned)")
        else:
            print(df_inst.to_string())
    except Exception as e:
        print(f"  Error: {e}")

    # Test 2: 融資融券
    print("\n--- [2] Margin Trading ---")
    try:
        df_margin = api.get_margin_trading(test_stock, days=5)
        if df_margin.empty:
            print("  (No data returned)")
        else:
            print(df_margin.to_string())
    except Exception as e:
        print(f"  Error: {e}")

    # Test 3: 本益比 / 殖利率 / 淨值比
    print("\n--- [3] PE / Dividend Yield / PB (All Stocks) ---")
    try:
        df_pe = api.get_pe_dividend_all()
        if df_pe.empty:
            print("  (No data returned)")
        else:
            # 只顯示測試股票
            target = df_pe[df_pe['stock_id'] == test_stock]
            if not target.empty:
                print(target.to_string(index=False))
            else:
                print(f"  Stock {test_stock} not found in PE data")
                print(f"  (Total {len(df_pe)} stocks fetched, showing first 5:)")
                print(df_pe.head().to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    # Test 4: 每月營收
    print("\n--- [4] Monthly Revenue (MOPS) ---")
    try:
        df_rev = api.get_monthly_revenue(test_stock, months=6)
        if df_rev.empty:
            print("  (No data returned)")
        else:
            print(df_rev.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    # Test 5: 上櫃三大法人 (用 6547 測試上櫃股)
    tpex_stock = '6547'
    print(f"\n--- [5] TPEX Institutional ({tpex_stock}) ---")
    try:
        df_tpex = api.get_tpex_institutional(tpex_stock, days=5)
        if df_tpex.empty:
            print("  (No data returned)")
        else:
            print(df_tpex.to_string())
    except Exception as e:
        print(f"  Error: {e}")

    print("\n" + "=" * 60)
    print("  All tests completed.")
    print("=" * 60)
