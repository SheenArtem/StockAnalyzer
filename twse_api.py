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
import re
import time
import urllib3
from datetime import datetime, timedelta

import pandas as pd
import requests

# 部分環境 TWSE SSL 憑證驗證失敗，停用相關警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        self._session.verify = False  # 部分環境 TWSE SSL 憑證驗證失敗
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
    def _parse_payload_date(raw):
        """解析 payload 自報的資料日期，回 pd.Timestamp（失敗回 None）。

        兩個 EOD endpoint 都會自報日期，格式不一：
          - TWSE MI_INDEX 頂層 `date`: "20260731"（西元）
          - TPEX stk_quote 頂層 `date`: "20260731"，`tables[0].date`: "115/07/31"（民國）
          - 表格 title: "115年07月31日 每日收盤行情(...)"
        """
        if raw is None:
            return None
        s = str(raw).strip()
        if not s:
            return None
        m = re.search(r'(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日', s)
        if m:
            y, mo, d = int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3))
            try:
                return pd.Timestamp(year=y, month=mo, day=d)
            except ValueError:
                return None
        m = re.fullmatch(r'(\d{4})(\d{2})(\d{2})', s)
        if m:
            try:
                return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)),
                                    day=int(m.group(3)))
            except ValueError:
                return None
        m = re.fullmatch(r'(\d{2,4})[/-](\d{1,2})[/-](\d{1,2})', s)
        if m:
            y = int(m.group(1))
            y = y + 1911 if y < 1000 else y            # 民國年 → 西元年
            try:
                return pd.Timestamp(year=y, month=int(m.group(2)), day=int(m.group(3)))
            except ValueError:
                return None
        return None

    def _enforce_payload_date(self, df, requested, market, strict_date):
        """確認 payload 自報日期 == 請求日期；不符時（strict）回空 frame。

        存在的理由：TPEX **舊**端點 `stk_quote_result.php` **完全無視** `d` 參數，
        2026-08-02 實測請求 115/06/16（6 週前）回的是 115/07/31 的橫斷面、價格一字
        不差；請求週六 115/08/01 同樣回 07/31。TWSE MI_INDEX 則正確分辨（非交易日
        直接回 stat="很抱歉，沒有符合條件的資料!"）。

        ✅ 該端點已於同日改為認日期的 `dailyQuotes`（見 `get_market_daily_tpex`），
        但**這道防線不因此拆除** —— 端點會變，自報日期的驗證是不變的守則。

        所以「請求日期」不可當資料日期用。指定日期的呼叫者（歷史回填、週報、官方
        EOD overlay）若拿到別天的橫斷面，會把舊行情蓋上錯誤日期，且每一欄都是正數
        的合理價格，任何數值健康度檢查都抓不到。寧可回空讓上游走 fallback。
        """
        if df is None or df.empty or requested is None:
            return df
        want = pd.Timestamp(requested).normalize()
        got = df['data_date'].dropna()
        got = pd.Timestamp(got.iloc[0]).normalize() if len(got) else None
        if got == want:
            return df
        if not strict_date:
            logger.warning("%s market daily: requested %s but payload self-reports %s "
                           "(strict_date=False, rows kept as-is)",
                           market, want.date(), got.date() if got is not None else None)
            return df
        logger.warning("%s market daily: requested %s but payload self-reports %s "
                       "-- dropping %d rows (endpoint ignored the date param)",
                       market, want.date(),
                       got.date() if got is not None else None, len(df))
        return df.iloc[0:0].copy()

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

                for key in ['證券代號', '股票代號']:
                    if key in field_map:
                        stock_id_val = str(row[field_map[key]]).strip()
                        break
                for key in ['證券名稱', '股票名稱']:
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

    def get_pe_dividend_all_tpex(self):
        """
        Fetch P/E, P/B, dividend yield for all TPEX (OTC) stocks.

        Returns:
            DataFrame with columns: ['stock_id', 'stock_name', 'PE', 'dividend_yield', 'PB']
        """
        cols = ['stock_id', 'stock_name', 'PE', 'dividend_yield', 'PB']
        cache_key = "tpex_pe_dividend_all"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        logger.info("Fetching TPEX P/E, P/B, dividend yield (all OTC stocks)")

        # Try recent trading dates
        dates_to_try = self._get_recent_trading_dates(days=5)
        for dt in dates_to_try:
            date_str = self._to_tpex_date(dt)
            url = "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php"
            params = {'l': 'zh-tw', 'd': date_str, 'o': 'json'}

            data = self._fetch_json(url, params=params)
            if data is None:
                continue

            tables = data.get('tables', [])
            rows = tables[0].get('data', []) if tables else []
            if not rows:
                continue

            results = []
            for row in rows:
                try:
                    if len(row) < 7:
                        continue
                    sid = str(row[0]).strip()
                    if not sid.isdigit() or len(sid) != 4:
                        continue
                    sname = str(row[1]).strip()
                    pe_val = self._safe_float(row[2])
                    dy_val = self._safe_float(row[5])
                    pb_val = self._safe_float(row[6])

                    results.append({
                        'stock_id': sid,
                        'stock_name': sname,
                        'PE': pe_val,
                        'dividend_yield': dy_val,
                        'PB': pb_val,
                    })
                except (IndexError, KeyError) as e:
                    logger.debug("Error parsing TPEX PE row: %s", e)
                    continue

            if results:
                df = pd.DataFrame(results, columns=cols)
                self._set_cache(cache_key, df)
                logger.info("Fetched TPEX PE/dividend data for %d stocks", len(df))
                return df

        logger.warning("Failed to fetch TPEX PE/dividend data")
        return pd.DataFrame(columns=cols)

    def get_pe_dividend_all_combined(self):
        """
        Fetch P/E, P/B, dividend yield for ALL stocks (TWSE + TPEX combined).

        Returns:
            DataFrame with columns: ['stock_id', 'stock_name', 'PE', 'dividend_yield', 'PB']
        """
        cache_key = "pe_dividend_all_combined"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        df_twse = self.get_pe_dividend_all()
        df_tpex = self.get_pe_dividend_all_tpex()

        if df_twse.empty and df_tpex.empty:
            return pd.DataFrame(columns=['stock_id', 'stock_name', 'PE', 'dividend_yield', 'PB'])

        df = pd.concat([df_twse, df_tpex], ignore_index=True)
        self._set_cache(cache_key, df)
        logger.info("Combined PE data: TWSE=%d + TPEX=%d = %d",
                     len(df_twse), len(df_tpex), len(df))
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

            # TPEX 回應格式 (新版): tables[0]['data'], (舊版): aaData
            rows = data.get('aaData', [])
            if not rows:
                tables = data.get('tables', [])
                if tables and isinstance(tables[0], dict):
                    rows = tables[0].get('data', [])
            if not rows:
                logger.debug("No TPEX data for date %s", roc_date)
                continue

            # 新版 24 欄: [代號, 名稱,
            #   外資及陸資(不含自營)-買進, 賣出, 買賣超,
            #   外資自營商-買進, 賣出, 買賣超,
            #   外資及陸資合計-買進, 賣出, 買賣超,
            #   投信-買進, 賣出, 買賣超,
            #   自營商(自行)-買進, 賣出, 買賣超,
            #   自營商(避險)-買進, 賣出, 買賣超,
            #   自營商合計-買進, 賣出, 買賣超,
            #   三大法人買賣超合計]
            # 舊版 15 欄: [代號, 名稱, 外資-買進, 賣出, 買賣超,
            #   投信-買進, 賣出, 買賣超,
            #   自營商(自行)-買進, 賣出, 買賣超,
            #   自營商(避險)-買進, 賣出, 買賣超,
            #   三大法人買賣超]
            is_new_fmt = len(rows[0]) >= 24 if rows else False

            for row in rows:
                if len(row) < 15:
                    continue
                row_id = str(row[0]).strip()
                if row_id == stock_id:
                    try:
                        if is_new_fmt:
                            # 新版: 外資合計買賣超=idx10, 投信買賣超=idx13,
                            #       自營商合計買賣超=idx22, 三大法人合計=idx23
                            foreign_net = self._safe_int(row[10])
                            trust_net = self._safe_int(row[13])
                            dealer_net = self._safe_int(row[22])
                            total_net = self._safe_int(row[23])
                        else:
                            foreign_net = self._safe_int(row[4])
                            trust_net = self._safe_int(row[7])
                            dealer_self = self._safe_int(row[10])
                            dealer_hedge = self._safe_int(row[13])
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
    #  Batch: 全市場三大法人 (供 scanner 批次使用)
    # ------------------------------------------------------------------ #

    def get_institutional_batch(self, days=5):
        """
        Fetch institutional trading data for ALL stocks over recent N days.
        Returns dict: { stock_id: DataFrame(外資, 投信, 自營商, 合計) }

        Only N+few API calls total (not per-stock), suitable for scanner.
        """
        cache_key = f"inst_batch_{days}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        logger.info("Batch fetching institutional data for last %d trading days", days)

        # Collect per-stock records: { stock_id: [{date, ...}, ...] }
        all_records = {}
        dates = self._get_recent_trading_dates(days=days + 10)
        fetched_days = 0

        for dt in dates:
            if fetched_days >= days:
                break

            # --- TWSE ---
            date_str = self._to_twse_date(dt)
            url = "https://www.twse.com.tw/rwd/zh/fund/T86"
            params = {'date': date_str, 'selectType': 'ALL', 'response': 'json'}
            data = self._fetch_json(url, params=params)

            twse_ok = False
            if data and data.get('stat') == 'OK':
                fields = data.get('fields', [])
                rows = data.get('data', [])
                if fields and rows:
                    twse_ok = True
                    field_map = {f.strip(): i for i, f in enumerate(fields)}
                    for row in rows:
                        if len(row) < 2:
                            continue
                        sid = str(row[0]).strip()
                        if not sid.isdigit():
                            continue
                        try:
                            foreign_net = 0
                            for key in ['外陸資買賣超股數(不含外資自營商)', '外資及陸資買賣超股數', '外資買賣超股數']:
                                if key in field_map:
                                    foreign_net = self._safe_int(row[field_map[key]])
                                    break
                            for key in ['外資自營商買賣超股數']:
                                if key in field_map:
                                    foreign_net += self._safe_int(row[field_map[key]])
                                    break
                            trust_net = 0
                            for key in ['投信買賣超股數']:
                                if key in field_map:
                                    trust_net = self._safe_int(row[field_map[key]])
                                    break
                            dealer_net = 0
                            for key in ['自營商買賣超股數']:
                                if key in field_map:
                                    dealer_net = self._safe_int(row[field_map[key]])
                                    break
                            if dealer_net == 0:
                                d1 = d2 = 0
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
                            total_net = 0
                            for key in ['三大法人買賣超股數']:
                                if key in field_map:
                                    total_net = self._safe_int(row[field_map[key]])
                                    break
                            if total_net == 0:
                                total_net = foreign_net + trust_net + dealer_net

                            all_records.setdefault(sid, []).append({
                                'date': dt.strftime('%Y-%m-%d'),
                                '外資': foreign_net, '投信': trust_net,
                                '自營商': dealer_net, '合計': total_net,
                            })
                        except Exception:
                            continue

            # --- TPEX ---
            roc_date = self._to_tpex_date(dt)
            url_tpex = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
            params_tpex = {'l': 'zh-tw', 'o': 'json', 'se': 'EW', 't': 'D', 'd': roc_date, 's': '0,asc,0'}
            data_tpex = self._fetch_json(url_tpex, params=params_tpex)

            tpex_ok = False
            if data_tpex:
                tpex_rows = data_tpex.get('aaData', [])
                if not tpex_rows:
                    tables = data_tpex.get('tables', [])
                    if tables and isinstance(tables[0], dict):
                        tpex_rows = tables[0].get('data', [])
                is_new_fmt = len(tpex_rows[0]) >= 24 if tpex_rows else False

                if tpex_rows:
                    tpex_ok = True
                    for row in tpex_rows:
                        if len(row) < 15:
                            continue
                        sid = str(row[0]).strip()
                        if not sid.isdigit():
                            continue
                        try:
                            if is_new_fmt:
                                foreign_net = self._safe_int(row[10])
                                trust_net = self._safe_int(row[13])
                                dealer_net = self._safe_int(row[22])
                                total_net = self._safe_int(row[23])
                            else:
                                foreign_net = self._safe_int(row[4])
                                trust_net = self._safe_int(row[7])
                                dealer_self = self._safe_int(row[10])
                                dealer_hedge = self._safe_int(row[13])
                                dealer_net = dealer_self + dealer_hedge
                                total_net = self._safe_int(row[14]) if len(row) > 14 else (
                                    foreign_net + trust_net + dealer_net)

                            all_records.setdefault(sid, []).append({
                                'date': dt.strftime('%Y-%m-%d'),
                                '外資': foreign_net, '投信': trust_net,
                                '自營商': dealer_net, '合計': total_net,
                            })
                        except Exception:
                            continue

            if twse_ok or tpex_ok:
                fetched_days += 1

        # Convert to DataFrames
        result = {}
        for sid, records in all_records.items():
            df = pd.DataFrame(records)
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date').sort_index()
            result[sid] = df

        logger.info("Batch institutional: %d stocks, %d days fetched", len(result), fetched_days)
        self._set_cache(cache_key, result)
        return result

    # ------------------------------------------------------------------ #
    #  Utility: 清除快取
    # ------------------------------------------------------------------ #

    def clear_cache(self):
        """Clear all in-memory cache."""
        count = len(self._cache)
        self._cache.clear()
        logger.info("Cleared %d cached entries", count)

    # ------------------------------------------------------------------ #
    #  7. 全市場每日行情 (Screening 用)
    # ------------------------------------------------------------------ #

    def get_market_daily_twse(self, date=None, strict_date=True):
        """
        Fetch daily trading summary for ALL TWSE-listed stocks.

        Args:
            date: datetime object or None (defaults to most recent trading day)
            strict_date: 指定 date 時，payload 自報日期不符即回空 frame
                （見 `_enforce_payload_date`）。date=None 不套用。

        Returns:
            DataFrame with columns:
            ['stock_id', 'stock_name', 'market', 'close', 'change',
             'open', 'high', 'low', 'volume', 'trading_value', 'trades',
             'data_date']
            `data_date` 是 payload 自報的資料日期，不是請求日期。
        """
        cols = ['stock_id', 'stock_name', 'market', 'close', 'change',
                'open', 'high', 'low', 'volume', 'trading_value', 'trades']
        cache_key = (f"market_daily_twse_{(date or datetime.now()).strftime('%Y%m%d')}"
                     f"_{int(bool(strict_date))}")
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        # Try recent trading dates until we get data
        if date:
            dates_to_try = [date]
        else:
            dates_to_try = self._get_recent_trading_dates(days=5)

        for dt in dates_to_try:
            date_str = self._to_twse_date(dt)
            url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
            params = {'response': 'json', 'date': date_str, 'type': 'ALLBUT0999'}

            data = self._fetch_json(url, params=params)
            if data is None or data.get('stat') != 'OK':
                continue

            # TWSE returns stock data in tables array or data9 field
            tables = data.get('tables', [])
            rows = None
            fields = None

            # New format: tables array with title containing stock data
            payload_date = self._parse_payload_date(data.get('date'))
            for table in tables:
                title = table.get('title', '')
                if '每日收盤行情' in title or '全部' in title:
                    fields = table.get('fields', [])
                    rows = table.get('data', [])
                    # 表格 title 內嵌民國日期，比頂層 date 更貼近這批 rows
                    payload_date = (self._parse_payload_date(title)
                                    or self._parse_payload_date(table.get('date'))
                                    or payload_date)
                    break

            # Legacy format: data9 / fields9
            if rows is None:
                fields = data.get('fields9', [])
                rows = data.get('data9', [])

            if not rows:
                continue

            # Build field index map
            field_map = {f.strip(): i for i, f in enumerate(fields)}

            results = []
            for row in rows:
                try:
                    sid = str(row[field_map.get('證券代號', 0)]).strip()
                    # Filter: only regular stocks (4-digit numeric IDs)
                    if not sid.isdigit() or len(sid) != 4:
                        continue

                    sname = str(row[field_map.get('證券名稱', 1)]).strip()

                    # Parse change direction and value
                    change_val = 0.0
                    if '漲跌價差' in field_map:
                        change_val = self._safe_float(row[field_map['漲跌價差']])
                    elif '漲跌(+/-)' in field_map:
                        # Some formats split direction and value
                        direction = str(row[field_map['漲跌(+/-)']]).strip()
                        diff_idx = field_map.get('漲跌價差', field_map['漲跌(+/-)'] + 1)
                        change_val = self._safe_float(row[diff_idx])
                        if direction == '-':
                            change_val = -abs(change_val)
                        elif direction == '+':
                            change_val = abs(change_val)

                    close_val = self._safe_float(row[field_map.get('收盤價', 8)])

                    results.append({
                        'stock_id': sid,
                        'stock_name': sname,
                        'market': 'twse',
                        'close': close_val,
                        'change': change_val,
                        'open': self._safe_float(row[field_map.get('開盤價', 5)]),
                        'high': self._safe_float(row[field_map.get('最高價', 6)]),
                        'low': self._safe_float(row[field_map.get('最低價', 7)]),
                        'volume': self._safe_int(row[field_map.get('成交股數', 2)]),
                        'trading_value': self._safe_int(row[field_map.get('成交金額', 4)]),
                        'trades': self._safe_int(row[field_map.get('成交筆數', 3)]),
                    })
                except (IndexError, KeyError) as e:
                    logger.debug("Error parsing TWSE row: %s", e)
                    continue

            if results:
                df = pd.DataFrame(results, columns=cols)
                df['change_pct'] = df.apply(
                    lambda r: (r['change'] / (r['close'] - r['change']) * 100)
                    if (r['close'] - r['change']) != 0 else 0.0, axis=1
                )
                df['data_date'] = payload_date
                logger.info("TWSE market daily: %d stocks fetched (requested=%s, "
                            "payload date=%s)", len(df), date_str,
                            payload_date.date() if payload_date is not None else None)
                df = self._enforce_payload_date(df, date, 'TWSE', strict_date)
                if df.empty:
                    continue
                self._set_cache(cache_key, df)
                return df

        logger.warning("Failed to fetch TWSE market daily data")
        return pd.DataFrame(columns=cols + ['change_pct', 'data_date'])

    def get_market_daily_tpex(self, date=None, strict_date=True):
        """
        Fetch daily trading summary for ALL TPEX (OTC) stocks.

        Args:
            date: datetime object or None (defaults to most recent trading day)
            strict_date: 指定 date 時，payload 自報日期不符即回空 frame。

        端點沿革（2026-08-02 兩輪實測）：
          - 舊版 `web/stock/aftertrading/daily_close_quotes/stk_quote_result.php`
            **完全無視 `d` 參數**（請求 115/06/16 回 115/07/31 的橫斷面，價格一字
            不差），只能拿到「最新」那天。
          - 現用 `www/zh-tw/afterTrading/dailyQuotes` **正確認日期**，且是同一份資料
            集（title 同為「上櫃股票行情」、19 個欄位含 `均價`、成交量同為含定價口徑）
            —— 所以下方的欄位索引與舊版共用，不必分支。

        ⚠️ **不要改用 `www/zh-tw/afterTrading/otc?type=EW`**：它也認日期，但那是
        「上櫃股票每日收盤行情(不含定價)」，成交量口徑不同（實測 876 檔上櫃股中只有
        31 檔與現有 panel 相符，dailyQuotes 則有 871 檔相符）。混用會讓 panel 裡出現
        兩種成交量定義。

        `strict_date` 仍保留為安全網 —— 端點換了不代表可以不驗自報日期。

        Returns:
            DataFrame with same columns as get_market_daily_twse()（含 `data_date`）
        """
        cols = ['stock_id', 'stock_name', 'market', 'close', 'change',
                'open', 'high', 'low', 'volume', 'trading_value', 'trades']
        cache_key = (f"market_daily_tpex_{(date or datetime.now()).strftime('%Y%m%d')}"
                     f"_{int(bool(strict_date))}")
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        if date:
            dates_to_try = [date]
        else:
            dates_to_try = self._get_recent_trading_dates(days=5)

        for dt in dates_to_try:
            # 這個端點吃西元 YYYY/MM/DD（不是民國）；_to_tpex_date 仍留給其他呼叫點用
            date_str = dt.strftime('%Y/%m/%d')
            url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
            params = {'date': date_str, 'type': 'EW', 'id': '', 'response': 'json'}

            data = self._fetch_json(url, params=params)
            if data is None:
                continue

            # TPEX format: tables[0]['data'] (newer) or aaData (legacy)
            payload_date = (self._parse_payload_date(data.get('date'))
                            or self._parse_payload_date(data.get('reportDate')))
            rows = data.get('aaData', [])
            if not rows:
                tables = data.get('tables', [])
                if tables:
                    rows = tables[0].get('data', [])
                    payload_date = (self._parse_payload_date(tables[0].get('date'))
                                    or payload_date)
            if not rows:
                continue

            results = []
            for row in rows:
                try:
                    if len(row) < 10:
                        continue
                    sid = str(row[0]).strip()
                    if not sid.isdigit() or len(sid) != 4:
                        continue

                    sname = str(row[1]).strip()
                    close_val = self._safe_float(row[2])
                    change_val = self._safe_float(row[3])

                    # TPEX volume is in shares, trading_value in TWD
                    volume_raw = self._safe_int(row[8]) if len(row) > 8 else 0
                    tv_raw = self._safe_int(row[9]) if len(row) > 9 else 0
                    trades_raw = self._safe_int(row[10]) if len(row) > 10 else 0

                    results.append({
                        'stock_id': sid,
                        'stock_name': sname,
                        'market': 'tpex',
                        'close': close_val,
                        'change': change_val,
                        'open': self._safe_float(row[4]) if len(row) > 4 else 0.0,
                        'high': self._safe_float(row[5]) if len(row) > 5 else 0.0,
                        'low': self._safe_float(row[6]) if len(row) > 6 else 0.0,
                        'volume': volume_raw,
                        'trading_value': tv_raw,
                        'trades': trades_raw,
                    })
                except (IndexError, KeyError, ValueError) as e:
                    logger.debug("Error parsing TPEX row: %s", e)
                    continue

            if results:
                df = pd.DataFrame(results, columns=cols)
                df['change_pct'] = df.apply(
                    lambda r: (r['change'] / (r['close'] - r['change']) * 100)
                    if (r['close'] - r['change']) != 0 else 0.0, axis=1
                )
                df['data_date'] = payload_date
                logger.info("TPEX market daily: %d stocks fetched (requested=%s, "
                            "payload date=%s)", len(df), date_str,
                            payload_date.date() if payload_date is not None else None)
                df = self._enforce_payload_date(df, date, 'TPEX', strict_date)
                if df.empty:
                    continue
                self._set_cache(cache_key, df)
                return df

        logger.warning("Failed to fetch TPEX market daily data")
        return pd.DataFrame(columns=cols + ['change_pct', 'data_date'])

    def get_market_daily_all(self, date=None, strict_date=True):
        """
        Fetch daily trading data for ALL listed stocks (TWSE + TPEX combined).

        Args:
            date: datetime object or None (defaults to most recent trading day)
            strict_date: 指定 date 時，只接受 payload 自報日期 == 請求日期的橫斷面。
                預設 True —— 問特定一天就該拿到那一天或空的，不該拿到「最近那天」。

        Returns:
            DataFrame with columns: stock_id, stock_name, market, close, change,
            change_pct, open, high, low, volume, trading_value, trades, data_date

        `data_date` 是 payload 自報日期，逐列可能不同（TWSE 與 TPEX 各自回報）。
        兩邊日期不一致時會 log warning：混日期的橫斷面對「當日全市場」語意的下游
        （overlay、廣度、週報金額換算）是錯的，呼叫者應自行按 `data_date` 過濾。
        """
        cache_key = (f"market_daily_all_{(date or datetime.now()).strftime('%Y%m%d')}"
                     f"_{int(bool(strict_date))}")
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        df_twse = self.get_market_daily_twse(date, strict_date=strict_date)
        df_tpex = self.get_market_daily_tpex(date, strict_date=strict_date)

        if df_twse.empty and df_tpex.empty:
            return pd.DataFrame()

        # 只 concat 非空的：strict_date 會讓其中一邊變空，而空 frame 進 concat 會讓
        # pandas 對結果 dtype 發 FutureWarning 並在未來版本改變行為。
        parts = [p for p in (df_twse, df_tpex) if not p.empty]
        df = pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0].copy()
        dates = sorted({pd.Timestamp(d).normalize()
                        for d in df['data_date'].dropna().unique()})
        if len(dates) > 1:
            logger.warning("Market daily all: TWSE/TPEX report DIFFERENT data dates %s "
                           "-- callers needing a single-day cross-section must filter "
                           "on data_date", [str(d.date()) for d in dates])
        self._set_cache(cache_key, df)
        logger.info("Market daily all: TWSE=%d + TPEX=%d = %d stocks (data_date=%s)",
                    len(df_twse), len(df_tpex), len(df),
                    [str(d.date()) for d in dates] or None)
        return df

    # ------------------------------------------------------------------ #
    #  處置有價證券 (Disposition Stocks)
    # ------------------------------------------------------------------ #

    def get_tw_disposition_stocks(self):
        """
        取得當前仍在處置期間的 TWSE 上市普通股代號集合。

        資料源：TWSE 公告「處置有價證券」(announcement/punish)
        過濾：
          - 僅 4 位數一般普通股（排除權證 6 位 / 特別股帶字母）
          - 處置期間（ROC 格式 YYY/MM/DD～YYY/MM/DD）涵蓋今日

        Returns:
            set[str]: {stock_id, ...}；失敗則回空 set。

        Note: 目前僅涵蓋 TWSE 上市，TPEX 上櫃 API 需另行開發。
        """
        import re

        cached = self._get_cache('disposition_tw')
        if cached is not None:
            return cached

        today = datetime.now().date()
        today_roc = f"{today.year - 1911}/{today.strftime('%m/%d')}"

        url = 'https://www.twse.com.tw/rwd/zh/announcement/punish'
        try:
            data = self._fetch_json(url, params={'response': 'json'})
            if not data or data.get('stat') != 'OK':
                logger.warning("Disposition fetch: stat=%s", data.get('stat') if data else None)
                return set()

            rows = data.get('data', []) or []
            # 處置起迄時間 pattern: "115/04/17～115/04/30"（全形波浪～）
            period_pat = re.compile(
                r'(\d{2,3}/\d{1,2}/\d{1,2})\s*[~～-]\s*(\d{2,3}/\d{1,2}/\d{1,2})'
            )
            active = set()
            for row in rows:
                if not isinstance(row, list) or len(row) < 7:
                    continue
                sid = str(row[2]).strip()
                # 只保留 4 位數一般普通股
                if not sid.isdigit() or len(sid) != 4 or sid.startswith('0'):
                    continue
                period = str(row[6]).strip()
                m = period_pat.search(period)
                if not m:
                    continue
                start_roc, end_roc = m.group(1), m.group(2)
                # 補零成 YYY/MM/DD 方便字串比較
                start_roc = self._normalize_roc(start_roc)
                end_roc = self._normalize_roc(end_roc)
                today_norm = self._normalize_roc(today_roc)
                if start_roc <= today_norm <= end_roc:
                    active.add(sid)

            self._set_cache('disposition_tw', active)
            logger.info("TW disposition: %d active stocks (%s)",
                        len(active), sorted(active))
            return active

        except Exception as e:
            logger.warning("Failed to fetch TW disposition: %s", e)
            return set()

    def get_tpex_disposition_stocks(self):
        """取得當前 TPEX 上櫃普通股處置代號 set —— 目前 PLACEHOLDER 回空 set。

        2026-05-16 探查 TPEX 公開 API 未果：
          - `/openapi/v1/announce_dispose` 等多個候選 URL 全回 HTML（SPA 渲染）
          - 沒有像 TWSE `announcement/punish` 的直接 JSON endpoint
          - TPEX 處置股需從 https://www.tpex.org.tw/zh-tw/announce/notice/info.html
            HTML SPA 解析或走 Selenium / Playwright

        Known gap：現存的 picks line（QM / Value）對 TPEX 處置股漏排，可能讓被處置中的
        TPEX 股票進 picks list。
        （原註解寫「所有 4 條 picks line (Whale / Strong / QM / Value)」—— Whale 已於
        2026-07-15 端到端移除、強勢股於 2026-05-21 停用，2026-08-02 更正。）
        修法 TODO：考慮 Playwright 抓 HTML SPA 或 FinMind/MOPS 替代資料源。

        Returns:
            set[str]: 空 set (placeholder)；未來實作後回 active TPEX disposed stocks。
        """
        logger.debug("get_tpex_disposition_stocks: PLACEHOLDER returning empty set "
                     "(known gap, see method docstring)")
        return set()

    def get_all_disposition_stocks(self):
        """取得當前所有處置股（TWSE + TPEX）合併 set。

        2026-05-16 加：TWSE 已實作、TPEX 目前 placeholder。callers 改呼叫此方法可
        在 TPEX 補實作後自動受惠不需改各 callers。
        """
        tw = self.get_tw_disposition_stocks()
        tpex = self.get_tpex_disposition_stocks()
        return tw | tpex

    @staticmethod
    def _normalize_roc(roc_str):
        """將 "115/4/2" / "115/04/02" 標準化為 "115/04/02"（方便字串比較）。"""
        parts = roc_str.split('/')
        if len(parts) != 3:
            return roc_str
        return f"{int(parts[0]):03d}/{int(parts[1]):02d}/{int(parts[2]):02d}"


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
