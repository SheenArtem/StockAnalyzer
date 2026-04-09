"""
SEC EDGAR 數據模組 (US Stock Filings & Insider Transactions)

功能:
1. Ticker → CIK 對應
2. 近期重要申報 (10-K, 10-Q, 8-K, 13F-HR)
3. 內部人交易摘要 (Form 4)
4. 機構持股概覽 (13F-HR)

數據來源: SEC EDGAR (https://www.sec.gov/edgar)
無需 API Key，但 SEC 要求 User-Agent 包含聯繫資訊
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List

import requests
import pandas as pd

logger = logging.getLogger(__name__)

# SEC EDGAR 要求自訂 User-Agent
EDGAR_HEADERS = {
    'User-Agent': 'StockAnalyzer/1.0 (stock-analyzer-app)',
    'Accept': 'application/json',
}

# 快取 TTL
CACHE_TTL = 60 * 60  # 1 小時 (SEC 數據更新不頻繁)

# 請求間隔 (SEC 限制 10 req/sec)
REQUEST_DELAY = 0.15


class _SimpleCache:
    """簡易 dict 快取"""

    def __init__(self, ttl: int = CACHE_TTL):
        self._store: Dict[str, Any] = {}
        self._timestamps: Dict[str, float] = {}
        self._ttl = ttl

    def get(self, key: str) -> Optional[Any]:
        if key in self._store:
            if time.time() - self._timestamps[key] < self._ttl:
                return self._store[key]
            del self._store[key]
            del self._timestamps[key]
        return None

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value
        self._timestamps[key] = time.time()


class SECEdgarAnalyzer:
    """
    SEC EDGAR 數據分析器
    提供美股公司申報文件、內部人交易、機構持股等資訊
    """

    def __init__(self):
        self._cache = _SimpleCache()
        self._cik_map: Optional[Dict[str, str]] = None
        self.session = requests.Session()
        self.session.headers.update(EDGAR_HEADERS)

    # ------------------------------------------------------------------
    # CIK 對應
    # ------------------------------------------------------------------
    def _load_cik_map(self) -> Dict[str, str]:
        """載入 ticker → CIK 對應表"""
        if self._cik_map is not None:
            return self._cik_map

        cached = self._cache.get('cik_map')
        if cached:
            self._cik_map = cached
            return cached

        try:
            url = 'https://www.sec.gov/files/company_tickers.json'
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # 建立 ticker → CIK (10位數左補零)
            cik_map = {}
            for item in data.values():
                ticker = item.get('ticker', '').upper()
                cik = str(item.get('cik_str', ''))
                if ticker and cik:
                    cik_map[ticker] = cik.zfill(10)

            self._cik_map = cik_map
            self._cache.set('cik_map', cik_map)
            return cik_map
        except Exception as e:
            logger.error(f"Failed to load CIK map: {e}")
            self._cik_map = {}
            return {}

    def _get_cik(self, ticker: str) -> Optional[str]:
        """取得 ticker 的 CIK"""
        cik_map = self._load_cik_map()
        return cik_map.get(ticker.upper())

    # ------------------------------------------------------------------
    # 公司申報文件
    # ------------------------------------------------------------------
    def get_recent_filings(self, ticker: str, count: int = 20) -> List[Dict]:
        """
        取得近期申報文件清單

        Returns:
            List[Dict]: 每筆包含 form, filingDate, description, url
        """
        cache_key = f'filings_{ticker}'
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        cik = self._get_cik(ticker)
        if not cik:
            logger.warning(f"CIK not found for {ticker}")
            return []

        try:
            url = f'https://data.sec.gov/submissions/CIK{cik}.json'
            time.sleep(REQUEST_DELAY)
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            recent = data.get('filings', {}).get('recent', {})
            forms = recent.get('form', [])
            dates = recent.get('filingDate', [])
            descriptions = recent.get('primaryDocument', [])
            accessions = recent.get('accessionNumber', [])

            filings = []
            important_forms = {'10-K', '10-Q', '8-K', '13F-HR', '4', 'S-1', 'DEF 14A', '6-K', '20-F'}

            for i in range(min(len(forms), 200)):
                form = forms[i]
                if form not in important_forms:
                    continue

                acc_clean = accessions[i].replace('-', '')
                doc = descriptions[i] if i < len(descriptions) else ''
                filing_url = f'https://www.sec.gov/Archives/edgar/data/{cik.lstrip("0")}/{acc_clean}/{doc}'

                filings.append({
                    'form': form,
                    'date': dates[i] if i < len(dates) else '',
                    'description': self._form_description(form),
                    'url': filing_url,
                })

                if len(filings) >= count:
                    break

            self._cache.set(cache_key, filings)
            return filings

        except Exception as e:
            logger.error(f"SEC filings error for {ticker}: {e}")
            return []

    # ------------------------------------------------------------------
    # 內部人交易 (Form 4)
    # ------------------------------------------------------------------
    def get_insider_summary(self, ticker: str) -> Dict:
        """
        取得內部人交易摘要 (近 90 天 Form 4)

        Returns:
            dict: buy_count, sell_count, net_shares, recent_trades (list)
        """
        cache_key = f'insider_{ticker}'
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        cik = self._get_cik(ticker)
        if not cik:
            return {'buy_count': 0, 'sell_count': 0, 'net_shares': 0, 'recent_trades': []}

        try:
            url = f'https://data.sec.gov/submissions/CIK{cik}.json'
            time.sleep(REQUEST_DELAY)
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            recent = data.get('filings', {}).get('recent', {})
            forms = recent.get('form', [])
            dates = recent.get('filingDate', [])
            descriptions = recent.get('primaryDocDescription', [])

            cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

            form4_count = 0
            form4_dates = []

            for i in range(min(len(forms), 500)):
                if forms[i] != '4':
                    continue
                if i < len(dates) and dates[i] < cutoff:
                    continue

                form4_count += 1
                form4_dates.append(dates[i] if i < len(dates) else '')

            # SEC 不直接在 submissions JSON 提供 buy/sell 細節
            # 用 Form 4 申報頻率作為活躍度指標
            result = {
                'form4_count_90d': form4_count,
                'latest_form4_dates': form4_dates[:10],
                'activity_level': '高' if form4_count > 10 else '中' if form4_count > 3 else '低',
            }

            self._cache.set(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"SEC insider error for {ticker}: {e}")
            return {'form4_count_90d': 0, 'latest_form4_dates': [], 'activity_level': '無資料'}

    # ------------------------------------------------------------------
    # 機構持股 (13F-HR)
    # ------------------------------------------------------------------
    def get_institutional_filings(self, ticker: str) -> Dict:
        """
        取得 13F-HR 申報統計 (機構持股申報)

        Returns:
            dict: recent_13f_count, latest_date
        """
        cache_key = f'inst_13f_{ticker}'
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        cik = self._get_cik(ticker)
        if not cik:
            return {'recent_13f_count': 0, 'latest_date': None}

        try:
            url = f'https://data.sec.gov/submissions/CIK{cik}.json'
            time.sleep(REQUEST_DELAY)
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            recent = data.get('filings', {}).get('recent', {})
            forms = recent.get('form', [])
            dates = recent.get('filingDate', [])

            count_13f = 0
            latest_date = None
            for i in range(min(len(forms), 500)):
                if forms[i] == '13F-HR':
                    count_13f += 1
                    if latest_date is None and i < len(dates):
                        latest_date = dates[i]

            result = {
                'recent_13f_count': count_13f,
                'latest_date': latest_date,
            }

            self._cache.set(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"SEC 13F error for {ticker}: {e}")
            return {'recent_13f_count': 0, 'latest_date': None}

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------
    def get_edgar_data(self, ticker: str) -> tuple:
        """
        取得 SEC EDGAR 完整數據

        Returns:
            (dict, Optional[str]): (數據字典, 錯誤訊息)
        """
        ticker = ticker.upper().strip()

        # 排除台股
        if ticker.endswith('.TW') or ticker.endswith('.TWO') or ticker.isdigit():
            return None, "SEC EDGAR 僅支援美股"

        try:
            result = {
                'filings': self.get_recent_filings(ticker),
                'insider': self.get_insider_summary(ticker),
                'institutional': self.get_institutional_filings(ticker),
                'company_name': self._get_company_name(ticker),
            }
            return result, None
        except Exception as e:
            logger.error(f"SEC EDGAR error for {ticker}: {e}")
            return None, str(e)

    # ------------------------------------------------------------------
    # 輔助方法
    # ------------------------------------------------------------------
    def _get_company_name(self, ticker: str) -> str:
        """從 CIK map 取得公司名稱"""
        try:
            url = 'https://www.sec.gov/files/company_tickers.json'
            cached = self._cache.get('cik_raw')
            if not cached:
                resp = self.session.get(url, timeout=15)
                resp.raise_for_status()
                cached = resp.json()
                self._cache.set('cik_raw', cached)

            for item in cached.values():
                if item.get('ticker', '').upper() == ticker.upper():
                    return item.get('title', '')
        except Exception:
            pass
        return ''

    @staticmethod
    def _form_description(form: str) -> str:
        """表單類型的中文描述"""
        desc_map = {
            '10-K': '年度報告',
            '10-Q': '季度報告',
            '8-K': '重大事件',
            '13F-HR': '機構持股申報',
            '4': '內部人交易',
            'S-1': 'IPO 招股書',
            'DEF 14A': '股東大會委託書',
            '6-K': '海外公司報告',
            '20-F': '海外公司年報',
        }
        return desc_map.get(form, form)
