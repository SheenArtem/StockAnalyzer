"""
Finviz 數據模組 (美股概覽/分析師目標價)

功能:
1. 股票快速概覽 (sector, industry, country, market cap)
2. 分析師目標價 (target price, price range)
3. 關鍵指標 (P/E, EPS, Beta, SMA, RSI)
4. 技術面快照 (52w range, volatility, relative volume)

數據來源: Finviz (https://finviz.com)
無需 API Key (爬蟲方式)
"""

import logging
import time
from datetime import datetime
from typing import Dict, Optional, Any

import requests
from bs4 import BeautifulSoup
import pandas as pd

logger = logging.getLogger(__name__)

CACHE_TTL = 60 * 30  # 30 分鐘

FINVIZ_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://finviz.com/',
}


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


class FinvizAnalyzer:
    """
    Finviz 股票數據分析器
    提供美股快速概覽、分析師目標價、技術指標快照
    """

    def __init__(self):
        self._cache = _SimpleCache()
        self.session = requests.Session()
        self.session.headers.update(FINVIZ_HEADERS)

    def get_stock_data(self, ticker: str) -> tuple:
        """
        取得 Finviz 股票數據

        Args:
            ticker: 美股代號 (如 AAPL, NVDA)

        Returns:
            (dict, Optional[str]): (數據字典, 錯誤訊息)
        """
        ticker = ticker.upper().strip()

        # 排除台股
        if ticker.endswith('.TW') or ticker.endswith('.TWO') or ticker.isdigit():
            return None, "Finviz 僅支援美股"

        cache_key = f'finviz_{ticker}'
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached, None

        try:
            url = f'https://finviz.com/quote.ashx?t={ticker}&p=d'
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'lxml')

            # 解析 snapshot table (class="snapshot-table2")
            raw_data = self._parse_snapshot_table(soup)

            if not raw_data:
                return None, f"Finviz 找不到 {ticker} 資料"

            # 整理結果
            result = {
                'overview': self._extract_overview(raw_data),
                'valuation': self._extract_valuation(raw_data),
                'technical': self._extract_technical(raw_data),
                'analyst': self._extract_analyst(raw_data),
                'raw': raw_data,
            }

            self._cache.set(cache_key, result)
            return result, None

        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 403:
                return None, "Finviz 暫時封鎖請求 (403)，請稍後再試"
            return None, f"Finviz HTTP 錯誤: {e}"
        except Exception as e:
            logger.error(f"Finviz error for {ticker}: {e}")
            return None, f"Finviz 數據取得失敗: {e}"

    def _parse_snapshot_table(self, soup: BeautifulSoup) -> Dict[str, str]:
        """解析 Finviz snapshot table"""
        data = {}

        # 找到 snapshot-table2 (主要數據表格)
        table = soup.find('table', class_='snapshot-table2')
        if not table:
            # 嘗試其他選擇器
            table = soup.find('table', {'class': 'snapshot-table2'})
        if not table:
            return data

        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            # 每兩個 cell 是一組 (label, value)
            for i in range(0, len(cells) - 1, 2):
                label = cells[i].get_text(strip=True)
                value = cells[i + 1].get_text(strip=True)
                if label:
                    data[label] = value

        return data

    def _extract_overview(self, raw: Dict) -> Dict:
        """擷取公司概覽"""
        return {
            'index': raw.get('Index', 'N/A'),
            'sector': raw.get('Sector', 'N/A'),
            'industry': raw.get('Industry', 'N/A'),
            'country': raw.get('Country', 'N/A'),
            'market_cap': raw.get('Market Cap', 'N/A'),
            'income': raw.get('Income', 'N/A'),
            'employees': raw.get('Employees', 'N/A'),
        }

    def _extract_valuation(self, raw: Dict) -> Dict:
        """擷取估值指標"""
        return {
            'pe': raw.get('P/E', 'N/A'),
            'forward_pe': raw.get('Forward P/E', 'N/A'),
            'peg': raw.get('PEG', 'N/A'),
            'ps': raw.get('P/S', 'N/A'),
            'pb': raw.get('P/B', 'N/A'),
            'pc': raw.get('P/C', 'N/A'),
            'pfcf': raw.get('P/FCF', 'N/A'),
            'eps_ttm': raw.get('EPS (ttm)', 'N/A'),
            'eps_next_y': raw.get('EPS next Y', 'N/A'),
            'eps_growth_next_5y': raw.get('EPS next 5Y', 'N/A'),
            'dividend_yield': raw.get('Dividend %', 'N/A'),
        }

    def _extract_technical(self, raw: Dict) -> Dict:
        """擷取技術面數據"""
        return {
            'price': raw.get('Price', 'N/A'),
            'change': raw.get('Change', 'N/A'),
            'volume': raw.get('Volume', 'N/A'),
            'avg_volume': raw.get('Avg Volume', 'N/A'),
            'rel_volume': raw.get('Rel Volume', 'N/A'),
            'beta': raw.get('Beta', 'N/A'),
            'sma20': raw.get('SMA20', 'N/A'),
            'sma50': raw.get('SMA50', 'N/A'),
            'sma200': raw.get('SMA200', 'N/A'),
            'rsi14': raw.get('RSI (14)', 'N/A'),
            'atr14': raw.get('ATR (14)', 'N/A'),
            'volatility_w': raw.get('Volatility', 'N/A').split(' ')[0] if raw.get('Volatility') else 'N/A',
            'volatility_m': raw.get('Volatility', 'N/A').split(' ')[-1] if raw.get('Volatility') else 'N/A',
            'high_52w': raw.get('52W High', 'N/A'),
            'low_52w': raw.get('52W Low', 'N/A'),
            'perf_week': raw.get('Perf Week', 'N/A'),
            'perf_month': raw.get('Perf Month', 'N/A'),
            'perf_quarter': raw.get('Perf Quarter', 'N/A'),
            'perf_ytd': raw.get('Perf YTD', 'N/A'),
            'perf_year': raw.get('Perf Year', 'N/A'),
            'short_float': raw.get('Short Float', 'N/A'),
            'short_ratio': raw.get('Short Ratio', 'N/A'),
        }

    def _extract_analyst(self, raw: Dict) -> Dict:
        """擷取分析師目標"""
        target_str = raw.get('Target Price', 'N/A')
        price_str = raw.get('Price', 'N/A')

        target_price = self._safe_float(target_str)
        current_price = self._safe_float(price_str)

        upside = None
        if target_price and current_price and current_price > 0:
            upside = ((target_price - current_price) / current_price) * 100

        return {
            'target_price': target_price,
            'current_price': current_price,
            'upside_pct': upside,
            'recommendation': raw.get('Recom', 'N/A'),
        }

    @staticmethod
    def _safe_float(val: str) -> Optional[float]:
        """安全轉換數值"""
        if not val or val == 'N/A' or val == '-':
            return None
        try:
            # 移除百分號、逗號
            cleaned = val.replace('%', '').replace(',', '').strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return None
