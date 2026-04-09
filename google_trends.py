"""
Google Trends 數據模組 (搜尋熱度分析)

功能:
1. 取得股票關鍵字的搜尋趨勢 (近 90 天)
2. 相關搜尋查詢
3. 搜尋熱度變化判斷 (暴增/持平/下降)

數據來源: Google Trends (透過 pytrends)
無需 API Key
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

import pandas as pd

logger = logging.getLogger(__name__)

CACHE_TTL = 60 * 60  # 1 小時

# 台股代號對應名稱 (常用)
TW_STOCK_NAMES = {
    '2330': '台積電',
    '2317': '鴻海',
    '2454': '聯發科',
    '2308': '台達電',
    '2382': '廣達',
    '2881': '富邦金',
    '2882': '國泰金',
    '2891': '中信金',
    '2303': '聯電',
    '2412': '中華電',
    '3711': '日月光投控',
    '2886': '兆豐金',
    '6505': '台塑化',
    '1301': '台塑',
    '1303': '南亞',
    '2002': '中鋼',
    '1216': '統一',
    '2884': '玉山金',
    '3008': '大立光',
    '2357': '華碩',
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


class GoogleTrendsAnalyzer:
    """
    Google Trends 搜尋熱度分析器
    用於衡量散戶對特定股票的關注程度
    """

    def __init__(self):
        self._cache = _SimpleCache()
        self._pytrends = None

    def _get_pytrends(self):
        """延遲載入 pytrends"""
        if self._pytrends is None:
            try:
                from pytrends.request import TrendReq
                self._pytrends = TrendReq(hl='zh-TW', tz=480)  # UTC+8
            except ImportError:
                raise ImportError(
                    "pytrends 未安裝。請執行: pip install pytrends"
                )
        return self._pytrends

    def get_search_trend(self, ticker: str, stock_name: str = None) -> Dict:
        """
        取得股票搜尋趨勢 (近 90 天)

        Args:
            ticker: 股票代號
            stock_name: 股票名稱 (可選，用於台股搜尋)

        Returns:
            dict: trend_df (DataFrame), current_interest, avg_interest,
                  change_pct, trend_label, related_queries
        """
        cache_key = f'gtrend_{ticker}'
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            pytrends = self._get_pytrends()

            # 決定搜尋關鍵字
            keywords = self._build_keywords(ticker, stock_name)

            # 建立請求 (近 90 天)
            pytrends.build_payload(
                keywords,
                cat=0,
                timeframe='today 3-m',
                geo='',  # 全球
            )

            # 取得趨勢數據
            trend_df = pytrends.interest_over_time()

            if trend_df.empty:
                return self._empty_result('無搜尋數據')

            # 移除 isPartial 欄位
            if 'isPartial' in trend_df.columns:
                trend_df = trend_df.drop(columns=['isPartial'])

            # 計算主要關鍵字的統計
            main_kw = keywords[0]
            if main_kw not in trend_df.columns:
                return self._empty_result('無搜尋數據')

            series = trend_df[main_kw]
            current = int(series.iloc[-1])
            avg = float(series.mean())
            recent_avg = float(series.iloc[-7:].mean()) if len(series) >= 7 else avg
            older_avg = float(series.iloc[-30:-7].mean()) if len(series) >= 30 else avg

            # 變化率
            change_pct = ((recent_avg - older_avg) / max(older_avg, 1)) * 100

            # 趨勢標籤
            if change_pct > 50:
                trend_label = '🔥 搜尋暴增'
            elif change_pct > 20:
                trend_label = '📈 搜尋升溫'
            elif change_pct > -10:
                trend_label = '➡️ 搜尋持平'
            elif change_pct > -30:
                trend_label = '📉 搜尋降溫'
            else:
                trend_label = '❄️ 搜尋冷淡'

            # 相關搜尋 (best effort)
            related = []
            try:
                related_queries = pytrends.related_queries()
                if main_kw in related_queries:
                    top = related_queries[main_kw].get('top')
                    if top is not None and not top.empty:
                        related = top.head(5)['query'].tolist()
            except Exception:
                pass

            result = {
                'trend_df': trend_df,
                'keywords': keywords,
                'current_interest': current,
                'avg_interest': avg,
                'recent_avg': recent_avg,
                'change_pct': change_pct,
                'trend_label': trend_label,
                'related_queries': related,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'),
            }

            self._cache.set(cache_key, result)
            return result

        except ImportError as e:
            return self._empty_result(str(e))
        except Exception as e:
            logger.error(f"Google Trends error for {ticker}: {e}")
            return self._empty_result(f'取得失敗: {e}')

    def _build_keywords(self, ticker: str, stock_name: str = None) -> list:
        """建立搜尋關鍵字清單"""
        keywords = []

        # 台股
        if ticker.isdigit() or ticker.endswith('.TW'):
            stock_id = ticker.replace('.TW', '').replace('.TWO', '')

            # 優先使用股票名稱
            if stock_name:
                keywords.append(f'{stock_name}')
            elif stock_id in TW_STOCK_NAMES:
                keywords.append(TW_STOCK_NAMES[stock_id])
            else:
                keywords.append(f'{stock_id} 股票')

            # 加入代號作為第二關鍵字
            if stock_name or stock_id in TW_STOCK_NAMES:
                keywords.append(stock_id)

        # 美股
        else:
            ticker_clean = ticker.upper().strip()
            keywords.append(f'{ticker_clean} stock')

        # pytrends 最多 5 個關鍵字
        return keywords[:5]

    @staticmethod
    def _empty_result(error_msg: str = '') -> Dict:
        """回傳空結果"""
        return {
            'trend_df': pd.DataFrame(),
            'keywords': [],
            'current_interest': 0,
            'avg_interest': 0,
            'recent_avg': 0,
            'change_pct': 0,
            'trend_label': '無資料',
            'related_queries': [],
            'error': error_msg,
        }
