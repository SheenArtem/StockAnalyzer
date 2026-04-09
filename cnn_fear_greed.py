"""
CNN Fear & Greed Index 模組 (美股市場情緒)

功能:
1. 取得 CNN Fear & Greed Index 當前分數
2. 歷史走勢 (1週/1月/1年)
3. 各子指標分數

數據來源: CNN Business (production.dataviz.cnn.io)
無需 API Key
"""

import logging
import time
from datetime import datetime
from typing import Dict, Optional, Any

import requests

logger = logging.getLogger(__name__)

CACHE_TTL = 30 * 60  # 30 分鐘

CNN_FG_URL = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'

CNN_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Referer': 'https://edition.cnn.com/markets/fear-and-greed',
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


class CNNFearGreedIndex:
    """
    CNN Fear & Greed Index
    0 = Extreme Fear → 100 = Extreme Greed
    """

    def __init__(self):
        self._cache = _SimpleCache()

    def get_index(self) -> Dict:
        """
        取得 CNN Fear & Greed Index

        Returns:
            dict: score, label, previous_close, one_week_ago, one_month_ago,
                  one_year_ago, components (dict of sub-indicators)
        """
        cached = self._cache.get('cnn_fg')
        if cached is not None:
            return cached

        try:
            # CNN 提供一個 JSON endpoint
            resp = requests.get(CNN_FG_URL, headers=CNN_HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # 解析主分數
            fear_greed = data.get('fear_and_greed', {})
            score = fear_greed.get('score', 50)
            rating = fear_greed.get('rating', 'Neutral')
            previous = fear_greed.get('previous_close', score)
            one_week = fear_greed.get('previous_1_week', score)
            one_month = fear_greed.get('previous_1_month', score)
            one_year = fear_greed.get('previous_1_year', score)

            # 解析子指標
            components = {}
            indicator_keys = [
                ('market_momentum_sp500', '市場動能 (S&P 500)'),
                ('stock_price_strength', '股價強度'),
                ('stock_price_breadth', '漲跌家數廣度'),
                ('put_call_options', 'Put/Call 選擇權'),
                ('market_volatility_vix', 'VIX 波動率'),
                ('junk_bond_demand', '垃圾債需求'),
                ('safe_haven_demand', '避險資產需求'),
            ]

            for key, label in indicator_keys:
                ind = data.get(key, {})
                if ind:
                    components[label] = {
                        'score': ind.get('score', None),
                        'rating': ind.get('rating', 'N/A'),
                    }

            # 標籤翻譯
            label_map = {
                'Extreme Fear': '極度恐懼',
                'Fear': '恐懼',
                'Neutral': '中性',
                'Greed': '貪婪',
                'Extreme Greed': '極度貪婪',
            }

            result = {
                'score': score,
                'label': label_map.get(rating, rating),
                'rating_en': rating,
                'previous_close': previous,
                'one_week_ago': one_week,
                'one_month_ago': one_month,
                'one_year_ago': one_year,
                'components': components,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'),
            }

            self._cache.set('cnn_fg', result)
            return result

        except Exception as e:
            logger.error(f"CNN Fear & Greed fetch error: {e}")
            return {
                'score': None,
                'label': '無法取得',
                'rating_en': 'Error',
                'previous_close': None,
                'one_week_ago': None,
                'one_month_ago': None,
                'one_year_ago': None,
                'components': {},
                'error': str(e),
            }

    @staticmethod
    def get_label(score: float) -> str:
        """根據分數回傳情緒標籤"""
        if score is None:
            return '無資料'
        if score < 25:
            return '極度恐懼'
        if score < 45:
            return '恐懼'
        if score < 55:
            return '中性'
        if score < 75:
            return '貪婪'
        return '極度貪婪'

    @staticmethod
    def get_color(score: float) -> str:
        """根據分數回傳色碼"""
        if score is None:
            return '#888888'
        if score < 25:
            return '#FF4444'
        if score < 45:
            return '#FF8800'
        if score < 55:
            return '#FFD700'
        if score < 75:
            return '#88CC00'
        return '#00CC44'
