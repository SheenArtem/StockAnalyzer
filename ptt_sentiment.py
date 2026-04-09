
import logging
import time
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# PTT Stock board base URL
PTT_BASE_URL = "https://www.ptt.cc"
PTT_STOCK_BOARD = f"{PTT_BASE_URL}/bbs/Stock"

# 快取 TTL (秒) - PTT 情緒分析結果快取 30 分鐘
SENTIMENT_CACHE_TTL = 60 * 30

# 請求間隔 (秒) - 避免過度爬取
REQUEST_DELAY = 0.5

# 多空關鍵字
BULLISH_KEYWORDS = ['多', '買', '漲', '噴', '飆', '看好', '加碼', '進場', '抄底']
BEARISH_KEYWORDS = ['空', '賣', '跌', '崩', '套', '看壞', '出場', '停損', '逃命']


class PTTSentimentAnalyzer:
    """
    PTT Stock Board sentiment analyzer.

    Scrapes the PTT Stock board to gauge market/stock sentiment
    based on post titles and push/boo counts.
    """

    def __init__(self):
        self.session = requests.Session()
        # PTT Stock 板需要 over18 cookie
        self.session.cookies.set('over18', '1')
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36'
        })
        # 簡易快取: key -> (timestamp, data)
        self._cache = {}

    # ------------------------------------------------------------------
    # 內部工具方法
    # ------------------------------------------------------------------

    def _get_cached(self, cache_key):
        """Check cache and return data if still valid, else None."""
        if cache_key in self._cache:
            ts, data = self._cache[cache_key]
            if (datetime.now() - ts).total_seconds() < SENTIMENT_CACHE_TTL:
                logger.debug("Cache hit: %s", cache_key)
                return data
        return None

    def _set_cached(self, cache_key, data):
        """Store data in cache with current timestamp."""
        self._cache[cache_key] = (datetime.now(), data)

    def _fetch_page(self, url):
        """
        Fetch a single page with error handling and rate limiting.
        Returns BeautifulSoup object or None on failure.
        """
        try:
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return BeautifulSoup(resp.text, 'html.parser')
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

    def _get_latest_index(self):
        """
        取得 PTT Stock 板最新的 index 頁碼.

        解析「上頁」連結取得目前最新頁碼。
        """
        soup = self._fetch_page(f"{PTT_STOCK_BOARD}/index.html")
        if soup is None:
            return None

        # 尋找「上頁」按鈕
        btn_group = soup.find('div', class_='btn-group-paging')
        if btn_group:
            links = btn_group.find_all('a')
            for link in links:
                # 「上頁」的 href 格式: /bbs/Stock/index{N}.html
                if '上頁' in link.text:
                    href = link.get('href', '')
                    match = re.search(r'index(\d+)\.html', href)
                    if match:
                        # 上頁的頁碼 + 1 = 目前最新頁碼
                        return int(match.group(1)) + 1
        return None

    def _parse_index_page(self, page_num):
        """
        解析一頁 index, 回傳文章列表.

        Returns list of dicts: [{'title': str, 'url': str, 'date': str, 'author': str}]
        """
        url = f"{PTT_STOCK_BOARD}/index{page_num}.html"
        soup = self._fetch_page(url)
        if soup is None:
            return []

        posts = []
        entries = soup.find_all('div', class_='r-ent')
        for entry in entries:
            title_tag = entry.find('div', class_='title')
            if title_tag is None:
                continue
            a_tag = title_tag.find('a')
            if a_tag is None:
                # 文章可能已被刪除
                continue

            title = a_tag.text.strip()
            href = a_tag.get('href', '')
            post_url = f"{PTT_BASE_URL}{href}" if href else ''

            # 日期
            date_tag = entry.find('div', class_='date')
            date_str = date_tag.text.strip() if date_tag else ''

            # 作者
            author_tag = entry.find('div', class_='author')
            author = author_tag.text.strip() if author_tag else ''

            posts.append({
                'title': title,
                'url': post_url,
                'date': date_str,
                'author': author,
            })

        return posts

    def _parse_post_pushes(self, post_url):
        """
        解析單篇文章的推/噓/箭頭數量.

        Returns (push_count, boo_count, neutral_count)
        """
        soup = self._fetch_page(post_url)
        if soup is None:
            return 0, 0, 0

        push_count = 0
        boo_count = 0
        neutral_count = 0

        push_tags = soup.find_all('div', class_='push')
        for tag in push_tags:
            push_tag_elem = tag.find('span', class_='push-tag')
            if push_tag_elem is None:
                continue
            push_type = push_tag_elem.text.strip()
            if push_type == '\u63a8':  # 推
                push_count += 1
            elif push_type == '\u5653':  # 噓
                boo_count += 1
            else:
                neutral_count += 1

        return push_count, boo_count, neutral_count

    def _collect_index_posts(self, pages, latest_index):
        """
        從最新頁往前收集多頁文章列表.

        Returns list of post dicts.
        """
        all_posts = []
        for i in range(pages):
            page_num = latest_index - i
            if page_num < 1:
                break
            posts = self._parse_index_page(page_num)
            all_posts.extend(posts)
        return all_posts

    @staticmethod
    def _calc_sentiment_score(push, boo):
        """Calculate sentiment score from -100 to +100."""
        return ((push - boo) / (push + boo + 1)) * 100

    @staticmethod
    def _score_to_label(score):
        """Convert sentiment score to Chinese label."""
        if score >= 60:
            return '\u6975\u5ea6\u770b\u591a'   # 極度看多
        elif score >= 20:
            return '\u770b\u591a'               # 看多
        elif score >= -20:
            return '\u4e2d\u6027'               # 中性
        elif score >= -60:
            return '\u770b\u7a7a'               # 看空
        else:
            return '\u6975\u5ea6\u770b\u7a7a'   # 極度看空

    # ------------------------------------------------------------------
    # 公開 API
    # ------------------------------------------------------------------

    def get_stock_sentiment(self, stock_id, pages=3):
        """
        Analyze PTT sentiment for a specific stock.

        Args:
            stock_id: Stock ticker, e.g. '2330'
            pages: Number of index pages to scan (each ~20 posts)

        Returns:
            dict with sentiment analysis results, or default empty result on error.
        """
        cache_key = f"stock_sentiment_{stock_id}_{pages}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # 預設回傳值 (錯誤或無資料時使用)
        default_result = {
            'stock_id': stock_id,
            'total_posts': 0,
            'total_push': 0,
            'total_boo': 0,
            'push_ratio': 0.0,
            'sentiment_score': 0.0,
            'sentiment_label': '\u4e2d\u6027',  # 中性
            'recent_posts': [],
            'contrarian_warning': False,
        }

        latest_index = self._get_latest_index()
        if latest_index is None:
            logger.error("Unable to determine latest PTT Stock index page")
            return default_result

        logger.info("Scanning PTT Stock board for stock_id=%s, pages=%d, latest_index=%d",
                     stock_id, pages, latest_index)

        all_posts = self._collect_index_posts(pages, latest_index)

        # 篩選標題中包含 stock_id 的文章
        matched_posts = [
            p for p in all_posts
            if stock_id in p['title']
        ]

        if not matched_posts:
            logger.info("No posts found mentioning stock_id=%s", stock_id)
            self._set_cached(cache_key, default_result)
            return default_result

        # 逐篇抓取推/噓數
        total_push = 0
        total_boo = 0
        recent_posts = []

        for post in matched_posts:
            if not post['url']:
                continue
            push, boo, _ = self._parse_post_pushes(post['url'])
            total_push += push
            total_boo += boo
            recent_posts.append({
                'title': post['title'],
                'date': post['date'],
                'push': push,
                'boo': boo,
            })

        # 計算情緒指標
        push_ratio = total_push / (total_push + total_boo) if (total_push + total_boo) > 0 else 0.0
        sentiment_score = self._calc_sentiment_score(total_push, total_boo)
        sentiment_label = self._score_to_label(sentiment_score)

        # 擦鞋童效應: 過度看多時觸發反向警示
        contrarian_warning = push_ratio > 0.85 and (total_push + total_boo) >= 5

        result = {
            'stock_id': stock_id,
            'total_posts': len(matched_posts),
            'total_push': total_push,
            'total_boo': total_boo,
            'push_ratio': round(push_ratio, 4),
            'sentiment_score': round(sentiment_score, 2),
            'sentiment_label': sentiment_label,
            'recent_posts': recent_posts,
            'contrarian_warning': contrarian_warning,
        }

        self._set_cached(cache_key, result)
        return result

    def get_market_sentiment(self, pages=2):
        """
        Analyze overall market sentiment from PTT Stock board.

        Counts bullish/bearish keywords in recent post titles.

        Args:
            pages: Number of index pages to scan

        Returns:
            dict with market-level sentiment results.
        """
        cache_key = f"market_sentiment_{pages}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        default_result = {
            'bullish_count': 0,
            'bearish_count': 0,
            'sentiment_ratio': 0.5,
            'sentiment_score': 0.0,
            'sentiment_label': '\u4e2d\u6027',  # 中性
            'sample_size': 0,
        }

        latest_index = self._get_latest_index()
        if latest_index is None:
            logger.error("Unable to determine latest PTT Stock index page")
            return default_result

        logger.info("Scanning PTT Stock board for market sentiment, pages=%d", pages)

        all_posts = self._collect_index_posts(pages, latest_index)
        if not all_posts:
            return default_result

        bullish_count = 0
        bearish_count = 0

        for post in all_posts:
            title = post['title']
            for kw in BULLISH_KEYWORDS:
                if kw in title:
                    bullish_count += 1
            for kw in BEARISH_KEYWORDS:
                if kw in title:
                    bearish_count += 1

        total_kw = bullish_count + bearish_count
        sentiment_ratio = bullish_count / total_kw if total_kw > 0 else 0.5
        # 將 ratio (0~1) 映射到 score (-100~+100)
        sentiment_score = (sentiment_ratio - 0.5) * 200
        sentiment_label = self._score_to_label(sentiment_score)

        result = {
            'bullish_count': bullish_count,
            'bearish_count': bearish_count,
            'sentiment_ratio': round(sentiment_ratio, 4),
            'sentiment_score': round(sentiment_score, 2),
            'sentiment_label': sentiment_label,
            'sample_size': len(all_posts),
        }

        self._set_cached(cache_key, result)
        return result

    def get_mention_volume(self, stock_id, pages=5):
        """
        Count mention volume of a stock on PTT Stock board.

        Args:
            stock_id: Stock ticker, e.g. '2330'
            pages: Number of index pages to scan

        Returns:
            dict with mention count, relative volume, and trending flag.
        """
        cache_key = f"mention_volume_{stock_id}_{pages}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        default_result = {
            'mentions': 0,
            'relative_volume': 0.0,
            'is_trending': False,
        }

        latest_index = self._get_latest_index()
        if latest_index is None:
            logger.error("Unable to determine latest PTT Stock index page")
            return default_result

        logger.info("Counting mention volume for stock_id=%s, pages=%d", stock_id, pages)

        all_posts = self._collect_index_posts(pages, latest_index)
        if not all_posts:
            return default_result

        # 計算目標股票提及次數
        mentions = sum(1 for p in all_posts if stock_id in p['title'])

        # 基準線: 收集所有出現的台股代號, 計算平均提及次數
        # 台股代號通常是 4 位數字
        stock_id_pattern = re.compile(r'\b(\d{4})\b')
        stock_mention_counts = {}
        for post in all_posts:
            found_ids = stock_id_pattern.findall(post['title'])
            for sid in found_ids:
                stock_mention_counts[sid] = stock_mention_counts.get(sid, 0) + 1

        # 計算平均提及次數 (排除提及次數為 0 的)
        if stock_mention_counts:
            baseline = sum(stock_mention_counts.values()) / len(stock_mention_counts)
        else:
            baseline = 1.0

        # 避免除以零
        baseline = max(baseline, 0.1)
        relative_volume = mentions / baseline
        is_trending = relative_volume > 3.0

        result = {
            'mentions': mentions,
            'relative_volume': round(relative_volume, 2),
            'is_trending': is_trending,
        }

        self._set_cached(cache_key, result)
        return result


if __name__ == '__main__':
    # 設定 logging 方便除錯
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    analyzer = PTTSentimentAnalyzer()

    print("=" * 60)
    print("PTT Stock Board Sentiment Analyzer - Test")
    print("=" * 60)

    # 測試 1: 個股情緒 (2330 台積電)
    print("\n[Test 1] get_stock_sentiment('2330', pages=2)")
    print("-" * 40)
    result = analyzer.get_stock_sentiment('2330', pages=2)
    print(f"  Total posts: {result['total_posts']}")
    print(f"  Push: {result['total_push']}, Boo: {result['total_boo']}")
    print(f"  Push ratio: {result['push_ratio']}")
    print(f"  Sentiment score: {result['sentiment_score']}")
    print(f"  Sentiment label: {result['sentiment_label']}")
    print(f"  Contrarian warning: {result['contrarian_warning']}")
    if result['recent_posts']:
        print(f"  Recent posts ({len(result['recent_posts'])}):")
        for p in result['recent_posts'][:3]:
            print(f"    - [{p['date']}] {p['title']} (push:{p['push']} boo:{p['boo']})")

    # 測試 2: 大盤情緒
    print("\n[Test 2] get_market_sentiment(pages=2)")
    print("-" * 40)
    market = analyzer.get_market_sentiment(pages=2)
    print(f"  Bullish count: {market['bullish_count']}")
    print(f"  Bearish count: {market['bearish_count']}")
    print(f"  Sentiment ratio: {market['sentiment_ratio']}")
    print(f"  Sentiment score: {market['sentiment_score']}")
    print(f"  Sentiment label: {market['sentiment_label']}")
    print(f"  Sample size: {market['sample_size']}")

    # 測試 3: 提及量
    print("\n[Test 3] get_mention_volume('2330', pages=3)")
    print("-" * 40)
    volume = analyzer.get_mention_volume('2330', pages=3)
    print(f"  Mentions: {volume['mentions']}")
    print(f"  Relative volume: {volume['relative_volume']}")
    print(f"  Is trending: {volume['is_trending']}")

    print("\n" + "=" * 60)
    print("Test complete.")
