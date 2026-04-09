
import logging
import time
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# PTT Stock board base URL
PTT_BASE_URL = "https://www.ptt.cc"
PTT_STOCK_BOARD = f"{PTT_BASE_URL}/bbs/Stock"

# 快取 TTL (秒) - PTT 情緒分析結果快取 30 分鐘
SENTIMENT_CACHE_TTL = 60 * 30

# 請求間隔 (秒) - PTT 可承受 0.3s 間隔
REQUEST_DELAY = 0.3

# 並行抓取的最大 worker 數量
MAX_WORKERS = 4

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

    @staticmethod
    def _build_search_terms(stock_id, stock_name=None):
        """
        建立完整的搜尋關鍵字清單.

        PTT 上對個股的常見稱呼方式：
        - 股票代號: 2330, 2330.TW
        - 全名: 台積電
        - 簡稱: 台積
        - 暱稱: GG (台積電專用)
        - 標的格式: [標的] 2330

        Returns:
            list of search terms (strings)
        """
        terms = [stock_id]

        # 加入 .TW 格式
        if stock_id.isdigit():
            terms.append(f"{stock_id}.TW")

        if stock_name and stock_name != stock_id:
            terms.append(stock_name)
            # 嘗試擷取簡稱 (去掉常見後綴: 電, 光, 化, etc.)
            # 至少保留 2 字元的前綴作為搜尋用
            if len(stock_name) >= 3:
                terms.append(stock_name[:2])

        # 知名個股暱稱 (hardcoded 常見映射)
        nickname_map = {
            '2330': ['GG'],
            '2317': ['鴻海'],
            '2454': ['聯發科', '發哥', 'MTK'],
            '3008': ['大立光'],
            '2412': ['中華電'],
            '2308': ['台達電', '台達'],
            '6505': ['台塑化'],
            '2002': ['中鋼'],
            '2882': ['國泰金'],
            '2881': ['富邦金'],
            '2884': ['玉山金'],
            '2886': ['兆豐金'],
            '2891': ['中信金'],
            '2303': ['聯電', 'UMC'],
            '3711': ['日月光'],
        }
        if stock_id in nickname_map:
            for nick in nickname_map[stock_id]:
                if nick not in terms:
                    terms.append(nick)

        return terms

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

    def _parse_post_pushes(self, post_url, return_body=False):
        """
        解析單篇文章的推/噓/箭頭數量, 可選回傳文章內文.

        Args:
            post_url: 文章完整 URL
            return_body: 是否一併回傳文章本文 (用於內容搜尋)

        Returns:
            若 return_body=False: (push_count, boo_count, neutral_count)
            若 return_body=True:  (push_count, boo_count, neutral_count, body_text)
        """
        soup = self._fetch_page(post_url)
        if soup is None:
            return (0, 0, 0, '') if return_body else (0, 0, 0)

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

        if return_body:
            # 擷取文章本文 (main-content div, 排除 meta-data 和推文)
            body_text = ''
            main_content = soup.find('div', id='main-content')
            if main_content:
                # 移除推文區塊和 metaline, 取得純文本
                # 直接操作 main_content (push 已計數完畢, 不再需要原始 soup)
                for unwanted in main_content.find_all(['div', 'span'],
                                                       class_=['push', 'article-metaline',
                                                               'article-metaline-right']):
                    unwanted.decompose()
                body_text = main_content.get_text(separator=' ', strip=True)
                # 只保留前 500 字 (搜尋用途, 不需全文)
                body_text = body_text[:500]
            return push_count, boo_count, neutral_count, body_text

        return push_count, boo_count, neutral_count

    def _collect_index_posts(self, pages, latest_index):
        """
        從最新頁往前收集多頁文章列表 (並行抓取).

        Returns list of post dicts.
        """
        page_nums = [latest_index - i for i in range(pages) if latest_index - i >= 1]

        all_posts = []
        # 並行抓取多頁 index (每頁之間無依賴關係)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {
                executor.submit(self._parse_index_page, pn): pn
                for pn in page_nums
            }
            results_by_page = {}
            for future in as_completed(future_to_page):
                pn = future_to_page[future]
                try:
                    results_by_page[pn] = future.result()
                except Exception as e:
                    logger.warning("Failed to parse index page %d: %s", pn, e)
                    results_by_page[pn] = []

        # 按頁碼降序排列 (最新在前)
        for pn in sorted(results_by_page.keys(), reverse=True):
            all_posts.extend(results_by_page[pn])

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

    def get_stock_sentiment(self, stock_id, pages=5, stock_name=None):
        """
        Analyze PTT sentiment for a specific stock.

        搜尋策略:
        1. 用完整搜尋關鍵字 (代號/名稱/暱稱) 比對標題 -> title_matched
        2. 標題含 [標的] 但未在 step 1 匹配的文章 -> 抓取本文檢查是否提及該股
        3. 合併兩者計算情緒

        Args:
            stock_id: Stock ticker, e.g. '2330'
            pages: Number of index pages to scan (each ~10 posts)
            stock_name: Stock name for broader search, e.g. '台積電'

        Returns:
            dict with sentiment analysis results, or default empty result on error.
        """
        cache_key = f"stock_sentiment_{stock_id}_{stock_name}_{pages}"
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

        # 建立完整搜尋關鍵字清單 (代號 + 名稱 + 暱稱 + 簡稱)
        search_terms = self._build_search_terms(stock_id, stock_name)

        logger.info("Scanning PTT Stock board for %s, pages=%d, latest_index=%d",
                     search_terms, pages, latest_index)

        all_posts = self._collect_index_posts(pages, latest_index)

        # Step 1: 標題匹配 — 任一搜尋關鍵字出現在標題中
        title_matched = []
        title_matched_urls = set()
        # Step 2 候選: 標題含 [標的] 但不在 title_matched 中的文章
        target_candidates = []

        for p in all_posts:
            title = p['title']
            if any(term in title for term in search_terms):
                title_matched.append(p)
                title_matched_urls.add(p['url'])
            elif re.search(r'\[\s*標的\s*\]', title):
                # [標的] 文章, 可能在本文中提及該股票
                target_candidates.append(p)

        logger.info("Title matched: %d, [biao-di] candidates: %d",
                     len(title_matched), len(target_candidates))

        # --- 並行抓取文章內容 ---
        total_push = 0
        total_boo = 0
        recent_posts = []

        # 用於收集所有要抓取的文章 (含 body 搜尋)
        def _fetch_title_matched(post):
            """抓取標題匹配文章的推噓數"""
            if not post['url']:
                return None
            push, boo, _ = self._parse_post_pushes(post['url'])
            return {'post': post, 'push': push, 'boo': boo, 'match_type': 'title'}

        def _fetch_and_check_body(post):
            """抓取 [標的] 文章的本文, 檢查是否提及目標股票"""
            if not post['url']:
                return None
            push, boo, _, body = self._parse_post_pushes(post['url'], return_body=True)
            # 在本文中搜尋股票代號或名稱
            if any(term in body for term in search_terms):
                return {'post': post, 'push': push, 'boo': boo, 'match_type': 'body'}
            return None

        # 並行抓取: 標題匹配 + [標的] 文章本文
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []

            # 提交標題匹配的文章
            for post in title_matched:
                futures.append(executor.submit(_fetch_title_matched, post))

            # 提交 [標的] 候選文章 (需要讀本文)
            for post in target_candidates:
                futures.append(executor.submit(_fetch_and_check_body, post))

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result is not None:
                        total_push += result['push']
                        total_boo += result['boo']
                        match_tag = '' if result['match_type'] == 'title' else ' [body]'
                        recent_posts.append({
                            'title': result['post']['title'] + match_tag,
                            'date': result['post']['date'],
                            'push': result['push'],
                            'boo': result['boo'],
                        })
                except Exception as e:
                    logger.warning("Error fetching post: %s", e)

        matched_count = len(recent_posts)

        if matched_count == 0:
            logger.info("No posts found mentioning stock_id=%s", stock_id)
            self._set_cached(cache_key, default_result)
            return default_result

        # 按日期排序 (最新在前)
        recent_posts.sort(key=lambda x: x['date'], reverse=True)

        # 計算情緒指標
        push_ratio = total_push / (total_push + total_boo) if (total_push + total_boo) > 0 else 0.0
        sentiment_score = self._calc_sentiment_score(total_push, total_boo)
        sentiment_label = self._score_to_label(sentiment_score)

        # 擦鞋童效應: 過度看多時觸發反向警示
        contrarian_warning = push_ratio > 0.85 and (total_push + total_boo) >= 5

        result = {
            'stock_id': stock_id,
            'total_posts': matched_count,
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

    def get_mention_volume(self, stock_id, pages=10, stock_name=None):
        """
        Count mention volume of a stock on PTT Stock board.

        搜尋策略: 標題匹配 (完整搜尋關鍵字) + [標的] 文章本文匹配.

        Args:
            stock_id: Stock ticker, e.g. '2330'
            pages: Number of index pages to scan
            stock_name: Stock name for broader search, e.g. '台積電'

        Returns:
            dict with mention count, relative volume, and trending flag.
        """
        cache_key = f"mention_volume_{stock_id}_{stock_name}_{pages}"
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

        # 建立完整搜尋關鍵字
        search_terms = self._build_search_terms(stock_id, stock_name)

        logger.info("Counting mention volume for %s, pages=%d", search_terms, pages)

        all_posts = self._collect_index_posts(pages, latest_index)
        if not all_posts:
            return default_result

        # 計算標題匹配的提及次數
        title_mentions = 0
        target_candidates = []
        for p in all_posts:
            title = p['title']
            if any(term in title for term in search_terms):
                title_mentions += 1
            elif re.search(r'\[\s*標的\s*\]', title) and p['url']:
                target_candidates.append(p)

        # 並行抓取 [標的] 文章本文, 檢查是否提及目標股票
        body_mentions = 0
        if target_candidates:
            def _check_body(post):
                _, _, _, body = self._parse_post_pushes(post['url'], return_body=True)
                return any(term in body for term in search_terms)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(_check_body, p): p for p in target_candidates}
                for future in as_completed(futures):
                    try:
                        if future.result():
                            body_mentions += 1
                    except Exception as e:
                        logger.warning("Error checking post body: %s", e)

        mentions = title_mentions + body_mentions
        logger.info("Mention volume: title=%d, body=%d, total=%d",
                     title_mentions, body_mentions, mentions)

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
    print("\n[Test 1] get_stock_sentiment('2330', pages=5, stock_name='tai-ji-dian')")
    print("-" * 40)
    result = analyzer.get_stock_sentiment('2330', pages=5, stock_name='\u53f0\u7a4d\u96fb')
    print(f"  Total posts: {result['total_posts']}")
    print(f"  Push: {result['total_push']}, Boo: {result['total_boo']}")
    print(f"  Push ratio: {result['push_ratio']}")
    print(f"  Sentiment score: {result['sentiment_score']}")
    print(f"  Sentiment label: {result['sentiment_label']}")
    print(f"  Contrarian warning: {result['contrarian_warning']}")
    if result['recent_posts']:
        print(f"  Recent posts ({len(result['recent_posts'])}):")
        for p in result['recent_posts'][:5]:
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
    print("\n[Test 3] get_mention_volume('2330', pages=5, stock_name='tai-ji-dian')")
    print("-" * 40)
    volume = analyzer.get_mention_volume('2330', pages=5, stock_name='\u53f0\u7a4d\u96fb')
    print(f"  Mentions: {volume['mentions']}")
    print(f"  Relative volume: {volume['relative_volume']}")
    print(f"  Is trending: {volume['is_trending']}")

    print("\n" + "=" * 60)
    print("Test complete.")
