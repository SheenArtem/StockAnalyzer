"""
新聞搜尋模組 — 從 Google News RSS 抓取股票相關新聞

用於 AI 研究報告 Phase 2，提供近期新聞標題+摘要給 Claude 做質化分析。
"""

import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from html import unescape

import requests

logger = logging.getLogger(__name__)

_CACHE = {}
_CACHE_TTL = 1800  # 30 minutes


def _cache_get(key):
    if key in _CACHE:
        data, ts = _CACHE[key]
        if time.time() - ts < _CACHE_TTL:
            return data
    return None


def _cache_set(key, data):
    _CACHE[key] = (data, time.time())


def _clean_html(text):
    """Remove HTML tags and decode entities."""
    if not text:
        return ''
    text = unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


def _parse_rss_date(date_str):
    """Parse RSS pubDate to datetime."""
    if not date_str:
        return None
    # Format: "Sun, 12 Apr 2026 07:00:00 GMT"
    for fmt in ['%a, %d %b %Y %H:%M:%S %Z', '%a, %d %b %Y %H:%M:%S %z']:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def fetch_stock_news(ticker, stock_name='', max_items=15, days=7):
    """
    Fetch recent news for a stock from Google News RSS.

    Args:
        ticker: Stock ticker (e.g. '2330', 'AAPL')
        stock_name: Stock name for better search (e.g. '台積電')
        max_items: Maximum news items to return
        days: Only include news from last N days

    Returns:
        list of dict: [{title, source, date, summary}, ...]
    """
    cache_key = f"news_{ticker}_{days}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    is_us = not ticker.replace('.TW', '').replace('.TWO', '').isdigit()

    # Build search query
    if is_us:
        query = f'{ticker} stock'
        url = f'https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en'
    else:
        # Taiwan stock: use ticker + name
        stock_id = ticker.replace('.TW', '').replace('.TWO', '')
        if stock_name:
            query = f'{stock_id} {stock_name}'
        else:
            query = stock_id
        url = f'https://news.google.com/rss/search?q={query}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant'

    try:
        resp = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        })
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Google News fetch failed for %s: %s", ticker, e)
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        logger.warning("RSS parse failed: %s", e)
        return []

    items = root.findall('.//item')
    cutoff = datetime.now() - timedelta(days=days)
    results = []

    for item in items:
        if len(results) >= max_items:
            break

        title = _clean_html(item.findtext('title', ''))
        pub_date = item.findtext('pubDate', '')
        description = _clean_html(item.findtext('description', ''))
        source = item.findtext('source', '')

        # Parse and filter by date
        dt = _parse_rss_date(pub_date)
        if dt and dt.replace(tzinfo=None) < cutoff:
            continue

        # Skip generic/useless results
        skip_keywords = ['股價', '個股概覽', 'Stock Quote', 'Price and Forecast',
                         '爆料同學會', '股市同學會']
        if any(kw in title for kw in skip_keywords):
            continue

        results.append({
            'title': title,
            'source': source,
            'date': dt.strftime('%Y-%m-%d') if dt else pub_date[:16],
            'summary': description[:200] if description else '',
        })

    logger.info("Fetched %d news items for %s", len(results), ticker)
    _cache_set(cache_key, results)
    return results


def format_news_for_prompt(news_items, max_chars=3000):
    """
    Format news items into a text block for AI prompt.

    Args:
        news_items: list from fetch_stock_news()
        max_chars: Maximum total characters

    Returns:
        str: Formatted news text
    """
    if not news_items:
        return "N/A (no recent news found)"

    lines = []
    total = 0
    for n in news_items:
        line = f"[{n['date']}] {n['title']}"
        if n['source']:
            line += f" — {n['source']}"
        if n['summary']:
            line += f"\n  {n['summary']}"

        if total + len(line) > max_chars:
            break
        lines.append(line)
        total += len(line)

    return "\n".join(lines)


# CLI test
if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')

    ticker = sys.argv[1] if len(sys.argv) > 1 else '2330'
    name = sys.argv[2] if len(sys.argv) > 2 else ''

    print(f"Fetching news for {ticker} {name}...")
    news = fetch_stock_news(ticker, stock_name=name)
    print(f"\nFound {len(news)} items:\n")
    print(format_news_for_prompt(news))
