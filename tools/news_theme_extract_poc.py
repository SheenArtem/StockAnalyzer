"""News theme discovery Phase 1 POC (2026-05-01 Day 1-3)

抓 Google News RSS（用 theme/sector 關鍵字查），餵 Claude Sonnet 萃取
(theme, ticker_mentions, sentiment, tone, confidence) 結構化輸出。

LLM 規範 (CLAUDE.md):
- News 解析強制用 Claude Sonnet (`--model sonnet`) + 600s timeout
- 不用 Haiku (accuracy 不夠)，不用 Opus (浪費)

POC scope (single source + small batch):
- 抓 10 個熱門題材關鍵字 × 每查 8 篇 = 最多 80 篇 (近 7 天)
- dedupe by url
- batch 送 Claude Sonnet 一次性萃取 (避免 80 次 LLM call)
- 輸出 JSON `data_cache/news_theme_pop/poc_YYYYMMDD.json`
- 人工 audit 取樣 20 篇看 accuracy

驗收閘門 (Day 2 才繼續做):
- accuracy >= 60% strict (theme 命中 + ticker 不亂寫)
- 不過閘門 → abort，3 層題材融合維持

CLI:
    python tools/news_theme_extract_poc.py            # 跑 POC
    python tools/news_theme_extract_poc.py --dry-run  # 只抓不解析
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / 'data_cache' / 'news_theme_pop'
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 10 個 catalyst-driven theme query (cover 你目前 manual.json 主流 + 幾個 emerging)
THEME_QUERIES = [
    'AI 伺服器 台股',
    'CoWoS 先進封裝 台股',
    'ABF 載板 台股',
    '矽光子 CPO 台股',
    '高速傳輸 台股',
    'AI PC 台股',
    'EV 電動車 台股',
    'AI 散熱 液冷 台股',
    '機器人 台股',
    '低軌衛星 台股',
]

# Claude CLI
import shutil
_CLAUDE_CLI = shutil.which("claude") or "claude"
CLAUDE_TIMEOUT = 600  # 10 min per LLM 規範


def _clean_html(text: str) -> str:
    import re
    from html import unescape
    if not text:
        return ''
    text = unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


def fetch_news_for_query(query: str, days: int = 7, max_items: int = 8) -> list[dict]:
    """Google News RSS query (zh-TW)."""
    url = f'https://news.google.com/rss/search?q={query}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant'
    try:
        resp = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0',
        })
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Fetch %s 失敗: %s", query, e)
        return []
    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        logger.warning("RSS parse failed for %s: %s", query, e)
        return []

    cutoff = datetime.now() - timedelta(days=days)
    out = []
    for item in root.findall('.//item')[:max_items]:
        title = _clean_html(item.findtext('title', ''))
        pub_date_raw = item.findtext('pubDate', '')
        link = item.findtext('link', '')
        desc = _clean_html(item.findtext('description', ''))
        source = item.findtext('source', '')
        # parse date
        dt = None
        for fmt in ['%a, %d %b %Y %H:%M:%S %Z', '%a, %d %b %Y %H:%M:%S %z']:
            try:
                dt = datetime.strptime(pub_date_raw.strip(), fmt)
                break
            except ValueError:
                continue
        if dt and dt.replace(tzinfo=None) < cutoff:
            continue
        out.append({
            'query': query,
            'title': title,
            'source': source,
            'date': dt.strftime('%Y-%m-%d') if dt else pub_date_raw[:16],
            'summary': desc[:300],
            'link': link,
        })
    return out


def build_extraction_prompt(articles: list[dict]) -> str:
    """組 batch prompt 給 Claude Sonnet。"""
    article_blocks = []
    for i, a in enumerate(articles, 1):
        article_blocks.append(
            f"=== Article {i} ===\n"
            f"Query: {a['query']}\n"
            f"Date: {a['date']}\n"
            f"Source: {a['source']}\n"
            f"Title: {a['title']}\n"
            f"Summary: {a['summary']}\n"
        )
    articles_text = "\n".join(article_blocks)

    return f"""你是台股新聞題材分析員。我給你 {len(articles)} 篇新聞，請每一篇萃取結構化標籤。

對每篇新聞輸出 JSON object 含以下 fields：
- id: 文章編號 (Article 1, 2, ...)
- themes: list[str]，1-3 個 catalyst-driven 題材中文標籤（例: "AI 伺服器 ODM", "CoWoS 先進封裝", "矽光子", "ABF 載板", "EV 供應鏈", "AI 散熱"等）。**禁止**寫太泛的 "AI" / "半導體" / "其他"。**沒題材**就回 []
- tickers: list[str]，4 位數字台股 ticker（從文章內容明確提到的）。**禁止**亂猜，只列文章字面提到。沒明確提到就回 []
- sentiment: float -1.0 ~ +1.0，文章對該題材的情緒（正面利多 / 負面利空 / 中性）
- tone: str ("bullish" / "bearish" / "neutral")
- confidence: int 0-100，**你對 themes + tickers 萃取的信心**（高信心 >= 80）

輸出格式：JSON array `[{{"id": 1, "themes": [...], "tickers": [...], ...}}, ...]`，**只輸出 JSON**，不要加 markdown fence、不要加說明文字。

新聞清單：

{articles_text}
"""


def call_claude_sonnet(prompt: str) -> tuple[str, str | None]:
    """呼叫 Claude CLI（per LLM 規範用 sonnet + 10 min timeout）。"""
    try:
        result = subprocess.run(
            f'{_CLAUDE_CLI} -p --model sonnet --output-format text',
            input=prompt,
            capture_output=True, text=True, timeout=CLAUDE_TIMEOUT,
            encoding='utf-8', errors='replace',
            shell=True,
        )
    except subprocess.TimeoutExpired:
        return '', f'claude CLI timeout after {CLAUDE_TIMEOUT}s'
    if result.returncode != 0:
        return result.stdout or '', f'claude exit {result.returncode}: {result.stderr[:300]}'
    return result.stdout, None


def parse_json_response(output: str) -> list[dict] | None:
    """容錯 JSON parse (markdown fence / 前後文字)。"""
    s = output.strip()
    # strip markdown fences
    if s.startswith('```'):
        lines = s.split('\n')
        s = '\n'.join(lines[1:-1] if len(lines) >= 3 else lines)
        if s.startswith('json'):
            s = s[4:].lstrip()
    # find first [ and last ]
    start = s.find('[')
    end = s.rfind(']')
    if start < 0 or end < 0:
        return None
    try:
        return json.loads(s[start:end + 1])
    except json.JSONDecodeError as e:
        logger.warning("JSON parse failed: %s", e)
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='只抓不解析 LLM')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--max-per-query', type=int, default=8)
    args = parser.parse_args()

    today = datetime.now().strftime('%Y%m%d')
    out_path = OUT_DIR / f'poc_{today}.json'

    # 1. 抓所有 query
    all_articles = []
    seen_titles = set()
    for q in THEME_QUERIES:
        items = fetch_news_for_query(q, days=args.days, max_items=args.max_per_query)
        logger.info("[%s] %d articles", q, len(items))
        for a in items:
            if a['title'] in seen_titles:
                continue
            seen_titles.add(a['title'])
            all_articles.append(a)
        time.sleep(1)  # be nice to Google News

    logger.info("Total unique articles: %d", len(all_articles))

    if args.dry_run:
        out_path = OUT_DIR / f'poc_{today}_dry.json'
        out_path.write_text(
            json.dumps(all_articles, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        logger.info("Dry run saved to %s", out_path)
        return

    if not all_articles:
        logger.error("No articles fetched, abort")
        sys.exit(1)

    # 2. batch send Claude Sonnet
    prompt = build_extraction_prompt(all_articles)
    logger.info("Prompt size: %d chars, sending to Claude Sonnet (timeout %ds)...",
                len(prompt), CLAUDE_TIMEOUT)
    t0 = time.time()
    output, err = call_claude_sonnet(prompt)
    elapsed = time.time() - t0
    logger.info("Claude returned (%.1fs, %d chars)", elapsed, len(output))

    if err:
        logger.error("Claude error: %s", err)
        # 仍寫出 raw output 供 debug
        (OUT_DIR / f'poc_{today}_raw.txt').write_text(output, encoding='utf-8')
        sys.exit(1)

    extracted = parse_json_response(output)
    if extracted is None:
        logger.error("JSON parse failed, raw output saved")
        (OUT_DIR / f'poc_{today}_raw.txt').write_text(output, encoding='utf-8')
        sys.exit(1)

    # 3. merge: align extracted to articles by id
    merged = []
    for i, a in enumerate(all_articles, 1):
        match = next((e for e in extracted if e.get('id') in (i, str(i), f'Article {i}')), None)
        if not match:
            continue
        merged.append({
            **a,
            'themes': match.get('themes', []),
            'tickers': match.get('tickers', []),
            'sentiment': match.get('sentiment', 0.0),
            'tone': match.get('tone', 'neutral'),
            'confidence': match.get('confidence', 0),
        })

    # 4. save
    out_path.write_text(
        json.dumps({
            'date': today,
            'n_articles_fetched': len(all_articles),
            'n_articles_extracted': len(merged),
            'queries': THEME_QUERIES,
            'llm_model': 'claude sonnet',
            'elapsed_s': round(elapsed, 1),
            'articles': merged,
        }, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    logger.info("Saved %d/%d to %s", len(merged), len(all_articles), out_path)

    # 5. summary stats
    theme_counts: dict[str, int] = {}
    ticker_counts: dict[str, int] = {}
    for m in merged:
        for t in m.get('themes') or []:
            theme_counts[t] = theme_counts.get(t, 0) + 1
        for tk in m.get('tickers') or []:
            ticker_counts[tk] = ticker_counts.get(tk, 0) + 1

    logger.info("Top themes:")
    for t, n in sorted(theme_counts.items(), key=lambda x: -x[1])[:10]:
        logger.info("  %3d  %s", n, t)
    logger.info("Top tickers:")
    for tk, n in sorted(ticker_counts.items(), key=lambda x: -x[1])[:15]:
        logger.info("  %3d  %s", n, tk)


if __name__ == '__main__':
    main()
