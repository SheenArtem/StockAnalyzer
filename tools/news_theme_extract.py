"""News theme discovery (2026-05-01 Day 2 production)

抓 Google News RSS（用 theme/sector 關鍵字查），餵 Claude Sonnet 萃取
(theme, ticker_mentions, sentiment, tone, confidence) 結構化輸出，
並 append 進 `data/news_themes.parquet` 供 Layer 4 _theme_tags_short 讀。

LLM 規範 (CLAUDE.md): Claude Sonnet `--model sonnet` + 600s timeout

Pipeline:
1. 抓 ~10 個 catalyst-driven theme query × Google News RSS
2. dedupe by title across queries
3. batch 1 次 LLM call → JSON 結構化輸出
4. 寫 daily JSON `data_cache/news_theme_pop/YYYYMMDD.json` (raw debug)
5. append 進 `data/news_themes.parquet` (Layer 4 用，30 天 TTL)

CLI:
    python tools/news_theme_extract.py            # 完整跑 + parquet append
    python tools/news_theme_extract.py --dry-run  # 只抓不解析 (debug)
    python tools/news_theme_extract.py --no-aggregate  # 解析但不寫 parquet
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
AGG_PATH = REPO / 'data' / 'news_themes.parquet'  # Layer 4 input
OUT_DIR.mkdir(parents=True, exist_ok=True)
AGG_PATH.parent.mkdir(parents=True, exist_ok=True)
NEWS_TTL_DAYS = 30  # parquet 只保留近 30 天，更舊的剃掉

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

# 經濟日報 (udn money) RSS direct categories (補深度報導)
# 2026-05-01 probe 結果：證券/產業/要聞 各 20 items 穩定，cnyes 無 public RSS,
# 工商時報 ctee.com.tw 403 Cloudflare block。
UDN_RSS_CATS = [
    ('udn_證券', 'https://money.udn.com/rssfeed/news/1001/5590'),
    ('udn_產業', 'https://money.udn.com/rssfeed/news/1001/5591'),
    ('udn_要聞', 'https://money.udn.com/rssfeed/news/1001/5589'),
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


def fetch_udn_rss(label: str, url: str, days: int = 7, max_items: int = 30) -> list[dict]:
    """經濟日報 (udn money) RSS direct."""
    try:
        resp = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        resp.raise_for_status()
    except Exception as e:
        logger.warning("UDN RSS %s 失敗: %s", url, e)
        return []
    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        logger.warning("UDN RSS parse failed %s: %s", url, e)
        return []

    cutoff = datetime.now() - timedelta(days=days)
    out = []
    for item in root.findall('.//item')[:max_items]:
        title = _clean_html(item.findtext('title', ''))
        pub_date_raw = item.findtext('pubDate', '')
        link = item.findtext('link', '')
        desc = _clean_html(item.findtext('description', ''))
        # parse date
        dt = None
        for fmt in ['%a, %d %b %Y %H:%M:%S %Z', '%a, %d %b %Y %H:%M:%S %z',
                    '%a, %d %b %Y %H:%M:%S GMT']:
            try:
                dt = datetime.strptime(pub_date_raw.strip(), fmt)
                break
            except ValueError:
                continue
        if dt and dt.replace(tzinfo=None) < cutoff:
            continue
        out.append({
            'query': label,
            'title': title,
            'source': '經濟日報',
            'date': dt.strftime('%Y-%m-%d') if dt else pub_date_raw[:16],
            'summary': desc[:300],
            'link': link,
        })
    return out


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


def aggregate_to_parquet(merged: list[dict]) -> dict:
    """Append today's merged articles into data/news_themes.parquet。

    Parquet schema:
      [date, source, ticker, theme, sentiment, tone, confidence,
       title (短), link]

    Each article 展開成 ticker × theme 的 multi-row。
    最後 dedupe by (date, ticker, theme, title) 並 trim 30 天 TTL。
    """
    import pandas as pd

    rows = []
    for a in merged:
        themes = a.get('themes') or []
        tickers = a.get('tickers') or []
        # 沒 ticker 的也保留 (theme-only)，ticker 設 ''；沒 theme 的整篇略過
        if not themes:
            continue
        if not tickers:
            tickers = ['']
        for t in tickers:
            for th in themes:
                rows.append({
                    'date': a.get('date'),
                    'source': a.get('source', ''),
                    'ticker': str(t),
                    'theme': str(th),
                    'sentiment': float(a.get('sentiment', 0.0) or 0.0),
                    'tone': str(a.get('tone', 'neutral')),
                    'confidence': int(a.get('confidence', 0) or 0),
                    'title': str(a.get('title', ''))[:200],
                    'link': str(a.get('link', '')),
                })

    new_df = pd.DataFrame(rows)
    if new_df.empty:
        return {'rows_added': 0, 'rows_total': 0}

    # 載既有 parquet
    if AGG_PATH.exists():
        try:
            existing = pd.read_parquet(AGG_PATH)
            combined = pd.concat([existing, new_df], ignore_index=True)
        except Exception as e:
            logger.warning("讀 %s 失敗，重建: %s", AGG_PATH, e)
            combined = new_df
    else:
        combined = new_df

    # dedupe + 30 天 TTL
    combined = combined.drop_duplicates(
        subset=['date', 'ticker', 'theme', 'title'], keep='last'
    )
    combined['date'] = pd.to_datetime(combined['date'], errors='coerce')
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=NEWS_TTL_DAYS)
    combined = combined[combined['date'] >= cutoff].copy()
    combined['date'] = combined['date'].dt.strftime('%Y-%m-%d')

    combined.to_parquet(AGG_PATH, index=False)
    return {
        'rows_added': len(new_df),
        'rows_total': len(combined),
        'tickers_total': combined['ticker'].nunique(),
        'themes_total': combined['theme'].nunique(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='只抓不解析 LLM')
    parser.add_argument('--no-aggregate', action='store_true',
                        help='解析但不 append 進 parquet (debug 用)')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--max-per-query', type=int, default=8)
    args = parser.parse_args()

    today = datetime.now().strftime('%Y%m%d')
    out_path = OUT_DIR / f'{today}.json'

    # 1a. 抓 Google News 所有 query
    all_articles = []
    seen_titles = set()
    for q in THEME_QUERIES:
        items = fetch_news_for_query(q, days=args.days, max_items=args.max_per_query)
        logger.info("[GoogleNews %s] %d articles", q, len(items))
        for a in items:
            if a['title'] in seen_titles:
                continue
            seen_titles.add(a['title'])
            all_articles.append(a)
        time.sleep(1)

    # 1b. 抓 經濟日報 RSS direct (補深度報導)
    for label, url in UDN_RSS_CATS:
        items = fetch_udn_rss(label, url, days=args.days)
        logger.info("[%s] %d articles (RSS direct)", label, len(items))
        for a in items:
            if a['title'] in seen_titles:
                continue
            seen_titles.add(a['title'])
            all_articles.append(a)
        time.sleep(0.5)

    logger.info("Total unique articles (Google + UDN): %d", len(all_articles))

    if args.dry_run:
        out_path = OUT_DIR / f'{today}_dry.json'
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
        (OUT_DIR / f'{today}_raw.txt').write_text(output, encoding='utf-8')
        sys.exit(1)

    extracted = parse_json_response(output)
    if extracted is None:
        logger.error("JSON parse failed, raw output saved")
        (OUT_DIR / f'{today}_raw.txt').write_text(output, encoding='utf-8')
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

    # 4b. aggregate to parquet
    if not args.no_aggregate:
        agg = aggregate_to_parquet(merged)
        logger.info("Aggregated to %s: +%d rows, total=%d, tickers=%d, themes=%d",
                    AGG_PATH.name, agg['rows_added'], agg['rows_total'],
                    agg.get('tickers_total', 0), agg.get('themes_total', 0))

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
