"""News theme discovery (2026-05-01)

抓 UDN money RSS direct + 鉅亨 cnyes JSON API（純專業財經媒體），餵 Claude
Sonnet 自由萃 (theme, ticker_mentions, sentiment, tone, confidence) 結構化輸出，
並 append 進 `data/news_themes.parquet` 供 Layer 4 _theme_tags_short 讀。

LLM 規範 (CLAUDE.md): Claude Sonnet `--model sonnet` + 600s timeout

設計原則 (2026-05-01 進化):
- 砍 Google News broad query — aggregator 雜訊，且我們可直連專業媒體
- UDN RSS direct (證券/產業/要聞 各 20 items) 提供 categorical TW finance stream
- cnyes JSON API (tw_stock_news + headline) 提供專業財經報導 +
  pre-tagged stock id + keyword（cnyes editor 已 tag 過）
- LLM 從原始流自由萃題材，看到什麼新題材就抽什麼

Pipeline:
1a. 抓 UDN money RSS direct 3 categories (證券/產業/要聞)
1b. 抓 cnyes JSON API 2 categories (tw_stock_news / headline)
2. dedupe by title across sources
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

# 經濟日報 (udn money) RSS direct
UDN_RSS_CATS = [
    ('udn_證券', 'https://money.udn.com/rssfeed/news/1001/5590'),
    ('udn_產業', 'https://money.udn.com/rssfeed/news/1001/5591'),
    ('udn_要聞', 'https://money.udn.com/rssfeed/news/1001/5589'),
]

# 鉅亨網 cnyes 公開 JSON API (2026-05-01 探到，無公開 doc 但無 auth)
# tw_stock_news: 520 篇 archive / headline: 1554 篇 archive
CNYES_API_CATS = [
    ('cnyes_台股新聞', 'tw_stock_news'),
    ('cnyes_頭條', 'headline'),
]
CNYES_API_LIMIT = 30  # 每 category 抓 30 篇

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


def fetch_cnyes_api(label: str, category: str, limit: int = 30, days: int = 7) -> list[dict]:
    """鉅亨 cnyes JSON API (非官方 endpoint，public 可訪)。

    返回 article 帶 cnyes editor pre-tagged stock + keyword fields，
    當作 LLM 萃 ticker 的 hint。
    """
    url = f'https://api.cnyes.com/media/api/v1/newslist/category/{category}'
    try:
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'},
                            params={'limit': limit}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("cnyes API %s 失敗: %s", category, e)
        return []
    items = data.get('items', {}).get('data', [])
    cutoff = datetime.now() - timedelta(days=days)
    out = []
    for it in items:
        ts = it.get('publishAt')
        dt = datetime.fromtimestamp(ts) if ts else None
        if dt and dt < cutoff:
            continue
        title = (it.get('title') or '').strip()
        summary = (it.get('summary') or '').strip()
        keywords = it.get('keyword') or []
        stock_hints = it.get('stock') or []
        if not title:
            continue
        # build summary 擴展含 cnyes pre-tagged keyword + stock 提示
        summary_aug = summary[:200]
        if stock_hints:
            summary_aug += f"\n[cnyes stock tags: {','.join(stock_hints)}]"
        if keywords:
            summary_aug += f"\n[cnyes keywords: {','.join(keywords[:8])}]"
        out.append({
            'query': label,
            'title': title,
            'source': '鉅亨網 cnyes',
            'date': dt.strftime('%Y-%m-%d') if dt else '',
            'summary': summary_aug[:400],
            'link': f'https://news.cnyes.com/news/id/{it.get("newsId", "")}',
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

    return f"""你是台股新聞題材分析員。我給你 {len(articles)} 篇 unfiltered 台股新聞 stream（不限特定題材），
你需要**自由萃取**每篇真實出現的 catalyst-driven 題材標籤 + 提到的 ticker。

對每篇新聞輸出 JSON object 含以下 fields：
- id: 文章編號 (Article 1, 2, ...)
- themes: list[str]，**0-3 個** catalyst-driven 題材中文標籤
  範例（不限於此清單，看到什麼新題材就抽什麼）：
    "AI 伺服器 ODM", "CoWoS 先進封裝", "ABF 載板", "矽光子", "CPO 共封裝光學",
    "AI 散熱", "AI PC SoC", "EV 供應鏈", "低軌衛星", "機器人", "高速傳輸",
    "ASIC 設計服務", "HBM", "Apple 蘋果供應鏈", "矽晶圓", "PCB 硬板",
    "量子運算", "AI 眼鏡", "車用功率半導體", "電動巴士", "工業機器人" 等
  **禁止**寫太泛 "AI" / "半導體" / "電子股" / "其他" / "權值股"。
  **若文章是大盤新聞 / 個股單純漲跌沒明確 catalyst** → themes 回 []
- tickers: list[str]，4 位數字台股 ticker（**只**從文章字面明確提到的）。
  **禁止**從題材推測（例如看到 ABF 不要自己補 8046/3037），只抓文章寫出來的。
  沒明確提到就回 []
- sentiment: float -1.0 ~ +1.0，文章對該題材的情緒（正面利多 / 負面利空 / 中性）
- tone: str ("bullish" / "bearish" / "neutral")
- confidence: int 0-100，**你對 themes + tickers 萃取的信心**（高信心 >= 80）

輸出格式：JSON array `[{{"id": 1, "themes": [...], "tickers": [...], ...}}, ...]`，
**只輸出 JSON**，不要加 markdown fence、不要加說明文字。

新聞清單：

{articles_text}
"""


def call_claude_sonnet(prompt: str) -> tuple[str, str | None]:
    """呼叫 Claude CLI（per LLM 規範用 sonnet + 10 min timeout）。

    Windows 平台用 stdin pipe 傳長 prompt (26KB+) 在 shell=True 時會被截
    （symptom: 開頭 5-10KB 的 model output 不見）。改用 stdin direct + shell=False。
    用 --output-format json 拿包含 result field 的 envelope，更可靠的 parse。
    """
    cmd = [_CLAUDE_CLI, '-p', '--model', 'sonnet', '--output-format', 'json']
    try:
        result = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True, text=True, timeout=CLAUDE_TIMEOUT,
            encoding='utf-8', errors='replace',
            shell=False,
        )
    except subprocess.TimeoutExpired:
        return '', f'claude CLI timeout after {CLAUDE_TIMEOUT}s'
    except FileNotFoundError:
        return '', 'claude CLI not found'
    if result.returncode != 0:
        return result.stdout or '', f'claude exit {result.returncode}: {result.stderr[:300]}'

    # 解外層 envelope 拿 result field
    raw = result.stdout
    try:
        envelope = json.loads(raw)
        text = envelope.get('result', '')
        if envelope.get('is_error'):
            return text, f'claude is_error=true (api_error_status={envelope.get("api_error_status")})'
        return text, None
    except json.JSONDecodeError as e:
        logger.warning("Claude JSON envelope parse failed: %s, returning raw", e)
        return raw, None


def _find_matching_bracket(s: str, start: int) -> int:
    """從 s[start] 是 '[' 開始找匹配的 ']' (字串中 [] 不算 depth)。

    回傳匹配的 ] 位置，找不到回 -1。處理 string literal 內的 [] 不增減 depth。
    """
    if start >= len(s) or s[start] != '[':
        return -1
    depth = 0
    in_string = False
    escape_next = False
    for i in range(start, len(s)):
        c = s[i]
        if escape_next:
            escape_next = False
            continue
        if c == '\\':
            escape_next = True
            continue
        if c == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                return i
    return -1


def parse_json_response(output: str) -> list[dict] | None:
    """容錯 JSON parse (markdown fence / 前後文字 / 開頭被截掉)。"""
    s = output.strip()
    # strip markdown fences
    if s.startswith('```'):
        lines = s.split('\n')
        s = '\n'.join(lines[1:-1] if len(lines) >= 3 else lines)
        if s.startswith('json'):
            s = s[4:].lstrip()

    # 1) 正常 case: [{...}, {...}] — 用 bracket-matching 找正確結尾
    # (avoid rfind(']') misfiring on `[]` literals in trailing markdown)
    start = s.find('[')
    if start >= 0:
        end = _find_matching_bracket(s, start)
        if end > start:
            try:
                return json.loads(s[start:end + 1])
            except json.JSONDecodeError:
                pass

    # 2) 開頭截斷 case: 直接是 {"id":N,...},{"id":N+1,...},...]
    # 也處理 trailing comma / missing 開頭 [
    if s.startswith('{'):
        # 補開頭 [，若結尾沒 ] 也補上
        candidate = s
        if not candidate.endswith(']'):
            candidate = candidate.rstrip(', \n\t') + ']'
        candidate = '[' + candidate
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as e:
            logger.warning("JSON parse failed (recovery attempt): %s", e)

    logger.warning("JSON parse failed: 找不到 valid array")
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

    # 1a. 抓 經濟日報 RSS direct
    all_articles = []
    seen_titles = set()
    for label, url in UDN_RSS_CATS:
        items = fetch_udn_rss(label, url, days=args.days)
        logger.info("[%s] %d articles (RSS direct)", label, len(items))
        for a in items:
            if a['title'] in seen_titles:
                continue
            seen_titles.add(a['title'])
            all_articles.append(a)
        time.sleep(0.5)

    # 1b. 抓 cnyes API
    for label, cat in CNYES_API_CATS:
        items = fetch_cnyes_api(label, cat, limit=CNYES_API_LIMIT, days=args.days)
        logger.info("[%s] %d articles (cnyes API)", label, len(items))
        for a in items:
            if a['title'] in seen_titles:
                continue
            seen_titles.add(a['title'])
            all_articles.append(a)
        time.sleep(0.5)

    logger.info("Total unique articles (UDN + cnyes): %d", len(all_articles))

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

    # 2. batch send Claude Sonnet (N=20 per batch, fault-isolated)
    # Why batch=20: single big call (115 articles) hit Windows subprocess
    # stdout truncation (~5KB threshold). 20 articles → output ~2KB safely
    # under threshold, fault-isolated (1 batch fail doesn't kill all).
    BATCH_SIZE = 20
    elapsed_total = 0.0
    raw_outputs = []
    merged = []
    n_batches = (len(all_articles) + BATCH_SIZE - 1) // BATCH_SIZE
    failed_batches = []

    for batch_idx in range(n_batches):
        start = batch_idx * BATCH_SIZE
        batch = all_articles[start:start + BATCH_SIZE]
        prompt = build_extraction_prompt(batch)
        logger.info("Batch %d/%d (%d articles, prompt %d chars)...",
                    batch_idx + 1, n_batches, len(batch), len(prompt))
        t0 = time.time()
        output, err = call_claude_sonnet(prompt)
        elapsed = time.time() - t0
        elapsed_total += elapsed

        if err:
            logger.error("  Batch %d Claude error: %s", batch_idx + 1, err)
            failed_batches.append(batch_idx)
            raw_outputs.append({'batch': batch_idx, 'output': output, 'err': err})
            continue

        extracted = parse_json_response(output)
        if extracted is None:
            logger.error("  Batch %d JSON parse failed", batch_idx + 1)
            failed_batches.append(batch_idx)
            raw_outputs.append({'batch': batch_idx, 'output': output, 'err': 'parse_failed'})
            continue

        # Map batch local id (1..20) back to global article (start..start+20)
        for local_i, a in enumerate(batch, 1):
            match = next((e for e in extracted
                          if e.get('id') in (local_i, str(local_i),
                                             f'Article {local_i}')), None)
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
        logger.info("  Batch %d done (%.1fs, %d chars output, +%d merged)",
                    batch_idx + 1, elapsed, len(output),
                    sum(1 for e in extracted if e.get('id')))

        time.sleep(2)  # be nice between LLM calls

    elapsed = elapsed_total
    logger.info("All batches done (%.1fs total, %d/%d batches OK, %d articles merged)",
                elapsed_total, n_batches - len(failed_batches), n_batches, len(merged))

    if failed_batches:
        # 失敗的 batch raw 寫出 debug
        (OUT_DIR / f'{today}_failed_batches.json').write_text(
            json.dumps(raw_outputs, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        logger.warning("Failed batches: %s (raw saved)", failed_batches)

    if not merged:
        logger.error("No articles successfully extracted, abort")
        sys.exit(1)

    # 3. merge 已在 batch loop 內完成

    # 4. save
    out_path.write_text(
        json.dumps({
            'date': today,
            'n_articles_fetched': len(all_articles),
            'n_articles_extracted': len(merged),
            'sources': ['udn money RSS direct', 'cnyes JSON API'],
            'udn_categories': [c[0] for c in UDN_RSS_CATS],
            'cnyes_categories': [c[0] for c in CNYES_API_CATS],
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
