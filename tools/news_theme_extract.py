"""News theme discovery (2026-05-01)

抓 UDN money RSS direct + 鉅亨 cnyes JSON API（純專業財經媒體），餵 Claude
Sonnet 自由萃 (theme, ticker_mentions, sentiment, tone, confidence) 結構化輸出。

LLM 規範 (CLAUDE.md): Claude Sonnet `--model sonnet` + 600s timeout

Storage 三層架構 (News Initiative 2026-05 Phase 0 Commit 1, dual-write 過渡期):
1. **Archive (cold, 永久 SoT)**: `data_cache/news_archive/YYYY-MM/articles.parquet`
   - partition key = article publish_date (BLOCKER #4)
   - append-only with atomic swap (.tmp → os.replace + retry, BLOCKER #3)
   - schema 加 extract_version=1 (BLOCKER #2 prerequisite)
2. **Legacy parquet (1 週 dual-write 過渡期)**: `data/news_themes.parquet`
   - 既有 5 處 reader 暫時繼續讀 (market_sentiment / ai_report / ui_helpers / ...)
   - 30 天 TTL 維持
   - Commit 6 整體 cutover 後才下線
3. **Daily JSON debug**: `data_cache/news_theme_pop/YYYYMMDD.json` (永久保留, 第四輪)

Pipeline:
1a. 抓 UDN money RSS direct 3 categories (證券/產業/要聞)
1b. 抓 cnyes JSON API 2 categories (tw_stock_news / headline)
2. dedupe by title across sources (Commit 4 後改 archive 不 dedupe)
3. N=20 batch split → 多次 LLM call (fault-isolated)
4. 寫 daily JSON debug
5. append 進 archive (新 SoT) + legacy parquet (dual write)

CLI:
    python tools/news_theme_extract.py             # 完整跑 + dual-write
    python tools/news_theme_extract.py --dry-run   # 只抓不解析 (debug)
    python tools/news_theme_extract.py --no-aggregate  # 解析但不寫 parquet
    python tools/news_theme_extract.py --migrate-legacy  # 一次性 backfill legacy parquet 進 archive
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
AGG_PATH = REPO / 'data' / 'news_themes.parquet'  # legacy (Layer 4 input, dual-write 1 週過渡)
ARCHIVE_DIR = REPO / 'data_cache' / 'news_archive'  # 新 SoT (永久, 按月 partition)
NEW_NEWS_DIR = REPO / 'data' / 'news'  # 新 hot view + derived parquet 目錄 (Commit 5+)
OUT_DIR.mkdir(parents=True, exist_ok=True)
AGG_PATH.parent.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
NEW_NEWS_DIR.mkdir(parents=True, exist_ok=True)
NEWS_TTL_DAYS = 30  # legacy parquet 仍 30 天 TTL；archive 永久不 trim
EXTRACT_VERSION = 1  # BLOCKER #2 prerequisite, bump 時必 backfill 舊 partition default

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


# BLOCKER #1 (Commit 2): dedupe key + event_id helpers
def normalize_title_hash(title: str) -> str:
    """md5(strip_punct(lower(title)))[:16]. Empty title → ''.

    Strips all whitespace + punctuation + 全形/半形 標點 + lowercase before
    hashing，使 cnyes / UDN 同事件不同標點變體歸為同一 hash.
    """
    import hashlib
    import re
    if not title:
        return ''
    # \W matches non-word; _ explicit; CJK chars are word chars (kept)
    norm = re.sub(r'[\s\W_]+', '', title.lower())
    if not norm:
        return ''
    return hashlib.md5(norm.encode('utf-8')).hexdigest()[:16]


def compute_event_id(title_hash: str, date_str: str) -> str:
    """event_id = '{title_hash}_{YYYY-MM-DD}' for clustering same-event reposts.

    同 hash + 同日 → 同 event_id（cnyes + UDN 同事件不同 source 歸為同一 event）。
    限制：跨午夜 < 24h 但跨日的 repost 會被當作不同 event（trade-off for batch
    processing 簡單性，之後若需要可改 sliding window）。
    """
    if not title_hash or not date_str:
        return ''
    return f"{title_hash}_{date_str[:10]}"


def dedupe_by_event_id(df, keep: str = 'first'):
    """Dedupe rows with same (event_id, ticker, theme), keep first source.

    給 derived rebuild (themes_core / market_sentiment / etc.) 用。
    Empty event_id rows 不 dedupe（legacy 資料兼容）。
    """
    import pandas as pd
    if df is None or len(df) == 0 or 'event_id' not in df.columns:
        return df
    has_id = df['event_id'].astype(str).str.len() > 0
    if not has_id.any():
        return df
    deduped = df[has_id].drop_duplicates(
        subset=['event_id', 'ticker', 'theme'], keep=keep
    )
    no_id = df[~has_id]
    return pd.concat([deduped, no_id], ignore_index=True)


def _clean_html(text: str) -> str:
    import re
    from html import unescape
    if not text:
        return ''
    text = unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


# Commit 4 (BLOCKER #5 archive 全文存): truncate cap
BODY_FULL_MAX_CHARS = 1500  # Legal F2 fair use 邊界 + Quant re-extract 需求平衡


def _truncate_body(text: str, max_chars: int = BODY_FULL_MAX_CHARS) -> str:
    """Strip HTML + unescape entities + truncate to max_chars."""
    if not text:
        return ''
    cleaned = _clean_html(text)
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[:max_chars]


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
            # Commit 4: body_full archive (UDN RSS 只給 description, 真全文需 GET HTML page,
            # 先用 description as body_full + 標 status='summary_only' fallback,
            # Phase 1+ 視需要加 GET HTML fetcher)
            'body_full': _truncate_body(desc),
            'body_status': 'summary_only',
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
        content = (it.get('content') or '').strip()  # Commit 4: 全文 (HTML escaped)
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
        # Commit 4 (BLOCKER #5): body_full from cnyes content field (≤1500 chars)
        # cnyes 'content' is HTML-escaped (e.g. &lt;p&gt;), needs unescape + strip
        if content:
            body_full = _truncate_body(content)
            body_status = 'cnyes_content'
        else:
            body_full = _truncate_body(summary)
            body_status = 'summary_only'
        out.append({
            'query': label,
            'title': title,
            'source': '鉅亨網 cnyes',
            'date': dt.strftime('%Y-%m-%d') if dt else '',
            'summary': summary_aug[:400],
            'link': f'https://news.cnyes.com/news/id/{it.get("newsId", "")}',
            'body_full': body_full,
            'body_status': body_status,
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
    """組 batch prompt 給 Claude Sonnet (4 類分流, Phase 0 Commit 3)。

    BLOCKER #3 macro/sector/price_only 不再丟棄；Council AIA F1 警告 single-prompt
    4-class accuracy 降，先 baseline + dry-run 驗 confusion matrix 再決定要不要拆兩階段。
    """
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

    return f"""你是台股新聞分析員。我給你 {len(articles)} 篇 unfiltered 台股新聞 stream，
你需要**先分類** article_type，再依類別抽相應欄位。

## 第一步：classify article_type 為下列 4 類之一

- **individual**: 有具體個股 catalyst（公司動態 / 法說會 / 新合作 / 擴產 / 砍單 /
  CoWoS / ABF / EV 供應鏈 等明確題材 + 文章字面提到 4 位數字 ticker）
- **sector**: 產業整體動向（半導體類股集體 / 金融類股走強 / 航運運價 / 觀光復甦
  等 sector-level 描述，**沒指名特定個股**）
- **macro**: 大盤總經（CPI / 利率決議 / 匯率 / 美股道瓊 / 地緣政治 / 政府政策 /
  油價 / 黃金 等大環境議題）
- **price_only**: 個股單純漲跌**無 catalyst**（如 "台股早盤上漲" / "XX 上漲 5%" 純報價文，
  未提具體理由 / 訂單 / 法說會 / 政策）

## 第二步：依 article_type 抽相應欄位

### 共通必填
- id: 文章編號 (1, 2, ...)
- article_type: 上述 4 選 1
- sentiment: float -1.0 ~ +1.0
- tone: "bullish" / "bearish" / "neutral"
- confidence: int 0-100（你對 article_type 分類的信心）

### 若 article_type='individual'（個股 catalyst 必填）
- themes: list[str]，**1-3 個** catalyst-driven 題材中文標籤
  範例（不限於此清單，看到什麼新題材就抽什麼）：
    "AI 伺服器 ODM", "CoWoS 先進封裝", "ABF 載板", "矽光子", "CPO 共封裝光學",
    "AI 散熱", "AI PC SoC", "EV 供應鏈", "低軌衛星", "機器人", "高速傳輸",
    "ASIC 設計服務", "HBM", "Apple 蘋果供應鏈", "矽晶圓", "PCB 硬板",
    "量子運算", "AI 眼鏡", "車用功率半導體", "電動巴士", "工業機器人" 等
  **禁止**寫太泛 "AI" / "半導體" / "電子股" / "其他" / "權值股"。
- tickers: list[str]，4 位數字台股 ticker（**只**從文章字面明確提到的）
  **禁止**從題材推測（例如看到 ABF 不要自己補 8046/3037）

### 若 article_type='sector'（產業類）
- sector_tag: str，**1 個**主要影響的產業
  範例: "半導體", "金融", "航運", "電子下游", "傳產", "生技醫療", "觀光餐飲",
        "塑化", "鋼鐵", "電動車", "綠能", "AI 應用", "光電" 等
- themes: []（不抽）
- tickers: []（不抽）

### 若 article_type='macro'（大盤總經）
- macro_topic: str，**1 個**之中：
  "rate" (利率) / "inflation" (通膨/CPI) / "currency" (匯率) /
  "fiscal" (財政/政策) / "geopolitical" (地緣政治) / "policy" (法規) /
  "commodity" (原物料/油金) / "global_market" (美股/國際盤) / "labor" (就業)
- themes: []（不抽）
- tickers: []（不抽）

### 若 article_type='price_only'（純漲跌報價）
- themes: [] / tickers: []（不抽）
- 仍給 sentiment / tone / confidence

輸出格式：JSON array
`[{{"id": 1, "article_type": "...", "themes": [...], ...}}, ...]`
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


def _build_rows(merged: list[dict]) -> list[dict]:
    """Expand merged articles into archive rows (Phase 0 Commit 3: 4-class).

    依 article_type 行為：
    - individual: ticker × theme expand (沿用舊邏輯)，必填 themes + tickers
    - sector: 1 row, theme/ticker 留空，填 sector_tag
    - macro: 1 row, theme/ticker 留空，填 macro_topic
    - price_only: 1 row, 全留空（archive 保留為未來研究, derived 不讀）

    Backward compat: article_type 缺失 (legacy 第三輪前) → default 'individual',
    與舊行為一致（必須有 themes + tickers 才寫）。

    Schema (Commit 3 後 14 欄):
    date / source / ticker / theme / sentiment / tone / confidence /
    title / link / extract_version / normalized_title_hash / event_id /
    article_type / sector_tag / macro_topic
    """
    rows = []
    for a in merged:
        article_type = (a.get('article_type') or 'individual').strip().lower()
        title = str(a.get('title', ''))[:200]
        title_hash = normalize_title_hash(title)
        date_str = str(a.get('date', ''))
        event_id = compute_event_id(title_hash, date_str)

        # 共通 base row template
        base = {
            'date': a.get('date'),
            'source': a.get('source', ''),
            'sentiment': float(a.get('sentiment', 0.0) or 0.0),
            'tone': str(a.get('tone', 'neutral')),
            'confidence': int(a.get('confidence', 0) or 0),
            'title': title,
            'link': str(a.get('link', '')),
            'extract_version': EXTRACT_VERSION,
            'normalized_title_hash': title_hash,
            'event_id': event_id,
            'article_type': article_type,
            'sector_tag': str(a.get('sector_tag', '') or ''),
            'macro_topic': str(a.get('macro_topic', '') or ''),
            # Commit 4 (BLOCKER #5): full body archive (≤1500 chars) for re-extract
            'body_full': str(a.get('body_full', '') or '')[:BODY_FULL_MAX_CHARS],
            'body_status': str(a.get('body_status', 'summary_only') or 'summary_only'),
        }

        if article_type == 'individual':
            themes = a.get('themes') or []
            tickers = a.get('tickers') or []
            if not themes:
                # individual 但 LLM 沒抽到 theme → degrade to price_only
                rows.append({**base, 'ticker': '', 'theme': '',
                             'article_type': 'price_only'})
                continue
            if not tickers:
                tickers = ['']
            for t in tickers:
                for th in themes:
                    rows.append({**base, 'ticker': str(t), 'theme': str(th)})

        elif article_type == 'sector':
            rows.append({**base, 'ticker': '', 'theme': ''})

        elif article_type == 'macro':
            rows.append({**base, 'ticker': '', 'theme': ''})

        elif article_type == 'price_only':
            rows.append({**base, 'ticker': '', 'theme': ''})

        else:
            # Unknown article_type, fallback to individual extraction with warning
            logger.warning("Unknown article_type=%r, fallback to individual",
                           article_type)
            themes = a.get('themes') or []
            if not themes:
                rows.append({**base, 'ticker': '', 'theme': '',
                             'article_type': 'price_only'})
                continue
            tickers = a.get('tickers') or ['']
            for t in tickers:
                for th in themes:
                    rows.append({**base, 'ticker': str(t), 'theme': str(th),
                                 'article_type': 'individual'})

    return rows


def _atomic_write_parquet(df, path: Path) -> None:
    """Atomic parquet write with .tmp → os.replace + retry (BLOCKER #3).

    Windows os.replace 對被讀取中的檔 raise PermissionError，加 3× retry。
    """
    import os
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    df.to_parquet(tmp, index=False)
    last_err = None
    for attempt in range(3):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(0.2 * (attempt + 1))
    raise IOError(f"atomic swap failed after 3 retries: {path}: {last_err}")


def _partition_for_article(article_date: str) -> str:
    """Map article publish_date 'YYYY-MM-DD' to partition 'YYYY-MM' (BLOCKER #4).

    Fallback to current month if publish_date 解析失敗（避免 lose article）。
    """
    if article_date and len(article_date) >= 7:
        # 'YYYY-MM-DD' or 'YYYY-MM' prefix
        candidate = article_date[:7]
        if len(candidate) == 7 and candidate[4] == '-':
            return candidate
    # Fallback: today's YYYY-MM
    return datetime.now().strftime('%Y-%m')


def append_to_archive(rows: list[dict]) -> dict:
    """Append rows into monthly-partitioned archive (新 SoT, 永久, append-only).

    Path: data_cache/news_archive/YYYY-MM/articles.parquet
    Partition key = article.date (publish_date), NOT now() — BLOCKER #4
    Atomic swap on write — BLOCKER #3
    No dedupe, no TTL — archive 是永久 SoT (Commit 4 後 + Commit 2 加 dedupe key)
    """
    import pandas as pd
    if not rows:
        return {'rows_added': 0, 'partitions': []}

    # Group rows by partition (publish_date YYYY-MM)
    by_partition: dict[str, list[dict]] = {}
    for r in rows:
        part = _partition_for_article(r.get('date', ''))
        by_partition.setdefault(part, []).append(r)

    partitions_written = []
    rows_added = 0
    for part, part_rows in by_partition.items():
        path = ARCHIVE_DIR / part / 'articles.parquet'
        new_df = pd.DataFrame(part_rows)
        if path.exists():
            try:
                existing = pd.read_parquet(path)
                combined = pd.concat([existing, new_df], ignore_index=True)
            except Exception as e:
                logger.warning("讀 archive %s 失敗，重建: %s", path, e)
                combined = new_df
        else:
            combined = new_df
        _atomic_write_parquet(combined, path)
        partitions_written.append({'partition': part, 'rows': len(part_rows),
                                   'total': len(combined)})
        rows_added += len(part_rows)

    return {'rows_added': rows_added, 'partitions': partitions_written}


def aggregate_to_parquet(merged: list[dict]) -> dict:
    """Dual-write: append into archive (new SoT) + legacy parquet (1 週過渡).

    News Initiative Phase 0 Commit 1:
    - Archive (新): data_cache/news_archive/YYYY-MM/articles.parquet 永久 append
    - Legacy (舊): data/news_themes.parquet 30d TTL，5 處 reader 暫時繼續讀
    Commit 6 整體 cutover 後 legacy write 才下線。
    """
    import pandas as pd

    rows = _build_rows(merged)
    if not rows:
        return {'rows_added': 0, 'rows_total': 0, 'archive': {}}

    # 1. Write to archive (新 SoT, 永久, partition by publish_date)
    archive_stats = append_to_archive(rows)
    logger.info("Archive write: +%d rows, partitions=%s",
                archive_stats['rows_added'],
                [p['partition'] for p in archive_stats['partitions']])

    # 2. Dual-write to legacy parquet (1 週過渡期, 5 處 reader 還在讀)
    new_df = pd.DataFrame(rows)
    if AGG_PATH.exists():
        try:
            existing = pd.read_parquet(AGG_PATH)
            combined = pd.concat([existing, new_df], ignore_index=True)
        except Exception as e:
            logger.warning("讀 legacy %s 失敗，重建: %s", AGG_PATH, e)
            combined = new_df
    else:
        combined = new_df

    # legacy: dedupe + 30 天 TTL (與舊行為一致)
    # 注意 extract_version 不進 dedupe key (避免新欄位破壞 dedupe)
    combined = combined.drop_duplicates(
        subset=['date', 'ticker', 'theme', 'title'], keep='last'
    )
    combined['date'] = pd.to_datetime(combined['date'], errors='coerce')
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=NEWS_TTL_DAYS)
    combined = combined[combined['date'] >= cutoff].copy()
    combined['date'] = combined['date'].dt.strftime('%Y-%m-%d')

    _atomic_write_parquet(combined, AGG_PATH)
    return {
        'rows_added': len(new_df),
        'rows_total': len(combined),
        'tickers_total': combined['ticker'].nunique(),
        'themes_total': combined['theme'].nunique(),
        'archive': archive_stats,
    }


def migrate_legacy_to_archive() -> dict:
    """One-shot: backfill existing data/news_themes.parquet rows into archive.

    Run via `python tools/news_theme_extract.py --migrate-legacy`.
    Only run once at News Initiative Phase 0 Commit 1 setup.
    Idempotent: 若 archive partition 已有同 (date, ticker, theme, title) 不重複加。
    """
    import pandas as pd

    if not AGG_PATH.exists():
        logger.error("Legacy parquet not found: %s", AGG_PATH)
        return {'rows_migrated': 0, 'partitions': []}

    legacy = pd.read_parquet(AGG_PATH)
    if legacy.empty:
        logger.info("Legacy parquet empty, nothing to migrate")
        return {'rows_migrated': 0, 'partitions': []}

    # 補 extract_version 欄位 (legacy 沒有)
    if 'extract_version' not in legacy.columns:
        legacy['extract_version'] = EXTRACT_VERSION

    # BLOCKER #1 (Commit 2): backfill normalized_title_hash + event_id
    if 'normalized_title_hash' not in legacy.columns:
        legacy['normalized_title_hash'] = legacy['title'].astype(str).map(
            normalize_title_hash
        )
    if 'event_id' not in legacy.columns:
        legacy['event_id'] = [
            compute_event_id(h, d)
            for h, d in zip(legacy['normalized_title_hash'].astype(str),
                            legacy['date'].astype(str))
        ]
    # Commit 3: backfill article_type + sector_tag + macro_topic (BLOCKER #2 default rule)
    # legacy 全是 individual (有 ticker + theme 才進 archive)
    if 'article_type' not in legacy.columns:
        legacy['article_type'] = 'individual'
    if 'sector_tag' not in legacy.columns:
        legacy['sector_tag'] = ''
    if 'macro_topic' not in legacy.columns:
        legacy['macro_topic'] = ''
    # Commit 4: backfill body_full + body_status (BLOCKER #5; legacy 沒抓全文)
    if 'body_full' not in legacy.columns:
        legacy['body_full'] = ''  # legacy 無全文; future re-extract 失敗
    if 'body_status' not in legacy.columns:
        legacy['body_status'] = 'legacy_no_body'

    logger.info("Legacy parquet: %d rows, dates %s..%s",
                len(legacy), legacy['date'].min(), legacy['date'].max())

    # Group by partition
    legacy['_partition'] = legacy['date'].astype(str).str[:7]
    parts_summary = []
    total_added = 0
    for part, sub in legacy.groupby('_partition'):
        sub = sub.drop(columns=['_partition'])
        path = ARCHIVE_DIR / part / 'articles.parquet'
        if path.exists():
            existing = pd.read_parquet(path)
            # Schema bump backfill (idempotent across re-runs after schema changes)
            if 'extract_version' not in existing.columns:
                existing['extract_version'] = EXTRACT_VERSION
            if 'normalized_title_hash' not in existing.columns:
                existing['normalized_title_hash'] = existing['title'].astype(str).map(
                    normalize_title_hash
                )
            if 'event_id' not in existing.columns:
                existing['event_id'] = [
                    compute_event_id(h, d)
                    for h, d in zip(existing['normalized_title_hash'].astype(str),
                                    existing['date'].astype(str))
                ]
            if 'article_type' not in existing.columns:
                existing['article_type'] = 'individual'
            if 'sector_tag' not in existing.columns:
                existing['sector_tag'] = ''
            if 'macro_topic' not in existing.columns:
                existing['macro_topic'] = ''
            if 'body_full' not in existing.columns:
                existing['body_full'] = ''
            if 'body_status' not in existing.columns:
                existing['body_status'] = 'legacy_no_body'
            # idempotent: dedupe by (date, ticker, theme, title)
            combined = pd.concat([existing, sub], ignore_index=True)
            before = len(existing)
            combined = combined.drop_duplicates(
                subset=['date', 'ticker', 'theme', 'title'], keep='first'
            )
            added = len(combined) - before
        else:
            combined = sub
            added = len(sub)
        _atomic_write_parquet(combined, path)
        parts_summary.append({'partition': part, 'rows_in_legacy': len(sub),
                              'rows_added': added, 'total_after': len(combined)})
        total_added += added
        logger.info("  Partition %s: +%d rows (legacy had %d, archive total now %d)",
                    part, added, len(sub), len(combined))

    return {'rows_migrated': total_added, 'partitions': parts_summary}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='只抓不解析 LLM')
    parser.add_argument('--no-aggregate', action='store_true',
                        help='解析但不 append 進 parquet (debug 用)')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--max-per-query', type=int, default=8)
    parser.add_argument('--migrate-legacy', action='store_true',
                        help='One-shot backfill data/news_themes.parquet → '
                             'data_cache/news_archive/YYYY-MM/ (Phase 0 Commit 1 only)')
    args = parser.parse_args()

    # Migration shortcut (no LLM call)
    if args.migrate_legacy:
        result = migrate_legacy_to_archive()
        logger.info("Migration done: %d rows migrated across %d partitions",
                    result['rows_migrated'], len(result['partitions']))
        for p in result['partitions']:
            logger.info("  %s: +%d rows (total %d)",
                        p['partition'], p['rows_added'], p['total_after'])
        return

    today = datetime.now().strftime('%Y%m%d')
    out_path = OUT_DIR / f'{today}.json'

    # Commit 4 (Council 第四輪 #4): 不 dedupe across sources -- 多 source 同事件
    # 各保留版本，讓「報導密度」可當 signal。同一 source 內仍 dedupe（同 source 同 title
    # 是真重複，不是不同視角）。Cost: ~25% 更多 LLM call，月 ~$1 增量 negligible。
    all_articles = []

    # 1a. 抓 經濟日報 RSS direct (UDN 內部 dedupe 跨 category)
    udn_seen = set()
    for label, url in UDN_RSS_CATS:
        items = fetch_udn_rss(label, url, days=args.days)
        logger.info("[%s] %d articles (RSS direct)", label, len(items))
        for a in items:
            key = (a['source'], a['title'])
            if key in udn_seen:
                continue
            udn_seen.add(key)
            all_articles.append(a)
        time.sleep(0.5)

    # 1b. 抓 cnyes API (cnyes 內部 dedupe 跨 category)
    cnyes_seen = set()
    for label, cat in CNYES_API_CATS:
        items = fetch_cnyes_api(label, cat, limit=CNYES_API_LIMIT, days=args.days)
        logger.info("[%s] %d articles (cnyes API)", label, len(items))
        for a in items:
            key = (a['source'], a['title'])
            if key in cnyes_seen:
                continue
            cnyes_seen.add(key)
            all_articles.append(a)
        time.sleep(0.5)

    logger.info("Total articles (UDN + cnyes, source-internal dedupe only): %d",
                len(all_articles))

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
                'article_type': match.get('article_type', 'individual'),
                'themes': match.get('themes', []),
                'tickers': match.get('tickers', []),
                'sector_tag': match.get('sector_tag', ''),
                'macro_topic': match.get('macro_topic', ''),
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
