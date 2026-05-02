"""News Initiative #2 - 法說會行事曆 fetcher

Multi-source HTML download → BS trim → Sonnet LLM extract → JSON schema →
post-validate → dedup → data/calendar/earnings_call.parquet

設計原則 (user 2026-05-02 拍板):
- LLM parse 路線（不寫 scraper），網站改版自動 adapt
- 多源 fallback chain，新網站只加 1 行 SOURCES entry
- Sonnet (per CLAUDE.md LLM 規範: news / metadata 萃取 = Sonnet 600s)

Schema (data/calendar/earnings_call.parquet):
- ticker: str (4 位數字台股 ticker)
- company_name: str
- event_date: str (YYYY-MM-DD)
- event_time: str | '' (HH:MM)
- event_type: str = 'earnings_call'
- location: str | '' (≤ 80 chars)
- description: str | '' (≤ 100 chars)
- source: str ('moneylink' / 'yahoo' / etc.)
- confidence: int (0-100)
- extracted_at: pd.Timestamp

CLI:
    python tools/earnings_calendar_fetch.py
    python tools/earnings_calendar_fetch.py --dry-run    # 不寫 parquet
    python tools/earnings_calendar_fetch.py --source moneylink
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "data" / "calendar"
OUT_PARQUET = OUT_DIR / "earnings_call.parquet"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

_CLAUDE_CLI = shutil.which("claude") or "claude"
CLAUDE_TIMEOUT = 600  # per CLAUDE.md LLM 規範

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")


# ============================================================
# Sources (multi-source fallback chain)
# ============================================================
SOURCES = [
    {
        'name': 'moneylink',
        'url': 'https://www.money-link.com.tw/stxba/imwcontent0.asp?page=INVC1&ID=INVC1',
        'encoding': 'big5',
    },
    # 未來可加 yahoo / wantgoo 等 (HTML download + same LLM prompt)
]


def fetch_html(source: dict) -> str | None:
    """GET HTML, decode by source-specific encoding."""
    headers = {'User-Agent': UA, 'Accept-Language': 'zh-TW,zh;q=0.9'}
    try:
        r = requests.get(source['url'], headers=headers, verify=False, timeout=20)
        if r.status_code != 200:
            logger.warning("%s status=%d", source['name'], r.status_code)
            return None
        return r.content.decode(source.get('encoding', 'utf-8'), errors='replace')
    except Exception as e:
        logger.warning("%s fetch failed: %s", source['name'], e)
        return None


def trim_html(html: str) -> str:
    """Strip script/style/nav/header/footer; extract structured text.

    Convert <td>...content...</td> sequences to one cell per line so LLM 能
    辨識 cells / rows 又不被 HTML tag attributes 灌爆 prompt。
    """
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'meta', 'link']):
        tag.decompose()
    body = soup.find('body') or soup

    # 把每個 td/th 內容輸出成獨立行，讓 LLM 看到 cells 結構又無 HTML 噪音
    cells = []
    for cell in body.find_all(['td', 'th']):
        text = cell.get_text(separator=' ', strip=True)
        if text:
            cells.append(text)
    if cells:
        return '\n'.join(cells)[:8000]  # cap 8K 純文字
    # fallback: plain get_text
    return body.get_text(separator='\n', strip=True)[:8000]


# ============================================================
# LLM extraction
# ============================================================
PROMPT_TEMPLATE = """你是台股法說會行事曆 extractor。我給你一頁 HTML，請從中抽出所有
法說會（法人說明會 / 投資人說明會）event 並回 JSON array。

## 抽取欄位 (per event)
- ticker: 4 位數字台股 ticker（無法確定請填 ""）
- company_name: 公司中文名稱
- event_date: YYYY-MM-DD 格式（HTML 可能用民國年「115 年」即 2026，或西元）
- event_time: HH:MM 24h 格式，沒填 "" (e.g. "14:00", "16:30")
- location: 召開地點（≤ 80 字元，沒填 ""）
- description: 摘要（≤ 100 字元，沒填 ""）
- confidence: 0-100，本筆資料的清晰度

## 規則
- **只抽真正的法說會**（法人說明會 / 法人說明 / investor conference / 投資人說明會）
- **跳過股東會 / 除權除息 / 配息公告 / 重大訊息**（不是 earnings call）
- 同一公司多場法說會（中文 / 英文 / 各券商主辦）→ 各算一筆 event
- 找不到日期就 skip（不要瞎猜）
- 民國年→西元年: 民國 X 年 = (X + 1911) 西元年
- 英文 / 內部測試文字 / 無 ticker 無公司名 → skip

## 輸出格式
JSON array, 開頭 `[` 結尾 `]`，無 markdown fence、無說明文字。
未填欄位用 "" 或 null（不要省略 key）。

範例 (sample 2 events):
[
  {{"ticker":"1590","company_name":"亞德客-KY","event_date":"2026-06-10","event_time":"15:00","location":"台北W飯店","description":"參加中國信託證券舉辦","confidence":95}},
  {{"ticker":"2882","company_name":"國泰金","event_date":"2026-05-29","event_time":"14:00","location":"國泰金融會議廳A廳","description":"召開法人說明會","confidence":90}}
]

## HTML 內容

{html}
"""


def call_claude_extract(prompt: str) -> tuple[str, str | None]:
    """LLM call for earnings_calendar table extraction.

    Uses Haiku (CLAUDE.md exemption: structured table extraction is Haiku's
    sweet spot, not Sonnet-level NLP reasoning). Sonnet timeout was 600s+
    when real-run repeated dry-run prompt; Haiku 5-10x faster and cheaper.
    """
    cmd = [_CLAUDE_CLI, '-p', '--model', 'haiku', '--output-format', 'json']
    try:
        result = subprocess.run(
            cmd, input=prompt, capture_output=True, text=True,
            timeout=CLAUDE_TIMEOUT, encoding='utf-8', errors='replace',
            shell=False,
        )
    except subprocess.TimeoutExpired:
        return '', f'claude CLI timeout after {CLAUDE_TIMEOUT}s'
    except FileNotFoundError:
        return '', 'claude CLI not found'

    if result.returncode != 0:
        return result.stdout or '', f'claude exit {result.returncode}: {result.stderr[:300]}'

    try:
        envelope = json.loads(result.stdout)
        text = envelope.get('result', '')
        if envelope.get('is_error'):
            return text, f'claude is_error=true ({envelope.get("api_error_status")})'
        return text, None
    except json.JSONDecodeError:
        return result.stdout, None


def parse_llm_output(text: str) -> list[dict]:
    """Strip optional markdown fence, parse JSON array."""
    t = text.strip()
    if t.startswith('```'):
        t = t.split('```', 2)[1]
        if t.startswith('json'):
            t = t[4:]
        t = t.rsplit('```', 1)[0]
    t = t.strip()
    try:
        data = json.loads(t)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError as e:
        logger.warning("LLM JSON parse failed: %s; first 300: %s", e, t[:300])
        return []


# ============================================================
# Post-validate
# ============================================================
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
TIME_RE = re.compile(r'^\d{1,2}:\d{2}$')
TICKER_RE = re.compile(r'^\d{4}$')


def validate_event(e: dict, source: str) -> dict | None:
    """Validate + normalize. Return clean dict or None to drop."""
    ticker = str(e.get('ticker', '')).strip()
    name = str(e.get('company_name', '')).strip()
    date = str(e.get('event_date', '')).strip()
    if not DATE_RE.match(date):
        return None
    if not name:  # ticker 可空但 name 必填
        return None
    if ticker and not TICKER_RE.match(ticker):
        ticker = ''  # 非標準 4 位數清空（如 0050.TW 之類）

    time_ = str(e.get('event_time', '')).strip()
    if time_ and not TIME_RE.match(time_):
        time_ = ''

    try:
        confidence = int(e.get('confidence', 0))
    except (TypeError, ValueError):
        confidence = 0
    if confidence < 70:
        return None

    return {
        'ticker': ticker,
        'company_name': name[:50],
        'event_date': date,
        'event_time': time_,
        'event_type': 'earnings_call',
        'location': str(e.get('location') or '')[:80],
        'description': str(e.get('description') or '')[:100],
        'source': source,
        'confidence': confidence,
        'extracted_at': pd.Timestamp.now(),
    }


def dedup_and_merge(new_rows: list[dict]) -> pd.DataFrame:
    """Dedup new rows by (ticker, event_date, event_time) keep highest confidence;
    merge with existing parquet (same dedup rule, keep latest extracted_at).
    """
    if not new_rows:
        return pd.read_parquet(OUT_PARQUET) if OUT_PARQUET.exists() else pd.DataFrame()

    df_new = pd.DataFrame(new_rows)
    df_new = df_new.sort_values(['confidence', 'extracted_at'], ascending=[False, False])
    df_new = df_new.drop_duplicates(subset=['ticker', 'event_date', 'event_time'], keep='first')

    if OUT_PARQUET.exists():
        df_old = pd.read_parquet(OUT_PARQUET)
        merged = pd.concat([df_old, df_new], ignore_index=True)
        merged = merged.sort_values('extracted_at', ascending=False)
        merged = merged.drop_duplicates(subset=['ticker', 'event_date', 'event_time'], keep='first')
        return merged.sort_values('event_date').reset_index(drop=True)
    return df_new.sort_values('event_date').reset_index(drop=True)


# ============================================================
# Main
# ============================================================
def fetch_one_source(source: dict) -> list[dict]:
    """Run full pipeline for 1 source: GET → trim → LLM → validate."""
    logger.info("[%s] fetching %s", source['name'], source['url'])
    html = fetch_html(source)
    if not html:
        return []

    trimmed = trim_html(html)
    logger.info("[%s] trimmed HTML to %d chars", source['name'], len(trimmed))

    prompt = PROMPT_TEMPLATE.format(html=trimmed)
    logger.info("[%s] calling Claude Haiku (prompt %d chars)", source['name'], len(prompt))
    text, err = call_claude_extract(prompt)
    if err:
        logger.error("[%s] LLM error: %s", source['name'], err)
        return []

    raw_events = parse_llm_output(text)
    logger.info("[%s] LLM returned %d raw events", source['name'], len(raw_events))

    clean = []
    for e in raw_events:
        v = validate_event(e, source['name'])
        if v is not None:
            clean.append(v)
    logger.info("[%s] %d events passed validation", source['name'], len(clean))
    return clean


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='不寫 parquet')
    parser.add_argument('--source', type=str, default=None,
                        help='限定 source (e.g. moneylink); default 跑全部')
    args = parser.parse_args()

    sources = [s for s in SOURCES if not args.source or s['name'] == args.source]
    if not sources:
        logger.error("No matching source for %s", args.source)
        return 1

    all_events = []
    for source in sources:
        all_events.extend(fetch_one_source(source))

    logger.info("Total %d events from %d source(s)", len(all_events), len(sources))

    df = dedup_and_merge(all_events)
    logger.info("After merge: %d total rows", len(df))

    if args.dry_run:
        logger.info("[dry-run] preview top 10:")
        if len(df):
            print(df.head(10).to_string())
        return 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PARQUET, index=False)
    logger.info("Saved: %s (%d rows)", OUT_PARQUET, len(df))
    return 0


if __name__ == '__main__':
    sys.exit(main())
