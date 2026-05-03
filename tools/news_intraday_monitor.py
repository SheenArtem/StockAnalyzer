"""News Initiative Phase 1 #7 — 盤中即時 alert (intraday push).

每 30 min 跑一次（盤中 09:00-13:30，Windows Task Scheduler 觸發）。

行為:
1. 抓 cnyes API + UDN RSS 增量（近 N 分鐘新文章, default 30 min）
2. dedupe by event_id 與既有 archive (避免重抽)
3. batch LLM (沿用 build_extraction_prompt + N=20 split)
4. append archive + rebuild articles_recent (即時 SoT)
5. 觸發 alert: news_count_30min >= ALERT_NEWS_THRESHOLD OR material_event 命中
   → Discord push (沿用 send_alert_notification)

Cost: 9 slot/day × 1 batch + 1 盤後 batch ≈ 10 LLM calls/day, ~$4-5/月

Council BLOCKER #7: informational only, NOT auto-feed paper_trade 直到
過 historical event study (validation_spec 在 project_news_data_extraction_roadmap.md).

CLI:
    python tools/news_intraday_monitor.py
    python tools/news_intraday_monitor.py --window-min 30
    python tools/news_intraday_monitor.py --dry-run    # 抓但不 LLM 不 push
    python tools/news_intraday_monitor.py --no-discord  # 跑全鏈但不送 Discord
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / 'tools'))

import pandas as pd  # noqa: E402

from news_theme_extract import (  # noqa: E402
    fetch_udn_rss, fetch_cnyes_api,
    build_extraction_prompt, call_claude_sonnet, parse_json_response,
    aggregate_to_parquet, rebuild_all_derived,
    UDN_RSS_CATS, CNYES_API_CATS, CNYES_API_LIMIT,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger("intraday")

ARCHIVE_DIR = REPO / 'data_cache' / 'news_archive'

# 觸發條件
ALERT_NEWS_THRESHOLD = 2  # news_count_30min >= 2 → push
BATCH_SIZE = 20  # 與盤後一致 (Windows stdout 5KB threshold)


def fetch_recent_articles(window_min: int) -> list[dict]:
    """抓 cnyes + UDN 增量, filter to last `window_min` minutes by published_at.

    Reuses 既有 fetcher (with days=1) 然後 in-memory filter timestamp.
    """
    cutoff = datetime.now() - timedelta(minutes=window_min)
    all_articles: list[dict] = []

    # UDN
    seen_udn = set()
    for label, url in UDN_RSS_CATS:
        items = fetch_udn_rss(label, url, days=1)
        for a in items:
            pub = a.get('published_at', '')
            if not pub:
                continue
            try:
                dt = datetime.fromisoformat(pub)
            except ValueError:
                continue
            if dt < cutoff:
                continue
            key = (a['source'], a['title'])
            if key in seen_udn:
                continue
            seen_udn.add(key)
            all_articles.append(a)

    # cnyes
    seen_cnyes = set()
    for label, cat in CNYES_API_CATS:
        items = fetch_cnyes_api(label, cat, limit=CNYES_API_LIMIT, days=1)
        for a in items:
            pub = a.get('published_at', '')
            if not pub:
                continue
            try:
                dt = datetime.fromisoformat(pub)
            except ValueError:
                continue
            if dt < cutoff:
                continue
            key = (a['source'], a['title'])
            if key in seen_cnyes:
                continue
            seen_cnyes.add(key)
            all_articles.append(a)

    return all_articles


def filter_already_archived(articles: list[dict]) -> list[dict]:
    """Drop articles already in archive (避免同 slot 重抽 + LLM cost 多花).

    Dedupe key: (source, title) 與既有 archive 內 source+title 比對。
    今天的 partition (YYYY-MM) 才需檢查 (intraday slot 同日窗口).
    """
    today_partition = datetime.now().strftime('%Y-%m')
    archive_path = ARCHIVE_DIR / today_partition / 'articles.parquet'
    if not archive_path.exists():
        return articles
    try:
        existing = pd.read_parquet(archive_path, columns=['source', 'title'])
    except Exception as e:
        logger.warning("Failed to read archive partition: %s", e)
        return articles
    existing_keys = set(zip(existing['source'].astype(str),
                             existing['title'].astype(str)))
    return [a for a in articles
            if (str(a.get('source', '')), str(a.get('title', '')))
            not in existing_keys]


def llm_extract_batches(articles: list[dict]) -> list[dict]:
    """Run batch LLM extraction (N=20 split, fault-isolated)."""
    if not articles:
        return []
    n_batches = (len(articles) + BATCH_SIZE - 1) // BATCH_SIZE
    merged: list[dict] = []
    for batch_idx in range(n_batches):
        batch = articles[batch_idx * BATCH_SIZE:(batch_idx + 1) * BATCH_SIZE]
        prompt = build_extraction_prompt(batch)
        logger.info("Batch %d/%d: %d articles, prompt %d chars",
                    batch_idx + 1, n_batches, len(batch), len(prompt))
        output, err = call_claude_sonnet(prompt)
        if err:
            logger.error("  Batch %d Claude err: %s", batch_idx + 1, err)
            continue
        extracted = parse_json_response(output)
        if extracted is None:
            logger.error("  Batch %d JSON parse failed", batch_idx + 1)
            continue
        # Map id (1..N) back to article
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
                'forward_eps_change': match.get('forward_eps_change', '') or '',
                'forward_revenue_guidance': match.get('forward_revenue_guidance', '') or '',
                'forward_gross_margin': match.get('forward_gross_margin', '') or '',
                'key_capacity_event': match.get('key_capacity_event', '') or '',
                'q_period': match.get('q_period', '') or '',
                'target_prices': match.get('target_prices', []) or [],
                'material_event_type': match.get('material_event_type', '') or '',
            })
    return merged


def evaluate_trigger(merged: list[dict]) -> tuple[bool, list[str]]:
    """評估觸發條件，回 (should_push, alert_lines).

    觸發:
    - news_count_30min >= ALERT_NEWS_THRESHOLD: 任一 ticker 30 min 內 >= 2 篇
    - material_event 命中: 任一 article material_event_type ∈ valid 6 類
    """
    valid_events = {'merger', 'buyback', 'lawsuit', 'capital_reduction',
                    'penalty', 'major_contract'}
    event_zh = {
        'merger': '併購', 'buyback': '庫藏股', 'lawsuit': '訴訟',
        'capital_reduction': '減資', 'penalty': '裁罰',
        'major_contract': '重大合約',
    }
    alerts: list[str] = []

    # 1. material_event 命中
    me_articles = [a for a in merged
                   if str(a.get('material_event_type', '')) in valid_events]
    for a in me_articles:
        et = a['material_event_type']
        tickers = a.get('tickers') or ['']
        ticker_str = ','.join(t for t in tickers if t) or '?'
        title = (a.get('title') or '')[:50]
        alerts.append(f"⚠️ [{event_zh.get(et, et)}] {ticker_str}: {title}")

    # 2. news_count_30min by ticker
    ticker_counts: dict[str, int] = {}
    for a in merged:
        if a.get('article_type') != 'individual':
            continue
        for t in (a.get('tickers') or []):
            if not t:
                continue
            ticker_counts[str(t)] = ticker_counts.get(str(t), 0) + 1
    hot_tickers = sorted(
        [(t, c) for t, c in ticker_counts.items() if c >= ALERT_NEWS_THRESHOLD],
        key=lambda x: -x[1])
    for t, c in hot_tickers[:5]:
        # Find sample title
        sample = next((a.get('title', '') for a in merged
                      if t in (a.get('tickers') or [])), '')
        alerts.append(f"🔥 {t}: {c} 則新聞 / {sample[:40]}")

    return (len(alerts) > 0, alerts)


def push_discord(slot_label: str, alerts: list[str], n_articles: int) -> bool:
    """送 Discord (沿用 scanner_job.send_alert_notification)."""
    try:
        from scanner_job import send_alert_notification
    except ImportError as e:
        logger.error("cannot import scanner_job: %s", e)
        return False
    issues = [
        f"窗口: 近 30 min ({slot_label})",
        f"新文章數: {n_articles}",
        "",
    ]
    issues.extend(alerts)
    return send_alert_notification(scan_type='news_intraday', market='TW',
                                    issues=issues)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--window-min', type=int, default=30,
                    help='抓近 N 分鐘新文章 (default 30)')
    ap.add_argument('--dry-run', action='store_true',
                    help='只抓不 LLM 不 push (debug)')
    ap.add_argument('--no-discord', action='store_true',
                    help='跑全鏈但不送 Discord')
    ap.add_argument('--force', action='store_true',
                    help='略過 weekday/holiday 檢查強跑 (debug)')
    args = ap.parse_args()

    slot_label = datetime.now().strftime('%H:%M')

    # 假日守門 — 週末 + (隱性) 國定假日跳過免燒 LLM
    # tw_calendar 只覆蓋 weekday()，國定假日如 5/1 / 春節 / 雙十仍會 fire，
    # 但那天 cnyes/UDN 流量本就低，trigger 不易命中 cost 微小（~$0.05/holiday）
    # 若 user 想完全省，可在 Task Scheduler 手動 Disable + Re-enable
    if not args.force:
        from tw_calendar import is_tw_trading_day  # noqa: E402
        if not is_tw_trading_day(datetime.now().date()):
            logger.info("Today is non-trading day (weekend), skip slot %s",
                        slot_label)
            return

    logger.info("=== Intraday monitor slot %s (window=%d min) ===",
                slot_label, args.window_min)

    # 1. Fetch
    articles = fetch_recent_articles(args.window_min)
    logger.info("Fetched %d articles in last %d min", len(articles),
                args.window_min)
    if not articles:
        logger.info("No new articles, exit clean")
        return

    # 2. Dedupe vs archive
    new_articles = filter_already_archived(articles)
    logger.info("After archive dedupe: %d new (skipped %d existing)",
                len(new_articles), len(articles) - len(new_articles))
    if not new_articles:
        logger.info("All already archived, exit clean")
        return

    if args.dry_run:
        logger.info("Dry run, skip LLM + archive write")
        for a in new_articles[:5]:
            logger.info("  %s | %s", a.get('published_at', '')[-8:],
                        a.get('title', '')[:60])
        return

    # 3. LLM batch extract
    merged = llm_extract_batches(new_articles)
    logger.info("LLM merged: %d / %d", len(merged), len(new_articles))
    if not merged:
        logger.warning("LLM extracted 0 articles, skip archive write")
        return

    # 4. Append archive + rebuild
    agg_stats = aggregate_to_parquet(merged)
    logger.info("Archive: +%d rows, total=%d",
                agg_stats.get('rows_added', 0),
                agg_stats.get('rows_total', 0))
    rebuild_stats = rebuild_all_derived()
    logger.info("Rebuild: hot_view=%d themes_core=%d earnings_schema=%d "
                "analyst_targets=%d material_events=%d",
                rebuild_stats['hot_view']['rows'],
                rebuild_stats['themes_core']['rows'],
                rebuild_stats['earnings_schema']['rows'],
                rebuild_stats['analyst_targets']['rows'],
                rebuild_stats['material_events']['rows'])

    # 5. Trigger eval + Discord
    should_push, alerts = evaluate_trigger(merged)
    if not should_push:
        logger.info("No trigger hit, exit clean")
        return

    logger.info("Trigger HIT: %d alerts", len(alerts))
    for a in alerts:
        logger.info("  %s", a)

    if args.no_discord:
        logger.info("--no-discord: skip push")
        return

    sent = push_discord(slot_label, alerts, len(merged))
    logger.info("Discord push: %s", "sent" if sent else "NOT sent (no webhook)")


if __name__ == '__main__':
    main()
