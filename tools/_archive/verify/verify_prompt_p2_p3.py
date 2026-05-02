"""一次性 verify script: P2/P3 prompt 改動效果驗證

抽 archive 中 7 篇 earnings_schema sample 文章，用新 prompt re-extract，
比對 forward_eps_change / key_capacity_event 抽取率是否從 0% 提升。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / 'tools'))

from news_theme_extract import build_extraction_prompt, call_claude_sonnet  # noqa: E402


def main():
    # 讀 archive 抓 7 篇法說會 sample（earnings_schema 7 ticker 對應的）
    archive = pd.read_parquet(REPO / 'data_cache' / 'news_archive' / '2026-04' / 'articles.parquet')
    archive_may = pd.read_parquet(REPO / 'data_cache' / 'news_archive' / '2026-05' / 'articles.parquet')
    archive = pd.concat([archive, archive_may], ignore_index=True)

    sample_event_ids = [
        '57c9d92e8f31fde2_2026-04-30',  # 2308 台達電
        '6b95ea9113e7a4b6_2026-04-30',  # 3563 牧德
        '1d75d6ffdb2bfaed_2026-04-30',  # 3105 穩懋
        'fd9dd2b37ee0240b_2026-04-30',  # 2360 致茂
        'a1b3760bbd8eed9d_2026-04-30',  # 7769 鴻勁（capacity event）
        # 5/1 兩件
    ]

    # 簡化：每個 ticker 取第一筆有 body_full 的 row
    target_tickers = ['2308', '3563', '3105', '2360', '7769', '6761', '2313']
    rows = []
    for t in target_tickers:
        sub = archive[(archive['ticker'].astype(str) == t)
                      & (archive['body_full'].astype(str).str.len() > 100)]
        if len(sub) > 0:
            r = sub.iloc[0]
            rows.append(r)

    if not rows:
        print('找不到 sample articles，archive 可能無 body_full')
        return

    articles = []
    for r in rows:
        articles.append({
            'query': str(r.get('ticker', '')),
            'date': str(r.get('date', '')),
            'source': str(r.get('source', '')),
            'title': str(r.get('title', '')),
            'summary': str(r.get('body_full', ''))[:1500],
        })

    print(f'Sample {len(articles)} articles:')
    for i, a in enumerate(articles, 1):
        print(f'  {i}. [{a["date"]}] {a["title"][:60]}')
    print()

    prompt = build_extraction_prompt(articles)
    print(f'Prompt length: {len(prompt)} chars')
    print('Calling Claude Sonnet...')
    text, err = call_claude_sonnet(prompt)
    if err:
        print(f'ERROR: {err}')
        return

    # parse JSON
    try:
        # strip optional markdown fence
        t = text.strip()
        if t.startswith('```'):
            t = t.split('```', 2)[1]
            if t.startswith('json'):
                t = t[4:]
            t = t.rsplit('```', 1)[0]
        results = json.loads(t.strip())
    except Exception as e:
        print(f'JSON parse failed: {e}')
        print(f'raw text first 500: {text[:500]}')
        return

    # report
    print()
    print('=== Re-extraction result ===')
    eps_hit = 0
    cap_hit = 0
    for r, art in zip(results, articles):
        eid = r.get('id', '?')
        eps = r.get('forward_eps_change') or '(null)'
        rev = r.get('forward_revenue_guidance') or '(null)'
        gm = r.get('forward_gross_margin') or '(null)'
        cap = r.get('key_capacity_event') or '(null)'
        q = r.get('q_period') or '(null)'
        if eps != '(null)':
            eps_hit += 1
        if cap != '(null)':
            cap_hit += 1
        print(f'{eid}. {art["title"][:50]}')
        print(f'    EPS={eps} Rev={rev} GM={gm} Cap={cap} q={q}')

    n = len(results)
    print()
    print(f'forward_eps_change hit: {eps_hit}/{n} = {eps_hit*100//n}%')
    print(f'key_capacity_event hit: {cap_hit}/{n} = {cap_hit*100//n}%')
    print()
    print('Baseline (舊 prompt v2): forward_eps_change 0/15, key_capacity_event 0/15')


if __name__ == '__main__':
    main()
