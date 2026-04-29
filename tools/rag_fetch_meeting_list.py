"""RAG #4 Phase 1: 從 MOPS 抓 top 300 公司法說會 PDF URL list

對每家公司每年發起一次 HTML query, 解析出該公司該年所有法說會公告的 PDF filename.
Output: data_cache/transcripts/_meta/{stock_id}_{year}.json
       data_cache/transcripts/master_pdf_list.csv (aggregated all PDF URLs)
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
UNIVERSE_CSV = ROOT / 'data_cache' / 'transcripts' / 'universe_top300.csv'
META_DIR = ROOT / 'data_cache' / 'transcripts' / '_meta'
MASTER_CSV = ROOT / 'data_cache' / 'transcripts' / 'master_pdf_list.csv'

META_DIR.mkdir(parents=True, exist_ok=True)

URL = 'https://mopsov.twse.com.tw/mops/web/ajax_t100sb02_1'
PDF_BASE = 'https://mopsov.twse.com.tw/nas/STR/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Referer': 'https://mopsov.twse.com.tw/mops/web/t100sb02_1',
}

YEARS = ['110', '111', '112', '113', '114']  # 民國 = 2021-2025
RATE_LIMIT_S = 2.0
MAX_RETRIES = 3


def fetch_meeting_list(stock_id: str, year: str, market: str = 'sii') -> list[dict]:
    """Fetch one (stock_id, year, market) HTML and parse PDF entries."""
    data = urllib.parse.urlencode({
        'encodeURIComponent': '1', 'step': '1', 'firstin': '1',
        'TYPEK': market, 'CO_ID': stock_id, 'YEAR': year, 'SEASON': '',
    }).encode()

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(URL, data=data, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                html = r.read().decode('utf-8', errors='replace')
            break
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(2 ** attempt)
    else:
        raise RuntimeError(f'Failed after {MAX_RETRIES} retries: {last_err}')

    soup = BeautifulSoup(html, 'html.parser')
    entries = []
    rows = soup.find_all('tr')
    for row in rows[1:]:  # skip header
        cells = row.find_all('td')
        if len(cells) < 5:
            continue
        # cells: [stock_id, name, date, time, type, desc, ch_pdf?, en_pdf?]
        pdf_files = []
        for a in row.find_all('a'):
            onclick = a.get('onclick', '') or ''
            m = re.search(r"fileName\.value\s*=\s*['\"]([^'\"]+\.pdf)['\"]", onclick)
            if m:
                pdf_files.append(m.group(1))

        if not pdf_files:
            continue

        # de-dup
        pdf_files = list(dict.fromkeys(pdf_files))
        date_str = cells[2].get_text(strip=True) if len(cells) > 2 else ''
        time_str = cells[3].get_text(strip=True) if len(cells) > 3 else ''
        type_str = cells[4].get_text(strip=True) if len(cells) > 4 else ''
        desc_str = cells[5].get_text(strip=True) if len(cells) > 5 else ''

        entries.append({
            'stock_id': stock_id,
            'market': market,
            'date': date_str,
            'time': time_str,
            'type': type_str,
            'desc': desc_str[:200],
            'pdfs': pdf_files,
        })
    return entries


def main():
    universe = pd.read_csv(UNIVERSE_CSV)
    universe['stock_id'] = universe['stock_id'].astype(str)
    universe['mops_market'] = universe['market'].apply(lambda m: 'sii' if m == 'TWSE' else 'otc')

    print(f'Universe: {len(universe)} stocks ({(universe["market"] == "TWSE").sum()} TWSE / '
          f'{(universe["market"] == "TPEX").sum()} TPEX)')
    print(f'Years: {YEARS}')
    total_jobs = len(universe) * len(YEARS)
    print(f'Total jobs: {total_jobs} (rate {RATE_LIMIT_S}s/req → ~{total_jobs * RATE_LIMIT_S / 60:.0f} min)')

    all_entries = []
    completed = 0
    failed = []
    t0 = time.time()
    for _, row in universe.iterrows():
        sid = row['stock_id']
        market = row['mops_market']
        for year in YEARS:
            cache_path = META_DIR / f'{sid}_{year}.json'
            if cache_path.exists():
                # Resume mode: skip if cached
                try:
                    entries = json.loads(cache_path.read_text(encoding='utf-8'))
                    all_entries.extend(entries)
                    completed += 1
                    continue
                except Exception:
                    pass

            try:
                entries = fetch_meeting_list(sid, year, market)
                cache_path.write_text(json.dumps(entries, ensure_ascii=False, indent=1),
                                      encoding='utf-8')
                all_entries.extend(entries)
                completed += 1
                if completed % 50 == 0:
                    elapsed = time.time() - t0
                    remain = (total_jobs - completed) * RATE_LIMIT_S
                    print(f'  [{completed}/{total_jobs}] {sid}_{year} '
                          f'(+{len(entries)} entries) elapsed={elapsed/60:.1f}min '
                          f'eta={remain/60:.0f}min')
                time.sleep(RATE_LIMIT_S)
            except Exception as e:
                failed.append((sid, year, str(e)))
                print(f'  [FAIL] {sid}_{year}: {e}')

    # Aggregate to master CSV
    master_rows = []
    for entry in all_entries:
        for pdf in entry['pdfs']:
            master_rows.append({
                'stock_id': entry['stock_id'],
                'market': entry['market'],
                'date': entry['date'],
                'time': entry['time'],
                'type': entry['type'],
                'desc': entry['desc'],
                'pdf_filename': pdf,
                'pdf_url': PDF_BASE + pdf,
            })
    if master_rows:
        df = pd.DataFrame(master_rows)
        # de-dup on pdf_filename
        df = df.drop_duplicates(subset=['pdf_filename']).reset_index(drop=True)
        df.to_csv(MASTER_CSV, index=False, encoding='utf-8')
        print(f'\nMaster CSV: {MASTER_CSV} ({len(df)} unique PDFs)')
        print(f'Coverage: {df["stock_id"].nunique()} unique stocks')
        print(f'Type breakdown:')
        print(df['type'].value_counts().head(10))

    if failed:
        print(f'\nFailed: {len(failed)}')
        for sid, year, err in failed[:10]:
            print(f'  {sid}_{year}: {err}')


if __name__ == '__main__':
    main()
