"""RAG #4 Phase 2: 從 master_pdf_list_clean.csv 下載 ~2720 PDFs

Output: data_cache/transcripts/{stock_id}/{民國date}_{filename}.pdf
Resume mode: skip if exists.
Rate limit: 2s/req sequential.

Run:
  python tools/rag_download_pdfs.py
"""
from __future__ import annotations

import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / 'data_cache' / 'transcripts' / 'master_pdf_list_clean.csv'
OUT_BASE = ROOT / 'data_cache' / 'transcripts'

PDF_BASE = 'https://mopsov.twse.com.tw/nas/STR/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Referer': 'https://mopsov.twse.com.tw/mops/web/t100sb02_1',
    'Accept': 'application/pdf,*/*',
}

RATE_LIMIT_S = 2.0
MAX_RETRIES = 3
TIMEOUT = 60  # PDF can be 1-15 MB


def download_pdf(url: str, out: Path) -> int:
    """Download single PDF; return bytes written."""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                data = r.read()
            if len(data) < 1000:
                # Sanity: PDFs always > 1KB
                raise RuntimeError(f'response too small ({len(data)} bytes), likely 404 page')
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(data)
            return len(data)
        except (urllib.error.URLError, TimeoutError, RuntimeError) as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f'Failed after {MAX_RETRIES} retries: {last_err}')


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f'Need {CSV_PATH} (run rag_fetch_meeting_list.py first)')

    df = pd.read_csv(CSV_PATH)
    df['stock_id'] = df['stock_id'].astype(str)
    print(f'Total PDFs to download: {len(df)}')

    # Existence pre-check
    def out_path_for(row) -> Path:
        sid = row['stock_id']
        # date e.g. "113/03/14" -> normalize to "113-03-14_"
        date_norm = str(row['date']).replace('/', '-').strip()
        # If date contains range like "114/01/08 ～ 114/01/16", take first
        if ' ' in date_norm:
            date_norm = date_norm.split()[0]
        fname = row['pdf_filename']
        return OUT_BASE / sid / f'{date_norm}_{fname}'

    df['out_path'] = df.apply(out_path_for, axis=1)
    df['exists'] = df['out_path'].apply(lambda p: p.exists())
    todo = df[~df['exists']].copy()
    skipped = (df['exists']).sum()
    print(f'Resume: {skipped} already exist; {len(todo)} to download')
    print(f'Estimated time: {len(todo) * RATE_LIMIT_S / 60:.0f} min @ {RATE_LIMIT_S}s/req')

    t0 = time.time()
    ok, fail = 0, []
    total_bytes = 0
    for idx, (_, row) in enumerate(todo.iterrows(), start=1):
        url = row['pdf_url']
        out = row['out_path']
        try:
            n = download_pdf(url, out)
            ok += 1
            total_bytes += n
            if idx % 50 == 0 or idx == 1:
                elapsed = time.time() - t0
                remain = (len(todo) - idx) * RATE_LIMIT_S
                print(f'  [{idx}/{len(todo)}] OK {row["stock_id"]} '
                      f'{out.name[:50]} ({n/1024:.0f} KB) '
                      f'elapsed={elapsed/60:.1f}min eta={remain/60:.0f}min')
        except Exception as e:
            fail.append((row['stock_id'], row['pdf_filename'], str(e)))
            print(f'  [FAIL] {row["stock_id"]} {row["pdf_filename"]}: {e}')
        time.sleep(RATE_LIMIT_S)

    print(f'\n=== Phase 2 Done ===')
    print(f'OK: {ok}, Failed: {len(fail)}, Total: {total_bytes/1024/1024:.1f} MB')
    if fail:
        # Save failures to CSV for retry
        fail_df = pd.DataFrame(fail, columns=['stock_id', 'pdf_filename', 'error'])
        fail_csv = OUT_BASE / 'phase2_failures.csv'
        fail_df.to_csv(fail_csv, index=False, encoding='utf-8')
        print(f'Failures saved to: {fail_csv}')


if __name__ == '__main__':
    main()
