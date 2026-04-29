"""RAG #4 Phase 3: 從 2720 PDFs 抽文字 + chunk + embed + chromadb

Output:
  - data_cache/transcripts/_chromadb (chromadb persistent client)
  - collection 'transcripts_top300' with metadata: ticker, date, pdf, speaker, chunk_idx
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path
import multiprocessing as mp

import pandas as pd
import pdfplumber
from sentence_transformers import SentenceTransformer
import chromadb

ROOT = Path(__file__).resolve().parent.parent
TRANSCRIPTS_DIR = ROOT / 'data_cache' / 'transcripts'
DB_DIR = TRANSCRIPTS_DIR / '_chromadb'
DB_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_TARGET = 400
COLLECTION_NAME = 'transcripts_top300'

FOOTER_PATTERNS = [
    r'郵箱\s*:\s*ir@[^\s\n]+\s*\|\s*官網\s*:\s*https?://[^\s\n]+',
    r'(?m)^\s*\d{1,3}\s*$',  # page number alone
]


def _clean_text(text: str) -> str:
    for pat in FOOTER_PATTERNS:
        text = re.sub(pat, '', text)
    # strip empty lines
    lines = [ln for ln in text.split('\n') if ln.strip()]
    return '\n'.join(lines).strip()


def _chunk(text: str, target: int = CHUNK_TARGET) -> list[str]:
    raw = [p.strip() for p in re.split(r'\n\s*\n', text) if p.strip()]
    chunks = []
    buf = ''
    for para in raw:
        if len(para) > target * 1.5:
            sents = re.split(r'(?<=[。！？\?\!])\s*', para)
            for sent in sents:
                if not sent.strip():
                    continue
                if len(buf) + len(sent) > target:
                    if buf:
                        chunks.append(buf)
                    buf = sent
                else:
                    buf = (buf + sent) if buf else sent
        else:
            if len(buf) + len(para) > target:
                if buf:
                    chunks.append(buf)
                buf = para
            else:
                buf = (buf + '\n' + para) if buf else para
    if buf:
        chunks.append(buf)
    return chunks


# Speaker detection generic
SPEAKER_PATTERNS = [
    (r'(董事長|Chairman|chairman)', 'Chairman'),
    (r'(財務長|CFO|cfo|Chief Financial)', 'CFO'),
    (r'(執行長|CEO|ceo|Chief Executive)', 'CEO'),
    (r'(總經理|President|president|Co-CEO)', 'President'),
    (r'(發言人|Spokesperson|spokesperson)', 'Spokesperson'),
    (r'(投資人關係|IR\s|Investor Relations|IRO)', 'IR'),
    (r'(分析師|Analyst|UBS|Morgan Stanley|JP Morgan|Citi|花旗|凱基|富邦|元大|永豐)', 'Analyst'),
    (r'(記者|Reporter|工商時報|經濟日報|聯合報|民視|中央社|三立|電視台|時報)', 'Reporter'),
]


def _detect_speaker(chunk: str) -> str:
    head = chunk[:120]
    for pat, role in SPEAKER_PATTERNS:
        if re.search(pat, head):
            return role
    return 'Unknown'


def _extract_pdf(pdf_path: Path) -> tuple[str, list[str]] | None:
    """Worker: extract text from one PDF, return (full_text, chunks)."""
    try:
        with pdfplumber.open(pdf_path) as p:
            pages = []
            for page in p.pages:
                t = page.extract_text() or ''
                pages.append(t)
        full = _clean_text('\n\n'.join(pages))
        if len(full) < 200:
            return None  # too short, skip
        chunks = _chunk(full)
        return full, chunks
    except Exception as e:
        return None


def _process_pdf_batch(args):
    """For multiprocessing pool."""
    pdf_path = args
    result = _extract_pdf(pdf_path)
    if result is None:
        return (str(pdf_path), None, None)
    full, chunks = result
    return (str(pdf_path), len(full), chunks)


def main():
    # Find all PDFs (excluding POC 2317_honhai)
    all_pdfs = sorted([p for p in TRANSCRIPTS_DIR.glob('*/*.pdf')
                       if p.parts[-2] != '2317_honhai'])
    print(f'Total PDFs: {len(all_pdfs)}')

    # Stage 1: extract + chunk (multiprocessing)
    t0 = time.time()
    print(f'\n=== Stage A: extract + chunk (multiprocessing {mp.cpu_count()} workers) ===')
    n_workers = max(1, mp.cpu_count() - 1)

    results = []
    with mp.Pool(n_workers) as pool:
        for i, res in enumerate(pool.imap_unordered(_process_pdf_batch, all_pdfs, chunksize=4), start=1):
            results.append(res)
            if i % 200 == 0 or i == 1:
                print(f'  [{i}/{len(all_pdfs)}] processed (elapsed {(time.time()-t0)/60:.1f} min)')

    valid = [(p, n, c) for p, n, c in results if c is not None]
    failed = [p for p, n, c in results if c is None]
    print(f'\nExtracted: {len(valid)} valid / {len(failed)} failed')
    print(f'Total chunks: {sum(len(c) for _, _, c in valid)}')
    print(f'Stage A time: {(time.time()-t0)/60:.1f} min')

    # Stage 2: load embedding model + embed all chunks (batched)
    print(f'\n=== Stage B: load model + embed ===')
    t1 = time.time()
    model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    print(f'Model loaded in {time.time()-t1:.1f}s')

    # Stage 3: chromadb insert
    client = chromadb.PersistentClient(path=str(DB_DIR))
    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass
    col = client.create_collection(COLLECTION_NAME, metadata={'hnsw:space': 'cosine'})

    print(f'\n=== Stage C: chunk → chromadb (batches of 256) ===')
    t2 = time.time()
    BATCH = 256
    pending_docs, pending_metas, pending_ids = [], [], []
    total_inserted = 0

    for pdf_path_str, _, chunks in valid:
        pdf_path = Path(pdf_path_str)
        # Path: data_cache/transcripts/{stock_id}/{date}_{filename}.pdf
        stock_id = pdf_path.parts[-2]
        # Filename pattern: {民國date}_{full_filename}.pdf
        # extract just date from prefix
        name_parts = pdf_path.stem.split('_', 1)
        date_str = name_parts[0] if name_parts else ''
        pdf_filename = name_parts[1] if len(name_parts) > 1 else pdf_path.stem

        for i, ch in enumerate(chunks):
            # Use full pdf_filename (e.g. 231720240314M001) for uniqueness
            chunk_id = f'{stock_id}_{date_str}_{pdf_filename}_c{i:03d}'
            pending_ids.append(chunk_id)
            pending_docs.append(ch)
            pending_metas.append({
                'ticker': stock_id,
                'date': date_str,
                'pdf_filename': pdf_filename,
                'chunk_idx': i,
                'speaker': _detect_speaker(ch),
                'char_count': len(ch),
            })
            if len(pending_docs) >= BATCH:
                embs = model.encode(pending_docs, show_progress_bar=False, batch_size=64)
                col.add(ids=pending_ids, documents=pending_docs,
                        metadatas=pending_metas, embeddings=embs.tolist())
                total_inserted += len(pending_docs)
                if total_inserted % 5120 == 0:
                    print(f'  inserted {total_inserted} chunks (elapsed {(time.time()-t2)/60:.1f} min)')
                pending_docs, pending_metas, pending_ids = [], [], []

    # Flush remainder
    if pending_docs:
        embs = model.encode(pending_docs, show_progress_bar=False, batch_size=64)
        col.add(ids=pending_ids, documents=pending_docs,
                metadatas=pending_metas, embeddings=embs.tolist())
        total_inserted += len(pending_docs)

    print(f'\n=== Phase 3 Done ===')
    print(f'Total chunks inserted: {total_inserted}')
    print(f'Collection size: {col.count()}')
    print(f'Stage C time: {(time.time()-t2)/60:.1f} min')
    print(f'Total Phase 3 time: {(time.time()-t0)/60:.1f} min')

    # Save metadata
    import json
    meta = {
        'collection': COLLECTION_NAME,
        'model': 'paraphrase-multilingual-MiniLM-L12-v2',
        'embedding_dim': 384,
        'chunk_target': CHUNK_TARGET,
        'total_chunks': total_inserted,
        'pdfs_processed': len(valid),
        'pdfs_failed': len(failed),
    }
    (DB_DIR / 'metadata_top300.json').write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    print(f'\nMetadata: {DB_DIR / "metadata_top300.json"}')


if __name__ == '__main__':
    if sys.platform == 'win32':
        mp.freeze_support()
    main()
