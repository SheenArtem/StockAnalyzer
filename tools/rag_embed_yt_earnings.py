"""RAG #4 Path A POC: parse YT earnings VTTs + chunk + add to chromadb.

Reads VTTs from data_cache/transcripts_yt_earnings/{ticker}/*.vtt.
Adds chunks to existing 'transcripts_top300' collection with metadata
source='yt' so retrieve can be filtered.

Idempotent: uses col.upsert (chromadb >= 0.4) to skip already-added chunks.

CLI:
    python tools/rag_embed_yt_earnings.py             # process all
    python tools/rag_embed_yt_earnings.py --ticker 2330
    python tools/rag_embed_yt_earnings.py --dry-run   # parse only, no embed
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
YT_DIR = ROOT / "data_cache" / "transcripts_yt_earnings"
DB_DIR = ROOT / "data_cache" / "transcripts" / "_chromadb"

CHUNK_TARGET = 400
COLLECTION_NAME = "transcripts_top300"


def parse_vtt(vtt_path: Path) -> str:
    """Strip timestamps + cue numbers + tags from VTT, dedup consecutive lines."""
    text = vtt_path.read_text(encoding="utf-8", errors="replace")
    out = []
    last = ""
    for ln in text.split("\n"):
        ln = ln.strip()
        if not ln:
            continue
        if ln == "WEBVTT" or ln.startswith("NOTE") or ln.startswith("Kind:") or ln.startswith("Language:"):
            continue
        if "-->" in ln:
            continue
        if ln.isdigit():
            continue
        # Strip auto-sub coloring tags <c.colorXXXX>...</c> and timestamps <00:00:00.000>
        ln = re.sub(r"<[^>]+>", "", ln)
        ln = ln.strip()
        if ln and ln != last:
            out.append(ln)
            last = ln
    return "\n".join(out)


def _chunk(text: str, target: int = CHUNK_TARGET) -> list[str]:
    """Same chunk strategy as rag_extract_embed.py: paragraph + sentence fallback."""
    raw = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    # YT VTT often single-line stream with no paragraph breaks; treat each line as candidate
    if len(raw) == 1 and len(raw[0]) > target * 4:
        # Re-split on newlines
        raw = [ln.strip() for ln in text.split("\n") if ln.strip()]
    chunks = []
    buf = ""
    for para in raw:
        if len(para) > target * 1.5:
            sents = re.split(r"(?<=[。！？\?\!])\s*", para)
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
                buf = (buf + "\n" + para) if buf else para
    if buf:
        chunks.append(buf)
    return chunks


def process_ticker(ticker: str, model, col, dry_run: bool = False) -> dict:
    ticker_dir = YT_DIR / ticker
    if not ticker_dir.exists():
        return {"ticker": ticker, "status": "no_dir", "chunks": 0}

    vtts = sorted(ticker_dir.glob("*.vtt"))
    if not vtts:
        return {"ticker": ticker, "status": "no_vtts", "chunks": 0}

    total_chunks = 0
    per_vtt = []
    for vtt in vtts:
        text = parse_vtt(vtt)
        if len(text) < 200:
            per_vtt.append({"vtt": vtt.name, "text_chars": len(text), "status": "too_short"})
            continue
        chunks = _chunk(text)
        per_vtt.append({"vtt": vtt.name, "text_chars": len(text), "chunks": len(chunks)})

        if dry_run:
            total_chunks += len(chunks)
            continue

        # Extract video_id + session from filename: {video_id}_{session}.lang.vtt
        # e.g. tlZl5Au0kMA_2025Q4.zh-Hant.vtt
        stem = vtt.stem
        # strip lang suffix
        for lang in (".zh-Hant", ".zh-Hans", ".zh", ".en"):
            if stem.endswith(lang):
                stem = stem[: -len(lang)]
                break
        parts = stem.split("_", 1)
        video_id = parts[0] if parts else stem
        session = parts[1] if len(parts) > 1 else "?"

        # Embed + add (upsert to skip existing IDs)
        ids = [f"yt_{ticker}_{video_id}_c{i:03d}" for i in range(len(chunks))]
        metas = [{
            "ticker": ticker,
            "video_id": video_id,
            "session": session,
            "chunk_idx": i,
            "source": "yt",
            "char_count": len(c),
        } for i, c in enumerate(chunks)]

        embs = model.encode(chunks, show_progress_bar=False, batch_size=32)
        col.upsert(ids=ids, documents=chunks, metadatas=metas, embeddings=embs.tolist())
        total_chunks += len(chunks)

    return {"ticker": ticker, "status": "ok", "chunks": total_chunks, "per_vtt": per_vtt}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", help="Only process this ticker")
    ap.add_argument("--dry-run", action="store_true", help="Parse + chunk only, no embed")
    args = ap.parse_args()

    # Discover tickers
    tickers = sorted([
        p.name for p in YT_DIR.glob("*/")
        if not p.name.startswith("_") and p.is_dir()
    ])
    if args.ticker:
        if args.ticker not in tickers:
            sys.stderr.write(f"Ticker {args.ticker} dir not found in {YT_DIR}\n")
            sys.exit(1)
        tickers = [args.ticker]
    if not tickers:
        sys.stderr.write(f"No ticker dirs in {YT_DIR}\n")
        sys.exit(1)

    model = None
    col = None
    if not args.dry_run:
        from sentence_transformers import SentenceTransformer
        import chromadb
        t0 = time.time()
        model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
        client = chromadb.PersistentClient(path=str(DB_DIR))
        col = client.get_collection(COLLECTION_NAME)
        sys.stderr.write(f"Model + collection loaded in {time.time()-t0:.1f}s "
                         f"(existing chunks: {col.count()})\n")

    results = []
    for t in tickers:
        r = process_ticker(t, model, col, dry_run=args.dry_run)
        results.append(r)
        sys.stderr.write(f"  {t}: {r['status']} ({r.get('chunks', 0)} chunks)\n")

    if not args.dry_run and col is not None:
        sys.stderr.write(f"\nFinal collection size: {col.count()}\n")

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
