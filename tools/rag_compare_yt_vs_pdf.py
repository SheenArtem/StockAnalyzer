"""RAG #4 Path A POC: compare retrieve quality PDF-only vs PDF+YT.

For a target ticker, runs the same multi-query that ai_report.py uses,
then splits results by metadata.source ('yt' vs default PDF) to compute:
  - baseline_top1_sim: top-1 sim from PDF chunks only
  - combined_top1_sim: top-1 sim from PDF + YT chunks
  - incremental_sim: combined - baseline (gate >= +0.10 to scale Path A)
  - yt_in_top5: how many YT chunks rank top 5 in combined view

Run AFTER tools/rag_embed_yt_earnings.py has added YT chunks.

CLI:
    python tools/rag_compare_yt_vs_pdf.py --ticker 2330
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_DIR = ROOT / "data_cache" / "transcripts" / "_chromadb"
COLLECTION_NAME = "transcripts_top300"

# Mirrors ai_report.py:_build_law_transcript_rag queries
QUERIES = [
    "明年 全年 業績展望 營收 預期",
    "重要新產品 客戶 出貨 進展",
    "AI 半導體 產能 毛利率",
]

BOILERPLATE_PATTERNS = [
    re.compile(r"著作權所有|All [Rr]ights [Rr]eserved|©\s*20\d\d", re.UNICODE),
    re.compile(r"免責聲明|投資安全聲明|[Dd]isclaimer", re.UNICODE),
    re.compile(r"預測性陳述|預測性資訊|[Ff]orward.?looking [Ss]tatements?", re.UNICODE),
    re.compile(r"簡報內所提供之資訊|本簡報.{0,20}(發佈|提供|揭露)", re.UNICODE),
    re.compile(r"第[一二三四1234]\s*季法人說明會\s*$|[Ii]nvestor [Cc]onference\s*$",
               re.UNICODE | re.MULTILINE),
    re.compile(r"[Cc]opyright|商業機密|本公司未來實際所發生", re.UNICODE),
]


def is_boilerplate(text: str) -> bool:
    return sum(1 for p in BOILERPLATE_PATTERNS if p.search(text)) >= 2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--n-fetch", type=int, default=20,
                    help="over-fetch n per query for split (default 20)")
    args = ap.parse_args()

    from sentence_transformers import SentenceTransformer
    import chromadb

    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    client = chromadb.PersistentClient(path=str(DB_DIR))
    col = client.get_collection(COLLECTION_NAME)

    # Per-source top-1 sim across all queries (multi-query merged)
    pdf_best = {"sim": -1, "doc": "", "meta": {}}
    yt_best = {"sim": -1, "doc": "", "meta": {}}
    combined_top5 = []  # (sim, source, doc[:80], meta)

    for q in QUERIES:
        qe = model.encode([q])[0].tolist()
        res = col.query(
            query_embeddings=[qe],
            n_results=args.n_fetch,
            where={"ticker": args.ticker},
        )
        if not res["documents"] or not res["documents"][0]:
            continue
        for doc, meta, dist in zip(
            res["documents"][0], res["metadatas"][0], res["distances"][0]
        ):
            if is_boilerplate(doc):
                continue
            sim = 1.0 - dist
            source = meta.get("source", "pdf")
            short = doc.replace("\n", " ").strip()[:80]
            combined_top5.append((sim, source, short, meta, q))
            if source == "yt":
                if sim > yt_best["sim"]:
                    yt_best = {"sim": sim, "doc": doc[:200], "meta": meta, "query": q}
            else:
                if sim > pdf_best["sim"]:
                    pdf_best = {"sim": sim, "doc": doc[:200], "meta": meta, "query": q}

    combined_top5.sort(key=lambda x: -x[0])
    top5 = combined_top5[:5]
    yt_in_top5 = sum(1 for r in top5 if r[1] == "yt")

    baseline = pdf_best["sim"] if pdf_best["sim"] >= 0 else 0.0
    yt_top1 = yt_best["sim"] if yt_best["sim"] >= 0 else 0.0
    combined = max(baseline, yt_top1)
    incremental = combined - baseline

    print("=" * 60)
    print(f"Ticker: {args.ticker}")
    print(f"Collection size: {col.count()}")
    print("-" * 60)
    print(f"PDF-only top-1 sim:    {baseline:.3f}")
    if pdf_best["sim"] >= 0:
        print(f"  Query: {pdf_best.get('query','?')}")
        print(f"  Doc:   {pdf_best['doc'][:120].replace(chr(10),' / ')}")
    print(f"YT-only top-1 sim:     {yt_top1:.3f}")
    if yt_best["sim"] >= 0:
        print(f"  Query: {yt_best.get('query','?')}")
        print(f"  Doc:   {yt_best['doc'][:120].replace(chr(10),' / ')}")
    print("-" * 60)
    print(f"Combined top-1 sim:    {combined:.3f}")
    print(f"Incremental sim:       {incremental:+.3f}  (gate: +0.100)")
    print(f"YT chunks in top-5:    {yt_in_top5} / 5")
    print("-" * 60)
    print("Combined top-5 (sim | source | snippet):")
    for sim, source, doc, meta, q in top5:
        tag = "[YT]" if source == "yt" else "[PDF]"
        print(f"  {sim:.3f} {tag:<5s} {doc}")
    print("=" * 60)

    # Verdict
    if yt_top1 < 0:
        verdict = "FAIL: no YT chunks retrieved (no YT data in collection?)"
    elif incremental >= 0.10:
        verdict = "PASS: incremental sim >= +0.10 -> consider Whisper scale to top 50"
    elif incremental >= 0.05:
        verdict = "MARGINAL: incremental +0.05~+0.10 -> defer; weigh against Whisper cost"
    else:
        verdict = "FAIL: incremental < +0.05 -> abort Path A, YT VTT not worth it"
    print(f"\nVERDICT: {verdict}")

    # JSON for programmatic consumption
    out = {
        "ticker": args.ticker,
        "baseline_top1_sim": round(baseline, 3),
        "yt_top1_sim": round(yt_top1, 3),
        "combined_top1_sim": round(combined, 3),
        "incremental_sim": round(incremental, 3),
        "yt_in_top5": yt_in_top5,
        "verdict": verdict,
    }
    out_path = ROOT / "data_cache" / "transcripts_yt_earnings" / f"compare_{args.ticker}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
