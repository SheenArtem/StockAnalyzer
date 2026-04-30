"""RAG #4 Path A POC: fetch YT earnings call VTT auto-subs.

Reads data_cache/transcripts_yt_earnings/_urls.json (ticker -> url list),
runs yt-dlp --write-auto-sub for each, saves VTT to per-ticker dir.

Output: data_cache/transcripts_yt_earnings/{ticker}/{video_id}.zh-Hant.vtt
Archive: data_cache/transcripts_yt_earnings/{ticker}/archive.txt

Designed to run in scheduled context (00:00 yt-sync window) to avoid
YT timedtext API IP-rate-limit (429) that hits during daytime testing.

CLI:
    python tools/rag_fetch_yt_earnings.py             # fetch all in _urls.json
    python tools/rag_fetch_yt_earnings.py --ticker 2330  # single ticker
    python tools/rag_fetch_yt_earnings.py --list      # list downloaded
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT_ROOT = REPO / "data_cache" / "transcripts_yt_earnings"
URLS_JSON = OUT_ROOT / "_urls.json"


def _load_urls():
    if not URLS_JSON.exists():
        return {}
    with open(URLS_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Skip "_comment" / other meta keys
    return {k: v for k, v in data.get("tickers", {}).items() if not k.startswith("_")}


def fetch_ticker(ticker: str, entries: list[dict], verbose: bool = True) -> dict:
    """Fetch VTT for all URLs of one ticker. Returns {url, status, vtt_path or err}."""
    out_dir = OUT_ROOT / ticker
    out_dir.mkdir(parents=True, exist_ok=True)
    archive = out_dir / "archive.txt"

    results = []
    for entry in entries:
        url = entry["url"]
        session = entry.get("session", "?")

        # yt-dlp args
        cmd = [
            "yt-dlp",
            "--skip-download",
            "--write-auto-sub",
            "--sub-lang", "zh-Hant,zh-Hans,zh,en",
            "--sub-format", "vtt",
            "--download-archive", str(archive),
            "--sleep-subtitles", "3",
            "--retries", "5",
            "-o", f"{out_dir}/%(id)s_{session}.%(ext)s",
            url,
        ]

        if verbose:
            sys.stderr.write(f"\n=== {ticker} {session} ===\n")
            sys.stderr.write(f"  url: {url}\n")

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600,
                encoding="utf-8", errors="replace", env=env,
            )
        except subprocess.TimeoutExpired:
            results.append({"url": url, "session": session, "status": "timeout"})
            continue
        except FileNotFoundError:
            results.append({"url": url, "session": session, "status": "yt-dlp not installed"})
            continue

        # Detect actual VTT files written
        vtts = list(out_dir.glob(f"*{session}*.vtt"))
        if vtts:
            best = max(vtts, key=lambda p: p.stat().st_size)
            results.append({
                "url": url, "session": session, "status": "ok",
                "vtt": str(best), "size_kb": best.stat().st_size // 1024,
            })
            if verbose:
                sys.stderr.write(f"  [OK] {best.name} ({best.stat().st_size // 1024} KB)\n")
        else:
            err_lines = [l for l in proc.stderr.splitlines() if "ERROR" in l or "429" in l]
            err_summary = "; ".join(err_lines[-3:]) if err_lines else f"exit {proc.returncode}"
            results.append({
                "url": url, "session": session, "status": "fail",
                "error": err_summary,
            })
            if verbose:
                sys.stderr.write(f"  [FAIL] {err_summary}\n")

    return {"ticker": ticker, "results": results}


def list_downloaded() -> list[dict]:
    items = []
    for ticker_dir in sorted(OUT_ROOT.glob("*/")):
        if ticker_dir.name.startswith("_"):
            continue
        for vtt in sorted(ticker_dir.glob("*.vtt")):
            items.append({
                "ticker": ticker_dir.name,
                "vtt": str(vtt),
                "size_kb": vtt.stat().st_size // 1024,
            })
    return items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", help="Only fetch this ticker")
    ap.add_argument("--list", action="store_true", help="List downloaded VTTs")
    args = ap.parse_args()

    if args.list:
        items = list_downloaded()
        print(json.dumps(items, ensure_ascii=False, indent=2))
        return

    tickers = _load_urls()
    if not tickers:
        sys.stderr.write(f"No URLs in {URLS_JSON}\n")
        sys.exit(1)

    if args.ticker:
        if args.ticker not in tickers:
            sys.stderr.write(f"Ticker {args.ticker} not in _urls.json\n")
            sys.exit(1)
        tickers = {args.ticker: tickers[args.ticker]}

    summary = []
    for ticker, info in tickers.items():
        entries = info.get("urls", [])
        if not entries:
            continue
        r = fetch_ticker(ticker, entries)
        summary.append(r)

    sys.stderr.write("\n\n== Summary ==\n")
    fail_count = 0
    ok_count = 0
    for r in summary:
        for sub in r["results"]:
            tag = "OK" if sub["status"] == "ok" else "FAIL"
            sys.stderr.write(f"  {r['ticker']:<6s} {sub['session']:<8s} [{tag}]")
            if sub["status"] == "ok":
                sys.stderr.write(f" {sub['size_kb']} KB\n")
                ok_count += 1
            else:
                sys.stderr.write(f" {sub.get('error', sub['status'])}\n")
                fail_count += 1

    sys.stderr.write(f"\nTotal: {ok_count} ok / {fail_count} fail\n")
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
