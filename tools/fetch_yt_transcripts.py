"""
E1 Stage 1: 抓 3 財經節目 YouTube 自動字幕 (VTT)

3 節目 playlist:
- 錢線百分百 (非凡電視 USTV)     每日 Mon-Fri
- 理財達人秀 (東森 EBCmoneyshow)  每日
- 鈔錢部署 (華視 + 盧燕俐)        Tue/Thu 20:00

輸出: data_cache/yt_transcripts/<show_key>/<YYYYMMDD>_<id>_<title>.zh-TW.vtt
排程: run_yt_sync.bat 每日 21:30 (避開 22:00 scanner 時段)

CLI:
    python tools/fetch_yt_transcripts.py                 # 抓過去 7 天
    python tools/fetch_yt_transcripts.py --days 30       # 抓過去 30 天 (backfill)
    python tools/fetch_yt_transcripts.py --show money100 # 只抓特定節目

實作注意:
- 用 --download-archive 避免重抓（已下載的影片 ID 會記在 archive.txt）
- 自動字幕 (--write-auto-sub) 品質優於人工字幕（這些節目很少有人工字幕）
- yt-dlp 印的 "Did not get any data blocks" 是 live stream 尾段警告，**非錯誤**
- VTT 保留時間碼（LLM 可自行過濾，不必預清洗）
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT_ROOT = REPO / "data_cache" / "yt_transcripts"

SHOWS = {
    "money100": {
        "name": "錢線百分百",
        "url": "https://www.youtube.com/playlist?list=PLlAWMYbuVkC_x_Hfk6vuA8FhWicFgzCN6",
        "source": "USTV 非凡電視",
        "schedule": "Mon-Fri 21:00 (YT 完整版 22:30/23:00/23:30)",
    },
    "money_deploy": {
        "name": "鈔錢部署",
        "url": "https://www.youtube.com/playlist?list=PLR2vWjaKlfQSpDclqTigQyBrJ6QCCWuV6",
        "source": "華視 + 盧燕俐",
        "schedule": "Tue/Thu 20:00-21:00",
    },
    # 曾嘗試但使用者 2026-04-24 要求 revert 的節目 (不加入 SHOWS):
    # - moneyshow 理財達人秀 (東森): YT 完全無字幕
    # - non_fan_stock 非凡股市現場
    # - guo_zherong 郭哲榮分析師
    # - non_fan_news 非凡財經新聞
}


@dataclass
class FetchResult:
    show_key: str
    new_videos: int
    total_archived: int
    error: str | None = None


def fetch_show(show_key: str, playlist_end: int = 10, verbose: bool = True) -> FetchResult:
    if show_key not in SHOWS:
        return FetchResult(show_key, 0, 0, f"unknown show: {show_key}")

    show = SHOWS[show_key]
    out_dir = OUT_ROOT / show_key
    out_dir.mkdir(parents=True, exist_ok=True)
    archive = out_dir / "archive.txt"

    # count pre-existing archived IDs
    pre_count = 0
    if archive.exists():
        pre_count = sum(1 for line in archive.read_text(encoding="utf-8").splitlines() if line.strip())

    cmd = [
        "yt-dlp",
        "--skip-download",
        "--write-sub",           # manual subtitles 優先
        "--write-auto-sub",      # fallback auto-generated
        "--sub-lang", "zh-TW,zh-Hant,zh-Hans,zh,zh.*",  # 台版/繁體/簡體/自動翻譯通吃
        "--download-archive", str(archive),
        "--playlist-end", str(playlist_end),  # 最新 N 部 (按新 → 舊排序)
        "--ignore-errors",
        "--match-filter", "duration > 300",  # 排除 Shorts / 預告 (<5 min)
        "-o", f"{out_dir}/%(upload_date)s_%(id)s_%(title).60B.%(ext)s",
        show["url"],  # playlist 或 channel /videos URL
    ]

    if verbose:
        sys.stderr.write(f"\n=== {show_key} ({show['name']}) ===\n")
        sys.stderr.write(f"  url: {show['url']}\n")
        sys.stderr.write(f"  playlist_end: {playlist_end}\n")
        sys.stderr.write(f"  pre-archived: {pre_count}\n")

    # 設 env PYTHONIOENCODING=utf-8 避免 CP950 encode error in Windows cmd
    import os
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=1200,
            encoding="utf-8", errors="replace", env=env,
        )
    except subprocess.TimeoutExpired:
        return FetchResult(show_key, 0, pre_count, "yt-dlp timeout (>20min)")
    except FileNotFoundError:
        return FetchResult(show_key, 0, pre_count, "yt-dlp not installed (pip install yt-dlp)")

    if verbose and result.stdout:
        tail = "\n".join(result.stdout.splitlines()[-10:])
        sys.stderr.write(f"  stdout tail:\n{tail}\n")
    if result.stderr:
        err_lines = [l for l in result.stderr.splitlines()
                     if "data blocks" not in l and "fragment not found" not in l]
        if err_lines and verbose:
            sys.stderr.write(f"  stderr (filtered):\n" + "\n".join(err_lines[:10]) + "\n")

    # Count post
    post_count = pre_count
    if archive.exists():
        post_count = sum(1 for line in archive.read_text(encoding="utf-8").splitlines() if line.strip())

    new_videos = post_count - pre_count
    err = None if result.returncode == 0 else f"yt-dlp exit {result.returncode}"
    return FetchResult(show_key, new_videos, post_count, err)


def list_downloaded(show_key: str | None = None) -> list[dict]:
    """列出已下載的 VTT 檔（給下游 Stage 2 extract 使用）。"""
    shows = [show_key] if show_key else list(SHOWS.keys())
    # Backwards compat: also include deprecated moneyshow dir if exists
    shows = list(dict.fromkeys(shows + (['moneyshow'] if (OUT_ROOT / 'moneyshow').exists() else [])))
    results = []
    for sk in shows:
        show_dir = OUT_ROOT / sk
        if not show_dir.exists():
            continue
        for vtt in sorted(show_dir.glob("*.vtt")):
            # Filename format: YYYYMMDD_videoid_title.zh-TW.vtt (or zh-Hant.vtt)
            # Use video id from filename as unique key
            stem = vtt.stem.rsplit(".", 1)[0]  # strip zh-TW / zh-Hant lang suffix
            parts = stem.split("_", 2)
            if len(parts) < 3:
                continue
            date_str, video_id, title = parts[0], parts[1], parts[2]
            results.append({
                "show_key": sk,
                "show_name": SHOWS[sk]["name"],
                "date": date_str,
                "video_id": video_id,
                "title": title,
                "vtt_path": str(vtt),
                "size_kb": vtt.stat().st_size // 1024,
            })
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", type=int, default=5, help="抓 playlist 最新 N 部 (default 5)")
    ap.add_argument("--show", type=str, default=None, choices=list(SHOWS.keys()),
                    help="只抓特定節目 (default 全抓)")
    ap.add_argument("--list", action="store_true", help="只列出已下載檔案，不抓新的")
    args = ap.parse_args()

    if args.list:
        items = list_downloaded(args.show)
        print(json.dumps(items, ensure_ascii=False, indent=2))
        return

    targets = [args.show] if args.show else list(SHOWS.keys())
    summary = []
    for show_key in targets:
        r = fetch_show(show_key, playlist_end=args.end)
        summary.append(r)

    sys.stderr.write("\n\n== Summary ==\n")
    for r in summary:
        status = "OK" if r.error is None else f"ERR: {r.error}"
        sys.stderr.write(f"  {r.show_key:<15s} new={r.new_videos:<4d} total_archived={r.total_archived:<4d} [{status}]\n")

    fail_count = sum(1 for r in summary if r.error)
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
