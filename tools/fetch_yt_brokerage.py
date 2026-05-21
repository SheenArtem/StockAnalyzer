"""
Brokerage YT Stage 1: 抓台股投顧分析師 YouTube 自動/人工字幕 (VTT)

跟 fetch_yt_transcripts.py (電視財經節目 pipeline) 完全獨立：
- 不共用 SHOWS dict / data_cache/yt_transcripts/ / sector_tags_dynamic.parquet
- 投顧資料寫到 data_cache/yt_brokerage_transcripts/<brokerage>/<analyst>/

兩層 BROKERAGES 結構支援多家投顧（目前只有摩爾投顧；未來可加凱基/永誠/統一）。

輸出: data_cache/yt_brokerage_transcripts/<brokerage>/<analyst_key>/YYYYMMDD_<id>_<title>.zh-XX.vtt
排程: 整合進 run_scanner.bat 為 best-effort stage（失敗不擋後續 scanner）

CLI:
    python tools/fetch_yt_brokerage.py                       # 全投顧全分析師 (end=3)
    python tools/fetch_yt_brokerage.py --brokerage moore     # 限定投顧
    python tools/fetch_yt_brokerage.py --analyst moore_guo --end 30  # 單分析師 backfill
    python tools/fetch_yt_brokerage.py --list                # 列已下載

實作注意:
- 摩爾分析師都有人工字幕 (zh-TW / zh) — sub-lang 優先序確保人工優先於 auto
- --download-archive 避免重抓
- --match-filter "duration > 300" 排除 Shorts / 預告
- 林鈺凱新頻道 @win16888 (UC9Pd7LN9potuHVafJCLX7Pw)，舊頻道 UCIWkfrpw6l-jFhFejLrzC-A 已停更
- 摩爾官方頻道 UCpbuBb0fhCQ3QnbEp2g4-1g 只 3 部 2022 廣告片 → 不收
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT_ROOT = REPO / "data_cache" / "yt_brokerage_transcripts"

# 兩層結構: brokerage_key -> {name, analysts: {analyst_key -> {name, channel_id}}}
BROKERAGES: dict[str, dict] = {
    "moore": {
        "name": "摩爾證券投顧",
        "channel_type": "personal",  # 分析師個人頻道 (1 channel per analyst)
        "analysts": {
            "moore_guo":   {"name": "郭哲榮", "channel_id": "UChfl3auNxAxOR3wy8a8ysQQ"},
            "moore_chen":  {"name": "陳昆仁", "channel_id": "UCiBLyIFu3KjG2opa7uQHZbQ"},
            "moore_zhong": {"name": "鐘崑禎", "channel_id": "UCZn9BeImRq3SDLC8WVrVmUw"},
            "moore_zhang": {"name": "張貽程", "channel_id": "UCalPYf4c96yADeRPBIdHOxw"},
            "moore_xie":   {"name": "謝晨彥", "channel_id": "UCWNzVtz0t-e4jMKxaXqvfYA"},
            "moore_lin":   {"name": "林鈺凱", "channel_id": "UC9Pd7LN9potuHVafJCLX7Pw"},  # @win16888 新頻道
            "moore_ye":    {"name": "葉俊敏", "channel_id": "UC8r0UHqwUeAArogHqgFXDVw"},
            "moore_he":    {"name": "何基鼎", "channel_id": "UCWHR2sdmPvJSJ6TYhX2r8YQ"},  # @gd1788
        },
    },
    "yuanta": {
        "name": "元大投顧",
        "channel_type": "rotating_guest",  # 單頻道輪換來賓 (1 channel + 多分析師)
        "analysts": {
            # @yuantachannel「元大看盤室」: 主持人 + 主分析師都輪換 (實測 5/2-5/21 抓到
            # 7 位輪換主持人: 宛瑩/森寶/祥維/囿羽/義忠/智中/蔚辰). analyst_name 寫節目名,
            # 當集實際分析師由 LLM 從字幕識別寫到 host_name. 60% 影片有 manual zh 字幕
            # (看盤室解盤都有,樂活理財/投資理財 podcast 無字幕會被 skip).
            "yuanta_room": {"name": "元大看盤室", "channel_id": "UCS1bMmw249R7R0wDjAmE6CA"},
        },
    },
    # 未來: "kgi" 凱基, "yongchen" 永誠, "uni" 統一 ... (需自跑 ASR, 目前無字幕)
}


def _all_analyst_keys(brokerage: str | None = None) -> list[tuple[str, str]]:
    """回傳 [(brokerage_key, analyst_key), ...]"""
    out = []
    targets = [brokerage] if brokerage else list(BROKERAGES.keys())
    for bk in targets:
        if bk not in BROKERAGES:
            continue
        for ak in BROKERAGES[bk]["analysts"].keys():
            out.append((bk, ak))
    return out


def _channel_url(brokerage: str, analyst_key: str) -> str:
    info = BROKERAGES[brokerage]["analysts"][analyst_key]
    # 優先 channel_id，fallback handle
    if info.get("channel_id"):
        return f"https://www.youtube.com/channel/{info['channel_id']}/videos"
    if info.get("handle"):
        return f"https://www.youtube.com/{info['handle']}/videos"
    raise ValueError(f"{brokerage}/{analyst_key} missing channel_id and handle")


@dataclass
class FetchResult:
    brokerage: str
    analyst_key: str
    analyst_name: str
    new_videos: int
    total_archived: int
    error: str | None = None


def fetch_analyst(brokerage: str, analyst_key: str, playlist_end: int = 3,
                  verbose: bool = True) -> FetchResult:
    info = BROKERAGES[brokerage]["analysts"][analyst_key]
    analyst_name = info["name"]
    out_dir = OUT_ROOT / brokerage / analyst_key
    out_dir.mkdir(parents=True, exist_ok=True)
    archive = out_dir / "archive.txt"

    pre_count = 0
    if archive.exists():
        pre_count = sum(1 for line in archive.read_text(encoding="utf-8").splitlines()
                        if line.strip())

    url = _channel_url(brokerage, analyst_key)

    cmd = [
        "yt-dlp",
        "--skip-download",
        "--write-sub",                      # 人工字幕優先 (摩爾分析師都有)
        "--write-auto-sub",                 # fallback auto
        "--sub-lang", "zh-TW,zh-Hant,zh,zh-Hans,zh.*",
        "--download-archive", str(archive),
        "--playlist-end", str(playlist_end),
        "--ignore-errors",
        "--match-filter", "duration > 300",  # 排除 Shorts / 預告
        "-o", f"{out_dir}/%(upload_date)s_%(id)s_%(title).60B.%(ext)s",
        url,
    ]

    if verbose:
        sys.stderr.write(f"\n=== {brokerage}/{analyst_key} ({analyst_name}) ===\n")
        sys.stderr.write(f"  url: {url}\n")
        sys.stderr.write(f"  playlist_end: {playlist_end}\n")
        sys.stderr.write(f"  pre-archived: {pre_count}\n")

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=1200,
            encoding="utf-8", errors="replace", env=env,
        )
    except subprocess.TimeoutExpired:
        return FetchResult(brokerage, analyst_key, analyst_name, 0, pre_count,
                           "yt-dlp timeout (>20min)")
    except FileNotFoundError:
        return FetchResult(brokerage, analyst_key, analyst_name, 0, pre_count,
                           "yt-dlp not installed (pip install yt-dlp)")

    if verbose and result.stdout:
        tail = "\n".join(result.stdout.splitlines()[-8:])
        sys.stderr.write(f"  stdout tail:\n{tail}\n")
    if result.stderr:
        err_lines = [l for l in result.stderr.splitlines()
                     if "data blocks" not in l and "fragment not found" not in l]
        if err_lines and verbose:
            sys.stderr.write("  stderr (filtered):\n" + "\n".join(err_lines[:8]) + "\n")

    post_count = pre_count
    if archive.exists():
        post_count = sum(1 for line in archive.read_text(encoding="utf-8").splitlines()
                         if line.strip())

    new_videos = post_count - pre_count
    err = None if result.returncode == 0 else f"yt-dlp exit {result.returncode}"
    return FetchResult(brokerage, analyst_key, analyst_name, new_videos, post_count, err)


def list_downloaded(brokerage: str | None = None,
                    analyst_key: str | None = None) -> list[dict]:
    """列出已下載的 VTT (給下游 Stage 2 extract 用)。"""
    pairs = _all_analyst_keys(brokerage)
    if analyst_key:
        pairs = [(bk, ak) for bk, ak in pairs if ak == analyst_key]

    results = []
    for bk, ak in pairs:
        d = OUT_ROOT / bk / ak
        if not d.exists():
            continue
        info = BROKERAGES[bk]["analysts"][ak]
        for vtt in sorted(d.glob("*.vtt")):
            # YYYYMMDD_<11char-vid>_<title>.zh-XX.vtt (可能多層 lang suffix)
            stem = vtt.stem
            while "." in stem and stem.split(".")[-1].startswith("zh"):
                stem = stem.rsplit(".", 1)[0]
            if len(stem) < 20 or stem[8] != "_":
                continue
            date_str = stem[:8]
            rest = stem[9:]
            if len(rest) >= 12 and rest[11] == "_":
                video_id = rest[:11]
                title = rest[12:]
            else:
                parts = rest.split("_", 1)
                video_id = parts[0]
                title = parts[1] if len(parts) > 1 else ""
            results.append({
                "brokerage": bk,
                "brokerage_name": BROKERAGES[bk]["name"],
                "analyst_key": ak,
                "analyst_name": info["name"],
                "date": date_str,
                "video_id": video_id,
                "title": title,
                "vtt_path": str(vtt),
                "size_kb": vtt.stat().st_size // 1024,
            })
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", type=int, default=3,
                    help="抓 playlist 最新 N 部 (default 3, 日更頻道夠用)")
    ap.add_argument("--brokerage", type=str, default=None,
                    choices=list(BROKERAGES.keys()),
                    help="只抓特定投顧 (default 全抓)")
    ap.add_argument("--analyst", type=str, default=None,
                    help="只抓單一分析師 (e.g. moore_guo)")
    ap.add_argument("--list", action="store_true",
                    help="只列出已下載 VTT，不抓新的")
    args = ap.parse_args()

    if args.list:
        items = list_downloaded(args.brokerage, args.analyst)
        print(json.dumps(items, ensure_ascii=False, indent=2))
        return

    pairs = _all_analyst_keys(args.brokerage)
    if args.analyst:
        pairs = [(bk, ak) for bk, ak in pairs if ak == args.analyst]
        if not pairs:
            sys.stderr.write(f"ERROR: analyst '{args.analyst}' not found\n")
            sys.exit(2)

    summary = []
    for bk, ak in pairs:
        r = fetch_analyst(bk, ak, playlist_end=args.end)
        summary.append(r)

    sys.stderr.write("\n\n== Summary ==\n")
    for r in summary:
        status = "OK" if r.error is None else f"ERR: {r.error}"
        sys.stderr.write(
            f"  {r.brokerage}/{r.analyst_key:<15s} "
            f"({r.analyst_name}) new={r.new_videos:<4d} "
            f"total_archived={r.total_archived:<4d} [{status}]\n"
        )

    fail_count = sum(1 for r in summary if r.error)
    # best-effort: 部分失敗不算整體失敗，除非全部都掛
    sys.exit(0 if fail_count < len(summary) else 1)


if __name__ == "__main__":
    main()
