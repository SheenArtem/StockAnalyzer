"""
E1 Stage 3: 彙整 data_cache/yt_extracts/*/*.json → data/sector_tags_dynamic.parquet

Input:
  data_cache/yt_extracts/<show_key>/<date>_<video_id>.json

Output (Level 1 raw mentions, Parquet columns):
  date (datetime64[D]), ticker, name, show_key, show_name,
  video_id, video_title, sentiment (int8: -1/0/+1),
  tags (list[str]), thesis, confidence (int), ticker_suspicious (bool),
  extracted_by_model, extracted_at

Level 2 aggregates (pandas groupby on-the-fly):
  per (date, ticker): mention_count, show_count, sentiment_avg, tags_union, ...

CLI:
  python tools/build_yt_sector_panel.py                 # 重建 panel
  python tools/build_yt_sector_panel.py --top           # 印最新 7 日熱度 top 20
  python tools/build_yt_sector_panel.py --since 7       # 限制近 7 日 merge (incremental)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
YT_EXTRACTS_DIR = REPO / "data_cache" / "yt_extracts"
OUT_PATH = REPO / "data" / "sector_tags_dynamic.parquet"
# Video-level panel (2026-04-25, Wave 1 #8 + #9): themes_discussed / macro_views / guests
OUT_VIDEOS_PATH = REPO / "data" / "yt_videos_panel.parquet"


def load_all_extracts(since_days: int | None = None) -> list[dict]:
    """讀全部 yt_extracts JSON，可選 since 過濾。"""
    records = []
    cutoff = None
    if since_days is not None:
        cutoff = (datetime.now() - timedelta(days=since_days)).strftime("%Y-%m-%d")

    for show_dir in YT_EXTRACTS_DIR.iterdir() if YT_EXTRACTS_DIR.exists() else []:
        if not show_dir.is_dir():
            continue
        for f in sorted(show_dir.glob("*.json")):
            try:
                d = json.loads(f.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                print(f"  [WARN] skip {f.name}: {e}", file=sys.stderr)
                continue

            if cutoff and d.get("date", "") < cutoff:
                continue
            if "error" in d:
                continue  # failed extracts

            records.append(d)
    return records


def flatten_to_rows(records: list[dict]) -> list[dict]:
    """每個 mention 展平成一行；同 record 多 mentions → 多行。"""
    rows = []
    for rec in records:
        base = {
            "date": rec.get("date", ""),
            "show_key": rec.get("show_key", ""),
            "show_name": rec.get("show_name", ""),
            "video_id": rec.get("video_id", ""),
            "video_title": rec.get("title", ""),
            "extracted_by_model": rec.get("extracted_by_model", ""),
            "extracted_at": rec.get("extracted_at", ""),
        }
        for m in rec.get("mentions", []):
            ticker = (m.get("ticker") or "").strip()
            name = (m.get("name") or "").strip()
            if not name and not ticker:
                continue  # 空 mention 跳過

            sent_str = str(m.get("sentiment", "0")).strip()
            try:
                sentiment = int(sent_str.replace("+", ""))
                sentiment = max(-1, min(1, sentiment))
            except ValueError:
                sentiment = 0

            tags = m.get("tag", []) or []
            if not isinstance(tags, list):
                tags = [str(tags)]

            rows.append({
                **base,
                "ticker": ticker,
                "name": name,
                "sentiment": sentiment,
                "tags": tags,
                "thesis": (m.get("thesis") or "").strip(),
                "confidence": int(m.get("confidence") or 0),
                "ticker_suspicious": bool(m.get("ticker_suspicious", False)),
            })
    return rows


def flatten_videos(records: list[dict]) -> list[dict]:
    """每 record 一行，保留 video-level 欄位 (themes / macro / guests)。

    Wave 1 #8 (themes->ticker 展開) + #9 (macro_views dashboard) 共用此 panel。
    """
    rows = []
    for rec in records:
        themes = rec.get("themes_discussed", []) or []
        if not isinstance(themes, list):
            themes = [str(themes)]
        guests = rec.get("guests", []) or []
        if not isinstance(guests, list):
            guests = [str(guests)]

        rows.append({
            "date": rec.get("date", ""),
            "show_key": rec.get("show_key", ""),
            "show_name": rec.get("show_name", ""),
            "video_id": rec.get("video_id", ""),
            "video_title": rec.get("title", ""),
            "guests": guests,
            "themes_discussed": themes,
            "macro_views": (rec.get("macro_views") or "").strip(),
            "mention_count": len(rec.get("mentions", []) or []),
            "extracted_by_model": rec.get("extracted_by_model", ""),
            "extracted_at": rec.get("extracted_at", ""),
        })
    return rows


def build_videos_panel(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "date", "show_key", "show_name", "video_id", "video_title",
            "guests", "themes_discussed", "macro_views", "mention_count",
            "extracted_by_model", "extracted_at",
        ])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    df = df.sort_values(["date", "show_key", "video_id"]).reset_index(drop=True)
    return df


def build_panel(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "date", "ticker", "name", "show_key", "show_name",
            "video_id", "video_title", "sentiment", "tags", "thesis",
            "confidence", "ticker_suspicious", "extracted_by_model", "extracted_at",
        ])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    df = df.sort_values(["date", "show_key", "video_id", "ticker"]).reset_index(drop=True)
    return df


def summarize(df: pd.DataFrame, top_n: int = 20, days: int = 7) -> None:
    """Level 2 aggregates for stderr 顯示。"""
    if df.empty:
        print("(empty panel)", file=sys.stderr)
        return

    latest_date = df["date"].max()
    cutoff = latest_date - timedelta(days=days - 1)
    recent = df[df["date"] >= cutoff]

    print(f"\n=== Panel stats ===", file=sys.stderr)
    print(f"  Total rows: {len(df)}", file=sys.stderr)
    print(f"  Date range: {df['date'].min()} ~ {df['date'].max()}", file=sys.stderr)
    print(f"  Shows: {df['show_key'].unique().tolist()}", file=sys.stderr)
    print(f"  Videos: {df['video_id'].nunique()}", file=sys.stderr)
    print(f"  Unique tickers: {df['ticker'].nunique()}", file=sys.stderr)
    print(f"  Suspicious tickers: {df['ticker_suspicious'].sum()}", file=sys.stderr)

    print(f"\n=== Top {top_n} most-mentioned tickers (last {days} days: {cutoff} to {latest_date}) ===", file=sys.stderr)
    agg = recent.groupby(["ticker", "name"]).agg(
        mention_count=("video_id", "count"),
        show_count=("show_key", "nunique"),
        sentiment_avg=("sentiment", "mean"),
        sentiment_plus=("sentiment", lambda s: (s > 0).sum()),
        confidence_avg=("confidence", "mean"),
    ).sort_values("mention_count", ascending=False).head(top_n)

    for idx, row in agg.iterrows():
        ticker, name = idx if isinstance(idx, tuple) else (idx, "")
        print(f"  {ticker:<6s} {name:<12s} mentions={int(row['mention_count']):<3d} "
              f"shows={int(row['show_count'])} sent_avg={row['sentiment_avg']:+.2f} "
              f"conf={row['confidence_avg']:.0f}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", type=int, default=None,
                    help="只彙整近 N 天 JSON (default 全部)")
    ap.add_argument("--top", action="store_true",
                    help="只印 summary 不重建 parquet")
    ap.add_argument("--days", type=int, default=7,
                    help="summary 計算熱度的視窗 (default 7)")
    args = ap.parse_args()

    records = load_all_extracts(since_days=args.since)
    print(f"Loaded {len(records)} extracted JSONs", file=sys.stderr)

    rows = flatten_to_rows(records)
    print(f"Flattened to {len(rows)} mention rows", file=sys.stderr)

    df = build_panel(rows)

    video_rows = flatten_videos(records)
    df_videos = build_videos_panel(video_rows)
    print(f"Built {len(df_videos)} video-level rows", file=sys.stderr)

    if not args.top:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUT_PATH, index=False)
        size_kb = OUT_PATH.stat().st_size / 1024
        print(f"Written: {OUT_PATH} ({size_kb:.1f} KB, {len(df)} rows)", file=sys.stderr)

        df_videos.to_parquet(OUT_VIDEOS_PATH, index=False)
        v_size_kb = OUT_VIDEOS_PATH.stat().st_size / 1024
        print(f"Written: {OUT_VIDEOS_PATH} ({v_size_kb:.1f} KB, {len(df_videos)} rows)", file=sys.stderr)

    summarize(df, top_n=20, days=args.days)


if __name__ == "__main__":
    main()
