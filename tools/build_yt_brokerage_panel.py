"""
Brokerage YT Stage 3: 彙整 data_cache/yt_brokerage_extracts/**/* .json
→ data/yt_brokerage_mentions.parquet (mention-level)
+ data/yt_brokerage_videos.parquet (video-level)

跟 build_yt_sector_panel.py 獨立 — **不寫進** sector_tags_dynamic.parquet 或
yt_videos_panel.parquet，保留電視節目 pipeline 純淨。

CLI:
  python tools/build_yt_brokerage_panel.py                # 重建 panel
  python tools/build_yt_brokerage_panel.py --top          # 印最新 7 日熱度 top 20
  python tools/build_yt_brokerage_panel.py --since 30     # 限定近 30 日
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
EXTRACTS_DIR = REPO / "data_cache" / "yt_brokerage_extracts"
OUT_MENTIONS = REPO / "data" / "yt_brokerage_mentions.parquet"
OUT_VIDEOS = REPO / "data" / "yt_brokerage_videos.parquet"


def load_all_extracts(since_days: int | None = None) -> list[dict]:
    """讀 data_cache/yt_brokerage_extracts/<brokerage>/<analyst>/*.json"""
    records = []
    cutoff = None
    if since_days is not None:
        cutoff = (datetime.now() - timedelta(days=since_days)).strftime("%Y-%m-%d")

    if not EXTRACTS_DIR.exists():
        return records

    for brok_dir in EXTRACTS_DIR.iterdir():
        if not brok_dir.is_dir():
            continue
        for analyst_dir in brok_dir.iterdir():
            if not analyst_dir.is_dir():
                continue
            for f in sorted(analyst_dir.glob("*.json")):
                try:
                    d = json.loads(f.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError) as e:
                    print(f"  [WARN] skip {f.name}: {e}", file=sys.stderr)
                    continue
                if cutoff and d.get("date", "") < cutoff:
                    continue
                if "error" in d:
                    continue
                records.append(d)
    return records


def flatten_mentions(records: list[dict]) -> list[dict]:
    """mention-level rows (1 mention = 1 row)"""
    rows = []
    for rec in records:
        base = {
            "date": rec.get("date", ""),
            "brokerage": rec.get("brokerage", ""),
            "brokerage_name": rec.get("brokerage_name", ""),
            "analyst_key": rec.get("analyst_key", ""),
            "analyst_name": rec.get("analyst_name", ""),
            "video_id": rec.get("video_id", ""),
            "video_title": rec.get("title", ""),
            "extracted_by_model": rec.get("extracted_by_model", ""),
            "extracted_at": rec.get("extracted_at", ""),
        }
        for m in rec.get("mentions", []):
            ticker = (m.get("ticker") or "").strip()
            name = (m.get("name") or "").strip()
            if not name and not ticker:
                continue

            sent_str = str(m.get("sentiment", "0")).strip()
            try:
                sentiment = int(sent_str.replace("+", ""))
                sentiment = max(-1, min(1, sentiment))
            except ValueError:
                sentiment = 0

            tags = m.get("tag", []) or []
            if not isinstance(tags, list):
                tags = [str(tags)]

            def _f(v):
                if v is None or v == "":
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            rows.append({
                **base,
                "ticker": ticker,
                "name": name,
                "sentiment": sentiment,
                "tags": tags,
                "thesis": (m.get("thesis") or "").strip(),
                "confidence": int(m.get("confidence") or 0),
                "entry": _f(m.get("entry")),
                "stop": _f(m.get("stop")),
                "target": _f(m.get("target")),
                "timeframe": (m.get("timeframe") or "unspecified").strip(),
                "ticker_suspicious": bool(m.get("ticker_suspicious", False)),
            })
    return rows


def flatten_videos(records: list[dict]) -> list[dict]:
    """video-level rows (1 影片 = 1 row, 保留分析師大盤觀點)"""
    rows = []
    for rec in records:
        themes = rec.get("themes_discussed", []) or []
        if not isinstance(themes, list):
            themes = [str(themes)]
        rows.append({
            "date": rec.get("date", ""),
            "brokerage": rec.get("brokerage", ""),
            "brokerage_name": rec.get("brokerage_name", ""),
            "analyst_key": rec.get("analyst_key", ""),
            "analyst_name": rec.get("analyst_name", ""),
            "host_name": (rec.get("host_name") or "").strip(),
            "video_id": rec.get("video_id", ""),
            "title": rec.get("title", ""),
            "themes_discussed": themes,
            "macro_views": (rec.get("macro_views") or "").strip(),
            "analyst_view": (rec.get("analyst_view") or "").strip(),
            "recommended_action": (rec.get("recommended_action") or "").strip(),
            "risk_warning": (rec.get("risk_warning") or "").strip(),
            "mention_count": len(rec.get("mentions", []) or []),
            "extracted_by_model": rec.get("extracted_by_model", ""),
            "extracted_at": rec.get("extracted_at", ""),
        })
    return rows


def build_mentions_panel(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "date", "brokerage", "brokerage_name", "analyst_key", "analyst_name",
            "video_id", "video_title", "ticker", "name", "sentiment", "tags",
            "thesis", "confidence", "entry", "stop", "target", "timeframe",
            "ticker_suspicious", "extracted_by_model", "extracted_at",
        ])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    df = df.sort_values(
        ["date", "brokerage", "analyst_key", "video_id", "ticker"]
    ).reset_index(drop=True)
    return df


def build_videos_panel(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "date", "brokerage", "brokerage_name", "analyst_key", "analyst_name",
            "host_name", "video_id", "title", "themes_discussed", "macro_views",
            "analyst_view", "recommended_action", "risk_warning", "mention_count",
            "extracted_by_model", "extracted_at",
        ])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    df = df.sort_values(
        ["date", "brokerage", "analyst_key", "video_id"]
    ).reset_index(drop=True)
    return df


def summarize(df_m: pd.DataFrame, df_v: pd.DataFrame, top_n: int = 20, days: int = 7) -> None:
    if df_m.empty:
        print("(empty mention panel)", file=sys.stderr)
        return

    latest = df_m["date"].max()
    cutoff = latest - timedelta(days=days - 1)
    recent = df_m[df_m["date"] >= cutoff]

    print("\n=== Mentions panel ===", file=sys.stderr)
    print(f"  Total rows: {len(df_m)}", file=sys.stderr)
    print(f"  Date range: {df_m['date'].min()} ~ {df_m['date'].max()}", file=sys.stderr)
    print(f"  Brokerages: {df_m['brokerage'].unique().tolist()}", file=sys.stderr)
    print(f"  Analysts: {df_m['analyst_key'].nunique()}", file=sys.stderr)
    print(f"  Videos: {df_m['video_id'].nunique()}", file=sys.stderr)
    print(f"  Unique tickers: {df_m['ticker'].nunique()}", file=sys.stderr)
    print(f"  Suspicious tickers: {df_m['ticker_suspicious'].sum()}", file=sys.stderr)
    priced = ((df_m["entry"].notna()) | (df_m["stop"].notna()) | (df_m["target"].notna())).sum()
    print(f"  Mentions with price points: {priced}", file=sys.stderr)

    print(f"\n=== Videos panel ===", file=sys.stderr)
    print(f"  Total videos: {len(df_v)}", file=sys.stderr)
    if not df_v.empty:
        action_counts = df_v["recommended_action"].value_counts().to_dict()
        print(f"  Action distribution: {action_counts}", file=sys.stderr)

    print(f"\n=== Top {top_n} mentioned tickers (last {days}d: {cutoff} ~ {latest}) ===",
          file=sys.stderr)
    agg = recent.groupby(["ticker", "name"]).agg(
        mention_count=("video_id", "count"),
        analyst_count=("analyst_key", "nunique"),
        sentiment_avg=("sentiment", "mean"),
        confidence_avg=("confidence", "mean"),
    ).sort_values("mention_count", ascending=False).head(top_n)
    for idx, row in agg.iterrows():
        ticker, name = idx if isinstance(idx, tuple) else (idx, "")
        print(
            f"  {ticker:<6s} {name:<12s} mentions={int(row['mention_count']):<3d} "
            f"analysts={int(row['analyst_count'])} sent={row['sentiment_avg']:+.2f} "
            f"conf={row['confidence_avg']:.0f}",
            file=sys.stderr,
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", type=int, default=None, help="只彙整近 N 日 JSON")
    ap.add_argument("--top", action="store_true", help="只印 summary 不重建 parquet")
    ap.add_argument("--days", type=int, default=7, help="summary 視窗 (default 7)")
    args = ap.parse_args()

    records = load_all_extracts(since_days=args.since)
    print(f"Loaded {len(records)} extracted JSONs", file=sys.stderr)

    mention_rows = flatten_mentions(records)
    df_m = build_mentions_panel(mention_rows)
    print(f"Flattened to {len(df_m)} mention rows", file=sys.stderr)

    video_rows = flatten_videos(records)
    df_v = build_videos_panel(video_rows)
    print(f"Built {len(df_v)} video-level rows", file=sys.stderr)

    if not args.top:
        OUT_MENTIONS.parent.mkdir(parents=True, exist_ok=True)
        df_m.to_parquet(OUT_MENTIONS, index=False)
        size_kb = OUT_MENTIONS.stat().st_size / 1024
        print(f"Written: {OUT_MENTIONS} ({size_kb:.1f} KB, {len(df_m)} rows)",
              file=sys.stderr)

        df_v.to_parquet(OUT_VIDEOS, index=False)
        v_size_kb = OUT_VIDEOS.stat().st_size / 1024
        print(f"Written: {OUT_VIDEOS} ({v_size_kb:.1f} KB, {len(df_v)} rows)",
              file=sys.stderr)

    summarize(df_m, df_v, top_n=20, days=args.days)


if __name__ == "__main__":
    main()
