"""
強勢股報告 — 本地新聞 context builder

讀 data/news/articles_recent.parquet + data/news/themes_core.parquet，
過濾本期 Top 30 stocks 的近 N 天文章 + theme 聚合，產出注入 prompt 的結構化資料。

供 tools/strong_stocks_ai_analysis.py 日報 (lookback 5) / 週報 (lookback 14) 共用。

設計原則:
  - 不存在 / 空檔 → 回傳空 dict 不爆 (graceful degrade, AI 仍可運作)
  - 不投入 body_full (省 token)
  - 每 (ticker, date) 至多 top 3 by confidence (避免單檔刷屏)
  - 全域 hard cap 1000 articles by confidence DESC (Opus 200K context 安全邊際)

Usage (作為 module):
    from tools.strong_stocks_news_builder import build_news_context
    ctx = build_news_context(daily_json_dict, lookback_days=5)
    # ctx["articles"] / ctx["themes"] / ctx["total_articles"] / ctx["lookback_days"]
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
ARTICLES_PATH = REPO / "data" / "news" / "articles_recent.parquet"
THEMES_PATH = REPO / "data" / "news" / "themes_core.parquet"

logger = logging.getLogger(__name__)

ARTICLE_FIELDS = [
    "date", "ticker", "title", "theme", "sentiment",
    "material_event_type", "forward_eps_change", "forward_revenue_guidance",
    "sector_tag",
]
THEME_FIELDS = ["ticker", "theme", "count", "sentiment_avg"]

CONFIDENCE_MIN = 50
TOP_ARTICLES_PER_TICKER_PER_DAY = 3
HARD_CAP_TOTAL_ARTICLES = 1000


def _parse_date_col(s: pd.Series) -> pd.Series:
    """Coerce date column to pandas datetime (tz-naive)."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s).dt.tz_localize(None) if getattr(s.dt, "tz", None) else pd.to_datetime(s)
    return pd.to_datetime(s, errors="coerce")


def _empty_context(lookback_days: int) -> dict[str, Any]:
    return {
        "articles": [],
        "themes": [],
        "lookback_days": lookback_days,
        "total_articles": 0,
        "note": "no local news available",
    }


def _extract_top_ids_sectors(daily: dict[str, Any]) -> tuple[set[str], set[str]]:
    """從 daily JSON 抓 Top 30 stock_id + 涵蓋 sector 集合."""
    ids: set[str] = set()
    sectors: set[str] = set()
    for r in (daily.get("twse_top", []) or []) + (daily.get("tpex_top", []) or []):
        sid = str(r.get("stock_id", "")).strip()
        if sid:
            ids.add(sid)
        sec = (r.get("primary_sector") or "").strip()
        if sec:
            sectors.add(sec)
    return ids, sectors


def _resolve_anchor_date(daily: dict[str, Any]) -> datetime:
    """報告對齊日: 優先 ref_date, fallback scan_date, fallback today."""
    for key in ("ref_date", "scan_date"):
        v = daily.get(key)
        if v:
            try:
                return datetime.strptime(str(v), "%Y-%m-%d")
            except ValueError:
                continue
    return datetime.now()


def build_news_context(
    daily: dict[str, Any],
    lookback_days: int,
    confidence_min: int = CONFIDENCE_MIN,
    top_per_ticker_per_day: int = TOP_ARTICLES_PER_TICKER_PER_DAY,
) -> dict[str, Any]:
    """主要 entry: 產出新聞 context dict 供 prompt 注入.

    Args:
        daily: strong_stocks_daily.json / strong_stocks_weekly.json 內容
        lookback_days: 5 (daily) / 14 (weekly)
        confidence_min: 過濾低信度文章門檻 (parquet 上的 confidence 欄位)
        top_per_ticker_per_day: 每 (ticker, date) 取前 N 篇 by confidence

    Returns:
        {
            "articles": list[dict],  # whitelist 欄位
            "themes": list[dict],
            "lookback_days": int,
            "total_articles": int,
            "note": str (optional, 退化原因),
        }
    """
    if not ARTICLES_PATH.exists():
        logger.warning("articles_recent.parquet not found: %s", ARTICLES_PATH)
        return _empty_context(lookback_days)

    try:
        df = pd.read_parquet(ARTICLES_PATH)
    except Exception as e:
        logger.warning("read articles_recent failed: %s", e)
        return _empty_context(lookback_days)

    if df.empty:
        return _empty_context(lookback_days)

    top_ids, top_sectors = _extract_top_ids_sectors(daily)
    if not top_ids:
        logger.warning("daily JSON has no twse_top / tpex_top stocks; skip news inject")
        return _empty_context(lookback_days)

    anchor = _resolve_anchor_date(daily)
    cutoff = anchor - timedelta(days=lookback_days)

    if "date" not in df.columns:
        logger.warning("articles_recent has no 'date' column; skip")
        return _empty_context(lookback_days)

    df = df.copy()
    df["date"] = _parse_date_col(df["date"])
    df = df[df["date"].notna() & (df["date"] >= cutoff) & (df["date"] <= anchor)]

    if df.empty:
        return _empty_context(lookback_days) | {"note": f"no articles in last {lookback_days} days"}

    df["ticker"] = df["ticker"].astype(str).str.strip()

    if "confidence" in df.columns:
        df = df[df["confidence"].fillna(0) >= confidence_min]

    ticker_match = df["ticker"].isin(top_ids)

    sector_match = pd.Series(False, index=df.index)
    if "article_type" in df.columns and "sector_tag" in df.columns:
        sector_match = (
            df["article_type"].astype(str).isin(["macro", "sector"])
            & df["sector_tag"].astype(str).isin(top_sectors)
        )

    df = df[ticker_match | sector_match]
    if df.empty:
        return _empty_context(lookback_days) | {
            "note": f"no relevant articles for top {len(top_ids)} stocks / {len(top_sectors)} sectors"
        }

    # De-dup: 同一篇文章可能因 multi-theme 重複出現 (parquet row per ticker-theme)
    # 優先 event_id, fallback normalized_title_hash, 最後 (ticker, date, title)
    if "event_id" in df.columns:
        df = df.drop_duplicates(subset=["event_id"], keep="first")
    elif "normalized_title_hash" in df.columns:
        df = df.drop_duplicates(subset=["normalized_title_hash"], keep="first")
    else:
        df = df.drop_duplicates(subset=["ticker", "date", "title"], keep="first")

    if "confidence" in df.columns:
        df = df.sort_values(
            ["ticker", "date", "confidence"], ascending=[True, False, False]
        )
        df = df.groupby(["ticker", df["date"].dt.date], sort=False, as_index=False).head(top_per_ticker_per_day)

    if "confidence" in df.columns:
        df = df.sort_values("confidence", ascending=False).head(HARD_CAP_TOTAL_ARTICLES)
    else:
        df = df.head(HARD_CAP_TOTAL_ARTICLES)

    articles: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        item: dict[str, Any] = {}
        for col in ARTICLE_FIELDS:
            if col not in df.columns:
                continue
            v = row.get(col)
            if pd.isna(v):
                continue
            if col == "date":
                item[col] = v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else str(v)[:10]
            elif col == "sentiment":
                try:
                    item[col] = round(float(v), 2)
                except (ValueError, TypeError):
                    continue
            else:
                s = str(v).strip()
                if s:
                    item[col] = s
        if item.get("title"):
            articles.append(item)

    themes: list[dict[str, Any]] = []
    if THEMES_PATH.exists():
        try:
            tdf = pd.read_parquet(THEMES_PATH)
            tdf["ticker"] = tdf["ticker"].astype(str).str.strip()
            tdf = tdf[tdf["ticker"].isin(top_ids)]
            if "last_seen" in tdf.columns:
                tdf["last_seen_dt"] = _parse_date_col(tdf["last_seen"])
                tdf = tdf[tdf["last_seen_dt"] >= cutoff]
            tdf = tdf.sort_values(["ticker", "count"], ascending=[True, False])
            for _, row in tdf.iterrows():
                t: dict[str, Any] = {}
                for col in THEME_FIELDS:
                    if col not in tdf.columns:
                        continue
                    v = row.get(col)
                    if pd.isna(v):
                        continue
                    if col == "count":
                        t[col] = int(v)
                    elif col == "sentiment_avg":
                        try:
                            t[col] = round(float(v), 2)
                        except (ValueError, TypeError):
                            continue
                    else:
                        s = str(v).strip()
                        if s:
                            t[col] = s
                if t.get("theme"):
                    themes.append(t)
        except Exception as e:
            logger.warning("themes_core read failed: %s", e)

    return {
        "articles": articles,
        "themes": themes,
        "lookback_days": lookback_days,
        "total_articles": len(articles),
        "anchor_date": anchor.strftime("%Y-%m-%d"),
        "cutoff_date": cutoff.strftime("%Y-%m-%d"),
    }


def format_articles_as_table(articles: list[dict[str, Any]]) -> str:
    """供 prompt 嵌入: pipe-table 格式 (與既有 prompt table style 一致)."""
    if not articles:
        return "(無相關新聞)"
    header = "| 日期 | 代號 | 標題 | 主題 | 情緒 | 事件 | 前瞻 |"
    sep = "|---|---|---|---|---|---|---|"
    rows = [header, sep]
    for a in articles:
        date = a.get("date", "-")
        tk = a.get("ticker", "-")
        title = (a.get("title", "") or "").replace("|", "/")[:80]
        theme = (a.get("theme", "") or "").replace("|", "/")[:30]
        sent = a.get("sentiment")
        sent_s = f"{sent:+.2f}" if isinstance(sent, (int, float)) else "-"
        evt = (a.get("material_event_type", "") or "").replace("|", "/")[:30]
        fwd_parts = []
        if a.get("forward_eps_change"):
            fwd_parts.append(f"EPS {a['forward_eps_change']}")
        if a.get("forward_revenue_guidance"):
            fwd_parts.append(f"營收 {a['forward_revenue_guidance']}")
        fwd = (" / ".join(fwd_parts))[:40] or "-"
        rows.append(f"| {date} | {tk} | {title} | {theme} | {sent_s} | {evt} | {fwd} |")
    return "\n".join(rows)


def format_themes_as_table(themes: list[dict[str, Any]]) -> str:
    if not themes:
        return "(無主題聚合)"
    header = "| 代號 | 主題 | 篇數 | 平均情緒 |"
    sep = "|---|---|---|---|"
    rows = [header, sep]
    for t in themes:
        tk = t.get("ticker", "-")
        theme = (t.get("theme", "") or "").replace("|", "/")[:40]
        cnt = t.get("count", "-")
        sa = t.get("sentiment_avg")
        sa_s = f"{sa:+.2f}" if isinstance(sa, (int, float)) else "-"
        rows.append(f"| {tk} | {theme} | {cnt} | {sa_s} |")
    return "\n".join(rows)


if __name__ == "__main__":
    import json as _json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python tools/strong_stocks_news_builder.py <daily.json> [lookback_days]")
        sys.exit(1)
    p = Path(sys.argv[1])
    if not p.exists():
        print(f"file not found: {p}", file=sys.stderr)
        sys.exit(1)
    lookback = int(sys.argv[2]) if len(sys.argv) >= 3 else 5
    with p.open("r", encoding="utf-8") as f:
        daily = _json.load(f)
    ctx = build_news_context(daily, lookback_days=lookback)
    print(f"=== news context: {ctx['total_articles']} articles, "
          f"{len(ctx.get('themes', []))} themes, lookback={lookback}d ===")
    print()
    print(format_articles_as_table(ctx["articles"][:20]))
    print()
    print(format_themes_as_table(ctx["themes"][:20]))
