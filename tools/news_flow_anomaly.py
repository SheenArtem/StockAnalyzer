"""News Initiative #4 - News flow 異常偵測 (catalyst alert)

每日 scanner 跑：
1. Read archive last 8d (today + 7 days back)
2. Dedupe by (event_id, ticker) per BLOCKER #1
3. Compute per ticker: count_today vs count_7d_avg
4. Anomaly: today >= 3 AND today >= 3x 7d_avg
5. Append data/news/news_flow_anomaly.parquet

Schema:
- detection_date: pd.Timestamp (today)
- ticker: str
- count_today: int
- count_7d_avg: float
- ratio: float (today / 7d_avg)
- top_themes: str (comma sep top 3 themes today)
- top_titles: str ('|' sep top 3 titles today)
- max_confidence: int (highest LLM confidence among today's articles)
- detected_at: pd.Timestamp

Council BLOCKER #7: informational only, 不入 scanner 排序。Backtest 閘門:
- archive >= 6 月後跑 cross-sectional decile ret 5d/20d
- baseline_strip 同 sector + 同 regime CAR
- pass gate: 4/5 OOS year + LOO COVID/2022 不顛覆 sign
- 不過閘門前: derived parquet + Discord push + AI 報告 [NEWS_FLOW_ALERT] 段

CLI:
    python tools/news_flow_anomaly.py
    python tools/news_flow_anomaly.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / 'tools'))

from news_theme_extract import dedupe_by_event_ticker  # noqa: E402

ARCHIVE_DIR = REPO / 'data_cache' / 'news_archive'
OUT_PATH = REPO / 'data' / 'news' / 'news_flow_anomaly.parquet'
UNIVERSE_PATH = REPO / 'data_cache' / 'backtest' / 'universe_tw_full.parquet'

# Anomaly thresholds (Council BLOCKER #7 spec)
MIN_COUNT_TODAY = 3
MIN_RATIO = 3.0

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


def _load_archive_last_8d() -> pd.DataFrame:
    """Read last 2 monthly partitions, filter individual + ticker non-empty."""
    today = pd.Timestamp.now().normalize()
    cutoff = today - pd.Timedelta(days=8)
    parts = []
    for offset in range(2):  # this month + last month
        target = today - pd.DateOffset(months=offset)
        partition = target.strftime('%Y-%m')
        path = ARCHIVE_DIR / partition / 'articles.parquet'
        if path.exists():
            parts.append(pd.read_parquet(path))
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df[df['date'] >= cutoff].copy()

    if 'article_type' in df.columns:
        df = df[df['article_type'] == 'individual']
    df = df[df['ticker'].astype(str).str.strip() != ''].copy()

    df = dedupe_by_event_ticker(df)
    return df


def _load_universe_names() -> dict:
    if not UNIVERSE_PATH.exists():
        return {}
    try:
        u = pd.read_parquet(UNIVERSE_PATH)
        name_col = next((c for c in ('stock_name', 'name', '名稱') if c in u.columns), None)
        if name_col:
            return dict(zip(u['stock_id'].astype(str), u[name_col]))
    except Exception:
        pass
    return {}


def detect_anomalies(df: pd.DataFrame, today: pd.Timestamp) -> pd.DataFrame:
    """Compute (today vs 7d_avg) per ticker, return anomaly rows."""
    if df.empty:
        return pd.DataFrame()

    # Group by (date, ticker)
    df['date_only'] = df['date'].dt.normalize()
    by_dt = df.groupby(['date_only', 'ticker']).size().reset_index(name='cnt')

    today_df = by_dt[by_dt['date_only'] == today]
    if today_df.empty:
        return pd.DataFrame()

    # 7d window = today - 7 ~ today - 1 (excluding today)
    win_start = today - pd.Timedelta(days=7)
    win = by_dt[(by_dt['date_only'] >= win_start) & (by_dt['date_only'] < today)]

    avg_7d = win.groupby('ticker')['cnt'].sum().reset_index()
    avg_7d['avg_7d'] = avg_7d['cnt'] / 7.0

    merged = today_df.merge(avg_7d[['ticker', 'avg_7d']], on='ticker', how='left')
    merged['avg_7d'] = merged['avg_7d'].fillna(0.0)
    merged = merged.rename(columns={'cnt': 'count_today'})

    # ratio: count_today / avg_7d (handle zero baseline)
    merged['ratio'] = merged.apply(
        lambda r: r['count_today'] / r['avg_7d'] if r['avg_7d'] > 0
        else float('inf') if r['count_today'] >= MIN_COUNT_TODAY else 0.0,
        axis=1
    )

    # Anomaly filter
    anom = merged[(merged['count_today'] >= MIN_COUNT_TODAY)
                  & (merged['ratio'] >= MIN_RATIO)].copy()
    if anom.empty:
        return pd.DataFrame()

    # Attach top_themes + top_titles + max_confidence (from today's articles)
    name_map = _load_universe_names()
    rows = []
    for _, ar in anom.iterrows():
        ticker = ar['ticker']
        sub = df[(df['date_only'] == today) & (df['ticker'] == ticker)]
        themes = (sub['theme'].dropna().astype(str).str.strip()
                  .replace('', pd.NA).dropna().unique())
        themes_str = ','.join(themes[:3])
        titles = sub['title'].dropna().astype(str).str.strip().head(3).tolist()
        titles_str = ' | '.join(t[:60] for t in titles)
        max_conf = int(sub['confidence'].max() or 0) if not sub.empty else 0

        rows.append({
            'detection_date': today,
            'ticker': ticker,
            'company_name': name_map.get(ticker, ''),
            'count_today': int(ar['count_today']),
            'count_7d_avg': float(round(ar['avg_7d'], 2)),
            'ratio': float(round(ar['ratio'], 2)) if ar['ratio'] != float('inf') else 999.99,
            'top_themes': themes_str,
            'top_titles': titles_str,
            'max_confidence': max_conf,
            'detected_at': pd.Timestamp.now(),
        })
    return pd.DataFrame(rows).sort_values('ratio', ascending=False).reset_index(drop=True)


def append_to_parquet(new_df: pd.DataFrame) -> int:
    """Append new anomaly rows to parquet, dedupe by (detection_date, ticker)."""
    if new_df.empty:
        # 仍寫一次空 parquet 確保 file 存在
        if not OUT_PATH.exists():
            OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
            new_df.to_parquet(OUT_PATH, index=False)
        return 0
    if OUT_PATH.exists():
        old = pd.read_parquet(OUT_PATH)
        merged = pd.concat([old, new_df], ignore_index=True)
        merged = merged.sort_values('detected_at', ascending=False)
        merged = merged.drop_duplicates(
            subset=['detection_date', 'ticker'], keep='first')
        merged = merged.sort_values('detection_date', ascending=False).reset_index(drop=True)
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(OUT_PATH, index=False)
        return len(new_df)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_df.to_parquet(OUT_PATH, index=False)
    return len(new_df)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='不寫 parquet')
    ap.add_argument('--date', type=str, default=None,
                    help='Override today (YYYY-MM-DD), default = today')
    args = ap.parse_args()

    today = pd.Timestamp(args.date).normalize() if args.date else pd.Timestamp.now().normalize()
    logger.info("Detecting anomalies for date=%s", today.date())

    df = _load_archive_last_8d()
    logger.info("Loaded %d archive rows last 8d (after dedupe)", len(df))
    if df.empty:
        logger.warning("No archive data; skip")
        return 0

    anom = detect_anomalies(df, today)
    logger.info("Detected %d anomaly tickers (today>=%d AND ratio>=%.1fx)",
                len(anom), MIN_COUNT_TODAY, MIN_RATIO)

    if not anom.empty:
        for _, r in anom.head(10).iterrows():
            logger.info("  %s %s: today=%d avg_7d=%.2f ratio=%.1fx themes=%s",
                        r['ticker'], r['company_name'][:6],
                        r['count_today'], r['count_7d_avg'], r['ratio'],
                        r['top_themes'][:30])

    if args.dry_run:
        logger.info("[dry-run] not writing parquet")
        return 0

    n = append_to_parquet(anom)
    logger.info("Appended %d new rows -> %s", n, OUT_PATH)
    return 0


if __name__ == '__main__':
    sys.exit(main())
