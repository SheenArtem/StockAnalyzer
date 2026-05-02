"""News Initiative #5 - Theme momentum 排行（升溫/降溫題材）

每日 scanner 跑：
1. Read archive last 30d
2. Dedupe by (event_id, ticker, theme) per BLOCKER #1 (themes_core 一致 dedupe)
3. Group by (date, theme) -> count
4. Per theme today: count_today vs count_7d_avg vs count_30d_avg
5. Heating: today >= 3 AND ratio_7d >= 2.0
6. Cooling: 30d_avg >= 3 AND today == 0 (連續沒新聞)
7. Append data/news/theme_momentum.parquet

Schema:
- detection_date / theme / count_today / count_7d_avg / count_30d_avg /
  ratio_7d / direction (heating|cooling) / top_tickers (3 個 csv) / detected_at

Council BLOCKER #7: informational only, 不入 scanner ranking.

CLI:
    python tools/theme_momentum.py
    python tools/theme_momentum.py --dry-run
    python tools/theme_momentum.py --date 2026-05-01
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / 'tools'))

from news_theme_extract import dedupe_by_event_id  # noqa: E402

ARCHIVE_DIR = REPO / 'data_cache' / 'news_archive'
OUT_PATH = REPO / 'data' / 'news' / 'theme_momentum.parquet'

# Heating / Cooling thresholds
MIN_HEAT_TODAY = 3
MIN_HEAT_RATIO = 2.0
MIN_COOL_BASELINE = 3.0   # 30d_avg minimum

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


def _load_archive_last_30d() -> pd.DataFrame:
    """Read last 2 monthly partitions (~30d), filter individual + theme non-empty."""
    today = pd.Timestamp.now().normalize()
    cutoff = today - pd.Timedelta(days=30)
    parts = []
    for offset in range(2):
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
    df = df[df['theme'].astype(str).str.strip() != ''].copy()

    df = dedupe_by_event_id(df)
    return df


def detect_momentum(df: pd.DataFrame, today: pd.Timestamp) -> pd.DataFrame:
    """Compute heating + cooling theme rows."""
    if df.empty:
        return pd.DataFrame()

    df['date_only'] = df['date'].dt.normalize()
    by_dt = df.groupby(['date_only', 'theme']).size().reset_index(name='cnt')

    today_df = by_dt[by_dt['date_only'] == today].rename(columns={'cnt': 'count_today'})

    # 7d window
    win7_start = today - pd.Timedelta(days=7)
    win7 = by_dt[(by_dt['date_only'] >= win7_start) & (by_dt['date_only'] < today)]
    avg7 = win7.groupby('theme')['cnt'].sum().reset_index()
    avg7['count_7d_avg'] = avg7['cnt'] / 7.0
    avg7 = avg7[['theme', 'count_7d_avg']]

    # 30d window
    win30_start = today - pd.Timedelta(days=30)
    win30 = by_dt[(by_dt['date_only'] >= win30_start) & (by_dt['date_only'] < today)]
    avg30 = win30.groupby('theme')['cnt'].sum().reset_index()
    avg30['count_30d_avg'] = avg30['cnt'] / 30.0
    avg30 = avg30[['theme', 'count_30d_avg']]

    # full theme universe (today ∪ avg30) so cooling also covered
    all_themes = pd.concat([
        today_df[['theme']], avg30[['theme']]
    ]).drop_duplicates()
    merged = (all_themes
              .merge(today_df[['theme', 'count_today']], on='theme', how='left')
              .merge(avg7, on='theme', how='left')
              .merge(avg30, on='theme', how='left'))
    merged['count_today'] = merged['count_today'].fillna(0).astype(int)
    merged['count_7d_avg'] = merged['count_7d_avg'].fillna(0.0)
    merged['count_30d_avg'] = merged['count_30d_avg'].fillna(0.0)

    # ratio_7d (cap at 999.99 when 7d_avg=0)
    merged['ratio_7d'] = merged.apply(
        lambda r: r['count_today'] / r['count_7d_avg'] if r['count_7d_avg'] > 0
        else 999.99 if r['count_today'] >= MIN_HEAT_TODAY else 0.0,
        axis=1
    )

    # Heating
    heat = merged[(merged['count_today'] >= MIN_HEAT_TODAY)
                  & (merged['ratio_7d'] >= MIN_HEAT_RATIO)].copy()
    heat['direction'] = 'heating'

    # Cooling: 30d_avg >= 3 AND today == 0
    cool = merged[(merged['count_30d_avg'] >= MIN_COOL_BASELINE)
                  & (merged['count_today'] == 0)].copy()
    cool['direction'] = 'cooling'

    out = pd.concat([heat, cool], ignore_index=True)
    if out.empty:
        return out

    # Top tickers per theme (use today articles for heating, last-7d for cooling)
    rows = []
    for _, ar in out.iterrows():
        theme = ar['theme']
        if ar['direction'] == 'heating':
            sub = df[(df['date_only'] == today) & (df['theme'] == theme)]
        else:
            cutoff7 = today - pd.Timedelta(days=7)
            sub = df[(df['date_only'] >= cutoff7) & (df['theme'] == theme)]
        tickers = (sub['ticker'].dropna().astype(str).str.strip()
                   .replace('', pd.NA).dropna()
                   .value_counts().head(3).index.tolist())
        rows.append({
            'detection_date': today,
            'theme': theme,
            'count_today': int(ar['count_today']),
            'count_7d_avg': float(round(ar['count_7d_avg'], 2)),
            'count_30d_avg': float(round(ar['count_30d_avg'], 2)),
            'ratio_7d': float(round(ar['ratio_7d'], 2)),
            'direction': ar['direction'],
            'top_tickers': ','.join(tickers),
            'detected_at': pd.Timestamp.now(),
        })

    out_df = pd.DataFrame(rows).sort_values(
        ['direction', 'ratio_7d', 'count_today'],
        ascending=[True, False, False]
    ).reset_index(drop=True)
    return out_df


def append_to_parquet(new_df: pd.DataFrame) -> int:
    if new_df.empty:
        if not OUT_PATH.exists():
            OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
            new_df.to_parquet(OUT_PATH, index=False)
        return 0
    if OUT_PATH.exists():
        old = pd.read_parquet(OUT_PATH)
        merged = pd.concat([old, new_df], ignore_index=True)
        merged = merged.sort_values('detected_at', ascending=False)
        merged = merged.drop_duplicates(
            subset=['detection_date', 'theme', 'direction'], keep='first')
        merged = merged.sort_values(
            ['detection_date', 'direction', 'ratio_7d'],
            ascending=[False, True, False]
        ).reset_index(drop=True)
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(OUT_PATH, index=False)
        return len(new_df)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_df.to_parquet(OUT_PATH, index=False)
    return len(new_df)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--date', type=str, default=None,
                    help='Override today (YYYY-MM-DD)')
    args = ap.parse_args()

    today = pd.Timestamp(args.date).normalize() if args.date else pd.Timestamp.now().normalize()
    logger.info("Detecting theme momentum for date=%s", today.date())

    df = _load_archive_last_30d()
    logger.info("Loaded %d archive rows last 30d (after dedupe)", len(df))
    if df.empty:
        return 0

    out = detect_momentum(df, today)
    heat_n = (out['direction'] == 'heating').sum() if not out.empty else 0
    cool_n = (out['direction'] == 'cooling').sum() if not out.empty else 0
    logger.info("Detected %d heating + %d cooling themes", heat_n, cool_n)

    if not out.empty:
        for _, r in out.head(10).iterrows():
            logger.info("  [%s] %s: today=%d 7d_avg=%.2f ratio=%.1fx tickers=%s",
                        r['direction'], r['theme'][:25], r['count_today'],
                        r['count_7d_avg'], r['ratio_7d'], r['top_tickers'])

    if args.dry_run:
        logger.info("[dry-run] not writing parquet")
        return 0

    n = append_to_parquet(out)
    logger.info("Appended %d new rows -> %s", n, OUT_PATH)
    return 0


if __name__ == '__main__':
    sys.exit(main())
