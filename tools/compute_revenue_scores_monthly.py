"""
compute_revenue_scores_monthly.py
==================================
從 data_cache/backtest/financials_revenue.parquet (月營收) 計算每支股票每月
依法定公告日可用的 1m 單月 YoY revenue_score。

輸出: data_cache/backtest/revenue_scores_monthly.parquet
Schema: stock_id, date (次月 10 日可用日, pd.Timestamp), revenue_score (0-100)

VF-VC 驗證 (2026-04-20) 結論: 1m 單月 YoY IR +0.335，quarterly walk-forward
+0.615 (A)。季度更新太慢 (IR -0.757)，必須月度更新才能抓到 alpha。
"""
from __future__ import annotations

import logging
import math
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_cache" / "backtest"
IN_PATH = DATA_DIR / "financials_revenue.parquet"
OUT_PATH = DATA_DIR / "revenue_scores_monthly.parquet"
MIN_LATEST_PERIOD_COVERAGE = 0.80

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("rev_monthly")


def yoy_to_score(yoy_latest: float, yoy_prev: float | None) -> float:
    score = 50.0
    if pd.isna(yoy_latest):
        return score
    if yoy_latest > 0:
        score += 10
    elif yoy_prev is not None and not pd.isna(yoy_prev):
        if abs(yoy_latest - yoy_prev) >= 0.5:
            if yoy_latest > yoy_prev:
                score += min(20, (yoy_latest - yoy_prev) * 2)
            else:
                score -= min(20, abs(yoy_latest - yoy_prev) * 2)
    return max(0.0, min(100.0, score))


def _prepare_revenue_input(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate FinMind/MOPS schema and attach the canonical revenue period."""
    required = {'stock_id', 'date', 'revenue', 'revenue_year', 'revenue_month'}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"monthly revenue input missing columns: {sorted(missing)}")

    df = frame.copy()
    if df['stock_id'].isna().any():
        raise RuntimeError("monthly revenue input contains missing stock_id")
    df['stock_id'] = df['stock_id'].astype(str)
    years = pd.to_numeric(df['revenue_year'], errors='coerce')
    months = pd.to_numeric(df['revenue_month'], errors='coerce')
    invalid_period = (
        years.isna() | months.isna()
        | years.mod(1).ne(0) | months.mod(1).ne(0)
        | months.lt(1) | months.gt(12)
    )
    if invalid_period.any():
        raise RuntimeError(
            f"monthly revenue input has {int(invalid_period.sum())} invalid year/month rows")

    periods = pd.PeriodIndex.from_fields(
        year=years.astype(int), month=months.astype(int), freq='M')
    raw_dates = pd.to_datetime(df['date'], errors='coerce').dt.normalize()
    canonical_raw_dates = pd.Series(
        (periods + 1).to_timestamp(), index=df.index, dtype='datetime64[ns]')
    date_mismatch = raw_dates.isna() | raw_dates.ne(canonical_raw_dates)
    if date_mismatch.any():
        sample = df.loc[date_mismatch, ['stock_id', 'date',
                                        'revenue_year', 'revenue_month']].head(3)
        raise RuntimeError(
            "monthly revenue raw date must equal the first day after its revenue "
            f"month; mismatches={int(date_mismatch.sum())}, sample="
            f"{sample.to_dict(orient='records')}")

    df['_period'] = periods
    df['date'] = raw_dates
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue']).copy()
    if df.empty:
        raise RuntimeError("monthly revenue input has no numeric revenue rows")
    if df.duplicated(['stock_id', '_period']).any():
        raise RuntimeError("monthly revenue input contains duplicate stock/period rows")
    return df.sort_values(['stock_id', '_period']).reset_index(drop=True)


def _score_revenue_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Score exact calendar-month comparisons; missing months never shift offsets."""
    out_rows = []
    for i, (sid, group) in enumerate(df.groupby('stock_id', sort=False)):
        if (i + 1) % 500 == 0:
            logger.info("  [%d/%d] scoring...", i + 1, df['stock_id'].nunique())
        values = dict(zip(group['_period'], group['revenue']))
        for period, latest in zip(group['_period'], group['revenue']):
            yr_ago = values.get(period - 12)
            if yr_ago is None or yr_ago <= 0:
                continue
            yoy_latest = (latest / yr_ago - 1) * 100

            yoy_prev = None
            prev = values.get(period - 3)
            yr_ago_prev = values.get(period - 15)
            if prev is not None and yr_ago_prev is not None and yr_ago_prev > 0:
                yoy_prev = (prev / yr_ago_prev - 1) * 100

            out_rows.append({
                'stock_id': sid,
                '_period': period,
                # Raw date is the next-month first.  The legal deadline is the
                # 10th, so this score first becomes usable on that date.
                'date': (period + 1).start_time + pd.Timedelta(days=9),
                'revenue_score': yoy_to_score(yoy_latest, yoy_prev),
            })
    return pd.DataFrame(
        out_rows, columns=['stock_id', '_period', 'date', 'revenue_score'])


def _validate_latest_period_coverage(
    raw: pd.DataFrame,
    scored: pd.DataFrame,
    previous_latest_count: int = 0,
    min_ratio: float = MIN_LATEST_PERIOD_COVERAGE,
) -> None:
    """Reject a partial latest monthly cross-section before replacing output."""
    if not 0 < min_ratio <= 1:
        raise ValueError("latest-period coverage ratio must be in (0, 1]")
    latest_period = raw['_period'].max()
    raw_counts = raw.groupby('_period')['stock_id'].nunique().sort_index()
    raw_reference = int(raw_counts.tail(13).max())
    raw_latest = int(raw_counts.get(latest_period, 0))
    raw_required = max(1, math.ceil(raw_reference * min_ratio))
    if raw_latest < raw_required:
        raise RuntimeError(
            f"raw latest-period coverage collapsed for {latest_period}: "
            f"{raw_latest} stocks, required at least {raw_required} from "
            f"recent reference {raw_reference}")

    score_counts = scored.groupby('_period')['stock_id'].nunique().sort_index()
    recent_score_reference = int(score_counts.tail(13).max()) if len(score_counts) else 0
    score_reference = max(recent_score_reference, int(previous_latest_count))
    score_latest = int(score_counts.get(latest_period, 0))
    score_required = max(1, math.ceil(score_reference * min_ratio))
    if score_latest < score_required:
        raise RuntimeError(
            f"scored latest-period coverage collapsed for {latest_period}: "
            f"{score_latest} stocks, required at least {score_required} from "
            f"reference {score_reference}")


def main():
    logger.info("Loading monthly revenue parquet...")
    df = _prepare_revenue_input(pd.read_parquet(IN_PATH))
    logger.info("  %d rows, %d stocks, %s - %s",
                len(df), df['stock_id'].nunique(),
                df['date'].min().date(), df['date'].max().date())

    out = _score_revenue_rows(df)
    if out.empty:
        raise RuntimeError("monthly revenue scoring produced no rows")
    eligible_stocks = int(
        df.groupby('stock_id')['_period'].nunique().ge(13).sum())
    previous_stocks = 0
    previous_latest_count = 0
    if OUT_PATH.exists():
        try:
            previous = pd.read_parquet(OUT_PATH, columns=['stock_id', 'date'])
            previous['stock_id'] = previous['stock_id'].astype(str)
            previous['date'] = pd.to_datetime(previous['date'], errors='coerce')
            previous_stocks = int(previous['stock_id'].nunique())
            previous_max = previous['date'].max()
            if pd.notna(previous_max):
                previous_latest_count = int(
                    previous.loc[previous['date'].eq(previous_max), 'stock_id'].nunique())
        except Exception as exc:
            raise RuntimeError(
                f"cannot validate existing revenue score output: {exc}") from exc
    coverage_reference = max(eligible_stocks, previous_stocks)
    required_stocks = max(1, math.ceil(coverage_reference * 0.80))
    output_stocks = int(out['stock_id'].astype(str).nunique())
    if output_stocks < required_stocks:
        raise RuntimeError(
            f"monthly revenue score coverage collapsed: {output_stocks} stocks, "
            f"required at least {required_stocks} from reference {coverage_reference}")
    if out.duplicated(['stock_id', 'date']).any():
        raise RuntimeError("monthly revenue scores contain duplicate stock/date rows")
    _validate_latest_period_coverage(
        df, out, previous_latest_count=previous_latest_count)
    latest_period = df['_period'].max()
    expected_latest = (latest_period + 1).start_time + pd.Timedelta(days=9)
    actual_latest = pd.to_datetime(out['date']).max()
    if actual_latest != expected_latest:
        raise RuntimeError(
            f"monthly revenue scores end at {actual_latest.date()}, expected "
            f"{expected_latest.date()} from raw input")
    logger.info("Built %d (stock, month) rows", len(out))
    logger.info("  distribution: min=%.1f p25=%.1f p50=%.1f p75=%.1f max=%.1f mean=%.2f std=%.2f",
                out['revenue_score'].min(),
                out['revenue_score'].quantile(0.25),
                out['revenue_score'].quantile(0.5),
                out['revenue_score'].quantile(0.75),
                out['revenue_score'].max(),
                out['revenue_score'].mean(),
                out['revenue_score'].std())

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = OUT_PATH.with_name(f".{OUT_PATH.name}.{os.getpid()}.tmp")
    try:
        out.drop(columns=['_period']).to_parquet(tmp_path, index=False)
        os.replace(tmp_path, OUT_PATH)
    finally:
        tmp_path.unlink(missing_ok=True)
    logger.info("Saved: %s", OUT_PATH)


if __name__ == "__main__":
    main()
