import pandas as pd
import pytest

from tools import refresh_backtest_panels as RBP
from tools import refresh_universe_prices as RUP


def test_panel_aggregation_drops_holiday_and_partial_dates():
    rows = []
    for sid in range(1000):
        rows.append((str(sid), '2026-07-09', 1_000_000))
        rows.append((str(sid), '2026-07-10', 0))
    for sid in range(5):
        rows.append((str(sid), '2026-07-13', 1_000_000))
    frame = pd.DataFrame(rows, columns=['stock_id', 'date', 'Volume'])

    cleaned, bad = RBP.drop_unhealthy_recent_market_dates(frame)

    assert set(pd.to_datetime(bad)) == {
        pd.Timestamp('2026-07-10'), pd.Timestamp('2026-07-13')}
    assert set(pd.to_datetime(cleaned['date'])) == {pd.Timestamp('2026-07-09')}
    assert len(cleaned) == 1000


def test_panel_health_rejects_uniformly_shrunken_universe():
    frame = pd.DataFrame({
        'stock_id': [str(i) for i in range(600)],
        'date': pd.Timestamp('2026-07-14'),
        'Volume': 1_000_000,
    })

    with pytest.raises(RuntimeError, match='no healthy recent market date'):
        RBP.drop_unhealthy_recent_market_dates(
            frame, expected_stock_count=1000)


def test_batch_health_rejects_zero_and_tiny_latest_dates():
    data = {}
    for sid in range(1000):
        dates = ['2026-07-09', '2026-07-10']
        volumes = [1_000_000, 0]
        if sid < 5:
            dates.append('2026-07-13')
            volumes.append(1_000_000)
        data[str(sid)] = pd.DataFrame(
            {'Volume': volumes}, index=pd.to_datetime(dates))

    latest, unhealthy, stats = RUP._market_date_health(data, expected_count=1000)

    assert latest == pd.Timestamp('2026-07-09')
    assert pd.Timestamp('2026-07-10') in unhealthy
    assert pd.Timestamp('2026-07-13') in unhealthy
    assert stats.loc[pd.Timestamp('2026-07-10'), 'positive_volume'] == 0


def test_official_overlay_replaces_same_day_yahoo_row():
    data = {'2330': pd.DataFrame({
        'Open': [100.0], 'High': [100.0], 'Low': [100.0], 'Close': [100.0],
        'Volume': [0.0], 'Adj Close': [99.0],
    }, index=pd.to_datetime(['2026-07-14']))}
    official = pd.DataFrame([{
        'stock_id': '2330', 'open': 101.0, 'high': 105.0, 'low': 100.0,
        'close': 104.0, 'volume': 2_000_000,
    }])

    result = RUP._merge_official_overlay(
        data, pd.Timestamp('2026-07-14'), official)

    row = result['2330'].loc[pd.Timestamp('2026-07-14')]
    assert row['Close'] == 104.0
    assert row['Volume'] == 2_000_000
    assert pd.isna(row['Adj Close'])


def test_refresh_summary_fails_on_partial_or_merge_error():
    with pytest.raises(RuntimeError, match='only 799/1000'):
        RUP._validate_refresh_summary(799, 1000, fail=0)
    with pytest.raises(RuntimeError, match='price CSV merge'):
        RUP._validate_refresh_summary(1000, 1000, fail=1)


def test_adjustment_ratio_handles_legacy_cache_without_adjusted_close():
    cached = pd.DataFrame({'Close': [100.0, 101.0]})
    assert RUP._latest_adjustment_ratio(cached) == 1.0


def test_partial_incoming_date_does_not_delete_complete_cached_history():
    cached = pd.DataFrame({
        'Open': [100.0], 'High': [105.0], 'Low': [99.0], 'Close': [104.0],
        'Volume': [2_000_000.0], 'Adj Close': [103.0],
    }, index=pd.to_datetime(['2026-07-13']))
    incoming = pd.DataFrame({
        'Open': [101.0, 104.0], 'High': [106.0, 108.0],
        'Low': [100.0, 103.0], 'Close': [105.0, 107.0],
        'Volume': [1_000_000.0, 2_000_000.0], 'Adj Close': [104.0, 106.0],
    }, index=pd.to_datetime(['2026-07-13', '2026-07-14']))

    merged = RUP._merge_cached_price_frame(
        cached, incoming, {pd.Timestamp('2026-07-13')})

    assert merged.loc[pd.Timestamp('2026-07-13'), 'Close'] == 104.0
    assert merged.loc[pd.Timestamp('2026-07-14'), 'Close'] == 107.0
