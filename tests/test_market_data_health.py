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


def _panel(per_day_counts, start='2025-01-01'):
    """組一段面板：{天數偏移: 當日有量檔數} → DataFrame[stock_id, date, Volume]。"""
    rows = []
    for offset, n in enumerate(per_day_counts):
        day = (pd.Timestamp(start) + pd.Timedelta(days=offset)).strftime('%Y-%m-%d')
        for sid in range(n):
            rows.append((str(sid), day, 1_000_000))
    return pd.DataFrame(rows, columns=['stock_id', 'date', 'Volume'])


def test_in_market_baseline_ignores_stocks_that_stopped_trading():
    """絕對門檻的分母必須自我修正。

    2026-08-02 實測：磁碟上 2,064 支 *_price.csv 含 99 檔減資後的舊股別（`*O`，
    2026-04-08 起停止交易、`is_common_stock=False`），CSV 永久保留且 `is_tw_ticker()`
    照樣匹配，所以舊分母 `len(tw_csv)` 只會單向變大。改由面板歷史推導後，停更的股票
    會隨時間滑出視窗。
    """
    # 前 100 天有 200 檔，之後 60 天只剩 150 檔（50 檔停止交易）
    frame = _panel([200] * 100 + [150] * 60)

    dates = pd.to_datetime(frame['date']).dt.normalize()
    cutoff = dates.max() - pd.Timedelta(days=45)
    baseline = RBP._in_market_baseline(dates, frame, cutoff, history_days=180)

    # 基準取近期視窗「之前」的歷史；此例含停更前後兩段，中位數落在 200
    assert baseline is not None
    assert 150 <= baseline <= 200

    # 視窗完全落在停更之後時，基準要收斂到 150
    later = _panel([150] * 200)
    ldates = pd.to_datetime(later['date']).dt.normalize()
    lcut = ldates.max() - pd.Timedelta(days=45)
    assert RBP._in_market_baseline(ldates, later, lcut, history_days=180) == 150


def test_in_market_baseline_returns_none_when_history_too_short():
    frame = _panel([200] * 5)
    dates = pd.to_datetime(frame['date']).dt.normalize()
    cutoff = dates.max() - pd.Timedelta(days=45)

    assert RBP._in_market_baseline(dates, frame, cutoff) is None


def test_in_market_baseline_ignores_zero_volume_rows():
    """有 row 但無量（冷門股無成交日）不算在市檔數。"""
    rows = []
    for offset in range(120):
        day = (pd.Timestamp('2025-01-01') + pd.Timedelta(days=offset)).strftime('%Y-%m-%d')
        for sid in range(100):
            rows.append((str(sid), day, 1_000_000))
        for sid in range(100, 180):          # 80 檔全程零成交
            rows.append((str(sid), day, 0))
    frame = pd.DataFrame(rows, columns=['stock_id', 'date', 'Volume'])
    dates = pd.to_datetime(frame['date']).dt.normalize()
    cutoff = dates.max() - pd.Timedelta(days=10)

    assert RBP._in_market_baseline(dates, frame, cutoff, history_days=180) == 100


def test_aggregation_derives_its_own_denominator_when_not_given():
    """不傳 expected_stock_count 時要自行推導，且仍抓得到整批縮水。"""
    # 150 天 200 檔健康歷史，最後 3 天只剩 60 檔（uniform shrink）
    frame = _panel([200] * 150 + [60] * 3)

    with pytest.raises(RuntimeError, match='no healthy recent market date'):
        RBP.drop_unhealthy_recent_market_dates(frame, lookback_days=5)


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


def test_refresh_summary_fails_on_partial_coverage():
    with pytest.raises(RuntimeError, match='only 799/1000'):
        RUP._validate_refresh_summary(799, 1000, fail=0)


def test_single_csv_merge_failure_does_not_kill_the_batch():
    """1964 檔壞 1 檔不該讓整條行情面板鏈被跳過（2026-08-02 code review）。"""
    budget = RUP._merge_fail_budget(1000)

    assert budget >= 5
    RUP._validate_refresh_summary(1000, 1000, fail=1)          # 不 raise
    RUP._validate_refresh_summary(1000, 1000, fail=budget)     # 剛好在容忍內

    with pytest.raises(RuntimeError, match='over the tolerance'):
        RUP._validate_refresh_summary(1000, 1000, fail=budget + 1)


def test_mass_merge_failure_still_caught_by_coverage_check():
    """失敗檔不計入 healthy_written，所以大量失敗仍會被覆蓋率門檻擋下。"""
    with pytest.raises(RuntimeError, match='only 100/1000'):
        RUP._validate_refresh_summary(100, 1000, fail=RUP._merge_fail_budget(1000))


def test_freshness_uses_official_trading_day_not_calendar_days():
    """長假不得誤報：判準是「有沒有落後官方交易日」，不是日曆天數。"""
    target = pd.Timestamp('2026-02-16')      # 春節期間，最後交易日 2/11
    official = pd.Timestamp('2026-02-11')

    # 跟上官方最新交易日 -> 通過（舊碼會因 5 個日曆天 > 4 而 raise）
    RUP._validate_market_freshness(official, official, target)

    # 落後官方交易日 -> 必須 raise
    with pytest.raises(RuntimeError, match='behind the official trading day'):
        RUP._validate_market_freshness(pd.Timestamp('2026-02-10'), official, target)


def test_freshness_tolerates_long_holiday_when_official_unavailable():
    """官方 lookback 內找不到交易日（長假/端點掛掉）只警告，超過上限才 raise。"""
    healthy = pd.Timestamp('2026-02-11')

    # 春節 2/11 -> 2/23 相隔 12 天，仍在容忍內
    RUP._validate_market_freshness(healthy, None, pd.Timestamp('2026-02-22'))

    with pytest.raises(RuntimeError, match='no official trading day in lookback'):
        RUP._validate_market_freshness(
            healthy, None, healthy + pd.Timedelta(days=RUP.MAX_NO_OFFICIAL_GAP_DAYS + 1))


def test_replace_retries_transient_windows_lock(tmp_path, monkeypatch):
    """常駐 Streamlit 讀同一支 CSV 造成的暫時性 PermissionError 應重試而非放棄。"""
    src, dst = tmp_path / 'a.tmp', tmp_path / 'b.csv'
    src.write_text('new', encoding='utf-8')
    dst.write_text('old', encoding='utf-8')
    calls = []
    real_replace = RUP.os.replace

    def flaky(a, b):
        calls.append(1)
        if len(calls) < 3:
            raise PermissionError(5, 'Access is denied')
        real_replace(a, b)

    monkeypatch.setattr(RUP.os, 'replace', flaky)
    monkeypatch.setattr(RUP.time, 'sleep', lambda _s: None)

    RUP._replace_with_retry(src, dst)

    assert len(calls) == 3
    assert dst.read_text(encoding='utf-8') == 'new'


def test_replace_gives_up_after_attempts(tmp_path, monkeypatch):
    src, dst = tmp_path / 'a.tmp', tmp_path / 'b.csv'
    src.write_text('new', encoding='utf-8')
    monkeypatch.setattr(
        RUP.os, 'replace',
        lambda *_a: (_ for _ in ()).throw(PermissionError(5, 'Access is denied')))
    monkeypatch.setattr(RUP.time, 'sleep', lambda _s: None)

    with pytest.raises(PermissionError):
        RUP._replace_with_retry(src, dst)


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
