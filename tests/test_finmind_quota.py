"""FinMind quota fail-fast (FinMindTracker) + tw_stock_info 3 層快取 (cache_manager)。

2026-07-14: FinMind 實測為 rolling 60-minute window，不能在鐘點整點清零。
貼近 tracker 視窗尾端 (<=90s) 才等待，否則 fail-fast + negative cache；
對照表落盤 data_cache/tw_stock_info.csv，FinMind 失敗回 stale。
"""
import os
import time
from collections import deque

import pandas as pd
import pytest

import cache_manager
from cache_manager import FinMindTracker, FinMindQuotaBlockedError


class _QuotaDeadDL:
    """模擬 server-side 額度爆：response 無 'data' key -> KeyError('data')"""
    def __init__(self):
        self.calls = 0

    def taiwan_stock_info(self):
        self.calls += 1
        raise KeyError('data')


class _FlakyThenOKDL:
    """第 1 次額度爆、第 2 次成功（模擬整點重置後復活）"""
    def __init__(self):
        self.calls = 0

    def taiwan_stock_info(self):
        self.calls += 1
        if self.calls == 1:
            raise KeyError('data')
        return pd.DataFrame({'stock_id': ['2330'], 'stock_name': ['台積電'],
                             'industry_category': ['半導體']})


class TestQuotaFailFast:
    def test_far_from_reset_no_sleep_and_negative_cache(self):
        dl = _QuotaDeadDL()
        tr = FinMindTracker(dl, has_token=False)
        tr._seconds_until_window_reset = lambda: 1800
        t0 = time.perf_counter()
        with pytest.raises(FinMindQuotaBlockedError):
            tr.taiwan_stock_info()
        assert time.perf_counter() - t0 < 5  # 不再盲睡 65s
        assert dl.calls == 1                 # 不做無望 retry
        # negative cache：同小時後續呼叫 fail-fast 且不打 API
        with pytest.raises(FinMindQuotaBlockedError):
            tr.taiwan_stock_info()
        assert dl.calls == 1

    def test_near_reset_waits_and_retries(self):
        dl = _FlakyThenOKDL()
        tr = FinMindTracker(dl, has_token=False)
        tr._seconds_until_window_reset = lambda: 0.05
        df = tr.taiwan_stock_info()
        assert dl.calls == 2
        assert not df.empty

    def test_non_quota_keyerror_passthrough(self):
        class _OtherErrDL:
            def taiwan_stock_info(self):
                raise KeyError('other')
        tr = FinMindTracker(_OtherErrDL(), has_token=False)
        with pytest.raises(KeyError):
            tr.taiwan_stock_info()
        assert tr._quota_blocked_until == 0.0  # 非額度錯誤不觸發 block

    def test_local_pause_uses_rolling_window_not_wall_hour(self, monkeypatch):
        class _OK:
            def taiwan_stock_info(self):
                return pd.DataFrame({'stock_id': ['2330']})

        tr = FinMindTracker(_OK(), has_token=True)
        tr._request_times = deque(
            [time.time()] * cache_manager._FINMIND_RATE_PAUSE
        )
        tr.request_count = len(tr._request_times)
        tr._seconds_until_window_reset = lambda: 42.0
        sleeps = []

        def release_window(seconds):
            sleeps.append(seconds)
            tr._request_times.clear()

        monkeypatch.setattr(cache_manager.time, 'sleep', release_window)

        out = tr.taiwan_stock_info()

        assert not out.empty
        assert sleeps == [42.0]
        assert tr.request_count == 1

    def test_second_quota_failure_sets_negative_cache_and_stops_batch(self, monkeypatch):
        dl = _QuotaDeadDL()
        tr = FinMindTracker(dl, has_token=True)
        waits = iter([0.01, 1800.0])
        tr._seconds_until_window_reset = lambda: next(waits)
        monkeypatch.setattr(cache_manager.time, 'sleep', lambda _seconds: None)

        with pytest.raises(FinMindQuotaBlockedError, match='remained exhausted'):
            tr.taiwan_stock_info()

        assert dl.calls == 2
        assert tr._quota_blocked_until > time.time()
        with pytest.raises(FinMindQuotaBlockedError):
            tr.taiwan_stock_info()
        assert dl.calls == 2

    def test_window_reset_is_based_on_oldest_request_not_tracker_start(self):
        tr = FinMindTracker(_QuotaDeadDL(), has_token=True)
        now = time.time()
        tr._hour_start = now - 3500
        tr._request_times = deque([now - 500] * cache_manager._FINMIND_RATE_PAUSE)
        tr.request_count = len(tr._request_times)

        wait = tr._seconds_until_window_reset()

        assert 3090 <= wait <= 3110

    def test_real_rolling_window_wait_releases_oldest_requests(self, monkeypatch):
        class _OK:
            def taiwan_stock_info(self):
                return pd.DataFrame({'stock_id': ['2330']})

        clock = {'now': 4_000.0}
        sleeps = []

        def advance(seconds):
            sleeps.append(seconds)
            clock['now'] += seconds

        monkeypatch.setattr(cache_manager.time, 'time', lambda: clock['now'])
        monkeypatch.setattr(cache_manager.time, 'sleep', advance)
        tr = FinMindTracker(_OK(), has_token=True)
        tr._request_times = deque(
            [clock['now'] - 500.0] * cache_manager._FINMIND_RATE_PAUSE
        )
        tr.request_count = len(tr._request_times)

        out = tr.taiwan_stock_info()

        assert not out.empty
        assert sleeps == [3105.0]
        assert tr.request_count == 1
        assert list(tr._request_times) == [clock['now']]

    def test_stats_prunes_expired_rolling_requests(self, monkeypatch):
        tr = FinMindTracker(_QuotaDeadDL(), has_token=True)
        now = 10_000.0
        tr._request_times = deque([now - 3_601.0])
        tr.request_count = 1
        monkeypatch.setattr(cache_manager.time, 'time', lambda: now)

        stats = tr.get_stats()

        assert stats['request_count'] == 0
        assert stats['remaining'] == cache_manager._FINMIND_RATE_LIMIT


class TestTwStockInfo3Tier:
    def setup_method(self):
        cache_manager._TW_STOCK_INFO_CACHE = None

    def teardown_method(self):
        cache_manager._TW_STOCK_INFO_CACHE = None

    def test_finmind_success_writes_disk_and_keeps_str_id(self, tmp_path, monkeypatch):
        disk = tmp_path / 'tw_stock_info.csv'
        monkeypatch.setattr(cache_manager, '_TW_STOCK_INFO_DISK', str(disk))
        df = pd.DataFrame({'stock_id': ['0050'], 'stock_name': ['元大台灣50'],
                           'industry_category': ['ETF']})

        class _OK:
            def taiwan_stock_info(self):
                return df
        monkeypatch.setattr(cache_manager, 'get_finmind_loader', lambda: _OK())
        out = cache_manager.get_tw_stock_info()
        assert out is not None and disk.exists()
        # 下次冷啟從 disk 載入時 '0050' 不能退化成整數 50
        cache_manager._TW_STOCK_INFO_CACHE = None
        monkeypatch.setattr(cache_manager, 'get_finmind_loader',
                            lambda: (_ for _ in ()).throw(AssertionError('不應打 FinMind')))
        out2 = cache_manager.get_tw_stock_info()
        assert out2['stock_id'].iloc[0] == '0050'

    def test_finmind_fail_falls_back_to_stale_disk(self, tmp_path, monkeypatch):
        disk = tmp_path / 'tw_stock_info.csv'
        pd.DataFrame({'stock_id': ['2330'], 'stock_name': ['台積電'],
                      'industry_category': ['半導體']}).to_csv(disk, index=False)
        old = time.time() - 30 * 86400  # 30 天前 -> 已過 7 天 TTL
        os.utime(disk, (old, old))
        monkeypatch.setattr(cache_manager, '_TW_STOCK_INFO_DISK', str(disk))

        class _Dead:
            def taiwan_stock_info(self):
                raise KeyError('data')
        monkeypatch.setattr(cache_manager, 'get_finmind_loader', lambda: _Dead())
        out = cache_manager.get_tw_stock_info()
        assert out is not None
        assert out['stock_id'].iloc[0] == '2330'  # stale 仍可用

    def test_both_fail_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cache_manager, '_TW_STOCK_INFO_DISK',
                            str(tmp_path / 'absent.csv'))

        class _Dead:
            def taiwan_stock_info(self):
                raise KeyError('data')
        monkeypatch.setattr(cache_manager, 'get_finmind_loader', lambda: _Dead())
        assert cache_manager.get_tw_stock_info() is None
