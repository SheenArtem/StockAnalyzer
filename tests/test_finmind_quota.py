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


class TestQuotaBlockBackoff:
    """server 端額度封鎖長度必須是固定上限的退避，不可綁自家 rolling window。

    2026-08-02 code review 實測：`_reserve_request` 在打 API **之前** 就把 now 塞進
    deque，所以剛啟動的 process 第一筆請求就撞到 server 端額度時，
    `_seconds_until_window_reset()` 回 ≈3605 秒 → 直接鎖滿一小時，且沒有任何成功
    後解除或提前重探的路徑。受害者是長時間多次呼叫者（chip_history_dl 的 per-stock
    迴圈、backfill、scanner、常駐 Streamlit）。
    """

    def test_fresh_process_first_request_does_not_lock_a_full_hour(self):
        dl = _QuotaDeadDL()
        tr = FinMindTracker(dl, has_token=True)

        # 不 stub _seconds_until_window_reset —— 就是要重現「第一筆請求即 oldest≈now」
        t0 = time.time()
        with pytest.raises(FinMindQuotaBlockedError):
            tr.taiwan_stock_info()

        # 舊版這裡會是 ~3605
        assert tr._seconds_until_window_reset() > 3500, '前提：自家視窗確實回接近一小時'
        blocked_for = tr._quota_blocked_until - t0
        assert blocked_for == pytest.approx(cache_manager._FINMIND_QUOTA_BLOCK_START,
                                            abs=5), \
            f'第一階應為 {cache_manager._FINMIND_QUOTA_BLOCK_START}s，實得 {blocked_for:.0f}s'

    def test_backoff_ladder_climbs_then_caps(self):
        dl = _QuotaDeadDL()
        tr = FinMindTracker(dl, has_token=True)
        seen = []
        for _ in range(5):
            tr._clear_quota_block()          # 只清 block，讓下一次真的打到 API
            tr._quota_block_level = len(seen)
            t0 = time.time()
            with pytest.raises(FinMindQuotaBlockedError):
                tr.taiwan_stock_info()
            seen.append(round(tr._quota_blocked_until - t0))

        assert seen[0] == pytest.approx(300, abs=5)
        assert seen[1] == pytest.approx(600, abs=5)
        assert seen[2] == pytest.approx(900, abs=5)
        assert all(s == pytest.approx(cache_manager._FINMIND_QUOTA_BLOCK_CAP, abs=5)
                   for s in seen[2:]), f'應在 900s 封頂，實得 {seen}'

    def test_success_resets_backoff_level_and_clears_block(self):
        dl = _FlakyThenOKDL()
        tr = FinMindTracker(dl, has_token=True)
        tr._seconds_until_window_reset = lambda: 0.01

        df = tr.taiwan_stock_info()          # 第 1 次爆、等待後第 2 次成功

        assert not df.empty
        assert tr._quota_block_level == 0, '成功一次就要歸零，否則背著上一輪長封鎖'
        assert tr._quota_blocked_until == 0.0

    def test_plain_success_clears_a_stale_block(self):
        class _OK:
            def taiwan_stock_info(self):
                return pd.DataFrame({'stock_id': ['2330']})

        tr = FinMindTracker(_OK(), has_token=True)
        tr._quota_block_level = 3
        # 過期的 block 不該讓後續成功呼叫仍背著退避階數
        tr._quota_blocked_until = time.time() - 1

        tr.taiwan_stock_info()

        assert tr._quota_block_level == 0
        assert tr._quota_blocked_until == 0.0


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


class TestGetFinmindCachedStaleFallback:
    """抓取失敗時必須用磁碟上的過期快取，不可回空 frame。

    2026-08-02 code review：`FinMindQuotaBlockedError` 的訊息自稱
    "callers fall back to stale cache"，但實際流程是「磁碟快取過期 → 去抓 → 失敗 →
    回空 frame」，磁碟上那份過期資料從頭到尾沒被用到。
    """

    def _stale_cache(self, tmp_path, monkeypatch, rows, age_days=400):
        monkeypatch.setattr(cache_manager, 'CACHE_DIR', str(tmp_path))
        path = tmp_path / 'finmind_cache' / 'month_revenue_2330.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(path, index=False)
        old = time.time() - age_days * 86400
        os.utime(path, (old, old))
        return path

    def test_fetch_exception_falls_back_to_stale_disk_cache(self, tmp_path, monkeypatch):
        self._stale_cache(tmp_path, monkeypatch,
                          {'stock_id': ['2330'], 'date': ['2025-01-01'], 'revenue': [100]})

        class _Dead:
            def taiwan_stock_month_revenue(self, stock_id=None, start_date=None):
                raise FinMindQuotaBlockedError('quota exhausted')

        out = cache_manager.get_finmind_cached(
            _Dead(), 'month_revenue', '2330', 'taiwan_stock_month_revenue',
            ttl_days=30)

        assert not out.empty, '過期快取存在時不可回空 frame'
        assert out['revenue'].iloc[0] == 100

    def test_empty_fetch_result_does_not_resurrect_stale_rows(self, tmp_path, monkeypatch):
        """抓到空結果是下市股的合法答案，拿舊資料頂替會把「已無資料」變成「還有」。"""
        self._stale_cache(tmp_path, monkeypatch,
                          {'stock_id': ['2330'], 'date': ['2025-01-01'], 'revenue': [100]})

        class _EmptyOK:
            def taiwan_stock_month_revenue(self, stock_id=None, start_date=None):
                return pd.DataFrame()

        out = cache_manager.get_finmind_cached(
            _EmptyOK(), 'month_revenue', '2330', 'taiwan_stock_month_revenue',
            ttl_days=30)

        assert out.empty, '空結果不可被過期資料頂替'

    def test_no_disk_cache_still_returns_empty_on_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cache_manager, 'CACHE_DIR', str(tmp_path))

        class _Dead:
            def taiwan_stock_month_revenue(self, stock_id=None, start_date=None):
                raise FinMindQuotaBlockedError('quota exhausted')

        out = cache_manager.get_finmind_cached(
            _Dead(), 'month_revenue', '9999', 'taiwan_stock_month_revenue',
            ttl_days=30)

        assert out.empty

    def test_fresh_cache_is_used_without_fetching(self, tmp_path, monkeypatch):
        self._stale_cache(tmp_path, monkeypatch,
                          {'stock_id': ['2330'], 'date': ['2026-07-01'], 'revenue': [7]},
                          age_days=0)

        class _MustNotCall:
            def taiwan_stock_month_revenue(self, stock_id=None, start_date=None):
                raise AssertionError('快取未過期時不應打 FinMind')

        out = cache_manager.get_finmind_cached(
            _MustNotCall(), 'month_revenue', '2330', 'taiwan_stock_month_revenue',
            ttl_days=30)

        assert out['revenue'].iloc[0] == 7
