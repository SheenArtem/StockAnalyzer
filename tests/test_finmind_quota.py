"""FinMind quota fail-fast (FinMindTracker) + tw_stock_info 3 層快取 (cache_manager)。

2026-06-10: 額度爆時舊版盲睡 65s 再 retry（離整點重置遠時必敗），AI 報告 prompt
組裝實測拖到 170s。改：貼近整點 (<=90s) 才等待，否則 fail-fast + 整小時
negative cache；對照表落盤 data_cache/tw_stock_info.csv，FinMind 失敗回 stale。
"""
import os
import time

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
        tr._seconds_until_next_wall_hour = lambda: 1800  # 離整點還 30 分
        t0 = time.perf_counter()
        with pytest.raises(KeyError):
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
        tr._seconds_until_next_wall_hour = lambda: 0.05  # 貼近整點 -> 等待重試有意義
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
