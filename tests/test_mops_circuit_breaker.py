"""Unit tests for mops_fetcher circuit breaker (2026-05-05 escape-logic fix).

Background: 2026-05-05 incident where scanner hung 8h18m in
trip -> sleep600s -> trip infinite loop. Fix: _check_breaker raises
MopsUnavailable (no block-sleep) and sticky-disables after N trips/day.

These tests cover the breaker logic in isolation (no network).
"""
import json
import time
from pathlib import Path

import pytest

import mops_fetcher


@pytest.fixture
def isolated_state(tmp_path, monkeypatch):
    """每個測試用獨立 daily-usage 檔，重置 module-level state。"""
    usage_file = tmp_path / "mops_daily_usage.json"
    monkeypatch.setattr(mops_fetcher, "_DAILY_USAGE_FILE", usage_file)
    monkeypatch.setattr(mops_fetcher, "_consecutive_errors", 0)
    monkeypatch.setattr(mops_fetcher, "_breaker_paused_until", 0.0)
    monkeypatch.setattr(mops_fetcher, "_BREAKER_THRESHOLD", 5)
    monkeypatch.setattr(mops_fetcher, "_MAX_TRIPS_PER_DAY", 3)
    monkeypatch.setattr(mops_fetcher, "_BREAKER_PAUSE", 600)
    return usage_file


def test_consecutive_failures_below_threshold_no_trip(isolated_state):
    for _ in range(4):
        mops_fetcher._record_failure()
    # 還沒到 5 -> 不該 pause
    assert mops_fetcher._breaker_paused_until <= time.time()
    state = mops_fetcher._load_daily_state()
    assert state["trips"] == 0


def test_threshold_failures_trip_breaker(isolated_state):
    for _ in range(5):
        mops_fetcher._record_failure()
    assert mops_fetcher._breaker_paused_until > time.time()
    state = mops_fetcher._load_daily_state()
    assert state["trips"] == 1


def test_check_breaker_raises_without_sleep(isolated_state):
    """Core bug fix: _check_breaker 不再 block-sleep，立刻 raise MopsUnavailable。"""
    for _ in range(5):
        mops_fetcher._record_failure()
    t0 = time.time()
    with pytest.raises(mops_fetcher.MopsUnavailable, match="circuit breaker active"):
        mops_fetcher._check_breaker()
    # 必須 < 100ms（vs 原本會 sleep 600s）
    assert (time.time() - t0) < 0.1


def test_pause_expiry_allows_call_through(isolated_state, monkeypatch):
    for _ in range(5):
        mops_fetcher._record_failure()
    # 模擬 pause 已過期（手動撥針）
    monkeypatch.setattr(mops_fetcher, "_breaker_paused_until", time.time() - 1)
    # trips 還沒爆 sticky -> 應該放行
    mops_fetcher._check_breaker()  # 不 raise


def test_max_trips_per_day_sticky_disable(isolated_state, monkeypatch):
    """達 _MAX_TRIPS_PER_DAY trip 後即使 pause 過期也持續 raise。"""
    for _trip_n in range(3):
        # 讓 pause 過期才能再 trip
        monkeypatch.setattr(mops_fetcher, "_breaker_paused_until", 0.0)
        for _ in range(5):
            mops_fetcher._record_failure()
    state = mops_fetcher._load_daily_state()
    assert state["trips"] == 3
    # 模擬 pause 已過期
    monkeypatch.setattr(mops_fetcher, "_breaker_paused_until", 0.0)
    with pytest.raises(mops_fetcher.MopsUnavailable, match="sticky-disabled"):
        mops_fetcher._check_breaker()


def test_cross_day_reset_clears_count_and_trips(isolated_state):
    """跨日：載入舊日期的檔案應全部歸零。"""
    today_state = {"date": "2020-01-01", "count": 100, "trips": 5}
    isolated_state.write_text(json.dumps(today_state), encoding="utf-8")
    state = mops_fetcher._load_daily_state()
    assert state["count"] == 0
    assert state["trips"] == 0


def test_cross_process_sticky_disable_via_persisted_file(isolated_state, monkeypatch):
    """同一天另一個 process 重啟，從檔案讀到 trips=3 也該 raise。"""
    from datetime import date
    today = date.today().isoformat()
    sticky = {"date": today, "count": 0, "trips": 3}
    isolated_state.write_text(json.dumps(sticky), encoding="utf-8")
    # 模擬全新 process（in-memory state 都是 0）
    monkeypatch.setattr(mops_fetcher, "_consecutive_errors", 0)
    monkeypatch.setattr(mops_fetcher, "_breaker_paused_until", 0.0)
    with pytest.raises(mops_fetcher.MopsUnavailable, match="sticky-disabled"):
        mops_fetcher._check_breaker()


def test_record_success_resets_consecutive_errors(isolated_state):
    for _ in range(3):
        mops_fetcher._record_failure()
    assert mops_fetcher._consecutive_errors == 3
    mops_fetcher._record_success()
    assert mops_fetcher._consecutive_errors == 0


def test_daily_cap_still_enforced(isolated_state, monkeypatch):
    """確認 trip 改造沒破壞 daily cap 機制。"""
    monkeypatch.setattr(mops_fetcher, "_DAILY_CAP", 3)
    for _ in range(3):
        mops_fetcher._check_daily_cap()
    with pytest.raises(mops_fetcher.MopsDailyCapExceeded):
        mops_fetcher._check_daily_cap()


def test_mops_unavailable_is_runtime_error_subclass():
    """確保 caller (cache_manager.py) 的 except Exception 能 catch。"""
    assert issubclass(mops_fetcher.MopsUnavailable, RuntimeError)
