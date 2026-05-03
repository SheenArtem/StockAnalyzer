"""Unit tests for news_intraday_monitor trigger evaluation logic.

純函式測試 evaluate_trigger() — 不 mock LLM / fetcher / Discord，只驗
觸發條件對 merged 結構的判斷正確。
"""
import sys
from datetime import date
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / 'tools'))

from news_intraday_monitor import evaluate_trigger  # noqa: E402
from tw_calendar import is_tw_trading_day  # noqa: E402


class TestWeekdayGate:
    """確保 weekday gate 行為正確 (script main 用 is_tw_trading_day)."""

    def test_monday_is_trading_day(self):
        assert is_tw_trading_day(date(2026, 5, 4))   # Mon

    def test_friday_is_trading_day(self):
        assert is_tw_trading_day(date(2026, 5, 8))   # Fri

    def test_saturday_blocked(self):
        assert not is_tw_trading_day(date(2026, 5, 2))  # Sat

    def test_sunday_blocked(self):
        assert not is_tw_trading_day(date(2026, 5, 3))  # Sun


class TestEvaluateTriggerEmpty:
    def test_empty_merged(self):
        push, alerts = evaluate_trigger([])
        assert push is False
        assert alerts == []

    def test_single_no_trigger(self):
        """1 篇普通 individual 不觸發 (count=1 < threshold 2)."""
        merged = [{
            'article_type': 'individual',
            'tickers': ['2330'], 'themes': ['CoWoS'],
            'material_event_type': '',
            'title': '台積電法說會 Q1 EPS 上修', 'date': '2026-05-03',
        }]
        push, alerts = evaluate_trigger(merged)
        assert push is False


class TestEvaluateTriggerNewsCount:
    def test_two_articles_same_ticker_triggers(self):
        merged = [
            {'article_type': 'individual', 'tickers': ['2330'],
             'themes': ['CoWoS'], 'material_event_type': '',
             'title': '台積電題材 1', 'date': '2026-05-03'},
            {'article_type': 'individual', 'tickers': ['2330'],
             'themes': ['ABF'], 'material_event_type': '',
             'title': '台積電題材 2', 'date': '2026-05-03'},
        ]
        push, alerts = evaluate_trigger(merged)
        assert push is True
        assert any('2330' in a and '🔥' in a for a in alerts)

    def test_one_article_per_ticker_no_trigger(self):
        """3 ticker 各 1 篇都不觸發 (各家 count=1 < 2)."""
        merged = [
            {'article_type': 'individual', 'tickers': ['2330'],
             'themes': ['x'], 'material_event_type': '',
             'title': 't1', 'date': '2026-05-03'},
            {'article_type': 'individual', 'tickers': ['2454'],
             'themes': ['x'], 'material_event_type': '',
             'title': 't2', 'date': '2026-05-03'},
            {'article_type': 'individual', 'tickers': ['2317'],
             'themes': ['x'], 'material_event_type': '',
             'title': 't3', 'date': '2026-05-03'},
        ]
        push, alerts = evaluate_trigger(merged)
        assert push is False

    def test_multi_ticker_each_counts(self):
        """1 篇有 2 ticker, 算各自 +1; 第 2 篇兩 ticker 都到 2 → 兩個 hot."""
        merged = [
            {'article_type': 'individual', 'tickers': ['2330', '2454'],
             'themes': ['x'], 'material_event_type': '',
             'title': 't1', 'date': '2026-05-03'},
            {'article_type': 'individual', 'tickers': ['2330', '2454'],
             'themes': ['x'], 'material_event_type': '',
             'title': 't2', 'date': '2026-05-03'},
        ]
        push, alerts = evaluate_trigger(merged)
        assert push is True
        # 兩檔都應該觸發
        ticker_alerts = [a for a in alerts if '🔥' in a]
        assert any('2330' in a for a in ticker_alerts)
        assert any('2454' in a for a in ticker_alerts)

    def test_sector_macro_not_counted(self):
        """sector / macro 文章不算進 ticker count."""
        merged = [
            {'article_type': 'sector', 'tickers': [], 'themes': [],
             'sector_tag': '半導體', 'material_event_type': '',
             'title': '半導體類股走強', 'date': '2026-05-03'},
            {'article_type': 'individual', 'tickers': ['2330'],
             'themes': ['x'], 'material_event_type': '',
             'title': 't1', 'date': '2026-05-03'},
        ]
        push, alerts = evaluate_trigger(merged)
        assert push is False  # 2330 only 1 篇


class TestEvaluateTriggerMaterialEvent:
    def test_merger_triggers(self):
        merged = [{
            'article_type': 'individual',
            'tickers': ['2330'], 'themes': ['x'],
            'material_event_type': 'merger',
            'title': '台積電宣布併購 X 公司', 'date': '2026-05-03',
        }]
        push, alerts = evaluate_trigger(merged)
        assert push is True
        assert any('併購' in a and '⚠️' in a for a in alerts)

    def test_all_6_event_types_trigger(self):
        for et in ('merger', 'buyback', 'lawsuit', 'capital_reduction',
                   'penalty', 'major_contract'):
            merged = [{
                'article_type': 'individual',
                'tickers': ['9999'], 'themes': ['x'],
                'material_event_type': et,
                'title': f'event {et}', 'date': '2026-05-03',
            }]
            push, alerts = evaluate_trigger(merged)
            assert push is True, f"event_type {et} should trigger"

    def test_invalid_event_type_no_trigger(self):
        """LLM 抽到非 6 類字串 (e.g. 'good_news') 不觸發 material event 路徑."""
        merged = [{
            'article_type': 'individual',
            'tickers': ['9999'], 'themes': ['x'],
            'material_event_type': 'random_string',
            'title': 'noise', 'date': '2026-05-03',
        }]
        push, alerts = evaluate_trigger(merged)
        # ticker count = 1 不觸發, material event invalid 也不觸發
        assert push is False


class TestEvaluateTriggerMixed:
    def test_both_triggers_combined(self):
        """News count + material event 同時命中, alerts 應含兩種."""
        merged = [
            {'article_type': 'individual', 'tickers': ['2330'],
             'themes': ['x'], 'material_event_type': 'major_contract',
             'title': '台積電拿到 X 大單', 'date': '2026-05-03'},
            {'article_type': 'individual', 'tickers': ['2330'],
             'themes': ['y'], 'material_event_type': '',
             'title': '台積電另一篇', 'date': '2026-05-03'},
        ]
        push, alerts = evaluate_trigger(merged)
        assert push is True
        # material event line + news count line 兩種都有
        assert any('⚠️' in a for a in alerts)
        assert any('🔥' in a for a in alerts)
