"""Unit tests for News Initiative Phase 0 Commit 2 dedupe helpers.

BLOCKER #1: archive 不 dedupe across sources, 但加 normalized_title_hash +
event_id 欄位; derived rebuild + market_sentiment reader 必須用 dedupe_by_event_id
過濾同事件不同 source 灌票。
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / 'tools'))

from news_theme_extract import (  # noqa: E402
    normalize_title_hash,
    compute_event_id,
    dedupe_by_event_id,
    dedupe_by_event_ticker,
)


class TestNormalizeTitleHash:
    def test_empty_returns_empty(self):
        assert normalize_title_hash('') == ''
        assert normalize_title_hash(None) == ''

    def test_deterministic(self):
        h1 = normalize_title_hash('台積電法說會 Q1 EPS 上修')
        h2 = normalize_title_hash('台積電法說會 Q1 EPS 上修')
        assert h1 == h2
        assert len(h1) == 16

    def test_punct_normalized(self):
        """同事件不同標點變體應歸為同一 hash."""
        h1 = normalize_title_hash('台積電 Q1 EPS 上修!')
        h2 = normalize_title_hash('台積電 Q1, EPS 上修')
        h3 = normalize_title_hash('台積電 Q1 EPS 上修')
        assert h1 == h2 == h3

    def test_whitespace_normalized(self):
        h1 = normalize_title_hash('台積電  Q1  EPS')
        h2 = normalize_title_hash('台積電 Q1 EPS')
        h3 = normalize_title_hash('台積電\tQ1\nEPS')
        assert h1 == h2 == h3

    def test_case_insensitive(self):
        h1 = normalize_title_hash('TSMC Q1 EPS')
        h2 = normalize_title_hash('tsmc q1 eps')
        assert h1 == h2

    def test_different_titles_different_hash(self):
        h1 = normalize_title_hash('台積電法說會')
        h2 = normalize_title_hash('鴻海法說會')
        assert h1 != h2

    def test_pure_punctuation_returns_empty(self):
        assert normalize_title_hash('!!!') == ''
        assert normalize_title_hash('   ') == ''


class TestComputeEventId:
    def test_empty_inputs(self):
        assert compute_event_id('', '2026-05-01') == ''
        assert compute_event_id('abc123', '') == ''
        assert compute_event_id('', '') == ''

    def test_format(self):
        eid = compute_event_id('abc123', '2026-05-01')
        assert eid == 'abc123_2026-05-01'

    def test_truncates_to_date(self):
        """date 帶時間部分會被切到 YYYY-MM-DD."""
        eid = compute_event_id('abc', '2026-05-01T10:30:00')
        assert eid == 'abc_2026-05-01'

    def test_same_hash_same_day_same_event(self):
        eid1 = compute_event_id('abc123', '2026-05-01')
        eid2 = compute_event_id('abc123', '2026-05-01')
        assert eid1 == eid2

    def test_same_hash_different_day_different_event(self):
        """跨日同 hash 視為不同 event (BLOCKER #1 spec trade-off)."""
        eid1 = compute_event_id('abc123', '2026-05-01')
        eid2 = compute_event_id('abc123', '2026-05-02')
        assert eid1 != eid2


class TestDedupeByEventId:
    def _build_df(self, records):
        return pd.DataFrame(records)

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame()
        assert len(dedupe_by_event_id(df)) == 0

    def test_no_event_id_column_returns_unchanged(self):
        df = pd.DataFrame([{'date': '2026-05-01', 'ticker': '2330'}])
        result = dedupe_by_event_id(df)
        assert len(result) == 1

    def test_counterfactual_same_event_diff_source(self):
        """核心 BLOCKER #1 case: cnyes + UDN 同事件 2 篇 → dedupe 後 count = 1."""
        df = self._build_df([
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'cnyes', 'title': '台積電法說會'},
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'udn', 'title': '台積電法說會'},
        ])
        result = dedupe_by_event_id(df)
        assert len(result) == 1
        assert result.iloc[0]['source'] == 'cnyes'  # keep first

    def test_same_event_diff_ticker_kept(self):
        """同事件多 ticker 提及應全保留."""
        df = self._build_df([
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'cnyes'},
            {'event_id': 'h1_2026-05-01', 'ticker': '3324', 'theme': 'CoWoS',
             'source': 'cnyes'},
        ])
        result = dedupe_by_event_id(df)
        assert len(result) == 2

    def test_same_event_diff_theme_kept(self):
        """同事件多 theme 應全保留."""
        df = self._build_df([
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'cnyes'},
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'AI server',
             'source': 'cnyes'},
        ])
        result = dedupe_by_event_id(df)
        assert len(result) == 2

    def test_empty_event_id_kept_unchanged(self):
        """legacy 資料 event_id='' 不 dedupe (兼容性)."""
        df = self._build_df([
            {'event_id': '', 'ticker': '2330', 'theme': 'CoWoS', 'source': 'old'},
            {'event_id': '', 'ticker': '2330', 'theme': 'CoWoS', 'source': 'old'},
        ])
        result = dedupe_by_event_id(df)
        assert len(result) == 2

    def test_mixed_empty_and_nonempty(self):
        """混合 legacy + 新資料：新 event_id dedupe，legacy 保留."""
        df = self._build_df([
            {'event_id': '', 'ticker': '2330', 'theme': 'CoWoS', 'source': 'legacy'},
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'cnyes'},
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'udn'},
        ])
        result = dedupe_by_event_id(df)
        assert len(result) == 2  # 1 legacy + 1 deduped (cnyes/udn 變 1)


class TestDedupeByEventTicker:
    """earnings_schema / material_events / analyst_targets 用的窄 dedupe key."""

    def _build_df(self, records):
        return pd.DataFrame(records)

    def test_empty_df_returns_empty(self):
        assert len(dedupe_by_event_ticker(pd.DataFrame())) == 0

    def test_no_event_id_column_returns_unchanged(self):
        df = pd.DataFrame([{'date': '2026-05-01', 'ticker': '2330'}])
        assert len(dedupe_by_event_ticker(df)) == 1

    def test_same_event_diff_theme_collapsed(self):
        """earnings_schema 核心 case: 同事件 N theme 應 collapse 為 1 row."""
        df = self._build_df([
            {'event_id': 'h1_2026-05-01', 'ticker': '3105', 'theme': '砷化鎵',
             'forward_revenue_guidance': '上修', 'source': 'cnyes'},
            {'event_id': 'h1_2026-05-01', 'ticker': '3105', 'theme': '光通訊',
             'forward_revenue_guidance': '上修', 'source': 'cnyes'},
            {'event_id': 'h1_2026-05-01', 'ticker': '3105', 'theme': '低軌衛星',
             'forward_revenue_guidance': '上修', 'source': 'cnyes'},
        ])
        result = dedupe_by_event_ticker(df)
        assert len(result) == 1
        assert result.iloc[0]['theme'] == '砷化鎵'  # keep first

    def test_same_event_diff_ticker_kept(self):
        """同事件多 ticker 提及應全保留."""
        df = self._build_df([
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS'},
            {'event_id': 'h1_2026-05-01', 'ticker': '3324', 'theme': 'CoWoS'},
        ])
        assert len(dedupe_by_event_ticker(df)) == 2

    def test_diff_event_same_ticker_kept(self):
        """同 ticker 不同事件應全保留."""
        df = self._build_df([
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS'},
            {'event_id': 'h2_2026-05-01', 'ticker': '2330', 'theme': 'AI server'},
        ])
        assert len(dedupe_by_event_ticker(df)) == 2

    def test_empty_event_id_kept_unchanged(self):
        """legacy event_id='' 不 dedupe."""
        df = self._build_df([
            {'event_id': '', 'ticker': '2330', 'theme': 'A'},
            {'event_id': '', 'ticker': '2330', 'theme': 'B'},
        ])
        assert len(dedupe_by_event_ticker(df)) == 2

    def test_earnings_schema_real_world_3105(self):
        """模擬實際 archive 觀察到的穩懋 1 篇法說會 → 3 themes 灌成 3 row.
        套 dedupe_by_event_ticker 後應收斂為 1 row.
        """
        df = self._build_df([
            {'event_id': '1d75d6ffdb2bfaed_2026-04-30', 'ticker': '3105',
             'theme': '砷化鎵', 'forward_revenue_guidance': '上修',
             'q_period': '2026Q2', 'source': 'cnyes'},
            {'event_id': '1d75d6ffdb2bfaed_2026-04-30', 'ticker': '3105',
             'theme': '光通訊', 'forward_revenue_guidance': '上修',
             'q_period': '2026Q2', 'source': 'cnyes'},
            {'event_id': '1d75d6ffdb2bfaed_2026-04-30', 'ticker': '3105',
             'theme': '低軌衛星', 'forward_revenue_guidance': '上修',
             'q_period': '2026Q2', 'source': 'cnyes'},
        ])
        result = dedupe_by_event_ticker(df)
        assert len(result) == 1
        assert result.iloc[0]['forward_revenue_guidance'] == '上修'
        assert result.iloc[0]['q_period'] == '2026Q2'


class TestEndToEndDedupeScenario:
    """模擬 themes_core ≥3 次升級門檻被同事件灌水的 BLOCKER #1 痛點."""

    def test_themes_core_count_not_inflated_by_same_event(self):
        """同事件同 ticker 同 theme 被 cnyes + UDN + Google 三 source 報導:
        如果 archive 不 dedupe 直接 count(ticker,theme) >= 3 會誤判晉升。
        套 dedupe_by_event_id 後 count = 1，正確語意。
        """
        df = pd.DataFrame([
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'cnyes', 'date': '2026-05-01'},
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'udn', 'date': '2026-05-01'},
            {'event_id': 'h1_2026-05-01', 'ticker': '2330', 'theme': 'CoWoS',
             'source': 'google_news', 'date': '2026-05-01'},
        ])

        # 不 dedupe 直接 count → 3 次假陽性晉升
        raw_count = df.groupby(['ticker', 'theme']).size()
        assert raw_count.iloc[0] == 3

        # 套 dedupe 後正確 = 1
        deduped = dedupe_by_event_id(df)
        true_count = deduped.groupby(['ticker', 'theme']).size()
        assert true_count.iloc[0] == 1
