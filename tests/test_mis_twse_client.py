"""Unit tests for mis_twse_client.

無網路測試: 全部 mock _MisTwseClient._fetch 來驗 prefix 探測 / z fallback /
stub element 過濾 / 五檔 mid 計算等邏輯。
"""
from datetime import datetime
from unittest.mock import patch

import pytest

import mis_twse_client as mtc


# ---------------------------------------------------------------
# Fixtures: 模擬 mis.twse 各種 response
# ---------------------------------------------------------------

def _stub_no_data():
    """mis.twse 對「給錯前綴」的 stub element (z='-' / a/b 空 / v=None)"""
    return {
        '@': 'X.tw', 'z': '-', 'pz': None, 'a': '', 'b': '',
        'o': '-', 'h': '-', 'l': '-', 'y': '-', 'v': None,
        't': '09:00:00', 'tlong': '0', 'ch': 'X.tw',
    }


def _live_quote(z='2305.0000', pz='2305.0000', a='2305.0000_2310.0000_',
                b='2300.0000_2295.0000_', v='6800',
                o='2300.0000', h='2310.0000', l='2295.0000', y='2310.0000'):
    """正常開盤後 quote。"""
    return {
        '@': '2330.tw', 'z': z, 'pz': pz, 'a': a, 'b': b, 'v': v,
        'o': o, 'h': h, 'l': l, 'y': y,
        't': '09:30:00', 'tlong': '1778204120000', 'ch': '2330.tw',
    }


@pytest.fixture(autouse=True)
def _reset_singleton():
    """每個 test 重設 module singleton + cookie state，避免互相污染。"""
    mtc._singleton = None
    yield
    mtc._singleton = None


# ---------------------------------------------------------------
# _has_real_data
# ---------------------------------------------------------------

class TestHasRealData:
    def test_stub_returns_false(self):
        assert mtc._has_real_data(_stub_no_data()) is False

    def test_live_quote_returns_true(self):
        assert mtc._has_real_data(_live_quote()) is True

    def test_volume_only_returns_true(self):
        # 開盤 trade 但五檔暫空 (極少見) — v 有就視為有效
        elem = _stub_no_data()
        elem['v'] = '100'
        assert mtc._has_real_data(elem) is True

    def test_bid_ask_only_returns_true(self):
        # 試撮合期間 v=None 但五檔有掛單
        elem = _stub_no_data()
        elem['a'] = '2305.0000_2310.0000_'
        assert mtc._has_real_data(elem) is True

    def test_open_only_returns_true(self):
        # 開盤瞬間 v 還沒累計，但 o 已有
        elem = _stub_no_data()
        elem['o'] = '2300.0000'
        assert mtc._has_real_data(elem) is True


# ---------------------------------------------------------------
# _parse_quote: price 取得順序
# ---------------------------------------------------------------

class TestParseQuotePriceSource:
    def test_z_when_present(self):
        q = mtc._parse_quote(_live_quote(z='2305.0000'), 'tse')
        assert q['price'] == 2305.0
        assert q['price_source'] == 'z'

    def test_pz_when_z_dash(self):
        q = mtc._parse_quote(_live_quote(z='-', pz='2304.0000'), 'tse')
        assert q['price'] == 2304.0
        assert q['price_source'] == 'pz'

    def test_mid_when_z_pz_dash(self):
        q = mtc._parse_quote(
            _live_quote(z='-', pz='-', a='2305.0000_2310.0000_', b='2300.0000_2295.0000_'),
            'tse'
        )
        assert q['price'] == 2302.5
        assert q['price_source'] == 'mid'

    def test_prev_close_when_all_dash(self):
        q = mtc._parse_quote(
            _live_quote(z='-', pz='-', a='', b='', y='2310.0000'),
            'tse'
        )
        assert q['price'] == 2310.0
        assert q['price_source'] == 'prev_close'

    def test_returns_none_when_nothing(self):
        elem = _stub_no_data()
        assert mtc._parse_quote(elem, 'tse') is None

    def test_single_side_quote_falls_back(self):
        # 漲停只有買單，賣單空
        q = mtc._parse_quote(
            _live_quote(z='-', pz='-', a='', b='2540.0000_2535.0000_'),
            'tse'
        )
        assert q['price'] == 2540.0
        assert q['price_source'] == 'mid'


# ---------------------------------------------------------------
# Client: prefix probing + cache
# ---------------------------------------------------------------

class TestPrefixResolution:
    def test_tse_hit_first_no_otc_call(self):
        """上市股第一次嘗試 tse_ 就拿到，不該再打 otc_。"""
        with patch.object(mtc.MisTwseClient, '_fetch') as mock_fetch:
            mock_fetch.return_value = _live_quote()
            c = mtc.MisTwseClient()
            c._cookie_initialized = True  # 跳過 cookie init
            q = c.get_quote('2330')
            assert q is not None
            assert q['listing'] == 'tse'
            # 只該打一次 (tse_2330.tw)
            assert mock_fetch.call_count == 1
            assert mock_fetch.call_args[0][0] == 'tse_2330.tw'

    def test_otc_fallback_after_tse_miss(self):
        """tse_ stub None 後該試 otc_。"""
        with patch.object(mtc.MisTwseClient, '_fetch') as mock_fetch:
            mock_fetch.side_effect = [None, _live_quote()]  # tse miss, otc hit
            c = mtc.MisTwseClient()
            c._cookie_initialized = True
            q = c.get_quote('6488')
            assert q is not None
            assert q['listing'] == 'otc'
            assert mock_fetch.call_count == 2
            assert [call[0][0] for call in mock_fetch.call_args_list] == \
                   ['tse_6488.tw', 'otc_6488.tw']

    def test_prefix_cache_avoids_re_probing(self):
        """同一檔第二次叫，不該重新探測 prefix。"""
        with patch.object(mtc.MisTwseClient, '_fetch') as mock_fetch:
            mock_fetch.side_effect = [None, _live_quote(), _live_quote()]
            c = mtc.MisTwseClient()
            c._cookie_initialized = True
            c.get_quote('6488')  # 探到 otc, 2 次 fetch
            c.get_quote('6488')  # cached, 1 次 fetch
            assert mock_fetch.call_count == 3
            assert [call[0][0] for call in mock_fetch.call_args_list] == \
                   ['tse_6488.tw', 'otc_6488.tw', 'otc_6488.tw']

    def test_negative_cache_after_both_miss(self):
        """tse + otc 都 miss 後再叫該檔不重新打 API。"""
        with patch.object(mtc.MisTwseClient, '_fetch') as mock_fetch:
            mock_fetch.return_value = None
            c = mtc.MisTwseClient()
            c._cookie_initialized = True
            assert c.get_quote('9999') is None
            assert c.get_quote('9999') is None
            # 第一次叫 2 個 fetch (tse + otc)，第二次 0 (cache hit None)
            assert mock_fetch.call_count == 2

    def test_index_uses_tse_directly(self):
        """指數 t00 該直接走 tse_ 不嘗試 otc_。"""
        with patch.object(mtc.MisTwseClient, '_fetch') as mock_fetch:
            mock_fetch.return_value = _live_quote(v=None)  # 指數無量
            c = mtc.MisTwseClient()
            c._cookie_initialized = True
            q = c.get_quote('t00')
            assert q is not None
            assert q['listing'] == 'tse'
            assert mock_fetch.call_count == 1
            assert mock_fetch.call_args[0][0] == 'tse_t00.tw'


# ---------------------------------------------------------------
# Ticker normalization
# ---------------------------------------------------------------

class TestTickerNormalization:
    def test_strips_tw_suffix(self):
        with patch.object(mtc.MisTwseClient, '_fetch') as mock_fetch:
            mock_fetch.return_value = _live_quote()
            c = mtc.MisTwseClient()
            c._cookie_initialized = True
            c.get_quote('2330.TW')
            assert mock_fetch.call_args[0][0] == 'tse_2330.tw'

    def test_strips_two_suffix(self):
        with patch.object(mtc.MisTwseClient, '_fetch') as mock_fetch:
            mock_fetch.side_effect = [None, _live_quote()]
            c = mtc.MisTwseClient()
            c._cookie_initialized = True
            c.get_quote('6488.TWO')
            # OTC ticker but caller used .TWO suffix — 仍該先試 tse 再 otc
            assert mock_fetch.call_args_list[-1][0][0] == 'otc_6488.tw'

    def test_index_uppercase(self):
        with patch.object(mtc.MisTwseClient, '_fetch') as mock_fetch:
            mock_fetch.return_value = _live_quote(v=None)
            c = mtc.MisTwseClient()
            c._cookie_initialized = True
            c.get_quote('T00')
            assert mock_fetch.call_args[0][0] == 'tse_t00.tw'


# ---------------------------------------------------------------
# Trading hours helper
# ---------------------------------------------------------------

class TestIntradayFallbackHelper:
    """`technical_analysis._try_intraday_quote_as_today_bar` 是 load_and_resample
    在 FinMind/yfinance 都拿不到 today bar 時用 mis.twse 補的關鍵 helper。"""

    def _patch_quote(self, monkeypatch, return_val):
        import mis_twse_client
        monkeypatch.setattr(mis_twse_client, 'is_tw_trading_hours', lambda *a, **kw: True)
        monkeypatch.setattr(mis_twse_client, 'get_quote', lambda sid: return_val)

    def test_returns_dataframe_on_hit(self, monkeypatch):
        from technical_analysis import _try_intraday_quote_as_today_bar
        self._patch_quote(monkeypatch, {
            'price': 2305.0, 'price_source': 'z', 'volume': 6800,
            'open': 2300.0, 'high': 2310.0, 'low': 2295.0,
            'prev_close': 2310.0, 'time': '09:30:00',
            'date': '2026-05-08', 'listing': 'tse', 'source': 'mis.twse',
        })
        df = _try_intraday_quote_as_today_bar('2330')
        assert df is not None
        assert len(df) == 1
        row = df.iloc[0]
        assert row['Close'] == 2305.0
        assert row['Open'] == 2300.0
        assert row['High'] == 2310.0
        assert row['Low'] == 2295.0
        assert row['Volume'] == 6800
        assert df.index[0] == __import__('pandas').Timestamp('2026-05-08')

    def test_non_digit_returns_none(self, monkeypatch):
        from technical_analysis import _try_intraday_quote_as_today_bar
        # 美股 ticker 不該 call mis.twse
        self._patch_quote(monkeypatch, None)  # 即使 mock 也不該 call 到
        assert _try_intraday_quote_as_today_bar('AAPL') is None

    def test_after_hours_returns_none(self, monkeypatch):
        from technical_analysis import _try_intraday_quote_as_today_bar
        import mis_twse_client
        monkeypatch.setattr(mis_twse_client, 'is_tw_trading_hours', lambda *a, **kw: False)
        monkeypatch.setattr(mis_twse_client, 'get_quote',
                            lambda sid: pytest.fail("get_quote should not be called after hours"))
        assert _try_intraday_quote_as_today_bar('2330') is None

    def test_quote_none_returns_none(self, monkeypatch):
        from technical_analysis import _try_intraday_quote_as_today_bar
        self._patch_quote(monkeypatch, None)
        assert _try_intraday_quote_as_today_bar('2330') is None

    def test_partial_open_high_low_filled_with_price(self, monkeypatch):
        """mis.twse 對冷門股 o/h/l 可能 None，要用 price 兜底，OHLC 才會 valid。"""
        from technical_analysis import _try_intraday_quote_as_today_bar
        self._patch_quote(monkeypatch, {
            'price': 100.0, 'price_source': 'mid', 'volume': None,
            'open': None, 'high': None, 'low': None,
            'prev_close': 99.0, 'time': '09:30:00',
            'date': '2026-05-08', 'listing': 'otc', 'source': 'mis.twse',
        })
        df = _try_intraday_quote_as_today_bar('1234')
        assert df is not None
        row = df.iloc[0]
        assert row['Open'] == 100.0
        assert row['High'] == 100.0
        assert row['Low'] == 100.0
        assert row['Close'] == 100.0
        assert row['Volume'] == 0  # None volume 兜底為 0

    def test_mis_twse_exception_does_not_propagate(self, monkeypatch):
        """mis.twse 任何 exception 都不該炸 load_and_resample。"""
        from technical_analysis import _try_intraday_quote_as_today_bar
        import mis_twse_client
        monkeypatch.setattr(mis_twse_client, 'is_tw_trading_hours', lambda *a, **kw: True)
        def boom(*a, **kw): raise RuntimeError("network died")
        monkeypatch.setattr(mis_twse_client, 'get_quote', boom)
        # 不該拋 exception，只該回 None
        assert _try_intraday_quote_as_today_bar('2330') is None


class TestTradingHours:
    def test_weekday_open(self):
        # 週一 (假設 2026-05-04) 10:00
        now = datetime(2026, 5, 4, 10, 0)
        assert mtc.is_tw_trading_hours(now) is True

    def test_weekday_before_open(self):
        now = datetime(2026, 5, 4, 8, 59)
        assert mtc.is_tw_trading_hours(now) is False

    def test_weekday_after_close(self):
        now = datetime(2026, 5, 4, 13, 31)
        assert mtc.is_tw_trading_hours(now) is False

    def test_weekend(self):
        # 2026-05-09 是週六
        now = datetime(2026, 5, 9, 10, 0)
        assert mtc.is_tw_trading_hours(now) is False
