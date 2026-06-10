"""banner 指數三層防線 + FRED 第二來源 (2026-06-10, 159f18d + 後續)。

防的病：yfinance 幽靈尾列 (Close=NaN) → price=nan 被當成功、快取到下次開盤；
app 重啟清掉 session 內舊好值 → S&P/那斯達克空白整個下午。
鏈：yfinance (OHLC 全指標) → FRED close-only (^GSPC/^IXIC) → last-good stale。
"""
import json

import numpy as np
import pandas as pd
import pytest

import market_banner as MB


def _fake_df(n=80, last_close_nan=False):
    if n == 0:
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])
    idx = pd.date_range('2026-02-01', periods=n, freq='B')
    close = pd.Series(100.0 + np.arange(n) * 0.5, index=idx)
    df = pd.DataFrame({'Open': close, 'High': close * 1.01,
                       'Low': close * 0.99, 'Close': close, 'Volume': 1e6})
    if last_close_nan:
        df.iloc[-1, df.columns.get_loc('Close')] = np.nan
    return df


class _FakeTicker:
    df = None

    def __init__(self, *a, **k):
        pass

    def history(self, **k):
        return _FakeTicker.df.copy()


@pytest.fixture
def fake_yf(monkeypatch):
    import yfinance
    monkeypatch.setattr(yfinance, 'Ticker', _FakeTicker)
    return _FakeTicker


@pytest.fixture
def tmp_lastgood(tmp_path, monkeypatch):
    p = tmp_path / 'last_good.json'
    monkeypatch.setattr(MB, '_INDEX_LAST_GOOD_PATH', p)
    return p


class TestNaNTailGuard:
    def test_nan_tail_filtered_uses_last_valid(self, fake_yf, tmp_lastgood):
        fake_yf.df = _fake_df(80, last_close_nan=True)
        r = MB._fetch_index_metrics('^GSPC', 'S&P 500')
        assert r['error'] is None
        assert r['price'] == round(100.0 + 78 * 0.5, 2)  # 倒數第 2 根 (最後有效)
        assert r['k'] is not None  # yfinance 路徑有 KD


class TestFredFallback:
    def test_empty_yf_falls_to_fred(self, fake_yf, tmp_lastgood, monkeypatch):
        fake_yf.df = _fake_df(0)
        idx = pd.date_range('2026-02-01', periods=80, freq='B')
        monkeypatch.setattr(MB, '_fetch_index_close_fred',
                            lambda t: pd.Series(200.0 + np.arange(80.0), index=idx))
        r = MB._fetch_index_metrics('^GSPC', 'S&P 500')
        assert r.get('source') == 'FRED'
        assert r['price'] == 279.0
        assert r['k'] is None  # close-only 無 KD
        assert r['error'] is None
        assert tmp_lastgood.exists()  # FRED 成功值也落盤 last-good

    def test_fred_not_supported_ticker_skips(self, fake_yf, tmp_lastgood, monkeypatch):
        fake_yf.df = _fake_df(0)
        called = []
        monkeypatch.setattr(MB, '_load_fred_key', lambda: called.append(1) or None)
        r = MB._fetch_index_metrics('^SOX', '費城半導體')
        assert r['error']  # ^SOX 無 FRED 序列 -> insufficient
        assert not called  # 連 key 都不該去讀 (sid mapping 先擋)


class TestStaleFallback:
    def test_insufficient_routes_through_lastgood(self, fake_yf, tmp_lastgood, monkeypatch):
        # early return 路徑也必須經過收口 -> 回 stale 而非空 error dict
        tmp_lastgood.write_text(json.dumps({'^GSPC': {
            'name': 'S&P 500', 'price': 123.0, 'data_date': '2026-06-08'}}),
            encoding='utf-8')
        fake_yf.df = _fake_df(0)
        monkeypatch.setattr(MB, '_fetch_index_close_fred', lambda t: None)
        r = MB._fetch_index_metrics('^GSPC', 'S&P 500')
        assert r.get('stale') is True
        assert r['price'] == 123.0

    def test_exception_routes_through_lastgood(self, tmp_lastgood, monkeypatch):
        import yfinance

        class _Dead:
            def __init__(self, *a):
                pass

            def history(self, **k):
                raise RuntimeError('outage')
        monkeypatch.setattr(yfinance, 'Ticker', _Dead)
        monkeypatch.setattr(MB, '_fetch_index_close_fred', lambda t: None)
        tmp_lastgood.write_text(json.dumps({'^IXIC': {
            'name': '那斯達克', 'price': 456.0, 'data_date': '2026-06-08'}}),
            encoding='utf-8')
        r = MB._fetch_index_metrics('^IXIC', '那斯達克')
        assert r.get('stale') is True
        assert r['price'] == 456.0
