import pytest

import portfolio_pricing as pp


def test_us_quote_uses_previous_daily_close_not_chart_window_base(monkeypatch):
    """Yahoo chartPreviousClose is the 5d window base, not always yesterday."""
    chart = {
        'meta': {
            'regularMarketPrice': 4.465,
            'chartPreviousClose': 3.9,
            'currency': 'USD',
            'regularMarketTime': 1782917137,
        },
        'indicators': {
            'quote': [{
                'close': [3.78, 4.18, 4.15, 4.08, 4.465],
            }],
        },
    }
    monkeypatch.setattr(pp, '_yahoo_chart_result', lambda _symbol: chart)

    q = pp.get_us_quotes(['CRMG'])['CRMG']

    assert q['price'] == pytest.approx(4.465)
    assert q['prev_close'] == pytest.approx(4.08)
    assert q['change_pct'] == pytest.approx((4.465 - 4.08) / 4.08)


def test_us_quote_uses_latest_daily_close_when_price_is_intraday(monkeypatch):
    chart = {
        'meta': {
            'regularMarketPrice': 4.2,
            'chartPreviousClose': 3.5,
            'currency': 'USD',
        },
        'indicators': {
            'quote': [{
                'close': [3.9, 4.08],
            }],
        },
    }
    monkeypatch.setattr(pp, '_yahoo_chart_result', lambda _symbol: chart)

    q = pp.get_us_quotes(['CRMG'])['CRMG']

    assert q['prev_close'] == pytest.approx(4.08)
    assert q['change_pct'] == pytest.approx((4.2 - 4.08) / 4.08)


def test_us_quote_does_not_use_taiwan_calendar_day_for_us_session(monkeypatch):
    chart = {
        'meta': {
            'regularMarketPrice': 101.0,
            'chartPreviousClose': 95.0,
            'currency': 'USD',
            # 2026-07-02 morning in Taiwan can still be the 2026-07-01
            # US trading session. The quote logic must follow Yahoo's bar
            # sequence, not the local Taiwan calendar date.
            'regularMarketTime': 1782957600,
        },
        'timestamp': [1782754200, 1782840600, 1782927000],
        'indicators': {
            'quote': [{
                'close': [98.0, 100.0, 101.0],
            }],
        },
    }
    monkeypatch.setattr(pp, '_yahoo_chart_result', lambda _symbol: chart)

    q = pp.get_us_quotes(['AAPL'])['AAPL']

    assert q['prev_close'] == pytest.approx(100.0)
    assert q['change_pct'] == pytest.approx(0.01)
