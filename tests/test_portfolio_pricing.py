import pytest

import portfolio_pricing as pp

# 合成 currentTradingPeriod：pre[0,1000) / regular[1000,2000) / post[2000,3000)
_CTP = {
    'pre': {'start': 0, 'end': 1000},
    'regular': {'start': 1000, 'end': 2000},
    'post': {'start': 2000, 'end': 3000},
}


def _chart(**meta_over):
    """常規盤價 100、前收 95、pre/regular/post 三根 K 棒 98/100.4/103。"""
    meta = {
        'regularMarketPrice': 100.0,
        'regularMarketTime': 1900,
        'previousClose': 95.0,
        'chartPreviousClose': 94.0,
        'currency': 'USD',
        'currentTradingPeriod': _CTP,
    }
    meta.update(meta_over)
    return {
        'meta': meta,
        'timestamp': [500, 1500, 2500],
        'indicators': {'quote': [{'close': [98.0, 100.4, 103.0]}]},
    }


# ---- _classify_period ------------------------------------------------

@pytest.mark.parametrize('now,expect', [
    (500, 'PRE'), (1500, 'REGULAR'), (2500, 'POST'),
    (3500, 'CLOSED'), (0, 'PRE'), (2000, 'POST'),
])
def test_classify_period(now, expect):
    assert pp._classify_period(now, _CTP) == expect


def test_classify_period_no_ctp():
    assert pp._classify_period(1500, None) is None


# ---- _pick_us_price --------------------------------------------------

def test_pick_regular_uses_regular_market_price():
    """常規盤取 meta.regularMarketPrice（秒級新鮮），不取可能落後 1 分的 K 棒。"""
    q = pp._pick_us_price(_chart(), now_epoch=1500)
    assert q['price'] == pytest.approx(100.0)
    assert q['market_state'] == 'REGULAR'
    assert q['asof'] == 1900
    assert q['prev_close'] == pytest.approx(95.0)


def test_pick_premarket_uses_last_bar():
    """盤前取最後一根 <= now 的非空 K 棒（此時 now=500 只有 pre 棒）。"""
    q = pp._pick_us_price(_chart(), now_epoch=500)
    assert q['price'] == pytest.approx(98.0)
    assert q['market_state'] == 'PRE'
    assert q['asof'] == 500
    assert q['prev_close'] == pytest.approx(95.0)


def test_pick_postmarket_uses_last_bar():
    q = pp._pick_us_price(_chart(), now_epoch=2500)
    assert q['price'] == pytest.approx(103.0)
    assert q['market_state'] == 'POST'
    assert q['asof'] == 2500


def test_pick_closed_uses_regular_market_price():
    q = pp._pick_us_price(_chart(), now_epoch=3500)
    assert q['price'] == pytest.approx(100.0)
    assert q['market_state'] == 'CLOSED'


def test_pick_no_ctp_falls_back_to_regular_price():
    chart = _chart()
    chart['meta'].pop('currentTradingPeriod')
    q = pp._pick_us_price(chart, now_epoch=1500)
    assert q['price'] == pytest.approx(100.0)
    assert q['market_state'] is None


def test_pick_premarket_no_bars_falls_back_to_regular():
    """盤前尚無成交（K 棒全空）-> 退回常規盤價。"""
    chart = _chart()
    chart['indicators']['quote'][0]['close'] = [None, None, None]
    q = pp._pick_us_price(chart, now_epoch=500)
    assert q['price'] == pytest.approx(100.0)
    assert q['market_state'] == 'PRE'


def test_prev_close_fallback_order():
    """previousClose 缺 -> 用 chartPreviousClose（range=1d 兩者皆為昨收）。"""
    chart = _chart()
    chart['meta'].pop('previousClose')
    q = pp._pick_us_price(chart, now_epoch=1500)
    assert q['prev_close'] == pytest.approx(94.0)


# ---- get_us_quotes (integration) -------------------------------------

def test_get_us_quotes_regular(monkeypatch):
    monkeypatch.setattr(pp, '_yahoo_chart_result', lambda _sym: _chart())
    monkeypatch.setattr(pp, '_now_epoch', lambda: 1500)
    q = pp.get_us_quotes(['AAPL'])['AAPL']
    assert q['price'] == pytest.approx(100.0)
    assert q['prev_close'] == pytest.approx(95.0)
    assert q['change_pct'] == pytest.approx((100.0 - 95.0) / 95.0)
    assert q['source'] == 'yahoo'
    assert q['market_state'] == 'REGULAR'


def test_get_us_quotes_postmarket(monkeypatch):
    monkeypatch.setattr(pp, '_yahoo_chart_result', lambda _sym: _chart())
    monkeypatch.setattr(pp, '_now_epoch', lambda: 2500)
    q = pp.get_us_quotes(['AAPL'])['AAPL']
    assert q['price'] == pytest.approx(103.0)
    # 前收仍是昨收 -> 當日漲跌含盤後
    assert q['change_pct'] == pytest.approx((103.0 - 95.0) / 95.0)
    assert q['market_state'] == 'POST'


def test_get_us_quotes_skips_when_no_meta(monkeypatch):
    monkeypatch.setattr(pp, '_yahoo_chart_result', lambda _sym: None)
    monkeypatch.setattr(pp, '_now_epoch', lambda: 1500)
    assert pp.get_us_quotes(['AAPL']) == {}
