"""台股 CSV 判別 + 廣度面板不得混入美股（2026-08-02 code review）。

data_cache/ 以通用 {ticker}_price.csv 同時放台股與美股（使用者在 App 分析任何美股
都會刷新一份）。build_tw_breadth 原本沒過濾，導致約 500 檔美股進入 advances /
declines / ADL / McClellan / 新高低 / %above MA，且「台股休市但美股開盤」的日期會
產生整列純美股的假台股廣度列 —— macro_dashboard 取 iloc[-1]，連假期間會把美股盤勢
當台股顯示。
"""
import pandas as pd
import pytest

from tools import build_tw_breadth as btb
from tools.tw_universe import (
    TW_TICKER_RE,
    is_tw_ticker,
    ticker_from_price_csv,
    tw_price_csvs,
)


@pytest.mark.parametrize('ticker', ['2330', '0050', '911616', '2891A', '00878'])
def test_tw_tickers_accepted(ticker):
    assert is_tw_ticker(ticker)


@pytest.mark.parametrize('ticker', ['AAPL', 'IEX', 'ULTA', 'A', 'BRK.B', '^TWII', ''])
def test_us_and_index_tickers_rejected(ticker):
    assert not is_tw_ticker(ticker)


def test_ticker_from_price_csv():
    assert ticker_from_price_csv('data_cache/2330_price.csv') == '2330'
    assert ticker_from_price_csv('AAPL_price.csv') == 'AAPL'


def _write_csv(path, dates, closes):
    pd.DataFrame({'Close': closes, 'Volume': [1000] * len(closes)},
                 index=pd.to_datetime(dates)).to_csv(path)


def test_tw_price_csvs_filters_us(tmp_path):
    for name in ['2330_price.csv', '2891A_price.csv', 'AAPL_price.csv', 'ULTA_price.csv']:
        (tmp_path / name).write_text('x', encoding='utf-8')

    found = [ticker_from_price_csv(p) for p in tw_price_csvs(tmp_path)]

    assert found == ['2330', '2891A']


def test_breadth_excludes_us_and_their_trading_days(tmp_path, monkeypatch):
    """關鍵回歸：台股休市而美股開盤的日期，不得產生一列台股廣度。"""
    tw_dates = pd.bdate_range('2026-01-05', periods=10)
    # 美股多出一天台股休市日（模擬農曆年）
    us_dates = tw_dates.append(pd.DatetimeIndex([pd.Timestamp('2026-02-17')]))

    _write_csv(tmp_path / '2330_price.csv', tw_dates, range(100, 110))
    _write_csv(tmp_path / '2317_price.csv', tw_dates, range(200, 210))
    _write_csv(tmp_path / 'AAPL_price.csv', us_dates, range(300, 311))

    monkeypatch.setattr(btb, 'CACHE', tmp_path)
    close_df, vol_df = btb.load_all_prices(min_rows=5)

    assert sorted(close_df.columns) == ['2317', '2330']
    assert pd.Timestamp('2026-02-17') not in close_df.index
    assert len(close_df) == len(tw_dates)
    assert 'AAPL' not in vol_df.columns
