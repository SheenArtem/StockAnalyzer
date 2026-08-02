"""TPEX 全市場 EOD 必須打「認日期」的端點。

2026-08-02 第一輪查到 TPEX 舊端點
`web/stock/aftertrading/daily_close_quotes/stk_quote_result.php` **完全無視 `d`
參數**（請求 6 週前回最新橫斷面，價格一字不差），當時的處置是加 `strict_date`
把日期不符的 payload 丟掉 —— 那是**防守**，代價是「TPEX 歷史橫斷面拿不到」，
連帶讓 `docs/agent/data-sources.md` 與交接檔寫下「TPEX 不可行，上櫃股補不回來」。

第二輪實測發現那個結論太早：`www/zh-tw/afterTrading/dailyQuotes` **正確認日期**，
而且是同一份資料集 —— title 同為「上櫃股票行情」、19 個欄位（含 `均價`）順序相同、
成交量同為含定價口徑。實打對帳：
  - 2021-04-06 與 panel 重疊 624 檔，**收盤價 624/624、成交量 624/624 完全相同**
  - 同日官方另有 1,123 檔是 panel 缺的（正是要回填的量）
  - 非交易日（2021-04-04 週日）回 0 列

⚠️ 另一個認日期的端點 `www/zh-tw/afterTrading/otc?type=EW` **不可用**：那是
「上櫃股票每日收盤行情(不含定價)」，成交量口徑不同（876 檔上櫃股中只有 31 檔與
panel 相符，dailyQuotes 有 871 檔相符）。混用會讓 panel 出現兩種成交量定義。

本檔釘住的是「別再改回無視日期的端點」這個方向 —— 那種退步不會讓任何測試變紅，
只會讓歷史回填靜默拿到最新橫斷面。
"""
from datetime import datetime

import pandas as pd
import pytest

import twse_api
from twse_api import TWSEOpenData


_FIELDS = ['代號', '名稱', '收盤', '漲跌', '開盤', '最高', '最低', '均價',
           '成交股數', '成交金額(元)', '成交筆數', '最後買價', '最後買量(張數)',
           '最後賣價', '最後賣量(張數)', '發行股數', '次日 參考價',
           '次日 漲停價', '次日 跌停價']


def _row(sid, close):
    return [sid, f'N{sid}', f'{close:.2f}', '+0.00', f'{close:.2f}',
            f'{close:.2f}', f'{close:.2f}', f'{close:.2f}', '1,000,000',
            '100,000,000', '500', f'{close:.2f}', '10', '0.00', '0',
            '100,000,000', f'{close:.2f}', f'{close:.2f}', f'{close:.2f}']


def _payload(ymd_compact, roc):
    return {'date': ymd_compact, 'stat': 'ok',
            'tables': [{'title': '上櫃股票行情', 'date': roc,
                        'fields': _FIELDS,
                        'data': [_row('6488', 855.0), _row('3105', 300.0)]}]}


@pytest.fixture()
def api():
    return TWSEOpenData()


def _capture(api, monkeypatch, payload):
    """攔下 _fetch_json，記錄實際打的 url 與 params。"""
    seen = {}

    def fake_fetch_json(url, params=None, **kw):
        seen['url'] = url
        seen['params'] = dict(params or {})
        return payload

    monkeypatch.setattr(api, '_fetch_json', fake_fetch_json)
    return seen


def test_uses_date_aware_endpoint_not_the_legacy_one(api, monkeypatch):
    seen = _capture(api, monkeypatch, _payload('20210406', '110/04/06'))

    api.get_market_daily_tpex(date=datetime(2021, 4, 6), strict_date=True)

    assert 'stk_quote_result.php' not in seen['url'], (
        '舊端點無視 d 參數，歷史日會靜默拿到最新橫斷面')
    assert 'dailyQuotes' in seen['url'], f"實際打的是 {seen['url']}"


def test_request_carries_the_requested_date(api, monkeypatch):
    """日期要真的送出去，且是這個端點吃的西元 YYYY/MM/DD（不是民國）。"""
    seen = _capture(api, monkeypatch, _payload('20210406', '110/04/06'))

    api.get_market_daily_tpex(date=datetime(2021, 4, 6), strict_date=True)

    assert seen['params'].get('date') == '2021/04/06', seen['params']
    assert '110/04/06' not in str(seen['params'].values()), \
        '這個端點吃西元；送民國會被當成無效日期'


def test_otc_ew_endpoint_is_not_used(api, monkeypatch):
    """otc?type=EW 也認日期但成交量是「不含定價」口徑，與 panel 不同調。"""
    seen = _capture(api, monkeypatch, _payload('20210406', '110/04/06'))

    api.get_market_daily_tpex(date=datetime(2021, 4, 6))

    assert not seen['url'].rstrip('/').endswith('/otc'), \
        'otc?type=EW 的成交量不含定價，混用會讓 panel 出現兩種量值定義'


def test_historical_payload_is_accepted_and_stamped(api, monkeypatch):
    """認日期的端點回歷史日時，strict_date 不該把它擋掉。"""
    _capture(api, monkeypatch, _payload('20210406', '110/04/06'))

    df = api.get_market_daily_tpex(date=datetime(2021, 4, 6), strict_date=True)

    assert len(df) == 2
    assert set(df['data_date'].dropna().unique()) == {pd.Timestamp('2021-04-06')}
    assert df.loc[df['stock_id'] == '6488', 'close'].iloc[0] == pytest.approx(855.0)


def test_volume_column_is_read_by_name_layout(api, monkeypatch):
    """19 欄版（含「均價」）的成交股數在 index 8 —— 若誤用 17 欄版的 index 7
    會把「均價」當成交量讀進來，量值整排錯位且仍是正數，健康度檢查抓不到。"""
    _capture(api, monkeypatch, _payload('20210406', '110/04/06'))

    df = api.get_market_daily_tpex(date=datetime(2021, 4, 6))

    row = df[df['stock_id'] == '6488'].iloc[0]
    assert row['volume'] == 1_000_000, f"讀到 {row['volume']}（均價 855 = 錯位）"
    assert row['trading_value'] == 100_000_000


def test_strict_date_still_rejects_mismatched_payload(api, monkeypatch):
    """端點修好不代表可以拆掉防線：自報日期不符仍要丟掉。"""
    _capture(api, monkeypatch, _payload('20260731', '115/07/31'))

    df = api.get_market_daily_tpex(date=datetime(2021, 4, 6), strict_date=True)

    assert df.empty, '請求 2021-04-06 卻回 2026-07-31 的橫斷面，必須拒收'
