"""持股表必須有「名稱」欄，且股名要能從各報價路徑一路傳到 UI。

2026-08-02 code review 把「`portfolio_view.py` 持股表『名稱』欄消失」列為待確認項，
並推測是刻意的 —— 理由寫「`mis.twse` 批次報價本來就不回股名」。**兩個前提都不對**：

1. 實打 `getStockInfo.jsp` 確認 payload 一直都有 `n`（簡稱，台積電）與 `nf`
   （全名，台灣積體電路製造股份有限公司），解碼也正常（U+53F0 U+7A4D U+96FB）。
   是 `_parse_quote` 自己把這兩欄丟掉，才讓 `get_tw_quotes` 只能寫死 `name: None`。
2. 「名稱」欄在投組 tab 首版（`15006ea`）就存在，是 `ff80466`（Whale 功能端到端移除，
   單一 commit 刪 234 檔）順手拿掉的 —— 那個 commit message 逐項列了它對
   `portfolio_view.py` 的必要修補，唯獨沒提刪欄與整批欄位重排。屬夾帶變更，不是決策。

所以這裡擋的方向是「大型重構把 UI 欄位無聲掃掉」：欄位存在與否要有測試背書，
不能只靠 review 時有人剛好看到。
"""
from datetime import date, timedelta

import pandas as pd
import pytest

import mis_twse_client
import portfolio_pricing as pp
import portfolio_view as pv


# ---- 1) mis.twse 的 n / nf 不可再被丟掉 ------------------------------

def _msg(**over):
    """一筆 msgArray element；價格欄位齊全以通過 _parse_quote 的取價順序。"""
    d = {'c': '2330', 'ex': 'tse', 'z': '2425.0', 'pz': '2420.0', 'y': '2400.0',
         'o': '2410.0', 'h': '2430.0', 'l': '2405.0', 'v': '4512',
         'n': '台積電', 'nf': '台灣積體電路製造股份有限公司',
         't': '13:30:00', 'tlong': '1785000000000'}
    d.update(over)
    return d


def test_parse_quote_carries_short_and_full_name():
    q = mis_twse_client._parse_quote(_msg(), 'tse')
    assert q['name'] == '台積電', 'payload 的 n 欄必須進報價 dict'
    assert q['full_name'] == '台灣積體電路製造股份有限公司'


def test_parse_quote_name_absent_or_blank_becomes_none():
    """缺欄與空字串都要回 None，UI 端才能用 `or ''` 一視同仁處理。"""
    no_field = mis_twse_client._parse_quote(_msg(n=None, nf=None), 'tse')
    blank = mis_twse_client._parse_quote(_msg(n='   ', nf=''), 'tse')
    assert no_field['name'] is None and no_field['full_name'] is None
    assert blank['name'] is None and blank['full_name'] is None


# ---- 2) get_tw_quotes 要傳遞股名（原本寫死 None）---------------------

def test_get_tw_quotes_propagates_name(monkeypatch):
    monkeypatch.setattr(mis_twse_client, 'get_quotes',
                        lambda ids: {'2330': mis_twse_client._parse_quote(_msg(), 'tse')})
    out = pp.get_tw_quotes(['2330'])
    assert out['2330']['name'] == '台積電', \
        'live 盤中路徑不會 fallback 到 EOD（只有 price is None 才會），' \
        '所以這裡回 None 等於盤中整欄空白'


def test_empty_quote_still_exposes_name_key():
    """抓不到時 name 欄要在且為 None —— UI 用 q.get('name') 不該因缺 key 而分歧。"""
    q = pp._empty_quote('2330')
    assert 'name' in q and q['name'] is None


# ---- 3) 持股表必須有「名稱」欄 ---------------------------------------

def _valued(ticker='2330', market='tw'):
    return [{
        'ticker': ticker, 'market': market, 'market_value': 1_000_000.0,
        'current_price': 2425.0, 'avg_cost': 2000.0, 'shares': 412.0,
        'unrealized_pnl': 175_100.0, 'return_pct': 0.2125,
        'entry_date': (date.today() - timedelta(days=30)).isoformat(),
    }]


@pytest.fixture()
def rendered(monkeypatch):
    """攔下 st.dataframe，取回它實際收到的 Styler 與底層 DataFrame。"""
    captured = {}

    def fake_dataframe(obj, **kwargs):
        captured['styler'] = obj if hasattr(obj, 'data') else None
        captured['df'] = obj.data if hasattr(obj, 'data') else obj

    monkeypatch.setattr(pv.st, 'dataframe', fake_dataframe)
    return captured


def test_holdings_table_has_name_column(rendered):
    quotes = {'2330': {'name': '台積電', 'change_pct': 0.0104}}
    pv._holdings_table(_valued(), quotes, 'tw', {'2330': 0.15})

    df = rendered['df']
    assert '名稱' in df.columns, \
        '首版就有的欄位；ff80466 夾帶刪除後兩個月沒人發現，所以要有測試釘住'
    assert df.loc[0, '名稱'] == '台積電'


def test_holdings_table_name_sits_next_to_ticker(rendered):
    """名稱要緊跟代號 —— 隔著一堆百分比欄位就失去「這是哪支股」的辨識作用。"""
    pv._holdings_table(_valued(), {'2330': {'name': '台積電'}}, 'tw', {})

    cols = list(rendered['df'].columns)
    assert cols[:2] == ['代號', '名稱'], f'實際欄序：{cols}'


def test_holdings_table_missing_name_renders_blank_not_crash(rendered):
    """抓不到股名時走空字串，不可讓整張表爆掉（停牌 / 下市股是真實情境）。"""
    pv._holdings_table(_valued(), {'2330': pp._empty_quote('2330')}, 'tw', {})

    df = rendered['df']
    assert df.loc[0, '名稱'] == ''
    assert pd.notna(df.loc[0, '代號'])


def test_holdings_table_styler_renders_name_as_text(rendered):
    """名稱是文字欄：若被誤塞進數值 format 或 `_color_signed`，Styler 實際算樣式時才會炸。
    所以這裡真的把 Styler 展開成 HTML（Streamlit 內部也是這樣消費它），
    只斷言 DataFrame 欄位存在擋不到這個方向。"""
    pv._holdings_table(_valued(), {'2330': {'name': '台積電', 'change_pct': -0.02}},
                       'tw', {'2330': -0.05})

    html = rendered['styler'].to_html()   # 格式化 + 上色都在這一步真正執行
    assert '名稱' in html and '台積電' in html
    assert '2,425.00' in html, '數值欄仍要正常格式化（千分位），不可因新增文字欄而整排壞掉'
