from datetime import datetime

from tools import chip_history_dl as CHIP


def test_tpex_short_sale_batch_uses_positional_duplicate_columns(monkeypatch):
    """TPEX 表內融券與借券欄名重複，必須依區段位置取值。

    實際 payload 的「前日餘額／當日餘額」各出現兩次；若建 field_map，借券餘額會
    靜默拿到融券欄。這裡也釘住 4 碼普通股篩選，避免 ETF 混進既有 stock panel。
    """
    payload = {
        "aaData": [
            [
                "3324", "雙鴻", "1,000", "11", "12", "13", "1,004", "9,999",
                "2,000", "21", "22", "-3", "1,996", "8,888", "",
            ],
            [
                "00679B", "元大美債", "100", "1", "2", "3", "99", "999",
                "200", "4", "5", "6", "205", "888", "",
            ],
        ]
    }
    seen = {}

    def fake_fetch(url, params):
        seen["url"] = url
        seen["params"] = params
        return payload

    monkeypatch.setattr(CHIP, "_fetch_json", fake_fetch)
    monkeypatch.setattr(CHIP.time, "sleep", lambda _seconds: None)

    rows = CHIP._fetch_shortsale_tpex_one_day(datetime(2026, 8, 21))

    assert seen["url"].endswith("/margin_sbl/margin_sbl_result.php")
    assert seen["params"]["d"] == "115/08/21"
    assert rows == [{
        "date": "2026-08-21",
        "stock_id": "3324",
        "sbl_balance": 1996,
        "sbl_sell": 21,
        "sbl_return": 22,
        "sbl_adjust": -3,
        "ms_sell": 11,
        "ms_buy": 12,
        "ms_balance": 1004,
        "market": "tpex",
    }]


def test_tpex_short_sale_batch_accepts_tables_payload(monkeypatch):
    """TPEX 新舊回應外殼不同時，tables[0].data 仍須可解析。"""
    row = [
        "6442", "光聖", "10", "1", "2", "0", "9", "100",
        "20", "3", "4", "5", "24", "200", "",
    ]
    monkeypatch.setattr(
        CHIP, "_fetch_json", lambda _url, _params: {"tables": [{"data": [row]}]})
    monkeypatch.setattr(CHIP.time, "sleep", lambda _seconds: None)

    rows = CHIP._fetch_shortsale_tpex_one_day(datetime(2026, 8, 21))

    assert len(rows) == 1
    assert rows[0]["stock_id"] == "6442"
    assert rows[0]["sbl_balance"] == 24
    assert rows[0]["ms_balance"] == 9
