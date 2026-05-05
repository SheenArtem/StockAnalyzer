"""Unit tests for TAIFEXData.get_minifutures_oi_ratio (#6 散戶倉位 proxy).

Mocks _fetch_futures_near_oi to avoid network. Verifies ratio computation,
empty data handling, and month mismatch logging.
"""
from unittest.mock import patch, MagicMock

import pytest

from taifex_data import TAIFEXData


def _build_fake_futures_csv(records):
    """Build TX/MTX CSV body matching real layout (19 cols).

    records: list of tuples (month_str, oi, session)
    """
    header = "交易日期,契約,到期月份(週別),開盤價,最高價,最低價,收盤價,漲跌價,漲跌%,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,價差對單式委託成交量"
    lines = [header]
    for month_str, oi, session in records:
        oi_str = "-" if oi is None else str(oi)
        row = [
            "2026/05/05", "TX", month_str, "40000", "40500", "39500", "40200",
            "200", "0.5%", "10000", "40200", oi_str,
            "40180", "40220", "41000", "39000", "", session, "0",
        ]
        lines.append(",".join(row))
    while len(lines) < 5:  # pad to >= 3
        lines.append(",".join(["x"] * 19))
    return "\n".join(lines)


def _mock_resp(text):
    m = MagicMock()
    m.text = text
    m.raise_for_status = MagicMock()
    return m


def test_basic_ratio_computation():
    """TX OI=80000, MTX OI=20000 -> ratio=0.25."""
    td = TAIFEXData()
    tx_csv = _build_fake_futures_csv([
        ("202605", 80000, "一般"),
        ("202606", 5000, "一般"),  # 次月，不算近月
    ])
    mtx_csv = _build_fake_futures_csv([
        ("202605", 20000, "一般"),
        ("202606", 1000, "一般"),
    ])

    def fake_post(url, **kw):
        cid = kw['data']['commodity_id']
        return _mock_resp(tx_csv if cid == 'TX' else mtx_csv)

    with patch.object(td._session, 'post', side_effect=fake_post):
        r = td.get_minifutures_oi_ratio()

    assert r['near_month'] == "202605"
    assert r['txf_oi'] == 80000
    assert r['mtx_oi'] == 20000
    assert r['mtx_txf_ratio'] == 0.25


def test_excludes_weekly_options():
    """週選 (202605W1) 不該計入近月 OI。"""
    td = TAIFEXData()
    tx_csv = _build_fake_futures_csv([
        ("202605W1", 99999, "一般"),  # 週選排除
        ("202605", 80000, "一般"),
    ])
    mtx_csv = _build_fake_futures_csv([
        ("202605W1", 99999, "一般"),
        ("202605", 20000, "一般"),
    ])

    def fake_post(url, **kw):
        cid = kw['data']['commodity_id']
        return _mock_resp(tx_csv if cid == 'TX' else mtx_csv)

    with patch.object(td._session, 'post', side_effect=fake_post):
        r = td.get_minifutures_oi_ratio()

    assert r['txf_oi'] == 80000  # 週選 99999 不算
    assert r['mtx_oi'] == 20000


def test_excludes_after_hours_session():
    """盤後 session 該被排除。"""
    td = TAIFEXData()
    tx_csv = _build_fake_futures_csv([
        ("202605", 80000, "一般"),
        ("202605", 70000, "盤後"),  # 排除
    ])
    mtx_csv = _build_fake_futures_csv([
        ("202605", 20000, "一般"),
        ("202605", 18000, "盤後"),
    ])

    def fake_post(url, **kw):
        cid = kw['data']['commodity_id']
        return _mock_resp(tx_csv if cid == 'TX' else mtx_csv)

    with patch.object(td._session, 'post', side_effect=fake_post):
        r = td.get_minifutures_oi_ratio()

    assert r['txf_oi'] == 80000
    assert r['mtx_oi'] == 20000


def test_handles_empty_oi():
    """OI='-' 該被跳過（盤後常見）。"""
    td = TAIFEXData()
    tx_csv = _build_fake_futures_csv([
        ("202605", None, "一般"),  # OI='-'
        ("202605", 80000, "一般"),
    ])
    mtx_csv = _build_fake_futures_csv([
        ("202605", 20000, "一般"),
    ])

    def fake_post(url, **kw):
        cid = kw['data']['commodity_id']
        return _mock_resp(tx_csv if cid == 'TX' else mtx_csv)

    with patch.object(td._session, 'post', side_effect=fake_post):
        r = td.get_minifutures_oi_ratio()

    # '-' 跳過，剩下 80000 一筆 = TX OI 該為 80000
    assert r['txf_oi'] == 80000


def test_returns_empty_when_either_commodity_missing():
    """TX 有 / MTX 無資料時應回 empty result（不該硬算）。"""
    td = TAIFEXData()
    tx_csv = _build_fake_futures_csv([("202605", 80000, "一般")])
    mtx_csv = _build_fake_futures_csv([])  # MTX 無資料

    def fake_post(url, **kw):
        cid = kw['data']['commodity_id']
        return _mock_resp(tx_csv if cid == 'TX' else mtx_csv)

    with patch.object(td._session, 'post', side_effect=fake_post):
        r = td.get_minifutures_oi_ratio()

    assert r['txf_oi'] == 0
    assert r['mtx_oi'] == 0
    assert r['mtx_txf_ratio'] == 0.0


def test_month_mismatch_still_returns_ratio(caplog):
    """TX 近月 vs MTX 近月不同（罕見），應仍回 ratio 但 log warning。"""
    td = TAIFEXData()
    tx_csv = _build_fake_futures_csv([
        ("202605", 80000, "一般"),
    ])
    mtx_csv = _build_fake_futures_csv([
        ("202606", 20000, "一般"),  # 不同月
    ])

    def fake_post(url, **kw):
        cid = kw['data']['commodity_id']
        return _mock_resp(tx_csv if cid == 'TX' else mtx_csv)

    import logging
    with patch.object(td._session, 'post', side_effect=fake_post):
        with caplog.at_level(logging.WARNING):
            r = td.get_minifutures_oi_ratio()

    assert r['near_month'] == "202605"  # 用 TX 月份
    assert r['mtx_txf_ratio'] == 0.25
    assert any("near-month mismatch" in rec.message for rec in caplog.records)


def test_cache_hits_second_call():
    """同 instance 第二次 call 應 hit cache（不再 POST）。"""
    td = TAIFEXData()
    tx_csv = _build_fake_futures_csv([("202605", 80000, "一般")])
    mtx_csv = _build_fake_futures_csv([("202605", 20000, "一般")])

    def fake_post(url, **kw):
        cid = kw['data']['commodity_id']
        return _mock_resp(tx_csv if cid == 'TX' else mtx_csv)

    with patch.object(td._session, 'post', side_effect=fake_post) as mp:
        td.get_minifutures_oi_ratio()
        td.get_minifutures_oi_ratio()
        # 第一次 call 兩 commodity 各 1 次 POST = 2 次；第二次 cache hit = 0
        assert mp.call_count == 2
