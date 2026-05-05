"""Unit tests for taifex_data.TAIFEXData.get_atm_put_premium.

Verifies CSV column layout assumptions + ATM/OTM5 strike picking + skew formula.
Mocks the underlying _session.post + get_futures_basis to avoid network calls.
"""
from unittest.mock import patch, MagicMock

import pytest

from taifex_data import TAIFEXData


def _build_fake_csv(records):
    """Build TXO CSV body matching real layout (21 cols).

    records: list of (month_str, strike, cp, close, session)
    Pads to >= 10 total lines (real-data sanity guard in get_atm_put_premium).
    """
    header = "交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,最後最佳買價,最後最佳賣價,歷史最高價,歷史最低價,是否因訊息面暫停交易,交易時段,漲跌價,漲跌%,契約到期日"
    lines = [header]
    for month_str, strike, cp, close, session in records:
        row = [
            "2026/05/05", "TXO", month_str, str(strike), cp,
            "0.1", "0.5", "0.1", str(close), "100", str(close), "500",
            "0.4", "0.6", "1.0", "0.1", "", session, "0.1", "0.5%", "20260521",
        ]
        lines.append(",".join(row))
    # Pad with throwaway rows (different month + already excluded via cp_type unmatched)
    # to satisfy the `len(lines) < 10` real-data sanity check inside the method.
    pad_row = ",".join([
        "2026/05/05", "TXO", "999999", "99999.0", "X",
        "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "", "X", "-", "-", "20260521",
    ])
    while len(lines) < 12:
        lines.append(pad_row)
    return "\n".join(lines)


@pytest.fixture
def taifex_with_mock_basis():
    td = TAIFEXData()
    # Mock get_futures_basis to return predictable reference
    td.get_futures_basis = MagicMock(return_value={
        'basis': -10.0, 'futures_price': 21000.0, 'spot_price': 21010.0,
        'basis_pct': -0.05, 'data_date': None,
    })
    return td


def test_atm_picks_strike_closest_to_spot(taifex_with_mock_basis):
    """ATM = strike 最接近 reference (spot=21010 -> 21000)."""
    csv = _build_fake_csv([
        ("202605", 20800, "賣權", 50, "一般"),
        ("202605", 20900, "賣權", 100, "一般"),
        ("202605", 21000, "賣權", 250, "一般"),  # ATM
        ("202605", 21100, "賣權", 380, "一般"),
        ("202605", 19950, "賣權", 30, "一般"),  # OTM5 (~21010 * 0.95 = 19960)
    ])
    mock_resp = MagicMock(text=csv)
    mock_resp.raise_for_status = MagicMock()

    with patch.object(taifex_with_mock_basis._session, 'post', return_value=mock_resp):
        r = taifex_with_mock_basis.get_atm_put_premium()

    assert r['atm_strike'] == 21000
    assert r['atm_put_close'] == 250.0
    assert r['otm5_strike'] == 19950
    assert r['otm5_put_close'] == 30.0
    # skew = otm / atm = 30 / 250 = 0.12
    assert r['put_skew'] == 0.12
    # atm_pct = 250 / 21010 * 100 ≈ 1.190
    assert abs(r['atm_put_pct'] - 1.190) < 0.01
    assert r['near_month'] == "202605"


def test_excludes_weekly_options(taifex_with_mock_basis):
    """週選 (W1/W2) 不應該被 picked into 近月計算。"""
    csv = _build_fake_csv([
        ("202605W1", 21000, "賣權", 50, "一般"),  # 週選 — 排除
        ("202605W2", 21000, "賣權", 60, "一般"),  # 週選 — 排除
        ("202605", 21000, "賣權", 250, "一般"),   # 月選 ATM
        ("202605", 19950, "賣權", 30, "一般"),    # 月選 OTM5
    ])
    mock_resp = MagicMock(text=csv)
    mock_resp.raise_for_status = MagicMock()

    with patch.object(taifex_with_mock_basis._session, 'post', return_value=mock_resp):
        r = taifex_with_mock_basis.get_atm_put_premium()

    assert r['atm_put_close'] == 250.0  # 月選 not 週選 50/60


def test_excludes_after_hours_session(taifex_with_mock_basis):
    """盤後 session 不該污染 ATM。只取 '一般' 盤。"""
    csv = _build_fake_csv([
        ("202605", 21000, "賣權", 250, "一般"),   # ATM 一般盤
        ("202605", 21000, "賣權", 240, "盤後"),   # 盤後 — 排除
        ("202605", 19950, "賣權", 30, "一般"),
    ])
    mock_resp = MagicMock(text=csv)
    mock_resp.raise_for_status = MagicMock()

    with patch.object(taifex_with_mock_basis._session, 'post', return_value=mock_resp):
        r = taifex_with_mock_basis.get_atm_put_premium()

    assert r['atm_put_close'] == 250.0  # 一般盤


def test_excludes_calls_and_dash_close(taifex_with_mock_basis):
    """買權不算; close='-' 跳過。"""
    csv = _build_fake_csv([
        ("202605", 21000, "買權", 500, "一般"),   # 買權 — 排除
        ("202605", 21000, "賣權", 250, "一般"),
        ("202605", 21100, "賣權", "-", "一般"),   # close='-' 排除
        ("202605", 19950, "賣權", 30, "一般"),
    ])
    mock_resp = MagicMock(text=csv)
    mock_resp.raise_for_status = MagicMock()

    with patch.object(taifex_with_mock_basis._session, 'post', return_value=mock_resp):
        r = taifex_with_mock_basis.get_atm_put_premium()

    assert r['atm_put_close'] == 250.0


def test_uses_nearest_month_when_multiple(taifex_with_mock_basis):
    """有多個月份時取字典序最早 (= 真正的近月)."""
    csv = _build_fake_csv([
        ("202605", 21000, "賣權", 250, "一般"),  # near
        ("202606", 21000, "賣權", 400, "一般"),  # next
        ("202605", 19950, "賣權", 30, "一般"),
    ])
    mock_resp = MagicMock(text=csv)
    mock_resp.raise_for_status = MagicMock()

    with patch.object(taifex_with_mock_basis._session, 'post', return_value=mock_resp):
        r = taifex_with_mock_basis.get_atm_put_premium()

    assert r['near_month'] == "202605"
    assert r['atm_put_close'] == 250.0  # 近月不是次月


def test_zero_reference_returns_empty():
    """spot/futures 都是 0 時回 empty result（不 crash）。"""
    td = TAIFEXData()
    td.get_futures_basis = MagicMock(return_value={
        'basis': 0.0, 'futures_price': 0.0, 'spot_price': 0.0,
        'basis_pct': 0.0, 'data_date': None,
    })
    r = td.get_atm_put_premium()
    assert r['atm_strike'] == 0
    assert r['atm_put_close'] == 0.0


def test_cache_hits_second_call(taifex_with_mock_basis):
    """同一 instance 第二次呼叫應該 hit cache, 不再 POST."""
    csv = _build_fake_csv([
        ("202605", 21000, "賣權", 250, "一般"),
        ("202605", 19950, "賣權", 30, "一般"),
    ])
    mock_resp = MagicMock(text=csv)
    mock_resp.raise_for_status = MagicMock()

    with patch.object(taifex_with_mock_basis._session, 'post', return_value=mock_resp) as mock_post:
        r1 = taifex_with_mock_basis.get_atm_put_premium()
        r2 = taifex_with_mock_basis.get_atm_put_premium()
        assert r1 == r2
        # POST 只該被呼叫 1 次（第二次走 cache）
        assert mock_post.call_count == 1
