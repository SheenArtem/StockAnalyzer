"""Unit tests for taifex_data.TAIFEXData.get_options_institutional.

2026-05-09: 端點換 FinMind TaiwanOptionInstitutionalInvestors，原 TAIFEX
callsAndPutsDate HTML 端點 GET 完全忽略 date 參數會回最新一筆 (見 commit
d58e794 + reference_taifex_endpoint_history.md)。

Tests verify:
  - FinMind JSON parsing + net OI calculation (long - short)
  - Institution name mapping (外資 / 投信 / 自營商)
  - Skip days with < 6 inst-cp combos, find latest complete day
  - Cache hit on second call
  - Empty / no-token graceful handling
  - inst_pc_oi_skew sign convention

Mocks: `requests.get` for FinMind API + `TAIFEXData._get_finmind_token`.
"""
from unittest.mock import patch, MagicMock

import pytest

from taifex_data import TAIFEXData


def _make_finmind_row(date_iso: str, cp: str, inst: str,
                      long_oi: int, short_oi: int) -> dict:
    """Build one FinMind TaiwanOptionInstitutionalInvestors row."""
    return {
        'option_id': 'TXO',
        'date': date_iso,
        'call_put': cp,
        'institutional_investors': inst,
        'long_deal_volume': 0, 'long_deal_amount': 0,
        'short_deal_volume': 0, 'short_deal_amount': 0,
        'long_open_interest_balance_volume': long_oi,
        'long_open_interest_balance_amount': 0,
        'short_open_interest_balance_volume': short_oi,
        'short_open_interest_balance_amount': 0,
    }


def _build_finmind_response(rows_by_date: dict) -> dict:
    """Build full FinMind response.

    rows_by_date: {date_iso: [(cp, inst, long_oi, short_oi), ...]}
    """
    data = []
    for d_iso, rows in rows_by_date.items():
        for cp, inst, long_oi, short_oi in rows:
            data.append(_make_finmind_row(d_iso, cp, inst, long_oi, short_oi))
    return {'msg': 'success', 'status': 200, 'data': data}


def _mock_response(payload: dict):
    """Mock requests.Response for FinMind."""
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture
def patch_token():
    """Always return fake token so the method doesn't short-circuit."""
    with patch.object(TAIFEXData, '_get_finmind_token',
                       MagicMock(return_value='fake_token_xxx')):
        yield


def test_happy_path_parses_six_institutions(patch_token):
    """Full TXO data with all 3 institutions x 2 CPs (one date)."""
    # net = long - short
    # 自營商 buy 8000-4132=3868, etc.
    payload = _build_finmind_response({
        '2026-05-08': [
            ('買權', '自營商', 8000, 4132),    # net 3868
            ('買權', '投信', 100, 621),        # net -521
            ('買權', '外資', 5000, 2246),      # net 2754
            ('賣權', '自營商', 12000, 1586),   # net 10414
            ('賣權', '投信', 100, 33),         # net 67
            ('賣權', '外資', 5000, 2476),      # net 2524
        ],
    })
    td = TAIFEXData()
    with patch('taifex_data.requests.get', return_value=_mock_response(payload)):
        r = td.get_options_institutional()

    assert r['foreign_call_net'] == 2754
    assert r['foreign_put_net'] == 2524
    assert r['trust_call_net'] == -521
    assert r['trust_put_net'] == 67
    assert r['dealer_call_net'] == 3868
    assert r['dealer_put_net'] == 10414
    assert r['inst_call_net_total'] == 3868 + (-521) + 2754
    assert r['inst_put_net_total'] == 10414 + 67 + 2524
    assert r['inst_pc_oi_skew'] == r['inst_put_net_total'] - r['inst_call_net_total']
    assert r['data_date'] is not None
    assert r['data_date'].isoformat() == '2026-05-08'


def test_picks_latest_complete_day(patch_token):
    """When multiple days returned, pick latest with all 6 inst-cp combos."""
    payload = _build_finmind_response({
        # 5/6 incomplete (only call side, 3/6)
        '2026-05-06': [
            ('買權', '自營商', 100, 0),
            ('買權', '投信', 200, 0),
            ('買權', '外資', 300, 0),
        ],
        # 5/7 complete (use this — latest complete)
        '2026-05-07': [
            ('買權', '自營商', 10, 0),
            ('買權', '投信', 20, 0),
            ('買權', '外資', 30, 0),
            ('賣權', '自營商', 40, 0),
            ('賣權', '投信', 50, 0),
            ('賣權', '外資', 60, 0),
        ],
        # 5/8 also incomplete (3/6)
        '2026-05-08': [
            ('買權', '自營商', 999, 0),
        ],
    })
    td = TAIFEXData()
    with patch('taifex_data.requests.get', return_value=_mock_response(payload)):
        r = td.get_options_institutional()

    # Should pick 5/7, not 5/8 (incomplete) and not 5/6 (older incomplete)
    assert r['data_date'].isoformat() == '2026-05-07'
    assert r['foreign_call_net'] == 30
    assert r['foreign_put_net'] == 60


def test_no_complete_day_returns_empty(patch_token):
    """If no day has all 6 inst-cp combos, return defaults (zeros)."""
    payload = _build_finmind_response({
        '2026-05-08': [
            ('買權', '自營商', 100, 0),
            ('買權', '投信', 200, 0),
            # missing 外資 + 賣權 entirely
        ],
    })
    td = TAIFEXData()
    with patch('taifex_data.requests.get', return_value=_mock_response(payload)):
        r = td.get_options_institutional()
    assert r['data_date'] is None
    assert r['foreign_call_net'] == 0
    assert r['inst_pc_oi_skew'] == 0


def test_empty_finmind_response(patch_token):
    """FinMind returns no data → result stays default zeros."""
    payload = {'msg': 'success', 'status': 200, 'data': []}
    td = TAIFEXData()
    with patch('taifex_data.requests.get', return_value=_mock_response(payload)):
        r = td.get_options_institutional()
    assert r['data_date'] is None
    assert r['foreign_call_net'] == 0


def test_no_token_short_circuits():
    """If FINMIND token missing, method returns defaults without making request."""
    td = TAIFEXData()
    with patch.object(TAIFEXData, '_get_finmind_token',
                       MagicMock(return_value='')), \
         patch('taifex_data.requests.get') as mock_get:
        r = td.get_options_institutional()
    mock_get.assert_not_called()
    assert r['data_date'] is None
    assert r['foreign_call_net'] == 0


def test_cache_hits_second_call(patch_token):
    """Second call within TTL hits cache, no new HTTP request."""
    payload = _build_finmind_response({
        '2026-05-08': [
            ('買權', '自營商', 100, 0),
            ('買權', '投信', 200, 0),
            ('買權', '外資', 300, 0),
            ('賣權', '自營商', 400, 0),
            ('賣權', '投信', 500, 0),
            ('賣權', '外資', 600, 0),
        ],
    })
    td = TAIFEXData()
    with patch('taifex_data.requests.get',
               return_value=_mock_response(payload)) as mock_get:
        r1 = td.get_options_institutional()
        r2 = td.get_options_institutional()
        assert r1 == r2
        assert mock_get.call_count == 1


def test_pc_oi_skew_sign_convention(patch_token):
    """skew > 0 means put_total > call_total (defensive/bearish)."""
    payload = _build_finmind_response({
        '2026-05-08': [
            ('買權', '自營商', 100, 0),
            ('買權', '投信', 0, 0),
            ('買權', '外資', 0, 0),
            ('賣權', '自營商', 500, 0),
            ('賣權', '投信', 0, 0),
            ('賣權', '外資', 0, 0),
        ],
    })
    td = TAIFEXData()
    with patch('taifex_data.requests.get', return_value=_mock_response(payload)):
        r = td.get_options_institutional()
    # call_total=100, put_total=500, skew=400 (bearish)
    assert r['inst_call_net_total'] == 100
    assert r['inst_put_net_total'] == 500
    assert r['inst_pc_oi_skew'] == 400
