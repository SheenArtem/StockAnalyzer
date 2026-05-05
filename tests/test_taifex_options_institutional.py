"""Unit tests for taifex_data.TAIFEXData.get_options_institutional.

Verifies HTML row layout assumptions (16/14/13-cell rows), product filter
(TXO only), institution alias map, and 衍生指標 calculation.
Mocks _session.get to avoid network calls.
"""
from unittest.mock import patch, MagicMock

import pytest

from taifex_data import TAIFEXData


def _build_row(cells):
    """Build <tr> with td cells (each is a string)."""
    tds = "".join(f"<td>{c}</td>" for c in cells)
    return f"<tr>{tds}</tr>"


def _build_html(product_rows):
    """Build a full callsAndPutsDate HTML page with table.table_f.

    product_rows: list of dicts:
        {product, cp_groups: [(cp, inst_rows)], where inst_rows is
         list of (identity, oi_net, [11 other nums optional])}

    Builds 16-cell row for first inst of each product/cp combo (with
    序號/商品/權別/身份別 prefix), 14-cell when product same but cp switches,
    13-cell when product+cp same and just institution differs.

    For each numeric row, the 12 numbers correspond to:
      [0]交易買口 [1]買金 [2]賣口 [3]賣金 [4]差額口 [5]差額金
      [6]OI買口 [7]OI買金 [8]OI賣口 [9]OI賣金 [10]OI差額口 [11]OI差額金

    We auto-fill all 12 with 0 except [10] which we set to oi_net.
    """
    rows_html = [
        '<tr><td></td><td>交易口數與契約金額</td><td>未平倉餘額</td></tr>',
        '<tr><td>買方</td><td>賣方</td><td>買賣差額</td><td>買方</td><td>賣方</td><td>買賣差額</td></tr>',
        '<tr><td>序號</td><td>商品名稱</td><td>權別</td><td>身份別</td>'
        + '<td>口數</td><td>契約金額</td>' * 6 + '</tr>',
    ]

    for idx, prod_block in enumerate(product_rows, 1):
        product = prod_block['product']
        first_cp_inst_seen = False
        for cp_idx, (cp, inst_rows) in enumerate(prod_block['cp_groups']):
            for inst_idx, inst_row in enumerate(inst_rows):
                identity, oi_net = inst_row[0], inst_row[1]
                nums = ['0'] * 12
                nums[10] = str(oi_net)
                if not first_cp_inst_seen:
                    # 16-cell row
                    cells = [str(idx), product, cp, identity] + nums
                    first_cp_inst_seen = True
                elif inst_idx == 0:
                    # 14-cell row (cp switched within same product)
                    cells = [cp, identity] + nums
                else:
                    # 13-cell row (next institution, same cp)
                    cells = [identity] + nums
                rows_html.append(_build_row(cells))

    table_html = '<table class="table_f">' + ''.join(rows_html) + '</table>'
    # Need >= 5000 chars to pass the sanity check inside the method.
    padding = '<div>' + 'x' * 6000 + '</div>'
    return f'<html><body>{table_html}{padding}</body></html>'


def _mock_response(html):
    resp = MagicMock(text=html)
    resp.raise_for_status = MagicMock()
    return resp


def test_happy_path_parses_six_institutions():
    """Full TXO data with all 3 institutions x 2 CPs."""
    html = _build_html([
        {
            'product': '臺指選擇權',
            'cp_groups': [
                ('買權', [('自營商', 3868), ('投信', -521), ('外資', 2754)]),
                ('賣權', [('自營商', 10414), ('投信', 67), ('外資', 2524)]),
            ],
        },
    ])
    td = TAIFEXData()
    with patch.object(td._session, 'get', return_value=_mock_response(html)):
        r = td.get_options_institutional()

    assert r['foreign_call_net'] == 2754
    assert r['foreign_put_net'] == 2524
    assert r['trust_call_net'] == -521
    assert r['trust_put_net'] == 67
    assert r['dealer_call_net'] == 3868
    assert r['dealer_put_net'] == 10414
    # totals = sum of three institutions
    assert r['inst_call_net_total'] == 3868 + (-521) + 2754
    assert r['inst_put_net_total'] == 10414 + 67 + 2524
    assert r['inst_pc_oi_skew'] == r['inst_put_net_total'] - r['inst_call_net_total']
    assert r['data_date'] is not None


def test_excludes_non_txo_products():
    """電子選擇權 / 股票選擇權 should be ignored — only TXO matters."""
    html = _build_html([
        {
            'product': '電子選擇權',
            'cp_groups': [
                ('買權', [('自營商', 999), ('投信', 999), ('外資', 999)]),
                ('賣權', [('自營商', 999), ('投信', 999), ('外資', 999)]),
            ],
        },
        {
            'product': '臺指選擇權',
            'cp_groups': [
                ('買權', [('自營商', 100), ('投信', 200), ('外資', 300)]),
                ('賣權', [('自營商', 400), ('投信', 500), ('外資', 600)]),
            ],
        },
    ])
    td = TAIFEXData()
    with patch.object(td._session, 'get', return_value=_mock_response(html)):
        r = td.get_options_institutional()

    # Only TXO numbers — 電子 999s should be filtered out
    assert r['foreign_call_net'] == 300
    assert r['foreign_put_net'] == 600
    assert r['dealer_call_net'] == 100
    assert r['dealer_put_net'] == 400


def test_foreign_alias_mapped():
    """外國機構投資人 also maps to foreign."""
    html = _build_html([
        {
            'product': '臺指選擇權',
            'cp_groups': [
                ('買權', [('自營商', 1), ('投信', 2), ('外國機構投資人', 3)]),
                ('賣權', [('自營商', 4), ('投信', 5), ('外國機構投資人', 6)]),
            ],
        },
    ])
    td = TAIFEXData()
    with patch.object(td._session, 'get', return_value=_mock_response(html)):
        r = td.get_options_institutional()

    assert r['foreign_call_net'] == 3
    assert r['foreign_put_net'] == 6


def test_incomplete_data_skips_day():
    """Missing institutions (only 4 of 6 needed) -> day rejected, retry next."""
    # Only call side filled
    html_incomplete = _build_html([
        {
            'product': '臺指選擇權',
            'cp_groups': [
                ('買權', [('自營商', 1), ('投信', 2), ('外資', 3)]),
                # Note: 賣權 missing -> only 3/6 entries
            ],
        },
    ])
    # Subsequent call (next day in retry loop) returns full data
    html_full = _build_html([
        {
            'product': '臺指選擇權',
            'cp_groups': [
                ('買權', [('自營商', 10), ('投信', 20), ('外資', 30)]),
                ('賣權', [('自營商', 40), ('投信', 50), ('外資', 60)]),
            ],
        },
    ])

    td = TAIFEXData()
    responses = [
        _mock_response(html_incomplete),
        _mock_response(html_full),
        _mock_response(html_full),
        _mock_response(html_full),
        _mock_response(html_full),
    ]
    with patch.object(td._session, 'get', side_effect=responses):
        r = td.get_options_institutional()

    # Should have parsed the full one, not the incomplete one
    assert r['foreign_call_net'] == 30
    assert r['foreign_put_net'] == 60


def test_cache_hits_second_call():
    """Second call within TTL hits cache, no new GET."""
    html = _build_html([
        {
            'product': '臺指選擇權',
            'cp_groups': [
                ('買權', [('自營商', 1), ('投信', 2), ('外資', 3)]),
                ('賣權', [('自營商', 4), ('投信', 5), ('外資', 6)]),
            ],
        },
    ])
    td = TAIFEXData()
    with patch.object(td._session, 'get', return_value=_mock_response(html)) as mock_get:
        r1 = td.get_options_institutional()
        r2 = td.get_options_institutional()
        assert r1 == r2
        assert mock_get.call_count == 1


def test_short_response_skipped():
    """HTML body < 5000 bytes is treated as empty/error response and skipped."""
    short_resp = MagicMock(text='<html></html>')
    short_resp.raise_for_status = MagicMock()
    td = TAIFEXData()
    with patch.object(td._session, 'get', return_value=short_resp):
        r = td.get_options_institutional()
    # Falls through all 5 retries without finding data -> default zeros
    assert r['data_date'] is None
    assert r['foreign_call_net'] == 0
    assert r['inst_pc_oi_skew'] == 0


def test_pc_oi_skew_sign_convention():
    """skew > 0 means 法人 put_total > call_total (defensive/bearish)."""
    html_bearish = _build_html([
        {
            'product': '臺指選擇權',
            'cp_groups': [
                ('買權', [('自營商', 100), ('投信', 0), ('外資', 0)]),
                ('賣權', [('自營商', 500), ('投信', 0), ('外資', 0)]),
            ],
        },
    ])
    td = TAIFEXData()
    with patch.object(td._session, 'get', return_value=_mock_response(html_bearish)):
        r = td.get_options_institutional()
    # call_total=100, put_total=500, skew=400 (bearish)
    assert r['inst_call_net_total'] == 100
    assert r['inst_put_net_total'] == 500
    assert r['inst_pc_oi_skew'] == 400
