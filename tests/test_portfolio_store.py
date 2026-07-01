"""portfolio_store 資料模型測試 — 市場判別 / CRUD round-trip / 移動平均成本 /
部分賣出已實現損益（含 fee-tax）/ 全平倉重置 / 賣超 fail-loud / 亂序時序化 / 估值彙總。

UI (render_portfolio) 不在 cover 範圍（見 tests/README.md 設計原則 3），
只測會算錯損益 / 弄丟使用者交易的純邏輯。
"""
import json

import pandas as pd
import pytest

import portfolio_store as ps


def _price(dates, vals):
    return pd.Series(vals, index=pd.to_datetime(dates), dtype=float)


def _patch_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, 'MANUAL_TRADES_DIR', tmp_path)


# ---------------- 市場 / 代號 ----------------

def test_detect_market():
    assert ps.detect_market('2330') == 'tw'
    assert ps.detect_market('2330.TW') == 'tw'
    assert ps.detect_market('6488.TWO') == 'tw'
    assert ps.detect_market('0050') == 'tw'
    assert ps.detect_market('AAPL') == 'us'
    assert ps.detect_market('BRK.B') == 'us'


def test_normalize_ticker():
    assert ps.normalize_ticker('2330.TW') == '2330'
    assert ps.normalize_ticker('6488.two') == '6488'
    assert ps.normalize_ticker(' aapl ') == 'AAPL'


# ---------------- 儲存層 ----------------

def test_load_empty_when_no_file(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    assert ps.load_transactions() == []


def test_save_load_roundtrip(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    rec = ps.add_transaction('2330', 'buy', '2026-06-10', 1000, 925.0,
                             fee=131.0, note='進場')
    assert rec['id'].startswith('MT-20260610-')
    assert rec['ticker'] == '2330' and rec['market'] == 'tw'
    loaded = ps.load_transactions()
    assert len(loaded) == 1 and loaded[0]['price'] == 925.0
    # 檔案格式含 schema_version
    data = json.loads(ps._store_file().read_text(encoding='utf-8'))
    assert data['schema_version'] == ps.SCHEMA_VERSION


def test_load_corrupt_raises(tmp_path, monkeypatch):
    """毀損檔要 fail loud，不可回 [] 讓後續 save 覆蓋。"""
    _patch_dir(tmp_path, monkeypatch)
    ps._store_file().write_text('{ this is not json', encoding='utf-8')
    with pytest.raises(ValueError):
        ps.load_transactions()


def test_id_unique_after_delete(tmp_path, monkeypatch):
    """刪除後再新增，新 id 不得與現存任何 id 碰撞（update/delete 靠 id 定位）。"""
    _patch_dir(tmp_path, monkeypatch)
    r1 = ps.add_transaction('2330', 'buy', '2026-06-10', 1000, 900)
    r2 = ps.add_transaction('2330', 'buy', '2026-06-10', 1000, 910)
    assert r1['id'].endswith('0001') and r2['id'].endswith('0002')
    ps.delete_transaction(r1['id'])           # 刪最早那筆
    r3 = ps.add_transaction('2330', 'buy', '2026-06-10', 1000, 920)
    existing = {t['id'] for t in ps.load_transactions()}
    assert existing == {r2['id'], r3['id']}   # 兩筆、互不碰撞
    assert r3['id'].endswith('0003')          # max(現存 0002) + 1


# ---------------- 驗證 ----------------

@pytest.mark.parametrize('kw,msg_has', [
    (dict(ticker='', action='buy', txn_date='2026-06-10', shares=1, price=1), '代號'),
    (dict(ticker='2330', action='hold', txn_date='2026-06-10', shares=1, price=1), 'action'),
    (dict(ticker='2330', action='buy', txn_date='2026-06-10', shares=0, price=1), '股數'),
    (dict(ticker='2330', action='buy', txn_date='2026-06-10', shares=1, price=0), '價格'),
    (dict(ticker='2330', action='buy', txn_date='2026-06-10', shares=1, price=1, fee=-1), '負'),
    (dict(ticker='2330', action='buy', txn_date='bad-date', shares=1, price=1), '日期'),
])
def test_validate_rejects(kw, msg_has):
    ok, err = ps.validate_transaction(**kw)
    assert not ok and msg_has in err


def test_add_invalid_raises(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        ps.add_transaction('2330', 'buy', '2026-06-10', -5, 100)


# ---------------- 推導：移動平均成本 ----------------

def test_single_buy_avg_includes_fee(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    ps.add_transaction('2330', 'buy', '2026-06-10', 1000, 100, fee=100)
    h = ps.derive_holdings(ps.load_transactions())['2330']
    assert h['shares'] == 1000
    assert h['cost_basis'] == pytest.approx(100100)   # 100*1000 + 100 fee
    assert h['avg_cost'] == pytest.approx(100.1)
    assert h['realized_pnl'] == 0


def test_weighted_avg_two_buys():
    txns = [
        dict(id='a', ticker='2330', market='tw', action='buy', date='2026-06-10',
             shares=1000, price=100, fee=100, tax=0, created_at='t1'),
        dict(id='b', ticker='2330', market='tw', action='buy', date='2026-06-11',
             shares=1000, price=120, fee=100, tax=0, created_at='t2'),
    ]
    h = ps.derive_holdings(txns)['2330']
    assert h['shares'] == 2000
    assert h['cost_basis'] == pytest.approx(220200)   # 100100 + 120100
    assert h['avg_cost'] == pytest.approx(110.1)


def test_partial_sell_realized_and_avg_unchanged():
    """買 1000@100(fee100) + 買 1000@120(fee100) -> avg 110.1；
    賣 500@130(fee50,tax195)：
      realized = 500*130 - 50 - 195 - 110.1*500 = 65000 - 245 - 55050 = 9705
      賣後 avg 不變、shares=1500、cost_basis=165150。
    """
    txns = [
        dict(id='a', ticker='2330', market='tw', action='buy', date='2026-06-10',
             shares=1000, price=100, fee=100, tax=0, created_at='t1'),
        dict(id='b', ticker='2330', market='tw', action='buy', date='2026-06-11',
             shares=1000, price=120, fee=100, tax=0, created_at='t2'),
        dict(id='c', ticker='2330', market='tw', action='sell', date='2026-06-12',
             shares=500, price=130, fee=50, tax=195, created_at='t3'),
    ]
    h = ps.derive_holdings(txns)['2330']
    assert h['shares'] == pytest.approx(1500)
    assert h['avg_cost'] == pytest.approx(110.1)       # 不變
    assert h['cost_basis'] == pytest.approx(165150)     # 220200 - 55050
    assert h['realized_pnl'] == pytest.approx(9705)


def test_full_close_resets_and_realized():
    txns = [
        dict(id='a', ticker='AAPL', market='us', action='buy', date='2026-06-10',
             shares=100, price=100, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAPL', market='us', action='sell', date='2026-06-20',
             shares=100, price=110, fee=0, tax=0, created_at='t2'),
    ]
    h = ps.derive_holdings(txns)['AAPL']
    assert h['shares'] == 0
    assert h['cost_basis'] == 0
    assert h['avg_cost'] == 0
    assert h['realized_pnl'] == pytest.approx(1000)     # (110-100)*100


def test_rebuy_after_close_starts_fresh_avg():
    txns = [
        dict(id='a', ticker='AAPL', market='us', action='buy', date='2026-06-10',
             shares=100, price=100, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAPL', market='us', action='sell', date='2026-06-20',
             shares=100, price=110, fee=0, tax=0, created_at='t2'),
        dict(id='c', ticker='AAPL', market='us', action='buy', date='2026-06-25',
             shares=50, price=200, fee=0, tax=0, created_at='t3'),
    ]
    h = ps.derive_holdings(txns)['AAPL']
    assert h['shares'] == 50
    assert h['avg_cost'] == pytest.approx(200)          # 重置後新均價
    assert h['realized_pnl'] == pytest.approx(1000)     # 保留前一輪已實現


def test_oversell_raises():
    txns = [
        dict(id='a', ticker='2330', market='tw', action='buy', date='2026-06-10',
             shares=1000, price=100, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='2330', market='tw', action='sell', date='2026-06-11',
             shares=1500, price=110, fee=0, tax=0, created_at='t2'),
    ]
    with pytest.raises(ValueError, match='賣超'):
        ps.derive_holdings(txns)


def test_out_of_order_input_is_time_sorted():
    """list 順序亂放（賣在前、買在後），但依 date 時序化後不應誤判賣超。"""
    txns = [
        dict(id='b', ticker='2330', market='tw', action='sell', date='2026-06-11',
             shares=1000, price=110, fee=0, tax=0, created_at='t2'),
        dict(id='a', ticker='2330', market='tw', action='buy', date='2026-06-10',
             shares=1000, price=100, fee=0, tax=0, created_at='t1'),
    ]
    h = ps.derive_holdings(txns)['2330']
    assert h['shares'] == 0
    assert h['realized_pnl'] == pytest.approx(10000)


# ---------------- 估值 + 彙總 ----------------

def test_value_positions_and_return():
    txns = [
        dict(id='a', ticker='2330', market='tw', action='buy', date='2026-06-10',
             shares=1000, price=100, fee=0, tax=0, created_at='t1'),
    ]
    h = ps.derive_holdings(txns)
    valued = ps.value_positions(h, {'2330': 125})
    assert len(valued) == 1
    row = valued[0]
    assert row['market_value'] == pytest.approx(125000)
    assert row['unrealized_pnl'] == pytest.approx(25000)
    assert row['return_pct'] == pytest.approx(0.25)


def test_value_positions_missing_price_is_none():
    txns = [dict(id='a', ticker='AAPL', market='us', action='buy',
                 date='2026-06-10', shares=10, price=100, fee=0, tax=0,
                 created_at='t1')]
    h = ps.derive_holdings(txns)
    valued = ps.value_positions(h, {})   # 無價
    assert valued[0]['current_price'] is None
    assert valued[0]['market_value'] is None


def test_summarize_splits_by_market():
    txns = [
        dict(id='a', ticker='2330', market='tw', action='buy', date='2026-06-10',
             shares=1000, price=100, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAPL', market='us', action='buy', date='2026-06-10',
             shares=10, price=200, fee=0, tax=0, created_at='t2'),
        dict(id='c', ticker='AAPL', market='us', action='sell', date='2026-06-20',
             shares=5, price=250, fee=0, tax=0, created_at='t3'),
    ]
    h = ps.derive_holdings(txns)
    by_mkt, valued = ps.summarize(h, {'2330': 110, 'AAPL': 260})
    # TW：成本 100000、市值 110000、未實現 10000、已實現 0
    assert by_mkt['tw']['cost'] == pytest.approx(100000)
    assert by_mkt['tw']['unrealized_pnl'] == pytest.approx(10000)
    assert by_mkt['tw']['realized_pnl'] == 0
    # US：買10@200 賣5@250 -> 已實現 250；剩 5 股成本 1000、市值 1300、未實現 300
    assert by_mkt['us']['realized_pnl'] == pytest.approx(250)
    assert by_mkt['us']['cost'] == pytest.approx(1000)
    assert by_mkt['us']['unrealized_pnl'] == pytest.approx(300)
    assert by_mkt['us']['total_pnl'] == pytest.approx(550)


def test_summarize_flags_missing_price():
    txns = [dict(id='a', ticker='2330', market='tw', action='buy',
                 date='2026-06-10', shares=1000, price=100, fee=0, tax=0,
                 created_at='t1')]
    h = ps.derive_holdings(txns)
    by_mkt, _ = ps.summarize(h, {})
    assert by_mkt['tw']['has_missing_price'] is True


# ---------------- CRUD update/delete ----------------

def test_update_transaction(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    rec = ps.add_transaction('2330', 'buy', '2026-06-10', 1000, 100)
    ps.update_transaction(rec['id'], price=105, shares=2000, note='改')
    t = ps.load_transactions()[0]
    assert t['price'] == 105 and t['shares'] == 2000 and t['note'] == '改'


def test_update_invalid_raises(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    rec = ps.add_transaction('2330', 'buy', '2026-06-10', 1000, 100)
    with pytest.raises(ValueError):
        ps.update_transaction(rec['id'], price=-5)


def test_update_missing_id_raises(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    with pytest.raises(KeyError):
        ps.update_transaction('MT-nope', price=1)


def test_delete_transaction(tmp_path, monkeypatch):
    _patch_dir(tmp_path, monkeypatch)
    rec = ps.add_transaction('2330', 'buy', '2026-06-10', 1000, 100)
    assert ps.delete_transaction(rec['id']) is True
    assert ps.load_transactions() == []
    assert ps.delete_transaction(rec['id']) is False   # 已不存在


# ---------------- NAV / TWR 淨值曲線（手算對拍） ----------------

def test_nav_single_ticker_twr():
    """買 100@10，價 10/11/12 -> NAV 1.0 / 1.10 / 1.20（= 純價漲幅 +20%）。"""
    txns = [dict(id='a', ticker='AAA', market='us', action='buy', date='2026-06-01',
                 shares=100, price=10, fee=0, tax=0, created_at='t1')]
    ph = {'AAA': _price(['2026-06-01', '2026-06-02', '2026-06-03'], [10, 11, 12])}
    nav = ps.build_nav_series(txns, ph, 'us')
    navs = nav['nav'].tolist()
    assert navs[0] == pytest.approx(1.0)
    assert navs[1] == pytest.approx(1.10)
    assert navs[2] == pytest.approx(1.20)
    assert nav['mv'].tolist() == [1000, 1100, 1200]


def test_nav_midperiod_add_flow_neutralized():
    """day2 加碼 100@11：TWR 中和現金流，NAV 仍 1.0/1.10/1.20（不因加碼稀釋 %）。"""
    txns = [
        dict(id='a', ticker='AAA', market='us', action='buy', date='2026-06-01',
             shares=100, price=10, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAA', market='us', action='buy', date='2026-06-02',
             shares=100, price=11, fee=0, tax=0, created_at='t2'),
    ]
    ph = {'AAA': _price(['2026-06-01', '2026-06-02', '2026-06-03'], [10, 11, 12])}
    nav = ps.build_nav_series(txns, ph, 'us')
    navs = nav['nav'].tolist()
    assert navs[0] == pytest.approx(1.0)
    assert navs[1] == pytest.approx(1.10)   # 只認 day1 那 100 股 10->11；新股當日 0 貢獻
    assert navs[2] == pytest.approx(1.20)
    assert nav['mv'].tolist() == [1000, 2200, 2400]
    assert nav['flow'].tolist()[1] == pytest.approx(1100)


def test_nav_full_exit_then_flat():
    """day2 全賣：當日 TWR +10%（10->11），出場後 NAV 打平不動。"""
    txns = [
        dict(id='a', ticker='AAA', market='us', action='buy', date='2026-06-01',
             shares=100, price=10, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAA', market='us', action='sell', date='2026-06-02',
             shares=100, price=11, fee=0, tax=0, created_at='t2'),
    ]
    ph = {'AAA': _price(['2026-06-01', '2026-06-02', '2026-06-03'], [10, 11, 12])}
    nav = ps.build_nav_series(txns, ph, 'us')
    navs = nav['nav'].tolist()
    assert navs[0] == pytest.approx(1.0)
    assert navs[1] == pytest.approx(1.10)
    assert navs[2] == pytest.approx(1.10)   # 已出場，之後打平
    assert nav['mv'].tolist() == [1000, 0, 0]


def test_ytd_baseline_spans_prev_year():
    s = _price(['2025-12-30', '2025-12-31', '2026-01-02', '2026-06-30'],
               [100, 110, 120, 140])
    assert ps.ytd_baseline(s, 2026) == pytest.approx(110)      # 去年最後交易日
    assert ps.ytd_return(s, 140, 2026) == pytest.approx(140 / 110 - 1)


def test_ytd_baseline_all_this_year():
    s = _price(['2026-01-02', '2026-06-30'], [120, 150])       # 無去年資料
    assert ps.ytd_baseline(s, 2026) == pytest.approx(120)      # 當年首個交易日
    assert ps.ytd_return(s, 150, 2026) == pytest.approx(150 / 120 - 1)


def test_ytd_return_none_cases():
    assert ps.ytd_return(None, 100, 2026) is None              # 無序列
    assert ps.ytd_return(_price(['2026-01-02'], [0]), 100, 2026) is None  # 基準 0
    assert ps.ytd_return(_price(['2026-01-02'], [120]), None, 2026) is None  # 無現值


def test_derive_holdings_entry_date():
    txns = [
        dict(id='a', ticker='AAA', market='us', action='buy', date='2026-03-01',
             shares=100, price=10, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAA', market='us', action='buy', date='2026-04-01',
             shares=100, price=12, fee=0, tax=0, created_at='t2'),
    ]
    assert ps.derive_holdings(txns)['AAA']['entry_date'] == '2026-03-01'


def test_entry_date_resets_on_close_and_rebuy():
    txns = [
        dict(id='a', ticker='AAA', market='us', action='buy', date='2025-11-01',
             shares=100, price=10, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAA', market='us', action='sell', date='2026-01-05',
             shares=100, price=11, fee=0, tax=0, created_at='t2'),
        dict(id='c', ticker='AAA', market='us', action='buy', date='2026-02-10',
             shares=50, price=20, fee=0, tax=0, created_at='t3'),
    ]
    assert ps.derive_holdings(txns)['AAA']['entry_date'] == '2026-02-10'


def test_position_ytd_built_this_year():
    # 今年建倉：從建倉均價算（不需歷史），等同該部位總報酬率
    ytd = ps.position_ytd('2026-03-01', 21.44, 22.09, None, 2026)
    assert ytd == pytest.approx(22.09 / 21.44 - 1)


def test_position_ytd_held_across_year():
    # 跨年持有：從去年末收盤算今年這段
    s = _price(['2025-12-31', '2026-06-30'], [186.5, 200.09])
    ytd = ps.position_ytd('2024-05-01', 100, 200.09, s, 2026)
    assert ytd == pytest.approx(200.09 / 186.5 - 1)


def test_position_ytd_none_cases():
    assert ps.position_ytd('2026-01-01', 0, 100, None, 2026) is None      # avg_cost 0
    assert ps.position_ytd('2026-01-01', 10, None, None, 2026) is None    # 無現價
    assert ps.position_ytd('2024-01-01', 10, 100, None, 2026) is None     # 跨年但無歷史


def test_closed_positions_basic():
    txns = [
        dict(id='a', ticker='AAA', market='us', action='buy', date='2026-01-05',
             shares=100, price=10, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAA', market='us', action='sell', date='2026-02-05',
             shares=100, price=12, fee=0, tax=0, created_at='t2'),
    ]
    c = ps.closed_positions(txns)
    assert len(c) == 1
    r = c[0]
    assert r['ticker'] == 'AAA'
    assert r['realized_pnl'] == pytest.approx(200)      # (12-10)*100
    assert r['return_pct'] == pytest.approx(0.20)
    assert r['entry_date'] == '2026-01-05' and r['exit_date'] == '2026-02-05'
    assert r['holding_days'] == 31
    assert r['avg_buy'] == pytest.approx(10) and r['avg_sell'] == pytest.approx(12)


def test_closed_positions_excludes_open():
    txns = [dict(id='a', ticker='AAA', market='us', action='buy', date='2026-01-05',
                 shares=100, price=10, fee=0, tax=0, created_at='t1')]
    assert ps.closed_positions(txns) == []


def test_closed_positions_partial_sell_not_closed():
    txns = [
        dict(id='a', ticker='AAA', market='us', action='buy', date='2026-01-05',
             shares=100, price=10, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAA', market='us', action='sell', date='2026-02-05',
             shares=50, price=12, fee=0, tax=0, created_at='t2'),
    ]
    assert ps.closed_positions(txns) == []              # 還持有 50 股，未平倉


def test_closed_positions_multiple_roundtrips():
    txns = [
        dict(id='a', ticker='AAA', market='us', action='buy', date='2026-01-05',
             shares=100, price=10, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAA', market='us', action='sell', date='2026-01-20',
             shares=100, price=11, fee=0, tax=0, created_at='t2'),
        dict(id='c', ticker='AAA', market='us', action='buy', date='2026-02-01',
             shares=50, price=20, fee=0, tax=0, created_at='t3'),
        dict(id='d', ticker='AAA', market='us', action='sell', date='2026-02-10',
             shares=50, price=25, fee=0, tax=0, created_at='t4'),
    ]
    c = ps.closed_positions(txns)
    assert len(c) == 2
    assert c[0]['realized_pnl'] == pytest.approx(100)   # (11-10)*100
    assert c[1]['realized_pnl'] == pytest.approx(250)   # (25-20)*50
    assert c[0]['exit_date'] == '2026-01-20' and c[1]['entry_date'] == '2026-02-01'


def test_closed_positions_realized_reconciles_with_holdings():
    """完全平倉+partial：closed round-trip 的已實現 + 未平倉 run 的已實現 = 總已實現。"""
    txns = [
        dict(id='a', ticker='AAA', market='us', action='buy', date='2026-01-05',
             shares=100, price=10, fee=0, tax=0, created_at='t1'),
        dict(id='b', ticker='AAA', market='us', action='sell', date='2026-01-20',
             shares=100, price=11, fee=0, tax=0, created_at='t2'),   # 平倉 rt +100
        dict(id='c', ticker='BBB', market='us', action='buy', date='2026-02-01',
             shares=100, price=20, fee=0, tax=0, created_at='t3'),
        dict(id='d', ticker='BBB', market='us', action='sell', date='2026-02-10',
             shares=40, price=25, fee=0, tax=0, created_at='t4'),   # 部分賣 +200，仍持有
    ]
    closed_total = sum(c['realized_pnl'] for c in ps.closed_positions(txns))
    holdings_total = sum(h['realized_pnl'] for h in ps.derive_holdings(txns).values())
    assert closed_total == pytest.approx(100)           # 只有 AAA round-trip
    assert holdings_total == pytest.approx(300)          # AAA 100 + BBB partial 200
    # 差額 = 未平倉部位的 partial 已實現
    assert holdings_total - closed_total == pytest.approx(200)


def test_estimate_tw_costs():
    fee, tax = ps.estimate_tw_costs(1000, 105, 'buy')
    assert fee == 150.0 and tax == 0.0          # 手續費 105000*0.001425=149.625 -> 150；買無稅
    fee, tax = ps.estimate_tw_costs(1000, 105, 'sell')
    assert fee == 150.0 and tax == 315.0        # 賣出證交稅 105000*0.003=315


def test_nav_market_filter_and_empty():
    txns = [dict(id='a', ticker='2330', market='tw', action='buy', date='2026-06-01',
                 shares=1000, price=100, fee=0, tax=0, created_at='t1')]
    ph = {'2330': _price(['2026-06-01', '2026-06-02'], [100, 110])}
    assert ps.build_nav_series(txns, ph, 'us').empty      # 無美股交易
    nav_tw = ps.build_nav_series(txns, ph, 'tw')
    assert nav_tw['nav'].tolist()[-1] == pytest.approx(1.10)
