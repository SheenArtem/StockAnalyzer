"""官方 EOD 橫斷面必須以 payload 自報日期蓋章，不可用「請求的日期」。

起因（2026-08-02 實測）：TPEX `stk_quote_result.php` **完全無視 `d` 參數** ——
請求 115/06/16（6 週前）回的是 115/07/31 的橫斷面，價格一字不差；請求週六
115/08/01 同樣回 07/31。TWSE MI_INDEX 則正確分辨，非交易日回
stat="很抱歉，沒有符合條件的資料!"。

拿請求日期當資料日期會有兩種後果，兩者都躲得過數值健康度檢查（每一欄都是正數的
合理價格）：
  - `refresh_universe_prices`：把「上一場」OHLCV 以錯誤日期寫進 1900+ 支 CSV。
  - `market_regime_logger`：TWSE 對未開盤的今天正確回空、TPEX 卻回上一場，於是
    橫斷面變成純上櫃，等權均價被高價上櫃股抬成 1.835 倍。
"""
import pandas as pd
import pytest

import twse_api
from tools import market_regime_logger as MRL
from tools import refresh_universe_prices as RUP

COLS = ['stock_id', 'stock_name', 'market', 'close', 'change', 'open', 'high',
        'low', 'volume', 'trading_value', 'trades', 'change_pct', 'data_date']


def _cross_section(sids, data_date, close=100.0):
    """一天的全市場橫斷面，data_date 是 payload 自報日期。"""
    return pd.DataFrame([{
        'stock_id': s, 'stock_name': f'N{s}', 'market': 'twse', 'close': close,
        'change': 0.0, 'open': close, 'high': close, 'low': close,
        'volume': 1_000_000, 'trading_value': close * 1_000_000, 'trades': 500,
        'change_pct': 0.0, 'data_date': pd.Timestamp(data_date),
    } for s in sids], columns=COLS)


# --------------------------------------------------------------------------- #
#  payload 日期解析
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize('raw,expected', [
    ('20260731', '2026-07-31'),                       # TWSE / TPEX 頂層 date
    ('115/07/31', '2026-07-31'),                      # TPEX tables[0].date（民國）
    ('115/7/3', '2026-07-03'),                        # 民國未補零
    ('2026/07/31', '2026-07-31'),                     # 西元帶斜線
    ('115年07月31日 每日收盤行情(全部)', '2026-07-31'),  # TWSE 表格 title
    ('115年7月3日 大盤統計資訊', '2026-07-03'),
])
def test_parse_payload_date_covers_every_observed_format(raw, expected):
    assert twse_api.TWSEOpenData._parse_payload_date(raw) == pd.Timestamp(expected)


@pytest.mark.parametrize('raw', [None, '', '   ', 'N/A', '--', '115/13/45'])
def test_parse_payload_date_returns_none_instead_of_guessing(raw):
    assert twse_api.TWSEOpenData._parse_payload_date(raw) is None


# --------------------------------------------------------------------------- #
#  strict_date 過濾
# --------------------------------------------------------------------------- #

def test_enforce_payload_date_drops_mismatched_cross_section():
    api = twse_api.TWSEOpenData()
    stale = _cross_section(['1101', '1102'], '2026-07-31')

    out = api._enforce_payload_date(stale, pd.Timestamp('2026-06-16'), 'TPEX',
                                    strict_date=True)

    assert out.empty, 'payload 自報 07-31，請求 06-16，必須整批丟掉'
    assert list(out.columns) == COLS


def test_enforce_payload_date_keeps_matching_cross_section():
    api = twse_api.TWSEOpenData()
    frame = _cross_section(['1101', '1102'], '2026-07-31')

    out = api._enforce_payload_date(frame, pd.Timestamp('2026-07-31'), 'TWSE',
                                    strict_date=True)

    assert len(out) == 2


def test_enforce_payload_date_non_strict_keeps_rows_but_warns(caplog):
    api = twse_api.TWSEOpenData()
    stale = _cross_section(['1101'], '2026-07-31')

    with caplog.at_level('WARNING'):
        out = api._enforce_payload_date(stale, pd.Timestamp('2026-06-16'), 'TPEX',
                                        strict_date=False)

    assert len(out) == 1
    assert 'payload self-reports' in caplog.text


def test_enforce_payload_date_noop_when_no_date_requested():
    api = twse_api.TWSEOpenData()
    frame = _cross_section(['1101'], '2026-07-31')

    # date=None 代表「給我最新的」，沒有可比對的請求日期。
    assert len(api._enforce_payload_date(frame, None, 'TWSE', True)) == 1


# --------------------------------------------------------------------------- #
#  refresh_universe_prices：overlay 不可蓋錯日期
# --------------------------------------------------------------------------- #

class _StubApi:
    """回傳固定橫斷面，無視請求日期 —— 模擬 TPEX 端點的真實行為。"""

    def __init__(self, frame):
        self._frame = frame
        self.requested = []

    def get_market_daily_all(self, date=None, strict_date=True):
        self.requested.append(pd.Timestamp(date).normalize())
        return self._frame.copy()


def _install_stub(monkeypatch, frame):
    stub = _StubApi(frame)
    monkeypatch.setattr(twse_api, 'TWSEOpenData', lambda *a, **k: stub)
    return stub


def test_overlay_rejects_previous_session_stamped_as_requested_day(monkeypatch):
    """原始缺陷：請求 08-01 拿到 07-31 的資料，卻以 08-01 蓋章寫進 CSV。"""
    sids = [str(2000 + i) for i in range(1000)]
    stale = _cross_section(sids, '2026-07-31')
    _install_stub(monkeypatch, stale)

    day, frame = RUP._official_daily_overlay(sids, pd.Timestamp('2026-08-01'),
                                             lookback_days=0)

    assert day is None, '自報 07-31 的橫斷面不可被當成 08-01 收下'
    assert frame.empty


def test_overlay_accepts_cross_section_whose_payload_date_matches(monkeypatch):
    sids = [str(2000 + i) for i in range(1000)]
    _install_stub(monkeypatch, _cross_section(sids, '2026-07-31'))

    day, frame = RUP._official_daily_overlay(sids, pd.Timestamp('2026-07-31'),
                                             lookback_days=0)

    assert day == pd.Timestamp('2026-07-31')
    assert len(frame) == 1000


def test_overlay_walks_back_and_stamps_with_payload_date(monkeypatch):
    """週日跑：08-02 / 08-01 沒有資料，退到 07-31 並以 payload 日期蓋章。"""
    sids = [str(2000 + i) for i in range(1000)]
    _install_stub(monkeypatch, _cross_section(sids, '2026-07-31'))

    day, frame = RUP._official_daily_overlay(sids, pd.Timestamp('2026-08-02'),
                                             lookback_days=7)

    assert day == pd.Timestamp('2026-07-31')
    assert len(frame) == 1000


def test_overlay_refuses_frame_without_data_date(monkeypatch):
    """payload 沒有自報日期時，寧可放棄也不用請求日期蓋章。"""
    sids = [str(2000 + i) for i in range(1000)]
    frame = _cross_section(sids, '2026-07-31').drop(columns=['data_date'])
    _install_stub(monkeypatch, frame)

    day, out = RUP._official_daily_overlay(sids, pd.Timestamp('2026-07-31'),
                                           lookback_days=0)

    assert day is None
    assert out.empty


def test_overlay_rejects_mixed_date_cross_section(monkeypatch):
    """TWSE 與 TPEX 回不同日期時，混合橫斷面不是任何一天的全市場。"""
    sids = [str(2000 + i) for i in range(1000)]
    mixed = pd.concat([_cross_section(sids[:500], '2026-07-31'),
                       _cross_section(sids[500:], '2026-07-30')],
                      ignore_index=True)
    _install_stub(monkeypatch, mixed)

    day, out = RUP._official_daily_overlay(sids, pd.Timestamp('2026-07-31'),
                                           lookback_days=0)

    assert day is None
    assert out.empty


# --------------------------------------------------------------------------- #
#  market_regime_logger：補值不可混日期，也不可混成分
# --------------------------------------------------------------------------- #

def test_regime_supplement_skips_day_whose_payload_is_another_date(monkeypatch):
    """原始缺陷的核心：未開盤的今天拿到上一場的純上櫃橫斷面。

    上櫃成分股是高價股集中地，只用它們算等權均價會把水位抬到 1.8 倍，
    讓 ret_20d 飆到 +70%、range_20d 中位數 0.934（volatile 門檻 0.08），
    使週一~週四被永久釘在 volatile。
    """
    members = {str(2000 + i) for i in range(100)}
    # 上一場的橫斷面，且只剩少數高價上櫃股
    stale = _cross_section([str(2000 + i) for i in range(10)], '2026-07-31',
                           close=5000.0)
    _install_stub(monkeypatch, stale)

    out = MRL._fetch_close_supplement(members, [pd.Timestamp('2026-08-03')])

    assert out.empty, '自報 07-31 的橫斷面不可被當成 08-03 補進序列'


def test_regime_supplement_skips_day_with_thin_member_coverage(monkeypatch):
    """日期正確但成分不足時也要跳過 —— 等權均價對成分極度敏感。"""
    members = {str(2000 + i) for i in range(100)}
    thin = _cross_section([str(2000 + i) for i in range(50)], '2026-08-03',
                          close=5000.0)
    _install_stub(monkeypatch, thin)

    out = MRL._fetch_close_supplement(members, [pd.Timestamp('2026-08-03')])

    assert out.empty, '只命中 50/100 成分股（< 90%）必須跳過'


def test_regime_supplement_accepts_correct_date_and_full_coverage(monkeypatch):
    members = {str(2000 + i) for i in range(100)}
    good = _cross_section(sorted(members), '2026-08-03', close=250.0)
    _install_stub(monkeypatch, good)

    out = MRL._fetch_close_supplement(members, [pd.Timestamp('2026-08-03')])

    assert list(out.index) == [pd.Timestamp('2026-08-03')]
    assert out.iloc[0] == pytest.approx(250.0)


def test_regime_supplement_ignores_non_members(monkeypatch):
    """全市場橫斷面含 1900+ 檔，只能取 parquet 那批成分股來算。"""
    members = {str(2000 + i) for i in range(100)}
    frame = pd.concat([
        _cross_section(sorted(members), '2026-08-03', close=250.0),
        _cross_section([str(9000 + i) for i in range(800)], '2026-08-03',
                       close=9999.0),
    ], ignore_index=True)
    _install_stub(monkeypatch, frame)

    out = MRL._fetch_close_supplement(members, [pd.Timestamp('2026-08-03')])

    assert out.iloc[0] == pytest.approx(250.0), '非成分股不得進入等權均價'


# --------------------------------------------------------------------------- #
#  regime log 的讀寫與修補範圍
# --------------------------------------------------------------------------- #

def _log_with(tmp_path, monkeypatch, lines):
    path = tmp_path / 'regime_log.jsonl'
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    monkeypatch.setattr(MRL, 'LOG_PATH', path)
    return path


def test_read_log_refuses_to_rewrite_over_unparseable_lines(tmp_path, monkeypatch):
    """壞行不可被靜默跳過 —— 後續 _write_log 會整檔重寫，等於永久刪掉它。"""
    _log_with(tmp_path, monkeypatch, [
        '{"date": "2026-07-30", "regime": "volatile", "ret_20d": -0.23}',
        '{"date": "2026-07-31", "regime": "volatile", "ret_20d": ',   # 截斷
    ])

    with pytest.raises(RuntimeError, match='unparseable'):
        MRL._read_log()


def test_read_log_tolerates_blank_lines(tmp_path, monkeypatch):
    _log_with(tmp_path, monkeypatch, [
        '{"date": "2026-07-30", "regime": "volatile", "ret_20d": -0.23}',
        '',
        '   ',
        '{"date": "2026-07-31", "regime": "volatile", "ret_20d": -0.18}',
    ])

    assert sorted(MRL._read_log()) == ['2026-07-30', '2026-07-31']


def test_repair_only_touches_physically_impossible_entries(tmp_path, monkeypatch):
    """版本差異（< 1pp）不該被當成毀損重寫。

    2026-08-02 實測：3,717 筆有 3,712 筆與現行 panel 不同，但 78.1% 差異 < 1pp，
    那是 panel 版本差而非毀損；只有 59 筆 |ret_20d| > 30% 是等權 300 檔代理不可能
    出現的值。
    """
    _log_with(tmp_path, monkeypatch, [
        # 物理不可能 —— 補值 bug 的產物，要修
        '{"date": "2026-07-30", "regime": "volatile", "ret_20d": 0.4388,'
        ' "range_20d": 0.697, "sharpe_60d": 0.747, "proxy": "equal_weight_top300"}',
        # 只差 0.3pp —— 版本差，不該動
        '{"date": "2026-07-31", "regime": "volatile", "ret_20d": -0.1795,'
        ' "range_20d": 0.2831, "sharpe_60d": -0.517, "proxy": "equal_weight_top300"}',
    ])
    clean = {
        '2026-07-30': {'date': '2026-07-30', 'regime': 'volatile', 'ret_20d': -0.2293,
                       'range_20d': 0.697, 'sharpe_60d': 0.747,
                       'proxy': 'equal_weight_top300'},
        '2026-07-31': {'date': '2026-07-31', 'regime': 'volatile', 'ret_20d': -0.1825,
                       'range_20d': 0.2831, 'sharpe_60d': -0.517,
                       'proxy': 'equal_weight_top300'},
    }
    monkeypatch.setattr(MRL, 'recompute_history_from_panel', lambda: clean)

    stats = MRL.repair_history(dry_run=False)

    assert stats['changed'] == 1
    after = MRL._read_log()
    assert after['2026-07-30']['ret_20d'] == -0.2293, '不可能值要被修掉'
    assert after['2026-07-31']['ret_20d'] == -0.1795, '版本差異必須原樣保留'


def test_repair_dry_run_writes_nothing(tmp_path, monkeypatch):
    path = _log_with(tmp_path, monkeypatch, [
        '{"date": "2026-07-30", "regime": "volatile", "ret_20d": 0.4388,'
        ' "range_20d": 0.697, "sharpe_60d": 0.747, "proxy": "equal_weight_top300"}',
    ])
    before = path.read_bytes()
    monkeypatch.setattr(MRL, 'recompute_history_from_panel', lambda: {
        '2026-07-30': {'date': '2026-07-30', 'regime': 'volatile', 'ret_20d': -0.2293,
                       'range_20d': 0.697, 'sharpe_60d': 0.747,
                       'proxy': 'equal_weight_top300'}})

    stats = MRL.repair_history(dry_run=True)

    assert stats['changed'] == 1
    assert path.read_bytes() == before


def test_repair_never_adds_or_drops_dates(tmp_path, monkeypatch):
    """panel 有但 log 沒有的日期不可被塞進來；log 有但 panel 沒有的不可被刪。"""
    _log_with(tmp_path, monkeypatch, [
        '{"date": "2026-07-30", "regime": "volatile", "ret_20d": 0.4388,'
        ' "range_20d": 0.697, "sharpe_60d": 0.747, "proxy": "equal_weight_top300"}',
        '{"date": "2026-01-01", "regime": "ranging", "ret_20d": 0.001,'
        ' "range_20d": 0.02, "sharpe_60d": 0.1, "proxy": "equal_weight_top300"}',
    ])
    monkeypatch.setattr(MRL, 'recompute_history_from_panel', lambda: {
        '2026-07-30': {'date': '2026-07-30', 'regime': 'volatile', 'ret_20d': -0.2293,
                       'range_20d': 0.697, 'sharpe_60d': 0.747,
                       'proxy': 'equal_weight_top300'},
        '2026-07-29': {'date': '2026-07-29', 'regime': 'volatile', 'ret_20d': -0.1974,
                       'range_20d': 0.7283, 'sharpe_60d': 0.811,
                       'proxy': 'equal_weight_top300'},
    })

    stats = MRL.repair_history(dry_run=False)

    after = MRL._read_log()
    assert sorted(after) == ['2026-01-01', '2026-07-30']
    assert stats['skipped'] == 1, 'panel 沒有的 2026-01-01 要算進 skipped 並保留原值'


def test_classify_regime_rules():
    assert MRL.classify_regime(0.0, 0.20) == 'volatile'      # range 優先
    assert MRL.classify_regime(0.10, 0.05) == 'trending'
    assert MRL.classify_regime(0.001, 0.05) == 'ranging'
    assert MRL.classify_regime(0.03, 0.05) == 'neutral'
    assert MRL.classify_regime(float('nan'), 0.05) == 'neutral'
