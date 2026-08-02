"""RF-1 一致性檢查：讀不到的 live parquet 必須被報成「未涵蓋」，不可靜默跳過。

舊版 `tools/rf1_cache_consistency_check.py:74-77` 對讀取失敗 `except Exception:
continue`，於是被丟掉的股票連 drift 都報不出來 —— 這支工具的職責正是偵測 drift，
破洞卻會自我隱藏：實際有一批股票根本沒比對過，畫面上仍印 `[OK] All categories
consistent!`。
"""
import pandas as pd
import pytest

from tools import rf1_cache_consistency_check as RF1


@pytest.fixture()
def cache_dirs(tmp_path, monkeypatch):
    live = tmp_path / 'fundamental_cache'
    bt = tmp_path / 'backtest'
    live.mkdir()
    bt.mkdir()
    monkeypatch.setattr(RF1, 'LIVE_DIR', live)
    monkeypatch.setattr(RF1, 'BT_DIR', bt)
    # income -> ('financial_statement', 'financials_income.parquet')
    pd.DataFrame({
        'stock_id': ['2330', '2317', '1101'],
        'date': pd.to_datetime(['2026-06-30'] * 3),
    }).to_parquet(bt / 'financials_income.parquet', index=False)
    return live, bt


def _live(live, sid, date='2026-06-30'):
    pd.DataFrame({'stock_id': [sid], 'date': pd.to_datetime([date])}).to_parquet(
        live / f'financial_statement_{sid}.parquet', index=False)


def test_unreadable_live_file_is_reported_not_skipped(cache_dirs):
    live, _bt = cache_dirs
    _live(live, '2330')
    # 不是合法 parquet —— pandas 讀取會丟例外
    (live / 'financial_statement_2317.parquet').write_bytes(b'not a parquet at all')

    drifts, uncovered = RF1.check_category('income', threshold_days=45)

    assert drifts == []
    assert [u['stock_id'] for u in uncovered] == ['2317']
    assert '讀取失敗' in uncovered[0]['reason']


def test_empty_and_schemaless_live_files_are_reported(cache_dirs):
    live, _bt = cache_dirs
    pd.DataFrame({'stock_id': [], 'date': []}).to_parquet(
        live / 'financial_statement_2330.parquet', index=False)
    pd.DataFrame({'stock_id': ['2317'], 'value': [1]}).to_parquet(
        live / 'financial_statement_2317.parquet', index=False)

    drifts, uncovered = RF1.check_category('income', threshold_days=45)

    assert drifts == []
    reasons = {u['stock_id']: u['reason'] for u in uncovered}
    assert set(reasons) == {'2330', '2317'}
    assert '為空' in reasons['2330']
    assert '無 date 欄' in reasons['2317']


def test_unparseable_dates_are_reported(cache_dirs):
    live, _bt = cache_dirs
    pd.DataFrame({'stock_id': ['2330'], 'date': ['not-a-date']}).to_parquet(
        live / 'financial_statement_2330.parquet', index=False)

    drifts, uncovered = RF1.check_category('income', threshold_days=45)

    assert drifts == []
    assert uncovered[0]['stock_id'] == '2330'
    assert '無法解析' in uncovered[0]['reason']


def test_healthy_files_produce_no_uncovered_rows(cache_dirs):
    live, _bt = cache_dirs
    for sid in ('2330', '2317', '1101'):
        _live(live, sid)

    drifts, uncovered = RF1.check_category('income', threshold_days=45)

    assert drifts == []
    assert uncovered == []


def test_real_drift_still_detected_alongside_coverage_holes(cache_dirs):
    live, _bt = cache_dirs
    _live(live, '2330', date='2026-01-31')          # 距 backtest 6/30 超過 45 天
    (live / 'financial_statement_2317.parquet').write_bytes(b'corrupt')

    drifts, uncovered = RF1.check_category('income', threshold_days=45)

    assert [d['stock_id'] for d in drifts] == ['2330']
    assert [u['stock_id'] for u in uncovered] == ['2317']


def test_missing_backtest_parquet_counts_as_uncovered(tmp_path, monkeypatch):
    """backtest 檔不存在時，整個類別都沒被檢查 —— 不可回空 list 假裝沒事。"""
    live = tmp_path / 'fundamental_cache'
    bt = tmp_path / 'backtest'
    live.mkdir()
    bt.mkdir()
    monkeypatch.setattr(RF1, 'LIVE_DIR', live)
    monkeypatch.setattr(RF1, 'BT_DIR', bt)

    drifts, uncovered = RF1.check_category('income', threshold_days=45)

    assert drifts == []
    assert len(uncovered) == 1
    assert uncovered[0]['stock_id'] == '*'
