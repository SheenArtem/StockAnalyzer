from pathlib import Path

import pandas as pd
import pytest

import mops_bulk_fetcher
from tools import vfvc_backfill_monthly_rev as bulk


def _bulk_frame(sii_count=600, otc_count=500, period=(2026, 6)):
    year, month = period
    rows = []
    for market, start, count in (
        ('SII', 1000, sii_count),
        ('OTC', 5000, otc_count),
    ):
        for offset in range(count):
            rows.append({
                'date': pd.Timestamp(year, month, 1) + pd.offsets.MonthBegin(1),
                'stock_id': str(start + offset),
                'country': 'Taiwan',
                'revenue': 1_000_000 + offset,
                'revenue_year': year,
                'revenue_month': month,
                '_source_market': market,
            })
    return pd.DataFrame(rows)


def _existing_frame(count=1500, period=(2026, 5)):
    year, month = period
    return pd.DataFrame({
        'stock_id': [str(10_000 + offset) for offset in range(count)],
        'revenue_year': year,
        'revenue_month': month,
    })


def test_bulk_requires_both_sii_and_otc():
    frame = _bulk_frame().query("_source_market == 'SII'")

    with pytest.raises(bulk.BulkRevenueSafetyError, match='missing required markets'):
        bulk.validate_bulk_cross_section(frame, existing_df=None)


def test_missing_market_blocks_bulk_before_any_write(monkeypatch):
    frame = _bulk_frame().query("_source_market == 'SII'")
    monkeypatch.setattr(
        mops_bulk_fetcher,
        'fetch_bulk_monthly_revenue',
        lambda include_otc=True: frame,
    )
    monkeypatch.setattr(bulk, '_load_existing_revenue_aggregate', lambda: None)
    monkeypatch.setattr(
        bulk,
        'merge_bulk_into_existing_cache',
        lambda *_args, **_kwargs: pytest.fail('invalid markets must block merge'),
    )

    assert bulk.run_bulk_update() is False


def test_latest_period_requires_80_percent_of_existing_cross_section():
    frame = _bulk_frame(sii_count=600, otc_count=500)

    with pytest.raises(bulk.BulkRevenueSafetyError, match='required at least 1200'):
        bulk.validate_bulk_cross_section(
            frame,
            existing_df=_existing_frame(count=1500),
            min_absolute=1000,
        )


def test_latest_period_requires_absolute_minimum_without_existing_panel():
    frame = _bulk_frame(sii_count=500, otc_count=300)

    with pytest.raises(bulk.BulkRevenueSafetyError, match='required at least 1000'):
        bulk.validate_bulk_cross_section(frame, existing_df=None)


def test_each_market_must_reach_the_same_latest_period():
    sii = _bulk_frame(sii_count=600, otc_count=0, period=(2026, 6))
    otc = _bulk_frame(sii_count=0, otc_count=500, period=(2026, 5))

    with pytest.raises(bulk.BulkRevenueSafetyError, match='does not match'):
        bulk.validate_bulk_cross_section(
            pd.concat([sii, otc], ignore_index=True),
            existing_df=None,
        )


def test_bulk_raw_date_must_match_revenue_period():
    frame = _bulk_frame()
    frame['date'] = pd.Timestamp('2026-06-01')

    with pytest.raises(bulk.BulkRevenueSafetyError, match='first day after'):
        bulk.validate_bulk_cross_section(frame, existing_df=None)


def test_atomic_parquet_failure_preserves_last_good_file(tmp_path, monkeypatch):
    target = tmp_path / 'month_revenue_2330.parquet'
    target.write_bytes(b'last-good')

    def fail_after_partial_write(_frame, path, **_kwargs):
        Path(path).write_bytes(b'partial')
        raise RuntimeError('serialization failed')

    monkeypatch.setattr(pd.DataFrame, 'to_parquet', fail_after_partial_write)

    with pytest.raises(RuntimeError, match='serialization failed'):
        bulk._atomic_write_parquet(pd.DataFrame({'value': [1]}), target)

    assert target.read_bytes() == b'last-good'
    assert list(tmp_path.glob('.*.tmp.parquet')) == []


def test_per_stock_atomic_write_error_is_reported_by_merge(tmp_path, monkeypatch):
    frame = _bulk_frame(sii_count=2, otc_count=0)
    writes = []

    def write_one_then_fail(_frame, path):
        writes.append(Path(path).name)
        if Path(path).stem.endswith('1001'):
            raise RuntimeError('disk error')

    monkeypatch.setattr(bulk, '_atomic_write_parquet', write_one_then_fail)

    stats = bulk.merge_bulk_into_existing_cache(frame, cache_dir=tmp_path)

    assert writes == ['month_revenue_1000.parquet', 'month_revenue_1001.parquet']
    assert stats['written'] == 1
    assert stats['errors'] == 1


def test_merge_error_blocks_sync_and_aggregate(tmp_path, monkeypatch):
    """merge 出錯必須擋下 cache sync 與 aggregate。

    ⚠️ 一定要 patch expected_revenue_period：payload 期別寫死 202606，而
    expected_revenue_period() 每月遞增。不 patch 的話 2026-08-10 之後新舊 gate 會
    先拋錯 return False，測試照樣綠燈但 merge 之後的哨兵永遠走不到（本測試在
    2026-08-02 code review 中被發現有此時間炸彈）。同時斷言 merge 真的被呼叫過，
    讓「有走到 merge 才失敗」成為顯式前提，而不是靠回傳值猜是哪一道 gate 擋的。
    """
    aggregate = tmp_path / 'financials_revenue.parquet'
    _existing_frame(count=1000).to_parquet(aggregate, index=False)
    monkeypatch.setattr(bulk, 'AGGREGATE_REVENUE_PATH', aggregate)
    monkeypatch.setattr(
        mops_bulk_fetcher,
        'fetch_bulk_monthly_revenue',
        lambda include_otc=True: _bulk_frame(),
    )
    monkeypatch.setattr(bulk, 'expected_revenue_period', lambda: 202606)

    merge_calls = []

    def _merge_with_error(_frame, dry_run=False):
        merge_calls.append(dry_run)
        return {
            'written': 1099,
            'skipped_already_exists': 0,
            'new_files': 1099,
            'append_to_existing': 0,
            'errors': 1,
        }

    monkeypatch.setattr(bulk, 'merge_bulk_into_existing_cache', _merge_with_error)
    monkeypatch.setattr(
        bulk,
        'sync_fundamental_to_finmind_cache',
        lambda **_kwargs: pytest.fail('merge error must block cache sync'),
    )
    monkeypatch.setattr(
        bulk.subprocess,
        'run',
        lambda *_args, **_kwargs: pytest.fail('merge error must block aggregate'),
    )

    assert bulk.run_bulk_update() is False
    assert len(merge_calls) == 1, 'gate 必須是 merge 的 errors，不是更早的 staleness'


def test_stale_published_period_blocks_merge(monkeypatch):
    monkeypatch.setattr(
        mops_bulk_fetcher,
        'fetch_bulk_monthly_revenue',
        lambda include_otc=True: _bulk_frame(period=(2026, 6)),
    )
    monkeypatch.setattr(bulk, '_load_existing_revenue_aggregate', lambda: None)
    monkeypatch.setattr(bulk, 'expected_revenue_period', lambda: 202607)
    monkeypatch.setattr(
        bulk,
        'merge_bulk_into_existing_cache',
        lambda *_args, **_kwargs: pytest.fail('stale period must block merge'),
    )

    assert bulk.run_bulk_update() is False


def test_bulk_cli_returns_nonzero_on_safety_failure(monkeypatch):
    monkeypatch.setattr(bulk, 'run_bulk_update', lambda: False)
    monkeypatch.setattr(bulk.sys, 'argv', ['vfvc_backfill_monthly_rev.py', '--bulk-update'])

    with pytest.raises(SystemExit) as exc:
        bulk.main()

    assert exc.value.code == 1


class _AlwaysFailLoader:
    """每檔都爆（FinMind 額度爆／token 失效／網路全斷都長這樣）。"""

    has_token = True

    def taiwan_stock_month_revenue(self, **_kwargs):
        raise RuntimeError('FinMind quota exhausted')


def _per_stock_cli(monkeypatch, tmp_path, stocks):
    universe = tmp_path / 'universe.txt'
    universe.write_text('\n'.join(stocks) + ('\n' if stocks else ''), encoding='utf-8')
    monkeypatch.setattr(bulk, 'LIVE_CACHE_DIR', tmp_path / 'fundamental_cache')
    monkeypatch.setattr(bulk.sys, 'argv', [
        'vfvc_backfill_monthly_rev.py', '--universe', str(universe), '--skip-aggregate'])


def test_per_stock_all_fail_exits_nonzero(monkeypatch, tmp_path):
    """per-stock 路徑全數失敗必須非 0 結束。

    2026-08-02 code review：舊版在 `if not ok_stocks:` 直接 `return` → exit 0，
    排程只會看到成功，而實際上一檔都沒抓到。
    """
    import cache_manager
    monkeypatch.setattr(cache_manager, 'get_finmind_loader', lambda: _AlwaysFailLoader())
    monkeypatch.setattr(
        bulk, 'sync_fundamental_to_finmind_cache',
        lambda **_kwargs: pytest.fail('全數失敗時不該進 sync'))
    _per_stock_cli(monkeypatch, tmp_path, ['2330', '2317', '1101'])

    with pytest.raises(SystemExit) as exc:
        bulk.main()

    assert exc.value.code == 1


def test_per_stock_empty_universe_still_exits_zero(monkeypatch, tmp_path):
    """沒有工作清單不是失敗 —— 不可與「全數失敗」混為一談。"""
    import cache_manager
    monkeypatch.setattr(cache_manager, 'get_finmind_loader', lambda: _AlwaysFailLoader())
    monkeypatch.setattr(
        bulk, 'sync_fundamental_to_finmind_cache',
        lambda **_kwargs: pytest.fail('空清單時不該進 sync'))
    _per_stock_cli(monkeypatch, tmp_path, [])

    bulk.main()          # 正常返回即 exit 0
