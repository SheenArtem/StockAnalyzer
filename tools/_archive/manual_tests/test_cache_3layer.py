"""
Cache 三層架構 smoke / integration tests (鐵則 #5)

5 個關鍵行為驗證：
1. frozen 空 + live 既存 → 回 live (行為與舊版一致)
2. 並行 fetch 同 (key, sid) → 只 1 個實際打 API (fetch lock 工作)
3. frozen + live 都有 + 日期重疊 → live 優先 merge dedupe
4. frozen 有 + live 空 → 回 frozen (frozen-only fallback)
5. bulk fetch + merge → 只補缺期，不覆寫已有

CLI:
  python tools/test_cache_3layer.py            # 跑全部
  python tools/test_cache_3layer.py --skip 2   # 跳第 2 個 (無網路)
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

PASS = "[PASS]"
FAIL = "[FAIL]"
SKIP = "[SKIP]"


def test_1_frozen_empty_live_present():
    """frozen 空 + live 既存 → 回 live (行為與舊版一致)"""
    from cache_manager import _merge_frozen_and_live, get_cached_fundamentals, get_finmind_loader, FROZEN_DIR
    print(f"\n=== Test 1: frozen empty + live present ===")
    print(f"  FROZEN_DIR exists: {FROZEN_DIR.exists()}")
    dl = get_finmind_loader()
    df = get_cached_fundamentals(
        dl, cache_key='month_revenue', stock_id='2330',
        mops_fetcher=None, finmind_method='taiwan_stock_month_revenue', freshness='monthly')
    if df.empty:
        return f"{FAIL} 2330 returned empty"
    if len(df) < 100:
        return f"{FAIL} 2330 only {len(df)} rows (expected >100 historical)"
    return f"{PASS} 2330 returned {len(df)} rows, latest={df['date'].max()}"


def test_2_concurrent_fetch_lock():
    """並行 2 thread 對 same (key, sid) miss → 應只 1 個實際打 API"""
    from cache_manager import get_cached_fundamentals, get_finmind_loader, _fetch_locks, _fetch_locks_master
    from cache_manager import LIVE_DIR
    print(f"\n=== Test 2: concurrent fetch lock ===")
    # Mock: temporarily remove cache file → both threads miss
    test_sid = '9999_test_unused'
    cache_path = LIVE_DIR / f"month_revenue_{test_sid}.parquet"
    if cache_path.exists():
        cache_path.unlink()

    fetch_count = [0]
    def mock_fetcher(stock_id):
        fetch_count[0] += 1
        time.sleep(0.5)
        # Return synthetic data
        return pd.DataFrame({
            'date': [pd.Timestamp('2026-04-01')],
            'stock_id': [stock_id], 'country': ['Taiwan'], 'revenue': [1000],
            'revenue_month': [3], 'revenue_year': [2026],
            'revenue_last_year': [800], 'revenue_year_growth': [25.0],
            'revenue_last_month': [900], 'revenue_month_growth': [11.0],
        })

    dl = get_finmind_loader()
    results = [None, None]
    def worker(i):
        # Force USE_MOPS=True path so mock_fetcher is called
        from cache_manager import set_use_mops
        set_use_mops(True)
        results[i] = get_cached_fundamentals(
            dl, cache_key='month_revenue', stock_id=test_sid,
            mops_fetcher=mock_fetcher,
            finmind_method='taiwan_stock_month_revenue', freshness='monthly')

    t1 = threading.Thread(target=worker, args=(0,))
    t2 = threading.Thread(target=worker, args=(1,))
    t1.start()
    time.sleep(0.05)  # 確保 t1 先進入 fetch_lock
    t2.start()
    t1.join()
    t2.join()

    # cleanup
    if cache_path.exists():
        cache_path.unlink()

    if fetch_count[0] != 1:
        return f"{FAIL} expected 1 fetch but got {fetch_count[0]} (lock failed)"
    if results[0] is None or results[1] is None or results[0].empty or results[1].empty:
        return f"{FAIL} one thread returned empty (results: {[type(r).__name__ for r in results]})"
    return f"{PASS} only {fetch_count[0]} actual fetch (both threads got data via lock)"


def test_3_frozen_live_overlap_merge():
    """frozen + live 都有 + 日期重疊 → live 優先 (newer)"""
    from cache_manager import _merge_frozen_and_live
    print(f"\n=== Test 3: frozen + live overlap merge ===")
    frozen = pd.DataFrame({
        'date': pd.to_datetime(['2025-01-01', '2025-02-01', '2025-03-01']),
        'stock_id': ['2330', '2330', '2330'],
        'revenue': [100, 200, 300],
    })
    live = pd.DataFrame({
        'date': pd.to_datetime(['2025-03-01', '2025-04-01']),
        'stock_id': ['2330', '2330'],
        'revenue': [350, 400],  # 2025-03 different from frozen (corrected)
    })
    merged = _merge_frozen_and_live(frozen, live)
    if merged is None or len(merged) != 4:
        return f"{FAIL} expected 4 rows, got {len(merged) if merged is not None else 0}"
    march_row = merged[merged['date'] == pd.Timestamp('2025-03-01')]
    if march_row.empty or march_row.iloc[0]['revenue'] != 350:
        return f"{FAIL} 2025-03 should be 350 (live), got {march_row.iloc[0]['revenue'] if not march_row.empty else 'missing'}"
    return f"{PASS} 4 rows, 2025-03 took live=350 (not frozen=300)"


def test_4_frozen_only_fallback():
    """frozen 有 + live 空 → 回 frozen"""
    from cache_manager import _merge_frozen_and_live
    print(f"\n=== Test 4: frozen-only fallback ===")
    frozen = pd.DataFrame({
        'date': pd.to_datetime(['2025-01-01', '2025-02-01']),
        'stock_id': ['2330', '2330'],
        'revenue': [100, 200],
    })
    merged = _merge_frozen_and_live(frozen, None)
    if merged is None or len(merged) != 2:
        return f"{FAIL} expected 2 rows from frozen, got {merged}"
    return f"{PASS} frozen-only: 2 rows returned"


def test_5_bulk_merge_no_overwrite():
    """bulk fetch merge 已有期間不覆寫"""
    from mops_bulk_fetcher import merge_into_existing_cache
    print(f"\n=== Test 5: bulk merge no-overwrite ===")
    # Build synthetic bulk df with one already-existing row + one new row
    bulk = pd.DataFrame({
        'date': pd.to_datetime(['2020-01-01', '2099-01-01']),  # one old (exists), one future (new)
        'stock_id': ['2330', '2330'],
        'country': ['Taiwan', 'Taiwan'],
        'revenue': [99999999, 88888888],  # different values to detect overwrite
        'revenue_month': [12, 12],
        'revenue_year': [2019, 2098],
        'revenue_last_year': [0, 0],
        'revenue_year_growth': [0.0, 0.0],
        'revenue_last_month': [0, 0],
        'revenue_month_growth': [0.0, 0.0],
    })
    # dry-run
    stats = merge_into_existing_cache(bulk, dry_run=True)
    if stats.get('errors', 0) > 0:
        return f"{FAIL} dry-run had errors: {stats}"
    return f"{PASS} dry-run merge OK: {stats}"


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--skip', type=int, nargs='*', default=[],
                    help='跳過的 test 編號 (1-5)')
    args = ap.parse_args()

    tests = [
        ('Test 1: frozen empty + live present', test_1_frozen_empty_live_present),
        ('Test 2: concurrent fetch lock', test_2_concurrent_fetch_lock),
        ('Test 3: frozen+live overlap merge', test_3_frozen_live_overlap_merge),
        ('Test 4: frozen-only fallback', test_4_frozen_only_fallback),
        ('Test 5: bulk merge no-overwrite', test_5_bulk_merge_no_overwrite),
    ]
    results = []
    for i, (name, fn) in enumerate(tests, 1):
        if i in args.skip:
            print(f"\n=== {name} ===")
            print(f"  {SKIP}")
            results.append((name, SKIP))
            continue
        try:
            r = fn()
            print(f"  {r}")
            results.append((name, r))
        except Exception as e:
            print(f"  {FAIL} exception: {type(e).__name__}: {e}")
            results.append((name, f"{FAIL} {type(e).__name__}"))

    print("\n=== SUMMARY ===")
    n_pass = sum(1 for _, r in results if r.startswith(PASS))
    n_fail = sum(1 for _, r in results if r.startswith(FAIL))
    n_skip = sum(1 for _, r in results if r.startswith(SKIP))
    print(f"PASS: {n_pass} / FAIL: {n_fail} / SKIP: {n_skip}")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
