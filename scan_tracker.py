"""
掃描結果追蹤系統 — 追蹤 scanner 選出的股票後續表現

功能:
1. 讀取 data/history/ 的掃描結果
2. 查詢每檔 pick 在 N 天後的價格
3. 計算報酬率、勝率、平均報酬
4. 儲存追蹤結果到 data/tracking/

追蹤間隔: 5 / 10 / 20 個交易日
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

TRACKING_INTERVALS = [5, 10, 20, 40, 60]  # 追蹤天數（含波段 horizon）
HISTORY_DIR = Path('data/history')
TRACKING_DIR = Path('data/tracking')
LATEST_DIR = Path('data/latest')

# Benchmarks for Information Ratio (BM-b, 2026-04-14)
# TW: 0050 大盤 beta + 00981A 主動型 ETF AI 主題代表（驗證是否只吃 AI beta）
# US: SPY 大盤 beta + QQQ 科技 beta（驗證 AI 多頭）
# 注意：00981A 需 .TW 後綴 yfinance 才能抓到（純字母數字混合非 isdigit，
# load_and_resample 會落到美股 yfinance 路徑，直接用 00981A 會 404）。
BENCHMARKS = {
    'tw': ['0050', '00981A.TW'],
    'us': ['SPY', 'QQQ'],
}


def _bm_display_name(bm_id):
    """顯示用去掉 .TW 後綴，讓 log 乾淨。"""
    return bm_id[:-3] if bm_id.endswith('.TW') else bm_id


class ScanTracker:
    """追蹤 scanner picks 的後續表現"""

    def __init__(self, progress_callback=None):
        self.progress = progress_callback or (lambda msg: print(msg))
        TRACKING_DIR.mkdir(parents=True, exist_ok=True)
        self._bm_price_cache = {}  # benchmark_id -> DataFrame (cache per run)

    # ================================================================
    # 1. Load scan history
    # ================================================================

    def load_scan_history(self):
        """
        讀取所有歷史掃描結果。

        Returns:
            list of dict: [{scan_date, scan_type, market, file, results}, ...]
        """
        scans = []
        if not HISTORY_DIR.exists():
            return scans

        for f in sorted(HISTORY_DIR.glob('*.json')):
            try:
                with open(f, 'r', encoding='utf-8') as fp:
                    data = json.load(fp)

                # Parse filename: 2026-04-11_momentum.json or 2026-04-11_value_us.json
                stem = f.stem  # e.g. "2026-04-11_momentum_us"
                parts = stem.split('_', 1)  # ["2026-04-11", "momentum_us"]
                scan_date = parts[0]
                rest = parts[1] if len(parts) > 1 else ''

                if '_us' in rest:
                    scan_type = rest.replace('_us', '')
                    market = 'us'
                else:
                    scan_type = rest
                    market = 'tw'

                results = data.get('results', [])
                if results:
                    scans.append({
                        'scan_date': scan_date,
                        'scan_type': scan_type,
                        'market': market,
                        'file': str(f),
                        'total_scanned': data.get('total_scanned', 0),
                        'scored_count': data.get('scored_count', 0),
                        'results': results,
                    })
            except Exception as e:
                logger.warning("Failed to load %s: %s", f, e)

        return scans

    # ================================================================
    # 2. Fetch post-scan prices
    # ================================================================

    def _get_price_after(self, stock_id, scan_date, days):
        """
        取得掃描日後第 N 個交易日的收盤價。

        Args:
            stock_id: 股票代號
            scan_date: 掃描日期 (str 'YYYY-MM-DD')
            days: 幾個交易日後

        Returns:
            float or None: 收盤價
        """
        try:
            # Use load_and_resample which has full cache support
            from technical_analysis import load_and_resample
            _, df, _, _ = load_and_resample(stock_id)

            if df is None or df.empty:
                return None

            # Ensure datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # Filter to dates >= scan_date
            scan_dt = pd.Timestamp(scan_date)
            future = df[df.index >= scan_dt]

            if len(future) <= days:
                return None  # 還沒到追蹤日

            # days=0 is scan day, days=5 is 5 trading days later
            target_row = future.iloc[days]
            close = target_row.get('Close', None)
            if close is not None and not np.isnan(close):
                return float(close)
            return None

        except Exception as e:
            logger.debug("Price fetch failed for %s +%dd: %s", stock_id, days, e)
            return None

    def _get_benchmark_price_series(self, benchmark_id):
        """載入 benchmark OHLCV，整個 run 只抓一次。"""
        if benchmark_id in self._bm_price_cache:
            return self._bm_price_cache[benchmark_id]
        try:
            from technical_analysis import load_and_resample
            _, df, _, _ = load_and_resample(benchmark_id)
            if df is None or df.empty:
                self._bm_price_cache[benchmark_id] = None
                return None
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            self._bm_price_cache[benchmark_id] = df
            return df
        except Exception as e:
            logger.debug("Benchmark %s fetch failed: %s", benchmark_id, e)
            self._bm_price_cache[benchmark_id] = None
            return None

    def _get_benchmark_returns(self, market, scan_date):
        """
        計算 benchmark 在 scan_date 後 5/10/20 交易日的報酬率。

        與 _get_price_after 用同樣的「scan_date 之後第 N 個交易日收盤」對齊方式
        （iloc[0] 為 scan 當日或之後第一個交易日，iloc[d] 為之後第 d 個），
        確保個股與 benchmark 用同一根日線比較。

        Returns:
            dict: {benchmark_id: {'5d': pct, '10d': pct, '20d': pct}}
                  值為 None 表示該 horizon 尚未到期或該 benchmark 當日無資料。
        """
        benchmarks = BENCHMARKS.get(market, [])
        result = {}
        scan_dt = pd.Timestamp(scan_date)
        for bm in benchmarks:
            df = self._get_benchmark_price_series(bm)
            null_row = {f'{d}d': None for d in TRACKING_INTERVALS}
            if df is None or df.empty:
                result[bm] = null_row
                continue
            future = df[df.index >= scan_dt]
            if future.empty:
                result[bm] = null_row
                continue
            try:
                price_at_scan = float(future.iloc[0].get('Close', np.nan))
            except Exception:
                price_at_scan = np.nan
            if not np.isfinite(price_at_scan) or price_at_scan <= 0:
                result[bm] = null_row
                continue
            bm_rets = {}
            for d in TRACKING_INTERVALS:
                if len(future) <= d:
                    bm_rets[f'{d}d'] = None
                    continue
                try:
                    price_later = float(future.iloc[d].get('Close', np.nan))
                except Exception:
                    price_later = np.nan
                if not np.isfinite(price_later):
                    bm_rets[f'{d}d'] = None
                else:
                    bm_rets[f'{d}d'] = round((price_later - price_at_scan) / price_at_scan * 100, 2)
            result[bm] = bm_rets
        return result

    # ================================================================
    # 3. Track a single scan
    # ================================================================

    def track_scan(self, scan_entry):
        """
        追蹤一次掃描的所有 picks。

        Args:
            scan_entry: dict from load_scan_history()

        Returns:
            dict: tracking result with per-stock performance
        """
        scan_date = scan_entry['scan_date']
        scan_type = scan_entry['scan_type']
        market = scan_entry['market']
        results = scan_entry['results']

        self.progress(f"Tracking {scan_type} {market} {scan_date}: {len(results)} picks")

        tracked = []
        for r in results:
            stock_id = r['stock_id']
            price_at_scan = r.get('price', 0)

            if price_at_scan <= 0:
                continue

            entry = {
                'stock_id': stock_id,
                'name': r.get('name', ''),
                'scan_date': scan_date,
                'scan_type': scan_type,
                'market': market,
                'price_at_scan': price_at_scan,
            }

            # Score info
            if scan_type == 'momentum':
                entry['trigger_score'] = r.get('trigger_score', 0)
                entry['trend_score'] = r.get('trend_score', 0)
            elif scan_type == 'value':
                entry['value_score'] = r.get('value_score', 0)

            # Fetch future prices
            for days in TRACKING_INTERVALS:
                price_later = self._get_price_after(stock_id, scan_date, days)
                if price_later is not None:
                    ret = (price_later - price_at_scan) / price_at_scan * 100
                    entry[f'price_{days}d'] = round(price_later, 2)
                    entry[f'return_{days}d'] = round(ret, 2)
                    entry[f'hit_{days}d'] = ret > 0
                else:
                    entry[f'price_{days}d'] = None
                    entry[f'return_{days}d'] = None
                    entry[f'hit_{days}d'] = None

            tracked.append(entry)
            time.sleep(0.1)  # Rate limit

        # Benchmark 報酬（scan level，一次算完 5/10/20d 對每個 benchmark）
        benchmark_returns = self._get_benchmark_returns(market, scan_date)

        return {
            'scan_date': scan_date,
            'scan_type': scan_type,
            'market': market,
            'tracked_count': len(tracked),
            'benchmark_returns': benchmark_returns,
            'picks': tracked,
        }

    # ================================================================
    # 4. Run full tracking update
    # ================================================================

    def run(self):
        """
        對所有歷史掃描結果進行追蹤更新。
        跳過已追蹤且完整的結果。

        Returns:
            dict: summary with all tracking results
        """
        scans = self.load_scan_history()
        if not scans:
            self.progress("No scan history found")
            return {'scans': [], 'summary': {}}

        self.progress(f"Found {len(scans)} historical scans")

        # Load existing tracking data
        existing = self._load_tracking_data()
        existing_keys = {(t['scan_date'], t['scan_type'], t['market'])
                         for t in existing.get('scans', [])}

        all_tracked = list(existing.get('scans', []))
        updated = False

        for scan in scans:
            key = (scan['scan_date'], scan['scan_type'], scan['market'])

            # Check if already fully tracked
            existing_scan = next(
                (t for t in all_tracked
                 if (t['scan_date'], t['scan_type'], t['market']) == key),
                None
            )

            if existing_scan:
                # Check if all intervals are filled
                picks = existing_scan.get('picks', [])
                max_interval = max(TRACKING_INTERVALS)
                all_complete = all(
                    p.get(f'return_{max_interval}d') is not None
                    for p in picks
                ) if picks else False

                if all_complete:
                    # Backfill benchmark_returns for scans tracked before BM-b shipped
                    if 'benchmark_returns' not in existing_scan:
                        existing_scan['benchmark_returns'] = self._get_benchmark_returns(
                            existing_scan['market'], existing_scan['scan_date']
                        )
                        updated = True
                        self.progress(f"  Backfilled benchmark_returns for {key}")
                    else:
                        self.progress(f"  Skip {key} (fully tracked)")
                    continue
                else:
                    # Re-track to fill missing intervals
                    all_tracked = [t for t in all_tracked
                                   if (t['scan_date'], t['scan_type'], t['market']) != key]

            tracked = self.track_scan(scan)
            all_tracked.append(tracked)
            updated = True

        # Compute summary
        summary = self._compute_summary(all_tracked)

        result = {
            'updated_at': datetime.now().isoformat(),
            'scans': all_tracked,
            'summary': summary,
        }

        if updated:
            self._save_tracking_data(result)
            self.progress("Tracking data saved")

        return result

    # ================================================================
    # 5. Performance summary
    # ================================================================

    def _compute_summary(self, all_tracked):
        """計算各 scan_type × market 的績效摘要"""
        summaries = {}

        for scan in all_tracked:
            scan_type = scan['scan_type']
            market = scan['market']
            key = f"{scan_type}_{market}"
            picks = scan.get('picks', [])

            if key not in summaries:
                summaries[key] = {
                    'scan_type': scan_type,
                    'market': market,
                    'total_scans': 0,
                    'total_picks': 0,
                }
                for d in TRACKING_INTERVALS:
                    summaries[key][f'tracked_{d}d'] = 0
                    summaries[key][f'win_rate_{d}d'] = 0
                    summaries[key][f'avg_return_{d}d'] = 0
                    summaries[key][f'best_{d}d'] = None
                    summaries[key][f'worst_{d}d'] = None

            s = summaries[key]
            s['total_scans'] += 1
            s['total_picks'] += len(picks)

            for d in TRACKING_INTERVALS:
                returns = [p[f'return_{d}d'] for p in picks
                           if p.get(f'return_{d}d') is not None]
                hits = [p[f'hit_{d}d'] for p in picks
                        if p.get(f'hit_{d}d') is not None]

                if returns:
                    s[f'tracked_{d}d'] += len(returns)
                    # Running averages will be computed at the end
                    s.setdefault(f'_returns_{d}d', []).extend(returns)
                    s.setdefault(f'_hits_{d}d', []).extend(hits)

        # Finalize averages
        for key, s in summaries.items():
            for d in TRACKING_INTERVALS:
                rets = s.pop(f'_returns_{d}d', [])
                hits = s.pop(f'_hits_{d}d', [])
                if rets:
                    s[f'tracked_{d}d'] = len(rets)
                    s[f'avg_return_{d}d'] = round(np.mean(rets), 2)
                    s[f'win_rate_{d}d'] = round(sum(1 for h in hits if h) / len(hits) * 100, 1)
                    s[f'best_{d}d'] = round(max(rets), 2)
                    s[f'worst_{d}d'] = round(min(rets), 2)
                    s[f'median_return_{d}d'] = round(np.median(rets), 2)

        # === Benchmark IR (BM-b) ===
        # 每個 pick 的超額報酬 = pick_return - 同 scan_date 的 benchmark_return
        # IR = mean(excess) / std(excess)
        for key, s in summaries.items():
            market = s['market']
            bm_list = BENCHMARKS.get(market, [])
            if not bm_list:
                continue
            s['benchmarks'] = {}
            for bm in bm_list:
                s['benchmarks'][bm] = {}
                for d in TRACKING_INTERVALS:
                    excess_list = []
                    for scan in all_tracked:
                        if scan['scan_type'] != s['scan_type'] or scan['market'] != market:
                            continue
                        bm_ret = scan.get('benchmark_returns', {}).get(bm, {}).get(f'{d}d')
                        if bm_ret is None:
                            continue
                        for p in scan.get('picks', []):
                            pr = p.get(f'return_{d}d')
                            if pr is None:
                                continue
                            excess_list.append(pr - bm_ret)
                    if excess_list:
                        avg_excess = float(np.mean(excess_list))
                        te = float(np.std(excess_list, ddof=1)) if len(excess_list) > 1 else 0.0
                        ir = avg_excess / te if te > 0 else 0.0
                        win = sum(1 for e in excess_list if e > 0) / len(excess_list) * 100
                        s['benchmarks'][bm][f'{d}d'] = {
                            'n': len(excess_list),
                            'avg_excess': round(avg_excess, 2),
                            'tracking_error': round(te, 2),
                            'ir': round(ir, 3),
                            'win_rate_vs_bm': round(win, 1),
                        }
                    else:
                        s['benchmarks'][bm][f'{d}d'] = None

        return summaries

    # ================================================================
    # 6. Persistence
    # ================================================================

    def _load_tracking_data(self):
        """Load existing tracking data"""
        path = TRACKING_DIR / 'tracking_data.json'
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {'scans': [], 'summary': {}}

    def _save_tracking_data(self, data):
        """Save tracking data"""
        TRACKING_DIR.mkdir(parents=True, exist_ok=True)
        path = TRACKING_DIR / 'tracking_data.json'
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    def load_latest(self):
        """Load the latest tracking summary (for UI display)"""
        data = self._load_tracking_data()
        return data

    # ================================================================
    # 7. Per-pick details (for UI drill-down)
    # ================================================================

    def get_picks_dataframe(self, scan_type=None, market=None):
        """
        Get all tracked picks as a DataFrame for display.

        Args:
            scan_type: filter by 'momentum' or 'value'
            market: filter by 'tw' or 'us'

        Returns:
            pd.DataFrame
        """
        data = self._load_tracking_data()
        all_picks = []
        for scan in data.get('scans', []):
            if scan_type and scan['scan_type'] != scan_type:
                continue
            if market and scan['market'] != market:
                continue
            for p in scan.get('picks', []):
                all_picks.append(p)

        if not all_picks:
            return pd.DataFrame()

        df = pd.DataFrame(all_picks)
        # Sort by scan_date desc, then by score desc
        sort_cols = ['scan_date']
        if 'trigger_score' in df.columns:
            sort_cols.append('trigger_score')
        elif 'value_score' in df.columns:
            sort_cols.append('value_score')
        df.sort_values(sort_cols, ascending=[False, False], inplace=True)
        return df.reset_index(drop=True)


# ================================================================
# CLI
# ================================================================

if __name__ == '__main__':
    import sys

    def progress(msg):
        print(msg)

    tracker = ScanTracker(progress_callback=progress)
    result = tracker.run()

    summary = result.get('summary', {})
    if not summary:
        print("\nNo tracking data yet. Run scanner first to generate history.")
        sys.exit(0)

    print("\n" + "=" * 60)
    print("SCAN PERFORMANCE TRACKING SUMMARY")
    print("=" * 60)

    for key, s in summary.items():
        print(f"\n--- {s['scan_type'].upper()} ({s['market'].upper()}) ---")
        print(f"Scans: {s['total_scans']}, Total Picks: {s['total_picks']}")

        for d in TRACKING_INTERVALS:
            tracked = s.get(f'tracked_{d}d', 0)
            if tracked > 0:
                wr = s.get(f'win_rate_{d}d', 0)
                avg = s.get(f'avg_return_{d}d', 0)
                med = s.get(f'median_return_{d}d', 0)
                best = s.get(f'best_{d}d', 0)
                worst = s.get(f'worst_{d}d', 0)
                print(f"  {d:2d}d: {tracked} tracked | "
                      f"Win {wr:.1f}% | "
                      f"Avg {avg:+.2f}% | Med {med:+.2f}% | "
                      f"Best {best:+.2f}% | Worst {worst:+.2f}%")
            else:
                print(f"  {d:2d}d: Not enough days elapsed yet")
