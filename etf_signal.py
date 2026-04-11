"""
etf_signal.py — 主動型 ETF 同步買賣超訊號

讀取 TWActiveETFCrawler (GitHub Pages) 公開 JSON 數據，
分析近 N 日主動型 ETF 持倉變化，偵測多檔 ETF 同步買超/賣超個股。

數據來源: https://sheenartem.github.io/TWActiveETFCrawler/
更新時間: 每日 18:00 (GitHub Actions)
"""

import logging
import time
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://sheenartem.github.io/TWActiveETFCrawler"
_CACHE_TTL = 3600  # 1 hour cache
_REQUEST_TIMEOUT = 15


class ETFSignal:
    """主動型 ETF 同步買賣超訊號分析"""

    def __init__(self):
        self._cache = {}  # key -> (data, timestamp)
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'StockAnalyzer/1.0',
        })

    # ----------------------------------------------------------------
    # Public API
    # ----------------------------------------------------------------

    def get_sync_signals(self, days=5):
        """
        取得近 N 日 ETF 同步買賣超統計。

        Args:
            days: 往回看幾個交易日 (default 5)

        Returns:
            dict: {
                stock_id: {
                    'buy_count': int,   # 買超的 ETF 檔數
                    'sell_count': int,  # 賣超的 ETF 檔數
                    'net_count': int,   # 淨買超檔數 (buy - sell)
                    'net_lots': float,  # 淨買超張數
                    'stock_name': str,
                    'details': [{etf_code, etf_name, diff, direction}, ...]
                }
            }
        """
        cache_key = f"sync_signals_{days}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        # 1. Fetch report index to get recent dates
        dates = self._get_recent_dates(days)
        if not dates:
            logger.warning("No recent ETF report dates found")
            return {}

        # 2. Fetch and aggregate data for each date
        stock_signals = {}
        for date_str in dates:
            data = self._fetch_daily_data(date_str)
            if not data:
                continue
            self._aggregate_changes(data, stock_signals)

        # 3. Compute summary counts
        result = {}
        for sid, info in stock_signals.items():
            buy_etfs = set()
            sell_etfs = set()
            net_lots = 0.0
            details = []

            for change in info['changes']:
                etf_code = change['etf_code']
                diff = change.get('diff', 0)
                direction = change.get('direction', '')

                if direction == 'up' or diff > 0:
                    buy_etfs.add(etf_code)
                elif direction == 'down' or diff < 0:
                    sell_etfs.add(etf_code)

                net_lots += diff
                details.append(change)

            result[sid] = {
                'buy_count': len(buy_etfs),
                'sell_count': len(sell_etfs),
                'net_count': len(buy_etfs) - len(sell_etfs),
                'net_lots': round(net_lots, 1),
                'stock_name': info.get('stock_name', ''),
                'details': details,
            }

        self._set_cache(cache_key, result)
        logger.info("ETF sync signals: %d stocks with activity (last %d days)",
                     len(result), days)
        return result

    def get_stock_signal(self, stock_id, days=5):
        """
        取得單一股票的 ETF 訊號。

        Returns:
            dict or None: {buy_count, sell_count, net_count, net_lots, details}
        """
        signals = self.get_sync_signals(days)
        return signals.get(str(stock_id))

    def get_top_buys(self, days=5, min_count=2):
        """
        取得被多檔 ETF 同步買超的股票，按買超 ETF 數排序。

        Args:
            days: 往回看幾個交易日
            min_count: 最少被幾檔 ETF 買超

        Returns:
            list of (stock_id, signal_dict), sorted by buy_count desc
        """
        signals = self.get_sync_signals(days)
        filtered = [
            (sid, info) for sid, info in signals.items()
            if info['buy_count'] >= min_count
        ]
        filtered.sort(key=lambda x: x[1]['buy_count'], reverse=True)
        return filtered

    # ----------------------------------------------------------------
    # Internal
    # ----------------------------------------------------------------

    def _get_recent_dates(self, days):
        """Fetch reports_index.json and return the most recent N dates."""
        cache_key = "reports_index"
        cached = self._get_cache(cache_key)
        if cached is not None:
            index_data = cached
        else:
            try:
                url = f"{_BASE_URL}/reports_index.json"
                resp = self._session.get(url, timeout=_REQUEST_TIMEOUT)
                resp.raise_for_status()
                index_data = resp.json()
                self._set_cache(cache_key, index_data)
            except Exception as e:
                logger.error("Failed to fetch reports_index.json: %s", e)
                return []

        # index_data is a list of {date, etf_count, total_changes, ...}
        if not isinstance(index_data, list):
            return []

        # Sort by date descending and take the most recent N
        sorted_entries = sorted(index_data, key=lambda x: x.get('date', ''), reverse=True)
        return [entry['date'] for entry in sorted_entries[:days] if 'date' in entry]

    def _fetch_daily_data(self, date_str):
        """Fetch data_{date}.json for a specific date."""
        cache_key = f"daily_{date_str}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        try:
            url = f"{_BASE_URL}/data_{date_str}.json"
            resp = self._session.get(url, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            self._set_cache(cache_key, data)
            return data
        except Exception as e:
            logger.warning("Failed to fetch data_%s.json: %s", date_str, e)
            return None

    def _aggregate_changes(self, daily_data, stock_signals):
        """
        Aggregate stock changes from one day's data into stock_signals dict.

        Processes: detailed_changes[].added/removed/modified
        """
        changes = daily_data.get('detailed_changes', [])

        for etf_entry in changes:
            etf_code = etf_entry.get('etf_code', '')
            etf_name = etf_entry.get('etf_name', '')

            # Added stocks (new position = buy)
            for stock in etf_entry.get('added', []):
                sid = str(stock.get('stock_code', '')).strip()
                if not sid:
                    continue
                if sid not in stock_signals:
                    stock_signals[sid] = {'stock_name': stock.get('stock_name', ''), 'changes': []}
                stock_signals[sid]['changes'].append({
                    'etf_code': etf_code,
                    'etf_name': etf_name,
                    'diff': stock.get('lots', 0),
                    'direction': 'up',
                    'type': 'added',
                })

            # Removed stocks (exited position = sell)
            for stock in etf_entry.get('removed', []):
                sid = str(stock.get('stock_code', '')).strip()
                if not sid:
                    continue
                if sid not in stock_signals:
                    stock_signals[sid] = {'stock_name': stock.get('stock_name', ''), 'changes': []}
                stock_signals[sid]['changes'].append({
                    'etf_code': etf_code,
                    'etf_name': etf_name,
                    'diff': -stock.get('lots', 0),
                    'direction': 'down',
                    'type': 'removed',
                })

            # Modified stocks (increased/decreased position)
            for stock in etf_entry.get('modified', []):
                sid = str(stock.get('stock_code', '')).strip()
                if not sid:
                    continue
                if sid not in stock_signals:
                    stock_signals[sid] = {'stock_name': stock.get('stock_name', ''), 'changes': []}
                diff = stock.get('diff', 0)
                direction = stock.get('direction', 'up' if diff > 0 else 'down')
                stock_signals[sid]['changes'].append({
                    'etf_code': etf_code,
                    'etf_name': etf_name,
                    'diff': diff,
                    'direction': direction,
                    'type': 'modified',
                })

    def _get_cache(self, key):
        if key in self._cache:
            data, ts = self._cache[key]
            if time.time() - ts < _CACHE_TTL:
                return data
            del self._cache[key]
        return None

    def _set_cache(self, key, data):
        self._cache[key] = (data, time.time())


# ====================================================================
# CLI Test
# ====================================================================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')

    etf = ETFSignal()

    print("=== ETF Sync Buy/Sell Signals (Last 5 Days) ===\n")

    top_buys = etf.get_top_buys(days=5, min_count=2)
    if top_buys:
        print(f"Stocks bought by >= 2 ETFs: {len(top_buys)}\n")
        print(f"{'ID':>6} {'Name':<10} {'Buy':>4} {'Sell':>4} {'Net':>4} {'Lots':>8}")
        print("-" * 50)
        for sid, info in top_buys[:20]:
            print(f"{sid:>6} {info['stock_name'][:10]:<10} "
                  f"{info['buy_count']:>4} {info['sell_count']:>4} "
                  f"{info['net_count']:>4} {info['net_lots']:>+8.0f}")
    else:
        print("No sync buy signals found.")

    # Single stock test
    print("\n--- Single Stock: 2330 ---")
    sig = etf.get_stock_signal('2330')
    if sig:
        print(f"Buy ETFs: {sig['buy_count']}, Sell ETFs: {sig['sell_count']}, "
              f"Net lots: {sig['net_lots']:+.0f}")
        for d in sig['details']:
            print(f"  {d['etf_code']} {d['etf_name'][:15]} "
                  f"{d['direction']} {d['diff']:+.0f} lots ({d['type']})")
    else:
        print("No ETF activity for 2330")
