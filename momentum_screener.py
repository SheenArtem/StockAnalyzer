"""
右側動能選股引擎 — 全市場掃描找出動能最強的股票

Stage 1: 快速初篩（TWSE/TPEX 全市場日行情）
  - 成交值佔比門檻過濾低流動性股票
  - 動能條件（漲幅 > 0 或近期有爆發）

Stage 2: 完整觸發分數（復用 analysis_engine.py）
  - 批量跑 TechnicalAnalyzer.run_analysis()
  - 排序輸出 Top N
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ================================================================
# Default Configuration
# ================================================================
DEFAULT_CONFIG = {
    # Stage 1: 初篩門檻
    'twse_value_pct': 0.0002,   # 上市：成交值佔比 > 0.02%（約 6000 萬）
    'tpex_value_pct': 0.0005,   # 上櫃：成交值佔比 > 0.05%（約 3000 萬）
    'min_price': 0,              # 最低股價門檻（預設關閉）
    'momentum_change_min': -1.0, # 當日漲跌幅下限 %（允許微跌）

    # Stage 2: 精篩設定
    'top_n': 50,                 # 輸出前 N 名
    'history_days': 365,         # 抓取歷史天數
    'include_chip': True,        # 是否抓籌碼資料（慢但更準）
    'batch_delay': 0.3,          # 每檔間隔秒數（控速）
    'max_failures': 10,          # 連續失敗上限，超過就停止

    # 排除清單
    'exclude_ids': set(),        # 手動排除的股票代號

    # US market settings
    'us_universe': 'sp500',      # 'sp500', 'nasdaq100', or list of tickers
    'us_min_volume': 500_000,    # Minimum daily volume
    'us_min_price': 5.0,         # Minimum price (skip penny stocks)
    'us_include_chip': False,    # US chip data is slow, default off
}


class MomentumScreener:
    """右側動能選股引擎"""

    def __init__(self, config=None, progress_callback=None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.progress = progress_callback or (lambda msg: print(msg))
        self._failures = []

    # ================================================================
    # Public API
    # ================================================================

    def run(self, market='tw'):
        """
        Execute full screening pipeline.

        Args:
            market: 'tw' for Taiwan, 'us' for US stocks

        Returns:
            dict with scan_date, total_scanned, passed_initial, results
        """
        start_time = time.time()
        self._market = market

        # --- Stage 1 ---
        self.progress(f"Stage 1: Fetching {'US' if market == 'us' else 'TW'} market data...")
        if market == 'us':
            market_df = self._fetch_us_market_data()
            candidates = self._stage1_filter_us(market_df) if not market_df.empty else pd.DataFrame()
        else:
            market_df = self._fetch_market_data()
            candidates = self._stage1_filter(market_df) if not market_df.empty else pd.DataFrame()

        if market_df.empty:
            return self._make_result([], 0, 0, time.time() - start_time)

        self.progress(f"Stage 1 done: {len(candidates)}/{len(market_df)} passed")

        if candidates.empty:
            return self._make_result([], len(market_df), 0, time.time() - start_time)

        # --- Stage 2 ---
        self.progress(f"Stage 2: Analyzing {len(candidates)} candidates...")
        scored = self._stage2_analyze(candidates)
        self.progress(f"Stage 2 done: {len(scored)} scored, {len(self._failures)} failed")

        elapsed = time.time() - start_time
        self.progress(f"Scan complete in {elapsed:.0f}s")
        return self._make_result(scored, len(market_df), len(candidates), elapsed)

    def run_stage1_only(self, market='tw'):
        """Only run Stage 1 for quick preview (no trigger scores)."""
        if market == 'us':
            market_df = self._fetch_us_market_data()
            return self._stage1_filter_us(market_df) if not market_df.empty else pd.DataFrame()
        market_df = self._fetch_market_data()
        if market_df.empty:
            return pd.DataFrame()
        return self._stage1_filter(market_df)

    # ================================================================
    # Stage 1: Quick Initial Filter
    # ================================================================

    def _fetch_market_data(self):
        """Fetch full market daily data from TWSE + TPEX."""
        from twse_api import TWSEOpenData
        api = TWSEOpenData()
        return api.get_market_daily_all()

    def _stage1_filter(self, df):
        """
        Filter stocks by liquidity and momentum.

        Criteria:
        1. Trading value > market-relative threshold
        2. Price > minimum
        3. Change % > minimum (allow small dips)
        4. Not in exclude list
        """
        cfg = self.config
        exclude = cfg['exclude_ids']
        results = []

        # Split by market for different thresholds
        for market, threshold_pct in [('twse', cfg['twse_value_pct']),
                                       ('tpex', cfg['tpex_value_pct'])]:
            mdf = df[df['market'] == market].copy()
            if mdf.empty:
                continue

            total_tv = mdf['trading_value'].sum()
            if total_tv <= 0:
                continue

            # 1. Trading value ratio
            mdf['tv_pct'] = mdf['trading_value'] / total_tv
            passed = mdf[mdf['tv_pct'] >= threshold_pct].copy()

            # 2. Price filter
            if cfg['min_price'] > 0:
                passed = passed[passed['close'] >= cfg['min_price']]

            # 3. Momentum filter
            passed = passed[passed['change_pct'] >= cfg['momentum_change_min']]

            # 4. Exclude list
            if exclude:
                passed = passed[~passed['stock_id'].isin(exclude)]

            results.append(passed)

        if not results:
            return pd.DataFrame()

        combined = pd.concat(results, ignore_index=True)
        # Sort by trading value descending (most liquid first)
        combined.sort_values('trading_value', ascending=False, inplace=True)
        return combined

    # ================================================================
    # US Market: Fetch + Filter
    # ================================================================

    def _get_us_universe(self):
        """Get list of US stock tickers based on config."""
        universe = self.config.get('us_universe', 'sp500')

        if isinstance(universe, list):
            return universe

        if universe == 'nasdaq100':
            return self._fetch_nasdaq100()

        # Default: S&P 500
        return self._fetch_sp500()

    @staticmethod
    def _fetch_sp500():
        """Fetch S&P 500 ticker list from Wikipedia."""
        try:
            import requests as _req
            from io import StringIO
            headers = {'User-Agent': 'StockAnalyzer/1.0'}
            resp = _req.get(
                'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
                headers=headers, timeout=15,
            )
            tables = pd.read_html(StringIO(resp.text))
            tickers = tables[0]['Symbol'].tolist()
            # Fix BRK.B → BRK-B for yfinance
            return [t.replace('.', '-') for t in tickers]
        except Exception as e:
            logger.error("Failed to fetch S&P 500 list: %s", e)
            return []

    @staticmethod
    def _fetch_nasdaq100():
        """Fetch Nasdaq 100 ticker list from Wikipedia."""
        try:
            import requests as _req
            from io import StringIO
            headers = {'User-Agent': 'StockAnalyzer/1.0'}
            resp = _req.get(
                'https://en.wikipedia.org/wiki/Nasdaq-100',
                headers=headers, timeout=15,
            )
            tables = pd.read_html(StringIO(resp.text))
            # Nasdaq-100 table usually has 'Ticker' or 'Symbol' column
            for t in tables:
                for col in ['Ticker', 'Symbol']:
                    if col in t.columns:
                        tickers = t[col].tolist()
                        return [str(tk).replace('.', '-') for tk in tickers]
            return []
        except Exception as e:
            logger.error("Failed to fetch Nasdaq 100 list: %s", e)
            return []

    def _fetch_us_market_data(self):
        """
        Fetch daily data for US stock universe via yfinance batch download.

        Returns:
            DataFrame with columns matching TW format:
            stock_id, stock_name, market, close, change, change_pct,
            open, high, low, volume, trading_value
        """
        import yfinance as yf

        tickers = self._get_us_universe()
        if not tickers:
            logger.error("Empty US stock universe")
            return pd.DataFrame()

        self.progress(f"  Downloading {len(tickers)} US tickers...")

        try:
            data = yf.download(
                tickers, period='2d', interval='1d',
                progress=False, auto_adjust=False, timeout=30,
            )
        except Exception as e:
            logger.error("yfinance batch download failed: %s", e)
            return pd.DataFrame()

        if data.empty:
            return pd.DataFrame()

        results = []
        for ticker in tickers:
            try:
                if data.columns.nlevels == 2:
                    close = data[('Close', ticker)].dropna()
                    volume = data[('Volume', ticker)].dropna()
                    _open = data[('Open', ticker)].dropna()
                    high = data[('High', ticker)].dropna()
                    low = data[('Low', ticker)].dropna()
                else:
                    # Single ticker
                    close = data['Close'].dropna()
                    volume = data['Volume'].dropna()
                    _open = data['Open'].dropna()
                    high = data['High'].dropna()
                    low = data['Low'].dropna()

                if len(close) < 1:
                    continue

                latest_close = float(close.iloc[-1])
                latest_vol = float(volume.iloc[-1])

                if len(close) >= 2:
                    prev_close = float(close.iloc[-2])
                    change = latest_close - prev_close
                    change_pct = (change / prev_close * 100) if prev_close != 0 else 0
                else:
                    change = 0
                    change_pct = 0

                results.append({
                    'stock_id': ticker,
                    'stock_name': ticker,  # yfinance doesn't give names in batch
                    'market': 'us',
                    'close': latest_close,
                    'change': round(change, 2),
                    'change_pct': round(change_pct, 2),
                    'open': float(_open.iloc[-1]) if len(_open) > 0 else 0,
                    'high': float(high.iloc[-1]) if len(high) > 0 else 0,
                    'low': float(low.iloc[-1]) if len(low) > 0 else 0,
                    'volume': int(latest_vol),
                    'trading_value': int(latest_close * latest_vol),
                })
            except Exception:
                continue

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        logger.info("US market data: %d tickers fetched", len(df))
        return df

    def _stage1_filter_us(self, df):
        """
        Filter US stocks by volume and momentum.

        Criteria:
        1. Volume > minimum
        2. Price > minimum (skip penny stocks)
        3. Change % > momentum minimum
        """
        cfg = self.config

        mask = pd.Series(True, index=df.index)
        mask &= df['volume'] >= cfg.get('us_min_volume', 500_000)
        mask &= df['close'] >= cfg.get('us_min_price', 5.0)
        mask &= df['change_pct'] >= cfg['momentum_change_min']

        result = df[mask].copy()
        result.sort_values('trading_value', ascending=False, inplace=True)
        return result

    # ================================================================
    # Stage 2: Full Trigger Score Analysis
    # ================================================================

    def _stage2_analyze(self, candidates):
        """
        Batch-run TechnicalAnalyzer on each candidate.

        For each stock:
        1. Download historical price data (with caching)
        2. Calculate technical indicators
        3. Run trigger score analysis
        4. Optionally fetch chip data

        Returns:
            list of dicts, sorted by trigger_score descending
        """
        from technical_analysis import (
            calculate_all_indicators,
            load_and_resample,
        )
        from analysis_engine import TechnicalAnalyzer

        cfg = self.config
        scored = []
        total = len(candidates)
        consecutive_fails = 0

        for idx, row in candidates.iterrows():
            sid = row['stock_id']
            sname = row.get('stock_name', '')
            pos = len(scored) + len(self._failures) + 1

            if pos % 10 == 0 or pos <= 3:
                self.progress(f"  [{pos}/{total}] {sid} {sname}")

            try:
                result = self._analyze_single(sid, row)
                if result:
                    scored.append(result)
                    consecutive_fails = 0
                else:
                    self._failures.append(sid)
                    consecutive_fails += 1
            except Exception as e:
                logger.warning("Failed to analyze %s: %s", sid, e)
                self._failures.append(sid)
                consecutive_fails += 1

            # Safety: stop if too many consecutive failures
            if consecutive_fails >= cfg['max_failures']:
                self.progress(f"  Stopping: {consecutive_fails} consecutive failures")
                break

            # Throttle
            if cfg['batch_delay'] > 0:
                time.sleep(cfg['batch_delay'])

        # Sort by trigger_score descending, take top N
        scored.sort(key=lambda x: x['trigger_score'], reverse=True)
        return scored[:cfg['top_n']]

    def _analyze_single(self, stock_id, market_row):
        """
        Analyze a single stock and return result dict.

        Returns:
            dict or None (if analysis failed)
        """
        from technical_analysis import calculate_all_indicators, load_and_resample
        from analysis_engine import TechnicalAnalyzer

        # 1. Load price data (uses existing cache_manager)
        try:
            ticker, df_day, df_week, meta = load_and_resample(stock_id)
        except Exception as e:
            logger.debug("load_and_resample failed for %s: %s", stock_id, e)
            return None

        if df_day.empty or len(df_day) < 60:
            logger.debug("Insufficient data for %s (%d rows)", stock_id, len(df_day))
            return None

        # 2. Calculate indicators
        try:
            df_day = calculate_all_indicators(df_day)
            df_week = calculate_all_indicators(df_week)
        except Exception as e:
            logger.debug("Indicator calculation failed for %s: %s", stock_id, e)
            return None

        # 3. Optionally fetch chip data
        chip_data = None
        us_chip_data = None
        is_us = not stock_id.isdigit()

        if is_us:
            if self.config.get('us_include_chip', False):
                try:
                    from us_stock_chip import USStockChipAnalyzer
                    usc = USStockChipAnalyzer()
                    us_chip_data, _ = usc.get_chip_data(stock_id)
                except Exception:
                    pass
        else:
            if self.config['include_chip']:
                try:
                    from chip_analysis import ChipAnalyzer
                    ca = ChipAnalyzer()
                    chip_data, _ = ca.get_chip_data(stock_id)
                except Exception:
                    pass

        # 4. Run analysis
        try:
            analyzer = TechnicalAnalyzer(
                stock_id, df_week, df_day,
                chip_data=chip_data,
                us_chip_data=us_chip_data,
            )
            report = analyzer.run_analysis()
        except Exception as e:
            logger.debug("TechnicalAnalyzer failed for %s: %s", stock_id, e)
            return None

        trigger = report.get('trigger_score', 0)
        trend = report.get('trend_score', 0)

        # 5. Extract key signals
        signals = self._extract_signals(report)

        # 6. ETF buy count (for display; scoring already in trigger via analysis_engine)
        etf_buy_count = 0
        breakdown = report.get('trigger_breakdown', {})
        if breakdown.get('etf_score', 0) > 0:
            # Estimate count from score: 0.3 → 2, 0.6 → 3+
            etf_buy_count = 3 if breakdown['etf_score'] >= 0.5 else 2

        return {
            'stock_id': stock_id,
            'name': market_row.get('stock_name', meta.get('name', '')),
            'market': market_row.get('market', 'twse'),
            'price': market_row.get('close', 0),
            'change_pct': round(market_row.get('change_pct', 0), 2),
            'trading_value': int(market_row.get('trading_value', 0)),
            'trigger_score': round(trigger, 2),
            'trend_score': round(trend, 2),
            'score_percentile': report.get('score_percentile', None),
            'regime': report.get('regime', {}).get('regime', 'unknown'),
            'etf_buy_count': etf_buy_count,
            'signals': signals,
            'trigger_details': report.get('trigger_details', []),
        }

    def _extract_signals(self, report):
        """Extract key signal tags from analysis report."""
        signals = []
        details = report.get('trigger_details', [])
        detail_text = ' '.join(details)

        # Supertrend
        if 'Supertrend 多方' in detail_text:
            signals.append('supertrend_bull')
        elif 'Supertrend 空方' in detail_text:
            signals.append('supertrend_bear')

        # MACD
        if 'MACD 黃金交叉' in detail_text or 'MACD 柱狀體翻正' in detail_text:
            signals.append('macd_golden')
        elif 'MACD 死亡交叉' in detail_text or 'MACD 柱狀體翻負' in detail_text:
            signals.append('macd_dead')

        # RSI
        if 'RSI 底背離' in detail_text:
            signals.append('rsi_bull_div')
        elif 'RSI 頂背離' in detail_text:
            signals.append('rsi_bear_div')

        # Volume
        if '爆量確認' in detail_text:
            signals.append('rvol_high')
        elif '量能萎縮' in detail_text:
            signals.append('rvol_low')

        # Chip (institutional)
        if '法人積極買超' in detail_text or '法人持續買超' in detail_text:
            signals.append('inst_buy')
        elif '法人大量賣超' in detail_text:
            signals.append('inst_sell')

        # ETF sync buy/sell
        if 'ETF 同步買超' in detail_text:
            signals.append('etf_sync_buy')
        elif 'ETF 買超' in detail_text:
            signals.append('etf_buy')
        elif 'ETF 同步賣超' in detail_text:
            signals.append('etf_sync_sell')

        # Squeeze
        if '壓縮' in detail_text and '釋放' in detail_text:
            signals.append('squeeze_fire')

        # Regime
        regime = report.get('regime', {}).get('regime', '')
        if regime:
            signals.append(f'regime_{regime}')

        return signals

    # ================================================================
    # Result Formatting
    # ================================================================

    def _make_result(self, scored, total_scanned, passed_initial, elapsed):
        """Build the final result dict."""
        now = datetime.now()
        market = getattr(self, '_market', 'tw')
        return {
            'scan_date': now.strftime('%Y-%m-%d'),
            'scan_time': now.strftime('%H:%M'),
            'market': market,
            'total_scanned': total_scanned,
            'passed_initial': passed_initial,
            'scored_count': len(scored),
            'elapsed_seconds': round(elapsed, 1),
            'failures': self._failures[:20],
            'config': {
                'momentum_change_min': self.config['momentum_change_min'],
                'top_n': self.config['top_n'],
            },
            'results': scored,
        }

    @staticmethod
    def save_results(result, output_dir='data'):
        """
        Save results to data/latest/ and data/history/.

        Args:
            result: dict from run()
            output_dir: base directory for output
        """
        base = Path(output_dir)
        latest_dir = base / 'latest'
        history_dir = base / 'history'
        latest_dir.mkdir(parents=True, exist_ok=True)
        history_dir.mkdir(parents=True, exist_ok=True)

        # Determine filename suffix based on market
        market = result.get('market', 'tw')
        suffix = '_us' if market == 'us' else ''

        # Latest result (overwritten each run)
        latest_file = latest_dir / f'momentum{suffix}_result.json'
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        # History (appended by date)
        date_str = result.get('scan_date', datetime.now().strftime('%Y-%m-%d'))
        history_file = history_dir / f'{date_str}_momentum{suffix}.json'
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        return str(latest_file), str(history_file)


# ====================================================================== #
#  CLI Entry Point (for testing)
# ====================================================================== #

if __name__ == '__main__':
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    )

    parser = argparse.ArgumentParser(description='Momentum Screener')
    parser.add_argument('--stage1-only', action='store_true',
                        help='Only run Stage 1 (no trigger scores)')
    parser.add_argument('--no-chip', action='store_true',
                        help='Skip chip data fetching (faster)')
    parser.add_argument('--top', type=int, default=50,
                        help='Number of results to return')
    parser.add_argument('--save', action='store_true',
                        help='Save results to data/ directory')
    args = parser.parse_args()

    config = {'top_n': args.top}
    if args.no_chip:
        config['include_chip'] = False

    screener = MomentumScreener(config=config)

    if args.stage1_only:
        df = screener.run_stage1_only()
        print(f"\nStage 1 Results: {len(df)} candidates")
        if not df.empty:
            cols = ['stock_id', 'stock_name', 'market', 'close',
                    'change_pct', 'trading_value', 'tv_pct']
            show_cols = [c for c in cols if c in df.columns]
            print(df[show_cols].head(30).to_string(index=False))
    else:
        result = screener.run()
        print(f"\nResults: {result['scored_count']} stocks scored")
        print(f"Scanned: {result['total_scanned']}, Passed: {result['passed_initial']}")
        print(f"Time: {result['elapsed_seconds']}s")

        if result['results']:
            print(f"\nTop {min(20, len(result['results']))}:")
            for i, r in enumerate(result['results'][:20], 1):
                sigs = ', '.join(r['signals'][:3])
                print(f"  {i:2d}. {r['stock_id']} {r['name'][:6]:6s} "
                      f"${r['price']:>8.1f}  {r['change_pct']:+5.1f}%  "
                      f"Score={r['trigger_score']:+5.1f}  [{sigs}]")

        if args.save:
            paths = MomentumScreener.save_results(result)
            print(f"\nSaved to: {paths[0]}")
