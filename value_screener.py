"""
左側價值選股引擎 — 全市場掃描找出被低估且有轉折跡象的股票

Stage 1: 快速初篩（PE/PB/殖利率 + 流動性）
  - PE > 0 且低於同業或歷史分位
  - 殖利率 > 一定門檻
  - 成交值過濾極低流動性

Stage 2: 完整估值分數（0-100）
  - 估值 30%: PE/PB 歷史分位 + 殖利率
  - 體質 25%: ROE + 三率趨勢 + 連續獲利
  - 營收 15%: 衰退收斂 or 已轉正
  - 技術轉折 15%: 超賣/背離/量能萎縮
  - 聰明錢 15%: 法人累積 + ETF 同步買超
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

_CHECKPOINT_DIR = Path('data/.checkpoints')

logger = logging.getLogger(__name__)

# TradingView batch cache
_tv_batch_cache = {}
_tv_batch_ts = 0


def _fetch_tradingview_batch(market='tw'):
    """
    Batch fetch fundamental data from TradingView for all stocks in a market.
    Returns dict: { stock_id: {gross_margin, operating_margin, net_margin, ROE, ROA, ...} }
    """
    import time
    global _tv_batch_cache, _tv_batch_ts
    cache_key = f"tv_{market}"
    if cache_key in _tv_batch_cache and time.time() - _tv_batch_ts < 3600:
        return _tv_batch_cache[cache_key]

    try:
        from tradingview_screener import Query

        tv_market = 'america' if market == 'us' else 'taiwan'
        result = (Query()
            .select('name', 'gross_margin', 'operating_margin', 'net_margin',
                    'return_on_equity', 'return_on_assets',
                    'total_revenue_yoy_growth_fq', 'debt_to_equity')
            .set_markets(tv_market)
            .limit(5000)
            .get_scanner_data()
        )

        df = result[1]
        batch = {}
        for _, row in df.iterrows():
            sid = str(row.get('name', '')).strip()
            if not sid:
                continue
            data = {}
            for field, key in [('gross_margin', 'gross_margin'),
                               ('operating_margin', 'operating_margin'),
                               ('net_margin', 'net_margin'),
                               ('return_on_equity', 'ROE'),
                               ('return_on_assets', 'ROA'),
                               ('total_revenue_yoy_growth_fq', 'revenue_yoy'),
                               ('debt_to_equity', 'debt_to_equity')]:
                val = row.get(field)
                if val is not None and not (isinstance(val, float) and np.isnan(val)):
                    data[key] = val
            if data:
                batch[sid] = data

        logger.info("TradingView batch: %d stocks for market %s", len(batch), market)
        _tv_batch_cache[cache_key] = batch
        _tv_batch_ts = time.time()
        return batch

    except Exception as e:
        logger.warning("TradingView batch failed for %s: %s", market, e)
        return {}

# ================================================================
# Default Configuration
# ================================================================
DEFAULT_CONFIG = {
    # Stage 1: 初篩門檻（研究來源：Graham/O'Shaughnessy/台股實證）
    'max_pe': 20,               # PE 上限（台股合理區間上緣，歷史均值 ~15）
    'min_pe': 0.1,              # PE 下限（排除虧損股）
    'max_pb': 3.0,              # PB 上限（從 5.0 收緊，Graham 建議 1.5）
    'pe_x_pb_max': 22.5,        # Graham 複合準則：PE × PB < 22.5
    'min_dividend_yield': 0,    # 殖利率下限（0=不篩，保留成長型低估值股）
    'min_trading_value': 3e7,   # 最低成交值 3000 萬 TWD（機構可交易水準）
    # TradingView 批次體質篩（免費，不耗 FinMind API）
    'min_roe': 0,               # ROE 下限 %（0=不篩，建議 3~5 排除爛公司）
    'min_operating_margin': -50, # 營益率下限 %（排除嚴重虧損，-50=幾乎不篩）
    'max_debt_to_equity': 0,    # 負債/權益上限（0=不篩，建議 2.0 排除高槓桿）

    # Stage 2: 精篩設定
    'top_n': 50,                # 輸出前 N 名
    'include_chip': True,       # 是否抓籌碼
    'batch_delay': 0.3,         # 每檔間隔秒數
    'max_failures': 10,

    # 評分權重
    'weight_valuation': 0.30,
    'weight_quality': 0.25,
    'weight_revenue': 0.15,
    'weight_technical': 0.15,
    'weight_smart_money': 0.15,
}


class ValueScreener:
    """左側價值選股引擎"""

    def __init__(self, config=None, progress_callback=None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.progress = progress_callback or (lambda msg: print(msg))
        self._failures = []

    # ================================================================
    # Public API
    # ================================================================

    def run(self, market='tw'):
        """Execute full value screening pipeline."""
        start_time = time.time()
        self._market = market

        # Stage 1
        label = 'US' if market == 'us' else 'TW'
        self.progress(f"Stage 1: Fetching {label} market + fundamental data...")
        if market == 'us':
            candidates = self._stage1_filter_us()
        else:
            candidates = self._stage1_filter()
        self.progress(f"Stage 1 done: {len(candidates)} candidates")

        if candidates.empty:
            return self._make_result([], 0, 0, time.time() - start_time)

        total_market = candidates.attrs.get('total_market', 0)

        # Stage 2
        self.progress(f"Stage 2: Scoring {len(candidates)} candidates...")
        scored = self._stage2_score(candidates)
        self.progress(f"Stage 2 done: {len(scored)} scored, {len(self._failures)} failed")

        elapsed = time.time() - start_time
        self.progress(f"Scan complete in {elapsed:.0f}s")
        return self._make_result(scored, total_market, len(candidates), elapsed)

    def run_stage1_only(self, market='tw'):
        """Only run Stage 1 for quick preview."""
        if market == 'us':
            return self._stage1_filter_us()
        return self._stage1_filter()

    # ================================================================
    # Stage 1: Quick Filter (PE/PB + Liquidity)
    # ================================================================

    def _stage1_filter(self):
        """
        Combine market daily + PE/PB + TradingView fundamentals for initial screening.

        Layer 1 — TWSE/TPEX batch (free):
          1. Liquidity: trading_value >= 3000 萬（機構可交易水準）
          2. PE: min_pe ~ max_pe（有獲利且不過貴）
          3. PB: < max_pb
          4. Graham 複合: PE × PB < 22.5（允許 PE 或 PB 單邊偏高，但乘積必須合理）
          5. Dividend yield >= min（可選）

        Layer 2 — TradingView batch (free, no FinMind cost):
          6. ROE >= min_roe（排除體質極差的公司）
          7. Operating margin >= min（排除嚴重虧損）
          8. Debt/Equity <= max（排除高槓桿）
        """
        from twse_api import TWSEOpenData
        api = TWSEOpenData()
        cfg = self.config

        # ---- Layer 1: TWSE/TPEX batch data ----
        market_df = api.get_market_daily_all()
        if market_df.empty:
            return pd.DataFrame()

        total_market = len(market_df)

        pe_df = api.get_pe_dividend_all_combined()

        if pe_df.empty:
            logger.warning("No PE data available, using market data only")
            result = market_df[market_df['trading_value'] >= cfg['min_trading_value']].copy()
            result.attrs['total_market'] = total_market
            return result

        merged = market_df.merge(
            pe_df[['stock_id', 'PE', 'dividend_yield', 'PB']],
            on='stock_id',
            how='left',
        )

        mask = pd.Series(True, index=merged.index)

        # 1. Liquidity
        mask &= merged['trading_value'] >= cfg['min_trading_value']

        # 2. PE filter
        has_pe = merged['PE'].notna() & (merged['PE'] > 0)
        mask &= has_pe
        mask &= merged['PE'] >= cfg['min_pe']
        mask &= merged['PE'] <= cfg['max_pe']

        # 3. PB filter
        if cfg['max_pb'] > 0:
            has_pb = merged['PB'].notna() & (merged['PB'] > 0)
            mask &= (has_pb & (merged['PB'] <= cfg['max_pb'])) | ~has_pb

        # 4. Graham compound: PE × PB < 22.5
        pe_x_pb_max = cfg.get('pe_x_pb_max', 0)
        if pe_x_pb_max > 0:
            has_both = has_pe & merged['PB'].notna() & (merged['PB'] > 0)
            pe_x_pb = merged['PE'] * merged['PB']
            # 有 PE 和 PB 的必須通過複合條件；只有 PE 沒 PB 的放行
            mask &= (has_both & (pe_x_pb <= pe_x_pb_max)) | ~has_both

        # 5. Dividend yield filter
        if cfg['min_dividend_yield'] > 0:
            mask &= merged['dividend_yield'] >= cfg['min_dividend_yield']

        layer1 = merged[mask].copy()
        self.progress(f"  Layer 1 (TWSE/TPEX): {total_market} -> {len(layer1)} stocks")

        if layer1.empty:
            layer1.attrs['total_market'] = total_market
            return layer1

        # ---- Layer 2: TradingView batch fundamentals (free) ----
        tv_batch = _fetch_tradingview_batch('tw')
        min_roe = cfg.get('min_roe', 0)
        min_om = cfg.get('min_operating_margin', -50)
        max_de = cfg.get('max_debt_to_equity', 0)

        if tv_batch and (min_roe > 0 or min_om > -50 or max_de > 0):
            drop_ids = set()
            for _, row in layer1.iterrows():
                sid = row['stock_id']
                tv = tv_batch.get(sid)
                if not tv:
                    continue  # 沒 TradingView 資料的放行（不懲罰資料缺失）

                # ROE filter
                if min_roe > 0:
                    roe = tv.get('ROE')
                    if roe is not None and roe < min_roe:
                        drop_ids.add(sid)
                        continue

                # Operating margin filter
                if min_om > -50:
                    om = tv.get('operating_margin')
                    if om is not None and om < min_om:
                        drop_ids.add(sid)
                        continue

                # Debt/Equity filter
                if max_de > 0:
                    de = tv.get('debt_to_equity')
                    if de is not None and de > max_de:
                        drop_ids.add(sid)
                        continue

            if drop_ids:
                layer1 = layer1[~layer1['stock_id'].isin(drop_ids)]
                self.progress(f"  Layer 2 (TradingView): removed {len(drop_ids)}, remaining {len(layer1)}")

        result = layer1
        result.sort_values('PE', ascending=True, inplace=True)
        result.attrs['total_market'] = total_market
        return result

    # ================================================================
    # Stage 1 US: yfinance batch for S&P 500 fundamentals
    # ================================================================

    def _stage1_filter_us(self):
        """
        Fetch S&P 500 with basic fundamental filter via yfinance batch.
        Filter: PE > 0, PE < max_pe, volume > threshold.
        """
        import yfinance as yf
        from momentum_screener import MomentumScreener
        cfg = self.config

        # Reuse S&P 500 list from momentum screener
        tickers = MomentumScreener._fetch_sp500()
        if not tickers:
            return pd.DataFrame()

        self.progress(f"  Downloading {len(tickers)} US tickers (2-day data)...")
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
                    close_s = data[('Close', ticker)].dropna()
                    vol_s = data[('Volume', ticker)].dropna()
                else:
                    close_s = data['Close'].dropna()
                    vol_s = data['Volume'].dropna()

                if len(close_s) < 1:
                    continue

                close = float(close_s.iloc[-1])
                volume = float(vol_s.iloc[-1])
                change_pct = 0
                if len(close_s) >= 2:
                    prev = float(close_s.iloc[-2])
                    change_pct = (close - prev) / prev * 100 if prev > 0 else 0

                if close < cfg.get('us_min_price', 5) or volume < cfg.get('us_min_volume', 500_000):
                    continue

                results.append({
                    'stock_id': ticker,
                    'stock_name': ticker,
                    'market': 'us',
                    'close': close,
                    'change_pct': round(change_pct, 2),
                    'volume': int(volume),
                    'trading_value': int(close * volume),
                    # PE/PB will be fetched per-stock in Stage 2 via finviz
                    'PE': 0, 'PB': 0, 'dividend_yield': 0,
                })
            except Exception:
                continue

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df.attrs['total_market'] = len(tickers)
        self.progress(f"  US Stage 1: {len(df)} stocks passed liquidity filter")
        return df

    # ================================================================
    # Stage 2: Full Value Scoring (0-100)
    # ================================================================

    def _stage2_score(self, candidates):
        """Score each candidate on 5 dimensions. Supports checkpoint/resume."""
        market = getattr(self, '_market', 'tw')
        cp_file = _CHECKPOINT_DIR / f'value_{market}.json'

        # Pre-fetch batch institutional data (TWSE/TPEX) for smart money scoring
        self._inst_batch = {}
        if market == 'tw' and self.config.get('include_chip', True):
            try:
                from twse_api import TWSEOpenData
                twse = TWSEOpenData()
                self._inst_batch = twse.get_institutional_batch(days=5)
                self.progress(f"  Pre-fetched institutional data: {len(self._inst_batch)} stocks")
            except Exception as e:
                logger.warning("Batch institutional fetch failed: %s", e)

        # Pre-fetch TradingView fundamental data (三率/ROE/ROA) for quality scoring
        self._tv_batch = {}
        try:
            self._tv_batch = _fetch_tradingview_batch(market)
            self.progress(f"  Pre-fetched TradingView fundamentals: {len(self._tv_batch)} stocks")
        except Exception as e:
            logger.warning("TradingView batch fetch failed: %s", e)

        # Load checkpoint
        scored, done_ids = self._load_checkpoint(cp_file)
        if scored:
            self.progress(f"  Resuming: {len(scored)} stocks already scored, {len(done_ids)} processed")

        total = len(candidates)
        consecutive_fails = 0

        for idx, row in candidates.iterrows():
            sid = row['stock_id']
            if sid in done_ids:
                continue

            sname = row.get('stock_name', '')
            pos = len(scored) + len(self._failures) + 1

            if pos % 10 == 0 or pos <= 3:
                self.progress(f"  [{pos}/{total}] {sid} {sname}")

            try:
                result = self._score_single(sid, row)
                if result:
                    scored.append(result)
                    consecutive_fails = 0
                else:
                    self._failures.append(sid)
                    consecutive_fails += 1
            except Exception as e:
                err_str = str(type(e).__name__)
                if 'RateLimit' in err_str or '429' in str(e):
                    self.progress(f"  [Rate Limit] Pausing 60s then retrying {sid}...")
                    time.sleep(60)
                    try:
                        result = self._score_single(sid, row)
                        if result:
                            scored.append(result)
                            consecutive_fails = 0
                        else:
                            self._failures.append(sid)
                    except Exception:
                        self._failures.append(sid)
                    continue
                logger.warning("Failed to score %s: %s", sid, e)
                self._failures.append(sid)
                consecutive_fails += 1

            done_ids.add(sid)
            self._save_checkpoint(cp_file, scored, done_ids)

            if consecutive_fails >= self.config['max_failures']:
                self.progress(f"  Stopping: {consecutive_fails} consecutive failures")
                break

            if self.config['batch_delay'] > 0:
                time.sleep(self.config['batch_delay'])

        self._clear_checkpoint(cp_file)
        scored.sort(key=lambda x: x['value_score'], reverse=True)
        return scored[:self.config['top_n']]

    # ================================================================
    # Checkpoint helpers
    # ================================================================

    @staticmethod
    def _load_checkpoint(cp_file):
        if cp_file.exists():
            try:
                with open(cp_file, 'r', encoding='utf-8') as f:
                    cp = json.load(f)
                return cp.get('scored', []), set(cp.get('done_ids', []))
            except Exception:
                pass
        return [], set()

    @staticmethod
    def _save_checkpoint(cp_file, scored, done_ids):
        if len(done_ids) % 5 != 0:
            return
        try:
            _CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
            with open(cp_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'scored': scored,
                    'done_ids': list(done_ids),
                    'timestamp': datetime.now().isoformat(),
                }, f, ensure_ascii=False)
        except Exception:
            pass

    @staticmethod
    def _clear_checkpoint(cp_file):
        try:
            if cp_file.exists():
                cp_file.unlink()
        except Exception:
            pass

    def _score_single(self, stock_id, market_row):
        """Score a single stock on all 5 dimensions."""
        cfg = self.config
        is_us = market_row.get('market') == 'us'
        scores = {}
        details = []

        # Pre-load price data ONCE (reused by quality/technical/trading value)
        _price_df = None
        _latest_close = 0
        avg_tv_5d = 0
        if not is_us:
            try:
                from technical_analysis import load_and_resample
                _, _price_df, _, _ = load_and_resample(stock_id)
                if _price_df is not None and not _price_df.empty:
                    if 'Close' in _price_df.columns:
                        _latest_close = float(_price_df['Close'].iloc[-1])
                    if 'Close' in _price_df.columns and 'Volume' in _price_df.columns:
                        tv = (_price_df['Close'] * _price_df['Volume']).tail(5)
                        avg_tv_5d = int(tv.mean()) if len(tv) > 0 else 0
            except Exception:
                pass

        # For US stocks: fetch finviz data to populate PE/PB/PEG/dividend
        finviz = None
        if is_us:
            try:
                from finviz_data import FinvizAnalyzer
                fv = FinvizAnalyzer()
                finviz, _ = fv.get_stock_data(stock_id)
                if finviz:
                    v = finviz.get('valuation', {})
                    market_row = dict(market_row)  # make mutable copy
                    market_row['PE'] = v.get('pe') or 0
                    market_row['PB'] = v.get('pb') or 0
                    market_row['dividend_yield'] = v.get('dividend_yield') or 0
                    market_row['_peg'] = v.get('peg') or 0
                    market_row['_forward_pe'] = v.get('forward_pe') or 0
                    market_row['_eps_growth'] = v.get('eps_growth_next_5y') or 0
                    market_row['_sales_qq'] = v.get('sales_growth_qq') or 0
                    market_row['_eps_qq'] = v.get('eps_growth_qq') or 0
                    a = finviz.get('analyst', {})
                    market_row['_target_upside'] = a.get('upside_pct') or 0
            except Exception:
                pass

        # --- 1. Valuation Score (0-100) ---
        scores['valuation'] = self._score_valuation(stock_id, market_row, details)

        # --- 2. Quality Score (0-100) ---
        if is_us:
            scores['quality'] = self._score_quality_us(stock_id, finviz, details)
        else:
            scores['quality'] = self._score_quality(stock_id, details, price=_latest_close)

        # --- 3. Revenue Trend Score (0-100) ---
        if is_us:
            scores['revenue'] = self._score_revenue_us(stock_id, market_row, details)
        else:
            scores['revenue'] = self._score_revenue(stock_id, details)

        # --- 4. Technical Reversal Score (0-100) ---
        scores['technical'] = self._score_technical(stock_id, details, price_df=_price_df)

        # --- 5. Smart Money Score (0-100) ---
        scores['smart_money'] = self._score_smart_money_us(stock_id, finviz, details) if is_us else self._score_smart_money(stock_id, details)

        # Weighted total
        total = (
            scores['valuation'] * cfg['weight_valuation'] +
            scores['quality'] * cfg['weight_quality'] +
            scores['revenue'] * cfg['weight_revenue'] +
            scores['technical'] * cfg['weight_technical'] +
            scores['smart_money'] * cfg['weight_smart_money']
        )

        # US: load price for avg trading value (TW already pre-loaded above)
        if is_us and avg_tv_5d == 0:
            try:
                from technical_analysis import load_and_resample
                _, _df, _, _ = load_and_resample(stock_id)
                if _df is not None and not _df.empty and 'Close' in _df.columns and 'Volume' in _df.columns:
                    tv = (_df['Close'] * _df['Volume']).tail(5)
                    avg_tv_5d = int(tv.mean()) if len(tv) > 0 else 0
            except Exception:
                pass

        return {
            'stock_id': stock_id,
            'name': market_row.get('stock_name', ''),
            'market': market_row.get('market', 'twse'),
            'price': market_row.get('close', 0),
            'change_pct': round(market_row.get('change_pct', 0), 2),
            'trading_value': int(market_row.get('trading_value', 0)),
            'avg_trading_value_5d': avg_tv_5d,
            'PE': market_row.get('PE', 0),
            'PB': market_row.get('PB', 0),
            'dividend_yield': market_row.get('dividend_yield', 0),
            'value_score': round(total, 1),
            'scores': {k: round(v, 1) for k, v in scores.items()},
            'details': details,
        }

    # ================================================================
    # Scoring Dimensions
    # ================================================================

    def _score_valuation(self, stock_id, row, details):
        """
        估值分數: PE/PB 歷史分位越低越高分 + 殖利率加分
        """
        score = 50  # Neutral baseline

        pe = row.get('PE', 0)
        pb = row.get('PB', 0)
        dy = row.get('dividend_yield', 0)

        # PE score: lower is better (within range)
        if pe > 0:
            if pe < 8:
                score += 25
                details.append(f"PE={pe:.1f} 極低 (+25)")
            elif pe < 12:
                score += 15
                details.append(f"PE={pe:.1f} 偏低 (+15)")
            elif pe < 16:
                score += 5
                details.append(f"PE={pe:.1f} 合理 (+5)")
            elif pe > 25:
                score -= 15
                details.append(f"PE={pe:.1f} 偏高 (-15)")

        # PB score
        if pb > 0:
            if pb < 1.0:
                score += 15
                details.append(f"PB={pb:.2f} 破淨 (+15)")
            elif pb < 1.5:
                score += 8
                details.append(f"PB={pb:.2f} 偏低 (+8)")
            elif pb > 3.0:
                score -= 5
                details.append(f"PB={pb:.2f} 偏高 (-5)")

        # Dividend yield bonus
        if dy > 0:
            if dy > 6:
                score += 10
                details.append(f"殖利率 {dy:.1f}% 高 (+10)")
            elif dy > 4:
                score += 5
                details.append(f"殖利率 {dy:.1f}% (+5)")

        is_us = row.get('market') == 'us'

        # Historical PE percentile (Taiwan only — FinMind data)
        if not is_us:
            try:
                from fundamental_analysis import get_per_history
                per_hist = get_per_history(stock_id, days=1200)
                if per_hist is not None and not per_hist.empty and 'PEratio' in per_hist.columns:
                    hist_pe = per_hist['PEratio'].dropna()
                    hist_pe = hist_pe[hist_pe > 0]
                    if len(hist_pe) > 50 and pe > 0:
                        percentile = (hist_pe < pe).mean() * 100
                        if percentile < 20:
                            score += 15
                            details.append(f"PE 歷史分位 {percentile:.0f}% (近5年最低20%) (+15)")
                        elif percentile < 40:
                            score += 8
                            details.append(f"PE 歷史分位 {percentile:.0f}% (+8)")
                        elif percentile > 80:
                            score -= 10
                            details.append(f"PE 歷史分位 {percentile:.0f}% (偏高) (-10)")
            except Exception:
                pass

        # PEG: PE / EPS growth rate (Taiwan: revenue YoY, US: finviz data)
        if not is_us:
            try:
                from dividend_revenue import RevenueTracker
                rt = RevenueTracker()
                rev_df = rt.get_monthly_revenue(stock_id, months=24)
                if rev_df is not None and not rev_df.empty and 'yoy_pct' in rev_df.columns:
                    yoy = rev_df['yoy_pct'].dropna()
                    if len(yoy) >= 6 and pe > 0:
                        avg_growth = yoy.iloc[-6:].mean()
                        if avg_growth > 1:
                            peg = pe / avg_growth
                            if peg < 0.5:
                                score += 12
                                details.append(f"PEG={peg:.2f} 極低 (PE={pe:.1f}/Growth={avg_growth:.1f}%) (+12)")
                            elif peg < 1.0:
                                score += 8
                                details.append(f"PEG={peg:.2f} 被低估 (Growth={avg_growth:.1f}%) (+8)")
                            elif peg > 3.0:
                                score -= 5
                                details.append(f"PEG={peg:.2f} 偏高 (-5)")
            except Exception:
                pass

        # DDM: Dividend Discount Model (for stable dividend payers)
        try:
            price = row.get('close', 0)
            if dy > 2 and price > 0:
                cash_div = dy * price / 100
                discount_rate = 0.10
                growth_rate = 0.02
                if not is_us:
                    try:
                        from dividend_revenue import RevenueTracker
                        _rt = RevenueTracker()
                        _alert = _rt.get_revenue_alert(stock_id)
                        if _alert and _alert.get('last_yoy_pct') is not None:
                            g_raw = _alert['last_yoy_pct'] / 100
                            growth_rate = max(0.0, min(0.05, g_raw))
                    except Exception:
                        pass
                else:
                    # US: use finviz EPS growth as proxy
                    eps_g = row.get('_eps_growth', 0)
                    if eps_g and eps_g > 0:
                        growth_rate = max(0.0, min(0.05, eps_g / 100))

                if discount_rate > growth_rate:
                    fair_price = cash_div / (discount_rate - growth_rate)
                    discount_pct = (fair_price - price) / price * 100
                    if discount_pct > 30:
                        score += 10
                        details.append(f"DDM fair={fair_price:.0f} (discount {discount_pct:.0f}%) (+10)")
                    elif discount_pct > 10:
                        score += 5
                        details.append(f"DDM fair={fair_price:.0f} (discount {discount_pct:.0f}%) (+5)")
                    elif discount_pct < -30:
                        score -= 8
                        details.append(f"DDM fair={fair_price:.0f} (premium {abs(discount_pct):.0f}%) (-8)")
        except Exception:
            pass

        # US: Forward PE discount (forward PE < trailing PE = earnings growing)
        if is_us:
            fwd_pe = row.get('_forward_pe', 0)
            if fwd_pe and pe and fwd_pe > 0 and pe > 0:
                pe_discount = (pe - fwd_pe) / pe * 100
                if pe_discount > 20:
                    score += 10
                    details.append(f"Forward PE={fwd_pe:.1f} vs PE={pe:.1f} ({pe_discount:.0f}% cheaper) (+10)")
                elif pe_discount > 10:
                    score += 5
                    details.append(f"Forward PE={fwd_pe:.1f} vs PE={pe:.1f} ({pe_discount:.0f}% cheaper) (+5)")

        # US: Finviz PEG (already calculated) + analyst target upside
        peg_fv = row.get('_peg', 0)
        if peg_fv and peg_fv > 0:
            if peg_fv < 0.5:
                score += 12
                details.append(f"Finviz PEG={peg_fv:.2f} very low (+12)")
            elif peg_fv < 1.0:
                score += 8
                details.append(f"Finviz PEG={peg_fv:.2f} undervalued (+8)")
            elif peg_fv > 3.0:
                score -= 5
                details.append(f"Finviz PEG={peg_fv:.2f} high (-5)")

        target_upside = row.get('_target_upside', 0)
        if target_upside and target_upside > 0:
            if target_upside > 30:
                score += 10
                details.append(f"Analyst target +{target_upside:.0f}% upside (+10)")
            elif target_upside > 15:
                score += 5
                details.append(f"Analyst target +{target_upside:.0f}% upside (+5)")

        return max(0, min(100, score))

    def _score_quality(self, stock_id, details, price=0):
        """
        體質分數: Piotroski F-Score + Altman Z-Score + ROE + 三率 + ROIC/FCF

        Uses calculate_all() for single-fetch optimization (3 API calls instead of 9).
        Args:
            price: latest close price (pre-loaded from _score_single to avoid extra API call)
        """
        score = 50
        mcap = price * 1e8 if price > 0 else 0  # Rough market cap placeholder

        # --- Combined: F-Score + Z-Score + ROIC/FCF (single FinMind fetch) ---
        all_result = None
        try:
            from piotroski import calculate_all
            all_result = calculate_all(stock_id, market_cap=mcap)
        except Exception:
            pass

        # --- F-Score ---
        if all_result and all_result.get('fscore'):
            fs_result = all_result['fscore']
            fscore = fs_result['fscore']
            comp = fs_result['components']
            if fscore >= 7:
                score += 25
                details.append(f"F-Score={fscore}/9 強 (獲利{comp['profitability']}/槓桿{comp['leverage']}/效率{comp['efficiency']}) (+25)")
            elif fscore >= 5:
                score += 10
                details.append(f"F-Score={fscore}/9 中等 (+10)")
            elif fscore <= 3:
                score -= 20
                details.append(f"F-Score={fscore}/9 弱 (價值陷阱風險) (-20)")
            else:
                details.append(f"F-Score={fscore}/9 (+0)")

            cr = fs_result['data'].get('current_ratio', 0)
            if cr > 0:
                if cr > 2.0:
                    score += 5
                    details.append(f"流動比率={cr:.1f} 安全 (+5)")
                elif cr < 1.0:
                    score -= 8
                    details.append(f"流動比率={cr:.1f} 偏低 (-8)")

        # --- Z-Score ---
        if all_result and all_result.get('zscore'):
            z_result = all_result['zscore']
            z = z_result['zscore']
            zone = z_result['zone']
            if zone == 'distress':
                score -= 20
                details.append(f"Z-Score={z:.1f} 危險區 (破產風險) (-20)")
            elif zone == 'safe':
                score += 8
                details.append(f"Z-Score={z:.1f} 安全區 (+8)")
            else:
                details.append(f"Z-Score={z:.1f} 灰色區 [資訊]")

        # --- ROIC / FCF ---
        if all_result and all_result.get('extra'):
            extras = all_result['extra']
            roic = extras.get('roic', 0)
            if roic and roic > 15:
                score += 8
                details.append(f"ROIC={roic:.1f}% 優 (+8)")
            elif roic and roic < 0:
                score -= 5
                details.append(f"ROIC={roic:.1f}% 虧損 (-5)")

            fcf_y = extras.get('fcf_yield', 0)
            if fcf_y and fcf_y > 8:
                score += 8
                details.append(f"FCF Yield={fcf_y:.1f}% 高 (+8)")
            elif fcf_y and fcf_y < -5:
                score -= 5
                details.append(f"FCF Yield={fcf_y:.1f}% 負 (-5)")

        # --- ROE + EPS from calculate_all's raw income data (no extra API call) ---
        _has_roe_eps = False
        if all_result and all_result.get('income') and all_result.get('balance'):
            try:
                income = all_result['income']
                balance = all_result['balance']
                periods = sorted(income.keys())
                if len(periods) >= 4:
                    # ROE = net_income / equity
                    curr_p = periods[-1]
                    if curr_p in balance:
                        equity = balance[curr_p].get('equity', 0)
                        net_inc = income[curr_p].get('net_income', 0)
                        if equity > 0:
                            roe = net_inc / equity * 100
                            if roe > 15:
                                score += 5
                                details.append(f"ROE={roe:.1f}% (+5)")
                            elif roe < 0:
                                score -= 10
                                details.append(f"ROE={roe:.1f}% 虧損 (-10)")

                    # EPS: check last 4 quarters
                    eps_vals = [income[p].get('eps', 0) for p in periods[-4:] if 'eps' in income[p]]
                    if len(eps_vals) >= 4:
                        profitable_q = sum(1 for e in eps_vals if e > 0)
                        if profitable_q == 4:
                            score += 5
                            details.append("連續 4 季獲利 (+5)")
                        elif profitable_q <= 1:
                            score -= 10
                            details.append(f"近 4 季僅 {profitable_q} 季獲利 (-10)")
                    _has_roe_eps = True
            except Exception:
                pass

        # --- Fallback: fetch ROE/EPS from FinMind only if calculate_all had no data ---
        if not _has_roe_eps:
            try:
                from fundamental_analysis import get_financial_statements
                fs = get_financial_statements(stock_id, quarters=12)

                if fs is not None and not fs.empty and len(fs) >= 4:
                    if 'ROE' in fs.columns:
                        recent_roe = fs['ROE'].iloc[-1]
                        if pd.notna(recent_roe):
                            if recent_roe > 15:
                                score += 5
                                details.append(f"ROE={recent_roe:.1f}% (+5)")
                            elif recent_roe < 0:
                                score -= 10
                                details.append(f"ROE={recent_roe:.1f}% 虧損 (-10)")

                    if 'EPS' in fs.columns:
                        eps = fs['EPS'].dropna()
                        if len(eps) >= 4:
                            profitable_q = (eps.iloc[-4:] > 0).sum()
                            if profitable_q == 4:
                                score += 5
                                details.append("連續 4 季獲利 (+5)")
                            elif profitable_q <= 1:
                                score -= 10
                                details.append(f"近 4 季僅 {profitable_q} 季獲利 (-10)")
            except Exception as e:
                logger.debug("Quality scoring failed for %s: %s", stock_id, e)

        # --- TradingView 三率/ROE 補充 (batch pre-fetched) ---
        tv = getattr(self, '_tv_batch', {}).get(stock_id, {})
        if tv:
            gm = tv.get('gross_margin')
            om = tv.get('operating_margin')
            roe = tv.get('ROE')
            de = tv.get('debt_to_equity')

            if gm is not None:
                if gm > 40:
                    score += 5
                    details.append(f"毛利率={gm:.1f}% 高 (+5) [TV]")
                elif gm < 10:
                    score -= 5
                    details.append(f"毛利率={gm:.1f}% 偏低 (-5) [TV]")

            if om is not None:
                if om > 20:
                    score += 5
                    details.append(f"營益率={om:.1f}% 優 (+5) [TV]")
                elif om < 0:
                    score -= 8
                    details.append(f"營益率={om:.1f}% 虧損 (-8) [TV]")

            if roe is not None and roe > 20:
                score += 5
                details.append(f"ROE={roe:.1f}% 高 (+5) [TV]")

            if de is not None and de > 2.0:
                score -= 5
                details.append(f"負債/權益={de:.2f} 偏高 (-5) [TV]")

        return max(0, min(100, score))

    def _score_revenue(self, stock_id, details):
        """
        營收趨勢分數: 衰退收斂=加分, 持續衰退=扣分, 轉正=不算左側
        """
        score = 50

        try:
            from dividend_revenue import RevenueTracker
            rt = RevenueTracker()
            rev_df = rt.get_monthly_revenue(stock_id, months=12)

            if rev_df is not None and not rev_df.empty and 'yoy_pct' in rev_df.columns:
                yoy = rev_df['yoy_pct'].dropna()
                if len(yoy) >= 3:
                    latest_yoy = yoy.iloc[-1]
                    prev_yoy = yoy.iloc[-3]

                    if latest_yoy > 0:
                        # Already positive — more right-side than left-side
                        score += 10
                        details.append(f"營收 YoY 已轉正 {latest_yoy:+.1f}% (+10)")
                    elif abs(latest_yoy - prev_yoy) < 0.5:
                        # Flat (e.g. 0→0 or -1→-1) — no signal
                        pass
                    elif latest_yoy > prev_yoy:
                        # Declining but converging — bottom signal
                        improvement = latest_yoy - prev_yoy
                        bonus = min(20, improvement * 2)
                        score += bonus
                        details.append(f"營收衰退收斂 {prev_yoy:.1f}→{latest_yoy:.1f}% (+{bonus:.0f})")
                    else:
                        # Accelerating decline
                        penalty = min(20, abs(latest_yoy - prev_yoy) * 2)
                        score -= penalty
                        details.append(f"營收加速衰退 {prev_yoy:.1f}→{latest_yoy:.1f}% (-{penalty:.0f})")

            # Revenue surprise
            surprise = rt.detect_revenue_surprise(stock_id)
            if surprise and surprise.get('is_surprise'):
                if surprise['direction'] == 'positive':
                    score += 12
                    details.append(f"營收正驚喜 +{surprise.get('magnitude', 0):.1f}% (+12)")
                elif surprise['direction'] == 'negative':
                    score -= 8
                    details.append(f"營收負驚喜 {surprise.get('magnitude', 0):.1f}% (-8)")

        except Exception as e:
            logger.debug("Revenue scoring failed for %s: %s", stock_id, e)

        return max(0, min(100, score))

    def _score_revenue_us(self, stock_id, market_row, details):
        """US revenue trend score using finviz Q/Q growth + yfinance quarterly data."""
        score = 50

        # 1) Finviz Sales Q/Q and EPS Q/Q (quick, no extra API call)
        sales_qq = market_row.get('_sales_qq', 0)
        eps_qq = market_row.get('_eps_qq', 0)

        if sales_qq:
            if sales_qq > 20:
                score += 15
                details.append(f"Sales Q/Q={sales_qq:+.1f}% strong (+15)")
            elif sales_qq > 5:
                score += 8
                details.append(f"Sales Q/Q={sales_qq:+.1f}% growing (+8)")
            elif sales_qq < -10:
                score -= 12
                details.append(f"Sales Q/Q={sales_qq:+.1f}% declining (-12)")
            elif sales_qq < 0:
                score -= 5
                details.append(f"Sales Q/Q={sales_qq:+.1f}% slightly down (-5)")

        if eps_qq:
            if eps_qq > 25:
                score += 10
                details.append(f"EPS Q/Q={eps_qq:+.1f}% strong (+10)")
            elif eps_qq > 10:
                score += 5
            elif eps_qq < -20:
                score -= 10
                details.append(f"EPS Q/Q={eps_qq:+.1f}% weak (-10)")

        # 2) yfinance quarterly revenue YoY trend (deeper analysis)
        try:
            import yfinance as yf
            stock = yf.Ticker(stock_id)
            inc = stock.quarterly_income_stmt
            if inc is not None and not inc.empty and len(inc.columns) >= 5:
                # Compare recent vs year-ago quarters
                rev_series = inc.loc['Total Revenue'] if 'Total Revenue' in inc.index else None
                if rev_series is not None:
                    rev_vals = rev_series.dropna().sort_index()
                    if len(rev_vals) >= 5:
                        # YoY: compare Q0 vs Q4 (4 quarters apart)
                        recent = float(rev_vals.iloc[-1])
                        year_ago = float(rev_vals.iloc[-5])
                        if year_ago > 0:
                            yoy_pct = (recent - year_ago) / year_ago * 100
                            prev_recent = float(rev_vals.iloc[-2])
                            prev_year_ago = float(rev_vals.iloc[-6]) if len(rev_vals) >= 6 else year_ago
                            prev_yoy = (prev_recent - prev_year_ago) / prev_year_ago * 100 if prev_year_ago > 0 else 0

                            # Convergence signal (value pattern)
                            if yoy_pct < 0 and yoy_pct > prev_yoy:
                                improvement = yoy_pct - prev_yoy
                                bonus = min(15, improvement * 1.5)
                                score += bonus
                                details.append(f"Revenue decline converging {prev_yoy:.0f}%->{yoy_pct:.0f}% (+{bonus:.0f})")
                            elif yoy_pct > 0 and prev_yoy < 0:
                                score += 12
                                details.append(f"Revenue turned positive YoY={yoy_pct:.0f}% (+12)")
        except Exception:
            pass

        return max(0, min(100, score))

    def _score_technical(self, stock_id, details, price_df=None):
        """
        技術面轉折分數: RSI 超賣 + 量能萎縮 + Squeeze 壓縮

        Args:
            price_df: pre-loaded daily price DataFrame (from _score_single) to avoid duplicate fetch
        """
        score = 50

        try:
            from technical_analysis import load_and_resample, calculate_all_indicators

            if price_df is not None and not price_df.empty:
                df_day = price_df
            else:
                _, df_day, _, _ = load_and_resample(stock_id)
            if df_day.empty or len(df_day) < 60:
                return score

            df_day = calculate_all_indicators(df_day)
            current = df_day.iloc[-1]

            # RSI oversold
            rsi = current.get('RSI', 50)
            if pd.notna(rsi):
                if rsi < 30:
                    score += 20
                    details.append(f"RSI={rsi:.0f} 超賣 (+20)")
                elif rsi < 40:
                    score += 10
                    details.append(f"RSI={rsi:.0f} 偏低 (+10)")
                elif rsi > 70:
                    score -= 10
                    details.append(f"RSI={rsi:.0f} 偏高 (-10)")

            # Volume dry-up (RVOL < 0.5 = selling exhaustion)
            rvol = current.get('RVOL', 1.0)
            if pd.notna(rvol):
                if rvol < 0.5:
                    score += 15
                    details.append(f"量能萎縮 RVOL={rvol:.2f} (賣壓枯竭) (+15)")
                elif rvol < 0.7:
                    score += 8
                    details.append(f"量能偏低 RVOL={rvol:.2f} (+8)")

            # Squeeze compression (Bollinger inside Keltner)
            squeeze = current.get('Squeeze_On', False)
            if squeeze:
                score += 12
                details.append("布林壓縮 Squeeze (+12)")

            # Price near 52-week low
            if len(df_day) >= 252:
                low_52w = df_day['Low'].iloc[-252:].min()
                close = current.get('Close', 0)
                if close > 0 and low_52w > 0:
                    from_low_pct = (close - low_52w) / low_52w * 100
                    if from_low_pct < 10:
                        score += 15
                        details.append(f"距 52 週低點 {from_low_pct:.1f}% (+15)")
                    elif from_low_pct < 20:
                        score += 8
                        details.append(f"距 52 週低點 {from_low_pct:.1f}% (+8)")

        except Exception as e:
            logger.debug("Technical scoring failed for %s: %s", stock_id, e)

        return max(0, min(100, score))

    def _score_smart_money(self, stock_id, details):
        """
        聰明錢分數: 法人買超 + ETF 同步買超
        """
        score = 50

        # ETF sync buy
        try:
            from etf_signal import ETFSignal
            etf = ETFSignal()
            sig = etf.get_stock_signal(stock_id, days=5)
            if sig:
                buy = sig['buy_count']
                sell = sig['sell_count']
                if buy >= 3 and buy > sell:
                    score += 20
                    details.append(f"ETF 同步買超 {buy} 檔 (+20)")
                elif buy >= 2 and buy > sell:
                    score += 12
                    details.append(f"ETF 買超 {buy} 檔 (+12)")
                elif sell >= 3 and sell > buy:
                    score -= 15
                    details.append(f"ETF 同步賣超 {sell} 檔 (-15)")
        except Exception:
            pass

        # Institutional accumulation
        if self.config.get('include_chip', True) and stock_id.isdigit():
            inst = None

            # 1st: use pre-fetched TWSE/TPEX batch data (fast, no extra API calls)
            batch = getattr(self, '_inst_batch', {})
            if stock_id in batch:
                inst = batch[stock_id]

            # 2nd: fallback to FinMind via ChipAnalyzer
            if inst is None or inst.empty:
                try:
                    from chip_analysis import ChipAnalyzer
                    ca = ChipAnalyzer()
                    chip_data, _ = ca.get_chip_data(stock_id)
                    if chip_data and 'institutional' in chip_data:
                        inst = chip_data['institutional']
                except Exception:
                    pass

            if inst is not None and not inst.empty and len(inst) >= 5:
                # Find total column (TWSE/TPEX: '合計', FinMind: '三大法人合計')
                total_col = None
                for col in ['合計', '三大法人合計']:
                    if col in inst.columns:
                        total_col = col
                        break
                if total_col:
                    recent_net = inst[total_col].iloc[-5:].sum()
                    if recent_net > 0:
                        score += 10
                        details.append(f"法人近 5 日淨買 {recent_net:+,.0f} (+10)")
                    elif recent_net < -1000:
                        score -= 10
                        details.append(f"法人近 5 日淨賣 {recent_net:+,.0f} (-10)")

        return max(0, min(100, score))

    def _score_quality_us(self, stock_id, finviz, details):
        """US quality score: F-Score + Z-Score + finviz fundamentals."""
        score = 50

        # --- Piotroski F-Score (primary quality signal) ---
        try:
            from piotroski import calculate_fscore_us
            fs_result = calculate_fscore_us(stock_id)
            if fs_result:
                fscore = fs_result['fscore']
                comp = fs_result['components']
                if fscore >= 7:
                    score += 25
                    details.append(f"F-Score={fscore}/9 strong (P{comp['profitability']}/L{comp['leverage']}/E{comp['efficiency']}) (+25)")
                elif fscore >= 5:
                    score += 10
                    details.append(f"F-Score={fscore}/9 average (+10)")
                elif fscore <= 3:
                    score -= 20
                    details.append(f"F-Score={fscore}/9 weak (value trap risk) (-20)")
                else:
                    details.append(f"F-Score={fscore}/9 (+0)")

                cr = fs_result['data'].get('current_ratio', 0)
                if cr > 0:
                    if cr > 2.0:
                        score += 5
                        details.append(f"Current Ratio={cr:.1f} safe (+5)")
                    elif cr < 1.0:
                        score -= 8
                        details.append(f"Current Ratio={cr:.1f} low (-8)")
        except Exception as e:
            logger.debug("US F-Score failed for %s: %s", stock_id, e)

        # --- Altman Z-Score ---
        try:
            from piotroski import calculate_zscore_us
            price = 0
            try:
                from cache_manager import CacheManager
                cm = CacheManager()
                df_price = cm.get_price_data(stock_id, period='5d')
                if df_price is not None and not df_price.empty:
                    price = float(df_price['Close'].iloc[-1])
            except Exception:
                pass
            if price > 0:
                import yfinance as yf
                info = yf.Ticker(stock_id).info
                mcap = info.get('marketCap', price * 1e8)
                z_result = calculate_zscore_us(stock_id, market_cap=mcap)
                if z_result:
                    z = z_result['zscore']
                    zone = z_result['zone']
                    if zone == 'distress':
                        score -= 20
                        details.append(f"Z-Score={z:.1f} distress (-20)")
                    elif zone == 'safe':
                        score += 8
                        details.append(f"Z-Score={z:.1f} safe (+8)")
                    else:
                        details.append(f"Z-Score={z:.1f} grey [info]")
        except Exception as e:
            logger.debug("US Z-Score failed for %s: %s", stock_id, e)

        # --- ROIC / FCF from yfinance ---
        try:
            from piotroski import calculate_extra_metrics_us
            extras = calculate_extra_metrics_us(stock_id, market_cap=mcap if price > 0 else 0)
            if extras:
                roic = extras.get('roic', 0)
                if roic and roic > 15:
                    score += 8
                    details.append(f"ROIC={roic:.1f}% high (+8)")
                elif roic and roic < 0:
                    score -= 5
                    details.append(f"ROIC={roic:.1f}% negative (-5)")

                fcf_y = extras.get('fcf_yield', 0)
                if fcf_y and fcf_y > 8:
                    score += 8
                    details.append(f"FCF Yield={fcf_y:.1f}% high (+8)")
                elif fcf_y and fcf_y < -5:
                    score -= 5
                    details.append(f"FCF Yield={fcf_y:.1f}% negative (-5)")
        except Exception:
            pass

        # --- Finviz fundamentals (ROE, margins, growth) ---
        if finviz:
            v = finviz.get('valuation', {})

            roe = v.get('roe')
            if roe and roe > 0:
                if roe > 20:
                    score += 5
                    details.append(f"ROE={roe:.1f}% excellent (+5)")
                elif roe > 10:
                    score += 3
            elif roe and roe < 0:
                score -= 5
                details.append(f"ROE={roe:.1f}% negative (-5)")

            margin = v.get('profit_margin')
            if margin and margin > 0:
                if margin > 20:
                    score += 5
                    details.append(f"Profit margin {margin:.1f}% high (+5)")
                elif margin > 10:
                    score += 3
            elif margin and margin < 0:
                score -= 5

            eps_g = v.get('eps_growth_next_5y')
            if eps_g and eps_g > 0:
                if eps_g > 15:
                    score += 5
                    details.append(f"EPS growth 5Y={eps_g:.1f}% (+5)")
                elif eps_g > 8:
                    score += 3

            # Debt/Equity
            de = v.get('debt_equity')
            if de is not None:
                if de < 0.3:
                    score += 5
                    details.append(f"Debt/Eq={de:.2f} low (+5)")
                elif de > 2.0:
                    score -= 8
                    details.append(f"Debt/Eq={de:.2f} high (-8)")

        return max(0, min(100, score))

    def _score_smart_money_us(self, stock_id, finviz, details):
        """US smart money: institutional %, short interest, insider activity."""
        score = 50

        # Try us_stock_chip for detailed data
        try:
            from us_stock_chip import USStockChipAnalyzer
            usc = USStockChipAnalyzer()
            chip, _ = usc.get_chip_data(stock_id)
            if chip:
                # Institutional holding
                inst = chip.get('institutional', {})
                inst_pct = inst.get('percent_held', 0)
                if inst_pct and inst_pct > 80:
                    score += 10
                    details.append(f"Institutional {inst_pct:.0f}% (+10)")

                # Short interest
                short = chip.get('short_interest', {})
                short_pct = short.get('percent_of_float', 0)
                if short_pct and short_pct > 10:
                    score -= 10
                    details.append(f"Short {short_pct:.1f}% of float (-10)")
                elif short_pct and short_pct < 2:
                    score += 5

                # Insider activity
                insider = chip.get('insider_trades', {})
                sentiment = insider.get('sentiment', '')
                if sentiment == 'bullish':
                    score += 12
                    details.append(f"Insider buying (+12)")
                elif sentiment == 'bearish':
                    score -= 8
        except Exception:
            pass

        # Analyst target upside (from finviz)
        if finviz:
            a = finviz.get('analyst', {})
            upside = a.get('upside_pct', 0)
            if upside and upside > 20:
                score += 8
                details.append(f"Analyst consensus +{upside:.0f}% (+8)")

        return max(0, min(100, score))

    # ================================================================
    # Result Formatting
    # ================================================================

    def _make_result(self, scored, total_scanned, passed_initial, elapsed):
        now = datetime.now()
        market = getattr(self, '_market', 'tw')
        return {
            'scan_date': now.strftime('%Y-%m-%d'),
            'scan_time': now.strftime('%H:%M'),
            'scan_type': 'value',
            'market': market,
            'total_scanned': total_scanned,
            'passed_initial': passed_initial,
            'scored_count': len(scored),
            'elapsed_seconds': round(elapsed, 1),
            'failures': self._failures[:20],
            'config': {
                'max_pe': self.config['max_pe'],
                'top_n': self.config['top_n'],
            },
            'results': scored,
        }

    @staticmethod
    def save_results(result, output_dir='data'):
        base = Path(output_dir)
        latest_dir = base / 'latest'
        history_dir = base / 'history'
        latest_dir.mkdir(parents=True, exist_ok=True)
        history_dir.mkdir(parents=True, exist_ok=True)

        market = result.get('market', 'tw')
        suffix = '_us' if market == 'us' else ''

        latest_file = latest_dir / f'value{suffix}_result.json'
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        date_str = result.get('scan_date', datetime.now().strftime('%Y-%m-%d'))
        history_file = history_dir / f'{date_str}_value{suffix}.json'
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        return str(latest_file), str(history_file)


# ====================================================================
# CLI Entry Point
# ====================================================================

if __name__ == '__main__':
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    )

    parser = argparse.ArgumentParser(description='Value Screener')
    parser.add_argument('--stage1-only', action='store_true')
    parser.add_argument('--no-chip', action='store_true')
    parser.add_argument('--top', type=int, default=50)
    parser.add_argument('--max-pe', type=float, default=30)
    parser.add_argument('--save', action='store_true')
    args = parser.parse_args()

    config = {'top_n': args.top, 'max_pe': args.max_pe}
    if args.no_chip:
        config['include_chip'] = False

    screener = ValueScreener(config=config)

    if args.stage1_only:
        df = screener.run_stage1_only()
        print(f"\nStage 1: {len(df)} candidates")
        if not df.empty:
            cols = ['stock_id', 'stock_name', 'market', 'close', 'PE', 'PB',
                    'dividend_yield', 'change_pct', 'trading_value']
            show_cols = [c for c in cols if c in df.columns]
            print(df[show_cols].head(30).to_string(index=False))
    else:
        result = screener.run()
        print(f"\nResults: {result['scored_count']} stocks scored")
        if result['results']:
            print(f"\nTop {min(20, len(result['results']))}:")
            for i, r in enumerate(result['results'][:20], 1):
                s = r['scores']
                print(f"  {i:2d}. {r['stock_id']} {r['name'][:8]:8s} "
                      f"${r['price']:>8.1f}  PE={r['PE']:>5.1f}  "
                      f"Score={r['value_score']:>5.1f}  "
                      f"V={s['valuation']:.0f} Q={s['quality']:.0f} "
                      f"R={s['revenue']:.0f} T={s['technical']:.0f} "
                      f"S={s['smart_money']:.0f}")

        if args.save:
            paths = ValueScreener.save_results(result)
            print(f"\nSaved to: {paths[0]}")
