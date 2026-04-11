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

# ================================================================
# Default Configuration
# ================================================================
DEFAULT_CONFIG = {
    # Stage 1: 初篩門檻
    'max_pe': 30,               # PE 上限（太高不算便宜）
    'min_pe': 0.1,              # PE 下限（排除虧損股或極端值）
    'max_pb': 5.0,              # PB 上限
    'min_dividend_yield': 0,    # 殖利率下限（0=不篩）
    'min_trading_value': 5e6,   # 最低成交值 500 萬 TWD

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
        Combine market daily data + PE/PB data for initial screening.

        Criteria:
        1. PE in (min_pe, max_pe) — profitable and not overvalued
        2. PB < max_pb
        3. Trading value > minimum
        4. Only regular stocks (4-digit numeric IDs)
        """
        from twse_api import TWSEOpenData
        api = TWSEOpenData()
        cfg = self.config

        # Get market prices + volumes
        market_df = api.get_market_daily_all()
        if market_df.empty:
            return pd.DataFrame()

        total_market = len(market_df)

        # Get PE/PB/dividend for all stocks (TWSE + TPEX)
        pe_df = api.get_pe_dividend_all_combined()

        if pe_df.empty:
            # Fallback: use market data without PE filter
            logger.warning("No PE data available, using market data only")
            result = market_df[market_df['trading_value'] >= cfg['min_trading_value']].copy()
            result.attrs['total_market'] = total_market
            return result

        # Merge market data with fundamentals
        merged = market_df.merge(
            pe_df[['stock_id', 'PE', 'dividend_yield', 'PB']],
            on='stock_id',
            how='left',
        )

        # Apply filters
        mask = pd.Series(True, index=merged.index)

        # 1. PE filter (only stocks with valid PE)
        has_pe = merged['PE'].notna() & (merged['PE'] > 0)
        mask &= has_pe
        mask &= merged['PE'] >= cfg['min_pe']
        mask &= merged['PE'] <= cfg['max_pe']

        # 2. PB filter
        if cfg['max_pb'] > 0:
            has_pb = merged['PB'].notna() & (merged['PB'] > 0)
            mask &= (has_pb & (merged['PB'] <= cfg['max_pb'])) | ~has_pb

        # 3. Dividend yield filter
        if cfg['min_dividend_yield'] > 0:
            mask &= merged['dividend_yield'] >= cfg['min_dividend_yield']

        # 4. Liquidity filter
        mask &= merged['trading_value'] >= cfg['min_trading_value']

        result = merged[mask].copy()
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

        # For US stocks: fetch finviz data to populate PE/PB/PEG/dividend
        finviz = None
        if is_us:
            try:
                from finviz_data import FinvizData
                fv = FinvizData()
                finviz, _ = fv.get_stock_data(stock_id)
                if finviz:
                    v = finviz.get('valuation', {})
                    market_row = dict(market_row)  # make mutable copy
                    market_row['PE'] = v.get('pe', 0) or 0
                    market_row['PB'] = v.get('pb', 0) or 0
                    market_row['dividend_yield'] = v.get('dividend_yield', 0) or 0
                    market_row['_peg'] = v.get('peg', 0) or 0
                    market_row['_forward_pe'] = v.get('forward_pe', 0) or 0
                    a = finviz.get('analyst', {})
                    market_row['_target_upside'] = a.get('upside_pct', 0) or 0
            except Exception:
                pass

        # --- 1. Valuation Score (0-100) ---
        scores['valuation'] = self._score_valuation(stock_id, market_row, details)

        # --- 2. Quality Score (0-100) ---
        if is_us:
            scores['quality'] = self._score_quality_us(stock_id, finviz, details)
        else:
            scores['quality'] = self._score_quality(stock_id, details)

        # --- 3. Revenue Trend Score (0-100) ---
        if is_us:
            scores['revenue'] = 50  # US revenue from finviz is limited
        else:
            scores['revenue'] = self._score_revenue(stock_id, details)

        # --- 4. Technical Reversal Score (0-100) ---
        scores['technical'] = self._score_technical(stock_id, details)

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

        return {
            'stock_id': stock_id,
            'name': market_row.get('stock_name', ''),
            'market': market_row.get('market', 'twse'),
            'price': market_row.get('close', 0),
            'change_pct': round(market_row.get('change_pct', 0), 2),
            'trading_value': int(market_row.get('trading_value', 0)),
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

        # Try to get historical PE percentile
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

        # PEG: PE / EPS growth rate (lower = more undervalued)
        try:
            from dividend_revenue import RevenueTracker
            rt = RevenueTracker()
            rev_df = rt.get_monthly_revenue(stock_id, months=24)
            if rev_df is not None and not rev_df.empty and 'yoy_pct' in rev_df.columns:
                yoy = rev_df['yoy_pct'].dropna()
                if len(yoy) >= 6 and pe > 0:
                    # Use average of last 6 months revenue YoY as growth proxy
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
                # Estimate cash dividend from yield
                cash_div = dy * price / 100
                # Gordon Growth Model: fair_price = D / (r - g)
                # r = 10% required return, g = estimated from revenue trend
                discount_rate = 0.10
                # Conservative growth: use min(revenue_growth, 5%) or 2% default
                growth_rate = 0.02
                try:
                    from dividend_revenue import RevenueTracker
                    _rt = RevenueTracker()
                    _alert = _rt.get_revenue_alert(stock_id)
                    if _alert and _alert.get('last_yoy_pct') is not None:
                        g_raw = _alert['last_yoy_pct'] / 100
                        growth_rate = max(0.0, min(0.05, g_raw))  # Cap 0-5%
                except Exception:
                    pass

                if discount_rate > growth_rate:
                    fair_price = cash_div / (discount_rate - growth_rate)
                    discount_pct = (fair_price - price) / price * 100
                    if discount_pct > 30:
                        score += 10
                        details.append(f"DDM 合理價 {fair_price:.0f} (折價 {discount_pct:.0f}%) (+10)")
                    elif discount_pct > 10:
                        score += 5
                        details.append(f"DDM 合理價 {fair_price:.0f} (折價 {discount_pct:.0f}%) (+5)")
                    elif discount_pct < -30:
                        score -= 8
                        details.append(f"DDM 合理價 {fair_price:.0f} (溢價 {abs(discount_pct):.0f}%) (-8)")
        except Exception:
            pass

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

    def _score_quality(self, stock_id, details):
        """
        體質分數: Piotroski F-Score + Altman Z-Score + ROE + 三率 + ROIC/FCF
        """
        score = 50

        # --- Piotroski F-Score (primary quality signal) ---
        try:
            from piotroski import calculate_fscore
            fs_result = calculate_fscore(stock_id)
            if fs_result:
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

                # Extract current ratio from F-Score data
                cr = fs_result['data'].get('current_ratio', 0)
                if cr > 0:
                    if cr > 2.0:
                        score += 5
                        details.append(f"流動比率={cr:.1f} 安全 (+5)")
                    elif cr < 1.0:
                        score -= 8
                        details.append(f"流動比率={cr:.1f} 偏低 (-8)")
        except Exception:
            pass

        # --- Altman Z-Score (bankruptcy risk, method B: penalty not exclude) ---
        try:
            from piotroski import calculate_zscore
            # Estimate market cap from price * shares (rough)
            price = 0
            try:
                from cache_manager import get_finmind_loader
                _dl = get_finmind_loader()
                _df = _dl.taiwan_stock_daily(stock_id=stock_id,
                    start_date=(datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d'))
                if not _df.empty:
                    price = float(_df.iloc[-1].get('close', 0))
            except Exception:
                pass
            if price > 0:
                # Rough market cap (will be refined when shares data available)
                mcap = price * 1e8  # Placeholder; Z-Score is ratio-based so scale matters less
                z_result = calculate_zscore(stock_id, market_cap=mcap)
                if z_result:
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
        except Exception:
            pass

        # --- ROIC / FCF (supplementary) ---
        try:
            from piotroski import calculate_extra_metrics
            extras = calculate_extra_metrics(stock_id, market_cap=mcap if price > 0 else 0)
            if extras:
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
        except Exception:
            pass

        # --- Fallback: basic ROE + margins (if F-Score unavailable) ---
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

    def _score_technical(self, stock_id, details):
        """
        技術面轉折分數: RSI 超賣 + 量能萎縮 + Squeeze 壓縮
        """
        score = 50

        try:
            from technical_analysis import load_and_resample, calculate_all_indicators

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
        if self.config['include_chip'] and stock_id.isdigit():
            try:
                from chip_analysis import ChipAnalyzer
                ca = ChipAnalyzer()
                chip_data, _ = ca.get_chip_data(stock_id)

                if chip_data and 'institutional' in chip_data:
                    inst = chip_data['institutional']
                    if not inst.empty and len(inst) >= 5:
                        # Sum last 5 days net buy
                        cols = [c for c in inst.columns if c in ['外資', '投信', '合計']]
                        if '合計' in cols:
                            recent_net = inst['合計'].iloc[-5:].sum()
                            if recent_net > 0:
                                score += 10
                                details.append(f"法人近 5 日淨買 {recent_net:+,.0f} (+10)")
                            elif recent_net < -1000:
                                score -= 10
                                details.append(f"法人近 5 日淨賣 {recent_net:+,.0f} (-10)")
            except Exception:
                pass

        return max(0, min(100, score))

    def _score_quality_us(self, stock_id, finviz, details):
        """US quality score from finviz data."""
        score = 50
        if not finviz:
            return score

        v = finviz.get('valuation', {})
        t = finviz.get('technical', {})

        # ROE (from finviz)
        roe = v.get('roe', 0)
        if roe and roe > 0:
            if roe > 20:
                score += 15
                details.append(f"ROE={roe:.1f}% excellent (+15)")
            elif roe > 10:
                score += 8
                details.append(f"ROE={roe:.1f}% good (+8)")
        elif roe and roe < 0:
            score -= 15
            details.append(f"ROE={roe:.1f}% negative (-15)")

        # Profit margin
        margin = v.get('profit_margin', 0)
        if margin and margin > 0:
            if margin > 20:
                score += 10
                details.append(f"Profit margin {margin:.1f}% high (+10)")
            elif margin > 10:
                score += 5
        elif margin and margin < 0:
            score -= 10

        # EPS growth
        eps_g = v.get('eps_growth_next_5y', 0)
        if eps_g and eps_g > 0:
            if eps_g > 15:
                score += 10
                details.append(f"EPS growth 5Y={eps_g:.1f}% (+10)")
            elif eps_g > 8:
                score += 5

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
