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

    def run(self):
        """Execute full value screening pipeline."""
        start_time = time.time()

        # Stage 1
        self.progress("Stage 1: Fetching market + fundamental data...")
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

    def run_stage1_only(self):
        """Only run Stage 1 for quick preview."""
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

        # Get PE/PB/dividend for all TWSE stocks
        pe_df = api.get_pe_dividend_all()

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
    # Stage 2: Full Value Scoring (0-100)
    # ================================================================

    def _stage2_score(self, candidates):
        """Score each candidate on 5 dimensions."""
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
                result = self._score_single(sid, row)
                if result:
                    scored.append(result)
                    consecutive_fails = 0
                else:
                    self._failures.append(sid)
                    consecutive_fails += 1
            except Exception as e:
                logger.warning("Failed to score %s: %s", sid, e)
                self._failures.append(sid)
                consecutive_fails += 1

            if consecutive_fails >= self.config['max_failures']:
                self.progress(f"  Stopping: {consecutive_fails} consecutive failures")
                break

            if self.config['batch_delay'] > 0:
                time.sleep(self.config['batch_delay'])

        scored.sort(key=lambda x: x['value_score'], reverse=True)
        return scored[:self.config['top_n']]

    def _score_single(self, stock_id, market_row):
        """Score a single stock on all 5 dimensions."""
        cfg = self.config
        scores = {}
        details = []

        # --- 1. Valuation Score (0-100) ---
        scores['valuation'] = self._score_valuation(stock_id, market_row, details)

        # --- 2. Quality Score (0-100) ---
        scores['quality'] = self._score_quality(stock_id, details)

        # --- 3. Revenue Trend Score (0-100) ---
        scores['revenue'] = self._score_revenue(stock_id, details)

        # --- 4. Technical Reversal Score (0-100) ---
        scores['technical'] = self._score_technical(stock_id, details)

        # --- 5. Smart Money Score (0-100) ---
        scores['smart_money'] = self._score_smart_money(stock_id, details)

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

        return max(0, min(100, score))

    def _score_quality(self, stock_id, details):
        """
        體質分數: ROE + 三率趨勢 + 連續獲利
        """
        score = 50

        try:
            from fundamental_analysis import get_financial_statements
            fs = get_financial_statements(stock_id, quarters=12)

            if fs is not None and not fs.empty and len(fs) >= 4:
                # ROE (most recent quarter)
                if 'ROE' in fs.columns:
                    recent_roe = fs['ROE'].iloc[-1]
                    if pd.notna(recent_roe):
                        if recent_roe > 15:
                            score += 15
                            details.append(f"ROE={recent_roe:.1f}% 優 (+15)")
                        elif recent_roe > 8:
                            score += 8
                            details.append(f"ROE={recent_roe:.1f}% 良 (+8)")
                        elif recent_roe < 0:
                            score -= 20
                            details.append(f"ROE={recent_roe:.1f}% 虧損 (-20)")

                # Gross margin trend (last 4 quarters)
                if 'GrossMargin' in fs.columns:
                    gm = fs['GrossMargin'].dropna()
                    if len(gm) >= 4:
                        recent_gm = gm.iloc[-1]
                        old_gm = gm.iloc[-4]
                        if recent_gm > old_gm + 2:
                            score += 10
                            details.append(f"毛利率回升 {old_gm:.1f}→{recent_gm:.1f}% (+10)")
                        elif recent_gm < old_gm - 5:
                            score -= 10
                            details.append(f"毛利率下滑 {old_gm:.1f}→{recent_gm:.1f}% (-10)")

                # Operating margin trend
                if 'OperatingMargin' in fs.columns:
                    om = fs['OperatingMargin'].dropna()
                    if len(om) >= 4:
                        recent_om = om.iloc[-1]
                        old_om = om.iloc[-4]
                        if recent_om > old_om + 2:
                            score += 8
                            details.append(f"營益率回升 {old_om:.1f}→{recent_om:.1f}% (+8)")

                # Consecutive profitable quarters
                if 'EPS' in fs.columns:
                    eps = fs['EPS'].dropna()
                    if len(eps) >= 4:
                        profitable_q = (eps.iloc[-4:] > 0).sum()
                        if profitable_q == 4:
                            score += 10
                            details.append("連續 4 季獲利 (+10)")
                        elif profitable_q <= 1:
                            score -= 15
                            details.append(f"近 4 季僅 {profitable_q} 季獲利 (-15)")
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

    # ================================================================
    # Result Formatting
    # ================================================================

    def _make_result(self, scored, total_scanned, passed_initial, elapsed):
        now = datetime.now()
        return {
            'scan_date': now.strftime('%Y-%m-%d'),
            'scan_time': now.strftime('%H:%M'),
            'scan_type': 'value',
            'total_scanned': total_scanned,
            'passed_initial': passed_initial,
            'scored_count': len(scored),
            'elapsed_seconds': round(elapsed, 1),
            'failures': self._failures[:20],
            'config': {
                'max_pe': self.config['max_pe'],
                'min_pe': self.config['min_pe'],
                'max_pb': self.config['max_pb'],
                'min_trading_value': self.config['min_trading_value'],
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

        latest_file = latest_dir / 'value_result.json'
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        date_str = result.get('scan_date', datetime.now().strftime('%Y-%m-%d'))
        history_file = history_dir / f'{date_str}_value.json'
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
