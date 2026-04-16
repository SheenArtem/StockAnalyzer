"""
TAIFEX Data Module - Taiwan Futures Exchange Data & Fear/Greed Index

Provides:
1. TAIFEXData - fetches futures basis, put/call ratio, institutional futures,
   and large trader positions from TAIFEX.
2. TaiwanFearGreedIndex - composite 0-100 index from 5 sub-indicators:
   Market Momentum, Market Breadth, Put/Call Ratio, Volatility, Margin Balance.

Data sources:
- TAIFEX (https://www.taifex.com.tw/)
- TWSE (https://www.twse.com.tw/)
- Yahoo Finance (yfinance)
"""

import logging
import time
import urllib3
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# 部分環境 TWSE SSL 憑證驗證失敗，停用相關警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 共用 HTTP headers -- TAIFEX 會封鎖缺少適當 headers 的請求
# ---------------------------------------------------------------------------
TAIFEX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'https://www.taifex.com.tw/',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

TWSE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'https://www.twse.com.tw/',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
}

# 快取 TTL (秒)
CACHE_TTL = 30 * 60  # 30 分鐘

# HTTP request timeout (秒)
REQUEST_TIMEOUT = 15


# ===========================================================================
# 簡易 dict 快取
# ===========================================================================
class _SimpleCache:
    """Simple dict cache with TTL (seconds)."""

    def __init__(self, ttl: int = CACHE_TTL):
        self._store: Dict[str, Any] = {}
        self._timestamps: Dict[str, float] = {}
        self._ttl = ttl

    def get(self, key: str) -> Optional[Any]:
        if key in self._store:
            if time.time() - self._timestamps[key] < self._ttl:
                return self._store[key]
            # 已過期，清除
            del self._store[key]
            del self._timestamps[key]
        return None

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value
        self._timestamps[key] = time.time()


# ===========================================================================
# TAIFEXData - 台灣期貨交易所數據
# ===========================================================================
class TAIFEXData:
    """
    Fetches data from the Taiwan Futures Exchange (TAIFEX).

    Methods:
        get_futures_basis()        - 台指期正逆價差
        get_put_call_ratio()       - 選擇權 Put/Call Ratio (未平倉)
        get_institutional_futures()- 三大法人台指期未平倉
        get_large_trader_positions()- 大額交易人未沖銷部位
    """

    def __init__(self):
        self._cache = _SimpleCache(ttl=CACHE_TTL)
        self._session = requests.Session()
        self._session.headers.update(TAIFEX_HEADERS)

    # ------------------------------------------------------------------
    # 台指期正逆價差 (Futures Basis)
    # ------------------------------------------------------------------
    def get_futures_basis(self) -> Dict[str, Any]:
        """
        Fetch TAIEX futures basis (正逆價差).
        使用 CSV 下載端點取得期貨結算價，比 HTML 解析更可靠。

        Returns:
            dict with keys: basis, futures_price, spot_price, basis_pct
        """
        cached = self._cache.get('futures_basis')
        if cached is not None:
            return cached

        result = {
            'basis': 0.0,
            'futures_price': 0.0,
            'spot_price': 0.0,
            'basis_pct': 0.0,
        }

        try:
            today = datetime.now()
            futures_price = 0.0

            # 嘗試最近 5 個交易日 (跳過假日)
            for delta in range(5):
                d = today - timedelta(days=delta)
                date_str = d.strftime('%Y/%m/%d')

                url = 'https://www.taifex.com.tw/cht/3/dlFutDataDown'
                payload = {
                    'down_type': '1',
                    'commodity_id': 'TX',
                    'queryStartDate': date_str,
                    'queryEndDate': date_str,
                }

                resp = self._session.post(url, data=payload, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                content = resp.text.strip()
                lines = content.split('\n')

                if len(lines) < 2:
                    continue

                # CSV 格式: 交易日期,契約,到期月份,開盤價,最高價,最低價,收盤價,...,結算價,未沖銷契約數,...
                # 取近月合約 (成交量最大的那一行)
                best_volume = 0
                for line in lines[1:]:
                    fields = line.split(',')
                    if len(fields) < 11:
                        continue
                    try:
                        close_str = fields[6].strip()
                        settle_str = fields[10].strip()
                        vol_str = fields[9].strip()
                        if close_str == '-' or not close_str:
                            continue
                        vol = int(vol_str) if vol_str else 0
                        if vol > best_volume:
                            best_volume = vol
                            # 優先用結算價，無結算價用收盤價
                            price_str = settle_str if settle_str and settle_str != '-' else close_str
                            futures_price = float(price_str)
                    except (ValueError, IndexError):
                        continue

                if futures_price > 0:
                    break

            # 取得加權指數現貨價
            spot_price = self._get_taiex_spot()

            if futures_price > 0 and spot_price > 0:
                basis = futures_price - spot_price
                basis_pct = (basis / spot_price) * 100
                result = {
                    'basis': round(basis, 2),
                    'futures_price': round(futures_price, 2),
                    'spot_price': round(spot_price, 2),
                    'basis_pct': round(basis_pct, 4),
                }

            self._cache.set('futures_basis', result)
            logger.info("Futures basis fetched: futures=%.0f, spot=%.0f, basis=%.2f",
                        result['futures_price'], result['spot_price'], result['basis'])

        except requests.RequestException as e:
            logger.warning("Failed to fetch futures basis (network): %s", e)
        except Exception as e:
            logger.error("Failed to fetch futures basis: %s", e, exc_info=True)

        return result

    # ------------------------------------------------------------------
    # 選擇權 Put/Call Ratio (未平倉)
    # ------------------------------------------------------------------
    def get_put_call_ratio(self) -> Dict[str, Any]:
        """
        Fetch options Put/Call ratio (open interest).
        使用 CSV 下載端點彙總 TXO 買權/賣權未平倉量。

        Returns:
            dict with keys: pc_ratio, call_oi, put_oi, total_oi
        """
        cached = self._cache.get('put_call_ratio')
        if cached is not None:
            return cached

        result = {
            'pc_ratio': 0.0,
            'call_oi': 0,
            'put_oi': 0,
            'total_oi': 0,
        }

        try:
            today = datetime.now()
            call_oi = 0
            put_oi = 0

            for delta in range(5):
                d = today - timedelta(days=delta)
                date_str = d.strftime('%Y/%m/%d')

                url = 'https://www.taifex.com.tw/cht/3/dlOptDataDown'
                payload = {
                    'down_type': '1',
                    'commodity_id': 'TXO',
                    'queryStartDate': date_str,
                    'queryEndDate': date_str,
                }

                resp = self._session.post(url, data=payload, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                content = resp.text.strip()
                lines = content.split('\n')

                if len(lines) < 10:
                    continue

                # CSV: 交易日期,契約,到期月份,履約價,買賣權,開盤價,...,結算價,未沖銷契約數,...
                # 彙總所有履約價的未平倉量
                call_oi_total = 0
                put_oi_total = 0
                for line in lines[1:]:
                    fields = line.split(',')
                    if len(fields) < 12:
                        continue
                    try:
                        cp_type = fields[4].strip()  # 買權 or 賣權
                        oi_str = fields[11].strip()
                        if not oi_str or oi_str == '-':
                            continue
                        oi = int(oi_str)
                        if cp_type == '買權':
                            call_oi_total += oi
                        elif cp_type == '賣權':
                            put_oi_total += oi
                    except (ValueError, IndexError):
                        continue

                if call_oi_total > 0 or put_oi_total > 0:
                    call_oi = call_oi_total
                    put_oi = put_oi_total
                    break

            if call_oi > 0:
                pc_ratio = round(put_oi / call_oi, 4)
            else:
                pc_ratio = 0.0

            total_oi = call_oi + put_oi
            result = {
                'pc_ratio': pc_ratio,
                'call_oi': call_oi,
                'put_oi': put_oi,
                'total_oi': total_oi,
            }

            self._cache.set('put_call_ratio', result)
            logger.info("Put/Call ratio fetched: P/C=%.4f, Call OI=%d, Put OI=%d",
                        pc_ratio, call_oi, put_oi)

        except requests.RequestException as e:
            logger.warning("Failed to fetch put/call ratio (network): %s", e)
        except Exception as e:
            logger.error("Failed to fetch put/call ratio: %s", e, exc_info=True)

        return result

    # ------------------------------------------------------------------
    # 三大法人台指期未平倉
    # ------------------------------------------------------------------
    def get_institutional_futures(self) -> Dict[str, Any]:
        """
        Fetch institutional investors' TAIEX futures open interest.

        Returns:
            dict with keys: foreign_net, trust_net, dealer_net, total_net,
            foreign_long, foreign_short, trust_long, trust_short,
            dealer_long, dealer_short
        """
        cached = self._cache.get('institutional_futures')
        if cached is not None:
            return cached

        result = {
            'foreign_net': 0, 'foreign_long': 0, 'foreign_short': 0,
            'trust_net': 0, 'trust_long': 0, 'trust_short': 0,
            'dealer_net': 0, 'dealer_long': 0, 'dealer_short': 0,
            'total_net': 0,
        }

        try:
            today = datetime.now()
            date_str = today.strftime('%Y/%m/%d')

            # 三大法人 -- 期貨區分 (台指期)
            url = 'https://www.taifex.com.tw/cht/3/futContractsDate'
            payload = {
                'queryType': '1',
                'marketCode': '0',
                'dateaddcnt': '',
                'commodity_id': 'TX',
                'queryDate': date_str,
            }

            resp = self._session.post(url, data=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')
            tables = soup.find_all('table', class_='table_f')
            if not tables:
                tables = soup.find_all('table')

            # 解析三大法人未平倉
            # 格式: 身份別 | 多方口數 | 多方契約金額 | 空方口數 | 空方契約金額 | 多空淨額口數 | 多空淨額金額
            institution_map = {
                '外資': 'foreign',
                '外國機構投資人': 'foreign',
                '投信': 'trust',
                '自營商': 'dealer',
                '自營': 'dealer',
            }

            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < 5:
                        continue

                    identity = cells[0].get_text(strip=True)
                    matched_key = None
                    for keyword, key in institution_map.items():
                        if keyword in identity:
                            matched_key = key
                            break

                    if matched_key is None:
                        continue

                    try:
                        numbers = []
                        for cell in cells[1:]:
                            cleaned = cell.get_text(strip=True).replace(',', '').replace(' ', '')
                            try:
                                numbers.append(int(cleaned))
                            except ValueError:
                                continue

                        if len(numbers) >= 3:
                            # 多方口數, (金額), 空方口數, (金額), 淨額口數
                            long_pos = numbers[0]
                            short_pos = numbers[2] if len(numbers) > 2 else 0
                            net_pos = numbers[4] if len(numbers) > 4 else (long_pos - short_pos)

                            result[f'{matched_key}_long'] = long_pos
                            result[f'{matched_key}_short'] = short_pos
                            result[f'{matched_key}_net'] = net_pos
                    except (ValueError, IndexError) as e:
                        logger.debug("Parse error for %s row: %s", identity, e)
                        continue

            result['total_net'] = (result['foreign_net'] +
                                   result['trust_net'] +
                                   result['dealer_net'])

            self._cache.set('institutional_futures', result)
            logger.info("Institutional futures: foreign=%d, trust=%d, dealer=%d, total=%d",
                        result['foreign_net'], result['trust_net'],
                        result['dealer_net'], result['total_net'])

        except requests.RequestException as e:
            logger.warning("Failed to fetch institutional futures (network): %s", e)
        except Exception as e:
            logger.error("Failed to fetch institutional futures: %s", e, exc_info=True)

        return result

    # ------------------------------------------------------------------
    # 大額交易人未沖銷部位
    # ------------------------------------------------------------------
    def get_large_trader_positions(self) -> Dict[str, Any]:
        """
        Fetch large trader open interest positions.

        Returns:
            dict with keys: top5_long, top5_short, top5_net,
            top10_long, top10_short, top10_net, market_oi
        """
        cached = self._cache.get('large_trader_positions')
        if cached is not None:
            return cached

        result = {
            'top5_long': 0, 'top5_short': 0, 'top5_net': 0,
            'top10_long': 0, 'top10_short': 0, 'top10_net': 0,
            'market_oi': 0,
        }

        try:
            today = datetime.now()
            date_str = today.strftime('%Y/%m/%d')

            url = 'https://www.taifex.com.tw/cht/3/largeTraderFutQry'
            payload = {
                'queryDate': date_str,
                'contractId': 'TX',
            }

            resp = self._session.post(url, data=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')
            tables = soup.find_all('table', class_='table_f')
            if not tables:
                tables = soup.find_all('table')

            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < 5:
                        continue

                    text = cells[0].get_text(strip=True)
                    numbers = []
                    for cell in cells[1:]:
                        cleaned = cell.get_text(strip=True).replace(',', '').replace(' ', '')
                        try:
                            numbers.append(int(cleaned))
                        except ValueError:
                            continue

                    # 前五大交易人
                    if '前五大' in text or 'Top 5' in text.lower():
                        if len(numbers) >= 2:
                            result['top5_long'] = numbers[0]
                            result['top5_short'] = numbers[1]
                            result['top5_net'] = numbers[0] - numbers[1]

                    # 前十大交易人
                    elif '前十大' in text or 'Top 10' in text.lower():
                        if len(numbers) >= 2:
                            result['top10_long'] = numbers[0]
                            result['top10_short'] = numbers[1]
                            result['top10_net'] = numbers[0] - numbers[1]

                    # 全市場未平倉
                    elif '全市場' in text or '市場' in text:
                        if numbers:
                            result['market_oi'] = numbers[0]

            self._cache.set('large_trader_positions', result)
            logger.info("Large trader positions: top5_net=%d, top10_net=%d, market_oi=%d",
                        result['top5_net'], result['top10_net'], result['market_oi'])

        except requests.RequestException as e:
            logger.warning("Failed to fetch large trader positions (network): %s", e)
        except Exception as e:
            logger.error("Failed to fetch large trader positions: %s", e, exc_info=True)

        return result

    # ------------------------------------------------------------------
    # 內部輔助 -- 取得加權指數現貨價
    # ------------------------------------------------------------------
    def _get_taiex_spot(self) -> float:
        """Fetch current TAIEX spot index from TWSE JSON API."""
        try:
            url = 'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json'
            resp = requests.get(url, headers=TWSE_HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
            resp.raise_for_status()
            data = resp.json()

            # TWSE JSON 格式: data 裡面有各指數
            # 嘗試從 tables 或 data 取得加權指數收盤
            if 'tables' in data:
                for table in data['tables']:
                    title = table.get('title', '')
                    if '每日收盤行情' in title or '發行量加權' in title:
                        # data 裡面找「發行量加權股價指數」
                        for row in table.get('data', []):
                            if isinstance(row, list) and len(row) > 1:
                                name = str(row[0]).strip()
                                if '發行量加權' in name or '加權' in name:
                                    try:
                                        close_val = str(row[1]).replace(',', '').strip()
                                        return float(close_val)
                                    except (ValueError, IndexError):
                                        continue

            # 備用方案: 使用 yfinance 取得 ^TWII
            import yfinance as yf
            twii = yf.Ticker('^TWII')
            hist = twii.history(period='1d')
            if not hist.empty:
                return float(hist['Close'].iloc[-1])

        except Exception as e:
            logger.warning("Failed to get TAIEX spot price: %s", e)

        # 最後備用: 嘗試 yfinance
        try:
            import yfinance as yf
            twii = yf.Ticker('^TWII')
            hist = twii.history(period='5d')
            if not hist.empty:
                return float(hist['Close'].iloc[-1])
        except Exception as e:
            logger.warning("yfinance fallback for TAIEX also failed: %s", e)

        return 0.0


# ===========================================================================
# TaiwanFearGreedIndex - 台灣恐懼與貪婪指數
# ===========================================================================
class TaiwanFearGreedIndex:
    """
    Composite Fear & Greed Index for the Taiwan stock market.

    5 sub-indicators, each scored 0-100:
        1. Market Momentum (20%) - TAIEX distance from 52-week high/low
        2. Market Breadth (20%) - Advance/Decline ratio
        3. Put/Call Ratio (20%) - Options P/C ratio (inverted)
        4. Volatility (20%) - 20-day realized volatility
        5. Margin Balance (20%) - Margin lending 20-day change rate

    Labels:
        0-20:   Extreme Fear
        20-40:  Fear
        40-60:  Neutral
        60-80:  Greed
        80-100: Extreme Greed
    """

    # 各指標權重
    WEIGHTS = {
        'market_momentum': 0.20,
        'market_breadth': 0.20,
        'put_call_ratio': 0.20,
        'volatility': 0.20,
        'margin_balance': 0.20,
    }

    def __init__(self):
        self._cache = _SimpleCache(ttl=CACHE_TTL)
        self._taifex = TAIFEXData()

    def calculate(self) -> Dict[str, Any]:
        """
        Calculate the composite Taiwan Fear & Greed Index.

        Returns:
            dict with keys:
                score (float 0-100),
                label (str),
                components (dict of sub-scores and details)
        """
        cached = self._cache.get('fear_greed_index')
        if cached is not None:
            return cached

        components = {}
        valid_scores = {}

        # --- 1. Market Momentum (市場動能) ---
        try:
            momentum_result = self._calc_market_momentum()
            components['market_momentum'] = momentum_result
            if momentum_result.get('score') is not None:
                valid_scores['market_momentum'] = momentum_result['score']
        except Exception as e:
            logger.warning("Market momentum calculation failed: %s", e)
            components['market_momentum'] = {'score': None, 'error': str(e)}

        # --- 2. Market Breadth (市場廣度) ---
        try:
            breadth_result = self._calc_market_breadth()
            components['market_breadth'] = breadth_result
            if breadth_result.get('score') is not None:
                valid_scores['market_breadth'] = breadth_result['score']
        except Exception as e:
            logger.warning("Market breadth calculation failed: %s", e)
            components['market_breadth'] = {'score': None, 'error': str(e)}

        # --- 3. Put/Call Ratio ---
        try:
            pcr_result = self._calc_put_call_score()
            components['put_call_ratio'] = pcr_result
            if pcr_result.get('score') is not None:
                valid_scores['put_call_ratio'] = pcr_result['score']
        except Exception as e:
            logger.warning("Put/Call ratio calculation failed: %s", e)
            components['put_call_ratio'] = {'score': None, 'error': str(e)}

        # --- 4. Volatility (波動度) ---
        try:
            vol_result = self._calc_volatility_score()
            components['volatility'] = vol_result
            if vol_result.get('score') is not None:
                valid_scores['volatility'] = vol_result['score']
        except Exception as e:
            logger.warning("Volatility calculation failed: %s", e)
            components['volatility'] = {'score': None, 'error': str(e)}

        # --- 5. Margin Balance (融資餘額) ---
        try:
            margin_result = self._calc_margin_score()
            components['margin_balance'] = margin_result
            if margin_result.get('score') is not None:
                valid_scores['margin_balance'] = margin_result['score']
        except Exception as e:
            logger.warning("Margin balance calculation failed: %s", e)
            components['margin_balance'] = {'score': None, 'error': str(e)}

        # --- 計算加權綜合分數 ---
        if valid_scores:
            # 若部分指標失敗，用可用指標的權重重新正規化
            total_weight = sum(self.WEIGHTS[k] for k in valid_scores)
            if total_weight > 0:
                weighted_sum = sum(
                    valid_scores[k] * self.WEIGHTS[k] / total_weight
                    for k in valid_scores
                )
                composite_score = round(max(0, min(100, weighted_sum)), 1)
            else:
                composite_score = 50.0
        else:
            composite_score = 50.0  # 全部失敗時回傳中性預設值

        label = self._score_to_label(composite_score)

        result = {
            'score': composite_score,
            'label': label,
            'components': components,
            'available_indicators': len(valid_scores),
            'total_indicators': len(self.WEIGHTS),
        }

        self._cache.set('fear_greed_index', result)
        logger.info("Taiwan Fear & Greed Index: %.1f (%s), %d/%d indicators available",
                     composite_score, label, len(valid_scores), len(self.WEIGHTS))

        return result

    # ------------------------------------------------------------------
    # Sub-indicator 1: Market Momentum (市場動能)
    # ------------------------------------------------------------------
    def _calc_market_momentum(self) -> Dict[str, Any]:
        """
        TAIEX distance from 52-week high/low.
        At 52-week high = 100, at 52-week low = 0, linear interpolation.
        """
        import yfinance as yf

        twii = yf.Ticker('^TWII')
        hist = twii.history(period='1y')

        if hist.empty or len(hist) < 5:
            logger.warning("Insufficient TAIEX history for momentum calculation")
            return {'score': None, 'error': 'Insufficient data'}

        current = float(hist['Close'].iloc[-1])
        high_52w = float(hist['High'].max())
        low_52w = float(hist['Low'].min())

        if high_52w == low_52w:
            score = 50.0
        else:
            score = ((current - low_52w) / (high_52w - low_52w)) * 100.0

        score = round(max(0, min(100, score)), 1)

        return {
            'score': score,
            'current': round(current, 2),
            'high_52w': round(high_52w, 2),
            'low_52w': round(low_52w, 2),
        }

    # ------------------------------------------------------------------
    # Sub-indicator 2: Market Breadth (市場廣度)
    # ------------------------------------------------------------------
    def _calc_market_breadth(self) -> Dict[str, Any]:
        """
        Advance/Decline ratio — 用 twse_api 全市場行情算純股票漲跌家數。
        Ratio > 2.0 = 100, < 0.5 = 0, linear interpolation.
        """
        try:
            from twse_api import TWSEOpenData
            api = TWSEOpenData()
            df = api.get_market_daily_all()

            if df.empty or 'change_pct' not in df.columns:
                return {'score': None, 'error': 'No market data'}

            changes = df['change_pct'].dropna()
            advances = int((changes > 0).sum())
            declines = int((changes < 0).sum())

            if advances == 0 and declines == 0:
                return {'score': None, 'error': 'No advance/decline data'}

            if declines > 0:
                ad_ratio = advances / declines
            else:
                ad_ratio = 2.0 if advances > 0 else 1.0

            # 線性映射: ratio 0.5 -> score 0, ratio 2.0 -> score 100
            score = ((ad_ratio - 0.5) / (2.0 - 0.5)) * 100.0
            score = round(max(0, min(100, score)), 1)

            return {
                'score': score,
                'advances': advances,
                'declines': declines,
                'ad_ratio': round(ad_ratio, 4),
            }

        except requests.RequestException as e:
            logger.warning("Market breadth fetch failed (network): %s", e)
            return {'score': None, 'error': str(e)}
        except Exception as e:
            logger.warning("Market breadth calculation failed: %s", e)
            return {'score': None, 'error': str(e)}

    # ------------------------------------------------------------------
    # Sub-indicator 3: Put/Call Ratio Score
    # ------------------------------------------------------------------
    def _calc_put_call_score(self) -> Dict[str, Any]:
        """
        Put/Call Ratio score. Inverted: high P/C = Fear, low P/C = Greed.
        P/C > 1.5 = score 0, P/C < 0.5 = score 100, linear interpolation.
        """
        pcr_data = self._taifex.get_put_call_ratio()
        pc_ratio = pcr_data.get('pc_ratio', 0.0)

        if pc_ratio <= 0:
            return {
                'score': None,
                'error': 'No P/C ratio data',
                'pc_ratio': 0.0,
            }

        # 反轉: 高 P/C -> 低分 (恐懼), 低 P/C -> 高分 (貪婪)
        # P/C=1.5 -> score=0, P/C=0.5 -> score=100
        score = ((1.5 - pc_ratio) / (1.5 - 0.5)) * 100.0
        score = round(max(0, min(100, score)), 1)

        return {
            'score': score,
            'pc_ratio': pc_ratio,
            'call_oi': pcr_data.get('call_oi', 0),
            'put_oi': pcr_data.get('put_oi', 0),
        }

    # ------------------------------------------------------------------
    # Sub-indicator 4: Volatility Score (波動度)
    # ------------------------------------------------------------------
    def _calc_volatility_score(self) -> Dict[str, Any]:
        """
        20-day realized volatility of TAIEX.
        High vol (>30%) = 0 (Fear), Low vol (<10%) = 100 (Greed).
        """
        import yfinance as yf

        twii = yf.Ticker('^TWII')
        hist = twii.history(period='3mo')

        if hist.empty or len(hist) < 20:
            return {'score': None, 'error': 'Insufficient TAIEX history for volatility'}

        # 計算 20 日年化波動率
        returns = hist['Close'].pct_change().dropna()
        if len(returns) < 20:
            return {'score': None, 'error': 'Insufficient return data'}

        recent_returns = returns.iloc[-20:]
        daily_vol = float(recent_returns.std())
        annual_vol = daily_vol * np.sqrt(252) * 100  # 轉為百分比

        # 映射: vol=30% -> score=0, vol=10% -> score=100
        score = ((30.0 - annual_vol) / (30.0 - 10.0)) * 100.0
        score = round(max(0, min(100, score)), 1)

        return {
            'score': score,
            'volatility_20d': round(annual_vol, 2),
            'daily_vol': round(daily_vol * 100, 4),
        }

    # ------------------------------------------------------------------
    # Sub-indicator 5: Margin Balance (融資餘額變化)
    # ------------------------------------------------------------------
    def _calc_margin_score(self) -> Dict[str, Any]:
        """
        Margin lending balance 20-day change rate.
        Increasing margin = Greed, Decreasing = Fear.
        Uses TWSE margin trading data.
        """
        try:
            today = datetime.now()
            date_str = today.strftime('%Y%m%d')

            # TWSE 融資融券統計
            url = (
                'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN'
                f'?date={date_str}&selectType=MS&response=json'
            )
            resp = requests.get(url, headers=TWSE_HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
            resp.raise_for_status()
            data = resp.json()

            margin_balance = 0
            margin_prev = 0

            # 解析融資餘額
            # MI_MARGN 欄位: [項目, 買進, 賣出, 現金償還, 前日餘額, 今日餘額]
            # 最後一行是「融資金額(仟元)」，取 row[5]=今日餘額, row[4]=前日餘額
            if 'tables' in data:
                for table in data['tables']:
                    rows = table.get('data', [])
                    for row in rows:
                        if isinstance(row, list) and len(row) >= 6:
                            name = str(row[0]).strip()
                            if '融資金額' in name:
                                try:
                                    margin_balance = int(str(row[5]).replace(',', '').strip())
                                    margin_prev = int(str(row[4]).replace(',', '').strip())
                                except (ValueError, IndexError):
                                    pass
                                break

            # 若 TWSE 即時資料不足，用 yfinance ^TWII 的 margin 趨勢替代
            if margin_balance == 0:
                # 備用方案: 使用融資成長率的替代指標
                # 嘗試不同日期 (往回最多 5 個交易日)
                for days_back in range(1, 6):
                    try:
                        prev_date = today - timedelta(days=days_back)
                        if prev_date.weekday() >= 5:
                            continue
                        prev_str = prev_date.strftime('%Y%m%d')
                        url_prev = (
                            'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN'
                            f'?date={prev_str}&selectType=MS&response=json'
                        )
                        resp_prev = requests.get(
                            url_prev, headers=TWSE_HEADERS, timeout=REQUEST_TIMEOUT, verify=False
                        )
                        if resp_prev.status_code == 200:
                            data_prev = resp_prev.json()
                            if 'tables' in data_prev:
                                for table in data_prev['tables']:
                                    for row in table.get('data', []):
                                        if isinstance(row, list) and len(row) >= 6 and '融資金額' in str(row[0]):
                                            try:
                                                bal = int(str(row[5]).replace(',', '').strip())
                                                if margin_balance == 0:
                                                    margin_balance = bal
                                                else:
                                                    margin_prev = bal
                                                    break
                                            except (ValueError, IndexError):
                                                pass
                                if margin_prev > 0:
                                    break
                    except Exception:
                        continue

            if margin_balance == 0:
                return {'score': None, 'error': 'No margin data available'}

            if margin_prev > 0:
                change_rate = ((margin_balance - margin_prev) / margin_prev) * 100
            else:
                change_rate = 0.0

            # 映射: change_rate +5% -> 100 (Extreme Greed), -5% -> 0 (Extreme Fear)
            score = ((change_rate + 5.0) / 10.0) * 100.0
            score = round(max(0, min(100, score)), 1)

            return {
                'score': score,
                'margin_balance': margin_balance,
                'margin_prev': margin_prev,
                'change_rate_pct': round(change_rate, 4),
            }

        except requests.RequestException as e:
            logger.warning("Margin data fetch failed (network): %s", e)
            return {'score': None, 'error': str(e)}
        except Exception as e:
            logger.warning("Margin balance calculation failed: %s", e)
            return {'score': None, 'error': str(e)}

    # ------------------------------------------------------------------
    # 分數 -> 標籤
    # ------------------------------------------------------------------
    @staticmethod
    def _score_to_label(score: float) -> str:
        """Convert numeric score to Fear/Greed label."""
        if score < 20:
            return 'Extreme Fear'
        elif score < 40:
            return 'Fear'
        elif score < 60:
            return 'Neutral'
        elif score < 80:
            return 'Greed'
        else:
            return 'Extreme Greed'


# ===========================================================================
# __main__ -- 測試區塊
# ===========================================================================
if __name__ == '__main__':
    # 設定 logging 到 console
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    print('=' * 60)
    print('  TAIFEX Data & Taiwan Fear/Greed Index Test')
    print('=' * 60)

    # --- TAIFEXData 測試 ---
    taifex = TAIFEXData()

    print('\n--- Futures Basis (正逆價差) ---')
    basis = taifex.get_futures_basis()
    for k, v in basis.items():
        print(f'  {k}: {v}')

    print('\n--- Put/Call Ratio (未平倉) ---')
    pcr = taifex.get_put_call_ratio()
    for k, v in pcr.items():
        print(f'  {k}: {v}')

    print('\n--- Institutional Futures (三大法人台指期) ---')
    inst = taifex.get_institutional_futures()
    for k, v in inst.items():
        print(f'  {k}: {v}')

    print('\n--- Large Trader Positions (大額交易人) ---')
    large = taifex.get_large_trader_positions()
    for k, v in large.items():
        print(f'  {k}: {v}')

    # --- TaiwanFearGreedIndex 測試 ---
    print('\n' + '=' * 60)
    print('  Taiwan Fear & Greed Index')
    print('=' * 60)

    fgi = TaiwanFearGreedIndex()
    result = fgi.calculate()

    print(f'\n  Composite Score: {result["score"]}')
    print(f'  Label: {result["label"]}')
    print(f'  Available Indicators: {result["available_indicators"]}/{result["total_indicators"]}')

    print('\n  --- Component Scores ---')
    for name, detail in result['components'].items():
        score_str = str(detail.get('score', 'N/A'))
        error_str = detail.get('error', '')
        extra = f' (error: {error_str})' if error_str else ''
        print(f'  {name}: {score_str}{extra}')
        # 印出子指標詳情
        for k, v in detail.items():
            if k not in ('score', 'error'):
                print(f'    {k}: {v}')

    print('\n' + '=' * 60)
    print('  Test Complete')
    print('=' * 60)
