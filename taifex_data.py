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
from typing import Dict, Optional, Any, Tuple

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
        get_full_session_quote()   - 台指期(全) 日盤結算 + 夜盤收盤 (隔夜訊號)
        get_put_call_ratio()       - 選擇權 Put/Call Ratio (未平倉)
        get_atm_put_premium()      - 近月 ATM PUT 權利金 + skew + top-OI (避險成本)
        get_minifutures_oi_ratio() - 小台/大台近月 OI 比 (散戶倉位 proxy)
        get_institutional_futures()- 三大法人台指期未平倉
        get_options_institutional()- 三大法人 TXO 買賣權未平倉淨額
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
            'data_date': None,
        }

        try:
            today = datetime.now()
            futures_price = 0.0
            matched_date = None

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
                    matched_date = d.date()
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
                    'data_date': matched_date,
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
    # 台指期(全) -- 日盤 + 夜盤(盤後時段) 報價
    # ------------------------------------------------------------------
    def get_full_session_quote(self) -> Dict[str, Any]:
        """
        台指期(全)：近月日盤 + 夜盤(盤後時段)報價。

        Primary：mis.taifex 即時報價 (getQuoteDetail)。夜盤進行中
        (15:00~次日05:00) 回最新成交價，收盤後回該時段收盤價——解決
        dlFutDataDown 只有「已收盤時段」害傍晚仍掛今晨舊夜盤的時效問題
        (2026-06-11 用戶回報)。
        Fallback：dlFutDataDown EOD CSV (mis 失敗時)。

        Returns:
            dict: day_settle / day_date / day_chg_pct (近月日盤；day_settle=
                      最近一次日盤結算價，即夜盤漲跌基準)
                  night_close / night_date / night_chg / night_chg_pct (近月夜盤)
                  night_base (夜盤漲跌基準=日盤結算) / night_time (HHMMSS)
                  night_live (bool, 夜盤進行中) / source ('mis'/'csv')
                  失敗時各值為 0.0 / None。
        """
        cached = self._cache.get('full_session_quote')
        if cached is not None:
            return cached

        result = self._full_session_from_mis()
        if result is None:
            result = self._full_session_from_csv()

        self._cache.set('full_session_quote', result)
        return result

    # mis.taifex 即時報價 (官網期貨報價頁 XHR，2026-06-11 逆向)。
    # SymbolID = TXF{月碼}{西元年尾數}-{時段}；月碼 A=1月..L=12月 (非 CME 碼)，
    # 時段 -F=日盤 / -M=盤後。CRefPrice = 該時段漲跌基準 (-M 的 ref = 當日日盤結算價)。
    _MIS_QUOTE_URL = 'https://mis.taifex.com.tw/futures/api/getQuoteDetail'
    _MIS_MONTH_CODES = 'ABCDEFGHIJKL'

    def _full_session_from_mis(self) -> Optional[Dict[str, Any]]:
        """mis.taifex getQuoteDetail 一次抓近 3 個月份 × 日盤/盤後共 6 個 symbol，
        各時段取 (tick 日期最新, 成交量最大) = 近月 (換月週/結算日自然換手)。
        失敗回 None 由 caller fallback CSV。"""
        def _f(s) -> Optional[float]:
            try:
                v = float(str(s).strip())
                return v if v != 0.0 else None  # 0.00 = 未成交月份
            except (ValueError, TypeError):
                return None

        try:
            now = datetime.now()
            symbols = []
            for k in range(3):
                mm0 = now.month - 1 + k
                yy = now.year + mm0 // 12
                code = f"TXF{self._MIS_MONTH_CODES[mm0 % 12]}{yy % 10}"
                symbols += [f"{code}-F", f"{code}-M"]

            resp = self._session.post(
                self._MIS_QUOTE_URL, json={'SymbolID': symbols},
                headers={'Referer': 'https://mis.taifex.com.tw/futures/'},
                timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            j = resp.json()
            if str(j.get('RtCode')) != '0':
                raise ValueError(f"RtCode={j.get('RtCode')}")
            quotes = (j.get('RtData') or {}).get('QuoteList') or []

            def _pick(suffix):
                """同時段取 (tick 日期最新, 量最大) 列。回 (date, quote, last, ref) 或 None。"""
                best = None
                for q in quotes:
                    if not str(q.get('SymbolID', '')).endswith(suffix):
                        continue
                    last = _f(q.get('CLastPrice'))
                    try:
                        d = datetime.strptime(str(q.get('CDate', '')).strip(), '%Y%m%d').date()
                    except ValueError:
                        continue
                    if last is None:
                        continue
                    vol = int(_f(q.get('CTotalVolume')) or 0)
                    if best is None or (d, vol) > best[0]:
                        best = ((d, vol), q, last, _f(q.get('CRefPrice')))
                return best

            day = _pick('-F')
            night = _pick('-M')
            if day is None and night is None:
                raise ValueError("no usable quote rows")

            result = {
                'day_settle': 0.0, 'day_date': None, 'day_chg_pct': None,
                'night_close': 0.0, 'night_date': None,
                'night_chg': None, 'night_chg_pct': None,
                'night_base': None, 'night_time': None, 'night_live': False,
                'source': 'mis',
            }
            if day is not None:
                (_d, _v), q, last, ref = day
                result['day_date'] = _d
                result['day_settle'] = round(last, 2)  # 無夜盤 ref 時的近似值，下方覆寫
                if ref:
                    result['day_chg_pct'] = round((last - ref) / ref * 100, 2)
            if night is not None:
                (_d, _v), q, last, ref = night
                result['night_close'] = round(last, 2)
                result['night_date'] = _d
                result['night_time'] = str(q.get('CTime', '')).strip() or None
                if ref:
                    result['night_base'] = round(ref, 2)
                    # -M 的 CRefPrice 即最近一次日盤結算價 (實測 2026-06-11: 43219 ✓)
                    result['day_settle'] = round(ref, 2)
                    result['night_chg'] = round(last - ref, 2)
                    result['night_chg_pct'] = round((last - ref) / ref * 100, 2)
                # 夜盤進行中 = 現在時間在 15:00~次日 05:05 窗內 且 最新 tick 是今天
                in_window = now.hour >= 15 or now.hour < 5 or (now.hour == 5 and now.minute <= 5)
                result['night_live'] = bool(in_window and _d == now.date())

            logger.info(
                "Full session quote (mis): day=%.0f (%s), night=%.0f (%s %s, %+.2f%%, live=%s)",
                result['day_settle'], result['day_date'],
                result['night_close'], result['night_date'],
                result['night_time'], result['night_chg_pct'] or 0.0,
                result['night_live'])
            return result

        except Exception as e:
            logger.warning("mis.taifex full session quote failed (%s), fallback to CSV", e)
            return None

    def _full_session_from_csv(self) -> Dict[str, Any]:
        """dlFutDataDown EOD CSV fallback (原 2026-06-06 實作)。

        同一 CSV 以「交易時段」欄區分 一般/盤後；盤後時段 (15:00~次日05:00)
        的交易日 = 次一交易日，其漲跌基準 = 前一日盤結算價 → 隔夜 gap 訊號。
        限制：只有已收盤時段，進行中的夜盤不在檔內。
        """
        result = {
            'day_settle': 0.0, 'day_date': None, 'day_chg_pct': None,
            'night_close': 0.0, 'night_date': None,
            'night_chg': None, 'night_chg_pct': None,
            'night_base': None, 'night_time': None, 'night_live': False,
            'source': 'csv',
        }

        def _f(s: str) -> Optional[float]:
            s = s.strip().replace('%', '')
            if not s or s == '-':
                return None
            try:
                return float(s)
            except ValueError:
                return None

        try:
            today = datetime.now()
            url = 'https://www.taifex.com.tw/cht/3/dlFutDataDown'
            payload = {
                'down_type': '1',
                'commodity_id': 'TX',
                # 範圍涵蓋 週末/連假：往前 4 天抓最近日盤、往後 4 天抓
                # 已掛在次一交易日名下的最新夜盤
                'queryStartDate': (today - timedelta(days=4)).strftime('%Y/%m/%d'),
                'queryEndDate': (today + timedelta(days=4)).strftime('%Y/%m/%d'),
            }
            resp = self._session.post(url, data=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = 'big5'  # CSV 為 Big5；需正確解碼「交易時段」中文欄

            # 各時段分開挑：日期最新 → 同日成交量最大列 = 近月 (價差單自然出局)
            best = {'一般': (None, 0, None), '盤後': (None, 0, None)}  # (date, vol, fields)
            for line in resp.text.strip().split('\n')[1:]:
                fields = [f.strip() for f in line.split(',')]
                if len(fields) < 18 or '/' in fields[2]:  # 跳過價差單 (202606/202607)
                    continue
                session = fields[17]
                if session not in best:
                    continue
                try:
                    d = datetime.strptime(fields[0], '%Y/%m/%d').date()
                except ValueError:
                    continue
                vol = int(_f(fields[9]) or 0)
                cur_date, cur_vol, _ = best[session]
                if cur_date is None or d > cur_date or (d == cur_date and vol > cur_vol):
                    best[session] = (d, vol, fields)

            d_date, _, d_fields = best['一般']
            if d_fields is not None:
                # 優先結算價 (idx 10)，無則收盤價 (idx 6)
                price = _f(d_fields[10]) or _f(d_fields[6])
                if price:
                    result['day_settle'] = round(price, 2)
                    result['day_date'] = d_date
                    result['day_chg_pct'] = _f(d_fields[8])

            n_date, _, n_fields = best['盤後']
            if n_fields is not None:
                price = _f(n_fields[6])  # 夜盤無結算價，用收盤價
                if price:
                    result['night_close'] = round(price, 2)
                    result['night_date'] = n_date
                    result['night_chg'] = _f(n_fields[7])
                    result['night_chg_pct'] = _f(n_fields[8])
                    # CSV 夜盤漲跌基準 = 前一日盤結算價 (close - chg 反推)
                    if result['night_chg'] is not None:
                        result['night_base'] = round(price - result['night_chg'], 2)

            logger.info("Full session quote (csv): day=%.0f (%s), night=%.0f (%s, %+.2f%%)",
                        result['day_settle'], result['day_date'],
                        result['night_close'], result['night_date'],
                        result['night_chg_pct'] or 0.0)

        except requests.RequestException as e:
            logger.warning("Failed to fetch full session quote (network): %s", e)
        except Exception as e:
            logger.error("Failed to fetch full session quote: %s", e, exc_info=True)

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
            'data_date': None,
        }

        try:
            today = datetime.now()
            call_oi = 0
            put_oi = 0
            matched_date = None

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
                    matched_date = d.date()
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
                'data_date': matched_date,
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
    # 近月 ATM 賣權權利金 + 5% OTM put skew (避險成本指標)
    # ------------------------------------------------------------------
    def get_atm_put_premium(self, top_oi_n: int = 5) -> Dict[str, Any]:
        """
        Fetch near-month ATM put premium (% of index) + OTM5 skew + top-OI strikes.

        避險成本指標：
          - atm_put_pct 高 = 買 PUT 避險權利金貴 = 市場恐慌定價
          - put_skew 高 = OTM PUT 比 ATM PUT 貴的程度（左尾恐慌定價，崩盤前常先升）
          - top_put_oi_strikes = 大戶買 PUT 集中的履約價 = 市場默契支撐線

        與 get_put_call_ratio 共用 TXO CSV 端點，僅取月選（排除週選 Wn）+ 一般盤。
        Reference 用 spot_price (TWII) 優先，無則 fallback futures_price。

        Args:
            top_oi_n: 回傳前 N 大 PUT OI strikes（預設 5）

        Returns:
            dict with keys:
              data_date, near_month, reference,
              atm_strike, atm_put_close, atm_put_pct,
              otm5_strike, otm5_put_close, put_skew,
              top_put_oi_strikes: List[Tuple[strike:int, oi:int]] 由高到低排序
        """
        cached = self._cache.get('atm_put_premium')
        if cached is not None:
            return cached

        result = {
            'data_date': None,
            'near_month': None,
            'reference': 0.0,
            'atm_strike': 0,
            'atm_put_close': 0.0,
            'atm_put_pct': 0.0,
            'otm5_strike': 0,
            'otm5_put_close': 0.0,
            'put_skew': 0.0,
            'top_put_oi_strikes': [],
        }

        try:
            # 1. 取 reference price（spot 優先，futures 備援）
            basis = self.get_futures_basis()
            reference = basis.get('spot_price') or basis.get('futures_price') or 0.0
            if reference <= 0:
                logger.warning("ATM PUT premium: no reference price available")
                return result

            # 2. 5 天回溯找最新有資料的交易日
            today = datetime.now()
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

                # 3. 收集月選 (排除 W 週選) + 一般盤 (排除盤後) 的 PUT records
                # CSV cols: [0]date [1]contract [2]month_str [3]strike [4]PC
                #          [5]open [6]high [7]low [8]close [9]volume [10]settle
                #          [11]OI ... [17]session
                put_records = []  # (month_str, strike, close, oi)
                for line in lines[1:]:
                    fields = line.split(',')
                    if len(fields) < 18:
                        continue
                    if fields[4].strip() != '賣權':
                        continue
                    month_str = fields[2].strip()
                    if 'W' in month_str:  # 排除週選
                        continue
                    session = fields[17].strip()
                    if session and session != '一般':  # 排除盤後 session
                        continue
                    try:
                        strike = float(fields[3].strip())
                        close_str = fields[8].strip()
                        if close_str in ('-', ''):
                            continue
                        close_f = float(close_str)
                        oi_str = fields[11].strip()
                        oi_int = int(oi_str) if oi_str and oi_str != '-' else 0
                        put_records.append((month_str, strike, close_f, oi_int))
                    except (ValueError, IndexError):
                        continue

                if not put_records:
                    continue

                # 4. 近月 = 字典序最早的 month_str (e.g. '202605' < '202606')
                near_month = sorted(set(r[0] for r in put_records))[0]
                near_records = [r for r in put_records if r[0] == near_month]
                if not near_records:
                    continue

                # 4a. 對齊 reference 到 matched_date (修 1-day-off bug, 2026-05-09)
                #     原 reference 是「當下」TWII spot；若 d != today (e.g. archiver
                #     跑 T+1 早上拿到 T 日 TXO CSV)，reference 會比 PUT 資料晚 1 天，
                #     ATM strike 算錯。改 fetch d 當天 ^TWII close 重新對齊。
                aligned_ref = self._get_taiex_close_for_date(d.date())
                if aligned_ref > 0:
                    reference = aligned_ref
                else:
                    logger.warning(
                        "ATM PUT: failed to align reference to %s, "
                        "fallback current spot %.2f", d.date(), reference)

                # 5. ATM = 履約價最接近 reference; OTM5 = 最接近 reference * 0.95
                atm_record = min(near_records, key=lambda r: abs(r[1] - reference))
                otm5_target = reference * 0.95
                otm5_record = min(near_records, key=lambda r: abs(r[1] - otm5_target))

                atm_pct = (atm_record[2] / reference) * 100
                put_skew = (otm5_record[2] / atm_record[2]) if atm_record[2] > 0 else 0.0

                # 6. Top-OI strikes（排除 OI=0 的稀薄 strike）
                near_with_oi = [(r[1], r[3]) for r in near_records if r[3] > 0]
                near_with_oi.sort(key=lambda x: x[1], reverse=True)
                top_strikes = [(int(s), int(oi)) for s, oi in near_with_oi[:top_oi_n]]

                result = {
                    'data_date': d.date(),
                    'near_month': near_month,
                    'reference': round(reference, 2),
                    'atm_strike': int(atm_record[1]),
                    'atm_put_close': round(atm_record[2], 2),
                    'atm_put_pct': round(atm_pct, 3),
                    'otm5_strike': int(otm5_record[1]),
                    'otm5_put_close': round(otm5_record[2], 2),
                    'put_skew': round(put_skew, 3),
                    'top_put_oi_strikes': top_strikes,
                }

                self._cache.set('atm_put_premium', result)
                logger.info(
                    "ATM PUT premium: ref=%.0f near=%s ATM=%d close=%.1f (%.3f%%) "
                    "OTM5=%d close=%.1f skew=%.3f top_OI=%s",
                    reference, near_month, result['atm_strike'], result['atm_put_close'],
                    result['atm_put_pct'], result['otm5_strike'], result['otm5_put_close'],
                    result['put_skew'],
                    [s for s, _ in top_strikes[:3]],
                )
                break

        except requests.RequestException as e:
            logger.warning("Failed to fetch ATM PUT premium (network): %s", e)
        except Exception as e:
            logger.error("Failed to fetch ATM PUT premium: %s", e, exc_info=True)

        return result

    # ------------------------------------------------------------------
    # 小台 (MTX) 對大台 (TX) 近月 OI 比例 — 散戶倉位 proxy
    # ------------------------------------------------------------------
    def _fetch_futures_near_oi(self, commodity_id: str, date_str: str) -> Tuple[Optional[str], Optional[int]]:
        """Fetch near-month OI for a single futures commodity (TX/MTX) on date.

        Filters: month-only (no W weekly) + 一般 session. Sums OI across all
        rows of the near month (usually 1 row but defensive sum).

        Returns:
            (near_month_str, oi_int) or (None, None) if no data.
        """
        url = 'https://www.taifex.com.tw/cht/3/dlFutDataDown'
        payload = {
            'down_type': '1',
            'commodity_id': commodity_id,
            'queryStartDate': date_str,
            'queryEndDate': date_str,
        }
        resp = self._session.post(url, data=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        lines = resp.text.strip().split('\n')
        if len(lines) < 3:
            return None, None
        # Futures CSV cols: [0]date [1]contract [2]month_str [3]open [4]high
        #   [5]low [6]close [7]chg [8]chg% [9]volume [10]settle [11]OI
        #   [12]bid [13]ask [14]hist_high [15]hist_low [16]suspended [17]session ...
        records: list = []  # (month_str, oi)
        for line in lines[1:]:
            fields = line.split(',')
            if len(fields) < 18:
                continue
            month_str = fields[2].strip()
            if 'W' in month_str:
                continue
            session = fields[17].strip()
            if session and session != '一般':
                continue
            try:
                oi_str = fields[11].strip()
                if not oi_str or oi_str == '-':
                    continue
                records.append((month_str, int(oi_str)))
            except (ValueError, IndexError):
                continue
        if not records:
            return None, None
        near = sorted(set(r[0] for r in records))[0]
        near_oi = sum(oi for m, oi in records if m == near)
        return near, near_oi

    def get_minifutures_oi_ratio(self) -> Dict[str, Any]:
        """
        小台 (MTX) 對大台 (TX) 近月 OI 比例 — 散戶倉位 proxy（反向指標經典）。

        MTX 合約規模是 TXF 1/4 (合約乘數 50 vs 200)，散戶為主用戶；TXF 法人為主。
        ratio 高 = 散戶倉位過大 = 反向訊號 (歷史頂部常見)。
        ratio 低 = 散戶撤退 = 可能落底訊號。

        Returns:
            dict with keys:
              data_date, near_month, txf_oi, mtx_oi, mtx_txf_ratio
        """
        cached = self._cache.get('minifutures_oi_ratio')
        if cached is not None:
            return cached

        result = {
            'data_date': None,
            'near_month': None,
            'txf_oi': 0,
            'mtx_oi': 0,
            'mtx_txf_ratio': 0.0,
        }

        try:
            today = datetime.now()
            for delta in range(5):
                d = today - timedelta(days=delta)
                date_str = d.strftime('%Y/%m/%d')
                try:
                    tx_month, tx_oi = self._fetch_futures_near_oi('TX', date_str)
                    mtx_month, mtx_oi = self._fetch_futures_near_oi('MTX', date_str)
                except requests.RequestException as e:
                    logger.debug("MTX/TX fetch %s network err: %s", date_str, e)
                    continue

                if not tx_oi or not mtx_oi:
                    continue
                if tx_month != mtx_month:
                    logger.warning(
                        "MTX/TX near-month mismatch: TX=%s MTX=%s; using TX",
                        tx_month, mtx_month,
                    )
                ratio = mtx_oi / tx_oi if tx_oi > 0 else 0.0
                result = {
                    'data_date': d.date(),
                    'near_month': tx_month,
                    'txf_oi': int(tx_oi),
                    'mtx_oi': int(mtx_oi),
                    'mtx_txf_ratio': round(ratio, 4),
                }
                self._cache.set('minifutures_oi_ratio', result)
                logger.info(
                    "MTX/TX OI ratio: near=%s TX_OI=%d MTX_OI=%d ratio=%.4f",
                    tx_month, tx_oi, mtx_oi, ratio,
                )
                break

        except Exception as e:
            logger.error("Failed to fetch MTX/TX OI ratio: %s", e, exc_info=True)

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
    # 三大法人 TXO 買賣權未平倉淨額 (FinMind, 2026-05-09 從 callsAndPutsDate 換)
    # ------------------------------------------------------------------
    def get_options_institutional(self) -> Dict[str, Any]:
        """
        Fetch institutional investors' TXO (台指選擇權) call/put open interest
        net positions, **via FinMind** (TaiwanOptionInstitutionalInvestors).

        三大法人選擇權持倉解讀:
          - foreign_call_net > 0 + foreign_put_net > 0 = 外資雙邊做多 (高勝率多頭訊號)
          - foreign_put_net 大幅增加 = 外資加碼避險 (大盤恐將回檔)
          - dealer_call_net 多頭 + foreign_put_net 多頭 = 自營做多但外資避險 (背離警訊)

        Data source switch (2026-05-09):
          原本 GET /cht/3/callsAndPutsDate HTML 端點實測**完全忽略 date 參數**，
          所有 query 都回傳當前最新一筆 (322032 bytes 固定)；同日跑「最新 = 今日」
          剛好正確不會炸但本質脆弱。改走 FinMind dataset
          TaiwanOptionInstitutionalInvestors，date param 真實有效，且歷史可回 2018-12+
          (見 backfill_taifex_signals_history.py)。FinMind 與 TAIFEX HTML 數值
          已交叉驗證 2026-05-08 完全一致 (foreign_call_net=1770/foreign_put_net=3551 等).

        Schema mapping:
          net_oi = long_open_interest_balance_volume - short_open_interest_balance_volume
          每日 6 row (3 inst × 2 cp)

        Returns:
            dict with keys:
              data_date,
              foreign_call_net, foreign_put_net,
              trust_call_net, trust_put_net,
              dealer_call_net, dealer_put_net,
              inst_call_net_total,    # sum of 三大法人 買權淨
              inst_put_net_total,     # sum of 三大法人 賣權淨
              inst_pc_oi_skew         # put_total - call_total (>0 法人偏空避險)
        """
        cached = self._cache.get('options_institutional')
        if cached is not None:
            return cached

        result = {
            'data_date': None,
            'foreign_call_net': 0, 'foreign_put_net': 0,
            'trust_call_net': 0, 'trust_put_net': 0,
            'dealer_call_net': 0, 'dealer_put_net': 0,
            'inst_call_net_total': 0,
            'inst_put_net_total': 0,
            'inst_pc_oi_skew': 0,
        }

        try:
            token = self._get_finmind_token()
            if not token:
                logger.warning(
                    "FINMIND token not found in env or local/.env; "
                    "options institutional fetch skipped")
                return result

            # 撈最近 ~10 個日曆日 (~7 個交易日)，找最新一個 6 inst-cp combos 齊全的日子
            end = datetime.now().date()
            start = end - timedelta(days=10)
            params = {
                'dataset': 'TaiwanOptionInstitutionalInvestors',
                'data_id': 'TXO',
                'start_date': start.isoformat(),
                'end_date': end.isoformat(),
                'token': token,
            }
            r = requests.get(
                'https://api.finmindtrade.com/api/v4/data',
                params=params, timeout=REQUEST_TIMEOUT, verify=False,
            )
            r.raise_for_status()
            raw = r.json().get('data', [])
            if not raw:
                logger.warning("FinMind options institutional empty response (%s ~ %s)",
                               start, end)
                return result

            inst_map = {'外資': 'foreign', '投信': 'trust', '自營商': 'dealer'}
            by_date: Dict[str, Dict[Tuple[str, str], int]] = {}
            for row in raw:
                d_iso = row.get('date')
                inst_key = inst_map.get(row.get('institutional_investors', ''))
                cp_str = row.get('call_put', '')
                cp_key = ('call' if cp_str == '買權'
                          else ('put' if cp_str == '賣權' else None))
                if not d_iso or not inst_key or not cp_key:
                    continue
                try:
                    long_oi = int(row.get('long_open_interest_balance_volume', 0))
                    short_oi = int(row.get('short_open_interest_balance_volume', 0))
                except (TypeError, ValueError):
                    continue
                by_date.setdefault(d_iso, {})[(inst_key, cp_key)] = long_oi - short_oi

            # 取最新有 6 combos 齊全的日子
            latest_d = None
            for d_iso in sorted(by_date.keys(), reverse=True):
                if len(by_date[d_iso]) >= 6:
                    latest_d = d_iso
                    break
            if not latest_d:
                logger.warning("No FinMind day with all 6 inst-cp combos in last 10d")
                return result

            parsed = by_date[latest_d]
            fc = parsed.get(('foreign', 'call'), 0)
            fp = parsed.get(('foreign', 'put'), 0)
            tc = parsed.get(('trust', 'call'), 0)
            tp = parsed.get(('trust', 'put'), 0)
            dc = parsed.get(('dealer', 'call'), 0)
            dp = parsed.get(('dealer', 'put'), 0)
            call_total = fc + tc + dc
            put_total = fp + tp + dp

            from datetime import date as _date
            result = {
                'data_date': _date.fromisoformat(latest_d),
                'foreign_call_net': fc, 'foreign_put_net': fp,
                'trust_call_net': tc, 'trust_put_net': tp,
                'dealer_call_net': dc, 'dealer_put_net': dp,
                'inst_call_net_total': call_total,
                'inst_put_net_total': put_total,
                'inst_pc_oi_skew': put_total - call_total,
            }
            self._cache.set('options_institutional', result)
            logger.info(
                "TXO institutional OI net (FinMind %s): foreign C/P=%d/%d "
                "trust C/P=%d/%d dealer C/P=%d/%d skew=%d",
                latest_d, fc, fp, tc, tp, dc, dp, result['inst_pc_oi_skew'],
            )

        except requests.RequestException as e:
            logger.warning("Failed to fetch TXO institutional (FinMind network): %s", e)
        except Exception as e:
            logger.error("Failed to fetch TXO institutional: %s", e, exc_info=True)

        return result

    @staticmethod
    def _get_finmind_token() -> str:
        """讀取 FinMind API token (env 或 local/.env)."""
        import os
        from pathlib import Path as _Path
        tok = (os.environ.get('FINMIND_TOKEN', '')
               or os.environ.get('FINMIND_API_TOKEN', ''))
        if tok:
            return tok
        env_path = _Path(__file__).resolve().parent / 'local' / '.env'
        if not env_path.exists():
            return ''
        try:
            for line in env_path.read_text(encoding='utf-8').splitlines():
                if 'FINMIND' in line and '=' in line:
                    return line.split('=', 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass
        return ''

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
    # 內部輔助 -- 取得指定交易日 TWII 收盤 (用於 atm_put 對齊 1-day-off fix)
    # ------------------------------------------------------------------
    def _get_taiex_close_for_date(self, target_date) -> float:
        """Fetch ^TWII close for a specific trading date via yfinance.

        Returns 0.0 if unavailable (e.g. holiday, network error). Caller should
        fallback to alternative reference price.
        """
        try:
            import yfinance as yf
            from datetime import timedelta as _td
            twii = yf.Ticker('^TWII')
            hist = twii.history(
                start=target_date.isoformat(),
                end=(target_date + _td(days=1)).isoformat(),
                auto_adjust=False,
            )
            if hist.empty:
                # 假日或非交易日：取 <= target_date 最近一筆收盤
                hist = twii.history(
                    start=(target_date - _td(days=7)).isoformat(),
                    end=(target_date + _td(days=1)).isoformat(),
                    auto_adjust=False,
                )
                if hist.empty:
                    return 0.0
            return float(hist['Close'].iloc[-1])
        except Exception as e:
            logger.debug("_get_taiex_close_for_date(%s) failed: %s", target_date, e)
            return 0.0

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

        # data_date 由最嚴格的子項 (margin 20:00) 決定，fallback 用 breadth 14:00
        margin_comp = components.get('margin_balance') or {}
        breadth_comp = components.get('market_breadth') or {}
        data_date = margin_comp.get('data_date') or breadth_comp.get('data_date')

        result = {
            'score': composite_score,
            'label': label,
            'components': components,
            'available_indicators': len(valid_scores),
            'total_indicators': len(self.WEIGHTS),
            'data_date': data_date,
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

            margin_balance = 0
            margin_prev = 0
            matched_date = None

            # 1) 先試 today（盤後 20:00 才更新）。
            # !! 非交易日 (週末/假日) 此 endpoint 回 404 -> 不可讓 raise_for_status 拋出
            #    中斷整個函式，否則下方往回 5 交易日的 fallback 永遠跑不到 (2026-05-30
            #    週六 archive 即因此存成 margin=None/0)。包 nested try 吞掉 today 失敗。
            try:
                date_str = today.strftime('%Y%m%d')
                url = (
                    'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN'
                    f'?date={date_str}&selectType=MS&response=json'
                )
                resp = requests.get(url, headers=TWSE_HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
                if resp.status_code == 200:
                    data = resp.json()
                    # MI_MARGN 欄位: [項目, 買進, 賣出, 現金償還, 前日餘額, 今日餘額]
                    # 「融資金額(仟元)」row: row[5]=今日餘額, row[4]=前日餘額
                    if 'tables' in data:
                        for table in data['tables']:
                            for row in table.get('data', []):
                                if isinstance(row, list) and len(row) >= 6 and '融資金額' in str(row[0]):
                                    try:
                                        margin_balance = int(str(row[5]).replace(',', '').strip())
                                        margin_prev = int(str(row[4]).replace(',', '').strip())
                                        matched_date = today.date()
                                    except (ValueError, IndexError):
                                        pass
                                    break
            except requests.RequestException as e:
                logger.debug("FGI margin today-fetch failed (將走 fallback): %s", e)

            # 2) today 無資料 (非交易日/盤前/20:00 前) -> 往回 5 交易日 fallback
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
                                                    matched_date = prev_date.date()
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
                'data_date': matched_date,
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
