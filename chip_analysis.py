
import logging
import pandas as pd
import datetime
from typing import Dict, Optional, Tuple
from cache_manager import get_finmind_loader

logger = logging.getLogger(__name__)


class ChipFetchError(Exception):
    """Raised by ChipAnalyzer.fetch_chip() when chip data cannot be retrieved.

    Use `get_chip_data()` (tuple return) if you need to inspect partial failure;
    use `fetch_chip()` (dict return, raises on total failure) for cleaner caller code.
    """


class ChipAnalyzer:
    def __init__(self):
        self.dl = get_finmind_loader()

    def fetch_chip(self, ticker: str, scan_mode: bool = False, force_update: bool = False) -> Dict:
        """取得籌碼面數據（乾淨 API，H5 2026-04-23 新增）。

        2026-04-22 三連 bug 之一是 caller 忘了 unpack tuple → 整個 tuple 當成
        dict 傳下去 → AttributeError 活到排程才爆。此 method 回傳純 dict，
        caller 不可能踩到 tuple-unpack footgun。

        Raises:
            ChipFetchError: 完全抓取失敗（無任何資料集可用）
        """
        data, err = self.get_chip_data(ticker, force_update=force_update, scan_mode=scan_mode)
        if data is None:
            raise ChipFetchError(err or f"Failed to fetch chip data for {ticker}")
        return data

    def get_chip_data(
        self, ticker: str, force_update: bool = False, scan_mode: bool = False,
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        取得籌碼面數據 (三大法人 + 融資融券)

        Returns:
            Tuple[Optional[Dict], Optional[str]]:
              - data: dict with keys institutional/margin/day_trading/shareholding/sbl
                      (None on total failure)
              - error: combined error string for partial/total failure (None on success)

        ⚠️ Tuple return. Callers 必須 unpack：`data, err = ca.get_chip_data(...)`.
        若不需要 err，使用新的 `fetch_chip()` method（raises on error, returns dict）。

        scan_mode=True: 只抓 institutional（評分用），跳過 margin/day_trading/
          shareholding/sbl（這些不計分，只是 UI 顯示）。節省 4 個 FinMind 呼叫/檔。
        """
        # 確保是台股代號
        if not ticker.endswith('.TW') and ticker.isdigit():
             stock_id = ticker
        elif ticker.endswith('.TW'):
             stock_id = ticker.split('.')[0]
        else:
             return None, "非台股代號，無法抓取籌碼數據"

        # [CACHE] Initialize Cache Manager
        from cache_manager import CacheManager
        cm = CacheManager()
        
        # 嘗試讀取快取
        # Caching strategy: Separate files for Inst and Margin
        cache_key_inst = f"{stock_id}_inst"
        cache_key_margin = f"{stock_id}_margin"
        cache_key_dt = f"{stock_id}_day_trading"
        cache_key_sh = f"{stock_id}_shareholding"
        cache_key_sbl = f"{stock_id}_sbl"

        df_inst, stat_inst, date_inst = cm.load_cache(cache_key_inst, 'chip', force_reload=force_update)
        df_margin, stat_margin, date_margin = cm.load_cache(cache_key_margin, 'chip', force_reload=force_update)
        df_dt, stat_dt, date_dt = cm.load_cache(cache_key_dt, 'chip', force_reload=force_update)
        df_sh, stat_sh, date_sh = cm.load_cache(cache_key_sh, 'chip', force_reload=force_update)
        df_sbl, stat_sbl, date_sbl = cm.load_cache(cache_key_sbl, 'chip', force_reload=force_update)

        # 判斷是否為「完全命中」
        if stat_inst == "hit" and stat_margin == "hit" and stat_dt == "hit" and stat_sh == "hit" and stat_sbl == "hit":
            logger.debug("Cache hit: %s chip data (all 5 types)", stock_id)
            if not df_inst.empty: df_inst.index = pd.to_datetime(df_inst.index)
            if not df_margin.empty: df_margin.index = pd.to_datetime(df_margin.index)
            if not df_dt.empty: df_dt.index = pd.to_datetime(df_dt.index)
            if not df_sh.empty: df_sh.index = pd.to_datetime(df_sh.index)
            if not df_sbl.empty: df_sbl.index = pd.to_datetime(df_sbl.index)
            return {"institutional": df_inst, "margin": df_margin, "day_trading": df_dt, "shareholding": df_sh, "sbl": df_sbl}, None

        # 準備增量或全量抓取
        logger.info("Fetching chip data for %s (cache miss)", stock_id)

        results = {}
        errors = []

        # --- 1. Institutional Investors (三大法人) ---
        # Priority: TWSE/TPEX 官方 API → FinMind fallback
        # (統一資料源策略: 所有功能共用同一優先順序，避免不同步)
        try:
            twse_inst_ok = False

            # 1st: try TWSE/TPEX official API (free, no token, no rate limit)
            try:
                from twse_api import TWSEOpenData
                twse = TWSEOpenData()
                # Determine market: try TWSE first, then TPEX
                twse_df = twse.get_institutional_trading(stock_id, days=10)
                if twse_df.empty:
                    twse_df = twse.get_tpex_institutional(stock_id, days=10)
                if not twse_df.empty:
                    logger.debug("%s institutional: TWSE/TPEX official API (%d days)", stock_id, len(twse_df))
                    # Add '合計' alias for compatibility with analysis_engine
                    if '合計' in twse_df.columns and '三大法人合計' not in twse_df.columns:
                        twse_df['三大法人合計'] = twse_df['合計']
                    df_inst = twse_df
                    results['institutional'] = df_inst
                    twse_inst_ok = True
            except Exception as e:
                logger.debug("TWSE/TPEX institutional failed for %s: %s", stock_id, e)

            # 2nd: fallback to FinMind (for longer history or if TWSE failed)
            if not twse_inst_ok:
                logger.debug("%s institutional: FinMind fallback", stock_id)
                if stat_inst == "partial" and date_inst:
                    start_date = (date_inst + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    start_date = '2016-01-01'

                raw_inst = self.dl.taiwan_stock_institutional_investors(
                    stock_id=stock_id,
                    start_date=start_date
                )

                processed_inst = pd.DataFrame()
                if not raw_inst.empty:
                    required_cols = {'name', 'date'}
                    if not required_cols.issubset(raw_inst.columns):
                        missing = required_cols - set(raw_inst.columns)
                        raise ValueError(f"Missing required columns: {missing}")

                    raw_inst['name'] = raw_inst['name'].replace({
                        'Foreign_Investor': '外資',
                        'Investment_Trust': '投信',
                        'Dealer_Self': '自營商',
                        'Dealer_Hedging': '自營商'
                    })
                    if 'buy_sell' not in raw_inst.columns:
                        if 'buy' in raw_inst.columns and 'sell' in raw_inst.columns:
                            raw_inst['buy_sell'] = raw_inst['buy'] - raw_inst['sell']
                        else:
                            raise ValueError("Cannot derive buy_sell")

                    processed_inst = raw_inst.groupby(['date', 'name'])['buy_sell'].sum().unstack(fill_value=0)
                    processed_inst.index = pd.to_datetime(processed_inst.index)
                    processed_inst['三大法人合計'] = processed_inst.sum(axis=1)

                if stat_inst == "partial" and not df_inst.empty:
                    df_inst.index = pd.to_datetime(df_inst.index)
                    if not processed_inst.empty:
                        df_inst = pd.concat([df_inst, processed_inst])
                        df_inst = df_inst[~df_inst.index.duplicated(keep='last')]
                        df_inst.sort_index(inplace=True)
                else:
                    df_inst = processed_inst

                results['institutional'] = df_inst

        except Exception as e:
            logger.warning(f"Institutional data fetch failed for {stock_id}: {e}")
            errors.append(f"法人: {e}")

        # scan_mode: institutional 已取得，跳過其餘 4 個資料集（不計分，UI only）
        if scan_mode:
            for k in ('margin', 'day_trading', 'shareholding', 'sbl'):
                results[k] = pd.DataFrame()
            inst = results.get('institutional')
            if inst is None or inst.empty:
                return None, "scan_mode: institutional 抓取失敗"
            return results, None

        # --- 2. Margin Trading (融資融券) ---
        try:
            if stat_margin == "partial" and date_margin:
                 start_date_m = (date_margin + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                 logger.debug("%s margin: incremental from %s", stock_id, start_date_m)
            else:
                 start_date_m = '2016-01-01'
                 logger.debug("%s margin: full download", stock_id)

            raw_margin = self.dl.taiwan_stock_margin_purchase_short_sale(
                stock_id=stock_id,
                start_date=start_date_m
            )

            processed_margin = pd.DataFrame()
            if not raw_margin.empty:
                # Guard: need 'date' column
                if 'date' not in raw_margin.columns:
                    logger.warning(f"Margin data for {stock_id} missing 'date' column, skipping")
                    raise ValueError("Missing 'date' column in margin data")

                raw_margin['date'] = pd.to_datetime(raw_margin['date'])
                raw_margin.set_index('date', inplace=True)
                keep_cols = ['MarginPurchaseTodayBalance', 'ShortSaleTodayBalance', 'MarginPurchaseLimit']
                avail_cols = [c for c in keep_cols if c in raw_margin.columns]
                if avail_cols:
                    processed_margin = raw_margin[avail_cols].copy()
                    col_map = {
                        'MarginPurchaseTodayBalance': '融資餘額',
                        'ShortSaleTodayBalance': '融券餘額',
                        'MarginPurchaseLimit': '融資限額'
                    }
                    processed_margin.rename(columns=col_map, inplace=True)
                else:
                    logger.warning(f"Margin data for {stock_id} has none of expected columns {keep_cols}, skipping")

            # Merge with Cache
            if stat_margin == "partial" and not df_margin.empty:
                df_margin.index = pd.to_datetime(df_margin.index)
                if not processed_margin.empty:
                    df_margin = pd.concat([df_margin, processed_margin])
                    df_margin = df_margin[~df_margin.index.duplicated(keep='last')]
                    df_margin.sort_index(inplace=True)
            else:
                df_margin = processed_margin

            results['margin'] = df_margin
        except Exception as e:
            logger.warning(f"Margin data fetch failed for {stock_id}: {e}")
            errors.append(f"融資融券: {e}")

        # --- 3. Day Trading (現股當沖) ---
        try:
            if stat_dt == "partial" and date_dt:
                 start_date_d = (date_dt + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                 logger.debug("%s day_trading: incremental from %s", stock_id, start_date_d)
            else:
                 start_date_d = '2016-01-01'
                 logger.debug("%s day_trading: full download", stock_id)

            raw_dt = self.dl.taiwan_stock_day_trading(
                stock_id=stock_id,
                start_date=start_date_d
            )

            processed_dt = pd.DataFrame()
            if not raw_dt.empty:
                # Guard: need 'date' column
                if 'date' not in raw_dt.columns:
                    logger.warning(f"Day trading data for {stock_id} missing 'date' column, skipping")
                    raise ValueError("Missing 'date' column in day trading data")

                raw_dt['date'] = pd.to_datetime(raw_dt['date'])
                raw_dt.set_index('date', inplace=True)
                keep_cols = ['Volume', 'BuyAmount', 'SellAmount']
                avail_cols = [c for c in keep_cols if c in raw_dt.columns]
                if avail_cols:
                    processed_dt = raw_dt[avail_cols].copy()
                    processed_dt.rename(columns={'Volume': 'DayTradingVolume', 'BuyAmount': 'DT_Buy', 'SellAmount': 'DT_Sell'}, inplace=True)
                else:
                    logger.warning(f"Day trading data for {stock_id} has none of expected columns {keep_cols}, skipping")

            # Merge with Cache
            if stat_dt == "partial" and not df_dt.empty:
                df_dt.index = pd.to_datetime(df_dt.index)
                if not processed_dt.empty:
                    df_dt = pd.concat([df_dt, processed_dt])
                    df_dt = df_dt[~df_dt.index.duplicated(keep='last')]
                    df_dt.sort_index(inplace=True)
            else:
                df_dt = processed_dt

            results['day_trading'] = df_dt
        except Exception as e:
            logger.warning(f"Day trading data fetch failed for {stock_id}: {e}")
            errors.append(f"當沖: {e}")

        # --- 4. Shareholding (外資持股) ---
        try:
            if stat_sh == "partial" and date_sh:
                 start_date_s = (date_sh + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                 logger.debug("%s shareholding: incremental from %s", stock_id, start_date_s)
            else:
                 start_date_s = '2016-01-01'
                 logger.debug("%s shareholding: full download", stock_id)

            raw_sh = self.dl.taiwan_stock_shareholding(
                stock_id=stock_id,
                start_date=start_date_s
            )

            processed_sh = pd.DataFrame()
            if not raw_sh.empty:
                # Guard: need 'date' column
                if 'date' not in raw_sh.columns:
                    logger.warning(f"Shareholding data for {stock_id} missing 'date' column, skipping")
                    raise ValueError("Missing 'date' column in shareholding data")

                raw_sh['date'] = pd.to_datetime(raw_sh['date'])
                raw_sh.set_index('date', inplace=True)
                if 'ForeignInvestmentSharesRatio' in raw_sh.columns:
                    processed_sh = raw_sh[['ForeignInvestmentSharesRatio']].copy()
                    processed_sh.rename(columns={'ForeignInvestmentSharesRatio': 'ForeignHoldingRatio'}, inplace=True)
                else:
                    logger.warning(f"Shareholding data for {stock_id} missing 'ForeignInvestmentSharesRatio' column, skipping")

            # Merge with Cache
            if stat_sh == "partial" and not df_sh.empty:
                df_sh.index = pd.to_datetime(df_sh.index)
                if not processed_sh.empty:
                    df_sh = pd.concat([df_sh, processed_sh])
                    df_sh = df_sh[~df_sh.index.duplicated(keep='last')]
                    df_sh.sort_index(inplace=True)
            else:
                df_sh = processed_sh

            results['shareholding'] = df_sh
        except Exception as e:
            logger.warning(f"Shareholding data fetch failed for {stock_id}: {e}")
            errors.append(f"持股: {e}")

        # --- 5. Securities Lending (借券賣出) ---
        try:
            if stat_sbl == "partial" and date_sbl:
                start_date_sbl = (date_sbl + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                logger.debug("%s sbl: incremental from %s", stock_id, start_date_sbl)
            else:
                start_date_sbl = '2016-01-01'
                logger.debug("%s sbl: full download", stock_id)

            raw_sbl = self.dl.get_data(
                dataset="TaiwanDailyShortSaleBalances",
                data_id=stock_id,
                start_date=start_date_sbl
            )

            processed_sbl = pd.DataFrame()
            if not raw_sbl.empty and 'date' in raw_sbl.columns:
                raw_sbl['date'] = pd.to_datetime(raw_sbl['date'])
                raw_sbl.set_index('date', inplace=True)
                keep_cols = [
                    'SBLShortSalesCurrentDayBalance',
                    'SBLShortSalesShortSales',
                    'SBLShortSalesReturns',
                    'SBLShortSalesAdjustments',
                ]
                avail_cols = [c for c in keep_cols if c in raw_sbl.columns]
                if avail_cols:
                    processed_sbl = raw_sbl[avail_cols].copy()
                    col_map = {
                        'SBLShortSalesCurrentDayBalance': '借券賣出餘額',
                        'SBLShortSalesShortSales': '借券賣出',
                        'SBLShortSalesReturns': '借券還券',
                        'SBLShortSalesAdjustments': '借券調整',
                    }
                    processed_sbl.rename(columns=col_map, inplace=True)

            if stat_sbl == "partial" and not df_sbl.empty:
                df_sbl.index = pd.to_datetime(df_sbl.index)
                if not processed_sbl.empty:
                    df_sbl = pd.concat([df_sbl, processed_sbl])
                    df_sbl = df_sbl[~df_sbl.index.duplicated(keep='last')]
                    df_sbl.sort_index(inplace=True)
            else:
                df_sbl = processed_sbl

            results['sbl'] = df_sbl
        except Exception as e:
            logger.warning(f"SBL data fetch failed for {stock_id}: {e}")
            errors.append(f"借券: {e}")

        # --- All failed: return error ---
        if not results:
            combined_err = "; ".join(errors)
            return None, f"FinMind 資料抓取全部失敗: {combined_err}"

        # --- Save successfully fetched data to cache ---
        if 'institutional' in results and not results['institutional'].empty:
            cm.save_cache(cache_key_inst, results['institutional'], 'chip')
        if 'margin' in results and not results['margin'].empty:
            cm.save_cache(cache_key_margin, results['margin'], 'chip')
        if 'day_trading' in results and not results['day_trading'].empty:
            cm.save_cache(cache_key_dt, results['day_trading'], 'chip')
        if 'shareholding' in results and not results['shareholding'].empty:
            cm.save_cache(cache_key_sh, results['shareholding'], 'chip')
        if 'sbl' in results and not results['sbl'].empty:
            cm.save_cache(cache_key_sbl, results['sbl'], 'chip')

        # --- Return partial results with combined error string (or None if no errors) ---
        error_str = "; ".join(errors) if errors else None
        return results, error_str

if __name__ == "__main__":
    # Test
    analyzer = ChipAnalyzer()
    data, err = analyzer.get_chip_data("2330")
    if data:
        if err:
            print(f"Partial errors: {err}")
        if 'institutional' in data:
            print("Inst Data Tail:")
            print(data['institutional'].tail())
        if 'margin' in data:
            print("Margin Data Tail:")
            print(data['margin'].tail())
        if 'day_trading' in data:
            print("Day Trading Tail:")
            print(data['day_trading'].tail())
        if 'shareholding' in data:
            print("Shareholding Tail:")
            print(data['shareholding'].tail())
    else:
        print(err)
