
import logging
import pandas as pd
import datetime
from cache_manager import get_finmind_loader

logger = logging.getLogger(__name__)

class ChipAnalyzer:
    def __init__(self):
        self.dl = get_finmind_loader()

    def get_chip_data(self, ticker, force_update=False):
        """
        取得籌碼面數據 (三大法人 + 融資融券)
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
        cache_key_sh = f"{stock_id}_shareholding" # [NEW]
        
        df_inst, stat_inst, date_inst = cm.load_cache(cache_key_inst, 'chip', force_reload=force_update)
        df_margin, stat_margin, date_margin = cm.load_cache(cache_key_margin, 'chip', force_reload=force_update)
        df_dt, stat_dt, date_dt = cm.load_cache(cache_key_dt, 'chip', force_reload=force_update)
        df_sh, stat_sh, date_sh = cm.load_cache(cache_key_sh, 'chip', force_reload=force_update) # [NEW]
        
        # 判斷是否為「完全命中」
        if stat_inst == "hit" and stat_margin == "hit" and stat_dt == "hit" and stat_sh == "hit":
            print(f"⚡ [Cache Hit] 讀取 {stock_id} 籌碼快取")
            if not df_inst.empty: df_inst.index = pd.to_datetime(df_inst.index)
            if not df_margin.empty: df_margin.index = pd.to_datetime(df_margin.index)
            if not df_dt.empty: df_dt.index = pd.to_datetime(df_dt.index)
            if not df_sh.empty: df_sh.index = pd.to_datetime(df_sh.index)
            return {"institutional": df_inst, "margin": df_margin, "day_trading": df_dt, "shareholding": df_sh}, None

        # 準備增量或全量抓取
        print(f"🔍 正在抓取 {stock_id} 籌碼數據 (FinMind)...")
        
        results = {}
        errors = []

        # --- 1. Institutional Investors (三大法人) ---
        try:
            if stat_inst == "partial" and date_inst:
                 start_date = (date_inst + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                 print(f"   ↳ 增量更新法人數據 (從 {start_date})...")
            else:
                 start_date = '2016-01-01'
                 print(f"   ↳ 全量下載法人數據...")

            raw_inst = self.dl.taiwan_stock_institutional_investors(
                stock_id=stock_id,
                start_date=start_date
            )

            processed_inst = pd.DataFrame()
            if not raw_inst.empty:
                # Guard: need 'name' and 'date' columns at minimum
                required_cols = {'name', 'date'}
                if not required_cols.issubset(raw_inst.columns):
                    missing = required_cols - set(raw_inst.columns)
                    logger.warning(f"Institutional data for {stock_id} missing required columns: {missing}, skipping")
                    raise ValueError(f"Missing required columns: {missing}")

                raw_inst['name'] = raw_inst['name'].replace({
                    'Foreign_Investor': '外資',
                    'Investment_Trust': '投信',
                    'Dealer_Self': '自營商',
                    'Dealer_Hedging': '自營商'
                })
                # Guard: need buy_sell or buy+sell to compute net
                if 'buy_sell' not in raw_inst.columns:
                    if 'buy' in raw_inst.columns and 'sell' in raw_inst.columns:
                        raw_inst['buy_sell'] = raw_inst['buy'] - raw_inst['sell']
                    else:
                        logger.warning(f"Institutional data for {stock_id} has no 'buy_sell', 'buy', or 'sell' columns, skipping")
                        raise ValueError("Cannot derive buy_sell: missing 'buy_sell', 'buy', and 'sell' columns")

                processed_inst = raw_inst.groupby(['date', 'name'])['buy_sell'].sum().unstack(fill_value=0)
                processed_inst.index = pd.to_datetime(processed_inst.index)
                processed_inst['三大法人合計'] = processed_inst.sum(axis=1)

            # Merge with Cache if partial
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

        # --- 2. Margin Trading (融資融券) ---
        try:
            if stat_margin == "partial" and date_margin:
                 start_date_m = (date_margin + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                 print(f"   ↳ 增量更新融資券數據 (從 {start_date_m})...")
            else:
                 start_date_m = '2016-01-01'
                 print(f"   ↳ 全量下載融資券數據...")

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
                 print(f"   ↳ 增量更新當沖數據 (從 {start_date_d})...")
            else:
                 start_date_d = '2016-01-01'
                 print(f"   ↳ 全量下載當沖數據...")

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
                 print(f"   ↳ 增量更新持股數據 (從 {start_date_s})...")
            else:
                 start_date_s = '2016-01-01'
                 print(f"   ↳ 全量下載持股數據...")

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

        # --- All 4 failed: return error ---
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
