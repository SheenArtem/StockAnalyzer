
import pandas as pd
from FinMind.data import DataLoader

class ChipAnalyzer:
    def __init__(self):
        self.dl = DataLoader()

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
        
        df_inst, stat_inst, date_inst = cm.load_cache(cache_key_inst, 'chip', force_reload=force_update)
        df_margin, stat_margin, date_margin = cm.load_cache(cache_key_margin, 'chip', force_reload=force_update)
        
        # 判斷是否為「完全命中」
        if stat_inst == "hit" and stat_margin == "hit":
            print(f"⚡ [Cache Hit] 讀取 {stock_id} 籌碼快取")
            if not df_inst.empty: df_inst.index = pd.to_datetime(df_inst.index)
            if not df_margin.empty: df_margin.index = pd.to_datetime(df_margin.index)
            return {"institutional": df_inst, "margin": df_margin}, None

        # 準備增量或全量抓取
        print(f"🔍 正在抓取 {stock_id} 籌碼數據 (FinMind)...")
        
        try:
            # --- 1. Institutional Investors (三大法人) ---
            new_inst = pd.DataFrame()
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
            
            # --- 2. Margin Trading (融資融券) ---
            new_margin = pd.DataFrame()
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

            # --- Process Institutional Data ---
            # Reuse logic: Transform raw_inst -> df_inst (formatted)
            # But wait, raw_inst needs to be pivoted FIRST before merging with cache? 
            # OR we merge raw then pivot?
            # Creating a helper or inline processing. 
            # To allow merging with cache (which is already pivoted), we should process raw_inst first.
            
            processed_inst = pd.DataFrame()
            if not raw_inst.empty:
                # ... Original Processing Logic ...
                # Pivot
                raw_inst['name'] = raw_inst['name'].replace({
                    'Foreign_Investor': '外資',
                    'Investment_Trust': '投信',
                    'Dealer_Self': '自營商',
                    'Dealer_Hedging': '自營商'
                })
                # Check buy_sell
                if 'buy_sell' not in raw_inst.columns:
                     if 'buy' in raw_inst.columns and 'sell' in raw_inst.columns:
                         raw_inst['buy_sell'] = raw_inst['buy'] - raw_inst['sell']

                processed_inst = raw_inst.groupby(['date', 'name'])['buy_sell'].sum().unstack(fill_value=0)
                processed_inst.index = pd.to_datetime(processed_inst.index)
                processed_inst['三大法人合計'] = processed_inst.sum(axis=1)
            
            # Merge with Cache if partial
            if stat_inst == "partial" and not df_inst.empty:
                df_inst.index = pd.to_datetime(df_inst.index) # Ensure index type
                if not processed_inst.empty:
                    # Concat
                    df_inst = pd.concat([df_inst, processed_inst])
                    df_inst = df_inst[~df_inst.index.duplicated(keep='last')]
                    df_inst.sort_index(inplace=True)
            else:
                # Full download or cache was empty
                df_inst = processed_inst

            # --- Process Margin Data ---
            processed_margin = pd.DataFrame()
            if not raw_margin.empty:
                raw_margin['date'] = pd.to_datetime(raw_margin['date'])
                raw_margin.set_index('date', inplace=True)
                keep_cols = ['MarginPurchaseTodayBalance', 'ShortSaleTodayBalance']
                # Check if columns exist (sometimes API returns partial)
                avail_cols = [c for c in keep_cols if c in raw_margin.columns]
                if avail_cols:
                    processed_margin = raw_margin[avail_cols].copy()
                    processed_margin.columns = ['融資餘額', '融券餘額'] if len(avail_cols)==2 else avail_cols
            
            # Merge with Cache
            if stat_margin == "partial" and not df_margin.empty:
                df_margin.index = pd.to_datetime(df_margin.index)
                if not processed_margin.empty:
                    df_margin = pd.concat([df_margin, processed_margin])
                    df_margin = df_margin[~df_margin.index.duplicated(keep='last')]
                    df_margin.sort_index(inplace=True)
            else:
                df_margin = processed_margin

            # [CACHE] Save Updated Data (Only if we have something)
            if not df_inst.empty:
                cm.save_cache(cache_key_inst, df_inst, 'chip')
            if not df_margin.empty:
                cm.save_cache(cache_key_margin, df_margin, 'chip')

            return {
                "institutional": df_inst,
                "margin": df_margin
            }, None

        except Exception as e:
            return None, f"FinMind 資料抓取失敗: {str(e)}"

if __name__ == "__main__":
    # Test
    analyzer = ChipAnalyzer()
    data, err = analyzer.get_chip_data("2330")
    if data:
        print("Inst Data Head:")
        print(data['institutional'].tail())
        print("Margin Data Head:")
        print(data['margin'].tail())
    else:
        print(err)
