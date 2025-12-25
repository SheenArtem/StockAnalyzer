import pandas as pd
from FinMind.data import DataLoader

class ChipAnalyzer:
    def __init__(self):
        self.dl = DataLoader()

    def get_chip_data(self, ticker):
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

        print(f"🔍 正在抓取 {stock_id} 籌碼數據...")
        
        try:
            # 1. 三大法人買賣超 (Institutional Investors)
            df_inst = self.dl.taiwan_stock_institutional_investors(
                stock_id=stock_id,
                start_date='2023-01-01', # 抓取近一年資料
            )
            
            # 2. 融資融券 (Margin Trading)
            df_margin = self.dl.taiwan_stock_margin_purchase_short_sale(
                stock_id=stock_id,
                start_date='2023-01-01',
            )

            # 資料處理 - 三大法人
            # 轉置表格: date 為 index, name 為 columns (Foreign_Investor, Investment_Trust, Dealer)
            if not df_inst.empty:
                # 簡化名稱
                # Foreign_Investor: 外資
                # Investment_Trust: 投信
                # Dealer_Self: 自營商(自行買賣) + Dealer_Hedging: 自營商(避險) -> 合併為自營商
                
                # Pivot
                df_inst['name'] = df_inst['name'].replace({
                    'Foreign_Investor': '外資',
                    'Investment_Trust': '投信',
                    'Dealer_Self': '自營商',
                    'Dealer_Hedging': '自營商' # 簡易合併
                })
                
                # 計算買賣超 (buy - sell) 如果沒有 buy_sell 欄位
                if 'buy_sell' not in df_inst.columns:
                     if 'buy' in df_inst.columns and 'sell' in df_inst.columns:
                         df_inst['buy_sell'] = df_inst['buy'] - df_inst['sell']
                     else:
                         # fallback, maybe 'amount' or check debug output
                         pass

                # Groupby date and name to sum up Dealer values
                df_inst = df_inst.groupby(['date', 'name'])['buy_sell'].sum().unstack(fill_value=0)
                df_inst.index = pd.to_datetime(df_inst.index)
                
                # 計算三大法人合計
                df_inst['三大法人合計'] = df_inst.sum(axis=1)

            # 資料處理 - 融資融券
            if not df_margin.empty:
                df_margin['date'] = pd.to_datetime(df_margin['date'])
                df_margin.set_index('date', inplace=True)
                # 我們主要看: 
                # MarginPurchaseLimit (融資餘額) -> 看散戶多單
                # ShortSaleLimit (融券餘額) -> 看散戶空單 (或軋空力道)
                # 修正: FinMind 欄位是 MarginPurchaseTodayBalance (融資今日餘額), ShortSaleTodayBalance (融券今日餘額)
                keep_cols = ['MarginPurchaseTodayBalance', 'ShortSaleTodayBalance']
                df_margin = df_margin[keep_cols]
                df_margin.columns = ['融資餘額', '融券餘額']

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
