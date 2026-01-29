"""
美股籌碼分析模組 (US Stock Chip Analysis)

功能:
1. 機構持股比例 (Institutional Holdings)
2. ETF 持倉資訊
3. 空頭持倉 (Short Interest)
4. 內部人交易 (Insider Trading)

數據來源: Yahoo Finance API
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class USStockChipAnalyzer:
    """
    美股籌碼分析器
    提供機構持股、ETF 持倉、空頭部位等籌碼面數據
    """
    
    def __init__(self):
        self.cache = {}
    
    def get_chip_data(self, ticker, force_update=False):
        """
        取得美股籌碼數據
        
        Args:
            ticker: 股票代號 (如 AAPL, NVDA, TSM)
            force_update: 是否強制更新快取
        
        Returns:
            dict: 包含各類籌碼數據的字典
            str: 錯誤訊息 (如果有的話)
        """
        # 清理 ticker
        ticker = ticker.upper().strip()
        
        # 排除台股代號
        if ticker.endswith('.TW') or ticker.endswith('.TWO') or ticker.isdigit():
            return None, "此功能僅支援美股代號"
        
        # 檢查快取
        cache_key = f"us_{ticker}"
        if cache_key in self.cache and not force_update:
            cached_data, cache_time = self.cache[cache_key]
            # 快取有效期: 1 小時
            if datetime.now() - cache_time < timedelta(hours=1):
                return cached_data, None
        
        try:
            print(f"📥 正在取得 {ticker} 美股籌碼數據...")
            stock = yf.Ticker(ticker)
            
            result = {
                'institutional': self._get_institutional_holdings(stock, ticker),
                'major_holders': self._get_major_holders(stock, ticker),
                'short_interest': self._get_short_interest(stock, ticker),
                'insider_trades': self._get_insider_trades(stock, ticker),
                'recommendations': self._get_analyst_recommendations(stock, ticker),
            }
            
            # 儲存快取
            self.cache[cache_key] = (result, datetime.now())
            
            return result, None
            
        except Exception as e:
            logger.error(f"US Chip Data Error for {ticker}: {e}")
            return None, f"取得 {ticker} 籌碼數據失敗: {str(e)}"
    
    def _get_institutional_holdings(self, stock, ticker):
        """
        取得機構持股資訊
        
        Returns:
            dict: {
                'holders_count': 機構數量,
                'shares_held': 總持股數,
                'percent_held': 機構持股比例,
                'value': 持股市值,
                'top_holders': DataFrame (前10大機構)
            }
        """
        try:
            inst_holders = stock.institutional_holders
            info = stock.info
            
            result = {
                'holders_count': 0,
                'shares_held': 0,
                'percent_held': info.get('heldPercentInstitutions', 0) * 100,
                'value': 0,
                'top_holders': pd.DataFrame(),
                'change_vs_prior': 0  # 相較於上季度的變化
            }
            
            if inst_holders is not None and not inst_holders.empty:
                result['holders_count'] = len(inst_holders)
                result['shares_held'] = inst_holders['Shares'].sum() if 'Shares' in inst_holders.columns else 0
                result['value'] = inst_holders['Value'].sum() if 'Value' in inst_holders.columns else 0
                result['top_holders'] = inst_holders.head(10)
                
                # 計算變化 (如果有 % Change 欄位)
                if '% Change' in inst_holders.columns:
                    result['change_vs_prior'] = inst_holders['% Change'].mean()
            
            return result
            
        except Exception as e:
            logger.warning(f"Institutional holdings error for {ticker}: {e}")
            return {'holders_count': 0, 'shares_held': 0, 'percent_held': 0, 'value': 0, 'top_holders': pd.DataFrame()}
    
    def _get_major_holders(self, stock, ticker):
        """
        取得主要股東結構
        
        Returns:
            dict: {
                'insiders_percent': 內部人持股比例,
                'institutions_percent': 機構持股比例,
                'float_percent': 流通股比例,
                'shares_outstanding': 總股數,
                'float_shares': 流通股數
            }
        """
        try:
            info = stock.info
            major = stock.major_holders
            
            result = {
                'insiders_percent': info.get('heldPercentInsiders', 0) * 100,
                'institutions_percent': info.get('heldPercentInstitutions', 0) * 100,
                'float_percent': info.get('floatShares', 0) / info.get('sharesOutstanding', 1) * 100 if info.get('sharesOutstanding') else 0,
                'shares_outstanding': info.get('sharesOutstanding', 0),
                'float_shares': info.get('floatShares', 0)
            }
            
            # 嘗試從 major_holders 補充數據
            if major is not None and not major.empty:
                # major_holders 格式: index=description, value=percentage
                for idx, row in major.iterrows():
                    desc = str(row.iloc[1]).lower() if len(row) > 1 else str(idx).lower()
                    val = row.iloc[0] if len(row) > 0 else 0
                    
                    if 'insider' in desc:
                        result['insiders_percent'] = float(str(val).replace('%', ''))
                    elif 'institution' in desc:
                        result['institutions_percent'] = float(str(val).replace('%', ''))
            
            return result
            
        except Exception as e:
            logger.warning(f"Major holders error for {ticker}: {e}")
            return {'insiders_percent': 0, 'institutions_percent': 0, 'float_percent': 0, 'shares_outstanding': 0, 'float_shares': 0}
    
    def _get_short_interest(self, stock, ticker):
        """
        取得空頭持倉資訊
        
        Returns:
            dict: {
                'short_percent_of_float': 空頭比例 (占流通股),
                'short_ratio': 空頭回補天數 (Days to Cover),
                'shares_short': 空頭股數,
                'shares_short_prior': 上期空頭股數,
                'short_change_pct': 空頭變化百分比
            }
        """
        try:
            info = stock.info
            
            short_float = info.get('shortPercentOfFloat', 0)
            if short_float and short_float < 1:  # yfinance 有時返回小數
                short_float *= 100
            
            result = {
                'short_percent_of_float': short_float,
                'short_ratio': info.get('shortRatio', 0),  # Days to Cover
                'shares_short': info.get('sharesShort', 0),
                'shares_short_prior': info.get('sharesShortPriorMonth', 0),
                'short_change_pct': 0,
                'short_date': info.get('dateShortInterest', 'N/A')
            }
            
            # 計算空頭變化
            if result['shares_short_prior'] > 0:
                result['short_change_pct'] = ((result['shares_short'] - result['shares_short_prior']) / result['shares_short_prior']) * 100
            
            return result
            
        except Exception as e:
            logger.warning(f"Short interest error for {ticker}: {e}")
            return {'short_percent_of_float': 0, 'short_ratio': 0, 'shares_short': 0, 'shares_short_prior': 0, 'short_change_pct': 0}
    
    def _get_insider_trades(self, stock, ticker):
        """
        取得內部人交易記錄
        
        Returns:
            dict: {
                'recent_trades': DataFrame (最近交易記錄),
                'net_shares_purchased': 淨買入股數 (近3個月),
                'buy_count': 買入次數,
                'sell_count': 賣出次數,
                'sentiment': 內部人情緒 ('bullish', 'bearish', 'neutral')
            }
        """
        try:
            insider = stock.insider_transactions
            
            result = {
                'recent_trades': pd.DataFrame(),
                'net_shares_purchased': 0,
                'buy_count': 0,
                'sell_count': 0,
                'sentiment': 'neutral'
            }
            
            if insider is not None and not insider.empty:
                result['recent_trades'] = insider.head(20)
                
                # 分析買賣傾向
                # 注意: yfinance 的 insider_transactions 格式可能變化
                if 'Shares' in insider.columns:
                    # 正數=買入, 負數=賣出
                    result['net_shares_purchased'] = insider['Shares'].sum()
                    result['buy_count'] = len(insider[insider['Shares'] > 0])
                    result['sell_count'] = len(insider[insider['Shares'] < 0])
                elif 'Transaction' in insider.columns:
                    buys = insider[insider['Transaction'].str.contains('Buy|Purchase|Exercise', case=False, na=False)]
                    sells = insider[insider['Transaction'].str.contains('Sale|Sell|Sold', case=False, na=False)]
                    result['buy_count'] = len(buys)
                    result['sell_count'] = len(sells)
                
                # 判斷情緒
                if result['buy_count'] > result['sell_count'] * 1.5:
                    result['sentiment'] = 'bullish'
                elif result['sell_count'] > result['buy_count'] * 1.5:
                    result['sentiment'] = 'bearish'
            
            return result
            
        except Exception as e:
            logger.warning(f"Insider trades error for {ticker}: {e}")
            return {'recent_trades': pd.DataFrame(), 'net_shares_purchased': 0, 'buy_count': 0, 'sell_count': 0, 'sentiment': 'neutral'}
    
    def _get_analyst_recommendations(self, stock, ticker):
        """
        取得分析師評等
        
        Returns:
            dict: {
                'recommendation': 建議 (Buy, Hold, Sell),
                'target_price': 目標價,
                'current_price': 現價,
                'upside': 上漲空間百分比,
                'num_analysts': 分析師數量,
                'rating_breakdown': 評等分佈
            }
        """
        try:
            info = stock.info
            recs = stock.recommendations
            
            result = {
                'recommendation': info.get('recommendationKey', 'N/A'),
                'target_price': info.get('targetMeanPrice', 0),
                'target_high': info.get('targetHighPrice', 0),
                'target_low': info.get('targetLowPrice', 0),
                'current_price': info.get('currentPrice', info.get('previousClose', 0)),
                'upside': 0,
                'num_analysts': info.get('numberOfAnalystOpinions', 0),
                'rating_breakdown': {}
            }
            
            # 計算上漲空間
            if result['current_price'] > 0 and result['target_price'] > 0:
                result['upside'] = ((result['target_price'] - result['current_price']) / result['current_price']) * 100
            
            # 獲取評等分佈 (最近一個月)
            if recs is not None and not recs.empty:
                recent_recs = recs.tail(30)  # 最近30條記錄
                if 'To Grade' in recent_recs.columns:
                    breakdown = recent_recs['To Grade'].value_counts().to_dict()
                    result['rating_breakdown'] = breakdown
            
            return result
            
        except Exception as e:
            logger.warning(f"Recommendations error for {ticker}: {e}")
            return {'recommendation': 'N/A', 'target_price': 0, 'upside': 0, 'num_analysts': 0}
    
    def analyze_chip_score(self, ticker, chip_data):
        """
        計算美股籌碼面評分
        
        Returns:
            tuple: (score, details_list)
        """
        if not chip_data:
            return 0, []
        
        score = 0
        details = []
        
        # 1. 機構持股分析
        inst = chip_data.get('institutional', {})
        inst_pct = inst.get('percent_held', 0)
        
        if inst_pct > 80:
            score += 1.5
            details.append(f"✅ 機構持股比例極高 ({inst_pct:.1f}%) (+1.5)")
        elif inst_pct > 60:
            score += 1
            details.append(f"✅ 機構持股比例偏高 ({inst_pct:.1f}%) (+1)")
        elif inst_pct < 20:
            score -= 0.5
            details.append(f"⚠️ 機構持股比例偏低 ({inst_pct:.1f}%) (-0.5)")
        
        # 機構增減持
        inst_change = inst.get('change_vs_prior', 0)
        if inst_change > 5:
            score += 1
            details.append(f"💰 機構近期增持 ({inst_change:+.1f}%) (+1)")
        elif inst_change < -5:
            score -= 1
            details.append(f"💸 機構近期減持 ({inst_change:+.1f}%) (-1)")
        
        # 2. 空頭持倉分析
        short = chip_data.get('short_interest', {})
        short_pct = short.get('short_percent_of_float', 0)
        short_ratio = short.get('short_ratio', 0)
        short_change = short.get('short_change_pct', 0)
        
        # 高空頭比例可能有軋空潛力
        if short_pct > 20:
            score += 0.5  # 軋空機會
            details.append(f"🔥 空頭比例極高 ({short_pct:.1f}%)，有軋空潛力 (+0.5)")
        elif short_pct > 10:
            details.append(f"⚠️ 空頭比例偏高 ({short_pct:.1f}%) (Info)")
        
        # 空頭回補天數
        if short_ratio > 5:
            score += 0.5
            details.append(f"🔥 空頭回補天數高 ({short_ratio:.1f}天)，軋空風險 (+0.5)")
        
        # 空頭變化
        if short_change < -20:
            score += 0.5
            details.append(f"✅ 空頭大幅回補 ({short_change:+.1f}%) (+0.5)")
        elif short_change > 20:
            score -= 0.5
            details.append(f"⚠️ 空頭大幅增加 ({short_change:+.1f}%) (-0.5)")
        
        # 3. 內部人交易分析
        insider = chip_data.get('insider_trades', {})
        sentiment = insider.get('sentiment', 'neutral')
        buy_count = insider.get('buy_count', 0)
        sell_count = insider.get('sell_count', 0)
        
        if sentiment == 'bullish' and buy_count > 3:
            score += 1.5
            details.append(f"💎 內部人近期積極買入 (買{buy_count}次/賣{sell_count}次) (+1.5)")
        elif sentiment == 'bullish':
            score += 0.5
            details.append(f"✅ 內部人偏向買入 (買{buy_count}次/賣{sell_count}次) (+0.5)")
        elif sentiment == 'bearish' and sell_count > 5:
            score -= 1.5
            details.append(f"💀 內部人近期大量拋售 (買{buy_count}次/賣{sell_count}次) (-1.5)")
        elif sentiment == 'bearish':
            score -= 0.5
            details.append(f"⚠️ 內部人偏向賣出 (買{buy_count}次/賣{sell_count}次) (-0.5)")
        
        # 4. 分析師評等分析
        recs = chip_data.get('recommendations', {})
        rec_key = recs.get('recommendation', 'N/A')
        upside = recs.get('upside', 0)
        
        if rec_key in ['strong_buy', 'buy'] and upside > 20:
            score += 1
            details.append(f"📈 分析師看好 ({rec_key})，目標上漲空間 {upside:.1f}% (+1)")
        elif rec_key in ['sell', 'strong_sell']:
            score -= 1
            details.append(f"📉 分析師看空 ({rec_key})，目標上漲空間 {upside:.1f}% (-1)")
        elif upside > 30:
            score += 0.5
            details.append(f"📊 目標價上漲空間大 ({upside:.1f}%) (+0.5)")
        elif upside < -10:
            score -= 0.5
            details.append(f"📊 目標價下跌空間 ({upside:.1f}%) (-0.5)")
        
        return score, details


def get_us_chip_data(ticker):
    """
    便捷函數: 取得美股籌碼數據
    """
    analyzer = USStockChipAnalyzer()
    return analyzer.get_chip_data(ticker)


if __name__ == "__main__":
    # 測試
    analyzer = USStockChipAnalyzer()
    
    test_tickers = ['AAPL', 'NVDA', 'TSLA']
    
    for ticker in test_tickers:
        print(f"\n{'='*50}")
        print(f"Testing {ticker}")
        print('='*50)
        
        data, err = analyzer.get_chip_data(ticker)
        
        if data:
            print("\n📊 Institutional Holdings:")
            inst = data['institutional']
            print(f"  - Institutions: {inst['holders_count']}")
            print(f"  - % Held: {inst['percent_held']:.2f}%")
            
            print("\n📊 Short Interest:")
            short = data['short_interest']
            print(f"  - Short % of Float: {short['short_percent_of_float']:.2f}%")
            print(f"  - Days to Cover: {short['short_ratio']:.2f}")
            
            print("\n📊 Insider Trades:")
            insider = data['insider_trades']
            print(f"  - Sentiment: {insider['sentiment']}")
            print(f"  - Buys: {insider['buy_count']}, Sells: {insider['sell_count']}")
            
            print("\n📊 Analyst Recommendations:")
            recs = data['recommendations']
            print(f"  - Recommendation: {recs['recommendation']}")
            print(f"  - Target Price: ${recs['target_price']:.2f}")
            print(f"  - Upside: {recs['upside']:.2f}%")
            
            # 計算評分
            score, details = analyzer.analyze_chip_score(ticker, data)
            print(f"\n📈 Chip Score: {score}")
            for d in details:
                print(f"  {d}")
        else:
            print(f"Error: {err}")
