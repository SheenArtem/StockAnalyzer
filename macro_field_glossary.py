"""macro_field_glossary.py -- 總經面板欄位「程式變數名 -> 中文正式名稱」中央對照表。

單一 source of truth，供兩個 consumer 共用，避免名稱 drift：
  1. tools/macro_compass_report.py  -- AI 風向報告 prompt（raw 變數名換中文正式名）
  2. macro_dashboard.py             -- 網頁 UI 顯示（中文名 + 單位 + 說明）

設計原則：
  - 中文名要可 google（即使使用者不熟，貼進搜尋引擎查得到正式術語）
  - 名稱會「騙人」的（up_down_vol_ratio 舊名 ad_ratio 其實是量比、buffett_indicator_tw 其實是指數值）
    把警語直接寫進中文名，讓任何 reader / LLM 第一眼就讀到正確語意
  - 純 dict + helper，無 streamlit / pandas 依賴，CLI 與 UI 皆可 import

格式：col -> (中文正式名稱, 單位, 判讀說明)
"""

FIELD_GLOSSARY = {
    # ---- FRED 國際 macro / 信用 / 流動性 ----
    'hy_oas': ('美國高收益債信用利差 (OAS)', '%', '高=信用壓力升；低=信用無壓'),
    'hy_oas_rank': ('高收益債利差 近10年百分位', '0-100', '本欄即排名(0=史上最緊)，與後方括號「全期百分位」意義重疊，以本值為準'),
    'yield_curve_10y_2y': ('美債殖利率曲線 10年減2年', '%', '負=倒掛(衰退預警)'),
    'yield_curve_10y_3m': ('美債殖利率曲線 10年減3月', '%', '負=倒掛(衰退預警)'),
    'dxy_close': ('美元指數 DXY', '點', '升=美元強，新興市場資金流出壓力'),
    'dxy_chg_4w': ('美元指數 近4週變化', '點', '正=美元走強'),
    'usdjpy_close': ('美元兌日圓匯率 USDJPY', '', '高=日圓弱(套利交易 risk-on)；急跌=全球 risk-off'),
    'usdjpy_chg_4w': ('美元兌日圓 近4週變化', '', '負(日圓急升)=risk-off 警訊'),
    'usdtwd_close': ('美元兌新台幣匯率 USDTWD', '', '高=台幣弱，外資流出壓力'),
    'vix_close': ('VIX 恐慌指數', '', '高=恐慌；低=平靜/自滿'),
    'chicago_nfci': ('芝加哥Fed 金融情勢指數 NFCI', '', '正=金融緊縮；負=寬鬆'),
    'chicago_anfci': ('芝加哥Fed 調整後金融情勢指數 ANFCI', '', '正=緊縮'),
    'st_louis_fsi': ('聖路易Fed 金融壓力指數', '', '正=壓力升'),
    'us_durable_yoy': ('美國耐久財訂單 年增率', '%', '降=景氣轉弱'),
    'us_unemployment_rate': ('美國失業率', '%', '升=景氣轉弱'),
    'us_initial_claims': ('美國初次申請失業金人數', '人', '升=勞動市場轉弱'),
    'us_consumer_sentiment': ('密西根大學 消費者信心指數', '', '低=消費信心弱'),
    'sp500_close': ('標普500指數', '點', ''),
    'fed_bs_trillion': ('Fed 資產負債表規模', '兆美元', '降=量化緊縮(抽流動性)'),
    'fed_bs_chg_4w': ('Fed 資產負債表 近4週變化', '兆美元', '負=縮表'),

    # ---- 估值 ----
    'buffett_indicator_us': ('美股巴菲特指標 (股市市值/GDP)', '', '高=美股估值貴；主要看百分位'),
    'buffett_rank_us': ('美股巴菲特指標 近10年百分位', '0-100', '100=史上最貴'),
    'buffett_indicator_tw': ('台股估值代理值 (=加權指數本身)', '點', '台股缺乾淨總市值/GDP，直接用指數當排名基準，raw 值無估值意義，只看下方百分位'),
    'buffett_rank_tw': ('台股估值(指數) 近10年百分位', '0-100', '100=史上最高'),
    'tw_market_pe': ('台股大盤 本益比 PE', '倍', '高=估值貴'),
    'tw_market_pb': ('台股大盤 股價淨值比 PB', '倍', '高=估值貴'),
    'tw_market_yield': ('台股大盤 現金殖利率', '%', '低=股價相對貴'),

    # ---- 市場廣度 ----
    'advances': ('上漲家數', '家', ''),
    'declines': ('下跌家數', '家', ''),
    'adl': ('累積騰落線 ADL', '家(累積)', '上漲減下跌家數累計；探底=底部參與度流失'),
    'mcclellan_oscillator': ('麥克連震盪指標 McClellan Oscillator', '', '正=廣度轉強，負=轉弱'),
    'up_down_vol_ratio': ('上漲下跌量能比 (Up/Down Volume Ratio, UVOL/DVOL)', '比', '上漲股總成交量÷下跌股總成交量；量能版漲跌比，非漲跌「家數」比(家數見上漲家數/下跌家數)；亦為 Arms Index/TRIN 的分母。舊欄名 ad_ratio'),
    'breadth_thrust_10d': ('Zweig 廣度衝力 10日', '', '>0.615=強勢起漲訊號'),
    'new_high_minus_low': ('52週新高家數 減 新低家數', '家', '正=創高占優'),
    'new_highs_52w': ('52週新高家數', '家', ''),
    'new_lows_52w': ('52週新低家數', '家', ''),
    'pct_above_50dma': ('站上50日均線 個股比例', '%', '>50=多數個股中期偏多'),
    'pct_above_200dma': ('站上200日均線 個股比例', '%', '>50=多數個股長期偏多'),

    # ---- Systemic chip 機構撤退 / 籌碼 ----
    'twii_close': ('加權指數 TWII 收盤 (大盤價位)', '點', '台股大盤指數價位'),
    'sbl_total': ('借券賣出總餘額', '元', '高=可供放空彈藥多'),
    'foreign_holding_avg': ('外資持股比例 均值', '%', '0050成分股外資平均持股'),
    'foreign_holding_chg_4w': ('外資持股比例 近4週變化', '百分點', '負值大=外資撤退'),
    'sbl_change_4w_pct': ('借券餘額 近4週變化', '%', '正值大=空方準備加碼'),
    'margin_to_index_ratio': ('融資餘額除以加權指數 比', '', '絕對值無意義，看下方 z-score'),
    'margin_ratio_z_252d': ('融資/指數比 252日 z-score', 'z', '負值大=散戶去槓桿/資金緊'),
    'short_to_long_ratio': ('券資比 (融券餘額/融資餘額)', '比', '上升=空方相對強'),
    'pcr_oi': ('臺指選擇權 Put/Call 未平倉比 (PCR-OI)', '比', '>1.2=避險升溫；極低=自滿無避險'),
    'foreign_net_oi': ('外資台指期 淨未平倉口數', '口', '負=淨空；負值擴大=外資轉空'),
    'foreign_fut_net_chg_4w': ('外資期貨淨部位 近4週變化', '口', '負值大=外資轉空'),
    'trust_buy_streak': ('投信 連續買賣超天數', '天', '正=連續買超天數'),
    'trust_net': ('投信 當日淨買賣超', '元', '正=投信買超'),
    'trust_5d_zscore': ('投信 5日買賣超 z-score', 'z', '極端負值=投信撤退'),
    'option_top1_concentration': ('選擇權 前1大交易人未平倉集中度', '', '過高=單一籌碼風險'),

    # ---- Banner 綜合風險 breakdown ----
    'composite': ('Banner v3 綜合風險分數', '0-100', '越高越危險'),
    'm1b_ratio': ('近20日成交值除以M1B貨幣供給 比', '%', '高=資金浮濫/過熱'),
    'rv10': ('10日 已實現波動率 (年化)', '%', '高=近期波動大'),
    'rv30': ('30日 已實現波動率 (年化)', '%', '高=波動大'),
    'pcr_volume': ('Put/Call 成交量比 (PCR-Volume)', '比', '極低=自滿無避險'),
    'fgi_score': ('恐懼貪婪指數 Fear and Greed Index', '0-100', '高=貪婪，低=恐懼'),

    # ---- ETF 流動 / 風險偏好 ----
    'tlt_spy_ratio': ('長債TLT 除以 股票SPY 比', '比', '低=risk-on；升=risk-off(轉防禦)'),
    'tlt_spy_chg_4w': ('TLT/SPY比 近4週變化', '%', '正=轉防禦 risk-off'),
    'hyg_to_lqd_ratio': ('高收益債HYG 除以 投資級債LQD 比', '比', '高=信用 risk-on'),
    'hyg_to_lqd_chg_4w': ('HYG/LQD比 近4週變化', '%', '負=信用轉弱'),
    'hyg_dollar_flow_z_252d': ('高收益債HYG 金額流向 252日 z-score', 'z', '極端值=信用流動性事件'),
    'move_close': ('MOVE 美債波動指數 (債市的VIX)', '', '高=利率/債市波動大'),
    'move_z_252d': ('MOVE 252日 z-score', 'z', '高=債市壓力升'),
    'eem_to_spy_ratio': ('新興市場EEM 除以 SPY 比', '比', '升=新興市場相對強(risk-on)'),
    'eem_to_spy_chg_4w': ('EEM/SPY比 近4週變化', '%', '正=新興市場轉強'),

    # ---- VIX 期限結構 + skew ----
    'vix': ('VIX 恐慌指數 (現貨)', '', '高=恐慌'),
    'vix3m': ('VIX3M 三個月期 VIX', '', '與 VIX 比較看期限結構'),
    'vix_vix3m_ratio': ('VIX 除以 VIX3M 比', '比', '大於等於1=backwardation(短期恐慌>遠期，風險觸發)；小於1=contango(平靜)'),
    'vvix': ('VVIX (VIX的波動率)', '', '高=波動的波動大，尾部不安'),
    'skew': ('CBOE SKEW 偏態指數', '', '高=市場為尾部崩跌付費(避險需求)'),
    'ovx': ('OVX 原油波動指數', '', '高=油價波動大'),
}


def label(col: str) -> str:
    """回傳中文正式名稱；查無則回原始 col。"""
    e = FIELD_GLOSSARY.get(col)
    return e[0] if e else col


def label_with_code(col: str) -> str:
    """回傳「中文名 [raw_col]」；保留 raw 變數名供 trigger 條件引用與追溯。"""
    e = FIELD_GLOSSARY.get(col)
    return f"{e[0]} [{col}]" if e else col


def unit(col: str) -> str:
    e = FIELD_GLOSSARY.get(col)
    return e[1] if e else ''


def desc(col: str) -> str:
    e = FIELD_GLOSSARY.get(col)
    return e[2] if e else ''
