"""
AI 研究報告模組 — Phase 1
收集個股所有數據，組裝 prompt，呼叫 Claude CLI 生成研究報告。
"""
import subprocess
import shutil
import json
import logging
import os
import re


def _find_claude_cli():
    """Resolve claude CLI absolute path.

    Windows subprocess.run with list args doesn't honor PATHEXT lookup for .cmd
    scripts. Use shutil.which which handles PATHEXT properly. Returns full path
    (e.g. ...\\npm\\claude.cmd) or 'claude' as fallback.
    """
    resolved = shutil.which("claude")
    return resolved or "claude"


_CLAUDE_CLI = _find_claude_cli()
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Prompt 模板路徑
_PROMPT_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'prompts', 'stock_analysis_system.md')
_DASHBOARD_PROMPT_PATH = os.path.join(os.path.dirname(__file__), 'prompts', 'stock_analysis_dashboard.md')
_DASHBOARD_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'prompts', 'report_dashboard_template.html')
_SONGFEN_FRAMEWORK_PATH = os.path.join(os.path.dirname(__file__), 'prompts', 'songfen_framework.md')
_SONGFEN_INDEX_PATH = os.path.join(os.path.dirname(__file__), 'knowledge', 'songfen', 'INDEX.md')


def _load_songfen_block():
    """載入宋分分析師框架 + 當期 themes，組合成附加 prompt block。
    若任何檔案缺失直接回空字串，由 caller 自行決定要不要加入。
    """
    try:
        with open(_SONGFEN_FRAMEWORK_PATH, 'r', encoding='utf-8') as f:
            framework = f.read()
    except FileNotFoundError:
        return ""

    themes_block = ""
    try:
        with open(_SONGFEN_INDEX_PATH, 'r', encoding='utf-8') as f:
            index_text = f.read()
        # 擷取 "## Current Themes" 到檔尾段落
        marker = "## Current Themes"
        idx = index_text.find(marker)
        if idx >= 0:
            themes_block = "\n\n### 當期時效性主題（從 INDEX.md 擷取）\n\n" + index_text[idx:]
    except FileNotFoundError:
        pass

    return framework + themes_block


def _load_system_prompt():
    """載入 system prompt 模板"""
    try:
        with open(_PROMPT_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error("System prompt template not found: %s", _PROMPT_TEMPLATE_PATH)
        return ""


def _load_dashboard_prompt():
    """載入儀表板模式的 system prompt"""
    try:
        with open(_DASHBOARD_PROMPT_PATH, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error("Dashboard prompt not found: %s", _DASHBOARD_PROMPT_PATH)
        return ""


def _load_dashboard_template():
    """載入儀表板 HTML 模板"""
    try:
        with open(_DASHBOARD_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error("Dashboard template not found: %s", _DASHBOARD_TEMPLATE_PATH)
        return ""


def _safe_val(v, fmt=".2f"):
    """安全格式化數值，處理 None/NaN/inf"""
    if v is None:
        return "N/A"
    try:
        if isinstance(v, (int, float, np.integer, np.floating)):
            if np.isnan(v) or np.isinf(v):
                return "N/A"
            return f"{v:{fmt}}"
        return str(v)
    except (TypeError, ValueError):
        return str(v)


def _build_stock_info(ticker, report, fund_data, df_day):
    """[STOCK_INFO] 基本資訊"""
    lines = []
    lines.append(f"股票代號: {ticker}")

    # 從 fund_data 取得產業資訊
    if fund_data:
        for key in ['Sector', 'Industry']:
            val = fund_data.get(key, '')
            if val:
                lines.append(f"{key}: {val}")

    # 最新價格
    if df_day is not None and not df_day.empty:
        last = df_day.iloc[-1]
        price = last.get('Close', 0)
        lines.append(f"最新收盤價: {_safe_val(price)}")
        if 'Volume' in df_day.columns:
            lines.append(f"最新成交量: {_safe_val(last.get('Volume', 0), '.0f')}")

    return "\n".join(lines)


def _build_trigger_score(report):
    """[TRIGGER_SCORE] 評分摘要"""
    lines = []
    lines.append(f"觸發分數: {_safe_val(report.get('trigger_score', 0))}")
    lines.append(f"趨勢分數: {_safe_val(report.get('trend_score', 0))}")
    lines.append(f"百分位: {_safe_val(report.get('score_percentile', 0), '.1f')}%")

    # Scenario
    sc = report.get('scenario', {})
    if sc:
        lines.append(f"劇本: {sc.get('code', '?')} - {sc.get('title', '')}")
        lines.append(f"劇本說明: {sc.get('desc', '')}")

    # Breakdown
    bd = report.get('trigger_breakdown', {})
    if bd:
        lines.append("\n分數明細:")
        for k, v in bd.items():
            if k in ('regime_weights',):
                lines.append(f"  {k}: {json.dumps(v)}")
            else:
                lines.append(f"  {k}: {_safe_val(v) if isinstance(v, (int, float)) else v}")

    return "\n".join(lines)


def _build_trigger_details(report):
    """[TRIGGER_DETAILS] 觸發訊號列表"""
    details = report.get('trigger_details', [])
    if not details:
        return "N/A"
    return "\n".join(f"- {d}" for d in details)


def _build_technical_data(df_day):
    """[TECHNICAL_DATA] 最新技術指標數值"""
    if df_day is None or df_day.empty:
        return "N/A"

    last = df_day.iloc[-1]

    # 收集所有技術指標 (欄位名對應 technical_analysis.py 實際產出)
    indicators = {
        # 均線
        'MA5': '.2f', 'MA10': '.2f', 'MA20': '.2f', 'MA60': '.2f', 'MA120': '.2f',
        # 布林
        'BB_Up': '.2f', 'BB_Lo': '.2f',
        # RSI / KD
        'RSI': '.1f', 'K': '.1f', 'D': '.1f',
        # MACD
        'MACD': '.4f', 'Signal': '.4f', 'Hist': '.4f',
        # ADX / DMI
        'ADX': '.1f', '+DI': '.1f', '-DI': '.1f',
        # Supertrend
        'Supertrend': '.2f', 'Supertrend_Dir': '.0f',
        # RVOL / OBV
        'RVOL': '.2f', 'OBV': '.0f',
        # ATR
        'ATR': '.2f',
        # Squeeze Momentum
        'Squeeze_Mom': '.4f',
        # VWAP
        'VWAP': '.2f',
        # TD Sequential
        'TD_Buy_Setup': '.0f', 'TD_Sell_Setup': '.0f',
        # EFI
        'EFI': '.0f',
        # 型態
        'Pattern': None, 'Pattern_Type': None,
    }

    # 輸出時用更易讀的名稱
    display_names = {
        'BB_Up': 'BB_Upper', 'BB_Lo': 'BB_Lower',
        'Signal': 'MACD_Signal', 'Hist': 'MACD_Hist',
        '+DI': 'DI+', '-DI': 'DI-',
        'Supertrend_Dir': 'Supertrend_Direction (1=多 -1=空)',
        'Squeeze_Mom': 'Squeeze_Momentum',
        'TD_Buy_Setup': 'TD_Buy_Setup (連續計數)',
        'TD_Sell_Setup': 'TD_Sell_Setup (連續計數)',
    }

    lines = []
    for col, fmt in indicators.items():
        if col in df_day.columns:
            val = last.get(col, None)
            if val is not None and not (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
                name = display_names.get(col, col)
                if fmt is None:
                    lines.append(f"{name}: {val}")
                else:
                    lines.append(f"{name}: {_safe_val(val, fmt)}")

    # 額外計算: 價格相對均線位置
    close = last.get('Close', 0)
    if close and close > 0:
        for ma_col in ['MA5', 'MA20', 'MA60', 'MA120']:
            if ma_col in df_day.columns:
                ma_val = last.get(ma_col, 0)
                if ma_val and ma_val > 0:
                    pct = (close - ma_val) / ma_val * 100
                    lines.append(f"Close vs {ma_col}: {pct:+.1f}%")

    # BB %B 位置
    bb_upper = last.get('BB_Up', 0)
    bb_lower = last.get('BB_Lo', 0)
    if bb_upper and bb_lower and bb_upper > bb_lower:
        bb_mid = (bb_upper + bb_lower) / 2
        bb_pct = (close - bb_lower) / (bb_upper - bb_lower) * 100
        lines.append(f"BB %B: {bb_pct:.1f}%")
        lines.append(f"BB Width: {(bb_upper - bb_lower) / bb_mid * 100:.2f}%")

    # DMI 方向判斷
    di_pos = last.get('+DI', 0)
    di_neg = last.get('-DI', 0)
    if di_pos and di_neg:
        if di_pos > di_neg:
            lines.append(f"DMI Direction: Bullish (DI+ {di_pos:.1f} > DI- {di_neg:.1f})")
        else:
            lines.append(f"DMI Direction: Bearish (DI- {di_neg:.1f} > DI+ {di_pos:.1f})")

    # OBV 趨勢 (近 20 日斜率)
    if 'OBV' in df_day.columns and len(df_day) >= 20:
        obv_recent = df_day['OBV'].tail(20).dropna()
        if len(obv_recent) >= 10:
            obv_slope = (obv_recent.iloc[-1] - obv_recent.iloc[0]) / len(obv_recent)
            obv_dir = "Rising" if obv_slope > 0 else "Falling"
            lines.append(f"OBV Trend (20d): {obv_dir} (slope={obv_slope:+.0f}/day)")

    # Squeeze 狀態 (從 Squeeze_Mom 判斷)
    sq_mom = last.get('Squeeze_Mom', None)
    if sq_mom is not None and not np.isnan(sq_mom):
        # 檢查前一根的值來判斷是否剛突破
        if len(df_day) >= 2:
            prev_sq = df_day.iloc[-2].get('Squeeze_Mom', 0)
            if prev_sq is not None and not np.isnan(prev_sq):
                if abs(sq_mom) > abs(prev_sq):
                    lines.append("Squeeze: Expanding (momentum accelerating)")
                else:
                    lines.append("Squeeze: Contracting (momentum decelerating)")

    return "\n".join(lines) if lines else "N/A"


def _build_chip_data(chip_data, us_chip_data, is_us, ticker=None):
    """[CHIP_DATA] 籌碼面數據"""
    lines = []

    if is_us and us_chip_data:
        # 美股籌碼
        inst = us_chip_data.get('institutional', {})
        if inst:
            lines.append(f"機構持股數: {inst.get('holders_count', 'N/A')}")
            lines.append(f"機構持股比: {_safe_val(inst.get('percent_held', 0), '.1f')}%")

        short = us_chip_data.get('short_interest', {})
        if short:
            lines.append(f"空單比例: {_safe_val(short.get('short_percent_of_float', 0), '.1f')}% of float")
            lines.append(f"Days to Cover (short_ratio): {_safe_val(short.get('short_ratio', 0), '.1f')}")
            if short.get('short_change_pct'):
                lines.append(f"空單月變化: {_safe_val(short.get('short_change_pct', 0), '+.1f')}%")

        insider = us_chip_data.get('insider_trades', {})
        if insider:
            sentiment = insider.get('sentiment', 'neutral')
            buy_cnt = insider.get('buy_count', 0)
            sell_cnt = insider.get('sell_count', 0)
            net_shares = insider.get('net_shares_purchased', 0)
            lines.append(f"\n內部人交易情緒: {sentiment} (買 {buy_cnt} / 賣 {sell_cnt}, 淨 {net_shares:+,} 股)")
            recent = insider.get('recent_trades')
            if isinstance(recent, pd.DataFrame) and not recent.empty:
                lines.append(f"近期內部人交易 (前 5 筆 / 共 {len(recent)} 筆):")
                for _, row in recent.head(5).iterrows():
                    lines.append(f"  - {row.to_dict()}")

        recs = us_chip_data.get('recommendations', {})
        if recs:
            lines.append(f"\n分析師評級: {json.dumps(recs, ensure_ascii=False)}")

        top = us_chip_data.get('major_holders', {})
        if isinstance(top, pd.DataFrame) and not top.empty:
            lines.append(f"\n主要持股人:\n{top.to_string()}")
        elif isinstance(top, dict) and top:
            lines.append(f"\n主要持股人: {json.dumps(top, ensure_ascii=False)}")

    elif not is_us:
        # 台股籌碼
        if chip_data:
            for key, label in [('institutional', '三大法人'), ('margin', '融資融券'),
                               ('day_trading', '當沖'), ('shareholding', '持股分布')]:
                df = chip_data.get(key)
                if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                    lines.append(f"\n{label} (近 5 日):")
                    tail = df.tail(5)
                    lines.append(tail.to_string())

        # TDCC 集保股權分散（獨立來源，不受 chip_data 缺失影響）
        if ticker:
            try:
                from tdcc_reader import format_shareholding_for_prompt
                stock_id = str(ticker).replace('.TW', '').strip()
                tdcc_txt = format_shareholding_for_prompt(stock_id)
                if tdcc_txt:
                    lines.append(f"\n{tdcc_txt}")
            except Exception as e:
                logger.debug("TDCC shareholding fetch failed for %s: %s: %s", ticker, type(e).__name__, e)

        # BL-4 Phase E: 三大法人週榜上的位置 (4 維度買賣超統計)
        if ticker:
            try:
                from weekly_chip_loader import format_summary_for_ai, get_metadata as _wc_md
                stock_id = str(ticker).replace('.TW', '').replace('.TWO', '').strip()
                wc_txt = format_summary_for_ai(stock_id)
                if wc_txt:
                    md = _wc_md()
                    we = md['week_end'].strftime('%Y-%m-%d') if md else ''
                    lines.append(f"\n本週三大法人週榜 (週末 {we}): {wc_txt}")
            except Exception as e:
                logger.debug("Weekly chip rank fetch failed for %s: %s: %s", ticker, type(e).__name__, e)

    return "\n".join(lines) if lines else "N/A"


def _build_fundamental_data(fund_data, ticker):
    """[FUNDAMENTAL_DATA] 基本面數據 + Piotroski/Z-Score/ROIC/FCF + 月營收"""
    lines = []

    # 基本面 (from get_fundamentals, includes TradingView overlay)
    if fund_data:
        for key in ['PE Ratio', 'Forward PE', 'PB Ratio', 'PEG Ratio',
                    'EPS (TTM)', 'ROE', 'ROA',
                    'Gross Margin', 'Operating Margin', 'Net Margin', 'Profit Margin',
                    'Dividend Yield', 'Debt/Equity',
                    'Market Cap', 'Revenue YoY', 'Monthly Revenue',
                    'Cash Dividend', 'Stock Dividend', 'Payout Ratio']:
            val = fund_data.get(key, '')
            if val and str(val) not in ('', 'N/A', 'nan', 'None'):
                lines.append(f"{key}: {val}")

    # Piotroski F-Score + Altman Z-Score + ROIC/FCF
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    try:
        if is_us:
            from piotroski import calculate_fscore_us, calculate_zscore_us, calculate_extra_metrics_us
            mc_str = fund_data.get('Market Cap', '0') if fund_data else '0'
            mc = _parse_market_cap(mc_str)

            fs = calculate_fscore_us(ticker)
            if fs:
                lines.append(f"\nPiotroski F-Score: {fs['fscore']}/9")
                lines.append(f"  Profitability: {fs['components']['profitability']}/4")
                lines.append(f"  Leverage: {fs['components']['leverage']}/3")
                lines.append(f"  Efficiency: {fs['components']['efficiency']}/2")
                for d in fs.get('details', []):
                    lines.append(f"  {d}")

            zs = calculate_zscore_us(ticker, mc) if mc > 0 else None
            if zs:
                lines.append(f"Altman Z-Score: {_safe_val(zs['zscore'])} ({zs['zone']})")

            em = calculate_extra_metrics_us(ticker, mc) if mc > 0 else None
            if em:
                if em.get('roic'):
                    lines.append(f"ROIC: {_safe_val(em['roic']*100, '.1f')}%")
                if em.get('fcf_yield'):
                    lines.append(f"FCF Yield: {_safe_val(em['fcf_yield']*100, '.1f')}%")
        else:
            from piotroski import calculate_all
            stock_id = ticker.replace('.TW', '')
            mc_str = fund_data.get('Market Cap', '0') if fund_data else '0'
            mc = _parse_market_cap(mc_str)

            # Single fetch: F-Score + Z-Score + Extra (3 API calls instead of 8)
            all_result = calculate_all(stock_id, market_cap=mc)

            fs = all_result.get('fscore') if all_result else None
            if fs:
                lines.append(f"\nPiotroski F-Score: {fs['fscore']}/9")
                lines.append(f"  Profitability: {fs['components']['profitability']}/4")
                lines.append(f"  Leverage: {fs['components']['leverage']}/3")
                lines.append(f"  Efficiency: {fs['components']['efficiency']}/2")
                for d in fs.get('details', []):
                    lines.append(f"  {d}")
                fd = fs.get('data', {})
                if fd:
                    if fd.get('gross_margin') is not None:
                        lines.append(f"Gross Margin: {_safe_val(fd['gross_margin']*100, '.1f')}% (prev: {_safe_val(fd.get('gross_margin_prev', 0)*100, '.1f')}%)")
                    if fd.get('roa') is not None:
                        lines.append(f"ROA: {_safe_val(fd['roa']*100, '.1f')}%")
                    if fd.get('operating_cf') is not None:
                        lines.append(f"Operating CF: {fd['operating_cf']:,.0f}")
                    if fd.get('current_ratio') is not None:
                        lines.append(f"Current Ratio: {_safe_val(fd['current_ratio'])} (prev: {_safe_val(fd.get('current_ratio_prev', 0))})")
                    if fd.get('asset_turnover') is not None:
                        lines.append(f"Asset Turnover: {_safe_val(fd['asset_turnover'])} (prev: {_safe_val(fd.get('asset_turnover_prev', 0))})")
                    if fd.get('shares_curr') is not None:
                        lines.append(f"Shares Outstanding: {fd['shares_curr']:,.0f} (prev: {fd.get('shares_prev', 0):,.0f})")

            zs = all_result.get('zscore') if all_result else None
            if zs:
                lines.append(f"Altman Z-Score: {_safe_val(zs['zscore'])} ({zs['zone']})")

            em = all_result.get('extra') if all_result else None
            if em:
                if em.get('roic'):
                    lines.append(f"ROIC: {_safe_val(em['roic'], '.1f')}%")
                if em.get('fcf') is not None:
                    lines.append(f"FCF: {em['fcf']:,.0f}")
                if em.get('fcf_yield'):
                    lines.append(f"FCF Yield: {_safe_val(em['fcf_yield'], '.1f')}%")
                if em.get('current_ratio'):
                    lines.append(f"Current Ratio: {_safe_val(em['current_ratio'])}")
    except Exception as e:
        logger.warning("Piotroski/ZScore data fetch failed: %s", e)
        lines.append(f"\nPiotroski/Z-Score: 取得失敗 ({e})")

    # 月營收趨勢 (台股)
    if not is_us:
        try:
            from dividend_revenue import RevenueTracker
            rt = RevenueTracker()
            stock_id = ticker.replace('.TW', '')
            rev_df = rt.get_monthly_revenue(stock_id, months=12)
            if rev_df is not None and not rev_df.empty:
                lines.append(f"\n月營收趨勢 (近 12 月):")
                # Show last 6 months
                recent = rev_df.tail(6)
                for _, row in recent.iterrows():
                    ym = row.get('year_month', '')
                    rev = row.get('revenue', 0)
                    yoy = row.get('yoy_pct', 0)
                    mom = row.get('mom_pct', 0)
                    lines.append(f"  {ym}: {rev:,.0f} (YoY {yoy:+.1f}%, MoM {mom:+.1f}%)")

            alert = rt.get_revenue_alert(stock_id)
            if alert and alert.get('alert_text') != '無營收資料':
                lines.append(f"營收趨勢: {alert.get('trend', 'N/A')}")
                lines.append(f"連續成長月數: {alert.get('consecutive_growth_months', 0)}")
                if alert.get('next_announcement_date'):
                    lines.append(f"下次營收公布: {alert['next_announcement_date']} ({alert.get('days_until', 0)} 天後)")
        except Exception as e:
            logger.warning("Revenue data fetch failed: %s", e)

    return "\n".join(lines) if lines else "N/A"


def _parse_market_cap(mc_str):
    """解析 Market Cap 字串為數值 (e.g. '1.5T' -> 1500000000000)"""
    if not mc_str or mc_str in ('N/A', 'None', '0'):
        return 0
    mc_str = str(mc_str).strip().upper()
    try:
        if mc_str.endswith('T'):
            return float(mc_str[:-1]) * 1e12
        elif mc_str.endswith('B'):
            return float(mc_str[:-1]) * 1e9
        elif mc_str.endswith('M'):
            return float(mc_str[:-1]) * 1e6
        # 台股可能是純數字 (單位: 元)
        cleaned = mc_str.replace(',', '').replace('$', '')
        return float(cleaned)
    except (ValueError, TypeError):
        return 0


def _build_value_score(ticker, fund_data, df_day):
    """[VALUE_SCORE] ValueScreener 5 維評分 (估值/體質/營收/技術轉折/聰明錢)"""
    try:
        from value_screener import ValueScreener

        is_us = ticker and not ticker.replace('.TW', '').isdigit()
        stock_id = ticker if is_us else ticker.replace('.TW', '')

        # 從現有數據組裝 market_row
        close = 0
        change_pct = 0
        volume = 0
        trading_value = 0
        if df_day is not None and not df_day.empty and len(df_day) >= 2:
            last = df_day.iloc[-1]
            prev = df_day.iloc[-2]
            close = float(last.get('Close', 0))
            prev_close = float(prev.get('Close', 0))
            if prev_close > 0:
                change_pct = (close - prev_close) / prev_close * 100
            volume = int(last.get('Volume', 0))
            trading_value = int(close * volume) if close > 0 else 0

        # 從 fund_data 取 PE/PB/Dividend
        pe = _parse_fund_float(fund_data, 'PE Ratio')
        pb = _parse_fund_float(fund_data, 'PB Ratio')
        div_yield = _parse_fund_float(fund_data, 'Dividend Yield')

        market_row = {
            'stock_id': stock_id,
            'stock_name': '',
            'market': 'us' if is_us else 'tw',
            'close': close,
            'change_pct': round(change_pct, 2),
            'volume': volume,
            'trading_value': trading_value,
            'PE': pe,
            'PB': pb,
            'dividend_yield': div_yield,
        }

        screener = ValueScreener()
        result = screener._score_single(stock_id, market_row)

        if not result:
            return "N/A (評分失敗)"

        lines = []
        lines.append(f"綜合分數: {_safe_val(result.get('value_score', 0), '.1f')} / 100")
        scores = result.get('scores', {})
        lines.append(f"  估值 (30%): {_safe_val(scores.get('valuation', 0), '.1f')}")
        lines.append(f"  體質 (25%): {_safe_val(scores.get('quality', 0), '.1f')}")
        lines.append(f"  營收 (15%): {_safe_val(scores.get('revenue', 0), '.1f')}")
        lines.append(f"  技術轉折 (15%): {_safe_val(scores.get('technical', 0), '.1f')}")
        lines.append(f"  聰明錢 (15%): {_safe_val(scores.get('smart_money', 0), '.1f')}")

        details = result.get('details', [])
        if details:
            lines.append(f"\n評分明細:")
            for d in details:
                lines.append(f"  - {d}")

        return "\n".join(lines)

    except Exception as e:
        logger.warning("Value score failed: %s", e)
        return f"N/A (評分失敗: {e})"


def _parse_fund_float(fund_data, key):
    """從 fund_data 解析數值，處理百分比和字串"""
    if not fund_data:
        return 0
    val = fund_data.get(key, '')
    if not val or str(val) in ('N/A', 'None', 'nan', ''):
        return 0
    try:
        s = str(val).replace('%', '').replace(',', '').strip()
        return float(s)
    except (ValueError, TypeError):
        return 0


def _build_forward_guidance_context(ticker, max_events: int = 4) -> str:
    """[FORWARD_GUIDANCE] B1 builder (Phase 1 #1b).

    讀 data/news/earnings_schema.parquet, filter 該 ticker, 取最近 N 件法說會 /
    財報展望. Output Tier 2 公司展望，與 Tier 1 yfinance forwardEps 共識並列
    讓 LLM 能比較 (差異本身是 signal, Council BLOCKER #5 spec)。

    Council BLOCKER #7: 此資料僅 informational, 不入 scanner 排序。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return "N/A (earnings_schema 目前只覆蓋台股)"
    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()
    try:
        import pandas as pd
        from pathlib import Path as _P
        path = _P(__file__).resolve().parent / 'data' / 'news' / 'earnings_schema.parquet'
        if not path.exists():
            return "N/A (earnings_schema.parquet 尚未產生，scanner 跑過後才有)"
        df = pd.read_parquet(path)
        if df.empty:
            return "N/A (近 1 年無法說會 / 財報展望抽取資料)"
        sub = df[df['ticker'].astype(str) == stock_id].copy()
        if sub.empty:
            return f"N/A (近 1 年 {stock_id} 無法說會 / 財報展望抽取資料)"

        sub['date'] = pd.to_datetime(sub['date'], errors='coerce')
        sub = sub.sort_values('date', ascending=False).head(max_events)

        lines = ["近 1 年法說會 / 公司展望抽取 (Tier 2 from news, dedupe by event):"]
        for _, row in sub.iterrows():
            d = row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else '?'
            qp = str(row.get('q_period', '') or '').strip()
            qp_str = f" Q={qp}" if qp else ''
            parts = []
            for col, label in [('forward_eps_change', 'EPS'),
                               ('forward_revenue_guidance', 'Revenue'),
                               ('forward_gross_margin', 'GM'),
                               ('key_capacity_event', '產能')]:
                v = str(row.get(col, '') or '').strip()
                if v:
                    parts.append(f"{label}={v}")
            tone = row.get('tone', 'neutral') if 'tone' in sub.columns else 'neutral'
            sent = float(row.get('sentiment', 0.0) or 0.0)
            title = str(row.get('title', ''))[:70]
            joined = ', '.join(parts) if parts else '(無 directional)'
            lines.append(f"  [{d}{qp_str}] ({tone} {sent:+.2f}) {joined}")
            lines.append(f"      {title}")

        lines.append("")
        lines.append("⚠️ Tier 2 source = news 抽取「公司自己法說會講的展望」, 非分析師共識")
        lines.append("   若與 [ANALYST_CONSENSUS] / yfinance forward EPS 不一致，差異本身是 signal")
        return "\n".join(lines)
    except Exception as e:
        return f"N/A (forward guidance 讀取失敗: {e})"


def _build_theme_momentum_context(ticker, lookback_days: int = 7) -> str:
    """[THEME_MOMENTUM] B8 builder (Phase 1 #5).

    讀 data/news/theme_momentum.parquet, 列近 N 天該檔的 theme 是否在升溫/降溫榜。
    用 4-layer themes (manual+News+YT+TV) 比對 momentum theme。

    informational only, Council BLOCKER #7: 不入 scanner 排序。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return ""
    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()
    try:
        import pandas as pd
        from pathlib import Path as _P
        path = _P(__file__).resolve().parent / 'data' / 'news' / 'theme_momentum.parquet'
        if not path.exists():
            return ""
        df = pd.read_parquet(path)
        if df.empty:
            return ""
        df['detection_date'] = pd.to_datetime(df['detection_date'])
        cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=lookback_days)
        df = df[df['detection_date'] >= cutoff]
        if df.empty:
            return ""

        # filter rows where ticker is in top_tickers csv list
        df['top_tickers'] = df['top_tickers'].astype(str)
        mask = df['top_tickers'].str.contains(rf'\b{stock_id}\b', regex=True, na=False)
        sub = df[mask].copy()
        if sub.empty:
            return ""

        sub = sub.sort_values('detection_date', ascending=False).head(8)
        lines = [f"近 {lookback_days} 天本檔在題材熱度榜中出現紀錄:"]
        for _, r in sub.iterrows():
            d = r['detection_date'].strftime('%Y-%m-%d')
            direction = str(r.get('direction', ''))
            theme = str(r.get('theme', ''))[:25]
            cnt = int(r.get('count_today', 0))
            ratio = float(r.get('ratio_7d', 0))
            arrow = '↑' if direction == 'heating' else '↓'
            ratio_str = "new" if ratio >= 999 else f"{ratio:.1f}x"
            lines.append(f"  [{d}] {arrow} {theme} | today {cnt} 篇 ({ratio_str})")

        lines.append("")
        lines.append("⚠️ Council BLOCKER #7: theme momentum informational only, 不入 scanner")
        return "\n".join(lines)
    except Exception as e:
        return f"N/A (theme_momentum 讀取失敗: {e})"


def _build_news_flow_alert_context(ticker, lookback_days: int = 14) -> str:
    """[NEWS_FLOW_ALERT] B7 builder (Phase 1 #4).

    讀 data/news/news_flow_anomaly.parquet, filter 該 ticker, 列近 N 天觸發
    過異常的紀錄。informational only, Council BLOCKER #7: 不入 scanner 排序。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return "N/A (news_flow_anomaly 目前只覆蓋台股)"
    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()
    try:
        import pandas as pd
        from pathlib import Path as _P
        path = _P(__file__).resolve().parent / 'data' / 'news' / 'news_flow_anomaly.parquet'
        if not path.exists():
            return ""  # empty section, 不污染 prompt
        df = pd.read_parquet(path)
        if df.empty:
            return ""
        sub = df[df['ticker'].astype(str) == stock_id].copy()
        if sub.empty:
            return ""  # 該檔近期沒爆量, 不顯示 section

        sub['detection_date'] = pd.to_datetime(sub['detection_date'])
        cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=lookback_days)
        sub = sub[sub['detection_date'] >= cutoff].sort_values(
            'detection_date', ascending=False).head(5)
        if sub.empty:
            return ""

        lines = [f"近 {lookback_days} 天 news flow 爆量紀錄 (today >= 3 篇 AND >= 3x 7d_avg):"]
        for _, r in sub.iterrows():
            d = r['detection_date'].strftime('%Y-%m-%d')
            cnt = int(r.get('count_today', 0))
            avg = float(r.get('count_7d_avg', 0))
            ratio = float(r.get('ratio', 0))
            ratio_str = "new (7d 0 篇)" if ratio >= 999 else f"{ratio:.1f}x"
            themes = str(r.get('top_themes', ''))[:50]
            lines.append(f"  [{d}] {cnt}篇 vs 7d avg {avg:.1f} ({ratio_str}) | themes={themes}")

        lines.append("")
        lines.append("⚠️ Council BLOCKER #7: 此訊號 informational only, 未過 backtest 閘門")
        lines.append("   不該作為 scanner ranking 或進場閘門, 但可作為 thesis 的 timing context")
        return "\n".join(lines)
    except Exception as e:
        return f"N/A (news_flow_alert 讀取失敗: {e})"


def _build_analyst_targets_context(ticker, days: int = 30, max_items: int = 10) -> str:
    """[ANALYST_TARGETS] B2 builder (Phase 1 #3).

    讀 data/news/analyst_targets.parquet (LLM extract from news)，filter ticker
    + 近 N 天，列個別券商目標價歷史。與既有 [ANALYST_CONSENSUS] (yfinance mean)
    並列，差異本身是 signal (per data_integration.md 場景 C)。

    Outlier 防護：news 抽到的目標價偏離 yfinance mean > 30% → 標 [outlier] 但仍列。
    Council BLOCKER #7: informational only, 不入 scanner 排序。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return "N/A (analyst_targets 目前只覆蓋台股)"
    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()
    try:
        import pandas as pd
        from pathlib import Path as _P
        path = _P(__file__).resolve().parent / 'data' / 'news' / 'analyst_targets.parquet'
        if not path.exists():
            return "N/A (analyst_targets.parquet 尚未產生，scanner 跑過後才有)"
        df = pd.read_parquet(path)
        if df.empty:
            return "N/A (近 1 年無券商目標價抽取資料)"
        sub = df[df['ticker'].astype(str) == stock_id].copy()
        if sub.empty:
            return f"N/A ({stock_id} 近期無券商目標價抽取)"

        sub['date'] = pd.to_datetime(sub['date'], errors='coerce')
        cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=days)
        sub = sub[sub['date'] >= cutoff]
        if sub.empty:
            return f"N/A ({stock_id} 近 {days} 天無券商目標價)"

        # yfinance mean for outlier check
        yf_mean = None
        try:
            import yfinance as yf
            yticker = f"{stock_id}.TW"
            info = yf.Ticker(yticker).info
            if not info.get('targetMeanPrice'):
                info = yf.Ticker(f"{stock_id}.TWO").info
            yf_mean = info.get('targetMeanPrice')
        except Exception:
            pass

        sub = sub.sort_values(['confidence', 'date'], ascending=[False, False]).head(max_items)

        lines = [f"近 {days} 天券商目標價抽取 (Tier 2 from news, source: moneylink/cnyes/UDN):"]
        if yf_mean:
            lines.append(f"yfinance 共識 mean = {yf_mean:.0f} 元 (參考)")

        for _, row in sub.iterrows():
            d = row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else '?'
            broker = str(row.get('broker', ''))[:20]
            price = float(row.get('target_price', 0) or 0)
            rating = str(row.get('rating', '') or '').strip()
            outlier = ''
            if yf_mean and yf_mean > 0:
                deviation = abs(price - yf_mean) / yf_mean
                if deviation > 0.30:
                    outlier = f' [outlier {deviation*100:+.0f}% vs共識]'
            rating_str = f' ({rating})' if rating else ''
            lines.append(f"  [{d}] {broker} 目標 {price:.0f} 元{rating_str}{outlier}")

        return "\n".join(lines)
    except Exception as e:
        return f"N/A (analyst_targets 讀取失敗: {e})"


def _build_earnings_calendar_context(ticker, past_n: int = 4, future_n: int = 1) -> str:
    """[EARNINGS_CALENDAR] B4 builder (Phase 1 #2).

    讀 data/calendar/earnings_call.parquet, filter 該 ticker, 顯示
    過去 N 場 actual + 下次 1 場（forward 取最近未來日期）。

    用途：模型可據時序判斷「下次法說會 X 日，前 N 天不建議重押」這類 thesis。
    Source: moneylink (LLM extract)，未來可加 yahoo / wantgoo 等。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return "N/A (earnings_calendar 目前只覆蓋台股)"
    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()
    try:
        import pandas as pd
        from pathlib import Path as _P
        path = _P(__file__).resolve().parent / 'data' / 'calendar' / 'earnings_call.parquet'
        if not path.exists():
            return "N/A (earnings_call.parquet 尚未產生，scanner 跑過後才有)"
        df = pd.read_parquet(path)
        if df.empty:
            return "N/A (法說會行事曆無資料)"
        sub = df[df['ticker'].astype(str) == stock_id].copy()
        if sub.empty:
            return f"N/A ({stock_id} 無法說會行事曆紀錄)"

        sub['event_date'] = pd.to_datetime(sub['event_date'], errors='coerce')
        sub = sub.dropna(subset=['event_date']).sort_values('event_date')

        today = pd.Timestamp.now().normalize()
        past = sub[sub['event_date'] < today].tail(past_n)
        future = sub[sub['event_date'] >= today].head(future_n)

        lines = [f"法說會行事曆 (source={', '.join(sorted(sub['source'].unique()))}):"]
        if not future.empty:
            lines.append("下次法說會:")
            for _, row in future.iterrows():
                d = row['event_date'].strftime('%Y-%m-%d')
                t = str(row.get('event_time', '') or '').strip()
                loc = str(row.get('location', '') or '').strip()[:40]
                lines.append(f"  [{d}] {t} {loc}")
        else:
            lines.append("下次法說會: (無已公告)")

        if not past.empty:
            lines.append("過去法說會:")
            for _, row in past.iterrows():
                d = row['event_date'].strftime('%Y-%m-%d')
                t = str(row.get('event_time', '') or '').strip()
                loc = str(row.get('location', '') or '').strip()[:40]
                lines.append(f"  [{d}] {t} {loc}")

        return "\n".join(lines)
    except Exception as e:
        return f"N/A (earnings_calendar 讀取失敗: {e})"


def _build_market_context_news(days: int = 7, max_events: int = 10) -> str:
    """[MARKET_CONTEXT] B6 macro/sector news section (Council BLOCKER #6).

    讀 articles_recent filter article_type IN ('macro','sector') 近 N 天，
    dedupe by event_id, cap N 篇，列日期 / topic / 影響產業 / sentiment / source。
    News Initiative Phase 0 Commit 6 — 第三輪「macro/sector 救起來」對應 reader。
    """
    try:
        import pandas as pd
        from pathlib import Path as _P
        repo = _P(__file__).resolve().parent
        path = repo / 'data' / 'news' / 'articles_recent.parquet'
        if not path.exists():
            return ''  # empty section, 不污染 prompt
        df = pd.read_parquet(path)
        if df.empty or 'article_type' not in df.columns:
            return ''
        sub = df[df['article_type'].isin(['macro', 'sector'])].copy()
        if sub.empty:
            return ''
        sub['date'] = pd.to_datetime(sub['date'], errors='coerce')
        cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=days)
        sub = sub[sub['date'] >= cutoff]
        if sub.empty:
            return ''
        # BLOCKER #1 dedupe
        if 'event_id' in sub.columns:
            try:
                import sys
                sys.path.insert(0, str(repo / 'tools'))
                from news_theme_extract import dedupe_by_event_id
                sub = dedupe_by_event_id(sub)
            except Exception:
                pass
        sub = sub.sort_values(['date', 'confidence'], ascending=[False, False]).head(max_events)
        lines = []
        seen_titles = set()
        for _, row in sub.iterrows():
            title = str(row['title'])[:80]
            if title in seen_titles:
                continue
            seen_titles.add(title)
            d = row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else '?'
            atype = row['article_type']
            tag = row.get('sector_tag') if atype == 'sector' else row.get('macro_topic')
            tone = row.get('tone', 'neutral')
            sent = float(row.get('sentiment', 0.0) or 0.0)
            lines.append(f"  [{d}] [{atype}/{tag}] ({tone} {sent:+.2f}) {title}")
            if len(lines) >= max_events:
                break
        if not lines:
            return ''
        return ("\n近 7 天大盤總經 + 產業整體新聞 (macro/sector, dedupe by event):\n"
                + "\n".join(lines))
    except Exception:
        return ''


def _build_market_context(report):
    """[MARKET_CONTEXT] 市場環境 + B6 macro/sector news"""
    lines = []
    regime = report.get('regime', {})
    if regime:
        lines.append(f"Regime: {regime.get('regime', 'unknown')}")
        lines.append(f"Confidence: {_safe_val(regime.get('confidence', 0), '.2f')}")
        lines.append(f"Position Adj: {_safe_val(regime.get('position_adj', 1.0), '.1f')}")
        lines.append(f"HMM State: {regime.get('hmm_state', 'N/A')}")
        for d in regime.get('details', []):
            lines.append(f"- {d}")

    # Action plan — 只丟 final 1 組，不丟 candidate list (sl_list/tp_list/sl_atr/sl_ma/sl_low)
    # 漂移 root cause：原本 for k, v in ap.items() 把所有 candidate 攤給 Claude，
    # Claude 從中挑/round/憑空生新數字 → 個股分析 vs AI 報告價位不一致
    ap = report.get('action_plan', {})
    if ap and ap.get('is_actionable'):
        lines.append(f"\nAction Plan (DETERMINISTIC — must be quoted verbatim):")
        lines.append(f"  rec_entry_low: {ap.get('rec_entry_low', 0)}")
        lines.append(f"  rec_entry_high: {ap.get('rec_entry_high', 0)}")
        lines.append(f"  rec_entry_desc: {ap.get('rec_entry_desc', '')}")
        lines.append(f"  rec_sl_price: {ap.get('rec_sl_price', 0)}")
        lines.append(f"  rec_sl_method: {ap.get('rec_sl_method', '')}")
        lines.append(f"  rec_tp_price: {ap.get('rec_tp_price', 0)}")
        lines.append(f"  rr_ratio: {_safe_val(ap.get('rr_ratio', 0), '.2f')}")
        lines.append(f"  entry_confidence: {ap.get('entry_confidence', 'standard')}")
        if ap.get('strategy'):
            lines.append(f"  strategy: {ap.get('strategy')}")
        if ap.get('pattern_note'):
            lines.append(f"  pattern_note: {ap.get('pattern_note')}")
    elif ap:
        lines.append(f"\nAction Plan: 觀望 (is_actionable=False)")

    # Checklist
    cl = report.get('checklist', [])
    if cl:
        lines.append(f"\nMonitoring Checklist:")
        for c in cl:
            lines.append(f"  - {c}")

    # Fundamental alerts
    fa = report.get('fundamental_alerts', [])
    if fa:
        lines.append(f"\nFundamental Alerts:")
        for a in fa:
            lines.append(f"  - {a}")

    # B6 (Council BLOCKER #6): macro/sector news 段 (第三輪「不浪費資訊」對應 reader)
    macro_news = _build_market_context_news(days=7, max_events=10)
    if macro_news:
        lines.append(macro_news)

    return "\n".join(lines) if lines else "N/A"


def _build_pattern_data(df_day):
    """[PATTERN_DATA] K 線型態"""
    if df_day is None or df_day.empty:
        return "N/A"
    if 'Pattern' not in df_day.columns:
        return "N/A"

    # 取近 5 日有型態的
    recent = df_day.tail(10)
    patterns = []
    for idx, row in recent.iterrows():
        p = row.get('Pattern', None)
        pt = row.get('Pattern_Type', None)
        if p and str(p) not in ('', 'None', 'nan'):
            date_str = idx.strftime('%Y-%m-%d') if hasattr(idx, 'strftime') else str(idx)
            patterns.append(f"{date_str}: {p} ({pt})")

    return "\n".join(patterns) if patterns else "近 10 日無明確 K 線型態"


def _build_news_data(ticker, fund_data):
    """[NEWS_DATA] Recent news + analyst targets from Google News RSS."""
    try:
        from news_fetcher import (fetch_stock_news, format_news_for_prompt,
                                  extract_analyst_targets, format_analyst_targets)

        # Get stock name for better search
        stock_name = ''
        if fund_data:
            for key in ['stock_name', 'Name', 'shortName']:
                val = fund_data.get(key, '')
                if val and str(val) not in ('', 'N/A', 'None'):
                    stock_name = str(val)
                    break

        news = fetch_stock_news(ticker, stock_name=stock_name, max_items=15, days=7)
        parts = [format_news_for_prompt(news, max_chars=2500)]

        # Extract analyst targets
        targets = extract_analyst_targets(news)
        target_text = format_analyst_targets(targets)
        if target_text:
            parts.append(f"\n{target_text}")

        return "\n".join(parts)

    except Exception as e:
        logger.warning("News fetch failed for %s: %s", ticker, e)
        return f"N/A (news fetch failed: {e})"


def _build_analyst_consensus(ticker):
    """[ANALYST_CONSENSUS] Analyst target prices, EPS estimates, ratings from yfinance."""
    try:
        import yfinance as yf

        # Determine proper Yahoo ticker
        is_us = ticker and not ticker.replace('.TW', '').isdigit()
        if is_us:
            yticker = ticker
        else:
            stock_id = ticker.replace('.TW', '').replace('.TWO', '')
            # Try .TW first, then .TWO
            yticker = f"{stock_id}.TW"
            t = yf.Ticker(yticker)
            if not t.info.get('targetMeanPrice'):
                yticker = f"{stock_id}.TWO"

        t = yf.Ticker(yticker)
        info = t.info

        lines = []

        # Analyst ratings
        n_analysts = info.get('numberOfAnalystOpinions', 0)
        if n_analysts:
            rec = info.get('recommendationKey', 'N/A')
            rec_mean = info.get('recommendationMean', 0)
            avg_rating = info.get('averageAnalystRating', '')
            lines.append(f"Analyst Count: {n_analysts}")
            lines.append(f"Consensus Rating: {rec} (mean: {_safe_val(rec_mean)})")
            if avg_rating:
                lines.append(f"Average Rating: {avg_rating}")

        # Target prices
        tp_mean = info.get('targetMeanPrice')
        tp_high = info.get('targetHighPrice')
        tp_low = info.get('targetLowPrice')
        tp_median = info.get('targetMedianPrice')
        if tp_mean:
            lines.append(f"\nTarget Price:")
            lines.append(f"  Mean: {_safe_val(tp_mean, '.0f')}")
            lines.append(f"  Median: {_safe_val(tp_median, '.0f')}")
            lines.append(f"  High: {_safe_val(tp_high, '.0f')}")
            lines.append(f"  Low: {_safe_val(tp_low, '.0f')}")

            # Upside/downside from current price
            current = info.get('currentPrice') or info.get('regularMarketPrice', 0)
            if current and current > 0:
                upside = (tp_mean - current) / current * 100
                lines.append(f"  Current: {_safe_val(current, '.0f')} (upside: {upside:+.1f}%)")

        # EPS estimates
        trailing_eps = info.get('trailingEps')
        forward_eps = info.get('forwardEps')
        eps_current_yr = info.get('epsCurrentYear')
        if trailing_eps or forward_eps:
            lines.append(f"\nEPS Estimates:")
            if trailing_eps:
                lines.append(f"  Trailing (TTM): {_safe_val(trailing_eps)}")
            if eps_current_yr:
                lines.append(f"  Current Year: {_safe_val(eps_current_yr)}")
            if forward_eps:
                lines.append(f"  Forward: {_safe_val(forward_eps)}")

        # Growth rates
        eg = info.get('earningsGrowth')
        rg = info.get('revenueGrowth')
        eqg = info.get('earningsQuarterlyGrowth')
        if eg or rg:
            lines.append(f"\nGrowth:")
            if eg:
                lines.append(f"  Earnings Growth: {eg*100:+.1f}%")
            if eqg:
                lines.append(f"  Earnings Q/Q Growth: {eqg*100:+.1f}%")
            if rg:
                lines.append(f"  Revenue Growth: {rg*100:+.1f}%")

        # Forward PE and PEG
        fpe = info.get('forwardPE')
        peg = info.get('pegRatio')
        if fpe:
            lines.append(f"\nForward PE: {_safe_val(fpe)}")
        if peg:
            lines.append(f"PEG Ratio: {_safe_val(peg)}")

        if not lines:
            return "N/A (no analyst data)"
        return "\n".join(lines)

    except Exception as e:
        logger.warning("Analyst consensus fetch failed for %s: %s", ticker, e)
        return f"N/A (fetch failed: {e})"


def _build_theme_context(ticker):
    """[THEME_CONTEXT] AI era 多題材 conditioning — 帶入 theme description + peer 成員清單.

    與 PEER_COMPARISON 互補：peer 是估值同業，theme 是 AI era catalyst 共振。
    一檔股票可同時跨多個 theme（如 2330=foundry+ai_chip+ai_packaging）。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return "N/A (theme metadata is TW-only, see sector_tags_manual.json)"

    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()
    try:
        from peer_comparison import get_ticker_themes, get_theme_peers, _load_theme_index, _THEME_NAMES
        _load_theme_index()
        themes = get_ticker_themes(stock_id)
        if not themes:
            return "本檔無 AI era 主流題材標記（不在 sector_tags_manual.json 140 ticker 名單）。"

        # Load full theme metadata for description
        from pathlib import Path as _P
        import json as _json
        theme_path = _P(__file__).resolve().parent / 'data' / 'sector_tags_manual.json'
        with theme_path.open(encoding='utf-8') as _f:
            manual = _json.load(_f)
        theme_full = {t['theme_id']: t for t in manual.get('themes', []) if isinstance(t, dict) and t.get('theme_id')}

        lines = [f"本檔屬於 {len(themes)} 個 AI era 主流題材："]
        for t in themes:
            tid = t['id']
            full = theme_full.get(tid, {})
            desc = full.get('description', '')
            tier1_ids = [s.get('ticker', '') for s in full.get('tier1', []) if isinstance(s, dict)]
            tier2_ids = [s.get('ticker', '') for s in full.get('tier2', []) if isinstance(s, dict)]
            tier_label = 'tier1' if stock_id in tier1_ids else ('tier2' if stock_id in tier2_ids else 'unknown')
            tier1_str = ', '.join(tier1_ids[:6]) + (f' (+{len(tier1_ids)-6})' if len(tier1_ids) > 6 else '')
            tier2_str = ', '.join(tier2_ids[:6]) + (f' (+{len(tier2_ids)-6})' if len(tier2_ids) > 6 else '')
            lines.append(f"\n- **{t['zh']}** ({tid}, 本檔屬 {tier_label})")
            if desc:
                lines.append(f"  描述: {desc}")
            if tier1_str:
                lines.append(f"  tier1 成員: {tier1_str}")
            if tier2_str:
                lines.append(f"  tier2 成員: {tier2_str}")

        if len(themes) > 1:
            lines.append(f"\n⚠️ 多題材交集: 本檔同時跨 {len(themes)} 個題材，catalyst 來源較分散；"
                          f"分析時請考慮各 theme 衝擊 weight 是否對等，或某一 theme 為主導。")
        return "\n".join(lines)
    except Exception as e:
        logger.warning("Theme context failed for %s: %s", ticker, e)
        return f"N/A (theme context failed: {e})"


def _build_sentiment_context(ticker):
    """[SENTIMENT_CONTEXT] 市場情緒 vs 個股情緒對比 (2026-05-01 Day 3 加).

    用 market_sentiment.get_sentiment_divergence() 輸出兩個 -100~+100 score
    + 4 種對比訊號 (個股獨立催化 / 個股逆風 / 相對強弱 / 跟盤同步)。

    幫 LLM 判斷：個股當前是不是有獨立 catalyst (market 弱 + stock 強)
    或被大盤拖累 (market 強 + stock 弱)。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return "N/A (sentiment module 目前只支援台股)"
    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()
    try:
        from market_sentiment import get_sentiment_divergence
        d = get_sentiment_divergence(stock_id)
        m = d['market']
        s = d['stock']
        lines = [
            f"市場情緒 (TWSE 大盤): {m['score']:+.1f}/100 ({m['label']})",
            f"  - 復用 TaiwanFearGreedIndex (Market Momentum / Breadth / PCR / Volatility / Margin)",
        ]
        if m.get('m1b_overlay'):
            lines.append(f"  - Overlay: {m['m1b_overlay']['msg']}")
        lines.append(f"")
        lines.append(f"個股情緒: {s['score']:+.1f}/100 ({s['label']})")
        comp = s.get('components', {})
        comp_parts = []
        if 'inst' in comp:
            comp_parts.append(f"法人 {comp['inst']:+.2f}")
        if 'margin' in comp:
            comp_parts.append(f"融資反向 {comp['margin']:+.2f}")
        if 'ma' in comp:
            comp_parts.append(f"MA 乖離 {comp['ma']:+.2f}")
        if 'news' in comp:
            comp_parts.append(f"News tone {comp['news']:+.2f}")
        if comp_parts:
            lines.append(f"  - 訊號 (-1~+1): {' | '.join(comp_parts)}")
        lines.append(f"")
        lines.append(f"對比訊號: **{d['signal']}** (個股 - 大盤 = {d['diff']:+.1f})")
        if abs(d['diff']) >= 50:
            if d['diff'] > 0:
                lines.append(f"⚠️ 強 divergence: 個股遠強於大盤，**獨立 catalyst**，"
                            f"分析時注重「為何此檔被選中而非跟盤」")
            else:
                lines.append(f"⚠️ 強 divergence: 個股遠弱於大盤，**逆風 warning**，"
                            f"分析時注意 trigger 風險或基本面問題")
        return "\n".join(lines)
    except Exception as e:
        logger.warning("Sentiment context failed for %s: %s", ticker, e)
        return f"N/A (sentiment context failed: {e})"


def _build_news_themes_context(ticker):
    """[NEWS_THEMES] 近 30 天該檔在 news 萃取裡出現的 catalyst (2026-05-01 Day 3 加).

    News Initiative Phase 0 Commit 6: 從 data/news_themes.parquet (legacy 30d)
    切到 data/news/articles_recent.parquet (新 hot view 90d)，filter 30 天視窗對齊
    既有行為。BLOCKER #1 dedupe by event_id 防同事件多 source 灌。
    Reader fallback to legacy: 新 path 不存在或讀取失敗時 fallback 舊 path,
    提供 graceful degradation (Robustness > cleanliness, 永久保留)。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return "N/A (news theme parquet 目前只覆蓋台股)"
    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()
    try:
        import pandas as pd
        from pathlib import Path as _P
        repo = _P(__file__).resolve().parent
        new_path = repo / 'data' / 'news' / 'articles_recent.parquet'
        legacy_path = repo / 'data' / 'news_themes.parquet'
        path = new_path if new_path.exists() else legacy_path
        if not path.exists():
            return "N/A (news parquet 尚未產生，scanner 跑過後才有)"
        df = pd.read_parquet(path)
        if df.empty:
            return "N/A (news parquet 為空)"
        # filter individual articles only (sector/macro/price_only 不提及具體 ticker)
        if 'article_type' in df.columns:
            df = df[df['article_type'] == 'individual']
        sub = df[df['ticker'].astype(str) == stock_id].copy()
        if sub.empty:
            return f"近 30 天 news 無提到本檔 (parquet 中不含 {stock_id})"

        # BLOCKER #1: dedupe by event_id 防同事件多 source 灌
        if 'event_id' in sub.columns:
            try:
                import sys
                sys.path.insert(0, str(repo / 'tools'))
                from news_theme_extract import dedupe_by_event_id
                sub = dedupe_by_event_id(sub)
            except Exception:
                pass

        # 按 confidence + date desc 排序，取 top 8 篇
        sub['date'] = pd.to_datetime(sub['date'], errors='coerce')
        sub = sub.sort_values(['date', 'confidence'], ascending=[False, False]).head(8)

        # theme 統計
        theme_counts = sub['theme'].value_counts().head(5)
        theme_lines = [f"- {t}: {n} 篇提及" for t, n in theme_counts.items()]

        # 文章列表
        article_lines = []
        seen_titles = set()
        for _, row in sub.iterrows():
            title = str(row['title'])[:80]
            if title in seen_titles:
                continue
            seen_titles.add(title)
            d = row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else '?'
            tone = row['tone']
            sent = float(row['sentiment'])
            article_lines.append(f"  [{d}] ({tone} {sent:+.2f}) {title}")
            if len(article_lines) >= 5:
                break

        return ("近 30 天 news catalyst 統計 (Sonnet 萃取):\n"
                + "\n".join(theme_lines)
                + "\n\nTop 文章:\n"
                + "\n".join(article_lines))
    except Exception as e:
        logger.warning("News themes context failed for %s: %s", ticker, e)
        return f"N/A (news themes failed: {e})"


def _build_peer_data(ticker, fund_data):
    """[PEER_COMPARISON] Peer industry comparison."""
    is_us = ticker and not ticker.replace('.TW', '').isdigit()

    try:
        if is_us:
            from peer_comparison import get_us_peer_comparison, format_peer_comparison
            result = get_us_peer_comparison(ticker)
        else:
            from peer_comparison import get_tw_peer_comparison, format_peer_comparison
            stock_id = ticker.replace('.TW', '')
            result = get_tw_peer_comparison(stock_id)

        return format_peer_comparison(result)

    except Exception as e:
        logger.warning("Peer comparison failed for %s: %s", ticker, e)
        return f"N/A (peer comparison failed: {e})"


# Module-level RAG resources (lazy-load shared across requests)
_RAG_MODEL = None
_RAG_COLLECTION = None
_RAG_DB_PATH = os.path.join(os.path.dirname(__file__),
                            'data_cache', 'transcripts', '_chromadb')
_RAG_SIM_GATE = 0.40  # top-1 sim < 此值 → 不帶進 prompt（避免簡報 PDF 雜訊）

# Post-filter: 簡報 PDF 共通的 boilerplate (封面/目錄/免責聲明)，無實質 forward guidance
# 過濾邏輯: chunk 命中 >= 2 個 pattern 視為 boilerplate, 跳過
import re as _re
_RAG_BOILERPLATE_PATTERNS = [
    _re.compile(r'著作權所有|All [Rr]ights [Rr]eserved|©\s*20\d\d', _re.UNICODE),
    _re.compile(r'免責聲明|投資安全聲明|[Dd]isclaimer', _re.UNICODE),
    _re.compile(r'預測性陳述|預測性資訊|[Ff]orward.?looking [Ss]tatements?', _re.UNICODE),
    _re.compile(r'簡報內所提供之資訊|本簡報.{0,20}(發佈|提供|揭露)', _re.UNICODE),
    _re.compile(r'第[一二三四1234]\s*季法人說明會\s*$|[Ii]nvestor [Cc]onference\s*$', _re.UNICODE | _re.MULTILINE),
    _re.compile(r'[Cc]opyright|商業機密|本公司未來實際所發生', _re.UNICODE),
]


def _is_rag_boilerplate(text: str) -> bool:
    """Detect if chunk is mostly boilerplate (cover/disclaimer/footer)."""
    matches = sum(1 for p in _RAG_BOILERPLATE_PATTERNS if p.search(text))
    return matches >= 2


def _ensure_rag_resources():
    """Lazy-load embedding model + chromadb collection. False = unavailable."""
    global _RAG_MODEL, _RAG_COLLECTION
    if _RAG_MODEL is False:
        return False  # already known failed
    if _RAG_MODEL is not None and _RAG_COLLECTION is not None:
        return True
    try:
        from sentence_transformers import SentenceTransformer
        import chromadb
        _RAG_MODEL = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        client = chromadb.PersistentClient(path=_RAG_DB_PATH)
        _RAG_COLLECTION = client.get_collection('transcripts_top300')
        return True
    except Exception as e:
        logger.warning("RAG resources unavailable: %s", e)
        _RAG_MODEL = False
        return False


def _build_law_transcript_rag(ticker, fund_data=None):
    """[LAW_TRANSCRIPT_RAG] 從本檔過去法說會 PDF 中 RAG 撈 top-5 相關段落。

    僅台股 (US 不支援)；similarity gate top-1 < 0.45 → 不帶進 prompt。
    Multi-query (forward guidance / 策略進展 / 風險) 提升 recall。
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    if is_us:
        return "N/A (RAG 目前只覆蓋台股 top 300 法說會 PDFs)"

    stock_id = ticker.replace('.TW', '').replace('.TWO', '').strip()

    if not _ensure_rag_resources():
        return "N/A (RAG resources 載入失敗，可能 chromadb 未建或 model 未下載)"

    try:
        # Multi-query for breadth - 用具體時間/業務詞避免 disclaimer match
        queries = ['明年 全年 業績展望 營收 預期', '重要新產品 客戶 出貨 進展', 'AI 半導體 產能 毛利率']
        merged = {}  # key: doc-prefix, value: dict
        for q in queries:
            qe = _RAG_MODEL.encode([q])[0].tolist()
            # over-fetch n=10 to leave room for boilerplate post-filter
            res = _RAG_COLLECTION.query(
                query_embeddings=[qe],
                n_results=10,
                where={'ticker': stock_id},
            )
            if not res['documents'] or not res['documents'][0]:
                continue
            for doc, meta, dist in zip(res['documents'][0], res['metadatas'][0], res['distances'][0]):
                # post-filter boilerplate
                if _is_rag_boilerplate(doc):
                    continue
                sim = 1.0 - dist
                key = doc[:60]
                if key not in merged or sim > merged[key]['sim']:
                    merged[key] = {'doc': doc, 'meta': meta, 'sim': sim}

        if not merged:
            return f"N/A (本檔 {stock_id} 在法說會 RAG 中無有效命中，過濾 boilerplate 後 0 hits)"

        ranked = sorted(merged.values(), key=lambda x: -x['sim'])[:5]
        top_sim = ranked[0]['sim']

        if top_sim < _RAG_SIM_GATE:
            return (f"N/A (本檔 {stock_id} 法說會 RAG semantic match 弱 "
                    f"(top sim={top_sim:.2f} < gate {_RAG_SIM_GATE})，可能為簡報 PDF 而非真逐字稿，"
                    f"避免低品質雜訊不帶入 prompt)")

        lines = [f"從本檔 ({stock_id}) 過去法說會 PDF 中 RAG retrieve 出最相關段落 "
                 f"(top {len(ranked)} hits, multi-query dedup)，**僅作背景參考**："]
        for i, r in enumerate(ranked, 1):
            speaker = r['meta'].get('speaker', 'Unknown')
            date = r['meta'].get('date', '?')
            sim = r['sim']
            doc = r['doc'].replace('\n', ' / ').replace('\r', '')[:280]
            lines.append(f"\n[{i}] {date} {speaker} (sim={sim:.2f}): {doc}")
        lines.append(f"\n⚠️ 此 section 是 PDF retrieval 結果，可能包含簡報投影片片段（非完整句子）。"
                     f"使用時優先看高 sim chunks (>= 0.6)，引用前自行判斷上下文連貫性。")
        return '\n'.join(lines)
    except Exception as e:
        logger.warning("RAG retrieval failed for %s: %s", ticker, e)
        return f"N/A (RAG retrieval 失敗: {type(e).__name__}: {e})"


def assemble_prompt(ticker, report, chip_data, us_chip_data, fund_data, df_day,
                    include_songfen=False):
    """
    組裝完整的 AI 分析 prompt。

    Args:
        ticker: 股票代號
        report: TechnicalAnalyzer.run_analysis() 的回傳結果
        chip_data: 台股籌碼數據 (dict of DataFrames)
        us_chip_data: 美股籌碼數據 (dict)
        fund_data: 基本面數據 (dict)
        df_day: 日線 DataFrame (含技術指標)
        include_songfen: bool，True 時附加「宋分視角補充分析」區塊（見 prompts/songfen_framework.md）

    Returns:
        str: 完整 prompt (system + data)
    """
    is_us = ticker and not ticker.replace('.TW', '').isdigit()

    system_prompt = _load_system_prompt()

    data_sections = []
    data_sections.append(f"[STOCK_INFO]\n{_build_stock_info(ticker, report, fund_data, df_day)}")
    data_sections.append(f"[TRIGGER_SCORE]\n{_build_trigger_score(report)}")
    data_sections.append(f"[TRIGGER_DETAILS]\n{_build_trigger_details(report)}")
    data_sections.append(f"[TECHNICAL_DATA]\n{_build_technical_data(df_day)}")
    data_sections.append(f"[CHIP_DATA]\n{_build_chip_data(chip_data, us_chip_data, is_us, ticker=ticker)}")
    data_sections.append(f"[FUNDAMENTAL_DATA]\n{_build_fundamental_data(fund_data, ticker)}")
    data_sections.append(f"[MARKET_CONTEXT]\n{_build_market_context(report)}")
    data_sections.append(f"[PATTERN_DATA]\n{_build_pattern_data(df_day)}")
    data_sections.append(f"[VALUE_SCORE]\n{_build_value_score(ticker, fund_data, df_day)}")
    data_sections.append(f"[NEWS_DATA]\n{_build_news_data(ticker, fund_data)}")
    data_sections.append(f"[ANALYST_CONSENSUS]\n{_build_analyst_consensus(ticker)}")
    data_sections.append(f"[PEER_COMPARISON]\n{_build_peer_data(ticker, fund_data)}")
    data_sections.append(f"[THEME_CONTEXT]\n{_build_theme_context(ticker)}")
    data_sections.append(f"[SENTIMENT_CONTEXT]\n{_build_sentiment_context(ticker)}")
    data_sections.append(f"[NEWS_THEMES]\n{_build_news_themes_context(ticker)}")
    data_sections.append(f"[FORWARD_GUIDANCE]\n{_build_forward_guidance_context(ticker)}")
    data_sections.append(f"[EARNINGS_CALENDAR]\n{_build_earnings_calendar_context(ticker)}")
    data_sections.append(f"[ANALYST_TARGETS]\n{_build_analyst_targets_context(ticker)}")
    _flow_alert = _build_news_flow_alert_context(ticker)
    if _flow_alert:  # 該檔近期爆量才加段, 避免污染 prompt
        data_sections.append(f"[NEWS_FLOW_ALERT]\n{_flow_alert}")
    _theme_mom = _build_theme_momentum_context(ticker)
    if _theme_mom:
        data_sections.append(f"[THEME_MOMENTUM]\n{_theme_mom}")
    data_sections.append(f"[LAW_TRANSCRIPT_RAG]\n{_build_law_transcript_rag(ticker, fund_data)}")

    data_block = "\n\n".join(data_sections)

    # Determine stock name for search context
    stock_name = ''
    if fund_data:
        for key in ['stock_name', 'Name', 'shortName', 'Sector']:
            val = fund_data.get(key, '')
            if val and str(val) not in ('', 'N/A', 'None'):
                stock_name = str(val)
                break

    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    stock_id = ticker.replace('.TW', '') if not is_us else ticker

    full_prompt = f"""{system_prompt}

---

# 以下是 StockPulse 系統提供的 {ticker} ({stock_name}) 完整分析數據

{data_block}

---

## 你的任務

1. **使用 WebSearch 工具**搜尋以下資訊來補充分析（搜尋 2-4 次即可）：
   - "{stock_id} {stock_name} 產業趨勢 2026" — 產業動態、上下游供需
   - "{stock_id} {stock_name} 法說會 營運展望" — 公司最新展望、產品線變化
   - "{stock_id} 競爭對手 比較" — 主要競爭者的營收/毛利率比較
   若為美股可搜尋英文: "{ticker} industry outlook 2026", "{ticker} competitors analysis"

2. **整合系統數據 + 搜尋結果**，產出完整研究報告
   - 系統數據用於量化分析（技術面、籌碼面、評分）
   - 搜尋結果用於質化分析（產業趨勢、護城河、風險）
   - 分析師共識數據用於情境目標價推導

3. 報告格式嚴格依照上方 Format 規範的 8 大區塊"""

    # 附加：宋分視角補充分析區塊（可選）
    if include_songfen:
        songfen_content = _load_songfen_block()
        if songfen_content:
            full_prompt += """

---

## 【附加任務】宋分視角補充分析區塊

除上方 9 個標準區塊外，**在報告最末尾新增第 10 區塊**，標題為 `## 10. 宋分視角補充分析`，使用下列「宋分分析師底層框架」套在本檔股票上。

**Format 規範**（嚴格遵守）：

1. 用繁體中文
2. 不要照抄框架原文 — 框架是思考工具，要輸出**針對 {TICKER} 的具體結論**
3. 子區塊固定四個視角 + 反面論點：
   - `### 10.1 re-rate 訊號檢核`（判斷目前在定價三階段哪一階段、若 re-rate 會是哪等級）
   - `### 10.2 5-layer 損益表拆解重點`（ROIC 趨勢 / 營業槓桿位置 / 標準化 EPS）
   - `### 10.3 擇時與紀律`（Thesis / 看錯條件 / 當前加碼訊號對應倉位）
   - `### 10.4 當期 theme 對照`（從下方 Current Themes 挑 1-3 項相關的）
   - `### 10.5 反面論點（至少 3 點）`（具體到訊號層級，不空泛）
4. 最後用一句話總結：此股票在宋分框架下處於什麼位置

**禁止**：
- 把框架原文照貼
- 用「可能」「或許」掩蓋不確定 → 改說「數據不足」
- 引用超過 3 個月的 time-sensitive 觀點

---

## 宋分分析師底層框架（供參考，不要原文照抄）

""" + songfen_content

    return full_prompt


def generate_report(ticker, report, chip_data, us_chip_data, fund_data, df_day,
                    timeout=600, include_songfen=False):
    """
    呼叫 Claude CLI 生成 AI 研究報告（含 WebSearch 能力）。

    LLM 規範 (2026-05-01)：
    - 強制 Opus model + `--allowedTools "*"` 允許所有工具
    - timeout 預設 10 min (600s)，可由 caller 覆寫

    Args:
        timeout: None = no timeout (default), or seconds

    Returns:
        tuple: (success: bool, content: str)
    """
    prompt = assemble_prompt(ticker, report, chip_data, us_chip_data, fund_data, df_day,
                             include_songfen=include_songfen)

    logger.info("AI Report prompt assembled for %s (%d chars, songfen=%s)",
                ticker, len(prompt), include_songfen)

    try:
        result = subprocess.run(
            [_CLAUDE_CLI, "-p",
             "--model", "opus",
             "--allowedTools", "*",
             "--output-format", "text"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding='utf-8',
        )

        if result.returncode != 0:
            err = result.stderr.strip() if result.stderr else "Unknown error"
            logger.error("Claude CLI error: %s", err)
            return False, f"Claude CLI 錯誤 (exit code {result.returncode}):\n{err}"

        output = result.stdout.strip()
        if not output:
            return False, "Claude CLI 回傳空白結果"

        return True, output

    except subprocess.TimeoutExpired:
        logger.error("Claude CLI timeout after %ds", timeout)
        return False, f"Claude CLI 逾時 ({timeout} 秒)，請稍後重試"
    except FileNotFoundError:
        logger.error("Claude CLI not found")
        return False, "找不到 claude 指令。請確認已安裝 Claude Code CLI 並在 PATH 中。"
    except Exception as e:
        logger.error("AI Report generation failed: %s", e, exc_info=True)
        return False, f"生成失敗: {e}"


# ================================================================
# Dashboard Mode — 輸出 HTML 互動儀表板
# ================================================================

def assemble_dashboard_prompt(ticker, report, chip_data, us_chip_data, fund_data, df_day):
    """組裝儀表板模式 prompt（要求 Claude 輸出 JSON）"""
    is_us = ticker and not ticker.replace('.TW', '').isdigit()
    system_prompt = _load_dashboard_prompt()

    data_sections = [
        f"[STOCK_INFO]\n{_build_stock_info(ticker, report, fund_data, df_day)}",
        f"[TRIGGER_SCORE]\n{_build_trigger_score(report)}",
        f"[TRIGGER_DETAILS]\n{_build_trigger_details(report)}",
        f"[TECHNICAL_DATA]\n{_build_technical_data(df_day)}",
        f"[CHIP_DATA]\n{_build_chip_data(chip_data, us_chip_data, is_us, ticker=ticker)}",
        f"[FUNDAMENTAL_DATA]\n{_build_fundamental_data(fund_data, ticker)}",
        f"[MARKET_CONTEXT]\n{_build_market_context(report)}",
        f"[PATTERN_DATA]\n{_build_pattern_data(df_day)}",
        f"[VALUE_SCORE]\n{_build_value_score(ticker, fund_data, df_day)}",
        f"[NEWS_DATA]\n{_build_news_data(ticker, fund_data)}",
        f"[ANALYST_CONSENSUS]\n{_build_analyst_consensus(ticker)}",
        f"[PEER_COMPARISON]\n{_build_peer_data(ticker, fund_data)}",
        f"[THEME_CONTEXT]\n{_build_theme_context(ticker)}",
        f"[SENTIMENT_CONTEXT]\n{_build_sentiment_context(ticker)}",
        f"[NEWS_THEMES]\n{_build_news_themes_context(ticker)}",
        f"[FORWARD_GUIDANCE]\n{_build_forward_guidance_context(ticker)}",
        f"[EARNINGS_CALENDAR]\n{_build_earnings_calendar_context(ticker)}",
        f"[ANALYST_TARGETS]\n{_build_analyst_targets_context(ticker)}",
    ]
    _flow_alert2 = _build_news_flow_alert_context(ticker)
    if _flow_alert2:
        data_sections.append(f"[NEWS_FLOW_ALERT]\n{_flow_alert2}")
    _theme_mom2 = _build_theme_momentum_context(ticker)
    if _theme_mom2:
        data_sections.append(f"[THEME_MOMENTUM]\n{_theme_mom2}")
    data_sections += [
        f"[LAW_TRANSCRIPT_RAG]\n{_build_law_transcript_rag(ticker, fund_data)}",
    ]
    data_block = "\n\n".join(data_sections)

    stock_name = ''
    if fund_data:
        for key in ['stock_name', 'Name', 'shortName']:
            val = fund_data.get(key, '')
            if val and str(val) not in ('', 'N/A', 'None'):
                stock_name = str(val)
                break

    stock_id = ticker.replace('.TW', '') if not is_us else ticker

    full_prompt = f"""{system_prompt}

---

# 以下是 StockPulse 系統提供的 {ticker} ({stock_name}) 完整分析數據

{data_block}

---

## 你的任務

1. **可選搜尋補充**（2-3 次即可）:
   - "{stock_id} {stock_name} 產業趨勢 2026"
   - "{stock_id} 競爭對手 比較"
   - 美股: "{ticker} industry outlook 2026"

2. **輸出嚴格符合 schema 的純 JSON**，必含 5 個頂層物件：meta / summary / technical / chip / valuation / bull_bear

3. **第一個字元必為 `{{`，最後一個字元必為 `}}`**。禁止輸出 markdown 程式碼圍欄、說明文字、或任何非 JSON 內容。"""

    return full_prompt


def _extract_json_from_output(text):
    """從 Claude 輸出提取 JSON。處理常見污染（markdown 圍欄、前後文字）。"""
    text = text.strip()
    # 剝除 markdown 程式碼圍欄
    if text.startswith('```'):
        # 移除第一行（```json 或 ```）
        first_nl = text.find('\n')
        if first_nl > 0:
            text = text[first_nl+1:]
        # 移除尾部圍欄
        text = text.rstrip()
        if text.endswith('```'):
            text = text[:-3].rstrip()
    # 若前後還有非 JSON 文字，嘗試找第一個 { 和最後一個 }
    if not text.startswith('{'):
        first_brace = text.find('{')
        if first_brace >= 0:
            text = text[first_brace:]
    if not text.endswith('}'):
        last_brace = text.rfind('}')
        if last_brace >= 0:
            text = text[:last_brace+1]
    return text


def generate_report_html(ticker, report, chip_data, us_chip_data, fund_data, df_day,
                         timeout=600):
    """
    生成 HTML 互動儀表板報告。

    LLM 規範 (2026-05-01)：強制 Opus model + `--allowedTools "*"`，timeout 10 min。

    Returns:
        tuple: (success: bool, html_or_error_msg: str, json_data: dict | None)
    """
    prompt = assemble_dashboard_prompt(ticker, report, chip_data, us_chip_data, fund_data, df_day)
    logger.info("Dashboard prompt assembled for %s (%d chars)", ticker, len(prompt))

    try:
        result = subprocess.run(
            [_CLAUDE_CLI, "-p",
             "--model", "opus",
             "--allowedTools", "*",
             "--output-format", "text"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding='utf-8',
        )

        if result.returncode != 0:
            err = result.stderr.strip() if result.stderr else "Unknown error"
            logger.error("Claude CLI error: %s", err)
            return False, f"Claude CLI 錯誤 (exit code {result.returncode}):\n{err}", None

        raw_output = result.stdout.strip()
        if not raw_output:
            return False, "Claude CLI 回傳空白結果", None

        # 提取 + 解析 JSON
        json_text = _extract_json_from_output(raw_output)
        try:
            data = json.loads(json_text)
        except json.JSONDecodeError as e:
            logger.error("JSON parse failed: %s", e)
            logger.error("First 1000 chars of output: %s", json_text[:1000])
            return False, f"Claude 回傳的 JSON 格式錯誤: {e}\n\n前 1000 字:\n{json_text[:1000]}", None

        # 驗證必要欄位
        required = ['meta', 'summary', 'technical', 'chip', 'valuation', 'bull_bear']
        missing = [k for k in required if k not in data]
        if missing:
            return False, f"JSON 缺少必要欄位: {missing}", data

        # 注入 HTML 模板
        template = _load_dashboard_template()
        if not template:
            return False, "HTML 模板載入失敗", data

        meta = data.get('meta', {})
        title = f"{meta.get('ticker', ticker)} {meta.get('name', '')} 研究報告"
        html = template.replace('__TITLE__', title)
        html = html.replace('__REPORT_JSON__', json.dumps(data, ensure_ascii=False))

        logger.info("HTML dashboard generated for %s (%d bytes)", ticker, len(html))
        return True, html, data

    except subprocess.TimeoutExpired:
        return False, f"Claude CLI 逾時 ({timeout} 秒)", None
    except FileNotFoundError:
        return False, "找不到 claude 指令。請確認已安裝 Claude Code CLI", None
    except Exception as e:
        logger.error("HTML report generation failed: %s", e, exc_info=True)
        return False, f"生成失敗: {e}", None


# ================================================================
# Report Library — Save / Load / List
# ================================================================

_REPORTS_DIR = os.path.join(os.path.dirname(__file__), 'data', 'ai_reports')
_INDEX_PATH = os.path.join(_REPORTS_DIR, 'index.json')


def _ensure_reports_dir():
    os.makedirs(_REPORTS_DIR, exist_ok=True)


def save_report(ticker, content, trigger_score=None, trend_score=None, value_score=None):
    """
    Save a markdown report to the library.

    Returns:
        str: report_id
    """
    _ensure_reports_dir()

    now = datetime.now()
    report_id = f"{ticker}_{now.strftime('%Y%m%d_%H%M%S')}"
    filename = f"{report_id}.md"
    filepath = os.path.join(_REPORTS_DIR, filename)

    # Save markdown content
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

    # Update index
    index = load_report_index()
    index.append({
        'report_id': report_id,
        'ticker': ticker,
        'date': now.strftime('%Y-%m-%d'),
        'time': now.strftime('%H:%M:%S'),
        'filename': filename,
        'format': 'md',
        'trigger_score': trigger_score,
        'trend_score': trend_score,
        'value_score': value_score,
    })

    with open(_INDEX_PATH, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    logger.info("Report saved: %s", report_id)
    return report_id


def save_report_html(ticker, html_content, trigger_score=None, trend_score=None,
                     value_score=None, json_data=None):
    """
    Save an HTML dashboard report to the library.
    選擇性把 JSON 原始資料一併保存（sidecar .json，利於日後換模板重新渲染）。

    Returns:
        str: report_id
    """
    _ensure_reports_dir()

    now = datetime.now()
    report_id = f"{ticker}_{now.strftime('%Y%m%d_%H%M%S')}"
    filename = f"{report_id}.html"
    filepath = os.path.join(_REPORTS_DIR, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)

    # Sidecar JSON (便於重新渲染 / 版本比對)
    if json_data is not None:
        try:
            json_path = os.path.join(_REPORTS_DIR, f"{report_id}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
        except Exception as _e:
            logger.warning("Sidecar JSON save failed: %s", _e)

    index = load_report_index()
    index.append({
        'report_id': report_id,
        'ticker': ticker,
        'date': now.strftime('%Y-%m-%d'),
        'time': now.strftime('%H:%M:%S'),
        'filename': filename,
        'format': 'html',
        'trigger_score': trigger_score,
        'trend_score': trend_score,
        'value_score': value_score,
    })

    with open(_INDEX_PATH, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    logger.info("HTML Report saved: %s", report_id)
    return report_id


def load_report_index():
    """Load the report index (list of metadata dicts)."""
    if os.path.exists(_INDEX_PATH):
        try:
            with open(_INDEX_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Report index load failed (%s): %s: %s. Falling back to empty index.",
                           _INDEX_PATH, type(e).__name__, e)
    return []


def load_report_content(report_id):
    """
    Load a report's content by report_id. 支援 .md 與 .html 兩種格式。
    會優先從 index 查 filename，查不到再 fallback 用副檔名猜測。
    """
    # 優先查 index
    index = load_report_index()
    for r in index:
        if r.get('report_id') == report_id:
            filename = r.get('filename', f"{report_id}.md")
            filepath = os.path.join(_REPORTS_DIR, filename)
            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    return f.read()
            break
    # Fallback: 嘗試兩種副檔名
    for ext in ('md', 'html'):
        filepath = os.path.join(_REPORTS_DIR, f"{report_id}.{ext}")
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read()
    return None


def get_report_filepath(report_id):
    """回傳報告的絕對路徑（供『在瀏覽器開啟』功能使用）。"""
    index = load_report_index()
    for r in index:
        if r.get('report_id') == report_id:
            filename = r.get('filename', f"{report_id}.md")
            filepath = os.path.join(_REPORTS_DIR, filename)
            if os.path.exists(filepath):
                return filepath
            break
    for ext in ('html', 'md'):
        filepath = os.path.join(_REPORTS_DIR, f"{report_id}.{ext}")
        if os.path.exists(filepath):
            return filepath
    return None


def delete_report(report_id):
    """Delete a report (both .md / .html / sidecar .json) from the library."""
    for ext in ('md', 'html', 'json'):
        filepath = os.path.join(_REPORTS_DIR, f"{report_id}.{ext}")
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception as _e:
                logger.warning("Failed to remove %s: %s", filepath, _e)

    index = load_report_index()
    index = [r for r in index if r['report_id'] != report_id]
    _ensure_reports_dir()
    with open(_INDEX_PATH, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    logger.info("Report deleted: %s", report_id)


def list_reports_for_ticker(ticker):
    """List all reports for a specific ticker, newest first."""
    index = load_report_index()
    return sorted(
        [r for r in index if r['ticker'] == ticker],
        key=lambda x: x.get('date', '') + x.get('time', ''),
        reverse=True,
    )


# ============================================================
# Post-validate (Phase 3 safety net) — 偵測 Section 8 三欄漂移
#
# 背景：Phase 1 砍 candidate 攤平 + prompt hard rule 後 2345 實測 100% 服從，
# 但 Council Prompt Engineer 警告長文 narrative 仍可能 5-15% violation
# (不同 ticker / 模型 / Section 8 表格寫法)，且 Auditor 在原 NVDA 報告
# 發現「第二類漂移」(憑空生成 $190 / $205 不在任何 candidate)。
#
# 這層 validator 是兜底：抓 Section 8 三欄關鍵 row 內所有看似價位數字，
# 跟 action_plan 4 final 數字 ±tolerance% 比對，超出即標 drift。
# 不檢查其他 section（情境目標價 EPS×PE 推導、毛利率、ATR 等屬合法計算）。
# ============================================================

def post_validate_numbers(report_md, action_plan, tolerance_pct=0.5):
    """檢查報告 Section 8 三欄是否漂移。

    Args:
        report_md: 報告 markdown content
        action_plan: report['action_plan'] dict (or ActionPlan dataclass)
        tolerance_pct: 容差 % (default 0.5)，最小絕對值 0.5

    Returns:
        dict {
            'drift': bool,
            'unexpected_numbers': list[float],  # 漂移數字
            'expected_numbers': list[float],    # ground truth 4 final
            'note': str,
        }
    """
    # ActionPlan dataclass 也支援 .get() (Phase 2)；dict 原本就支援
    is_actionable = action_plan.get('is_actionable') if action_plan else False
    if not is_actionable:
        return {'drift': False, 'unexpected_numbers': [],
                'expected_numbers': [], 'note': 'not actionable, skip'}

    # 抓 Section 8（## 8 開頭到 ## 9 之前 / 文末）
    m = re.search(r'##\s*8[\.、]?\s*投資建議.*?(?=\n##\s*9|\Z)',
                  report_md, re.DOTALL)
    if not m:
        return {'drift': False, 'unexpected_numbers': [],
                'expected_numbers': [], 'note': 'Section 8 not found, skip'}
    sec8 = m.group(0)

    # ground truth 4 final
    gt = []
    for k in ('rec_entry_low', 'rec_entry_high', 'rec_sl_price', 'rec_tp_price'):
        v = action_plan.get(k)
        if v and v > 0:
            gt.append(round(float(v), 2))
    if not gt:
        return {'drift': False, 'unexpected_numbers': [],
                'expected_numbers': [], 'note': 'no ground truth available'}

    # 抽出三欄 row（| 標籤 | 內容 | 模式，標籤前後可有 ** 加粗）
    target_labels = ('建議進場區間', '停損價位', '停利價位')
    # 過濾門檻：< gt 最小值 × 30% 視為非價位（避免抓到 5MA / 20MA / 0.7 倉位等）
    gt_min = min(gt)
    price_threshold = max(10.0, gt_min * 0.3)
    nums_in_rows = []
    for label in target_labels:
        row = re.search(
            rf'\|\s*\*{{0,2}}{re.escape(label)}\*{{0,2}}\s*\|\s*(.*?)\s*\|',
            sec8, re.DOTALL,
        )
        if not row:
            continue
        row_text = row.group(1)
        # 抓所有看似價位數字（支援千分位 2,212 / $XXX / 純數字）
        # negative lookahead (?![A-Za-z]) 排除「5MA」「20MA」「60d」這類後綴是字母的技術指標
        for raw in re.findall(r'\$?\s*([\d,]+\.?\d*)(?![A-Za-z])', row_text):
            num_str = raw.replace(',', '')
            try:
                f = float(num_str)
                # 排除年份（無小數 1900-2100）
                if num_str.find('.') == -1 and 1900 <= f <= 2100:
                    continue
                # 必須是合理價位（門檻來自 ground truth 動態計算）
                if f < price_threshold or f > 100000:
                    continue
                nums_in_rows.append(round(f, 2))
            except ValueError:
                continue

    # 比對：每個三欄抓出的數字 必須在 gt ±tolerance% 或 0.5 絕對容差
    unexpected = []
    for n in nums_in_rows:
        ok = any(abs(n - g) <= max(g * tolerance_pct / 100, 0.5) for g in gt)
        if not ok:
            unexpected.append(n)

    unexpected = sorted(set(unexpected))
    return {
        'drift': bool(unexpected),
        'unexpected_numbers': unexpected,
        'expected_numbers': gt,
        'note': (f'{len(unexpected)} drift number(s) in Section 8 三欄'
                 if unexpected else 'all verbatim'),
    }


def send_drift_discord(ticker, drift_check, webhook_url=None):
    """Send drift warning to Discord. Returns True on success.

    Reads DISCORD_WEBHOOK_URL from local/.env if webhook_url not given.
    Silently no-op (returns False) if webhook unconfigured.
    """
    if not drift_check or not drift_check.get('drift'):
        return False
    if not webhook_url:
        env_path = os.path.join(os.path.dirname(__file__), 'local', '.env')
        if os.path.exists(env_path):
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith('DISCORD_WEBHOOK_URL='):
                        webhook_url = line.strip().split('=', 1)[1].strip()
                        break
    if not webhook_url:
        return False

    expected = drift_check.get('expected_numbers', [])
    unexpected = drift_check.get('unexpected_numbers', [])
    content = (
        f"**[DRIFT_DETECTED] {ticker} AI 報告 Section 8 漂移**\n"
        f"Ground truth (action_plan): `{expected}`\n"
        f"Unexpected in 三欄: `{unexpected}`\n"
        f"Note: {drift_check.get('note', '')}\n"
        f"_提示：Phase 1 hard rule 在此案例失效，請人工 audit 並考慮 Phase 2 dataclass refactor 或調整 prompt_"
    )
    try:
        import requests
        resp = requests.post(webhook_url, json={'content': content}, timeout=10)
        return resp.status_code == 204
    except Exception as e:
        logger.error("Discord drift notification failed: %s", e)
        return False


from datetime import datetime
