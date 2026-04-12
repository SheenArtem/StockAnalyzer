"""
AI 研究報告模組 — Phase 1
收集個股所有數據，組裝 prompt，呼叫 Claude CLI 生成研究報告。
"""
import subprocess
import json
import logging
import os
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Prompt 模板路徑
_PROMPT_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'prompts', 'stock_analysis_system.md')


def _load_system_prompt():
    """載入 system prompt 模板"""
    try:
        with open(_PROMPT_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error("System prompt template not found: %s", _PROMPT_TEMPLATE_PATH)
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

    # 收集所有技術指標
    indicators = {
        # 均線
        'MA5': '.2f', 'MA10': '.2f', 'MA20': '.2f', 'MA60': '.2f', 'MA120': '.2f',
        # 布林
        'BB_upper': '.2f', 'BB_middle': '.2f', 'BB_lower': '.2f',
        # RSI / KD
        'RSI': '.1f', 'K': '.1f', 'D': '.1f',
        # MACD
        'MACD': '.4f', 'MACD_signal': '.4f', 'MACD_hist': '.4f',
        # ADX / DMI
        'ADX': '.1f', 'DI_pos': '.1f', 'DI_neg': '.1f',
        # Supertrend
        'Supertrend': '.2f', 'Supertrend_direction': '.0f',
        # RVOL / OBV
        'RVOL': '.2f', 'OBV': '.0f',
        # ATR
        'ATR': '.2f',
        # Squeeze
        'KC_upper': '.2f', 'KC_lower': '.2f',
        'Squeeze_Momentum': '.4f',
        # VWAP
        'VWAP': '.2f',
        # TD Sequential
        'TD_count': '.0f',
        # EFI
        'EFI': '.0f',
        # 型態
        'Pattern': None, 'Pattern_Type': None,
    }

    lines = []
    for col, fmt in indicators.items():
        if col in df_day.columns:
            val = last.get(col, None)
            if val is not None and not (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
                if fmt is None:
                    lines.append(f"{col}: {val}")
                else:
                    lines.append(f"{col}: {_safe_val(val, fmt)}")

    # 額外計算: 價格相對均線位置
    close = last.get('Close', 0)
    if close and close > 0:
        for ma_col in ['MA5', 'MA20', 'MA60', 'MA120']:
            if ma_col in df_day.columns:
                ma_val = last.get(ma_col, 0)
                if ma_val and ma_val > 0:
                    pct = (close - ma_val) / ma_val * 100
                    lines.append(f"Close vs {ma_col}: {pct:+.1f}%")

    # BB 位置
    bb_upper = last.get('BB_upper', 0)
    bb_lower = last.get('BB_lower', 0)
    if bb_upper and bb_lower and bb_upper > bb_lower:
        bb_pct = (close - bb_lower) / (bb_upper - bb_lower) * 100
        lines.append(f"BB %B: {bb_pct:.1f}%")
        lines.append(f"BB Width: {(bb_upper - bb_lower) / last.get('BB_middle', close) * 100:.2f}%")

    # Squeeze 狀態
    if 'KC_upper' in df_day.columns and 'BB_upper' in df_day.columns:
        kc_upper = last.get('KC_upper', 0)
        kc_lower = last.get('KC_lower', 0)
        if kc_upper > 0 and bb_upper > 0:
            squeeze_on = bb_upper < kc_upper and bb_lower > kc_lower
            lines.append(f"Squeeze: {'ON (壓縮中)' if squeeze_on else 'OFF'}")

    return "\n".join(lines) if lines else "N/A"


def _build_chip_data(chip_data, us_chip_data, is_us):
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
            lines.append(f"空單比例: {_safe_val(short.get('short_percent', 0), '.1f')}%")
            lines.append(f"Days to Cover: {_safe_val(short.get('days_to_cover', 0), '.1f')}")

        insider = us_chip_data.get('insider_trades', [])
        if insider:
            lines.append(f"\n近期內部人交易 ({len(insider)} 筆):")
            for t in insider[:5]:
                lines.append(f"  - {t}")

        recs = us_chip_data.get('recommendations', {})
        if recs:
            lines.append(f"\n分析師評級: {json.dumps(recs, ensure_ascii=False)}")

        top = us_chip_data.get('major_holders', {})
        if isinstance(top, pd.DataFrame) and not top.empty:
            lines.append(f"\n主要持股人:\n{top.to_string()}")
        elif isinstance(top, dict) and top:
            lines.append(f"\n主要持股人: {json.dumps(top, ensure_ascii=False)}")

    elif not is_us and chip_data:
        # 台股籌碼
        for key, label in [('institutional', '三大法人'), ('margin', '融資融券'),
                           ('day_trading', '當沖'), ('shareholding', '持股分布')]:
            df = chip_data.get(key)
            if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                lines.append(f"\n{label} (近 5 日):")
                tail = df.tail(5)
                lines.append(tail.to_string())

    return "\n".join(lines) if lines else "N/A"


def _build_fundamental_data(fund_data):
    """[FUNDAMENTAL_DATA] 基本面數據"""
    if not fund_data:
        return "N/A"

    lines = []
    for key in ['PE Ratio', 'Forward PE', 'PB Ratio', 'PEG Ratio',
                'EPS (TTM)', 'ROE', 'Profit Margin', 'Dividend Yield',
                'Market Cap', 'Revenue YoY', 'Monthly Revenue',
                'Cash Dividend', 'Stock Dividend', 'Payout Ratio']:
        val = fund_data.get(key, '')
        if val and str(val) not in ('', 'N/A', 'nan', 'None'):
            lines.append(f"{key}: {val}")

    return "\n".join(lines) if lines else "N/A"


def _build_market_context(report):
    """[MARKET_CONTEXT] 市場環境"""
    lines = []
    regime = report.get('regime', {})
    if regime:
        lines.append(f"Regime: {regime.get('regime', 'unknown')}")
        lines.append(f"Confidence: {_safe_val(regime.get('confidence', 0), '.2f')}")
        lines.append(f"Position Adj: {_safe_val(regime.get('position_adj', 1.0), '.1f')}")
        lines.append(f"HMM State: {regime.get('hmm_state', 'N/A')}")
        for d in regime.get('details', []):
            lines.append(f"- {d}")

    # Action plan
    ap = report.get('action_plan', {})
    if ap:
        lines.append(f"\nAction Plan:")
        for k, v in ap.items():
            lines.append(f"  {k}: {v}")

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


def assemble_prompt(ticker, report, chip_data, us_chip_data, fund_data, df_day):
    """
    組裝完整的 AI 分析 prompt。

    Args:
        ticker: 股票代號
        report: TechnicalAnalyzer.run_analysis() 的回傳結果
        chip_data: 台股籌碼數據 (dict of DataFrames)
        us_chip_data: 美股籌碼數據 (dict)
        fund_data: 基本面數據 (dict)
        df_day: 日線 DataFrame (含技術指標)

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
    data_sections.append(f"[CHIP_DATA]\n{_build_chip_data(chip_data, us_chip_data, is_us)}")
    data_sections.append(f"[FUNDAMENTAL_DATA]\n{_build_fundamental_data(fund_data)}")
    data_sections.append(f"[MARKET_CONTEXT]\n{_build_market_context(report)}")
    data_sections.append(f"[PATTERN_DATA]\n{_build_pattern_data(df_day)}")

    data_block = "\n\n".join(data_sections)

    full_prompt = f"""{system_prompt}

---

# 以下是 StockPulse 系統提供的 {ticker} 完整分析數據

{data_block}

---

請根據以上所有數據，產出完整的研究報告。"""

    return full_prompt


def generate_report(ticker, report, chip_data, us_chip_data, fund_data, df_day,
                    timeout=300):
    """
    呼叫 Claude CLI 生成 AI 研究報告。

    Returns:
        tuple: (success: bool, content: str)
    """
    prompt = assemble_prompt(ticker, report, chip_data, us_chip_data, fund_data, df_day)

    logger.info("AI Report prompt assembled for %s (%d chars)", ticker, len(prompt))

    try:
        result = subprocess.run(
            ["claude", "-p", "--output-format", "text"],
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
