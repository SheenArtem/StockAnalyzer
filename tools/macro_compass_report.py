"""
macro_compass_report.py -- 總經大盤風向 AI 報告產生器

流程：
  1. 收集所有 panel 資料（FRED / breadth / systemic chip / sentiment / valuation）
  2. 組裝統一 context（含當前值 + 30 天歷史 + 百分位 + 警戒線）
  3. 平行呼叫 Claude Opus + Gemini 3.1 Pro
  4. Claude Sonnet council 統整為單一 HTML 報告
  5. 報告必含「資料缺口建議」段，回頭指引下一輪要補哪些指標
  6. 存 data/macro_reports/YYYY-MM-DD_HHMMSS.html

LLM 規範 (CLAUDE.md):
  - Claude: --model opus --allowedTools "*" (timeout 600s)
  - Gemini: gemini-3.1-pro-preview (timeout 900s)
  - Council 統整: --model sonnet --allowedTools "WebSearch,WebFetch" (timeout 600s)

執行：
  python tools/macro_compass_report.py [--no-gemini] [--no-claude]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from macro_field_glossary import label, label_with_code  # noqa: E402

DATA = REPO / "data"
OUT_DIR = DATA / "macro_reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CLAUDE_CLI = shutil.which("claude") or "claude"
GEMINI_CLI = shutil.which("gemini") or "gemini"

CLAUDE_OPUS_TIMEOUT = 600
CLAUDE_SONNET_TIMEOUT = 600
GEMINI_TIMEOUT = 900


# ============================================================
#  資料收集
# ============================================================

def _safe_read_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        logger.warning("read failed %s: %s", path, e)
        return None


def _format_series_summary(df: pd.DataFrame, col: str, n_recent: int = 30) -> str:
    """格式化一個 column 的近 30 天 summary。"""
    if df is None or df.empty or col not in df.columns:
        return f"  {col}: N/A"
    s = df[col].dropna()
    if s.empty:
        return f"  {col}: N/A"
    last = s.iloc[-1]
    n = min(n_recent, len(s))
    recent = s.tail(n)
    p_now = (s <= last).mean() * 100  # 全期百分位
    return (f"  {label_with_code(col)}: 當前 {last:.4g} "
            f"(全期百分位 {p_now:.0f}%；"
            f"近{n}天 min={recent.min():.4g} max={recent.max():.4g} "
            f"avg={recent.mean():.4g})")


def collect_context() -> str:
    """組裝給 LLM 的完整 panel context。"""
    lines = [
        "=" * 70,
        f"市場日期：{datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 70,
        "格式說明：每列為「中文正式名稱 [程式變數名]: 當前值 (全期百分位；近N期區間)」。",
        "全期百分位 = 該指標在全部歷史中的位置 (0%=史上最低，100%=史上最高)。",
        "請以中文名稱判讀語意；[方括號內] 為程式變數名，可用於 trigger 條件引用。",
        "",
    ]

    # FRED macro
    fred = _safe_read_parquet(DATA / "macro" / "fred_panel.parquet")
    lines.append("### A. 國際 macro / 信用 / 流動性 (FRED, 2010-2026)")
    if fred is not None and not fred.empty:
        fred = fred.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={fred['date'].iloc[-1]}")
        for col in ['hy_oas', 'hy_oas_rank', 'ccc_oas', 'ccc_oas_rank',
                    'yield_curve_10y_2y', 'yield_curve_10y_3m',
                    'dxy_close', 'dxy_chg_4w', 'usdjpy_close', 'usdjpy_chg_4w', 'usdtwd_close',
                    'vix_close', 'chicago_nfci', 'chicago_anfci', 'st_louis_fsi',
                    'us_durable_yoy', 'us_unemployment_rate', 'us_initial_claims',
                    'us_consumer_sentiment', 'sp500_close', 'fed_bs_trillion', 'fed_bs_chg_4w',
                    'net_liquidity_bil', 'net_liquidity_chg_4w', 'rrp_balance', 'tga_balance', 'sofr',
                    'iorb', 'sofr_iorb_spread', 'bank_reserves', 'bank_reserves_chg_4w',
                    'us_real_yield_10y', 'us_real_yield_10y_chg_4w', 'us_breakeven_10y',
                    'ig_oas', 'ig_oas_rank']:
            lines.append(_format_series_summary(fred, col))
    else:
        lines.append("  (尚未建立 - 執行 tools/fetch_fred_macro.py)")
    lines.append("")

    # Valuation (Buffett US/TW) -- 專案 IC 最強單一特徵 (buffett_indicator_us 60d IC -0.371)
    val = _safe_read_parquet(DATA / "macro" / "valuation_panel.parquet")
    lines.append("### B. 估值 (Buffett US/TW + 台股 PE/PB/殖利率, 2011-2026)")
    if val is not None and not val.empty:
        val = val.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={val['date'].iloc[-1]}")
        for col in ['buffett_indicator_us', 'buffett_rank_us', 'buffett_indicator_tw',
                    'buffett_rank_tw', 'tw_market_pe', 'tw_market_pb', 'tw_market_yield',
                    'tw_earnings_yield']:
            lines.append(_format_series_summary(val, col))
    else:
        lines.append("  (尚未建立 - 執行 tools/build_valuation_panel.py)")
    lines.append("")

    # Breadth
    breadth = _safe_read_parquet(DATA / "breadth" / "tw_breadth.parquet")
    lines.append("### C. 台股市場廣度 (1548 檔聚合, 2006-2026)")
    if breadth is not None and not breadth.empty:
        breadth = breadth.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={breadth['date'].iloc[-1]}")
        for col in ['advances', 'declines', 'adl', 'mcclellan_oscillator',
                    'up_down_vol_ratio', 'breadth_thrust_10d', 'new_high_minus_low',
                    'new_highs_52w', 'new_lows_52w', 'pct_above_50dma', 'pct_above_200dma',
                    'avg_correlation_20d', 'return_dispersion_20d']:
            lines.append(_format_series_summary(breadth, col))
    else:
        lines.append("  (尚未建立 - 執行 tools/build_tw_breadth.py)")
    lines.append("")

    # Systemic chip
    sys_chip = _safe_read_parquet(DATA / "macro" / "systemic_chip.parquet")
    lines.append("### D. 機構撤退訊號 / Systemic Chip (含指數價位 twii_close + 外資台指期 OI, 2016-2026)")
    if sys_chip is not None and not sys_chip.empty:
        sys_chip = sys_chip.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={sys_chip['date'].iloc[-1]}")
        for col in ['twii_close', 'twii_dist_ma20', 'twii_dist_ma50', 'twii_dist_ma200',
                    'sbl_total', 'foreign_holding_avg', 'foreign_holding_chg_4w',
                    'sbl_change_4w_pct', 'margin_to_index_ratio', 'margin_ratio_z_252d',
                    'short_to_long_ratio', 'margin_to_mktcap_pct', 'margin_mktcap_z_252d',
                    'margin_maintenance_pct',
                    'pcr_oi', 'foreign_net_oi', 'foreign_fut_net_chg_4w',
                    'foreign_investor_net', 'foreign_cum_5d', 'foreign_cum_20d',
                    'trust_buy_streak', 'trust_net', 'trust_5d_zscore', 'option_top1_concentration']:
            lines.append(_format_series_summary(sys_chip, col))
        last = sys_chip.iloc[-1]
        # 指數均線「絕對點位」供報告引用具體 trigger price（乖離率上方 col 已給）
        if all(c in sys_chip.columns for c in ['twii_ma20', 'twii_ma50', 'twii_ma200']):
            lines.append(f"  指數均線點位 (trigger price 參考): MA20={last.get('twii_ma20'):.0f} "
                         f"/ MA50={last.get('twii_ma50'):.0f} / MA200={last.get('twii_ma200'):.0f} "
                         f"(現價 twii_close={last.get('twii_close'):.0f})")
        group_names = {'a': '外資撤退', 'b': '籌碼鬆動', 'c': '投信動能',
                       'd': '期權對沖', 'e': 'ETF流動'}
        flag_parts = []
        for g in ['a', 'b', 'c', 'd', 'e']:
            fl = last.get(f'group_{g}_flag')
            rs = last.get(f'group_{g}_reason', '') or ''
            flag_parts.append(f"{g.upper()}.{group_names[g]}={fl}"
                              + (f" ({rs})" if rs else " (driver 未填/stub)"))
        lines.append("  風險燈號 (low/mid/high): " + " | ".join(flag_parts))
    else:
        lines.append("  (尚未建立 - 執行 tools/build_systemic_chip_panel.py)")
    # 台指期基差 + 台指期(全)夜盤 (live TAIFEX；real-time 快訊號，不存歷史故無百分位)
    try:
        from taifex_data import TAIFEXData
        _tfx = TAIFEXData()
    except Exception as e:
        _tfx = None
        logger.warning("TAIFEXData init failed: %s", e)
    if _tfx is not None:
        try:
            _b = _tfx.get_futures_basis()
            if _b and _b.get('futures_price'):
                lines.append(
                    f"  台指期基差 (正逆價差, 即時): {_b.get('basis'):+.1f} 點 "
                    f"({_b.get('basis_pct'):+.3f}%) — 期 {_b.get('futures_price'):.0f} vs 現貨 "
                    f"{_b.get('spot_price'):.0f}；正價差=偏多/逆價差=避險情緒升")
        except Exception as e:
            logger.warning("futures basis fetch failed: %s", e)
        try:
            # 台指期(全)：夜盤 (盤後時段 15:00~次日05:00, 交易日=次一交易日)
            # 漲跌基準=前一日盤結算 → 直接是隔夜 gap，預示次一交易日開盤跳空
            _q = _tfx.get_full_session_quote()
            if _q and _q.get('night_close'):
                lines.append(
                    f"  台指期(全) 夜盤收盤 (即時): {_q.get('night_close'):.0f} "
                    f"({_q.get('night_chg') or 0:+.0f} 點 / {_q.get('night_chg_pct') or 0:+.2f}% "
                    f"vs 日盤結算 {_q.get('day_settle'):.0f} @{_q.get('day_date')})"
                    f" — 夜盤交易日 {_q.get('night_date')}；夜盤大幅偏離日盤結算="
                    f"隔夜訊號，預示次一交易日開盤跳空方向")
        except Exception as e:
            logger.warning("full session quote fetch failed: %s", e)
    lines.append("")

    # Banner risk
    try:
        from market_banner import _get_banner_data
        banner = _get_banner_data()
        risk = (banner or {}).get('risk_score', {}) or {}
        if risk.get('composite') is not None:
            lines.append("### E. Banner 綜合風險 (v3 calibration, 2002-2026)")
            lines.append(f"  {label('composite')}={risk.get('composite'):.1f} zone={risk.get('zone')}")
            for sig, info in (risk.get('breakdown') or {}).items():
                rk = info.get('rank')
                rk_s = f"{rk:.1f}" if isinstance(rk, (int, float)) else rk
                lines.append(f"  {label_with_code(sig)}: rank={rk_s} weight={info.get('weight')}")
            lines.append("")
    except Exception as e:
        logger.warning("banner data fetch failed: %s", e)

    # ETF flows / 風險偏好 (tlt_spy_ratio 是最佳短 lead, 60d IC +0.317 lag=3d)
    etf = _safe_read_parquet(DATA / "macro" / "etf_flows.parquet")
    lines.append("### F. ETF 流動 / 風險偏好 (HYG/LQD/TLT/SPY/MOVE/EEM, 2011-2026)")
    if etf is not None and not etf.empty:
        etf = etf.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={etf['date'].iloc[-1]}")
        for col in ['tlt_spy_ratio', 'tlt_spy_chg_4w', 'hyg_to_lqd_ratio', 'hyg_to_lqd_chg_4w',
                    'hyg_dollar_flow_z_252d', 'move_close', 'move_z_252d',
                    'eem_to_spy_ratio', 'eem_to_spy_chg_4w',
                    'copper_gold_ratio', 'copper_gold_chg_4w', 'cl_close', 'cl_chg_4w']:
            lines.append(_format_series_summary(etf, col))
    else:
        lines.append("  (尚未建立 - 執行 tools/fetch_etf_flows.py)")
    lines.append("")

    # Vol complex (VIX 期限結構 + skew) -- spot VIX 之外的高品質壓力擇時訊號
    volc = _safe_read_parquet(DATA / "sentiment" / "vol_complex_history.parquet")
    lines.append("### G. VIX 期限結構 + skew (vol complex, 2007-2026)")
    if volc is not None and not volc.empty:
        volc = volc.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={volc['date'].iloc[-1]}")
        for col in ['vix', 'vix3m', 'vix_vix3m_ratio', 'vvix', 'skew', 'ovx']:
            lines.append(_format_series_summary(volc, col))
    else:
        lines.append("  (尚未建立 - 執行 tools/archive_vol_complex.py)")
    lines.append("")

    # Sentiment 既有
    pcr = _safe_read_parquet(DATA / "sentiment" / "pcr_history.parquet")
    if pcr is not None and not pcr.empty and 'pcr_oi' in pcr.columns:
        lines.append("### H. 情緒/期權 (pcr_history)")
        for col in ['pcr_oi', 'pcr_volume']:
            if col in pcr.columns:
                lines.append(_format_series_summary(pcr, col))
        lines.append("")

    # 最後 7 天 trend table (表頭用精簡中文，避免表格過寬)
    trend_hdr = {
        'date': '日期', 'advances': '上漲家數', 'declines': '下跌家數',
        'mcclellan_oscillator': '麥克連', 'new_high_minus_low': '新高減新低',
        'hy_oas': 'HY利差', 'yield_curve_10y_2y': '殖利率10Y-2Y',
        'vix_close': 'VIX', 'dxy_close': '美元指數',
        'twii_close': '加權指數', 'foreign_net_oi': '外資期淨OI(口)',
    }
    lines.append("### I. 近 7 個交易日重點指標 trend")
    if breadth is not None:
        last7 = breadth.tail(7)[['date', 'advances', 'declines', 'mcclellan_oscillator',
                                  'new_high_minus_low']].rename(columns=trend_hdr).to_string(index=False)
        lines.append("廣度:")
        lines.append(last7)
        lines.append("")
    if fred is not None:
        last7 = fred.tail(7)[['date', 'hy_oas', 'yield_curve_10y_2y',
                              'vix_close', 'dxy_close']].rename(columns=trend_hdr).to_string(index=False)
        lines.append("Macro:")
        lines.append(last7)
    if sys_chip is not None and 'twii_close' in sys_chip.columns:
        cols = [c for c in ['date', 'twii_close', 'foreign_net_oi'] if c in sys_chip.columns]
        last7 = sys_chip.tail(7)[cols].rename(columns=trend_hdr).to_string(index=False)
        lines.append("")
        lines.append("指數價位 / 外資台指期淨 OI:")
        lines.append(last7)

    # Leadership / 跨市場領先 (SOX/Nasdaq 位階+相對強弱 + TSM ADR 溢價) -- 窄幅
    # 領漲 regime 下「領頭羊鬆動」的早期裂痕警報 (informational, 1 日 lead)
    lead = _safe_read_parquet(DATA / "macro" / "leadership_panel.parquet")
    lines.append("")
    lines.append("### J. 領頭羊 / 跨市場領先 (SOX/Nasdaq 絕對位階+相對強弱 + TSM ADR 溢價, informational)")
    if lead is not None and not lead.empty:
        lead = lead.sort_values('date').reset_index(drop=True)
        lines.append(f"資料日期 last={lead['date'].iloc[-1]}")
        for col in ['sox_to_twii_ratio', 'sox_rs_chg_4w', 'nasdaq_rs_chg_4w',
                    'tsm_adr_premium_pct',
                    'sox_close', 'sox_chg_1d',
                    'sox_dist_ma20', 'sox_dist_ma50', 'sox_dist_ma200',
                    'nasdaq_close', 'nasdaq_chg_1d',
                    'nasdaq_dist_ma20', 'nasdaq_dist_ma50', 'nasdaq_dist_ma200']:
            lines.append(_format_series_summary(lead, col))
        _ll = lead.iloc[-1]
        # 美指數均線「絕對點位」(乖離率上方 col 已給；點位供引用 trigger level)
        if all(c in lead.columns for c in ('sox_ma50', 'sox_ma200', 'nasdaq_ma50', 'nasdaq_ma200')):
            lines.append(f"  美指數均線點位: SOX MA50={_ll.get('sox_ma50'):.0f} "
                         f"/ MA200={_ll.get('sox_ma200'):.0f} (現價 {_ll.get('sox_close'):.0f})；"
                         f"Nasdaq MA50={_ll.get('nasdaq_ma50'):.0f} "
                         f"/ MA200={_ll.get('nasdaq_ma200'):.0f} (現價 {_ll.get('nasdaq_close'):.0f})")
    else:
        lines.append("  (尚未建立 - 執行 tools/build_leadership_panel.py)")

    # 台灣總經/基本面領先指標 (國發會 LEI, 月頻) -- 補「面板零台灣基本面」缺口；
    # 月頻=改善 1-3 月基本面背景，非 1-4 週戰術預警，請勿當近端領先訊號
    lei = _safe_read_parquet(DATA / "macro" / "tw_lei_panel.parquet")
    lines.append("")
    lines.append("### K. 台灣總經/基本面領先指標 (國發會 LEI, 月頻, 慢速領先 1-3 月)")
    if lei is not None and not lei.empty:
        if lei.index.name == 'date':
            lei = lei.reset_index()
        if 'date' in lei.columns:
            lei = lei.sort_values('date').reset_index(drop=True)
            lines.append(f"資料月份 last={pd.to_datetime(lei['date'].iloc[-1]).date()}")
        for col in ['lei_composite', 'leading_export_order_idx', 'leading_m1b',
                    'leading_semi_equip_import']:
            lines.append(_format_series_summary(lei, col))
        lines.append("  註：月頻慢速領先（改善 1-3 月基本面背景，非 1-4 週戰術預警）；"
                     "外銷訂單為「動向指數」(>50=擴張) 而非訂單金額 YoY")
    else:
        lines.append("  (尚未建立 - 執行 tools/fetch_tw_lei_panel.py)")

    return "\n".join(lines)


# ============================================================
#  Prompt 組裝
# ============================================================

def build_prompt(context: str, fmt: str = "html") -> str:
    """組裝報告 prompt。

    fmt='html'   ：本地 LLM pipeline 用（要求輸出 HTML body 內嵌 iframe）。
    fmt='md'     ：使用者複製到 claude.ai 用（要求輸出 Markdown，網頁端較好讀）。
    fmt='webpage'：使用者複製到 claude.ai 用（要求輸出單檔自包含 HTML 網頁，
                   可當 Artifact 預覽 + 下載，再貼回報告庫）。
    各版段落內容相同，只有標題語法 + 輸出格式指示不同。
    """
    if fmt == "webpage":
        directive = (
            "請產出一份「總經大盤風向研究報告」，並輸出**一個自包含的完整 HTML 網頁**"
            "（完整 `<!DOCTYPE html>` ... `</html>`，所有樣式用 inline `<style>`，"
            "深色主題、卡片式排版、可直接用瀏覽器開啟、也能在 claude.ai Artifact 預覽）。"
            "內容必須包含以下五段：")
        def H(n, t):
            return f"<h2>{n}. {t}</h2>"
        out_fmt_rule = ("- 用台灣繁體中文\n"
                        "- **只輸出 HTML 本身**，HTML 前後不要加任何說明文字、不要包 markdown code fence\n"
                        "- 深色主題（背景 #0a0f1e、文字 #e2e8f0），標題/卡片清楚分區，行動裝置也可讀")
    elif fmt == "md":
        directive = ("請產出一份「總經大盤風向研究報告」，用 **Markdown** 表達"
                     "（## 主標 / ### 次標 / 段落 / `-` 條列），內容必須包含以下五段：")
        def H(n, t):
            return f"## {n}. {t}"
        out_fmt_rule = "- 用台灣繁體中文，Markdown 格式輸出"
    else:
        directive = ("請產出一份「總經大盤風向研究報告」，內容必須包含以下五段"
                     "（用 HTML <h2>/<h3>/<p>/<ul> 表達，最後輸出整段乾淨的 HTML body 即可，"
                     "不要 <html>/<head>/<body> wrapper）：")
        def H(n, t):
            return f"<h2>{n}. {t}</h2>"
        out_fmt_rule = "- 用台灣繁體中文"

    return f"""你是一位資深總體經濟與量化研究員，以下是台股 + 美股大盤的當前完整資料面板。

【資料面板】
{context}

【本系統已維護資料清單 — 第 5 段請勿把以下「已有」項目當成缺口重複推薦】
上方面板 A-J 已涵蓋：
- 美國 macro/信用/流動性：HY OAS、CCC 級 OAS、IG 投資級公司債 OAS(BAMLC0A0CM)、殖利率曲線(10Y-2Y/10Y-3M)、DXY、USDJPY、USDTWD、VIX、芝加哥 NFCI/ANFCI、聖路易 FSI、失業率/初請/消費信心/耐久財、Fed 資產負債表、RRP/TGA/SOFR + 淨流動性(net_liquidity)、銀行存準餘額(WRESBAL)、IORB + SOFR-IORB 利差、10年實質殖利率(DFII10)、10年通膨預期 breakeven(T10YIE)
- 估值：美股 Buffett 指標+rank、台股估值(指數 proxy)+rank、台股大盤 PE/PB/殖利率、盈餘殖利率(1/PE)
- 台股廣度：漲跌家數、ADL、McClellan、上漲量/下跌量比、廣度衝力、52週新高低、站上 50/200 日均線比例、個股平均相關性/報酬離散度(20日)
- 機構/籌碼：借券餘額、融資/指數比 z-score、券資比、PCR-OI/Volume、外資台指期淨 OI、外資持股、外資現貨日買賣超(當日+5/20日累積)、投信買賣超、選擇權集中度
- 風險偏好/波動：HYG/LQD、長債/股票比(TLT/SPY)、MOVE、EEM/SPY、VIX 期限結構(VIX3M)、SKEW、VVIX、OVX、銅金比(HG/GC,成長代理)、原油價格(CL=WTI)
- 加權指數位階：twii_close + 20/50/200 日均線及乖離率（含近 7 日 trend）、台指期基差(即時正逆價差)、台指期(全)夜盤收盤+隔夜 gap(即時)
- 領頭羊/跨市場：費城半導體 SOX + 那斯達克 各自的收盤/1日漲跌/20/50/200日均線乖離率、SOX 與 Nasdaq 對台股相對強弱(4週變化)、TSM ADR 對 2330 隔夜溢價
- 台灣總經/基本面領先指標(月頻)：國發會 LEI 綜合指數、外銷訂單動向指數、M1B、半導體設備進口值（註：外銷訂單為「動向指數」非訂單金額 YoY）
→ 以上皆「已有」。第 5 段只列「上方面板沒有、且不在下方『已評估、決定不補』清單」的真缺口（仍缺候選：JGB 10年殖利率「日頻」+JPY 套利壓力〔FRED 僅月頻落後，日頻無乾淨免費源〕、中國信用脈衝(PBoC TSF YoY,月頻慢速)、0DTE 短天期選擇權占比、台積電月營收〔MOPS〕、外銷訂單「金額」YoY + S&P Global 台灣 PMI〔LEI 只有動向指數〕、美 ISM 新訂單〔NAPMNOI 已從 FRED 下架，需 ISM 官方〕）。

【已評估、決定不補 — 第 5 段請勿重複推薦（已查證：無免費源 / 付費 / IC 否決）】
- 盈餘上修/下修廣度：TEJ/IBES 付費
- CDX HY/IG 信用違約交換：付費(Markit/Bloomberg)；HY OAS + CCC OAS 已為免費 proxy
- 台股 ERP：TW 10年公債無免費 daily 源(FRED/yfinance/TPEx/FinMind 皆無或付費)，已改呈現盈餘殖利率(1/PE) 供自行對照公債，勿再要求補 ERP
- SKEW 當崩盤擇時 gate：台股 IC 驗證為反向(高 SKEW→反而跌少)，僅 informational，勿建議當預警門檻
- 北向資金 / 韓國半導體相對強弱：已評估，前者資料源跳過、後者效益不足
- VIX 前月期貨 roll (VX1/VX2)：VIX/VIX3M 期限結構已覆蓋且經 IC 驗證(vix_vix3m_ratio 是唯一 marginal-pass 的 vol 訊號)；VX1/VX2 與之高度相關、增量低，且 vol_complex 多數 vol 訊號台股 IC FAIL
- CNH/CNY 即期匯率：已做 IC 驗證 vs TWII = marginal/fail（線性 |IC|=0.04<0.10、sign 跨期間反轉、去除 2018+2022 事件後 IC 崩 62%）；僅極端尾端(RMB 急貶)對台股崩跌有凸性但 fragile；且真 offshore CNH 無免費 daily 源(yfinance CNH=X 壞、FRED DEXCHUS 阻塞，只剩 onshore CNY proxy)。不採用

【指標領先性分類（本系統已做 IC 驗證，請據此區分「預警」與「確認」）】
- 真領先（1-4 週 lead，可作預警）：美股 Buffett 估值(~10d)、長債/股票比 TLT/SPY(~3d)、美耐久財 YoY(~1d)、聖路易 FSI(~12d)、融資比 z-score(~13d)、外資持股 4 週變化(~16d)
- 同步（lag≈0，只能「確認」當下狀態，不可當「即將發生」的領先訊號）：VIX、殖利率曲線、漲跌家數、McClellan、ADL、廣度衝力、PCR
- 慢速領先（30-60d）：HY OAS、台股 Buffett、Fed 資產負債表
- 未經 IC 驗證（informational，僅供研判背景，勿當成已驗證的領先訊號）：SOX/Nasdaq 位階與對台股相對強弱、TSM ADR 溢價、台指期(全)夜盤隔夜 gap、外資現貨流量、SKEW/VVIX/OVX、機構撤退 A-E 燈號（註：SKEW 在台股經 IC 驗證為反向，高 SKEW 不等於跌深）
→ 情境推演的觸發條件請優先押在「真領先」指標；同步指標只用來描述當下，不要寫成預測未來的領先警訊。

【任務】
{directive}

{H(1, "當前風險定調")}
- 5 階燈號：危機/嚴重/警戒/留意/安全 -- 給出明確選一
- 一句話定調 (50 字內)
- 主要驅動訊號 top 3 (依重要性排序，每條附「為何重要」)

{H(2, "1-4 週情境推演")}
- Scenario A (基本情境，機率 % 估計)：什麼會發生 + 觸發條件
- Scenario B (悲觀情境)：同上
- Scenario C (樂觀情境)：同上

{H(3, "訊號交叉驗證")}
- 哪些訊號互相印證？(例：HY OAS 高 + 廣度轉弱 + SBL 增 = 多重共振)
- 哪些訊號彼此衝突？怎麼解讀？
- 每個訊號的 false positive 風險 (歷史上幾次假警報？)

{H(4, "操作建議 (informational only, SOP-14)")}
- 部位水位建議區間（如 5-7 成）
- 避險工具建議（PUT / 反向 ETF / 提高現金）
- 進場/觀察條件：面板已含加權指數價位 twii_close（含近 7 日 trend），請盡量給「具體指數點位區間」（例如跌破/站上某點位）；確實無資料可定價處（如均線）再以 indicator level 表達
- 強調這是 informational tier，非自動 portfolio rebalance gate

{H(5, "資料缺口與下一步建議")}
這段最重要，請仔細思考（務必對照上方「本系統已維護資料清單」，不要把清單內已有的當成缺口）：
- 對照已維護清單，真正「上方面板沒有」的資料缺口是什麼？(列 5-10 個)
- 這些缺口中哪些能讓 1-4 週 lead 更可靠？(列出具體 FRED ID / 資料源 / 取得方式)
- 按 IC 預期 + 取得難度排序，優先補哪些
- 目前哪些 flag 是 stub（風險燈號 group A/C/D/E 的 driver 字串空白），怎麼用規則式邏輯填補
- 是否有跨市場資料（中國/日本/歐洲）能加強？

【輸出規範】
{out_fmt_rule}
- 數字精準到小數點 2 位
- 文字嚴謹但不過度套話，避免空話
- 必須引用 panel 的具體數字（不能說「市場可能波動」這種空話）
- 第 5 段是真正的價值，不要敷衍
"""


def export_prompt(fmt: str = "md") -> Path:
    """只組 prompt 不呼叫任何 LLM，寫檔供使用者複製到 claude.ai 自行產生報告。
    （2026-05-30：避免本地 Opus CLI 消耗 Agent SDK Credit。）"""
    context = collect_context()
    prompt = build_prompt(context, fmt="md")  # claude.ai 端用 markdown 輸出較好讀
    if fmt == "json":
        out_path = OUT_DIR / "latest_prompt.json"
        out_path.write_text(json.dumps(
            {"generated_at": datetime.now().isoformat(timespec="seconds"),
             "panel_context": context,
             "report_prompt": prompt},
            ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        out_path = OUT_DIR / "latest_prompt.md"
        out_path.write_text(prompt, encoding="utf-8")
    logger.info("Prompt exported (%s, %d chars) -> %s", fmt, len(prompt), out_path)
    return out_path


# ============================================================
#  CLI 呼叫
# ============================================================

def call_claude_opus(prompt: str) -> tuple[bool, str]:
    """呼叫 Claude Opus CLI。"""
    logger.info("Calling Claude Opus (timeout=%ds)...", CLAUDE_OPUS_TIMEOUT)
    try:
        result = subprocess.run(
            [CLAUDE_CLI, "-p",
             "--model", "opus",
             "--effort", "xhigh",  # 2026-05-21: 必須 CLI 帶 (settings.json effortLevel 不影響 -p)
             "--allowedTools", "*",
             "--output-format", "text"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=CLAUDE_OPUS_TIMEOUT,
            encoding='utf-8',
        )
        if result.returncode != 0:
            return False, f"Claude exit {result.returncode}: {result.stderr.strip()}"
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, f"Claude timeout {CLAUDE_OPUS_TIMEOUT}s"
    except FileNotFoundError:
        return False, "Claude CLI not found"


def call_gemini(prompt: str) -> tuple[bool, str]:
    """呼叫 Gemini CLI（gemini-3.1-pro-preview）。"""
    logger.info("Calling Gemini 3.1 Pro (timeout=%ds)...", GEMINI_TIMEOUT)
    try:
        result = subprocess.run(
            [GEMINI_CLI, "-p", prompt,
             "-m", "gemini-3.1-pro-preview", "-y"],
            capture_output=True,
            text=True,
            timeout=GEMINI_TIMEOUT,
            encoding='utf-8',
        )
        if result.returncode != 0:
            return False, f"Gemini exit {result.returncode}: {result.stderr.strip()[:500]}"
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, f"Gemini timeout {GEMINI_TIMEOUT}s"
    except FileNotFoundError:
        return False, "Gemini CLI not found"


def call_claude_sonnet_council(claude_out: str, gemini_out: str, context: str) -> tuple[bool, str]:
    """Council 統整：給 Sonnet 看兩家結果 → 統整出最終 HTML 報告。"""
    council_prompt = f"""你是研究 council 的主席。下面兩位研究員針對同一份 panel 各自產出了報告，請你統整出最終版。

【原始 panel 摘要】
{context[:3000]}

【研究員 A: Claude Opus】
{claude_out}

【研究員 B: Gemini 3.1 Pro】
{gemini_out}

【你的任務】
1. 整合兩家結論，明確指出「兩家共識點」與「分歧點 + 你的判讀」
2. 以 5 段結構輸出最終 HTML：1. 風險定調 / 2. 情境推演 / 3. 訊號交叉驗證 / 4. 操作建議 / 5. **資料缺口與下一步**
3. 最終 HTML body 只用 <h2>/<h3>/<p>/<ul>/<table>/<strong>/<em>，不要 <html>/<head>/<body> wrapper
4. 在最開頭加一個 <div class="meta"> 寫「兩家共識度 X/10」
5. 第 5 段必須具體列出 5-10 個建議補充的指標與資料源

開始輸出 HTML body：
"""
    logger.info("Calling Claude Sonnet council (timeout=%ds)...", CLAUDE_SONNET_TIMEOUT)
    try:
        result = subprocess.run(
            [CLAUDE_CLI, "-p",
             "--model", "sonnet",
             "--effort", "xhigh",  # 2026-05-21: 必須 CLI 帶
             "--allowedTools", "WebSearch,WebFetch",
             "--output-format", "text"],
            input=council_prompt,
            capture_output=True,
            text=True,
            timeout=CLAUDE_SONNET_TIMEOUT,
            encoding='utf-8',
        )
        if result.returncode != 0:
            return False, f"Sonnet exit {result.returncode}: {result.stderr.strip()}"
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, f"Sonnet timeout {CLAUDE_SONNET_TIMEOUT}s"


# ============================================================
#  HTML 包裝
# ============================================================

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<title>總經大盤風向 — {date}</title>
<style>
  body {{ font-family: 'Segoe UI', 'Microsoft JhengHei', sans-serif; max-width: 980px;
         margin: 30px auto; padding: 20px; line-height: 1.7; color: #222;
         background: #fafbfc; }}
  h1 {{ border-bottom: 3px solid #2c3e50; padding-bottom: 12px; color: #2c3e50; }}
  h2 {{ color: #2980b9; border-left: 5px solid #2980b9; padding-left: 12px;
        margin-top: 32px; }}
  h3 {{ color: #34495e; margin-top: 20px; }}
  .meta {{ background: #ecf0f1; padding: 12px 16px; border-radius: 6px;
           font-size: 0.95em; margin-bottom: 24px; }}
  .header-meta {{ font-size: 0.85em; color: #7f8c8d; margin-bottom: 24px; }}
  table {{ border-collapse: collapse; margin: 12px 0; }}
  table th, table td {{ border: 1px solid #bdc3c7; padding: 6px 10px; }}
  ul {{ padding-left: 22px; }}
  strong {{ color: #c0392b; }}
  em {{ color: #16a085; font-style: normal; font-weight: 600; }}
  .agent-block {{ border: 1px dashed #95a5a6; padding: 16px; margin-top: 30px;
                  border-radius: 6px; background: #fff; }}
  .footer {{ margin-top: 50px; padding-top: 16px; border-top: 1px solid #ddd;
             color: #7f8c8d; font-size: 0.85em; }}
</style>
</head>
<body>
<h1>🧭 總經大盤風向 AI 研究報告</h1>
<div class="header-meta">產出時間：{datetime} | 報告 ID：{rid} | informational tier (SOP-14)</div>

{council_html}

<details>
  <summary><strong>原始 LLM 回答 (兩家研究員獨立產出)</strong></summary>
  <div class="agent-block">
    <h3>📘 Claude Opus</h3>
    <div>{claude_html}</div>
  </div>
  <div class="agent-block">
    <h3>📗 Gemini 3.1 Pro</h3>
    <div>{gemini_html}</div>
  </div>
</details>

<div class="footer">
  資料來源：FRED / TWSE / FinMind / TDCC / TAIFEX / 主計處<br>
  注意：此報告為 informational tier (SOP-14)，僅供研究參考，<strong>不是自動 portfolio rebalance gate</strong>，<br>
  也不構成投資建議。任何下單決策請佐以個股分析、風險容忍度與獨立判斷。
</div>
</body>
</html>"""


def _to_html_safe(text: str) -> str:
    """簡易 markdown -> HTML 轉換 (基本 fallback)；如果 LLM 已輸 HTML 就保留。"""
    if "<h2" in text or "<p>" in text:
        return text
    # markdown 簡轉
    import html
    text = html.escape(text)
    lines = text.split("\n")
    out = []
    in_list = False
    for ln in lines:
        ln_strip = ln.strip()
        if ln_strip.startswith("## "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h2>{ln_strip[3:]}</h2>")
        elif ln_strip.startswith("### "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h3>{ln_strip[4:]}</h3>")
        elif ln_strip.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{ln_strip[2:]}</li>")
        elif ln_strip == "":
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("")
        else:
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<p>{ln_strip}</p>" if ln_strip else "")
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


# ============================================================
#  主流程
# ============================================================

def run(use_claude: bool = True, use_gemini: bool = True, council: bool = True) -> Path:
    context = collect_context()
    prompt = build_prompt(context)
    logger.info("Prompt assembled: %d chars", len(prompt))

    claude_ok, claude_out = (False, "(disabled)")
    gemini_ok, gemini_out = (False, "(disabled)")

    # 平行呼叫
    threads = []
    results = {}

    if use_claude:
        def _run_claude():
            results['claude'] = call_claude_opus(prompt)
        t = threading.Thread(target=_run_claude)
        t.start()
        threads.append(t)

    if use_gemini:
        def _run_gemini():
            results['gemini'] = call_gemini(prompt)
        t = threading.Thread(target=_run_gemini)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if 'claude' in results:
        claude_ok, claude_out = results['claude']
        logger.info("Claude result: ok=%s len=%d", claude_ok, len(claude_out))
    if 'gemini' in results:
        gemini_ok, gemini_out = results['gemini']
        logger.info("Gemini result: ok=%s len=%d", gemini_ok, len(gemini_out))

    # Council
    if council and claude_ok and gemini_ok:
        council_ok, council_out = call_claude_sonnet_council(claude_out, gemini_out, context)
    else:
        # Fallback：哪家成功就用哪家當 council
        council_ok = claude_ok or gemini_ok
        council_out = claude_out if claude_ok else gemini_out
        logger.warning("Skipping council (claude_ok=%s gemini_ok=%s)", claude_ok, gemini_ok)

    # HTML 組裝
    rid = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    html_body = HTML_TEMPLATE.format(
        date=datetime.now().strftime('%Y-%m-%d'),
        datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        rid=rid,
        council_html=_to_html_safe(council_out) if council_ok else
            f'<div style="color:red"><b>Council 失敗：</b>{council_out}</div>',
        claude_html=_to_html_safe(claude_out) if claude_ok else
            f'<div style="color:gray">Claude 不可用：{claude_out}</div>',
        gemini_html=_to_html_safe(gemini_out) if gemini_ok else
            f'<div style="color:gray">Gemini 不可用：{gemini_out}</div>',
    )

    out_path = OUT_DIR / f"{rid}.html"
    out_path.write_text(html_body, encoding='utf-8')
    logger.info("Saved -> %s", out_path)

    # 同時寫一份 latest.html
    latest = OUT_DIR / "latest.html"
    latest.write_text(html_body, encoding='utf-8')

    # metadata
    meta = {
        'rid': rid,
        'datetime': datetime.now().isoformat(),
        'claude_ok': claude_ok,
        'gemini_ok': gemini_ok,
        'council_ok': council_ok,
        'context_chars': len(context),
        'prompt_chars': len(prompt),
        'claude_chars': len(claude_out) if claude_ok else 0,
        'gemini_chars': len(gemini_out) if gemini_ok else 0,
    }
    (OUT_DIR / f"{rid}.meta.json").write_text(json.dumps(meta, indent=2), encoding='utf-8')

    return out_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-claude', action='store_true')
    parser.add_argument('--no-gemini', action='store_true')
    parser.add_argument('--no-council', action='store_true')
    parser.add_argument('--export-prompt', action='store_true',
                        help='只匯出 prompt 供複製到 claude.ai，不呼叫本地 LLM')
    parser.add_argument('--format', choices=['md', 'json'], default='md',
                        help='--export-prompt 格式 (預設 md)')
    args = parser.parse_args()

    if args.export_prompt:
        out = export_prompt(fmt=args.format)
        sys.stdout.write(f"\n[OK] Prompt exported: {out}\n")
        sys.stdout.flush()
        return

    out = run(
        use_claude=not args.no_claude,
        use_gemini=not args.no_gemini,
        council=not args.no_council,
    )
    sys.stdout.write(f"\n[OK] Report saved: {out}\n")
    sys.stdout.flush()


if __name__ == '__main__':
    main()
