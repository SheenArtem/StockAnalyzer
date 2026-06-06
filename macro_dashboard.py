"""
macro_dashboard.py -- 總經大盤風向 (Macro Compass)

新功能 tab：把原 market_banner 大盤儀表板搬過來，外加 macro / breadth /
systemic chip / 估值 等資料源整合，給使用者一個「容易讀」的全景視圖。

7 大區塊（從上到下：總結 → 領先 → 同步 → 滯後）:
  0. 🚦 總風向卡片 (Top Card)：5 階燈號 + 一句話定調 + 主導訊號 top 3
  1. 機構撤退訊號 (Systemic Chip, 1-4w lead)：A/B/C/D/E 5 組
  2. 市場廣度 (Market Breadth, 同步~短 lead)：ADL/McClellan/新高低/A/D 量能
  2.5 領頭羊/跨市場 (Leadership & Overnight)：SOX/Nasdaq 位階+相對強弱/TSM ADR/台指期(全)夜盤
  3. 情緒與波動 (Sentiment & Volatility, 0-1w lead)：原 banner 內容
  4. 流動性與資金 (Liquidity & Flow)：M1B 比/DXY/Fed BS/USDTWD
  5. 信用與景氣 (Credit & Business Cycle, 1-3mo lead)：HY OAS/Yield Curve/ISM/TW LEI
  6. 估值 (Valuation, slow)：PE/PB/Buffett/CAPE

設計原則：
  - 不阻塞個股分析等其他功能：dashboard 只在自己 tab 內渲染
  - 重用 market_banner 既有 fetcher（綜合風險/HMM/跌深 D/FGI/PCR/M1B 等）
  - 新增資料源以 parquet 落地後再讀，避免 streamlit rerun 卡住
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from macro_field_glossary import FIELD_GLOSSARY

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
DATA = REPO / "data"
MACRO_DIR = DATA / "macro"
BREADTH_DIR = DATA / "breadth"
SENT_DIR = DATA / "sentiment"
REPORTS_DIR = DATA / "macro_reports"

# ============================================================
#  資料載入 helpers
# ============================================================

def _safe_read_parquet(path: Path) -> pd.DataFrame | None:
    """讀 parquet，檔不在或失敗回 None。"""
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        logger.warning("read_parquet failed %s: %s", path, e)
        return None


def _last_row(df: pd.DataFrame | None) -> dict:
    """取最後一筆轉 dict，空回 {}。"""
    if df is None or df.empty:
        return {}
    return df.iloc[-1].to_dict()


def _fmt_date(d) -> str:
    if pd.isna(d) or d is None:
        return "N/A"
    if isinstance(d, str):
        return d[:10]
    try:
        return pd.Timestamp(d).strftime("%Y-%m-%d")
    except Exception:
        return str(d)[:10]


def _color_rank(rank: float | None, hi_is_bad: bool = True) -> str:
    """rank 0-100 → 顏色（hi_is_bad=True 高分=紅；False 高分=綠）。"""
    if rank is None or pd.isna(rank):
        return "#888888"
    if hi_is_bad:
        if rank >= 85:
            return "#FF4444"
        if rank >= 65:
            return "#FF8800"
        if rank >= 35:
            return "#888888"
        return "#00AA00"
    else:
        if rank <= 15:
            return "#FF4444"
        if rank <= 35:
            return "#FF8800"
        if rank <= 65:
            return "#888888"
        return "#00AA00"


# ============================================================
#  Section 0：總風向卡片
# ============================================================

def _compute_compass_verdict(banner_data: dict, sys_chip: dict, breadth: dict,
                             macro: dict, valuation: dict) -> dict:
    """
    彙總 5 大區塊的 risk score → 5 階燈號。

    規則 (簡化版，未來可改 IC weighted)：
      red    (危機): banner risk orange + chip A/B 紅 + macro yield curve inv
      orange (嚴重): banner risk orange OR chip 多紅 OR breadth 嚴重轉弱
      yellow (警戒): banner risk yellow OR chip 中度 OR macro 警示
      blue   (留意): 1 個次要訊號偏負
      green  (安全): 多數綠

    Returns dict: level / color / emoji / verdict / top_signals (list of 3)
    """
    risk = (banner_data or {}).get('risk_score', {}) or {}
    composite = risk.get('composite')
    zone = risk.get('zone', 'unknown')

    # 收集所有訊號狀態
    signals = []

    # Banner 綜合風險
    if composite is not None:
        if zone == 'orange':
            signals.append(('high', f'綜合風險 {composite:.0f} (橘燈)'))
        elif zone == 'yellow':
            signals.append(('mid', f'綜合風險 {composite:.0f} (黃燈)'))
        else:
            signals.append(('low', f'綜合風險 {composite:.0f} (綠燈)'))

    # 跌深延伸 D
    regime_ext = (banner_data or {}).get('regime_ext', {}) or {}
    ext_level = regime_ext.get('level')
    if ext_level == 'red':
        signals.append(('high', '跌深延伸 D：極端'))
    elif ext_level == 'orange':
        signals.append(('high', '跌深延伸 D：偏高'))
    elif ext_level == 'yellow':
        signals.append(('mid', '跌深延伸 D：注意'))

    # Systemic chip A/B (外資撤退/籌碼鬆動)
    chip_a = (sys_chip or {}).get('group_a', {})
    chip_b = (sys_chip or {}).get('group_b', {})
    if chip_a.get('flag') == 'high':
        signals.append(('high', f"外資撤退：{chip_a.get('reason', '訊號異常')}"))
    if chip_b.get('flag') == 'high':
        signals.append(('high', f"籌碼鬆動：{chip_b.get('reason', '訊號異常')}"))

    # Macro 信用 / 殖利率曲線
    cred = (macro or {}).get('credit', {})
    if cred.get('hy_oas_rank', 0) >= 85:
        signals.append(('high', f"HY OAS 高位 (rank {cred.get('hy_oas_rank', 0):.0f})"))
    yc = (macro or {}).get('yield_curve', {})
    if yc.get('inverted'):
        signals.append(('mid', '殖利率曲線倒掛 (1-2yr lead)'))

    # Breadth
    br = breadth or {}
    if br.get('mcclellan', 0) is not None and br.get('mcclellan', 0) < -100:
        signals.append(('mid', f"McClellan {br.get('mcclellan'):.0f} 嚴重轉弱"))

    # 估值
    val = valuation or {}
    if val.get('buffett_rank', 0) >= 90:
        signals.append(('mid', f"巴菲特指標 P{val.get('buffett_rank'):.0f} 極高"))

    # 5 階燈號決策
    n_high = sum(1 for s, _ in signals if s == 'high')
    n_mid = sum(1 for s, _ in signals if s == 'mid')

    if n_high >= 3:
        level, color, emoji, verdict = 'red', '#CC0000', '🔴', '危機 — 多重高風險訊號齊發，建議大幅降部位'
    elif n_high >= 2:
        level, color, emoji, verdict = 'orange', '#FF6600', '🟠', '嚴重 — 多訊號警示，主動降部位 + 提高避險'
    elif n_high >= 1 or n_mid >= 3:
        level, color, emoji, verdict = 'yellow', '#FFAA00', '🟡', '警戒 — 風險升溫中，停止新進場'
    elif n_mid >= 1:
        level, color, emoji, verdict = 'blue', '#3388FF', '🔵', '留意 — 個別次要訊號偏負，維持中性部位'
    else:
        level, color, emoji, verdict = 'green', '#00AA44', '🟢', '安全 — 多數指標正常，可維持部位'

    # top 3 訊號（high 優先）
    top = sorted(signals, key=lambda x: 0 if x[0] == 'high' else 1 if x[0] == 'mid' else 2)[:3]
    top_signals = [t[1] for t in top]

    return {
        'level': level, 'color': color, 'emoji': emoji, 'verdict': verdict,
        'top_signals': top_signals,
        'n_high': n_high, 'n_mid': n_mid,
    }


def _render_compass_card(compass: dict):
    """頂部總風向卡片：5 階燈號 + 一句話 + top 3 訊號 bullet。"""
    color = compass.get('color', '#888888')
    emoji = compass.get('emoji', '⚪')
    level = compass.get('level', 'unknown')
    verdict = compass.get('verdict', '資料不足')
    top = compass.get('top_signals', [])

    label_zh = {
        'red': '危機', 'orange': '嚴重', 'yellow': '警戒',
        'blue': '留意', 'green': '安全', 'unknown': '資料不足',
    }.get(level, '未知')

    bullets_html = ''.join(
        f'<li style="font-size:0.9rem;margin-bottom:2px">{s}</li>' for s in top
    ) if top else '<li style="opacity:0.6">無顯著訊號</li>'

    # 顏色策略：標題用 zone color (紅/黃/綠語意必要)；body 文字不寫死顏色，
    # 讓 Streamlit theme 自動適配 (light/dark mode)；muted 用 opacity。
    st.markdown(
        f'''
        <div style="border:2px solid {color};border-radius:12px;padding:16px;
                    background:linear-gradient(135deg, {color}11, {color}22);
                    margin-bottom:16px">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <div style="font-size:1.6rem;font-weight:bold;color:{color}">
              {emoji} 總風向：{label_zh}
            </div>
            <div style="font-size:0.85rem;opacity:0.65">
              SOP-14 informational tier；非 portfolio rebalance gate
            </div>
          </div>
          <div style="font-size:1.05rem;margin-top:6px">
            {verdict}
          </div>
          <div style="margin-top:8px">
            <div style="font-size:0.85rem;opacity:0.65;margin-bottom:4px">主導訊號</div>
            <ul style="margin:0;padding-left:20px">
              {bullets_html}
            </ul>
          </div>
        </div>
        ''',
        unsafe_allow_html=True,
    )


# ============================================================
#  Section 1：機構撤退訊號 (Systemic Chip)
# ============================================================

def _load_systemic_chip() -> dict:
    """讀 data/macro/systemic_chip.parquet（若已建立）。

    Schema sync 2026-05-10 (S2 5 組全實做後)：
      Group A: foreign_holding_chg_4w (0050 fixed universe) / sbl_change_4w_pct
               + foreign_fut_net_chg_4w (futures_institutional)
      Group B: margin_ratio_z_252d / short_to_long_ratio
      Group C: trust_buy_streak / trust_5d_zscore (institutional_total)
      Group D: pcr_oi (rename fix) / option_top1_concentration
      Group E: hyg_volume_z_252d / tlt_spy_chg_4w
    """
    df = _safe_read_parquet(MACRO_DIR / "systemic_chip.parquet")
    if df is None or df.empty:
        return {}
    last = df.iloc[-1].to_dict()
    return {
        'as_of': last.get('date'),
        'group_a': {  # 外資撤退
            'foreign_holding_chg_4w_pp': last.get('foreign_holding_chg_4w'),
            'sbl_change_4w_pct': last.get('sbl_change_4w_pct'),
            'foreign_fut_net_chg_4w': last.get('foreign_fut_net_chg_4w'),
            'flag': last.get('group_a_flag', 'low'),
            'reason': last.get('group_a_reason', ''),
        },
        'group_b': {  # 籌碼鬆動
            'margin_maintenance_pct': last.get('margin_maintenance_pct'),
            'margin_to_mktcap_pct': last.get('margin_to_mktcap_pct'),
            'margin_mktcap_z_252d': last.get('margin_mktcap_z_252d'),
            'margin_ratio_z_252d': last.get('margin_ratio_z_252d'),
            'short_to_long_ratio': last.get('short_to_long_ratio'),
            'flag': last.get('group_b_flag', 'low'),
            'reason': last.get('group_b_reason', ''),
        },
        'group_c': {  # 投信動能
            'trust_buy_streak': last.get('trust_buy_streak'),
            'trust_5d_zscore': last.get('trust_5d_zscore'),
            'flag': last.get('group_c_flag', 'low'),
            'reason': last.get('group_c_reason', ''),
        },
        'group_d': {  # 期權對沖
            'pcr_oi': last.get('pcr_oi'),
            'option_top1_concentration': last.get('option_top1_concentration'),
            'flag': last.get('group_d_flag', 'low'),
            'reason': last.get('group_d_reason', ''),
        },
        'group_e': {  # ETF 流動
            'hyg_volume_z_252d': last.get('hyg_volume_z_252d'),
            'tlt_spy_chg_4w_pct': last.get('tlt_spy_chg_4w'),
            'flag': last.get('group_e_flag', 'low'),
            'reason': last.get('group_e_reason', ''),
        },
    }


def _render_systemic_chip(sys_chip: dict):
    st.markdown("### 🏦 機構撤退訊號 (Systemic Chip, 1-4w lead)")
    if not sys_chip:
        st.info("⏳ 系統籌碼面板尚未建立，請先執行 `python tools/build_systemic_chip_panel.py`")
        return

    as_of = _fmt_date(sys_chip.get('as_of'))
    st.caption(f"資料日期：{as_of}")

    cols = st.columns(5)
    groups = [
        ('group_a', 'A. 外資撤退', '🔴'),
        ('group_b', 'B. 籌碼鬆動', '🟠'),
        ('group_c', 'C. 投信動能', '🔵'),
        ('group_d', 'D. 期權對沖', '🟣'),
        ('group_e', 'E. ETF 流動', '🟢'),
    ]
    flag_color = {'high': '#FF4444', 'mid': '#FF8800', 'low': '#888888'}
    flag_emoji = {'high': '🔴', 'mid': '🟡', 'low': '⚪'}

    for col, (key, name, _) in zip(cols, groups):
        g = sys_chip.get(key, {})
        flag = g.get('flag', 'low')
        c = flag_color.get(flag, '#888')
        e = flag_emoji.get(flag, '⚪')
        col.markdown(
            f'<div style="font-size:0.95rem"><b>{name}</b></div>'
            f'<div style="font-size:1.4rem;color:{c};font-weight:bold">{e} {flag.upper()}</div>',
            unsafe_allow_html=True,
        )

    # detail 展開（中文化欄位 + 單位 + 說明）
    metric_meta = {
        # Group A
        'foreign_holding_chg_4w_pp': ('外資持股 4 週變化', 'pp (百分點)', '0050 成分股外資持股比例 4 週淨變化；負值大 = 外資撤退'),
        'sbl_change_4w_pct': ('借券餘額 4 週變化', '%', '可借券餘額 4 週變化；正值大 = 空方準備加碼'),
        'foreign_fut_net_chg_4w': ('外資期貨淨部位 4 週變化', '口', '台指期 + 小台外資多空淨口數 4 週變化；負值大 = 外資轉空'),
        # Group B
        'margin_maintenance_pct': ('大盤融資維持率', '% (上市)', '融資擔保品市值 ÷ 官方融資金額（自算，同 M平方/XQ 公式）；新倉基準 166.7%（融資成數 6 成）；2021-2026 實測多頭 170~210、空頭壓縮至 125~150；<140% = 追繳壓力區（法定追繳線 130%；2022-10 谷 123 / 2025-04 谷 125）；急跌時驟降 = 斷頭賣壓，恐慌/見底參考、非頂部指標'),
        'margin_to_mktcap_pct': ('融資餘額佔市值比重', '% (上市)', '官方融資金額 ÷ 上市總市值；越高 = 散戶槓桿越重(過熱)；台積電灌大市值後結構性偏低 (近期約 0.4%)'),
        'margin_mktcap_z_252d': ('融資佔市值比 z-score', 'z (252 日)', '融資佔市值比相對過去 252 日的 z-score；正值大 = 槓桿擴張'),
        'margin_ratio_z_252d': ('融資/指數比 z-score', 'z (252 日)', '融資餘額÷加權指數 相對過去 252 日的 z-score；正值大 = 散戶槓桿擴張(與融資佔市值比訊號高度一致)'),
        'short_to_long_ratio': ('券資比', '比', '融券餘額 ÷ 融資餘額；上升 = 空方相對強'),
        # Group C
        'trust_buy_streak': ('投信連續買賣超', '天', '正 = 連續買超天數，負 = 連續賣超天數'),
        'trust_5d_zscore': ('投信 5 日量能 z-score', 'z', '投信近 5 日買賣超強度；極端負值 = 投信撤退'),
        # Group D
        'pcr_oi': ('Put/Call 未平倉比', '比', '臺指選擇權 Put/Call 未平倉比；>1.2 = 避險升溫'),
        'option_top1_concentration': ('選擇權大戶前 1 集中度', '%', '前 1 大交易人未平倉佔比；過高 = 單一籌碼風險'),
        # Group E
        'hyg_volume_z_252d': ('HYG 成交量 z-score', 'z (252 日)', '美國高收益債 ETF 成交量 z-score；極端值 = 信用流動性事件'),
        'tlt_spy_chg_4w_pct': ('TLT/SPY 比 4 週變化', '%', '長債 / 股票相對表現 4 週變化；正值 = risk-off'),
    }

    with st.expander("詳細數據"):
        rows = []
        for key, name, _ in groups:
            g = sys_chip.get(key, {})
            for k, v in g.items():
                if k in ('flag', 'reason'):
                    continue
                label, unit, desc = metric_meta.get(k) or FIELD_GLOSSARY.get(k, (k, '', ''))
                try:
                    val_str = f"{float(v):.3f}" if v is not None else "—"
                except (TypeError, ValueError):
                    val_str = str(v) if v is not None else "—"
                rows.append({
                    "組": name,
                    "指標": label,
                    "值": val_str,
                    "單位": unit,
                    "說明": desc,
                })
        if rows:
            st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)


# ============================================================
#  Section 2：市場廣度 (Breadth)
# ============================================================

def _load_breadth() -> dict:
    df = _safe_read_parquet(BREADTH_DIR / "tw_breadth.parquet")
    if df is None or df.empty:
        return {}
    last = df.iloc[-1]
    return {
        'as_of': last.get('date'),
        'adl': last.get('adl'),
        'adl_ma20': last.get('adl_ma20'),
        'mcclellan': last.get('mcclellan_oscillator'),
        'up_down_vol_ratio': last.get('up_down_vol_ratio'),
        'new_high_low_diff': last.get('new_high_minus_low'),
        'breadth_thrust': last.get('breadth_thrust_10d'),
        'advances': last.get('advances'),
        'declines': last.get('declines'),
        'avg_correlation_20d': last.get('avg_correlation_20d'),
        'return_dispersion_20d': last.get('return_dispersion_20d'),
    }


def _render_breadth(breadth: dict):
    st.markdown("### 📊 市場廣度 (Breadth)")
    if not breadth:
        st.info("⏳ 廣度資料尚未建立，請先執行 `python tools/build_tw_breadth.py`")
        return

    as_of = _fmt_date(breadth.get('as_of'))
    st.caption(f"資料日期：{as_of}")

    c1, c2, c3, c4, c5 = st.columns(5)

    adl = breadth.get('adl')
    adl_ma20 = breadth.get('adl_ma20')
    if adl is not None:
        delta = (adl - adl_ma20) if adl_ma20 is not None else None
        c1.metric("累積騰落線 ADL", f"{adl:,.0f}",
                  delta=f"{delta:+,.0f} vs MA20" if delta is not None else None)

    mco = breadth.get('mcclellan')
    if mco is not None:
        mco_color = "🔴" if mco < -100 else "🟠" if mco < -50 else "🟢" if mco > 50 else "⚪"
        c2.metric("麥克連震盪指標", f"{mco:.0f}", delta=mco_color)

    ad = breadth.get('up_down_vol_ratio')
    if ad is not None:
        ad_label = "強" if ad > 1.5 else "弱" if ad < 0.6 else "中"
        c3.metric("上漲下跌量能比 (UVOL/DVOL)", f"{ad:.2f}", delta=ad_label,
                  help="上漲股成交量 ÷ 下跌股成交量（量能版漲跌比，非漲跌家數比；家數見下方漲跌家數）；Up/Down Volume Ratio，亦為 Arms Index/TRIN 分母")

    nh_nl = breadth.get('new_high_low_diff')
    if nh_nl is not None:
        c4.metric("52週新高減新低家數", f"{nh_nl:+.0f}",
                  delta="多頭" if nh_nl > 0 else "空頭")

    bt = breadth.get('breadth_thrust')
    if bt is not None:
        bt_label = "🚀 啟動" if bt > 0.65 else "正常"
        c5.metric("Zweig 廣度衝力 10日", f"{bt:.2%}", delta=bt_label,
                  help="Zweig Breadth Thrust 10 日；>61.5% = 強勢起漲訊號")

    advances = breadth.get('advances')
    declines = breadth.get('declines')
    if advances is not None and declines is not None:
        st.caption(f"今日漲家數 {int(advances)} / 跌家數 {int(declines)}")

    # 市場結構：平均成對相關 + 報酬離散度 (P3 2026-05-30)
    corr = breadth.get('avg_correlation_20d')
    disp = breadth.get('return_dispersion_20d')
    if corr is not None and not pd.isna(corr):
        st.markdown("##### 🔗 市場結構 (相關性 / 離散度)")
        s_cols = st.columns(2)
        c_corr = '#FF4444' if corr > 0.4 else '#FF8800' if corr > 0.25 else '#00AA00'
        s_cols[0].markdown(
            f'<div style="font-size:0.9rem">平均成對相關係數 (20日)</div>'
            f'<div style="font-size:1.5rem;color:{c_corr};font-weight:bold">{corr:.3f}</div>'
            f'<div style="font-size:0.75rem;color:#888">高=齊漲齊跌(系統性脆弱) / 低=分散(選股環境)</div>',
            unsafe_allow_html=True,
        )
        if disp is not None and not pd.isna(disp):
            s_cols[1].metric("個股報酬離散度 (20日均)", f"{disp:.2f}%",
                             help="個股漲跌幅橫斷面標準差；低離散+高相關=齊漲齊跌脆弱，高離散=個股分化")

    # 大盤位階：指數對均線乖離率 (從 systemic_chip 讀 twii_dist_ma*；補 trigger price)
    sc = _safe_read_parquet(MACRO_DIR / "systemic_chip.parquet")
    if sc is not None and not sc.empty and 'twii_dist_ma200' in sc.columns:
        last_sc = sc.sort_values('date').iloc[-1]
        st.markdown("##### 📈 大盤位階 (指數對均線乖離率)")
        # fail loud: dawn 鏈斷掉時 panel 停更 (2026-06-06 案例: FRED timeout 拖 59min
        # 觸 Task Scheduler 1hr kill, systemic_chip 沒 rebuild) — 標日期 + stale 警示
        sc_date = pd.to_datetime(last_sc['date'])
        stale_days = (pd.Timestamp.now().normalize() - sc_date.normalize()).days
        if stale_days > 3:
            st.warning(
                f"⚠️ 大盤位階資料停在 {sc_date:%Y-%m-%d}（{stale_days} 天前）— "
                f"dawn 排程鏈可能斷掉，檢查 macro_panels.log 或手動跑 "
                f"`python tools/build_systemic_chip_panel.py`")
        else:
            st.caption(f"資料日期：{sc_date:%Y-%m-%d}")
        m_cols = st.columns(4)
        m_cols[0].metric("加權指數", f"{last_sc['twii_close']:,.0f}")
        for i, w in enumerate((20, 50, 200), start=1):
            d = last_sc.get(f'twii_dist_ma{w}')
            ma = last_sc.get(f'twii_ma{w}')
            if d is not None and not pd.isna(d):
                m_cols[i].metric(
                    f"對 MA{w} 乖離率", f"{d:+.2f}%",
                    delta=f"MA{w}={ma:,.0f}", delta_color="off",
                    help=f"(指數−{w}日均線)/{w}日均線；MA{w} 點位可作 trigger price 參考",
                )


# ============================================================
#  Section 2.5：領頭羊 / 跨市場領先（SOX / Nasdaq / TSM ADR / 台指期(全)）
# ============================================================

@st.cache_data(ttl=300, show_spinner=False)
def _get_full_session_quote() -> dict:
    """台指期(全) 日盤結算 + 夜盤收盤 (live TAIFEX，跨 rerun 快取 5 分鐘)。"""
    try:
        from taifex_data import TAIFEXData
        return TAIFEXData().get_full_session_quote()
    except Exception as e:
        logger.warning("full session quote fetch failed: %s", e)
        return {}


def _render_leadership():
    st.markdown("### 🛰️ 領頭羊 / 跨市場領先 (Leadership & Overnight)")
    lead = _safe_read_parquet(MACRO_DIR / "leadership_panel.parquet")
    if lead is None or lead.empty:
        st.info("⏳ 領頭羊資料尚未建立，請先執行 `python tools/build_leadership_panel.py`")
        return
    lead = lead.sort_values('date')
    last = lead.iloc[-1]
    st.caption(f"資料日期：{_fmt_date(last.get('date'))}（美盤收於台北深夜，為台股次日開盤的隔夜領先；informational tier 未經 IC 驗證）")

    # 兩列美指數位階：收盤(Δ=1日%) + 對 MA20/50/200 乖離（格式同上方「大盤位階」）
    for prefix, name in (('sox', '費城半導體 SOX'), ('nasdaq', '那斯達克')):
        close = last.get(f'{prefix}_close')
        if close is None or pd.isna(close):
            continue
        cols = st.columns(4)
        chg = last.get(f'{prefix}_chg_1d')
        cols[0].metric(name, f"{close:,.0f}",
                       delta=f"{chg:+.2f}% (1日)" if chg is not None and not pd.isna(chg) else None)
        for i, w in enumerate((20, 50, 200), start=1):
            d = last.get(f'{prefix}_dist_ma{w}')
            ma = last.get(f'{prefix}_ma{w}')
            if d is not None and not pd.isna(d):
                cols[i].metric(
                    f"對 MA{w} 乖離率", f"{d:+.2f}%",
                    delta=f"MA{w}={ma:,.0f}" if ma is not None and not pd.isna(ma) else None,
                    delta_color="off",
                    help=f"(指數−{w}日均線)/{w}日均線；於指數自身交易日序列計算",
                )

    # 第三列：相對強弱 + ADR 溢價 + 台指期(全)夜盤
    cols = st.columns(4)
    sox_rs = last.get('sox_rs_chg_4w')
    if sox_rs is not None and not pd.isna(sox_rs):
        cols[0].metric("SOX/TWII 相對強弱 4週", f"{sox_rs:+.2f}%",
                       delta="半導體領先" if sox_rs > 0 else "領頭羊鬆動",
                       delta_color="normal" if sox_rs > 0 else "inverse",
                       help="SOX÷TWII 比值近 4 週變化；轉負=窄幅領漲行情的早期裂痕警報")
    ndq_rs = last.get('nasdaq_rs_chg_4w')
    if ndq_rs is not None and not pd.isna(ndq_rs):
        cols[1].metric("Nasdaq/TWII 相對強弱 4週", f"{ndq_rs:+.2f}%",
                       delta="美科技領先" if ndq_rs > 0 else "外部動能轉弱",
                       delta_color="normal" if ndq_rs > 0 else "inverse",
                       help="Nasdaq÷TWII 比值近 4 週變化")
    adr = last.get('tsm_adr_premium_pct')
    if adr is not None and not pd.isna(adr):
        cols[2].metric("TSM ADR 溢價", f"{adr:+.2f}%",
                       delta="ADR 溢價 (外資偏多)" if adr > 0 else "ADR 折價 (外資偏空)",
                       delta_color="normal" if adr > 0 else "inverse",
                       help="(TSM_ADR×匯率÷5)÷2330−1；1 ADR=5 股；隔夜領先台股開盤")
    q = _get_full_session_quote()
    if q and q.get('night_close'):
        chg_pct = q.get('night_chg_pct')
        cols[3].metric(
            "台指期(全) 夜盤收盤", f"{q['night_close']:,.0f}",
            delta=f"{chg_pct:+.2f}% vs 日盤結算 {q.get('day_settle'):,.0f}" if chg_pct is not None else None,
            help=f"盤後時段 15:00~次日 05:00（交易日 {q.get('night_date')}）；漲跌基準=前一日盤結算"
                 f"（{q.get('day_date')}）→ 隔夜 gap，預示次一交易日開盤跳空方向",
        )


# ============================================================
#  Section 3：情緒與波動（呼叫 market_banner）
# ============================================================

def _render_sentiment_section():
    """呼叫既有 market_banner 完整版，當作 Section 3。"""
    st.markdown("### 🌡️ 情緒與波動 (Sentiment & Volatility)")
    try:
        from market_banner import render_market_banner
        render_market_banner()
    except Exception as e:
        st.error(f"Banner 載入失敗：{e}")

    # ----------------------------------------------------------
    #  Vol Complex 共振面板 (informational tier, 2026-05-25)
    #  美股 4 訊號 → VIX/VIX3M 期限結構 + VVIX + SKEW + OVX
    #  ⚠️ 美股經驗閾值，未在台股 IC 驗證；用戶 framework 直接接觀察
    # ----------------------------------------------------------
    vc_df = _safe_read_parquet(SENT_DIR / "vol_complex_history.parquet")
    if vc_df is not None and not vc_df.empty:
        latest = vc_df.iloc[-1]
        regime = latest['regime']
        lit = int(latest['lit_count'])
        regime_color = {'green': '🟢', 'monitor': '🟡', 'warning': '🟠',
                        'high_alert': '🔴', 'defensive': '🔴'}.get(regime, '⚪')

        st.markdown(f"##### {regime_color} Vol Complex 共振 (US, informational) — regime: **{regime}** (lit {lit}/4)")
        st.caption(f"資料日期：{_fmt_date(latest['date'])} / "
                   f"⚠️ 美股經驗閾值未經台股 IC 驗證，僅觀察用不接 portfolio")

        light_emoji = {'green': '🟢', 'yellow': '🟡', 'orange': '🟠', 'red': '🔴'}
        vc_cols = st.columns(4)

        vc_cols[0].metric(
            "VIX/VIX3M 期限結構",
            f"{latest['vix_vix3m_ratio']:.3f}",
            delta=f"{light_emoji[latest['vix_vix3m_ratio_light']]} >1.00 = backwardation",
            help="近月/3個月 VIX 比；>1.00 急性恐慌，<0.95 contango 正常"
        )
        vc_cols[1].metric(
            "VVIX 波動率的波動率",
            f"{latest['vvix']:.1f}",
            delta=f"{light_emoji[latest['vvix_light']]} 尾端對沖需求",
            help="VIX 選擇權隱波；>110 機構搶尾端保險"
        )
        vc_cols[2].metric(
            "CBOE SKEW 偏態指數",
            f"{latest['skew']:.1f}",
            delta=f"{light_emoji[latest['skew_light']]} 左尾溢價",
            help="OTM put 相對價格；>145 機構偷偷對沖"
        )
        vc_cols[3].metric(
            "OVX 原油波動指數",
            f"{latest['ovx']:.1f}",
            delta=f"{light_emoji[latest['ovx_light']]} 地緣事件 lead",
            help="CBOE Crude Oil VIX；中東衝突常領先 VIX"
        )

        if lit >= 2:
            st.warning(f"⚠️ Vol Complex {lit}/4 訊號亮燈 — 用戶 framework 建議: "
                       f"{'2 燈減倉 30%' if lit == 2 else '3 燈減倉 60% + 買保護' if lit == 3 else '4 燈防禦模式'}（美股經驗值未驗 TW）")


# ============================================================
#  Section 4：流動性與資金
# ============================================================

def _load_macro() -> dict:
    df = _safe_read_parquet(MACRO_DIR / "fred_panel.parquet")
    if df is None or df.empty:
        return {}
    last = df.iloc[-1].to_dict()
    last['as_of'] = df.index[-1] if df.index.name else last.get('date')
    return last


def _render_liquidity(macro: dict, banner_data: dict):
    st.markdown("### 💵 流動性與資金 (Liquidity & Flow)")
    cols = st.columns(4)

    # M1B 比 (從 banner 拿)
    m1b = (banner_data or {}).get('m1b_ratio') or {}
    r = m1b.get('ratio_pct')
    if r is not None:
        cols[0].metric("近20日成交值/M1B 比", f"{r:.1f}%",
                       delta=m1b.get('label', ''))

    # DXY
    dxy = macro.get('dxy_close')
    dxy_chg = macro.get('dxy_chg_4w')
    if dxy is not None:
        cols[1].metric("美元指數 DXY", f"{dxy:.2f}",
                       delta=f"{dxy_chg:+.2f}% (4w)" if dxy_chg is not None else None)

    # NFCI Chicago Fed (取代 USDTWD 因為更高訊號)
    nfci = macro.get('chicago_nfci')
    if nfci is not None:
        c_nfci = '#FF4444' if nfci > 0.5 else '#FF8800' if nfci > 0 else '#00AA00'
        cols[2].metric("芝加哥Fed 金融情勢指數 NFCI", f"{nfci:+.2f}",
                       delta='收緊' if nfci > 0 else '寬鬆',
                       delta_color="inverse" if nfci > 0 else "normal",
                       help="Chicago Fed National Financial Conditions Index — 105 個市場指標複合，>0=收緊 <0=寬鬆")

    # Fed Balance Sheet (WALCL)
    walcl = macro.get('fed_bs_trillion')
    walcl_chg = macro.get('fed_bs_chg_4w')
    if walcl is not None:
        cols[3].metric("Fed 資產負債表", f"${walcl:.2f}T",
                       delta=f"{walcl_chg:+.2f}% (4w)" if walcl_chg is not None else None)

    # Net Liquidity plumbing (Fed BS - RRP - TGA + RRP/TGA/SOFR, 2026-05-30 P3 added)
    net_liq = macro.get('net_liquidity_bil')
    if net_liq is not None and not pd.isna(net_liq):
        st.markdown("##### 🚰 淨流動性 plumbing (Fed BS − RRP − TGA)")
        nl_cols = st.columns(4)
        nl_chg = macro.get('net_liquidity_chg_4w')
        nl_cols[0].metric(
            "淨流動性 (Net Liquidity)", f"${net_liq/1000:.2f}T",
            delta=f"{nl_chg:+.0f}B (4w)" if nl_chg is not None and not pd.isna(nl_chg) else None,
            help="Fed 資產 − 逆回購RRP − 國庫帳TGA；升=注水 risk-on，降=抽水")
        rrp = macro.get('rrp_balance')
        tga = macro.get('tga_balance')
        sofr = macro.get('sofr')
        if rrp is not None and not pd.isna(rrp):
            nl_cols[1].metric("逆回購 RRP", f"${rrp:,.0f}B", help="隔夜逆回購餘額；升=資金回籠到 Fed (抽流動性)")
        if tga is not None and not pd.isna(tga):
            nl_cols[2].metric("國庫帳 TGA", f"${tga:,.0f}B", help="財政部國庫帳；升=抽走銀行準備金")
        if sofr is not None and not pd.isna(sofr):
            nl_cols[3].metric("SOFR", f"{sofr:.2f}%", help="擔保隔夜融資利率；飆升=短期資金面緊張")

    # Tier 1 ETF flows (HYG/JNK/TLT/SPY)
    etf = _safe_read_parquet(MACRO_DIR / "etf_flows.parquet")
    if etf is not None and not etf.empty:
        st.markdown("##### 🏦 風險偏好 ETF 流動 (Tier 1, 信用避險 proxy)")
        last = etf.iloc[-1]
        f_cols = st.columns(4)

        hyg_chg = last.get('hyg_chg_4w')
        if hyg_chg is not None and not pd.isna(hyg_chg):
            c = '#FF4444' if hyg_chg < -3 else '#00AA00' if hyg_chg > 0 else '#888'
            f_cols[0].metric("高收益債HYG 4週變化", f"{hyg_chg:+.2f}%",
                             delta='避險' if hyg_chg < -1 else '冒險',
                             help="iShares HY Bond 4w 變化；負值大 = 信用避險升溫")

        hyg_lqd = last.get('hyg_to_lqd_ratio')
        hyg_lqd_chg = last.get('hyg_to_lqd_chg_4w')
        if hyg_lqd is not None and not pd.isna(hyg_lqd):
            f_cols[1].metric("高收益債/投資級債 比 (HYG/LQD)", f"{hyg_lqd:.4f}",
                             delta=f"{hyg_lqd_chg:+.2f}% 4w" if hyg_lqd_chg is not None and not pd.isna(hyg_lqd_chg) else None,
                             help="HY/IG 相對表現；下跌 = HY 弱於 IG = 信用避險")

        tlt_spy = last.get('tlt_spy_ratio')
        tlt_spy_chg = last.get('tlt_spy_chg_4w')
        if tlt_spy is not None and not pd.isna(tlt_spy):
            f_cols[2].metric("長債/股票 比 (TLT/SPY)", f"{tlt_spy:.4f}",
                             delta=f"{tlt_spy_chg:+.2f}% 4w" if tlt_spy_chg is not None and not pd.isna(tlt_spy_chg) else None,
                             help="長債/股票相對；上升 = risk-off")

        hyg_vol_z = last.get('hyg_volume_z_252d')
        if hyg_vol_z is not None and not pd.isna(hyg_vol_z):
            c = '#FF4444' if abs(hyg_vol_z) > 2 else '#888'
            f_cols[3].metric("高收益債HYG成交量 z-score", f"{hyg_vol_z:+.2f}",
                             help="HYG 成交量相對 252 日 z-score；極端值 = 流動性事件")

    if not any([r, dxy, walcl]):
        st.info("⏳ 流動性面板資料尚未建立，請執行 `python tools/fetch_fred_macro.py`")

    # ----------------------------------------------------------
    #  台灣總定存餘額 (CBC EF15M01 月頻)
    #  訊號邏輯：定存 MoM 連 2 月為負 → 錢離開銀行體系 → risk-on 強化
    # ----------------------------------------------------------
    td_df = _safe_read_parquet(SENT_DIR / "time_deposits_history.parquet")
    if td_df is not None and not td_df.empty:
        st.markdown("##### 🏦 台灣總定存餘額 (CBC, 月頻, 1.5-2 月 lag)")
        latest = td_df.iloc[-1]
        td_cols = st.columns(4)

        td_cols[0].metric(
            "定存餘額",
            f"{latest['time_deposits_mil_twd']/1e6:.2f} 兆",
            delta=f"period {latest['period']}",
            help="定期存款 + 定期儲蓄存款；中央銀行 EF15M01 月底日平均餘額"
        )

        mom = latest['time_deposits_mom_pct']
        td_cols[1].metric(
            "定存 MoM",
            f"{mom:+.2f}%",
            delta='錢離開存款' if mom < 0 else '錢持續進存款',
            delta_color="inverse" if mom < 0 else "normal",
            help="月增率為負 = 定存外流，可能流入股市/房市 (risk-on 訊號)"
        )

        yoy = latest['time_deposits_yoy_pct']
        td_cols[2].metric("定存 YoY", f"{yoy:+.2f}%")

        ratio = latest['m1b_to_time_deposits_ratio']
        ratio_label = '偏活期化' if ratio > 1.3 else ('偏定存' if ratio < 1.0 else '正常')
        td_cols[3].metric(
            "M1B / 定存 比",
            f"{ratio:.3f}",
            delta=ratio_label,
            help=">1.3 偏活期化 (流動性過剩, 偏熱) / <1.0 偏定存 (資金鎖死)"
        )

        # 連續月變動判讀
        last3 = td_df.tail(3)['time_deposits_mom_pct'].tolist()
        if len(last3) == 3 and all(v < 0 for v in last3):
            st.warning(f"⚠️ 定存 MoM 連 3 月為負 ({last3[0]:+.2f} / {last3[1]:+.2f} / {last3[2]:+.2f} %) — 資金離開存款體系，risk-on 強訊號")
        elif len(last3) >= 2 and all(v < 0 for v in last3[-2:]):
            st.info(f"💡 定存 MoM 連 2 月為負 ({last3[-2]:+.2f} / {last3[-1]:+.2f} %) — 觀察是否續為第 3 月")


# ============================================================
#  Section 5：信用與景氣
# ============================================================

def _render_credit_cycle(macro: dict):
    st.markdown("### 📉 信用與景氣 (Credit & Business Cycle, 1-3mo lead)")
    cols = st.columns(5)

    # HY OAS
    hy = macro.get('hy_oas')
    hy_rank = macro.get('hy_oas_rank')
    if hy is not None:
        c = _color_rank(hy_rank, hi_is_bad=True)
        rank_str = f"rank {hy_rank:.0f}" if hy_rank is not None else ""
        cols[0].markdown(
            f'<div style="font-size:0.9rem">HY OAS</div>'
            f'<div style="font-size:1.5rem;color:{c};font-weight:bold">{hy:.2f}</div>'
            f'<div style="font-size:0.8rem;color:#888">{rank_str}</div>',
            unsafe_allow_html=True,
        )

    # 殖利率曲線 10Y-2Y
    yc2 = macro.get('yield_curve_10y_2y')
    if yc2 is not None:
        c = '#FF4444' if yc2 < 0 else '#FF8800' if yc2 < 0.3 else '#888888'
        label = '倒掛' if yc2 < 0 else '正常'
        cols[1].markdown(
            f'<div style="font-size:0.9rem">10Y-2Y</div>'
            f'<div style="font-size:1.5rem;color:{c};font-weight:bold">{yc2:+.2f}%</div>'
            f'<div style="font-size:0.8rem;color:#888">{label}</div>',
            unsafe_allow_html=True,
        )

    # 殖利率曲線 10Y-3M
    yc3 = macro.get('yield_curve_10y_3m')
    if yc3 is not None:
        c = '#FF4444' if yc3 < 0 else '#FF8800' if yc3 < 0.3 else '#888888'
        label = '倒掛' if yc3 < 0 else '正常'
        cols[2].markdown(
            f'<div style="font-size:0.9rem">10Y-3M</div>'
            f'<div style="font-size:1.5rem;color:{c};font-weight:bold">{yc3:+.2f}%</div>'
            f'<div style="font-size:0.8rem;color:#888">{label}</div>',
            unsafe_allow_html=True,
        )

    # FSI (取代 ISM 新訂單，因 NAPMNOI FRED 已下架)
    fsi = macro.get('st_louis_fsi')
    if fsi is not None:
        c = '#FF4444' if fsi > 1 else '#FF8800' if fsi > 0 else '#00AA00'
        label = '高壓力' if fsi > 1 else '中度' if fsi > 0 else '低壓力'
        cols[3].metric("聖路易Fed 金融壓力指數", f"{fsi:+.2f}", delta=label,
                       help="St. Louis Fed Financial Stress Index — 18 個市場指標複合，0 = 歷史均值")

    # CCC OAS — 尾部信用 (HY OAS 的高風險層，2026-05-30 P3 added)
    ccc = macro.get('ccc_oas')
    ccc_rank = macro.get('ccc_oas_rank')
    if ccc is not None and not pd.isna(ccc):
        cc = _color_rank(ccc_rank, hi_is_bad=True)
        rank_str = f"rank {ccc_rank:.0f}" if ccc_rank is not None and not pd.isna(ccc_rank) else ""
        cols[4].markdown(
            f'<div style="font-size:0.9rem">CCC OAS (尾部信用)</div>'
            f'<div style="font-size:1.5rem;color:{cc};font-weight:bold">{ccc:.2f}</div>'
            f'<div style="font-size:0.8rem;color:#888">{rank_str}</div>',
            unsafe_allow_html=True,
        )

    # Tier 1 (2026-05-09) 補充：失業/初請/消費者信心/嚴格 Buffett
    st.markdown("##### 🇺🇸 美國經濟先行 (Tier 1, 2026-05-09 added)")
    e_cols = st.columns(5)

    unemp = macro.get('us_unemployment_rate')
    unemp_chg = macro.get('us_unemp_chg_3m')
    if unemp is not None:
        c = '#FF4444' if unemp_chg and unemp_chg > 0.3 else '#FF8800' if unemp_chg and unemp_chg > 0.1 else '#00AA00'
        e_cols[0].metric("失業率", f"{unemp:.1f}%",
                         delta=f"{unemp_chg:+.2f}pp 3m" if unemp_chg is not None else None,
                         delta_color="inverse")

    claims_yoy = macro.get('us_claims_yoy')
    if claims_yoy is not None:
        e_cols[1].metric("初請失業金 YoY", f"{claims_yoy:+.1f}%", delta_color="inverse")

    sent_yoy = macro.get('us_sent_yoy')
    if sent_yoy is not None:
        c = '#FF4444' if sent_yoy < -10 else '#FF8800' if sent_yoy < 0 else '#00AA00'
        e_cols[2].metric("消費信心 YoY", f"{sent_yoy:+.1f}%")

    durable_yoy = macro.get('us_durable_yoy')
    if durable_yoy is not None:
        e_cols[3].metric("耐久財訂單 YoY", f"{durable_yoy:+.1f}%")

    buffett_strict = macro.get('us_buffett_strict')
    buffett_strict_rank = macro.get('us_buffett_strict_rank')
    if buffett_strict is not None:
        c = _color_rank(buffett_strict_rank, hi_is_bad=True)
        e_cols[4].markdown(
            f'<div style="font-size:0.85rem">嚴格 Buffett (Nonfin Eq/GDP)</div>'
            f'<div style="font-size:1.3rem;color:{c};font-weight:bold">{buffett_strict:.0f}%</div>'
            f'<div style="font-size:0.75rem;color:#888">rank {buffett_strict_rank:.0f}</div>',
            unsafe_allow_html=True,
        )

    # TW LEI 7 components (NDC private JSON API, 2026-05-10 commit f95ae52)
    df_tw = _safe_read_parquet(MACRO_DIR / "tw_lei_panel.parquet")
    if df_tw is not None and not df_tw.empty:
        st.markdown("##### 🇹🇼 TW 景氣領先指標 7 components (NDC, 月頻)")
        # 確保 date 是 index
        if 'date' in df_tw.columns:
            df_tw = df_tw.set_index('date')
        df_tw = df_tw.sort_index()
        last_tw = df_tw.iloc[-1]
        last_dt = df_tw.index[-1]

        # composite YoY 12 個月
        if 'lei_composite' in df_tw.columns and len(df_tw) >= 13:
            yoy = (last_tw['lei_composite'] / df_tw['lei_composite'].iloc[-13] - 1) * 100
            mom = (last_tw['lei_composite'] / df_tw['lei_composite'].iloc[-2] - 1) * 100
        else:
            yoy = mom = None

        tw_cols = st.columns(3)
        if yoy is not None:
            c = '#FF4444' if yoy < 0 else '#00AA00'
            tw_cols[0].metric("景氣領先指標 YoY", f"{yoy:+.2f}%",
                              delta="衰退" if yoy < 0 else "擴張")
        if mom is not None:
            tw_cols[1].metric("景氣領先指標 MoM", f"{mom:+.2f}%",
                              delta="加速" if mom > 0 else "減速")
        tw_cols[2].caption(f"資料日期：{_fmt_date(last_dt)}")

        with st.expander("LEI 7 components 詳細數據"):
            # (中文名, 單位, 說明)
            comp_meta = {
                'leading_export_order_idx': ('外銷訂單動向指數', '指數', '製造業外銷訂單動向；>50 = 擴張，<50 = 收縮'),
                'leading_m1b': ('貨幣總計數 M1B', '百萬元', 'M1A + 活期儲蓄存款；上升 = 資金寬鬆'),
                'leading_stock_price_idx': ('股價指數', '點 (月平均)', '台股加權股價月平均；領先實體經濟 6-9 個月'),
                'leading_employee_entry_rt': ('工業/服務業員工淨進入率', '%', '進入率 - 退出率；正值 = 就業擴張'),
                'leading_building_area': ('建築物開工樓地板面積', '千平方公尺', '新建案開工面積；上升 = 房市/營建活絡'),
                'leading_semi_equip_import': ('半導體設備進口值', '萬美元', '半導體業資本支出 proxy；上升 = 廠商擴產'),
                'leading_mfg_climate': ('製造業營業氣候測驗點', '燈號分數', 'TIER 製造業景氣燈號；5=紅燈過熱 / 1=藍燈低迷'),
            }
            rows = []
            for k, (name, unit, desc) in comp_meta.items():
                v = last_tw.get(k)
                if v is None or pd.isna(v):
                    continue
                if len(df_tw) >= 13 and k in df_tw.columns:
                    prev = df_tw[k].iloc[-13]
                    yoy_c = (v / prev - 1) * 100 if prev and prev != 0 else None
                else:
                    yoy_c = None
                # 大數字加千分位，小數字保留兩位
                fv = float(v)
                if abs(fv) >= 1000:
                    val_str = f"{fv:,.0f}"
                else:
                    val_str = f"{fv:,.2f}"
                rows.append({
                    "項目": name,
                    "數值": val_str,
                    "單位": unit,
                    "YoY %": f"{yoy_c:+.2f}" if yoy_c is not None else "n/a",
                    "說明": desc,
                })
            if rows:
                st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)
            st.caption("資料源：國發會 (NDC) `index.ndc.gov.tw` private JSON API；歷史 1967-01+，all-7 from 2013-07")


# ============================================================
#  Section 6：估值
# ============================================================

def _load_valuation() -> dict:
    df = _safe_read_parquet(MACRO_DIR / "valuation_panel.parquet")
    if df is None or df.empty:
        return {}
    return df.iloc[-1].to_dict()


def _render_valuation(val: dict):
    st.markdown("### 💎 估值 (Valuation)")
    cols = st.columns(4)

    # TW PE
    tw_pe = val.get('tw_market_pe')
    if tw_pe is not None:
        c = '#FF4444' if tw_pe > 25 else '#FF8800' if tw_pe > 20 else '#00AA00'
        cols[0].metric("台股大盤 PE", f"{tw_pe:.2f}",
                       delta="偏高" if tw_pe > 22 else "合理")

    # TW Yield
    tw_yield = val.get('tw_market_yield')
    if tw_yield is not None:
        cols[1].metric("台股殖利率", f"{tw_yield:.2f}%")

    # 台股市值 rank (TWII proxy)
    # 註：valuation_panel.buffett_indicator_tw 公式 = TWII close（缺 TW market
    # cap 公開資料），數值本身沒意義，只看 rank。原 label "巴菲特指標 (TW)"
    # 顯示 41603% 會誤導，2026-05-09 改成只顯示 rank。
    buffett_rank = val.get('buffett_rank_tw')
    if buffett_rank is not None:
        c = _color_rank(buffett_rank, hi_is_bad=True)
        cols[2].markdown(
            f'<div style="font-size:0.9rem">台股市值 rank (proxy)</div>'
            f'<div style="font-size:1.5rem;color:{c};font-weight:bold">{buffett_rank:.0f}</div>'
            f'<div style="font-size:0.75rem;opacity:0.65">10yr 百分位（TWII 替代，缺 TW 市值資料）</div>',
            unsafe_allow_html=True,
        )

    # 美股 Buffett 用 fred_panel.us_buffett_strict (Nonfin Corp Equity / GDP，
    # 真正定義 ~200% 區間)；valuation_panel.buffett_indicator_us 是 sp500/gdp
    # 比例 ~23%，rank 跟 strict 高度相關但數值會誤導，2026-05-09 改顯示 strict。
    macro_ctx = _load_macro()
    buffett_strict = macro_ctx.get('us_buffett_strict')
    buffett_strict_rank = macro_ctx.get('us_buffett_strict_rank')
    if buffett_strict is not None:
        c = _color_rank(buffett_strict_rank, hi_is_bad=True)
        rank_str = (f"rank {buffett_strict_rank:.0f}"
                    if buffett_strict_rank is not None else "")
        cols[3].markdown(
            f'<div style="font-size:0.9rem">巴菲特指標 (US)</div>'
            f'<div style="font-size:1.5rem;color:{c};font-weight:bold">{buffett_strict:.0f}%</div>'
            f'<div style="font-size:0.75rem;opacity:0.65">{rank_str}・Nonfin Eq / GDP (FRED)</div>',
            unsafe_allow_html=True,
        )

    if not any([tw_pe, tw_yield, buffett_rank, buffett_strict]):
        st.info("⏳ 估值面板資料尚未建立，請執行 `python tools/build_valuation_panel.py`")


# ============================================================
#  主入口
# ============================================================

def _render_ai_report_section():
    """AI 風向研究報告 -- 匯出 claude.ai 提示詞 (單檔 HTML 網頁) + 貼回存檔。

    2026-05-30：從本地 Opus CLI 改為「匯出提示詞」工作流。提示詞要求 claude.ai
    輸出單檔自包含 HTML 網頁（可當 Artifact 預覽/下載），使用者再把整頁 HTML 貼回
    下方存進報告庫（data/macro_reports/）。不消耗本地 Agent SDK Credit。
    本地 LLM 版仍可手動跑 `python tools/macro_compass_report.py`。
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # 標題 + 產生按鈕緊鄰（標題左、按鈕貼著放，右側留白），不折疊
    hcol, bcol, _sp = st.columns([2, 1.3, 4], vertical_alignment="bottom")
    hcol.markdown("### 🤖 AI 風向研究報告")
    gen_html = bcol.button("📋 產生 HTML 提示詞", type="primary",
                           help="組裝資料面板 + 報告指示為提示詞（要求 claude.ai "
                                "輸出單檔 HTML 網頁），複製/下載後貼到 claude.ai")

    if gen_html:
        try:
            import sys as _sys
            _tools = str(REPO / "tools")
            if _tools not in _sys.path:
                _sys.path.insert(0, _tools)
            from macro_compass_report import collect_context, build_prompt
            with st.spinner("組裝資料面板中（約數秒）..."):
                ctx = collect_context()
                st.session_state['macro_prompt'] = build_prompt(ctx, fmt="webpage")
        except Exception as e:
            st.error(f"產生提示詞失敗：{e}")

    prompt_txt = st.session_state.get('macro_prompt')
    if prompt_txt:
        st.success(f"已產生 HTML 提示詞（{len(prompt_txt):,} 字）。點下方按鈕複製 → 貼到 "
                   "claude.ai → 它會生出 HTML 網頁，把整頁 HTML 貼回下方存檔。")
        import html as _htmlmod
        _esc = _htmlmod.escape(prompt_txt)  # 防 </textarea>/</script> 注入
        _btn = f"""
        <textarea id="macro_prompt_src" style="position:absolute;left:-9999px;">{_esc}</textarea>
        <button id="macro_copy_btn"
          style="background:#2563eb;color:#fff;border:none;padding:8px 18px;border-radius:8px;
                 font-size:15px;font-weight:600;cursor:pointer;">
          📋 一鍵複製提示詞到剪貼簿
        </button>
        <span id="macro_copy_status" style="margin-left:12px;color:#0f9d58;font-weight:600;"></span>
        <script>
          document.getElementById("macro_copy_btn").addEventListener("click", async () => {{
            const src = document.getElementById("macro_prompt_src");
            const status = document.getElementById("macro_copy_status");
            try {{
              if (navigator.clipboard && navigator.clipboard.writeText) {{
                await navigator.clipboard.writeText(src.value);
              }} else {{
                src.style.left = "0"; src.select(); document.execCommand("copy"); src.style.left = "-9999px";
              }}
              status.textContent = "✅ 已複製 " + src.value.length.toLocaleString() + " chars";
            }} catch (e) {{
              status.textContent = "❌ 複製失敗: " + e.message;
            }}
          }});
        </script>
        """
        st.components.v1.html(_btn, height=56)
        with st.expander("📄 顯示提示詞全文（檢視/手動選取）", expanded=False):
            st.code(prompt_txt, language="markdown")

    # 貼回 claude.ai 的 HTML 輸出 → 存進報告庫
    st.markdown("##### 📥 貼回 claude.ai 的 HTML 輸出")
    st.caption("把 claude.ai 生成的整頁 HTML 貼到下方 → 存進報告庫 "
               "（data/macro_reports/，並更新 latest.html）。")
    paste = st.text_area("貼上整頁 HTML", value="", height=180,
                         placeholder="<!DOCTYPE html><html>...</html>",
                         key='macro_paste_html')
    if st.button("📥 儲存到報告庫", type='primary', key='macro_paste_save'):
        if not paste or not paste.strip():
            st.error("請先貼上 claude.ai 生成的 HTML")
        elif '<html' not in paste.lower():
            st.error("貼上的內容看起來不是完整 HTML（找不到 <html> 標籤）")
        else:
            ts = datetime.now().strftime('%Y-%m-%d_%H%M%S')
            out = REPORTS_DIR / f"{ts}.html"
            out.write_text(paste, encoding='utf-8')
            (REPORTS_DIR / "latest.html").write_text(paste, encoding='utf-8')
            st.success(f"已存成 {out.name} 並更新 latest.html。可在下方歷史報告查看。")

    # 歷史報告（含剛貼回存檔的，與本地 LLM legacy 報告）
    htmls = sorted(REPORTS_DIR.glob("*.html"), reverse=True)
    htmls = [h for h in htmls if h.name != 'latest.html']
    if htmls:
        with st.expander("查看歷史報告", expanded=False):
            options = ['(最新)'] + [h.stem for h in htmls[:20]]
            sel = st.selectbox("歷史報告", options=options, index=0,
                               key='macro_hist_report')
            target = (REPORTS_DIR / "latest.html" if sel == '(最新)'
                      else REPORTS_DIR / f"{sel}.html")
            if target.exists():
                st.components.v1.html(target.read_text(encoding='utf-8'),
                                      height=900, scrolling=True)


def render_macro_dashboard():
    """總經大盤風向 tab 主入口。"""
    st.markdown("# 🧭 總經大盤風向 (Macro Compass)")
    st.caption("整合 籌碼 / 廣度 / 情緒 / 流動性 / 信用景氣 / 估值 — informational tier (SOP-14)")

    # AI 風向研究報告（置頂，最先呈現）
    _render_ai_report_section()

    # 載入所有資料
    try:
        from market_banner import _get_banner_data
        banner_data = _get_banner_data()
    except Exception as e:
        logger.warning("get_banner_data failed: %s", e)
        banner_data = {}

    sys_chip = _load_systemic_chip()
    breadth = _load_breadth()
    macro = _load_macro()
    valuation = _load_valuation()

    # 總風向
    compass = _compute_compass_verdict(banner_data, sys_chip, breadth, macro, valuation)
    _render_compass_card(compass)
    st.markdown("---")

    # 情緒與波動（緊接總風向下方）
    _render_sentiment_section()
    st.markdown("---")

    # 估值（接情緒與波動下方）
    _render_valuation(valuation)
    st.markdown("---")

    # 機構撤退
    _render_systemic_chip(sys_chip)
    st.markdown("---")

    # 市場廣度
    _render_breadth(breadth)
    st.markdown("---")

    # 領頭羊 / 跨市場領先（SOX / Nasdaq / TSM ADR / 台指期(全)夜盤）
    _render_leadership()
    st.markdown("---")

    # 流動性與資金
    _render_liquidity(macro, banner_data)
    st.markdown("---")

    # 信用與景氣
    _render_credit_cycle(macro)
    st.markdown("---")

    # Slow Track 60d composite (Banner v4，IC-validated)：移至最下方
    try:
        from banner_risk_score_v4_slow import compute_slow_track_score, render as render_slow
        st.markdown("### 🐢 Slow Track Composite (60d 區間警示, IC-validated)")
        slow_score = compute_slow_track_score()
        render_slow(slow_score)
    except Exception as e:
        logger.warning("Slow track render failed: %s", e)

    # footer
    st.markdown("---")
    st.caption(
        "資料來源：FRED (HY OAS / Yield Curve / DXY / VIX / Fed BS)、"
        "主計處 (TW LEI / 出口訂單)、TWSE (廣度 / PE / 巴菲特)、"
        "FinMind (籌碼)、TDCC (集保)、TAIFEX (期權)。"
        f"渲染時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
