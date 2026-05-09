"""
macro_dashboard.py -- 總經大盤風向 (Macro Compass)

新功能 tab：把原 market_banner 大盤儀表板搬過來，外加 macro / breadth /
systemic chip / 估值 等資料源整合，給使用者一個「容易讀」的全景視圖。

7 大區塊（從上到下：總結 → 領先 → 同步 → 滯後）:
  0. 🚦 總風向卡片 (Top Card)：5 階燈號 + 一句話定調 + 主導訊號 top 3
  1. 機構撤退訊號 (Systemic Chip, 1-4w lead)：A/B/C/D/E 5 組
  2. 市場廣度 (Market Breadth, 同步~短 lead)：ADL/McClellan/新高低/A/D 量能
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
    ) if top else '<li style="color:#888">無顯著訊號</li>'

    st.markdown(
        f'''
        <div style="border:2px solid {color};border-radius:12px;padding:16px;
                    background:linear-gradient(135deg, {color}11, {color}22);
                    margin-bottom:16px">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <div style="font-size:1.6rem;font-weight:bold;color:{color}">
              {emoji} 總風向：{label_zh}
            </div>
            <div style="font-size:0.85rem;color:#666">
              SOP-14 informational tier；非 portfolio rebalance gate
            </div>
          </div>
          <div style="font-size:1.05rem;margin-top:6px;color:#222">
            {verdict}
          </div>
          <div style="margin-top:8px">
            <div style="font-size:0.85rem;color:#666;margin-bottom:4px">主導訊號</div>
            <ul style="margin:0;padding-left:20px;color:#333">
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
    """讀 data/macro/systemic_chip.parquet（若已建立）。"""
    df = _safe_read_parquet(MACRO_DIR / "systemic_chip.parquet")
    if df is None or df.empty:
        return {}
    last = df.iloc[-1].to_dict()
    return {
        'as_of': last.get('date'),
        'group_a': {  # 外資撤退
            'foreign_streak': last.get('foreign_sell_streak'),
            'sbl_change_4w': last.get('sbl_change_4w_pct'),
            'foreign_fut_oi': last.get('foreign_fut_net_oi'),
            'flag': last.get('group_a_flag', 'low'),
            'reason': last.get('group_a_reason', ''),
        },
        'group_b': {  # 籌碼鬆動
            'tdcc_chunky_change': last.get('tdcc_chunky_change_4w'),
            'margin_ratio': last.get('margin_to_index_ratio'),
            'short_ratio': last.get('short_to_long_ratio'),
            'flag': last.get('group_b_flag', 'low'),
            'reason': last.get('group_b_reason', ''),
        },
        'group_c': {  # 投信動能
            'trust_streak': last.get('trust_buy_streak'),
            'flag': last.get('group_c_flag', 'low'),
        },
        'group_d': {  # 期權對沖
            'pcr_oi': last.get('pcr_oi'),
            'top5_top10_diff': last.get('top5_top10_oi_diff'),
            'flag': last.get('group_d_flag', 'low'),
        },
        'group_e': {  # ETF 流動
            'etf_redemption': last.get('etf_redemption_streak'),
            'flag': last.get('group_e_flag', 'low'),
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

    # detail 展開
    with st.expander("詳細數據"):
        rows = []
        for key, name, _ in groups:
            g = sys_chip.get(key, {})
            for k, v in g.items():
                if k in ('flag', 'reason'):
                    continue
                rows.append({"組": name, "指標": k, "值": v})
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


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
        'ad_ratio': last.get('ad_ratio'),
        'new_high_low_diff': last.get('new_high_minus_low'),
        'breadth_thrust': last.get('breadth_thrust_10d'),
        'advances': last.get('advances'),
        'declines': last.get('declines'),
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
        c1.metric("ADL", f"{adl:,.0f}",
                  delta=f"{delta:+,.0f} vs MA20" if delta is not None else None)

    mco = breadth.get('mcclellan')
    if mco is not None:
        mco_color = "🔴" if mco < -100 else "🟠" if mco < -50 else "🟢" if mco > 50 else "⚪"
        c2.metric("McClellan", f"{mco:.0f}", delta=mco_color)

    ad = breadth.get('ad_ratio')
    if ad is not None:
        ad_label = "強" if ad > 1.5 else "弱" if ad < 0.6 else "中"
        c3.metric("A/D 量比", f"{ad:.2f}", delta=ad_label)

    nh_nl = breadth.get('new_high_low_diff')
    if nh_nl is not None:
        c4.metric("52w 新高-新低", f"{nh_nl:+.0f}",
                  delta="多頭" if nh_nl > 0 else "空頭")

    bt = breadth.get('breadth_thrust')
    if bt is not None:
        bt_label = "🚀 啟動" if bt > 0.65 else "正常"
        c5.metric("Breadth Thrust 10d", f"{bt:.2%}", delta=bt_label)

    advances = breadth.get('advances')
    declines = breadth.get('declines')
    if advances is not None and declines is not None:
        st.caption(f"今日漲家數 {int(advances)} / 跌家數 {int(declines)}")


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
        cols[0].metric("成交量 / M1B", f"{r:.1f}%",
                       delta=m1b.get('label', ''))

    # DXY
    dxy = macro.get('dxy_close')
    dxy_chg = macro.get('dxy_chg_4w')
    if dxy is not None:
        cols[1].metric("DXY (美元指數)", f"{dxy:.2f}",
                       delta=f"{dxy_chg:+.2f}% (4w)" if dxy_chg is not None else None)

    # USD/TWD
    usdtwd = macro.get('usdtwd_close')
    if usdtwd is not None:
        cols[2].metric("USD/TWD", f"{usdtwd:.2f}")

    # Fed Balance Sheet (WALCL)
    walcl = macro.get('fed_bs_trillion')
    walcl_chg = macro.get('fed_bs_chg_4w')
    if walcl is not None:
        cols[3].metric("Fed BS", f"${walcl:.2f}T",
                       delta=f"{walcl_chg:+.2f}% (4w)" if walcl_chg is not None else None)

    if not any([r, dxy, usdtwd, walcl]):
        st.info("⏳ 流動性面板資料尚未建立，請執行 `python tools/fetch_fred_macro.py`")


# ============================================================
#  Section 5：信用與景氣
# ============================================================

def _render_credit_cycle(macro: dict):
    st.markdown("### 📉 信用與景氣 (Credit & Business Cycle, 1-3mo lead)")
    cols = st.columns(4)

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

    # ISM 新訂單
    ism = macro.get('ism_new_orders')
    if ism is not None:
        c = '#FF4444' if ism < 45 else '#FF8800' if ism < 50 else '#00AA00'
        label = '收縮' if ism < 50 else '擴張'
        cols[3].metric("ISM 新訂單", f"{ism:.1f}", delta=label)

    # TW LEI
    df_tw = _safe_read_parquet(MACRO_DIR / "tw_business_cycle.parquet")
    if df_tw is not None and not df_tw.empty:
        last_tw = df_tw.iloc[-1]
        tw_lei_yoy = last_tw.get('tw_lei_yoy')
        export_diffusion = last_tw.get('export_diffusion')
        if tw_lei_yoy is not None or export_diffusion is not None:
            tw_cols = st.columns(3)
            if tw_lei_yoy is not None:
                c = '#FF4444' if tw_lei_yoy < 0 else '#00AA00'
                tw_cols[0].metric("TW 景氣領先 YoY", f"{tw_lei_yoy:+.2f}%",
                                  delta="衰退" if tw_lei_yoy < 0 else "擴張")
            if export_diffusion is not None:
                c = '#FF4444' if export_diffusion < 50 else '#00AA00'
                tw_cols[1].metric("出口訂單擴散", f"{export_diffusion:.1f}",
                                  delta="收縮" if export_diffusion < 50 else "擴張")
            tw_cols[2].caption(f"資料日期：{_fmt_date(last_tw.get('date'))}")


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

    # 巴菲特指標 (TW)
    buffett_tw = val.get('buffett_indicator_tw')
    buffett_rank = val.get('buffett_rank_tw')
    if buffett_tw is not None:
        c = _color_rank(buffett_rank, hi_is_bad=True)
        rank_str = f"rank {buffett_rank:.0f}" if buffett_rank is not None else ""
        cols[2].markdown(
            f'<div style="font-size:0.9rem">巴菲特指標 (TW)</div>'
            f'<div style="font-size:1.5rem;color:{c};font-weight:bold">{buffett_tw:.0f}%</div>'
            f'<div style="font-size:0.8rem;color:#888">{rank_str}</div>',
            unsafe_allow_html=True,
        )

    # 巴菲特指標 (US, Wilshire / GDP)
    buffett_us = val.get('buffett_indicator_us')
    buffett_us_rank = val.get('buffett_rank_us')
    if buffett_us is not None:
        c = _color_rank(buffett_us_rank, hi_is_bad=True)
        rank_str = f"rank {buffett_us_rank:.0f}" if buffett_us_rank is not None else ""
        cols[3].markdown(
            f'<div style="font-size:0.9rem">巴菲特指標 (US)</div>'
            f'<div style="font-size:1.5rem;color:{c};font-weight:bold">{buffett_us:.0f}%</div>'
            f'<div style="font-size:0.8rem;color:#888">{rank_str}</div>',
            unsafe_allow_html=True,
        )

    if not any([tw_pe, tw_yield, buffett_tw, buffett_us]):
        st.info("⏳ 估值面板資料尚未建立，請執行 `python tools/build_valuation_panel.py`")


# ============================================================
#  主入口
# ============================================================

def _render_ai_report_section():
    """AI 風向研究報告 — 雙 LLM (Claude Opus + Gemini 3.1 Pro) + Sonnet council。"""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    with st.expander("🤖 AI 風向研究報告 (Claude Opus + Gemini 雙視角)", expanded=False):
        col1, col2 = st.columns([1, 2])
        with col1:
            if st.button("🚀 產生新報告", type="primary",
                         help="呼叫 Claude Opus + Gemini 3.1 Pro，再用 Sonnet council 統整。約 5-15 分鐘。"):
                _run_ai_report_async()

        with col2:
            # 顯示最新報告 metadata
            metas = sorted(REPORTS_DIR.glob("*.meta.json"))
            if metas:
                import json
                latest_meta = json.loads(metas[-1].read_text(encoding='utf-8'))
                st.caption(
                    f"最新報告：{latest_meta.get('datetime', 'N/A')[:19]} | "
                    f"Claude: {'✅' if latest_meta.get('claude_ok') else '❌'} "
                    f"Gemini: {'✅' if latest_meta.get('gemini_ok') else '❌'} "
                    f"Council: {'✅' if latest_meta.get('council_ok') else '❌'}"
                )
            else:
                st.caption("尚無報告。點選左方「產生新報告」啟動 5-15 分鐘的雙 LLM 分析。")

        # 報告選單
        htmls = sorted(REPORTS_DIR.glob("*.html"), reverse=True)
        htmls = [h for h in htmls if h.name != 'latest.html']
        if htmls:
            options = ['(最新)'] + [h.stem for h in htmls[:20]]
            sel = st.selectbox("查看歷史報告", options=options, index=0)
            target = REPORTS_DIR / "latest.html" if sel == '(最新)' else REPORTS_DIR / f"{sel}.html"
            if target.exists():
                html = target.read_text(encoding='utf-8')
                st.components.v1.html(html, height=900, scrolling=True)


def _run_ai_report_async():
    """背景呼叫 macro_compass_report.py，通過 session_state 顯示進度。"""
    import subprocess
    import sys as _sys

    cmd = [_sys.executable, str(REPO / "tools" / "macro_compass_report.py")]
    st.info(f"🔄 已啟動報告產生器（{datetime.now().strftime('%H:%M:%S')}）。"
            "預計 5-15 分鐘，請保持頁面開啟，完成後重新整理本 expander 即可看到。")

    # 用 Popen 不阻塞 UI
    log_path = REPO / "macro_compass_report.log"
    with open(log_path, 'w', encoding='utf-8') as f:
        subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=str(REPO))
    st.caption(f"日誌：{log_path}")


def render_macro_dashboard():
    """總經大盤風向 tab 主入口。"""
    st.markdown("# 🧭 總經大盤風向 (Macro Compass)")
    st.caption("整合 籌碼 / 廣度 / 情緒 / 流動性 / 信用景氣 / 估值 — informational tier (SOP-14)")

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

    # Section 0: 總風向
    compass = _compute_compass_verdict(banner_data, sys_chip, breadth, macro, valuation)
    _render_compass_card(compass)

    # AI 報告（緊接總風向卡片）
    _render_ai_report_section()

    # Section 1: 機構撤退
    _render_systemic_chip(sys_chip)
    st.markdown("---")

    # Section 2: 市場廣度
    _render_breadth(breadth)
    st.markdown("---")

    # Section 3: 情緒與波動
    _render_sentiment_section()
    st.markdown("---")

    # Section 4: 流動性與資金
    _render_liquidity(macro, banner_data)
    st.markdown("---")

    # Section 5: 信用與景氣
    _render_credit_cycle(macro)
    st.markdown("---")

    # Section 6: 估值
    _render_valuation(valuation)

    # footer
    st.markdown("---")
    st.caption(
        "資料來源：FRED (HY OAS / Yield Curve / DXY / VIX / Fed BS)、"
        "主計處 (TW LEI / 出口訂單)、TWSE (廣度 / PE / 巴菲特)、"
        "FinMind (籌碼)、TDCC (集保)、TAIFEX (期權)。"
        f"渲染時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
