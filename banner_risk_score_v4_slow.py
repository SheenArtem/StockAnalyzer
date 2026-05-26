"""
banner_risk_score_v4_slow.py -- Slow-track composite (60d 區間警示)

對應 IC validation 2026-05-09 的 6 個 leading features (lag 1-21d)，組合成
informational tier slow-track score。

設計理念：
  - 既有 v3 banner (m1b/rv10/rv30/pcr_v/pcr_oi/fgi) 是 fast track，0-1 週 lead
  - v4 slow track 補 1-3 週 lead 缺口（macro/credit/外資撤退）
  - 兩 track 並列顯示，**不合併**避免 horizon mismatch
    (V1 42-feat composite 60d=-0.402 PASS；V2 89-feat composite=-0.293 FAIL；
     20d 一直在 noise level)

過 IC gate 的 6 leading features (per `reports/macro_panel_ic_validation_2026-05-09.md`):

| Feature | Source panel | IC 60d | Lag | Direction |
|---|---|---|---|---|
| `tlt_spy_ratio` | etf_flows | +0.317 | 3d | high=danger |
| `us_durable_yoy` | fred_panel | -0.274 | 1d | low=danger |
| `buffett_indicator_us` | valuation | -0.371 | 10d | high=danger |
| `st_louis_fsi` | fred_panel | +0.229 | 12d | high=danger |
| `margin_ratio_z_252d` | systemic_chip | +0.158 | 13d | high=danger |
| `foreign_holding_chg_4w` | systemic_chip | +0.183 | 16d | high=danger |
| `buffett_rank_us` | valuation | -0.165 | 21d | high=danger |

(buffett_rank_us 已是 100-rank 形式，特殊處理)

SOP-14 informational tier:
  - 不接 portfolio rebalance gate
  - 不發紅燈（最高 orange）
  - 文案禁「預警/預測」，使用「historical co-occurrence」
  - 提供 zone breakdown 讓 user 自行判斷
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
MACRO = REPO / "data" / "macro"
BREADTH = REPO / "data" / "breadth"

# IC validation V3 (commit cf74765 跑出 dedup_top8) -- top-8 list 仍是當前最強 composite。
#   原 V3 報告: 60d IC=-0.422 / 40d=-0.348 / 20d=-0.246 (vs best single buffett_us -0.371)
#   2026-05-10 V4 audit: panel input 漂移 (commit 3a1d741 修了 DXY 來源 + SBL/margin
#   stable-sample) 後 V3 重跑 IC = -0.329 / -0.275 / -0.194 vs single -0.371。
#   Top-8 list 沒變，但「預期 IC」應改 -0.33 (不是 -0.42)。詳見 V4 報告對照表。
# Top-8 features after Pearson>0.75 dedup (砍掉 buffett 高度相關 11+ features)
# Lag-weighted: 真 lead (1-30d)=1.0 / coincident (0)=0.7 / slow (>30d)=0.5
SLOW_FEATURES = {
    # rank 1 (lag 10, real lead, weight 1.0)
    'buffett_indicator_us':    {'weight': 0.371, 'high_is_danger': True,  'panel': 'valuation_panel', 'lag_factor': 1.0},
    # rank 7 (lag 0 coincident, weight 0.7)
    'us_buffett_strict_rank':  {'weight': 0.289 * 0.7, 'high_is_danger': False, 'panel': 'fred_panel', 'lag_factor': 0.7},
    # rank 8 (lag 1, real lead, weight 1.0)
    'us_durable_yoy':          {'weight': 0.274, 'high_is_danger': False, 'panel': 'fred_panel', 'lag_factor': 1.0},
    # rank 10 (lag 60, slow, weight 0.5)
    'fed_bs_trillion':         {'weight': 0.230 * 0.5, 'high_is_danger': True,  'panel': 'fred_panel', 'lag_factor': 0.5},
    # rank 11 (lag 12, real lead, weight 1.0)
    'st_louis_fsi':            {'weight': 0.229, 'high_is_danger': True,  'panel': 'fred_panel', 'lag_factor': 1.0},
    # rank 12 (lag 60, slow, weight 0.5)
    'buffett_rank_tw':         {'weight': 0.221 * 0.5, 'high_is_danger': True,  'panel': 'valuation_panel', 'lag_factor': 0.5},
    # rank 13 (lag 0 coincident, weight 0.7)
    'hyg_dollar_flow':         {'weight': 0.218 * 0.7, 'high_is_danger': False, 'panel': 'etf_flows', 'lag_factor': 0.7},
    # rank 14 (lag 16, real lead, weight 1.0) — NEW from Phase 3-C
    'usdjpy_close':            {'weight': 0.206, 'high_is_danger': False, 'panel': 'fred_panel', 'lag_factor': 1.0},
}

# Zone thresholds (P85 / P65 vs in-sample)
# Calibration TBD when N events accumulate; 先用對稱 33/66
ORANGE_THRESH = 70.0
YELLOW_THRESH = 50.0

# 表格顯示用：把英文變數名翻成一般人看得懂的中文 + 一句解讀
FEATURE_LABELS = {
    'buffett_indicator_us': {
        'name': '美股巴菲特指標',
        'desc': '美股總市值/GDP，越高代表估值越貴',
    },
    'us_buffett_strict_rank': {
        'name': '美股估值嚴格分位',
        'desc': 'CAPE 調整版 Buffett 排名，越高估值越貴',
    },
    'us_durable_yoy': {
        'name': '美國耐久財訂單年增率',
        'desc': '景氣領先指標，年增率越低越像衰退',
    },
    'fed_bs_trillion': {
        'name': 'Fed 資產負債表 (兆美元)',
        'desc': '聯準會放水規模，急縮=流動性收緊',
    },
    'st_louis_fsi': {
        'name': '聖路易聯儲金融壓力指數',
        'desc': '系統性金融壓力，越高代表信用越緊縮',
    },
    'buffett_rank_tw': {
        'name': '台股巴菲特指標分位',
        'desc': '台股總市值/GDP 排名，越高估值越貴',
    },
    'hyg_dollar_flow': {
        'name': '高收益債 ETF (HYG) 資金流',
        'desc': '信用偏好指標，資金流出=避險意願升高',
    },
    'usdjpy_close': {
        'name': '美元/日圓匯率',
        'desc': '套利交易指標，急貶代表風險偏好下降',
    },
}

PANEL_LABELS = {
    'valuation_panel': '估值面板',
    'fred_panel': 'FRED 總經',
    'etf_flows': 'ETF 資金流',
    'systemic_chip': '系統性籌碼',
    'tw_breadth': '台股廣度',
}


def _load_panel(name: str) -> Optional[pd.DataFrame]:
    """Load 1 of 5 panels by short name."""
    if name == 'fred_panel':
        path = MACRO / "fred_panel.parquet"
    elif name == 'etf_flows':
        path = MACRO / "etf_flows.parquet"
    elif name == 'valuation_panel':
        path = MACRO / "valuation_panel.parquet"
    elif name == 'systemic_chip':
        path = MACRO / "systemic_chip.parquet"
    elif name == 'tw_breadth':
        path = BREADTH / "tw_breadth.parquet"
    else:
        return None
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').set_index('date')
    return df


def _percentile_rank_today(panel_df: pd.DataFrame, col: str,
                            high_is_danger: bool, lookback_days: int = 2520) -> Optional[float]:
    """今天的 rolling-rank (0-100). 高分 = 危險 (依 direction 調整)."""
    if panel_df is None or col not in panel_df.columns:
        return None
    s = panel_df[col].dropna()
    if len(s) < 252:
        return None
    today_val = s.iloc[-1]
    hist = s.iloc[-lookback_days:] if len(s) > lookback_days else s
    rank = (hist <= today_val).mean() * 100
    if not high_is_danger:
        rank = 100 - rank
    return float(rank)


def compute_slow_track_score() -> dict:
    """計算今天的 slow track composite score。

    Returns
    -------
    {
      'composite': float (0-100) | None,
      'zone': 'green'/'yellow'/'orange'/'unknown',
      'zone_color': hex,
      'breakdown': {feat_name: {value, rank, weight, contribution, panel}},
      'as_of': last data date,
      'horizon': '60d MDD' (informational only),
    }
    """
    breakdown = {}
    weighted_sum = 0.0
    total_weight_used = 0.0
    last_date = None

    # Cache panels to avoid re-load
    panels = {}
    target_total_w = sum(c['weight'] for c in SLOW_FEATURES.values())

    for feat, conf in SLOW_FEATURES.items():
        panel_name = conf['panel']
        if panel_name not in panels:
            panels[panel_name] = _load_panel(panel_name)
        df = panels[panel_name]

        rank = _percentile_rank_today(df, feat, conf['high_is_danger'])
        value = float(df[feat].dropna().iloc[-1]) if df is not None and feat in df.columns and not df[feat].dropna().empty else None
        if df is not None and not df.empty:
            this_last = df.index[-1]
            if last_date is None or this_last > last_date:
                last_date = this_last

        if rank is None:
            breakdown[feat] = {
                'value': value, 'rank': None,
                'weight': conf['weight'],
                'lag_factor': conf.get('lag_factor', 1.0),
                'contribution': None,
                'panel': panel_name, 'missing': True,
            }
            continue

        contribution = rank * conf['weight']
        breakdown[feat] = {
            'value': value, 'rank': rank,
            'weight': conf['weight'],
            'lag_factor': conf.get('lag_factor', 1.0),
            'contribution': contribution,
            'panel': panel_name, 'missing': False,
        }
        weighted_sum += contribution
        total_weight_used += conf['weight']

    if total_weight_used >= 0.5 * target_total_w:
        composite = weighted_sum / total_weight_used
    else:
        composite = None

    if composite is None:
        zone, color = 'unknown', '#888888'
    elif composite >= ORANGE_THRESH:
        zone, color = 'orange', '#FF6600'
    elif composite >= YELLOW_THRESH:
        zone, color = 'yellow', '#FFAA00'
    else:
        zone, color = 'green', '#00AA44'

    # Best-single primary metric (per 2026-05-10 strategic decision (c)):
    # buffett_indicator_us 三 horizon IC 全勝 composite (60d=-0.371 / 40d=-0.329 / 20d=-0.281)
    # Single 為主 trigger，composite 為 sanity check / confirmation
    single_rank = breakdown.get('buffett_indicator_us', {}).get('rank')
    if single_rank is None:
        single_zone, single_color = 'unknown', '#888888'
    elif single_rank >= ORANGE_THRESH:
        single_zone, single_color = 'orange', '#FF6600'
    elif single_rank >= YELLOW_THRESH:
        single_zone, single_color = 'yellow', '#FFAA00'
    else:
        single_zone, single_color = 'green', '#00AA44'

    # Disagreement: |single - composite| > 15 points 視為 confirm 不一致
    if single_rank is not None and composite is not None:
        disagree = abs(single_rank - composite) > 15
    else:
        disagree = False

    return {
        'composite': composite,
        'zone': zone,
        'zone_color': color,
        'breakdown': breakdown,
        'as_of': last_date.strftime('%Y-%m-%d') if last_date is not None else None,
        'horizon': '60d MDD (informational only, SOP-14)',
        'sop12_verdict': ('V3 dedup_top8 (top-8 list 仍最強) — '
                          '60d IC=-0.33 / 40d=-0.28 / 20d=-0.19 (post 3a1d741 panel fix); '
                          'lag-weighted + Pearson>0.75 dedup 移除 buffett 系列 11 個冗餘 features'),
        # Strategic (c): single 主 + composite sanity (2026-05-10)
        'single': single_rank,
        'single_zone': single_zone,
        'single_color': single_color,
        'single_feature': 'buffett_indicator_us',
        'single_ic_60d': -0.371,
        'disagree': disagree,
    }


def render(score: dict):
    """在 macro_dashboard.py 內呼叫的 streamlit 渲染函式。"""
    import streamlit as st

    if score is None or score.get('composite') is None:
        st.info("⏳ Slow track 資料不足，請先執行 4 個 fetcher 建立 panel")
        return

    composite = score['composite']
    zone = score['zone']
    color = score['zone_color']
    bk = score.get('breakdown', {})
    as_of = score.get('as_of', 'N/A')

    # Strategic (c) 2026-05-10: single 主 + composite sanity check
    single = score.get('single')
    single_color = score.get('single_color', '#888888')
    single_zone = score.get('single_zone', 'unknown')
    disagree = score.get('disagree', False)

    label_zh = {'green': '安全', 'yellow': '留意', 'orange': '警戒', 'unknown': '資料不足'}.get(zone, '?')
    label_zh_s = {'green': '安全', 'yellow': '留意', 'orange': '警戒', 'unknown': '資料不足'}.get(single_zone, '?')
    emoji = {'green': '🟢', 'yellow': '🟡', 'orange': '🟠', 'unknown': '⚪'}.get(zone, '⚪')
    emoji_s = {'green': '🟢', 'yellow': '🟡', 'orange': '🟠', 'unknown': '⚪'}.get(single_zone, '⚪')

    # 主 metric: best single (buffett_indicator_us, IC=-0.371 三 horizon 全最強)
    # 副 metric: composite (8-feature dedup_top8, 為 sanity check / confirmation)
    single_str = f"{single:.1f}" if single is not None else "N/A"
    disagree_badge = (' <span style="background:#FF6600;color:white;padding:2px 8px;'
                      'border-radius:4px;font-size:0.7rem;margin-left:8px">⚠️ DISAGREE</span>'
                      ) if disagree else ''
    st.markdown(
        f'''
        <div style="border:2px solid {single_color};border-radius:12px;padding:14px;
                    background:linear-gradient(135deg, {single_color}11, {single_color}22);
                    margin-bottom:12px">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <div style="font-size:1.3rem;font-weight:bold;color:{single_color}">
              {emoji_s} Slow Track (主) : <span style="font-size:1.7rem">{single_str}</span>
              <span style="margin-left:10px">{label_zh_s}</span>
              {disagree_badge}
            </div>
            <div style="font-size:0.78rem;opacity:0.65">資料日期 {as_of}</div>
          </div>
          <div style="font-size:0.78rem;opacity:0.6;margin-top:6px;
                      border-top:1px solid {single_color}44;padding-top:6px">
            <span style="color:{color};font-weight:bold">{emoji} Composite (sanity)：{composite:.1f} {label_zh}</span>
            <span style="margin-left:12px;opacity:0.7">8-feature dedup_top8 confirmation</span>
          </div>
          <div style="font-size:0.78rem;opacity:0.65;margin-top:4px">
            <strong>主</strong>=美股巴菲特指標分位（IC 60d=-0.371，三 horizon 全最強的單一 leading 指標），
            <strong>副</strong>=8 指標 composite 互補確認．
            informational tier (SOP-14)，<strong>僅供觀察，不接 portfolio rebalance</strong>
          </div>
        </div>
        ''',
        unsafe_allow_html=True,
    )

    # Breakdown table
    st.caption("📋 8 個 leading indicator 細項（IC 驗證通過；**今日分位越高 = 越像歷史 60d 重挫前夕**）")
    rows = []
    for feat, info in bk.items():
        label = FEATURE_LABELS.get(feat, {'name': feat, 'desc': ''})
        panel_zh = PANEL_LABELS.get(info.get('panel', '-'), info.get('panel', '-'))
        rank = info.get('rank')
        rank_str = f"{rank:.0f}" if rank is not None else "N/A"
        rows.append({
            "指標": label['name'],
            "解讀": label['desc'],
            "今日分位": rank_str,
            "權重": f"{info.get('weight', 0):.3f}",
            "貢獻分": f"{info.get('contribution'):.2f}" if info.get('contribution') is not None else "N/A",
            "資料源": panel_zh,
        })
    if rows:
        st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    score = compute_slow_track_score()
    print(f"Composite: {score.get('composite')}")
    print(f"Zone: {score.get('zone')}")
    print(f"As of: {score.get('as_of')}")
    print("\nBreakdown:")
    for feat, info in score.get('breakdown', {}).items():
        rank = info.get('rank')
        print(f"  {feat}: rank={rank} weight={info['weight']:.3f} "
              f"value={info.get('value'):.4g}")
