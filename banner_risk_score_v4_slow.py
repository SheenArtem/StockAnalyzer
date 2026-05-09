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

# IC validation V3 (commit cf74765 跑出 dedup_top8) — SOP-12 ✅ PASS at all horizons
#   60d IC=-0.422 / 40d=-0.348 / 20d=-0.246 (vs best single buffett_us -0.371)
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

    return {
        'composite': composite,
        'zone': zone,
        'zone_color': color,
        'breakdown': breakdown,
        'as_of': last_date.strftime('%Y-%m-%d') if last_date is not None else None,
        'horizon': '60d MDD (informational only, SOP-14)',
        'sop12_verdict': ('V3 dedup_top8 ✅ PASS — '
                          '60d IC=-0.422 / 40d=-0.348 / 20d=-0.246 vs best single -0.371; '
                          'lag-weighted + Pearson>0.75 dedup 移除 buffett 系列 11 個冗餘 features'),
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

    label_zh = {'green': '安全', 'yellow': '留意', 'orange': '警戒', 'unknown': '資料不足'}.get(zone, '?')
    emoji = {'green': '🟢', 'yellow': '🟡', 'orange': '🟠', 'unknown': '⚪'}.get(zone, '⚪')

    # 主分數 + 燈號 + 警語
    st.markdown(
        f'''
        <div style="border:2px solid {color};border-radius:12px;padding:14px;
                    background:linear-gradient(135deg, {color}11, {color}22);
                    margin-bottom:12px">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <div style="font-size:1.3rem;font-weight:bold;color:{color}">
              {emoji} Slow Track 60d : <span style="font-size:1.6rem">{composite:.1f}</span>
              <span style="margin-left:10px">{label_zh}</span>
            </div>
            <div style="font-size:0.78rem;color:#666">資料日期 {as_of}</div>
          </div>
          <div style="font-size:0.82rem;color:#666;margin-top:4px">
            6 leading features (lag 1-21d) IC-weighted composite．informational tier (SOP-14)，
            <strong>不接 portfolio rebalance</strong>．composite 60d IC=-0.402 marginally pass SOP-12
          </div>
        </div>
        ''',
        unsafe_allow_html=True,
    )

    # Breakdown table
    rows = []
    for feat, info in bk.items():
        rank = info.get('rank')
        rank_str = f"{rank:.0f}" if rank is not None else "N/A"
        rank_color = '#FF4444' if rank is not None and rank >= 85 else '#FF8800' if rank is not None and rank >= 65 else '#888'
        rows.append({
            "Feature": feat,
            "今日 rank": rank_str,
            "Weight": f"{info.get('weight', 0):.3f}",
            "Contribution": f"{info.get('contribution'):.2f}" if info.get('contribution') is not None else "N/A",
            "Source": info.get('panel', '-'),
        })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


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
