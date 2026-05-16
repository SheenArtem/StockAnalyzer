"""Whale Picks tab — render top-K candidates from data/latest/whale_picks_top20.json.

Per docs/whale_picks_spec.md v0.4 informational tier — display only, no trade gating.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

REPO = Path(__file__).resolve().parent
JSON_PATH = REPO / "data" / "latest" / "whale_picks_top20.json"
SNAPSHOT_DIR = REPO / "data" / "whale_picks"


def _load_latest_json() -> dict | None:
    if not JSON_PATH.exists():
        return None
    try:
        return json.loads(JSON_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        st.error(f"無法讀取 {JSON_PATH}: {e}")
        return None


def _list_snapshots() -> list[str]:
    if not SNAPSHOT_DIR.exists():
        return []
    return sorted([f.stem for f in SNAPSHOT_DIR.glob('20*.parquet')], reverse=True)


def render_whale_picks() -> None:
    st.title("🐋 主力選股 (Whale Picks)")
    st.caption(
        "8-factor composite_parsi / industry-neutral / monthly rebalance / "
        "informational tier — 不接 portfolio gating"
    )

    obj = _load_latest_json()
    if obj is None:
        st.warning(
            "尚未產生 Whale Picks 名單。請先跑：\n"
            "```\npython tools/whale_picks_screener.py\n```\n"
            "或設 Windows Task Scheduler 跑 `run_whale_picks_monthly.bat`"
        )
        return

    # Header metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Asof", obj.get('asof', '?'))
    col2.metric("Universe", f"{obj.get('universe_size', 0):,}")
    col3.metric("Valid scored", f"{obj.get('valid_scored', 0):,}")
    col4.metric("Top-K", len(obj.get('top', [])))

    # Promotion warning
    st.warning(
        "⚠️ **永遠 informational tier** — Per SPEC §13: live winrate 預期 < backtest Sharpe 1.92 "
        "(survivorship + market regime drift)。**不可直接下單**，建議當 Mode D 觀察候選池。"
    )

    # Top-K table
    top = obj.get('top', [])
    if not top:
        st.info("Top list 為空。")
        return

    df = pd.DataFrame(top)
    show_cols = ['stock_id', 'stock_name', 'industry_category', 'composite_parsi',
                 'f_score', 'eps_yoy', 'dist_52w_high', 'turnover_log',
                 'stealth_volume_20d', 'revenue_score_6m_delta',
                 'f_score_4q_delta', 'capex_intensity', 'Close']
    df_show = df[[c for c in show_cols if c in df.columns]].copy()
    df_show.index = range(1, len(df_show) + 1)
    df_show.index.name = 'rank'

    # Format
    if 'eps_yoy' in df_show.columns:
        df_show['eps_yoy'] = df_show['eps_yoy'].apply(lambda v: f"{v*100:+.1f}%" if pd.notna(v) else "n/a")
    for col in ['composite_parsi', 'f_score', 'dist_52w_high', 'turnover_log',
                'stealth_volume_20d', 'revenue_score_6m_delta',
                'f_score_4q_delta', 'capex_intensity']:
        if col in df_show.columns:
            df_show[col] = df_show[col].apply(lambda v: f"{v:+.2f}" if pd.notna(v) else "n/a")
    if 'Close' in df_show.columns:
        df_show['Close'] = df_show['Close'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "n/a")

    st.dataframe(df_show, use_container_width=True)

    # Config expander
    with st.expander("ℹ️ 配置 / 方法論"):
        cfg = obj.get('config', {})
        st.markdown(f"**SPEC version**: v{cfg.get('spec_version', '?')}")
        st.markdown(f"**Standardization**: {cfg.get('standardization', '?')}")
        st.markdown(f"**K**: {cfg.get('K', '?')}")
        st.markdown("**Composite 8-factor weights**:")
        comp = cfg.get('composite', {})
        for f, w in comp.items():
            sign = "+" if w >= 0 else ""
            st.markdown(f"- `{f}`: {sign}{w}")
        st.markdown("---")
        st.markdown(
            "**Backtest performance (2021-2025 OOS walk-forward)**:\n"
            "- Sharpe **1.92** (vs B&H TWII 0.64, f_score 1.53)\n"
            "- CAGR **33.6%** (B&H 8.7%)\n"
            "- MDD **-10.0%** (B&H -28.9%)\n"
            "- WF positive windows **6/6 = 100%**\n"
            "- Cross-regime: Bull +0.121 / Bear +0.107 / Sideways +0.106 (全正)"
        )
        st.caption("詳見 `docs/whale_picks_spec.md` §13 + `reports/whale_picks_phase2_v11_ind_kgrid/report_v2.md`")

    # Historical snapshot picker
    snapshots = _list_snapshots()
    if len(snapshots) > 1:
        with st.expander("📅 歷史快照"):
            picked = st.selectbox("查看歷史日期", snapshots)
            try:
                snap_df = pd.read_parquet(SNAPSHOT_DIR / f"{picked}.parquet")
                snap_top = snap_df.dropna(subset=['composite_parsi']).nlargest(20, 'composite_parsi')
                cols = [c for c in show_cols if c in snap_top.columns]
                st.dataframe(snap_top[cols].reset_index(drop=True), use_container_width=True)
            except Exception as e:
                st.error(f"快照載入失敗: {e}")

    st.caption(
        f"資料生成於 {obj.get('asof', '?')}。下次更新：每月最末交易日（依 Task Scheduler 設定）。"
    )
