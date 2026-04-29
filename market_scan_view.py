"""市場掃描 view (Phase D 從 app.py 抽出)

對應 app_mode == 'market_scan' 的整段邏輯，目前 1 個 tab:
- 法人週榜 (BL-4 三大法人週報，4 維度 × 4 排行 = 16 Top 10)
"""

import logging

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


def render_market_scan():
    """渲染市場掃描 mode (1 tab: 法人週榜)。"""
    # ====================================================================
    #  📡 市場掃描 mode (2026-04-27 新增)
    #  目前 1 個 tab: 法人週榜 (BL-4 三大法人週報，4 維度 × 4 排行 = 16 個 Top 10)
    #  未來可擴展 ETF 換手榜 / 月營收熱度 / 處置股清單 等 market-wide 報告
    # ====================================================================
    st.title("📡 市場掃描")

    _ms_tab_chip, = st.tabs(["📊 法人週榜"])

    with _ms_tab_chip:
        from weekly_chip_loader import (
            load_latest as _wc_load,
            get_metadata as _wc_meta,
            get_rankings as _wc_rank,
            DIM_LABELS_ZH as _WC_DIM_LABELS,
        )

        _wc_df = _wc_load()
        _wc_md = _wc_meta()
        if _wc_df is None or _wc_md is None:
            st.warning("⚠️ 週榜資料尚未產出。請先跑 `python tools/weekly_chip_report.py` 或等週六 08:00 自動 batch。")
        else:
            _week_end_str = _wc_md['week_end'].strftime('%Y-%m-%d')
            st.caption(
                f"統計窗口收尾於 **{_week_end_str}** · 共 {_wc_md['unique_stocks']} 檔上榜 · "
                f"全市場掃描 · 4 維度 × 4 排行 = 16 個 Top 10 · "
                f"每週六 08:00 自動更新"
            )

            # 維度 selectbox
            _dim_choice = st.selectbox(
                "維度",
                options=['total', 'foreign', 'trust', 'dealer'],
                format_func=lambda d: _WC_DIM_LABELS[d],
                key='ms_dim_choice',
            )

            # 4 個 ranking 欄位顯示
            _col_a, _col_b = st.columns(2)
            _rank_specs = [
                ('consec_buy', '🔥 連續買超天數 Top 10', _col_a),
                ('consec_sell', '🧊 連續賣超天數 Top 10', _col_b),
                ('week_buy', '💰 當週買超金額 Top 10', _col_a),
                ('week_sell', '💸 當週賣超金額 Top 10', _col_b),
            ]
            for _rt_key, _rt_label, _col in _rank_specs:
                with _col:
                    st.markdown(f"**{_rt_label}**")
                    _rdf = _wc_rank(_dim_choice, _rt_key, top_n=10)
                    if _rdf.empty:
                        st.caption("(本週無此類標的)")
                        continue
                    # 美化 columns 給 UI 顯示
                    _disp = _rdf[['rank', 'stock_id', 'stock_name', 'consec_days', 'weekly_amount_k']].copy()
                    _disp.columns = ['#', 'ID', '名稱', '連續日', '金額(千)']
                    # 金額單位千→億 顯示
                    _disp['金額(億)'] = (_disp['金額(千)'] / 1e5).round(1)
                    _disp = _disp.drop(columns=['金額(千)'])
                    st.dataframe(_disp, hide_index=True, use_container_width=True,
                                  column_config={
                                      '#': st.column_config.NumberColumn(width='small'),
                                      'ID': st.column_config.TextColumn(width='small'),
                                      '名稱': st.column_config.TextColumn(width='small'),
                                      '連續日': st.column_config.NumberColumn(width='small'),
                                      '金額(億)': st.column_config.NumberColumn(format="%+.1f"),
                                  })

            st.markdown("---")
            # 跳轉個股分析功能
            with st.expander("🔍 跳轉個股分析（從週榜挑股深入研究）", expanded=False):
                _all_ids = _wc_df[_wc_df['dim'] == _dim_choice]['stock_id'].unique().tolist()
                if _all_ids:
                    _picked = st.selectbox(
                        "選股",
                        options=[''] + _all_ids,
                        format_func=lambda s: '— 請選擇 —' if not s else f"{s} {_wc_df[_wc_df['stock_id']==s]['stock_name'].iloc[0]}",
                        key='ms_jump_pick',
                    )
                    if _picked and st.button("🚀 跳到個股分析", key='ms_jump_btn'):
                        st.session_state['ticker_input'] = _picked
                        st.session_state['analysis_active'] = True
                        st.session_state['app_mode'] = 'analysis'
                        st.rerun()

