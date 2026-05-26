"""市場掃描 view (Phase D 從 app.py 抽出)

對應 app_mode == 'market_scan' 的整段邏輯，目前 2 個 tab:
- 法人週榜 (BL-4 三大法人週報，4 維度 × 6 排行：連買連賣天數 + 當週買賣超金額 + 當週買賣超張數)
- 當週成交活躍榜 (個股總成交金額/張數 Top 30，從 ohlcv_tw.parquet 即時計算)

窗口邏輯：取「當週週一 → 最新可得交易日」全部交易日 (skip 國定假日)，
不硬寫 5 天，避免 5/1 勞動節等假期週誤算。
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parent
OHLCV_PARQUET = REPO / "data_cache" / "backtest" / "ohlcv_tw.parquet"
UNIVERSE_PARQUET = REPO / "data_cache" / "backtest" / "universe_tw_full.parquet"


@st.cache_data(ttl=3600)
def _compute_weekly_turnover(top_n: int = 30):
    """從 ohlcv_tw.parquet 計算當週個股成交活躍榜。

    窗口：當週週一 → 最新可得交易日 (per ohlcv 實際日期，skip 假日)。
    過濾：只留普通股 (排除 ETF / 權證)。
    回 (metadata, {'amount': df_amount_top, 'volume': df_volume_top}) 或 (None, None)。
    """
    if not OHLCV_PARQUET.exists():
        return None, None

    df = pd.read_parquet(
        OHLCV_PARQUET,
        columns=['stock_id', 'stock_name', 'date', 'Close', 'Volume'],
    )
    df['date'] = pd.to_datetime(df['date'])
    df['stock_id'] = df['stock_id'].astype(str)

    # 找出有「廣覆蓋」的最新交易日 (>= 500 檔報告) 作 week_end，
    # 避免 ohlcv 部分 stale (e.g., 只有 6 檔 IPO 更新到 4/30) 導致窗口失效
    daily_counts = df.groupby('date')['stock_id'].nunique().sort_index()
    fully_covered = daily_counts[daily_counts >= 500]
    if fully_covered.empty:
        return None, None
    week_end = pd.Timestamp(fully_covered.index[-1])

    # 當週週一 → week_end，取所有實際出現的交易日 (skip 假日)
    monday = week_end - pd.Timedelta(days=week_end.weekday())
    available = sorted(df['date'].unique())
    window = sorted([d for d in available if monday <= d <= week_end])

    sub = df[df['date'].isin(window)].copy()
    if sub.empty:
        return None, None
    sub['turnover'] = sub['Close'] * sub['Volume']
    grouped = sub.groupby('stock_id', as_index=False).agg(
        weekly_volume=('Volume', 'sum'),
        weekly_turnover=('turnover', 'sum'),
        days=('date', 'nunique'),
        stock_name=('stock_name', 'last'),
    )

    # 過濾 ETF / 權證 (只保留普通股)
    if UNIVERSE_PARQUET.exists():
        try:
            u = pd.read_parquet(
                UNIVERSE_PARQUET,
                columns=['stock_id', 'is_common_stock'],
            )
            u['stock_id'] = u['stock_id'].astype(str)
            common = set(u.loc[u['is_common_stock'] == True, 'stock_id'])
            grouped = grouped[grouped['stock_id'].isin(common)]
        except Exception:
            grouped = grouped[grouped['stock_id'].str.len() == 4]
            grouped = grouped[~grouped['stock_id'].str.startswith('00')]
    else:
        grouped = grouped[grouped['stock_id'].str.len() == 4]
        grouped = grouped[~grouped['stock_id'].str.startswith('00')]

    # weekly_turnover 單位是 NTD; weekly_volume 單位是「股」(1 張 = 1000 股)
    grouped['weekly_amount_b'] = (grouped['weekly_turnover'] / 1e8).round(2)  # 億元
    grouped['weekly_volume_lots'] = (grouped['weekly_volume'] / 1000).round(0).astype('int64')  # 張

    keep = ['stock_id', 'stock_name', 'days', 'weekly_amount_b', 'weekly_volume_lots']
    top_amount = (
        grouped.sort_values('weekly_amount_b', ascending=False).head(top_n)[keep].reset_index(drop=True)
    )
    top_amount.insert(0, 'rank', range(1, len(top_amount) + 1))
    top_volume = (
        grouped.sort_values('weekly_volume_lots', ascending=False).head(top_n)[keep].reset_index(drop=True)
    )
    top_volume.insert(0, 'rank', range(1, len(top_volume) + 1))

    metadata = {
        'week_end': week_end,
        'window_start': window[0],
        'window_end': window[-1],
        'window_days': len(window),
        'universe_size': len(grouped),
    }
    return metadata, {'amount': top_amount, 'volume': top_volume}


def render_market_scan():
    """渲染市場掃描 mode (2 tab: 法人週榜 / 成交活躍榜)。"""
    # ====================================================================
    #  📡 市場掃描 mode
    #  Tab 1: 法人週榜 (BL-4 三大法人週報，4 維度 × 6 排行 = 24 個 Top 10)
    #  Tab 2: 當週成交活躍榜 (個股總成交金額/張數 Top 30)
    # ====================================================================
    st.title("📡 市場掃描")

    _ms_tab_chip, _ms_tab_turnover = st.tabs(["📊 法人週榜", "📈 當週成交活躍榜"])

    # ----------------------------------------------------------------
    # Tab 1: 法人週榜
    # ----------------------------------------------------------------
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
                f"全市場掃描 · 4 維度 × 6 排行 = 24 個 Top 10 · "
                f"每週六 08:00 自動更新"
            )

            # 維度 selectbox
            _dim_choice = st.selectbox(
                "維度",
                options=['total', 'foreign', 'trust', 'dealer'],
                format_func=lambda d: _WC_DIM_LABELS[d],
                key='ms_dim_choice',
            )

            # 6 個 ranking 欄位顯示 (2 row × 3 col：第 1 row 買超，第 2 row 賣超)
            _row_buy = st.columns(3)
            _row_sell = st.columns(3)
            _rank_specs = [
                ('consec_buy', '🔥 連續買超天數 Top 10', _row_buy[0], 'days'),
                ('week_buy', '💰 當週買超金額 Top 10', _row_buy[1], 'amount'),
                ('week_buy_shares', '📦 當週買超張數 Top 10', _row_buy[2], 'shares'),
                ('consec_sell', '🧊 連續賣超天數 Top 10', _row_sell[0], 'days'),
                ('week_sell', '💸 當週賣超金額 Top 10', _row_sell[1], 'amount'),
                ('week_sell_shares', '📤 當週賣超張數 Top 10', _row_sell[2], 'shares'),
            ]
            for _rt_key, _rt_label, _col, _kind in _rank_specs:
                with _col:
                    st.markdown(f"**{_rt_label}**")
                    _rdf = _wc_rank(_dim_choice, _rt_key, top_n=10)
                    if _rdf.empty:
                        st.caption("(本週無此類標的)")
                        continue
                    if _kind == 'amount':
                        _disp = _rdf[['rank', 'stock_id', 'stock_name', 'consec_days', 'weekly_amount_k']].copy()
                        _disp.columns = ['#', 'ID', '名稱', '連續日', '金額(千)']
                        _disp['金額(億)'] = (_disp['金額(千)'] / 1e5).round(1)
                        _disp = _disp.drop(columns=['金額(千)'])
                        _col_cfg = {
                            '#': st.column_config.NumberColumn(width='small'),
                            'ID': st.column_config.TextColumn(width='small'),
                            '名稱': st.column_config.TextColumn(width='small'),
                            '連續日': st.column_config.NumberColumn(width='small'),
                            '金額(億)': st.column_config.NumberColumn(format="%+.1f"),
                        }
                    else:
                        # 連買連賣 + 張數版本都用 weekly_shares 欄位（連買連賣的 weekly_shares 即連續期間累計股數）
                        _disp = _rdf[['rank', 'stock_id', 'stock_name', 'consec_days', 'weekly_shares']].copy()
                        _disp.columns = ['#', 'ID', '名稱', '連續日', '股數']
                        # 股數 → 千張顯示 (1張=1000股，weekly_shares 是股；除 1e6 = 千張)
                        _disp['張數(千)'] = (_disp['股數'] / 1000 / 1000).round(1)
                        _disp = _disp.drop(columns=['股數'])
                        _col_cfg = {
                            '#': st.column_config.NumberColumn(width='small'),
                            'ID': st.column_config.TextColumn(width='small'),
                            '名稱': st.column_config.TextColumn(width='small'),
                            '連續日': st.column_config.NumberColumn(width='small'),
                            '張數(千)': st.column_config.NumberColumn(format="%+.1f"),
                        }
                    st.dataframe(_disp, hide_index=True, width='stretch',
                                  column_config=_col_cfg)

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

    # ----------------------------------------------------------------
    # Tab 2: 當週成交活躍榜
    # ----------------------------------------------------------------
    with _ms_tab_turnover:
        try:
            _to_md, _to_results = _compute_weekly_turnover(top_n=30)
        except Exception as e:
            st.error(f"❌ 成交活躍榜計算失敗: {type(e).__name__}: {e}")
            _to_md, _to_results = None, None

        if _to_md is None or _to_results is None:
            st.warning("⚠️ ohlcv 資料缺失或本週尚無交易日資料，請先跑 `python tools/build_ohlcv_tw.py`。")
        else:
            _w_start = _to_md['window_start'].strftime('%Y-%m-%d')
            _w_end = _to_md['window_end'].strftime('%Y-%m-%d')
            st.caption(
                f"統計窗口 **{_w_start} ~ {_w_end}** ({_to_md['window_days']} 個交易日，自動 skip 國定假日) · "
                f"Universe 普通股 {_to_md['universe_size']} 檔 (排除 ETF/權證) · "
                f"資料源: ohlcv_tw.parquet"
            )

            # 兩個表格並列顯示：成交金額 + 成交張數，全部 30 筆無 scrollbar
            _df_amt = _to_results['amount'].copy()
            _df_vol = _to_results['volume'].copy()

            _col_cfg_amt = {
                '#': st.column_config.NumberColumn(width='small'),
                'ID': st.column_config.TextColumn(width='small'),
                '名稱': st.column_config.TextColumn(width='medium'),
                '交易日': st.column_config.NumberColumn(width='small'),
                '週成交額(億)': st.column_config.NumberColumn(format="%.1f"),
                '週成交量(張)': st.column_config.NumberColumn(format="%,d"),
            }
            _col_cfg_vol = dict(_col_cfg_amt)

            _ms_col_a, _ms_col_b = st.columns(2)
            with _ms_col_a:
                st.markdown("**💰 週成交金額 Top 30**")
                _disp_amt = _df_amt[['rank', 'stock_id', 'stock_name', 'days',
                                     'weekly_amount_b', 'weekly_volume_lots']].copy()
                _disp_amt.columns = ['#', 'ID', '名稱', '交易日', '週成交額(億)', '週成交量(張)']
                # 30 rows: row=35px, header=38px, +3px padding = 1091
                st.dataframe(_disp_amt, hide_index=True, width='stretch',
                             column_config=_col_cfg_amt,
                             height=35 * (len(_disp_amt) + 1) + 3)
            with _ms_col_b:
                st.markdown("**📦 週成交張數 Top 30**")
                _disp_vol = _df_vol[['rank', 'stock_id', 'stock_name', 'days',
                                     'weekly_volume_lots', 'weekly_amount_b']].copy()
                _disp_vol.columns = ['#', 'ID', '名稱', '交易日', '週成交量(張)', '週成交額(億)']
                st.dataframe(_disp_vol, hide_index=True, width='stretch',
                             column_config=_col_cfg_vol,
                             height=35 * (len(_disp_vol) + 1) + 3)

            with st.expander("🔍 跳轉個股分析（從活躍榜挑股深入研究）", expanded=False):
                # 跳轉用 union 兩榜的 stock_id（金額榜 + 張數榜）
                _df = pd.concat([_df_amt, _df_vol]).drop_duplicates(subset='stock_id')
                _ids = _df['stock_id'].tolist()
                _name_map = dict(zip(_df['stock_id'], _df['stock_name']))
                if _ids:
                    _picked = st.selectbox(
                        "選股",
                        options=[''] + _ids,
                        format_func=lambda s: '— 請選擇 —' if not s else f"{s} {_name_map.get(s, '')}",
                        key='ms_turnover_jump_pick',
                    )
                    if _picked and st.button("🚀 跳到個股分析", key='ms_turnover_jump_btn'):
                        st.session_state['ticker_input'] = _picked
                        st.session_state['analysis_active'] = True
                        st.session_state['app_mode'] = 'analysis'
                        st.rerun()
