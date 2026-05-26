"""Whale Picks tab — 今日 BUY/SELL 訊號 + 歷史回測表格 + Top-K 持倉清單。

訊號定義 (per docs/whale_picks_spec.md v0.10, M15 rebal since 2026-05-22)：
  - BUY: M15 rebalance (每月 15 號或之前最後交易日) 新進 top-10 / alerts 模組偵測 7d 急升 rank
  - SELL: M15 rebalance 掉出 top-10 / alerts 偵測持倉 ≥ -15% drawdown

⚠️ SPEC §13 紅線：永遠不接自動下單；訊號展示用，下單由人類判斷。
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

REPO = Path(__file__).resolve().parent
JSON_PATH = REPO / "data" / "latest" / "whale_picks_top20.json"
SNAPSHOT_DIR = REPO / "data" / "whale_picks"
HOLDINGS_PATH = SNAPSHOT_DIR / "_active_holdings.json"
LEDGER_PATH = SNAPSHOT_DIR / "trade_ledger.parquet"
LEDGER_META_PATH = SNAPSHOT_DIR / "trade_ledger_meta.json"
PORTFOLIO_NAV_PATH = SNAPSHOT_DIR / "portfolio_nav.parquet"
PORTFOLIO_ANNUAL_PATH = SNAPSHOT_DIR / "portfolio_annual.parquet"
PORTFOLIO_STATS_PATH = SNAPSHOT_DIR / "portfolio_stats.json"


def _load_latest_json() -> dict | None:
    if not JSON_PATH.exists():
        return None
    try:
        return json.loads(JSON_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        st.error(f"無法讀取 {JSON_PATH}: {e}")
        return None


def _load_active_holdings() -> dict | None:
    if not HOLDINGS_PATH.exists():
        return None
    try:
        return json.loads(HOLDINGS_PATH.read_text(encoding='utf-8'))
    except Exception:
        return None


def _list_snapshots() -> list[str]:
    if not SNAPSHOT_DIR.exists():
        return []
    return sorted([f.stem for f in SNAPSHOT_DIR.glob('20*.parquet')], reverse=True)


def _color_pnl_tw(val):
    """TW 慣例：正報酬 = 紅、負報酬 = 綠 (跟歐美顏色相反)。NaN 不上色。"""
    if pd.isna(val):
        return ''
    if val > 0:
        return 'color: #d62728'  # 紅
    if val < 0:
        return 'color: #2ca02c'  # 綠
    return ''


@st.cache_data(ttl=300, show_spinner=False)
def _load_latest_snapshot_close() -> tuple[date | None, dict[str, float]]:
    """讀最新 daily snapshot, 回 (snapshot_date, {stock_id: Close}).

    2026-05-22 取代 mis.twse 即時報價：whale_picks 是 monthly hold，秒級即時沒必要，
    daily snapshot Close 已夠用且 instant (0 API call / 0 rate limit / 0 cold-start).
    """
    snapshots = _list_snapshots()
    if not snapshots:
        return None, {}
    latest_str = snapshots[0]
    try:
        df = pd.read_parquet(SNAPSHOT_DIR / f"{latest_str}.parquet", columns=['stock_id', 'Close'])
        return date.fromisoformat(latest_str), df.set_index('stock_id')['Close'].to_dict()
    except Exception:
        return None, {}


# =============================================================================
# Section 1 — 今日訊號 (BUY/SELL)
# =============================================================================

def _render_today_signals(obj: dict) -> None:
    """M15 rebalance 訊號 + 與上次 snapshot diff 的 BUY/SELL 列表。"""
    st.subheader("📡 今日訊號 (BUY / SELL)")
    st.caption(
        "BUY = M15 rebalance (每月 15 號或之前最後交易日) 新進 top-10，或 alerts 模組 7d 急升 rank。"
        "SELL = M15 rebalance 掉出 top-10，或 alerts 偵測持倉 -15% drawdown。"
        "**SPEC §13: 訊號展示用，永不自動下單。**"
    )

    holdings = _load_active_holdings()
    if holdings:
        rebal_date = holdings.get('rebalance_date', '?')
        reason = holdings.get('reason', '?')
        tickers = holdings.get('tickers', [])
        cA, cB, cC = st.columns(3)
        cA.metric("最後 rebalance", rebal_date)
        cB.metric("rebalance 觸發", reason)
        cC.metric("持倉檔數", len(tickers))

    snaps = _list_snapshots()
    if len(snaps) >= 2:
        latest_d, prev_d = snaps[0], snaps[1]
        try:
            snap_now = pd.read_parquet(SNAPSHOT_DIR / f"{latest_d}.parquet")
            snap_prev = pd.read_parquet(SNAPSHOT_DIR / f"{prev_d}.parquet")
            # 2026-05-16: composite_score 切為 default，但舊 snapshot (5e10f6e 前)
            # 只有 composite_parsi。各 snapshot 各自選可用的 score column 排序。
            def _pick_score_col(snap):
                return 'composite_score' if 'composite_score' in snap.columns else 'composite_parsi'
            score_now = _pick_score_col(snap_now)
            score_prev = _pick_score_col(snap_prev)
            top_now = snap_now.dropna(subset=[score_now]).nlargest(10, score_now)
            top_prev = snap_prev.dropna(subset=[score_prev]).nlargest(10, score_prev)
            ids_now = set(top_now['stock_id'])
            ids_prev = set(top_prev['stock_id'])
            buys = ids_now - ids_prev
            sells = ids_prev - ids_now
            holds = ids_now & ids_prev

            cBuy, cSell, cHold = st.columns(3)
            cBuy.metric("🟢 BUY 訊號", len(buys), help=f"從 {prev_d} → {latest_d} 新進 top-10")
            cSell.metric("🔴 SELL 訊號", len(sells), help=f"從 {prev_d} → {latest_d} 掉出 top-10")
            cHold.metric("⚪ 維持", len(holds))

            if buys:
                buy_rows = top_now[top_now['stock_id'].isin(buys)].copy()
                buy_rows['信號類型'] = '🟢 BUY (新進 top-10)'
                buy_rows = buy_rows[['信號類型', 'stock_id', 'stock_name',
                                      'industry_category', score_now, 'Close']].rename(
                    columns={'industry_category': '產業', score_now: '分數', 'Close': '當前收盤'}
                )
                buy_rows['分數'] = buy_rows['分數'].apply(lambda v: f"{v:+.2f}" if pd.notna(v) else "n/a")
                buy_rows['當前收盤'] = buy_rows['當前收盤'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "n/a")
                st.markdown("**🟢 今日 BUY 候選**")
                st.dataframe(buy_rows.reset_index(drop=True), width='stretch', hide_index=True)

            if sells:
                prev_lookup = top_prev.set_index('stock_id')
                sell_data = []
                for sid in sorted(sells):
                    if sid in prev_lookup.index:
                        r = prev_lookup.loc[sid]
                        sell_data.append({
                            '信號類型': '🔴 SELL (掉出 top-10)',
                            'stock_id': sid,
                            'stock_name': r.get('stock_name', '?'),
                            '產業': r.get('industry_category', ''),
                            '前次分數': f"{r[score_prev]:+.2f}" if pd.notna(r.get(score_prev)) else "n/a",
                            '前次收盤': f"{r['Close']:.2f}" if pd.notna(r.get('Close')) else "n/a",
                        })
                if sell_data:
                    st.markdown("**🔴 今日 SELL 候選**")
                    st.dataframe(pd.DataFrame(sell_data), width='stretch', hide_index=True)

            if not buys and not sells:
                st.info(f"與上次 snapshot ({prev_d}) 相比沒有新的 BUY/SELL 訊號。")
        except Exception as e:
            st.warning(f"BUY/SELL diff 計算失敗: {e}")
    else:
        st.info("尚未累積 2 份以上的歷史 snapshot — 等 daily scan 跑幾天後就有 BUY/SELL diff 訊號。")


# =============================================================================
# Section 2 — 今日 Top-10 持倉清單
# =============================================================================

def _render_current_holdings(obj: dict) -> None:
    """目前 Top-10 名單 — 視同 active BUY list。"""
    st.subheader("📋 今日 Top-10 持倉清單 (BUY List)")
    st.caption(f"M15 rebalance 之後 → 持有至下次 M15 rebalance；資料生成於 {obj.get('asof', '?')}")

    top = obj.get('top', [])
    if not top:
        st.info("Top list 為空。")
        return

    df = pd.DataFrame(top)
    show_cols = ['stock_id', 'stock_name', 'industry_category', 'composite_score',
                 'f_score', 'eps_yoy', 'dist_52w_high', 'turnover_log',
                 'stealth_volume_20d', 'revenue_score_6m_delta',
                 'f_score_4q_delta', 'capex_intensity', 'Close']
    df_show = df[[c for c in show_cols if c in df.columns]].copy()
    df_show.index = range(1, len(df_show) + 1)
    df_show.index.name = '排名'

    if 'eps_yoy' in df_show.columns:
        df_show['eps_yoy'] = df_show['eps_yoy'].apply(lambda v: f"{v*100:+.1f}%" if pd.notna(v) else "n/a")
    for col in ['composite_score', 'f_score', 'dist_52w_high', 'turnover_log',
                'stealth_volume_20d', 'revenue_score_6m_delta',
                'f_score_4q_delta', 'capex_intensity']:
        if col in df_show.columns:
            df_show[col] = df_show[col].apply(lambda v: f"{v:+.2f}" if pd.notna(v) else "n/a")
    if 'Close' in df_show.columns:
        df_show['Close'] = df_show['Close'].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "n/a")

    # 中文欄位 + 把標準化分數正負方向加進標題（負號因子加「(反向)」）
    df_show = df_show.rename(columns={
        'stock_id':               '股票代號',
        'stock_name':              '股票名稱',
        'industry_category':        '產業',
        'composite_score':          '綜合分數',
        'f_score':                  'F-Score (體質)',
        'eps_yoy':                  'EPS 年增率',
        'dist_52w_high':            '距 52 週高 (反向)',
        'turnover_log':             '成交值 log (反向)',
        'stealth_volume_20d':       '量縮爆量 (主力吸籌)',
        'revenue_score_6m_delta':   '營收 6 月改善',
        'f_score_4q_delta':         'F-Score 年增',
        'capex_intensity':          'Capex 強度 (反向)',
        'Close':                    '當前收盤',
    })

    st.dataframe(df_show, width='stretch')
    st.caption(
        "**(反向)** 標記的因子：分數越**高**反而**扣分**（例：成交值大 = 大型股不利、"
        "距 52 週高近 = 動能已盡、Capex 重 = 資本黑洞）。綜合分數已經是 7 因子加權後的結果，"
        "正分數 = 整體 favorable。"
    )


# =============================================================================
# Section 3a — 當前持倉即時損益 (Live Holdings PnL)
# =============================================================================

def _build_pnl_rows(tickers: list, snapshot_close: dict, default_drivers: str = 'n/a') -> pd.DataFrame:
    """Build PnL rows for either system tickers or alert adds."""
    rows = []
    for t in tickers:
        sid = str(t.get('stock_id', ''))
        entry = t.get('entry_close')
        latest = snapshot_close.get(sid)
        if entry and latest and entry > 0:
            pnl_pct = (latest - entry) / entry
        else:
            pnl_pct = None
        rows.append({
            'stock_id': sid,
            'stock_name': t.get('stock_name', ''),
            'industry': t.get('industry', ''),
            'entry_close': entry,
            'latest': latest,
            'entry_drivers': t.get('entry_drivers') or t.get('entry_date') or default_drivers,
            'entry_date': t.get('entry_date', ''),
            'pnl_pct': pnl_pct,
        })
    return pd.DataFrame(rows)


def _render_pnl_table(df: pd.DataFrame, show_entry_date: bool = False) -> None:
    """Render PnL table with TW color convention (red=+ / green=-)."""
    if len(df) == 0:
        return
    cols = {
        '股票': df['stock_id'] + ' ' + df['stock_name'].fillna(''),
        '產業': df['industry'].fillna(''),
        '進場價': df['entry_close'],
        '最近收盤': df['latest'],
        '損益%': df['pnl_pct'] * 100,
    }
    if show_entry_date:
        cols['進場日'] = df['entry_date'].fillna('')
    cols['進場理由'] = df['entry_drivers'].fillna('n/a')
    display = pd.DataFrame(cols)
    display = display.sort_values('損益%', ascending=False, na_position='last').reset_index(drop=True)
    styled = display.style.map(_color_pnl_tw, subset=['損益%'])
    st.dataframe(
        styled,
        width='stretch',
        hide_index=True,
        column_config={
            '進場價': st.column_config.NumberColumn('進場價', format="%.2f"),
            '最近收盤': st.column_config.NumberColumn('最近收盤', format="%.2f"),
            '損益%': st.column_config.NumberColumn('損益%', format="%+.1f%%",
                                                  help="(最近收盤 − 進場價) / 進場價"),
        },
    )


def _render_current_holdings_pnl() -> None:
    """當前 _active_holdings.json 對照最新 daily snapshot 收盤算 PnL + 顯示進場理由。

    2026-05-26 加 alert_adds 分組: 系統選股 (10) + Alert 加碼 (N) 分組顯示，PnL 總和算。
    """
    holdings = _load_active_holdings()
    if not holdings or not holdings.get('tickers'):
        st.info("📭 尚未產生當前持倉清單 (`_active_holdings.json`)。等下次 M15 rebalance (每月 15 號或之前最後交易日) 或手動跑 `python tools/whale_picks_alerts.py --update-holdings`。")
        return

    rebalance_date = holdings.get('rebalance_date', '?')
    reason = holdings.get('reason', '?')
    tickers = holdings['tickers']
    alert_adds = holdings.get('alert_adds') or []

    snapshot_date, snapshot_close = _load_latest_snapshot_close()
    snapshot_date_str = snapshot_date.isoformat() if snapshot_date else '?'

    total_n = len(tickers) + len(alert_adds)

    st.subheader("💼 當前持倉損益")
    st.caption(
        f"進場日: **{rebalance_date}** ({reason}) / 最近收盤: **{snapshot_date_str}** / "
        f"持有 **{total_n}** 檔 (系統 {len(tickers)} + Alert 加碼 {len(alert_adds)})。"
        f" 純 EOD 收盤對比 (monthly hold 不需秒級即時)，盤中即時價請查券商 app。"
    )

    sys_df = _build_pnl_rows(tickers, snapshot_close)
    alert_df = _build_pnl_rows(alert_adds, snapshot_close)
    all_df = pd.concat([sys_df, alert_df], ignore_index=True) if len(alert_df) else sys_df

    # 加總指標 (合計所有持倉)
    closed = all_df.dropna(subset=['pnl_pct'])
    cM1, cM2, cM3, cM4 = st.columns(4)
    cM1.metric("持倉檔數", f"{total_n}", help=f"有最新收盤: {len(closed)} 檔")
    if len(closed):
        avg_pnl = closed['pnl_pct'].mean() * 100
        cM2.metric("平均報酬", f"{avg_pnl:+.2f}%")
        cM3.metric("勝率", f"{(closed['pnl_pct'] > 0).mean() * 100:.0f}%",
                   help=f"{int((closed['pnl_pct'] > 0).sum())}/{len(closed)} 檔正報酬")
        cM4.metric("最佳/最差", f"{closed['pnl_pct'].max()*100:+.1f}% / {closed['pnl_pct'].min()*100:+.1f}%")
    else:
        cM2.metric("平均報酬", "n/a")
        cM3.metric("勝率", "n/a")
        cM4.metric("最佳/最差", "n/a")

    # 系統選股表
    st.markdown(f"#### 📡 系統選股 ({len(tickers)})")
    _render_pnl_table(sys_df, show_entry_date=False)

    # Alert 加碼表 (only render if any)
    if len(alert_adds):
        st.markdown(f"#### 🔔 Alert 加碼 ({len(alert_adds)})")
        st.caption("Mid-month BUY alert 觸發後 user 自帶外部資金手動加碼，下月 M15 rebal 強制結算或升級系統選股。")
        _render_pnl_table(alert_df, show_entry_date=True)

    with st.expander("ℹ️ 進場理由說明", expanded=False):
        st.markdown(
            "- 進場理由 = 進場日當天 composite_score 7 因子裡，"
            "對該股 ranking 貢獻最大的前 3 個（factor × weight 為正且最大）\n"
            "- 例如「近 52 週低點 / 上 20 日上半部 / Piotroski F-Score」= "
            "進場時這檔因為「股價接近年低 + 近期股價偏強 + 財務體質好」被選中\n"
            "- 完整因子對照表見 `tools/whale_picks_trade_ledger.py::FACTOR_LABEL_ZH`\n"
            "- Alert 加碼透過 `python tools/whale_picks_ledger_append.py --alert-add <stock_id>` 記入 ledger"
        )


# =============================================================================
# Section 3 — 歷史回測訊號 (Trade Ledger)
# =============================================================================

def _render_trade_ledger() -> None:
    """2021-2026 歷史回測：每筆 BUY/SELL position 的進出場記錄。"""
    # 上方先顯示「當前持倉即時損益」(每天最有用的 panel)
    _render_current_holdings_pnl()
    st.markdown("---")

    st.subheader("📊 歷史回測訊號 (Trade Ledger)")
    st.caption(
        "依 v0.10 production 策略 (M15 K=10 / industry-neutral / liquidity ≥ 10M TWD) "
        "在歷史每月 15 號 (週末延前一交易日) 實際會發出的 BUY/SELL 訊號 — 連續持有合併為單筆 position，純價格報酬。 "
        "**當前實際持倉看上方「💼 當前持倉即時損益」**。"
    )

    if not LEDGER_PATH.exists():
        st.warning(
            "尚未產生 trade ledger。請先跑：\n"
            "```\npython tools/whale_picks_trade_ledger.py --start 2021-01-01 --end 2026-04-30\n```\n"
            "加 `--with-reasons` 可生成 LLM 中文進出場理由 (Sonnet, ~30min)。"
        )
        return

    try:
        df = pd.read_parquet(LEDGER_PATH)
    except Exception as e:
        st.error(f"無法讀取 ledger: {e}")
        return

    meta = {}
    if LEDGER_META_PATH.exists():
        try:
            meta = json.loads(LEDGER_META_PATH.read_text(encoding='utf-8'))
        except Exception:
            pass

    # Header metrics
    closed = df[~df['still_holding']].copy()
    cM1, cM2, cM3, cM4, cM5 = st.columns(5)
    cM1.metric("總筆數", f"{len(df):,}", help=f"持有中 {int(df['still_holding'].sum())} 筆未計入勝率")
    cM2.metric("勝率", f"{(closed['pnl_pct'] > 0).mean() * 100:.1f}%" if len(closed) else "n/a")
    cM3.metric("平均報酬", f"{closed['pnl_pct'].mean() * 100:+.2f}%" if len(closed) else "n/a")
    cM4.metric("最佳", f"{closed['pnl_pct'].max() * 100:+.1f}%" if len(closed) else "n/a")
    cM5.metric("最差", f"{closed['pnl_pct'].min() * 100:+.1f}%" if len(closed) else "n/a")

    # Filters
    with st.expander("🔍 篩選", expanded=False):
        years = sorted(pd.to_datetime(df['entry_date']).dt.year.unique())
        sel_years = st.multiselect("年份 (進場年)", years, default=years, key='wp_ledger_years')
        industries = sorted([i for i in df['industry'].dropna().unique()])
        sel_inds = st.multiselect("產業", industries, default=industries, key='wp_ledger_inds')
        pnl_filter = st.radio("損益", ["全部", "🟢 獲利", "🔴 虧損", "🟡 持有中"],
                               horizontal=True, key='wp_ledger_pnl')
        max_hold = int(df['holding_months'].max()) if len(df) else 12
        min_hold = st.slider("最少持有月數", 1, max(max_hold, 2), 1, key='wp_ledger_minhold')

    mask = (
        pd.to_datetime(df['entry_date']).dt.year.isin(sel_years)
        & df['industry'].fillna('').isin(sel_inds + [''])
        & (df['holding_months'] >= min_hold)
    )
    if pnl_filter == "🟢 獲利":
        mask = mask & (df['pnl_pct'] > 0) & (~df['still_holding'])
    elif pnl_filter == "🔴 虧損":
        mask = mask & (df['pnl_pct'] < 0) & (~df['still_holding'])
    elif pnl_filter == "🟡 持有中":
        mask = mask & df['still_holding']
    fdf = df[mask].copy().sort_values('entry_date', ascending=False)

    if len(fdf) == 0:
        st.info("篩選後無 position。")
        return

    # Display table (損益% 用 numeric 讓 Streamlit 排序正常)
    # 持有中的 pnl_pct 設 NaN (空白顯示, 排序時 na_position='last')
    pnl_numeric = fdf.apply(
        lambda r: float('nan') if r['still_holding'] else float(r['pnl_pct']) * 100,
        axis=1,
    )
    display = pd.DataFrame({
        '股票': fdf['stock_id'] + ' ' + fdf['stock_name'].fillna(''),
        '產業': fdf['industry'].fillna(''),
        '進場日': pd.to_datetime(fdf['entry_date']).dt.strftime('%Y-%m-%d'),
        '進場價': fdf['entry_price'],
        '出場日': fdf['exit_date'].apply(
            lambda v: pd.Timestamp(v).strftime('%Y-%m-%d') if pd.notna(v) else "持有中"
        ),
        '出場價': fdf['exit_price'],
        '持有月': fdf['holding_months'],
        '損益%': pnl_numeric,
        '進場理由 (top driver)': fdf['entry_top_drivers'].fillna(''),
        '出場理由 (top driver)': fdf['exit_top_drivers'].fillna(''),
    })
    if 'entry_reason_zh' in fdf.columns and fdf['entry_reason_zh'].astype(str).str.len().max() > 0:
        display['🤖 LLM 進場理由'] = fdf['entry_reason_zh'].fillna('')
        display['🤖 LLM 出場理由'] = fdf['exit_reason_zh'].fillna('')

    # TW 慣例配色：正報酬 = 紅、負報酬 = 綠
    styled = display.reset_index(drop=True).style.map(_color_pnl_tw, subset=['損益%'])
    st.dataframe(
        styled,
        width='stretch',
        hide_index=True,
        height=500,
        column_config={
            '進場價': st.column_config.NumberColumn('進場價', format="%.2f"),
            '出場價': st.column_config.NumberColumn('出場價', format="%.2f"),
            '損益%': st.column_config.NumberColumn(
                '損益%', format="%+.1f%%",
                help="(出場價 − 進場價) / 進場價。持有中的部位顯示空白，排序時排到最後。",
            ),
            '持有月': st.column_config.NumberColumn('持有月', format="%d"),
        },
    )

    with st.expander("ℹ️ 回測 metadata + 方法論", expanded=False):
        if meta:
            st.markdown(f"**回測期間**: {meta.get('start', '?')} ~ {meta.get('end', '?')}")
            st.markdown(f"**重新平衡**: M15 (每月 15 號或之前最後交易日) / K={meta.get('K', '?')} / 流動性門檻 ≥ NT$ {meta.get('min_avg_tv_twd', 0) / 1e6:.0f}M")
            st.markdown(f"**生成時間**: {meta.get('generated_at', '?')}")
            st.markdown(f"**LLM 理由**: {'✅ 已生成' if meta.get('with_llm_reasons') else '❌ 未生成 (`--with-reasons` flag)'}")
            st.markdown(f"**Win rate** (含持有中): {meta.get('win_rate', 0) * 100:.1f}% / "
                        f"**平均報酬**: {meta.get('avg_pnl_pct', 0) * 100:+.2f}% / "
                        f"**中位數**: {meta.get('median_pnl_pct', 0) * 100:+.2f}%")
        st.markdown("---")
        st.markdown(
            "**回測說明**:\n"
            "- 每月 15 號 (週末延前一交易日) 依 composite_score 排名取 top-10，連續入榜合併成 1 個 position\n"
            "- 進場價 = 進場日收盤；出場價 = 掉出 top-10 那個 M15 rebal 日收盤\n"
            "- P&L = 純價格報酬 (exit/entry - 1)，**未扣手續費 + 證交稅**\n"
            "- 仍在 top-10 的最新 position 標「持有中」，不計入勝率\n"
            "- 此為 **歷史模擬**，live 績效預期受 survivorship + regime drift 拖累"
        )


# =============================================================================
# Section 4 — Portfolio-level backtest vs TWII
# =============================================================================

def _render_portfolio_backtest() -> None:
    """Equal-weight K=10 monthly portfolio NAV vs TWII benchmark."""
    if not PORTFOLIO_STATS_PATH.exists():
        return
    try:
        stats = json.loads(PORTFOLIO_STATS_PATH.read_text(encoding='utf-8'))
        nav = pd.read_parquet(PORTFOLIO_NAV_PATH)
        annual = pd.read_parquet(PORTFOLIO_ANNUAL_PATH)
    except Exception as e:
        st.warning(f"Portfolio backtest 讀取失敗: {e}")
        return

    st.subheader("📈 Portfolio-level Backtest vs TWII (B&H)")
    st.caption(
        f"K=10 equal-weight 每月 M15 rebal / 期間 {stats.get('start_date', '?')} ~ "
        f"{stats.get('end_date', '?')} / 未扣手續費 / 未含股息 / "
        f"資料生成於 {stats.get('generated_at', '?')[:10]}"
    )

    wp = stats.get('whale_picks', {})
    tw = stats.get('twii', {})
    dlt = stats.get('delta', {})

    # Headline metrics
    cA, cB, cC, cD, cE = st.columns(5)
    cA.metric("Whale Picks CAGR", f"{wp.get('cagr', 0)*100:+.2f}%",
              delta=f"{dlt.get('cagr_pp', 0):+.2f}pp vs TWII")
    cB.metric("Sharpe", f"{wp.get('sharpe', 0):.3f}",
              delta=f"{dlt.get('sharpe', 0):+.3f}")
    cC.metric("MDD", f"{wp.get('mdd', 0)*100:.2f}%",
              delta=f"{dlt.get('mdd_pp', 0):+.2f}pp", delta_color="inverse")
    cD.metric("Vol", f"{wp.get('vol', 0)*100:.2f}%",
              delta=f"{dlt.get('vol_pp', 0):+.2f}pp", delta_color="inverse")
    cE.metric("Beats TWII",
              f"{stats.get('annual_wins', 0)}/{stats.get('annual_years', 0)} 年",
              delta=f"{stats.get('annual_hit_rate', 0)*100:.0f}% hit rate")

    st.caption(
        f"Whale Picks total {wp.get('total_return', 0)*100:+.1f}% vs "
        f"TWII {tw.get('total_return', 0)*100:+.1f}% / "
        f"{wp.get('years', 0)} 年 {wp.get('days', 0)} 個交易日"
    )

    # NAV chart
    nav_chart = nav[['date', 'wp_nav', 'twii_nav']].dropna().copy()
    nav_chart = nav_chart.rename(columns={'wp_nav': 'Whale Picks', 'twii_nav': 'TWII (B&H)'})
    nav_chart = nav_chart.set_index('date')
    st.line_chart(nav_chart, height=350,
                  y_label='NAV (cumulative return + 1)')

    # Year-by-year alpha bar chart
    annual_display = annual.copy()
    annual_display['year'] = annual_display['year'].astype(str)
    annual_display['Alpha (pp)'] = (annual_display['alpha'] * 100).round(2)
    annual_display['Whale Picks (%)'] = (annual_display['whale_picks'] * 100).round(2)
    annual_display['TWII (%)'] = (annual_display['twii'] * 100).round(2)

    cTab1, cTab2 = st.columns([2, 1])
    with cTab1:
        st.markdown("**📊 Year-by-year alpha (Whale Picks − TWII, pp)**")
        st.bar_chart(annual_display.set_index('year')['Alpha (pp)'], height=250)
    with cTab2:
        st.markdown("**🏆 Year-by-year hits**")
        display_yr = annual_display[['year', 'Whale Picks (%)', 'TWII (%)', 'Alpha (pp)']]
        styled = display_yr.style.map(_color_pnl_tw, subset=['Whale Picks (%)', 'TWII (%)', 'Alpha (pp)'])
        st.dataframe(styled, width='stretch', hide_index=True, height=300)

    with st.expander("ℹ️ Caveats / 限制"):
        for c in stats.get('caveats', []):
            st.markdown(f"- {c}")


# =============================================================================
# Section 5 — 任意兩日 snapshot diff (進階)
# =============================================================================

def _render_history_diff() -> None:
    snapshots = _list_snapshots()
    if len(snapshots) <= 1:
        return
    with st.expander("📅 任意兩日 snapshot diff (進階)", expanded=False):
        colA, colB = st.columns(2)
        with colA:
            pick_a = st.selectbox("基準日 (A)", snapshots, index=min(1, len(snapshots) - 1), key='whale_diff_a')
        with colB:
            pick_b = st.selectbox("對照日 (B)", snapshots, index=0, key='whale_diff_b')

        try:
            snap_a = pd.read_parquet(SNAPSHOT_DIR / f"{pick_a}.parquet")
            snap_b = pd.read_parquet(SNAPSHOT_DIR / f"{pick_b}.parquet")
            # 舊 snapshot fallback：composite_score 不存在則用 composite_parsi
            col_a = 'composite_score' if 'composite_score' in snap_a.columns else 'composite_parsi'
            col_b = 'composite_score' if 'composite_score' in snap_b.columns else 'composite_parsi'
            top_a = set(snap_a.dropna(subset=[col_a]).nlargest(10, col_a)['stock_id'].tolist())
            top_b = set(snap_b.dropna(subset=[col_b]).nlargest(10, col_b)['stock_id'].tolist())
            entered = top_b - top_a
            exited = top_a - top_b
            kept = top_a & top_b

            col_in, col_out, col_keep = st.columns(3)
            col_in.metric("📈 新進", len(entered))
            col_out.metric("📉 掉出", len(exited))
            col_keep.metric("➖ 維持", len(kept))

            if entered or exited:
                name_lookup = snap_b.drop_duplicates('stock_id').set_index('stock_id')['stock_name'].to_dict()
                name_lookup_a = snap_a.drop_duplicates('stock_id').set_index('stock_id')['stock_name'].to_dict()

                if entered:
                    st.markdown(f"**📈 新進 (A→B, {len(entered)})**")
                    rows_in = [{'stock_id': s, 'stock_name': name_lookup.get(s, '?')} for s in sorted(entered)]
                    st.dataframe(pd.DataFrame(rows_in), width='stretch', hide_index=True)
                if exited:
                    st.markdown(f"**📉 掉出 (A→B, {len(exited)})**")
                    rows_out = [{'stock_id': s, 'stock_name': name_lookup_a.get(s, '?')} for s in sorted(exited)]
                    st.dataframe(pd.DataFrame(rows_out), width='stretch', hide_index=True)
        except Exception as e:
            st.error(f"diff 計算失敗: {e}")


# =============================================================================
# Main entry
# =============================================================================

def render_whale_picks() -> None:
    st.title("🐋 主力選股 (Whale Picks) — BUY/SELL 訊號")
    st.caption(
        "7-feature composite_score / industry-neutral / **M15 rebalance** K=10 / "
        "11 年 portfolio backtest CAGR 28% vs TWII 17% (Sharpe 1.67 vs 0.99)。"
    )

    with st.expander("📋 **operational SOP — 100% Whale Picks** (展開)", expanded=False):
        st.markdown("""
**這是 production 推薦策略**（2026-05-23 用戶拍板 simplified mode）

**資金配置**：
- 全資金 / 10 檔 = 每檔等權
- Cash buffer: 0%

**每月 1 件事 — M15 rebal day (每月 15 號或之前最後交易日)**：
1. 開「📡 今日訊號 (BUY/SELL)」看 BUY/SELL 名單
2. 開盤 ~10:00 執行：先全 SELL 再全 BUY（市價）
3. 結束，下月再來

**全年時數**：12 次 × 30 min ≈ 6 小時

**強制紀律**：
- M15 那天**強制換股**，不擇時、不情緒
- 月中任何信號（QM / 新聞 / Discord push）= **informational only**
- 看 NAV「年度」單位，不看「月度」

**完全不用做的事**：
- 每日盯盤 / 停損停利 / 個股研究 / 經濟新聞解讀 / Buy-sell timing

**預期表現** (10-11 年實證, 含手續費 + 股息估算)：
- CAGR ~20-25% (扣 -1.8% 交易成本 + 加 3-5% 股息)
- Sharpe ~1.5-1.7
- MDD ~-25-28%
- 8/11 年勝 TWII (73%)

**會略輸 TWII 的情境**：
- covid V 反彈 (2020) / AI bubble (2025) 那種急速上漲
- 略輸範圍 -1~-14pp，不大爆損
- **不要 panic 換策略** — 長期 alpha 來自其他 8/11 年

**詳細回測**：見下方「📈 Portfolio-level Backtest vs TWII」section
        """)

    obj = _load_latest_json()
    if obj is None:
        st.warning(
            "尚未產生 Whale Picks 名單。請先跑：\n"
            "```\npython tools/whale_picks_screener.py\n```\n"
            "或等 daily scan (`run_scanner.bat`) 自動跑 — 已整合進每日 stage"
        )
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Asof", obj.get('asof', '?'))
    col2.metric("Universe", f"{obj.get('universe_size', 0):,}")
    col3.metric("Valid scored", f"{obj.get('valid_scored', 0):,}")
    col4.metric("Top-K", len(obj.get('top', [])))

    st.warning(
        "⚠️ **SPEC §13 紅線**: 訊號為「下單參考」，**永不接自動下單**。"
        "live 績效預期低於 backtest 誠實 Sharpe 1.52 (K=10 composite_score, production default) / 1.46 (K=20 對照)，"
        "建議當 Mode D 觀察候選池。"
    )

    _render_today_signals(obj)
    st.divider()
    _render_current_holdings(obj)
    st.divider()
    _render_trade_ledger()
    st.divider()
    _render_portfolio_backtest()
    st.divider()
    _render_history_diff()

    with st.expander("ℹ️ 配置 / 方法論 (composite_score 7 因子)"):
        cfg = obj.get('config', {})
        st.markdown(f"**SPEC version**: v{cfg.get('spec_version', '?')}")
        st.markdown(f"**Standardization**: {cfg.get('standardization', '?')}")
        st.markdown(f"**K**: {cfg.get('K', '?')}")
        st.markdown("**Composite 7-feature weights**:")
        # config 從 v0.7 後分 weights_score / weights_parsi（向後相容 'composite'）
        comp = cfg.get('weights_score') or cfg.get('composite', {})
        for f, w in comp.items():
            sign = "+" if w >= 0 else ""
            st.markdown(f"- `{f}`: {sign}{w}")
        st.markdown("---")
        st.markdown(
            "**Backtest (2021-2025 OOS walk-forward, v13.2 4-blocker fix 誠實 baseline)**:\n"
            "- Sharpe **1.49** (composite_score, vs B&H TWII 0.73, composite_parsi 1.01)\n"
            "- CAGR **19.5%** (B&H 11.5%)\n"
            "- MDD **-12.3%** (B&H -28.9%)\n"
            "- ⚠️ 之前宣稱 Sharpe 1.52/1.70 含 look-ahead+survivor leak (aa045f6 揭露)\n"
            "- 詳見 `project_audit_4_blocker_fix` memory"
        )
        st.caption("詳見 `docs/whale_picks_spec.md` v0.8 + `reports/whale_picks_phase2_v13_blocker_fix/report_v2.md`")

    st.caption(
        f"資料生成於 {obj.get('asof', '?')}。**每日自動更新** (run_scanner.bat)，"
        f"M15 rebal 日 (每月 15 號或之前最後交易日) 自動 Discord push (`DISCORD_WEBHOOK_WHALE_PICKS`)。"
    )
